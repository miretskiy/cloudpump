// Package cloudpump_test contains end-to-end download benchmarks against the
// public NOAA GOES-16 S3 bucket (us-east-1, no AWS credentials required).
//
// Benchmark suites:
//
//   - BenchmarkDownload        cloudpump, best scheduler (io_uring on Linux)
//   - BenchmarkDownloadPwrite  cloudpump, forced pwrite(2) — isolates io_uring benefit
//   - BenchmarkDownloadSerial  cloudpump, j=1 — isolates O_DIRECT + mmap benefit
//   - BenchmarkDownloadNaive   AWS manager.Downloader (parallel, heap buffers, page cache)
//
// Every benchmark reports:
//   - MB/s via b.SetBytes
//   - vmstat(1) system metrics: cs/s, blk-procs, cpu-usr%, cpu-sys%
//   - process-level CPU time via getrusage(RUSAGE_SELF): cpu-usr-ms/op, cpu-sys-ms/op
//   - io_uring batch stats where applicable: uring-avg-batch, uring-max-batch
//
// # What BenchmarkDownloadNaive measures
//
// The "naive" baseline uses aws-sdk-go-v2's manager.Downloader configured with
// the same concurrency (GOMAXPROCS) and chunk size (4 MiB) as cloudpump. The
// HTTP transport is also identical. The only differences are:
//
//   - I/O path: manager writes via f.WriteAt (buffered, page cache) vs
//     cloudpump's O_DIRECT pwrite/io_uring (bypasses page cache)
//   - Memory: manager allocates heap buffers per chunk (GC-visible) vs
//     cloudpump's pre-allocated mmap slabs (GC-invisible)
//   - Pre-allocation: no Fallocate vs cloudpump's upfront extent reservation
//
// f.Sync() is called after the download so both benchmarks measure the same
// endpoint: data durably on NVMe (not merely in the dirty page cache).
//
// Suggested remote run:
//
//	CLOUDPUMP_BENCH_DIR=/instance_storage \
//	  go test -bench=. -benchtime=10x -count=1 -run='^$' -v .
package cloudpump_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	cloudpump "github.com/miretskiy/cloudpump"
	"github.com/miretskiy/cloudpump/cloud"
	"github.com/miretskiy/dio/iosched"
)

// ─── Corpus ───────────────────────────────────────────────────────────────────

const (
	benchBucket = "noaa-goes16"
	benchRegion = "us-east-1"
)

// benchFiles is the full NOAA GOES-16 corpus: 10 MB → 90 MB in ~10 MB steps.
var benchFiles = []struct {
	name string
	key  string
}{
	{"10MB", "ABI-L2-CMIPF/2024/001/23/OR_ABI-L2-CMIPF-M6C06_G16_s20240012310206_e20240012319520_c20240012319563.nc"},
	{"20MB", "ABI-L2-CMIPF/2024/001/19/OR_ABI-L2-CMIPF-M6C06_G16_s20240011930206_e20240011939520_c20240011939567.nc"},
	{"30MB", "ABI-L2-CMIPF/2024/001/10/OR_ABI-L2-CMIPF-M6C01_G16_s20240011000208_e20240011009516_c20240011009574.nc"},
	{"40MB", "ABI-L2-CMIPF/2024/001/23/OR_ABI-L2-CMIPF-M6C03_G16_s20240012330206_e20240012339514_c20240012339583.nc"},
	{"50MB", "ABI-L2-CMIPF/2024/001/11/OR_ABI-L2-CMIPF-M6C01_G16_s20240011150208_e20240011159516_c20240011159579.nc"},
	{"60MB", "ABI-L2-CMIPF/2024/001/12/OR_ABI-L2-CMIPF-M6C01_G16_s20240011240208_e20240011249516_c20240011249574.nc"},
	{"70MB", "ABI-L2-CMIPF/2024/001/20/OR_ABI-L2-CMIPF-M6C03_G16_s20240012040206_e20240012049514_c20240012049578.nc"},
	{"80MB", "ABI-L2-CMIPF/2024/001/19/OR_ABI-L2-CMIPF-M6C05_G16_s20240011940206_e20240011949514_c20240011949573.nc"},
	{"90MB", "ABI-L2-CMIPF/2024/001/17/OR_ABI-L2-CMIPF-M6C05_G16_s20240011740206_e20240011749514_c20240011749578.nc"},
}

// serialFiles is a subset used by BenchmarkDownloadSerial (j=1 is slow;
// testing a range still covers small, medium, and large objects).
var serialFiles = []struct{ name, key string }{
	benchFiles[1], // 20MB
	benchFiles[4], // 50MB
	benchFiles[6], // 70MB
	benchFiles[8], // 90MB
}

// ─── Shared state ─────────────────────────────────────────────────────────────

var (
	// uringEngine is the primary Engine; uses io_uring on Linux or pwrite fallback.
	// All BenchmarkDownload iterations share this engine (pool pre-allocated once).
	uringEngine *cloudpump.Engine

	// pwriteEngine forces pwrite(2) regardless of io_uring availability.
	// Identical settings to uringEngine except for the scheduler.
	pwriteEngine *cloudpump.Engine

	// serialEngine uses j=1 to isolate the O_DIRECT + mmap benefit
	// from the parallelism benefit.
	serialEngine *cloudpump.Engine

	// uringS3 / pwriteS3 are S3 clients whose HTTP transports are tuned to
	// match their respective engine's concurrency.
	uringS3  *awss3.Client
	pwriteS3 *awss3.Client

	// naiveS3 uses the same tuned transport as uringS3 so the HTTP path is
	// identical. The only difference versus cloudpump is the I/O path:
	// manager.Downloader writes through the page cache with heap buffers.
	naiveS3         *awss3.Client
	naiveDownloader *s3manager.Downloader

	benchDir     string
	benchTempDir bool
)

func TestMain(m *testing.M) {
	code := setupBench()
	if code == 0 {
		code = m.Run()
	}
	teardownBench()
	os.Exit(code)
}

func setupBench() int {
	// ── Output directory ─────────────────────────────────────────────────────
	benchDir = os.Getenv("CLOUDPUMP_BENCH_DIR")
	switch {
	case benchDir != "":
	case dirExists("/instance_storage"):
		benchDir = "/instance_storage"
	default:
		var err error
		benchDir, err = os.MkdirTemp("", "cloudpump-bench-*")
		if err != nil {
			slog.Error("cannot create bench temp dir", "err", err)
			return 1
		}
		benchTempDir = true
		slog.Warn("CLOUDPUMP_BENCH_DIR not set; using temp dir (page-cache active, NVMe results will differ)")
	}
	if err := os.MkdirAll(benchDir, 0o755); err != nil {
		slog.Error("mkdir bench dir", "dir", benchDir, "err", err)
		return 1
	}
	slog.Info("bench dir", "path", benchDir)

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(benchRegion),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
	)
	if err != nil {
		slog.Error("aws config", "err", err)
		return 1
	}

	// ── Engines ───────────────────────────────────────────────────────────────
	// 4 MiB chunks: ≥3 parallel requests for even a 10 MB file.
	uringEngine, err = cloudpump.NewEngine(cloudpump.WithChunkSize(4 << 20))
	if err != nil {
		slog.Error("uring engine", "err", err)
		return 1
	}

	pwSched, err := iosched.NewPwriteScheduler()
	if err != nil {
		slog.Error("pwrite scheduler", "err", err)
		return 1
	}
	pwriteEngine, err = cloudpump.NewEngine(
		cloudpump.WithChunkSize(4<<20),
		cloudpump.WithIOScheduler(pwSched),
	)
	if err != nil {
		slog.Error("pwrite engine", "err", err)
		return 1
	}

	serialEngine, err = cloudpump.NewEngine(
		cloudpump.WithChunkSize(4<<20),
		cloudpump.WithConcurrency(1),
	)
	if err != nil {
		slog.Error("serial engine", "err", err)
		return 1
	}

	// ── S3 clients ────────────────────────────────────────────────────────────
	uringS3 = awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.HTTPClient = uringEngine.HTTPClient()
	})
	pwriteS3 = awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.HTTPClient = pwriteEngine.HTTPClient()
	})
	// naiveS3 shares the engine's tuned HTTP transport so the comparison
	// isolates I/O path (page cache vs O_DIRECT) not transport config.
	naiveS3 = awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.HTTPClient = uringEngine.HTTPClient()
	})
	// naiveDownloader mirrors cloudpump's parallelism and chunk size so
	// the only variables are: heap allocations vs mmap slabs, page-cache
	// writes vs O_DIRECT, and no Fallocate vs upfront extent reservation.
	naiveDownloader = s3manager.NewDownloader(naiveS3, func(d *s3manager.Downloader) {
		d.PartSize    = int64(4 << 20)           // match cloudpump's chunk size
		d.Concurrency = runtime.GOMAXPROCS(0)    // match cloudpump's worker count
	})

	return 0
}

func teardownBench() {
	for _, eng := range []*cloudpump.Engine{uringEngine, pwriteEngine, serialEngine} {
		if eng != nil {
			_ = eng.Close()
		}
	}
	if benchTempDir && benchDir != "" {
		_ = os.RemoveAll(benchDir)
	}
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// ─── BenchmarkDownload (io_uring / best scheduler) ───────────────────────────

// BenchmarkDownload uses the cloudpump engine with the best available I/O
// scheduler (io_uring on Linux ≥5.1, pwrite(2) elsewhere). Reports MB/s,
// io_uring batch stats, and vmstat system metrics.
func BenchmarkDownload(b *testing.B) {
	runDownloadBench(b, uringEngine, uringS3, benchFiles[:])
}

// ─── BenchmarkDownloadPwrite (forced pwrite) ─────────────────────────────────

// BenchmarkDownloadPwrite is identical to BenchmarkDownload but uses
// pwrite(2) unconditionally. Comparing the two answers: "does io_uring
// batching reduce syscall overhead enough to matter for this workload?"
//
// Expected: similar MB/s (NIC/NVMe bound), lower cs/s with io_uring.
func BenchmarkDownloadPwrite(b *testing.B) {
	runDownloadBench(b, pwriteEngine, pwriteS3, benchFiles[:])
}

// ─── BenchmarkDownloadSerial (j=1, O_DIRECT isolation) ───────────────────────

// BenchmarkDownloadSerial restricts cloudpump to a single worker (j=1).
// With parallelism removed, the comparison against BenchmarkDownloadNaive
// isolates what O_DIRECT + pre-allocated mmap slabs contribute on their own.
//
// Expected: faster than Naive due to O_DIRECT avoiding page-cache pressure,
// but far slower than the parallel BenchmarkDownload due to single TCP stream.
func BenchmarkDownloadSerial(b *testing.B) {
	runDownloadBench(b, serialEngine, uringS3, serialFiles[:])
}

// ─── BenchmarkDownloadNaive (io.Copy baseline) ───────────────────────────────

// BenchmarkDownloadNaive is the baseline: a single GetObject → io.Copy →
// os.Create pipeline. No range splitting, no O_DIRECT, no mmap, no io_uring.
// The naiveS3 client uses the SDK's default http.Client (no tuning).
func BenchmarkDownloadNaive(b *testing.B) {
	for _, bf := range benchFiles {
		bf := bf
		b.Run(bf.name, func(b *testing.B) {
			size := headObject(b, naiveS3, bf.key)
			naiveDownloadKey = bf.key // thread key to naiveDownload via package var
			b.SetBytes(size)
			mon := startVmstat()
			ru0 := getrusage()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				dst := dstPath(bf.name, "naive", i)
				if err := naiveDownload(context.Background(), dst); err != nil {
					b.Fatal(err)
				}
				mustRemove(b, dst)
			}

			b.StopTimer()
			ru1 := getrusage()
			reportVmstat(b, mon.stop())
			reportCPU(b, ru0, ru1)
		})
	}
}

// ─── Shared benchmark runner ──────────────────────────────────────────────────

// runDownloadBench is the common body for all cloudpump download benchmarks.
// It runs each file as a sub-benchmark, captures vmstat, and reports
// io_uring batch stats (non-zero only when the engine uses URingScheduler).
func runDownloadBench(
	b *testing.B,
	eng *cloudpump.Engine,
	s3Client *awss3.Client,
	files []struct{ name, key string },
) {
	b.Helper()
	for _, bf := range files {
		bf := bf
		b.Run(bf.name, func(b *testing.B) {
			reader := cloud.NewS3Reader(s3Client, benchBucket, bf.key)
			size := headObject(b, s3Client, bf.key)
			b.SetBytes(size)

			// Snapshot scheduler counters BEFORE the run so we compute
			// only the delta attributable to this sub-benchmark.
			before := eng.SchedStats()
			mon := startVmstat()
			ru0 := getrusage()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				dst := dstPath(bf.name, "cp", i)
				if err := eng.Download(context.Background(), reader, dst); err != nil {
					b.Fatal(err)
				}
				mustRemove(b, dst)
			}

			b.StopTimer()
			ru1 := getrusage()
			reportVmstat(b, mon.stop())
			reportCPU(b, ru0, ru1)
			reportUringStats(b, before, eng.SchedStats())
		})
	}
}

// ─── naiveDownload ────────────────────────────────────────────────────────────

// naiveDownload uses manager.Downloader with the same concurrency and chunk
// size as cloudpump. The manager writes via f.WriteAt — buffered page-cache
// writes — whereas cloudpump uses O_DIRECT. This isolates the I/O path
// difference with all other variables (HTTP transport, parallelism, chunk
// size) held constant.
//
// f.Sync() is called so the benchmark measures time until data is on NVMe,
// not merely time until dirty pages are in the kernel's page cache.
func naiveDownload(ctx context.Context, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = naiveDownloader.Download(ctx, f, &awss3.GetObjectInput{
		Bucket: aws.String(benchBucket),
		Key:    aws.String(naiveDownloadKey),
	}); err != nil {
		return fmt.Errorf("manager.Download: %w", err)
	}
	return f.Sync()
}

// naiveDownloadKey is set by BenchmarkDownloadNaive before each sub-benchmark.
// The manager.Downloader doesn't take the key at construction time, so we
// thread it through a package-level variable (benchmarks are not concurrent).
var naiveDownloadKey string

// ─── vmstat monitor ───────────────────────────────────────────────────────────

// vmstatMonitor runs `vmstat 1` in the background and collects samples.
type vmstatMonitor struct {
	cmd *exec.Cmd
	buf bytes.Buffer
}

// startVmstat launches vmstat with 1-second intervals.
// Returns nil (silently) if vmstat is not available (non-Linux).
func startVmstat() *vmstatMonitor {
	m := &vmstatMonitor{}
	m.cmd = exec.Command("vmstat", "1")
	m.cmd.Stdout = &m.buf
	if err := m.cmd.Start(); err != nil {
		return nil // vmstat not available
	}
	return m
}

func (m *vmstatMonitor) stop() vmstatSample {
	if m == nil || m.cmd == nil {
		return vmstatSample{}
	}
	_ = m.cmd.Process.Kill()
	_ = m.cmd.Wait()
	return parseVmstat(m.buf.String())
}

// vmstatSample holds per-second averages from vmstat output.
type vmstatSample struct {
	AvgCS      float64 // context switches / second
	AvgBlocked float64 // processes blocked on I/O (b column)
	AvgUserCPU float64 // user CPU %
	AvgSysCPU  float64 // system CPU %
}

// parseVmstat parses `vmstat 1` output.
//
// vmstat column layout (17 fields):
//
//	r b swpd free buff cache si so bi bo in cs us sy id wa st
//	0 1  2    3    4    5    6  7  8  9  10 11 12 13 14 15 16
//
// We skip the 2 header lines and the first data line (boot averages).
func parseVmstat(output string) vmstatSample {
	var cs, blocked, us, sy []float64
	lines := strings.Split(strings.TrimSpace(output), "\n")
	// lines[0] = "procs …" header
	// lines[1] = "r  b  swpd …" header
	// lines[2] = first data line (since-boot averages — skip)
	// lines[3..] = 1-second samples
	for _, line := range lines[3:] {
		fields := strings.Fields(line)
		if len(fields) < 17 {
			continue
		}
		if v, err := strconv.ParseFloat(fields[1], 64); err == nil {
			blocked = append(blocked, v)
		}
		if v, err := strconv.ParseFloat(fields[11], 64); err == nil {
			cs = append(cs, v)
		}
		if v, err := strconv.ParseFloat(fields[12], 64); err == nil {
			us = append(us, v)
		}
		if v, err := strconv.ParseFloat(fields[13], 64); err == nil {
			sy = append(sy, v)
		}
	}
	return vmstatSample{
		AvgCS:      mean(cs),
		AvgBlocked: mean(blocked),
		AvgUserCPU: mean(us),
		AvgSysCPU:  mean(sy),
	}
}

func reportVmstat(b *testing.B, s vmstatSample) {
	b.Helper()
	if s.AvgCS == 0 && s.AvgSysCPU == 0 {
		return // vmstat not available or no samples collected
	}
	b.ReportMetric(s.AvgCS, "cs/s")
	b.ReportMetric(s.AvgBlocked, "blk-procs")
	b.ReportMetric(s.AvgUserCPU, "cpu-usr%")
	b.ReportMetric(s.AvgSysCPU, "cpu-sys%")
}

// ─── io_uring batch stats ─────────────────────────────────────────────────────

// reportUringStats computes the delta between before/after Stats snapshots and
// reports io_uring batching efficiency. For PwriteScheduler, before.Batches
// and after.Batches are both 0, so nothing is reported — which is intentional:
// "no batch stats" is itself evidence that pwrite made no batching calls.
func reportUringStats(b *testing.B, before, after iosched.Stats) {
	b.Helper()
	batches := after.Batches - before.Batches
	if batches == 0 {
		return
	}
	requests := after.Requests - before.Requests
	avgBatch := float64(requests) / float64(batches)
	b.ReportMetric(avgBatch, "uring-avg-batch")
	b.ReportMetric(float64(after.MaxBatch), "uring-max-batch")
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// headObject calls HeadObject once (outside timing) to get the true file size
// and pre-warm the TLS session. Skips the sub-benchmark on network error.
func headObject(b *testing.B, client *awss3.Client, key string) int64 {
	b.Helper()
	resp, err := client.HeadObject(context.Background(), &awss3.HeadObjectInput{
		Bucket: aws.String(benchBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		b.Skipf("HeadObject s3://%s/%s: %v (no network?)", benchBucket, key, err)
	}
	if resp.ContentLength == nil {
		b.Skipf("HeadObject returned nil ContentLength")
	}
	return *resp.ContentLength
}

func dstPath(name, prefix string, i int) string {
	return filepath.Join(benchDir, fmt.Sprintf("%s_%s_%d.nc", prefix, name, i))
}

func mustRemove(b *testing.B, path string) {
	b.Helper()
	if err := os.Remove(path); err != nil {
		b.Fatal(err)
	}
}

func mean(vs []float64) float64 {
	if len(vs) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vs {
		sum += v
	}
	return sum / float64(len(vs))
}

// ─── CPU time via getrusage ───────────────────────────────────────────────────

// getrusage returns the current process resource usage.
// Uses RUSAGE_SELF so it captures all goroutines in this process, including
// the io_uring coordinator goroutine and the Go runtime — giving the true
// CPU cost of each download path.
func getrusage() syscall.Rusage {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return ru
}

// tvNanos converts a syscall.Timeval to nanoseconds.
// Works on both Linux (int64 Usec) and Darwin (int32 Usec) by widening.
func tvNanos(tv syscall.Timeval) int64 {
	return int64(tv.Sec)*1e9 + int64(tv.Usec)*1e3
}

// reportCPU reports per-operation user-space and kernel CPU time.
//
// For io_uring vs pwrite: expect lower cpu-sys-ms/op with uring (batched
// syscalls) but potentially higher cpu-usr-ms/op (coordinator goroutine +
// channel overhead). The sum shows net CPU cost per download.
//
// For serial vs parallel: parallel has more total CPU (N goroutines) but
// lower elapsed time; the per-op CPU numbers capture the true compute cost.
func reportCPU(b *testing.B, before, after syscall.Rusage) {
	b.Helper()
	userNs := tvNanos(after.Utime) - tvNanos(before.Utime)
	sysNs := tvNanos(after.Stime) - tvNanos(before.Stime)
	b.ReportMetric(float64(userNs)/float64(b.N)/1e6, "cpu-usr-ms/op")
	b.ReportMetric(float64(sysNs)/float64(b.N)/1e6, "cpu-sys-ms/op")
}
