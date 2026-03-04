// Package cloudpump_test contains end-to-end download benchmarks.
//
// By default the benchmarks run against the public NOAA GOES-16 S3 bucket
// (us-east-1, no credentials required).  Set CLOUDPUMP_BENCH_S3 to target
// any other object:
//
//	CLOUDPUMP_BENCH_S3=s3://dd-points-s-staging/<uuid>/v8_o2_....h5s \
//	  go test -bench=. -benchtime=5x -run=^$ .
//
// When CLOUDPUMP_BENCH_S3 is set the benchmark uses standard AWS credential
// resolution (env vars, ~/.aws/credentials, IMDS) instead of anonymous access.
//
// # Benchmark suites
//
//   - BenchmarkDownload          cloudpump, best scheduler (io_uring on Linux)
//   - BenchmarkDownloadPwrite    cloudpump, forced pwrite(2)
//   - BenchmarkDownloadSerial    cloudpump, j=1
//   - BenchmarkDownloadManager   AWS manager.Downloader (parallel, heap buffers, page cache)
//   - BenchmarkDownloadGetObject single GetObject → io.Copy (no range splitting)
//
// # What each benchmark measures
//
// BenchmarkDownloadGetObject: the absolute baseline. One HTTP request, one
// streaming io.Copy into a page-cache-backed file. Bottlenecked by single-
// stream TCP throughput; no parallelism overhead; lowest CPU of all methods.
//
// BenchmarkDownloadManager: AWS SDK parallel downloader. Splits the object
// into 4 MiB parts (GOMAXPROCS parallel requests) and writes via f.WriteAt
// into the page cache. Heap-allocates one buffer per part per round.
//
// BenchmarkDownload / BenchmarkDownloadPwrite: cloudpump with O_DIRECT,
// pre-allocated mmap slabs, fallocate, and (on Linux) io_uring batching.
// No page-cache involvement — data lands directly on NVMe.
//
// # Metrics reported per benchmark
//
//   - MB/s via b.SetBytes
//   - cpu-usr-ms/op, cpu-sys-ms/op via getrusage(RUSAGE_SELF)
//   - uring-avg-batch, uring-max-batch (cloudpump only, when io_uring active)
//
// # Recommended isolated run (NVMe-equipped Linux)
//
//	echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid
//	for BENCH in BenchmarkDownloadGetObject BenchmarkDownloadManager \
//	             BenchmarkDownload BenchmarkDownloadPwrite; do
//	  iostat -dx nvme1n1 1 > /tmp/iostat-${BENCH}.txt &
//	  CLOUDPUMP_BENCH_DIR=/instance_storage \
//	  CLOUDPUMP_BENCH_S3=s3://dd-points-s-staging/<uuid>/v8_o2_...h5s \
//	    perf stat -e cycles,instructions,cache-misses,page-faults \
//	    go test -bench=^${BENCH}$ -benchtime=10x -run=^$ \
//	            -cpuprofile=/tmp/${BENCH}.prof . 2>&1 | tee /tmp/${BENCH}.txt
//	  kill %1
//	done
package cloudpump_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
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

// benchBucket / benchRegion may be overridden by CLOUDPUMP_BENCH_S3.
var (
	benchBucket = "noaa-goes16"
	benchRegion = "us-east-1"
)

// benchFiles is the corpus used by all benchmarks. Populated by setupBench:
// either the public NOAA GOES-16 multi-file set, or a single object from
// CLOUDPUMP_BENCH_S3.
var benchFiles []struct{ name, key string }

// noaaBenchFiles is the default NOAA corpus: 10 MB → 90 MB in ~10 MB steps.
var noaaBenchFiles = []struct{ name, key string }{
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

// serialFiles is the NOAA subset used by BenchmarkDownloadSerial (j=1 is slow).
var serialFiles []struct{ name, key string }

// ─── Shared state ─────────────────────────────────────────────────────────────

var (
	uringEngine  *cloudpump.Engine
	pwriteEngine *cloudpump.Engine
	serialEngine *cloudpump.Engine

	uringS3  *awss3.Client
	pwriteS3 *awss3.Client

	// naiveS3 is shared by both naive benchmarks; uses the same tuned HTTP
	// transport as uringS3 so the comparison isolates I/O path, not transport.
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

	// ── Corpus and credentials ────────────────────────────────────────────────
	// CLOUDPUMP_BENCH_S3=s3://bucket/key targets a private object instead of
	// the default NOAA public corpus.
	ctx := context.Background()
	var cfgOpts []func(*config.LoadOptions) error
	cfgOpts = append(cfgOpts, config.WithRegion(benchRegion))

	if s3url := os.Getenv("CLOUDPUMP_BENCH_S3"); s3url != "" {
		bucket, key, err := parseS3URL(s3url)
		if err != nil {
			slog.Error("CLOUDPUMP_BENCH_S3 parse error", "url", s3url, "err", err)
			return 1
		}
		benchBucket = bucket
		// Use standard credential resolution (env, ~/.aws, IMDS) for private buckets.
		slog.Info("custom S3 target", "bucket", benchBucket, "key", key)
		benchFiles = []struct{ name, key string }{{"custom", key}}
		serialFiles = benchFiles
	} else {
		// Public NOAA bucket: anonymous access, no credentials needed.
		cfgOpts = append(cfgOpts, config.WithCredentialsProvider(aws.AnonymousCredentials{}))
		benchFiles = noaaBenchFiles
		serialFiles = []struct{ name, key string }{
			noaaBenchFiles[1], // 20MB
			noaaBenchFiles[4], // 50MB
			noaaBenchFiles[6], // 70MB
			noaaBenchFiles[8], // 90MB
		}
	}

	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		slog.Error("aws config", "err", err)
		return 1
	}

	// ── Engines ───────────────────────────────────────────────────────────────
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
	naiveS3 = awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.HTTPClient = uringEngine.HTTPClient()
	})
	naiveDownloader = s3manager.NewDownloader(naiveS3, func(d *s3manager.Downloader) {
		d.PartSize    = int64(4 << 20)
		d.Concurrency = runtime.GOMAXPROCS(0)
	})

	// When using a custom file, HEAD it once to get the real size and update
	// the corpus entry name so the report shows e.g. "21MB" not "custom".
	if len(benchFiles) == 1 && benchFiles[0].name == "custom" {
		resp, err := naiveS3.HeadObject(ctx, &awss3.HeadObjectInput{
			Bucket: aws.String(benchBucket),
			Key:    aws.String(benchFiles[0].key),
		})
		if err != nil {
			slog.Error("HeadObject custom file", "err", err)
			return 1
		}
		if resp.ContentLength != nil {
			benchFiles[0].name = fmt.Sprintf("%dMB", *resp.ContentLength>>20)
			serialFiles = benchFiles
		}
	}

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

// ─── BenchmarkDownloadGetObject (true naive baseline) ─────────────────────────

// BenchmarkDownloadGetObject is the absolute baseline: one GetObject request,
// one streaming io.Copy into a newly created file (page cache, no O_DIRECT).
//
// This isolates single-stream TCP throughput with no range splitting,
// no parallelism overhead, and minimal CPU overhead. Everything else
// (manager, cloudpump) adds complexity to exceed this ceiling.
func BenchmarkDownloadGetObject(b *testing.B) {
	for _, bf := range benchFiles {
		bf := bf
		b.Run(bf.name, func(b *testing.B) {
			size := headObject(b, naiveS3, bf.key)
			b.SetBytes(size)
			ru0 := getrusage()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				dst := dstPath(bf.name, "getobj", i)
				if err := getObjectDownload(context.Background(), bf.key, dst); err != nil {
					b.Fatal(err)
				}
				mustRemove(b, dst)
			}

			b.StopTimer()
			ru1 := getrusage()
			reportCPU(b, ru0, ru1)
		})
	}
}

// getObjectDownload fetches key with a single GetObject and streams it to dst
// via io.Copy. No range splitting, no parallelism, no O_DIRECT.
func getObjectDownload(ctx context.Context, key, dst string) error {
	resp, err := naiveS3.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(benchBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("GetObject: %w", err)
	}
	defer resp.Body.Close()

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}
	return f.Sync()
}

// ─── BenchmarkDownloadManager (parallel manager, page cache) ─────────────────

// BenchmarkDownloadManager uses aws-sdk-go-v2's manager.Downloader with the
// same concurrency (GOMAXPROCS) and chunk size (4 MiB) as cloudpump.
// Writes via f.WriteAt into the page cache with one heap buffer per chunk.
// f.Sync() is called so both manager and cloudpump measure time until data
// is durable on NVMe, not merely in dirty page cache.
func BenchmarkDownloadManager(b *testing.B) {
	for _, bf := range benchFiles {
		bf := bf
		b.Run(bf.name, func(b *testing.B) {
			size := headObject(b, naiveS3, bf.key)
			b.SetBytes(size)
			ru0 := getrusage()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				dst := dstPath(bf.name, "mgr", i)
				if err := managerDownload(context.Background(), bf.key, dst); err != nil {
					b.Fatal(err)
				}
				mustRemove(b, dst)
			}

			b.StopTimer()
			ru1 := getrusage()
			reportCPU(b, ru0, ru1)
		})
	}
}

func managerDownload(ctx context.Context, key, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = naiveDownloader.Download(ctx, f, &awss3.GetObjectInput{
		Bucket: aws.String(benchBucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("manager.Download: %w", err)
	}
	return f.Sync()
}

// ─── BenchmarkDownload (io_uring / best scheduler) ───────────────────────────

// BenchmarkDownload uses cloudpump with the best available I/O scheduler
// (io_uring on Linux ≥5.1, pwrite(2) elsewhere).
func BenchmarkDownload(b *testing.B) {
	runDownloadBench(b, uringEngine, uringS3, benchFiles)
}

// ─── BenchmarkDownloadPwrite (forced pwrite) ─────────────────────────────────

// BenchmarkDownloadPwrite forces pwrite(2) regardless of io_uring availability.
// Comparing to BenchmarkDownload answers: "does io_uring batching reduce
// syscall overhead enough to matter for this workload?"
func BenchmarkDownloadPwrite(b *testing.B) {
	runDownloadBench(b, pwriteEngine, pwriteS3, benchFiles)
}

// ─── BenchmarkDownloadSerial (j=1, O_DIRECT isolation) ───────────────────────

// BenchmarkDownloadSerial restricts cloudpump to a single worker (j=1).
// With parallelism removed, comparison against BenchmarkDownloadGetObject
// isolates what O_DIRECT + pre-allocated mmap slabs contribute on their own.
func BenchmarkDownloadSerial(b *testing.B) {
	runDownloadBench(b, serialEngine, uringS3, serialFiles)
}

// ─── Shared benchmark runner ──────────────────────────────────────────────────

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

			before := eng.SchedStats()
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
			reportCPU(b, ru0, ru1)
			reportUringStats(b, before, eng.SchedStats())
		})
	}
}

// ─── io_uring batch stats ─────────────────────────────────────────────────────

func reportUringStats(b *testing.B, before, after iosched.Stats) {
	b.Helper()
	batches := after.Batches - before.Batches
	if batches == 0 {
		return
	}
	requests := after.Requests - before.Requests
	b.ReportMetric(float64(requests)/float64(batches), "uring-avg-batch")
	b.ReportMetric(float64(after.MaxBatch), "uring-max-batch")
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

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

func parseS3URL(s3url string) (bucket, key string, err error) {
	u, err := url.Parse(s3url)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("expected s3:// scheme, got %q", u.Scheme)
	}
	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	if bucket == "" || key == "" {
		return "", "", fmt.Errorf("invalid S3 URL: bucket=%q key=%q", bucket, key)
	}
	return bucket, key, nil
}

func dstPath(name, prefix string, i int) string {
	return filepath.Join(benchDir, fmt.Sprintf("%s_%s_%d.bin", prefix, name, i))
}

func mustRemove(b *testing.B, path string) {
	b.Helper()
	if err := os.Remove(path); err != nil {
		b.Fatal(err)
	}
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// ─── CPU time via getrusage ───────────────────────────────────────────────────

func getrusage() syscall.Rusage {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return ru
}

func tvNanos(tv syscall.Timeval) int64 {
	return int64(tv.Sec)*1e9 + int64(tv.Usec)*1e3
}

func reportCPU(b *testing.B, before, after syscall.Rusage) {
	b.Helper()
	userNs := tvNanos(after.Utime) - tvNanos(before.Utime)
	sysNs := tvNanos(after.Stime) - tvNanos(before.Stime)
	b.ReportMetric(float64(userNs)/float64(b.N)/1e6, "cpu-usr-ms/op")
	b.ReportMetric(float64(sysNs)/float64(b.N)/1e6, "cpu-sys-ms/op")
}
