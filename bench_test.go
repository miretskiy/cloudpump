// Package cloudpump_test contains end-to-end download benchmarks against the
// public NOAA GOES-16 S3 bucket (us-east-1, no AWS credentials required).
//
// Two benchmark suites:
//
//   - BenchmarkDownload: cloudpump engine — parallel mmap slabs, O_DIRECT writes,
//     io_uring (on Linux), pre-allocated extents.
//   - BenchmarkDownloadNaive: baseline — single GetObject → io.Copy → os.Create,
//     default http.Client, no page-cache bypass, no parallelism.
//
// Run on the m7gd NVMe workstation:
//
//	go test -bench=. -benchtime=5x -count=3 -run='^$' -v .
//
// Override the write destination:
//
//	CLOUDPUMP_BENCH_DIR=/instance_storage/bench go test ...
package cloudpump_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	cloudpump "github.com/miretskiy/cloudpump"
	"github.com/miretskiy/cloudpump/cloud"
)

// ─── Benchmark corpus ────────────────────────────────────────────────────────

// benchBucket is the NOAA GOES-16 public bucket. No credentials required.
const (
	benchBucket = "noaa-goes16"
	benchRegion = "us-east-1"
)

// benchFiles is the NOAA GOES-16 corpus: 10 MB → 90 MB in ~10 MB steps.
// Sizes are approximate; each benchmark calls HeadObject for the true value.
var benchFiles = []struct {
	name string
	key  string
}{
	{
		name: "10MB",
		key:  "ABI-L2-CMIPF/2024/001/23/OR_ABI-L2-CMIPF-M6C06_G16_s20240012310206_e20240012319520_c20240012319563.nc",
	},
	{
		name: "20MB",
		key:  "ABI-L2-CMIPF/2024/001/19/OR_ABI-L2-CMIPF-M6C06_G16_s20240011930206_e20240011939520_c20240011939567.nc",
	},
	{
		name: "30MB",
		key:  "ABI-L2-CMIPF/2024/001/10/OR_ABI-L2-CMIPF-M6C01_G16_s20240011000208_e20240011009516_c20240011009574.nc",
	},
	{
		name: "40MB",
		key:  "ABI-L2-CMIPF/2024/001/23/OR_ABI-L2-CMIPF-M6C03_G16_s20240012330206_e20240012339514_c20240012339583.nc",
	},
	{
		name: "50MB",
		key:  "ABI-L2-CMIPF/2024/001/11/OR_ABI-L2-CMIPF-M6C01_G16_s20240011150208_e20240011159516_c20240011159579.nc",
	},
	{
		name: "60MB",
		key:  "ABI-L2-CMIPF/2024/001/12/OR_ABI-L2-CMIPF-M6C01_G16_s20240011240208_e20240011249516_c20240011249574.nc",
	},
	{
		name: "70MB",
		key:  "ABI-L2-CMIPF/2024/001/20/OR_ABI-L2-CMIPF-M6C03_G16_s20240012040206_e20240012049514_c20240012049578.nc",
	},
	{
		name: "80MB",
		key:  "ABI-L2-CMIPF/2024/001/19/OR_ABI-L2-CMIPF-M6C05_G16_s20240011940206_e20240011949514_c20240011949573.nc",
	},
	{
		name: "90MB",
		key:  "ABI-L2-CMIPF/2024/001/17/OR_ABI-L2-CMIPF-M6C05_G16_s20240011740206_e20240011749514_c20240011749578.nc",
	},
}

// ─── Shared state ─────────────────────────────────────────────────────────────

var (
	// benchEngine is the cloudpump engine shared across all BenchmarkDownload
	// iterations. Pre-allocated at construction; never rebuilt.
	benchEngine *cloudpump.Engine

	// benchS3 is the S3 client used by BenchmarkDownload, configured with
	// the engine's tuned HTTP transport (MaxIdleConnsPerHost == concurrency).
	benchS3 *awss3.Client

	// naiveS3 is the S3 client used by BenchmarkDownloadNaive, configured
	// with a plain http.Client so the comparison is fair: naive gets no
	// connection pooling tuning, no transport customisation.
	naiveS3 *awss3.Client

	// benchDir is the output directory. Defaults to /instance_storage on Linux
	// (the m7gd NVMe drive) or a system temp dir when that path is absent.
	// Override with CLOUDPUMP_BENCH_DIR.
	benchDir string

	// benchTempDir is true if benchDir was created by TestMain and should
	// be removed on exit.
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
		// Explicit override — trust it.
	case dirExists("/instance_storage"):
		// NVMe drive on m7gd workstation.
		benchDir = "/instance_storage"
	default:
		// Fallback for local dev (slower; page cache active).
		var err error
		benchDir, err = os.MkdirTemp("", "cloudpump-bench-*")
		if err != nil {
			slog.Error("cannot create bench temp dir", "err", err)
			return 1
		}
		benchTempDir = true
		slog.Warn("CLOUDPUMP_BENCH_DIR not set and /instance_storage absent; " +
			"using temp dir (page-cache active, results will differ from NVMe)")
	}
	if err := os.MkdirAll(benchDir, 0o755); err != nil {
		slog.Error("cannot create bench dir", "dir", benchDir, "err", err)
		return 1
	}
	slog.Info("bench dir", "path", benchDir)

	// ── AWS config (anonymous — public NOAA bucket) ───────────────────────────
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(benchRegion),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
	)
	if err != nil {
		slog.Error("aws config", "err", err)
		return 1
	}

	// ── cloudpump Engine ─────────────────────────────────────────────────────
	// 4 MiB chunks: gives ≥3 parallel requests for even the smallest (10 MB)
	// file, while amortising per-request overhead for the largest (90 MB).
	benchEngine, err = cloudpump.NewEngine(
		cloudpump.WithChunkSize(4 << 20),
	)
	if err != nil {
		slog.Error("engine init", "err", err)
		return 1
	}

	// ── S3 clients ────────────────────────────────────────────────────────────
	// benchS3 shares the engine's tuned transport for maximum connection reuse.
	benchS3 = awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.HTTPClient = benchEngine.HTTPClient()
	})
	// naiveS3 uses the SDK's own default transport — no tuning, no pooling.
	naiveS3 = awss3.NewFromConfig(cfg)

	return 0
}

func teardownBench() {
	if benchEngine != nil {
		_ = benchEngine.Close()
	}
	if benchTempDir && benchDir != "" {
		_ = os.RemoveAll(benchDir)
	}
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// ─── BenchmarkDownload ────────────────────────────────────────────────────────

// BenchmarkDownload measures cloudpump end-to-end download throughput.
//
// Each sub-benchmark downloads the same S3 object b.N times using the shared
// Engine (pre-allocated mmap pool, io_uring / pwrite, O_DIRECT on ext4/xfs).
// The destination file is removed after each iteration so NVMe write
// bandwidth is exercised on every pass.
//
// Interpretation:
//
//	BenchmarkDownload/50MB-32  5  1_234_567_890 ns/op  40.50 MB/s
//
// "40.50 MB/s" is the sustained end-to-end throughput averaged over 5 passes.
func BenchmarkDownload(b *testing.B) {
	for _, bf := range benchFiles {
		bf := bf
		b.Run(bf.name, func(b *testing.B) {
			reader := cloud.NewS3Reader(benchS3, benchBucket, bf.key)

			// HeadObject once (outside timing) to obtain the true file size
			// and pre-warm the TLS session for the first iteration.
			size, err := reader.Size(context.Background())
			if err != nil {
				b.Skipf("HeadObject s3://%s/%s: %v (no network?)", benchBucket, bf.key, err)
			}
			b.SetBytes(size)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				dst := filepath.Join(benchDir,
					fmt.Sprintf("cp_%s_%d.nc", bf.name, i))
				if err := benchEngine.Download(context.Background(), reader, dst); err != nil {
					b.Fatal(err)
				}
				if err := os.Remove(dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ─── BenchmarkDownloadNaive ───────────────────────────────────────────────────

// BenchmarkDownloadNaive is the baseline: a single GetObject call with no
// Range header, piped through io.Copy to an os.Create'd file.
//
// Differences vs BenchmarkDownload:
//   - Single HTTP connection (no parallel range requests).
//   - io.Copy with a 32 KiB heap buffer (no mmap slabs, no DMA-ready memory).
//   - os.Create (buffered writes, kernel page cache active).
//   - No Fallocate, no O_DIRECT, no io_uring.
//   - Default SDK http.Client (no MaxIdleConnsPerHost tuning).
//
// The gap between Naive and Download illustrates the combined benefit of
// cloudpump's architecture for a given file size and network path.
func BenchmarkDownloadNaive(b *testing.B) {
	for _, bf := range benchFiles {
		bf := bf
		b.Run(bf.name, func(b *testing.B) {
			// HeadObject once (outside timing) to get size and pre-warm TLS.
			resp, err := naiveS3.HeadObject(context.Background(), &awss3.HeadObjectInput{
				Bucket: aws.String(benchBucket),
				Key:    aws.String(bf.key),
			})
			if err != nil {
				b.Skipf("HeadObject s3://%s/%s: %v (no network?)", benchBucket, bf.key, err)
			}
			if resp.ContentLength == nil {
				b.Skipf("HeadObject returned nil ContentLength")
			}
			size := *resp.ContentLength
			b.SetBytes(size)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				dst := filepath.Join(benchDir,
					fmt.Sprintf("naive_%s_%d.nc", bf.name, i))
				if err := naiveDownload(context.Background(), naiveS3, benchBucket, bf.key, dst); err != nil {
					b.Fatal(err)
				}
				if err := os.Remove(dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// naiveDownload downloads key from bucket to dstPath using a single GetObject
// request and io.Copy. No range splitting, no O_DIRECT, no mmap.
func naiveDownload(ctx context.Context, client *awss3.Client, bucket, key, dstPath string) error {
	resp, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("GetObject: %w", err)
	}
	defer resp.Body.Close()

	f, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// io.Copy uses a 32 KiB heap buffer internally — standard Go I/O.
	if _, err := io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}
	return nil
}

