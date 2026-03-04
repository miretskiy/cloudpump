// Command cp is a bare-metal, maximum-throughput cloud ↔ local file pump.
//
// Usage:
//
//	cp [flags] <src> <dst>
//
// Direction is inferred from the URI schemes:
//
//	s3://bucket/key  → /mnt/nvme/file   download from S3
//	gs://bucket/obj  → /mnt/nvme/file   download from GCS
//	/mnt/nvme/file   → s3://bucket/key  upload to S3
//	/mnt/nvme/file   → gs://bucket/obj  upload to GCS
//
// Examples:
//
//	# Download with defaults (GOMAXPROCS workers, 1 MiB chunks)
//	cp s3://my-bucket/data/huge.bin /mnt/nvme/huge.bin
//
//	# Upload to S3 (minimum chunk 5 MiB for multipart)
//	cp -c $((8*1024*1024)) /mnt/nvme/huge.bin s3://my-bucket/data/huge.bin
//
//	# Force io_uring, 5 total attempts per chunk
//	cp --uring --retries 4 gs://my-bucket/data/huge.bin /mnt/nvme/huge.bin
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	cloudpump "github.com/miretskiy/cloudpump"
	"github.com/miretskiy/cloudpump/cloud"
	"github.com/miretskiy/dio/iosched"
)

func main() {
	if err := run(); err != nil {
		slog.Error("cloudpump failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		concurrency = flag.Int("j", runtime.GOMAXPROCS(0),
			"parallel chunk workers")
		chunkSize = flag.Int64("c", 1<<20,
			"chunk size in bytes (must be a multiple of 4096; ≥5 MiB for S3 uploads)")
		maxRetries  = flag.Int("retries", 2, "extra retry attempts per chunk (0 = no retry)")
		forceURing  = flag.Bool("uring", false, "use io_uring scheduler (Linux ≥5.1 only)")
		forcePwrite = flag.Bool("pwrite", false, "use synchronous pwrite(2) scheduler")
	)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr,
			"usage: cp [flags] <s3://… | gs://… | local-path> <s3://… | gs://… | local-path>")
		fmt.Fprintln(os.Stderr)
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		return errors.New("expected exactly 2 positional arguments")
	}
	if *forceURing && *forcePwrite {
		return errors.New("--uring and --pwrite are mutually exclusive")
	}

	src, dst := flag.Arg(0), flag.Arg(1)
	ctx := context.Background()

	// ── I/O Scheduler ────────────────────────────────────────────────────────
	var sched iosched.IOScheduler
	switch {
	case *forceURing:
		var err error
		sched, err = iosched.NewURingScheduler(iosched.URingConfig{})
		if err != nil {
			return fmt.Errorf("io_uring scheduler unavailable: %w", err)
		}
	case *forcePwrite:
		var err error
		sched, err = iosched.NewPwriteScheduler()
		if err != nil {
			return err
		}
	}

	// ── Engine ────────────────────────────────────────────────────────────────
	opts := []cloudpump.Option{
		cloudpump.WithConcurrency(*concurrency),
		cloudpump.WithChunkSize(*chunkSize),
	}
	if sched != nil {
		opts = append(opts, cloudpump.WithIOScheduler(sched))
	}
	eng, err := cloudpump.NewEngine(opts...)
	if err != nil {
		return fmt.Errorf("init engine: %w", err)
	}
	defer eng.Close()

	// ── Route by direction ────────────────────────────────────────────────────
	srcCloud, dstCloud := isCloudURI(src), isCloudURI(dst)
	switch {
	case srcCloud && !dstCloud:
		return runDownload(ctx, eng, src, dst, *maxRetries)
	case !srcCloud && dstCloud:
		return runUpload(ctx, eng, src, dst)
	case srcCloud && dstCloud:
		return errors.New("cross-cloud copy not yet supported; download then upload separately")
	default:
		return errors.New("at least one of src or dst must be a cloud URI (s3://, gs://)")
	}
}

// ─── Download ────────────────────────────────────────────────────────────────

func runDownload(ctx context.Context, eng *cloudpump.Engine, srcURI, dstPath string, maxRetries int) error {
	reader, isRetryable, err := buildReader(ctx, srcURI, eng)
	if err != nil {
		return err
	}

	eng.SetRetryPolicy(cloudpump.RetryPolicy{
		MaxAttempts: maxRetries + 1,
		IsRetryable: func(e error) bool {
			return isRetryable(e) || cloudpump.DefaultIsRetryable(e)
		},
		Backoff: cloudpump.ExponentialBackoff(100*time.Millisecond, 10*time.Second),
	})

	start := time.Now()
	slog.Info("download starting", "src", srcURI, "dst", dstPath)
	if err := eng.Download(ctx, reader, dstPath); err != nil {
		return err
	}
	slog.Info("download complete", "elapsed", time.Since(start).Round(time.Millisecond))
	return nil
}

// buildReader parses a cloud URI, constructs the chunk reader, and returns
// a provider-specific IsRetryable predicate to compose into the retry policy.
func buildReader(
	ctx context.Context,
	uri string,
	eng *cloudpump.Engine,
) (cloud.CloudChunkReader, func(error) bool, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid URI %q: %w", uri, err)
	}
	switch u.Scheme {
	case "s3":
		bucket, key := u.Host, strings.TrimPrefix(u.Path, "/")
		if bucket == "" || key == "" {
			return nil, nil, fmt.Errorf("invalid S3 URI %q: expected s3://bucket/key", uri)
		}
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("aws config: %w", err)
		}
		client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
			o.HTTPClient = eng.HTTPClient()
		})
		return cloud.NewS3Reader(client, bucket, key), cloud.S3IsRetryable, nil

	case "gs":
		bucket, object := u.Host, strings.TrimPrefix(u.Path, "/")
		if bucket == "" || object == "" {
			return nil, nil, fmt.Errorf("invalid GCS URI %q: expected gs://bucket/object", uri)
		}
		gcsClient, err := cloud.NewGCSClient(ctx, eng.HTTPClient())
		if err != nil {
			return nil, nil, fmt.Errorf("gcs client: %w", err)
		}
		return cloud.NewGCSReader(gcsClient.Bucket(bucket).Object(object)), cloud.GCSIsRetryable, nil

	default:
		return nil, nil, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
}

// ─── Upload ───────────────────────────────────────────────────────────────────

func runUpload(ctx context.Context, eng *cloudpump.Engine, srcPath, dstURI string) error {
	u, err := url.Parse(dstURI)
	if err != nil {
		return fmt.Errorf("invalid URI %q: %w", dstURI, err)
	}

	// Stat the source to get size (needed for S3 multipart validation).
	fi, err := os.Stat(srcPath)
	if err != nil {
		return fmt.Errorf("stat %q: %w", srcPath, err)
	}
	size := fi.Size()

	var writer cloud.CloudChunkWriter
	switch u.Scheme {
	case "s3":
		bucket, key := u.Host, strings.TrimPrefix(u.Path, "/")
		if bucket == "" || key == "" {
			return fmt.Errorf("invalid S3 URI %q: expected s3://bucket/key", dstURI)
		}
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return fmt.Errorf("aws config: %w", err)
		}
		client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
			o.HTTPClient = eng.HTTPClient()
		})
		w, err := cloud.NewS3MultipartWriter(ctx, client, bucket, key, size, eng.ChunkSize())
		if err != nil {
			return err
		}
		writer = w

	case "gs":
		bucket, object := u.Host, strings.TrimPrefix(u.Path, "/")
		if bucket == "" || object == "" {
			return fmt.Errorf("invalid GCS URI %q: expected gs://bucket/object", dstURI)
		}
		gcsClient, err := cloud.NewGCSClient(ctx, eng.HTTPClient())
		if err != nil {
			return fmt.Errorf("gcs client: %w", err)
		}
		writer = cloud.NewGCSWriter(ctx, gcsClient.Bucket(bucket).Object(object))

	default:
		return fmt.Errorf("unsupported scheme %q", u.Scheme)
	}

	start := time.Now()
	slog.Info("upload starting", "src", srcPath, "dst", dstURI, "size", size)
	if err := eng.Upload(ctx, srcPath, writer); err != nil {
		return err
	}
	slog.Info("upload complete", "elapsed", time.Since(start).Round(time.Millisecond))
	return nil
}

// isCloudURI reports whether s looks like a cloud storage URI.
func isCloudURI(s string) bool {
	return strings.HasPrefix(s, "s3://") || strings.HasPrefix(s, "gs://")
}
