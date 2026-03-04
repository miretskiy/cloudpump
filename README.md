# cloudpump

**cloudpump** is a high-performance I/O toolkit for moving data between cloud
object storage (AWS S3, Google Cloud Storage) and local NVMe drives — and a
foundation for building other latency-sensitive, throughput-critical storage
primitives.

The CLI alias `cp` reflects the tool's primary use: drop-in cloud ↔ NVMe
transfers at speeds that standard SDK helpers cannot approach.

---

## Why it exists

Standard cloud SDK downloaders are designed for correctness and convenience.
They allocate heap buffers for every chunk, route writes through the kernel
page cache, and use a single HTTP connection for small objects. On a machine
with fast local NVMe and a high-bandwidth cloud link, these choices leave the
majority of available throughput on the table.

cloudpump attacks each bottleneck directly:

| Bottleneck | cloudpump approach |
|---|---|
| Single-stream HTTP | Parallel range requests — N workers × chunk size |
| Heap allocations per chunk | Pre-allocated, GC-invisible `mmap` slab pool |
| Page-cache churn on write | `O_DIRECT` / `F_NOCACHE` — data bypasses kernel buffer |
| Generic pwrite loop | `io_uring` sliding-window batching (Linux ≥ 5.1) |
| inode lock contention | `fallocate` pre-reserves extents before any worker writes |
| Stale cache on retry | `posix_fadvise(FADV_DONTNEED)` before download starts |
| Memory budget across jobs | Shared `MmapPool` — global DMA ceiling, not per-download |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           Engine                                │
│                                                                 │
│  MmapPool ──► pool.Acquire() ◄── back-pressure token           │
│  (N slabs,         │              (blocks when disk-bound)      │
│  GC-invisible)     │                                            │
│                    │   errgroup, limit = N workers              │
│                    ▼                                            │
│   Download:  cloud.ReadChunk ──► iosched.WriteAt (O_DIRECT fd)  │
│   Upload:    iosched.ReadAt  ──► cloud.WriteChunk               │
│                                                                 │
│  http.Client  MaxIdleConns=1000  MaxIdleConnsPerHost=64         │
│               ResponseHeaderTimeout=30s  DisableCompression     │
└─────────────────────────────────────────────────────────────────┘
```

### Key design points

- **Zero GC** — `mmap(MAP_ANON)` slabs pre-warmed at construction; pool stores
  raw `[]byte`, not structs (ABA-safe). With 32 × 4 MiB slabs: ~128 MiB of
  DMA memory invisible to the Go runtime.
- **Back-pressure** — `pool.Acquire()` is the *first* operation in every chunk
  worker. When disk write bandwidth is exhausted, workers block on the pool
  rather than queuing unbounded HTTP data in heap memory.
- **O_DIRECT tail-padding** — the final chunk is zero-padded to the nearest
  4 KiB boundary for the write, then `ftruncate(2)` snaps the logical EOF
  back to the true file size.
- **Per-chunk retry, zero alloc** — the `MmapBuffer` is already held when a
  retry fires; the fetch sequence replays into the same physical memory.
- **Shared pool** — `WithMmapPool` injects a pool shared across multiple Engine
  instances, enforcing a process-wide DMA-memory ceiling regardless of how many
  concurrent downloads or uploads are running.

---

## Performance

Benchmarked on **AWS m7gd.8xlarge** (32 vCPU Graviton3, 3 GB/s NVMe,
`linux/arm64`) against the public NOAA GOES-16 S3 bucket (same region,
no egress cost). Each row is the best of 3 runs × 5 iterations.

| File size | cloudpump | Naive `io.Copy` | Speedup |
|---|---|---|---|
| 10 MB | 177 MB/s | 97 MB/s | **1.8×** |
| 20 MB | 296 MB/s | 98 MB/s | **3.0×** |
| 30 MB | 390 MB/s | 98 MB/s | **4.0×** |
| 40 MB | 451 MB/s | 98 MB/s | **4.6×** |
| 50 MB | 612 MB/s | 98 MB/s | **6.2×** |
| 60 MB | 681 MB/s | 98 MB/s | **6.9×** |
| 70 MB | **779 MB/s** | 98 MB/s | **7.9×** |
| 80 MB | 755 MB/s | 99 MB/s | **7.6×** |
| 90 MB | 660 MB/s | 99 MB/s | **6.7×** |

The naive baseline is capped at ~98 MB/s (a single TCP stream). cloudpump
scales with file size as parallel range requests saturate the available network
bandwidth. The first run of each sub-benchmark is consistently lower (TLS
cold-start + S3 request-rate ramp-up); subsequent runs stabilise.

Reproduce:

```bash
CLOUDPUMP_BENCH_DIR=/instance_storage \
  go test -bench=. -benchtime=5x -count=3 -run='^$' -v .
```

---

## Installation

```bash
go install github.com/miretskiy/cloudpump/cmd/cp@latest
```

Requires [github.com/miretskiy/dio](https://github.com/miretskiy/dio).

---

## Usage

Direction is inferred from the URI scheme:

```
cp [flags] <src> <dst>

  src / dst:
    s3://bucket/key       AWS S3
    gs://bucket/object    Google Cloud Storage
    /path/to/file         local path

Flags:
  -j  int     parallel chunk workers         (default: GOMAXPROCS)
  -c  int     chunk size in bytes            (default: 1 MiB;
                                              S3 uploads require ≥ 5 MiB)
  --retries   extra retry attempts per chunk (default: 2)
  --uring     force io_uring scheduler       (Linux ≥ 5.1)
  --pwrite    force pwrite(2) scheduler
```

### Examples

```bash
# Download from S3
cp s3://noaa-goes16/ABI-L2-CMIPF/2024/001/17/OR_ABI-L2-CMIPF-M6C05_G16_....nc \
   /mnt/nvme/goes16.nc

# Download from GCS, 32 workers, 8 MiB chunks
cp -j 32 -c $((8<<20)) gs://my-bucket/data/huge.bin /mnt/nvme/huge.bin

# Upload to S3 (chunk ≥ 5 MiB for S3 multipart)
cp -c $((8<<20)) /mnt/nvme/huge.bin s3://my-bucket/archive/huge.bin

# Upload to GCS
cp /mnt/nvme/huge.bin gs://my-bucket/archive/huge.bin
```

### Credentials

- **S3** — standard AWS credential chain (`AWS_ACCESS_KEY_ID`,
  `~/.aws/credentials`, IMDSv2, …)
- **GCS** — `GOOGLE_APPLICATION_CREDENTIALS` or Application Default Credentials

---

## Using the Engine directly

```go
import (
    cloudpump "github.com/miretskiy/cloudpump"
    "github.com/miretskiy/cloudpump/cloud"
)

eng, err := cloudpump.NewEngine(
    cloudpump.WithConcurrency(32),
    cloudpump.WithChunkSize(8 << 20),
    cloudpump.WithRetryPolicy(cloudpump.RetryPolicy{
        MaxAttempts: 5,
        IsRetryable: func(err error) bool {
            return cloud.S3IsRetryable(err) || cloudpump.DefaultIsRetryable(err)
        },
        Backoff: cloudpump.ExponentialBackoff(100*time.Millisecond, 10*time.Second),
    }),
)
defer eng.Close()

// Download
reader := cloud.NewS3Reader(s3Client, "my-bucket", "data/huge.bin")
err = eng.Download(ctx, reader, "/mnt/nvme/huge.bin")

// Upload
writer, err := cloud.NewS3MultipartWriter(ctx, s3Client, "my-bucket", "archive/huge.bin",
    fileSize, eng.ChunkSize())
err = eng.Upload(ctx, "/mnt/nvme/huge.bin", writer)

// Shared pool for bounded memory across multiple engines
pool := mempool.NewMmapPool("global", 8<<20, 64) // 64 × 8 MiB = 512 MiB
defer pool.Close()
eng1, _ := cloudpump.NewEngine(cloudpump.WithMmapPool(pool))
eng2, _ := cloudpump.NewEngine(cloudpump.WithMmapPool(pool))
```

---

## Package structure

```
cloudpump/
├── engine.go               Engine, Options, Download, Upload
├── bench_test.go           BenchmarkDownload / BenchmarkDownloadNaive
├── cloud/
│   ├── reader.go           CloudChunkReader interface
│   ├── s3.go               S3Reader  + S3IsRetryable
│   ├── gcs.go              GCSReader + GCSIsRetryable + NewGCSClient
│   ├── writer.go           CloudChunkWriter interface
│   ├── s3_writer.go        S3MultipartWriter (parallel UploadPart)
│   └── gcs_writer.go       GCSWriter (resumable stream, ordered flush)
└── cmd/cp/
    └── main.go             CLI — direction inferred from URI scheme
```

Depends on [github.com/miretskiy/dio](https://github.com/miretskiy/dio) for:

| Package | Provides |
|---|---|
| `dio/mempool` | `MmapPool`, `MmapBuffer` — GC-invisible slab pool |
| `dio/iosched` | `IOScheduler` — `pwrite(2)` and `io_uring` backends |
| `dio/sys` | `CreateDirect`, `Fallocate`, `Fadvise`, `OpenDirect` |
| `dio/align` | `BlockSize`, `PageAlign`, `AllocAligned` |

---

## Roadmap

The engine's pluggable interfaces are designed to support additional
high-performance I/O workloads beyond simple cloud transfers:

- **Multi-file pipelining** — download manifests of objects in a single
  engine pass, saturating both the network and the NVMe write queue
- **Cross-cloud copy** — S3 → GCS / GCS → S3 without materialising to disk,
  reading into mmap slabs and writing out to the destination provider in parallel
- **Parallel verification** — post-download checksum (xxHash3 / CRC32C) computed
  in a background goroutine reading from the same O_DIRECT fd, overlapping with
  the next chunk's download
- **NVMe → NVMe copy** — use the same pool and iosched for local copies with
  `copy_file_range(2)` / `io_uring` splice, bypassing double-buffering

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
