# cloudpump

**cloudpump** (`cp`) is a bare-metal, maximum-throughput tool for downloading
massive objects from AWS S3 or Google Cloud Storage directly to local NVMe
drives — and for uploading local files back to the cloud.

It is built around three principles:

1. **Zero page-cache pollution** — writes use `O_DIRECT` (Linux) / `F_NOCACHE`
   (Darwin) so downloaded data bypasses the kernel buffer cache entirely.
2. **Zero GC pressure** — chunk buffers come from a pre-allocated pool of
   `mmap`-backed, page-aligned slabs that are invisible to the Go garbage
   collector.
3. **Strict back-pressure** — `pool.Acquire()` is the *first* operation in
   every chunk worker. When disk I/O is slower than the network, workers block
   on the pool instead of queuing unbounded HTTP data in heap memory.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           Engine                                │
│                                                                 │
│  MmapPool ──► pool.Acquire() ◄── back-pressure token           │
│  (N slabs)         │                                            │
│                    │   parallel workers (errgroup, limit=N)     │
│                    ▼                                            │
│          ┌─────────────────┐        ┌──────────────────────┐   │
│          │  cloud.ReadChunk │        │  iosched.WriteAt     │   │
│          │  (TLS stream)    │──────►│  (pwrite / io_uring) │   │
│          │  into mmap slab  │        │  O_DIRECT fd         │   │
│          └─────────────────┘        └──────────────────────┘   │
│                                                                 │
│  http.Client  MaxIdleConns=1000  MaxIdleConnsPerHost=64         │
│               ResponseHeaderTimeout=30s  DisableCompression     │
└─────────────────────────────────────────────────────────────────┘
```

### Key design points

| Concern | Implementation |
|---|---|
| Page-cache bypass | `O_DIRECT` on Linux, `F_NOCACHE` on Darwin; graceful fallback on ZFS / tmpfs |
| Zero GC | `mmap(MAP_ANON)` slabs pre-warmed at construction; pool stores raw `[]byte`, not structs (ABA-safe) |
| Back-pressure | `pool.Acquire()` blocks when all N slabs are in use; pool capacity == concurrency |
| Disk pre-allocation | `fallocate(2)` to `PageAlign(size)` prevents inode lock contention under parallel `pwrite` |
| Page-cache hygiene | `posix_fadvise(FADV_DONTNEED)` before download evicts any stale cached data |
| Tail-padding | Final chunk zero-padded to 4 KiB boundary for `O_DIRECT`; `ftruncate(2)` snaps logical EOF back |
| I/O scheduler | `io_uring` (Linux ≥ 5.1) with sliding-window batching; `pwrite(2)` fallback everywhere else |
| Retry | Per-chunk, in-place: same slab reused on retry — zero extra allocation |
| HTTP | `MaxIdleConns=1000` (global), `MaxIdleConnsPerHost=64`, `ResponseHeaderTimeout=30s` |
| Shared pool | `WithMmapPool` injects a shared pool across engines for a global DMA-memory ceiling |

---

## Installation

```bash
go install github.com/miretskiy/cloudpump/cmd/cp@latest
```

Requires [github.com/miretskiy/dio](https://github.com/miretskiy/dio) for the
low-level primitives (`mempool`, `iosched`, `sys`, `align`).

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
  -j  int     parallel chunk workers    (default: GOMAXPROCS)
  -c  int     chunk size in bytes       (default: 1048576 = 1 MiB;
                                         ≥ 5 MiB required for S3 uploads)
  --retries   extra retry attempts per chunk  (default: 2)
  --uring     force io_uring scheduler  (Linux ≥ 5.1)
  --pwrite    force pwrite(2) scheduler
```

### Examples

```bash
# Download from S3 (uses GOMAXPROCS workers, 1 MiB chunks, best scheduler)
cp s3://noaa-goes16/ABI-L2-CMIPF/2024/001/17/OR_ABI-L2-CMIPF-M6C05_G16_....nc \
   /mnt/nvme/goes16.nc

# Download from GCS with 32 workers and 8 MiB chunks
cp -j 32 -c $((8<<20)) gs://my-bucket/data/huge.bin /mnt/nvme/huge.bin

# Upload to S3 (chunk ≥ 5 MiB for multipart)
cp -c $((8<<20)) /mnt/nvme/huge.bin s3://my-bucket/data/huge.bin

# Upload to GCS
cp /mnt/nvme/huge.bin gs://my-bucket/data/huge.bin
```

### Credentials

- **S3** — standard AWS credential chain (`AWS_ACCESS_KEY_ID`, `~/.aws/credentials`, IMDSv2, …)
- **GCS** — `GOOGLE_APPLICATION_CREDENTIALS` or Application Default Credentials

---

## Retry policy

Each chunk worker retries the `ReadChunk + io.ReadFull` sequence in-place.
Because the `MmapBuffer` is already held, a retry is a plain HTTP re-open
with no extra memory allocation.

The default policy (3 total attempts, 100 ms → 10 s exponential backoff)
handles:
- HTTP 408 / 429 / 5xx from S3 (via smithy-go `HTTPStatusCode`) and GCS
  (via `googleapi.Error`)
- `ECONNRESET`, `ECONNABORTED`, `EPIPE`
- `net.Error.Timeout()`
- `io.ErrUnexpectedEOF` (server closed connection mid-stream)

---

## Benchmarks

The `bench_test.go` file benchmarks cloudpump against a naive `io.Copy`
baseline using the public [NOAA GOES-16](https://registry.opendata.aws/noaa-goes/)
S3 bucket (no AWS credentials required).

```bash
# Run on an NVMe-equipped Linux machine (e.g. AWS m7gd.8xlarge)
CLOUDPUMP_BENCH_DIR=/instance_storage \
  go test -bench=. -benchtime=5x -count=3 -run='^$' -v .
```

### Benchmark corpus

| Approx. size | S3 key (noaa-goes16 bucket) |
|---|---|
| 10 MB | `ABI-L2-CMIPF/2024/001/23/OR_ABI-L2-CMIPF-M6C06_G16_s20240012310206_…` |
| 20 MB | `ABI-L2-CMIPF/2024/001/19/OR_ABI-L2-CMIPF-M6C06_G16_s20240011930206_…` |
| 30 MB | `ABI-L2-CMIPF/2024/001/10/OR_ABI-L2-CMIPF-M6C01_G16_s20240011000208_…` |
| … | … |
| 90 MB | `ABI-L2-CMIPF/2024/001/17/OR_ABI-L2-CMIPF-M6C05_G16_s20240011740206_…` |

### What the benchmarks measure

**`BenchmarkDownload`** — cloudpump engine with `O_DIRECT` writes,
parallel mmap slabs, `io_uring` (on Linux), `fallocate` pre-allocation.

**`BenchmarkDownloadNaive`** — baseline: single `GetObject` request (no
`Range` header), `io.Copy` with a 32 KiB heap buffer, `os.Create` (page
cache active, no `O_DIRECT`, no pre-allocation, no parallelism).

The MB/s delta between the two suites at each file size quantifies the combined
effect of parallel range requests, mmap slabs, `O_DIRECT` bypass, and
`io_uring`.

---

## Package structure

```
cloudpump/
├── engine.go               Engine, Options, Download, Upload
├── bench_test.go           BenchmarkDownload / BenchmarkDownloadNaive
├── cloud/
│   ├── reader.go           CloudChunkReader interface
│   ├── s3.go               S3Reader  + S3IsRetryable
│   ├── gcs.go              GCSReader + GCSIsRetryable
│   ├── writer.go           CloudChunkWriter interface
│   ├── s3_writer.go        S3MultipartWriter (parallel UploadPart)
│   └── gcs_writer.go       GCSWriter (resumable stream, ordered flush)
└── cmd/cp/
    └── main.go             CLI — direction inferred from URI scheme
```

Depends on [github.com/miretskiy/dio](https://github.com/miretskiy/dio) for:
- `dio/mempool` — `MmapPool` / `MmapBuffer`
- `dio/iosched` — `IOScheduler` (`pwrite` + `io_uring`)
- `dio/sys`     — `CreateDirect`, `Fallocate`, `Fadvise`, `OpenDirect`
- `dio/align`   — `BlockSize`, `PageAlign`, `AllocAligned`

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
