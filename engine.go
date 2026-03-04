// Package cloudpump provides a bare-metal, maximum-throughput downloader
// that transfers massive objects from cloud storage (S3, GCS) directly to
// local NVMe drives.
//
// # Design
//
// The central abstraction is [Engine]: a long-lived object that holds a
// pre-allocated [mempool.MmapPool], a pluggable [iosched.IOScheduler], and a
// tuned [net/http.Client]. These are constructed once and reused across calls
// to [Engine.Download], amortising TLS handshake and mmap overhead.
//
// # Backpressure
//
// The pool capacity gates memory consumption globally. When a shared
// [mempool.MmapPool] is injected via [WithMmapPool], multiple concurrent
// downloads draw from the same physical RAM budget: if all slabs are busy
// across all downloads, [mempool.MmapPool.Acquire] blocks the caller,
// providing cross-download back-pressure without any additional semaphores.
//
// # Retry
//
// Each chunk worker retries the ReadChunk + io.ReadFull sequence in-place
// (the [mempool.MmapBuffer] is already held). Tail-padding and WriteAt run
// only once, on the first successful read.
//
// # O_DIRECT tail-padding
//
// O_DIRECT requires write size and file offset to be multiples of 4 KiB.
// The final chunk rarely satisfies this. The engine zero-pads the slab to the
// next 4 KiB boundary, issues the block-aligned write, then calls
// syscall.Ftruncate to restore the file's true logical size.
package cloudpump

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/miretskiy/cloudpump/cloud"
	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/iosched"
	"github.com/miretskiy/dio/mempool"
	"github.com/miretskiy/dio/sys"
)

const defaultChunkSize int64 = 1 << 20 // 1 MiB

// ─── RetryPolicy ─────────────────────────────────────────────────────────────

// RetryPolicy configures per-chunk retry behaviour.
// A failed ReadChunk or io.ReadFull is retried in-place: the engine already
// holds the [mempool.MmapBuffer], so a retry just re-opens the range-read
// stream and fills the same pre-aligned slab from byte zero.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts (1 = no retry, ≥2 = retries).
	MaxAttempts int

	// IsRetryable reports whether err should trigger a retry.
	// If nil, [DefaultIsRetryable] is used.
	// Compose provider-specific predicates with DefaultIsRetryable:
	//   func(err error) bool { return cloud.S3IsRetryable(err) || cloudpump.DefaultIsRetryable(err) }
	IsRetryable func(err error) bool

	// Backoff returns the delay before attempt n (1-based, so n ≥ 2).
	// If nil, [ExponentialBackoff](100ms, 10s) is used.
	Backoff func(attempt int) time.Duration
}

// DefaultRetryPolicy returns 3 attempts with exponential backoff and
// generic network-error detection. Augment IsRetryable with a
// provider-specific predicate (S3IsRetryable, GCSIsRetryable) in the CLI.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		IsRetryable: DefaultIsRetryable,
		Backoff:     ExponentialBackoff(100*time.Millisecond, 10*time.Second),
	}
}

// NoRetryPolicy returns a policy that makes exactly one attempt.
func NoRetryPolicy() RetryPolicy { return RetryPolicy{MaxAttempts: 1} }

// DefaultIsRetryable reports true for generic transient network errors:
// unexpected EOF, connection reset/abort/pipe, and network timeouts.
// Context cancellation and deadline exceeded are NOT retryable.
func DefaultIsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNRESET, syscall.ECONNABORTED, syscall.EPIPE:
			return true
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// ExponentialBackoff returns a backoff function that doubles the delay on each
// successive attempt, clamped to max. The first call (attempt=2) returns base.
func ExponentialBackoff(base, max time.Duration) func(int) time.Duration {
	return func(attempt int) time.Duration {
		// attempt is 1-based; first retry is attempt 2.
		shift := min(uint(attempt-2), 30)
		return min(base<<shift, max)
	}
}

// ─── Engine ───────────────────────────────────────────────────────────────────

// Engine orchestrates parallel chunk downloads from cloud storage to local disk.
//
// Create a single Engine per process (or per storage tier) and reuse it
// across downloads. Safe for concurrent use by multiple goroutines.
type Engine struct {
	pool        *mempool.MmapPool
	ownPool     bool // true → Engine closes pool on Close()
	sched       iosched.IOScheduler
	httpClient  *http.Client
	concurrency int
	chunkSize   int64
	retry       RetryPolicy
}

// Option is a functional option for configuring an [Engine].
type Option func(*Engine)

// WithConcurrency sets the number of parallel chunk workers per download.
// Defaults to runtime.GOMAXPROCS(0).
func WithConcurrency(n int) Option {
	return func(e *Engine) {
		if n > 0 {
			e.concurrency = n
		}
	}
}

// WithChunkSize sets the per-chunk download size in bytes.
// Must be a positive multiple of [align.BlockSize] (4 KiB).
// Defaults to 1 MiB.
func WithChunkSize(size int64) Option {
	return func(e *Engine) {
		if size > 0 {
			e.chunkSize = size
		}
	}
}

// WithMmapPool injects a pre-allocated pool shared across Engine instances.
//
// The injected pool is NOT closed when the Engine is closed — the caller owns
// its lifecycle. Multiple engines can draw from the same pool, creating a
// global DMA-memory ceiling: if all slabs are in flight, Acquire blocks every
// worker across all concurrent downloads until one slab is returned.
//
// Pool capacity may exceed Engine.concurrency; in that case the errgroup limit
// provides the per-download throttle and the pool enforces the global budget.
func WithMmapPool(pool *mempool.MmapPool) Option {
	return func(e *Engine) {
		e.pool = pool
		e.ownPool = false
	}
}

// WithIOScheduler overrides the default [iosched.IOScheduler].
// By default [NewEngine] selects the best available backend via
// [iosched.NewDefaultScheduler] (io_uring on Linux ≥ 5.1, pwrite(2) elsewhere).
func WithIOScheduler(s iosched.IOScheduler) Option {
	return func(e *Engine) { e.sched = s }
}

// WithHTTPTransport injects a custom [http.RoundTripper].
// When set, the engine's default transport tuning is skipped entirely.
func WithHTTPTransport(t http.RoundTripper) Option {
	return func(e *Engine) { e.httpClient = &http.Client{Transport: t} }
}

// WithRetryPolicy overrides the default chunk retry behaviour.
// Use [DefaultRetryPolicy] as a starting point and override IsRetryable with
// a provider-specific composite predicate.
func WithRetryPolicy(p RetryPolicy) Option {
	return func(e *Engine) { e.retry = p }
}

// NewEngine constructs and initialises an Engine.
//
// Unless [WithMmapPool] is provided, the engine creates its own
// [mempool.MmapPool] with capacity == concurrency so every in-flight worker
// has exactly one slab and [mempool.MmapPool.Acquire] never races with kernel
// allocations on the hot path.
//
// The HTTP transport separates per-host connection limits from the global idle
// pool: MaxConnsPerHost == concurrency (hard per-download throttle),
// MaxIdleConnsPerHost == 64 (warm TLS sessions per endpoint),
// MaxIdleConns == 1000 (global ceiling so S3/GCS sessions never evict each
// other).
func NewEngine(opts ...Option) (*Engine, error) {
	e := &Engine{
		concurrency: runtime.GOMAXPROCS(0),
		chunkSize:   defaultChunkSize,
		retry:       DefaultRetryPolicy(),
	}
	for _, o := range opts {
		o(e)
	}

	if e.chunkSize%align.BlockSize != 0 {
		return nil, fmt.Errorf("cloudpump: chunkSize %d is not a multiple of BlockSize (%d)",
			e.chunkSize, align.BlockSize)
	}

	// Resolve retry policy defaults (nil fields → package defaults).
	if e.retry.MaxAttempts <= 0 {
		e.retry.MaxAttempts = 1
	}
	if e.retry.IsRetryable == nil {
		e.retry.IsRetryable = DefaultIsRetryable
	}
	if e.retry.Backoff == nil {
		e.retry.Backoff = ExponentialBackoff(100*time.Millisecond, 10*time.Second)
	}

	// Build pool only if one was not injected via WithMmapPool.
	if e.pool == nil {
		e.pool = mempool.NewMmapPool("cloudpump-write", e.chunkSize, e.concurrency)
		e.ownPool = true
	}

	if e.sched == nil {
		var err error
		e.sched, err = iosched.NewDefaultScheduler()
		if err != nil {
			if e.ownPool {
				e.pool.Close()
			}
			return nil, fmt.Errorf("cloudpump: init iosched: %w", err)
		}
	}

	if e.httpClient == nil {
		e.httpClient = &http.Client{
			Transport: &http.Transport{
				// Global idle-connection ceiling: keeps S3 and GCS TLS
				// sessions warm simultaneously without evicting each other.
				MaxIdleConns: 1000,
				// Per-host saturation point: typical single-endpoint limit.
				MaxIdleConnsPerHost: 64,
				// Hard per-host active-connection cap matches worker count.
				MaxConnsPerHost:     e.concurrency,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
				// Kill zombie connections that accept TCP but stall on headers.
				ResponseHeaderTimeout: 30 * time.Second,
				// Fast-rejection for upload path: don't send payload if the
				// server would 403 immediately (e.g. wrong credentials).
				ExpectContinueTimeout: 1 * time.Second,
				// Never transparently decompress: we download raw binary data
				// and gzip injection breaks Range request semantics.
				DisableCompression: true,
			},
		}
	}

	return e, nil
}

// ChunkSize returns the configured chunk size in bytes.
// Callers that create a CloudChunkWriter (e.g. S3MultipartWriter) need this
// to validate provider-specific size constraints before Upload is called.
func (e *Engine) ChunkSize() int64 { return e.chunkSize }

// SetRetryPolicy replaces the engine's retry policy.
// Must be called before the first Download. Not safe for concurrent use.
func (e *Engine) SetRetryPolicy(p RetryPolicy) { e.retry = p }

// HTTPClient returns the engine's tuned HTTP client.
// Pass this to cloud reader constructors to share the connection pool and
// TLS sessions across all chunk requests.
func (e *Engine) HTTPClient() *http.Client { return e.httpClient }

// Close releases the I/O scheduler and, if the engine owns the pool, the
// mmap pool. All outstanding [Engine.Download] calls must have returned
// before Close is called.
func (e *Engine) Close() error {
	if e.ownPool {
		e.pool.Close()
	}
	return e.sched.Close()
}

// ─── Download ─────────────────────────────────────────────────────────────────

// Download fetches the object described by src and writes it to dstPath.
//
//  1. src.Size → total byte count.
//  2. Create dstPath with O_DIRECT; fall back to buffered I/O on EINVAL.
//  3. sys.Fadvise(FadvDontNeed) to evict stale page-cache data from any
//     previous partial download of the same path.
//  4. sys.Fallocate to reserve the full file size, preventing metadata lock
//     contention under parallel writes to the same inode.
//  5. Spawn up to Engine.concurrency goroutines via errgroup.
//  6. Each worker: pool.Acquire (back-pressure token) → fetchWithRetry →
//     zero-pad tail → iosched.WriteAt → buf.Unpin.
//  7. syscall.Ftruncate (O_DIRECT path only) to restore the true file size.
func (e *Engine) Download(ctx context.Context, src cloud.CloudChunkReader, dstPath string) (retErr error) {
	// 1. Object size.
	size, err := src.Size(ctx)
	if err != nil {
		return fmt.Errorf("cloudpump: size: %w", err)
	}
	if size == 0 {
		f, err := sys.CreateDirect(dstPath, sys.SyncNone)
		if err != nil {
			return fmt.Errorf("cloudpump: create %q: %w", dstPath, err)
		}
		return f.Close()
	}

	// 2. Open destination: try O_DIRECT, fall back on rejection.
	f, directIO, err := openDst(dstPath)
	if err != nil {
		return fmt.Errorf("cloudpump: open %q: %w", dstPath, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && retErr == nil {
			retErr = fmt.Errorf("cloudpump: close %q: %w", dstPath, cerr)
		}
	}()

	// 3. Pre-evict stale page-cache data from any prior partial download.
	// On O_DIRECT this is defense-in-depth (O_DIRECT bypasses the cache
	// anyway); on the buffered fallback it prevents reads of stale data.
	if sys.UseFadvise {
		_ = sys.Fadvise(f, 0, size, sys.FadvDontNeed)
	}

	// 4. Pre-allocate the page-aligned extent.
	// Fallocate to PageAlign(size) so the tail-padded write of the final
	// chunk lands within an already-allocated extent, avoiding mid-write
	// extent splits and inode lock contention under parallel pwrite.
	// Non-fatal: tmpfs and some network filesystems silently ignore it.
	_ = sys.Fallocate(f, align.PageAlign(size))

	// f.Fd() puts the file into blocking mode and returns the raw fd.
	// All I/O goes through iosched (pwrite / io_uring), bypassing the Go
	// runtime net-poller. runtime.KeepAlive(f) at the end prevents the GC
	// from finalising f (and closing fd) while the fd is still live.
	fd := int(f.Fd())

	// 5 & 6. Parallel chunk download.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(e.concurrency)

	for off := int64(0); off < size; off += e.chunkSize {
		chunkOff := off
		chunkLen := min(e.chunkSize, size-chunkOff)
		g.Go(func() error {
			return e.writeChunk(gctx, src, fd, chunkOff, chunkLen, directIO)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// 7. Snap the file back to its true logical size.
	// The tail-padded write may have set the OS-visible file size to
	// align.PageAlign(lastChunkOffset + lastChunkLen). Ftruncate is a
	// metadata-only operation on a pre-allocated file — effectively free.
	// Only needed when O_DIRECT was active (tail-padding was applied).
	if directIO {
		if err := syscall.Ftruncate(fd, size); err != nil {
			return fmt.Errorf("cloudpump: ftruncate %q to %d: %w", dstPath, size, err)
		}
	}

	runtime.KeepAlive(f)
	return nil
}

// writeChunk downloads one chunk and writes it to fd.
//
// pool.Acquire is intentionally the FIRST operation: it blocks this goroutine
// if all slabs are busy (disk-bound or global memory budget exhausted),
// providing token-bucket back-pressure without any additional semaphores.
func (e *Engine) writeChunk(
	ctx context.Context,
	src cloud.CloudChunkReader,
	fd int,
	offset, length int64,
	directIO bool,
) error {
	// Back-pressure: block until a slab is available.
	buf := e.pool.Acquire()
	defer buf.Unpin()

	// Fetch with retry — rewrites the same slab on each attempt.
	if err := e.fetchWithRetry(ctx, src, buf, offset, length); err != nil {
		return err
	}

	// Tail-padding for O_DIRECT: the final chunk is almost never
	// block-aligned. Zero-fill slab[length:aligned] so we don't write
	// stale data, then use the aligned length for the write call.
	// The slab always has capacity ≥ align.PageAlign(chunkSize) == chunkSize
	// (chunkSize is block-aligned), so PageAlign(length) ≤ chunkSize and
	// buf.Bytes()[:alignedLen] is always in-bounds.
	writeLen := length
	if directIO && length%align.BlockSize != 0 {
		alignedLen := align.PageAlign(length)
		clear(buf.Bytes()[int(length):int(alignedLen)])
		writeLen = alignedLen
	}

	n, err := e.sched.WriteAt(fd, buf.Bytes()[:int(writeLen)], offset)
	if err != nil {
		return fmt.Errorf("cloudpump: WriteAt offset=%d: %w", offset, err)
	}
	if int64(n) < writeLen {
		return fmt.Errorf("cloudpump: short write at offset=%d: wrote %d of %d bytes",
			offset, n, writeLen)
	}
	return nil
}

// fetchWithRetry wraps the ReadChunk + io.ReadFull pair with the engine's
// retry policy. On each attempt it opens a fresh range-read stream and
// fills buf from byte zero, so partial reads from a failed attempt are
// always overwritten.
func (e *Engine) fetchWithRetry(
	ctx context.Context,
	src cloud.CloudChunkReader,
	buf *mempool.MmapBuffer,
	offset, length int64,
) error {
	p := e.retry
	var lastErr error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		// Wait before retrying (skip on first attempt).
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.Backoff(attempt)):
			}
		}

		lastErr = fetchOnce(ctx, src, buf, offset, length)
		if lastErr == nil {
			return nil
		}
		if !p.IsRetryable(lastErr) || ctx.Err() != nil {
			break
		}
	}
	return fmt.Errorf("cloudpump: fetch offset=%d length=%d after %d attempt(s): %w",
		offset, length, p.MaxAttempts, lastErr)
}

// fetchOnce opens a single range-read stream and reads exactly length bytes
// into buf. The stream is always closed before returning.
func fetchOnce(
	ctx context.Context,
	src cloud.CloudChunkReader,
	buf *mempool.MmapBuffer,
	offset, length int64,
) error {
	stream, err := src.ReadChunk(ctx, offset, length)
	if err != nil {
		return err
	}
	defer stream.Close()
	_, err = io.ReadFull(stream, buf.Bytes()[:int(length)])
	return err
}

// ─── Upload ───────────────────────────────────────────────────────────────────

// Upload reads srcPath from local disk and streams it to dst in parallel chunks.
//
//  1. Open srcPath with O_DIRECT; fall back to buffered I/O on rejection.
//  2. sys.Fadvise(FadvSequential) to prefetch the file into the drive queue.
//  3. Spawn up to Engine.concurrency goroutines.
//  4. Each worker: pool.Acquire → iosched.ReadAt into slab → dst.WriteChunk
//     (passing only the true data bytes, not the O_DIRECT alignment padding)
//     → buf.Unpin.
//  5. On success: dst.Commit. On any error: dst.Abort (best-effort).
//
// The caller is responsible for creating dst (e.g. [cloud.NewS3MultipartWriter])
// before calling Upload, and for not calling dst.Commit or dst.Abort again.
func (e *Engine) Upload(ctx context.Context, srcPath string, dst cloud.CloudChunkWriter) (retErr error) {
	// 1. Open source.
	f, directIO, err := openSrc(srcPath)
	if err != nil {
		return fmt.Errorf("cloudpump: open source %q: %w", srcPath, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && retErr == nil {
			retErr = fmt.Errorf("cloudpump: close %q: %w", srcPath, cerr)
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("cloudpump: stat %q: %w", srcPath, err)
	}
	size := fi.Size()

	// 2. Sequential prefetch hint: the upload reads the file from front to back.
	if sys.UseFadvise {
		_ = sys.Fadvise(f, 0, size, sys.FadvSequential)
	}

	fd := int(f.Fd())

	// 3 & 4. Parallel chunk read + upload.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(e.concurrency)

	partNum := 0
	for off := int64(0); off < size; off += e.chunkSize {
		chunkOff := off
		chunkLen := min(e.chunkSize, size-chunkOff)
		partNum++
		pNum := partNum
		g.Go(func() error {
			return e.readAndUpload(gctx, dst, fd, chunkOff, chunkLen, pNum, directIO)
		})
	}

	if err := g.Wait(); err != nil {
		// Best-effort abort: the error from g.Wait() takes precedence.
		_ = dst.Abort(ctx)
		return err
	}

	// 5. Commit.
	if err := dst.Commit(ctx); err != nil {
		_ = dst.Abort(ctx)
		return fmt.Errorf("cloudpump: upload commit: %w", err)
	}

	runtime.KeepAlive(f)
	return nil
}

// readAndUpload reads one chunk from fd into a pool slab and uploads it.
//
// O_DIRECT read alignment: the READ SIZE must be a multiple of BlockSize.
// We issue an aligned read of align.PageAlign(chunkLen) bytes; the OS
// returns at most chunkLen bytes (it does not fabricate zeros beyond EOF).
// Only the true chunkLen bytes are passed to WriteChunk — the alignment
// padding in the slab tail is never sent to the cloud provider.
func (e *Engine) readAndUpload(
	ctx context.Context,
	dst cloud.CloudChunkWriter,
	fd int,
	offset, length int64,
	partNum int,
	directIO bool,
) error {
	buf := e.pool.Acquire()
	defer buf.Unpin()

	readLen := length
	if directIO && length%align.BlockSize != 0 {
		readLen = align.PageAlign(length)
	}

	n, err := e.sched.ReadAt(fd, buf.Bytes()[:int(readLen)], offset)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cloudpump: ReadAt offset=%d: %w", offset, err)
	}
	if int64(n) < length {
		return fmt.Errorf("cloudpump: short read at offset=%d: got %d of %d bytes",
			offset, n, length)
	}

	// Pass exactly length bytes — the aligned-read tail is invisible to the cloud.
	return dst.WriteChunk(ctx, partNum, offset, buf.Bytes()[:int(length)])
}

// openSrc opens srcPath for reading with O_DIRECT, falling back to buffered
// I/O if the filesystem rejects it. The directIO flag tells the caller
// whether alignment padding is needed for reads.
func openSrc(path string) (f *os.File, directIO bool, err error) {
	f, err = sys.OpenDirect(path, sys.FlDirectIO)
	if err == nil {
		return f, true, nil
	}
	var errno syscall.Errno
	if errors.As(err, &errno) && (errno == syscall.EINVAL || errno == syscall.EOPNOTSUPP) {
		f, err = sys.OpenDirect(path, sys.SyncNone)
		if err != nil {
			return nil, false, err
		}
		return f, false, nil
	}
	return nil, false, err
}

// openDst creates dstPath for writing and reports whether O_DIRECT is active.
//
// O_DIRECT is attempted first; if the filesystem rejects it (Linux EINVAL for
// ZFS / tmpfs / overlayfs; EOPNOTSUPP for some network filesystems), the
// function retries with standard buffered I/O and sets directIO=false.
// The tail-padding logic and Ftruncate are skipped when directIO is false,
// so correctness is preserved in the fallback path.
func openDst(path string) (f *os.File, directIO bool, err error) {
	f, err = sys.CreateDirect(path, sys.FlDirectIO)
	if err == nil {
		return f, true, nil
	}
	var errno syscall.Errno
	if errors.As(err, &errno) && (errno == syscall.EINVAL || errno == syscall.EOPNOTSUPP) {
		f, err = sys.CreateDirect(path, sys.SyncNone)
		if err != nil {
			return nil, false, err
		}
		return f, false, nil
	}
	return nil, false, err
}
