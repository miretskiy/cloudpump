package cloudpump_test

// Tests for fetchWithRetry partial-reconnect behaviour.
//
// Two levels of testing:
//
//  1. Unit level — faultReader: an in-memory CloudChunkReader that injects
//     errors at configurable byte offsets within a ReadChunk call. No I/O,
//     fully deterministic, exercises the resumeAt accounting in isolation.
//
//  2. Integration level — antagonistServer: a real HTTP/1.1 server that
//     either stalls mid-body (triggering the deadlineConn read timeout) or
//     abruptly closes the connection (triggering io.ErrUnexpectedEOF /
//     ECONNRESET). The engine makes real TCP connections so the full stack —
//     deadlineConn → timeout → DefaultIsRetryable → fetchWithRetry → reconnect
//     — is exercised end-to-end.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	cloudpump "github.com/miretskiy/cloudpump"
	"github.com/miretskiy/cloudpump/cloud"
)

// ── delayReader ───────────────────────────────────────────────────────────────

// delayReader is a CloudChunkReader that sleeps before delivering the first
// byte on specific ReadChunk calls. Used to trigger hedge behaviour in tests.
type delayReader struct {
	data   []byte
	delays map[int]time.Duration // call (1-based) → pre-first-byte sleep
	mu     sync.Mutex
	calls  int
}

func (r *delayReader) Size(_ context.Context) (int64, error) {
	return int64(len(r.data)), nil
}

func (r *delayReader) ReadChunk(_ context.Context, offset, length int64) (io.ReadCloser, error) {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()
	if d, ok := r.delays[call]; ok {
		return &delayStream{data: r.data[offset : offset+length], delay: d}, nil
	}
	return io.NopCloser(bytes.NewReader(r.data[offset : offset+length])), nil
}

// Calls returns the total number of ReadChunk invocations.
func (r *delayReader) Calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// ── faultReader ───────────────────────────────────────────────────────────────

// faultReader is a CloudChunkReader backed by in-memory data.
// Each ReadChunk call is assigned a 1-based sequence number; a matching
// callFault entry causes that call to deliver only afterBytes bytes before
// returning the configured error. Calls with no matching fault succeed.
//
// Use WithConcurrency(1) in the engine to make call ordering deterministic
// across multi-chunk downloads.
type faultReader struct {
	data   []byte
	faults []callFault
	mu     sync.Mutex
	calls  int
}

// callFault describes how a specific ReadChunk invocation misbehaves.
type callFault struct {
	// call is the 1-based ReadChunk call index to affect.
	call int
	// afterBytes is the number of bytes to deliver before returning err.
	// -1 means the error is returned from ReadChunk itself (open failure),
	// before any bytes are delivered.
	afterBytes int64
	err        error
}

func newFaultReader(data []byte, faults ...callFault) *faultReader {
	return &faultReader{data: data, faults: faults}
}

func (r *faultReader) Size(_ context.Context) (int64, error) {
	return int64(len(r.data)), nil
}

func (r *faultReader) ReadChunk(_ context.Context, offset, length int64) (io.ReadCloser, error) {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()

	for _, f := range r.faults {
		if f.call != call {
			continue
		}
		if f.afterBytes < 0 {
			// Open-time failure: ReadChunk itself returns the error.
			return nil, f.err
		}
		return &faultStream{
			data:       r.data[offset : offset+length],
			faultAfter: f.afterBytes,
			faultErr:   f.err,
		}, nil
	}
	return io.NopCloser(bytes.NewReader(r.data[offset : offset+length])), nil
}

// faultStream delivers up to faultAfter bytes from data, then returns faultErr.
// Subsequent Read calls after the fault also return faultErr.
type faultStream struct {
	data       []byte
	pos        int64
	faultAfter int64
	faultErr   error
}

func (s *faultStream) Read(p []byte) (int, error) {
	if s.pos >= s.faultAfter {
		return 0, s.faultErr
	}
	// Cap the read to the bytes available before the fault point.
	avail := s.faultAfter - s.pos
	if int64(len(p)) > avail {
		p = p[:avail]
	}
	n := copy(p, s.data[s.pos:])
	s.pos += int64(n)
	return n, nil
}

func (s *faultStream) Close() error { return nil }

// delayStream sleeps once before the first Read, then behaves normally.
type delayStream struct {
	data  []byte
	pos   int64
	delay time.Duration
	once  sync.Once
}

func (s *delayStream) Read(p []byte) (int, error) {
	s.once.Do(func() { time.Sleep(s.delay) })
	if s.pos >= int64(len(s.data)) {
		return 0, io.EOF
	}
	n := copy(p, s.data[s.pos:])
	s.pos += int64(n)
	return n, nil
}

func (s *delayStream) Close() error { return nil }

// ── test engine ───────────────────────────────────────────────────────────────

// testEngine creates an Engine suitable for deterministic unit tests:
//   - single worker (concurrency=1): ReadChunk call order is predictable
//   - minimum block-aligned chunk size
//   - no backoff between retries: tests finish instantly
//   - maxAttempts: total number of attempts per chunk (1 = no retry)
func testEngine(t *testing.T, chunkSize int64, maxAttempts int, opts ...cloudpump.Option) *cloudpump.Engine {
	t.Helper()
	base := []cloudpump.Option{
		cloudpump.WithConcurrency(1),
		cloudpump.WithChunkSize(chunkSize),
		cloudpump.WithRetryPolicy(cloudpump.RetryPolicy{
			MaxAttempts: maxAttempts,
			IsRetryable: cloudpump.DefaultIsRetryable,
			Backoff:     func(int) time.Duration { return 0 }, // instant retry in tests
		}),
	}
	eng, err := cloudpump.NewEngine(append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

// download runs eng.Download into a temp file and returns its contents.
func download(t *testing.T, eng *cloudpump.Engine, r cloud.CloudChunkReader) []byte {
	t.Helper()
	dst := t.TempDir() + "/out.bin"
	if err := eng.Download(context.Background(), r, dst); err != nil {
		t.Fatalf("Download: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return got
}

// mustFailDownload asserts that eng.Download returns a non-nil error.
func mustFailDownload(t *testing.T, eng *cloudpump.Engine, r cloud.CloudChunkReader) error {
	t.Helper()
	dst := t.TempDir() + "/out.bin"
	err := eng.Download(context.Background(), r, dst)
	if err == nil {
		t.Fatal("expected Download to fail, got nil")
	}
	return err
}

// makeData returns n bytes of recognisable pseudo-random content.
func makeData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + 13)
	}
	return b
}

const blockSize = 4096 // align.BlockSize — minimum chunk granularity

// ── Unit tests ────────────────────────────────────────────────────────────────

func TestFetch_HappyPath(t *testing.T) {
	data := makeData(3 * blockSize)
	eng := testEngine(t, blockSize, 1)
	got := download(t, eng, newFaultReader(data))
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch")
	}
}

// TestFetch_PartialReconnect_EOF: a single chunk delivers half its bytes then
// returns io.ErrUnexpectedEOF. The engine should resume from mid-chunk and
// produce a correct output file without re-downloading the first half.
func TestFetch_PartialReconnect_EOF(t *testing.T) {
	data := makeData(blockSize)
	faultAt := int64(blockSize / 2)

	eng := testEngine(t, blockSize, 2)
	got := download(t, eng, newFaultReader(data,
		callFault{call: 1, afterBytes: faultAt, err: io.ErrUnexpectedEOF},
	))
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch after EOF reconnect")
	}
}

// TestFetch_PartialReconnect_ECONNRESET: same scenario but the error is
// ECONNRESET. DefaultIsRetryable must classify it as retryable.
func TestFetch_PartialReconnect_ECONNRESET(t *testing.T) {
	data := makeData(blockSize)
	faultAt := int64(blockSize / 3)

	eng := testEngine(t, blockSize, 2)
	got := download(t, eng, newFaultReader(data,
		callFault{call: 1, afterBytes: faultAt, err: syscall.ECONNRESET},
	))
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch after ECONNRESET reconnect")
	}
}

// TestFetch_PartialReconnect_MultipleReconnects: two consecutive partial faults
// on the same chunk; only the third attempt succeeds. Verifies that resumeAt
// accumulates correctly across multiple reconnects.
func TestFetch_PartialReconnect_MultipleReconnects(t *testing.T) {
	data := makeData(blockSize)

	eng := testEngine(t, blockSize, 3)
	got := download(t, eng, newFaultReader(data,
		callFault{call: 1, afterBytes: blockSize / 4, err: io.ErrUnexpectedEOF},
		callFault{call: 2, afterBytes: blockSize / 4, err: syscall.ECONNRESET},
		// call 3: no fault — delivers the remaining half
	))
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch after multiple reconnects")
	}
}

// TestFetch_OpenFails: ReadChunk itself returns an error (no bytes delivered).
// The engine should retry from the same offset with resumeAt unchanged.
func TestFetch_OpenFails(t *testing.T) {
	data := makeData(blockSize)

	eng := testEngine(t, blockSize, 2)
	got := download(t, eng, newFaultReader(data,
		callFault{call: 1, afterBytes: -1, err: syscall.ECONNRESET},
	))
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch after open-failure retry")
	}
}

// TestFetch_AllAttemptsExhausted: every ReadChunk call faults; Download must
// return a non-nil error.
func TestFetch_AllAttemptsExhausted(t *testing.T) {
	data := makeData(blockSize)

	eng := testEngine(t, blockSize, 3)
	mustFailDownload(t, eng, newFaultReader(data,
		callFault{call: 1, afterBytes: blockSize / 2, err: io.ErrUnexpectedEOF},
		callFault{call: 2, afterBytes: blockSize / 4, err: io.ErrUnexpectedEOF},
		callFault{call: 3, afterBytes: 0, err: io.ErrUnexpectedEOF},
	))
}

// TestFetch_NonRetryableError: a context.Canceled error must not be retried.
// Only one ReadChunk call should be made.
func TestFetch_NonRetryableError(t *testing.T) {
	data := makeData(blockSize)

	eng := testEngine(t, blockSize, 3)
	r := newFaultReader(data,
		callFault{call: 1, afterBytes: -1, err: context.Canceled},
	)
	mustFailDownload(t, eng, r)

	r.mu.Lock()
	calls := r.calls
	r.mu.Unlock()
	if calls != 1 {
		t.Errorf("expected 1 ReadChunk call for non-retryable error, got %d", calls)
	}
}

// TestFetch_MultiChunk_MiddleChunkReconnects: a three-chunk download where the
// middle chunk (chunk index 1) has a partial fault. Chunks 0 and 2 complete
// without retries; chunk 1 reconnects mid-way. The output must equal the
// full original data.
func TestFetch_MultiChunk_MiddleChunkReconnects(t *testing.T) {
	// With concurrency=1, ReadChunk call order is:
	//   call 1 → chunk 0 (no fault, succeeds)
	//   call 2 → chunk 1 (fault after half)
	//   call 3 → chunk 1 retry from mid-point (succeeds)
	//   call 4 → chunk 2 (no fault, succeeds)
	data := makeData(3 * blockSize)

	eng := testEngine(t, blockSize, 2)
	got := download(t, eng, newFaultReader(data,
		callFault{call: 2, afterBytes: blockSize / 2, err: io.ErrUnexpectedEOF},
	))
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch with middle-chunk reconnect")
	}
}

// TestFetch_ZeroBytesBeforeError: the fault fires before any bytes are delivered
// (afterBytes=0). resumeAt must stay at 0; the retry opens the same range.
func TestFetch_ZeroBytesBeforeError(t *testing.T) {
	data := makeData(blockSize)

	eng := testEngine(t, blockSize, 2)
	got := download(t, eng, newFaultReader(data,
		callFault{call: 1, afterBytes: 0, err: syscall.ECONNRESET},
	))
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch after zero-byte-fault retry")
	}
}

// ── Integration tests — antagonistServer ─────────────────────────────────────

// antagonistServer is an httptest.Server whose handler delivers partial data
// then either stalls (read-deadline test) or closes the connection abruptly
// (ECONNRESET / ErrUnexpectedEOF test). Subsequent requests are served
// normally.
//
// It implements a minimal HTTP/1.1 range-request server backed by in-memory
// data, so an httpChunkReader can drive a real cloudpump Engine against it.
type antagonistServer struct {
	srv      *httptest.Server
	data     []byte
	stallAt  int64 // bytes to deliver on the first request before stalling
	abrupt   bool  // if true, close the conn abruptly instead of stalling
	calls    atomic.Int32
	stallCh  chan struct{} // closed by t.Cleanup to unblock stalled handlers
}

func newStallServer(t *testing.T, data []byte, stallAt int64) *antagonistServer {
	t.Helper()
	a := &antagonistServer{data: data, stallAt: stallAt, stallCh: make(chan struct{})}
	a.srv = httptest.NewServer(a)
	t.Cleanup(func() {
		close(a.stallCh)
		a.srv.Close()
	})
	return a
}

func newAbruptCloseServer(t *testing.T, data []byte, closeAfter int64) *antagonistServer {
	t.Helper()
	a := &antagonistServer{data: data, stallAt: closeAfter, abrupt: true, stallCh: make(chan struct{})}
	a.srv = httptest.NewServer(a)
	t.Cleanup(func() {
		close(a.stallCh)
		a.srv.Close()
	})
	return a
}

func (a *antagonistServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	call := a.calls.Add(1)
	offset, length := parseRangeHeader(r.Header.Get("Range"), int64(len(a.data)))

	if call == 1 {
		deliver := a.stallAt
		if deliver > length {
			deliver = length
		}

		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, len(a.data)))
		w.Header().Set("Content-Length", strconv.FormatInt(deliver, 10))
		w.WriteHeader(http.StatusPartialContent)

		_, _ = w.Write(a.data[offset : offset+deliver])
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}

		if a.abrupt {
			// Hijack the connection and close it with a RST-like abrupt close.
			hj, ok := w.(http.Hijacker)
			if !ok {
				return
			}
			conn, _, _ := hj.Hijack()
			if tc, ok := conn.(*net.TCPConn); ok {
				// SO_LINGER=0 causes a RST on close.
				_ = tc.SetLinger(0)
			}
			_ = conn.Close()
			return
		}

		// Stall: block until the test's cleanup fires.
		<-a.stallCh
		return
	}

	// Subsequent requests: serve the requested range normally.
	w.Header().Set("Content-Range",
		fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, len(a.data)))
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(a.data[offset : offset+length])
}

// parseRangeHeader parses "bytes=N-M" and returns (offset, length).
// Falls back to (0, totalSize) on any parse error.
func parseRangeHeader(hdr string, totalSize int64) (offset, length int64) {
	hdr = strings.TrimPrefix(hdr, "bytes=")
	parts := strings.SplitN(hdr, "-", 2)
	if len(parts) != 2 {
		return 0, totalSize
	}
	offset, _ = strconv.ParseInt(parts[0], 10, 64)
	end, _ := strconv.ParseInt(parts[1], 10, 64)
	return offset, end - offset + 1
}

// httpChunkReader implements CloudChunkReader by issuing HTTP Range GET
// requests to url using client. Designed for use with antagonistServer in
// integration tests; not for production use.
type httpChunkReader struct {
	client *http.Client
	url    string
	size   int64
}

func (r *httpChunkReader) Size(_ context.Context) (int64, error) { return r.size, nil }

func (r *httpChunkReader) ReadChunk(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", r.url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusPartialContent {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}
	return resp.Body, nil
}

// TestDownload_DeadlineReconnect: a real HTTP server stalls mid-body; the
// engine's read deadline fires, the connection is classified as retryable
// (net.Error.Timeout == true), and the engine reconnects from the bytes
// already received, producing a correct output file.
func TestDownload_DeadlineReconnect(t *testing.T) {
	const (
		readDeadline = 30 * time.Millisecond
		stallAfter   = blockSize / 2 // bytes delivered before stall
	)
	data := makeData(blockSize)

	srv := newStallServer(t, data, stallAfter)

	eng := testEngine(t, blockSize, 3,
		cloudpump.WithReadDeadline(readDeadline),
	)

	reader := &httpChunkReader{
		client: eng.HTTPClient(),
		url:    srv.srv.URL + "/data",
		size:   int64(len(data)),
	}

	got := download(t, eng, reader)
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch after deadline-triggered reconnect")
	}
	if n := srv.calls.Load(); n < 2 {
		t.Errorf("expected ≥2 requests (stall + retry), got %d", n)
	}
}

// TestDownload_AbruptCloseReconnect: a real HTTP server closes the TCP
// connection abruptly (SO_LINGER=0) after delivering partial data.
// The resulting io.ErrUnexpectedEOF or ECONNRESET is classified as retryable
// and the engine reconnects from the byte where the connection dropped.
func TestDownload_AbruptCloseReconnect(t *testing.T) {
	const closeAfter = blockSize / 3
	data := makeData(blockSize)

	srv := newAbruptCloseServer(t, data, closeAfter)

	eng := testEngine(t, blockSize, 3)

	reader := &httpChunkReader{
		client: eng.HTTPClient(),
		url:    srv.srv.URL + "/data",
		size:   int64(len(data)),
	}

	got := download(t, eng, reader)
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch after abrupt-close reconnect")
	}
	if n := srv.calls.Load(); n < 2 {
		t.Errorf("expected ≥2 requests (abrupt close + retry), got %d", n)
	}
}

// ── Hedge tests ───────────────────────────────────────────────────────────────
//
// All hedge tests run inside synctest.Test so that:
//   - time.NewTimer (hedge threshold) uses the bubble's fake clock
//   - time.Sleep (delayStream / delayErrorStream) uses the fake clock
//   - Tests complete in microseconds regardless of the configured durations
//   - drainAndCancel cleanup goroutines are fully awaited before Test returns
//   - No wall-clock timing assertions needed or used
//
// Pattern: download results are captured into outer variables and asserted
// after synctest.Test returns. This avoids t.Fatal inside the bubble while
// cleanup goroutines may still be sleeping.

// delayErrorStream sleeps once on the first Read then returns err.
// Used to simulate a connection that stalls then resets without delivering
// any payload — distinct from delayStream which stalls then delivers data.
type delayErrorStream struct {
	delay time.Duration
	err   error
	once  sync.Once
}

func (s *delayErrorStream) Read([]byte) (int, error) {
	s.once.Do(func() { time.Sleep(s.delay) })
	return 0, s.err
}

func (s *delayErrorStream) Close() error { return nil }

// TestHedge_SlowFirstAttempt: the first ReadChunk sleeps past the hedge
// threshold; the hedge fires, a second attempt wins immediately, and the
// download produces the correct output. Uses synctest fake time — no real
// milliseconds elapse.
func TestHedge_SlowFirstAttempt(t *testing.T) {
	const (
		threshold    = 50 * time.Millisecond
		attempt1Slow = 400 * time.Millisecond
	)
	data := makeData(blockSize)
	reader := &delayReader{
		data:   data,
		delays: map[int]time.Duration{1: attempt1Slow},
	}
	eng := testEngine(t, blockSize, 1,
		cloudpump.WithHedgePolicy(cloudpump.HedgePolicy{Threshold: threshold}),
	)

	var got []byte
	synctest.Test(t, func(t *testing.T) {
		got = download(t, eng, reader)
		// attempt 1 is still sleeping in fake time. Drain it:
		// wait for all goroutines to be durably blocked, advance time past
		// the sleep, then wait for them to finish.
		synctest.Wait()
		time.Sleep(attempt1Slow)
		synctest.Wait()
	})

	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch after hedge")
	}
	if n := reader.Calls(); n < 2 {
		t.Errorf("expected ≥2 ReadChunk calls (original + hedge), got %d", n)
	}
}

// TestHedge_FastFirstAttempt: when the first attempt returns its first byte
// before the threshold fires, the hedge must not be issued — exactly one
// ReadChunk call occurs. The long threshold (10 s fake) never fires.
func TestHedge_FastFirstAttempt(t *testing.T) {
	data := makeData(blockSize)
	reader := &delayReader{
		data:   data,
		delays: map[int]time.Duration{},
	}
	eng := testEngine(t, blockSize, 1,
		cloudpump.WithHedgePolicy(cloudpump.HedgePolicy{Threshold: 10 * time.Second}),
	)

	var got []byte
	synctest.Test(t, func(t *testing.T) {
		got = download(t, eng, reader)
		// No slow goroutines remain, but Wait confirms the bubble is clean
		// before we read the call count outside.
		synctest.Wait()
	})

	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch")
	}
	if n := reader.Calls(); n != 1 {
		t.Errorf("expected exactly 1 ReadChunk call (no hedge), got %d", n)
	}
}

// TestHedge_BothAttemptsFail: both the original attempt (stalls then errors)
// and the hedge attempt (immediate open error) fail. Download must return a
// non-nil error.
//
// call 1: delayErrorStream — stalls for 100ms (fake) then returns ECONNRESET
// call 2: open-time ECONNRESET (launched when hedge fires at 20ms fake)
func TestHedge_BothAttemptsFail(t *testing.T) {
	data := makeData(blockSize)
	reader := &hedgeBothFailReader{data: data}
	eng := testEngine(t, blockSize, 1,
		cloudpump.WithHedgePolicy(cloudpump.HedgePolicy{Threshold: 20 * time.Millisecond}),
	)

	synctest.Test(t, func(t *testing.T) {
		dst := t.TempDir() + "/out.bin"
		err := eng.Download(context.Background(), reader, dst)
		if err == nil {
			t.Error("expected Download to fail, got nil")
		}
		// Both attempts fail before delivering any bytes, so drainAndCancel
		// is never launched. Fake time advances inside fetchWithHedge to
		// unblock the slow attempt (call 1) before returning the error, so
		// all goroutines have already exited when Download returns.
	})
}

// hedgeBothFailReader: call 1 stalls for 100ms then errors; call 2 fails
// immediately at open time.
type hedgeBothFailReader struct {
	data []byte
	mu   sync.Mutex
	n    int
}

func (r *hedgeBothFailReader) Size(_ context.Context) (int64, error) {
	return int64(len(r.data)), nil
}

func (r *hedgeBothFailReader) ReadChunk(_ context.Context, offset, length int64) (io.ReadCloser, error) {
	r.mu.Lock()
	r.n++
	n := r.n
	r.mu.Unlock()
	if n == 1 {
		return &delayErrorStream{delay: 100 * time.Millisecond, err: syscall.ECONNRESET}, nil
	}
	return nil, syscall.ECONNRESET
}

// TestHedge_MultiChunk: three-chunk download where every chunk's first attempt
// is slow. The hedge fires for each chunk; the output must be correct.
// With concurrency=1 the ReadChunk call sequence is deterministic:
//
//	call 1 → chunk 0, attempt 1 (slow)  call 2 → chunk 0, attempt 2 (fast, wins)
//	call 3 → chunk 1, attempt 1 (slow)  call 4 → chunk 1, attempt 2 (fast, wins)
//	call 5 → chunk 2, attempt 1 (slow)  call 6 → chunk 2, attempt 2 (fast, wins)
func TestHedge_MultiChunk(t *testing.T) {
	const (
		threshold    = 30 * time.Millisecond
		attempt1Slow = 200 * time.Millisecond
	)
	data := makeData(3 * blockSize)
	reader := &delayReader{
		data:   data,
		delays: map[int]time.Duration{1: attempt1Slow, 3: attempt1Slow, 5: attempt1Slow},
	}
	eng := testEngine(t, blockSize, 1,
		cloudpump.WithHedgePolicy(cloudpump.HedgePolicy{Threshold: threshold}),
	)

	var got []byte
	synctest.Test(t, func(t *testing.T) {
		got = download(t, eng, reader)
		// Each of the 3 slow attempts is still sleeping in fake time after
		// its chunk's fast attempt won. Drain all of them.
		synctest.Wait()
		time.Sleep(attempt1Slow)
		synctest.Wait()
	})

	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch in multi-chunk hedge test")
	}
}
