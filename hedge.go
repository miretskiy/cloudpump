package cloudpump

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/miretskiy/cloudpump/cloud"
)

// ── HedgePolicy ───────────────────────────────────────────────────────────────

// HedgePolicy configures speculative parallel chunk requests.
//
// When a ReadChunk call does not deliver its first byte within the threshold,
// a second parallel ReadChunk is issued for the same byte range. Whichever
// delivers its first byte first is used; the other is cancelled.
//
// This addresses tail latency caused by cloud LBs routing requests to
// temporarily slow backend nodes. Racing a second attempt drives the
// probability that both land on a slow node from p to p².
//
// The hedge fires based on time-to-first-byte (TTFB): the interval from
// the ReadChunk call to the first byte arriving from the response body.
// It does not replace the retry policy — if the winning stream later fails
// mid-body, [RetryPolicy] handles the reconnect from the partial-progress
// point as usual.
type HedgePolicy struct {
	// Threshold is the TTFB deadline after which the hedge fires.
	// Zero disables hedging entirely.
	Threshold time.Duration

	// AutoThreshold adapts the threshold to the observed p99 TTFB.
	// Threshold is used as the fallback until ttfbMinSamples chunks have
	// been observed. Requires Threshold > 0 (AutoThreshold alone has no effect).
	AutoThreshold bool
}

// DefaultHedgePolicy returns a policy with a 1 s initial threshold and
// automatic p99-based tuning. Suitable for cloud storage workloads where
// TTFB is typically <100 ms and the p99 captures slow-node events.
func DefaultHedgePolicy() HedgePolicy {
	return HedgePolicy{Threshold: time.Second, AutoThreshold: true}
}

// NoHedgePolicy disables hedging. This is the engine default.
func NoHedgePolicy() HedgePolicy { return HedgePolicy{} }

// WithHedgePolicy enables speculative parallel ReadChunk requests.
// See [HedgePolicy] for details.
func WithHedgePolicy(p HedgePolicy) Option {
	return func(e *Engine) { e.hedge = p }
}

// ── TTFB tracker ──────────────────────────────────────────────────────────────

const (
	ttfbCap        = 512 // ring-buffer capacity
	ttfbMinSamples = 100 // minimum samples before auto-threshold is trusted

	// ttfbProjectionFactor is multiplied by the threshold to produce a
	// projected latency sample for a canceled slow attempt. Must be > 1 to
	// prevent the p99 estimate from drifting downward by excluding slow
	// samples that were cut short.
	ttfbProjectionFactor = 1.1
	ttfbProjectionMax    = 10 * time.Second
)

// ttfbTracker is a fixed-capacity ring buffer of TTFB observations that
// supports approximate p99 computation. Safe for concurrent use.
type ttfbTracker struct {
	mu      sync.Mutex
	samples [ttfbCap]time.Duration
	head    int // next write position
	count   int // valid entries (≤ ttfbCap)
}

func newTTFBTracker() *ttfbTracker { return &ttfbTracker{} }

func (t *ttfbTracker) record(d time.Duration) {
	t.mu.Lock()
	t.samples[t.head] = d
	t.head = (t.head + 1) % ttfbCap
	if t.count < ttfbCap {
		t.count++
	}
	t.mu.Unlock()
}

// recordProjected adds a synthetic sample for a cancelled attempt.
// Projecting threshold×1.1 prevents the tracker from being biased toward
// fast samples when slow attempts are systematically cut short.
func (t *ttfbTracker) recordProjected(threshold time.Duration) {
	p := time.Duration(float64(threshold) * ttfbProjectionFactor)
	if p > ttfbProjectionMax {
		p = ttfbProjectionMax
	}
	t.record(p)
}

// p99 returns the 99th-percentile TTFB and true if at least ttfbMinSamples
// have been recorded. Returns (0, false) during the cold-start period.
func (t *ttfbTracker) p99() (time.Duration, bool) {
	t.mu.Lock()
	n := t.count
	if n < ttfbMinSamples {
		t.mu.Unlock()
		return 0, false
	}
	live := make([]time.Duration, n)
	for i := range n {
		live[i] = t.samples[(t.head-n+i+ttfbCap)%ttfbCap]
	}
	t.mu.Unlock()

	sort.Slice(live, func(i, j int) bool { return live[i] < live[j] })
	return live[int(float64(n-1)*0.99)], true
}

// ── Engine helpers ────────────────────────────────────────────────────────────

// currentThreshold returns the effective hedge threshold for this request.
// When AutoThreshold is on and enough samples exist, uses the observed p99;
// otherwise returns HedgePolicy.Threshold (or 0 if hedging is disabled).
func (e *Engine) currentThreshold() time.Duration {
	if e.hedge.Threshold <= 0 {
		return 0
	}
	if e.hedge.AutoThreshold && e.ttfb != nil {
		if p99, ok := e.ttfb.p99(); ok {
			if p99 > ttfbProjectionMax {
				p99 = ttfbProjectionMax
			}
			return p99
		}
	}
	return e.hedge.Threshold
}

// ── fetchWithHedge ────────────────────────────────────────────────────────────

// fetchWithHedge fetches [offset, offset+length) into dst, racing a second
// ReadChunk if the first does not deliver its initial byte within the hedge
// threshold. Returns bytes written and any error.
//
// When hedging is disabled (threshold == 0), delegates directly to fetchOnce.
//
// Goroutine contract: at most two goroutines are live simultaneously (one per
// racing attempt). The loser is cancelled and drained — either synchronously
// before this function returns, or in a short-lived cleanup goroutine.
func (e *Engine) fetchWithHedge(
	ctx context.Context,
	src cloud.CloudChunkReader,
	dst []byte,
	offset, length int64,
) (int64, error) {
	threshold := e.currentThreshold()
	if threshold <= 0 {
		return fetchOnce(ctx, src, dst, offset, length)
	}

	type raceResult struct {
		first  byte
		stream io.ReadCloser
		cancel context.CancelFunc // keep alive while reading stream
		ttfb   time.Duration
		err    error
	}

	start := time.Now()

	// openAttempt launches a goroutine that opens a ReadChunk stream and reads
	// exactly one byte to establish TTFB. Sends exactly one raceResult on the
	// returned buffered channel. The cancel func must be called by the caller
	// after it is done with the stream (or immediately on error/lose).
	openAttempt := func() (<-chan raceResult, context.CancelFunc) {
		childCtx, cc := context.WithCancel(ctx)
		ch := make(chan raceResult, 1)
		go func() {
			stream, err := src.ReadChunk(childCtx, offset, length)
			if err != nil {
				cc()
				ch <- raceResult{err: err, cancel: func() {}}
				return
			}
			var buf [1]byte
			_, err = io.ReadFull(stream, buf[:])
			if err != nil {
				_ = stream.Close()
				cc()
				ch <- raceResult{err: err, cancel: func() {}}
				return
			}
			// Success: caller owns the stream and must call cancel when done.
			ch <- raceResult{first: buf[0], stream: stream, cancel: cc, ttfb: time.Since(start)}
		}()
		return ch, cc
	}

	// drainAndCancel cancels an attempt and discards its result, closing any
	// open stream. Intended for use as a goroutine for the losing attempt.
	drainAndCancel := func(ch <-chan raceResult, cc context.CancelFunc) {
		cc()
		if r := <-ch; r.stream != nil {
			_ = r.stream.Close()
			r.cancel()
		}
	}

	ch1, cancel1 := openAttempt()

	timer := time.NewTimer(threshold)
	defer timer.Stop()

	// Phase 1: wait for attempt 1 or the hedge threshold.
	select {
	case r := <-ch1:
		if r.err != nil {
			cancel1()
			return 0, r.err
		}
		e.ttfb.record(r.ttfb)
		n, err := fillFromStream(dst, r.first, r.stream, length)
		r.cancel()
		return n, err

	case <-timer.C:
		// Attempt 1 is slow. Record projected sample, launch hedge.
		e.ttfb.recordProjected(threshold)
		ch2, cancel2 := openAttempt()

		// Phase 2: use whichever attempt delivers its first byte first.
		// Nil a channel after consuming it to remove it from the select.
		var lastErr error
		for ch1 != nil || ch2 != nil {
			select {
			case r := <-ch1:
				ch1 = nil
				if r.err != nil {
					cancel1()
					lastErr = r.err
					continue
				}
				if ch2 != nil {
					go drainAndCancel(ch2, cancel2)
				}
				e.ttfb.record(r.ttfb)
				n, err := fillFromStream(dst, r.first, r.stream, length)
				r.cancel()
				return n, err

			case r := <-ch2:
				ch2 = nil
				if r.err != nil {
					cancel2()
					lastErr = r.err
					continue
				}
				if ch1 != nil {
					go drainAndCancel(ch1, cancel1)
				}
				e.ttfb.record(r.ttfb)
				n, err := fillFromStream(dst, r.first, r.stream, length)
				r.cancel()
				return n, err

			case <-ctx.Done():
				if ch1 != nil {
					go drainAndCancel(ch1, cancel1)
				}
				if ch2 != nil {
					go drainAndCancel(ch2, cancel2)
				}
				return 0, ctx.Err()
			}
		}
		return 0, fmt.Errorf("cloudpump: hedge: all attempts failed: %w", lastErr)

	case <-ctx.Done():
		go drainAndCancel(ch1, cancel1)
		return 0, ctx.Err()
	}
}

// fillFromStream writes first into dst[0], reads the remaining length-1 bytes
// from stream into dst[1:], then closes stream. Returns total bytes written.
func fillFromStream(dst []byte, first byte, stream io.ReadCloser, length int64) (int64, error) {
	defer stream.Close()
	if length <= 0 {
		return 0, nil
	}
	dst[0] = first
	if length == 1 {
		return 1, nil
	}
	n, err := io.ReadFull(stream, dst[1:int(length)])
	return int64(n) + 1, err
}
