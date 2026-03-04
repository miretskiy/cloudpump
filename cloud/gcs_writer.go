package cloud

import (
	"context"
	"fmt"
	"sync"

	gstorage "cloud.google.com/go/storage"
)

// GCSWriter implements [CloudChunkWriter] for Google Cloud Storage using a
// single resumable upload stream (obj.NewWriter).
//
// GCS does not support true parallel random-write uploads via the JSON API,
// so chunks are serialised internally in offset order. Parallel reads from
// the local NVMe still fully utilise disk bandwidth; the GCS connection
// becomes the throughput ceiling, which is expected for a single stream.
//
// Out-of-order chunks (which arise because the errgroup dispatches goroutines
// concurrently) are buffered in a small in-memory map until their predecessor
// has been written. The map size is bounded by Engine.concurrency.
type GCSWriter struct {
	wc  *gstorage.Writer
	obj *gstorage.ObjectHandle // retained for Abort (delete partial object)

	mu       sync.Mutex
	nextOff  int64
	pending  map[int64][]byte // out-of-order chunks keyed by byte offset
	firstErr error            // first write error; gate all subsequent calls
}

// NewGCSWriter opens a resumable upload stream for obj.
func NewGCSWriter(ctx context.Context, obj *gstorage.ObjectHandle) *GCSWriter {
	return &GCSWriter{
		wc:      obj.NewWriter(ctx),
		obj:     obj,
		pending: make(map[int64][]byte),
	}
}

// WriteChunk accepts data at offset and writes it to the GCS stream in order.
// Chunks that arrive out of order are copied into a temporary buffer and
// written as soon as their predecessor completes. Safe for concurrent use.
func (w *GCSWriter) WriteChunk(_ context.Context, _ int, offset int64, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.firstErr != nil {
		return w.firstErr
	}

	if offset == w.nextOff {
		// In-order: write directly. data is still backed by the caller's
		// mmap slab (which is alive until WriteChunk returns), so no copy needed.
		if err := w.writeAndFlush(data, offset); err != nil {
			w.firstErr = err
			return err
		}
		return nil
	}

	// Out-of-order: copy data so the caller's mmap slab can be returned to
	// the pool immediately after WriteChunk returns.
	cp := make([]byte, len(data))
	copy(cp, data)
	w.pending[offset] = cp
	return nil
}

// writeAndFlush writes data at expectedOff to the GCS stream, then drains
// any contiguous pending chunks. Must be called with w.mu held.
func (w *GCSWriter) writeAndFlush(data []byte, expectedOff int64) error {
	if _, err := w.wc.Write(data); err != nil {
		return err
	}
	w.nextOff = expectedOff + int64(len(data))

	// Drain contiguous buffered chunks.
	for {
		chunk, ok := w.pending[w.nextOff]
		if !ok {
			break
		}
		delete(w.pending, w.nextOff)
		if _, err := w.wc.Write(chunk); err != nil {
			return err
		}
		w.nextOff += int64(len(chunk))
	}
	return nil
}

// Commit closes the GCS writer and finalises the object.
func (w *GCSWriter) Commit(_ context.Context) error {
	if err := w.wc.Close(); err != nil {
		return fmt.Errorf("gcs: writer close: %w", err)
	}
	return nil
}

// Abort closes the GCS writer and deletes any partial upload.
func (w *GCSWriter) Abort(ctx context.Context) error {
	// Close the writer to release the upload stream (error is expected here).
	_ = w.wc.Close()
	// Delete the partial object; best-effort.
	if err := w.obj.Delete(ctx); err != nil {
		return fmt.Errorf("gcs: delete partial object: %w", err)
	}
	return nil
}
