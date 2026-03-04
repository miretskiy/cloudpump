// Package cloud defines the CloudChunkReader interface and provides
// cloud-storage implementations for S3 and GCS.
//
// Implementations deliberately avoid the high-level transfer helpers
// (manager.Downloader, transfermanager) whose hidden buffering and
// goroutine management would conflict with cloudpump's explicit memory
// accounting. Instead, each implementation issues a raw ranged request
// and returns the response body directly to the engine.
package cloud

import (
	"context"
	"io"
)

// CloudChunkReader abstracts range-read access to a single cloud object.
//
// Implementations must be safe for concurrent use by multiple goroutines,
// because the engine calls ReadChunk from several workers simultaneously.
type CloudChunkReader interface {
	// Size returns the total object size in bytes.
	// Called exactly once before chunking begins.
	Size(ctx context.Context) (int64, error)

	// ReadChunk opens a streaming range-read for the half-open byte range
	// [offset, offset+length). The caller is responsible for closing the
	// returned ReadCloser. Each call may consume a connection from the
	// underlying HTTP pool; Close releases it back.
	ReadChunk(ctx context.Context, offset, length int64) (io.ReadCloser, error)
}
