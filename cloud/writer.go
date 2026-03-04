package cloud

import "context"

// CloudChunkWriter abstracts a parallel multipart upload session for a single
// cloud object. Multiple goroutines may call WriteChunk concurrently; the
// implementation is responsible for any serialisation required by the provider.
//
// Lifecycle:
//
//  1. Create (e.g. [NewS3MultipartWriter] or [NewGCSWriter]).
//  2. Call WriteChunk concurrently from N workers.
//  3. On success: call Commit to finalise.
//  4. On failure: call Abort to cancel and clean up.
type CloudChunkWriter interface {
	// WriteChunk uploads data as part partNum (1-indexed, contiguous from 1).
	// offset is the byte offset of data within the source object.
	// Concurrent calls with different partNums are safe.
	WriteChunk(ctx context.Context, partNum int, offset int64, data []byte) error

	// Commit finalises the upload after all WriteChunk calls have succeeded.
	Commit(ctx context.Context) error

	// Abort cancels the in-progress upload and removes any partial remote state.
	// A best-effort call: the error may be ignored if a higher-level error
	// already dominates.
	Abort(ctx context.Context) error
}
