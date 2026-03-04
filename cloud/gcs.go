package cloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"

	gstorage "cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// GCSReader implements CloudChunkReader for Google Cloud Storage objects.
//
// It calls NewRangeReader directly on the ObjectHandle, bypassing the
// transfermanager to keep memory allocation under the caller's control.
type GCSReader struct {
	obj *gstorage.ObjectHandle
}

// NewGCSReader constructs a GCSReader for the given GCS object handle.
func NewGCSReader(obj *gstorage.ObjectHandle) *GCSReader {
	return &GCSReader{obj: obj}
}

// NewGCSClient creates a GCS storage client, optionally using the provided
// http.Client so that chunk requests share the engine's tuned transport.
// Pass the returned client to client.Bucket(b).Object(k) to get an
// ObjectHandle for NewGCSReader.
func NewGCSClient(ctx context.Context, httpClient *http.Client) (*gstorage.Client, error) {
	opts := []option.ClientOption{}
	if httpClient != nil {
		opts = append(opts, option.WithHTTPClient(httpClient))
	}
	return gstorage.NewClient(ctx, opts...)
}

// Size returns the object size via Attrs.
func (r *GCSReader) Size(ctx context.Context) (int64, error) {
	attrs, err := r.obj.Attrs(ctx)
	if err != nil {
		return 0, fmt.Errorf("gcs: Attrs: %w", err)
	}
	return attrs.Size, nil
}

// ReadChunk opens a range-read stream for [offset, offset+length).
func (r *GCSReader) ReadChunk(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	rc, err := r.obj.NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, fmt.Errorf("gcs: NewRangeReader offset=%d length=%d: %w",
			offset, length, err)
	}
	return rc, nil
}

// GCSIsRetryable reports whether err warrants retrying a GCS chunk request.
//
// Detects:
//   - HTTP 408 (Request Timeout), 429 (Too Many Requests), and all 5xx
//     server-side errors via googleapi.Error.
//   - Transient syscall errors (ECONNRESET, ECONNABORTED, EPIPE).
//   - Network timeouts.
//   - Unexpected EOF (connection dropped mid-stream).
//
// Compose this with [cloudpump.DefaultIsRetryable] for full coverage:
//
//	func(err error) bool { return cloud.GCSIsRetryable(err) || cloudpump.DefaultIsRetryable(err) }
func GCSIsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		code := apiErr.Code
		return code == 408 || code == 429 || (code >= 500 && code < 600)
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
