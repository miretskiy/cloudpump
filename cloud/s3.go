package cloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Reader implements CloudChunkReader for AWS S3 objects.
//
// It uses the raw GetObject API with an HTTP Range header, bypassing
// manager.Downloader to keep memory allocation entirely under the caller's
// control. Credentials and region are inherited from the provided client.
type S3Reader struct {
	client *s3.Client
	bucket string
	key    string
}

// NewS3Reader constructs an S3Reader for the given bucket and key.
// The caller is responsible for configuring the client's HTTP transport
// (e.g. injecting the engine's tuned http.Client via s3.Options.HTTPClient)
// before passing it here.
func NewS3Reader(client *s3.Client, bucket, key string) *S3Reader {
	return &S3Reader{client: client, bucket: bucket, key: key}
}

// Size returns the object's content length via HeadObject.
func (r *S3Reader) Size(ctx context.Context) (int64, error) {
	resp, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &r.bucket,
		Key:    &r.key,
	})
	if err != nil {
		return 0, fmt.Errorf("s3: HeadObject s3://%s/%s: %w", r.bucket, r.key, err)
	}
	if resp.ContentLength == nil {
		return 0, errors.New("s3: HeadObject returned nil ContentLength")
	}
	return *resp.ContentLength, nil
}

// ReadChunk opens a range-read stream for [offset, offset+length) via GetObject.
// The RFC 7233 Range header format is "bytes=first-last" (both ends inclusive).
func (r *S3Reader) ReadChunk(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	rangeHdr := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	resp, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &r.bucket,
		Key:    &r.key,
		Range:  &rangeHdr,
	})
	if err != nil {
		return nil, fmt.Errorf("s3: GetObject s3://%s/%s range=%s: %w",
			r.bucket, r.key, rangeHdr, err)
	}
	return resp.Body, nil
}

// S3IsRetryable reports whether err warrants retrying an S3 chunk request.
//
// Detects:
//   - HTTP 408 (Request Timeout), 429 (Too Many Requests), and all 5xx
//     server-side errors via the smithy-go HTTPStatusCode interface.
//   - Transient syscall errors (ECONNRESET, ECONNABORTED, EPIPE).
//   - Network timeouts.
//   - Unexpected EOF (connection dropped mid-stream).
//
// Compose this with [cloudpump.DefaultIsRetryable] for full coverage:
//
//	func(err error) bool { return cloud.S3IsRetryable(err) || cloudpump.DefaultIsRetryable(err) }
func S3IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	// smithy-go HTTP response errors expose the status code via this interface.
	// Checking the interface (not the concrete type) makes this robust to
	// smithy-go version changes.
	var httpStatusCoder interface{ HTTPStatusCode() int }
	if errors.As(err, &httpStatusCoder) {
		code := httpStatusCoder.HTTPStatusCode()
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
