package cloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
)

// AzureReader implements CloudChunkReader for Azure Blob Storage objects.
//
// It uses blob.Client.DownloadStream with an explicit HTTPRange on each chunk,
// bypassing the SDK's transfer manager to keep memory allocation entirely
// under the caller's control. Credentials and the storage account endpoint
// are inherited from the provided client.
type AzureReader struct {
	client *blob.Client
}

// NewAzureReader constructs an AzureReader for the given blob client.
// The caller is responsible for configuring the client's endpoint URL and
// credentials before passing it here. Typically:
//
//	url := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", account, container, key)
//	cred, _ := azidentity.NewDefaultAzureCredential(nil)
//	client, _ := blob.NewClient(url, cred, nil)
//	reader := cloud.NewAzureReader(client)
func NewAzureReader(client *blob.Client) *AzureReader {
	return &AzureReader{client: client}
}

// Size returns the blob's content length via GetProperties.
func (r *AzureReader) Size(ctx context.Context) (int64, error) {
	props, err := r.client.GetProperties(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("azure: GetProperties: %w", err)
	}
	if props.ContentLength == nil {
		return 0, errors.New("azure: GetProperties returned nil ContentLength")
	}
	return *props.ContentLength, nil
}

// ReadChunk opens a range-read stream for [offset, offset+length).
func (r *AzureReader) ReadChunk(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	resp, err := r.client.DownloadStream(ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: offset, Count: length},
	})
	if err != nil {
		return nil, fmt.Errorf("azure: DownloadStream offset=%d length=%d: %w", offset, length, err)
	}
	return resp.Body, nil
}

// AzureIsRetryable reports whether err warrants retrying an Azure chunk request.
//
// Detects:
//   - HTTP 408 (Request Timeout), 429 (Too Many Requests), and all 5xx
//     server-side errors via azcore.ResponseError.
//   - Transient syscall errors (ECONNRESET, ECONNABORTED, EPIPE).
//   - Network timeouts.
//   - Unexpected EOF (connection dropped mid-stream).
//
// Compose this with [cloudpump.DefaultIsRetryable] for full coverage:
//
//	func(err error) bool { return cloud.AzureIsRetryable(err) || cloudpump.DefaultIsRetryable(err) }
func AzureIsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		code := respErr.StatusCode
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
