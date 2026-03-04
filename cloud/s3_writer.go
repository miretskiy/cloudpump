package cloud

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// minS3PartSize is the S3-enforced minimum for all parts except the last.
const minS3PartSize = 5 << 20 // 5 MiB

// S3MultipartWriter implements [CloudChunkWriter] using the S3 Multipart
// Upload API. Each WriteChunk call issues one UploadPart RPC, which may
// execute concurrently from multiple goroutines.
//
// S3 constraints:
//   - Part numbers are 1-indexed and must be contiguous.
//   - All parts except the final one must be ≥ 5 MiB.
//   - Maximum 10,000 parts per upload.
type S3MultipartWriter struct {
	client   *s3.Client
	bucket   string
	key      string
	uploadID string

	mu    sync.Mutex
	parts []s3types.CompletedPart // guarded by mu
}

// NewS3MultipartWriter initiates a multipart upload and returns a writer.
//
// chunkSize is validated against the 5 MiB S3 minimum for non-final parts.
// When there is only one part (size ≤ chunkSize), the minimum does not apply.
func NewS3MultipartWriter(
	ctx context.Context,
	client *s3.Client,
	bucket, key string,
	size, chunkSize int64,
) (*S3MultipartWriter, error) {
	totalParts := (size + chunkSize - 1) / chunkSize
	if totalParts > 1 && chunkSize < minS3PartSize {
		return nil, fmt.Errorf(
			"s3: chunkSize %d B is below the 5 MiB minimum for S3 multipart uploads "+
				"(use -c %d or larger)",
			chunkSize, minS3PartSize)
	}

	resp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("s3: CreateMultipartUpload s3://%s/%s: %w", bucket, key, err)
	}
	return &S3MultipartWriter{
		client:   client,
		bucket:   bucket,
		key:      key,
		uploadID: *resp.UploadId,
	}, nil
}

// WriteChunk uploads data as the given part. Safe for concurrent use.
func (w *S3MultipartWriter) WriteChunk(ctx context.Context, partNum int, _ int64, data []byte) error {
	pn := int32(partNum)
	cl := int64(len(data))
	resp, err := w.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        &w.bucket,
		Key:           &w.key,
		UploadId:      &w.uploadID,
		PartNumber:    &pn,
		Body:          bytes.NewReader(data),
		ContentLength: &cl,
	})
	if err != nil {
		return fmt.Errorf("s3: UploadPart %d s3://%s/%s: %w", partNum, w.bucket, w.key, err)
	}

	// Store the ETag under lock; Commit will sort by part number.
	pnCopy := pn
	w.mu.Lock()
	w.parts = append(w.parts, s3types.CompletedPart{
		PartNumber: &pnCopy,
		ETag:       resp.ETag,
	})
	w.mu.Unlock()
	return nil
}

// Commit assembles all uploaded parts into the final object.
func (w *S3MultipartWriter) Commit(ctx context.Context) error {
	w.mu.Lock()
	parts := make([]s3types.CompletedPart, len(w.parts))
	copy(parts, w.parts)
	w.mu.Unlock()

	// S3 requires parts in ascending part-number order.
	sort.Slice(parts, func(i, j int) bool {
		return *parts[i].PartNumber < *parts[j].PartNumber
	})

	_, err := w.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &w.bucket,
		Key:             &w.key,
		UploadId:        &w.uploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		return fmt.Errorf("s3: CompleteMultipartUpload s3://%s/%s: %w", w.bucket, w.key, err)
	}
	return nil
}

// Abort cancels the multipart upload and discards all uploaded parts.
func (w *S3MultipartWriter) Abort(ctx context.Context) error {
	_, err := w.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   &w.bucket,
		Key:      &w.key,
		UploadId: &w.uploadID,
	})
	if err != nil {
		return fmt.Errorf("s3: AbortMultipartUpload s3://%s/%s: %w", w.bucket, w.key, err)
	}
	return nil
}
