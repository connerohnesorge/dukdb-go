//go:build !js || !wasm

// Package filesystem provides multipart upload support for S3.
package filesystem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/minio/minio-go/v7"
)

// MultipartUploader handles multipart uploads to S3.
// It splits large files into parts and uploads them concurrently.
type MultipartUploader struct {
	client      *minio.Client
	core        *minio.Core
	bucket      string
	key         string
	config      S3Config
	uploadID    string
	parts       []minio.CompletePart
	partsMu     sync.Mutex
	nextPartNum int
}

// NewMultipartUploader creates a new multipart uploader.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity
func NewMultipartUploader(
	client *minio.Client,
	bucket, key string,
	config S3Config,
) *MultipartUploader {
	// Wrap client in Core to access low-level APIs
	core := &minio.Core{Client: client}

	return &MultipartUploader{
		client:      client,
		core:        core,
		bucket:      bucket,
		key:         key,
		config:      config,
		parts:       make([]minio.CompletePart, 0),
		nextPartNum: 1,
	}
}

// Start initiates a multipart upload and returns the upload ID.
func (m *MultipartUploader) Start(ctx context.Context) error {
	uploadID, err := m.core.NewMultipartUpload(ctx, m.bucket, m.key, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("s3: failed to initiate multipart upload: %w", err)
	}

	m.uploadID = uploadID

	return nil
}

// UploadPart uploads a single part of the multipart upload.
func (m *MultipartUploader) UploadPart(ctx context.Context, partNum int, data []byte) error {
	reader := bytes.NewReader(data)

	objectPart, err := m.core.PutObjectPart(
		ctx,
		m.bucket,
		m.key,
		m.uploadID,
		partNum,
		reader,
		int64(len(data)),
		minio.PutObjectPartOptions{},
	)
	if err != nil {
		return fmt.Errorf("s3: failed to upload part %d: %w", partNum, err)
	}

	m.partsMu.Lock()
	m.parts = append(m.parts, minio.CompletePart{
		PartNumber: partNum,
		ETag:       objectPart.ETag,
	})
	m.partsMu.Unlock()

	return nil
}

// Complete finalizes the multipart upload.
func (m *MultipartUploader) Complete(ctx context.Context) error {
	m.partsMu.Lock()
	// Sort parts by part number
	sort.Slice(m.parts, func(i, j int) bool {
		return m.parts[i].PartNumber < m.parts[j].PartNumber
	})
	parts := m.parts
	m.partsMu.Unlock()

	_, err := m.core.CompleteMultipartUpload(
		ctx,
		m.bucket,
		m.key,
		m.uploadID,
		parts,
		minio.PutObjectOptions{},
	)
	if err != nil {
		return fmt.Errorf("s3: failed to complete multipart upload: %w", err)
	}

	return nil
}

// Abort cancels the multipart upload and cleans up any uploaded parts.
func (m *MultipartUploader) Abort(ctx context.Context) error {
	if m.uploadID == "" {
		return nil
	}

	err := m.core.AbortMultipartUpload(ctx, m.bucket, m.key, m.uploadID)
	if err != nil {
		return fmt.Errorf("s3: failed to abort multipart upload: %w", err)
	}

	return nil
}

// UploadData uploads data using multipart upload with concurrent parts.
func (m *MultipartUploader) UploadData(ctx context.Context, data []byte) error {
	if err := m.Start(ctx); err != nil {
		return err
	}

	partSize := m.config.MultipartPartSize
	if partSize < DefaultMultipartPartSize {
		partSize = DefaultMultipartPartSize
	}

	numParts := (int64(len(data)) + partSize - 1) / partSize
	concurrency := m.config.MultipartConcurrency
	if concurrency <= 0 {
		concurrency = DefaultMultipartConcurrency
	}

	// Create worker pool
	type uploadTask struct {
		partNum int
		data    []byte
	}

	tasks := make(chan uploadTask, numParts)
	errChan := make(chan error, numParts)

	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for task := range tasks {
				select {
				case <-ctx.Done():
					errChan <- ctx.Err()

					return
				default:
				}

				if err := m.UploadPart(ctx, task.partNum, task.data); err != nil {
					errChan <- err

					return
				}
			}
		}()
	}

	// Send tasks
	var offset int64

	for partNum := 1; int64(partNum) <= numParts; partNum++ {
		end := offset + partSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}

		partData := make([]byte, end-offset)
		copy(partData, data[offset:end])

		tasks <- uploadTask{partNum: partNum, data: partData}
		offset = end
	}

	close(tasks)
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			_ = m.Abort(ctx)

			return err
		}
	}

	return m.Complete(ctx)
}

// MultipartWriter is a streaming writer that uses multipart upload.
// It buffers data and uploads parts when the buffer reaches the part size.
type MultipartWriter struct {
	uploader    *MultipartUploader
	buffer      *bytes.Buffer
	partSize    int64
	nextPartNum int
	ctx         context.Context
	mu          sync.Mutex
	started     bool
	completed   bool
	err         error
}

// NewMultipartWriter creates a new streaming multipart writer.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity
func NewMultipartWriter(
	ctx context.Context,
	client *minio.Client,
	bucket, key string,
	config S3Config,
) *MultipartWriter {
	partSize := config.MultipartPartSize
	if partSize < DefaultMultipartPartSize {
		partSize = DefaultMultipartPartSize
	}

	return &MultipartWriter{
		uploader:    NewMultipartUploader(client, bucket, key, config),
		buffer:      bytes.NewBuffer(make([]byte, 0, partSize)),
		partSize:    partSize,
		nextPartNum: 1,
		ctx:         ctx,
	}
}

// Write writes data to the multipart upload buffer.
// When the buffer reaches the part size, it uploads the part.
func (w *MultipartWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, w.err
	}

	if w.completed {
		return 0, fmt.Errorf("s3: multipart upload already completed")
	}

	// Start upload if not started
	if !w.started {
		if err := w.uploader.Start(w.ctx); err != nil {
			w.err = err

			return 0, err
		}

		w.started = true
	}

	totalWritten := 0

	for len(p) > 0 {
		// Calculate how much we can write to the buffer
		remaining := w.partSize - int64(w.buffer.Len())
		toWrite := int64(len(p))

		if toWrite > remaining {
			toWrite = remaining
		}

		n, err := w.buffer.Write(p[:toWrite])
		if err != nil {
			w.err = err

			return totalWritten, err
		}

		totalWritten += n
		p = p[n:]

		// Upload part if buffer is full
		if int64(w.buffer.Len()) >= w.partSize {
			if err := w.flushBuffer(); err != nil {
				return totalWritten, err
			}
		}
	}

	return totalWritten, nil
}

// flushBuffer uploads the current buffer as a part.
func (w *MultipartWriter) flushBuffer() error {
	if w.buffer.Len() == 0 {
		return nil
	}

	data := make([]byte, w.buffer.Len())
	copy(data, w.buffer.Bytes())

	if err := w.uploader.UploadPart(w.ctx, w.nextPartNum, data); err != nil {
		w.err = err

		return err
	}

	w.nextPartNum++
	w.buffer.Reset()

	return nil
}

// Close completes the multipart upload.
// Any remaining buffered data is uploaded as the final part.
func (w *MultipartWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.completed {
		return nil
	}

	if w.err != nil {
		_ = w.uploader.Abort(w.ctx)

		return w.err
	}

	// Flush any remaining data
	if w.buffer.Len() > 0 {
		if err := w.flushBuffer(); err != nil {
			_ = w.uploader.Abort(w.ctx)

			return err
		}
	}

	// Complete upload only if we actually started it
	if w.started {
		if err := w.uploader.Complete(w.ctx); err != nil {
			_ = w.uploader.Abort(w.ctx)

			return err
		}
	}

	w.completed = true

	return nil
}

// Abort cancels the multipart upload.
func (w *MultipartWriter) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.completed || !w.started {
		return nil
	}

	return w.uploader.Abort(w.ctx)
}

// ConcurrentReader reads from S3 using multiple concurrent range requests.
type ConcurrentReader struct {
	client    *minio.Client
	bucket    string
	key       string
	config    S3Config
	size      int64
	chunkSize int64
	workers   int
}

// NewConcurrentReader creates a new concurrent reader.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity
func NewConcurrentReader(
	client *minio.Client,
	bucket, key string,
	size int64,
	config S3Config,
) *ConcurrentReader {
	chunkSize := config.ConcurrentReadChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultConcurrentReadChunkSize
	}

	workers := config.ConcurrentReadWorkers
	if workers <= 0 {
		workers = DefaultConcurrentReadWorkers
	}

	return &ConcurrentReader{
		client:    client,
		bucket:    bucket,
		key:       key,
		config:    config,
		size:      size,
		chunkSize: chunkSize,
		workers:   workers,
	}
}

// Range represents a byte range to read.
type Range struct {
	Start int64
	End   int64
}

// ReadRanges reads multiple byte ranges concurrently and returns the data.
func (r *ConcurrentReader) ReadRanges(ctx context.Context, ranges []Range) ([][]byte, error) {
	results := make([][]byte, len(ranges))
	errChan := make(chan error, len(ranges))

	type readTask struct {
		index int
		rng   Range
	}

	tasks := make(chan readTask, len(ranges))

	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < r.workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for task := range tasks {
				select {
				case <-ctx.Done():
					errChan <- ctx.Err()

					return
				default:
				}

				data, err := r.readRange(ctx, task.rng)
				if err != nil {
					errChan <- fmt.Errorf("failed to read range %d-%d: %w", task.rng.Start, task.rng.End, err)

					return
				}

				results[task.index] = data
			}
		}()
	}

	// Send tasks
	for i, rng := range ranges {
		tasks <- readTask{index: i, rng: rng}
	}

	close(tasks)
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// readRange reads a single byte range from S3.
func (r *ConcurrentReader) readRange(ctx context.Context, rng Range) ([]byte, error) {
	opts := minio.GetObjectOptions{}

	if err := opts.SetRange(rng.Start, rng.End); err != nil {
		return nil, fmt.Errorf("s3: failed to set range: %w", err)
	}

	reader, err := r.client.GetObject(ctx, r.bucket, r.key, opts)
	if err != nil {
		return nil, fmt.Errorf("s3: failed to get object: %w", err)
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("s3: failed to read data: %w", err)
	}

	return data, nil
}

// ReadAll reads the entire file using concurrent range requests.
func (r *ConcurrentReader) ReadAll(ctx context.Context) ([]byte, error) {
	if r.size <= 0 {
		// Size unknown, fall back to single read
		reader, err := r.client.GetObject(ctx, r.bucket, r.key, minio.GetObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("s3: failed to get object: %w", err)
		}
		defer func() { _ = reader.Close() }()

		return io.ReadAll(reader)
	}

	// Calculate ranges
	numChunks := (r.size + r.chunkSize - 1) / r.chunkSize
	ranges := make([]Range, 0, numChunks)

	var offset int64
	for offset < r.size {
		end := offset + r.chunkSize - 1
		if end >= r.size {
			end = r.size - 1
		}

		ranges = append(ranges, Range{Start: offset, End: end})
		offset = end + 1
	}

	// Read all ranges concurrently
	chunks, err := r.ReadRanges(ctx, ranges)
	if err != nil {
		return nil, err
	}

	// Concatenate results
	result := make([]byte, 0, r.size)
	for _, chunk := range chunks {
		result = append(result, chunk...)
	}

	return result, nil
}
