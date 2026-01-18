//go:build !js || !wasm

// Package filesystem provides S3File implementation for S3 object operations.
// S3File supports read, write, seek, and range request operations on S3 objects.
package filesystem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/minio/minio-go/v7"
)

// S3File represents an open S3 object for reading or writing.
// For read mode, data is streamed directly from S3 with range request support.
// For write mode, data is buffered in memory and uploaded on Close().
// S3File is safe for concurrent use - all operations are mutex-protected.
type S3File struct {
	// client is the minio client used for S3 operations.
	client *minio.Client
	// bucket is the S3 bucket name.
	bucket string
	// key is the S3 object key (path within the bucket).
	key string
	// reader is the underlying minio object reader for read operations.
	reader *minio.Object
	// offset is the current read position within the object.
	offset int64
	// size is the total size of the object (cached for SeekEnd).
	size int64
	// writeMode indicates if this file was opened for writing.
	writeMode bool
	// writeBuffer accumulates write data until Close() uploads it.
	writeBuffer *bytes.Buffer
	// mu protects concurrent access to file state.
	mu sync.Mutex
	// config holds S3 configuration for advanced features.
	config S3Config
	// useMultipart indicates whether to use multipart upload.
	useMultipart bool
	// multipartWriter is used for streaming multipart uploads.
	multipartWriter *MultipartWriter
}

// newS3FileForReading creates a new S3 file handle for reading from the given bucket and key.
// Data is streamed from S3 on Read calls with optional range request support.
func newS3FileForReading(client *minio.Client, bucket, key string) *S3File {
	return &S3File{
		client:    client,
		bucket:    bucket,
		key:       key,
		writeMode: false,
		config:    DefaultS3Config(),
	}
}

// newS3FileForReadingWithConfig creates a new S3 file handle for reading with configuration.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity
func newS3FileForReadingWithConfig(
	client *minio.Client,
	bucket, key string,
	config S3Config,
) *S3File {
	return &S3File{
		client:    client,
		bucket:    bucket,
		key:       key,
		writeMode: false,
		config:    config,
	}
}

// newS3FileForWriting creates a new S3 file handle for writing to the given bucket and key.
// Data is buffered in memory and uploaded to S3 when Close() is called.
func newS3FileForWriting(client *minio.Client, bucket, key string) *S3File {
	return &S3File{
		client:      client,
		bucket:      bucket,
		key:         key,
		writeMode:   true,
		writeBuffer: &bytes.Buffer{},
		config:      DefaultS3Config(),
	}
}

// newS3FileForWritingWithConfig creates a new S3 file handle for writing with configuration.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity
func newS3FileForWritingWithConfig(
	client *minio.Client,
	bucket, key string,
	config S3Config,
) *S3File {
	return &S3File{
		client:      client,
		bucket:      bucket,
		key:         key,
		writeMode:   true,
		writeBuffer: &bytes.Buffer{},
		config:      config,
	}
}

// newS3File creates a new S3 file handle for the given bucket and key.
// Deprecated: Use newS3FileForReading or newS3FileForWriting instead.
// This function is kept for backward compatibility with existing tests.
//
//nolint:revive // flag-parameter: kept for backward compatibility with existing tests
func newS3File(client *minio.Client, bucket, key string, write bool) *S3File {
	if write {
		return newS3FileForWriting(client, bucket, key)
	}

	return newS3FileForReading(client, bucket, key)
}

// Read reads data from the S3 object into p.
// The first Read call opens a connection to S3 and streams the data.
// Subsequent reads continue from where the previous read left off.
// Returns io.EOF when the end of the object is reached.
func (f *S3File) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode {
		return 0, errors.New("s3: cannot read in write mode")
	}

	// Lazily open the reader on first Read call.
	if f.reader == nil {
		if err := f.openReader(); err != nil {
			return 0, err
		}
	}

	n, err = f.reader.Read(p)
	f.offset += int64(n)

	return n, err
}

// openReader initializes the S3 object reader with optional range request.
// If offset > 0, uses an HTTP Range header to resume from that position.
func (f *S3File) openReader() error {
	ctx := context.Background()
	opts := minio.GetObjectOptions{}

	// Set range if we're resuming from a non-zero offset.
	if f.offset > 0 {
		if err := opts.SetRange(f.offset, 0); err != nil {
			return fmt.Errorf("s3: failed to set range: %w", err)
		}
	}

	var err error

	f.reader, err = f.client.GetObject(ctx, f.bucket, f.key, opts)
	if err != nil {
		return fmt.Errorf("s3: failed to get object: %w", err)
	}

	return nil
}

// Write writes data to the S3 object buffer.
// Data is accumulated in memory and uploaded when Close() is called.
// Returns an error if the file was opened for reading.
func (f *S3File) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writeMode {
		return 0, errors.New("s3: cannot write in read mode")
	}

	return f.writeBuffer.Write(p)
}

// Seek sets the offset for the next Read operation.
// Seeking is only supported in read mode.
// Seeking closes the current reader; a new one opens on the next Read.
func (f *S3File) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode {
		return 0, errors.New("s3: seek not supported in write mode")
	}

	// Close existing reader - a new one will open on next Read.
	if f.reader != nil {
		_ = f.reader.Close()
		f.reader = nil
	}

	newOffset, err := f.calculateOffset(offset, whence)
	if err != nil {
		return 0, err
	}

	if newOffset < 0 {
		return 0, errors.New("s3: negative seek position")
	}

	f.offset = newOffset

	return f.offset, nil
}

// calculateOffset computes the new offset based on whence and current position.
// For SeekEnd, fetches object size from S3 if not already cached.
func (f *S3File) calculateOffset(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		return offset, nil
	case io.SeekCurrent:
		return f.offset + offset, nil
	case io.SeekEnd:
		// Fetch size from S3 if not cached.
		if f.size == 0 {
			ctx := context.Background()

			info, err := f.client.StatObject(ctx, f.bucket, f.key, minio.StatObjectOptions{})
			if err != nil {
				return 0, fmt.Errorf("s3: failed to get object size: %w", err)
			}

			f.size = info.Size
		}

		return f.size + offset, nil
	default:
		return 0, errors.New("s3: invalid seek whence")
	}
}

// ReadAt reads len(p) bytes starting at offset off using S3 range requests.
// This is more efficient than Seek+Read for random access patterns.
// ReadAt does not affect the current file offset.
func (f *S3File) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	ctx := context.Background()
	opts := minio.GetObjectOptions{}

	// Use HTTP Range header for efficient partial reads.
	err = opts.SetRange(off, off+int64(len(p))-1)
	if err != nil {
		return 0, fmt.Errorf("s3: failed to set range: %w", err)
	}

	reader, err := f.client.GetObject(ctx, f.bucket, f.key, opts)
	if err != nil {
		return 0, fmt.Errorf("s3: failed to get object: %w", err)
	}
	defer func() { _ = reader.Close() }()

	return io.ReadFull(reader, p)
}

// WriteAt writes data at the specified offset in the write buffer.
// Note: S3 does not support partial writes. All data is buffered in memory
// and uploaded as a single object when Close() is called.
// For sparse writes, zero-byte padding is inserted to fill gaps.
func (f *S3File) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writeMode {
		return 0, errors.New("s3: cannot write in read mode")
	}

	// Pad buffer with zeros if writing beyond current length.
	currentLen := int64(f.writeBuffer.Len())
	if off > currentLen {
		padding := make([]byte, off-currentLen)

		_, _ = f.writeBuffer.Write(padding)
	}

	// Append if writing at or beyond buffer end.
	if off >= int64(f.writeBuffer.Len()) {
		return f.writeBuffer.Write(p)
	}

	// Overwrite existing data in buffer.
	buf := f.writeBuffer.Bytes()
	copy(buf[off:], p)

	return len(p), nil
}

// Stat returns file info for this S3 object by calling HeadObject on S3.
func (f *S3File) Stat() (FileInfo, error) {
	ctx := context.Background()

	info, err := f.client.StatObject(ctx, f.bucket, f.key, minio.StatObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("s3: failed to stat object: %w", err)
	}

	return &S3FileInfo{info: info}, nil
}

// Close closes the S3 file and releases resources.
// For write mode, uploads buffered data to S3 using PutObject or multipart upload.
// For read mode, closes the underlying reader if open.
// Close must be called to ensure data is persisted for write operations.
func (f *S3File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.reader != nil {
		_ = f.reader.Close()
		f.reader = nil
	}

	// Close multipart writer if in use
	if f.multipartWriter != nil {
		return f.multipartWriter.Close()
	}

	if f.writeMode && f.writeBuffer != nil && f.writeBuffer.Len() > 0 {
		return f.upload()
	}

	return nil
}

// upload uploads the buffered write data to S3 using PutObject or multipart upload.
// Called internally by Close() when there is data to upload.
// Uses multipart upload for data exceeding the multipart threshold.
func (f *S3File) upload() error {
	ctx := context.Background()
	dataLen := int64(f.writeBuffer.Len())

	// Use multipart upload for large data
	threshold := f.config.MultipartThreshold
	if threshold <= 0 {
		threshold = DefaultMultipartThreshold
	}

	if dataLen >= threshold {
		return f.uploadMultipart(ctx)
	}

	// Use simple PutObject for smaller data
	return f.uploadSimple(ctx)
}

// uploadSimple uploads data using simple PutObject.
func (f *S3File) uploadSimple(ctx context.Context) error {
	uploadFn := func() (struct{}, error) {
		_, err := f.client.PutObject(
			ctx,
			f.bucket,
			f.key,
			f.writeBuffer,
			int64(f.writeBuffer.Len()),
			minio.PutObjectOptions{},
		)
		if err != nil {
			return struct{}{}, fmt.Errorf("s3: failed to upload object: %w", err)
		}
		return struct{}{}, nil
	}

	// Apply retry logic if configured
	if f.config.RetryConfig.MaxRetries > 0 {
		_, err := WithRetryFunc(ctx, f.config.RetryConfig, uploadFn)
		if err != nil {
			return err
		}
	} else {
		if _, err := uploadFn(); err != nil {
			return err
		}
	}

	f.writeBuffer.Reset()

	return nil
}

// uploadMultipart uploads data using multipart upload for large files.
func (f *S3File) uploadMultipart(ctx context.Context) error {
	uploader := NewMultipartUploader(f.client, f.bucket, f.key, f.config)

	data := f.writeBuffer.Bytes()
	if err := uploader.UploadData(ctx, data); err != nil {
		return err
	}

	f.writeBuffer.Reset()

	return nil
}

// EnableMultipartStreaming enables streaming multipart upload mode.
// In this mode, data is uploaded as parts during Write() calls instead of buffering.
// This is useful for very large files that exceed available memory.
func (f *S3File) EnableMultipartStreaming(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writeMode {
		return errors.New("s3: multipart streaming only available in write mode")
	}

	if f.multipartWriter != nil {
		return errors.New("s3: multipart streaming already enabled")
	}

	f.multipartWriter = NewMultipartWriter(ctx, f.client, f.bucket, f.key, f.config)
	f.useMultipart = true

	// Flush any existing buffered data to multipart writer
	if f.writeBuffer != nil && f.writeBuffer.Len() > 0 {
		_, err := f.multipartWriter.Write(f.writeBuffer.Bytes())
		if err != nil {
			return fmt.Errorf("s3: failed to flush buffer to multipart writer: %w", err)
		}

		f.writeBuffer.Reset()
	}

	return nil
}

// ReadContext reads data with context support for cancellation.
func (f *S3File) ReadContext(ctx context.Context, p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode {
		return 0, errors.New("s3: cannot read in write mode")
	}

	readFn := func() (int, error) {
		// Lazily open the reader on first Read call.
		if f.reader == nil {
			if err := f.openReaderWithContext(ctx); err != nil {
				return 0, err
			}
		}

		n, err := f.reader.Read(p)
		f.offset += int64(n)

		return n, err
	}

	// Apply retry for transient errors
	if f.config.RetryConfig.MaxRetries > 0 && IsRetryableError(err) {
		return WithRetryFunc(ctx, f.config.RetryConfig, readFn)
	}

	return readFn()
}

// openReaderWithContext initializes the S3 object reader with context support.
func (f *S3File) openReaderWithContext(ctx context.Context) error {
	opts := minio.GetObjectOptions{}

	// Set range if we're resuming from a non-zero offset.
	if f.offset > 0 {
		if err := opts.SetRange(f.offset, 0); err != nil {
			return fmt.Errorf("s3: failed to set range: %w", err)
		}
	}

	var err error

	f.reader, err = f.client.GetObject(ctx, f.bucket, f.key, opts)
	if err != nil {
		return fmt.Errorf("s3: failed to get object: %w", err)
	}

	return nil
}

// WriteContext writes data with context support for cancellation.
func (f *S3File) WriteContext(ctx context.Context, p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writeMode {
		return 0, errors.New("s3: cannot write in read mode")
	}

	// Use multipart writer if enabled
	if f.multipartWriter != nil {
		return f.multipartWriter.Write(p)
	}

	// Check if context is done
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	return f.writeBuffer.Write(p)
}

// ReadAtContext reads data at offset with context support.
func (f *S3File) ReadAtContext(ctx context.Context, p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	readAtFn := func() (int, error) {
		opts := minio.GetObjectOptions{}

		// Use HTTP Range header for efficient partial reads.
		err := opts.SetRange(off, off+int64(len(p))-1)
		if err != nil {
			return 0, fmt.Errorf("s3: failed to set range: %w", err)
		}

		reader, err := f.client.GetObject(ctx, f.bucket, f.key, opts)
		if err != nil {
			return 0, fmt.Errorf("s3: failed to get object: %w", err)
		}
		defer func() { _ = reader.Close() }()

		return io.ReadFull(reader, p)
	}

	// Apply retry logic if configured
	if f.config.RetryConfig.MaxRetries > 0 {
		return WithRetryFunc(ctx, f.config.RetryConfig, readAtFn)
	}

	return readAtFn()
}

// Verify S3File implements File interface at compile time.
var _ File = (*S3File)(nil)
