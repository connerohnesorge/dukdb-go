//go:build !js || !wasm

// Package filesystem provides GCSFile implementation for GCS object operations.
// GCSFile supports read, write, seek, and range request operations on GCS objects.
package filesystem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"cloud.google.com/go/storage"
)

// GCSFile represents an open GCS object for reading or writing.
// For read mode, data is streamed directly from GCS with range request support.
// For write mode, data is buffered in memory and uploaded on Close().
// GCSFile is safe for concurrent use - all operations are mutex-protected.
type GCSFile struct {
	// client is the GCS client used for operations.
	client *storage.Client
	// bucket is the GCS bucket name.
	bucket string
	// object is the GCS object name (path within the bucket).
	object string
	// reader is the underlying GCS object reader for read operations.
	reader *storage.Reader
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
	// config holds GCS configuration for advanced features.
	config GCSConfig
}

// newGCSFileForReading creates a new GCS file handle for reading from the given bucket and object.
// Data is streamed from GCS on Read calls with optional range request support.
func newGCSFileForReading(client *storage.Client, bucket, object string) *GCSFile {
	return &GCSFile{
		client:    client,
		bucket:    bucket,
		object:    object,
		writeMode: false,
		config:    DefaultGCSConfig(),
	}
}

// newGCSFileForReadingWithConfig creates a new GCS file handle for reading with configuration.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity
func newGCSFileForReadingWithConfig(client *storage.Client, bucket, object string, config GCSConfig) *GCSFile {
	return &GCSFile{
		client:    client,
		bucket:    bucket,
		object:    object,
		writeMode: false,
		config:    config,
	}
}

// newGCSFileForWriting creates a new GCS file handle for writing to the given bucket and object.
// Data is buffered in memory and uploaded to GCS when Close() is called.
func newGCSFileForWriting(client *storage.Client, bucket, object string) *GCSFile {
	return &GCSFile{
		client:      client,
		bucket:      bucket,
		object:      object,
		writeMode:   true,
		writeBuffer: &bytes.Buffer{},
		config:      DefaultGCSConfig(),
	}
}

// newGCSFileForWritingWithConfig creates a new GCS file handle for writing with configuration.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity
func newGCSFileForWritingWithConfig(client *storage.Client, bucket, object string, config GCSConfig) *GCSFile {
	return &GCSFile{
		client:      client,
		bucket:      bucket,
		object:      object,
		writeMode:   true,
		writeBuffer: &bytes.Buffer{},
		config:      config,
	}
}

// Read reads data from the GCS object into p.
// The first Read call opens a connection to GCS and streams the data.
// Subsequent reads continue from where the previous read left off.
// Returns io.EOF when the end of the object is reached.
func (f *GCSFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode {
		return 0, errors.New("gcs: cannot read in write mode")
	}

	// Lazily open the reader on first Read call.
	if f.reader == nil {
		if err := f.openReader(context.Background()); err != nil {
			return 0, err
		}
	}

	n, err = f.reader.Read(p)
	f.offset += int64(n)

	return n, err
}

// openReader initializes the GCS object reader with optional range request.
// If offset > 0, uses a range reader to resume from that position.
func (f *GCSFile) openReader(ctx context.Context) error {
	obj := f.client.Bucket(f.bucket).Object(f.object)

	var err error
	if f.offset > 0 {
		// Use NewRangeReader for offset reads
		f.reader, err = obj.NewRangeReader(ctx, f.offset, -1)
	} else {
		f.reader, err = obj.NewReader(ctx)
	}

	if err != nil {
		return fmt.Errorf("gcs: failed to get object: %w", err)
	}

	return nil
}

// Write writes data to the GCS object buffer.
// Data is accumulated in memory and uploaded when Close() is called.
// Returns an error if the file was opened for reading.
func (f *GCSFile) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writeMode {
		return 0, errors.New("gcs: cannot write in read mode")
	}

	return f.writeBuffer.Write(p)
}

// Seek sets the offset for the next Read operation.
// Seeking is only supported in read mode.
// Seeking closes the current reader; a new one opens on the next Read.
func (f *GCSFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode {
		return 0, errors.New("gcs: seek not supported in write mode")
	}

	// Close existing reader - a new one will open on next Read.
	if f.reader != nil {
		_ = f.reader.Close()
		f.reader = nil
	}

	newOffset, err := f.calculateOffset(context.Background(), offset, whence)
	if err != nil {
		return 0, err
	}

	if newOffset < 0 {
		return 0, errors.New("gcs: negative seek position")
	}

	f.offset = newOffset

	return f.offset, nil
}

// calculateOffset computes the new offset based on whence and current position.
// For SeekEnd, fetches object size from GCS if not already cached.
func (f *GCSFile) calculateOffset(ctx context.Context, offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		return offset, nil
	case io.SeekCurrent:
		return f.offset + offset, nil
	case io.SeekEnd:
		// Fetch size from GCS if not cached.
		if f.size == 0 {
			obj := f.client.Bucket(f.bucket).Object(f.object)

			attrs, err := obj.Attrs(ctx)
			if err != nil {
				return 0, fmt.Errorf("gcs: failed to get object size: %w", err)
			}

			f.size = attrs.Size
		}

		return f.size + offset, nil
	default:
		return 0, errors.New("gcs: invalid seek whence")
	}
}

// ReadAt reads len(p) bytes starting at offset off using GCS range requests.
// This is more efficient than Seek+Read for random access patterns.
// ReadAt does not affect the current file offset.
func (f *GCSFile) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	ctx := context.Background()
	obj := f.client.Bucket(f.bucket).Object(f.object)

	// Use range reader for efficient partial reads.
	reader, err := obj.NewRangeReader(ctx, off, int64(len(p)))
	if err != nil {
		return 0, fmt.Errorf("gcs: failed to get object: %w", err)
	}
	defer func() { _ = reader.Close() }()

	return io.ReadFull(reader, p)
}

// WriteAt writes data at the specified offset in the write buffer.
// Note: GCS does not support partial writes. All data is buffered in memory
// and uploaded as a single object when Close() is called.
// For sparse writes, zero-byte padding is inserted to fill gaps.
func (f *GCSFile) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writeMode {
		return 0, errors.New("gcs: cannot write in read mode")
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

// Stat returns file info for this GCS object by calling Attrs on GCS.
func (f *GCSFile) Stat() (FileInfo, error) {
	ctx := context.Background()
	obj := f.client.Bucket(f.bucket).Object(f.object)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs: failed to stat object: %w", err)
	}

	return &GCSFileInfo{attrs: attrs}, nil
}

// Close closes the GCS file and releases resources.
// For write mode, uploads buffered data to GCS.
// For read mode, closes the underlying reader if open.
// Close must be called to ensure data is persisted for write operations.
func (f *GCSFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.reader != nil {
		_ = f.reader.Close()
		f.reader = nil
	}

	if f.writeMode && f.writeBuffer != nil && f.writeBuffer.Len() > 0 {
		return f.upload(context.Background())
	}

	return nil
}

// upload uploads the buffered write data to GCS.
// Called internally by Close() when there is data to upload.
func (f *GCSFile) upload(ctx context.Context) error {
	obj := f.client.Bucket(f.bucket).Object(f.object)
	writer := obj.NewWriter(ctx)

	// Set chunk size if configured
	if f.config.ChunkSize > 0 {
		writer.ChunkSize = int(f.config.ChunkSize)
	}

	_, err := io.Copy(writer, f.writeBuffer)
	if err != nil {
		_ = writer.Close()
		return fmt.Errorf("gcs: failed to upload object: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("gcs: failed to finalize upload: %w", err)
	}

	f.writeBuffer.Reset()

	return nil
}

// ReadContext reads data with context support for cancellation.
func (f *GCSFile) ReadContext(ctx context.Context, p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode {
		return 0, errors.New("gcs: cannot read in write mode")
	}

	// Lazily open the reader on first Read call.
	if f.reader == nil {
		if err := f.openReader(ctx); err != nil {
			return 0, err
		}
	}

	n, err = f.reader.Read(p)
	f.offset += int64(n)

	return n, err
}

// WriteContext writes data with context support for cancellation.
func (f *GCSFile) WriteContext(ctx context.Context, p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writeMode {
		return 0, errors.New("gcs: cannot write in read mode")
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
func (f *GCSFile) ReadAtContext(ctx context.Context, p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	obj := f.client.Bucket(f.bucket).Object(f.object)

	readAtFn := func() (int, error) {
		// Use range reader for efficient partial reads.
		reader, err := obj.NewRangeReader(ctx, off, int64(len(p)))
		if err != nil {
			return 0, fmt.Errorf("gcs: failed to get object: %w", err)
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

// Verify GCSFile implements File interface at compile time.
var _ File = (*GCSFile)(nil)
