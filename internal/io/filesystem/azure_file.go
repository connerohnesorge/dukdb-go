//go:build !js || !wasm

// Package filesystem provides AzureFile implementation for Azure Blob operations.
// AzureFile supports read, write, seek, and range request operations on Azure blobs.
package filesystem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

// AzureFile represents an open Azure blob for reading or writing.
// For read mode, data is streamed directly from Azure with range request support.
// For write mode, data is buffered in memory and uploaded on Close().
// AzureFile is safe for concurrent use - all operations are mutex-protected.
type AzureFile struct {
	// blobClient is the Azure blob client used for operations.
	blobClient *blob.Client
	// blockBlobClient is the Azure block blob client for uploads.
	blockBlobClient *blockblob.Client
	// container is the Azure container name.
	container string
	// blobName is the blob name (path within the container).
	blobName string
	// offset is the current read position within the blob.
	offset int64
	// size is the total size of the blob (cached for SeekEnd).
	size int64
	// writeMode indicates if this file was opened for writing.
	writeMode bool
	// writeBuffer accumulates write data until Close() uploads it.
	writeBuffer *bytes.Buffer
	// mu protects concurrent access to file state.
	mu sync.Mutex
	// config holds Azure configuration for advanced features.
	config AzureConfig
}

// newAzureFileForReading creates a new Azure file handle for reading from the given container and blob.
// Data is streamed from Azure on Read calls with optional range request support.
func newAzureFileForReading(blobClient *blob.Client, container, blobName string) *AzureFile {
	return &AzureFile{
		blobClient: blobClient,
		container:  container,
		blobName:   blobName,
		writeMode:  false,
		config:     DefaultAzureConfig(),
	}
}

// newAzureFileForReadingWithConfig creates a new Azure file handle for reading with configuration.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity
func newAzureFileForReadingWithConfig(blobClient *blob.Client, container, blobName string, config AzureConfig) *AzureFile {
	return &AzureFile{
		blobClient: blobClient,
		container:  container,
		blobName:   blobName,
		writeMode:  false,
		config:     config,
	}
}

// newAzureFileForWriting creates a new Azure file handle for writing to the given container and blob.
// Data is buffered in memory and uploaded to Azure when Close() is called.
func newAzureFileForWriting(blockBlobClient *blockblob.Client, container, blobName string) *AzureFile {
	return &AzureFile{
		blockBlobClient: blockBlobClient,
		container:       container,
		blobName:        blobName,
		writeMode:       true,
		writeBuffer:     &bytes.Buffer{},
		config:          DefaultAzureConfig(),
	}
}

// newAzureFileForWritingWithConfig creates a new Azure file handle for writing with configuration.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity
func newAzureFileForWritingWithConfig(blockBlobClient *blockblob.Client, container, blobName string, config AzureConfig) *AzureFile {
	return &AzureFile{
		blockBlobClient: blockBlobClient,
		container:       container,
		blobName:        blobName,
		writeMode:       true,
		writeBuffer:     &bytes.Buffer{},
		config:          config,
	}
}

// Read reads data from the Azure blob into p.
// Uses range requests to download only the requested bytes.
// Returns io.EOF when the end of the blob is reached.
func (f *AzureFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode {
		return 0, errors.New("azure: cannot read in write mode")
	}

	if f.blobClient == nil {
		return 0, errors.New("azure: blob client not initialized")
	}

	ctx := context.Background()

	// Use range request for efficient reads
	n, err = f.readRange(ctx, p, f.offset)
	if err != nil {
		return 0, err
	}

	f.offset += int64(n)

	return n, nil
}

// readRange performs a range download from Azure.
func (f *AzureFile) readRange(ctx context.Context, p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	count := int64(len(p))

	opts := &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{
			Offset: off,
			Count:  count,
		},
	}

	resp, err := f.blobClient.DownloadStream(ctx, opts)
	if err != nil {
		return 0, fmt.Errorf("azure: failed to download blob: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	return io.ReadFull(resp.Body, p)
}

// Write writes data to the Azure blob buffer.
// Data is accumulated in memory and uploaded when Close() is called.
// Returns an error if the file was opened for reading.
func (f *AzureFile) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writeMode {
		return 0, errors.New("azure: cannot write in read mode")
	}

	return f.writeBuffer.Write(p)
}

// Seek sets the offset for the next Read operation.
// Seeking is only supported in read mode.
func (f *AzureFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode {
		return 0, errors.New("azure: seek not supported in write mode")
	}

	newOffset, err := f.calculateOffset(context.Background(), offset, whence)
	if err != nil {
		return 0, err
	}

	if newOffset < 0 {
		return 0, errors.New("azure: negative seek position")
	}

	f.offset = newOffset

	return f.offset, nil
}

// calculateOffset computes the new offset based on whence and current position.
// For SeekEnd, fetches blob size from Azure if not already cached.
func (f *AzureFile) calculateOffset(ctx context.Context, offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		return offset, nil
	case io.SeekCurrent:
		return f.offset + offset, nil
	case io.SeekEnd:
		// Fetch size from Azure if not cached.
		if f.size == 0 && f.blobClient != nil {
			props, err := f.blobClient.GetProperties(ctx, nil)
			if err != nil {
				return 0, fmt.Errorf("azure: failed to get blob size: %w", err)
			}

			if props.ContentLength != nil {
				f.size = *props.ContentLength
			}
		}

		return f.size + offset, nil
	default:
		return 0, errors.New("azure: invalid seek whence")
	}
}

// ReadAt reads len(p) bytes starting at offset off using Azure range requests.
// This is more efficient than Seek+Read for random access patterns.
// ReadAt does not affect the current file offset.
func (f *AzureFile) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode {
		return 0, errors.New("azure: cannot read in write mode")
	}

	if f.blobClient == nil {
		return 0, errors.New("azure: blob client not initialized")
	}

	ctx := context.Background()

	return f.readRange(ctx, p, off)
}

// WriteAt writes data at the specified offset in the write buffer.
// Note: Azure Blob Storage does not support partial writes. All data is buffered in memory
// and uploaded as a single blob when Close() is called.
// For sparse writes, zero-byte padding is inserted to fill gaps.
func (f *AzureFile) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writeMode {
		return 0, errors.New("azure: cannot write in read mode")
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

// Stat returns file info for this Azure blob by calling GetProperties on Azure.
func (f *AzureFile) Stat() (FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.blobClient == nil {
		return nil, errors.New("azure: blob client not initialized")
	}

	ctx := context.Background()

	props, err := f.blobClient.GetProperties(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("azure: failed to stat blob: %w", err)
	}

	return NewAzureFileInfo(f.blobName, &props), nil
}

// Close closes the Azure file and releases resources.
// For write mode, uploads buffered data to Azure.
// For read mode, this is a no-op.
// Close must be called to ensure data is persisted for write operations.
func (f *AzureFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode && f.writeBuffer != nil && f.writeBuffer.Len() > 0 {
		return f.upload(context.Background())
	}

	return nil
}

// upload uploads the buffered write data to Azure.
// Called internally by Close() when there is data to upload.
func (f *AzureFile) upload(ctx context.Context) error {
	if f.blockBlobClient == nil {
		return errors.New("azure: block blob client not initialized")
	}

	_, err := f.blockBlobClient.UploadBuffer(ctx, f.writeBuffer.Bytes(), nil)
	if err != nil {
		return fmt.Errorf("azure: failed to upload blob: %w", err)
	}

	f.writeBuffer.Reset()

	return nil
}

// ReadContext reads data with context support for cancellation.
func (f *AzureFile) ReadContext(ctx context.Context, p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode {
		return 0, errors.New("azure: cannot read in write mode")
	}

	if f.blobClient == nil {
		return 0, errors.New("azure: blob client not initialized")
	}

	readFn := func() (int, error) {
		n, err := f.readRange(ctx, p, f.offset)
		if err != nil {
			return 0, err
		}

		f.offset += int64(n)

		return n, nil
	}

	// Apply retry logic if configured
	if f.config.RetryConfig.MaxRetries > 0 {
		return WithRetryFunc(ctx, f.config.RetryConfig, readFn)
	}

	return readFn()
}

// WriteContext writes data with context support for cancellation.
func (f *AzureFile) WriteContext(ctx context.Context, p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.writeMode {
		return 0, errors.New("azure: cannot write in read mode")
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
func (f *AzureFile) ReadAtContext(ctx context.Context, p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writeMode {
		return 0, errors.New("azure: cannot read in write mode")
	}

	if f.blobClient == nil {
		return 0, errors.New("azure: blob client not initialized")
	}

	readAtFn := func() (int, error) {
		return f.readRange(ctx, p, off)
	}

	// Apply retry logic if configured
	if f.config.RetryConfig.MaxRetries > 0 {
		return WithRetryFunc(ctx, f.config.RetryConfig, readAtFn)
	}

	return readAtFn()
}

// Verify AzureFile implements File interface at compile time.
var _ File = (*AzureFile)(nil)
