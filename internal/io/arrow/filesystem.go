//go:build !js || !wasm

// Package arrow provides Apache Arrow IPC file reading and writing capabilities for dukdb-go.
// This file provides cloud storage integration via the FileSystem interface.
package arrow

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dukdb/dukdb-go/internal/io/filesystem"
)

// NewReaderFromURL creates a new Arrow IPC file reader from a URL.
// Supports local files (file://), S3 (s3://), GCS (gs://), Azure (az://), and HTTP/HTTPS URLs.
//
// The URL scheme determines which filesystem implementation is used:
//   - file:// or no scheme: Local filesystem
//   - s3://, s3a://, s3n://: Amazon S3
//   - gs://, gcs://: Google Cloud Storage
//   - az://, azure://: Azure Blob Storage
//   - http://, https://: HTTP/HTTPS (read-only)
//
// Example URLs:
//   - "/path/to/file.arrow" (local file)
//   - "file:///path/to/file.arrow" (local file with scheme)
//   - "s3://bucket/key/file.arrow" (S3)
//   - "gs://bucket/object/file.arrow" (GCS)
//   - "https://example.com/data.arrow" (HTTP)
func NewReaderFromURL(url string, opts *ReaderOptions) (*Reader, error) {
	return NewReaderFromURLWithContext(context.Background(), url, opts)
}

// NewReaderFromURLWithContext creates a new Arrow IPC file reader from a URL with context.
// The context can be used for cancellation and timeout control.
func NewReaderFromURLWithContext(
	ctx context.Context,
	url string,
	opts *ReaderOptions,
) (*Reader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}
	readerOpts.applyDefaults()

	// Get appropriate filesystem for the URL
	fs, err := filesystem.GetFileSystem(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to get filesystem for URL %q: %w", url, err)
	}

	// Extract path from URL
	path := extractPath(url)

	// Open file for reading
	file, err := fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to open file %q: %w", url, err)
	}

	// Get file size
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("arrow: failed to stat file %q: %w", url, err)
	}

	_ = info.Size() // File size is embedded in the IPC reader

	// The filesystem.File interface already implements ReadAtSeeker
	return NewReader(file, readerOpts)
}

// NewStreamReaderFromURL creates a new Arrow IPC stream reader from a URL.
// See NewReaderFromURL for supported URL schemes.
func NewStreamReaderFromURL(url string, opts *ReaderOptions) (*StreamReader, error) {
	return NewStreamReaderFromURLWithContext(context.Background(), url, opts)
}

// NewStreamReaderFromURLWithContext creates a new Arrow IPC stream reader from a URL with context.
func NewStreamReaderFromURLWithContext(
	ctx context.Context,
	url string,
	opts *ReaderOptions,
) (*StreamReader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}
	readerOpts.applyDefaults()

	// Get appropriate filesystem for the URL
	fs, err := filesystem.GetFileSystem(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to get filesystem for URL %q: %w", url, err)
	}

	// Extract path from URL
	path := extractPath(url)

	// Open file for reading
	file, err := fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to open file %q: %w", url, err)
	}

	// Create stream reader with closer
	return createStreamReaderFromFile(file, readerOpts)
}

// createStreamReaderFromFile creates a StreamReader from a filesystem.File.
func createStreamReaderFromFile(file filesystem.File, opts *ReaderOptions) (*StreamReader, error) {
	reader, err := NewStreamReader(file, opts)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	// The StreamReader takes ownership of closing the file
	return reader, nil
}

// NewWriterToURL creates a new Arrow IPC file writer to a URL.
// See NewReaderFromURL for supported URL schemes.
// HTTP/HTTPS URLs are not supported for writing.
func NewWriterToURL(url string, opts *WriterOptions) (*Writer, error) {
	return NewWriterToURLWithContext(context.Background(), url, opts)
}

// NewWriterToURLWithContext creates a new Arrow IPC file writer to a URL with context.
func NewWriterToURLWithContext(
	ctx context.Context,
	url string,
	opts *WriterOptions,
) (*Writer, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}
	writerOpts.applyDefaults()

	// Check for HTTP/HTTPS (read-only)
	scheme := extractScheme(url)
	if scheme == "http" || scheme == "https" {
		return nil, fmt.Errorf("arrow: HTTP/HTTPS URLs do not support writing")
	}

	// Get appropriate filesystem for the URL
	fs, err := filesystem.GetFileSystem(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to get filesystem for URL %q: %w", url, err)
	}

	// Check if filesystem supports writing
	caps := fs.Capabilities()
	if !caps.SupportsWrite {
		return nil, fmt.Errorf("arrow: filesystem does not support writing")
	}

	// Extract path from URL
	path := extractPath(url)

	// Create file for writing
	file, err := fs.Create(path)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to create file %q: %w", url, err)
	}

	return createWriter(file, file, writerOpts)
}

// NewStreamWriterToURL creates a new Arrow IPC stream writer to a URL.
// See NewReaderFromURL for supported URL schemes.
// HTTP/HTTPS URLs are not supported for writing.
func NewStreamWriterToURL(url string, opts *WriterOptions) (*StreamWriter, error) {
	return NewStreamWriterToURLWithContext(context.Background(), url, opts)
}

// NewStreamWriterToURLWithContext creates a new Arrow IPC stream writer to a URL with context.
func NewStreamWriterToURLWithContext(
	ctx context.Context,
	url string,
	opts *WriterOptions,
) (*StreamWriter, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}
	writerOpts.applyDefaults()

	// Check for HTTP/HTTPS (read-only)
	scheme := extractScheme(url)
	if scheme == "http" || scheme == "https" {
		return nil, fmt.Errorf("arrow: HTTP/HTTPS URLs do not support writing")
	}

	// Get appropriate filesystem for the URL
	fs, err := filesystem.GetFileSystem(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to get filesystem for URL %q: %w", url, err)
	}

	// Check if filesystem supports writing
	caps := fs.Capabilities()
	if !caps.SupportsWrite {
		return nil, fmt.Errorf("arrow: filesystem does not support writing")
	}

	// Extract path from URL
	path := extractPath(url)

	// Create file for writing
	file, err := fs.Create(path)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to create file %q: %w", url, err)
	}

	return createStreamWriter(file, file, writerOpts)
}

// extractScheme extracts the scheme from a URL.
// Returns an empty string if no scheme is present.
func extractScheme(url string) string {
	idx := strings.Index(url, "://")
	if idx < 0 {
		return ""
	}
	return strings.ToLower(url[:idx])
}

// extractPath extracts the path from a URL.
// For cloud URLs, returns the full URL (filesystem handles parsing).
// For local files, strips the file:// prefix.
func extractPath(url string) string {
	scheme := extractScheme(url)

	// For local files, strip file:// prefix
	if scheme == "file" {
		return strings.TrimPrefix(url, "file://")
	}

	// For cloud URLs, return full URL (filesystem will parse it)
	if scheme != "" {
		return url
	}

	// No scheme - treat as local path
	return url
}

// URLFileReader wraps a filesystem.File to implement ReadAtSeeker.
// This is used internally for cloud storage integration.
type URLFileReader struct {
	file filesystem.File
}

// Read implements io.Reader.
func (r *URLFileReader) Read(p []byte) (int, error) {
	return r.file.Read(p)
}

// ReadAt implements io.ReaderAt.
func (r *URLFileReader) ReadAt(p []byte, off int64) (int, error) {
	return r.file.ReadAt(p, off)
}

// Seek implements io.Seeker.
func (r *URLFileReader) Seek(offset int64, whence int) (int64, error) {
	return r.file.Seek(offset, whence)
}

// Close implements io.Closer.
func (r *URLFileReader) Close() error {
	return r.file.Close()
}

// Verify URLFileReader implements ReadAtSeeker.
var _ ReadAtSeeker = (*URLFileReader)(nil)

// Verify URLFileReader implements io.Closer.
var _ io.Closer = (*URLFileReader)(nil)
