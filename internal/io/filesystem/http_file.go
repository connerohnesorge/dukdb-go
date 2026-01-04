// Package filesystem provides HTTPFile implementation for HTTP/HTTPS resources.
// HTTPFile supports read and range request operations on HTTP resources.
// HTTP resources are read-only by design.
package filesystem

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// httpAcceptRangesBytes is the value for Accept-Ranges header indicating byte range support.
const httpAcceptRangesBytes = "bytes"

// HTTPFile represents an open HTTP resource for reading.
// HTTP resources are read-only, so Write operations return an error.
// HTTPFile is safe for concurrent use - all operations are mutex-protected.
type HTTPFile struct {
	// url is the full URL of the HTTP resource.
	url string
	// client is the HTTP client used for requests.
	client *http.Client
	// config holds HTTP configuration for requests.
	config HTTPConfig
	// resp is the current HTTP response body for streaming reads.
	resp *http.Response
	// offset is the current read position within the resource.
	offset int64
	// size is the total size of the resource (from Content-Length or HEAD).
	size int64
	// sizeKnown indicates whether the size has been determined.
	sizeKnown bool
	// acceptRanges indicates whether the server supports range requests.
	acceptRanges bool
	// mu protects concurrent access to file state.
	mu sync.Mutex
}

// newHTTPFile creates a new HTTP file handle for reading from the given URL.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity
func newHTTPFile(url string, client *http.Client, config HTTPConfig) *HTTPFile {
	return &HTTPFile{
		url:    url,
		client: client,
		config: config,
	}
}

// Read reads data from the HTTP resource into p.
// The first Read call opens a connection to the server and streams the data.
// Subsequent reads continue from where the previous read left off.
// Returns io.EOF when the end of the resource is reached.
func (f *HTTPFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Lazily open the response on first Read call
	if f.resp == nil {
		if err := f.openResponse(context.Background()); err != nil {
			return 0, err
		}
	}

	n, err = f.resp.Body.Read(p)
	f.offset += int64(n)

	return n, err
}

// openResponse initializes the HTTP response with optional range request.
// If offset > 0, uses a Range header to resume from that position.
func (f *HTTPFile) openResponse(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.url, http.NoBody)
	if err != nil {
		return fmt.Errorf("http: failed to create request: %w", err)
	}

	// Apply configuration headers
	f.applyHeaders(req)

	// Set range if we're resuming from a non-zero offset
	if f.offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", f.offset))
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("http: failed to perform request: %w", err)
	}

	// Check for valid status codes
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		_ = resp.Body.Close()
		return fmt.Errorf("http: unexpected status %d %s", resp.StatusCode, resp.Status)
	}

	// Cache size information
	if !f.sizeKnown {
		f.extractMetadata(resp)
	}

	f.resp = resp

	return nil
}

// applyHeaders applies configuration headers to the request.
func (f *HTTPFile) applyHeaders(req *http.Request) {
	// Set User-Agent
	if f.config.UserAgent != "" {
		req.Header.Set("User-Agent", f.config.UserAgent)
	}

	// Set custom headers
	for key, value := range f.config.Headers {
		req.Header.Set(key, value)
	}

	// Set basic auth
	if f.config.BasicAuth != nil {
		req.SetBasicAuth(f.config.BasicAuth.Username, f.config.BasicAuth.Password)
	}

	// Set bearer token
	if f.config.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+f.config.BearerToken)
	}
}

// extractMetadata extracts size and capabilities from HTTP response.
func (f *HTTPFile) extractMetadata(resp *http.Response) {
	// Try to get content length
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if size, err := strconv.ParseInt(cl, 10, 64); err == nil {
			f.size = size
			f.sizeKnown = true
		}
	}

	// Check for Content-Range header (partial content)
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		// Format: bytes start-end/total or bytes start-end/*
		if _, err := fmt.Sscanf(cr, "bytes %*d-%*d/%d", &f.size); err == nil {
			f.sizeKnown = true
		}
	}

	// Check Accept-Ranges header
	f.acceptRanges = resp.Header.Get("Accept-Ranges") == httpAcceptRangesBytes
}

// Write is not supported for HTTP resources.
// HTTP resources are read-only.
func (*HTTPFile) Write(_ []byte) (n int, err error) {
	return 0, errors.New("http: write not supported")
}

// Seek sets the offset for the next Read operation.
// Seeking closes the current response; a new one opens on the next Read.
func (f *HTTPFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Close existing response - a new one will open on next Read
	if f.resp != nil {
		_ = f.resp.Body.Close()
		f.resp = nil
	}

	newOffset, err := f.calculateOffset(context.Background(), offset, whence)
	if err != nil {
		return 0, err
	}

	if newOffset < 0 {
		return 0, errors.New("http: negative seek position")
	}

	f.offset = newOffset

	return f.offset, nil
}

// calculateOffset computes the new offset based on whence and current position.
// For SeekEnd, fetches resource size from server if not already cached.
func (f *HTTPFile) calculateOffset(ctx context.Context, offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		return offset, nil
	case io.SeekCurrent:
		return f.offset + offset, nil
	case io.SeekEnd:
		// Fetch size from server if not cached
		if !f.sizeKnown {
			if err := f.fetchSize(ctx); err != nil {
				return 0, err
			}
		}

		return f.size + offset, nil
	default:
		return 0, errors.New("http: invalid seek whence")
	}
}

// fetchSize fetches the resource size using a HEAD request.
func (f *HTTPFile) fetchSize(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, f.url, http.NoBody)
	if err != nil {
		return fmt.Errorf("http: failed to create HEAD request: %w", err)
	}

	f.applyHeaders(req)

	resp, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("http: failed to perform HEAD request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http: HEAD request failed with status %d", resp.StatusCode)
	}

	f.extractMetadata(resp)

	if !f.sizeKnown {
		return errors.New("http: unable to determine resource size")
	}

	return nil
}

// ReadAt reads len(p) bytes starting at offset off using HTTP range requests.
// This is more efficient than Seek+Read for random access patterns.
// ReadAt does not affect the current file offset.
func (f *HTTPFile) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	return f.ReadAtContext(context.Background(), p, off)
}

// ReadAtContext reads data at offset with context support.
func (f *HTTPFile) ReadAtContext(ctx context.Context, p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	readAtFn := func() (int, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.url, http.NoBody)
		if err != nil {
			return 0, fmt.Errorf("http: failed to create request: %w", err)
		}

		f.applyHeaders(req)

		// Set range header
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1))

		resp, err := f.client.Do(req)
		if err != nil {
			return 0, fmt.Errorf("http: failed to perform request: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		// 206 Partial Content is expected for range requests
		// 200 OK is also acceptable (server ignores range)
		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("http: range request failed with status %d", resp.StatusCode)
		}

		return io.ReadFull(resp.Body, p)
	}

	// Apply retry logic if configured
	if f.config.RetryConfig.MaxRetries > 0 {
		return WithRetryFunc(ctx, f.config.RetryConfig, readAtFn)
	}

	return readAtFn()
}

// WriteAt is not supported for HTTP resources.
func (*HTTPFile) WriteAt(_ []byte, _ int64) (n int, err error) {
	return 0, errors.New("http: write not supported")
}

// Stat returns file info for this HTTP resource using a HEAD request.
func (f *HTTPFile) Stat() (FileInfo, error) {
	return f.StatContext(context.Background())
}

// StatContext returns file info with context support.
func (f *HTTPFile) StatContext(ctx context.Context) (FileInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, f.url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("http: failed to create HEAD request: %w", err)
	}

	f.applyHeaders(req)

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: failed to perform HEAD request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http: HEAD request failed with status %d", resp.StatusCode)
	}

	// Extract metadata
	var size int64
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		size, _ = strconv.ParseInt(cl, 10, 64)
	}

	var modTime time.Time
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		modTime, _ = http.ParseTime(lm)
	}

	contentType := resp.Header.Get("Content-Type")
	etag := resp.Header.Get("ETag")
	acceptRanges := resp.Header.Get("Accept-Ranges") == httpAcceptRangesBytes

	return NewHTTPFileInfoFull(f.url, size, modTime, contentType, etag, acceptRanges), nil
}

// Close closes the HTTP file and releases resources.
func (f *HTTPFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.resp != nil {
		err := f.resp.Body.Close()
		f.resp = nil

		return err
	}

	return nil
}

// ReadContext reads data with context support for cancellation.
func (f *HTTPFile) ReadContext(ctx context.Context, p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Lazily open the response on first Read call
	if f.resp == nil {
		if err := f.openResponse(ctx); err != nil {
			return 0, err
		}
	}

	// Note: the response body doesn't directly support context cancellation,
	// but the request that created it does. For a truly context-aware read,
	// we'd need to wrap the body read in a select.
	n, err = f.resp.Body.Read(p)
	f.offset += int64(n)

	return n, err
}

// WriteContext is not supported for HTTP resources.
func (*HTTPFile) WriteContext(_ context.Context, _ []byte) (n int, err error) {
	return 0, errors.New("http: write not supported")
}

// URL returns the URL of this HTTP file.
func (f *HTTPFile) URL() string {
	return f.url
}

// Size returns the cached size of the resource, or -1 if unknown.
func (f *HTTPFile) Size() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.sizeKnown {
		return f.size
	}

	return -1
}

// AcceptRanges returns whether the server supports range requests.
func (f *HTTPFile) AcceptRanges() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.acceptRanges
}

// Verify HTTPFile implements File interface at compile time.
var _ File = (*HTTPFile)(nil)
