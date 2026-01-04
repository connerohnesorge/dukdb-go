// Package filesystem provides HTTPFileSystem implementation for HTTP/HTTPS resources.
// The HTTPFileSystem implements the FileSystem interface for reading files over HTTP.
// It supports read, stat, and range request operations on HTTP resources.
// HTTP filesystems are read-only by design.
package filesystem

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrNotSupported is returned for operations not supported on HTTP filesystems.
var ErrNotSupported = errors.New("operation not supported on HTTP filesystem")

// HTTPFileSystem implements FileSystem for HTTP/HTTPS resources.
// It uses the standard net/http client for all HTTP operations.
// HTTPFileSystem is read-only - Create, MkdirAll, Remove, and RemoveDir return ErrNotSupported.
type HTTPFileSystem struct {
	// client is the underlying HTTP client for operations.
	client *http.Client
	// config holds the HTTP configuration used to create this filesystem.
	config HTTPConfig
	// mu protects concurrent access to the client.
	mu sync.RWMutex
}

// NewHTTPFileSystem creates a new HTTP filesystem with the given configuration.
// The context parameter is currently unused but reserved for future use.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity and immutability
func NewHTTPFileSystem(_ context.Context, config HTTPConfig) (*HTTPFileSystem, error) {
	client := buildHTTPClient(&config)

	return &HTTPFileSystem{client: client, config: config}, nil
}

// NewHTTPFileSystemWithClient creates a new HTTP filesystem with a pre-configured client.
// This is useful for testing with mock clients or for sharing clients across filesystems.
// If config is nil, uses default HTTP configuration values.
func NewHTTPFileSystemWithClient(client *http.Client, config *HTTPConfig) *HTTPFileSystem {
	cfg := HTTPConfig{}
	if config != nil {
		cfg = *config
	}

	return &HTTPFileSystem{client: client, config: cfg}
}

// buildHTTPClient creates an HTTP client from the configuration.
func buildHTTPClient(config *HTTPConfig) *http.Client {
	transport := &http.Transport{
		TLSClientConfig: buildTLSConfig(config),
	}

	// Configure redirect policy
	checkRedirect := func(_ *http.Request, via []*http.Request) error {
		if !config.FollowRedirects {
			return http.ErrUseLastResponse
		}
		if len(via) >= config.MaxRedirects {
			return fmt.Errorf("http: too many redirects (max %d)", config.MaxRedirects)
		}

		return nil
	}

	return &http.Client{
		Timeout:       config.Timeout,
		Transport:     transport,
		CheckRedirect: checkRedirect,
	}
}

// buildTLSConfig creates TLS configuration from the HTTP config.
func buildTLSConfig(config *HTTPConfig) *tls.Config {
	if config.TLSConfig != nil {
		return config.TLSConfig
	}

	if config.InsecureSkipVerify {
		return &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // User explicitly requested this
		}
	}

	return nil
}

// parseHTTPURL parses an HTTP URL and returns the full URL.
// Supports http://, https:// URL schemes.
// If no scheme is provided, defaults to https://.
func parseHTTPURL(urlStr string) (string, error) {
	// Strip any whitespace
	urlStr = strings.TrimSpace(urlStr)

	if urlStr == "" {
		return "", errors.New("http: URL is required")
	}

	// Add scheme if missing
	if !strings.HasPrefix(strings.ToLower(urlStr), "http://") &&
		!strings.HasPrefix(strings.ToLower(urlStr), "https://") {
		urlStr = "https://" + urlStr
	}

	return urlStr, nil
}

// Open opens an HTTP resource for reading.
// Returns a File that streams data from the URL on Read calls.
func (fs *HTTPFileSystem) Open(path string) (File, error) {
	url, err := parseHTTPURL(path)
	if err != nil {
		return nil, err
	}

	return newHTTPFile(url, fs.client, fs.config), nil
}

// Create is not supported for HTTP resources.
// HTTP filesystems are read-only.
func (*HTTPFileSystem) Create(_ string) (File, error) {
	return nil, ErrNotSupported
}

// MkdirAll is not supported for HTTP resources.
// HTTP filesystems are read-only.
func (*HTTPFileSystem) MkdirAll(_ string) error {
	return ErrNotSupported
}

// Stat returns file info for an HTTP resource using a HEAD request.
func (fs *HTTPFileSystem) Stat(path string) (FileInfo, error) {
	return fs.StatContext(context.Background(), path)
}

// Remove is not supported for HTTP resources.
// HTTP filesystems are read-only.
func (*HTTPFileSystem) Remove(_ string) error {
	return ErrNotSupported
}

// RemoveDir is not supported for HTTP resources.
// HTTP filesystems are read-only.
func (*HTTPFileSystem) RemoveDir(_ string) error {
	return ErrNotSupported
}

// ReadDir is not supported for HTTP resources.
// Standard HTTP doesn't support directory listings.
func (*HTTPFileSystem) ReadDir(_ string) ([]DirEntry, error) {
	return nil, ErrNotSupported
}

// Exists checks if an HTTP resource exists using a HEAD request.
func (fs *HTTPFileSystem) Exists(path string) (bool, error) {
	return fs.ExistsContext(context.Background(), path)
}

// ExistsContext checks if an HTTP resource exists with context support.
func (fs *HTTPFileSystem) ExistsContext(ctx context.Context, path string) (bool, error) {
	url, err := parseHTTPURL(path)
	if err != nil {
		return false, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, http.NoBody)
	if err != nil {
		return false, fmt.Errorf("http: failed to create HEAD request: %w", err)
	}

	fs.applyHeaders(req)

	resp, err := fs.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("http: failed to perform HEAD request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 2xx status codes indicate the resource exists
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true, nil
	}

	// 404 means the resource doesn't exist
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	// Other error status codes
	return false, fmt.Errorf("http: HEAD request returned status %d", resp.StatusCode)
}

// URI returns the base URI for this filesystem.
// Returns "http://" for HTTP filesystems (also handles https://).
func (*HTTPFileSystem) URI() string {
	return "http://"
}

// Capabilities returns the capabilities of this filesystem.
// HTTP supports seek (via range requests) and range reads.
// Write, delete, and directory listing are not supported.
func (*HTTPFileSystem) Capabilities() FileSystemCapabilities {
	return FileSystemCapabilities{
		SupportsSeek:    true,  // Via range requests
		SupportsAppend:  false, // Read-only
		SupportsRange:   true,  // Via Range header
		SupportsDirList: false, // No directory listing
		SupportsWrite:   false, // Read-only
		SupportsDelete:  false, // Read-only
		ContextTimeout:  true,
	}
}

// Verify HTTPFileSystem implements FileSystem interface at compile time.
var _ FileSystem = (*HTTPFileSystem)(nil)

// OpenContext opens an HTTP resource for reading with context support.
func (fs *HTTPFileSystem) OpenContext(ctx context.Context, path string) (File, error) {
	// Check context
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	url, err := parseHTTPURL(path)
	if err != nil {
		return nil, err
	}

	return newHTTPFile(url, fs.client, fs.config), nil
}

// CreateContext is not supported for HTTP resources.
func (*HTTPFileSystem) CreateContext(_ context.Context, _ string) (File, error) {
	return nil, ErrNotSupported
}

// StatContext returns file info with context support.
func (fs *HTTPFileSystem) StatContext(ctx context.Context, path string) (FileInfo, error) {
	url, err := parseHTTPURL(path)
	if err != nil {
		return nil, err
	}

	statFn := func() (FileInfo, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, http.NoBody)
		if err != nil {
			return nil, fmt.Errorf("http: failed to create HEAD request: %w", err)
		}

		fs.applyHeaders(req)

		resp, err := fs.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("http: failed to perform HEAD request: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("http: resource not found: %s", url)
		}

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

		return NewHTTPFileInfoFull(url, size, modTime, contentType, etag, acceptRanges), nil
	}

	// Apply retry logic if configured
	if fs.config.RetryConfig.MaxRetries > 0 {
		return WithRetryFunc(ctx, fs.config.RetryConfig, statFn)
	}

	return statFn()
}

// ReadDirContext is not supported for HTTP resources.
func (*HTTPFileSystem) ReadDirContext(_ context.Context, _ string) ([]DirEntry, error) {
	return nil, ErrNotSupported
}

// applyHeaders applies configuration headers to the request.
func (fs *HTTPFileSystem) applyHeaders(req *http.Request) {
	// Set User-Agent
	if fs.config.UserAgent != "" {
		req.Header.Set("User-Agent", fs.config.UserAgent)
	}

	// Set custom headers
	for key, value := range fs.config.Headers {
		req.Header.Set(key, value)
	}

	// Set basic auth
	if fs.config.BasicAuth != nil {
		req.SetBasicAuth(fs.config.BasicAuth.Username, fs.config.BasicAuth.Password)
	}

	// Set bearer token
	if fs.config.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+fs.config.BearerToken)
	}
}

// GetConfig returns the current HTTP configuration.
func (fs *HTTPFileSystem) GetConfig() HTTPConfig {
	return fs.config
}

// GetClient returns the underlying HTTP client.
// This can be useful for advanced operations not exposed through the FileSystem interface.
func (fs *HTTPFileSystem) GetClient() *http.Client {
	return fs.client
}

// Close closes the HTTP filesystem and releases resources.
// Closes idle connections in the connection pool.
func (fs *HTTPFileSystem) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.client != nil && fs.client.Transport != nil {
		if transport, ok := fs.client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}

	return nil
}

// Verify HTTPFileSystem implements ContextFileSystem interface.
var _ ContextFileSystem = (*HTTPFileSystem)(nil)

// registerHTTPSchemesInternal registers HTTP schemes with a factory.
// This is called internally during factory initialization.
func registerHTTPSchemesInternal(f *factory) {
	httpFactory := func(ctx context.Context, _ string) (FileSystem, error) {
		config := DefaultHTTPConfig()

		return NewHTTPFileSystem(ctx, config)
	}

	f.schemes["http"] = httpFactory
	f.schemes["https"] = httpFactory
}
