package filesystem

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseHTTPURL tests the HTTP URL parsing function.
func TestParseHTTPURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    string
		wantErr bool
	}{
		{
			name:    "full http URL",
			url:     "http://example.com/path/to/file.csv",
			want:    "http://example.com/path/to/file.csv",
			wantErr: false,
		},
		{
			name:    "full https URL",
			url:     "https://example.com/path/to/file.csv",
			want:    "https://example.com/path/to/file.csv",
			wantErr: false,
		},
		{
			name:    "URL without scheme",
			url:     "example.com/path/to/file.csv",
			want:    "https://example.com/path/to/file.csv",
			wantErr: false,
		},
		{
			name:    "URL with port",
			url:     "http://example.com:8080/file.csv",
			want:    "http://example.com:8080/file.csv",
			wantErr: false,
		},
		{
			name:    "URL with query params",
			url:     "https://example.com/file.csv?token=abc",
			want:    "https://example.com/file.csv?token=abc",
			wantErr: false,
		},
		{
			name:    "uppercase scheme",
			url:     "HTTP://example.com/file",
			want:    "HTTP://example.com/file",
			wantErr: false,
		},
		{
			name:    "empty URL",
			url:     "",
			want:    "",
			wantErr: true,
		},
		{
			name:    "whitespace URL",
			url:     "   ",
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseHTTPURL(tt.url)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestDefaultHTTPConfig tests the default HTTP configuration.
func TestDefaultHTTPConfig(t *testing.T) {
	config := DefaultHTTPConfig()

	assert.Equal(t, DefaultHTTPTimeout, config.Timeout)
	assert.Equal(t, DefaultHTTPMaxRedirects, config.MaxRedirects)
	assert.Equal(t, DefaultHTTPUserAgent, config.UserAgent)
	assert.True(t, config.FollowRedirects)
	assert.False(t, config.InsecureSkipVerify)
	assert.Nil(t, config.BasicAuth)
	assert.Empty(t, config.BearerToken)
	assert.NotNil(t, config.Headers)
}

// TestHTTPConfig tests HTTP configuration.
func TestHTTPConfig(t *testing.T) {
	config := HTTPConfig{
		Timeout:            60 * time.Second,
		FollowRedirects:    false,
		MaxRedirects:       5,
		Headers:            map[string]string{"X-Custom": "value"},
		UserAgent:          "test-agent/1.0",
		InsecureSkipVerify: true,
		BasicAuth: &BasicAuth{
			Username: "user",
			Password: "pass",
		},
		BearerToken: "token123",
	}

	assert.Equal(t, 60*time.Second, config.Timeout)
	assert.False(t, config.FollowRedirects)
	assert.Equal(t, 5, config.MaxRedirects)
	assert.Equal(t, "value", config.Headers["X-Custom"])
	assert.Equal(t, "test-agent/1.0", config.UserAgent)
	assert.True(t, config.InsecureSkipVerify)
	assert.Equal(t, "user", config.BasicAuth.Username)
	assert.Equal(t, "pass", config.BasicAuth.Password)
	assert.Equal(t, "token123", config.BearerToken)
}

// TestHTTPConfigOptions tests HTTP configuration option functions.
func TestHTTPConfigOptions(t *testing.T) {
	config := NewHTTPConfig(
		WithHTTPTimeout(45*time.Second),
		WithHTTPFollowRedirects(false),
		WithHTTPMaxRedirects(3),
		WithHTTPHeader("X-Test", "value"),
		WithHTTPHeaders(map[string]string{"X-Another": "another"}),
		WithHTTPUserAgent("custom-agent"),
		WithHTTPInsecureSkipVerify(true),
		WithHTTPBasicAuth("user", "pass"),
		WithHTTPBearerToken("bearer-token"),
		WithHTTPRetryConfig(RetryConfig{MaxRetries: 5}),
	)

	assert.Equal(t, 45*time.Second, config.Timeout)
	assert.False(t, config.FollowRedirects)
	assert.Equal(t, 3, config.MaxRedirects)
	assert.Equal(t, "value", config.Headers["X-Test"])
	assert.Equal(t, "another", config.Headers["X-Another"])
	assert.Equal(t, "custom-agent", config.UserAgent)
	assert.True(t, config.InsecureSkipVerify)
	assert.Equal(t, "user", config.BasicAuth.Username)
	assert.Equal(t, "pass", config.BasicAuth.Password)
	assert.Equal(t, "bearer-token", config.BearerToken)
	assert.Equal(t, 5, config.RetryConfig.MaxRetries)
}

// TestHTTPFileSystem_Capabilities tests the HTTP filesystem capabilities.
func TestHTTPFileSystem_Capabilities(t *testing.T) {
	fs := &HTTPFileSystem{
		client: nil,
		config: DefaultHTTPConfig(),
	}

	caps := fs.Capabilities()

	assert.True(t, caps.SupportsSeek)
	assert.False(t, caps.SupportsAppend)
	assert.True(t, caps.SupportsRange)
	assert.False(t, caps.SupportsDirList)
	assert.False(t, caps.SupportsWrite)
	assert.False(t, caps.SupportsDelete)
	assert.True(t, caps.ContextTimeout)
}

// TestHTTPFileSystem_URI tests the HTTP filesystem URI.
func TestHTTPFileSystem_URI(t *testing.T) {
	fs := &HTTPFileSystem{
		client: nil,
		config: DefaultHTTPConfig(),
	}

	assert.Equal(t, "http://", fs.URI())
}

// TestHTTPFileSystem_NotSupported tests that write operations are not supported.
func TestHTTPFileSystem_NotSupported(t *testing.T) {
	fs := &HTTPFileSystem{
		client: nil,
		config: DefaultHTTPConfig(),
	}

	// Create is not supported
	_, err := fs.Create("http://example.com/file.txt")
	require.Error(t, err)
	assert.Equal(t, ErrNotSupported, err)

	// MkdirAll is not supported
	err = fs.MkdirAll("http://example.com/dir/")
	require.Error(t, err)
	assert.Equal(t, ErrNotSupported, err)

	// Remove is not supported
	err = fs.Remove("http://example.com/file.txt")
	require.Error(t, err)
	assert.Equal(t, ErrNotSupported, err)

	// RemoveDir is not supported
	err = fs.RemoveDir("http://example.com/dir/")
	require.Error(t, err)
	assert.Equal(t, ErrNotSupported, err)

	// ReadDir is not supported
	_, err = fs.ReadDir("http://example.com/dir/")
	require.Error(t, err)
	assert.Equal(t, ErrNotSupported, err)

	// CreateContext is not supported
	_, err = fs.CreateContext(context.Background(), "http://example.com/file.txt")
	require.Error(t, err)
	assert.Equal(t, ErrNotSupported, err)

	// ReadDirContext is not supported
	_, err = fs.ReadDirContext(context.Background(), "http://example.com/dir/")
	require.Error(t, err)
	assert.Equal(t, ErrNotSupported, err)
}

// TestHTTPFileInfo tests the HTTPFileInfo implementation.
func TestHTTPFileInfo(t *testing.T) {
	modTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	info := NewHTTPFileInfo("https://example.com/path/to/file.csv", 1024, modTime)

	assert.Equal(t, "file.csv", info.Name())
	assert.Equal(t, int64(1024), info.Size())
	assert.Equal(t, modTime, info.ModTime())
	assert.False(t, info.IsDir())
	assert.Equal(t, FileMode(0), info.Mode())
	assert.Equal(t, "https://example.com/path/to/file.csv", info.URL())
}

// TestHTTPFileInfo_Name tests file info name extraction for various URLs.
func TestHTTPFileInfo_Name(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{"https://example.com/path/to/file.csv", "file.csv"},
		{"https://example.com/file.parquet", "file.parquet"},
		{"https://example.com/", "example.com"},
		{"https://example.com", "example.com"},
		{"https://example.com/file?query=param", "file"},
		{"https://sub.example.com:8080/path/file.json", "file.json"},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			info := NewHTTPFileInfo(tt.url, 100, time.Time{})
			assert.Equal(t, tt.expected, info.Name())
		})
	}
}

// TestHTTPFileInfo_Full tests HTTPFileInfo with all metadata.
func TestHTTPFileInfo_Full(t *testing.T) {
	modTime := time.Now()
	info := NewHTTPFileInfoFull(
		"https://example.com/file.csv",
		2048,
		modTime,
		"text/csv",
		"\"abc123\"",
		true,
	)

	assert.Equal(t, "file.csv", info.Name())
	assert.Equal(t, int64(2048), info.Size())
	assert.Equal(t, modTime, info.ModTime())
	assert.Equal(t, "text/csv", info.ContentType())
	assert.Equal(t, "\"abc123\"", info.ETag())
	assert.True(t, info.AcceptRanges())

	// Check Sys() returns metadata
	sys := info.Sys()
	require.NotNil(t, sys)
	sysMap, ok := sys.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "https://example.com/file.csv", sysMap["url"])
	assert.Equal(t, "text/csv", sysMap["contentType"])
}

// MockHTTPServer creates a mock HTTP server for testing.
type MockHTTPServer struct {
	*httptest.Server
	Files map[string][]byte
}

// NewMockHTTPServer creates a new mock HTTP server with test files.
func NewMockHTTPServer() *MockHTTPServer {
	mock := &MockHTTPServer{
		Files: make(map[string][]byte),
	}

	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		mock.handleRequest(w, r, path)
	}))

	return mock
}

func (m *MockHTTPServer) handleRequest(w http.ResponseWriter, r *http.Request, path string) {
	data, ok := m.Files[path]
	if !ok {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	switch r.Method {
	case http.MethodHead:
		m.handleHead(w, data)
	case http.MethodGet:
		m.handleGet(w, r, data)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (m *MockHTTPServer) handleHead(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
}

func (m *MockHTTPServer) handleGet(w http.ResponseWriter, r *http.Request, data []byte) {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		m.handleRangeRequest(w, rangeHeader, data)

		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (m *MockHTTPServer) handleRangeRequest(
	w http.ResponseWriter,
	rangeHeader string,
	data []byte,
) {
	// Parse Range header: bytes=start-end or bytes=start-
	var start, end int64
	if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
		// Try bytes=start- format
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-", &start); err != nil {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		end = int64(len(data)) - 1
	}

	if start > int64(len(data)) || end > int64(len(data))-1 {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)

		return
	}

	if end < start {
		end = int64(len(data)) - 1
	}

	sliceData := data[start : end+1]

	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(sliceData)))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(sliceData)
}

// TestHTTPFileSystem_Open_Read tests opening and reading an HTTP file.
func TestHTTPFileSystem_Open_Read(t *testing.T) {
	server := NewMockHTTPServer()
	defer server.Close()

	server.Files["test.txt"] = []byte("Hello, World!")

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	// Open file
	file, err := fs.Open(server.URL + "/test.txt")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	// Read content
	data, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, "Hello, World!", string(data))
}

// TestHTTPFileSystem_Stat tests getting file info.
func TestHTTPFileSystem_Stat(t *testing.T) {
	server := NewMockHTTPServer()
	defer server.Close()

	server.Files["data.csv"] = []byte("a,b,c\n1,2,3\n")

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	info, err := fs.Stat(server.URL + "/data.csv")
	require.NoError(t, err)

	assert.Equal(t, "data.csv", info.Name())
	assert.Equal(t, int64(12), info.Size())
	assert.False(t, info.IsDir())
}

// TestHTTPFileSystem_Exists tests checking if a file exists.
func TestHTTPFileSystem_Exists(t *testing.T) {
	server := NewMockHTTPServer()
	defer server.Close()

	server.Files["exists.txt"] = []byte("content")

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	// File exists
	exists, err := fs.Exists(server.URL + "/exists.txt")
	require.NoError(t, err)
	assert.True(t, exists)

	// File does not exist
	exists, err = fs.Exists(server.URL + "/notfound.txt")
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestHTTPFile_ReadAt tests random access reading.
func TestHTTPFile_ReadAt(t *testing.T) {
	server := NewMockHTTPServer()
	defer server.Close()

	server.Files["data.bin"] = []byte("0123456789ABCDEF")

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/data.bin")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	// Read from middle of file
	buf := make([]byte, 4)
	n, err := file.ReadAt(buf, 4)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "4567", string(buf))

	// Read from end
	n, err = file.ReadAt(buf, 12)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "CDEF", string(buf))
}

// TestHTTPFile_ReadAt_EmptyBuffer tests ReadAt with empty buffer.
func TestHTTPFile_ReadAt_EmptyBuffer(t *testing.T) {
	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)

	file, err := fs.Open("https://example.com/file.txt")
	require.NoError(t, err)

	emptyBuf := make([]byte, 0)
	n, err := file.ReadAt(emptyBuf, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// TestHTTPFile_Seek tests seeking in an HTTP file.
func TestHTTPFile_Seek(t *testing.T) {
	server := NewMockHTTPServer()
	defer server.Close()

	server.Files["seek.txt"] = []byte("0123456789")

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/seek.txt")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	// Seek to position 5
	pos, err := file.Seek(5, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(5), pos)

	// Read from position 5
	buf := make([]byte, 3)
	n, err := file.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, "567", string(buf))

	// Seek relative to current position
	pos, err = file.Seek(-2, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(6), pos)
}

// TestHTTPFile_Seek_Invalid tests invalid seek operations.
func TestHTTPFile_Seek_Invalid(t *testing.T) {
	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)

	file, err := fs.Open("https://example.com/file.txt")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	// Invalid whence
	_, err = file.Seek(0, 999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid seek whence")

	// Negative position
	_, err = file.Seek(-100, io.SeekStart)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative seek position")
}

// TestHTTPFile_Write tests that write operations fail.
func TestHTTPFile_Write(t *testing.T) {
	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)

	file, err := fs.Open("https://example.com/file.txt")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	// Write should fail
	_, err = file.Write([]byte("test"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write not supported")

	// WriteAt should fail
	_, err = file.WriteAt([]byte("test"), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write not supported")

	// WriteContext should fail
	httpFile := file.(*HTTPFile)
	_, err = httpFile.WriteContext(context.Background(), []byte("test"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write not supported")
}

// TestHTTPFile_Stat tests getting file info from a file handle.
func TestHTTPFile_Stat(t *testing.T) {
	server := NewMockHTTPServer()
	defer server.Close()

	server.Files["stat.txt"] = []byte("content for stat")

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/stat.txt")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	require.NoError(t, err)

	assert.Equal(t, "stat.txt", info.Name())
	assert.Equal(t, int64(16), info.Size())
}

// TestHTTPFile_Close tests closing an HTTP file.
func TestHTTPFile_Close(t *testing.T) {
	server := NewMockHTTPServer()
	defer server.Close()

	server.Files["close.txt"] = []byte("test data here")

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/close.txt")
	require.NoError(t, err)

	// Read to open the connection (read less than file size)
	buf := make([]byte, 4)
	n, err := file.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "test", string(buf))

	// Close should succeed
	err = file.Close()
	require.NoError(t, err)

	// Closing again should also succeed
	err = file.Close()
	require.NoError(t, err)
}

// TestHTTPFileSystem_Redirects tests redirect handling.
func TestHTTPFileSystem_Redirects(t *testing.T) {
	redirectCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			redirectCount++
			http.Redirect(w, r, "/final", http.StatusFound)

			return
		}
		if r.URL.Path == "/final" {
			w.Header().Set("Content-Length", "5")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("hello"))

			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/redirect")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	data, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data))
	assert.Equal(t, 1, redirectCount)
}

// TestHTTPFileSystem_NoRedirects tests disabling redirects.
func TestHTTPFileSystem_NoRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/final", http.StatusFound)

			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	config := NewHTTPConfig(WithHTTPFollowRedirects(false))
	fs, err := NewHTTPFileSystem(context.Background(), config)
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/redirect")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	// With FollowRedirects=false, reading should fail because we get a 302 response
	// which is not a valid status for reading content
	_, err = io.ReadAll(file)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "302")
}

// TestHTTPFileSystem_MaxRedirects tests max redirect limit.
func TestHTTPFileSystem_MaxRedirects(t *testing.T) {
	redirectNum := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectNum++
		http.Redirect(w, r, fmt.Sprintf("/redirect%d", redirectNum), http.StatusFound)
	}))
	defer server.Close()

	config := NewHTTPConfig(WithHTTPMaxRedirects(3))
	fs, err := NewHTTPFileSystem(context.Background(), config)
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/start")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	// Should fail after max redirects
	_, err = io.ReadAll(file)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many redirects")
}

// TestHTTPFileSystem_CustomHeaders tests custom header support.
func TestHTTPFileSystem_CustomHeaders(t *testing.T) {
	receivedHeaders := make(map[string]string)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders["X-Custom"] = r.Header.Get("X-Custom")
		receivedHeaders["User-Agent"] = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	config := NewHTTPConfig(
		WithHTTPHeader("X-Custom", "custom-value"),
		WithHTTPUserAgent("test-agent/2.0"),
	)
	fs, err := NewHTTPFileSystem(context.Background(), config)
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/test")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	_, err = io.ReadAll(file)
	require.NoError(t, err)

	assert.Equal(t, "custom-value", receivedHeaders["X-Custom"])
	assert.Equal(t, "test-agent/2.0", receivedHeaders["User-Agent"])
}

// TestHTTPFileSystem_BasicAuth tests basic authentication.
func TestHTTPFileSystem_BasicAuth(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		user, pass, ok := r.BasicAuth()
		if !ok || user != "testuser" || pass != "testpass" {
			w.WriteHeader(http.StatusUnauthorized)

			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("authenticated"))
	}))
	defer server.Close()

	config := NewHTTPConfig(WithHTTPBasicAuth("testuser", "testpass"))
	fs, err := NewHTTPFileSystem(context.Background(), config)
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/protected")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	data, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, "authenticated", string(data))
	assert.NotEmpty(t, authHeader)
	assert.True(t, strings.HasPrefix(authHeader, "Basic "))
}

// TestHTTPFileSystem_BearerToken tests bearer token authentication.
func TestHTTPFileSystem_BearerToken(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		if authHeader != "Bearer secret-token-123" {
			w.WriteHeader(http.StatusUnauthorized)

			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("authorized"))
	}))
	defer server.Close()

	config := NewHTTPConfig(WithHTTPBearerToken("secret-token-123"))
	fs, err := NewHTTPFileSystem(context.Background(), config)
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/api")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	data, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, "authorized", string(data))
	assert.Equal(t, "Bearer secret-token-123", authHeader)
}

// TestHTTPFileSystem_Timeout tests timeout configuration.
func TestHTTPFileSystem_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := NewHTTPConfig(WithHTTPTimeout(100 * time.Millisecond))
	fs, err := NewHTTPFileSystem(context.Background(), config)
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/slow")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	// Read should timeout
	_, err = io.ReadAll(file)
	require.Error(t, err)
}

// TestHTTPFileSystem_404 tests handling of 404 responses.
func TestHTTPFileSystem_404(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	// Stat should fail with 404
	_, err = fs.Stat(server.URL + "/notfound")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestHTTPFileSystem_500 tests handling of server errors.
func TestHTTPFileSystem_500(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/error")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	// Read should fail
	_, err = io.ReadAll(file)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

// TestHTTPFileSystem_OpenContext_Cancelled tests context cancellation.
func TestHTTPFileSystem_OpenContext_Cancelled(t *testing.T) {
	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = fs.OpenContext(cancelledCtx, "https://example.com/file.txt")
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestFactoryHTTPSchemeRegistration tests that HTTP schemes are registered with the factory.
func TestFactoryHTTPSchemeRegistration(t *testing.T) {
	factory := NewFileSystemFactory()

	schemes := factory.SupportedSchemes()

	// Check that HTTP schemes are registered
	assert.Contains(t, schemes, "http")
	assert.Contains(t, schemes, "https")
}

// TestIsCloudURL_HTTP tests cloud URL detection for HTTP URLs.
func TestIsCloudURL_HTTP(t *testing.T) {
	tests := []struct {
		url      string
		expected bool
	}{
		{"http://example.com/file.csv", true},
		{"https://example.com/file.parquet", true},
		{"HTTP://EXAMPLE.COM/FILE", true},
		{"HTTPS://EXAMPLE.COM/FILE", true},
		{"s3://bucket/key", true},
		{"file:///tmp/file.txt", false},
		{"/local/path", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			result := IsCloudURL(tt.url)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestHTTPFileSystem_GetConfig tests getting the config from the filesystem.
func TestHTTPFileSystem_GetConfig(t *testing.T) {
	config := HTTPConfig{
		Timeout:   45 * time.Second,
		UserAgent: "test-agent",
	}

	fs := NewHTTPFileSystemWithClient(nil, &config)
	retrieved := fs.GetConfig()

	assert.Equal(t, 45*time.Second, retrieved.Timeout)
	assert.Equal(t, "test-agent", retrieved.UserAgent)
}

// TestHTTPFileSystem_Close tests the Close method.
func TestHTTPFileSystem_Close(t *testing.T) {
	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)

	// Close should succeed
	err = fs.Close()
	require.NoError(t, err)
}

// TestNewHTTPFileSystemWithClient tests creating HTTPFileSystem with a pre-configured client.
func TestNewHTTPFileSystemWithClient(t *testing.T) {
	config := &HTTPConfig{
		Timeout:   60 * time.Second,
		UserAgent: "custom-client",
	}

	// Create with nil client (for testing)
	fs := NewHTTPFileSystemWithClient(nil, config)

	assert.NotNil(t, fs)
	assert.Equal(t, 60*time.Second, fs.config.Timeout)
	assert.Equal(t, "custom-client", fs.config.UserAgent)
	assert.Equal(t, "http://", fs.URI())
}

// TestHTTPFile_Size tests the Size method.
func TestHTTPFile_Size(t *testing.T) {
	server := NewMockHTTPServer()
	defer server.Close()

	server.Files["sized.txt"] = []byte("1234567890")

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/sized.txt")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	httpFile := file.(*HTTPFile)

	// Size unknown before first read
	assert.Equal(t, int64(-1), httpFile.Size())

	// Read to populate size
	buf := make([]byte, 1)
	_, err = file.Read(buf)
	require.NoError(t, err)

	// Size should now be known
	assert.Equal(t, int64(10), httpFile.Size())
}

// TestHTTPFile_AcceptRanges tests the AcceptRanges method.
func TestHTTPFile_AcceptRanges(t *testing.T) {
	server := NewMockHTTPServer()
	defer server.Close()

	server.Files["ranges.txt"] = []byte("range content")

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)
	defer func() { _ = fs.Close() }()

	file, err := fs.Open(server.URL + "/ranges.txt")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	httpFile := file.(*HTTPFile)

	// AcceptRanges unknown before first read
	assert.False(t, httpFile.AcceptRanges())

	// Read to populate metadata
	buf := make([]byte, 1)
	_, err = file.Read(buf)
	require.NoError(t, err)

	// AcceptRanges should now be known
	assert.True(t, httpFile.AcceptRanges())
}

// TestHTTPFile_URL tests the URL method.
func TestHTTPFile_URL(t *testing.T) {
	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	require.NoError(t, err)

	file, err := fs.Open("https://example.com/path/to/file.txt")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	httpFile := file.(*HTTPFile)
	assert.Equal(t, "https://example.com/path/to/file.txt", httpFile.URL())
}

// Verify interface implementations at compile time.
var (
	_ FileSystem        = (*HTTPFileSystem)(nil)
	_ ContextFileSystem = (*HTTPFileSystem)(nil)
	_ File              = (*HTTPFile)(nil)
	_ FileInfo          = (*HTTPFileInfo)(nil)
)
