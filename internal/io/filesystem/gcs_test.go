package filesystem

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseGCSPath tests the GCS URL parsing function.
func TestParseGCSPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantBucket string
		wantObject string
		wantErr    bool
	}{
		{
			name:       "simple bucket and object",
			path:       "gs://mybucket/myobject",
			wantBucket: "mybucket",
			wantObject: "myobject",
			wantErr:    false,
		},
		{
			name:       "nested object path",
			path:       "gs://mybucket/path/to/object.csv",
			wantBucket: "mybucket",
			wantObject: "path/to/object.csv",
			wantErr:    false,
		},
		{
			name:       "gcs scheme",
			path:       "gcs://mybucket/object",
			wantBucket: "mybucket",
			wantObject: "object",
			wantErr:    false,
		},
		{
			name:       "bucket only",
			path:       "gs://mybucket/",
			wantBucket: "mybucket",
			wantObject: "",
			wantErr:    false,
		},
		{
			name:       "bucket without trailing slash",
			path:       "gs://mybucket",
			wantBucket: "mybucket",
			wantObject: "",
			wantErr:    false,
		},
		{
			name:       "with query parameters",
			path:       "gs://mybucket/object?project=myproject",
			wantBucket: "mybucket",
			wantObject: "object",
			wantErr:    false,
		},
		{
			name:       "raw path without scheme",
			path:       "mybucket/myobject",
			wantBucket: "mybucket",
			wantObject: "myobject",
			wantErr:    false,
		},
		{
			name:       "uppercase scheme",
			path:       "GS://mybucket/object",
			wantBucket: "mybucket",
			wantObject: "object",
			wantErr:    false,
		},
		{
			name:       "empty bucket",
			path:       "gs:///object",
			wantBucket: "",
			wantObject: "",
			wantErr:    true,
		},
		{
			name:       "empty path",
			path:       "",
			wantBucket: "",
			wantObject: "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, object, err := parseGCSPath(tt.path)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantBucket, bucket)
			assert.Equal(t, tt.wantObject, object)
		})
	}
}

// TestDefaultGCSConfig tests the default GCS configuration.
func TestDefaultGCSConfig(t *testing.T) {
	config := DefaultGCSConfig()

	assert.Equal(t, DefaultGCSTimeout, config.Timeout)
	assert.Equal(t, int64(DefaultGCSChunkSize), config.ChunkSize)
	assert.Empty(t, config.ProjectID)
	assert.Empty(t, config.Bucket)
	assert.Empty(t, config.KeyFile)
	assert.Empty(t, config.CredentialsJSON)
	assert.Empty(t, config.Endpoint)
}

// TestGCSConfig tests GCS configuration options.
func TestGCSConfig(t *testing.T) {
	config := GCSConfig{
		ProjectID:       "my-project",
		Bucket:          "my-bucket",
		KeyFile:         "/path/to/key.json",
		CredentialsJSON: `{"type": "service_account"}`,
		Endpoint:        "http://localhost:4443",
		Timeout:         60 * time.Second,
		ChunkSize:       32 * 1024 * 1024,
	}

	assert.Equal(t, "my-project", config.ProjectID)
	assert.Equal(t, "my-bucket", config.Bucket)
	assert.Equal(t, "/path/to/key.json", config.KeyFile)
	assert.Equal(t, `{"type": "service_account"}`, config.CredentialsJSON)
	assert.Equal(t, "http://localhost:4443", config.Endpoint)
	assert.Equal(t, 60*time.Second, config.Timeout)
	assert.Equal(t, int64(32*1024*1024), config.ChunkSize)
}

// TestGCSConfigOptions tests GCS configuration option functions.
func TestGCSConfigOptions(t *testing.T) {
	config := NewGCSConfig(
		WithGCSProjectID("test-project"),
		WithGCSBucket("test-bucket"),
		WithGCSEndpoint("http://localhost:4443"),
		WithGCSTimeout(45*time.Second),
		WithGCSChunkSize(64*1024*1024),
		WithGCSConcurrentReads(8, 16*1024*1024),
	)

	assert.Equal(t, "test-project", config.ProjectID)
	assert.Equal(t, "test-bucket", config.Bucket)
	assert.Equal(t, "http://localhost:4443", config.Endpoint)
	assert.Equal(t, 45*time.Second, config.Timeout)
	assert.Equal(t, int64(64*1024*1024), config.ChunkSize)
	assert.Equal(t, 8, config.ConcurrentReadWorkers)
	assert.Equal(t, int64(16*1024*1024), config.ConcurrentReadChunkSize)
}

// TestGCSFileSystem_Capabilities tests the GCS filesystem capabilities.
func TestGCSFileSystem_Capabilities(t *testing.T) {
	// Create a mock filesystem with nil client (for testing capabilities only)
	fs := &GCSFileSystem{
		client: nil,
		config: DefaultGCSConfig(),
	}

	caps := fs.Capabilities()

	assert.True(t, caps.SupportsSeek)
	assert.False(t, caps.SupportsAppend)
	assert.True(t, caps.SupportsRange)
	assert.True(t, caps.SupportsDirList)
	assert.True(t, caps.SupportsWrite)
	assert.True(t, caps.SupportsDelete)
	assert.True(t, caps.ContextTimeout)
}

// TestGCSFileSystem_URI tests the GCS filesystem URI.
func TestGCSFileSystem_URI(t *testing.T) {
	fs := &GCSFileSystem{
		client: nil,
		config: DefaultGCSConfig(),
	}

	assert.Equal(t, "gs://", fs.URI())
}

// TestGCSFileSystem_MkdirAll tests that MkdirAll is a no-op for GCS.
func TestGCSFileSystem_MkdirAll(t *testing.T) {
	fs := &GCSFileSystem{
		client: nil,
		config: DefaultGCSConfig(),
	}

	// MkdirAll should always succeed for GCS
	err := fs.MkdirAll("gs://bucket/path/to/directory")
	require.NoError(t, err)
}

// TestGCSFileInfo tests the GCSFileInfo implementation.
func TestGCSFileInfo(t *testing.T) {
	modTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	info := &GCSFileInfo{
		attrs: &storage.ObjectAttrs{
			Name:    "path/to/file.csv",
			Size:    1024,
			Updated: modTime,
		},
	}

	assert.Equal(t, "file.csv", info.Name())
	assert.Equal(t, int64(1024), info.Size())
	assert.Equal(t, modTime, info.ModTime())
	assert.False(t, info.IsDir())
	assert.Equal(t, FileMode(0), info.Mode())
	assert.NotNil(t, info.Sys())
}

// TestGCSFileInfo_Name_RootLevel tests file info for root-level objects.
func TestGCSFileInfo_Name_RootLevel(t *testing.T) {
	info := &GCSFileInfo{
		attrs: &storage.ObjectAttrs{
			Name: "file.csv",
		},
	}

	assert.Equal(t, "file.csv", info.Name())
}

// TestGCSDirEntry tests the GCSDirEntry implementation.
func TestGCSDirEntry(t *testing.T) {
	// Test regular file entry
	fileEntry := &GCSDirEntry{
		name:  "file.csv",
		isDir: false,
		attrs: &storage.ObjectAttrs{
			Name: "path/file.csv",
			Size: 512,
		},
	}

	assert.Equal(t, "file.csv", fileEntry.Name())
	assert.False(t, fileEntry.IsDir())
	assert.Equal(t, FileMode(0), fileEntry.Type())

	fileInfo, err := fileEntry.Info()
	require.NoError(t, err)
	assert.Equal(t, int64(512), fileInfo.Size())

	// Test directory entry
	dirEntry := &GCSDirEntry{
		name:  "subdir",
		isDir: true,
	}

	assert.Equal(t, "subdir", dirEntry.Name())
	assert.True(t, dirEntry.IsDir())
	assert.Equal(t, ModeDir, dirEntry.Type())

	// Directory entry Info should still work
	dirInfo, err := dirEntry.Info()
	require.NoError(t, err)
	assert.NotNil(t, dirInfo)
}

// TestGCSFile_WriteMode tests GCSFile in write mode.
func TestGCSFile_WriteMode(t *testing.T) {
	// Create a file in write mode (nil client is okay for testing buffer operations)
	f := newGCSFileForWriting(nil, "test-bucket", "test-object")

	// Write some data
	n, err := f.Write([]byte("hello "))
	require.NoError(t, err)
	assert.Equal(t, 6, n)

	n, err = f.Write([]byte("world"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// Verify buffer contents
	f.mu.Lock()
	assert.Equal(t, "hello world", f.writeBuffer.String())
	f.mu.Unlock()

	// Attempting to read in write mode should fail
	buf := make([]byte, 10)
	_, err = f.Read(buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot read in write mode")

	// Attempting to seek in write mode should fail
	_, err = f.Seek(0, io.SeekStart)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "seek not supported in write mode")
}

// TestGCSFile_WriteAt tests WriteAt functionality.
func TestGCSFile_WriteAt(t *testing.T) {
	f := newGCSFileForWriting(nil, "test-bucket", "test-object")

	// Write at beginning
	n, err := f.WriteAt([]byte("hello"), 0)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// Write at offset (with padding)
	n, err = f.WriteAt([]byte("world"), 10)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// Verify buffer has padding
	f.mu.Lock()
	bufLen := f.writeBuffer.Len()
	f.mu.Unlock()
	assert.Equal(t, 15, bufLen)
}

// TestGCSFile_ReadModeError tests that writing in read mode returns an error.
func TestGCSFile_ReadModeError(t *testing.T) {
	f := newGCSFileForReading(nil, "test-bucket", "test-object")

	// Attempting to write in read mode should fail
	_, err := f.Write([]byte("test"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot write in read mode")

	// WriteAt should also fail
	_, err = f.WriteAt([]byte("test"), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot write in read mode")
}

// TestGCSFile_SeekWhence tests different seek whence values.
func TestGCSFile_SeekWhence(t *testing.T) {
	// Create a file in read mode (seek is supported)
	f := newGCSFileForReading(nil, "test-bucket", "test-object")

	// SeekStart
	pos, err := f.Seek(10, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(10), pos)

	// SeekCurrent
	pos, err = f.Seek(5, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(15), pos)

	// Invalid whence
	_, err = f.Seek(0, 999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid seek whence")

	// Negative position
	_, err = f.Seek(-100, io.SeekStart)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative seek position")
}

// TestGCSFile_ReadAt_EmptyBuffer tests ReadAt with empty buffer.
func TestGCSFile_ReadAt_EmptyBuffer(t *testing.T) {
	f := newGCSFileForReading(nil, "test-bucket", "test-object")

	// ReadAt with empty buffer should return immediately
	emptyBuf := make([]byte, 0)
	n, err := f.ReadAt(emptyBuf, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// TestGCSFile_Close_ReadMode tests closing a file in read mode.
func TestGCSFile_Close_ReadMode(t *testing.T) {
	f := newGCSFileForReading(nil, "test-bucket", "test-object")

	// Close should succeed even without a reader
	err := f.Close()
	require.NoError(t, err)
}

// TestGCSFile_Close_WriteMode_EmptyBuffer tests closing with empty write buffer.
func TestGCSFile_Close_WriteMode_EmptyBuffer(t *testing.T) {
	f := newGCSFileForWriting(nil, "test-bucket", "test-object")

	// Close with empty buffer should succeed without upload
	err := f.Close()
	require.NoError(t, err)
}

// TestFactoryGCSSchemeRegistration tests that GCS schemes are registered with the factory.
func TestFactoryGCSSchemeRegistration(t *testing.T) {
	factory := NewFileSystemFactory()

	schemes := factory.SupportedSchemes()

	// Check that GCS schemes are registered
	assert.Contains(t, schemes, "gs")
	assert.Contains(t, schemes, "gcs")
	assert.Contains(t, schemes, "file")
}

// TestIsCloudURL_GCS tests cloud URL detection for GCS URLs.
func TestIsCloudURL_GCS(t *testing.T) {
	tests := []struct {
		url      string
		expected bool
	}{
		{"gs://bucket/object", true},
		{"gcs://bucket/object", true},
		{"GS://BUCKET/OBJECT", true},
		{"GCS://BUCKET/OBJECT", true},
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

// MockGCSServer creates a mock GCS server for testing.
type MockGCSServer struct {
	*httptest.Server
	Objects map[string][]byte
}

// NewMockGCSServer creates a new mock GCS server.
func NewMockGCSServer() *MockGCSServer {
	mock := &MockGCSServer{
		Objects: make(map[string][]byte),
	}

	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		mock.handleRequest(w, r, key)
	}))

	return mock
}

func (m *MockGCSServer) handleRequest(w http.ResponseWriter, r *http.Request, key string) {
	switch r.Method {
	case http.MethodGet:
		m.handleGet(w, r, key)
	case http.MethodPost, http.MethodPut:
		m.handlePut(w, r, key)
	case http.MethodHead:
		m.handleHead(w, key)
	case http.MethodDelete:
		delete(m.Objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (m *MockGCSServer) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	data, ok := m.Objects[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if r.Header.Get("Range") != "" {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	_, _ = w.Write(data)
}

func (m *MockGCSServer) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, _ := io.ReadAll(r.Body)
	m.Objects[key] = body
	w.WriteHeader(http.StatusOK)
}

func (m *MockGCSServer) handleHead(w http.ResponseWriter, key string) {
	data, ok := m.Objects[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	w.Header().Set("Content-Length", string(rune(len(data))))
	w.Header().Set("Last-Modified", time.Now().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

// TestGCSFileSystem_NewWithClient tests creating GCSFileSystem with a pre-configured client.
func TestGCSFileSystem_NewWithClient(t *testing.T) {
	config := &GCSConfig{
		ProjectID: "test-project",
		Bucket:    "test-bucket",
	}

	// Create with nil client (for testing)
	fs := NewGCSFileSystemWithClient(nil, config)

	assert.NotNil(t, fs)
	assert.Equal(t, "test-project", fs.config.ProjectID)
	assert.Equal(t, "test-bucket", fs.config.Bucket)
	assert.Equal(t, "gs://", fs.URI())
}

// TestGCSFileSystem_GetConfig tests getting the config from the filesystem.
func TestGCSFileSystem_GetConfig(t *testing.T) {
	config := GCSConfig{
		ProjectID: "my-project",
		Bucket:    "my-bucket",
		Timeout:   45 * time.Second,
	}

	fs := NewGCSFileSystemWithClient(nil, &config)
	retrieved := fs.GetConfig()

	assert.Equal(t, "my-project", retrieved.ProjectID)
	assert.Equal(t, "my-bucket", retrieved.Bucket)
	assert.Equal(t, 45*time.Second, retrieved.Timeout)
}

// TestStripGCSPrefix tests the prefix stripping function.
func TestStripGCSPrefix(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"gs://bucket/object", "bucket/object"},
		{"gcs://bucket/object", "bucket/object"},
		{"GS://bucket/object", "bucket/object"},
		{"GCS://bucket/object", "bucket/object"},
		{"bucket/object", "bucket/object"},
		{"s3://bucket/key", "s3://bucket/key"}, // Not a GCS prefix
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := stripGCSPrefix(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestGCSFile_WriteContext tests WriteContext with context cancellation.
func TestGCSFile_WriteContext(t *testing.T) {
	f := newGCSFileForWriting(nil, "test-bucket", "test-object")

	// Normal write should succeed
	ctx := context.Background()
	n, err := f.WriteContext(ctx, []byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// Write with cancelled context should fail
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = f.WriteContext(cancelledCtx, []byte("world"))
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestGCSFileSystem_OpenContext_Cancelled tests OpenContext with cancelled context.
func TestGCSFileSystem_OpenContext_Cancelled(t *testing.T) {
	fs := &GCSFileSystem{
		client: nil,
		config: DefaultGCSConfig(),
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := fs.OpenContext(cancelledCtx, "gs://bucket/object")
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestGCSFileSystem_CreateContext_Cancelled tests CreateContext with cancelled context.
func TestGCSFileSystem_CreateContext_Cancelled(t *testing.T) {
	fs := &GCSFileSystem{
		client: nil,
		config: DefaultGCSConfig(),
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := fs.CreateContext(cancelledCtx, "gs://bucket/object")
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// Verify interface implementations at compile time.
var (
	_ FileSystem        = (*GCSFileSystem)(nil)
	_ ContextFileSystem = (*GCSFileSystem)(nil)
	_ File              = (*GCSFile)(nil)
	_ FileInfo          = (*GCSFileInfo)(nil)
	_ DirEntry          = (*GCSDirEntry)(nil)
)
