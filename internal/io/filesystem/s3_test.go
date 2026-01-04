package filesystem

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseS3Path tests the S3 URL parsing function.
func TestParseS3Path(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{
			name:       "simple bucket and key",
			path:       "s3://mybucket/mykey",
			wantBucket: "mybucket",
			wantKey:    "mykey",
			wantErr:    false,
		},
		{
			name:       "nested key path",
			path:       "s3://mybucket/path/to/object.csv",
			wantBucket: "mybucket",
			wantKey:    "path/to/object.csv",
			wantErr:    false,
		},
		{
			name:       "s3a scheme",
			path:       "s3a://mybucket/key",
			wantBucket: "mybucket",
			wantKey:    "key",
			wantErr:    false,
		},
		{
			name:       "s3n scheme",
			path:       "s3n://mybucket/key",
			wantBucket: "mybucket",
			wantKey:    "key",
			wantErr:    false,
		},
		{
			name:       "bucket only",
			path:       "s3://mybucket/",
			wantBucket: "mybucket",
			wantKey:    "",
			wantErr:    false,
		},
		{
			name:       "bucket without trailing slash",
			path:       "s3://mybucket",
			wantBucket: "mybucket",
			wantKey:    "",
			wantErr:    false,
		},
		{
			name:       "with query parameters",
			path:       "s3://mybucket/key?region=us-west-2",
			wantBucket: "mybucket",
			wantKey:    "key",
			wantErr:    false,
		},
		{
			name:       "raw path without scheme",
			path:       "mybucket/mykey",
			wantBucket: "mybucket",
			wantKey:    "mykey",
			wantErr:    false,
		},
		{
			name:       "uppercase scheme",
			path:       "S3://mybucket/key",
			wantBucket: "mybucket",
			wantKey:    "key",
			wantErr:    false,
		},
		{
			name:       "empty bucket",
			path:       "s3:///key",
			wantBucket: "",
			wantKey:    "",
			wantErr:    true,
		},
		{
			name:       "empty path",
			path:       "",
			wantBucket: "",
			wantKey:    "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := parseS3Path(tt.path)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantBucket, bucket)
			assert.Equal(t, tt.wantKey, key)
		})
	}
}

// TestDefaultS3Config tests the default S3 configuration.
func TestDefaultS3Config(t *testing.T) {
	config := DefaultS3Config()

	assert.Equal(t, "us-east-1", config.Region)
	assert.True(t, config.UseSSL)
	assert.Equal(t, S3URLStyleVirtual, config.URLStyle)
	assert.Empty(t, config.Endpoint)
	assert.Empty(t, config.AccessKeyID)
	assert.Empty(t, config.SecretAccessKey)
}

// TestS3FileSystem_Capabilities tests the S3 filesystem capabilities.
func TestS3FileSystem_Capabilities(t *testing.T) {
	// Create a mock server for testing
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := S3Config{
		Endpoint:        strings.TrimPrefix(server.URL, "http://"),
		UseSSL:          false,
		URLStyle:        S3URLStylePath,
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	fs, err := NewS3FileSystem(context.Background(), config)
	require.NoError(t, err)

	caps := fs.Capabilities()

	assert.True(t, caps.SupportsSeek)
	assert.False(t, caps.SupportsAppend)
	assert.True(t, caps.SupportsRange)
	assert.True(t, caps.SupportsDirList)
	assert.True(t, caps.SupportsWrite)
	assert.True(t, caps.SupportsDelete)
	assert.True(t, caps.ContextTimeout)
}

// TestS3FileSystem_URI tests the S3 filesystem URI.
func TestS3FileSystem_URI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := S3Config{
		Endpoint:        strings.TrimPrefix(server.URL, "http://"),
		UseSSL:          false,
		URLStyle:        S3URLStylePath,
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	fs, err := NewS3FileSystem(context.Background(), config)
	require.NoError(t, err)

	assert.Equal(t, "s3://", fs.URI())
}

// TestS3FileSystem_MkdirAll tests that MkdirAll is a no-op for S3.
func TestS3FileSystem_MkdirAll(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := S3Config{
		Endpoint:        strings.TrimPrefix(server.URL, "http://"),
		UseSSL:          false,
		URLStyle:        S3URLStylePath,
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	fs, err := NewS3FileSystem(context.Background(), config)
	require.NoError(t, err)

	// MkdirAll should always succeed for S3
	err = fs.MkdirAll("s3://bucket/path/to/directory")
	require.NoError(t, err)
}

// TestS3FileInfo tests the S3FileInfo implementation.
func TestS3FileInfo(t *testing.T) {
	modTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	info := &S3FileInfo{
		info: minio.ObjectInfo{
			Key:          "path/to/file.csv",
			Size:         1024,
			LastModified: modTime,
		},
	}

	assert.Equal(t, "file.csv", info.Name())
	assert.Equal(t, int64(1024), info.Size())
	assert.Equal(t, modTime, info.ModTime())
	assert.False(t, info.IsDir())
	assert.Equal(t, FileMode(0), info.Mode())
	assert.NotNil(t, info.Sys())
}

// TestS3FileInfo_Name_RootLevel tests file info for root-level objects.
func TestS3FileInfo_Name_RootLevel(t *testing.T) {
	info := &S3FileInfo{
		info: minio.ObjectInfo{
			Key: "file.csv",
		},
	}

	assert.Equal(t, "file.csv", info.Name())
}

// TestS3DirEntry tests the S3DirEntry implementation.
func TestS3DirEntry(t *testing.T) {
	// Test regular file entry
	fileEntry := &S3DirEntry{
		name:  "file.csv",
		isDir: false,
		info: minio.ObjectInfo{
			Key:  "path/file.csv",
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
	dirEntry := &S3DirEntry{
		name:  "subdir",
		isDir: true,
	}

	assert.Equal(t, "subdir", dirEntry.Name())
	assert.True(t, dirEntry.IsDir())
	assert.Equal(t, ModeDir, dirEntry.Type())
}

// TestS3File_WriteMode tests S3File in write mode.
func TestS3File_WriteMode(t *testing.T) {
	// Create a mock minio client (nil is okay for testing buffer operations)
	f := newS3File(nil, "test-bucket", "test-key", true)

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

// TestS3File_WriteAt tests WriteAt functionality.
func TestS3File_WriteAt(t *testing.T) {
	f := newS3File(nil, "test-bucket", "test-key", true)

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

// TestS3URLStyle tests URL style constants.
func TestS3URLStyle(t *testing.T) {
	assert.Equal(t, S3URLStyle("path"), S3URLStylePath)
	assert.Equal(t, S3URLStyle("virtual"), S3URLStyleVirtual)
}

// TestS3Config tests S3 configuration options.
func TestS3Config(t *testing.T) {
	config := S3Config{
		Region:          "eu-west-1",
		Endpoint:        "custom.s3.endpoint.com",
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		SessionToken:    "session-token",
		UseSSL:          true,
		URLStyle:        S3URLStylePath,
		BucketLookup:    minio.BucketLookupPath,
	}

	assert.Equal(t, "eu-west-1", config.Region)
	assert.Equal(t, "custom.s3.endpoint.com", config.Endpoint)
	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", config.AccessKeyID)
	assert.Equal(t, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.SecretAccessKey)
	assert.Equal(t, "session-token", config.SessionToken)
	assert.True(t, config.UseSSL)
	assert.Equal(t, S3URLStylePath, config.URLStyle)
	assert.Equal(t, minio.BucketLookupPath, config.BucketLookup)
}

// TestS3FileSystem_NewWithClient tests creating S3FileSystem with a pre-configured client.
func TestS3FileSystem_NewWithClient(t *testing.T) {
	config := &S3Config{
		Region: "us-east-1",
	}

	// Create with nil client (for testing)
	fs := NewS3FileSystemWithClient(nil, config)

	assert.NotNil(t, fs)
	assert.Equal(t, "us-east-1", fs.config.Region)
	assert.Equal(t, "s3://", fs.URI())
}

// TestFactoryS3SchemeRegistration tests that S3 schemes are registered with the factory.
func TestFactoryS3SchemeRegistration(t *testing.T) {
	factory := NewFileSystemFactory()

	schemes := factory.SupportedSchemes()

	// Check that S3 schemes are registered
	assert.Contains(t, schemes, "s3")
	assert.Contains(t, schemes, "s3a")
	assert.Contains(t, schemes, "s3n")
	assert.Contains(t, schemes, "file")
}

// TestIsCloudURL_S3 tests cloud URL detection for S3 URLs.
func TestIsCloudURL_S3(t *testing.T) {
	tests := []struct {
		url      string
		expected bool
	}{
		{"s3://bucket/key", true},
		{"s3a://bucket/key", true},
		{"s3n://bucket/key", true},
		{"S3://BUCKET/KEY", true},
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

// MockS3Server creates a mock S3 server for testing.
type MockS3Server struct {
	*httptest.Server
	Objects map[string][]byte
}

// NewMockS3Server creates a new mock S3 server.
func NewMockS3Server() *MockS3Server {
	mock := &MockS3Server{
		Objects: make(map[string][]byte),
	}

	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		mock.handleRequest(w, r, key)
	}))

	return mock
}

func (m *MockS3Server) handleRequest(w http.ResponseWriter, r *http.Request, key string) {
	switch r.Method {
	case http.MethodGet:
		m.handleGet(w, r, key)
	case http.MethodPut:
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

func (m *MockS3Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
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

func (m *MockS3Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, _ := io.ReadAll(r.Body)
	m.Objects[key] = body
	w.WriteHeader(http.StatusOK)
}

func (m *MockS3Server) handleHead(w http.ResponseWriter, key string) {
	data, ok := m.Objects[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	w.Header().Set("Content-Length", string(rune(len(data))))
	w.Header().Set("Last-Modified", time.Now().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

// TestS3FileSystem_Integration tests S3FileSystem with a mock server.
func TestS3FileSystem_Integration(t *testing.T) {
	mock := NewMockS3Server()
	defer mock.Close()

	// Add test object
	mock.Objects["test-bucket/test-key.txt"] = []byte("hello world")

	config := S3Config{
		Endpoint:        strings.TrimPrefix(mock.URL, "http://"),
		UseSSL:          false,
		URLStyle:        S3URLStylePath,
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	fs, err := NewS3FileSystem(context.Background(), config)
	require.NoError(t, err)

	// Test capabilities
	caps := fs.Capabilities()
	assert.True(t, caps.SupportsRange)

	// Test URI
	assert.Equal(t, "s3://", fs.URI())

	// Test MkdirAll (no-op for S3)
	err = fs.MkdirAll("s3://bucket/path")
	require.NoError(t, err)
}

// TestS3File_ReadModeError tests that writing in read mode returns an error.
func TestS3File_ReadModeError(t *testing.T) {
	f := newS3File(nil, "test-bucket", "test-key", false)

	// Attempting to write in read mode should fail
	_, err := f.Write([]byte("test"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot write in read mode")

	// WriteAt should also fail
	_, err = f.WriteAt([]byte("test"), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot write in read mode")
}

// TestS3File_SeekWhence tests different seek whence values.
func TestS3File_SeekWhence(t *testing.T) {
	// Create a file in read mode (seek is supported)
	f := newS3File(nil, "test-bucket", "test-key", false)

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

// TestS3File_ReadAt_EmptyBuffer tests ReadAt with empty buffer.
func TestS3File_ReadAt_EmptyBuffer(t *testing.T) {
	f := newS3File(nil, "test-bucket", "test-key", false)

	// ReadAt with empty buffer should return immediately
	emptyBuf := make([]byte, 0)
	n, err := f.ReadAt(emptyBuf, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// TestS3File_Close_ReadMode tests closing a file in read mode.
func TestS3File_Close_ReadMode(t *testing.T) {
	f := newS3File(nil, "test-bucket", "test-key", false)

	// Close should succeed even without a reader
	err := f.Close()
	require.NoError(t, err)
}

// TestS3File_Close_WriteMode_EmptyBuffer tests closing with empty write buffer.
func TestS3File_Close_WriteMode_EmptyBuffer(t *testing.T) {
	f := newS3File(nil, "test-bucket", "test-key", true)

	// Close with empty buffer should succeed without upload
	err := f.Close()
	require.NoError(t, err)
}

// TestNewS3FileSystem_DefaultEndpoint tests default endpoint generation.
func TestNewS3FileSystem_DefaultEndpoint(t *testing.T) {
	// We can't actually create a working client without real credentials,
	// but we can test that the function handles different config scenarios

	// Test with region specified
	config := S3Config{
		Region:          "eu-west-2",
		UseSSL:          true,
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	// This will fail to connect but shouldn't panic
	fs, err := NewS3FileSystem(context.Background(), config)
	require.NoError(t, err)
	assert.NotNil(t, fs)
	assert.Equal(t, "eu-west-2", fs.config.Region)

	// Test without region
	config = S3Config{
		UseSSL:          true,
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	fs, err = NewS3FileSystem(context.Background(), config)
	require.NoError(t, err)
	assert.NotNil(t, fs)
}

// TestNewS3FileSystem_WithCustomEndpoint tests custom endpoint configuration.
func TestNewS3FileSystem_WithCustomEndpoint(t *testing.T) {
	config := S3Config{
		Endpoint:        "minio.local:9000",
		Region:          "us-east-1",
		UseSSL:          false,
		URLStyle:        S3URLStylePath,
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
	}

	fs, err := NewS3FileSystem(context.Background(), config)
	require.NoError(t, err)
	assert.NotNil(t, fs)
	assert.Equal(t, "minio.local:9000", fs.config.Endpoint)
}

// Verify interface implementations at compile time.
var (
	_ FileSystem = (*S3FileSystem)(nil)
	_ File       = (*S3File)(nil)
	_ FileInfo   = (*S3FileInfo)(nil)
	_ DirEntry   = (*S3DirEntry)(nil)
)
