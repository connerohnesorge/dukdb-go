package filesystem

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseAzurePath tests the Azure URL parsing function.
func TestParseAzurePath(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		wantContainer string
		wantBlob      string
		wantErr       bool
	}{
		{
			name:          "simple container and blob",
			path:          "azure://mycontainer/myblob",
			wantContainer: "mycontainer",
			wantBlob:      "myblob",
			wantErr:       false,
		},
		{
			name:          "nested blob path",
			path:          "azure://mycontainer/path/to/blob.csv",
			wantContainer: "mycontainer",
			wantBlob:      "path/to/blob.csv",
			wantErr:       false,
		},
		{
			name:          "az scheme",
			path:          "az://mycontainer/blob",
			wantContainer: "mycontainer",
			wantBlob:      "blob",
			wantErr:       false,
		},
		{
			name:          "container only",
			path:          "azure://mycontainer/",
			wantContainer: "mycontainer",
			wantBlob:      "",
			wantErr:       false,
		},
		{
			name:          "container without trailing slash",
			path:          "azure://mycontainer",
			wantContainer: "mycontainer",
			wantBlob:      "",
			wantErr:       false,
		},
		{
			name:          "with query parameters",
			path:          "azure://mycontainer/blob?sastoken=abc",
			wantContainer: "mycontainer",
			wantBlob:      "blob",
			wantErr:       false,
		},
		{
			name:          "raw path without scheme",
			path:          "mycontainer/myblob",
			wantContainer: "mycontainer",
			wantBlob:      "myblob",
			wantErr:       false,
		},
		{
			name:          "uppercase scheme",
			path:          "AZURE://mycontainer/blob",
			wantContainer: "mycontainer",
			wantBlob:      "blob",
			wantErr:       false,
		},
		{
			name:          "empty container",
			path:          "azure:///blob",
			wantContainer: "",
			wantBlob:      "",
			wantErr:       true,
		},
		{
			name:          "empty path",
			path:          "",
			wantContainer: "",
			wantBlob:      "",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			container, blob, err := parseAzurePath(tt.path)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantContainer, container)
			assert.Equal(t, tt.wantBlob, blob)
		})
	}
}

// TestDefaultAzureConfig tests the default Azure configuration.
func TestDefaultAzureConfig(t *testing.T) {
	config := DefaultAzureConfig()

	assert.Equal(t, DefaultAzureTimeout, config.Timeout)
	assert.Equal(t, int64(DefaultAzureBlockSize), config.BlockSize)
	assert.True(t, config.UseHTTPS)
	assert.Empty(t, config.AccountName)
	assert.Empty(t, config.AccountKey)
	assert.Empty(t, config.ConnectionString)
	assert.Empty(t, config.SASToken)
	assert.Empty(t, config.Container)
	assert.Empty(t, config.Endpoint)
}

// TestAzureConfig tests Azure configuration options.
func TestAzureConfig(t *testing.T) {
	config := AzureConfig{
		AccountName:      "mystorageaccount",
		AccountKey:       "myaccountkey123",
		ConnectionString: "DefaultEndpointsProtocol=https;...",
		SASToken:         "sv=2021-06-08&ss=b&srt=o&sp=r",
		Container:        "mycontainer",
		Endpoint:         "http://127.0.0.1:10000/devstoreaccount1",
		TenantID:         "tenant-123",
		ClientID:         "client-456",
		ClientSecret:     "secret-789",
		UseHTTPS:         false,
		Timeout:          60 * time.Second,
		BlockSize:        8 * 1024 * 1024,
	}

	assert.Equal(t, "mystorageaccount", config.AccountName)
	assert.Equal(t, "myaccountkey123", config.AccountKey)
	assert.Equal(t, "DefaultEndpointsProtocol=https;...", config.ConnectionString)
	assert.Equal(t, "sv=2021-06-08&ss=b&srt=o&sp=r", config.SASToken)
	assert.Equal(t, "mycontainer", config.Container)
	assert.Equal(t, "http://127.0.0.1:10000/devstoreaccount1", config.Endpoint)
	assert.Equal(t, "tenant-123", config.TenantID)
	assert.Equal(t, "client-456", config.ClientID)
	assert.Equal(t, "secret-789", config.ClientSecret)
	assert.False(t, config.UseHTTPS)
	assert.Equal(t, 60*time.Second, config.Timeout)
	assert.Equal(t, int64(8*1024*1024), config.BlockSize)
}

// TestAzureConfigOptions tests Azure configuration option functions.
func TestAzureConfigOptions(t *testing.T) {
	config := NewAzureConfig(
		WithAzureAccountName("teststorage"),
		WithAzureAccountKey("testkey"),
		WithAzureContainer("testcontainer"),
		WithAzureEndpoint("http://localhost:10000"),
		WithAzureHTTPS(false),
		WithAzureTimeout(45*time.Second),
		WithAzureBlockSize(16*1024*1024),
		WithAzureConcurrentReads(8, 16*1024*1024),
		WithAzureServicePrincipal("tenant", "client", "secret"),
	)

	assert.Equal(t, "teststorage", config.AccountName)
	assert.Equal(t, "testkey", config.AccountKey)
	assert.Equal(t, "testcontainer", config.Container)
	assert.Equal(t, "http://localhost:10000", config.Endpoint)
	assert.False(t, config.UseHTTPS)
	assert.Equal(t, 45*time.Second, config.Timeout)
	assert.Equal(t, int64(16*1024*1024), config.BlockSize)
	assert.Equal(t, 8, config.ConcurrentReadWorkers)
	assert.Equal(t, int64(16*1024*1024), config.ConcurrentReadChunkSize)
	assert.Equal(t, "tenant", config.TenantID)
	assert.Equal(t, "client", config.ClientID)
	assert.Equal(t, "secret", config.ClientSecret)
}

// TestAzureFileSystem_Capabilities tests the Azure filesystem capabilities.
func TestAzureFileSystem_Capabilities(t *testing.T) {
	// Create a mock filesystem with nil client (for testing capabilities only)
	fs := &AzureFileSystem{
		client: nil,
		config: DefaultAzureConfig(),
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

// TestAzureFileSystem_URI tests the Azure filesystem URI.
func TestAzureFileSystem_URI(t *testing.T) {
	fs := &AzureFileSystem{
		client: nil,
		config: DefaultAzureConfig(),
	}

	assert.Equal(t, "azure://", fs.URI())
}

// TestAzureFileSystem_MkdirAll tests that MkdirAll is a no-op for Azure.
func TestAzureFileSystem_MkdirAll(t *testing.T) {
	fs := &AzureFileSystem{
		client: nil,
		config: DefaultAzureConfig(),
	}

	// MkdirAll should always succeed for Azure
	err := fs.MkdirAll("azure://container/path/to/directory")
	require.NoError(t, err)
}

// TestAzureFileInfo tests the AzureFileInfo implementation.
func TestAzureFileInfo(t *testing.T) {
	modTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	info := NewAzureFileInfoFromSize("path/to/file.csv", 1024, modTime)

	assert.Equal(t, "file.csv", info.Name())
	assert.Equal(t, int64(1024), info.Size())
	assert.Equal(t, modTime, info.ModTime())
	assert.False(t, info.IsDir())
	assert.Equal(t, FileMode(0), info.Mode())
}

// TestAzureFileInfo_Name_RootLevel tests file info for root-level blobs.
func TestAzureFileInfo_Name_RootLevel(t *testing.T) {
	info := NewAzureFileInfoFromSize("file.csv", 512, time.Time{})

	assert.Equal(t, "file.csv", info.Name())
}

// TestAzureDirEntry tests the AzureDirEntry implementation.
func TestAzureDirEntry(t *testing.T) {
	// Test regular file entry
	fileEntry := NewAzureDirEntry("file.csv", false, 512, time.Now())

	assert.Equal(t, "file.csv", fileEntry.Name())
	assert.False(t, fileEntry.IsDir())
	assert.Equal(t, FileMode(0), fileEntry.Type())

	fileInfo, err := fileEntry.Info()
	require.NoError(t, err)
	assert.Equal(t, int64(512), fileInfo.Size())

	// Test directory entry
	dirEntry := NewAzureDirEntry("subdir", true, 0, time.Time{})

	assert.Equal(t, "subdir", dirEntry.Name())
	assert.True(t, dirEntry.IsDir())
	assert.Equal(t, ModeDir, dirEntry.Type())

	// Directory entry Info should still work
	dirInfo, err := dirEntry.Info()
	require.NoError(t, err)
	assert.NotNil(t, dirInfo)
}

// TestAzureFile_WriteMode tests AzureFile in write mode.
func TestAzureFile_WriteMode(t *testing.T) {
	// Create a file in write mode (nil client is okay for testing buffer operations)
	f := newAzureFileForWriting(nil, "test-container", "test-blob")

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

// TestAzureFile_WriteAt tests WriteAt functionality.
func TestAzureFile_WriteAt(t *testing.T) {
	f := newAzureFileForWriting(nil, "test-container", "test-blob")

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

// TestAzureFile_ReadModeError tests that writing in read mode returns an error.
func TestAzureFile_ReadModeError(t *testing.T) {
	f := newAzureFileForReading(nil, "test-container", "test-blob")

	// Attempting to write in read mode should fail
	_, err := f.Write([]byte("test"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot write in read mode")

	// WriteAt should also fail
	_, err = f.WriteAt([]byte("test"), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot write in read mode")
}

// TestAzureFile_SeekWhence tests different seek whence values.
func TestAzureFile_SeekWhence(t *testing.T) {
	// Create a file in read mode (seek is supported)
	f := newAzureFileForReading(nil, "test-container", "test-blob")

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

// TestAzureFile_ReadAt_EmptyBuffer tests ReadAt with empty buffer.
func TestAzureFile_ReadAt_EmptyBuffer(t *testing.T) {
	f := newAzureFileForReading(nil, "test-container", "test-blob")

	// ReadAt with empty buffer should return immediately
	emptyBuf := make([]byte, 0)
	n, err := f.ReadAt(emptyBuf, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// TestAzureFile_Close_ReadMode tests closing a file in read mode.
func TestAzureFile_Close_ReadMode(t *testing.T) {
	f := newAzureFileForReading(nil, "test-container", "test-blob")

	// Close should succeed even without a client
	err := f.Close()
	require.NoError(t, err)
}

// TestAzureFile_Close_WriteMode_EmptyBuffer tests closing with empty write buffer.
func TestAzureFile_Close_WriteMode_EmptyBuffer(t *testing.T) {
	f := newAzureFileForWriting(nil, "test-container", "test-blob")

	// Close with empty buffer should succeed without upload
	err := f.Close()
	require.NoError(t, err)
}

// TestFactoryAzureSchemeRegistration tests that Azure schemes are registered with the factory.
func TestFactoryAzureSchemeRegistration(t *testing.T) {
	factory := NewFileSystemFactory()

	schemes := factory.SupportedSchemes()

	// Check that Azure schemes are registered
	assert.Contains(t, schemes, "azure")
	assert.Contains(t, schemes, "az")
	assert.Contains(t, schemes, "file")
}

// TestIsCloudURL_Azure tests cloud URL detection for Azure URLs.
func TestIsCloudURL_Azure(t *testing.T) {
	tests := []struct {
		url      string
		expected bool
	}{
		{"azure://container/blob", true},
		{"az://container/blob", true},
		{"AZURE://CONTAINER/BLOB", true},
		{"AZ://CONTAINER/BLOB", true},
		{"s3://bucket/key", true},
		{"gs://bucket/object", true},
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

// MockAzureServer creates a mock Azure Blob Storage server for testing.
type MockAzureServer struct {
	*httptest.Server
	Blobs map[string][]byte
}

// NewMockAzureServer creates a new mock Azure server.
func NewMockAzureServer() *MockAzureServer {
	mock := &MockAzureServer{
		Blobs: make(map[string][]byte),
	}

	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		mock.handleRequest(w, r, key)
	}))

	return mock
}

func (m *MockAzureServer) handleRequest(w http.ResponseWriter, r *http.Request, key string) {
	switch r.Method {
	case http.MethodGet:
		m.handleGet(w, r, key)
	case http.MethodPut:
		m.handlePut(w, r, key)
	case http.MethodHead:
		m.handleHead(w, key)
	case http.MethodDelete:
		delete(m.Blobs, key)
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (m *MockAzureServer) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	// Check if this is a list operation
	if strings.Contains(r.URL.RawQuery, "restype=container") &&
		strings.Contains(r.URL.RawQuery, "comp=list") {
		m.handleListBlobs(w, r)

		return
	}

	data, ok := m.Blobs[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<?xml version="1.0"?><Error><Code>BlobNotFound</Code></Error>`))

		return
	}

	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		w.Header().Set("Content-Range", rangeHeader)
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	_, _ = w.Write(data)
}

func (m *MockAzureServer) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, _ := io.ReadAll(r.Body)
	m.Blobs[key] = body
	w.WriteHeader(http.StatusCreated)
}

func (m *MockAzureServer) handleHead(w http.ResponseWriter, key string) {
	data, ok := m.Blobs[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	w.Header().Set("Content-Length", string(rune(len(data))))
	w.Header().Set("Last-Modified", time.Now().Format(http.TimeFormat))
	w.Header().Set("x-ms-blob-content-length", string(rune(len(data))))
	w.WriteHeader(http.StatusOK)
}

func (m *MockAzureServer) handleListBlobs(w http.ResponseWriter, _ *http.Request) {
	type blobItem struct {
		Name string `json:"Name"`
	}

	type blobList struct {
		Blobs []blobItem `json:"Blobs"`
	}

	list := blobList{
		Blobs: make([]blobItem, 0, len(m.Blobs)),
	}
	for name := range m.Blobs {
		list.Blobs = append(list.Blobs, blobItem{Name: name})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(list)
}

// TestAzureFileSystem_GetConfig tests getting the config from the filesystem.
func TestAzureFileSystem_GetConfig(t *testing.T) {
	config := AzureConfig{
		AccountName: "mystorageaccount",
		Container:   "mycontainer",
		Timeout:     45 * time.Second,
	}

	fs := NewAzureFileSystemWithClient(nil, &config)
	retrieved := fs.GetConfig()

	assert.Equal(t, "mystorageaccount", retrieved.AccountName)
	assert.Equal(t, "mycontainer", retrieved.Container)
	assert.Equal(t, 45*time.Second, retrieved.Timeout)
}

// TestStripAzurePrefix tests the prefix stripping function.
func TestStripAzurePrefix(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"azure://container/blob", "container/blob"},
		{"az://container/blob", "container/blob"},
		{"AZURE://container/blob", "container/blob"},
		{"AZ://container/blob", "container/blob"},
		{"container/blob", "container/blob"},
		{"s3://bucket/key", "s3://bucket/key"}, // Not an Azure prefix
		{"gs://bucket/object", "gs://bucket/object"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := stripAzurePrefix(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestAzureFile_WriteContext tests WriteContext with context cancellation.
func TestAzureFile_WriteContext(t *testing.T) {
	f := newAzureFileForWriting(nil, "test-container", "test-blob")

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

// TestAzureFileSystem_OpenContext_Cancelled tests OpenContext with cancelled context.
func TestAzureFileSystem_OpenContext_Cancelled(t *testing.T) {
	fs := &AzureFileSystem{
		client: nil,
		config: DefaultAzureConfig(),
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := fs.OpenContext(cancelledCtx, "azure://container/blob")
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestAzureFileSystem_CreateContext_Cancelled tests CreateContext with cancelled context.
func TestAzureFileSystem_CreateContext_Cancelled(t *testing.T) {
	fs := &AzureFileSystem{
		client: nil,
		config: DefaultAzureConfig(),
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := fs.CreateContext(cancelledCtx, "azure://container/blob")
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestAzureFileSystem_Close tests the Close method.
func TestAzureFileSystem_Close(t *testing.T) {
	fs := &AzureFileSystem{
		client: nil,
		config: DefaultAzureConfig(),
	}

	// Close should be a no-op and succeed
	err := fs.Close()
	require.NoError(t, err)
}

// TestBuildAzureServiceURL tests the Azure service URL building.
func TestBuildAzureServiceURL(t *testing.T) {
	tests := []struct {
		name     string
		config   AzureConfig
		expected string
	}{
		{
			name: "default with account name",
			config: AzureConfig{
				AccountName: "mystorageaccount",
				UseHTTPS:    true,
			},
			expected: "https://mystorageaccount.blob.core.windows.net/",
		},
		{
			name: "http with account name",
			config: AzureConfig{
				AccountName: "mystorageaccount",
				UseHTTPS:    false,
			},
			expected: "http://mystorageaccount.blob.core.windows.net/",
		},
		{
			name: "custom endpoint",
			config: AzureConfig{
				AccountName: "mystorageaccount",
				Endpoint:    "http://127.0.0.1:10000/devstoreaccount1",
				UseHTTPS:    true,
			},
			expected: "http://127.0.0.1:10000/devstoreaccount1",
		},
		{
			name: "no account name",
			config: AzureConfig{
				UseHTTPS: true,
			},
			expected: "https://localhost/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildAzureServiceURL(&tt.config)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestNewAzureFileSystemWithClient tests creating AzureFileSystem with a pre-configured client.
func TestNewAzureFileSystemWithClient(t *testing.T) {
	config := &AzureConfig{
		AccountName: "teststorage",
		Container:   "testcontainer",
	}

	// Create with nil client (for testing)
	fs := NewAzureFileSystemWithClient(nil, config)

	assert.NotNil(t, fs)
	assert.Equal(t, "teststorage", fs.config.AccountName)
	assert.Equal(t, "testcontainer", fs.config.Container)
	assert.Equal(t, "azure://", fs.URI())
}

// Verify interface implementations at compile time.
var (
	_ FileSystem        = (*AzureFileSystem)(nil)
	_ ContextFileSystem = (*AzureFileSystem)(nil)
	_ File              = (*AzureFile)(nil)
	_ FileInfo          = (*AzureFileInfo)(nil)
	_ DirEntry          = (*AzureDirEntry)(nil)
)
