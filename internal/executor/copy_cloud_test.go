// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/secret"
)

// TestIsCloudURL tests the IsCloudURL helper function.
func TestIsCloudURL(t *testing.T) {
	tests := []struct {
		url      string
		expected bool
	}{
		// Cloud URLs
		{"s3://bucket/key.csv", true},
		{"s3a://bucket/key.csv", true},
		{"s3n://bucket/key.csv", true},
		{"gs://bucket/key.csv", true},
		{"gcs://bucket/key.csv", true},
		{"azure://account/container/key.csv", true},
		{"az://account/container/key.csv", true},
		{"http://example.com/file.csv", true},
		{"https://example.com/file.csv", true},
		// Note: HuggingFace URLs are not currently recognized as cloud URLs
		// because the HuggingFace filesystem is not implemented yet
		// {"hf://datasets/user/repo/file.csv", true},

		// Local URLs
		{"/path/to/file.csv", false},
		{"./relative/path.csv", false},
		{"file:///path/to/file.csv", false},
		{"C:\\Windows\\file.csv", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			result := IsCloudURL(tt.url)
			assert.Equal(t, tt.expected, result, "IsCloudURL(%s)", tt.url)
		})
	}
}

// TestIsLocalURL tests the IsLocalURL helper function.
func TestIsLocalURL(t *testing.T) {
	tests := []struct {
		url      string
		expected bool
	}{
		// Local URLs
		{"/path/to/file.csv", true},
		{"./relative/path.csv", true},
		{"file:///path/to/file.csv", true},

		// Cloud URLs
		{"s3://bucket/key.csv", false},
		{"gs://bucket/key.csv", false},
		{"http://example.com/file.csv", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			result := IsLocalURL(tt.url)
			assert.Equal(t, tt.expected, result, "IsLocalURL(%s)", tt.url)
		})
	}
}

// TestFileSystemProviderLocal tests that FileSystemProvider handles local files.
func TestFileSystemProviderLocal(t *testing.T) {
	provider := NewFileSystemProvider(nil)
	require.NotNil(t, provider)

	ctx := context.Background()

	// Get filesystem for a local path
	fs, err := provider.GetFileSystem(ctx, "/some/local/path.csv")
	require.NoError(t, err)
	require.NotNil(t, fs)

	// Should be a local filesystem
	_, ok := fs.(*filesystem.LocalFileSystem)
	assert.True(t, ok, "Expected LocalFileSystem, got %T", fs)
}

// TestFileSystemProviderS3 tests that FileSystemProvider handles S3 URLs.
func TestFileSystemProviderS3(t *testing.T) {
	// Skip if no AWS credentials (this is an integration test aspect)
	t.Skip("S3 integration test - requires AWS credentials")

	provider := NewFileSystemProvider(nil)
	require.NotNil(t, provider)

	ctx := context.Background()

	// Get filesystem for an S3 path
	fs, err := provider.GetFileSystem(ctx, "s3://test-bucket/test-key.csv")
	require.NoError(t, err)
	require.NotNil(t, fs)

	// Should be an S3 filesystem
	_, ok := fs.(*filesystem.S3FileSystem)
	assert.True(t, ok, "Expected S3FileSystem, got %T", fs)
}

// TestFileSystemProviderHTTP tests that FileSystemProvider handles HTTP URLs.
func TestFileSystemProviderHTTP(t *testing.T) {
	provider := NewFileSystemProvider(nil)
	require.NotNil(t, provider)

	ctx := context.Background()

	// Get filesystem for an HTTP path
	fs, err := provider.GetFileSystem(ctx, "https://example.com/data.csv")
	require.NoError(t, err)
	require.NotNil(t, fs)

	// Should be an HTTP filesystem
	_, ok := fs.(*filesystem.HTTPFileSystem)
	assert.True(t, ok, "Expected HTTPFileSystem, got %T", fs)
}

// TestFileSystemProviderWithSecrets tests that FileSystemProvider uses secrets.
func TestFileSystemProviderWithSecrets(t *testing.T) {
	// Create a secret manager (nil catalog for in-memory only)
	mgr := secret.NewManager(nil)
	ctx := context.Background()

	// Create an S3 secret
	err := mgr.CreateSecret(ctx, secret.Secret{
		Name:     "my_s3_secret",
		Type:     secret.SecretTypeS3,
		Provider: secret.ProviderConfig,
		Scope: secret.SecretScope{
			Type:   secret.ScopePath,
			Prefix: "s3://test-bucket/",
		},
		Options: map[string]string{
			secret.OptionKeyID:  "test-key-id",
			secret.OptionSecret: "test-secret",
			secret.OptionRegion: "us-east-1",
		},
	})
	require.NoError(t, err)

	provider := NewFileSystemProvider(mgr)
	require.NotNil(t, provider)

	// Verify the provider has the secret manager
	assert.NotNil(t, provider.secretManager)
}

// TestGetSecretTypeForScheme tests the secret type mapping.
func TestGetSecretTypeForScheme(t *testing.T) {
	tests := []struct {
		scheme   string
		expected secret.SecretType
	}{
		{"s3", secret.SecretTypeS3},
		{"s3a", secret.SecretTypeS3},
		{"s3n", secret.SecretTypeS3},
		{"gs", secret.SecretTypeGCS},
		{"gcs", secret.SecretTypeGCS},
		{"azure", secret.SecretTypeAzure},
		{"az", secret.SecretTypeAzure},
		{"http", secret.SecretTypeHTTP},
		{"https", secret.SecretTypeHTTP},
		{"hf", secret.SecretTypeHuggingFace},
		{"huggingface", secret.SecretTypeHuggingFace},
		{"unknown", ""},
		{"file", ""},
	}

	for _, tt := range tests {
		t.Run(tt.scheme, func(t *testing.T) {
			result := getSecretTypeForScheme(tt.scheme)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestBytesReaderAt tests the bytesReaderAt implementation.
func TestBytesReaderAt(t *testing.T) {
	data := []byte("Hello, World!")
	br := &bytesReaderAt{data: data}

	// Test Read
	buf := make([]byte, 5)
	n, err := br.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "Hello", string(buf))

	// Test ReadAt
	buf2 := make([]byte, 5)
	n, err = br.ReadAt(buf2, 7)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "World", string(buf2))

	// Test Seek
	offset, err := br.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), offset)

	offset, err = br.Seek(7, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(7), offset)

	// Read after seek
	buf3 := make([]byte, 6)
	n, err = br.Read(buf3)
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, "World!", string(buf3))

	// Test SeekEnd
	offset, err = br.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(12), offset)

	// Test Close (no-op)
	err = br.Close()
	require.NoError(t, err)
}

// TestBytesReaderAtEOF tests EOF handling.
func TestBytesReaderAtEOF(t *testing.T) {
	data := []byte("Short")
	br := &bytesReaderAt{data: data}

	// Seek to end
	_, err := br.Seek(0, io.SeekEnd)
	require.NoError(t, err)

	// Read should return EOF
	buf := make([]byte, 10)
	_, err = br.Read(buf)
	assert.Equal(t, io.EOF, err)

	// ReadAt at end should return EOF
	_, err = br.ReadAt(buf, int64(len(data)))
	assert.Equal(t, io.EOF, err)
}

// TestCreateFileReaderFromHTTP tests reading from HTTP URL.
func TestCreateFileReaderFromHTTP(t *testing.T) {
	// Create a test HTTP server
	csvContent := `id,name,value
1,Alice,100
2,Bob,200
`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Length", "42")
		_, _ = w.Write([]byte(csvContent))
	}))
	defer server.Close()

	provider := NewFileSystemProvider(nil)
	ctx := context.Background()

	// Create CSV reader from HTTP URL
	reader, err := createFileReaderFromFS(
		ctx,
		provider,
		server.URL+"/data.csv",
		fileio.FormatCSV,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer func() { _ = reader.Close() }()

	// Read schema
	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "value"}, schema)

	// Read data
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())
}

// TestCreateFileReaderFromHTTPJSON tests reading JSON from HTTP URL.
func TestCreateFileReaderFromHTTPJSON(t *testing.T) {
	// Create a test HTTP server
	jsonContent := `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jsonContent))
	}))
	defer server.Close()

	provider := NewFileSystemProvider(nil)
	ctx := context.Background()

	// Create JSON reader from HTTP URL
	reader, err := createFileReaderFromFS(
		ctx,
		provider,
		server.URL+"/data.json",
		fileio.FormatJSON,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer func() { _ = reader.Close() }()

	// Read data
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())
}

// TestCreateFileReaderFromHTTPNDJSON tests reading NDJSON from HTTP URL.
func TestCreateFileReaderFromHTTPNDJSON(t *testing.T) {
	// Create a test HTTP server
	ndjsonContent := `{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(ndjsonContent))
	}))
	defer server.Close()

	provider := NewFileSystemProvider(nil)
	ctx := context.Background()

	// Create NDJSON reader from HTTP URL
	reader, err := createFileReaderFromFS(
		ctx,
		provider,
		server.URL+"/data.ndjson",
		fileio.FormatNDJSON,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer func() { _ = reader.Close() }()

	// Read data
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())
}

// TestHTTPFileSystemReadOnly tests that HTTP filesystem is read-only for COPY TO.
func TestHTTPFileSystemReadOnly(t *testing.T) {
	provider := NewFileSystemProvider(nil)
	ctx := context.Background()

	// Trying to create a file on HTTP should fail
	_, err := provider.createFileForWriting(ctx, "https://example.com/output.csv")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not support writing")
}

// TestCloudURLParsing tests that cloud URLs are correctly identified.
func TestCloudURLParsing(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		isCloud bool
		isLocal bool
	}{
		{
			name:    "S3 URL",
			url:     "s3://my-bucket/path/to/file.csv",
			isCloud: true,
			isLocal: false,
		},
		{
			name:    "GCS URL",
			url:     "gs://my-bucket/path/to/file.csv",
			isCloud: true,
			isLocal: false,
		},
		{
			name:    "Azure URL",
			url:     "azure://account/container/file.csv",
			isCloud: true,
			isLocal: false,
		},
		{
			name:    "HTTP URL",
			url:     "http://example.com/data.csv",
			isCloud: true,
			isLocal: false,
		},
		{
			name:    "HTTPS URL",
			url:     "https://example.com/data.csv",
			isCloud: true,
			isLocal: false,
		},
		{
			name:    "Absolute path",
			url:     "/home/user/data.csv",
			isCloud: false,
			isLocal: true,
		},
		{
			name:    "Relative path",
			url:     "./data.csv",
			isCloud: false,
			isLocal: true,
		},
		{
			name:    "File URL",
			url:     "file:///home/user/data.csv",
			isCloud: false,
			isLocal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.isCloud, IsCloudURL(tt.url), "IsCloudURL(%s)", tt.url)
			assert.Equal(t, tt.isLocal, IsLocalURL(tt.url), "IsLocalURL(%s)", tt.url)
		})
	}
}

// TestFormatUnknownError tests error handling for unknown format.
func TestFormatUnknownError(t *testing.T) {
	provider := NewFileSystemProvider(nil)
	ctx := context.Background()

	// Create a test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Try to create reader with unknown format
	_, err := createFileReaderFromFS(
		ctx,
		provider,
		server.URL+"/data.xyz",
		fileio.FormatUnknown,
		nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown format")
}

// TestCreateWriterFormatUnknownError tests error handling for unknown format in writer.
func TestCreateWriterFormatUnknownError(t *testing.T) {
	provider := NewFileSystemProvider(nil)
	ctx := context.Background()

	// Create temp directory
	tempDir := t.TempDir()

	// Try to create writer with unknown format
	_, err := createFileWriterFromFS(
		ctx,
		provider,
		tempDir+"/output.xyz",
		fileio.FormatUnknown,
		nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown format")
}
