// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTableFunctionCSVWithHTTP tests reading CSV from HTTP URLs.
func TestTableFunctionCSVWithHTTP(t *testing.T) {
	// Create a test HTTP server
	csvData := `id,name,value
1,alice,100
2,bob,200
3,charlie,300`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(csvData)))
		_, _ = w.Write([]byte(csvData))
	}))
	defer server.Close()

	// Test that the URL is recognized as a cloud URL
	assert.True(t, filesystem.IsCloudURL(server.URL+"/data.csv"))
	assert.False(t, filesystem.IsLocalURL(server.URL+"/data.csv"))
}

// TestTableFunctionJSONWithHTTP tests reading JSON from HTTP URLs.
func TestTableFunctionJSONWithHTTP(t *testing.T) {
	// Create a test HTTP server
	jsonData := `[
		{"id": 1, "name": "alice", "value": 100},
		{"id": 2, "name": "bob", "value": 200},
		{"id": 3, "name": "charlie", "value": 300}
	]`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(jsonData)))
		_, _ = w.Write([]byte(jsonData))
	}))
	defer server.Close()

	// Test that the URL is recognized as a cloud URL
	assert.True(t, filesystem.IsCloudURL(server.URL+"/data.json"))
	assert.False(t, filesystem.IsLocalURL(server.URL+"/data.json"))
}

// TestTableFunctionNDJSONWithHTTP tests reading NDJSON from HTTP URLs.
func TestTableFunctionNDJSONWithHTTP(t *testing.T) {
	// Create a test HTTP server
	ndjsonData := `{"id": 1, "name": "alice", "value": 100}
{"id": 2, "name": "bob", "value": 200}
{"id": 3, "name": "charlie", "value": 300}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(ndjsonData)))
		_, _ = w.Write([]byte(ndjsonData))
	}))
	defer server.Close()

	// Test that the URL is recognized as a cloud URL
	assert.True(t, filesystem.IsCloudURL(server.URL+"/data.ndjson"))
	assert.False(t, filesystem.IsLocalURL(server.URL+"/data.ndjson"))
}

// TestTableFunctionIsCloudURL tests the cloud URL detection.
func TestTableFunctionIsCloudURL(t *testing.T) {
	tests := []struct {
		url     string
		isCloud bool
		isLocal bool
	}{
		// Cloud URLs
		{"s3://bucket/path/file.csv", true, false},
		{"s3a://bucket/path/file.csv", true, false},
		{"s3n://bucket/path/file.csv", true, false},
		{"gs://bucket/path/file.parquet", true, false},
		{"gcs://bucket/path/file.parquet", true, false},
		{"azure://container/path/file.json", true, false},
		{"az://container/path/file.json", true, false},
		{"http://example.com/data.csv", true, false},
		{"https://example.com/data.csv", true, false},

		// Local URLs
		{"/path/to/file.csv", false, true},
		{"./relative/path/file.csv", false, true},
		{"file:///absolute/path/file.csv", false, true},
		{"file://relative/path/file.csv", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			assert.Equal(t, tt.isCloud, filesystem.IsCloudURL(tt.url), "IsCloudURL mismatch")
			assert.Equal(t, tt.isLocal, filesystem.IsLocalURL(tt.url), "IsLocalURL mismatch")
		})
	}
}

// TestGlobPatternSplit tests splitting glob patterns.
func TestGlobPatternSplit(t *testing.T) {
	tests := []struct {
		pattern     string
		wantBase    string
		wantPattern string
	}{
		// S3 patterns
		{"s3://bucket/data/*.csv", "s3://bucket/data/", "*.csv"},
		{"s3://bucket/data/file.csv", "s3://bucket/data/file.csv", ""},
		{"s3://bucket/data/**/*.parquet", "s3://bucket/data/", "**/*.parquet"},
		{"s3://bucket/*.json", "s3://bucket/", "*.json"},

		// HTTP patterns
		{"https://example.com/data/*.csv", "https://example.com/data/", "*.csv"},
		{"https://example.com/files/**/*.json", "https://example.com/files/", "**/*.json"},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			base, pattern := splitGlobPattern(tt.pattern)
			assert.Equal(t, tt.wantBase, base, "base path mismatch")
			assert.Equal(t, tt.wantPattern, pattern, "pattern mismatch")
		})
	}
}

// TestGlobPatternMatch tests glob pattern matching.
func TestGlobPatternMatch(t *testing.T) {
	tests := []struct {
		pattern string
		input   string
		want    bool
	}{
		// Simple wildcard
		{"*.csv", "data.csv", true},
		{"*.csv", "data.parquet", false},
		{"*.csv", "subdir/data.csv", false},

		// Single character wildcard
		{"file?.csv", "file1.csv", true},
		{"file?.csv", "file12.csv", false},
		{"file?.csv", "file.csv", false},

		// Complex patterns
		{"data_*.csv", "data_2024.csv", true},
		{"data_*.csv", "data_.csv", true},
		{"data_*.csv", "other.csv", false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s matches %s", tt.pattern, tt.input), func(t *testing.T) {
			got := matchGlobPattern(tt.pattern, tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestHasGlobPattern tests detection of glob patterns.
func TestHasGlobPattern(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"file.csv", false},
		{"path/to/file.csv", false},
		{"*.csv", true},
		{"file*.csv", true},
		{"file?.csv", true},
		{"file[0-9].csv", true},
		{"path/*.csv", true},
		{"path/**/file.csv", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := hasGlobPattern(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestGlobMatcherExpandLocalGlob tests expanding local globs.
func TestGlobMatcherExpandLocalGlob(t *testing.T) {
	// Create a temporary directory with test files
	tmpDir, err := os.MkdirTemp("", "glob_test")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create test files
	testFiles := []string{
		"data1.csv",
		"data2.csv",
		"info.json",
		"readme.txt",
	}
	for _, f := range testFiles {
		err := os.WriteFile(filepath.Join(tmpDir, f), []byte("test"), 0644)
		require.NoError(t, err)
	}

	// Test glob expansion with no wildcards
	matcher := NewGlobMatcher(nil)
	matches, err := matcher.ExpandGlob(context.Background(), filepath.Join(tmpDir, "data1.csv"))
	require.NoError(t, err)
	assert.Len(t, matches, 1)
}

// TestFileSystemProviderGetFileSystem tests the filesystem provider.
func TestFileSystemProviderGetFileSystem(t *testing.T) {
	provider := NewFileSystemProvider(nil)

	tests := []struct {
		url       string
		wantError bool
	}{
		// Local filesystem should work
		{"/path/to/file.csv", false},
		{"./relative/path.csv", false},

		// HTTP should work (creates the filesystem)
		{"https://example.com/data.csv", false},
		{"http://example.com/data.csv", false},

		// S3 should work (creates the filesystem)
		{"s3://bucket/key.csv", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			fs, err := provider.GetFileSystem(context.Background(), tt.url)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, fs)
			}
		})
	}
}

// TestTableFunctionBytesReaderAt tests the bytesReaderAt implementation.
func TestTableFunctionBytesReaderAt(t *testing.T) {
	data := []byte("hello world")
	br := &bytesReaderAt{data: data}

	// Test Read
	buf := make([]byte, 5)
	n, err := br.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(buf))

	// Test ReadAt
	buf = make([]byte, 5)
	n, err = br.ReadAt(buf, 6)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "world", string(buf))

	// Test Seek
	offset, err := br.Seek(0, 0) // SeekStart
	require.NoError(t, err)
	assert.Equal(t, int64(0), offset)

	// Test Seek from current
	_, _ = br.Read(make([]byte, 3)) // Read 3 bytes
	offset, err = br.Seek(2, 1)     // SeekCurrent
	require.NoError(t, err)
	assert.Equal(t, int64(5), offset)

	// Test Seek from end
	offset, err = br.Seek(-5, 2) // SeekEnd
	require.NoError(t, err)
	assert.Equal(t, int64(6), offset)

	// Test Close (no-op)
	err = br.Close()
	require.NoError(t, err)
}

// TestOpenFileForReading tests opening files for reading.
func TestOpenFileForReading(t *testing.T) {
	// Create a temporary test file
	tmpFile, err := os.CreateTemp("", "test*.csv")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.WriteString("id,name\n1,alice\n2,bob")
	require.NoError(t, err)
	_ = tmpFile.Close()

	// Create an executor
	exec := &Executor{}

	// Test opening local file
	file, err := exec.openFileForReading(context.Background(), tmpFile.Name())
	require.NoError(t, err)
	require.NotNil(t, file)
	_ = file.Close()
}
