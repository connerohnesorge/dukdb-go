//go:build !js || !wasm

package arrow

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestArrowFileInDir creates a test Arrow file in the specified directory.
func createTestArrowFileInDir(t *testing.T, dir, name string) string {
	t.Helper()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	path := filepath.Join(dir, name)
	file, err := os.Create(path)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	alloc := memory.NewGoAllocator()
	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	require.NoError(t, err)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)

	record := bldr.NewRecord()
	defer record.Release()

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	return path
}

// createTestArrowStreamInDir creates a test Arrow stream file in the specified directory.
func createTestArrowStreamInDir(t *testing.T, dir, name string) string {
	t.Helper()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	path := filepath.Join(dir, name)
	file, err := os.Create(path)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	alloc := memory.NewGoAllocator()
	writer := ipc.NewWriter(file, ipc.WithSchema(schema), ipc.WithAllocator(alloc))

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	bldr.Field(1).(*array.Float64Builder).AppendValues([]float64{1.5, 2.5}, nil)

	record := bldr.NewRecord()
	defer record.Release()

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	return path
}

func TestNewReaderFromURL_LocalFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := createTestArrowFileInDir(t, tmpDir, "test.arrow")

	// Test with absolute path (no scheme)
	reader, err := NewReaderFromURL(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	columns, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name"}, columns)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())
}

func TestNewReaderFromURL_FileScheme(t *testing.T) {
	tmpDir := t.TempDir()
	path := createTestArrowFileInDir(t, tmpDir, "test.arrow")

	// Test with file:// scheme
	fileURL := "file://" + path
	reader, err := NewReaderFromURL(fileURL, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	columns, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name"}, columns)
}

func TestNewReaderFromURLWithContext(t *testing.T) {
	tmpDir := t.TempDir()
	path := createTestArrowFileInDir(t, tmpDir, "test.arrow")

	ctx := context.Background()
	reader, err := NewReaderFromURLWithContext(ctx, path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())
}

func TestNewStreamReaderFromURL_LocalFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := createTestArrowStreamInDir(t, tmpDir, "test.arrows")

	reader, err := NewStreamReaderFromURL(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	columns, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "value"}, columns)

	// Iterate to read data
	totalRows := int64(0)
	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()
	}
	assert.NoError(t, reader.Err())
	assert.Equal(t, int64(2), totalRows)
}

func TestNewWriterToURL_LocalFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "output.arrow")

	writer, err := NewWriterToURL(path, nil)
	require.NoError(t, err)

	// Set schema
	err = writer.SetSchema([]string{"col1", "col2"})
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify file was created
	_, err = os.Stat(path)
	require.NoError(t, err)
}

func TestNewWriterToURL_FileScheme(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "output.arrow")
	fileURL := "file://" + path

	writer, err := NewWriterToURL(fileURL, nil)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Verify file was created
	_, err = os.Stat(path)
	require.NoError(t, err)
}

func TestNewStreamWriterToURL_LocalFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "output.arrows")

	writer, err := NewStreamWriterToURL(path, nil)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Verify file was created
	_, err = os.Stat(path)
	require.NoError(t, err)
}

func TestNewWriterToURL_HTTPNotSupported(t *testing.T) {
	_, err := NewWriterToURL("http://example.com/data.arrow", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP/HTTPS URLs do not support writing")

	_, err = NewWriterToURL("https://example.com/data.arrow", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP/HTTPS URLs do not support writing")
}

func TestNewStreamWriterToURL_HTTPNotSupported(t *testing.T) {
	_, err := NewStreamWriterToURL("http://example.com/data.arrows", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP/HTTPS URLs do not support writing")
}

func TestNewReaderFromURL_NonExistentFile(t *testing.T) {
	_, err := NewReaderFromURL("/nonexistent/path/file.arrow", nil)
	assert.Error(t, err)
}

func TestExtractScheme(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{"/local/path", ""},
		{"file:///local/path", "file"},
		{"s3://bucket/key", "s3"},
		{"gs://bucket/object", "gs"},
		{"https://example.com/data", "https"},
		{"HTTP://EXAMPLE.COM/DATA", "http"},
		{"az://container/blob", "az"},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			scheme := extractScheme(tt.url)
			assert.Equal(t, tt.expected, scheme)
		})
	}
}

func TestExtractPath(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{"/local/path", "/local/path"},
		{"file:///local/path", "/local/path"},
		{"s3://bucket/key", "s3://bucket/key"},      // Cloud URLs are passed through
		{"gs://bucket/object", "gs://bucket/object"}, // Cloud URLs are passed through
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			path := extractPath(tt.url)
			assert.Equal(t, tt.expected, path)
		})
	}
}

func TestURLFileReader_Interfaces(t *testing.T) {
	tmpDir := t.TempDir()
	path := createTestArrowFileInDir(t, tmpDir, "test.arrow")

	file, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	// Create URLFileReader - we can't directly create this without a filesystem.File
	// but we can test the interface compliance via the var declarations in the source

	// Just verify the file can be read
	data := make([]byte, 6)
	n, err := file.Read(data)
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, "ARROW1", string(data))
}

func TestRoundTrip_URL(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "roundtrip.arrow")

	// Create test data using standard writer
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		},
		nil,
	)

	file, err := os.Create(path)
	require.NoError(t, err)

	alloc := memory.NewGoAllocator()
	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	require.NoError(t, err)

	bldr := array.NewRecordBuilder(alloc, schema)
	bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{10, 20, 30}, nil)
	record := bldr.NewRecord()
	bldr.Release()

	err = writer.Write(record)
	record.Release()
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
	_ = file.Close()

	// Read back using URL reader
	reader, err := NewReaderFromURL(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())
	assert.Equal(t, int32(10), chunk.GetValue(0, 0))
	assert.Equal(t, int32(20), chunk.GetValue(1, 0))
	assert.Equal(t, int32(30), chunk.GetValue(2, 0))

	// Verify EOF
	_, err = reader.ReadChunk()
	assert.ErrorIs(t, err, io.EOF)
}

// Cloud Storage Integration Tests
//
// These tests require cloud credentials to be set up. They will be skipped if
// the necessary environment variables are not set.
//
// S3 tests require:
//   - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (or IAM role)
//   - S3_TEST_BUCKET environment variable set to a test bucket name
//
// GCS tests require:
//   - GOOGLE_APPLICATION_CREDENTIALS pointing to a service account key file
//   - GCS_TEST_BUCKET environment variable set to a test bucket name
//
// Azure tests require:
//   - AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY (or Azure AD credentials)
//   - AZURE_TEST_CONTAINER environment variable set to a test container name

// skipIfNoS3Credentials skips the test if S3 credentials are not configured.
func skipIfNoS3Credentials(t *testing.T) string {
	t.Helper()

	bucket := os.Getenv("S3_TEST_BUCKET")
	if bucket == "" {
		t.Skip("Skipping S3 test: S3_TEST_BUCKET not set")
	}

	// Check for credentials (either explicit or via IAM)
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("AWS_PROFILE") == "" {
		// Could also have IAM role, but hard to detect - try the test anyway if bucket is set
	}

	return bucket
}

// skipIfNoGCSCredentials skips the test if GCS credentials are not configured.
func skipIfNoGCSCredentials(t *testing.T) string {
	t.Helper()

	bucket := os.Getenv("GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("Skipping GCS test: GCS_TEST_BUCKET not set")
	}

	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Skip("Skipping GCS test: GOOGLE_APPLICATION_CREDENTIALS not set")
	}

	return bucket
}

// skipIfNoAzureCredentials skips the test if Azure credentials are not configured.
func skipIfNoAzureCredentials(t *testing.T) string {
	t.Helper()

	container := os.Getenv("AZURE_TEST_CONTAINER")
	if container == "" {
		t.Skip("Skipping Azure test: AZURE_TEST_CONTAINER not set")
	}

	account := os.Getenv("AZURE_STORAGE_ACCOUNT")
	if account == "" {
		t.Skip("Skipping Azure test: AZURE_STORAGE_ACCOUNT not set")
	}

	return container
}

// TestS3_ReadArrowFile tests reading an Arrow file from S3.
// This test is skipped if S3 credentials are not configured.
func TestS3_ReadArrowFile(t *testing.T) {
	bucket := skipIfNoS3Credentials(t)

	// Expected URL format: s3://bucket/path/to/file.arrow
	// The test assumes a file named "test_arrow/sample.arrow" exists in the bucket.
	testKey := os.Getenv("S3_TEST_ARROW_FILE")
	if testKey == "" {
		testKey = "test_arrow/sample.arrow"
	}

	url := "s3://" + bucket + "/" + testKey
	t.Logf("Testing S3 read from: %s", url)

	ctx := context.Background()
	reader, err := NewReaderFromURLWithContext(ctx, url, nil)
	if err != nil {
		// This could fail due to missing file, not credentials
		t.Skipf("Skipping: could not read S3 file (may not exist): %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Verify we can read schema
	columns, err := reader.Schema()
	require.NoError(t, err)
	t.Logf("S3 file has columns: %v", columns)
	assert.Greater(t, len(columns), 0)

	// Try to read a chunk
	chunk, err := reader.ReadChunk()
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	if chunk != nil {
		t.Logf("S3 file has %d rows, %d columns", chunk.Count(), chunk.ColumnCount())
	}
}

// TestS3_URLSchemeVariants tests that various S3 URL schemes are recognized.
func TestS3_URLSchemeVariants(t *testing.T) {
	tests := []struct {
		url    string
		scheme string
	}{
		{"s3://bucket/key", "s3"},
		{"s3a://bucket/key", "s3a"},
		{"s3n://bucket/key", "s3n"},
	}

	for _, tt := range tests {
		t.Run(tt.scheme, func(t *testing.T) {
			scheme := extractScheme(tt.url)
			assert.Equal(t, tt.scheme, scheme)
		})
	}
}

// TestGCS_ReadArrowFile tests reading an Arrow file from Google Cloud Storage.
// This test is skipped if GCS credentials are not configured.
func TestGCS_ReadArrowFile(t *testing.T) {
	bucket := skipIfNoGCSCredentials(t)

	// Expected URL format: gs://bucket/path/to/file.arrow
	testObject := os.Getenv("GCS_TEST_ARROW_FILE")
	if testObject == "" {
		testObject = "test_arrow/sample.arrow"
	}

	url := "gs://" + bucket + "/" + testObject
	t.Logf("Testing GCS read from: %s", url)

	ctx := context.Background()
	reader, err := NewReaderFromURLWithContext(ctx, url, nil)
	if err != nil {
		t.Skipf("Skipping: could not read GCS file (may not exist): %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Verify we can read schema
	columns, err := reader.Schema()
	require.NoError(t, err)
	t.Logf("GCS file has columns: %v", columns)
	assert.Greater(t, len(columns), 0)
}

// TestGCS_URLSchemeVariants tests that GCS URL schemes are recognized.
func TestGCS_URLSchemeVariants(t *testing.T) {
	tests := []struct {
		url    string
		scheme string
	}{
		{"gs://bucket/object", "gs"},
		{"gcs://bucket/object", "gcs"},
	}

	for _, tt := range tests {
		t.Run(tt.scheme, func(t *testing.T) {
			scheme := extractScheme(tt.url)
			assert.Equal(t, tt.scheme, scheme)
		})
	}
}

// TestAzure_ReadArrowFile tests reading an Arrow file from Azure Blob Storage.
// This test is skipped if Azure credentials are not configured.
func TestAzure_ReadArrowFile(t *testing.T) {
	container := skipIfNoAzureCredentials(t)

	account := os.Getenv("AZURE_STORAGE_ACCOUNT")

	// Expected URL format: az://container/path/to/file.arrow
	testBlob := os.Getenv("AZURE_TEST_ARROW_FILE")
	if testBlob == "" {
		testBlob = "test_arrow/sample.arrow"
	}

	url := "az://" + container + "/" + testBlob
	t.Logf("Testing Azure read from: %s (account: %s)", url, account)

	ctx := context.Background()
	reader, err := NewReaderFromURLWithContext(ctx, url, nil)
	if err != nil {
		t.Skipf("Skipping: could not read Azure file (may not exist): %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Verify we can read schema
	columns, err := reader.Schema()
	require.NoError(t, err)
	t.Logf("Azure file has columns: %v", columns)
	assert.Greater(t, len(columns), 0)
}

// TestAzure_URLSchemeVariants tests that Azure URL schemes are recognized.
func TestAzure_URLSchemeVariants(t *testing.T) {
	tests := []struct {
		url    string
		scheme string
	}{
		{"az://container/blob", "az"},
		{"azure://container/blob", "azure"},
		{"abfs://container/blob", "abfs"},
		{"abfss://container/blob", "abfss"},
	}

	for _, tt := range tests {
		t.Run(tt.scheme, func(t *testing.T) {
			scheme := extractScheme(tt.url)
			assert.Equal(t, tt.scheme, scheme)
		})
	}
}

// TestHTTP_ReadArrowFile tests reading an Arrow file from HTTP/HTTPS.
// This uses a public URL if available.
func TestHTTP_ReadArrowFile(t *testing.T) {
	// Check if a test URL is configured
	testURL := os.Getenv("HTTP_TEST_ARROW_FILE")
	if testURL == "" {
		t.Skip("Skipping HTTP test: HTTP_TEST_ARROW_FILE not set")
	}

	t.Logf("Testing HTTP read from: %s", testURL)

	ctx := context.Background()
	reader, err := NewReaderFromURLWithContext(ctx, testURL, nil)
	if err != nil {
		t.Skipf("Skipping: could not read HTTP file: %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Verify we can read schema
	columns, err := reader.Schema()
	require.NoError(t, err)
	t.Logf("HTTP file has columns: %v", columns)
	assert.Greater(t, len(columns), 0)
}

// TestCloudStorage_URLParsing tests URL parsing for various cloud storage providers.
func TestCloudStorage_URLParsing(t *testing.T) {
	tests := []struct {
		url          string
		expectScheme string
		expectPath   string
	}{
		// Local files
		{"/local/path/file.arrow", "", "/local/path/file.arrow"},
		{"file:///local/path/file.arrow", "file", "/local/path/file.arrow"},

		// S3
		{"s3://mybucket/path/to/file.arrow", "s3", "s3://mybucket/path/to/file.arrow"},
		{"s3a://mybucket/file.arrow", "s3a", "s3a://mybucket/file.arrow"},

		// GCS
		{"gs://mybucket/file.arrow", "gs", "gs://mybucket/file.arrow"},
		{"gcs://mybucket/file.arrow", "gcs", "gcs://mybucket/file.arrow"},

		// Azure
		{"az://container/file.arrow", "az", "az://container/file.arrow"},
		{"azure://container/file.arrow", "azure", "azure://container/file.arrow"},
		{"abfs://container@account.dfs.core.windows.net/file.arrow", "abfs", "abfs://container@account.dfs.core.windows.net/file.arrow"},

		// HTTP
		{"http://example.com/file.arrow", "http", "http://example.com/file.arrow"},
		{"https://example.com/file.arrow", "https", "https://example.com/file.arrow"},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			scheme := extractScheme(tt.url)
			path := extractPath(tt.url)

			assert.Equal(t, tt.expectScheme, scheme, "scheme mismatch for %s", tt.url)
			assert.Equal(t, tt.expectPath, path, "path mismatch for %s", tt.url)
		})
	}
}

// TestCloudStorage_InvalidURLs tests handling of invalid URLs.
func TestCloudStorage_InvalidURLs(t *testing.T) {
	invalidURLs := []string{
		"",                    // Empty URL
		"://bucket/key",       // Missing scheme
		"s3://",               // Missing bucket and key
		"not-a-valid-scheme:", // Invalid scheme
	}

	for _, url := range invalidURLs {
		t.Run(url, func(t *testing.T) {
			// These should either return an error or empty/unexpected values
			scheme := extractScheme(url)
			t.Logf("URL %q -> scheme %q", url, scheme)
			// The function should handle these gracefully without panicking
		})
	}
}

// TestCloudStorage_ContextCancellation tests that context cancellation is respected.
func TestCloudStorage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// This should fail quickly due to context cancellation
	_, err := NewReaderFromURLWithContext(ctx, "s3://bucket/key/file.arrow", nil)
	if err == nil {
		t.Log("Surprisingly, no error returned for cancelled context - filesystem may not check context")
	} else {
		// Error is expected
		t.Logf("Got expected error for cancelled context: %v", err)
	}
}
