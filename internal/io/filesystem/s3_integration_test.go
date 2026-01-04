//go:build integration

// Package filesystem provides integration tests for S3 filesystem operations.
// These tests require either real AWS credentials or a LocalStack instance.
//
// To run these tests:
//   - With LocalStack: LOCALSTACK_ENDPOINT=localhost:4566 go test -tags=integration ./...
//   - With real AWS: AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=xxx go test -tags=integration ./...
package filesystem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration test configuration from environment variables.
var (
	testEndpoint  = os.Getenv("LOCALSTACK_ENDPOINT")
	testAccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	testSecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	testRegion    = os.Getenv("AWS_REGION")
	testBucket    = os.Getenv("S3_TEST_BUCKET")
)

// skipIfNoCredentials skips the test if no S3 credentials are available.
func skipIfNoCredentials(t *testing.T) {
	t.Helper()

	if testEndpoint == "" && (testAccessKey == "" || testSecretKey == "") {
		t.Skip("Skipping integration test: no S3 credentials or LocalStack endpoint configured")
	}
}

// getTestConfig returns an S3Config for integration testing.
func getTestConfig() S3Config {
	config := DefaultS3Config()

	if testEndpoint != "" {
		// LocalStack configuration
		config.Endpoint = testEndpoint
		config.UseSSL = false
		config.URLStyle = S3URLStylePath
		config.AccessKeyID = "test"
		config.SecretAccessKey = "test"
		config.Region = "us-east-1"
	} else {
		// Real AWS configuration
		config.AccessKeyID = testAccessKey
		config.SecretAccessKey = testSecretKey

		if testRegion != "" {
			config.Region = testRegion
		}
	}

	return config
}

// getTestBucket returns the bucket name for testing.
func getTestBucket() string {
	if testBucket != "" {
		return testBucket
	}

	return "dukdb-go-test-bucket"
}

// TestS3Integration_ReadWrite tests basic read/write operations.
func TestS3Integration_ReadWrite(t *testing.T) {
	skipIfNoCredentials(t)

	ctx := context.Background()
	config := getTestConfig()

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	bucket := getTestBucket()
	key := fmt.Sprintf("test/read-write-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Write data
	testData := []byte("Hello, S3 Integration Test!")

	file, err := fs.Create(path)
	require.NoError(t, err)

	n, err := file.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n)

	err = file.Close()
	require.NoError(t, err)

	// Read data back
	file, err = fs.Open(path)
	require.NoError(t, err)

	readData, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, testData, readData)

	err = file.Close()
	require.NoError(t, err)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestS3Integration_Stat tests file stat operations.
func TestS3Integration_Stat(t *testing.T) {
	skipIfNoCredentials(t)

	ctx := context.Background()
	config := getTestConfig()

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	bucket := getTestBucket()
	key := fmt.Sprintf("test/stat-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Write data
	testData := []byte("Test data for stat operation")

	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Stat the file
	info, err := fs.Stat(path)
	require.NoError(t, err)

	assert.Equal(t, int64(len(testData)), info.Size())
	assert.False(t, info.IsDir())
	assert.NotEmpty(t, info.Name())

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestS3Integration_Exists tests file existence checking.
func TestS3Integration_Exists(t *testing.T) {
	skipIfNoCredentials(t)

	ctx := context.Background()
	config := getTestConfig()

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	bucket := getTestBucket()
	key := fmt.Sprintf("test/exists-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Check non-existent file
	exists, err := fs.Exists(path)
	require.NoError(t, err)
	assert.False(t, exists)

	// Create file
	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write([]byte("test"))
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Check existing file
	exists, err = fs.Exists(path)
	require.NoError(t, err)
	assert.True(t, exists)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestS3Integration_ReadDir tests directory listing.
func TestS3Integration_ReadDir(t *testing.T) {
	skipIfNoCredentials(t)

	ctx := context.Background()
	config := getTestConfig()

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	bucket := getTestBucket()
	prefix := fmt.Sprintf("test/readdir-%d", time.Now().UnixNano())
	basePath := fmt.Sprintf("s3://%s/%s", bucket, prefix)

	// Create multiple files
	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, fname := range files {
		path := fmt.Sprintf("%s/%s", basePath, fname)

		file, err := fs.Create(path)
		require.NoError(t, err)

		_, err = file.Write([]byte("test content"))
		require.NoError(t, err)

		err = file.Close()
		require.NoError(t, err)
	}

	// List directory
	entries, err := fs.ReadDir(basePath)
	require.NoError(t, err)
	assert.Len(t, entries, len(files))

	// Verify file names
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}

	for _, fname := range files {
		assert.Contains(t, names, fname)
	}

	// Cleanup
	for _, fname := range files {
		path := fmt.Sprintf("%s/%s", basePath, fname)
		err := fs.Remove(path)
		require.NoError(t, err)
	}
}

// TestS3Integration_ReadAt tests random read operations.
func TestS3Integration_ReadAt(t *testing.T) {
	skipIfNoCredentials(t)

	ctx := context.Background()
	config := getTestConfig()

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	bucket := getTestBucket()
	key := fmt.Sprintf("test/readat-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Write data
	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Read at various offsets
	file, err = fs.Open(path)
	require.NoError(t, err)

	s3File, ok := file.(*S3File)
	require.True(t, ok)

	// Read from middle
	buf := make([]byte, 10)
	n, err := s3File.ReadAt(buf, 10)
	require.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.Equal(t, testData[10:20], buf)

	// Read from beginning
	n, err = s3File.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.Equal(t, testData[0:10], buf)

	err = file.Close()
	require.NoError(t, err)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestS3Integration_Seek tests seek operations.
func TestS3Integration_Seek(t *testing.T) {
	skipIfNoCredentials(t)

	ctx := context.Background()
	config := getTestConfig()

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	bucket := getTestBucket()
	key := fmt.Sprintf("test/seek-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Write data
	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Test seeking
	file, err = fs.Open(path)
	require.NoError(t, err)

	// Seek to offset 10
	pos, err := file.Seek(10, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(10), pos)

	// Read from current position
	buf := make([]byte, 5)
	n, err := file.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, testData[10:15], buf)

	err = file.Close()
	require.NoError(t, err)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestS3Integration_LargeFile tests operations on large files.
func TestS3Integration_LargeFile(t *testing.T) {
	skipIfNoCredentials(t)

	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	ctx := context.Background()
	config := getTestConfig()

	// Configure for large file handling
	config.MultipartPartSize = 5 * 1024 * 1024
	config.MultipartThreshold = 10 * 1024 * 1024
	config.MultipartConcurrency = 4

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	bucket := getTestBucket()
	key := fmt.Sprintf("test/large-%d.bin", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Create 15MB test data (larger than multipart threshold)
	dataSize := 15 * 1024 * 1024
	testData := make([]byte, dataSize)

	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write data
	file, err := fs.Create(path)
	require.NoError(t, err)

	written, err := file.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, dataSize, written)

	err = file.Close()
	require.NoError(t, err)

	// Read data back
	file, err = fs.Open(path)
	require.NoError(t, err)

	readData, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, len(testData), len(readData))
	assert.True(t, bytes.Equal(testData, readData))

	err = file.Close()
	require.NoError(t, err)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestS3Integration_ConcurrentReader tests the concurrent reader.
func TestS3Integration_ConcurrentReader(t *testing.T) {
	skipIfNoCredentials(t)

	if testing.Short() {
		t.Skip("Skipping concurrent reader test in short mode")
	}

	ctx := context.Background()
	config := getTestConfig()
	config.ConcurrentReadWorkers = 4
	config.ConcurrentReadChunkSize = 1 * 1024 * 1024 // 1MB chunks

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	bucket := getTestBucket()
	key := fmt.Sprintf("test/concurrent-%d.bin", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Create 10MB test data
	dataSize := 10 * 1024 * 1024
	testData := make([]byte, dataSize)

	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write data
	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Read using concurrent reader
	reader, err := fs.CreateConcurrentReader(ctx, path)
	require.NoError(t, err)

	readData, err := reader.ReadAll(ctx)
	require.NoError(t, err)
	assert.Equal(t, len(testData), len(readData))
	assert.True(t, bytes.Equal(testData, readData))

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestS3Integration_MultipartWriter tests the streaming multipart writer.
func TestS3Integration_MultipartWriter(t *testing.T) {
	skipIfNoCredentials(t)

	if testing.Short() {
		t.Skip("Skipping multipart writer test in short mode")
	}

	ctx := context.Background()
	config := getTestConfig()
	config.MultipartPartSize = 5 * 1024 * 1024 // 5MB parts

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	bucket := getTestBucket()
	key := fmt.Sprintf("test/multipart-%d.bin", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Create 15MB test data (3 parts)
	dataSize := 15 * 1024 * 1024
	testData := make([]byte, dataSize)

	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write using multipart writer
	writer, err := fs.CreateMultipartWriter(ctx, path)
	require.NoError(t, err)

	// Write in chunks
	chunkSize := 1 * 1024 * 1024 // 1MB chunks
	for offset := 0; offset < dataSize; offset += chunkSize {
		end := offset + chunkSize
		if end > dataSize {
			end = dataSize
		}

		n, err := writer.Write(testData[offset:end])
		require.NoError(t, err)
		assert.Equal(t, end-offset, n)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Read data back
	file, err := fs.Open(path)
	require.NoError(t, err)

	readData, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, len(testData), len(readData))
	assert.True(t, bytes.Equal(testData, readData))

	err = file.Close()
	require.NoError(t, err)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestS3Integration_RetryOnError tests retry behavior on transient errors.
func TestS3Integration_RetryOnError(t *testing.T) {
	skipIfNoCredentials(t)

	ctx := context.Background()
	config := getTestConfig()
	config.RetryConfig = RetryConfig{
		MaxRetries:    3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      1 * time.Second,
		BackoffFactor: 2.0,
	}

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	bucket := getTestBucket()
	key := fmt.Sprintf("test/retry-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Write data
	testData := []byte("Test data for retry")

	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Read with retry config - this should work on first try
	file, err = fs.Open(path)
	require.NoError(t, err)

	readData, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, testData, readData)

	err = file.Close()
	require.NoError(t, err)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestS3Integration_RegionAutoDetect tests automatic region detection.
func TestS3Integration_RegionAutoDetect(t *testing.T) {
	skipIfNoCredentials(t)

	// Skip for LocalStack as region detection doesn't work the same way
	if testEndpoint != "" {
		t.Skip("Skipping region auto-detect test for LocalStack")
	}

	ctx := context.Background()
	config := getTestConfig()
	config.AutoDetectRegion = true
	config.Region = "" // Clear region to test auto-detection

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	bucket := getTestBucket()

	// Detect region for bucket
	region, err := fs.detectBucketRegion(ctx, bucket)
	require.NoError(t, err)
	assert.NotEmpty(t, region)

	t.Logf("Detected region for bucket %s: %s", bucket, region)
}

// TestS3Integration_CustomEndpoint tests custom endpoint support (MinIO/LocalStack).
func TestS3Integration_CustomEndpoint(t *testing.T) {
	if testEndpoint == "" {
		t.Skip("Skipping custom endpoint test: LOCALSTACK_ENDPOINT not set")
	}

	ctx := context.Background()
	config := S3Config{
		Endpoint:        testEndpoint,
		UseSSL:          false,
		URLStyle:        S3URLStylePath,
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
	}

	fs, err := NewS3FileSystem(ctx, config)
	require.NoError(t, err)

	// Test basic operations
	bucket := getTestBucket()
	key := fmt.Sprintf("test/custom-endpoint-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	testData := []byte("Custom endpoint test data")

	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Verify
	exists, err := fs.Exists(path)
	require.NoError(t, err)
	assert.True(t, exists)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}
