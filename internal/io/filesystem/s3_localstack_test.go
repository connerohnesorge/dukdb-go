//go:build integration

// Package filesystem provides LocalStack-specific integration tests for S3 filesystem operations.
// These tests verify S3 compatibility using LocalStack, an AWS S3 emulator.
//
// To run these tests:
//
//	# Start LocalStack
//	docker run -d -p 4566:4566 localstack/localstack
//
//	# Run tests
//	LOCALSTACK_ENDPOINT=http://localhost:4566 go test -tags integration ./internal/io/filesystem/... -run LocalStack
//
// Or with a specific LocalStack endpoint:
//
//	LOCALSTACK_ENDPOINT=localhost:4566 go test -tags integration ./internal/io/filesystem/... -run LocalStack
//
// Note: LocalStack accepts any credentials, so tests use "test"/"test" for access keys.
package filesystem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getLocalStackConfig returns an S3Config configured for LocalStack testing.
// Tests are skipped if LOCALSTACK_ENDPOINT environment variable is not set.
func getLocalStackConfig(t *testing.T) *S3Config {
	t.Helper()

	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		t.Skip("LOCALSTACK_ENDPOINT not set, skipping LocalStack tests")
	}

	// Strip http:// or https:// prefix if present
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	config := &S3Config{
		Endpoint:        endpoint,
		Region:          "us-east-1",
		AccessKeyID:     "test", // LocalStack accepts any credentials
		SecretAccessKey: "test",
		UseSSL:          false,
		URLStyle:        S3URLStylePath, // LocalStack requires path style
	}

	return config
}

// getLocalStackBucket returns the test bucket name.
// Uses S3_TEST_BUCKET environment variable if set, otherwise defaults to "dukdb-localstack-test".
func getLocalStackBucket() string {
	if bucket := os.Getenv("S3_TEST_BUCKET"); bucket != "" {
		return bucket
	}

	return "dukdb-localstack-test"
}

// ensureLocalStackBucket creates the test bucket if it does not exist.
func ensureLocalStackBucket(ctx context.Context, t *testing.T, fs *S3FileSystem, bucket string) {
	t.Helper()

	client := fs.GetClient()

	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		t.Logf("Warning: failed to check bucket existence: %v", err)
	}

	if !exists {
		err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{
			Region: "us-east-1",
		})
		if err != nil {
			// Ignore error if bucket already exists (race condition)
			if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") &&
				!strings.Contains(err.Error(), "BucketAlreadyExists") {
				t.Logf("Warning: failed to create bucket: %v", err)
			}
		}
	}
}

// TestLocalStackS3Read tests reading objects from LocalStack S3.
func TestLocalStackS3Read(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	// First, write a test object
	key := fmt.Sprintf("test/read-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)
	testData := []byte("Hello, LocalStack S3!")

	file, err := fs.Create(path)
	require.NoError(t, err)

	n, err := file.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n)

	err = file.Close()
	require.NoError(t, err)

	// Now read it back
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

// TestLocalStackS3Write tests writing objects to LocalStack S3.
func TestLocalStackS3Write(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	key := fmt.Sprintf("test/write-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Test writing various sizes
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("small content")},
		{"medium", bytes.Repeat([]byte("x"), 1024)},         // 1 KB
		{"large", bytes.Repeat([]byte("y"), 1024*100)},      // 100 KB
		{"very-large", bytes.Repeat([]byte("z"), 1024*500)}, // 500 KB
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testPath := fmt.Sprintf("%s-%s", path, tc.name)

			file, err := fs.Create(testPath)
			require.NoError(t, err)

			n, err := file.Write(tc.data)
			require.NoError(t, err)
			assert.Equal(t, len(tc.data), n)

			err = file.Close()
			require.NoError(t, err)

			// Verify content
			file, err = fs.Open(testPath)
			require.NoError(t, err)

			readData, err := io.ReadAll(file)
			require.NoError(t, err)
			assert.Equal(t, tc.data, readData)

			err = file.Close()
			require.NoError(t, err)

			// Cleanup
			err = fs.Remove(testPath)
			require.NoError(t, err)
		})
	}
}

// TestLocalStackS3Stat tests stat operations against LocalStack S3.
func TestLocalStackS3Stat(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	key := fmt.Sprintf("test/stat-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)
	testData := []byte("Test content for stat operation")

	// Write test object
	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Stat the object
	info, err := fs.Stat(path)
	require.NoError(t, err)

	assert.Equal(t, int64(len(testData)), info.Size())
	assert.False(t, info.IsDir())
	assert.NotEmpty(t, info.Name())
	assert.False(t, info.ModTime().IsZero())

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestLocalStackS3ReadDir tests directory listing against LocalStack S3.
func TestLocalStackS3ReadDir(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	// Create a unique prefix for this test
	prefix := fmt.Sprintf("test/readdir-%d", time.Now().UnixNano())
	basePath := fmt.Sprintf("s3://%s/%s", bucket, prefix)

	// Create test files
	files := []string{"file1.txt", "file2.txt", "file3.csv"}
	for _, fname := range files {
		path := fmt.Sprintf("%s/%s", basePath, fname)

		file, err := fs.Create(path)
		require.NoError(t, err)

		_, err = file.Write([]byte("test content"))
		require.NoError(t, err)

		err = file.Close()
		require.NoError(t, err)
	}

	// Create a subdirectory with a file
	subdir := fmt.Sprintf("%s/subdir/nested.txt", basePath)
	file, err := fs.Create(subdir)
	require.NoError(t, err)

	_, err = file.Write([]byte("nested content"))
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// List directory
	entries, err := fs.ReadDir(basePath)
	require.NoError(t, err)

	// Should have 3 files + 1 directory
	assert.GreaterOrEqual(t, len(entries), 3)

	// Verify file names are present
	names := make(map[string]bool)
	for _, entry := range entries {
		names[entry.Name()] = true
	}

	for _, fname := range files {
		assert.True(t, names[fname], "Expected file %s in directory listing", fname)
	}

	// Cleanup
	for _, fname := range files {
		path := fmt.Sprintf("%s/%s", basePath, fname)
		err := fs.Remove(path)
		require.NoError(t, err)
	}

	err = fs.Remove(subdir)
	require.NoError(t, err)
}

// TestLocalStackS3Remove tests object removal against LocalStack S3.
func TestLocalStackS3Remove(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	key := fmt.Sprintf("test/remove-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)
	testData := []byte("Test content for removal")

	// Write test object
	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Verify it exists
	exists, err := fs.Exists(path)
	require.NoError(t, err)
	assert.True(t, exists)

	// Remove it
	err = fs.Remove(path)
	require.NoError(t, err)

	// Verify it no longer exists
	exists, err = fs.Exists(path)
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestLocalStackS3ReadAt tests random read operations (range requests) against LocalStack S3.
func TestLocalStackS3ReadAt(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	key := fmt.Sprintf("test/readat-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)
	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

	// Write test object
	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Open for reading
	file, err = fs.Open(path)
	require.NoError(t, err)

	s3File, ok := file.(*S3File)
	require.True(t, ok)

	// Test ReadAt at various offsets
	testCases := []struct {
		name     string
		offset   int64
		length   int
		expected []byte
	}{
		{"beginning", 0, 10, testData[0:10]},
		{"middle", 10, 10, testData[10:20]},
		{"end", 52, 10, testData[52:62]},
		{"single-byte", 5, 1, testData[5:6]},
		{"large-range", 0, 30, testData[0:30]},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.length)
			n, err := s3File.ReadAt(buf, tc.offset)
			require.NoError(t, err)
			assert.Equal(t, tc.length, n)
			assert.Equal(t, tc.expected, buf)
		})
	}

	err = file.Close()
	require.NoError(t, err)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestLocalStackS3Multipart tests multipart upload operations against LocalStack S3.
func TestLocalStackS3Multipart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multipart test in short mode")
	}

	config := getLocalStackConfig(t)
	ctx := context.Background()

	// Configure for multipart uploads with smaller part size for testing
	config.MultipartPartSize = 5 * 1024 * 1024  // 5MB minimum for S3
	config.MultipartThreshold = 5 * 1024 * 1024 // Use multipart for files > 5MB
	config.MultipartConcurrency = 2

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	key := fmt.Sprintf("test/multipart-%d.bin", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Create test data larger than multipart threshold (11MB to trigger 3 parts)
	dataSize := 11 * 1024 * 1024
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Use multipart writer
	writer, err := fs.CreateMultipartWriter(ctx, path)
	require.NoError(t, err)

	// Write in chunks
	chunkSize := 1024 * 1024 // 1MB chunks
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

	// Verify the uploaded data
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

// TestLocalStackS3ConcurrentReader tests concurrent read operations against LocalStack S3.
func TestLocalStackS3ConcurrentReader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent reader test in short mode")
	}

	config := getLocalStackConfig(t)
	ctx := context.Background()

	// Configure for concurrent reads
	config.ConcurrentReadWorkers = 4
	config.ConcurrentReadChunkSize = 1 * 1024 * 1024 // 1MB chunks

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	key := fmt.Sprintf("test/concurrent-read-%d.bin", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Create test data (5MB)
	dataSize := 5 * 1024 * 1024
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write test data
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

// TestLocalStackS3Exists tests object existence checking against LocalStack S3.
func TestLocalStackS3Exists(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	key := fmt.Sprintf("test/exists-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)

	// Check non-existent object
	exists, err := fs.Exists(path)
	require.NoError(t, err)
	assert.False(t, exists)

	// Create object
	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write([]byte("test"))
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Check existing object
	exists, err = fs.Exists(path)
	require.NoError(t, err)
	assert.True(t, exists)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestLocalStackS3MkdirAll tests that MkdirAll is a no-op for S3.
func TestLocalStackS3MkdirAll(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()

	// MkdirAll should always succeed for S3 (it's a no-op)
	err = fs.MkdirAll(fmt.Sprintf("s3://%s/some/nested/path", bucket))
	require.NoError(t, err)
}

// TestLocalStackS3SeekRead tests seek operations during reads against LocalStack S3.
func TestLocalStackS3SeekRead(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	key := fmt.Sprintf("test/seek-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)
	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

	// Write test object
	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Open and seek
	file, err = fs.Open(path)
	require.NoError(t, err)

	// Seek to position 10
	pos, err := file.Seek(10, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(10), pos)

	// Read 10 bytes
	buf := make([]byte, 10)
	n, err := file.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.Equal(t, testData[10:20], buf)

	// Seek relative to current
	pos, err = file.Seek(5, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(25), pos)

	err = file.Close()
	require.NoError(t, err)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestLocalStackS3RemoveDir tests directory removal against LocalStack S3.
func TestLocalStackS3RemoveDir(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	// Create a unique prefix for this test
	prefix := fmt.Sprintf("test/removedir-%d", time.Now().UnixNano())
	basePath := fmt.Sprintf("s3://%s/%s", bucket, prefix)

	// Create test files
	files := []string{"file1.txt", "file2.txt", "subdir/nested.txt"}
	for _, fname := range files {
		path := fmt.Sprintf("%s/%s", basePath, fname)

		file, err := fs.Create(path)
		require.NoError(t, err)

		_, err = file.Write([]byte("test content"))
		require.NoError(t, err)

		err = file.Close()
		require.NoError(t, err)
	}

	// Verify files exist
	for _, fname := range files {
		path := fmt.Sprintf("%s/%s", basePath, fname)
		exists, err := fs.Exists(path)
		require.NoError(t, err)
		assert.True(t, exists, "Expected file %s to exist", path)
	}

	// Remove directory
	err = fs.RemoveDir(basePath)
	require.NoError(t, err)

	// Verify files are gone
	for _, fname := range files {
		path := fmt.Sprintf("%s/%s", basePath, fname)
		exists, err := fs.Exists(path)
		require.NoError(t, err)
		assert.False(t, exists, "Expected file %s to be removed", path)
	}
}

// TestLocalStackS3ContextOperations tests context-aware operations against LocalStack S3.
func TestLocalStackS3ContextOperations(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	key := fmt.Sprintf("test/context-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("s3://%s/%s", bucket, key)
	testData := []byte("Context test data")

	// Create with context
	file, err := fs.CreateContext(ctx, path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Open with context
	file, err = fs.OpenContext(ctx, path)
	require.NoError(t, err)

	readData, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, testData, readData)

	err = file.Close()
	require.NoError(t, err)

	// Stat with context
	info, err := fs.StatContext(ctx, path)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), info.Size())

	// ReadDir with context
	entries, err := fs.ReadDirContext(ctx, fmt.Sprintf("s3://%s/test", bucket))
	require.NoError(t, err)
	assert.Greater(t, len(entries), 0)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestLocalStackS3SpecialCharacters tests handling of special characters in object keys.
func TestLocalStackS3SpecialCharacters(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	bucket := getLocalStackBucket()
	ensureLocalStackBucket(ctx, t, fs, bucket)

	// Test various special characters in keys
	testCases := []struct {
		name string
		key  string
	}{
		{"spaces", "test/with spaces/file.txt"},
		{"unicode", "test/unicode-\xe4\xb8\xad\xe6\x96\x87.txt"},
		{"special", "test/special+chars=yes&more.txt"},
		{"hyphens-underscores", "test/file-with_mixed-chars.txt"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			timestamp := time.Now().UnixNano()
			key := fmt.Sprintf("%s-%d", tc.key, timestamp)
			path := fmt.Sprintf("s3://%s/%s", bucket, key)
			testData := []byte(fmt.Sprintf("Content for %s", tc.name))

			// Write
			file, err := fs.Create(path)
			require.NoError(t, err)

			_, err = file.Write(testData)
			require.NoError(t, err)

			err = file.Close()
			require.NoError(t, err)

			// Read back
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
		})
	}
}

// TestLocalStackS3Capabilities verifies S3 filesystem capabilities.
func TestLocalStackS3Capabilities(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	caps := fs.Capabilities()

	assert.True(t, caps.SupportsSeek, "S3 should support seek via range requests")
	assert.False(t, caps.SupportsAppend, "S3 should not support append")
	assert.True(t, caps.SupportsRange, "S3 should support range requests")
	assert.True(t, caps.SupportsDirList, "S3 should support directory listing")
	assert.True(t, caps.SupportsWrite, "S3 should support write")
	assert.True(t, caps.SupportsDelete, "S3 should support delete")
	assert.True(t, caps.ContextTimeout, "S3 should support context timeout")
}

// TestLocalStackS3URI verifies the filesystem URI.
func TestLocalStackS3URI(t *testing.T) {
	config := getLocalStackConfig(t)
	ctx := context.Background()

	fs, err := NewS3FileSystem(ctx, *config)
	require.NoError(t, err)

	assert.Equal(t, "s3://", fs.URI())
}
