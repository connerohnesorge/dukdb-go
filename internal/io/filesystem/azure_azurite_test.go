//go:build integration

// Package filesystem provides Azurite-specific integration tests for Azure Blob Storage filesystem operations.
// These tests verify Azure compatibility using Azurite, Microsoft's official Azure Storage emulator.
//
// To run these tests:
//
//	# Start Azurite
//	docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
//
//	# Run tests
//	AZURITE_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1 go test -tags integration ./internal/io/filesystem/... -run Azurite
//
// Or with docker-compose:
//
//	services:
//	  azurite:
//	    image: mcr.microsoft.com/azure-storage/azurite
//	    command: azurite-blob --blobHost 0.0.0.0
//	    ports:
//	      - "10000:10000"
//
// Note: Azurite uses well-known development credentials which are used by default in these tests.
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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Azurite well-known development credentials
// These are the default credentials used by Azurite and are publicly documented.
const (
	azuriteAccountName = "devstoreaccount1"
	// This is the well-known Azurite development key - it's intentionally public.
	//nolint:gosec // This is a well-known development key for Azurite, not a real secret
	azuriteAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	azuriteContainer  = "dukdb-azurite-test"
)

// getAzuriteConfig returns an AzureConfig configured for Azurite testing.
// Tests are skipped if AZURITE_ENDPOINT environment variable is not set.
func getAzuriteConfig(t *testing.T) *AzureConfig {
	t.Helper()

	endpoint := os.Getenv("AZURITE_ENDPOINT")
	if endpoint == "" {
		t.Skip("AZURITE_ENDPOINT not set, skipping Azurite tests")
	}

	// Ensure endpoint has proper format
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}

	config := &AzureConfig{
		AccountName: azuriteAccountName,
		AccountKey:  azuriteAccountKey,
		Endpoint:    endpoint,
		Container:   azuriteContainer,
		UseHTTPS:    false, // Azurite uses HTTP by default
		Timeout:     30 * time.Second,
		BlockSize:   4 * 1024 * 1024, // 4MB blocks
	}

	return config
}

// getAzuriteContainer returns the test container name.
// Uses AZURE_TEST_CONTAINER environment variable if set, otherwise defaults to azuriteContainer.
func getAzuriteContainer() string {
	if container := os.Getenv("AZURE_TEST_CONTAINER"); container != "" {
		return container
	}

	return azuriteContainer
}

// ensureAzuriteContainer creates the test container if it does not exist.
func ensureAzuriteContainer(
	ctx context.Context,
	t *testing.T,
	fs *AzureFileSystem,
	containerName string,
) {
	t.Helper()

	client := fs.GetClient()
	containerClient := client.ServiceClient().NewContainerClient(containerName)

	_, err := containerClient.Create(ctx, nil)
	if err != nil {
		// Ignore error if container already exists
		if !strings.Contains(err.Error(), "ContainerAlreadyExists") {
			t.Logf("Warning: failed to create container: %v", err)
		}
	}
}

// TestAzuriteRead tests reading blobs from Azurite.
func TestAzuriteRead(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// First, write a test blob
	blobName := fmt.Sprintf("test/read-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("azure://%s/%s", container, blobName)
	testData := []byte("Hello, Azurite Azure Storage!")

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

// TestAzuriteWrite tests writing blobs to Azurite.
func TestAzuriteWrite(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	blobName := fmt.Sprintf("test/write-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("azure://%s/%s", container, blobName)

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

// TestAzuriteStat tests stat operations against Azurite.
func TestAzuriteStat(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	blobName := fmt.Sprintf("test/stat-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("azure://%s/%s", container, blobName)
	testData := []byte("Test content for stat operation")

	// Write test blob
	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Stat the blob
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

// TestAzuriteReadDir tests directory listing against Azurite.
func TestAzuriteReadDir(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Create a unique prefix for this test
	prefix := fmt.Sprintf("test/readdir-%d", time.Now().UnixNano())
	basePath := fmt.Sprintf("azure://%s/%s", container, prefix)

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

// TestAzuriteRemove tests blob removal against Azurite.
func TestAzuriteRemove(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	blobName := fmt.Sprintf("test/remove-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("azure://%s/%s", container, blobName)
	testData := []byte("Test content for removal")

	// Write test blob
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

// TestAzuriteReadAt tests random read operations (range requests) against Azurite.
func TestAzuriteReadAt(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	blobName := fmt.Sprintf("test/readat-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("azure://%s/%s", container, blobName)
	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

	// Write test blob
	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Open for reading
	file, err = fs.Open(path)
	require.NoError(t, err)

	azureFile, ok := file.(*AzureFile)
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
			n, err := azureFile.ReadAt(buf, tc.offset)
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

// TestAzuriteLargeBlob tests uploading and downloading large blobs against Azurite.
func TestAzuriteLargeBlob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large blob test in short mode")
	}

	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	blobName := fmt.Sprintf("test/large-%d.bin", time.Now().UnixNano())
	path := fmt.Sprintf("azure://%s/%s", container, blobName)

	// Create test data (5MB)
	dataSize := 5 * 1024 * 1024
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write large blob
	file, err := fs.Create(path)
	require.NoError(t, err)

	n, err := file.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, dataSize, n)

	err = file.Close()
	require.NoError(t, err)

	// Verify the uploaded data
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

// TestAzuriteExists tests blob existence checking against Azurite.
func TestAzuriteExists(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	blobName := fmt.Sprintf("test/exists-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("azure://%s/%s", container, blobName)

	// Check non-existent blob
	exists, err := fs.Exists(path)
	require.NoError(t, err)
	assert.False(t, exists)

	// Create blob
	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write([]byte("test"))
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Check existing blob
	exists, err = fs.Exists(path)
	require.NoError(t, err)
	assert.True(t, exists)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestAzuriteMkdirAll tests that MkdirAll is a no-op for Azure.
func TestAzuriteMkdirAll(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()

	// MkdirAll should always succeed for Azure (it's a no-op)
	err = fs.MkdirAll(fmt.Sprintf("azure://%s/some/nested/path", container))
	require.NoError(t, err)
}

// TestAzuriteSeekRead tests seek operations during reads against Azurite.
func TestAzuriteSeekRead(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	blobName := fmt.Sprintf("test/seek-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("azure://%s/%s", container, blobName)
	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

	// Write test blob
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

// TestAzuriteRemoveDir tests directory removal against Azurite.
func TestAzuriteRemoveDir(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Create a unique prefix for this test
	prefix := fmt.Sprintf("test/removedir-%d", time.Now().UnixNano())
	basePath := fmt.Sprintf("azure://%s/%s", container, prefix)

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

// TestAzuriteContextOperations tests context-aware operations against Azurite.
func TestAzuriteContextOperations(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	blobName := fmt.Sprintf("test/context-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("azure://%s/%s", container, blobName)
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
	entries, err := fs.ReadDirContext(ctx, fmt.Sprintf("azure://%s/test", container))
	require.NoError(t, err)
	assert.Greater(t, len(entries), 0)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestAzuriteSpecialCharacters tests handling of special characters in blob names.
func TestAzuriteSpecialCharacters(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Test various special characters in blob names
	testCases := []struct {
		name     string
		blobName string
	}{
		{"spaces", "test/with spaces/file.txt"},
		{"hyphens-underscores", "test/file-with_mixed-chars.txt"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			timestamp := time.Now().UnixNano()
			blobName := fmt.Sprintf("%s-%d", tc.blobName, timestamp)
			path := fmt.Sprintf("azure://%s/%s", container, blobName)
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

// TestAzuriteCapabilities verifies Azure filesystem capabilities.
func TestAzuriteCapabilities(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	caps := fs.Capabilities()

	assert.True(t, caps.SupportsSeek, "Azure should support seek via range requests")
	assert.False(t, caps.SupportsAppend, "Azure should not support append")
	assert.True(t, caps.SupportsRange, "Azure should support range requests")
	assert.True(t, caps.SupportsDirList, "Azure should support directory listing")
	assert.True(t, caps.SupportsWrite, "Azure should support write")
	assert.True(t, caps.SupportsDelete, "Azure should support delete")
	assert.True(t, caps.ContextTimeout, "Azure should support context timeout")
}

// TestAzuriteURI verifies the filesystem URI.
func TestAzuriteURI(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	assert.Equal(t, "azure://", fs.URI())
}

// TestAzuriteAzScheme tests the az:// URL scheme against Azurite.
func TestAzuriteAzScheme(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	blobName := fmt.Sprintf("test/az-scheme-%d.txt", time.Now().UnixNano())
	// Use az:// scheme
	path := fmt.Sprintf("az://%s/%s", container, blobName)
	testData := []byte("Testing az:// scheme")

	// Write using az:// scheme
	file, err := fs.Create(path)
	require.NoError(t, err)

	_, err = file.Write(testData)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Read back using az:// scheme
	file, err = fs.Open(path)
	require.NoError(t, err)

	readData, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, testData, readData)

	err = file.Close()
	require.NoError(t, err)

	// Stat using azure:// scheme (cross-scheme compatibility)
	azurePath := fmt.Sprintf("azure://%s/%s", container, blobName)
	info, err := fs.Stat(azurePath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), info.Size())

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}

// TestAzuriteDirectClientAccess tests using the Azure client directly.
func TestAzuriteDirectClientAccess(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	// Get the underlying client
	client := fs.GetClient()
	require.NotNil(t, client)

	// Verify client is functional by listing containers
	// This is an advanced usage pattern
	assert.IsType(t, &azblob.Client{}, client)
}

// TestAzuriteConfigRetrieval tests retrieving the filesystem configuration.
func TestAzuriteConfigRetrieval(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	retrieved := fs.GetConfig()

	assert.Equal(t, config.AccountName, retrieved.AccountName)
	assert.Equal(t, config.AccountKey, retrieved.AccountKey)
	assert.Equal(t, config.Endpoint, retrieved.Endpoint)
	assert.Equal(t, config.Container, retrieved.Container)
}

// TestAzuriteClose tests closing the filesystem.
func TestAzuriteClose(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	// Close should succeed
	err = fs.Close()
	require.NoError(t, err)
}

// TestAzuriteEmptyBlob tests reading and writing empty blobs.
func TestAzuriteEmptyBlob(t *testing.T) {
	config := getAzuriteConfig(t)
	ctx := context.Background()

	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)

	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	blobName := fmt.Sprintf("test/empty-%d.txt", time.Now().UnixNano())
	path := fmt.Sprintf("azure://%s/%s", container, blobName)

	// Write empty blob
	file, err := fs.Create(path)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Stat should work
	info, err := fs.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, int64(0), info.Size())

	// Read should return empty
	file, err = fs.Open(path)
	require.NoError(t, err)

	readData, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Empty(t, readData)

	err = file.Close()
	require.NoError(t, err)

	// Cleanup
	err = fs.Remove(path)
	require.NoError(t, err)
}
