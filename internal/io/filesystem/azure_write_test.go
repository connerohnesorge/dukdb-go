//go:build integration

// Package filesystem provides integration tests for Azure Blob Storage write operations.
// These tests verify that COPY TO statements work correctly with Azure Blob Storage.
// Tests use testcontainers-go with Azurite for local testing.
package filesystem

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAzureWriteParquet tests writing Parquet files to Azure Blob Storage.
func TestAzureWriteParquet(t *testing.T) {
	if azuriteEndpoint == "" {
		t.Skip("Azurite not available")
	}

	ctx := context.Background()
	config := getAzuriteConfig(t)
	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)
	defer fs.Close()

	// Ensure container exists
	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Create a test file by writing Parquet-like data
	testPath := fmt.Sprintf("azure://%s/test-data.parquet", container)
	file, err := fs.Create(testPath)
	require.NoError(t, err)

	// Write test data (simulating Parquet format)
	testData := []byte("PAR1") // Parquet magic number
	testData = append(testData, []byte("test data for Parquet format")...)

	n, err := file.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n)

	err = file.Close()
	require.NoError(t, err)

	// Verify file was created
	exists, err := fs.Exists(testPath)
	require.NoError(t, err)
	assert.True(t, exists)

	// Verify we can read it back
	readFile, err := fs.Open(testPath)
	require.NoError(t, err)
	defer readFile.Close()

	buf := make([]byte, len(testData))
	n, err = readFile.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n)
	assert.Equal(t, testData, buf)
}

// TestAzureWriteCSV tests writing CSV files to Azure Blob Storage.
func TestAzureWriteCSV(t *testing.T) {
	if azuriteEndpoint == "" {
		t.Skip("Azurite not available")
	}

	ctx := context.Background()
	config := getAzuriteConfig(t)
	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)
	defer fs.Close()

	// Ensure container exists
	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Create a test CSV file
	testPath := fmt.Sprintf("azure://%s/test-data.csv", container)
	file, err := fs.Create(testPath)
	require.NoError(t, err)

	// Write CSV data
	csvData := []byte("id,name,value\n1,test1,100\n2,test2,200\n3,test3,300")

	n, err := file.Write(csvData)
	require.NoError(t, err)
	assert.Equal(t, len(csvData), n)

	err = file.Close()
	require.NoError(t, err)

	// Verify file was created
	exists, err := fs.Exists(testPath)
	require.NoError(t, err)
	assert.True(t, exists)

	// Verify file size
	info, err := fs.Stat(testPath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(csvData)), info.Size())
}

// TestAzureWriteJSON tests writing JSON files to Azure Blob Storage.
func TestAzureWriteJSON(t *testing.T) {
	if azuriteEndpoint == "" {
		t.Skip("Azurite not available")
	}

	ctx := context.Background()
	config := getAzuriteConfig(t)
	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)
	defer fs.Close()

	// Ensure container exists
	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Create a test JSON file
	testPath := fmt.Sprintf("azure://%s/test-data.json", container)
	file, err := fs.Create(testPath)
	require.NoError(t, err)

	// Write JSON data
	jsonData := []byte(`[
  {"id": 1, "name": "test1", "value": 100},
  {"id": 2, "name": "test2", "value": 200},
  {"id": 3, "name": "test3", "value": 300}
]`)

	n, err := file.Write(jsonData)
	require.NoError(t, err)
	assert.Equal(t, len(jsonData), n)

	err = file.Close()
	require.NoError(t, err)

	// Verify file was created
	exists, err := fs.Exists(testPath)
	require.NoError(t, err)
	assert.True(t, exists)
}

// TestAzureWriteLargeFile tests writing large files (>1MB) to Azure.
func TestAzureWriteLargeFile(t *testing.T) {
	if azuriteEndpoint == "" {
		t.Skip("Azurite not available")
	}

	ctx := context.Background()
	config := getAzuriteConfig(t)
	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)
	defer fs.Close()

	// Ensure container exists
	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Create a large test file (5MB)
	testPath := fmt.Sprintf("azure://%s/large-file.bin", container)
	file, err := fs.Create(testPath)
	require.NoError(t, err)

	// Write 5MB of data in chunks
	chunkSize := 1024 * 1024 // 1MB chunks
	totalSize := 5 * chunkSize
	chunk := make([]byte, chunkSize)
	for i := range chunk {
		chunk[i] = byte(i % 256)
	}

	written := 0
	for i := 0; i < 5; i++ {
		n, err := file.Write(chunk)
		require.NoError(t, err)
		written += n
	}
	assert.Equal(t, totalSize, written)

	err = file.Close()
	require.NoError(t, err)

	// Verify file was created with correct size
	info, err := fs.Stat(testPath)
	require.NoError(t, err)
	assert.Equal(t, int64(totalSize), info.Size())
}

// TestAzureWriteConcurrent tests concurrent write operations.
func TestAzureWriteConcurrent(t *testing.T) {
	if azuriteEndpoint == "" {
		t.Skip("Azurite not available")
	}

	ctx := context.Background()
	config := getAzuriteConfig(t)
	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)
	defer fs.Close()

	// Ensure container exists
	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Test concurrent writes to different files
	numFiles := 5
	done := make(chan error, numFiles)

	for i := 0; i < numFiles; i++ {
		go func(idx int) {
			path := fmt.Sprintf("azure://%s/concurrent-%d.txt", container, idx)
			file, err := fs.Create(path)
			if err != nil {
				done <- err
				return
			}

			data := []byte(fmt.Sprintf("File %d content", idx))
			_, err = file.Write(data)
			if err != nil {
				file.Close()
				done <- err
				return
			}

			// Close the file before signaling completion
			err = file.Close()
			done <- err
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numFiles; i++ {
		err := <-done
		require.NoError(t, err)
	}

	// Verify all files were created
	for i := 0; i < numFiles; i++ {
		path := fmt.Sprintf("azure://%s/concurrent-%d.txt", container, i)
		exists, err := fs.Exists(path)
		require.NoError(t, err)
		assert.True(t, exists)
	}
}

// TestAzureWriteWithConnectionString tests writing using connection string authentication.
func TestAzureWriteWithConnectionString(t *testing.T) {
	if azuriteEndpoint == "" {
		t.Skip("Azurite not available")
	}

	ctx := context.Background()

	// Build connection string for Azurite
	connStr := fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s",
		azuriteAccountName, azuriteAccountKey, azuriteEndpoint)

	config := NewAzureConfig(
		WithAzureConnectionString(connStr),
		WithAzureContainer(getAzuriteContainer()),
	)

	fs, err := NewAzureFileSystem(ctx, config)
	require.NoError(t, err)
	defer fs.Close()

	// Ensure container exists
	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Create a test file
	testPath := fmt.Sprintf("azure://%s/conn-str-test.txt", container)
	file, err := fs.Create(testPath)
	require.NoError(t, err)

	testData := []byte("Test with connection string auth")
	n, err := file.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n)

	err = file.Close()
	require.NoError(t, err)

	// Verify file was created
	exists, err := fs.Exists(testPath)
	require.NoError(t, err)
	assert.True(t, exists)
}

// TestAzureWriteOverwrite tests overwriting existing files.
func TestAzureWriteOverwrite(t *testing.T) {
	if azuriteEndpoint == "" {
		t.Skip("Azurite not available")
	}

	ctx := context.Background()
	config := getAzuriteConfig(t)
	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)
	defer fs.Close()

	// Ensure container exists
	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	testPath := fmt.Sprintf("azure://%s/overwrite-test.txt", container)

	// Create initial file
	file1, err := fs.Create(testPath)
	require.NoError(t, err)

	initialData := []byte("Initial content")
	_, err = file1.Write(initialData)
	require.NoError(t, err)
	err = file1.Close()
	require.NoError(t, err)

	// Overwrite with new content
	file2, err := fs.Create(testPath)
	require.NoError(t, err)

	newData := []byte("Overwritten content")
	_, err = file2.Write(newData)
	require.NoError(t, err)
	err = file2.Close()
	require.NoError(t, err)

	// Verify new content
	readFile, err := fs.Open(testPath)
	require.NoError(t, err)
	defer readFile.Close()

	buf := make([]byte, len(newData))
	_, err = readFile.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, newData, buf)
}

// TestAzureWriteEmptyFile tests writing empty files.
func TestAzureWriteEmptyFile(t *testing.T) {
	if azuriteEndpoint == "" {
		t.Skip("Azurite not available")
	}

	ctx := context.Background()
	config := getAzuriteConfig(t)
	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)
	defer fs.Close()

	// Ensure container exists
	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Create empty file
	testPath := fmt.Sprintf("azure://%s/empty-file.txt", container)
	file, err := fs.Create(testPath)
	require.NoError(t, err)

	// Close without writing anything
	err = file.Close()
	require.NoError(t, err)

	// Verify empty file was created
	info, err := fs.Stat(testPath)
	require.NoError(t, err)
	assert.Equal(t, int64(0), info.Size())
}

// TestAzureWriteWithPrefix tests writing to paths with directory-like prefixes.
func TestAzureWriteWithPrefix(t *testing.T) {
	if azuriteEndpoint == "" {
		t.Skip("Azurite not available")
	}

	ctx := context.Background()
	config := getAzuriteConfig(t)
	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)
	defer fs.Close()

	// Ensure container exists
	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Test various path formats
	testPaths := []string{
		fmt.Sprintf("azure://%s/data/file.txt", container),
		fmt.Sprintf("azure://%s/2024/01/15/file.txt", container),
		fmt.Sprintf("azure://%s/exports/sales/january.csv", container),
	}

	for _, path := range testPaths {
		t.Run("path: "+path, func(t *testing.T) {
			file, err := fs.Create(path)
			require.NoError(t, err)

			testData := []byte("Test data for " + path)
			_, err = file.Write(testData)
			require.NoError(t, err)

			err = file.Close()
			require.NoError(t, err)

			// Verify file exists
			exists, err := fs.Exists(path)
			require.NoError(t, err)
			assert.True(t, exists)
		})
	}
}

// TestAzureURLSchemes tests different Azure URL schemes.
func TestAzureURLSchemes(t *testing.T) {
	if azuriteEndpoint == "" {
		t.Skip("Azurite not available")
	}

	ctx := context.Background()
	config := getAzuriteConfig(t)
	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)
	defer fs.Close()

	// Ensure container exists
	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Test different URL schemes
	schemes := []string{"azure://", "az://"}

	for _, scheme := range schemes {
		t.Run("scheme: "+scheme, func(t *testing.T) {
			path := fmt.Sprintf("%s%s/scheme-test.txt", scheme, container)
			file, err := fs.Create(path)
			require.NoError(t, err)

			testData := []byte("Test with " + scheme + " scheme")
			_, err = file.Write(testData)
			require.NoError(t, err)

			err = file.Close()
			require.NoError(t, err)

			// Verify file exists
			exists, err := fs.Exists(path)
			require.NoError(t, err)
			assert.True(t, exists)
		})
	}
}

// TestAzureWriteWithContext tests write operations with context cancellation.
func TestAzureWriteWithContext(t *testing.T) {
	if azuriteEndpoint == "" {
		t.Skip("Azurite not available")
	}

	ctx := context.Background()
	config := getAzuriteConfig(t)
	fs, err := NewAzureFileSystem(ctx, *config)
	require.NoError(t, err)
	defer fs.Close()

	// Ensure container exists
	container := getAzuriteContainer()
	ensureAzuriteContainer(ctx, t, fs, container)

	// Test with cancelled context
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	testPath := fmt.Sprintf("azure://%s/context-test.txt", container)
	file, err := fs.CreateContext(cancelCtx, testPath)

	// Should either fail to create or fail on write
	if err == nil {
		_, err = file.Write([]byte("test"))
		if err == nil {
			err = file.Close()
		}
	}

	// We expect some kind of error due to cancelled context
	assert.Error(t, err)
}