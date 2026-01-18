package filesystem

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// filePermission is the default permission for test files.
const filePermission = 0o644

// TestLocalFileSystem_Open tests opening files for reading.
func TestLocalFileSystem_Open(t *testing.T) {
	// Create a temp file
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.txt")
	content := []byte("hello, world!")

	err := os.WriteFile(tmpFile, content, filePermission)
	require.NoError(t, err)

	fs := NewLocalFileSystem("")

	// Open the file
	file, err := fs.Open(tmpFile)
	require.NoError(t, err)

	defer func() { _ = file.Close() }()

	// Read content
	buf := make([]byte, len(content))
	n, err := file.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, len(content), n)
	assert.Equal(t, content, buf)
}

// TestLocalFileSystem_Open_FileURLPrefix tests opening files with file:// prefix.
func TestLocalFileSystem_Open_FileURLPrefix(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.txt")
	content := []byte("hello from file URL!")

	err := os.WriteFile(tmpFile, content, filePermission)
	require.NoError(t, err)

	fs := NewLocalFileSystem("")

	// Open with file:// prefix
	file, err := fs.Open("file://" + tmpFile)
	require.NoError(t, err)

	defer func() { _ = file.Close() }()

	buf := make([]byte, len(content))
	n, err := file.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, len(content), n)
	assert.Equal(t, content, buf)
}

// TestLocalFileSystem_Create tests creating files for writing.
func TestLocalFileSystem_Create(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "new_file.txt")
	content := []byte("new content")

	fs := NewLocalFileSystem("")

	// Create the file
	file, err := fs.Create(tmpFile)
	require.NoError(t, err)

	// Write content
	n, err := file.Write(content)
	require.NoError(t, err)
	assert.Equal(t, len(content), n)

	err = file.Close()
	require.NoError(t, err)

	// Verify file was created
	readContent, err := os.ReadFile(tmpFile)
	require.NoError(t, err)
	assert.Equal(t, content, readContent)
}

// TestLocalFileSystem_Create_NestedDir tests creating files in nested directories.
func TestLocalFileSystem_Create_NestedDir(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "a", "b", "c", "file.txt")
	content := []byte("nested content")

	fs := NewLocalFileSystem("")

	// Create should create parent directories
	file, err := fs.Create(tmpFile)
	require.NoError(t, err)

	_, err = file.Write(content)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	// Verify
	readContent, err := os.ReadFile(tmpFile)
	require.NoError(t, err)
	assert.Equal(t, content, readContent)
}

// TestLocalFileSystem_Stat tests getting file info.
func TestLocalFileSystem_Stat(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.txt")
	content := []byte("stat test content")

	err := os.WriteFile(tmpFile, content, filePermission)
	require.NoError(t, err)

	fs := NewLocalFileSystem("")

	// Get file info
	info, err := fs.Stat(tmpFile)
	require.NoError(t, err)

	assert.Equal(t, "test.txt", info.Name())
	assert.Equal(t, int64(len(content)), info.Size())
	assert.False(t, info.IsDir())
}

// TestLocalFileSystem_Stat_Directory tests getting directory info.
func TestLocalFileSystem_Stat_Directory(t *testing.T) {
	tmpDir := t.TempDir()

	fs := NewLocalFileSystem("")

	info, err := fs.Stat(tmpDir)
	require.NoError(t, err)

	assert.True(t, info.IsDir())
	assert.True(t, info.Mode()&ModeDir != 0)
}

// TestLocalFileSystem_ReadDir tests reading directory contents.
func TestLocalFileSystem_ReadDir(t *testing.T) {
	tmpDir := t.TempDir()

	// Create some files and directories
	require.NoError(
		t,
		os.WriteFile(filepath.Join(tmpDir, "file1.txt"), []byte("1"), filePermission),
	)
	require.NoError(
		t,
		os.WriteFile(filepath.Join(tmpDir, "file2.txt"), []byte("2"), filePermission),
	)
	require.NoError(t, os.Mkdir(filepath.Join(tmpDir, "subdir"), dirPermission))

	fs := NewLocalFileSystem("")

	entries, err := fs.ReadDir(tmpDir)
	require.NoError(t, err)

	assert.Len(t, entries, 3)

	// Create a map for easy lookup
	names := make(map[string]bool)
	for _, entry := range entries {
		names[entry.Name()] = entry.IsDir()
	}

	assert.Contains(t, names, "file1.txt")
	assert.Contains(t, names, "file2.txt")
	assert.Contains(t, names, "subdir")
	assert.False(t, names["file1.txt"])
	assert.False(t, names["file2.txt"])
	assert.True(t, names["subdir"])
}

// TestLocalFileSystem_Exists tests checking file existence.
func TestLocalFileSystem_Exists(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "exists.txt")

	require.NoError(t, os.WriteFile(tmpFile, []byte("test"), filePermission))

	fs := NewLocalFileSystem("")

	// Existing file
	exists, err := fs.Exists(tmpFile)
	require.NoError(t, err)
	assert.True(t, exists)

	// Non-existing file
	exists, err = fs.Exists(filepath.Join(tmpDir, "nonexistent.txt"))
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestLocalFileSystem_Remove tests removing files.
func TestLocalFileSystem_Remove(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "to_delete.txt")

	require.NoError(t, os.WriteFile(tmpFile, []byte("delete me"), filePermission))

	fs := NewLocalFileSystem("")

	// File exists
	exists, err := fs.Exists(tmpFile)
	require.NoError(t, err)
	assert.True(t, exists)

	// Remove it
	err = fs.Remove(tmpFile)
	require.NoError(t, err)

	// File no longer exists
	exists, err = fs.Exists(tmpFile)
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestLocalFileSystem_MkdirAll tests creating directories.
func TestLocalFileSystem_MkdirAll(t *testing.T) {
	tmpDir := t.TempDir()
	newDir := filepath.Join(tmpDir, "a", "b", "c")

	fs := NewLocalFileSystem("")

	err := fs.MkdirAll(newDir)
	require.NoError(t, err)

	info, err := os.Stat(newDir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

// TestLocalFileSystem_RemoveDir tests removing directories.
func TestLocalFileSystem_RemoveDir(t *testing.T) {
	tmpDir := t.TempDir()
	newDir := filepath.Join(tmpDir, "empty_dir")

	require.NoError(t, os.Mkdir(newDir, dirPermission))

	fs := NewLocalFileSystem("")

	err := fs.RemoveDir(newDir)
	require.NoError(t, err)

	exists, err := fs.Exists(newDir)
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestLocalFileSystem_URI tests the URI method.
func TestLocalFileSystem_URI(t *testing.T) {
	fs1 := NewLocalFileSystem("")
	assert.Equal(t, "file://", fs1.URI())

	fs2 := NewLocalFileSystem("/tmp/base")
	assert.Equal(t, "file:///tmp/base", fs2.URI())
}

// TestLocalFileSystem_Capabilities tests the Capabilities method.
func TestLocalFileSystem_Capabilities(t *testing.T) {
	fs := NewLocalFileSystem("")
	caps := fs.Capabilities()

	assert.True(t, caps.SupportsSeek)
	assert.True(t, caps.SupportsAppend)
	assert.True(t, caps.SupportsRange)
	assert.True(t, caps.SupportsDirList)
	assert.True(t, caps.SupportsWrite)
	assert.True(t, caps.SupportsDelete)
	assert.False(t, caps.ContextTimeout)
}

// TestLocalFile_ReadAt tests reading at specific offsets.
func TestLocalFile_ReadAt(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "readat.txt")
	content := []byte("0123456789")

	require.NoError(t, os.WriteFile(tmpFile, content, filePermission))

	fs := NewLocalFileSystem("")

	file, err := fs.Open(tmpFile)
	require.NoError(t, err)

	defer func() { _ = file.Close() }()

	// Read at offset 5
	buf := make([]byte, 3)
	n, err := file.ReadAt(buf, 5)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("567"), buf)

	// Read at offset 0
	n, err = file.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("012"), buf)
}

// TestLocalFile_Seek tests seeking within a file.
func TestLocalFile_Seek(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "seek.txt")
	content := []byte("abcdefghij")

	require.NoError(t, os.WriteFile(tmpFile, content, filePermission))

	fs := NewLocalFileSystem("")

	file, err := fs.Open(tmpFile)
	require.NoError(t, err)

	defer func() { _ = file.Close() }()

	// Seek to position 5
	pos, err := file.Seek(5, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(5), pos)

	// Read from position 5
	buf := make([]byte, 3)
	n, err := file.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("fgh"), buf)
}

// TestLocalFile_Stat tests getting file info from an open file.
func TestLocalFile_Stat(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "stat.txt")
	content := []byte("stat content")

	require.NoError(t, os.WriteFile(tmpFile, content, filePermission))

	fs := NewLocalFileSystem("")

	file, err := fs.Open(tmpFile)
	require.NoError(t, err)

	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	require.NoError(t, err)

	assert.Equal(t, "stat.txt", info.Name())
	assert.Equal(t, int64(len(content)), info.Size())
}

// TestLocalDirEntry_Info tests getting FileInfo from a DirEntry.
func TestLocalDirEntry_Info(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "entry.txt")
	content := []byte("entry content")

	require.NoError(t, os.WriteFile(tmpFile, content, filePermission))

	fs := NewLocalFileSystem("")

	entries, err := fs.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	info, err := entries[0].Info()
	require.NoError(t, err)

	assert.Equal(t, "entry.txt", info.Name())
	assert.Equal(t, int64(len(content)), info.Size())
}

// TestFileSystemFactory_GetFileSystem tests the factory.
func TestFileSystemFactory_GetFileSystem(t *testing.T) {
	ctx := context.Background()

	fsFactory := NewFileSystemFactory()

	// Get filesystem for local path
	fs, err := fsFactory.GetFileSystem(ctx, "/tmp/test.txt")
	require.NoError(t, err)
	assert.NotNil(t, fs)

	// Get filesystem for file:// URL
	fs, err = fsFactory.GetFileSystem(ctx, "file:///tmp/test.txt")
	require.NoError(t, err)
	assert.NotNil(t, fs)
}

// TestFileSystemFactory_UnsupportedScheme tests handling of unsupported schemes.
func TestFileSystemFactory_UnsupportedScheme(t *testing.T) {
	ctx := context.Background()

	fsFactory := NewFileSystemFactory()

	// Try unsupported scheme
	_, err := fsFactory.GetFileSystem(ctx, "ftp://example.com/file.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported scheme")
}

// TestFileSystemFactory_RegisterScheme tests registering custom schemes.
func TestFileSystemFactory_RegisterScheme(t *testing.T) {
	ctx := context.Background()

	fsFactory := NewFileSystemFactory()

	// Register a custom scheme
	customCalled := false
	fsFactory.RegisterScheme("custom", func(_ context.Context, _ string) (FileSystem, error) {
		customCalled = true

		return NewLocalFileSystem(""), nil
	})

	// Use custom scheme
	fs, err := fsFactory.GetFileSystem(ctx, "custom://path/to/file")
	require.NoError(t, err)
	assert.NotNil(t, fs)
	assert.True(t, customCalled)
}

// TestFileSystemFactory_SupportedSchemes tests listing supported schemes.
func TestFileSystemFactory_SupportedSchemes(t *testing.T) {
	fsFactory := NewFileSystemFactory()

	schemes := fsFactory.SupportedSchemes()
	assert.Contains(t, schemes, "file")
}

// TestFileSystemFactory_UnregisterScheme tests unregistering schemes.
func TestFileSystemFactory_UnregisterScheme(t *testing.T) {
	ctx := context.Background()

	fsFactory := NewFileSystemFactory()

	// Unregister file scheme
	fsFactory.UnregisterScheme("file")

	// Now file:// should fail
	_, err := fsFactory.GetFileSystem(ctx, "file:///tmp/test.txt")
	require.Error(t, err)
}

// TestIsCloudURL tests URL type detection.
func TestIsCloudURL(t *testing.T) {
	tests := []struct {
		url      string
		expected bool
	}{
		{"s3://bucket/key", true},
		{"s3a://bucket/key", true},
		{"gs://bucket/object", true},
		{"gcs://bucket/object", true},
		{"azure://container/blob", true},
		{"az://container/blob", true},
		{"http://example.com/file", true},
		{"https://example.com/file", true},
		{"file:///tmp/file.txt", false},
		{"/tmp/file.txt", false},
		{"./relative/path", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			result := IsCloudURL(tt.url)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsLocalURL tests local URL detection.
func TestIsLocalURL(t *testing.T) {
	tests := []struct {
		url      string
		expected bool
	}{
		{"file:///tmp/file.txt", true},
		{"/tmp/file.txt", true},
		{"./relative/path", true},
		{"s3://bucket/key", false},
		{"https://example.com/file", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			result := IsLocalURL(tt.url)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractScheme tests scheme extraction.
func TestExtractScheme(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{"s3://bucket/key", "s3"},
		{"file:///tmp/file", "file"},
		{"https://example.com", "https"},
		{"/absolute/path", ""},
		{"relative/path", ""},
		{"S3://BUCKET/KEY", "s3"}, // Case insensitive
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			result := extractScheme(tt.url)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestGetFileSystem_Convenience tests the convenience function.
func TestGetFileSystem_Convenience(t *testing.T) {
	ctx := context.Background()

	fs, err := GetFileSystem(ctx, "/tmp/test.txt")
	require.NoError(t, err)
	assert.NotNil(t, fs)
}

// TestLocalFileSystem_BasePath tests filesystem with base path.
func TestLocalFileSystem_BasePath(t *testing.T) {
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "subdir")

	require.NoError(t, os.Mkdir(subDir, dirPermission))
	require.NoError(
		t,
		os.WriteFile(filepath.Join(subDir, "file.txt"), []byte("test"), filePermission),
	)

	fs := NewLocalFileSystem(subDir)

	// Open relative path
	file, err := fs.Open("file.txt")
	require.NoError(t, err)

	defer func() { _ = file.Close() }()

	buf := make([]byte, 4)
	n, err := file.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("test"), buf)
}
