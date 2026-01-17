//go:build !js || !wasm

package filesystem

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// dirPermission is the default permission mode for directories.
const dirPermission = 0o755

// LocalFileSystem implements FileSystem for the local filesystem.
type LocalFileSystem struct {
	// basePath is an optional base directory for relative paths.
	basePath string
}

// NewLocalFileSystem creates a new local filesystem with an optional base path.
// If basePath is empty, the filesystem operates on absolute paths.
func NewLocalFileSystem(basePath string) *LocalFileSystem {
	return &LocalFileSystem{basePath: basePath}
}

// resolvePath resolves a path relative to the base path.
func (l *LocalFileSystem) resolvePath(path string) string {
	// Strip file:// prefix if present
	cleanPath := strings.TrimPrefix(path, "file://")

	if l.basePath == "" || filepath.IsAbs(cleanPath) {
		return cleanPath
	}

	return filepath.Join(l.basePath, cleanPath)
}

// Open opens a file for reading.
func (l *LocalFileSystem) Open(path string) (File, error) {
	resolved := l.resolvePath(path)

	file, err := os.Open(resolved)
	if err != nil {
		return nil, fmt.Errorf("local: failed to open file: %w", err)
	}

	return &LocalFile{file: file}, nil
}

// Create creates a file for writing.
func (l *LocalFileSystem) Create(path string) (File, error) {
	resolved := l.resolvePath(path)

	// Ensure parent directory exists
	dir := filepath.Dir(resolved)
	if err := os.MkdirAll(dir, dirPermission); err != nil {
		return nil, fmt.Errorf("local: failed to create directory: %w", err)
	}

	file, err := os.Create(resolved)
	if err != nil {
		return nil, fmt.Errorf("local: failed to create file: %w", err)
	}

	return &LocalFile{file: file}, nil
}

// MkdirAll creates a directory along with any necessary parents.
func (l *LocalFileSystem) MkdirAll(path string) error {
	resolved := l.resolvePath(path)

	if err := os.MkdirAll(resolved, dirPermission); err != nil {
		return fmt.Errorf("local: failed to create directory: %w", err)
	}

	return nil
}

// Stat returns file info for the given path.
func (l *LocalFileSystem) Stat(path string) (FileInfo, error) {
	resolved := l.resolvePath(path)

	info, err := os.Stat(resolved)
	if err != nil {
		return nil, fmt.Errorf("local: failed to stat file: %w", err)
	}

	return &LocalFileInfo{info: info}, nil
}

// Remove removes a file.
func (l *LocalFileSystem) Remove(path string) error {
	resolved := l.resolvePath(path)

	if err := os.Remove(resolved); err != nil {
		return fmt.Errorf("local: failed to remove file: %w", err)
	}

	return nil
}

// RemoveDir removes an empty directory.
func (l *LocalFileSystem) RemoveDir(path string) error {
	resolved := l.resolvePath(path)

	if err := os.Remove(resolved); err != nil {
		return fmt.Errorf("local: failed to remove directory: %w", err)
	}

	return nil
}

// ReadDir reads the contents of a directory.
func (l *LocalFileSystem) ReadDir(path string) ([]DirEntry, error) {
	resolved := l.resolvePath(path)

	entries, err := os.ReadDir(resolved)
	if err != nil {
		return nil, fmt.Errorf("local: failed to read directory: %w", err)
	}

	result := make([]DirEntry, len(entries))
	for i, entry := range entries {
		result[i] = &LocalDirEntry{entry: entry}
	}

	return result, nil
}

// Exists returns true if the path exists.
func (l *LocalFileSystem) Exists(path string) (bool, error) {
	resolved := l.resolvePath(path)

	_, err := os.Stat(resolved)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, fmt.Errorf("local: failed to check existence: %w", err)
	}

	return true, nil
}

// URI returns the base URI for this filesystem.
func (l *LocalFileSystem) URI() string {
	if l.basePath != "" {
		return "file://" + l.basePath
	}

	return "file://"
}

// Capabilities returns the capabilities of this filesystem.
func (*LocalFileSystem) Capabilities() FileSystemCapabilities {
	return FileSystemCapabilities{
		SupportsSeek:    true,
		SupportsAppend:  true,
		SupportsRange:   true,
		SupportsDirList: true,
		SupportsWrite:   true,
		SupportsDelete:  true,
		ContextTimeout:  false,
	}
}

// Glob expands a glob pattern to a list of matching file paths.
// Uses the GlobMatcher for pattern expansion with full recursive support.
// The pattern should be relative to the filesystem's basePath (if any).
func (l *LocalFileSystem) Glob(pattern string) ([]string, error) {
	// Use the GlobMatcher for pattern expansion
	// The GlobMatcher uses the filesystem's ReadDir/Stat which already handle basePath resolution
	matcher := NewGlobMatcher(l)

	return matcher.Match(pattern)
}

// SupportsGlob returns true because LocalFileSystem has native glob support.
func (*LocalFileSystem) SupportsGlob() bool {
	return true
}

// LocalFile wraps os.File to implement the File interface.
type LocalFile struct {
	file *os.File
}

// Read reads data from the file.
func (lf *LocalFile) Read(p []byte) (n int, err error) {
	return lf.file.Read(p)
}

// Write writes data to the file.
func (lf *LocalFile) Write(p []byte) (n int, err error) {
	return lf.file.Write(p)
}

// Seek sets the offset for the next Read or Write.
func (lf *LocalFile) Seek(offset int64, whence int) (int64, error) {
	return lf.file.Seek(offset, whence)
}

// ReadAt reads data at the specified offset.
func (lf *LocalFile) ReadAt(p []byte, off int64) (n int, err error) {
	return lf.file.ReadAt(p, off)
}

// WriteAt writes data at the specified offset.
func (lf *LocalFile) WriteAt(p []byte, off int64) (n int, err error) {
	return lf.file.WriteAt(p, off)
}

// Close closes the file.
func (lf *LocalFile) Close() error {
	return lf.file.Close()
}

// Stat returns file info for this file.
func (lf *LocalFile) Stat() (FileInfo, error) {
	info, err := lf.file.Stat()
	if err != nil {
		return nil, err
	}

	return &LocalFileInfo{info: info}, nil
}

// LocalFileInfo wraps os.FileInfo to implement the FileInfo interface.
type LocalFileInfo struct {
	info os.FileInfo
}

// Name returns the base name of the file.
func (fi *LocalFileInfo) Name() string {
	return fi.info.Name()
}

// Size returns the length in bytes.
func (fi *LocalFileInfo) Size() int64 {
	return fi.info.Size()
}

// Mode returns the file mode bits.
func (fi *LocalFileInfo) Mode() FileMode {
	mode := fi.info.Mode()
	var fm FileMode

	if mode.IsDir() {
		fm |= ModeDir
	}

	if mode&fs.ModeAppend != 0 {
		fm |= ModeAppend
	}

	if mode&fs.ModeSymlink != 0 {
		fm |= ModeSymlink
	}

	return fm
}

// ModTime returns the modification time.
func (fi *LocalFileInfo) ModTime() time.Time {
	return fi.info.ModTime()
}

// IsDir reports whether the file is a directory.
func (fi *LocalFileInfo) IsDir() bool {
	return fi.info.IsDir()
}

// Sys returns underlying data source.
func (fi *LocalFileInfo) Sys() any {
	return fi.info.Sys()
}

// LocalDirEntry wraps fs.DirEntry to implement the DirEntry interface.
type LocalDirEntry struct {
	entry fs.DirEntry
}

// Name returns the base name of the file.
func (de *LocalDirEntry) Name() string {
	return de.entry.Name()
}

// IsDir reports whether the entry is a directory.
func (de *LocalDirEntry) IsDir() bool {
	return de.entry.IsDir()
}

// Type returns the file mode bits.
func (de *LocalDirEntry) Type() FileMode {
	mode := de.entry.Type()
	var fm FileMode

	if mode.IsDir() {
		fm |= ModeDir
	}

	if mode&fs.ModeAppend != 0 {
		fm |= ModeAppend
	}

	if mode&fs.ModeSymlink != 0 {
		fm |= ModeSymlink
	}

	return fm
}

// Info returns the FileInfo for this entry.
func (de *LocalDirEntry) Info() (FileInfo, error) {
	info, err := de.entry.Info()
	if err != nil {
		return nil, err
	}

	return &LocalFileInfo{info: info}, nil
}

// Verify LocalFileSystem implements FileSystem interface.
var _ FileSystem = (*LocalFileSystem)(nil)

// Verify LocalFile implements File interface.
var _ File = (*LocalFile)(nil)

// Verify LocalFileInfo implements FileInfo interface.
var _ FileInfo = (*LocalFileInfo)(nil)

// Verify LocalDirEntry implements DirEntry interface.
var _ DirEntry = (*LocalDirEntry)(nil)
