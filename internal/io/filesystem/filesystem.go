// Package filesystem provides a pluggable filesystem interface for cloud and local storage.
// It supports local files, S3, GCS, Azure, and HTTP/HTTPS protocols through a unified API.
package filesystem

import (
	"context"
	"io"
	"time"
)

// FileMode represents file mode and permission bits.
type FileMode uint32

const (
	// ModeDir indicates the file is a directory.
	ModeDir FileMode = 1 << 31
	// ModeAppend indicates the file is append-only.
	ModeAppend FileMode = 1 << 30
	// ModeSymlink indicates the file is a symbolic link.
	ModeSymlink FileMode = 1 << 27
)

// FileInfo describes a file and is returned by Stat.
type FileInfo interface {
	// Name returns the base name of the file.
	Name() string
	// Size returns the length in bytes for regular files.
	Size() int64
	// Mode returns the file mode bits.
	Mode() FileMode
	// ModTime returns the modification time.
	ModTime() time.Time
	// IsDir reports whether the file is a directory.
	IsDir() bool
	// Sys returns underlying data source (can return nil).
	Sys() any
}

// File represents an open file descriptor.
type File interface {
	io.ReadWriteSeeker
	io.ReaderAt
	io.WriterAt
	// Close closes the file and releases any resources.
	Close() error
	// Stat returns the FileInfo describing the file.
	Stat() (FileInfo, error)
}

// DirEntry is an entry read from a directory.
type DirEntry interface {
	// Name returns the base name of the file.
	Name() string
	// IsDir reports whether the entry describes a directory.
	IsDir() bool
	// Type returns the file mode bits for the file.
	Type() FileMode
	// Info returns the FileInfo for the file described by the entry.
	Info() (FileInfo, error)
}

// FileSystemCapabilities describes the capabilities of a filesystem.
type FileSystemCapabilities struct {
	// SupportsSeek indicates if the filesystem supports seeking.
	SupportsSeek bool
	// SupportsAppend indicates if the filesystem supports append operations.
	SupportsAppend bool
	// SupportsRange indicates if the filesystem supports range requests.
	SupportsRange bool
	// SupportsDirList indicates if the filesystem supports directory listing.
	SupportsDirList bool
	// SupportsWrite indicates if the filesystem supports write operations.
	SupportsWrite bool
	// SupportsDelete indicates if the filesystem supports delete operations.
	SupportsDelete bool
	// ContextTimeout indicates if the filesystem supports context cancellation.
	ContextTimeout bool
}

// FileSystem is the interface for a pluggable filesystem.
// Implementations provide access to local files, S3, GCS, Azure, or HTTP/HTTPS resources.
type FileSystem interface {
	// Open opens a file for reading.
	Open(path string) (File, error)
	// Create creates a file for writing.
	Create(path string) (File, error)
	// MkdirAll creates a directory along with any necessary parents.
	MkdirAll(path string) error
	// Stat returns file info for the given path.
	Stat(path string) (FileInfo, error)
	// Remove removes a file.
	Remove(path string) error
	// RemoveDir removes an empty directory.
	RemoveDir(path string) error
	// ReadDir reads the contents of a directory.
	ReadDir(path string) ([]DirEntry, error)
	// Exists returns true if the path exists.
	Exists(path string) (bool, error)
	// URI returns the base URI for this filesystem (e.g., "file://", "s3://bucket").
	URI() string
	// Capabilities returns the capabilities of this filesystem.
	Capabilities() FileSystemCapabilities
	// Glob expands a glob pattern to a list of matching file paths.
	// Supports wildcards: * (any characters), ? (single character), ** (recursive),
	// and character classes [abc], [a-z], [!abc].
	// Returns files sorted alphabetically.
	Glob(pattern string) ([]string, error)
	// SupportsGlob returns true if the filesystem has native glob support.
	// If false, callers should use FallbackGlob for glob expansion.
	SupportsGlob() bool
}

// FallbackGlob provides glob pattern expansion for filesystems that don't have native glob support.
// It uses the filesystem's ReadDir and Stat methods to walk the directory tree.
// This function should be used when SupportsGlob() returns false.
func FallbackGlob(fs FileSystem, pattern string) ([]string, error) {
	matcher := NewGlobMatcher(fs)

	return matcher.Match(pattern)
}

// ContextFile extends File with context-aware operations for cancellation support.
type ContextFile interface {
	File
	// ReadContext reads data with context support for cancellation.
	ReadContext(ctx context.Context, p []byte) (n int, err error)
	// WriteContext writes data with context support for cancellation.
	WriteContext(ctx context.Context, p []byte) (n int, err error)
}

// ContextFileSystem extends FileSystem with context-aware operations.
type ContextFileSystem interface {
	FileSystem
	// OpenContext opens a file for reading with context support.
	OpenContext(ctx context.Context, path string) (File, error)
	// CreateContext creates a file for writing with context support.
	CreateContext(ctx context.Context, path string) (File, error)
	// StatContext returns file info with context support.
	StatContext(ctx context.Context, path string) (FileInfo, error)
	// ReadDirContext reads directory contents with context support.
	ReadDirContext(ctx context.Context, path string) ([]DirEntry, error)
}
