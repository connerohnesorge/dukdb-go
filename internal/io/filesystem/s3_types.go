//go:build !js || !wasm

package filesystem

import (
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// S3FileInfo implements FileInfo for S3 objects.
type S3FileInfo struct {
	info minio.ObjectInfo
}

// Name returns the base name of the object.
func (fi *S3FileInfo) Name() string {
	parts := strings.Split(fi.info.Key, s3PathSeparator)
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}

	return fi.info.Key
}

// Size returns the size of the object in bytes.
func (fi *S3FileInfo) Size() int64 {
	return fi.info.Size
}

// Mode returns the file mode bits.
func (*S3FileInfo) Mode() FileMode {
	return 0
}

// ModTime returns the modification time.
func (fi *S3FileInfo) ModTime() time.Time {
	return fi.info.LastModified
}

// IsDir reports whether this is a directory.
func (*S3FileInfo) IsDir() bool {
	return false
}

// Sys returns the underlying data source.
func (fi *S3FileInfo) Sys() any {
	return fi.info
}

// S3DirEntry implements DirEntry for S3 directory listings.
type S3DirEntry struct {
	name  string
	isDir bool
	info  minio.ObjectInfo
}

// Name returns the base name of the entry.
func (de *S3DirEntry) Name() string {
	return de.name
}

// IsDir reports whether the entry is a directory.
func (de *S3DirEntry) IsDir() bool {
	return de.isDir
}

// Type returns the file mode bits.
func (de *S3DirEntry) Type() FileMode {
	if de.isDir {
		return ModeDir
	}

	return 0
}

// Info returns the FileInfo for this entry.
func (de *S3DirEntry) Info() (FileInfo, error) {
	return &S3FileInfo{info: de.info}, nil
}

// Verify S3FileInfo implements FileInfo interface.
var _ FileInfo = (*S3FileInfo)(nil)

// Verify S3DirEntry implements DirEntry interface.
var _ DirEntry = (*S3DirEntry)(nil)
