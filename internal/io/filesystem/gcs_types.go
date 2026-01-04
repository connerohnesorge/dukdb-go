//go:build !js || !wasm

package filesystem

import (
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

// GCSFileInfo implements FileInfo for GCS objects.
type GCSFileInfo struct {
	attrs *storage.ObjectAttrs
}

// Name returns the base name of the object.
func (fi *GCSFileInfo) Name() string {
	parts := strings.Split(fi.attrs.Name, gcsPathSeparator)
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}

	return fi.attrs.Name
}

// Size returns the size of the object in bytes.
func (fi *GCSFileInfo) Size() int64 {
	return fi.attrs.Size
}

// Mode returns the file mode bits.
func (*GCSFileInfo) Mode() FileMode {
	return 0
}

// ModTime returns the modification time.
func (fi *GCSFileInfo) ModTime() time.Time {
	return fi.attrs.Updated
}

// IsDir reports whether this is a directory.
func (*GCSFileInfo) IsDir() bool {
	return false
}

// Sys returns the underlying data source.
func (fi *GCSFileInfo) Sys() any {
	return fi.attrs
}

// GCSDirEntry implements DirEntry for GCS directory listings.
type GCSDirEntry struct {
	name  string
	isDir bool
	attrs *storage.ObjectAttrs
}

// Name returns the base name of the entry.
func (de *GCSDirEntry) Name() string {
	return de.name
}

// IsDir reports whether the entry is a directory.
func (de *GCSDirEntry) IsDir() bool {
	return de.isDir
}

// Type returns the file mode bits.
func (de *GCSDirEntry) Type() FileMode {
	if de.isDir {
		return ModeDir
	}

	return 0
}

// Info returns the FileInfo for this entry.
func (de *GCSDirEntry) Info() (FileInfo, error) {
	if de.attrs == nil {
		// For directory entries without attributes, return basic info
		return &GCSFileInfo{
			attrs: &storage.ObjectAttrs{
				Name: de.name,
			},
		}, nil
	}

	return &GCSFileInfo{attrs: de.attrs}, nil
}

// Verify GCSFileInfo implements FileInfo interface.
var _ FileInfo = (*GCSFileInfo)(nil)

// Verify GCSDirEntry implements DirEntry interface.
var _ DirEntry = (*GCSDirEntry)(nil)
