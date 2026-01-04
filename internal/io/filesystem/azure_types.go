//go:build !js || !wasm

package filesystem

import (
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
)

// AzureFileInfo implements FileInfo for Azure blobs.
type AzureFileInfo struct {
	name     string
	size     int64
	modified time.Time
	props    *blob.GetPropertiesResponse
}

// NewAzureFileInfo creates a new AzureFileInfo from blob properties.
func NewAzureFileInfo(name string, props *blob.GetPropertiesResponse) *AzureFileInfo {
	fi := &AzureFileInfo{
		name:  name,
		props: props,
	}

	if props != nil {
		if props.ContentLength != nil {
			fi.size = *props.ContentLength
		}
		if props.LastModified != nil {
			fi.modified = *props.LastModified
		}
	}

	return fi
}

// NewAzureFileInfoFromSize creates a simple AzureFileInfo with name and size.
func NewAzureFileInfoFromSize(name string, size int64, modified time.Time) *AzureFileInfo {
	return &AzureFileInfo{
		name:     name,
		size:     size,
		modified: modified,
	}
}

// Name returns the base name of the blob.
func (fi *AzureFileInfo) Name() string {
	parts := strings.Split(fi.name, azurePathSeparator)
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}

	return fi.name
}

// Size returns the size of the blob in bytes.
func (fi *AzureFileInfo) Size() int64 {
	return fi.size
}

// Mode returns the file mode bits.
func (*AzureFileInfo) Mode() FileMode {
	return 0
}

// ModTime returns the modification time.
func (fi *AzureFileInfo) ModTime() time.Time {
	return fi.modified
}

// IsDir reports whether this is a directory.
func (*AzureFileInfo) IsDir() bool {
	return false
}

// Sys returns the underlying data source.
func (fi *AzureFileInfo) Sys() any {
	return fi.props
}

// AzureDirEntry implements DirEntry for Azure directory listings.
type AzureDirEntry struct {
	name     string
	isDir    bool
	size     int64
	modified time.Time
}

// NewAzureDirEntry creates a new AzureDirEntry.
func NewAzureDirEntry(name string, isDir bool, size int64, modified time.Time) *AzureDirEntry {
	return &AzureDirEntry{
		name:     name,
		isDir:    isDir,
		size:     size,
		modified: modified,
	}
}

// Name returns the base name of the entry.
func (de *AzureDirEntry) Name() string {
	return de.name
}

// IsDir reports whether the entry is a directory.
func (de *AzureDirEntry) IsDir() bool {
	return de.isDir
}

// Type returns the file mode bits.
func (de *AzureDirEntry) Type() FileMode {
	if de.isDir {
		return ModeDir
	}

	return 0
}

// Info returns the FileInfo for this entry.
func (de *AzureDirEntry) Info() (FileInfo, error) {
	return NewAzureFileInfoFromSize(de.name, de.size, de.modified), nil
}

// Verify AzureFileInfo implements FileInfo interface.
var _ FileInfo = (*AzureFileInfo)(nil)

// Verify AzureDirEntry implements DirEntry interface.
var _ DirEntry = (*AzureDirEntry)(nil)
