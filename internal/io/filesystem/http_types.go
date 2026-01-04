package filesystem

import (
	"path"
	"strings"
	"time"
)

// HTTPFileInfo implements FileInfo for HTTP resources.
type HTTPFileInfo struct {
	// url is the full URL of the resource.
	url string
	// size is the content length in bytes.
	size int64
	// modTime is the last modification time.
	modTime time.Time
	// contentType is the MIME type of the resource.
	contentType string
	// etag is the ETag header value.
	etag string
	// acceptRanges indicates whether the server supports range requests.
	acceptRanges bool
}

// NewHTTPFileInfo creates a new HTTPFileInfo with the given parameters.
func NewHTTPFileInfo(url string, size int64, modTime time.Time) *HTTPFileInfo {
	return &HTTPFileInfo{
		url:     url,
		size:    size,
		modTime: modTime,
	}
}

// NewHTTPFileInfoFull creates a new HTTPFileInfo with all parameters.
func NewHTTPFileInfoFull(url string, size int64, modTime time.Time, contentType, etag string, acceptRanges bool) *HTTPFileInfo {
	return &HTTPFileInfo{
		url:          url,
		size:         size,
		modTime:      modTime,
		contentType:  contentType,
		etag:         etag,
		acceptRanges: acceptRanges,
	}
}

// Name returns the base name of the resource (last path component of URL).
func (fi *HTTPFileInfo) Name() string {
	// Extract the path from URL
	urlPath := fi.url
	if idx := strings.Index(urlPath, "://"); idx >= 0 {
		urlPath = urlPath[idx+3:]
	}
	if idx := strings.Index(urlPath, "/"); idx >= 0 {
		urlPath = urlPath[idx:]
	}
	if idx := strings.Index(urlPath, "?"); idx >= 0 {
		urlPath = urlPath[:idx]
	}

	name := path.Base(urlPath)
	if name == "." || name == "/" || name == "" {
		// Use host as name if no path
		host := fi.url
		if idx := strings.Index(host, "://"); idx >= 0 {
			host = host[idx+3:]
		}
		if idx := strings.Index(host, "/"); idx >= 0 {
			host = host[:idx]
		}
		if idx := strings.Index(host, ":"); idx >= 0 {
			host = host[:idx]
		}
		return host
	}

	return name
}

// Size returns the size of the resource in bytes.
func (fi *HTTPFileInfo) Size() int64 {
	return fi.size
}

// Mode returns the file mode bits.
// HTTP resources are always read-only files.
func (*HTTPFileInfo) Mode() FileMode {
	return 0
}

// ModTime returns the modification time.
func (fi *HTTPFileInfo) ModTime() time.Time {
	return fi.modTime
}

// IsDir reports whether this is a directory.
// HTTP resources are never directories.
func (*HTTPFileInfo) IsDir() bool {
	return false
}

// Sys returns the underlying data source.
func (fi *HTTPFileInfo) Sys() any {
	return map[string]any{
		"url":          fi.url,
		"contentType":  fi.contentType,
		"etag":         fi.etag,
		"acceptRanges": fi.acceptRanges,
	}
}

// ContentType returns the MIME type of the resource.
func (fi *HTTPFileInfo) ContentType() string {
	return fi.contentType
}

// ETag returns the ETag header value.
func (fi *HTTPFileInfo) ETag() string {
	return fi.etag
}

// AcceptRanges returns whether the server supports range requests.
func (fi *HTTPFileInfo) AcceptRanges() bool {
	return fi.acceptRanges
}

// URL returns the full URL of the resource.
func (fi *HTTPFileInfo) URL() string {
	return fi.url
}

// Verify HTTPFileInfo implements FileInfo interface.
var _ FileInfo = (*HTTPFileInfo)(nil)
