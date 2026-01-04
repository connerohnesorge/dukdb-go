// Package url provides URL parsing for cloud storage URLs.
// It supports S3, GCS, Azure, HTTP/HTTPS, and local file URLs with
// a unified API for extracting components like bucket, key, region, etc.
package url

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

// Common constants for URL parsing.
const (
	schemeFile  = "file"
	schemeS3    = "s3"
	schemeS3A   = "s3a"
	schemeS3N   = "s3n"
	schemeGCS   = "gcs"
	schemeGS    = "gs"
	schemeAzure = "azure"
	schemeAZ    = "az"
	schemeHTTP  = "http"
	schemeHTTPS = "https"

	schemeSeparator  = "://"
	pathSeparator    = "/"
	querySeparator   = "?"
	fragmentPrefix   = "#"
	filePrefixLen    = 7 // len("file://")
	minBucketNameLen = 3
	maxBucketNameLen = 63
	ipv4Parts        = 4
	splitLimitTwo    = 2
)

// knownSchemes is the list of supported cloud and network schemes.
var knownSchemes = []string{
	schemeS3, schemeS3A, schemeS3N,
	schemeGCS, schemeGS,
	schemeAzure, schemeAZ,
	schemeHTTP, schemeHTTPS,
	"hf", "huggingface",
}

// s3VirtualHostPattern matches virtual-host-style S3 URLs.
// Pattern: bucket.s3[.region].amazonaws.com or bucket.s3-region.amazonaws.com
var s3VirtualHostPattern = regexp.MustCompile(
	`^([a-z0-9][a-z0-9.-]*)\.s3[.-]([a-z0-9-]+)\.amazonaws\.com$`,
)

// ParsedURL represents a parsed cloud storage URL.
type ParsedURL struct {
	// Scheme is the URL scheme (s3, gs, azure, http, https, file, etc.).
	Scheme string
	// Authority is the host portion of the URL (bucket for S3 path-style, full host for HTTP).
	Authority string
	// Path is the path portion of the URL (key for S3, blob path for Azure).
	Path string
	// Query contains parsed query parameters.
	Query url.Values
	// Fragment is the URL fragment (after #).
	Fragment string
	// RawPath is the original unprocessed path.
	RawPath string
	// RawQuery is the original unparsed query string.
	RawQuery string

	// Derived fields for cloud storage
	bucket   string
	key      string
	region   string
	isVHost  bool
	endpoint string
}

// Parse parses a raw URL string into a ParsedURL.
// It handles cloud storage URLs (s3://, gs://, azure://, http://, https://)
// as well as local file paths and file:// URLs.
func Parse(rawURL string) (*ParsedURL, error) {
	if rawURL == "" {
		return nil, errors.New("empty URL")
	}

	// Handle duckdb:// prefix for local files
	input := strings.TrimPrefix(rawURL, "duckdb://")

	// Detect scheme and extract rest
	scheme, rest := detectScheme(input)

	result := &ParsedURL{
		Scheme:  scheme,
		Query:   make(url.Values),
		RawPath: rest,
	}

	// For file scheme, the rest is just the path
	if scheme == schemeFile {
		result.Path = rest
		result.parseFragment()

		return result, nil
	}

	// Parse authority and path for non-file schemes
	authority, path := parseAuthorityAndPath(rest)
	result.Authority = authority

	// Handle fragment for authority-only URLs (no path)
	if path == "" && strings.Contains(rest, fragmentPrefix) {
		if fIdx := strings.Index(rest, fragmentPrefix); fIdx >= 0 {
			result.Fragment = rest[fIdx+1:]
		}
	}

	// Parse query parameters and fragment from path
	result.parsePath(path)

	// Derive cloud-specific fields
	result.deriveCloudFields()

	return result, nil
}

// detectScheme finds the URL scheme and returns it along with the remainder.
func detectScheme(rawURL string) (scheme, remainder string) {
	lowerRaw := strings.ToLower(rawURL)

	for _, s := range knownSchemes {
		prefix := s + schemeSeparator
		if strings.HasPrefix(lowerRaw, prefix) {
			return s, rawURL[len(prefix):]
		}
	}

	// Handle file:// separately
	if strings.HasPrefix(lowerRaw, schemeFile+schemeSeparator) {
		return schemeFile, rawURL[filePrefixLen:]
	}

	return schemeFile, rawURL
}

// parseAuthorityAndPath splits the rest of the URL into authority and path.
func parseAuthorityAndPath(rest string) (authority, path string) {
	// Look for first slash to separate authority from path
	if idx := strings.Index(rest, pathSeparator); idx >= 0 {
		return rest[:idx], rest[idx:]
	}

	// No slash found, check for query or fragment
	if qIdx := strings.Index(rest, querySeparator); qIdx >= 0 {
		return rest[:qIdx], rest[qIdx:]
	}

	if fIdx := strings.Index(rest, fragmentPrefix); fIdx >= 0 {
		return rest[:fIdx], ""
	}

	// Just authority, no path
	return rest, ""
}

// parsePath extracts path, query, and fragment from the path portion.
func (u *ParsedURL) parsePath(pathStr string) {
	path := pathStr

	// Handle fragment
	if fIdx := strings.Index(path, fragmentPrefix); fIdx >= 0 {
		u.Fragment = path[fIdx+1:]
		path = path[:fIdx]
	}

	// Handle query parameters
	if qIdx := strings.Index(path, querySeparator); qIdx >= 0 {
		queryStr := path[qIdx+1:]
		u.RawQuery = queryStr
		path = path[:qIdx]

		if parsed, err := url.ParseQuery(queryStr); err == nil {
			u.Query = parsed
		}
	}

	u.Path = path
}

// parseFragment parses fragment from a file path.
func (u *ParsedURL) parseFragment() {
	if fIdx := strings.Index(u.Path, fragmentPrefix); fIdx >= 0 {
		u.Fragment = u.Path[fIdx+1:]
		u.Path = u.Path[:fIdx]
	}
}

// String returns the string representation of the URL.
func (u *ParsedURL) String() string {
	var result strings.Builder

	result.WriteString(u.Scheme)
	result.WriteString(schemeSeparator)

	if u.Authority != "" {
		result.WriteString(u.Authority)
	}

	result.WriteString(u.Path)

	if u.RawQuery != "" {
		result.WriteString(querySeparator)
		result.WriteString(u.RawQuery)
	} else if len(u.Query) > 0 {
		result.WriteString(querySeparator)
		result.WriteString(u.Query.Encode())
	}

	if u.Fragment != "" {
		result.WriteString(fragmentPrefix)
		result.WriteString(u.Fragment)
	}

	return result.String()
}
