package url

import (
	"net/url"
	"strings"
)

// deriveCloudFields extracts bucket, key, region, etc. based on the scheme.
func (u *ParsedURL) deriveCloudFields() {
	switch u.Scheme {
	case schemeS3, schemeS3A, schemeS3N:
		u.deriveS3Fields()
	case schemeGS, schemeGCS:
		u.deriveGCSFields()
	case schemeAzure, schemeAZ:
		u.deriveAzureFields()
	case schemeHTTP, schemeHTTPS:
		u.deriveHTTPFields()
	}
}

// deriveS3Fields extracts S3-specific fields.
func (u *ParsedURL) deriveS3Fields() {
	// Check for virtual-host-style URL
	// Pattern: bucket.s3.region.amazonaws.com or bucket.s3-region.amazonaws.com
	if matches := s3VirtualHostPattern.FindStringSubmatch(u.Authority); matches != nil {
		u.bucket = matches[1]
		u.region = matches[2]
		u.isVHost = true
		u.key = strings.TrimPrefix(u.Path, pathSeparator)

		return
	}

	// Check for endpoint override in authority (e.g., s3://endpoint:9000/bucket/key)
	if strings.Contains(u.Authority, ":") || strings.Contains(u.Authority, ".") {
		if u.tryParseEndpointStyle() {
			return
		}
	}

	// Path-style URL: s3://bucket/key
	u.bucket = u.Authority
	u.key = strings.TrimPrefix(u.Path, pathSeparator)

	// Check for region in query params
	if region := u.Query.Get("region"); region != "" {
		u.region = region
	}
}

// tryParseEndpointStyle attempts to parse as endpoint/bucket/key format.
// Returns true if successful.
func (u *ParsedURL) tryParseEndpointStyle() bool {
	pathParts := strings.SplitN(
		strings.TrimPrefix(u.Path, pathSeparator),
		pathSeparator,
		splitLimitTwo,
	)

	if len(pathParts) < 1 || pathParts[0] == "" {
		return false
	}

	// Check if authority looks like an endpoint (has dots but not a valid bucket)
	if !strings.Contains(u.Authority, ".") || isValidBucketName(u.Authority) {
		return false
	}

	u.endpoint = u.Authority
	u.bucket = pathParts[0]

	if len(pathParts) > 1 {
		u.key = pathParts[1]
	}

	return true
}

// deriveGCSFields extracts GCS-specific fields.
// GCS URLs follow the format: gs://bucket/object or gcs://bucket/object
func (u *ParsedURL) deriveGCSFields() {
	u.bucket = u.Authority
	u.key = strings.TrimPrefix(u.Path, pathSeparator)
}

// deriveAzureFields extracts Azure-specific fields.
// Supports both formats:
//   - azure://account.blob.core.windows.net/container/blob
//   - az://container/blob (simplified)
func (u *ParsedURL) deriveAzureFields() {
	if u.Scheme == schemeAZ {
		u.bucket = u.Authority
		u.key = strings.TrimPrefix(u.Path, pathSeparator)

		return
	}

	// Full format: azure://account.blob.core.windows.net/container/blob
	if strings.HasSuffix(u.Authority, ".blob.core.windows.net") {
		u.endpoint = u.Authority
		pathParts := strings.SplitN(
			strings.TrimPrefix(u.Path, pathSeparator),
			pathSeparator,
			splitLimitTwo,
		)

		if len(pathParts) < 1 {
			return
		}

		u.bucket = pathParts[0]

		if len(pathParts) > 1 {
			u.key = pathParts[1]
		}

		return
	}

	// Simple format: azure://container/blob
	u.bucket = u.Authority
	u.key = strings.TrimPrefix(u.Path, pathSeparator)
}

// deriveHTTPFields extracts HTTP-specific fields.
func (u *ParsedURL) deriveHTTPFields() {
	// For HTTP/HTTPS, the full URL is the key
	u.endpoint = u.Authority
	u.key = u.Path
}

// isValidBucketName checks if a string could be a valid S3 bucket name.
func isValidBucketName(name string) bool {
	// Bucket names must be 3-63 characters long
	if len(name) < minBucketNameLen || len(name) > maxBucketNameLen {
		return false
	}

	// Must not contain uppercase letters
	if name != strings.ToLower(name) {
		return false
	}

	// Must not look like an IP address
	parts := strings.Split(name, ".")
	if len(parts) == ipv4Parts {
		allNumeric := true

		for _, p := range parts {
			if !isNumeric(p) {
				allNumeric = false

				break
			}
		}

		if allNumeric {
			return false
		}
	}

	return true
}

// isNumeric checks if a string represents a numeric value.
func isNumeric(s string) bool {
	if s == "" {
		return false
	}

	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}

// IsCloudScheme returns true if the URL uses a cloud storage scheme.
func (u *ParsedURL) IsCloudScheme() bool {
	switch u.Scheme {
	case schemeS3,
		schemeS3A,
		schemeS3N,
		schemeGS,
		schemeGCS,
		schemeAzure,
		schemeAZ,
		schemeHTTP,
		schemeHTTPS:
		return true
	default:
		return false
	}
}

// IsS3 returns true if the URL uses an S3-compatible scheme.
func (u *ParsedURL) IsS3() bool {
	switch u.Scheme {
	case schemeS3, schemeS3A, schemeS3N:
		return true
	default:
		return false
	}
}

// IsGCS returns true if the URL uses a GCS scheme.
func (u *ParsedURL) IsGCS() bool {
	return u.Scheme == schemeGS || u.Scheme == schemeGCS
}

// IsAzure returns true if the URL uses an Azure scheme.
func (u *ParsedURL) IsAzure() bool {
	return u.Scheme == schemeAzure || u.Scheme == schemeAZ
}

// IsHTTP returns true if the URL uses an HTTP or HTTPS scheme.
func (u *ParsedURL) IsHTTP() bool {
	return u.Scheme == schemeHTTP || u.Scheme == schemeHTTPS
}

// IsLocal returns true if the URL uses a local file scheme.
func (u *ParsedURL) IsLocal() bool {
	return u.Scheme == schemeFile
}

// Bucket returns the bucket name for cloud storage URLs.
// For S3, this is the bucket name.
// For GCS, this is the bucket name.
// For Azure, this is the container name.
// For HTTP, this returns empty string.
func (u *ParsedURL) Bucket() string {
	return u.bucket
}

// Container is an alias for Bucket, used for Azure terminology.
func (u *ParsedURL) Container() string {
	return u.bucket
}

// Key returns the object key/path for cloud storage URLs.
// For S3, this is the object key.
// For GCS, this is the object name.
// For Azure, this is the blob path.
// For HTTP, this is the URL path.
func (u *ParsedURL) Key() string {
	return u.key
}

// ObjectPath is an alias for Key.
func (u *ParsedURL) ObjectPath() string {
	return u.key
}

// Region returns the region for the cloud storage URL.
// This may be extracted from the URL or query parameters.
func (u *ParsedURL) Region() string {
	return u.region
}

// Endpoint returns the endpoint URL for cloud storage.
// For S3, this is set for virtual-host-style URLs or custom endpoints.
// For Azure, this is the account.blob.core.windows.net host.
// For HTTP, this is the host authority.
func (u *ParsedURL) Endpoint() string {
	return u.endpoint
}

// IsVirtualHostStyle returns true if this is a virtual-host-style S3 URL.
func (u *ParsedURL) IsVirtualHostStyle() bool {
	return u.isVHost
}

// WithRegion returns a new ParsedURL with the specified region.
func (u *ParsedURL) WithRegion(region string) *ParsedURL {
	result := *u
	result.region = region
	result.Query = make(url.Values)

	for k, v := range u.Query {
		result.Query[k] = v
	}

	result.Query.Set("region", region)

	return &result
}

// WithBucket returns a new ParsedURL with the specified bucket.
func (u *ParsedURL) WithBucket(bucket string) *ParsedURL {
	result := *u
	result.bucket = bucket

	if !result.isVHost {
		result.Authority = bucket
	}

	return &result
}

// WithKey returns a new ParsedURL with the specified key.
func (u *ParsedURL) WithKey(key string) *ParsedURL {
	result := *u
	result.key = key

	if !result.isVHost && result.endpoint == "" {
		result.Path = pathSeparator + key
	}

	return &result
}

// Host returns the host portion of the URL.
// For cloud storage, this includes the bucket in virtual-host style.
func (u *ParsedURL) Host() string {
	return u.Authority
}

// FullPath returns the complete path including bucket/container for path-style URLs.
func (u *ParsedURL) FullPath() string {
	if u.bucket != "" && !u.isVHost && u.endpoint == "" {
		return pathSeparator + u.bucket + u.Path
	}

	return u.Path
}
