//go:build !js || !wasm

// Package filesystem provides S3FileSystem implementation for Amazon S3 and S3-compatible stores.
// The S3FileSystem implements the FileSystem interface using the minio-go client library.
// It supports read, write, stat, remove, and directory listing operations on S3 buckets.
package filesystem

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3FileSystem implements FileSystem for Amazon S3 and S3-compatible stores.
// It uses the minio-go client library for all S3 operations.
// S3FileSystem supports path-style and virtual-host-style bucket addressing.
type S3FileSystem struct {
	// client is the underlying minio client for S3 operations.
	client *minio.Client
	// config holds the S3 configuration used to create this filesystem.
	config S3Config
	// regionCache caches detected regions for buckets.
	regionCache sync.Map
}

// NewS3FileSystem creates a new S3 filesystem with the given configuration.
// The context parameter is currently unused but reserved for future use.
// Returns an error if the minio client cannot be created.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity and immutability
func NewS3FileSystem(_ context.Context, config S3Config) (*S3FileSystem, error) {
	endpoint := buildEndpoint(&config)
	creds := buildCredentials(&config)
	opts := buildClientOptions(&config, creds)

	client, err := minio.New(endpoint, opts)
	if err != nil {
		return nil, fmt.Errorf("s3: failed to create client: %w", err)
	}

	return &S3FileSystem{client: client, config: config}, nil
}

// NewS3FileSystemWithClient creates a new S3 filesystem with a pre-configured client.
// This is useful for testing with mock clients or for sharing clients across filesystems.
// If config is nil, uses default S3 configuration values.
func NewS3FileSystemWithClient(client *minio.Client, config *S3Config) *S3FileSystem {
	cfg := S3Config{}
	if config != nil {
		cfg = *config
	}

	return &S3FileSystem{client: client, config: cfg}
}

// buildEndpoint determines the S3 endpoint URL from the configuration.
// If Endpoint is set, uses that. Otherwise, constructs AWS S3 endpoint from Region.
func buildEndpoint(config *S3Config) string {
	if config.Endpoint != "" {
		return config.Endpoint
	}

	if config.Region != "" {
		return fmt.Sprintf("s3.%s.amazonaws.com", config.Region)
	}

	return "s3.amazonaws.com"
}

// buildCredentials creates a credentials provider from the configuration.
// Uses static credentials if AccessKeyID and SecretAccessKey are provided.
// Otherwise, uses the default credential chain (env vars, IAM role, etc.).
func buildCredentials(config *S3Config) *credentials.Credentials {
	if config.AccessKeyID != "" && config.SecretAccessKey != "" {
		return credentials.NewStaticV4(
			config.AccessKeyID,
			config.SecretAccessKey,
			config.SessionToken,
		)
	}

	return credentials.NewChainCredentials([]credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.EnvMinio{},
		&credentials.FileAWSCredentials{},
		&credentials.IAM{},
	})
}

// buildClientOptions creates minio client options from the configuration.
// Sets up TLS, region, and bucket lookup type based on URL style preference.
func buildClientOptions(config *S3Config, creds *credentials.Credentials) *minio.Options {
	opts := &minio.Options{
		Creds:  creds,
		Secure: config.UseSSL,
		Region: config.Region,
	}

	if config.URLStyle == S3URLStylePath {
		opts.BucketLookup = minio.BucketLookupPath
	} else {
		opts.BucketLookup = minio.BucketLookupAuto
	}

	if config.BucketLookup != 0 {
		opts.BucketLookup = config.BucketLookup
	}

	return opts
}

// parseS3Path parses an S3 URL into bucket and key components.
// Supports s3://, s3a://, and s3n:// URL schemes.
// Also handles raw bucket/key paths without a scheme prefix.
func parseS3Path(path string) (bucket, key string, err error) {
	cleanPath := stripS3Prefix(path)

	// Only parse as URL if '?' is followed by something that looks like a query parameter
	// (i.e., it's not a glob wildcard pattern). A '?' is a glob wildcard if it's not
	// preceded by a real query parameter name.
	if strings.Contains(cleanPath, "?") {
		// Check if this looks like a query parameter (e.g., ?key=value) vs glob wildcard
		// Split on the first '?' to check what comes before and after
		parts := strings.SplitN(cleanPath, "?", 2)
		if len(parts) == 2 {
			afterQuestion := parts[1]
			// If what follows looks like query parameters (contains = or &), parse as URL
			// Otherwise, treat the ? as a glob wildcard and don't parse as URL
			if strings.Contains(afterQuestion, "=") || strings.Contains(afterQuestion, "&") {
				parsed, err := url.Parse("s3://" + cleanPath)
				if err != nil {
					return "", "", fmt.Errorf("s3: failed to parse URL: %w", err)
				}
				cleanPath = parsed.Host + parsed.Path
			}
			// If no = or &, the ? is likely a glob wildcard, so don't parse as URL
		}
	}

	parts := strings.SplitN(cleanPath, s3PathSeparator, 2)
	if len(parts) == 0 || parts[0] == "" {
		return "", "", errors.New("s3: bucket name is required")
	}

	bucket = parts[0]
	if len(parts) > 1 {
		key = parts[1]
	}

	return bucket, key, nil
}

// stripS3Prefix removes s3://, s3a://, or s3n:// prefix from a path.
// Returns the original path if no recognized prefix is found.
func stripS3Prefix(path string) string {
	for _, prefix := range []string{"s3://", "s3a://", "s3n://"} {
		if strings.HasPrefix(strings.ToLower(path), prefix) {
			return path[len(prefix):]
		}
	}

	return path
}

// Open opens an S3 object for reading.
// Returns a File that streams data from S3 on Read calls.
func (fs *S3FileSystem) Open(path string) (File, error) {
	bucket, key, err := parseS3Path(path)
	if err != nil {
		return nil, err
	}

	return newS3FileForReading(fs.client, bucket, key), nil
}

// Create creates an S3 object for writing.
// Data is buffered in memory and uploaded when Close() is called.
func (fs *S3FileSystem) Create(path string) (File, error) {
	bucket, key, err := parseS3Path(path)
	if err != nil {
		return nil, err
	}

	return newS3FileForWriting(fs.client, bucket, key), nil
}

// MkdirAll is a no-op for S3 since directories are implicit.
// S3 uses key prefixes as virtual directories.
func (*S3FileSystem) MkdirAll(_ string) error {
	return nil
}

// Stat returns file info for an S3 object.
// Uses HeadObject to retrieve metadata without downloading the object.
func (fs *S3FileSystem) Stat(path string) (FileInfo, error) {
	bucket, key, err := parseS3Path(path)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	info, err := fs.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("s3: failed to stat object: %w", err)
	}

	return &S3FileInfo{info: info}, nil
}

// Remove removes an S3 object.
// Returns an error if the delete operation fails.
func (fs *S3FileSystem) Remove(path string) error {
	bucket, key, err := parseS3Path(path)
	if err != nil {
		return err
	}

	ctx := context.Background()

	err = fs.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("s3: failed to remove object: %w", err)
	}

	return nil
}

// RemoveDir removes an S3 "directory" (all objects with a given prefix).
// Lists all objects with the prefix and deletes them one by one.
func (fs *S3FileSystem) RemoveDir(path string) error {
	bucket, prefix, err := parseS3Path(path)
	if err != nil {
		return err
	}

	if !strings.HasSuffix(prefix, s3PathSeparator) {
		prefix += s3PathSeparator
	}

	ctx := context.Background()
	objectsCh := fs.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	for obj := range objectsCh {
		if obj.Err != nil {
			return fmt.Errorf("s3: failed to list objects: %w", obj.Err)
		}

		err := fs.client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
		if err != nil {
			return fmt.Errorf("s3: failed to remove object %s: %w", obj.Key, err)
		}
	}

	return nil
}

// ReadDir reads the contents of an S3 "directory" (objects with a given prefix).
// Uses ListObjects with Recursive=false to list only direct children.
func (fs *S3FileSystem) ReadDir(path string) ([]DirEntry, error) {
	bucket, prefix, err := parseS3Path(path)
	if err != nil {
		return nil, err
	}

	if prefix != "" && !strings.HasSuffix(prefix, s3PathSeparator) {
		prefix += s3PathSeparator
	}

	ctx := context.Background()
	objectsCh := fs.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})

	entries := make([]DirEntry, 0)
	for obj := range objectsCh {
		if obj.Err != nil {
			return nil, fmt.Errorf("s3: failed to list objects: %w", obj.Err)
		}

		if obj.Key == prefix {
			continue
		}

		name := strings.TrimPrefix(obj.Key, prefix)
		isDir := strings.HasSuffix(name, s3PathSeparator)
		if isDir {
			name = strings.TrimSuffix(name, s3PathSeparator)
		}

		entries = append(entries, &S3DirEntry{name: name, isDir: isDir, info: obj})
	}

	return entries, nil
}

// Exists checks if an S3 object exists.
// Uses HeadObject and checks for NoSuchKey or NotFound error codes.
func (fs *S3FileSystem) Exists(path string) (bool, error) {
	bucket, key, err := parseS3Path(path)
	if err != nil {
		return false, err
	}

	ctx := context.Background()

	_, err = fs.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		errResp := minio.ToErrorResponse(err)
		if errResp.Code == "NoSuchKey" || errResp.Code == "NotFound" {
			return false, nil
		}

		return false, fmt.Errorf("s3: failed to check existence: %w", err)
	}

	return true, nil
}

// URI returns the base URI for this filesystem.
// Always returns "s3://" for S3 filesystems.
func (*S3FileSystem) URI() string {
	return "s3://"
}

// Capabilities returns the capabilities of this filesystem.
// S3 supports seek (via range requests), range reads, directory listing,
// write, and delete operations. Append is not supported.
func (*S3FileSystem) Capabilities() FileSystemCapabilities {
	return FileSystemCapabilities{
		SupportsSeek:    true,
		SupportsAppend:  false,
		SupportsRange:   true,
		SupportsDirList: true,
		SupportsWrite:   true,
		SupportsDelete:  true,
		ContextTimeout:  true,
	}
}

// Glob expands a glob pattern to a list of matching S3 object keys.
// Uses ListObjectsV2 with prefix optimization to efficiently list objects.
// Handles pagination automatically for buckets with more than 1000 objects.
// Applies retry logic with exponential backoff for rate limiting.
func (fs *S3FileSystem) Glob(pattern string) ([]string, error) {
	return fs.GlobContext(context.Background(), pattern)
}

// GlobContext expands a glob pattern with context support for cancellation.
func (fs *S3FileSystem) GlobContext(ctx context.Context, pattern string) ([]string, error) {
	// Validate the pattern
	if err := ValidateGlobPattern(pattern); err != nil {
		return nil, err
	}

	// Normalize path separators
	pattern = normalizePath(pattern)

	// Parse S3 path from pattern to extract bucket and key pattern
	bucket, keyPattern, err := parseS3Path(pattern)
	if err != nil {
		return nil, fmt.Errorf("s3: glob parse error: %w", err)
	}

	// If no glob characters, check if the object exists
	if !ContainsGlobPattern(keyPattern) {
		exists, err := fs.Exists(pattern)
		if err != nil {
			return nil, fmt.Errorf("s3: checking path existence: %w", err)
		}
		if exists {
			return []string{pattern}, nil
		}
		return nil, nil
	}

	// Extract the prefix (literal part before any wildcards) for optimization
	prefix := ExtractPrefix(keyPattern)

	// List objects with prefix optimization
	matches, err := fs.listObjectsWithPrefix(ctx, bucket, prefix, keyPattern)
	if err != nil {
		return nil, err
	}

	// Sort results alphabetically
	sort.Strings(matches)

	return matches, nil
}

// listObjectsWithPrefix lists S3 objects using the given prefix and filters by pattern.
// Handles pagination automatically for buckets with more than 1000 objects.
// Uses retry logic with exponential backoff for transient errors.
func (fs *S3FileSystem) listObjectsWithPrefix(
	ctx context.Context,
	bucket, prefix, pattern string,
) ([]string, error) {
	var matches []string

	// Create the list operation function for retry
	listFn := func(continuationToken string) ([]string, string, error) {
		var localMatches []string
		var nextToken string

		opts := minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true, // List all objects with the prefix recursively
		}

		// Handle pagination using minio's iterator
		objectsCh := fs.client.ListObjects(ctx, bucket, opts)

		for obj := range objectsCh {
			if obj.Err != nil {
				return nil, "", fmt.Errorf("s3: failed to list objects: %w", obj.Err)
			}

			// Skip "directory" markers (keys ending with /)
			if strings.HasSuffix(obj.Key, s3PathSeparator) {
				continue
			}

			// Check if the object key matches the pattern
			fullPath := "s3://" + bucket + s3PathSeparator + obj.Key
			matched, err := MatchPattern(pattern, obj.Key)
			if err != nil {
				return nil, "", fmt.Errorf("s3: pattern match error: %w", err)
			}
			if matched {
				localMatches = append(localMatches, fullPath)
			}
		}

		return localMatches, nextToken, nil
	}

	// Use retry logic if configured
	if fs.config.RetryConfig.MaxRetries > 0 {
		result := WithRetry(ctx, fs.config.RetryConfig, func() ([]string, error) {
			m, _, err := listFn("")
			return m, err
		})
		if result.LastError != nil {
			return nil, result.LastError
		}
		matches = result.Value
	} else {
		m, _, err := listFn("")
		if err != nil {
			return nil, err
		}
		matches = m
	}

	return matches, nil
}

// SupportsGlob returns true because S3FileSystem has native glob support with prefix optimization.
func (*S3FileSystem) SupportsGlob() bool {
	return true
}

// Verify S3FileSystem implements FileSystem interface at compile time.
var _ FileSystem = (*S3FileSystem)(nil)

// OpenContext opens an S3 object for reading with context support.
func (fs *S3FileSystem) OpenContext(ctx context.Context, path string) (File, error) {
	bucket, key, err := parseS3Path(path)
	if err != nil {
		return nil, err
	}

	// Auto-detect region if configured
	if fs.config.AutoDetectRegion {
		if _, err := fs.detectBucketRegion(ctx, bucket); err != nil {
			// Log warning but continue - region detection is best-effort
			_ = err
		}
	}

	return newS3FileForReadingWithConfig(fs.client, bucket, key, fs.config), nil
}

// CreateContext creates an S3 object for writing with context support.
func (fs *S3FileSystem) CreateContext(ctx context.Context, path string) (File, error) {
	bucket, key, err := parseS3Path(path)
	if err != nil {
		return nil, err
	}

	// Auto-detect region if configured
	if fs.config.AutoDetectRegion {
		if _, err := fs.detectBucketRegion(ctx, bucket); err != nil {
			_ = err
		}
	}

	return newS3FileForWritingWithConfig(fs.client, bucket, key, fs.config), nil
}

// CreateMultipartWriter creates a streaming multipart writer for large uploads.
// This is more efficient than buffering the entire file in memory.
func (fs *S3FileSystem) CreateMultipartWriter(
	ctx context.Context,
	path string,
) (*MultipartWriter, error) {
	bucket, key, err := parseS3Path(path)
	if err != nil {
		return nil, err
	}

	return NewMultipartWriter(ctx, fs.client, bucket, key, fs.config), nil
}

// CreateConcurrentReader creates a concurrent reader for efficient large file reads.
func (fs *S3FileSystem) CreateConcurrentReader(
	ctx context.Context,
	path string,
) (*ConcurrentReader, error) {
	bucket, key, err := parseS3Path(path)
	if err != nil {
		return nil, err
	}

	// Get file size for concurrent reads
	info, err := fs.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("s3: failed to stat object: %w", err)
	}

	return NewConcurrentReader(fs.client, bucket, key, info.Size, fs.config), nil
}

// StatContext returns file info with context support.
func (fs *S3FileSystem) StatContext(ctx context.Context, path string) (FileInfo, error) {
	bucket, key, err := parseS3Path(path)
	if err != nil {
		return nil, err
	}

	statFn := func() (FileInfo, error) {
		info, err := fs.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("s3: failed to stat object: %w", err)
		}
		return &S3FileInfo{info: info}, nil
	}

	// Apply retry logic if configured
	if fs.config.RetryConfig.MaxRetries > 0 {
		return WithRetryFunc(ctx, fs.config.RetryConfig, statFn)
	}

	return statFn()
}

// ReadDirContext reads directory contents with context support.
func (fs *S3FileSystem) ReadDirContext(ctx context.Context, path string) ([]DirEntry, error) {
	bucket, prefix, err := parseS3Path(path)
	if err != nil {
		return nil, err
	}

	if prefix != "" && !strings.HasSuffix(prefix, s3PathSeparator) {
		prefix += s3PathSeparator
	}

	objectsCh := fs.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})

	entries := make([]DirEntry, 0)
	for obj := range objectsCh {
		if obj.Err != nil {
			return nil, fmt.Errorf("s3: failed to list objects: %w", obj.Err)
		}

		if obj.Key == prefix {
			continue
		}

		name := strings.TrimPrefix(obj.Key, prefix)
		isDir := strings.HasSuffix(name, s3PathSeparator)
		if isDir {
			name = strings.TrimSuffix(name, s3PathSeparator)
		}

		entries = append(entries, &S3DirEntry{name: name, isDir: isDir, info: obj})
	}

	return entries, nil
}

// detectBucketRegion detects the region for a bucket using the GetBucketLocation API.
// Results are cached to avoid repeated API calls.
func (fs *S3FileSystem) detectBucketRegion(ctx context.Context, bucket string) (string, error) {
	// Check cache first
	if cached, ok := fs.regionCache.Load(bucket); ok {
		return cached.(string), nil
	}

	// Try to get bucket location
	region, err := fs.client.GetBucketLocation(ctx, bucket)
	if err != nil {
		// If GetBucketLocation fails, try HEAD request method
		region, err = fs.detectRegionFromHead(ctx, bucket)
		if err != nil {
			return "", fmt.Errorf("s3: failed to detect region for bucket %s: %w", bucket, err)
		}
	}

	// Empty region means us-east-1
	if region == "" {
		region = "us-east-1"
	}

	// Cache the result
	fs.regionCache.Store(bucket, region)

	return region, nil
}

// detectRegionFromHead tries to detect bucket region from a HEAD request error response.
// This is a fallback method when GetBucketLocation is not available.
func (fs *S3FileSystem) detectRegionFromHead(ctx context.Context, bucket string) (string, error) {
	// Make a HEAD request to the bucket
	endpoint := "s3.amazonaws.com"
	if fs.config.Endpoint != "" {
		endpoint = fs.config.Endpoint
	}

	scheme := "https"
	if !fs.config.UseSSL {
		scheme = "http"
	}

	url := fmt.Sprintf("%s://%s/%s", scheme, endpoint, bucket)

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, http.NoBody)
	if err != nil {
		return "", err
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	// Check for region header in response
	if region := resp.Header.Get("x-amz-bucket-region"); region != "" {
		return region, nil
	}

	// Default to us-east-1
	return "us-east-1", nil
}

// GetConfig returns the current S3 configuration.
func (fs *S3FileSystem) GetConfig() S3Config {
	return fs.config
}

// GetClient returns the underlying minio client.
// This can be useful for advanced operations not exposed through the FileSystem interface.
func (fs *S3FileSystem) GetClient() *minio.Client {
	return fs.client
}

// Verify S3FileSystem implements ContextFileSystem interface.
var _ ContextFileSystem = (*S3FileSystem)(nil)
