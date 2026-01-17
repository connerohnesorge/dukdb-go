//go:build !js || !wasm

// Package filesystem provides GCSFileSystem implementation for Google Cloud Storage.
// The GCSFileSystem implements the FileSystem interface using the cloud.google.com/go/storage client.
// It supports read, write, stat, remove, and directory listing operations on GCS buckets.
package filesystem

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSFileSystem implements FileSystem for Google Cloud Storage.
// It uses the official GCS Go client library for all GCS operations.
type GCSFileSystem struct {
	// client is the underlying GCS client for operations.
	client *storage.Client
	// config holds the GCS configuration used to create this filesystem.
	config GCSConfig
	// mu protects concurrent access to the client.
	mu sync.RWMutex
}

// NewGCSFileSystem creates a new GCS filesystem with the given configuration.
// The context parameter is used for client initialization.
// Returns an error if the GCS client cannot be created.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity and immutability
func NewGCSFileSystem(ctx context.Context, config GCSConfig) (*GCSFileSystem, error) {
	opts := buildGCSClientOptions(&config)

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcs: failed to create client: %w", err)
	}

	return &GCSFileSystem{client: client, config: config}, nil
}

// NewGCSFileSystemWithClient creates a new GCS filesystem with a pre-configured client.
// This is useful for testing with mock clients or for sharing clients across filesystems.
// If config is nil, uses default GCS configuration values.
func NewGCSFileSystemWithClient(client *storage.Client, config *GCSConfig) *GCSFileSystem {
	cfg := GCSConfig{}
	if config != nil {
		cfg = *config
	}

	return &GCSFileSystem{client: client, config: cfg}
}

// buildGCSClientOptions creates GCS client options from the configuration.
func buildGCSClientOptions(config *GCSConfig) []option.ClientOption {
	var opts []option.ClientOption

	// Set credentials if provided
	if config.KeyFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.KeyFile))
	} else if config.CredentialsJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(config.CredentialsJSON)))
	}

	// Set custom endpoint if provided (for emulators)
	if config.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(config.Endpoint))
		// When using a custom endpoint, we typically need to disable authentication
		// This is common for local emulators like fake-gcs-server
		opts = append(opts, option.WithoutAuthentication())
	}

	return opts
}

// parseGCSPath parses a GCS URL into bucket and object components.
// Supports gs://, gcs:// URL schemes.
// Also handles raw bucket/object paths without a scheme prefix.
func parseGCSPath(path string) (bucket, object string, err error) {
	cleanPath := stripGCSPrefix(path)

	if strings.Contains(cleanPath, "?") {
		parsed, err := url.Parse("gs://" + cleanPath)
		if err != nil {
			return "", "", fmt.Errorf("gcs: failed to parse URL: %w", err)
		}

		cleanPath = parsed.Host + parsed.Path
	}

	parts := strings.SplitN(cleanPath, gcsPathSeparator, 2)
	if len(parts) == 0 || parts[0] == "" {
		return "", "", errors.New("gcs: bucket name is required")
	}

	bucket = parts[0]
	if len(parts) > 1 {
		object = parts[1]
	}

	return bucket, object, nil
}

// stripGCSPrefix removes gs:// or gcs:// prefix from a path.
// Returns the original path if no recognized prefix is found.
func stripGCSPrefix(path string) string {
	for _, prefix := range []string{"gs://", "gcs://"} {
		if strings.HasPrefix(strings.ToLower(path), prefix) {
			return path[len(prefix):]
		}
	}

	return path
}

// Open opens a GCS object for reading.
// Returns a File that streams data from GCS on Read calls.
func (fs *GCSFileSystem) Open(path string) (File, error) {
	bucket, object, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}

	return newGCSFileForReading(fs.client, bucket, object), nil
}

// Create creates a GCS object for writing.
// Data is buffered in memory and uploaded when Close() is called.
func (fs *GCSFileSystem) Create(path string) (File, error) {
	bucket, object, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}

	return newGCSFileForWriting(fs.client, bucket, object), nil
}

// MkdirAll is a no-op for GCS since directories are implicit.
// GCS uses object prefixes as virtual directories.
func (*GCSFileSystem) MkdirAll(_ string) error {
	return nil
}

// Stat returns file info for a GCS object.
// Uses object Attrs to retrieve metadata without downloading the object.
func (fs *GCSFileSystem) Stat(path string) (FileInfo, error) {
	bucket, object, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	obj := fs.client.Bucket(bucket).Object(object)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs: failed to stat object: %w", err)
	}

	return &GCSFileInfo{attrs: attrs}, nil
}

// Remove removes a GCS object.
// Returns an error if the delete operation fails.
func (fs *GCSFileSystem) Remove(path string) error {
	bucket, object, err := parseGCSPath(path)
	if err != nil {
		return err
	}

	ctx := context.Background()
	obj := fs.client.Bucket(bucket).Object(object)

	err = obj.Delete(ctx)
	if err != nil {
		return fmt.Errorf("gcs: failed to remove object: %w", err)
	}

	return nil
}

// RemoveDir removes a GCS "directory" (all objects with a given prefix).
// Lists all objects with the prefix and deletes them one by one.
func (fs *GCSFileSystem) RemoveDir(path string) error {
	bucket, prefix, err := parseGCSPath(path)
	if err != nil {
		return err
	}

	if !strings.HasSuffix(prefix, gcsPathSeparator) {
		prefix += gcsPathSeparator
	}

	ctx := context.Background()
	it := fs.client.Bucket(bucket).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("gcs: failed to list objects: %w", err)
		}

		obj := fs.client.Bucket(bucket).Object(attrs.Name)
		if err := obj.Delete(ctx); err != nil {
			return fmt.Errorf("gcs: failed to remove object %s: %w", attrs.Name, err)
		}
	}

	return nil
}

// ReadDir reads the contents of a GCS "directory" (objects with a given prefix).
// Uses Objects with Delimiter to list only direct children.
func (fs *GCSFileSystem) ReadDir(path string) ([]DirEntry, error) {
	bucket, prefix, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}

	if prefix != "" && !strings.HasSuffix(prefix, gcsPathSeparator) {
		prefix += gcsPathSeparator
	}

	ctx := context.Background()
	it := fs.client.Bucket(bucket).Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: gcsPathSeparator,
	})

	entries := make([]DirEntry, 0)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs: failed to list objects: %w", err)
		}

		// Handle prefix (directory) entries
		if attrs.Prefix != "" {
			name := strings.TrimPrefix(attrs.Prefix, prefix)
			name = strings.TrimSuffix(name, gcsPathSeparator)
			if name != "" {
				entries = append(entries, &GCSDirEntry{name: name, isDir: true, attrs: nil})
			}
			continue
		}

		// Skip the prefix itself
		if attrs.Name == prefix {
			continue
		}

		name := strings.TrimPrefix(attrs.Name, prefix)
		isDir := strings.HasSuffix(name, gcsPathSeparator)
		if isDir {
			name = strings.TrimSuffix(name, gcsPathSeparator)
		}

		entries = append(entries, &GCSDirEntry{name: name, isDir: isDir, attrs: attrs})
	}

	return entries, nil
}

// Exists checks if a GCS object exists.
// Uses object Attrs and checks for ErrObjectNotExist.
func (fs *GCSFileSystem) Exists(path string) (bool, error) {
	bucket, object, err := parseGCSPath(path)
	if err != nil {
		return false, err
	}

	ctx := context.Background()
	obj := fs.client.Bucket(bucket).Object(object)

	_, err = obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}

		return false, fmt.Errorf("gcs: failed to check existence: %w", err)
	}

	return true, nil
}

// URI returns the base URI for this filesystem.
// Always returns "gs://" for GCS filesystems.
func (*GCSFileSystem) URI() string {
	return "gs://"
}

// Capabilities returns the capabilities of this filesystem.
// GCS supports seek (via range requests), range reads, directory listing,
// write, and delete operations. Append is not supported.
func (*GCSFileSystem) Capabilities() FileSystemCapabilities {
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

// Glob expands a glob pattern to a list of matching GCS object keys.
// Uses storage.Query with prefix optimization to efficiently list objects.
// Handles pagination automatically using the GCS iterator.
// Applies retry logic with exponential backoff for rate limiting.
func (fs *GCSFileSystem) Glob(pattern string) ([]string, error) {
	return fs.GlobContext(context.Background(), pattern)
}

// GlobContext expands a glob pattern with context support for cancellation.
func (fs *GCSFileSystem) GlobContext(ctx context.Context, pattern string) ([]string, error) {
	// Validate the pattern
	if err := ValidateGlobPattern(pattern); err != nil {
		return nil, err
	}

	// Normalize path separators
	pattern = normalizePath(pattern)

	// Parse GCS path from pattern to extract bucket and object pattern
	bucket, objectPattern, err := parseGCSPath(pattern)
	if err != nil {
		return nil, fmt.Errorf("gcs: glob parse error: %w", err)
	}

	// If no glob characters, check if the object exists
	if !ContainsGlobPattern(objectPattern) {
		exists, err := fs.Exists(pattern)
		if err != nil {
			return nil, fmt.Errorf("gcs: checking path existence: %w", err)
		}
		if exists {
			return []string{pattern}, nil
		}
		return nil, nil
	}

	// Extract the prefix (literal part before any wildcards) for optimization
	prefix := ExtractPrefix(objectPattern)

	// List objects with prefix optimization
	matches, err := fs.listObjectsWithPrefix(ctx, bucket, prefix, objectPattern)
	if err != nil {
		return nil, err
	}

	// Sort results alphabetically
	sort.Strings(matches)

	return matches, nil
}

// listObjectsWithPrefix lists GCS objects using the given prefix and filters by pattern.
// Handles pagination automatically using the GCS iterator.
// Uses retry logic with exponential backoff for transient errors.
func (fs *GCSFileSystem) listObjectsWithPrefix(
	ctx context.Context,
	bucket, prefix, pattern string,
) ([]string, error) {
	// Create the list operation function for retry
	listFn := func() ([]string, error) {
		var matches []string

		// Use storage.Query with prefix for efficient listing
		query := &storage.Query{
			Prefix: prefix,
		}

		it := fs.client.Bucket(bucket).Objects(ctx, query)

		for {
			attrs, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("gcs: failed to list objects: %w", err)
			}

			// Skip "directory" markers (keys ending with /)
			if strings.HasSuffix(attrs.Name, gcsPathSeparator) {
				continue
			}

			// Check if the object key matches the pattern
			matched, err := MatchPattern(pattern, attrs.Name)
			if err != nil {
				return nil, fmt.Errorf("gcs: pattern match error: %w", err)
			}
			if matched {
				fullPath := "gs://" + bucket + gcsPathSeparator + attrs.Name
				matches = append(matches, fullPath)
			}
		}

		return matches, nil
	}

	// Use retry logic if configured
	if fs.config.RetryConfig.MaxRetries > 0 {
		return WithRetryFunc(ctx, fs.config.RetryConfig, listFn)
	}

	return listFn()
}

// SupportsGlob returns true because GCSFileSystem has native glob support with prefix optimization.
func (*GCSFileSystem) SupportsGlob() bool {
	return true
}

// Verify GCSFileSystem implements FileSystem interface at compile time.
var _ FileSystem = (*GCSFileSystem)(nil)

// OpenContext opens a GCS object for reading with context support.
func (fs *GCSFileSystem) OpenContext(ctx context.Context, path string) (File, error) {
	bucket, object, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}

	// Check context
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return newGCSFileForReadingWithConfig(fs.client, bucket, object, fs.config), nil
}

// CreateContext creates a GCS object for writing with context support.
func (fs *GCSFileSystem) CreateContext(ctx context.Context, path string) (File, error) {
	bucket, object, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}

	// Check context
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return newGCSFileForWritingWithConfig(fs.client, bucket, object, fs.config), nil
}

// StatContext returns file info with context support.
func (fs *GCSFileSystem) StatContext(ctx context.Context, path string) (FileInfo, error) {
	bucket, object, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}

	statFn := func() (FileInfo, error) {
		obj := fs.client.Bucket(bucket).Object(object)

		attrs, err := obj.Attrs(ctx)
		if err != nil {
			return nil, fmt.Errorf("gcs: failed to stat object: %w", err)
		}

		return &GCSFileInfo{attrs: attrs}, nil
	}

	// Apply retry logic if configured
	if fs.config.RetryConfig.MaxRetries > 0 {
		return WithRetryFunc(ctx, fs.config.RetryConfig, statFn)
	}

	return statFn()
}

// ReadDirContext reads directory contents with context support.
func (fs *GCSFileSystem) ReadDirContext(ctx context.Context, path string) ([]DirEntry, error) {
	bucket, prefix, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}

	if prefix != "" && !strings.HasSuffix(prefix, gcsPathSeparator) {
		prefix += gcsPathSeparator
	}

	it := fs.client.Bucket(bucket).Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: gcsPathSeparator,
	})

	entries := make([]DirEntry, 0)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs: failed to list objects: %w", err)
		}

		// Handle prefix (directory) entries
		if attrs.Prefix != "" {
			name := strings.TrimPrefix(attrs.Prefix, prefix)
			name = strings.TrimSuffix(name, gcsPathSeparator)
			if name != "" {
				entries = append(entries, &GCSDirEntry{name: name, isDir: true, attrs: nil})
			}
			continue
		}

		// Skip the prefix itself
		if attrs.Name == prefix {
			continue
		}

		name := strings.TrimPrefix(attrs.Name, prefix)
		isDir := strings.HasSuffix(name, gcsPathSeparator)
		if isDir {
			name = strings.TrimSuffix(name, gcsPathSeparator)
		}

		entries = append(entries, &GCSDirEntry{name: name, isDir: isDir, attrs: attrs})
	}

	return entries, nil
}

// GetConfig returns the current GCS configuration.
func (fs *GCSFileSystem) GetConfig() GCSConfig {
	return fs.config
}

// GetClient returns the underlying GCS client.
// This can be useful for advanced operations not exposed through the FileSystem interface.
func (fs *GCSFileSystem) GetClient() *storage.Client {
	return fs.client
}

// Close closes the GCS filesystem and releases resources.
func (fs *GCSFileSystem) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.client != nil {
		return fs.client.Close()
	}

	return nil
}

// Verify GCSFileSystem implements ContextFileSystem interface.
var _ ContextFileSystem = (*GCSFileSystem)(nil)

// registerGCSSchemesInternal registers GCS schemes with a factory.
// This is called internally during factory initialization.
func registerGCSSchemesInternal(f *factory) {
	gcsFactory := func(ctx context.Context, urlStr string) (FileSystem, error) {
		config := DefaultGCSConfig()

		// Check for STORAGE_EMULATOR_HOST environment variable (for emulators)
		if emulatorHost := os.Getenv("STORAGE_EMULATOR_HOST"); emulatorHost != "" {
			config.Endpoint = emulatorHost
		}

		return NewGCSFileSystem(ctx, config)
	}

	f.schemes["gs"] = gcsFactory
	f.schemes["gcs"] = gcsFactory
}
