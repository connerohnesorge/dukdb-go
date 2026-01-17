//go:build !js || !wasm

// Package filesystem provides AzureFileSystem implementation for Azure Blob Storage.
// The AzureFileSystem implements the FileSystem interface using the Azure SDK for Go.
// It supports read, write, stat, remove, and directory listing operations on Azure containers.
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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// AzureFileSystem implements FileSystem for Azure Blob Storage.
// It uses the official Azure SDK for Go for all blob operations.
type AzureFileSystem struct {
	// client is the underlying Azure blob service client.
	client *azblob.Client
	// config holds the Azure configuration used to create this filesystem.
	config AzureConfig
	// mu protects concurrent access to the client.
	mu sync.RWMutex
}

// NewAzureFileSystem creates a new Azure filesystem with the given configuration.
// The context parameter is used for client initialization.
// Returns an error if the Azure client cannot be created.
//
//nolint:gocritic // hugeParam: config is passed by value for API simplicity and immutability
func NewAzureFileSystem(ctx context.Context, config AzureConfig) (*AzureFileSystem, error) {
	client, err := buildAzureClient(ctx, &config)
	if err != nil {
		return nil, fmt.Errorf("azure: failed to create client: %w", err)
	}

	return &AzureFileSystem{client: client, config: config}, nil
}

// NewAzureFileSystemWithClient creates a new Azure filesystem with a pre-configured client.
// This is useful for testing with mock clients or for sharing clients across filesystems.
// If config is nil, uses default Azure configuration values.
func NewAzureFileSystemWithClient(client *azblob.Client, config *AzureConfig) *AzureFileSystem {
	cfg := AzureConfig{}
	if config != nil {
		cfg = *config
	}

	return &AzureFileSystem{client: client, config: cfg}
}

// buildAzureClient creates an Azure blob client from the configuration.
func buildAzureClient(_ context.Context, config *AzureConfig) (*azblob.Client, error) {
	serviceURL := buildAzureServiceURL(config)

	// Try different authentication methods in order of preference
	// 1. Connection string
	if config.ConnectionString != "" {
		return azblob.NewClientFromConnectionString(config.ConnectionString, nil)
	}

	// 2. Shared key (account name + account key)
	if config.AccountName != "" && config.AccountKey != "" {
		cred, err := azblob.NewSharedKeyCredential(config.AccountName, config.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create shared key credential: %w", err)
		}

		return azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	}

	// 3. SAS token
	if config.SASToken != "" {
		sasURL := serviceURL
		if !strings.Contains(sasURL, "?") {
			sasURL += "?"
		} else {
			sasURL += "&"
		}
		sasURL += config.SASToken

		return azblob.NewClientWithNoCredential(sasURL, nil)
	}

	// 4. Service principal
	if config.TenantID != "" && config.ClientID != "" && config.ClientSecret != "" {
		cred, err := azidentity.NewClientSecretCredential(
			config.TenantID,
			config.ClientID,
			config.ClientSecret,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create service principal credential: %w", err)
		}

		return azblob.NewClient(serviceURL, cred, nil)
	}

	// 5. Default Azure credential (includes managed identity, environment, etc.)
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		// If default credential fails and we have a custom endpoint (emulator),
		// allow anonymous access
		if config.Endpoint != "" {
			return azblob.NewClientWithNoCredential(serviceURL, nil)
		}

		return nil, fmt.Errorf("failed to create default credential: %w", err)
	}

	return azblob.NewClient(serviceURL, cred, nil)
}

// buildAzureServiceURL constructs the Azure service URL from configuration.
func buildAzureServiceURL(config *AzureConfig) string {
	if config.Endpoint != "" {
		// Custom endpoint (e.g., Azurite emulator)
		return config.Endpoint
	}

	scheme := "https"
	if !config.UseHTTPS {
		scheme = "http"
	}

	if config.AccountName != "" {
		return fmt.Sprintf("%s://%s.blob.core.windows.net/", scheme, config.AccountName)
	}

	return fmt.Sprintf("%s://localhost/", scheme)
}

// parseAzurePath parses an Azure URL into container and blob components.
// Supports azure://, az:// URL schemes.
// Also handles raw container/blob paths without a scheme prefix.
func parseAzurePath(path string) (containerName, blobName string, err error) {
	cleanPath := stripAzurePrefix(path)

	if strings.Contains(cleanPath, "?") {
		parsed, err := url.Parse("azure://" + cleanPath)
		if err != nil {
			return "", "", fmt.Errorf("azure: failed to parse URL: %w", err)
		}

		cleanPath = parsed.Host + parsed.Path
	}

	parts := strings.SplitN(cleanPath, azurePathSeparator, 2)
	if len(parts) == 0 || parts[0] == "" {
		return "", "", errors.New("azure: container name is required")
	}

	containerName = parts[0]
	if len(parts) > 1 {
		blobName = parts[1]
	}

	return containerName, blobName, nil
}

// stripAzurePrefix removes azure:// or az:// prefix from a path.
// Returns the original path if no recognized prefix is found.
func stripAzurePrefix(path string) string {
	for _, prefix := range []string{"azure://", "az://"} {
		if strings.HasPrefix(strings.ToLower(path), prefix) {
			return path[len(prefix):]
		}
	}

	return path
}

// Open opens an Azure blob for reading.
// Returns a File that streams data from Azure on Read calls.
func (fs *AzureFileSystem) Open(path string) (File, error) {
	containerName, blobName, err := parseAzurePath(path)
	if err != nil {
		return nil, err
	}

	blobClient := fs.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)

	return newAzureFileForReading(blobClient, containerName, blobName), nil
}

// Create creates an Azure blob for writing.
// Data is buffered in memory and uploaded when Close() is called.
func (fs *AzureFileSystem) Create(path string) (File, error) {
	containerName, blobName, err := parseAzurePath(path)
	if err != nil {
		return nil, err
	}

	blockBlobClient := fs.client.ServiceClient().NewContainerClient(containerName).NewBlockBlobClient(blobName)

	return newAzureFileForWriting(blockBlobClient, containerName, blobName), nil
}

// MkdirAll is a no-op for Azure since directories are implicit.
// Azure uses blob prefixes as virtual directories.
func (*AzureFileSystem) MkdirAll(_ string) error {
	return nil
}

// Stat returns file info for an Azure blob.
// Uses GetProperties to retrieve metadata without downloading the blob.
func (fs *AzureFileSystem) Stat(path string) (FileInfo, error) {
	containerName, blobName, err := parseAzurePath(path)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	blobClient := fs.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)

	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("azure: failed to stat blob: %w", err)
	}

	return NewAzureFileInfo(blobName, &props), nil
}

// Remove removes an Azure blob.
// Returns an error if the delete operation fails.
func (fs *AzureFileSystem) Remove(path string) error {
	containerName, blobName, err := parseAzurePath(path)
	if err != nil {
		return err
	}

	ctx := context.Background()
	blobClient := fs.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)

	_, err = blobClient.Delete(ctx, nil)
	if err != nil {
		return fmt.Errorf("azure: failed to remove blob: %w", err)
	}

	return nil
}

// RemoveDir removes an Azure "directory" (all blobs with a given prefix).
// Lists all blobs with the prefix and deletes them one by one.
func (fs *AzureFileSystem) RemoveDir(path string) error {
	containerName, prefix, err := parseAzurePath(path)
	if err != nil {
		return err
	}

	if !strings.HasSuffix(prefix, azurePathSeparator) {
		prefix += azurePathSeparator
	}

	ctx := context.Background()
	containerClient := fs.client.ServiceClient().NewContainerClient(containerName)

	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("azure: failed to list blobs: %w", err)
		}

		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}

			blobClient := containerClient.NewBlobClient(*item.Name)
			if _, err := blobClient.Delete(ctx, nil); err != nil {
				return fmt.Errorf("azure: failed to remove blob %s: %w", *item.Name, err)
			}
		}
	}

	return nil
}

// ReadDir reads the contents of an Azure "directory" (blobs with a given prefix).
// Uses ListBlobsHierarchy to list only direct children.
func (fs *AzureFileSystem) ReadDir(path string) ([]DirEntry, error) {
	containerName, prefix, err := parseAzurePath(path)
	if err != nil {
		return nil, err
	}

	if prefix != "" && !strings.HasSuffix(prefix, azurePathSeparator) {
		prefix += azurePathSeparator
	}

	ctx := context.Background()
	containerClient := fs.client.ServiceClient().NewContainerClient(containerName)

	delimiter := azurePathSeparator
	pager := containerClient.NewListBlobsHierarchyPager(delimiter, &container.ListBlobsHierarchyOptions{
		Prefix: &prefix,
	})

	entries := make([]DirEntry, 0)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("azure: failed to list blobs: %w", err)
		}

		// Handle prefix (directory) entries
		for _, prefixItem := range page.Segment.BlobPrefixes {
			if prefixItem.Name == nil {
				continue
			}

			name := strings.TrimPrefix(*prefixItem.Name, prefix)
			name = strings.TrimSuffix(name, azurePathSeparator)
			if name != "" {
				entries = append(entries, NewAzureDirEntry(name, true, 0, time.Time{}))
			}
		}

		// Handle blob entries
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}

			// Skip the prefix itself
			if *item.Name == prefix {
				continue
			}

			name := strings.TrimPrefix(*item.Name, prefix)
			isDir := strings.HasSuffix(name, azurePathSeparator)
			if isDir {
				name = strings.TrimSuffix(name, azurePathSeparator)
			}

			size := int64(0)
			if item.Properties != nil && item.Properties.ContentLength != nil {
				size = *item.Properties.ContentLength
			}

			var modified time.Time
			if item.Properties != nil && item.Properties.LastModified != nil {
				modified = *item.Properties.LastModified
			}

			entries = append(entries, NewAzureDirEntry(name, isDir, size, modified))
		}
	}

	return entries, nil
}

// Exists checks if an Azure blob exists.
// Uses GetProperties and checks for BlobNotFound error.
func (fs *AzureFileSystem) Exists(path string) (bool, error) {
	containerName, blobName, err := parseAzurePath(path)
	if err != nil {
		return false, err
	}

	ctx := context.Background()
	blobClient := fs.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)

	_, err = blobClient.GetProperties(ctx, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			if bloberror.HasCode(err, bloberror.BlobNotFound) {
				return false, nil
			}
		}

		return false, fmt.Errorf("azure: failed to check existence: %w", err)
	}

	return true, nil
}

// URI returns the base URI for this filesystem.
// Always returns "azure://" for Azure filesystems.
func (*AzureFileSystem) URI() string {
	return "azure://"
}

// Capabilities returns the capabilities of this filesystem.
// Azure supports seek (via range requests), range reads, directory listing,
// write, and delete operations. Append is not supported in the same way as local files.
func (*AzureFileSystem) Capabilities() FileSystemCapabilities {
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

// Glob expands a glob pattern to a list of matching Azure blob keys.
// Uses blob list API with prefix optimization to efficiently list blobs.
// Handles pagination automatically using the Azure SDK pager.
// Applies retry logic with exponential backoff for rate limiting.
func (fs *AzureFileSystem) Glob(pattern string) ([]string, error) {
	return fs.GlobContext(context.Background(), pattern)
}

// GlobContext expands a glob pattern with context support for cancellation.
func (fs *AzureFileSystem) GlobContext(ctx context.Context, pattern string) ([]string, error) {
	// Validate the pattern
	if err := ValidateGlobPattern(pattern); err != nil {
		return nil, err
	}

	// Normalize path separators
	pattern = normalizePath(pattern)

	// Parse Azure path from pattern to extract container and blob pattern
	containerName, blobPattern, err := parseAzurePath(pattern)
	if err != nil {
		return nil, fmt.Errorf("azure: glob parse error: %w", err)
	}

	// If no glob characters, check if the blob exists
	if !ContainsGlobPattern(blobPattern) {
		exists, err := fs.Exists(pattern)
		if err != nil {
			return nil, fmt.Errorf("azure: checking path existence: %w", err)
		}
		if exists {
			return []string{pattern}, nil
		}
		return nil, nil
	}

	// Extract the prefix (literal part before any wildcards) for optimization
	prefix := ExtractPrefix(blobPattern)

	// List blobs with prefix optimization
	matches, err := fs.listBlobsWithPrefix(ctx, containerName, prefix, blobPattern)
	if err != nil {
		return nil, err
	}

	// Sort results alphabetically
	sort.Strings(matches)

	return matches, nil
}

// listBlobsWithPrefix lists Azure blobs using the given prefix and filters by pattern.
// Handles pagination automatically using the Azure SDK pager.
// Uses retry logic with exponential backoff for transient errors.
func (fs *AzureFileSystem) listBlobsWithPrefix(
	ctx context.Context,
	containerName, prefix, pattern string,
) ([]string, error) {
	// Create the list operation function for retry
	listFn := func() ([]string, error) {
		var matches []string

		containerClient := fs.client.ServiceClient().NewContainerClient(containerName)

		// Use flat listing with prefix for efficient listing
		pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
			Prefix: &prefix,
		})

		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("azure: failed to list blobs: %w", err)
			}

			for _, item := range page.Segment.BlobItems {
				if item.Name == nil {
					continue
				}

				// Skip "directory" markers (keys ending with /)
				if strings.HasSuffix(*item.Name, azurePathSeparator) {
					continue
				}

				// Check if the blob name matches the pattern
				matched, err := MatchPattern(pattern, *item.Name)
				if err != nil {
					return nil, fmt.Errorf("azure: pattern match error: %w", err)
				}
				if matched {
					fullPath := "azure://" + containerName + azurePathSeparator + *item.Name
					matches = append(matches, fullPath)
				}
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

// SupportsGlob returns true because AzureFileSystem has native glob support with prefix optimization.
func (*AzureFileSystem) SupportsGlob() bool {
	return true
}

// Verify AzureFileSystem implements FileSystem interface at compile time.
var _ FileSystem = (*AzureFileSystem)(nil)

// OpenContext opens an Azure blob for reading with context support.
func (fs *AzureFileSystem) OpenContext(ctx context.Context, path string) (File, error) {
	containerName, blobName, err := parseAzurePath(path)
	if err != nil {
		return nil, err
	}

	// Check context
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	blobClient := fs.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)

	return newAzureFileForReadingWithConfig(blobClient, containerName, blobName, fs.config), nil
}

// CreateContext creates an Azure blob for writing with context support.
func (fs *AzureFileSystem) CreateContext(ctx context.Context, path string) (File, error) {
	containerName, blobName, err := parseAzurePath(path)
	if err != nil {
		return nil, err
	}

	// Check context
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	blockBlobClient := fs.client.ServiceClient().NewContainerClient(containerName).NewBlockBlobClient(blobName)

	return newAzureFileForWritingWithConfig(blockBlobClient, containerName, blobName, fs.config), nil
}

// StatContext returns file info with context support.
func (fs *AzureFileSystem) StatContext(ctx context.Context, path string) (FileInfo, error) {
	containerName, blobName, err := parseAzurePath(path)
	if err != nil {
		return nil, err
	}

	statFn := func() (FileInfo, error) {
		blobClient := fs.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)

		props, err := blobClient.GetProperties(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("azure: failed to stat blob: %w", err)
		}

		return NewAzureFileInfo(blobName, &props), nil
	}

	// Apply retry logic if configured
	if fs.config.RetryConfig.MaxRetries > 0 {
		return WithRetryFunc(ctx, fs.config.RetryConfig, statFn)
	}

	return statFn()
}

// ReadDirContext reads directory contents with context support.
func (fs *AzureFileSystem) ReadDirContext(ctx context.Context, path string) ([]DirEntry, error) {
	containerName, prefix, err := parseAzurePath(path)
	if err != nil {
		return nil, err
	}

	if prefix != "" && !strings.HasSuffix(prefix, azurePathSeparator) {
		prefix += azurePathSeparator
	}

	containerClient := fs.client.ServiceClient().NewContainerClient(containerName)

	delimiter := azurePathSeparator
	pager := containerClient.NewListBlobsHierarchyPager(delimiter, &container.ListBlobsHierarchyOptions{
		Prefix: &prefix,
	})

	entries := make([]DirEntry, 0)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("azure: failed to list blobs: %w", err)
		}

		// Handle prefix (directory) entries
		for _, prefixItem := range page.Segment.BlobPrefixes {
			if prefixItem.Name == nil {
				continue
			}

			name := strings.TrimPrefix(*prefixItem.Name, prefix)
			name = strings.TrimSuffix(name, azurePathSeparator)
			if name != "" {
				entries = append(entries, NewAzureDirEntry(name, true, 0, time.Time{}))
			}
		}

		// Handle blob entries
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}

			// Skip the prefix itself
			if *item.Name == prefix {
				continue
			}

			name := strings.TrimPrefix(*item.Name, prefix)
			isDir := strings.HasSuffix(name, azurePathSeparator)
			if isDir {
				name = strings.TrimSuffix(name, azurePathSeparator)
			}

			size := int64(0)
			if item.Properties != nil && item.Properties.ContentLength != nil {
				size = *item.Properties.ContentLength
			}

			var modified time.Time
			if item.Properties != nil && item.Properties.LastModified != nil {
				modified = *item.Properties.LastModified
			}

			entries = append(entries, NewAzureDirEntry(name, isDir, size, modified))
		}
	}

	return entries, nil
}

// GetConfig returns the current Azure configuration.
func (fs *AzureFileSystem) GetConfig() AzureConfig {
	return fs.config
}

// GetClient returns the underlying Azure client.
// This can be useful for advanced operations not exposed through the FileSystem interface.
func (fs *AzureFileSystem) GetClient() *azblob.Client {
	return fs.client
}

// Close closes the Azure filesystem and releases resources.
// Currently a no-op as the Azure SDK handles connection pooling.
func (*AzureFileSystem) Close() error {
	return nil
}

// Verify AzureFileSystem implements ContextFileSystem interface.
var _ ContextFileSystem = (*AzureFileSystem)(nil)

// registerAzureSchemesInternal registers Azure schemes with a factory.
// This is called internally during factory initialization.
func registerAzureSchemesInternal(f *factory) {
	azureFactory := func(ctx context.Context, _ string) (FileSystem, error) {
		config := DefaultAzureConfig()

		// Check for environment variables
		if accountName := os.Getenv("AZURE_STORAGE_ACCOUNT"); accountName != "" {
			config.AccountName = accountName
		}
		if accountKey := os.Getenv("AZURE_STORAGE_KEY"); accountKey != "" {
			config.AccountKey = accountKey
		}
		if connStr := os.Getenv("AZURE_STORAGE_CONNECTION_STRING"); connStr != "" {
			config.ConnectionString = connStr
		}
		if sasToken := os.Getenv("AZURE_STORAGE_SAS_TOKEN"); sasToken != "" {
			config.SASToken = sasToken
		}
		// Check for Azurite emulator endpoint
		if emulatorHost := os.Getenv("AZURITE_BLOB_HOST"); emulatorHost != "" {
			config.Endpoint = emulatorHost
		}

		return NewAzureFileSystem(ctx, config)
	}

	f.schemes["azure"] = azureFactory
	f.schemes["az"] = azureFactory
}
