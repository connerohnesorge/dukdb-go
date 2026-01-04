//go:build !js || !wasm

package filesystem

import (
	"time"
)

// azurePathSeparator is the path separator used in Azure blob paths.
const azurePathSeparator = "/"

// Default values for Azure configuration.
const (
	// DefaultAzureTimeout is the default timeout for Azure operations.
	DefaultAzureTimeout = 30 * time.Second
	// DefaultAzureBlockSize is the default block size for uploads (4MB, Azure's default).
	DefaultAzureBlockSize = 4 * 1024 * 1024
	// DefaultAzureConcurrentReadWorkers is the default number of concurrent read workers.
	DefaultAzureConcurrentReadWorkers = 4
	// DefaultAzureConcurrentReadChunkSize is the default chunk size for concurrent reads.
	DefaultAzureConcurrentReadChunkSize = 8 * 1024 * 1024 // 8 MB
	// DefaultAzureMaxRetries is the default number of retries for transient errors.
	DefaultAzureMaxRetries = 3
)

// AzureConfig contains configuration for Azure Blob Storage filesystem.
type AzureConfig struct {
	// AccountName is the Azure storage account name.
	AccountName string
	// AccountKey is the shared key for authentication.
	AccountKey string
	// ConnectionString is an alternative connection string authentication.
	ConnectionString string
	// SASToken is a Shared Access Signature token for authentication.
	SASToken string
	// Container is the default container name.
	Container string
	// Endpoint is an optional custom endpoint (for emulators like Azurite).
	Endpoint string
	// TenantID is for service principal authentication.
	TenantID string
	// ClientID is for service principal authentication.
	ClientID string
	// ClientSecret is for service principal authentication.
	ClientSecret string
	// UseHTTPS indicates whether to use HTTPS. Defaults to true.
	UseHTTPS bool
	// Timeout is the timeout for individual Azure operations.
	Timeout time.Duration

	// Upload configuration
	// BlockSize is the size of blocks for uploads.
	BlockSize int64

	// Concurrent read configuration
	// ConcurrentReadWorkers is the number of concurrent read workers for large files.
	ConcurrentReadWorkers int
	// ConcurrentReadChunkSize is the chunk size for concurrent reads.
	ConcurrentReadChunkSize int64

	// Retry configuration
	// RetryConfig configures retry behavior for transient errors.
	RetryConfig RetryConfig
}

// DefaultAzureConfig returns a default Azure configuration.
func DefaultAzureConfig() AzureConfig {
	return AzureConfig{
		UseHTTPS:                true,
		Timeout:                 DefaultAzureTimeout,
		BlockSize:               DefaultAzureBlockSize,
		ConcurrentReadWorkers:   DefaultAzureConcurrentReadWorkers,
		ConcurrentReadChunkSize: DefaultAzureConcurrentReadChunkSize,
		RetryConfig:             DefaultRetryConfig(),
	}
}

// AzureConfigOption is a function that modifies an AzureConfig.
type AzureConfigOption func(*AzureConfig)

// WithAzureAccountName sets the Azure storage account name.
func WithAzureAccountName(name string) AzureConfigOption {
	return func(c *AzureConfig) {
		c.AccountName = name
	}
}

// WithAzureAccountKey sets the shared key for authentication.
func WithAzureAccountKey(key string) AzureConfigOption {
	return func(c *AzureConfig) {
		c.AccountKey = key
	}
}

// WithAzureConnectionString sets the connection string for authentication.
func WithAzureConnectionString(connStr string) AzureConfigOption {
	return func(c *AzureConfig) {
		c.ConnectionString = connStr
	}
}

// WithAzureSASToken sets the SAS token for authentication.
func WithAzureSASToken(token string) AzureConfigOption {
	return func(c *AzureConfig) {
		c.SASToken = token
	}
}

// WithAzureContainer sets the default container name.
func WithAzureContainer(container string) AzureConfigOption {
	return func(c *AzureConfig) {
		c.Container = container
	}
}

// WithAzureEndpoint sets a custom endpoint for Azure emulators.
func WithAzureEndpoint(endpoint string) AzureConfigOption {
	return func(c *AzureConfig) {
		c.Endpoint = endpoint
	}
}

// WithAzureServicePrincipal sets service principal authentication credentials.
func WithAzureServicePrincipal(tenantID, clientID, clientSecret string) AzureConfigOption {
	return func(c *AzureConfig) {
		c.TenantID = tenantID
		c.ClientID = clientID
		c.ClientSecret = clientSecret
	}
}

// WithAzureHTTPS sets whether to use HTTPS.
func WithAzureHTTPS(useHTTPS bool) AzureConfigOption {
	return func(c *AzureConfig) {
		c.UseHTTPS = useHTTPS
	}
}

// WithAzureTimeout sets the timeout for individual operations.
func WithAzureTimeout(timeout time.Duration) AzureConfigOption {
	return func(c *AzureConfig) {
		c.Timeout = timeout
	}
}

// WithAzureBlockSize sets the block size for uploads.
func WithAzureBlockSize(size int64) AzureConfigOption {
	return func(c *AzureConfig) {
		if size > 0 {
			c.BlockSize = size
		}
	}
}

// WithAzureConcurrentReads sets concurrent read configuration.
func WithAzureConcurrentReads(workers int, chunkSize int64) AzureConfigOption {
	return func(c *AzureConfig) {
		if workers > 0 {
			c.ConcurrentReadWorkers = workers
		}
		if chunkSize > 0 {
			c.ConcurrentReadChunkSize = chunkSize
		}
	}
}

// WithAzureRetryConfig sets the retry configuration.
func WithAzureRetryConfig(cfg RetryConfig) AzureConfigOption {
	return func(c *AzureConfig) {
		c.RetryConfig = cfg
	}
}

// NewAzureConfig creates a new AzureConfig with the given options applied.
func NewAzureConfig(opts ...AzureConfigOption) AzureConfig {
	cfg := DefaultAzureConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}
