//go:build !js || !wasm

package filesystem

import (
	"time"
)

// gcsPathSeparator is the path separator used in GCS object keys.
const gcsPathSeparator = "/"

// Default values for GCS configuration.
const (
	// DefaultGCSTimeout is the default timeout for GCS operations.
	DefaultGCSTimeout = 30 * time.Second
	// DefaultGCSChunkSize is the default chunk size for resumable uploads (256KB minimum for GCS).
	DefaultGCSChunkSize = 16 * 1024 * 1024 // 16 MB
	// DefaultGCSConcurrentReadWorkers is the default number of concurrent read workers.
	DefaultGCSConcurrentReadWorkers = 4
	// DefaultGCSConcurrentReadChunkSize is the default chunk size for concurrent reads.
	DefaultGCSConcurrentReadChunkSize = 8 * 1024 * 1024 // 8 MB
)

// GCSConfig contains configuration for GCS filesystem.
type GCSConfig struct {
	// ProjectID is the Google Cloud project ID.
	ProjectID string
	// Bucket is the default GCS bucket name.
	Bucket string
	// KeyFile is the path to a service account JSON key file.
	KeyFile string
	// CredentialsJSON is inline service account credentials JSON.
	CredentialsJSON string
	// Endpoint is an optional custom endpoint for GCS emulators.
	// If empty, defaults to the standard GCS endpoint.
	Endpoint string
	// Timeout is the timeout for individual GCS operations.
	Timeout time.Duration

	// Upload configuration
	// ChunkSize is the size of chunks for resumable uploads.
	ChunkSize int64

	// Concurrent read configuration
	// ConcurrentReadWorkers is the number of concurrent read workers for large files.
	ConcurrentReadWorkers int
	// ConcurrentReadChunkSize is the chunk size for concurrent reads.
	ConcurrentReadChunkSize int64

	// Retry configuration
	// RetryConfig configures retry behavior for transient errors.
	RetryConfig RetryConfig
}

// DefaultGCSConfig returns a default GCS configuration.
func DefaultGCSConfig() GCSConfig {
	return GCSConfig{
		Timeout:                 DefaultGCSTimeout,
		ChunkSize:               DefaultGCSChunkSize,
		ConcurrentReadWorkers:   DefaultGCSConcurrentReadWorkers,
		ConcurrentReadChunkSize: DefaultGCSConcurrentReadChunkSize,
		RetryConfig:             DefaultRetryConfig(),
	}
}

// GCSConfigOption is a function that modifies a GCSConfig.
type GCSConfigOption func(*GCSConfig)

// WithGCSProjectID sets the Google Cloud project ID.
func WithGCSProjectID(projectID string) GCSConfigOption {
	return func(c *GCSConfig) {
		c.ProjectID = projectID
	}
}

// WithGCSBucket sets the default GCS bucket.
func WithGCSBucket(bucket string) GCSConfigOption {
	return func(c *GCSConfig) {
		c.Bucket = bucket
	}
}

// WithGCSKeyFile sets the path to a service account key file.
func WithGCSKeyFile(keyFile string) GCSConfigOption {
	return func(c *GCSConfig) {
		c.KeyFile = keyFile
	}
}

// WithGCSCredentialsJSON sets inline credentials JSON.
func WithGCSCredentialsJSON(json string) GCSConfigOption {
	return func(c *GCSConfig) {
		c.CredentialsJSON = json
	}
}

// WithGCSEndpoint sets a custom endpoint for GCS emulators.
func WithGCSEndpoint(endpoint string) GCSConfigOption {
	return func(c *GCSConfig) {
		c.Endpoint = endpoint
	}
}

// WithGCSTimeout sets the timeout for individual operations.
func WithGCSTimeout(timeout time.Duration) GCSConfigOption {
	return func(c *GCSConfig) {
		c.Timeout = timeout
	}
}

// WithGCSChunkSize sets the chunk size for resumable uploads.
func WithGCSChunkSize(size int64) GCSConfigOption {
	return func(c *GCSConfig) {
		if size > 0 {
			c.ChunkSize = size
		}
	}
}

// WithGCSConcurrentReads sets concurrent read configuration.
func WithGCSConcurrentReads(workers int, chunkSize int64) GCSConfigOption {
	return func(c *GCSConfig) {
		if workers > 0 {
			c.ConcurrentReadWorkers = workers
		}
		if chunkSize > 0 {
			c.ConcurrentReadChunkSize = chunkSize
		}
	}
}

// WithGCSRetryConfig sets the retry configuration.
func WithGCSRetryConfig(cfg RetryConfig) GCSConfigOption {
	return func(c *GCSConfig) {
		c.RetryConfig = cfg
	}
}

// NewGCSConfig creates a new GCSConfig with the given options applied.
func NewGCSConfig(opts ...GCSConfigOption) GCSConfig {
	cfg := DefaultGCSConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}
