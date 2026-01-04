//go:build !js || !wasm

package filesystem

import (
	"time"

	"github.com/minio/minio-go/v7"
)

// S3URLStyle represents the URL style for S3 bucket addressing.
type S3URLStyle string

const (
	// S3URLStylePath uses path-style addressing (e.g., https://s3.amazonaws.com/bucket/key).
	S3URLStylePath S3URLStyle = "path"
	// S3URLStyleVirtual uses virtual-host-style addressing (e.g., https://bucket.s3.region.amazonaws.com/key).
	S3URLStyleVirtual S3URLStyle = "virtual"
	// S3URLStyleAuto automatically selects the URL style based on the endpoint.
	S3URLStyleAuto S3URLStyle = "auto"
)

// s3PathSeparator is the path separator used in S3 keys.
const s3PathSeparator = "/"

// Default values for S3 configuration.
const (
	// DefaultS3Region is the default AWS region.
	DefaultS3Region = "us-east-1"
	// DefaultMultipartPartSize is the default size for multipart upload parts (5MB minimum for S3).
	DefaultMultipartPartSize = 5 * 1024 * 1024 // 5 MB
	// DefaultMultipartThreshold is the file size above which multipart upload is used.
	DefaultMultipartThreshold = 100 * 1024 * 1024 // 100 MB
	// DefaultMultipartConcurrency is the default number of concurrent upload parts.
	DefaultMultipartConcurrency = 4
	// DefaultConcurrentReadWorkers is the default number of concurrent read workers.
	DefaultConcurrentReadWorkers = 4
	// DefaultConcurrentReadChunkSize is the default chunk size for concurrent reads.
	DefaultConcurrentReadChunkSize = 8 * 1024 * 1024 // 8 MB
)

// S3Config contains configuration for S3 filesystem.
type S3Config struct {
	// Region is the AWS region (e.g., "us-east-1").
	Region string
	// Endpoint is an optional custom endpoint for S3-compatible stores (e.g., MinIO).
	// If empty, defaults to AWS S3.
	Endpoint string
	// AccessKeyID is the AWS access key ID.
	AccessKeyID string
	// SecretAccessKey is the AWS secret access key.
	SecretAccessKey string
	// SessionToken is an optional session token for temporary credentials.
	SessionToken string
	// UseSSL indicates whether to use HTTPS. Defaults to true.
	UseSSL bool
	// URLStyle is the addressing style for S3 buckets.
	URLStyle S3URLStyle
	// BucketLookup controls how bucket lookups are performed.
	BucketLookup minio.BucketLookupType

	// Multipart upload configuration
	// MultipartPartSize is the size of each part in multipart uploads (minimum 5MB for S3).
	MultipartPartSize int64
	// MultipartThreshold is the file size above which multipart upload is used.
	MultipartThreshold int64
	// MultipartConcurrency is the number of concurrent upload parts.
	MultipartConcurrency int

	// Concurrent read configuration
	// ConcurrentReadWorkers is the number of concurrent read workers for large files.
	ConcurrentReadWorkers int
	// ConcurrentReadChunkSize is the chunk size for concurrent reads.
	ConcurrentReadChunkSize int64

	// Retry configuration
	// RetryConfig configures retry behavior for transient errors.
	RetryConfig RetryConfig

	// AutoDetectRegion enables automatic region detection for buckets.
	AutoDetectRegion bool

	// Timeout configuration
	// OperationTimeout is the timeout for individual S3 operations.
	OperationTimeout time.Duration
}

// DefaultS3Config returns a default S3 configuration.
func DefaultS3Config() S3Config {
	return S3Config{
		Region:                  DefaultS3Region,
		UseSSL:                  true,
		URLStyle:                S3URLStyleVirtual,
		MultipartPartSize:       DefaultMultipartPartSize,
		MultipartThreshold:      DefaultMultipartThreshold,
		MultipartConcurrency:    DefaultMultipartConcurrency,
		ConcurrentReadWorkers:   DefaultConcurrentReadWorkers,
		ConcurrentReadChunkSize: DefaultConcurrentReadChunkSize,
		RetryConfig:             DefaultRetryConfig(),
		AutoDetectRegion:        false,
		OperationTimeout:        30 * time.Second,
	}
}

// S3ConfigOption is a function that modifies an S3Config.
type S3ConfigOption func(*S3Config)

// WithRegion sets the AWS region.
func WithRegion(region string) S3ConfigOption {
	return func(c *S3Config) {
		c.Region = region
	}
}

// WithEndpoint sets a custom endpoint for S3-compatible stores.
func WithEndpoint(endpoint string) S3ConfigOption {
	return func(c *S3Config) {
		c.Endpoint = endpoint
	}
}

// WithCredentials sets the access key and secret key.
func WithCredentials(accessKeyID, secretAccessKey string) S3ConfigOption {
	return func(c *S3Config) {
		c.AccessKeyID = accessKeyID
		c.SecretAccessKey = secretAccessKey
	}
}

// WithSessionToken sets a session token for temporary credentials.
func WithSessionToken(token string) S3ConfigOption {
	return func(c *S3Config) {
		c.SessionToken = token
	}
}

// WithSSL sets whether to use HTTPS.
func WithSSL(useSSL bool) S3ConfigOption {
	return func(c *S3Config) {
		c.UseSSL = useSSL
	}
}

// WithURLStyle sets the URL addressing style.
func WithURLStyle(style S3URLStyle) S3ConfigOption {
	return func(c *S3Config) {
		c.URLStyle = style
	}
}

// WithMultipartConfig sets multipart upload configuration.
func WithMultipartConfig(partSize, threshold int64, concurrency int) S3ConfigOption {
	return func(c *S3Config) {
		if partSize >= DefaultMultipartPartSize {
			c.MultipartPartSize = partSize
		}
		if threshold > 0 {
			c.MultipartThreshold = threshold
		}
		if concurrency > 0 {
			c.MultipartConcurrency = concurrency
		}
	}
}

// WithConcurrentReads sets concurrent read configuration.
func WithConcurrentReads(workers int, chunkSize int64) S3ConfigOption {
	return func(c *S3Config) {
		if workers > 0 {
			c.ConcurrentReadWorkers = workers
		}
		if chunkSize > 0 {
			c.ConcurrentReadChunkSize = chunkSize
		}
	}
}

// WithRetryConfig sets the retry configuration.
func WithRetryConfig(cfg RetryConfig) S3ConfigOption {
	return func(c *S3Config) {
		c.RetryConfig = cfg
	}
}

// WithAutoDetectRegion enables or disables automatic region detection.
func WithAutoDetectRegion(enable bool) S3ConfigOption {
	return func(c *S3Config) {
		c.AutoDetectRegion = enable
	}
}

// WithOperationTimeout sets the timeout for individual operations.
func WithOperationTimeout(timeout time.Duration) S3ConfigOption {
	return func(c *S3Config) {
		c.OperationTimeout = timeout
	}
}

// NewS3Config creates a new S3Config with the given options applied.
func NewS3Config(opts ...S3ConfigOption) S3Config {
	cfg := DefaultS3Config()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}
