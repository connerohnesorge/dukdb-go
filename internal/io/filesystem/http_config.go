package filesystem

import (
	"crypto/tls"
	"time"
)

// Default values for HTTP configuration.
const (
	// DefaultHTTPTimeout is the default timeout for HTTP operations.
	DefaultHTTPTimeout = 30 * time.Second
	// DefaultHTTPMaxRedirects is the default maximum number of redirects to follow.
	DefaultHTTPMaxRedirects = 10
	// DefaultHTTPUserAgent is the default User-Agent header.
	DefaultHTTPUserAgent = "dukdb-go/1.0"
)

// BasicAuth contains credentials for HTTP Basic Authentication.
type BasicAuth struct {
	// Username is the basic auth username.
	Username string
	// Password is the basic auth password.
	Password string
}

// HTTPConfig contains configuration for HTTP/HTTPS filesystem.
type HTTPConfig struct {
	// Timeout is the timeout for HTTP operations.
	Timeout time.Duration
	// FollowRedirects indicates whether to follow HTTP redirects.
	FollowRedirects bool
	// MaxRedirects is the maximum number of redirects to follow.
	MaxRedirects int
	// Headers contains custom headers to include in all requests.
	Headers map[string]string
	// UserAgent is the User-Agent header value.
	UserAgent string
	// InsecureSkipVerify skips TLS certificate verification (for self-signed certs).
	InsecureSkipVerify bool
	// BasicAuth contains credentials for HTTP Basic Authentication.
	BasicAuth *BasicAuth
	// BearerToken is a bearer token for authentication.
	BearerToken string
	// RetryConfig configures retry behavior for transient errors.
	RetryConfig RetryConfig
	// TLSConfig allows custom TLS configuration.
	TLSConfig *tls.Config
}

// DefaultHTTPConfig returns a default HTTP configuration.
func DefaultHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Timeout:            DefaultHTTPTimeout,
		FollowRedirects:    true,
		MaxRedirects:       DefaultHTTPMaxRedirects,
		Headers:            make(map[string]string),
		UserAgent:          DefaultHTTPUserAgent,
		InsecureSkipVerify: false,
		RetryConfig:        DefaultRetryConfig(),
	}
}

// HTTPConfigOption is a function that modifies an HTTPConfig.
type HTTPConfigOption func(*HTTPConfig)

// WithHTTPTimeout sets the timeout for HTTP operations.
func WithHTTPTimeout(timeout time.Duration) HTTPConfigOption {
	return func(c *HTTPConfig) {
		c.Timeout = timeout
	}
}

// WithHTTPFollowRedirects sets whether to follow redirects.
func WithHTTPFollowRedirects(follow bool) HTTPConfigOption {
	return func(c *HTTPConfig) {
		c.FollowRedirects = follow
	}
}

// WithHTTPMaxRedirects sets the maximum number of redirects to follow.
func WithHTTPMaxRedirects(maxRedirects int) HTTPConfigOption {
	return func(c *HTTPConfig) {
		if maxRedirects > 0 {
			c.MaxRedirects = maxRedirects
		}
	}
}

// WithHTTPHeaders sets custom headers for all requests.
func WithHTTPHeaders(headers map[string]string) HTTPConfigOption {
	return func(c *HTTPConfig) {
		if c.Headers == nil {
			c.Headers = make(map[string]string)
		}
		for k, v := range headers {
			c.Headers[k] = v
		}
	}
}

// WithHTTPHeader adds a single custom header.
func WithHTTPHeader(key, value string) HTTPConfigOption {
	return func(c *HTTPConfig) {
		if c.Headers == nil {
			c.Headers = make(map[string]string)
		}
		c.Headers[key] = value
	}
}

// WithHTTPUserAgent sets the User-Agent header.
func WithHTTPUserAgent(userAgent string) HTTPConfigOption {
	return func(c *HTTPConfig) {
		c.UserAgent = userAgent
	}
}

// WithHTTPInsecureSkipVerify sets whether to skip TLS certificate verification.
func WithHTTPInsecureSkipVerify(skip bool) HTTPConfigOption {
	return func(c *HTTPConfig) {
		c.InsecureSkipVerify = skip
	}
}

// WithHTTPBasicAuth sets basic authentication credentials.
func WithHTTPBasicAuth(username, password string) HTTPConfigOption {
	return func(c *HTTPConfig) {
		c.BasicAuth = &BasicAuth{
			Username: username,
			Password: password,
		}
	}
}

// WithHTTPBearerToken sets a bearer token for authentication.
func WithHTTPBearerToken(token string) HTTPConfigOption {
	return func(c *HTTPConfig) {
		c.BearerToken = token
	}
}

// WithHTTPRetryConfig sets the retry configuration.
func WithHTTPRetryConfig(cfg RetryConfig) HTTPConfigOption {
	return func(c *HTTPConfig) {
		c.RetryConfig = cfg
	}
}

// WithHTTPTLSConfig sets custom TLS configuration.
func WithHTTPTLSConfig(tlsConfig *tls.Config) HTTPConfigOption {
	return func(c *HTTPConfig) {
		c.TLSConfig = tlsConfig
	}
}

// NewHTTPConfig creates a new HTTPConfig with the given options applied.
func NewHTTPConfig(opts ...HTTPConfigOption) HTTPConfig {
	cfg := DefaultHTTPConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}
