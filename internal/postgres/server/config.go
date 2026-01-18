// Package server provides a PostgreSQL wire protocol server for dukdb-go.
// It uses the psql-wire library to implement a PostgreSQL-compatible server
// that can accept connections from PostgreSQL clients (psql, pgx, JDBC, etc.).
package server

import (
	"crypto/tls"
	"time"

	"github.com/dukdb/dukdb-go/internal/postgres/server/auth"
)

// DefaultHost is the default host to bind to.
const DefaultHost = "127.0.0.1"

// DefaultPort is the default PostgreSQL port.
const DefaultPort = 5432

// DefaultMaxConnections is the default maximum number of concurrent connections.
const DefaultMaxConnections = 100

// DefaultShutdownTimeout is the default timeout for graceful shutdown.
const DefaultShutdownTimeout = 30 * time.Second

// DefaultServerVersion is the PostgreSQL version string to report to clients.
const DefaultServerVersion = "16.0.0"

// Config holds configuration options for the PostgreSQL wire protocol server.
type Config struct {
	// Host is the address to bind to (default: 127.0.0.1).
	Host string

	// Port is the port to listen on (default: 5432).
	Port int

	// Database is the default database name to use.
	// This is reported to clients during handshake.
	Database string

	// MaxConnections is the maximum number of concurrent connections (default: 100).
	MaxConnections int

	// ShutdownTimeout is the timeout for graceful shutdown (default: 30s).
	ShutdownTimeout time.Duration

	// ServerVersion is the PostgreSQL version string to report to clients.
	// This should be a valid PostgreSQL version string (e.g., "16.0.0").
	ServerVersion string

	// Connection timeout options

	// ConnectTimeout is the maximum time to establish a connection (default: 30s).
	// Zero means no timeout.
	ConnectTimeout time.Duration

	// IdleTimeout is the maximum time a connection can remain idle before being closed (default: 5m).
	// Zero means connections are never closed due to being idle.
	IdleTimeout time.Duration

	// SessionTimeout is the maximum duration for a session (default: 0, no limit).
	// Zero means sessions have no maximum duration.
	SessionTimeout time.Duration

	// QueueTimeout is the maximum time to wait in queue for a connection slot (default: 30s).
	// This applies when MaxConnections is reached and new connections are queued.
	QueueTimeout time.Duration

	// MaxQueueSize is the maximum number of connections that can wait in queue (default: 1000).
	// Zero means use the default of 1000.
	MaxQueueSize int

	// EnableConnectionPooling enables the connection manager for pooling and statistics (default: true).
	EnableConnectionPooling bool

	// Authentication options

	// RequireAuth indicates whether authentication is required.
	// If false, all connections are accepted without authentication.
	RequireAuth bool

	// Username is the username to accept (when RequireAuth is true).
	// Deprecated: Use UserProvider instead for more flexible authentication.
	Username string

	// Password is the password to accept (when RequireAuth is true).
	// Deprecated: Use UserProvider instead for more flexible authentication.
	Password string

	// AuthMethod specifies the authentication method to use.
	// Default is MethodPassword when RequireAuth is true.
	AuthMethod auth.Method

	// UserProvider provides user credentials for authentication.
	// If nil and RequireAuth is true, uses Username/Password for simple auth.
	UserProvider auth.UserProvider

	// Authenticator is the authentication strategy to use.
	// If nil, one will be created based on AuthMethod and UserProvider.
	Authenticator auth.Authenticator

	// LogStartupParams enables logging of startup parameters received from clients.
	LogStartupParams bool

	// TLS configuration

	// TLSConfig is the TLS configuration for secure connections.
	// If nil, TLS is disabled.
	TLSConfig *tls.Config
}

// NewConfig creates a new Config with default values.
func NewConfig() *Config {
	return &Config{
		Host:                    DefaultHost,
		Port:                    DefaultPort,
		Database:                "dukdb",
		MaxConnections:          DefaultMaxConnections,
		ShutdownTimeout:         DefaultShutdownTimeout,
		ServerVersion:           DefaultServerVersion,
		ConnectTimeout:          30 * time.Second,
		IdleTimeout:             5 * time.Minute,
		SessionTimeout:          0, // No session timeout by default
		QueueTimeout:            30 * time.Second,
		MaxQueueSize:            1000,
		EnableConnectionPooling: true,
		RequireAuth:             false,
		AuthMethod:              auth.MethodPassword,
		LogStartupParams:        false,
	}
}

// Address returns the full address string (host:port) for the server.
func (c *Config) Address() string {
	if c.Host == "" {
		return ""
	}
	// Format as host:port
	return c.Host + ":" + itoa(c.Port)
}

// GetConnectionTimeouts returns the connection timeout configuration.
func (c *Config) GetConnectionTimeouts() *ConnectionTimeouts {
	return &ConnectionTimeouts{
		ConnectTimeout: c.ConnectTimeout,
		IdleTimeout:    c.IdleTimeout,
		SessionTimeout: c.SessionTimeout,
		QueueTimeout:   c.QueueTimeout,
	}
}

// Validate validates the configuration and returns an error if invalid.
func (c *Config) Validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return &ConfigError{Field: "Port", Message: "must be between 1 and 65535"}
	}
	if c.MaxConnections < 1 {
		return &ConfigError{Field: "MaxConnections", Message: "must be at least 1"}
	}
	if c.ShutdownTimeout < 0 {
		return &ConfigError{Field: "ShutdownTimeout", Message: "cannot be negative"}
	}

	// Validate authentication configuration
	if c.RequireAuth {
		// If we have an authenticator, we're good
		if c.Authenticator != nil {
			return nil
		}

		// If we have a user provider, we can create an authenticator
		if c.UserProvider != nil {
			return nil
		}

		// Otherwise, we need username for simple auth
		if c.Username == "" {
			return &ConfigError{
				Field:   "Username",
				Message: "required when authentication is enabled without UserProvider or Authenticator",
			}
		}
	}
	return nil
}

// GetAuthenticator returns the authenticator to use based on configuration.
// If an Authenticator is already set, it is returned.
// Otherwise, one is created based on AuthMethod and UserProvider.
func (c *Config) GetAuthenticator() auth.Authenticator {
	// If we already have an authenticator, use it
	if c.Authenticator != nil {
		return c.Authenticator
	}

	// If auth is not required, return no-op authenticator
	if !c.RequireAuth {
		return auth.NewNoAuthenticator()
	}

	// If we have a user provider, use password authenticator
	if c.UserProvider != nil {
		return auth.NewPasswordAuthenticator(c.UserProvider)
	}

	// Fall back to simple authenticator with username/password
	return auth.NewSimpleAuthenticator(c.Username, c.Password)
}

// ConfigError represents a configuration validation error.
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return "config error: " + e.Field + ": " + e.Message
}

// itoa converts an integer to a string without importing strconv.
// This is a simple implementation for port numbers.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	negative := n < 0
	if negative {
		n = -n
	}

	// Maximum int has 10 digits
	var buf [11]byte
	i := len(buf)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	if negative {
		i--
		buf[i] = '-'
	}

	return string(buf[i:])
}
