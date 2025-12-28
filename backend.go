package dukdb

import (
	"context"
	"database/sql/driver"
)

// Backend defines the interface for a DuckDB backend implementation.
// A backend is responsible for managing connections to the database.
type Backend interface {
	// Open opens a new connection to the database at the given path.
	// The config parameter provides configuration options for the connection.
	Open(
		path string,
		config *Config,
	) (BackendConn, error)

	// Close closes the backend and releases any associated resources.
	Close() error
}

// BackendConn represents a connection to a DuckDB database.
// It provides methods for executing queries and managing the connection.
type BackendConn interface {
	// Execute executes a query that doesn't return rows (INSERT, UPDATE, DELETE, etc.)
	// and returns the number of rows affected.
	Execute(
		ctx context.Context,
		query string,
		args []driver.NamedValue,
	) (int64, error)

	// Query executes a query that returns rows (SELECT, etc.) and returns
	// the results as a slice of maps, along with the column names.
	Query(
		ctx context.Context,
		query string,
		args []driver.NamedValue,
	) ([]map[string]any, []string, error)

	// Prepare prepares a statement for execution and returns a BackendStmt.
	Prepare(
		ctx context.Context,
		query string,
	) (BackendStmt, error)

	// Close closes the connection and releases any associated resources.
	Close() error

	// Ping verifies that the connection is still alive.
	Ping(ctx context.Context) error
}

// BackendStmt represents a prepared statement from a backend.
type BackendStmt interface {
	// Execute executes the prepared statement with the given arguments
	// and returns the number of rows affected.
	Execute(
		ctx context.Context,
		args []driver.NamedValue,
	) (int64, error)

	// Query executes the prepared statement with the given arguments
	// and returns the results as a slice of maps, along with the column names.
	Query(
		ctx context.Context,
		args []driver.NamedValue,
	) ([]map[string]any, []string, error)

	// Close closes the statement and releases any associated resources.
	Close() error

	// NumInput returns the number of placeholder parameters in the statement.
	NumInput() int
}

// Config holds configuration options for a DuckDB connection.
type Config struct {
	// Path specifies the database path.
	// Use ":memory:" for an in-memory database or a file path for persistent storage.
	Path string

	// AccessMode specifies the access mode for the database.
	// Valid values are "automatic", "read_only", and "read_write".
	// Default is "automatic".
	AccessMode string

	// Threads specifies the number of worker threads.
	// 1-128 is an explicit count. Default is runtime.NumCPU().
	Threads int

	// MaxMemory specifies the maximum memory limit for the database.
	// Examples: "1GB", "512MB", "80%"
	// Default is "80%".
	MaxMemory string
}

// Type enum is defined in type_enum.go

// StmtType represents the type of a SQL statement.
type StmtType int

// Statement type constants.
const (
	STATEMENT_TYPE_INVALID      StmtType = 0
	STATEMENT_TYPE_SELECT       StmtType = 1
	STATEMENT_TYPE_INSERT       StmtType = 2
	STATEMENT_TYPE_UPDATE       StmtType = 3
	STATEMENT_TYPE_EXPLAIN      StmtType = 4
	STATEMENT_TYPE_DELETE       StmtType = 5
	STATEMENT_TYPE_PREPARE      StmtType = 6
	STATEMENT_TYPE_CREATE       StmtType = 7
	STATEMENT_TYPE_EXECUTE      StmtType = 8
	STATEMENT_TYPE_ALTER        StmtType = 9
	STATEMENT_TYPE_TRANSACTION  StmtType = 10
	STATEMENT_TYPE_COPY         StmtType = 11
	STATEMENT_TYPE_ANALYZE      StmtType = 12
	STATEMENT_TYPE_VARIABLE_SET StmtType = 13
	STATEMENT_TYPE_CREATE_FUNC  StmtType = 14
	STATEMENT_TYPE_DROP         StmtType = 15
	STATEMENT_TYPE_EXPORT       StmtType = 16
	STATEMENT_TYPE_PRAGMA       StmtType = 17
	STATEMENT_TYPE_VACUUM       StmtType = 18
	STATEMENT_TYPE_CALL         StmtType = 19
	STATEMENT_TYPE_SET          StmtType = 20
	STATEMENT_TYPE_LOAD         StmtType = 21
	STATEMENT_TYPE_RELATION     StmtType = 22
	STATEMENT_TYPE_EXTENSION    StmtType = 23
	STATEMENT_TYPE_LOGICAL_PLAN StmtType = 24
	STATEMENT_TYPE_ATTACH       StmtType = 25
	STATEMENT_TYPE_DETACH       StmtType = 26
	STATEMENT_TYPE_MULTI        StmtType = 27
)

// defaultBackend holds the registered backend implementation.
var defaultBackend Backend

// RegisterBackend registers a backend implementation.
// This should be called by backend implementations in their init() function.
func RegisterBackend(b Backend) {
	defaultBackend = b
}

// GetBackend returns the registered backend implementation.
func GetBackend() Backend {
	return defaultBackend
}
