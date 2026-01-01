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

	// AppendDataChunk appends a DataChunk directly to a table, bypassing SQL parsing.
	// This provides efficient bulk data loading for the Appender.
	// Returns the number of rows appended.
	AppendDataChunk(
		ctx context.Context,
		schema, table string,
		chunk *DataChunk,
	) (int64, error)

	// GetTableTypeInfos returns the TypeInfo for all columns in a table.
	// This is used by the Appender to create DataChunks with the correct types.
	GetTableTypeInfos(
		schema, table string,
	) ([]TypeInfo, []string, error)
}

// BackendConnCatalog is an optional interface for backends that provide
// access to the catalog. This is needed for virtual table registration.
type BackendConnCatalog interface {
	BackendConn
	// GetCatalog returns the catalog for this connection.
	// Returns nil if catalog access is not supported.
	GetCatalog() any
}

// BackendConnIdentifiable is an optional interface for backends that support
// connection identification.
//
// This interface enables the public [ConnId] API to retrieve unique connection
// IDs for debugging, tracing, and connection pool management. Backend
// implementations that support connection identification should implement
// this interface on their connection types.
//
// # Implementation Requirements
//
// Backends implementing this interface must guarantee:
//   - ID uniqueness: Each connection gets a distinct ID
//   - ID stability: The same connection always returns the same ID
//   - Never-reuse: IDs are never recycled within a process lifetime
//   - Thread-safety: Both methods must be safe for concurrent use
//
// # Example Implementation
//
// See [internal/engine.EngineConn] for a reference implementation that uses
// atomic counters for ID generation.
type BackendConnIdentifiable interface {
	// ID returns the unique connection ID.
	//
	// The ID is assigned when the connection is created and remains stable
	// throughout the connection's lifetime. IDs are unique within a process
	// and never reused (monotonically increasing).
	//
	// Returns 0 only if the implementation failed to assign an ID, which
	// should not occur in normal operation. The public [ConnId] API treats
	// 0 as an invalid ID returned only on error.
	ID() uint64

	// IsClosed returns whether the connection has been closed.
	//
	// This is used by [ConnId] to return an appropriate error when
	// attempting to get the ID of a closed connection. Implementations
	// should return true after [BackendConn.Close] has been called.
	IsClosed() bool
}

// BackendConnTableExtractor is an optional interface for backends that support
// extracting table names from SQL queries.
//
// This interface enables the public [GetTableNames] API to extract table
// references from SQL queries without executing them. Implementations should
// parse the query and return all table names referenced in the statement.
//
// # Implementation Requirements
//
// Backends implementing this interface must:
//   - Parse the query to extract table references
//   - Return an empty slice (not nil) for queries with no table references
//   - Return an error for queries that cannot be parsed
//   - Handle the qualified parameter to return qualified (schema.table) or unqualified names
//
// # Supported Statement Types
//
// Implementations should support at minimum:
//   - SELECT (including JOINs and subqueries)
//   - INSERT (target table and SELECT subquery)
//   - UPDATE (target table and WHERE subqueries)
//   - DELETE (target table and WHERE subqueries)
//   - CREATE TABLE (the table being created)
//   - DROP TABLE (the table being dropped)
type BackendConnTableExtractor interface {
	// ExtractTableNames parses the query and returns all referenced table names.
	//
	// If qualified is true, returns fully qualified names (e.g., "schema.table").
	// If qualified is false, returns just the table name (e.g., "table").
	//
	// Returns:
	//   - A sorted, deduplicated slice of table names
	//   - An empty slice (not nil) if no tables are referenced
	//   - An error if the query cannot be parsed
	ExtractTableNames(query string, qualified bool) ([]string, error)
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

// BackendStmtIntrospector is an optional interface for backends that support
// statement introspection. Backends can implement this to provide metadata
// about prepared statements.
type BackendStmtIntrospector interface {
	BackendStmt

	// StatementType returns the type of the prepared statement.
	StatementType() StmtType

	// ParamName returns the name of the parameter at the given index (1-based).
	// Returns empty string for positional parameters.
	ParamName(index int) string

	// ParamType returns the type of the parameter at the given index (1-based).
	ParamType(index int) Type

	// ColumnCount returns the number of result columns.
	// Returns 0 for non-SELECT statements.
	ColumnCount() int

	// ColumnName returns the name of the result column at the given index (0-based).
	ColumnName(index int) string

	// ColumnType returns the type of the result column at the given index (0-based).
	ColumnType(index int) Type

	// ColumnTypeInfo returns extended type info for the result column at the given index (0-based).
	ColumnTypeInfo(index int) TypeInfo
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
	STATEMENT_TYPE_INVALID           StmtType = 0
	STATEMENT_TYPE_SELECT            StmtType = 1
	STATEMENT_TYPE_INSERT            StmtType = 2
	STATEMENT_TYPE_UPDATE            StmtType = 3
	STATEMENT_TYPE_EXPLAIN           StmtType = 4
	STATEMENT_TYPE_DELETE            StmtType = 5
	STATEMENT_TYPE_PREPARE           StmtType = 6
	STATEMENT_TYPE_CREATE            StmtType = 7
	STATEMENT_TYPE_EXECUTE           StmtType = 8
	STATEMENT_TYPE_ALTER             StmtType = 9
	STATEMENT_TYPE_TRANSACTION       StmtType = 10
	STATEMENT_TYPE_COPY              StmtType = 11
	STATEMENT_TYPE_ANALYZE           StmtType = 12
	STATEMENT_TYPE_VARIABLE_SET      StmtType = 13
	STATEMENT_TYPE_CREATE_FUNC       StmtType = 14
	STATEMENT_TYPE_DROP              StmtType = 15
	STATEMENT_TYPE_EXPORT            StmtType = 16
	STATEMENT_TYPE_PRAGMA            StmtType = 17
	STATEMENT_TYPE_VACUUM            StmtType = 18
	STATEMENT_TYPE_CALL              StmtType = 19
	STATEMENT_TYPE_SET               StmtType = 20
	STATEMENT_TYPE_LOAD              StmtType = 21
	STATEMENT_TYPE_RELATION          StmtType = 22
	STATEMENT_TYPE_EXTENSION         StmtType = 23
	STATEMENT_TYPE_LOGICAL_PLAN      StmtType = 24
	STATEMENT_TYPE_ATTACH            StmtType = 25
	STATEMENT_TYPE_DETACH            StmtType = 26
	STATEMENT_TYPE_MULTI             StmtType = 27
	STATEMENT_TYPE_MERGE_INTO        StmtType = 28
	STATEMENT_TYPE_UPDATE_EXTENSIONS StmtType = 29
	STATEMENT_TYPE_COPY_DATABASE     StmtType = 30
)

// StmtReturnType indicates what a statement returns when executed.
type StmtReturnType uint8

const (
	// RETURN_QUERY_RESULT - Statement returns rows (SELECT, PRAGMA, SHOW, EXPLAIN)
	RETURN_QUERY_RESULT StmtReturnType = iota

	// RETURN_CHANGED_ROWS - Statement returns affected row count (INSERT, UPDATE, DELETE)
	RETURN_CHANGED_ROWS

	// RETURN_NOTHING - Statement returns nothing (DDL, SET, ATTACH, etc.)
	RETURN_NOTHING
)

// StmtProperties provides metadata about statement behavior.
type StmtProperties struct {
	Type        StmtType       // Statement type (SELECT, INSERT, etc.)
	ReturnType  StmtReturnType // What the statement returns
	IsReadOnly  bool           // True if statement doesn't modify data
	IsStreaming bool           // True if result can be streamed
	ColumnCount int            // Number of result columns (0 for non-query)
	ParamCount  int            // Number of parameters
}

// BackendStmtProperties extends BackendStmt with properties access.
type BackendStmtProperties interface {
	BackendStmt
	// Properties returns metadata about the statement's behavior.
	Properties() StmtProperties
}

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
