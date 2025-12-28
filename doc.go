// Package dukdb provides a database/sql driver for DuckDB.
//
// This package is a pure Go implementation that maintains API compatibility
// with the original duckdb-go driver while requiring zero cgo dependencies.
//
// The driver is registered with database/sql under the name "dukdb".
//
// # Usage
//
// To use this driver, import it in your application:
//
//	import (
//	    "database/sql"
//	    _ "github.com/dukdb/dukdb-go"
//	)
//
// Then open a database connection:
//
//	// In-memory database
//	db, err := sql.Open("dukdb", ":memory:")
//
//	// File-based database
//	db, err := sql.Open("dukdb", "/path/to/database.db")
//
// # Backend Interface
//
// The driver uses a pluggable backend architecture. By default, a backend
// must be registered that provides the actual DuckDB implementation.
// The Backend interface defines how the driver communicates with DuckDB.
//
// # Configuration
//
// Database configuration options can be passed via the DSN query string:
//
//	db, err := sql.Open("dukdb", ":memory:?access_mode=read_write&threads=4")
//
// Supported configuration options include:
//   - access_mode: "read_only" or "read_write" (default: "read_write")
//   - threads: Worker thread count (0 = auto, 1-128 = explicit)
//   - max_memory: Memory limit ("1GB", "512MB", "80%")
//   - default_order: "asc" or "desc" (default: "asc")
package dukdb
