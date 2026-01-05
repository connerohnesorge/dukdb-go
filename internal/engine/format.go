// Package engine provides the core execution engine for the native Go DuckDB implementation.
package engine

import (
	"os"
	"strings"
)

// StorageFormat represents the storage format type.
type StorageFormat string

const (
	// StorageFormatAuto automatically detects the format from file or uses default.
	StorageFormatAuto StorageFormat = "auto"
	// StorageFormatDuckDB uses the native DuckDB file format.
	StorageFormatDuckDB StorageFormat = "duckdb"
	// StorageFormatWAL uses the internal WAL-based format (default for new files).
	StorageFormatWAL StorageFormat = "wal"
)

// DuckDB magic bytes constant for format detection.
const (
	duckDBMagicBytes  = "DUCK"
	duckDBMagicOffset = 8
)

// detectDuckDBFile checks if a file is a DuckDB format file by reading
// the magic bytes at the expected offset.
//
// DuckDB files have "DUCK" at offset 8 in the file header.
// Returns false if the file doesn't exist, is too small, or doesn't have
// the magic bytes.
func detectDuckDBFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()

	// Read magic bytes at the correct offset
	magic := make([]byte, 4)
	_, err = f.ReadAt(magic, duckDBMagicOffset)
	if err != nil {
		return false
	}

	return string(magic) == duckDBMagicBytes
}

// resolveStorageFormat determines the actual storage format to use based on
// the configured format and file existence/contents.
//
// Logic:
//   - If format is "duckdb", always use DuckDB format
//   - If format is "wal", always use WAL format
//   - If format is "auto" or empty:
//   - If file exists and has DuckDB magic bytes, use DuckDB format
//   - Otherwise, use WAL format (existing default behavior)
func resolveStorageFormat(path string, configFormat string) StorageFormat {
	format := StorageFormat(strings.ToLower(configFormat))

	// Explicit format selection
	switch format {
	case StorageFormatDuckDB:
		return StorageFormatDuckDB
	case StorageFormatWAL:
		return StorageFormatWAL
	}

	// Auto-detect: check if existing file is DuckDB format
	if _, err := os.Stat(path); err == nil {
		if detectDuckDBFile(path) {
			return StorageFormatDuckDB
		}
	}

	// Default to WAL format for new files or non-DuckDB files
	return StorageFormatWAL
}
