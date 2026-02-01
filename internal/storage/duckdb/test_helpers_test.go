package duckdb

import (
	"path/filepath"
	"testing"
)

// createDuckDBTestFile creates a temporary DuckDB file with the given SQL.
// It returns the path to the file and a cleanup function.
// The SQL should include CREATE TABLE and INSERT statements.
func createDuckDBTestFile(t *testing.T, sql string) (path string, cleanup func()) {
	t.Helper()

	// Skip if DuckDB CLI is not available
	skipIfNoDuckDBCLI(t)

	// Create temp directory
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	// Execute SQL with DuckDB CLI
	runDuckDBCommand(t, dbPath, sql)

	// Return path and cleanup function
	cleanup = func() {
		// Cleanup is handled by t.TempDir()
	}

	return dbPath, cleanup
}
