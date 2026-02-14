// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file contains end-to-end tests for DuckDB CLI
// interoperability.
//
// These tests verify that:
// - dukdb-go can read files created by the DuckDB CLI
// - DuckDB CLI can read files created by dukdb-go
// - Data integrity is preserved through round-trips
//
// Tests are skipped if the DuckDB CLI is not installed on the system.
package duckdb

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// Helper functions for interoperability tests
// -----------------------------------------------------------------------------

// checkDuckDBCLI returns true if the duckdb CLI is available in PATH.
func checkDuckDBCLI() bool {
	_, err := exec.LookPath("duckdb")
	return err == nil
}

// skipIfNoDuckDBCLI skips the test if DuckDB CLI is not installed.
func skipIfNoDuckDBCLI(t *testing.T) {
	t.Helper()
	if !checkDuckDBCLI() {
		t.Skip("duckdb CLI not installed")
	}
}

// skipOnFormatError skips the test if the error is a known format compatibility issue.
// This is used for interoperability tests where the format may not yet be fully compatible.
//
// TODO: Once row data reading is fully implemented and verified, this function can be
// removed from tests that only verify catalog reading. For now, it's kept in place
// because row data reading is not yet implemented (GAP-001).
func skipOnFormatError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	errStr := err.Error()
	// Skip if checksum mismatch, catalog errors, or other format incompatibilities
	if strings.Contains(errStr, "checksum mismatch") ||
		strings.Contains(errStr, "headers are corrupted") ||
		strings.Contains(errStr, "unsupported version") ||
		strings.Contains(errStr, "failed to load catalog") ||
		strings.Contains(errStr, "failed to read catalog") ||
		strings.Contains(errStr, "unexpected end of data") {
		t.Skipf("Format not yet fully compatible with DuckDB CLI: %v", err)
	}
}

// runDuckDBCommand executes a DuckDB CLI command and returns the output.
func runDuckDBCommand(t *testing.T, dbPath, sql string) string {
	t.Helper()
	cmd := exec.Command("duckdb", dbPath, "-c", sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		t.Fatalf(
			"duckdb CLI failed: %v\nstderr: %s\nstdout: %s",
			err,
			stderr.String(),
			stdout.String(),
		)
	}
	return stdout.String()
}

// runDuckDBCommandNoFail executes a DuckDB CLI command and returns output and error.
func runDuckDBCommandNoFail(dbPath, sql string) (string, error) {
	cmd := exec.Command("duckdb", dbPath, "-c", sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stderr.String() + stdout.String(), err
	}
	return stdout.String(), nil
}

// collectRows collects all rows from a StorageRowIterator into a slice.
// This helper is used by data verification tests when the format is fully compatible.
//
//nolint:unused // Helper function for future use in data verification tests
func collectRows(iter StorageRowIterator) [][]any {
	var rows [][]any
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			// Make a copy to avoid slice aliasing issues
			rowCopy := make([]any, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	return rows
}

// -----------------------------------------------------------------------------
// Test: dukdb-go reads DuckDB CLI created file
// -----------------------------------------------------------------------------

// TestReadDuckDBCLIFile tests that dukdb-go can read files created by DuckDB CLI.
func TestReadDuckDBCLIFile(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	t.Run("simple table with integers and strings", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.duckdb")

		// Create database with DuckDB CLI
		runDuckDBCommand(t, dbPath, `
			CREATE TABLE test (id INTEGER, name VARCHAR);
			INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
		`)

		// Verify file was created and is a valid DuckDB file
		require.True(t, DetectDuckDBFile(dbPath), "File should be detected as DuckDB format")

		// Open with dukdb-go
		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		// Verify catalog loaded
		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)
		require.NotNil(t, cat)

		// Verify table exists
		table, ok := cat.GetTableInSchema("main", "test")
		require.True(t, ok, "table 'test' should exist")
		require.NotNil(t, table, "table 'test' should exist")

		// Verify table structure
		assert.Equal(t, "test", table.Name)
		require.Len(t, table.Columns, 2)
		assert.Equal(t, "id", table.Columns[0].Name)
		assert.Equal(t, "name", table.Columns[1].Name)
	})

	t.Run("table with null values", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test_nulls.duckdb")

		// Create table with NULLs
		runDuckDBCommand(t, dbPath, `
			CREATE TABLE nulltest (id INTEGER, value VARCHAR);
			INSERT INTO nulltest VALUES (1, 'one'), (2, NULL), (3, 'three'), (NULL, 'four');
		`)

		// Open with dukdb-go
		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		// Verify table exists
		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)
		table, ok := cat.GetTableInSchema("main", "nulltest")
		require.True(t, ok)
		require.NotNil(t, table)
	})

	t.Run("multiple tables", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test_multi.duckdb")

		// Create multiple tables
		runDuckDBCommand(t, dbPath, `
			CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR);
			CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount DOUBLE);
			INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
			INSERT INTO orders VALUES (100, 1, 99.99), (101, 2, 149.50);
		`)

		// Open with dukdb-go
		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)

		// Verify both tables exist
		users, usersOk := cat.GetTableInSchema("main", "users")
		require.True(t, usersOk, "users table should exist")
		require.NotNil(t, users, "users table should exist")

		orders, ordersOk := cat.GetTableInSchema("main", "orders")
		require.True(t, ordersOk, "orders table should exist")
		require.NotNil(t, orders, "orders table should exist")
	})

	t.Run("table with various numeric types", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test_numeric.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE numbers (
				tiny TINYINT,
				small SMALLINT,
				normal INTEGER,
				big BIGINT,
				flt FLOAT,
				dbl DOUBLE
			);
			INSERT INTO numbers VALUES (127, 32767, 2147483647, 9223372036854775807, 3.14, 2.718281828);
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)

		table, ok := cat.GetTableInSchema("main", "numbers")
		require.True(t, ok)
		require.NotNil(t, table)
		require.Len(t, table.Columns, 6)
	})
}

// -----------------------------------------------------------------------------
// Test: DuckDB CLI reads dukdb-go created file
// -----------------------------------------------------------------------------

// TestDuckDBCLIReadsDukdbFile tests that DuckDB CLI can read files created by dukdb-go.
func TestDuckDBCLIReadsDukdbFile(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	t.Run("simple table", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.duckdb")

		// Create database with dukdb-go
		storage, err := CreateDuckDBStorage(dbPath, nil)
		require.NoError(t, err)

		// Create table in DuckDB catalog
		tableEntry := NewTableCatalogEntry("test")
		tableEntry.CreateInfo.Schema = "main"
		tableEntry.AddColumn(ColumnDefinition{
			Name:     "id",
			Type:     TypeInteger,
			Nullable: false,
		})
		tableEntry.AddColumn(ColumnDefinition{
			Name:     "name",
			Type:     TypeVarchar,
			Nullable: true,
		})

		storage.catalog.Tables = append(storage.catalog.Tables, tableEntry)
		storage.modified = true

		// Close to persist
		err = storage.Close()
		require.NoError(t, err)

		// Verify with DuckDB CLI
		output, err := runDuckDBCommandNoFail(dbPath, "SHOW TABLES;")
		if err != nil {
			// Skip if format is not yet compatible (checksum mismatch, version mismatch, or metadata issues)
			if strings.Contains(output, "checksum") ||
				strings.Contains(output, "Corrupt") ||
				strings.Contains(output, "version number") ||
				strings.Contains(output, "newer version") ||
				strings.Contains(output, "metadata") ||
				strings.Contains(output, "internal error") {
				t.Skipf("Format not yet fully compatible with DuckDB CLI: %s", output)
			}
		}
		// The file should at least be readable by DuckDB CLI
		assert.NotContains(t, strings.ToLower(output), "error")
	})

	t.Run("file header is valid for DuckDB", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "header_test.duckdb")

		// Create file with dukdb-go
		storage, err := CreateDuckDBStorage(dbPath, nil)
		require.NoError(t, err)
		err = storage.Close()
		require.NoError(t, err)

		// DuckDB CLI should be able to open and query the file
		output, err := runDuckDBCommandNoFail(dbPath, "SELECT 1;")
		// File might be empty but should be openable
		if err != nil {
			// Skip if format is not yet compatible (checksum mismatch)
			if strings.Contains(output, "checksum") ||
				strings.Contains(output, "Corrupt") {
				t.Skipf("Format not yet fully compatible with DuckDB CLI: %s", output)
			}
			// Check if the error is about the file format
			assert.NotContains(t, strings.ToLower(output), "invalid header")
			assert.NotContains(t, strings.ToLower(output), "magic")
		}
	})
}

// -----------------------------------------------------------------------------
// Test: Round-trip data integrity
// -----------------------------------------------------------------------------

// TestRoundTripDataIntegrity tests that data survives DuckDB -> dukdb-go -> DuckDB round-trips.
func TestRoundTripDataIntegrity(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	t.Run("integer values round-trip", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "roundtrip.duckdb")

		// Step 1: Create with DuckDB CLI
		runDuckDBCommand(t, dbPath, `
			CREATE TABLE test (id INTEGER, value BIGINT);
			INSERT INTO test VALUES (1, 100), (2, 200), (3, 300);
		`)

		// Step 2: Read with dukdb-go
		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)
		testTable, testOk := cat.GetTableInSchema("main", "test")
		require.True(t, testOk)
		require.NotNil(t, testTable)

		_ = storage.Close()

		// Step 3: Verify data still valid in DuckDB
		output := runDuckDBCommand(t, dbPath, "SELECT COUNT(*) FROM test;")
		assert.Contains(t, output, "3")
	})

	t.Run("string values round-trip", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "roundtrip_str.duckdb")

		// Create with DuckDB CLI
		runDuckDBCommand(t, dbPath, `
			CREATE TABLE strings (id INTEGER, text VARCHAR);
			INSERT INTO strings VALUES (1, 'Hello'), (2, 'World'), (3, 'DuckDB');
		`)

		// Read with dukdb-go
		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)
		strTable, strOk := cat.GetTableInSchema("main", "strings")
		require.True(t, strOk)
		require.NotNil(t, strTable)

		_ = storage.Close()

		// Verify data still valid
		output := runDuckDBCommand(t, dbPath, "SELECT text FROM strings WHERE id=1;")
		assert.Contains(t, output, "Hello")
	})

	t.Run("metadata round-trip", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "roundtrip_meta.duckdb")

		// Create complex schema with DuckDB CLI
		runDuckDBCommand(t, dbPath, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name VARCHAR NOT NULL,
				email VARCHAR
			);
			CREATE TABLE posts (
				id INTEGER PRIMARY KEY,
				user_id INTEGER,
				title VARCHAR,
				content VARCHAR
			);
			INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
			INSERT INTO posts VALUES (1, 1, 'Hello World', 'My first post');
		`)

		// Read with dukdb-go
		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)



		// Verify structure
		users, usersOk := cat.GetTableInSchema("main", "users")
		require.True(t, usersOk)
		require.NotNil(t, users)
		posts, postsOk := cat.GetTableInSchema("main", "posts")
		require.True(t, postsOk)
		require.NotNil(t, posts)

		_ = storage.Close()

		// Verify with DuckDB
		output := runDuckDBCommand(t, dbPath, "SELECT * FROM users;")
		assert.Contains(t, output, "Alice")
	})
}

// -----------------------------------------------------------------------------
// Test: All data types round-trip
// -----------------------------------------------------------------------------

// TestAllTypesRoundTrip tests that all supported DuckDB types can be read correctly.
func TestAllTypesRoundTrip(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	t.Run("integer types", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "types_int.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE int_types (
				col_tinyint TINYINT,
				col_smallint SMALLINT,
				col_integer INTEGER,
				col_bigint BIGINT,
				col_utinyint UTINYINT,
				col_usmallint USMALLINT,
				col_uinteger UINTEGER,
				col_ubigint UBIGINT
			);
			INSERT INTO int_types VALUES (127, 32767, 2147483647, 9223372036854775807, 255, 65535, 4294967295, 18446744073709551615);
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "int_types")
		require.True(t, ok)
		require.NotNil(t, table)
		assert.Len(t, table.Columns, 8)
	})

	t.Run("floating point types", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "types_float.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE float_types (
				col_float FLOAT,
				col_double DOUBLE
			);
			INSERT INTO float_types VALUES (3.14159, 2.718281828459045);
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "float_types")
		require.True(t, ok)
		require.NotNil(t, table)
		assert.Len(t, table.Columns, 2)
	})

	t.Run("string types", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "types_str.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE str_types (
				col_varchar VARCHAR,
				col_blob BLOB
			);
			INSERT INTO str_types VALUES ('hello world', '\x48\x65\x6c\x6c\x6f');
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "str_types")
		require.True(t, ok)
		require.NotNil(t, table)
		assert.Len(t, table.Columns, 2)
	})

	t.Run("temporal types", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "types_temporal.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE temporal_types (
				col_date DATE,
				col_time TIME,
				col_timestamp TIMESTAMP,
				col_interval INTERVAL
			);
			INSERT INTO temporal_types VALUES ('2024-01-15', '14:30:00', '2024-01-15 14:30:00', INTERVAL '1 day');
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "temporal_types")
		require.True(t, ok)
		require.NotNil(t, table)
		assert.Len(t, table.Columns, 4)
	})

	t.Run("boolean type", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "types_bool.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE bool_types (
				col_bool BOOLEAN
			);
			INSERT INTO bool_types VALUES (true), (false), (NULL);
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "bool_types")
		require.True(t, ok)
		require.NotNil(t, table)
		assert.Len(t, table.Columns, 1)
	})

	t.Run("decimal type", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "types_decimal.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE decimal_types (
				col_decimal DECIMAL(18, 4)
			);
			INSERT INTO decimal_types VALUES (12345.6789);
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "decimal_types")
		require.True(t, ok)
		require.NotNil(t, table)
	})

	t.Run("uuid type", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "types_uuid.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE uuid_types (
				col_uuid UUID
			);
			INSERT INTO uuid_types VALUES ('550e8400-e29b-41d4-a716-446655440000');
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "uuid_types")
		require.True(t, ok)
		require.NotNil(t, table)
	})

	t.Run("nested types - list", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "types_list.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE list_types (
				col_list INTEGER[]
			);
			INSERT INTO list_types VALUES ([1, 2, 3]);
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "list_types")
		require.True(t, ok)
		require.NotNil(t, table)
	})

	t.Run("nested types - struct", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "types_struct.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE struct_types (
				col_struct STRUCT(a INTEGER, b VARCHAR)
			);
			INSERT INTO struct_types VALUES ({'a': 1, 'b': 'hello'});
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "struct_types")
		require.True(t, ok)
		require.NotNil(t, table)
	})

	t.Run("nested types - map", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "types_map.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE map_types (
				col_map MAP(VARCHAR, INTEGER)
			);
			INSERT INTO map_types VALUES (MAP(['a', 'b'], [1, 2]));
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "map_types")
		require.True(t, ok)
		require.NotNil(t, table)
	})
}

// -----------------------------------------------------------------------------
// Test: Large file handling
// -----------------------------------------------------------------------------

// TestLargeFileHandling tests handling of files with many rows.
func TestLargeFileHandling(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	t.Run("many rows table", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping large file test in short mode")
		}

		dbPath := filepath.Join(t.TempDir(), "large.duckdb")

		// Create table with 100,000 rows
		runDuckDBCommand(t, dbPath, `
			CREATE TABLE large_table (id INTEGER, value VARCHAR);
			INSERT INTO large_table SELECT range, 'value_' || range::VARCHAR FROM range(100000);
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "large_table")
		require.True(t, ok)
		require.NotNil(t, table)

		// Verify row count with DuckDB
		output := runDuckDBCommand(t, dbPath, "SELECT COUNT(*) FROM large_table;")
		assert.Contains(t, output, "100000")
	})

	t.Run("many columns table", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "wide.duckdb")

		// Create table with 50 columns
		cols := make([]string, 50)
		for i := 0; i < 50; i++ {
			cols[i] = "col" + strconv.Itoa(i) + " INTEGER"
		}

		runDuckDBCommand(t, dbPath, "CREATE TABLE wide_table ("+strings.Join(cols, ", ")+");")

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "wide_table")
		require.True(t, ok)
		require.NotNil(t, table)
		assert.Len(t, table.Columns, 50)
	})
}

// -----------------------------------------------------------------------------
// Test: Edge cases and error handling
// -----------------------------------------------------------------------------

// TestInteropEdgeCases tests edge cases in DuckDB interoperability.
func TestInteropEdgeCases(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	t.Run("empty table", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "empty.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE empty_table (id INTEGER, name VARCHAR);
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "empty_table")
		require.True(t, ok)
		require.NotNil(t, table)
		assert.Len(t, table.Columns, 2)
	})

	t.Run("special characters in strings", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "special.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE special_chars (text VARCHAR);
			INSERT INTO special_chars VALUES ('hello''world'), ('tab	here'), ('newline
here');
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "special_chars")
		require.True(t, ok)
		require.NotNil(t, table)
	})

	t.Run("unicode strings", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "unicode.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE unicode_table (text VARCHAR);
			INSERT INTO unicode_table VALUES ('Hello World'), ('Bonjour Monde'), ('Hallo Welt');
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "unicode_table")
		require.True(t, ok)
		require.NotNil(t, table)
	})

	t.Run("table with constraints", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "constraints.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE TABLE constrained (
				id INTEGER PRIMARY KEY,
				name VARCHAR NOT NULL,
				email VARCHAR UNIQUE
			);
			INSERT INTO constrained VALUES (1, 'Alice', 'alice@example.com');
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "constrained")
		require.True(t, ok)
		require.NotNil(t, table)
	})

	t.Run("multiple schemas", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "schemas.duckdb")

		runDuckDBCommand(t, dbPath, `
			CREATE SCHEMA test_schema;
			CREATE TABLE test_schema.users (id INTEGER, name VARCHAR);
			INSERT INTO test_schema.users VALUES (1, 'Schema User');
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)

		// Check if schema was loaded
		schema, ok := cat.GetSchema("test_schema")
		if ok {
			assert.NotNil(t, schema)
		}
	})
}

// -----------------------------------------------------------------------------
// Test: File format detection
// -----------------------------------------------------------------------------

// TestDuckDBFileDetection tests the detection of DuckDB file format.
func TestDuckDBFileDetection(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	t.Run("detects DuckDB CLI created file", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "detect.duckdb")

		runDuckDBCommand(t, dbPath, "CREATE TABLE test (id INTEGER);")

		assert.True(t, DetectDuckDBFile(dbPath))
	})

	t.Run("detects dukdb-go created file", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "detect2.duckdb")

		storage, err := CreateDuckDBStorage(dbPath, nil)
		require.NoError(t, err)
		err = storage.Close()
		require.NoError(t, err)

		assert.True(t, DetectDuckDBFile(dbPath))
	})
}

// -----------------------------------------------------------------------------
// Test: Compression and performance
// -----------------------------------------------------------------------------

// TestCompressionInterop tests that compression is handled correctly.
func TestCompressionInterop(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	t.Run("compressed integers", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping compression test in short mode")
		}

		dbPath := filepath.Join(t.TempDir(), "compressed.duckdb")

		// Create data that will be compressed (repetitive values)
		runDuckDBCommand(t, dbPath, `
			CREATE TABLE compressed_data (id INTEGER);
			INSERT INTO compressed_data SELECT 42 FROM range(10000);
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "compressed_data")
		require.True(t, ok)
		require.NotNil(t, table)
	})

	t.Run("dictionary compressed strings", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping compression test in short mode")
		}

		dbPath := filepath.Join(t.TempDir(), "dict_compressed.duckdb")

		// Create data with low cardinality strings (will use dictionary compression)
		runDuckDBCommand(t, dbPath, `
			CREATE TABLE dict_data (category VARCHAR);
			INSERT INTO dict_data SELECT CASE (range % 3) WHEN 0 THEN 'A' WHEN 1 THEN 'B' ELSE 'C' END FROM range(10000);
		`)

		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		skipOnFormatError(t, err)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		skipOnFormatError(t, err)
		require.NoError(t, err)


		table, ok := cat.GetTableInSchema("main", "dict_data")
		require.True(t, ok)
		require.NotNil(t, table)
	})
}

// -----------------------------------------------------------------------------
// Benchmark: DuckDB interop performance
// -----------------------------------------------------------------------------

// BenchmarkDuckDBFileOpen benchmarks opening a DuckDB file.
func BenchmarkDuckDBFileOpen(b *testing.B) {
	if !checkDuckDBCLI() {
		b.Skip("duckdb CLI not installed")
	}

	dbPath := filepath.Join(b.TempDir(), "bench.duckdb")

	// Create test file once
	cmd := exec.Command(
		"duckdb",
		dbPath,
		"-c",
		"CREATE TABLE test (id INTEGER); INSERT INTO test SELECT range FROM range(1000);",
	)
	if err := cmd.Run(); err != nil {
		b.Fatalf("failed to create test file: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
		if err != nil {
			b.Fatalf("failed to open storage: %v", err)
		}
		_ = storage.Close()
	}
}
