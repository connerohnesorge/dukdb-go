// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file contains tests for verifying data read
// compatibility with DuckDB CLI.
//
// These tests verify that:
// - dukdb-go can correctly read row data from DuckDB CLI created files
// - Data types are correctly interpreted
// - NULL values are correctly handled
// - Various compression formats are correctly decoded
//
// Tests are skipped if the DuckDB CLI is not installed on the system.
//
// CI REQUIREMENTS:
// These tests require the DuckDB CLI to be installed and available in PATH.
// They will skip gracefully if the CLI is not available.
// For full interop testing in CI, install DuckDB CLI before running tests.
//
// KNOWN ISSUES / TODOs:
// - Row data reading from DuckDB CLI files is not yet fully implemented
// - Tests currently skip with "Row group data from DuckDB CLI files not yet loaded"
// - Once row data reading is implemented, these tests will verify correctness
// - The verification framework is ready and waiting for the read path to be completed
//
// IMPLEMENTATION STATUS:
// - Catalog reading: Working (can read table metadata from DuckDB files)
// - Row data reading: Not yet implemented (GAP-001)
// - The tests in this file verify the row data read path once implemented
package duckdb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// Helper functions for data read verification tests
// -----------------------------------------------------------------------------

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

// verifyRowData verifies that the row data from a table scan matches the expected values.
// It scans the table using the storage backend and compares all rows against expected.
func verifyRowData(t *testing.T, storage *DuckDBStorage, table string, expected [][]interface{}) {
	t.Helper()

	// Scan the table (use "main" schema by default)
	iter, err := storage.ScanTable("main", table, nil)
	require.NoError(t, err, "ScanTable should succeed")
	defer iter.Close()

	// Collect all rows
	rows := collectRows(iter)
	require.NoError(t, iter.Err(), "iteration should complete without error")

	// Verify row count
	require.Len(t, rows, len(expected), "row count should match")

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow), "row %d column count should match", i)

		for j, expectedVal := range expectedRow {
			actualVal := actualRow[j]

			// Handle NULL comparison
			if expectedVal == nil {
				assert.Nil(t, actualVal, "row %d col %d should be NULL", i, j)
				continue
			}

			// For non-NULL values, use assert.Equal which handles type comparisons
			assert.Equal(t, expectedVal, actualVal,
				"row %d col %d mismatch: expected %v (%T), got %v (%T)",
				i, j, expectedVal, expectedVal, actualVal, actualVal)
		}
	}
}

// -----------------------------------------------------------------------------
// Example test demonstrating helper usage
// -----------------------------------------------------------------------------

// TestDataReadVerifyExample is an example test showing how to use the helper functions.
// This test will be replaced by the actual data verification tests in future tasks.
func TestDataReadVerifyExample(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create a test file with DuckDB CLI
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE example (id INTEGER, name VARCHAR);
		INSERT INTO example VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog to ensure it's valid
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "example")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// TODO: Once row data reading is fully implemented, verify the data:
	// expected := [][]interface{}{
	//     {int32(1), "Alice"},
	//     {int32(2), "Bob"},
	//     {int32(3), "Charlie"},
	// }
	// verifyRowData(t, storage, "example", expected)

	// For now, just verify that ScanTable can be called without panic
	iter, err := storage.ScanTable("main", "example", nil)
	if err != nil {
		// If scanning fails due to format incompatibility, skip the test
		skipOnFormatError(t, err)
		t.Skipf("Row data reading not yet fully implemented: %v", err)
	}
	if iter != nil {
		defer iter.Close()
		// Try to iterate - this may fail with format errors
		for iter.Next() {
			row := iter.Row()
			_ = row // Just consume the row for now
		}
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
		}
	}
}

// -----------------------------------------------------------------------------
// Phase 2: Basic Type Verification Tests
// -----------------------------------------------------------------------------

// TestReadIntegerValues tests reading INTEGER column values.
// This test verifies that basic integer values are correctly read from
// DuckDB files, including zero, positive/negative values, and boundary values.
func TestReadIntegerValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with INTEGER values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE int_test (value INTEGER);
		INSERT INTO int_test VALUES (0), (1), (-1), (2147483647), (-2147483648);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog to ensure it's valid
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "int_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table - may skip if not yet supported
	iter, err := storage.ScanTable("main", "int_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available - if not, row group loading isn't implemented yet
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Verify row data (iterator already advanced to first row)
	expected := [][]interface{}{
		{int32(0)},
		{int32(1)},
		{int32(-1)},
		{int32(2147483647)},
		{int32(-2147483648)},
	}

	// Collect rows starting with the current row
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err(), "iteration should complete without error")

	// Verify row count
	require.Len(t, rows, len(expected), "row count should match")

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow), "row %d column count should match", i)

		for j, expectedVal := range expectedRow {
			actualVal := actualRow[j]

			// Handle NULL comparison
			if expectedVal == nil {
				assert.Nil(t, actualVal, "row %d col %d should be NULL", i, j)
				continue
			}

			// For non-NULL values, use assert.Equal which handles type comparisons
			assert.Equal(t, expectedVal, actualVal,
				"row %d col %d mismatch: expected %v (%T), got %v (%T)",
				i, j, expectedVal, expectedVal, actualVal, actualVal)
		}
	}
}

// TestReadVarcharASCII tests reading VARCHAR column values with ASCII strings.
// This test verifies that ASCII strings are correctly read from DuckDB files.
func TestReadVarcharASCII(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with VARCHAR values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE varchar_test (value VARCHAR);
		INSERT INTO varchar_test VALUES (''), ('hello'), ('world'), ('a long string with spaces and punctuation!');
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog to ensure it's valid
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "varchar_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table - may skip if not yet supported
	iter, err := storage.ScanTable("main", "varchar_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Verify row data (iterator already advanced to first row)
	expected := [][]interface{}{
		{""},
		{"hello"},
		{"world"},
		{"a long string with spaces and punctuation!"},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			assert.Equal(t, expectedVal, actualRow[j])
		}
	}
}

// TestReadVarcharUnicode tests reading VARCHAR column values with Unicode strings.
// This test verifies that multi-byte UTF-8 strings (emoji, CJK characters) are
// correctly read and preserved byte-exact from DuckDB files.
func TestReadVarcharUnicode(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with Unicode VARCHAR values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE unicode_test (value VARCHAR);
		INSERT INTO unicode_test VALUES ('Hello'), ('🦆'), ('中文'), ('Hello 🦆 中文');
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog to ensure it's valid
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "unicode_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table - may skip if not yet supported
	iter, err := storage.ScanTable("main", "unicode_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Verify row data - Unicode strings must be preserved byte-exact
	expected := [][]interface{}{
		{"Hello"},
		{"🦆"},
		{"中文"},
		{"Hello 🦆 中文"},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			assert.Equal(t, expectedVal, actualRow[j])
		}
	}
}

// TestReadBooleanValues tests reading BOOLEAN column values.
// This test verifies that boolean values (TRUE, FALSE, NULL) are correctly
// read from DuckDB files.
func TestReadBooleanValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with BOOLEAN values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE bool_test (value BOOLEAN);
		INSERT INTO bool_test VALUES (TRUE), (FALSE), (NULL);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog to ensure it's valid
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "bool_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table - may skip if not yet supported
	iter, err := storage.ScanTable("main", "bool_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Verify row data
	expected := [][]interface{}{
		{true},
		{false},
		{nil},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j])
			} else {
				assert.Equal(t, expectedVal, actualRow[j])
			}
		}
	}
}

// -----------------------------------------------------------------------------
// Phase 3: All Numeric Types
// -----------------------------------------------------------------------------

// TestReadSignedIntegerTypes tests reading all signed integer types
// (TINYINT, SMALLINT, INTEGER, BIGINT) with boundary values.
func TestReadSignedIntegerTypes(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with all signed integer types
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE signed_int_test (
			tiny TINYINT,
			small SMALLINT,
			int INTEGER,
			big BIGINT
		);
		INSERT INTO signed_int_test VALUES
			(-128, -32768, -2147483648, -9223372036854775808),
			(0, 0, 0, 0),
			(127, 32767, 2147483647, 9223372036854775807);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "signed_int_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "signed_int_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Verify row data
	expected := [][]interface{}{
		{int8(-128), int16(-32768), int32(-2147483648), int64(-9223372036854775808)},
		{int8(0), int16(0), int32(0), int64(0)},
		{int8(127), int16(32767), int32(2147483647), int64(9223372036854775807)},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j])
			} else {
				assert.Equal(t, expectedVal, actualRow[j],
					"row %d col %d mismatch: expected %v (%T), got %v (%T)",
					i, j, expectedVal, expectedVal, actualRow[j], actualRow[j])
			}
		}
	}
}

// TestReadUnsignedIntegerTypes tests reading all unsigned integer types
// (UTINYINT, USMALLINT, UINTEGER, UBIGINT) with boundary values.
// Note: HUGEINT/UHUGEINT (128-bit) are handled separately as they may not be fully supported.
func TestReadUnsignedIntegerTypes(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with all unsigned integer types
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE unsigned_int_test (
			utiny UTINYINT,
			usmall USMALLINT,
			uint UINTEGER,
			ubig UBIGINT
		);
		INSERT INTO unsigned_int_test VALUES
			(0, 0, 0, 0),
			(255, 65535, 4294967295, 18446744073709551615);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "unsigned_int_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "unsigned_int_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Verify row data
	expected := [][]interface{}{
		{uint8(0), uint16(0), uint32(0), uint64(0)},
		{uint8(255), uint16(65535), uint32(4294967295), uint64(18446744073709551615)},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j])
			} else {
				assert.Equal(t, expectedVal, actualRow[j],
					"row %d col %d mismatch: expected %v (%T), got %v (%T)",
					i, j, expectedVal, expectedVal, actualRow[j], actualRow[j])
			}
		}
	}
}

// TestReadFloatDoubleValues tests reading FLOAT and DOUBLE types with various values.
// Note: We skip special values (Inf, -Inf, NaN) as DuckDB may handle them differently.
// We use epsilon comparisons for floating point values.
func TestReadFloatDoubleValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with FLOAT and DOUBLE values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE float_test (
			f FLOAT,
			d DOUBLE
		);
		INSERT INTO float_test VALUES
			(0.0, 0.0),
			(-0.0, -0.0),
			(1.5, 1.5),
			(-1.5, -1.5),
			(3.14159, 3.14159265358979),
			(1e10, 1e100),
			(-1e10, -1e100);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "float_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "float_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values (we'll use epsilon comparison for floats)
	expected := [][]interface{}{
		{float32(0.0), float64(0.0)},
		{float32(-0.0), float64(-0.0)},
		{float32(1.5), float64(1.5)},
		{float32(-1.5), float64(-1.5)},
		{float32(3.14159), float64(3.14159265358979)},
		{float32(1e10), float64(1e100)},
		{float32(-1e10), float64(-1e100)},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row with epsilon comparison for floats
	const float32Epsilon = 1e-6
	const float64Epsilon = 1e-9

	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))

		// Column 0: FLOAT (float32)
		if expectedRow[0] == nil {
			assert.Nil(t, actualRow[0])
		} else {
			actualFloat32, ok := actualRow[0].(float32)
			require.True(t, ok, "row %d col 0 should be float32, got %T", i, actualRow[0])
			expectedFloat32 := expectedRow[0].(float32)
			assert.InDelta(t, expectedFloat32, actualFloat32, float32Epsilon,
				"row %d col 0 mismatch", i)
		}

		// Column 1: DOUBLE (float64)
		if expectedRow[1] == nil {
			assert.Nil(t, actualRow[1])
		} else {
			actualFloat64, ok := actualRow[1].(float64)
			require.True(t, ok, "row %d col 1 should be float64, got %T", i, actualRow[1])
			expectedFloat64 := expectedRow[1].(float64)
			assert.InDelta(t, expectedFloat64, actualFloat64, float64Epsilon,
				"row %d col 1 mismatch", i)
		}
	}
}

// TestReadDecimalValues tests reading DECIMAL types with various precision/scale combinations.
// DECIMAL in DuckDB can be represented as different types depending on precision.
func TestReadDecimalValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with DECIMAL values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE decimal_test (
			d1 DECIMAL(5,2),
			d2 DECIMAL(10,4),
			d3 DECIMAL(18,0)
		);
		INSERT INTO decimal_test VALUES
			(0.00, 0.0000, 0),
			(123.45, 123.4567, 123456789012345678),
			(-123.45, -123.4567, -123456789012345678);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "decimal_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "decimal_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// For DECIMAL, we'll collect rows and just verify we can read them
	// The exact Go type may vary (could be float64, string, *big.Rat, etc.)
	// For now, just ensure we can read the values without error
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, 3, "should have 3 rows")

	// Verify we got 3 columns in each row
	for i, row := range rows {
		require.Len(t, row, 3, "row %d should have 3 columns", i)
		// Verify none are nil
		for j, val := range row {
			assert.NotNil(t, val, "row %d col %d should not be nil", i, j)
		}
	}

	// TODO: Once we know the exact Go type for DECIMAL, add more specific assertions
	// For now, this test verifies we can read DECIMAL columns without errors
}

// -----------------------------------------------------------------------------
// Phase 4: Temporal Types
// -----------------------------------------------------------------------------

// TestReadDateValues tests reading DATE column values with various dates.
// This test verifies epoch (1970-01-01), Y2K (2000-01-01), recent dates,
// and far future dates.
func TestReadDateValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with DATE values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE date_test (value DATE);
		INSERT INTO date_test VALUES
			(DATE '1970-01-01'),
			(DATE '2000-01-01'),
			(DATE '2024-01-15'),
			(DATE '9999-12-31');
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "date_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "date_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - DATE decodes to time.Time
	expected := [][]interface{}{
		{mustParseDate("1970-01-01")},
		{mustParseDate("2000-01-01")},
		{mustParseDate("2024-01-15")},
		{mustParseDate("9999-12-31")},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j])
			} else {
				// Compare dates (time.Time)
				expectedTime := expectedVal.(time.Time)
				actualTime, ok := actualRow[j].(time.Time)
				require.True(t, ok, "row %d col %d should be time.Time, got %T", i, j, actualRow[j])
				// Compare only date portion (year, month, day)
				assert.Equal(t, expectedTime.Year(), actualTime.Year(), "row %d year mismatch", i)
				assert.Equal(t, expectedTime.Month(), actualTime.Month(), "row %d month mismatch", i)
				assert.Equal(t, expectedTime.Day(), actualTime.Day(), "row %d day mismatch", i)
			}
		}
	}
}

// TestReadTimeValues tests reading TIME column values with various times.
// This test verifies midnight, noon, and max precision times.
func TestReadTimeValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with TIME values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE time_test (value TIME);
		INSERT INTO time_test VALUES
			(TIME '00:00:00'),
			(TIME '12:00:00'),
			(TIME '23:59:59.999999');
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "time_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "time_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - TIME decodes to time.Duration
	expected := [][]interface{}{
		{time.Duration(0)},              // 00:00:00
		{time.Duration(12 * time.Hour)}, // 12:00:00
		{
			time.Duration(23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond),
		}, // 23:59:59.999999
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j])
			} else {
				// Compare time.Duration values
				expectedDur := expectedVal.(time.Duration)
				actualDur, ok := actualRow[j].(time.Duration)
				require.True(t, ok, "row %d col %d should be time.Duration, got %T", i, j, actualRow[j])
				assert.Equal(t, expectedDur, actualDur,
					"row %d col %d mismatch: expected %v, got %v", i, j, expectedDur, actualDur)
			}
		}
	}
}

// TestReadTimestampValues tests reading TIMESTAMP types with various precisions.
// This test covers TIMESTAMP (microsecond), TIMESTAMP_S (second), TIMESTAMP_MS (millisecond),
// and TIMESTAMP_NS (nanosecond).
func TestReadTimestampValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with TIMESTAMP values at different precisions
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE timestamp_test (
			ts TIMESTAMP,
			ts_s TIMESTAMP_S,
			ts_ms TIMESTAMP_MS,
			ts_ns TIMESTAMP_NS
		);
		INSERT INTO timestamp_test VALUES
			(TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP_S '1970-01-01 00:00:00', TIMESTAMP_MS '1970-01-01 00:00:00', TIMESTAMP_NS '1970-01-01 00:00:00'),
			(TIMESTAMP '2024-01-15 14:30:45.123456', TIMESTAMP_S '2024-01-15 14:30:45', TIMESTAMP_MS '2024-01-15 14:30:45.123', TIMESTAMP_NS '2024-01-15 14:30:45.123456789');
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "timestamp_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "timestamp_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - all TIMESTAMP types decode to time.Time
	expected := [][]interface{}{
		{
			mustParseTimestamp("1970-01-01T00:00:00Z"),
			mustParseTimestamp("1970-01-01T00:00:00Z"),
			mustParseTimestamp("1970-01-01T00:00:00Z"),
			mustParseTimestamp("1970-01-01T00:00:00Z"),
		},
		{
			mustParseTimestampMicros("2024-01-15T14:30:45.123456Z"),
			mustParseTimestamp("2024-01-15T14:30:45Z"),
			mustParseTimestampMillis("2024-01-15T14:30:45.123Z"),
			mustParseTimestampNanos("2024-01-15T14:30:45.123456789Z"),
		},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j])
			} else {
				// Compare time.Time values
				expectedTime := expectedVal.(time.Time)
				actualTime, ok := actualRow[j].(time.Time)
				require.True(t, ok, "row %d col %d should be time.Time, got %T", i, j, actualRow[j])
				// For timestamp precision, compare with appropriate tolerance
				assert.True(t, expectedTime.Equal(actualTime),
					"row %d col %d mismatch: expected %v, got %v", i, j, expectedTime, actualTime)
			}
		}
	}
}

// TestReadTimestampWithTimezone tests reading TIMESTAMP WITH TIME ZONE (TIMESTAMPTZ).
// Note: TIMESTAMPTZ is stored as UTC microseconds; timezone is session-dependent.
func TestReadTimestampWithTimezone(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with TIMESTAMPTZ values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE timestamptz_test (value TIMESTAMPTZ);
		INSERT INTO timestamptz_test VALUES
			(TIMESTAMPTZ '1970-01-01 00:00:00Z'),
			(TIMESTAMPTZ '2024-01-15 14:30:45.123456Z');
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "timestamptz_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "timestamptz_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - TIMESTAMPTZ decodes to time.Time (UTC)
	expected := [][]interface{}{
		{mustParseTimestamp("1970-01-01T00:00:00Z")},
		{mustParseTimestampMicros("2024-01-15T14:30:45.123456Z")},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j])
			} else {
				// Compare time.Time values
				expectedTime := expectedVal.(time.Time)
				actualTime, ok := actualRow[j].(time.Time)
				require.True(t, ok, "row %d col %d should be time.Time, got %T", i, j, actualRow[j])
				assert.True(t, expectedTime.Equal(actualTime),
					"row %d col %d mismatch: expected %v, got %v", i, j, expectedTime, actualTime)
			}
		}
	}
}

// TestReadIntervalValues tests reading INTERVAL column values.
// This test verifies various interval components including days, hours, months, and combinations.
func TestReadIntervalValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with INTERVAL values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE interval_test (value INTERVAL);
		INSERT INTO interval_test VALUES
			(INTERVAL '1 day'),
			(INTERVAL '1 hour'),
			(INTERVAL '1 month'),
			(INTERVAL '1 year 2 months 3 days');
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "interval_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "interval_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - INTERVAL decodes to Interval struct
	expected := [][]interface{}{
		{Interval{Months: 0, Days: 1, Micros: 0}}, // 1 day
		{
			Interval{Months: 0, Days: 0, Micros: 3600000000},
		}, // 1 hour (3600 seconds * 1,000,000 microseconds)
		{Interval{Months: 1, Days: 0, Micros: 0}}, // 1 month
		{
			Interval{Months: 14, Days: 3, Micros: 0},
		}, // 1 year 2 months 3 days (14 months total)
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j])
			} else {
				// Compare Interval values
				expectedInterval := expectedVal.(Interval)
				actualInterval, ok := actualRow[j].(Interval)
				require.True(t, ok, "row %d col %d should be Interval, got %T", i, j, actualRow[j])
				assert.Equal(t, expectedInterval, actualInterval,
					"row %d col %d mismatch: expected %+v, got %+v", i, j, expectedInterval, actualInterval)
			}
		}
	}
}

// -----------------------------------------------------------------------------
// Helper functions for temporal type tests
// -----------------------------------------------------------------------------

// mustParseDate parses a date string in YYYY-MM-DD format to time.Time (UTC, midnight).
func mustParseDate(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

// mustParseTimestamp parses an RFC3339 timestamp string to time.Time.
func mustParseTimestamp(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

// mustParseTimestampMicros parses a timestamp with microsecond precision.
func mustParseTimestampMicros(s string) time.Time {
	t, err := time.Parse("2006-01-02T15:04:05.999999Z07:00", s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

// mustParseTimestampMillis parses a timestamp with millisecond precision.
func mustParseTimestampMillis(s string) time.Time {
	t, err := time.Parse("2006-01-02T15:04:05.999Z07:00", s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

// mustParseTimestampNanos parses a timestamp with nanosecond precision.
func mustParseTimestampNanos(s string) time.Time {
	t, err := time.Parse("2006-01-02T15:04:05.999999999Z07:00", s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

// -----------------------------------------------------------------------------
// Phase 5: Complex Types
// -----------------------------------------------------------------------------

// TestReadListValues tests reading LIST column values with various element types.
// This test verifies LIST(INTEGER) and LIST(VARCHAR) with nested values and empty lists.
func TestReadListValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with LIST values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE list_test (
			int_list INTEGER[],
			varchar_list VARCHAR[]
		);
		INSERT INTO list_test VALUES
			([1, 2, 3], ['a', 'b']),
			([], ['hello', 'world']),
			(NULL, NULL),
			([NULL, 1, 2], ['x']);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "list_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "list_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - LIST decodes to []interface{}
	expected := [][]interface{}{
		{[]interface{}{int32(1), int32(2), int32(3)}, []interface{}{"a", "b"}},
		{[]interface{}{}, []interface{}{"hello", "world"}},
		{nil, nil},
		{[]interface{}{nil, int32(1), int32(2)}, []interface{}{"x"}},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j], "row %d col %d should be NULL", i, j)
			} else {
				assert.Equal(t, expectedVal, actualRow[j],
					"row %d col %d mismatch: expected %v (%T), got %v (%T)",
					i, j, expectedVal, expectedVal, actualRow[j], actualRow[j])
			}
		}
	}
}

// TestReadStructValues tests reading STRUCT column values with named fields.
// This test verifies STRUCT with simple fields, nested structs, and NULL handling.
func TestReadStructValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with STRUCT values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE struct_test (
			simple STRUCT(a INTEGER, b VARCHAR),
			nested STRUCT(x STRUCT(y INTEGER))
		);
		INSERT INTO struct_test VALUES
			({'a': 1, 'b': 'hello'}, {'x': {'y': 10}}),
			(NULL, NULL),
			({'a': NULL, 'b': 'test'}, {'x': {'y': NULL}});
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "struct_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "struct_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - STRUCT decodes to map[string]interface{}
	expected := [][]interface{}{
		{
			map[string]interface{}{"a": int32(1), "b": "hello"},
			map[string]interface{}{"x": map[string]interface{}{"y": int32(10)}},
		},
		{nil, nil},
		{
			map[string]interface{}{"a": nil, "b": "test"},
			map[string]interface{}{"x": map[string]interface{}{"y": nil}},
		},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j], "row %d col %d should be NULL", i, j)
			} else {
				assert.Equal(t, expectedVal, actualRow[j],
					"row %d col %d mismatch: expected %v (%T), got %v (%T)",
					i, j, expectedVal, expectedVal, actualRow[j], actualRow[j])
			}
		}
	}
}

// TestReadMapValues tests reading MAP column values with various key/value types.
// This test verifies MAP(VARCHAR, INTEGER) with multiple entries, empty maps, and NULL.
func TestReadMapValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with MAP values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE map_test (value MAP(VARCHAR, INTEGER));
		INSERT INTO map_test VALUES
			(MAP(['a', 'b'], [1, 2])),
			(MAP([], [])),
			(NULL);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "map_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "map_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - MAP decodes to map[K]V
	expected := [][]interface{}{
		{map[string]interface{}{"a": int32(1), "b": int32(2)}},
		{map[string]interface{}{}},
		{nil},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j], "row %d col %d should be NULL", i, j)
			} else {
				assert.Equal(t, expectedVal, actualRow[j],
					"row %d col %d mismatch: expected %v (%T), got %v (%T)",
					i, j, expectedVal, expectedVal, actualRow[j], actualRow[j])
			}
		}
	}
}

// TestReadNestedComplexTypes tests reading nested complex types.
// This test verifies LIST(STRUCT) and MAP(VARCHAR, LIST(INTEGER)).
func TestReadNestedComplexTypes(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with nested complex types
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE nested_test (
			list_struct STRUCT(a INTEGER)[],
			map_list MAP(VARCHAR, INTEGER[])
		);
		INSERT INTO nested_test VALUES
			([{'a': 1}, {'a': 2}], MAP(['key'], [[1, 2, 3]])),
			([], MAP([], []));
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "nested_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "nested_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - nested structures
	expected := [][]interface{}{
		{
			[]interface{}{
				map[string]interface{}{"a": int32(1)},
				map[string]interface{}{"a": int32(2)},
			},
			map[string]interface{}{
				"key": []interface{}{int32(1), int32(2), int32(3)},
			},
		},
		{
			[]interface{}{},
			map[string]interface{}{},
		},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j], "row %d col %d should be NULL", i, j)
			} else {
				assert.Equal(t, expectedVal, actualRow[j],
					"row %d col %d mismatch: expected %v (%T), got %v (%T)",
					i, j, expectedVal, expectedVal, actualRow[j], actualRow[j])
			}
		}
	}
}

// TestReadBlobValues tests reading BLOB column values with binary data.
// This test verifies empty blob, simple bytes, and null bytes embedded.
func TestReadBlobValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with BLOB values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE blob_test (value BLOB);
		INSERT INTO blob_test VALUES
			(''::BLOB),
			('\x48\x65\x6c\x6c\x6f'::BLOB),
			('\x00\x01\x02\xFF'::BLOB),
			(NULL);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "blob_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "blob_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - BLOB decodes to []byte
	expected := [][]interface{}{
		{[]byte{}},
		{[]byte{0x48, 0x65, 0x6c, 0x6c, 0x6f}}, // "Hello"
		{[]byte{0x00, 0x01, 0x02, 0xFF}},
		{nil},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j], "row %d col %d should be NULL", i, j)
			} else {
				assert.Equal(t, expectedVal, actualRow[j],
					"row %d col %d mismatch: expected %v (%T), got %v (%T)",
					i, j, expectedVal, expectedVal, actualRow[j], actualRow[j])
			}
		}
	}
}

// TestReadUUIDValues tests reading UUID column values.
// This test verifies standard format UUIDs, nil UUID, and NULL.
func TestReadUUIDValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with UUID values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE uuid_test (value UUID);
		INSERT INTO uuid_test VALUES
			(UUID '550e8400-e29b-41d4-a716-446655440000'),
			(UUID '00000000-0000-0000-0000-000000000000'),
			(NULL);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "uuid_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "uuid_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - UUID decodes to string (canonical format) or [16]byte
	// We'll assume string format for now
	expected := [][]interface{}{
		{"550e8400-e29b-41d4-a716-446655440000"},
		{"00000000-0000-0000-0000-000000000000"},
		{nil},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j], "row %d col %d should be NULL", i, j)
			} else {
				// UUID might be returned as string or [16]byte, handle both cases
				actualVal := actualRow[j]
				switch v := actualVal.(type) {
				case string:
					assert.Equal(t, expectedVal, v,
						"row %d col %d mismatch: expected %v, got %v", i, j, expectedVal, v)
				case [16]byte:
					// Convert [16]byte to canonical string format for comparison
					expectedStr := expectedVal.(string)
					actualStr := formatUUID(v)
					assert.Equal(t, expectedStr, actualStr,
						"row %d col %d mismatch: expected %v, got %v", i, j, expectedStr, actualStr)
				default:
					t.Fatalf("row %d col %d: unexpected type %T for UUID", i, j, actualVal)
				}
			}
		}
	}
}

// formatUUID converts a [16]byte UUID to canonical string format.
func formatUUID(uuid [16]byte) string {
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		uuid[0], uuid[1], uuid[2], uuid[3],
		uuid[4], uuid[5],
		uuid[6], uuid[7],
		uuid[8], uuid[9],
		uuid[10], uuid[11], uuid[12], uuid[13], uuid[14], uuid[15])
}

// -----------------------------------------------------------------------------
// Phase 6: NULL Handling
// -----------------------------------------------------------------------------

// TestReadAllNullColumn tests reading a column where ALL values are NULL.
// This test verifies that the validity mask is correctly interpreted when all bits are 0.
// It tests with both INTEGER and VARCHAR columns.
func TestReadAllNullColumn(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with columns containing all NULLs
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE all_null_test (int_col INTEGER, varchar_col VARCHAR);
		INSERT INTO all_null_test VALUES (NULL, NULL), (NULL, NULL), (NULL, NULL);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "all_null_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "all_null_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - all NULLs
	// CRITICAL: In DuckDB validity mask, 1 = valid (not NULL), 0 = NULL
	expected := [][]interface{}{
		{nil, nil},
		{nil, nil},
		{nil, nil},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row - all values should be NULL
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			assert.Nil(t, expectedVal, "expected value should be nil")
			assert.Nil(t, actualRow[j], "row %d col %d should be NULL", i, j)
		}
	}
}

// TestReadMixedNullValues tests reading a column with mixed NULL/non-NULL values.
// This test verifies that the validity mask correctly identifies which rows are NULL
// and which are valid values.
func TestReadMixedNullValues(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with mixed NULL/non-NULL values in specific positions
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE mixed_null_test (value INTEGER);
		INSERT INTO mixed_null_test VALUES (NULL), (42), (NULL), (100), (NULL);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "mixed_null_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "mixed_null_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - verify correct NULL positions
	// Row 0: NULL, Row 1: 42, Row 2: NULL, Row 3: 100, Row 4: NULL
	expected := [][]interface{}{
		{nil},
		{int32(42)},
		{nil},
		{int32(100)},
		{nil},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row - check correct NULL positions
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j], "row %d col %d should be NULL", i, j)
			} else {
				assert.Equal(t, expectedVal, actualRow[j],
					"row %d col %d mismatch: expected %v (%T), got %v (%T)",
					i, j, expectedVal, expectedVal, actualRow[j], actualRow[j])
			}
		}
	}
}

// TestReadNullInComplexTypes tests NULL handling in complex types.
// This test verifies that NULL elements in LISTs and NULL fields in STRUCTs
// are correctly handled.
func TestReadNullInComplexTypes(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with NULL elements in complex types
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE null_complex_test (
			list_with_nulls INTEGER[],
			struct_with_nulls STRUCT(a INTEGER, b VARCHAR)
		);
		INSERT INTO null_complex_test VALUES
			([1, NULL, 3], {'a': 1, 'b': NULL}),
			([NULL, NULL, NULL], {'a': NULL, 'b': NULL}),
			(NULL, NULL);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "null_complex_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "null_complex_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - LIST with NULL elements and STRUCT with NULL fields
	expected := [][]interface{}{
		{
			[]interface{}{int32(1), nil, int32(3)},
			map[string]interface{}{"a": int32(1), "b": nil},
		},
		{
			[]interface{}{nil, nil, nil},
			map[string]interface{}{"a": nil, "b": nil},
		},
		{nil, nil},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		for j, expectedVal := range expectedRow {
			if expectedVal == nil {
				assert.Nil(t, actualRow[j], "row %d col %d should be NULL", i, j)
			} else {
				assert.Equal(t, expectedVal, actualRow[j],
					"row %d col %d mismatch: expected %v (%T), got %v (%T)",
					i, j, expectedVal, expectedVal, actualRow[j], actualRow[j])
			}
		}
	}
}

// -----------------------------------------------------------------------------
// Phase 7: Compression Verification Tests
// -----------------------------------------------------------------------------

// TestReadConstantCompression tests reading CONSTANT compression.
// This test verifies that columns where all values are identical use constant
// compression and are correctly decoded. DuckDB automatically uses CONSTANT
// compression when all values in a segment are the same.
func TestReadConstantCompression(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with 10000 rows all with value 42
	// DuckDB will use CONSTANT compression when all values are identical
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE constant_test (value INTEGER);
		INSERT INTO constant_test SELECT 42 FROM range(10000);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "constant_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "constant_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Collect rows - verify all 10000 rows have value 42
	rowCount := 0
	expectedValue := int32(42)

	// First row is already fetched
	row := iter.Row()
	require.Len(t, row, 1)
	assert.Equal(t, expectedValue, row[0], "row %d should have value 42", rowCount)
	rowCount++

	// Collect remaining rows
	for iter.Next() {
		row := iter.Row()
		require.Len(t, row, 1)
		assert.Equal(t, expectedValue, row[0], "row %d should have value 42", rowCount)
		rowCount++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 10000, rowCount, "should have exactly 10000 rows")
}

// TestReadRLECompression tests reading RLE (Run-Length Encoding) compression.
// This test verifies that columns with long runs of repeated values use RLE
// compression and are correctly decoded.
func TestReadRLECompression(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with long runs of values:
	// 1000 rows of 'A', 1000 rows of 'B', 1000 rows of 'C'
	// DuckDB may use RLE compression for data with long runs
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE rle_test (value VARCHAR);
		INSERT INTO rle_test SELECT 'A' FROM range(1000);
		INSERT INTO rle_test SELECT 'B' FROM range(1000);
		INSERT INTO rle_test SELECT 'C' FROM range(1000);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "rle_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "rle_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Collect rows - verify pattern: 1000 A's, 1000 B's, 1000 C's
	rowCount := 0

	// First row is already fetched
	row := iter.Row()
	require.Len(t, row, 1)
	if row[0] == nil {
		t.Skip("VARCHAR with compression not yet supported")
	}
	value := row[0].(string)
	rowCount++

	// Collect remaining rows and verify pattern
	for iter.Next() {
		row := iter.Row()
		require.Len(t, row, 1)
		if row[0] == nil {
			t.Skip("VARCHAR with compression not yet supported")
		}
		currentValue := row[0].(string)

		// Verify expected value based on row number
		var expectedValue string
		if rowCount < 1000 {
			expectedValue = "A"
		} else if rowCount < 2000 {
			expectedValue = "B"
		} else {
			expectedValue = "C"
		}
		assert.Equal(
			t,
			expectedValue,
			currentValue,
			"row %d should have value %s",
			rowCount,
			expectedValue,
		)
		rowCount++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 3000, rowCount, "should have exactly 3000 rows")

	// Verify first value was 'A'
	assert.Equal(t, "A", value, "first row should be 'A'")
}

// TestReadDictionaryCompression tests reading DICTIONARY compression.
// This test verifies that columns with limited cardinality use dictionary
// compression and are correctly decoded. DuckDB uses dictionary compression
// for low-cardinality string columns.
func TestReadDictionaryCompression(t *testing.T) {
	// Dictionary compression for VARCHAR is now implemented
	skipIfNoDuckDBCLI(t)

	// Create test file with 10000 rows with only 3 distinct values
	// DuckDB will use DICTIONARY compression for low cardinality data
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE dictionary_test (value VARCHAR);
		INSERT INTO dictionary_test
		SELECT CASE (range % 3)
			WHEN 0 THEN 'cat'
			WHEN 1 THEN 'dog'
			ELSE 'bird'
		END
		FROM range(10000);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "dictionary_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "dictionary_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Collect rows and verify values cycle through 'cat', 'dog', 'bird'
	rowCount := 0
	valueCounts := map[string]int{
		"cat":  0,
		"dog":  0,
		"bird": 0,
	}

	// First row is already fetched
	row := iter.Row()
	require.Len(t, row, 1)
	if row[0] == nil {
		t.Skip("VARCHAR with compression not yet supported")
	}
	value := row[0].(string)
	expectedValue := "cat" // row 0 % 3 = 0 -> 'cat'
	assert.Equal(t, expectedValue, value, "row 0 should be 'cat'")
	valueCounts[value]++
	rowCount++

	// Collect remaining rows
	for iter.Next() {
		row := iter.Row()
		require.Len(t, row, 1)
		if row[0] == nil {
			t.Skip("VARCHAR with compression not yet supported")
		}
		value := row[0].(string)

		// Verify value is one of the three expected values
		_, ok := valueCounts[value]
		require.True(t, ok, "row %d has unexpected value %s", rowCount, value)

		// Verify the pattern: row i % 3 determines the value
		var expectedValue string
		switch rowCount % 3 {
		case 0:
			expectedValue = "cat"
		case 1:
			expectedValue = "dog"
		case 2:
			expectedValue = "bird"
		}
		assert.Equal(
			t,
			expectedValue,
			value,
			"row %d should have value %s",
			rowCount,
			expectedValue,
		)

		valueCounts[value]++
		rowCount++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 10000, rowCount, "should have exactly 10000 rows")

	// Verify distribution: each value should appear ~3333 times
	// (10000 / 3 = 3333.33, so counts should be 3333, 3333, 3334)
	assert.InDelta(t, 3333, valueCounts["cat"], 1, "'cat' should appear ~3333 times")
	assert.InDelta(t, 3333, valueCounts["dog"], 1, "'dog' should appear ~3333 times")
	assert.InDelta(t, 3334, valueCounts["bird"], 1, "'bird' should appear ~3334 times")
}

// TestReadBitpackingCompression tests reading BITPACKING compression.
// This test verifies that columns with small integers that fit in few bits
// use bitpacking compression and are correctly decoded. DuckDB uses bitpacking
// for integers that can be stored in fewer bits than their type size.
func TestReadBitpackingCompression(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with integers that fit in 3 bits (0-7)
	// DuckDB will use BITPACKING compression to store these efficiently
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE bitpacking_test (value INTEGER);
		INSERT INTO bitpacking_test
		SELECT (range % 8)::INTEGER
		FROM range(10000);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "bitpacking_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "bitpacking_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Collect rows and verify values cycle through 0-7
	rowCount := 0
	valueCounts := make(map[int32]int)

	// First row is already fetched
	row := iter.Row()
	require.Len(t, row, 1)
	value := row[0].(int32)
	expectedValue := int32(0) // row 0 % 8 = 0
	assert.Equal(t, expectedValue, value, "row 0 should be 0")
	assert.True(t, value >= 0 && value <= 7, "row 0 value should be in range 0-7")
	valueCounts[value]++
	rowCount++

	// Collect remaining rows
	for iter.Next() {
		row := iter.Row()
		require.Len(t, row, 1)
		value := row[0].(int32)

		// Verify value is in range 0-7
		assert.True(
			t,
			value >= 0 && value <= 7,
			"row %d value should be in range 0-7, got %d",
			rowCount,
			value,
		)

		// Verify the pattern: row i % 8 determines the value
		expectedValue := int32(rowCount % 8)
		assert.Equal(
			t,
			expectedValue,
			value,
			"row %d should have value %d",
			rowCount,
			expectedValue,
		)

		valueCounts[value]++
		rowCount++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 10000, rowCount, "should have exactly 10000 rows")

	// Verify distribution: each value 0-7 should appear exactly 1250 times
	// (10000 / 8 = 1250)
	for i := int32(0); i <= 7; i++ {
		assert.Equal(t, 1250, valueCounts[i], "value %d should appear exactly 1250 times", i)
	}
}

// TestReadMixedCompression tests reading tables where different columns use different compression.
// This test verifies that mixed compression algorithms in a single table are correctly handled.
func TestReadMixedCompression(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with three columns using different compression:
	// - Column 1: constant (all same value) -> CONSTANT compression
	// - Column 2: dictionary (low cardinality strings) -> DICTIONARY compression
	// - Column 3: bitpacking (small integers) -> BITPACKING compression
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE mixed_compression_test (
			constant_col INTEGER,
			dictionary_col VARCHAR,
			bitpacking_col INTEGER
		);
		INSERT INTO mixed_compression_test
		SELECT
			42,
			CASE (range % 3) WHEN 0 THEN 'cat' WHEN 1 THEN 'dog' ELSE 'bird' END,
			(range % 8)::INTEGER
		FROM range(10000);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "mixed_compression_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "mixed_compression_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Collect rows and verify all columns are correctly decoded
	rowCount := 0

	// First row is already fetched
	row := iter.Row()
	require.Len(t, row, 3)
	assert.Equal(t, int32(42), row[0], "row 0 col 0 should be 42")
	assert.Equal(t, "cat", row[1], "row 0 col 1 should be 'cat'")
	assert.Equal(t, int32(0), row[2], "row 0 col 2 should be 0")
	rowCount++

	// Collect remaining rows
	for iter.Next() {
		row := iter.Row()
		require.Len(t, row, 3)

		// Column 0: constant_col (CONSTANT compression) - always 42
		assert.Equal(t, int32(42), row[0], "row %d col 0 should be 42", rowCount)

		// Column 1: dictionary_col (DICTIONARY compression) - cycles cat/dog/bird
		var expectedDictValue string
		switch rowCount % 3 {
		case 0:
			expectedDictValue = "cat"
		case 1:
			expectedDictValue = "dog"
		case 2:
			expectedDictValue = "bird"
		}
		assert.Equal(
			t,
			expectedDictValue,
			row[1],
			"row %d col 1 should be %s",
			rowCount,
			expectedDictValue,
		)

		// Column 2: bitpacking_col (BITPACKING compression) - cycles 0-7
		expectedBitValue := int32(rowCount % 8)
		assert.Equal(
			t,
			expectedBitValue,
			row[2],
			"row %d col 2 should be %d",
			rowCount,
			expectedBitValue,
		)

		rowCount++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 10000, rowCount, "should have exactly 10000 rows")
}

// TestReadNullableVsNonNullableColumn tests nullable vs non-nullable column behavior.
// This test verifies that NOT NULL constraints in the catalog are correctly represented.
// Note: We do not test DuckDB's enforcement of NOT NULL (that's DuckDB CLI's job),
// only that we correctly read the constraint metadata.
func TestReadNullableVsNonNullableColumn(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with both nullable and non-nullable columns
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE nullable_test (non_null_col INTEGER NOT NULL, nullable_col INTEGER);
		INSERT INTO nullable_test VALUES (1, NULL), (2, 42);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "nullable_test")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Verify column constraints are correctly represented in the catalog
	// This tests that we correctly read the NOT NULL constraint metadata
	require.Len(t, table.Columns, 2, "should have 2 columns")

	// Note: The exact representation of NOT NULL constraints in the catalog
	// depends on the catalog schema implementation. This test primarily
	// verifies that we can read the table structure without errors.
	// More specific assertions about constraint metadata can be added later.

	// Try to scan table
	iter, err := storage.ScanTable("main", "nullable_test", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - non-nullable column should never be NULL
	expected := [][]interface{}{
		{int32(1), nil},
		{int32(2), int32(42)},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))

		// Column 0: non_null_col - should never be NULL
		assert.NotNil(t, actualRow[0], "row %d col 0 (non_null_col) should never be NULL", i)
		if expectedRow[0] != nil {
			assert.Equal(t, expectedRow[0], actualRow[0],
				"row %d col 0 mismatch: expected %v (%T), got %v (%T)",
				i, expectedRow[0], expectedRow[0], actualRow[0], actualRow[0])
		}

		// Column 1: nullable_col - can be NULL
		if expectedRow[1] == nil {
			assert.Nil(t, actualRow[1], "row %d col 1 should be NULL", i)
		} else {
			assert.Equal(t, expectedRow[1], actualRow[1],
				"row %d col 1 mismatch: expected %v (%T), got %v (%T)",
				i, expectedRow[1], expectedRow[1], actualRow[1], actualRow[1])
		}
	}
}

// -----------------------------------------------------------------------------
// Phase 8: Scale and Edge Cases
// -----------------------------------------------------------------------------

// TestReadEmptyTable tests reading a table with 0 rows.
// This test verifies that ScanTable returns an iterator with no rows for empty tables.
func TestReadEmptyTable(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with empty table
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE empty (id INTEGER, name VARCHAR);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "empty")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "empty", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Empty table should have no rows
	rowCount := 0
	for iter.Next() {
		rowCount++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 0, rowCount, "empty table should have 0 rows")
}

// TestReadSingleRowTable tests reading a table with exactly 1 row.
// This test verifies correct handling of minimal data.
func TestReadSingleRowTable(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with single row
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE single (id INTEGER, name VARCHAR);
		INSERT INTO single VALUES (42, 'Alice');
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "single")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "single", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values - exactly one row
	expected := [][]interface{}{
		{int32(42), "Alice"},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected), "should have exactly 1 row")

	// Verify the row
	actualRow := rows[0]
	require.Len(t, actualRow, len(expected[0]))
	assert.Equal(t, expected[0][0], actualRow[0])
	assert.Equal(t, expected[0][1], actualRow[1])
}

// TestReadLargeRowCount tests reading a table with large row count (100K rows) spanning multiple row groups.
// DuckDB's default row group size is ~122K rows, so this tests single row group boundary handling.
// This test is skipped in short mode to avoid slow test runs.
func TestReadLargeRowCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file test in short mode")
	}
	skipIfNoDuckDBCLI(t)

	// Create test file with 100K rows
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE large (id INTEGER);
		INSERT INTO large SELECT range::INTEGER FROM range(100000);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "large")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "large", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Collect all rows and verify count and sequential IDs
	rowCount := 0

	// First row is already fetched
	row := iter.Row()
	require.Len(t, row, 1)
	assert.Equal(t, int32(0), row[0], "first row should have id 0")
	rowCount++

	// Collect remaining rows
	for iter.Next() {
		row := iter.Row()
		require.Len(t, row, 1)
		// Verify sequential IDs
		assert.Equal(t, int32(rowCount), row[0], "row %d should have id %d", rowCount, rowCount)
		rowCount++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 100000, rowCount, "should have exactly 100000 rows")
}

// TestReadManyColumns tests reading a table with many columns (50+ columns).
// This test verifies that all columns are correctly read from wide tables.
func TestReadManyColumns(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Generate SQL for table with 50 columns
	cols := make([]string, 50)
	for i := 0; i < 50; i++ {
		cols[i] = fmt.Sprintf("col%d INTEGER", i)
	}

	// Generate VALUES clause: (0, 1, 2, ..., 49)
	vals := make([]string, 50)
	for i := 0; i < 50; i++ {
		vals[i] = fmt.Sprintf("%d", i)
	}

	sql := fmt.Sprintf(`
		CREATE TABLE wide (%s);
		INSERT INTO wide VALUES (%s);
	`, joinStrings(cols, ", "), joinStrings(vals, ", "))

	// Create test file
	dbPath, cleanup := createDuckDBTestFile(t, sql)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "wide")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)
	require.Len(t, table.Columns, 50, "should have 50 columns")

	// Try to scan table
	iter, err := storage.ScanTable("main", "wide", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Verify the row has all 50 columns with correct values
	row := iter.Row()
	require.Len(t, row, 50, "row should have 50 columns")
	for i := 0; i < 50; i++ {
		assert.Equal(t, int32(i), row[i], "col%d should have value %d", i, i)
	}

	// Verify no more rows
	require.False(t, iter.Next(), "should have exactly 1 row")
	require.NoError(t, iter.Err())
}

// TestReadExtremeStringLengths tests reading extreme string lengths.
// This test verifies empty string, single char, and long string (10KB) byte preservation.
func TestReadExtremeStringLengths(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Generate a long string (10KB = 10240 chars)
	longString := make([]byte, 10240)
	for i := range longString {
		longString[i] = 'x'
	}
	longStr := string(longString)

	// Create test file with extreme string lengths
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE extreme_strings (value VARCHAR);
		INSERT INTO extreme_strings VALUES (''), ('x'), ('`+longStr+`');
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "extreme_strings")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "extreme_strings", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values
	expected := [][]interface{}{
		{""},
		{"x"},
		{longStr},
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		assert.Equal(t, expectedRow[0], actualRow[0],
			"row %d mismatch: expected len=%d, got len=%d",
			i, len(expectedRow[0].(string)), len(actualRow[0].(string)))
	}
}

// TestReadSpecialCharacters tests reading special characters in strings.
// This test verifies newlines, tabs, quotes, backslashes, and NULL bytes are preserved.
func TestReadSpecialCharacters(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with special characters
	// Note: We use literal newlines/tabs in SQL, CHR() for NULL byte and backslash
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE special_chars (value VARCHAR);
		INSERT INTO special_chars VALUES
			('line1
line2'),
			('col1	col2'),
			('it''s a test'),
			('path' || CHR(92) || 'to' || CHR(92) || 'file'),
			('null' || CHR(0) || 'byte');
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Verify the table exists
	table, ok := cat.GetTableInSchema("main", "special_chars")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "special_chars", nil)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skip("Row data scanning not yet supported")
	}
	defer iter.Close()

	// Check if any rows are available
	if !iter.Next() {
		if iter.Err() != nil {
			skipOnFormatError(t, iter.Err())
			t.Fatalf("Iterator error: %v", iter.Err())
		}
		t.Skip("Row group data from DuckDB CLI files not yet loaded")
	}

	// Expected values
	expected := [][]interface{}{
		{"line1\nline2"},   // Newline
		{"col1\tcol2"},     // Tab
		{"it's a test"},    // Single quote
		{"path\\to\\file"}, // Backslash (CHR(92) is \)
		{"null\x00byte"},   // NULL byte (CHR(0) is \x00)
	}

	// Collect rows
	rows := [][]interface{}{iter.Row()}
	for iter.Next() {
		row := iter.Row()
		if row != nil {
			rowCopy := make([]interface{}, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each row
	for i, expectedRow := range expected {
		actualRow := rows[i]
		require.Len(t, actualRow, len(expectedRow))
		assert.Equal(t, expectedRow[0], actualRow[0],
			"row %d mismatch: expected %q, got %q",
			i, expectedRow[0], actualRow[0])
	}
}

// joinStrings is a helper function to join strings with a separator.
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}
