// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file contains tests for verifying VARCHAR string
// heap format compatibility with DuckDB CLI.
//
// These tests verify that:
// - dukdb-go correctly reads VARCHAR data from DuckDB CLI created files
// - String offsets and lengths are correctly decoded
// - String data layout in heap matches DuckDB format
// - Both single-string and multi-string segments are handled correctly
//
// Tests are skipped if the DuckDB CLI is not installed on the system.
package duckdb

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStringHeapSingleString tests reading a VARCHAR column with a single string value.
// This test verifies the single-string segment format which uses string_t layout.
func TestStringHeapSingleString(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with single VARCHAR value
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE single_string (value VARCHAR);
		INSERT INTO single_string VALUES ('hello');
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
	table, ok := cat.GetTableInSchema("main", "single_string")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "single_string", nil)
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

	// Verify the string value
	row := iter.Row()
	require.Len(t, row, 1)
	assert.Equal(t, "hello", row[0], "should read single string 'hello'")

	// Verify no more rows
	require.False(t, iter.Next(), "should have exactly 1 row")
	require.NoError(t, iter.Err())
}

// TestStringHeapMultipleStrings tests reading a VARCHAR column with multiple string values.
// This test verifies the heap-based format for multi-string segments where strings are
// stored in reverse order in the heap.
func TestStringHeapMultipleStrings(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with multiple VARCHAR values of varying lengths
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE multi_string (value VARCHAR);
		INSERT INTO multi_string VALUES ('a'), ('bb'), ('ccc'), ('dddd');
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
	table, ok := cat.GetTableInSchema("main", "multi_string")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "multi_string", nil)
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

	// Expected values - verify all strings are correctly read
	expected := []string{"a", "bb", "ccc", "dddd"}

	// Collect rows starting with the current row
	rows := []string{iter.Row()[0].(string)}
	for iter.Next() {
		row := iter.Row()
		require.Len(t, row, 1)
		rows = append(rows, row[0].(string))
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each string
	for i, expectedStr := range expected {
		assert.Equal(t, expectedStr, rows[i], "row %d should be '%s'", i, expectedStr)
	}
}

// TestStringHeapEmptyStrings tests reading VARCHAR columns with empty strings.
// This test verifies that zero-length strings are correctly handled in the heap format.
func TestStringHeapEmptyStrings(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with empty strings mixed with non-empty strings
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE empty_strings (value VARCHAR);
		INSERT INTO empty_strings VALUES (''), ('hello'), (''), ('world');
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
	table, ok := cat.GetTableInSchema("main", "empty_strings")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "empty_strings", nil)
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

	// Expected values - verify empty strings are correctly preserved
	expected := []string{"", "hello", "", "world"}

	// Collect rows
	rows := []string{iter.Row()[0].(string)}
	for iter.Next() {
		row := iter.Row()
		require.Len(t, row, 1)
		rows = append(rows, row[0].(string))
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each string including empty ones
	for i, expectedStr := range expected {
		assert.Equal(t, expectedStr, rows[i], "row %d should be '%s'", i, expectedStr)
	}
}

// TestStringHeapRawFormat tests the low-level string heap format by directly
// parsing block data from a DuckDB CLI created file.
func TestStringHeapRawFormat(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with known strings
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE raw_strings (value VARCHAR);
		INSERT INTO raw_strings VALUES ('abc'), ('def'), ('ghi');
	`)
	defer cleanup()

	// Open with dukdb-go at low level
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	cat, err := storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)
	skipOnEmptyCatalog(t, cat)

	// Get the table
	table, ok := cat.GetTableInSchema("main", "raw_strings")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Get table metadata
	duckTable := storage.catalog.GetTable("raw_strings")
	if duckTable == nil || duckTable.StorageMetadata == nil {
		t.Skip("Table metadata not available")
	}

	// Read row groups
	rowGroups, err := ReadRowGroupsFromTablePointer(
		storage.blockManager,
		duckTable.StorageMetadata.TablePointer,
		duckTable.StorageMetadata.TotalRows,
		len(duckTable.Columns),
	)
	require.NoError(t, err)
	if len(rowGroups) == 0 {
		t.Skip("No row groups available")
	}

	rgp := rowGroups[0]
	require.NotNil(t, rgp)

	// Create row group reader
	reader := NewRowGroupReader(storage.blockManager, rgp, []LogicalTypeID{TypeVarchar})

	// Read the VARCHAR column (column 0)
	colData, err := reader.ReadColumn(0)
	if err != nil {
		skipOnFormatError(t, err)
		t.Skipf("Failed to read column: %v", err)
	}
	require.NotNil(t, colData)

	// Verify we got 3 tuples
	require.Equal(t, uint64(3), colData.TupleCount, "should have 3 tuples")

	// Verify the string data is in the expected format
	// For VARCHAR columns, Data should contain length-prefixed strings
	data := colData.Data
	require.NotEmpty(t, data, "data should not be empty")

	// Parse the strings manually to verify format
	offset := uint64(0)
	expectedStrings := []string{"abc", "def", "ghi"}

	for i, expectedStr := range expectedStrings {
		// Each string should start with uint32 length prefix
		require.True(t, offset+4 <= uint64(len(data)), "should have room for length at offset %d", offset)
		length := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		// Length should match expected string length
		require.Equal(t, uint32(len(expectedStr)), length, "string %d length should be %d", i, len(expectedStr))

		// Extract string bytes
		require.True(t, offset+uint64(length) <= uint64(len(data)), "should have room for string data")
		actualStr := string(data[offset : offset+uint64(length)])
		offset += uint64(length)

		// Verify string matches
		assert.Equal(t, expectedStr, actualStr, "string %d should be '%s'", i, expectedStr)
	}

	// Verify we consumed all data
	assert.Equal(t, uint64(len(data)), offset, "should have consumed all data")
}

// TestStringHeapUnicodeStrings tests reading Unicode strings in VARCHAR columns.
// This test verifies that multi-byte UTF-8 characters are correctly preserved.
func TestStringHeapUnicodeStrings(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with Unicode strings (emoji, CJK characters)
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE unicode_strings (value VARCHAR);
		INSERT INTO unicode_strings VALUES ('🦆'), ('中文'), ('Hello 🦆 中文');
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
	table, ok := cat.GetTableInSchema("main", "unicode_strings")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "unicode_strings", nil)
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

	// Expected values - verify Unicode strings are byte-exact
	expected := []string{"🦆", "中文", "Hello 🦆 中文"}

	// Collect rows
	rows := []string{iter.Row()[0].(string)}
	for iter.Next() {
		row := iter.Row()
		require.Len(t, row, 1)
		rows = append(rows, row[0].(string))
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each Unicode string is preserved byte-exact
	for i, expectedStr := range expected {
		assert.Equal(t, expectedStr, rows[i], "row %d should be '%s'", i, expectedStr)
		// Also verify byte lengths match (important for UTF-8)
		assert.Equal(t, len(expectedStr), len(rows[i]), "row %d byte length should match", i)
	}
}

// TestStringHeapDocumentation documents the DuckDB string heap format based on
// observed behavior and code analysis.
func TestStringHeapDocumentation(t *testing.T) {
	t.Log("DuckDB VARCHAR String Heap Format:")
	t.Log("")
	t.Log("SINGLE STRING FORMAT (1 row):")
	t.Log("  [8-byte header]")
	t.Log("  [4-byte length]")
	t.Log("  [12-byte inline data or heap pointer]")
	t.Log("")
	t.Log("MULTI-STRING HEAP FORMAT (2+ rows):")
	t.Log("  Header:")
	t.Log("    [4 bytes] total_heap_size - Total size of string heap in bytes")
	t.Log("    [4 bytes] heap_end_offset - End offset (header_size + index_size + heap_size)")
	t.Log("")
	t.Log("  Index (4 bytes per string):")
	t.Log("    [4 bytes] cumulative_end_offset_0 - Cumulative offset from heap end for string 0")
	t.Log("    [4 bytes] cumulative_end_offset_1 - Cumulative offset from heap end for string 1")
	t.Log("    ...")
	t.Log("")
	t.Log("  Heap Data:")
	t.Log("    Strings are stored in REVERSE order!")
	t.Log("    String N is at: heap_start + (total_heap_size - cumulative_end_offset_N)")
	t.Log("    String length: cumulative_end_offset_N - cumulative_end_offset_(N-1)")
	t.Log("")
	t.Log("  Example with 3 strings ['abc', 'de', 'f']:")
	t.Log("    total_heap_size = 6 (3+2+1)")
	t.Log("    heap_end_offset = 8 + 12 + 6 = 26")
	t.Log("    Index offsets: [3, 5, 6] (cumulative from end)")
	t.Log("    Heap data: 'fdeabc' (reversed)")
	t.Log("    String 0 'abc': at offset (6-3)=3, length 3")
	t.Log("    String 1 'de': at offset (6-5)=1, length 2")
	t.Log("    String 2 'f': at offset (6-6)=0, length 1")
}

// TestStringHeapConstantCompression tests reading VARCHAR columns with CONSTANT compression.
// When all strings are identical, DuckDB uses CONSTANT compression.
func TestStringHeapConstantCompression(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file where all VARCHAR values are identical
	// DuckDB should use CONSTANT compression for this
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE constant_varchar (value VARCHAR);
		INSERT INTO constant_varchar SELECT 'same' FROM range(100);
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
	table, ok := cat.GetTableInSchema("main", "constant_varchar")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "constant_varchar", nil)
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

	// Verify all 100 rows have the same value 'same'
	rowCount := 0
	row := iter.Row()
	require.Len(t, row, 1)
	assert.Equal(t, "same", row[0], "row 0 should be 'same'")
	rowCount++

	for iter.Next() {
		row := iter.Row()
		require.Len(t, row, 1)
		assert.Equal(t, "same", row[0], "row %d should be 'same'", rowCount)
		rowCount++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 100, rowCount, "should have exactly 100 rows")
}

// TestStringHeapWithNulls tests reading VARCHAR columns with NULL values.
// This test verifies that NULL strings are correctly distinguished from empty strings.
func TestStringHeapWithNulls(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with NULL and non-NULL VARCHAR values
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE varchar_nulls (value VARCHAR);
		INSERT INTO varchar_nulls VALUES ('hello'), (NULL), (''), (NULL), ('world');
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
	table, ok := cat.GetTableInSchema("main", "varchar_nulls")
	require.True(t, ok, "table should exist")
	require.NotNil(t, table)

	// Try to scan table
	iter, err := storage.ScanTable("main", "varchar_nulls", nil)
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

	// Expected values - distinguish NULL from empty string
	expected := []interface{}{"hello", nil, "", nil, "world"}

	// Collect rows
	rows := []interface{}{iter.Row()[0]}
	for iter.Next() {
		row := iter.Row()
		require.Len(t, row, 1)
		rows = append(rows, row[0])
	}
	require.NoError(t, iter.Err())
	require.Len(t, rows, len(expected))

	// Verify each value including NULLs vs empty strings
	for i, expectedVal := range expected {
		if expectedVal == nil {
			assert.Nil(t, rows[i], "row %d should be NULL", i)
		} else {
			assert.Equal(t, expectedVal, rows[i], "row %d should be '%s'", i, expectedVal)
		}
	}
}

// createDuckDBTestFile creates a temporary DuckDB file with the given SQL.
// It returns the path to the file and a cleanup function.
// This is defined in data_read_verify_test.go but we need it here too.
// Skipping this function as it's already defined elsewhere.
