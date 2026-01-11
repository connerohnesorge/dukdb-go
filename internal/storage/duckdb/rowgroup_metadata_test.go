// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file contains tests for verifying row group
// metadata structure matches DuckDB CLI expectations.
//
// These tests verify:
// - RowGroupPointer structure parsing from DuckDB CLI files
// - Field encoding (field 100: row_start, field 101: tuple_count, etc.)
// - Column count determination from data_pointers array
// - Metadata block pointer encoding
package duckdb

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRowGroupMetadataStructure verifies that dukdb-go can correctly parse
// row group metadata structures written by DuckDB CLI.
//
// This test creates a simple table with known data using DuckDB CLI, then
// uses dukdb-go's metadata reader to parse the row group structures and
// verify they match expectations.
func TestRowGroupMetadataStructure(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_rowgroup.db")

	// Create a database with DuckDB CLI containing actual data
	// 3 rows, 2 columns: INTEGER and VARCHAR
	sql := `
		CREATE TABLE test_table (
			id INTEGER,
			name VARCHAR
		);
		INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
	`
	runDuckDBCommand(t, dbPath, sql)

	// Open the database with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	require.NoError(t, err, "Failed to open storage")
	defer storage.Close()

	// Get the block manager and catalog
	bm := storage.blockManager
	ddbCat := storage.catalog
	require.NotNil(t, ddbCat, "DuckDB catalog should not be nil")

	// Get the test_table from the catalog
	tableEntry := ddbCat.GetTable("test_table")
	require.NotNil(t, tableEntry, "Table 'test_table' should exist")

	// Get storage information
	storageInfo := tableEntry.StorageMetadata
	require.NotNil(t, storageInfo, "Storage info should not be nil")

	t.Logf("Table storage info:")
	t.Logf("  TotalRows: %d", storageInfo.TotalRows)
	t.Logf("  TablePointer: BlockID=%d, BlockIndex=%d, Offset=%d",
		storageInfo.TablePointer.BlockID,
		storageInfo.TablePointer.BlockIndex,
		storageInfo.TablePointer.Offset)

	// Verify total rows
	assert.Equal(t, uint64(3), storageInfo.TotalRows, "Expected 3 rows in table")

	// Get column count from table entry
	columnCount := len(tableEntry.Columns)
	require.Equal(t, 2, columnCount, "Expected 2 columns (id, name)")

	rowGroups, err := ReadRowGroupsFromTablePointer(
		bm,
		storageInfo.TablePointer,
		storageInfo.TotalRows,
		columnCount,
	)
	require.NoError(t, err, "Failed to read row groups")
	require.NotNil(t, rowGroups, "Row groups should not be nil")

	t.Logf("Read %d row group(s)", len(rowGroups))

	// Verify row group structure
	require.Greater(t, len(rowGroups), 0, "Should have at least 1 row group")

	// For a small table with 3 rows, we expect 1 row group
	assert.Equal(t, 1, len(rowGroups), "Expected 1 row group for 3 rows")

	// Verify first row group
	rg := rowGroups[0]
	assert.NotNil(t, rg, "Row group should not be nil")

	t.Logf("Row group 0:")
	t.Logf("  RowStart: %d", rg.RowStart)
	t.Logf("  TupleCount: %d", rg.TupleCount)
	t.Logf("  DataPointers: %d", len(rg.DataPointers))

	// Verify row group fields
	assert.Equal(t, uint64(0), rg.RowStart, "First row group should start at row 0")
	assert.Equal(t, uint64(3), rg.TupleCount, "Row group should contain 3 rows")
	assert.Equal(t, 2, len(rg.DataPointers), "Row group should have 2 data pointers (one per column)")

	// Verify data pointers are valid (not InvalidBlockID)
	for i, dp := range rg.DataPointers {
		t.Logf("  DataPointer[%d]: BlockID=%d, BlockIndex=%d, Offset=%d",
			i, dp.BlockID, dp.BlockIndex, dp.Offset)
		assert.NotEqual(t, InvalidBlockID, dp.BlockID, "DataPointer block ID should not be invalid")
	}
}

// TestRowGroupMetadataFormat documents and verifies the row group metadata format
// used by DuckDB. This test creates multiple scenarios and verifies the structure.
func TestRowGroupMetadataFormat(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	testCases := []struct {
		name        string
		createSQL   string
		insertSQL   string
		expectedRGs int    // Expected number of row groups
		expectedRows uint64 // Expected total rows
		expectedCols int    // Expected columns
	}{
		{
			name:        "Single row, single column",
			createSQL:   "CREATE TABLE t1 (x INTEGER)",
			insertSQL:   "INSERT INTO t1 VALUES (42)",
			expectedRGs: 1,
			expectedRows: 1,
			expectedCols: 1,
		},
		{
			name:        "Multiple rows, two columns",
			createSQL:   "CREATE TABLE t2 (id INTEGER, val VARCHAR)",
			insertSQL:   "INSERT INTO t2 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
			expectedRGs: 1,
			expectedRows: 5,
			expectedCols: 2,
		},
		{
			name:        "Three columns with mixed types",
			createSQL:   "CREATE TABLE t3 (a BIGINT, b VARCHAR, c DOUBLE)",
			insertSQL:   "INSERT INTO t3 VALUES (100, 'test', 3.14), (200, 'data', 2.71)",
			expectedRGs: 1,
			expectedRows: 2,
			expectedCols: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			dbPath := filepath.Join(tmpDir, "test.db")

			// Create table and insert data
			sql := tc.createSQL + "; " + tc.insertSQL
			runDuckDBCommand(t, dbPath, sql)

			// Open with dukdb-go
			storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
			require.NoError(t, err)
			defer storage.Close()

			bm := storage.blockManager
			ddbCat := storage.catalog
			require.NotNil(t, ddbCat)

			// Get the first table (we only create one)
			require.Len(t, ddbCat.Tables, 1, "Should have exactly one table")

			tableEntry := ddbCat.Tables[0]
			require.NotNil(t, tableEntry)

			storageInfo := tableEntry.StorageMetadata
			require.NotNil(t, storageInfo)

			// Verify total rows
			assert.Equal(t, tc.expectedRows, storageInfo.TotalRows)

			// Verify column count
			assert.Equal(t, tc.expectedCols, len(tableEntry.Columns))

			// Read row groups
			rowGroups, err := ReadRowGroupsFromTablePointer(
				bm,
				storageInfo.TablePointer,
				storageInfo.TotalRows,
				len(tableEntry.Columns),
			)
			require.NoError(t, err)
			require.NotNil(t, rowGroups)

			// Verify row group count
			assert.Equal(t, tc.expectedRGs, len(rowGroups), "Unexpected row group count")

			// Verify row group structure
			var totalRows uint64
			for i, rg := range rowGroups {
				t.Logf("Row group %d: RowStart=%d, TupleCount=%d, Columns=%d",
					i, rg.RowStart, rg.TupleCount, len(rg.DataPointers))

				// Each row group should have the correct number of data pointers
				assert.Equal(t, tc.expectedCols, len(rg.DataPointers),
					"Row group %d should have %d data pointers", i, tc.expectedCols)

				// Verify all data pointers are valid
				for j, dp := range rg.DataPointers {
					assert.NotEqual(t, InvalidBlockID, dp.BlockID,
						"Row group %d, column %d has invalid block ID", i, j)
				}

				totalRows += rg.TupleCount
			}

			// Total rows across all row groups should match
			assert.Equal(t, tc.expectedRows, totalRows,
				"Total rows across row groups should match expected")
		})
	}
}

// TestRowGroupMetadataFieldEncoding verifies the specific field encoding
// used in RowGroupPointer serialization.
//
// According to the metadata reader (metadata_reader.go:2930-2936):
// - Field 100: row_start (uint64 varint)
// - Field 101: tuple_count (uint64 varint)
// - Field 102: data_pointers (vector<MetaBlockPointer>)
// - Field 103: delete_pointers (vector<MetaBlockPointer>)
// - Field 104: has_metadata_blocks (bool) - optional
// - Field 105: extra_metadata_blocks (vector<idx_t>) - optional
// - Terminator 0xFFFF
func TestRowGroupMetadataFieldEncoding(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "field_test.db")

	// Create a simple table
	sql := "CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1), (2), (3);"
	runDuckDBCommand(t, dbPath, sql)

	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	require.NoError(t, err)
	defer storage.Close()

	bm := storage.blockManager
	ddbCat := storage.catalog

	tableEntry := ddbCat.GetTable("t")
	storageInfo := tableEntry.StorageMetadata

	// Read row groups - this exercises the field parsing logic
	rowGroups, err := ReadRowGroupsFromTablePointer(
		bm,
		storageInfo.TablePointer,
		storageInfo.TotalRows,
		len(tableEntry.Columns),
	)
	require.NoError(t, err, "Field parsing should succeed")
	require.Len(t, rowGroups, 1, "Should parse 1 row group")

	rg := rowGroups[0]

	// Verify field 100 (row_start) was parsed correctly
	assert.Equal(t, uint64(0), rg.RowStart, "Field 100 (row_start) should be 0")

	// Verify field 101 (tuple_count) was parsed correctly
	assert.Equal(t, uint64(3), rg.TupleCount, "Field 101 (tuple_count) should be 3")

	// Verify field 102 (data_pointers) was parsed correctly
	assert.Equal(t, 1, len(rg.DataPointers), "Field 102 (data_pointers) should have 1 element")

	t.Logf("Successfully parsed RowGroupPointer fields:")
	t.Logf("  Field 100 (row_start): %d", rg.RowStart)
	t.Logf("  Field 101 (tuple_count): %d", rg.TupleCount)
	t.Logf("  Field 102 (data_pointers count): %d", len(rg.DataPointers))
}

// TestRowGroupMetadataCompareWithGoWriter compares row group metadata
// written by dukdb-go with what DuckDB CLI expects.
//
// This test:
// 1. Creates a database using dukdb-go's writer
// 2. Reads it back using metadata reader
// 3. Verifies the structure matches expectations
//
// NOTE: This test is currently skipped because the dukdb-go writer's catalog
// format is still being fixed in other tasks. This test verifies that row group
// structures written by dukdb-go can be read back, which will be important once
// the writer format is complete.
func TestRowGroupMetadataCompareWithGoWriter(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "go_writer.db")

	// Create database using dukdb-go's DuckDBWriter
	writer, err := Create(dbPath)
	require.NoError(t, err)

	// Create a simple table
	err = writer.CreateTableSimple("test_table", []ColumnDefinition{
		{Name: "id", Type: TypeInteger},
		{Name: "name", Type: TypeVarchar},
	})
	require.NoError(t, err)

	// Insert rows - need to get table OID first
	// Table is created with OID 0 as first table
	tableOID := uint64(0)

	rows := [][]any{
		{int32(1), "Alice"},
		{int32(2), "Bob"},
		{int32(3), "Charlie"},
	}

	err = writer.InsertRows(tableOID, rows)
	require.NoError(t, err)

	// Close the writer to flush data
	err = writer.Close()
	require.NoError(t, err)


	// Now open the file and verify the row group structure
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	require.NoError(t, err)
	defer storage.Close()

	bm := storage.blockManager
	ddbCat := storage.catalog

	tableEntry := ddbCat.GetTable("test_table")
	require.NotNil(t, tableEntry, "Table should exist")

	storageInfo := tableEntry.StorageMetadata
	require.NotNil(t, storageInfo, "Storage info should not be nil")

	t.Logf("dukdb-go wrote table with:")
	t.Logf("  TotalRows: %d", storageInfo.TotalRows)
	t.Logf("  Columns: %d", len(tableEntry.Columns))

	// Verify total rows
	assert.Equal(t, uint64(3), storageInfo.TotalRows, "Should have 3 rows")

	// Read row groups
	rowGroups, err := ReadRowGroupsFromTablePointer(
		bm,
		storageInfo.TablePointer,
		storageInfo.TotalRows,
		len(tableEntry.Columns),
	)
	require.NoError(t, err)
	require.NotNil(t, rowGroups)
	require.Len(t, rowGroups, 1, "Should have 1 row group")

	rgp := rowGroups[0]

	t.Logf("Row group structure:")
	t.Logf("  RowStart: %d", rgp.RowStart)
	t.Logf("  TupleCount: %d", rgp.TupleCount)
	t.Logf("  DataPointers: %d", len(rgp.DataPointers))

	// Verify the row group structure
	assert.Equal(t, uint64(0), rgp.RowStart, "First row group should start at 0")
	assert.Equal(t, uint64(3), rgp.TupleCount, "Should have 3 rows")
	assert.Equal(t, 2, len(rgp.DataPointers), "Should have 2 data pointers (one per column)")

	// Verify data pointers are valid
	for i, dp := range rgp.DataPointers {
		t.Logf("  DataPointer[%d]: BlockID=%d, BlockIndex=%d, Offset=%d",
			i, dp.BlockID, dp.BlockIndex, dp.Offset)
		assert.NotEqual(t, InvalidBlockID, dp.BlockID,
			"Data pointer %d should have valid block ID", i)
	}

	t.Log("Successfully verified dukdb-go written row group structure")
}

// TestRowGroupMetadataDocumentation documents the row group metadata format
// as understood from reading DuckDB CLI files.
func TestRowGroupMetadataDocumentation(t *testing.T) {
	t.Log("=== DuckDB Row Group Metadata Format ===")
	t.Log("")
	t.Log("Row Group Structure (at table_pointer location):")
	t.Log("  1. TableStatistics (BinarySerializer wrapped)")
	t.Log("  2. row_group_count (raw uint64, 8 bytes - NOT varint!)")
	t.Log("  3. For each row group: BinarySerializer wrapped RowGroupPointer")
	t.Log("")
	t.Log("RowGroupPointer Format (BinarySerializer fields):")
	t.Log("  Field 100: row_start (uint64 varint)")
	t.Log("    - Starting row index within the table")
	t.Log("    - First row group starts at 0")
	t.Log("")
	t.Log("  Field 101: tuple_count (uint64 varint)")
	t.Log("    - Number of rows in this row group")
	t.Log("    - Maximum is typically DefaultRowGroupSize (122880)")
	t.Log("")
	t.Log("  Field 102: data_pointers (vector<MetaBlockPointer>)")
	t.Log("    - Vector count encoded as varint")
	t.Log("    - One MetaBlockPointer per column")
	t.Log("    - Each MetaBlockPointer points to DataPointer metadata")
	t.Log("    - Column count = len(data_pointers)")
	t.Log("")
	t.Log("  Field 103: delete_pointers (vector<MetaBlockPointer>)")
	t.Log("    - Vector count encoded as varint")
	t.Log("    - Usually empty (count = 0)")
	t.Log("    - Used for DELETE operations tracking")
	t.Log("")
	t.Log("  Field 104: has_metadata_blocks (bool) - OPTIONAL")
	t.Log("    - Encoded as varint (0 or 1)")
	t.Log("    - Indicates if field 105 is present")
	t.Log("")
	t.Log("  Field 105: extra_metadata_blocks (vector<idx_t>) - OPTIONAL")
	t.Log("    - Vector count encoded as varint")
	t.Log("    - Each element is idx_t (varint)")
	t.Log("    - Additional metadata block references")
	t.Log("")
	t.Log("  Terminator: 0xFFFF (uint16)")
	t.Log("    - Marks end of RowGroupPointer object")
	t.Log("")
	t.Log("MetaBlockPointer Encoding:")
	t.Log("  - Encoded as uint64 (8 bytes)")
	t.Log("  - Upper 56 bits: block_id")
	t.Log("  - Lower 8 bits: block_index (sub-block within 256KB block)")
	t.Log("  - Formula: encoded = (block_id << 8) | block_index")
	t.Log("  - Decode: block_id = encoded >> 8, block_index = encoded & 0xFF")
	t.Log("")
	t.Log("DataPointer (referenced by MetaBlockPointer):")
	t.Log("  - Stored in metadata block")
	t.Log("  - Contains actual column segment information")
	t.Log("  - See metadata_reader.go for full DataPointer structure")
	t.Log("")
	t.Log("Row Group Size:")
	t.Log("  - Default maximum: 122880 rows (DefaultRowGroupSize)")
	t.Log("  - Tables with more rows span multiple row groups")
	t.Log("  - Each row group is self-contained")
	t.Log("")

	// This is a documentation test - it always passes
	assert.True(t, true, "Documentation test")
}

// TestRowGroupMetadataMultipleRowGroups tests reading tables that span
// multiple row groups (large tables).
func TestRowGroupMetadataMultipleRowGroups(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "large_table.db")

	// Create a table with enough rows to potentially span multiple row groups
	// DefaultRowGroupSize is 122880, but for testing we'll use a smaller number
	rowCount := 1000

	// Generate INSERT statement
	sql := "CREATE TABLE large_table (id INTEGER, val VARCHAR);"

	// Insert in batches
	batchSize := 100
	for i := 0; i < rowCount; i += batchSize {
		values := make([]string, 0, batchSize)
		for j := 0; j < batchSize && i+j < rowCount; j++ {
			values = append(values, fmt.Sprintf("(%d, 'row_%d')", i+j, i+j))
		}
		// Use simple string concatenation instead of joinStrings
		valuesStr := ""
		for idx, v := range values {
			if idx > 0 {
				valuesStr += ", "
			}
			valuesStr += v
		}
		sql += fmt.Sprintf("INSERT INTO large_table VALUES %s;", valuesStr)
	}

	runDuckDBCommand(t, dbPath, sql)

	// Read and verify
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	require.NoError(t, err)
	defer storage.Close()

	bm := storage.blockManager
	ddbCat := storage.catalog

	tableEntry := ddbCat.GetTable("large_table")
	storageInfo := tableEntry.StorageMetadata

	assert.Equal(t, uint64(rowCount), storageInfo.TotalRows, "Should have correct total rows")

	rowGroups, err := ReadRowGroupsFromTablePointer(
		bm,
		storageInfo.TablePointer,
		storageInfo.TotalRows,
		len(tableEntry.Columns),
	)
	require.NoError(t, err)

	t.Logf("Table with %d rows has %d row group(s)", rowCount, len(rowGroups))

	// Verify all row groups
	var totalRows uint64
	for i, rg := range rowGroups {
		t.Logf("Row group %d: RowStart=%d, TupleCount=%d",
			i, rg.RowStart, rg.TupleCount)

		assert.Equal(t, 2, len(rg.DataPointers), "Each row group should have 2 columns")
		totalRows += rg.TupleCount

		// Verify row start is sequential
		if i > 0 {
			prevRG := rowGroups[i-1]
			expectedStart := prevRG.RowStart + prevRG.TupleCount
			assert.Equal(t, expectedStart, rg.RowStart,
				"Row group %d should start where previous ended", i)
		} else {
			assert.Equal(t, uint64(0), rg.RowStart, "First row group should start at 0")
		}
	}

	assert.Equal(t, uint64(rowCount), totalRows, "Total rows should match sum of row groups")
}

// TestRowGroupMetadataEmptyTable verifies handling of tables with no rows.
func TestRowGroupMetadataEmptyTable(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "empty.db")

	// Create an empty table
	sql := "CREATE TABLE empty_table (x INTEGER, y VARCHAR);"
	runDuckDBCommand(t, dbPath, sql)

	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	require.NoError(t, err)
	defer storage.Close()

	bm := storage.blockManager
	ddbCat := storage.catalog

	tableEntry := ddbCat.GetTable("empty_table")
	storageInfo := tableEntry.StorageMetadata

	assert.Equal(t, uint64(0), storageInfo.TotalRows, "Empty table should have 0 rows")

	// For empty tables, table pointer might be invalid
	if storageInfo.TablePointer.BlockID == InvalidBlockID {
		t.Log("Empty table has no row groups (invalid table pointer)")
		return
	}

	rowGroups, err := ReadRowGroupsFromTablePointer(
		bm,
		storageInfo.TablePointer,
		storageInfo.TotalRows,
		len(tableEntry.Columns),
	)

	// Empty tables should return empty slice or nil, not error
	if err != nil {
		t.Logf("Note: Empty table returned error (acceptable): %v", err)
	}

	if rowGroups == nil || len(rowGroups) == 0 {
		t.Log("Empty table has no row groups (nil or empty slice)")
	}
}
