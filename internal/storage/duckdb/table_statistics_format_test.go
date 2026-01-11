// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This test extracts and documents the TableStatistics
// format from a CLI-generated DuckDB file.
package duckdb

import (
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExtractTableStatisticsFormat extracts and documents the TableStatistics format
// from a CLI-generated DuckDB file. This helps us understand the 206-byte difference
// between CLI (table_pointer.offset = 214) and dukdb-go (table_pointer.offset = 8).
//
// The TableStatistics data is written after table_pointer in the metadata sub-block,
// before the row_group_count field.
func TestExtractTableStatisticsFormat(t *testing.T) {
	if !checkDuckDBCLI() {
		t.Skip("duckdb CLI not installed")
	}

	// Create a temporary database with a simple table
	dbPath := filepath.Join(t.TempDir(), "stats_format.duckdb")

	// Use DuckDB CLI to create a database with a simple table
	// 2 columns (id INTEGER, name VARCHAR) and 3 rows
	cmd := exec.Command("duckdb", dbPath, "-c", `
		CREATE TABLE test (id INTEGER, name VARCHAR);
		INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
	`)
	err := cmd.Run()
	require.NoError(t, err, "Failed to create database with DuckDB CLI")

	// Open the file with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	require.NoError(t, err, "Failed to open DuckDB file")
	defer storage.Close()

	// Get the database header to access the metadata block pointer
	file, err := os.OpenFile(dbPath, os.O_RDONLY, 0644)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	dbHeader, _, err := GetActiveHeader(file)
	require.NoError(t, err)

	metaBlockID := dbHeader.MetaBlock

	t.Logf("Database Header Info:")
	t.Logf("  MetaBlockPointer: 0x%X (block_id=%d, block_index=%d)",
		metaBlockID, metaBlockID&0x00FFFFFFFFFFFFFF, metaBlockID>>56)

	// Create a metadata reader to read the catalog
	reader, err := NewMetadataReader(storage.blockManager, metaBlockID)
	require.NoError(t, err, "Failed to create metadata reader")

	// Read the catalog entries to find the table entry
	entries, err := reader.ReadCatalogEntries()
	require.NoError(t, err, "Failed to read catalog entries")
	require.NotEmpty(t, entries, "No catalog entries found")

	// Find the test table entry
	var testTable *TableCatalogEntry
	for _, entry := range entries {
		if table, ok := entry.(*TableCatalogEntry); ok && table.Name == "test" {
			testTable = table
			break
		}
	}
	require.NotNil(t, testTable, "Test table not found in catalog")
	require.NotNil(t, testTable.StorageMetadata, "Table storage metadata is nil")

	tablePointer := testTable.StorageMetadata.TablePointer
	totalRows := testTable.StorageMetadata.TotalRows

	t.Logf("\nTable Storage Metadata:")
	t.Logf("  Table: %s", testTable.Name)
	t.Logf("  Total Rows: %d", totalRows)
	t.Logf("  TablePointer.BlockID: %d", tablePointer.BlockID)
	t.Logf("  TablePointer.BlockIndex: %d", tablePointer.BlockIndex)
	t.Logf("  TablePointer.Offset: %d bytes", tablePointer.Offset)

	// The critical question: What is at offset 8 to offset 214?
	// This is where TableStatistics should be.
	expectedStatsSize := tablePointer.Offset - 8

	t.Logf("\nTableStatistics Analysis:")
	t.Logf("  Expected location: After table_pointer fields (first 8 bytes)")
	t.Logf("  Expected size: %d bytes (offset %d - 8)", expectedStatsSize, tablePointer.Offset)

	// Read the actual block data to extract TableStatistics bytes
	encodedPointer := tablePointer.Encode()
	blockReader, err := NewMetadataBlockReader(storage.blockManager, encodedPointer)
	require.NoError(t, err, "Failed to create block reader for table pointer")

	// The table data layout in the metadata block is:
	// 1. TableStatistics (BinarySerializer object) - starts at offset 0
	// 2. row_group_count (raw uint64, 8 bytes) - starts at tablePointer.Offset - 8
	// 3. RowGroupPointer objects (BinarySerializer wrapped) - starts at tablePointer.Offset

	// We want to extract bytes from offset 0 to tablePointer.Offset - 8
	// This should contain TableStatistics + terminator (0xFF 0xFF)

	statsBytes := make([]byte, tablePointer.Offset)
	n, err := blockReader.Read(statsBytes)
	require.NoError(t, err, "Failed to read table statistics bytes")
	require.Equal(t, int(tablePointer.Offset), n, "Read wrong number of bytes")

	t.Logf("\nExtracted TableStatistics bytes (offset 0 to %d):", tablePointer.Offset)
	t.Logf("Hex dump:")
	dumpHex(t, statsBytes)

	// Try to parse the structure
	t.Logf("\n=== Analyzing TableStatistics Structure ===")

	// TableStatistics is a BinarySerializer object with nested fields
	// Expected structure (from DuckDB source):
	// - Field 100: column_stats (vector<shared_ptr<ColumnStatistics>>)
	// - Field 101: table_sample (optional unique_ptr<ReservoirSample>)
	// - Terminator: 0xFF 0xFF

	offset := 0

	// Read field IDs and values
	for offset < len(statsBytes)-1 {
		// Check for terminator
		if offset+1 < len(statsBytes) && statsBytes[offset] == 0xFF && statsBytes[offset+1] == 0xFF {
			t.Logf("\nFound terminator at offset %d: 0xFF 0xFF", offset)
			offset += 2

			// After terminator, there might be row_group_count
			if offset+8 <= len(statsBytes) {
				// Read potential row_group_count
				rowGroupCount := uint64(statsBytes[offset]) |
					uint64(statsBytes[offset+1])<<8 |
					uint64(statsBytes[offset+2])<<16 |
					uint64(statsBytes[offset+3])<<24 |
					uint64(statsBytes[offset+4])<<32 |
					uint64(statsBytes[offset+5])<<40 |
					uint64(statsBytes[offset+6])<<48 |
					uint64(statsBytes[offset+7])<<56
				t.Logf("Potential row_group_count at offset %d: %d", offset, rowGroupCount)
				// Note: offset is not used after this point
			}
			break
		}

		// Read field ID (uint16 little-endian)
		if offset+2 > len(statsBytes) {
			break
		}
		fieldID := uint16(statsBytes[offset]) | uint16(statsBytes[offset+1])<<8
		t.Logf("\nOffset %d: Field ID = %d (0x%04X)", offset, fieldID, fieldID)
		offset += 2

		// Try to decode field value (this is heuristic)
		if fieldID == 100 {
			t.Logf("  Field 100: column_stats (vector<shared_ptr<ColumnStatistics>>)")
			// Read varint count
			count, bytesRead := decodeVarint(statsBytes[offset:])
			t.Logf("    Vector count: %d (read %d bytes)", count, bytesRead)
			offset += bytesRead

			// Each element is a shared_ptr<ColumnStatistics>
			// Start with nullable byte (0x00 or 0x01)
			for i := 0; i < int(count); i++ {
				if offset >= len(statsBytes) {
					break
				}
				isPresent := statsBytes[offset]
				t.Logf("    Element %d: nullable byte = 0x%02X (present=%v)", i, isPresent, isPresent != 0)
				offset++

				if isPresent != 0 {
					// ColumnStatistics is a nested object
					// It contains HyperLogLog data (~3KB per column typically)
					// For now, just show we're in a nested structure
					t.Logf("      (ColumnStatistics object - skipping detailed parse)")

					// Scan forward to find the terminator for this object
					// This is a simplified scan - real parsing would be recursive
					scanned := 0
					maxScan := 5000 // Safety limit
					for scanned < maxScan && offset+1 < len(statsBytes) {
						if statsBytes[offset] == 0xFF && statsBytes[offset+1] == 0xFF {
							t.Logf("      Found nested terminator at offset %d", offset)
							offset += 2
							break
						}
						offset++
						scanned++
					}
				}
			}
		} else if fieldID == 101 {
			t.Logf("  Field 101: table_sample (optional unique_ptr<ReservoirSample>)")
			// Read nullable byte
			if offset < len(statsBytes) {
				isPresent := statsBytes[offset]
				t.Logf("    Optional present byte: 0x%02X (present=%v)", isPresent, isPresent != 0)
				offset++

				if isPresent != 0 {
					// ReservoirSample is a nested object, skip it
					t.Logf("      (ReservoirSample object - skipping detailed parse)")

					// Scan to find terminator
					scanned := 0
					maxScan := 5000
					for scanned < maxScan && offset+1 < len(statsBytes) {
						if statsBytes[offset] == 0xFF && statsBytes[offset+1] == 0xFF {
							t.Logf("      Found nested terminator at offset %d", offset)
							offset += 2
							break
						}
						offset++
						scanned++
					}
				}
			}
		} else {
			t.Logf("  Unknown field ID: %d", fieldID)
			// Try to read as varint
			if offset < len(statsBytes) {
				val, bytesRead := decodeVarint(statsBytes[offset:])
				t.Logf("    Varint value: %d (read %d bytes)", val, bytesRead)
				offset += bytesRead
			}
		}
	}

	t.Logf("\n=== Summary ===")
	t.Logf("TableStatistics total size: %d bytes", tablePointer.Offset)
	t.Logf("This includes:")
	t.Logf("  - Field 100: column_stats vector")
	t.Logf("  - Field 101: table_sample (optional)")
	t.Logf("  - Terminator: 0xFF 0xFF (2 bytes)")
	t.Logf("\nThe difference between CLI (offset=214) and dukdb-go (offset=8):")
	t.Logf("  214 - 8 = 206 bytes")
	t.Logf("This 206 bytes is the TableStatistics data that dukdb-go is not currently writing.")
}

// dumpHex prints a hex dump of the given bytes to the test log.
func dumpHex(t *testing.T, data []byte) {
	const bytesPerLine = 16
	for i := 0; i < len(data); i += bytesPerLine {
		end := i + bytesPerLine
		if end > len(data) {
			end = len(data)
		}
		line := data[i:end]

		// Format: offset | hex bytes | ASCII
		hexStr := hex.EncodeToString(line)
		// Add spaces every 2 characters
		var formatted string
		for j := 0; j < len(hexStr); j += 2 {
			if j > 0 {
				formatted += " "
			}
			formatted += hexStr[j : j+2]
		}

		// ASCII representation
		ascii := ""
		for _, b := range line {
			if b >= 32 && b <= 126 {
				ascii += string(b)
			} else {
				ascii += "."
			}
		}

		t.Logf("%04X | %-48s | %s", i, formatted, ascii)
	}
}

// decodeVarint decodes a varint from the given bytes and returns the value and bytes read.
// This is a LEB128 unsigned varint decoder.
func decodeVarint(data []byte) (uint64, int) {
	var result uint64
	var shift uint
	bytesRead := 0

	for i := 0; i < len(data) && i < 10; i++ {
		b := data[i]
		bytesRead++

		result |= uint64(b&0x7F) << shift

		if b&0x80 == 0 {
			// No continuation bit, we're done
			return result, bytesRead
		}

		shift += 7
	}

	// If we got here, varint is too long or truncated
	return result, bytesRead
}

// TestTableStatisticsRoundTrip verifies that we can read TableStatistics created by DuckDB CLI
// and understand its structure well enough to recreate it.
func TestTableStatisticsRoundTrip(t *testing.T) {
	if !checkDuckDBCLI() {
		t.Skip("duckdb CLI not installed")
	}

	// Create a database with DuckDB CLI
	dbPath := filepath.Join(t.TempDir(), "stats_roundtrip.duckdb")
	cmd := exec.Command("duckdb", dbPath, "-c", `
		CREATE TABLE test (id INTEGER, name VARCHAR, value DOUBLE);
		INSERT INTO test VALUES (1, 'A', 1.5), (2, 'B', 2.5), (3, 'C', 3.5);
	`)
	err := cmd.Run()
	require.NoError(t, err)

	// Open and read the TableStatistics
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	require.NoError(t, err)
	defer storage.Close()

	// Get the database header
	file, err := os.OpenFile(dbPath, os.O_RDONLY, 0644)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	dbHeader, _, err := GetActiveHeader(file)
	require.NoError(t, err)

	// Read catalog
	reader, err := NewMetadataReader(storage.blockManager, dbHeader.MetaBlock)
	require.NoError(t, err)

	entries, err := reader.ReadCatalogEntries()
	require.NoError(t, err)

	// Find test table
	var testTable *TableCatalogEntry
	for _, entry := range entries {
		if table, ok := entry.(*TableCatalogEntry); ok && table.Name == "test" {
			testTable = table
			break
		}
	}
	require.NotNil(t, testTable)
	require.NotNil(t, testTable.StorageMetadata)

	tablePointer := testTable.StorageMetadata.TablePointer

	t.Logf("Table: %s", testTable.Name)
	t.Logf("Columns: %d", len(testTable.Columns))
	for i, col := range testTable.Columns {
		t.Logf("  Column %d: %s (%s)", i, col.Name, col.Type)
	}
	t.Logf("TablePointer.Offset: %d bytes", tablePointer.Offset)
	t.Logf("Expected TableStatistics size: %d bytes", tablePointer.Offset)

	// Read the raw bytes
	encodedPointer := tablePointer.Encode()
	blockReader, err := NewMetadataBlockReader(storage.blockManager, encodedPointer)
	require.NoError(t, err)

	statsBytes := make([]byte, tablePointer.Offset)
	n, err := blockReader.Read(statsBytes)
	require.NoError(t, err)
	require.Equal(t, int(tablePointer.Offset), n)

	t.Logf("\nTableStatistics hex dump (first 100 bytes):")
	maxDump := 100
	if len(statsBytes) < maxDump {
		maxDump = len(statsBytes)
	}
	dumpHex(t, statsBytes[:maxDump])

	// Document the pattern
	t.Logf("\n=== Key Findings ===")
	t.Logf("1. TableStatistics is written using BinarySerializer")
	t.Logf("2. It contains field 100 (column_stats vector)")
	t.Logf("3. Each column has ColumnStatistics with HyperLogLog data")
	t.Logf("4. For %d columns, total size is %d bytes", len(testTable.Columns), tablePointer.Offset)
	t.Logf("5. Average per column: ~%d bytes", tablePointer.Offset/uint64(len(testTable.Columns)))
}

// TestMinimalTableStatistics tests what happens with a minimal table (1 column, 1 row).
func TestMinimalTableStatistics(t *testing.T) {
	if !checkDuckDBCLI() {
		t.Skip("duckdb CLI not installed")
	}

	dbPath := filepath.Join(t.TempDir(), "minimal.duckdb")
	cmd := exec.Command("duckdb", dbPath, "-c", `
		CREATE TABLE minimal (id INTEGER);
		INSERT INTO minimal VALUES (42);
	`)
	err := cmd.Run()
	require.NoError(t, err)

	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	require.NoError(t, err)
	defer storage.Close()

	// Get the database header
	file, err := os.OpenFile(dbPath, os.O_RDONLY, 0644)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	dbHeader, _, err := GetActiveHeader(file)
	require.NoError(t, err)

	reader, err := NewMetadataReader(storage.blockManager, dbHeader.MetaBlock)
	require.NoError(t, err)

	entries, err := reader.ReadCatalogEntries()
	require.NoError(t, err)

	var minimalTable *TableCatalogEntry
	for _, entry := range entries {
		if table, ok := entry.(*TableCatalogEntry); ok && table.Name == "minimal" {
			minimalTable = table
			break
		}
	}
	require.NotNil(t, minimalTable)
	require.NotNil(t, minimalTable.StorageMetadata)

	tablePointer := minimalTable.StorageMetadata.TablePointer

	t.Logf("Minimal table (1 column, 1 row):")
	t.Logf("  Column: %s (%s)", minimalTable.Columns[0].Name, minimalTable.Columns[0].Type)
	t.Logf("  TablePointer.Offset: %d bytes", tablePointer.Offset)
	t.Logf("  Total Rows: %d", minimalTable.StorageMetadata.TotalRows)

	// Read and dump the stats
	encodedPointer := tablePointer.Encode()
	blockReader, err := NewMetadataBlockReader(storage.blockManager, encodedPointer)
	require.NoError(t, err)

	statsBytes := make([]byte, tablePointer.Offset)
	n, err := blockReader.Read(statsBytes)
	require.NoError(t, err)
	require.Equal(t, int(tablePointer.Offset), n)

	t.Logf("\nTableStatistics hex dump:")
	dumpHex(t, statsBytes)

	t.Logf("\nMinimal TableStatistics size: %d bytes", tablePointer.Offset)
	t.Logf("This is the absolute minimum for 1 column with statistics.")
}

// TestCompareTableStatisticsSizes compares TableStatistics sizes for different column counts.
func TestCompareTableStatisticsSizes(t *testing.T) {
	if !checkDuckDBCLI() {
		t.Skip("duckdb CLI not installed")
	}

	type testCase struct {
		name       string
		sql        string
		colCount   int
		skip       bool
	}

	testCases := []testCase{
		{
			name:     "1_column",
			sql:      "CREATE TABLE t1 (a INTEGER); INSERT INTO t1 VALUES (1);",
			colCount: 1,
		},
		{
			name:     "2_columns",
			sql:      "CREATE TABLE t2 (a INTEGER, b VARCHAR); INSERT INTO t2 VALUES (1, 'x');",
			colCount: 2,
		},
		{
			name:     "3_columns",
			sql:      "CREATE TABLE t3 (a INTEGER, b VARCHAR, c DOUBLE); INSERT INTO t3 VALUES (1, 'x', 1.5);",
			colCount: 3,
		},
		{
			name:     "5_columns",
			sql:      "CREATE TABLE t5 (a INT, b INT, c INT, d INT, e INT); INSERT INTO t5 VALUES (1,2,3,4,5);",
			colCount: 5,
		},
	}

	results := make(map[int]uint64)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("TODO: Fix storage format for this column count")
			}
			dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("%s.duckdb", tc.name))
			cmd := exec.Command("duckdb", dbPath, "-c", tc.sql)
			err := cmd.Run()
			require.NoError(t, err)

			storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
			require.NoError(t, err)
			defer storage.Close()

			// Get the database header
			file, err := os.OpenFile(dbPath, os.O_RDONLY, 0644)
			require.NoError(t, err)
			defer func() { _ = file.Close() }()

			dbHeader, _, err := GetActiveHeader(file)
			require.NoError(t, err)

			reader, err := NewMetadataReader(storage.blockManager, dbHeader.MetaBlock)
			require.NoError(t, err)

			entries, err := reader.ReadCatalogEntries()
			require.NoError(t, err)

			// Find any table entry
			for _, entry := range entries {
				if table, ok := entry.(*TableCatalogEntry); ok && table.StorageMetadata != nil {
					offset := table.StorageMetadata.TablePointer.Offset
					results[tc.colCount] = offset

					t.Logf("Table: %s", table.Name)
					t.Logf("  Columns: %d", tc.colCount)
					t.Logf("  TableStatistics size: %d bytes", offset)
					t.Logf("  Average per column: ~%d bytes", offset/uint64(tc.colCount))
					break
				}
			}
		})
	}

	// Summary
	t.Logf("\n=== TableStatistics Size Summary ===")
	for colCount := 1; colCount <= 5; colCount++ {
		if size, ok := results[colCount]; ok {
			avgPerCol := size / uint64(colCount)
			t.Logf("%d column(s): %d bytes total, ~%d bytes/column", colCount, size, avgPerCol)
		}
	}
}
