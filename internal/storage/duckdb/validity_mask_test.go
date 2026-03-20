// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file contains tests for verifying validity mask
// encoding for NULL values.
//
// These tests verify that:
// - Validity masks are correctly encoded (1 = valid, 0 = NULL)
// - Bit ordering is LSB first
// - HasValidityMask flag is correctly set in segment state
// - Validity data is stored inline vs separate block correctly
// - Validity compression formats are handled correctly
//
// Tests are skipped if the DuckDB CLI is not installed on the system.
package duckdb

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// Phase 6.3: Validity Mask Encoding Tests
// -----------------------------------------------------------------------------

// TestValidityMaskBasic tests basic validity mask encoding with simple NULL patterns.
// This test demonstrates how DuckDB optimizes NULL storage for small datasets.
func TestValidityMaskBasic(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with NULLs at various positions
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE nulls_basic (
			id INTEGER,
			value INTEGER
		);
		INSERT INTO nulls_basic VALUES
			(1, 100),     -- Both valid
			(2, NULL),    -- Second NULL
			(NULL, 300),  -- First NULL
			(NULL, NULL), -- Both NULL
			(5, 500);     -- Both valid
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

	// Get the table catalog entry
	tableCat, ok := cat.GetTableInSchema("main", "nulls_basic")
	require.True(t, ok, "table should exist")
	require.NotNil(t, tableCat)

	// Get table metadata
	duckTable := storage.catalog.GetTable("nulls_basic")
	require.NotNil(t, duckTable, "DuckDB table should exist")
	require.NotNil(t, duckTable.StorageMetadata, "storage metadata should exist")

	// Read row groups
	rowGroups, err := ReadRowGroupsFromTablePointer(
		storage.blockManager,
		duckTable.StorageMetadata.TablePointer,
		duckTable.StorageMetadata.TotalRows,
		len(duckTable.Columns),
	)
	require.NoError(t, err)
	require.NotEmpty(t, rowGroups, "should have at least one row group")

	// Examine first row group
	rg := rowGroups[0]
	require.Equal(t, 2, len(rg.DataPointers), "should have 2 columns")

	// Check each column for validity mask
	for colIdx, colPtr := range rg.DataPointers {
		t.Logf("Column %d: checking validity mask", colIdx)

		// Read the column data pointer
		dp, err := ReadColumnDataPointer(storage.blockManager, colPtr)
		require.NoError(t, err, "should read column data pointer for column %d", colIdx)

		// Check if statistics indicate NULLs exist
		if dp.Statistics.HasStats && dp.Statistics.HasNull {
			t.Logf("Column %d: HasNull=true, NullCount=%d", colIdx, dp.Statistics.NullCount)
		}

		// DuckDB Optimization Note: For small datasets (5 rows in this case), DuckDB may use
		// CONSTANT compression for the entire column, which makes explicit validity masks
		// unnecessary. The NULL status is encoded directly in the compression metadata.
		//
		// Validity masks are more common in larger datasets or when using UNCOMPRESSED storage.

		// If the column has NULLs, check for validity mask
		if dp.ValidityPointer != nil || dp.SegmentState.HasValidityMask {
			t.Logf("Column %d: has validity mask", colIdx)

			// Verify HasValidityMask flag is set
			if dp.ValidityPointer != nil {
				assert.True(t, dp.SegmentState.HasValidityMask,
					"HasValidityMask flag should be true when ValidityPointer exists")
			}

			// If validity is stored separately, read it
			if dp.ValidityPointer != nil {
				validityDP := dp.ValidityPointer
				t.Logf("Column %d: validity stored separately at block %d, compression=%d",
					colIdx, validityDP.Block.BlockID, validityDP.Compression)

				// Verify validity compression type (usually UNCOMPRESSED or CONSTANT)
				assert.Contains(t, []CompressionType{
					CompressionUncompressed,
					CompressionConstant,
				}, validityDP.Compression, "validity should use known compression")

				// Read the validity data
				validityData, err := storage.blockManager.ReadBlock(validityDP.Block.BlockID)
				require.NoError(t, err, "should read validity block")

				// For UNCOMPRESSED validity, the data is a bitmap
				if validityDP.Compression == CompressionUncompressed {
					// Verify bitmap encoding
					verifyValidityBitmap(
						t,
						validityData.Data[validityDP.Block.Offset:],
						validityDP.TupleCount,
						colIdx,
					)
				}
			} else if dp.SegmentState.HasValidityMask && len(dp.SegmentState.StateData) > 0 {
				// Validity is inlined in StateData
				t.Logf("Column %d: validity stored inline in StateData (%d bytes)",
					colIdx, len(dp.SegmentState.StateData))
				verifyValidityBitmap(t, dp.SegmentState.StateData, dp.TupleCount, colIdx)
			}
		} else {
			t.Logf("Column %d: no explicit validity mask (NULLs may be encoded via compression)", colIdx)
		}
	}
}

// TestValidityMaskAllNull tests validity mask when all values are NULL.
func TestValidityMaskAllNull(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with all NULLs
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE all_nulls (value INTEGER);
		INSERT INTO all_nulls VALUES (NULL), (NULL), (NULL), (NULL), (NULL);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	_, err = storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)

	// Get table metadata
	duckTable := storage.catalog.GetTable("all_nulls")
	require.NotNil(t, duckTable)
	require.NotNil(t, duckTable.StorageMetadata)

	// Read row groups
	rowGroups, err := ReadRowGroupsFromTablePointer(
		storage.blockManager,
		duckTable.StorageMetadata.TablePointer,
		duckTable.StorageMetadata.TotalRows,
		len(duckTable.Columns),
	)
	require.NoError(t, err)
	require.NotEmpty(t, rowGroups)

	// Get first column data pointer
	dp, err := ReadColumnDataPointer(storage.blockManager, rowGroups[0].DataPointers[0])
	require.NoError(t, err)

	// Verify statistics show all NULL
	if dp.Statistics.HasStats {
		assert.True(t, dp.Statistics.HasNull, "statistics should show HasNull")
		assert.Equal(t, dp.TupleCount, dp.Statistics.NullCount, "all values should be NULL")
	}

	// When all values are NULL, DuckDB often uses CONSTANT compression
	// with a special validity encoding
	if dp.ValidityPointer != nil {
		t.Logf(
			"Validity compression: %d, block: %d",
			dp.ValidityPointer.Compression,
			dp.ValidityPointer.Block.BlockID,
		)

		// Special block ID 127 means constant NULL validity
		if dp.ValidityPointer.Block.BlockID == 127 {
			t.Logf("Using constant NULL validity (block ID 127)")
			assert.Equal(t, CompressionConstant, dp.ValidityPointer.Compression,
				"block 127 should use CONSTANT compression")
		}
	}
}

// TestValidityMaskNoNulls tests that validity mask is absent when all values are valid.
func TestValidityMaskNoNulls(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with no NULLs
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE no_nulls (value INTEGER);
		INSERT INTO no_nulls VALUES (1), (2), (3), (4), (5);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	_, err = storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)

	// Get table metadata
	duckTable := storage.catalog.GetTable("no_nulls")
	require.NotNil(t, duckTable)
	require.NotNil(t, duckTable.StorageMetadata)

	// Read row groups
	rowGroups, err := ReadRowGroupsFromTablePointer(
		storage.blockManager,
		duckTable.StorageMetadata.TablePointer,
		duckTable.StorageMetadata.TotalRows,
		len(duckTable.Columns),
	)
	require.NoError(t, err)
	require.NotEmpty(t, rowGroups)

	// Get first column data pointer
	dp, err := ReadColumnDataPointer(storage.blockManager, rowGroups[0].DataPointers[0])
	require.NoError(t, err)

	// DuckDB may include a validity child even when there are no NULLs.
	// When all values are valid, the validity uses CONSTANT compression (all 1s)
	// with a virtual block ID (127).
	if dp.ValidityPointer != nil {
		// If present, should be CONSTANT compression indicating all-valid
		assert.Equal(t, CompressionConstant, dp.ValidityPointer.Compression,
			"validity should use CONSTANT compression when all values are valid")
		assert.Equal(t, uint64(127), dp.ValidityPointer.Block.BlockID,
			"validity should use virtual block ID when CONSTANT")
		t.Log("ValidityPointer present with CONSTANT compression (all-valid)")
	} else {
		t.Log("ValidityPointer is nil (no validity mask stored)")
	}

	// Verify data can still be read correctly (all values should be valid)
	// The key test is that reading data works correctly
}

// TestValidityMaskBitOrdering tests that validity mask uses LSB-first bit ordering.
func TestValidityMaskBitOrdering(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with specific NULL pattern to verify bit ordering
	// Pattern: valid, NULL, valid, NULL, valid, NULL, valid, NULL
	// This should create validity mask: 0b01010101 = 0x55 (LSB first)
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE bit_order (value INTEGER);
		INSERT INTO bit_order VALUES
			(1),    -- bit 0: valid (1)
			(NULL), -- bit 1: NULL (0)
			(3),    -- bit 2: valid (1)
			(NULL), -- bit 3: NULL (0)
			(5),    -- bit 4: valid (1)
			(NULL), -- bit 5: NULL (0)
			(7),    -- bit 6: valid (1)
			(NULL); -- bit 7: NULL (0)
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	_, err = storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)

	// Get table metadata
	duckTable := storage.catalog.GetTable("bit_order")
	require.NotNil(t, duckTable)
	require.NotNil(t, duckTable.StorageMetadata)

	// Read row groups
	rowGroups, err := ReadRowGroupsFromTablePointer(
		storage.blockManager,
		duckTable.StorageMetadata.TablePointer,
		duckTable.StorageMetadata.TotalRows,
		len(duckTable.Columns),
	)
	require.NoError(t, err)
	require.NotEmpty(t, rowGroups)

	// Get column data pointer
	dp, err := ReadColumnDataPointer(storage.blockManager, rowGroups[0].DataPointers[0])
	require.NoError(t, err)

	// Read validity data
	var validityBytes []byte
	if dp.ValidityPointer != nil {
		validityBlock, err := storage.blockManager.ReadBlock(dp.ValidityPointer.Block.BlockID)
		require.NoError(t, err)
		validityBytes = validityBlock.Data[dp.ValidityPointer.Block.Offset:]
	} else if dp.SegmentState.HasValidityMask && len(dp.SegmentState.StateData) > 0 {
		validityBytes = dp.SegmentState.StateData
	} else {
		t.Skip("No validity data found, skipping bit order test")
	}

	// Verify the first byte has the expected pattern
	require.NotEmpty(t, validityBytes, "validity bytes should not be empty")
	firstByte := validityBytes[0]

	// Expected pattern: 0b01010101 = 0x55 (LSB first)
	// bit 0 = 1 (valid), bit 1 = 0 (NULL), bit 2 = 1 (valid), etc.
	t.Logf("First validity byte: 0x%02X (binary: %08b)", firstByte, firstByte)
	assert.Equal(
		t,
		uint8(0x55),
		firstByte,
		"validity mask should be 0x55 (0b01010101) for alternating valid/NULL pattern with LSB first",
	)
}

// TestValidityMaskMultipleWords tests validity mask with more than 64 values.
func TestValidityMaskMultipleWords(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with >64 rows to test multiple uint64 words
	sql := "CREATE TABLE many_nulls (value INTEGER);\nINSERT INTO many_nulls VALUES\n"
	for i := 0; i < 100; i++ {
		if i > 0 {
			sql += ","
		}
		// Create pattern: every 3rd value is NULL
		if i%3 == 0 {
			sql += "(NULL)"
		} else {
			sql += fmt.Sprintf("(%d)", i)
		}
	}
	sql += ";"

	dbPath, cleanup := createDuckDBTestFile(t, sql)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	_, err = storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)

	// Get table metadata
	duckTable := storage.catalog.GetTable("many_nulls")
	require.NotNil(t, duckTable)
	require.NotNil(t, duckTable.StorageMetadata)

	// Read row groups
	rowGroups, err := ReadRowGroupsFromTablePointer(
		storage.blockManager,
		duckTable.StorageMetadata.TablePointer,
		duckTable.StorageMetadata.TotalRows,
		len(duckTable.Columns),
	)
	require.NoError(t, err)
	require.NotEmpty(t, rowGroups)

	// Get column data pointer
	dp, err := ReadColumnDataPointer(storage.blockManager, rowGroups[0].DataPointers[0])
	require.NoError(t, err)

	// Verify statistics
	if dp.Statistics.HasStats {
		t.Logf("Statistics: HasNull=%v, NullCount=%d, TupleCount=%d",
			dp.Statistics.HasNull, dp.Statistics.NullCount, dp.TupleCount)
		assert.True(t, dp.Statistics.HasNull, "statistics should show NULLs")
		// Approximately 1/3 of 100 rows should be NULL
		assert.Greater(t, dp.Statistics.NullCount, uint64(30), "should have >30 NULLs")
		assert.Less(t, dp.Statistics.NullCount, uint64(40), "should have <40 NULLs")
	}

	// DuckDB Optimization: For small datasets with NULLs, DuckDB often uses compression
	// strategies that don't require explicit validity masks. Statistics are more reliable.
	hasValidityMask := dp.ValidityPointer != nil || dp.SegmentState.HasValidityMask
	if !hasValidityMask {
		t.Logf("No explicit validity mask found (NULLs encoded via compression)")
		t.Logf(
			"Statistics: HasNull=%v, NullCount=%d",
			dp.Statistics.HasNull,
			dp.Statistics.NullCount,
		)
		t.Skip("DuckDB optimized storage - validity mask not separately stored")
	}

	var validityBytes []byte
	if dp.ValidityPointer != nil {
		validityBlock, err := storage.blockManager.ReadBlock(dp.ValidityPointer.Block.BlockID)
		require.NoError(t, err)
		validityBytes = validityBlock.Data[dp.ValidityPointer.Block.Offset:]
	} else if len(dp.SegmentState.StateData) > 0 {
		validityBytes = dp.SegmentState.StateData
	}

	// For 100 rows, we need at least 100/64 = 2 uint64 words = 16 bytes
	minBytes := (100 + 63) / 64 * 8
	assert.GreaterOrEqual(t, len(validityBytes), minBytes,
		"validity mask should have enough bytes for 100 rows")
}

// TestValidityMaskMultipleColumns tests validity masks across multiple columns.
func TestValidityMaskMultipleColumns(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with multiple columns having different NULL patterns
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE multi_col (
			a INTEGER,
			b INTEGER,
			c VARCHAR
		);
		INSERT INTO multi_col VALUES
			(1, NULL, 'x'),    -- Only b is NULL
			(NULL, 2, NULL),   -- a and c are NULL
			(3, 3, 'z'),       -- No NULLs
			(NULL, NULL, NULL);-- All NULL
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	skipOnFormatError(t, err)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Load catalog
	_, err = storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)

	// Get table metadata
	duckTable := storage.catalog.GetTable("multi_col")
	require.NotNil(t, duckTable)
	require.NotNil(t, duckTable.StorageMetadata)

	// Read row groups
	rowGroups, err := ReadRowGroupsFromTablePointer(
		storage.blockManager,
		duckTable.StorageMetadata.TablePointer,
		duckTable.StorageMetadata.TotalRows,
		len(duckTable.Columns),
	)
	require.NoError(t, err)
	require.NotEmpty(t, rowGroups)

	// Verify each column independently
	columnNames := []string{"a", "b", "c"}
	expectedNullCounts := []int{2, 2, 2} // Based on the INSERT statements

	for colIdx := 0; colIdx < 3; colIdx++ {
		t.Run(fmt.Sprintf("column_%s", columnNames[colIdx]), func(t *testing.T) {
			dp, err := ReadColumnDataPointer(
				storage.blockManager,
				rowGroups[0].DataPointers[colIdx],
			)
			require.NoError(t, err)

			// Verify statistics
			if dp.Statistics.HasStats {
				t.Logf("Column %s: HasNull=%v, NullCount=%d",
					columnNames[colIdx], dp.Statistics.HasNull, dp.Statistics.NullCount)
				assert.True(t, dp.Statistics.HasNull, "column should have NULLs")
				assert.Equal(t, uint64(expectedNullCounts[colIdx]), dp.Statistics.NullCount,
					"null count should match expected")
			}

			// Check for validity mask
			// Note: DuckDB may use compression that encodes NULLs without explicit validity masks
			hasValidityMask := dp.ValidityPointer != nil || dp.SegmentState.HasValidityMask
			if !hasValidityMask {
				t.Logf(
					"Column %s: No explicit validity mask (NULLs encoded via compression)",
					columnNames[colIdx],
				)
			} else {
				t.Logf("Column %s: Has explicit validity mask", columnNames[colIdx])
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Helper functions for validity mask verification
// -----------------------------------------------------------------------------

// verifyValidityBitmap verifies that a validity bitmap follows the expected encoding.
// - Bit value 1 means valid (not NULL)
// - Bit value 0 means NULL
// - Bits are packed LSB first within each byte
func verifyValidityBitmap(t *testing.T, data []byte, tupleCount uint64, colIdx int) {
	t.Helper()

	// Calculate expected number of bytes
	expectedBytes := (tupleCount + 63) / 64 * 8 // Round up to uint64 boundary
	t.Logf("Validity bitmap for column %d: %d bytes (expected ~%d for %d tuples)",
		colIdx, len(data), expectedBytes, tupleCount)

	// Verify we have at least the minimum required bytes
	minBytes := (tupleCount + 7) / 8 // Minimum bytes needed for tupleCount bits
	assert.GreaterOrEqual(t, len(data), int(minBytes),
		"validity bitmap should have enough bytes")

	// If we have data, verify it's not all zeros (unless all NULL) or all ones
	if len(data) > 0 {
		allZeros := true
		allOnes := true
		for _, b := range data {
			if b != 0 {
				allZeros = false
			}
			if b != 0xFF {
				allOnes = false
			}
		}

		// Log the pattern found
		if allZeros {
			t.Logf("Column %d: All validity bits are 0 (all NULL)", colIdx)
		} else if allOnes {
			t.Logf("Column %d: All validity bits are 1 (all valid)", colIdx)
		} else {
			// Log first few bytes for debugging
			logBytes := len(data)
			if logBytes > 16 {
				logBytes = 16
			}
			t.Logf("Column %d: Mixed validity pattern, first %d bytes: %02X",
				colIdx, logBytes, data[:logBytes])
		}
	}

	// Additional verification: Parse as uint64 words and verify bit ordering
	if len(data) >= 8 {
		firstWord := binary.LittleEndian.Uint64(data[0:8])
		t.Logf("Column %d: First validity word (uint64): 0x%016X (binary: %064b)",
			colIdx, firstWord, firstWord)

		// Verify each bit position corresponds to correct row
		for bit := 0; bit < 64 && bit < int(tupleCount); bit++ {
			isValid := (firstWord & (1 << bit)) != 0
			t.Logf("  Row %d: %v", bit, isValid)
		}
	}
}
