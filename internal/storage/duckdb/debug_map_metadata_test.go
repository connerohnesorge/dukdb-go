package duckdb

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebugMapMetadataParsing is a debug test to understand how MAP column
// metadata is different from LIST column metadata.
func TestDebugMapMetadataParsing(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with simple MAP values - same structure as TestReadMapValues
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

	// Load catalog which populates rowGroups
	_, err = storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)

	// Get the DuckDB catalog directly
	cat := storage.catalog
	require.NotEmpty(t, cat.Tables, "should have tables")

	// Find the map_test table
	var table *TableCatalogEntry
	for _, tbl := range cat.Tables {
		if tbl.Name == "map_test" {
			table = tbl
			break
		}
	}
	require.NotNil(t, table, "table 'map_test' should exist")

	t.Logf("Table: %s", table.Name)
	t.Logf("Table StorageMetadata: %+v", table.StorageMetadata)
	t.Logf("Column count: %d", len(table.Columns))
	for i, col := range table.Columns {
		t.Logf("  Column %d: %s (type=%d)", i, col.Name, col.Type)
	}

	// Get the row groups
	var rowGroupPtrs []*RowGroupPointer
	for _, rgps := range storage.rowGroups {
		if len(rgps) > 0 {
			rowGroupPtrs = rgps
			break
		}
	}
	require.NotEmpty(t, rowGroupPtrs, "should have at least one row group")

	t.Logf("Found %d row groups", len(rowGroupPtrs))

	for rgIdx, rg := range rowGroupPtrs {
		t.Logf("Row group %d: RowStart=%d, TupleCount=%d, DataPointers=%d",
			rgIdx, rg.RowStart, rg.TupleCount, len(rg.DataPointers))

		for dpIdx, colPtr := range rg.DataPointers {
			t.Logf("  Column %d metadata pointer: BlockID=%d, BlockIndex=%d, Offset=%d",
				dpIdx, colPtr.BlockID, colPtr.BlockIndex, colPtr.Offset)

			// Read raw bytes from the metadata block to see what's there
			dumpColumnMetadataRaw(t, storage.blockManager, colPtr, 512) // Dump up to 512 bytes

			// Now try to parse it with debug logging
			dp, err := ReadColumnDataPointerDebug(t, storage.blockManager, colPtr)
			if err != nil {
				t.Logf("  Error reading column data pointer: %v", err)
				continue
			}

			t.Logf("  Parsed DataPointer:")
			t.Logf("    TupleCount: %d", dp.TupleCount)
			t.Logf("    Block: ID=%d, Offset=%d", dp.Block.BlockID, dp.Block.Offset)
			t.Logf("    Compression: %d (%s)", dp.Compression, dp.Compression.String())
			t.Logf("    Statistics.HasStats: %v", dp.Statistics.HasStats)
			t.Logf("    Statistics.HasNull: %v", dp.Statistics.HasNull)
			t.Logf("    Statistics.StatData len: %d", len(dp.Statistics.StatData))
			t.Logf("    SegmentState.HasValidityMask: %v", dp.SegmentState.HasValidityMask)

			if dp.ValidityPointer != nil {
				t.Logf(
					"    ValidityPointer: TupleCount=%d, Block.ID=%d, Block.Offset=%d",
					dp.ValidityPointer.TupleCount,
					dp.ValidityPointer.Block.BlockID,
					dp.ValidityPointer.Block.Offset,
				)
			} else {
				t.Logf("    ValidityPointer: nil (BUG! MAP column should have ValidityPointer for NULL rows)")
			}

			if dp.ChildPointer != nil {
				t.Logf(
					"    ChildPointer: TupleCount=%d, Block.ID=%d, Block.Offset=%d",
					dp.ChildPointer.TupleCount,
					dp.ChildPointer.Block.BlockID,
					dp.ChildPointer.Block.Offset,
				)
			} else {
				t.Logf("    ChildPointer: nil (BUG! MAP column should have ChildPointer for STRUCT(key,value) child)")
			}
		}
	}

	// Force the test to fail so we can see the output
	t.Log("=== Debug test complete - check logs above ===")
}

// ReadColumnDataPointerDebug is a debug version that logs the parsing process
func ReadColumnDataPointerDebug(
	t *testing.T,
	bm *BlockManager,
	mbp MetaBlockPointer,
) (*DataPointer, error) {
	// Create a metadata reader at the ColumnData location
	encodedPointer := mbp.Encode()
	reader, err := NewMetadataReaderWithOffset(bm, encodedPointer, mbp.Offset)
	if err != nil {
		return nil, err
	}

	t.Logf("    DEBUG: Starting at offset %d", reader.offset())

	// Peek at the first field
	firstFieldID, err := reader.PeekField()
	if err != nil {
		return nil, err
	}
	t.Logf("    DEBUG: First field is %d at offset %d", firstFieldID, reader.offset())

	// Handle STRUCT column format
	if firstFieldID >= 101 {
		t.Logf("    DEBUG: STRUCT format detected (first field >= 101)")
		return readStructColumnData(reader)
	}

	// Read Field 100: data_pointers
	if err := reader.OnPropertyBegin(100); err != nil {
		return nil, err
	}
	t.Logf("    DEBUG: After consuming Field 100, offset=%d", reader.offset())

	// Read the count
	dataPointerCount, err := reader.ReadVarint()
	if err != nil {
		return nil, err
	}
	t.Logf("    DEBUG: data_pointers count=%d, offset=%d", dataPointerCount, reader.offset())

	if dataPointerCount == 0 {
		return nil, errNoDataPointers
	}

	// Read the first DataPointer
	dp, err := readDataPointer(reader)
	if err != nil {
		return nil, err
	}
	t.Logf("    DEBUG: After reading DataPointer, offset=%d", reader.offset())

	// Skip remaining DataPointers
	for i := uint64(1); i < dataPointerCount; i++ {
		if _, err := readDataPointer(reader); err != nil {
			return nil, err
		}
	}
	t.Logf("    DEBUG: After all DataPointers, offset=%d", reader.offset())

	// Peek next field
	fieldID, err := reader.PeekField()
	if err != nil {
		t.Logf("    DEBUG: PeekField error: %v, offset=%d", err, reader.offset())
		return dp, nil
	}
	t.Logf(
		"    DEBUG: Next field after DataPointers is %d (0x%04x) at offset %d",
		fieldID,
		fieldID,
		reader.offset(),
	)

	// Skip terminators
	terminatorCount := 0
	for fieldID == ddbFieldTerminator && terminatorCount < 10 {
		reader.ConsumeField()
		terminatorCount++
		fieldID, err = reader.PeekField()
		if err != nil {
			t.Logf(
				"    DEBUG: After skipping %d terminators, PeekField error: %v",
				terminatorCount,
				err,
			)
			return dp, nil
		}
		t.Logf(
			"    DEBUG: Skipped terminator %d, next field is %d at offset %d",
			terminatorCount,
			fieldID,
			reader.offset(),
		)
	}

	t.Logf(
		"    DEBUG: After skipping %d terminators, field is %d at offset %d",
		terminatorCount,
		fieldID,
		reader.offset(),
	)

	// Check for Field 101 (validity)
	if fieldID == 101 {
		t.Logf("    DEBUG: Found Field 101 (validity) at offset %d", reader.offset())
		reader.ConsumeField()

		validityDP, err := readValidityColumnData(reader)
		if err != nil {
			t.Logf("    DEBUG: readValidityColumnData error: %v", err)
			return dp, nil
		}
		dp.ValidityPointer = validityDP
		dp.SegmentState.HasValidityMask = true
		t.Logf("    DEBUG: After reading validity, offset=%d", reader.offset())

		fieldID, err = reader.PeekField()
		if err != nil {
			return dp, nil
		}
		t.Logf("    DEBUG: Next field after validity is %d at offset %d", fieldID, reader.offset())
	} else {
		t.Logf("    DEBUG: Field 101 NOT found, got field %d instead", fieldID)
	}

	// Check for Field 102 (child)
	if fieldID == 102 {
		t.Logf("    DEBUG: Found Field 102 (child) at offset %d", reader.offset())
		reader.ConsumeField()

		childDP, err := readChildColumnData(reader)
		if err != nil {
			t.Logf("    DEBUG: readChildColumnData error: %v", err)
			return dp, nil
		}
		dp.ChildPointer = childDP
		t.Logf("    DEBUG: After reading child, offset=%d", reader.offset())
	} else {
		t.Logf("    DEBUG: Field 102 NOT found, got field %d instead", fieldID)
	}

	return dp, nil
}

// dumpColumnMetadataRaw reads raw bytes from a column metadata location and logs them
func dumpColumnMetadataRaw(t *testing.T, bm *BlockManager, mbp MetaBlockPointer, maxBytes int) {
	t.Helper()

	// Create a metadata reader at the location
	encodedPointer := mbp.Encode()
	reader, err := NewMetadataReaderWithOffset(bm, encodedPointer, mbp.Offset)
	if err != nil {
		t.Logf("    Failed to create reader for raw dump: %v", err)
		return
	}

	// Read raw bytes from the block reader
	data := reader.data()
	offset := reader.offset()

	endOffset := offset + maxBytes
	if endOffset > len(data) {
		endOffset = len(data)
	}

	rawBytes := data[offset:endOffset]
	t.Logf("    Raw metadata bytes (from offset %d, %d bytes):", offset, len(rawBytes))

	// Hex dump with annotations
	for i := 0; i < len(rawBytes); i += 16 {
		end := i + 16
		if end > len(rawBytes) {
			end = len(rawBytes)
		}
		chunk := rawBytes[i:end]

		hexStr := hex.EncodeToString(chunk)
		// Add spacing for readability
		formatted := ""
		for j := 0; j < len(hexStr); j += 2 {
			if j > 0 {
				formatted += " "
			}
			formatted += hexStr[j : j+2]
		}

		// ASCII representation
		ascii := ""
		for _, b := range chunk {
			if b >= 32 && b < 127 {
				ascii += string(b)
			} else {
				ascii += "."
			}
		}

		t.Logf("      %04x: %-48s  %s", offset+i, formatted, ascii)
	}

	// Scan for specific patterns
	t.Logf("    Field markers found:")
	for i := 0; i < len(rawBytes)-1; i++ {
		low := rawBytes[i]
		high := rawBytes[i+1]

		if low == 0x64 && high == 0x00 {
			t.Logf("      Field 100 at offset %d (0x%04x)", offset+i, offset+i)
		}
		if low == 0x65 && high == 0x00 {
			t.Logf("      Field 101 at offset %d (0x%04x)", offset+i, offset+i)
		}
		if low == 0x66 && high == 0x00 {
			t.Logf("      Field 102 at offset %d (0x%04x)", offset+i, offset+i)
		}
		if low == 0xff && high == 0xff {
			t.Logf("      Terminator at offset %d (0x%04x)", offset+i, offset+i)
		}
	}
}

// TestCompareListVsMapMetadata compares LIST and MAP metadata parsing side by side
func TestCompareListVsMapMetadata(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test files with similar structures
	listDbPath, listCleanup := createDuckDBTestFile(t, `
		CREATE TABLE list_test (value INTEGER[]);
		INSERT INTO list_test VALUES
			([1, 2]),
			([]),
			(NULL);
	`)
	defer listCleanup()

	mapDbPath, mapCleanup := createDuckDBTestFile(t, `
		CREATE TABLE map_test (value MAP(VARCHAR, INTEGER));
		INSERT INTO map_test VALUES
			(MAP(['a', 'b'], [1, 2])),
			(MAP([], [])),
			(NULL);
	`)
	defer mapCleanup()

	// Compare both
	for _, tc := range []struct {
		name   string
		path   string
		table  string
		typeID LogicalTypeID
	}{
		{"LIST", listDbPath, "list_test", TypeList},
		{"MAP", mapDbPath, "map_test", TypeMap},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage, err := OpenDuckDBStorage(tc.path, &Config{ReadOnly: true})
			skipOnFormatError(t, err)
			require.NoError(t, err)
			defer func() { _ = storage.Close() }()

			_, err = storage.LoadCatalog()
			skipOnFormatError(t, err)
			require.NoError(t, err)

			// Get row groups
			var rowGroups []*RowGroupPointer
			for _, rgps := range storage.rowGroups {
				if len(rgps) > 0 {
					rowGroups = rgps
					break
				}
			}
			require.NotEmpty(t, rowGroups)

			for _, rg := range rowGroups {
				for dpIdx, colPtr := range rg.DataPointers {
					t.Logf("%s Column %d metadata:", tc.name, dpIdx)

					// Read the data pointer
					dp, err := ReadColumnDataPointer(storage.blockManager, colPtr)
					if err != nil {
						t.Logf("  Error: %v", err)
						continue
					}

					t.Logf("  TupleCount: %d", dp.TupleCount)
					t.Logf("  ValidityPointer: %v", dp.ValidityPointer != nil)
					t.Logf("  ChildPointer: %v", dp.ChildPointer != nil)

					if tc.typeID == TypeMap && dp.ChildPointer == nil {
						t.Errorf("MAP column missing ChildPointer - this is the bug!")
					}
					if tc.typeID == TypeList && dp.ChildPointer == nil && rg.TupleCount > 0 {
						t.Logf(
							"LIST column has no ChildPointer (might be OK for small inline data)",
						)
					}
				}
			}
		})
	}
}

// errNoDataPointers is returned when ColumnData has no data_pointers
var errNoDataPointers = fmt.Errorf("ColumnData has no data_pointers")
