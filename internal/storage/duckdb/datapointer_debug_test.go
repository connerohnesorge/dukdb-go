package duckdb

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDebugDataPointerDeserialization creates a test file and examines the raw bytes
// of the metadata block to understand the DataPointer format.
func TestDebugDataPointerDeserialization(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create a simple test file with UUID values (one of the failing cases)
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

	// Load catalog which populates rowGroups
	_, err = storage.LoadCatalog()
	skipOnFormatError(t, err)
	require.NoError(t, err)

	// Try to scan the table - this will attempt to deserialize DataPointers
	iter, err := storage.ScanTable("main", "uuid_test", nil)
	if err != nil {
		t.Logf("ScanTable failed: %v", err)
		// Continue with manual inspection
	} else {
		defer iter.Close()
		if iter.Next() {
			t.Logf("Iterator succeeded!")
			return
		}
		if iter.Err() != nil {
			t.Logf("Iterator error: %v", iter.Err())
		}
	}

	// Get the row groups manually from storage - find any table with data
	var rowGroupPtrs []*RowGroupPointer
	for _, rgps := range storage.rowGroups {
		// Find a non-empty row group collection (likely our table)
		if len(rgps) > 0 {
			rowGroupPtrs = rgps
			break
		}
	}
	require.NotEmpty(t, rowGroupPtrs, "should have at least one row group")

	t.Logf("Found %d row groups", len(rowGroupPtrs))

	// Examine the first row group
	rgp := rowGroupPtrs[0]
	t.Logf("Row group 0: TupleCount=%d, DataPointers=%d", rgp.TupleCount, len(rgp.DataPointers))

	require.NotEmpty(t, rgp.DataPointers, "should have data pointers")

	// Get the first column's MetaBlockPointer
	mbp := rgp.DataPointers[0]
	t.Logf("Column 0 MetaBlockPointer: BlockID=%d, Offset=%d", mbp.BlockID, mbp.Offset)

	// Read the metadata block
	metaBlock, err := storage.blockManager.ReadBlock(mbp.BlockID)
	require.NoError(t, err)
	require.NotNil(t, metaBlock)

	t.Logf("Metadata block %d: Size=%d bytes", mbp.BlockID, len(metaBlock.Data))

	// Validate offset
	require.Less(t, mbp.Offset, uint64(len(metaBlock.Data)), "offset should be within block")

	// Get the data at the offset
	dataAtOffset := metaBlock.Data[mbp.Offset:]
	t.Logf("Data at offset %d: %d bytes available", mbp.Offset, len(dataAtOffset))

	// Dump the first 256 bytes (or all available) in hex
	dumpSize := 256
	if len(dataAtOffset) < dumpSize {
		dumpSize = len(dataAtOffset)
	}

	t.Logf("Hex dump of first %d bytes:", dumpSize)
	t.Logf("\n%s", hex.Dump(dataAtOffset[:dumpSize]))

	// Try to manually parse the DataPointer fields
	t.Logf("\nManual parsing attempt:")
	if len(dataAtOffset) >= 29 { // Minimum size for basic DataPointer fields
		offset := 0

		rowStart := readUint64(dataAtOffset[offset:])
		t.Logf("RowStart: %d (offset %d)", rowStart, offset)
		offset += 8

		tupleCount := readUint64(dataAtOffset[offset:])
		t.Logf("TupleCount: %d (offset %d)", tupleCount, offset)
		offset += 8

		blockID := readUint64(dataAtOffset[offset:])
		t.Logf("Block.BlockID: %d (offset %d)", blockID, offset)
		offset += 8

		blockOffset := readUint32(dataAtOffset[offset:])
		t.Logf("Block.Offset: %d (offset %d)", blockOffset, offset)
		offset += 4

		compression := dataAtOffset[offset]
		t.Logf("Compression: %d (%s) (offset %d)", compression, CompressionType(compression).String(), offset)
		offset += 1

		// Now comes BaseStatistics
		if len(dataAtOffset) > offset {
			hasStats := dataAtOffset[offset] != 0
			t.Logf("HasStats: %v (offset %d)", hasStats, offset)
			offset += 1

			if hasStats && len(dataAtOffset) > offset {
				hasNull := dataAtOffset[offset] != 0
				t.Logf("HasNull: %v (offset %d)", hasNull, offset)
				offset += 1

				if len(dataAtOffset) >= offset+16 {
					nullCount := readUint64(dataAtOffset[offset:])
					t.Logf("NullCount: %d (offset %d)", nullCount, offset)
					offset += 8

					distinctCount := readUint64(dataAtOffset[offset:])
					t.Logf("DistinctCount: %d (offset %d)", distinctCount, offset)
					offset += 8

					if len(dataAtOffset) >= offset+4 {
						statDataLen := readUint32(dataAtOffset[offset:])
						t.Logf("StatDataLen: %d (offset %d)", statDataLen, offset)
						offset += 4

						if statDataLen > 0 && len(dataAtOffset) >= offset+int(statDataLen) {
							t.Logf("StatData: %d bytes (offset %d)", statDataLen, offset)
							offset += int(statDataLen)
						} else if statDataLen > 0 {
							t.Logf("ERROR: StatDataLen=%d but only %d bytes available (need %d)",
								statDataLen, len(dataAtOffset)-offset, statDataLen)
						}
					}
				}
			}

			// Now comes ColumnSegmentState
			if len(dataAtOffset) > offset {
				t.Logf("\nSegmentState starting at offset %d (%d bytes remaining)", offset, len(dataAtOffset)-offset)
				hasValidityMask := dataAtOffset[offset] != 0
				t.Logf("HasValidityMask: %v (offset %d)", hasValidityMask, offset)
				offset += 1

				if hasValidityMask && len(dataAtOffset) >= offset+12 {
					validityBlockID := readUint64(dataAtOffset[offset:])
					t.Logf("ValidityBlock.BlockID: %d (offset %d)", validityBlockID, offset)
					offset += 8

					validityBlockOffset := readUint32(dataAtOffset[offset:])
					t.Logf("ValidityBlock.Offset: %d (offset %d)", validityBlockOffset, offset)
					offset += 4
				}

				if len(dataAtOffset) >= offset+4 {
					stateDataLen := readUint32(dataAtOffset[offset:])
					t.Logf("StateDataLen: %d (offset %d)", stateDataLen, offset)
					offset += 4

					if stateDataLen > 0 && len(dataAtOffset) >= offset+int(stateDataLen) {
						t.Logf("StateData: %d bytes (offset %d)", stateDataLen, offset)
						offset += int(stateDataLen)
					} else if stateDataLen > 0 {
						t.Logf("ERROR: StateDataLen=%d but only %d bytes available (need %d)",
							stateDataLen, len(dataAtOffset)-offset, stateDataLen)
					}
				} else {
					t.Logf("ERROR: Not enough bytes for StateDataLen at offset %d (need 4, have %d)",
						offset, len(dataAtOffset)-offset)
				}

				t.Logf("Total bytes consumed: %d", offset)
			}
		}
	}

	// Now try the actual deserialization and see where it fails
	t.Logf("\nAttempting actual deserialization:")
	dp, err := DeserializeDataPointer(dataAtOffset)
	if err != nil {
		t.Logf("Deserialization failed: %v", err)
		t.Logf("This confirms the issue - the deserializer expects more data than is available")
	} else {
		t.Logf("Deserialization succeeded!")
		t.Logf("DataPointer: RowStart=%d, TupleCount=%d, BlockID=%d, Offset=%d, Compression=%s",
			dp.RowStart, dp.TupleCount, dp.Block.BlockID, dp.Block.Offset, dp.Compression.String())
	}
}

// Helper function to read uint64 from bytes
func readUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

// Helper function to read uint32 from bytes
func readUint32(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
