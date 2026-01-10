package duckdb

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTableScanner(t *testing.T) {
	t.Parallel()

	// Create test environment
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, bm, cleanup := setupTestFile(t, tmpFile)
	defer cleanup()
	_ = file

	// Create a table entry
	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "name", Type: TypeVarchar})
	table.AddColumn(ColumnDefinition{Name: "value", Type: TypeDouble})

	// Create row groups
	rowGroups := []*RowGroupPointer{
		{TableOID: 1, RowStart: 0, TupleCount: 100},
		{TableOID: 1, RowStart: 100, TupleCount: 50},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	require.NotNil(t, scanner)

	assert.Equal(t, 2, scanner.RowGroupCount())
	assert.Equal(t, uint64(150), scanner.TotalTableRows())
	assert.Equal(t, -1, scanner.CurrentRowGroupIndex())
	assert.Nil(t, scanner.Projection())

	scanner.Close()
}

func TestTableScannerSetProjection(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFile(t, tmpFile)
	defer cleanup()

	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "name", Type: TypeVarchar})
	table.AddColumn(ColumnDefinition{Name: "value", Type: TypeDouble})

	rowGroups := []*RowGroupPointer{
		{TableOID: 1, RowStart: 0, TupleCount: 100},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	t.Run("valid projection", func(t *testing.T) {
		err := scanner.SetProjection([]int{0, 2})
		require.NoError(t, err)

		proj := scanner.Projection()
		assert.Equal(t, []int{0, 2}, proj)
	})

	t.Run("nil projection resets to all columns", func(t *testing.T) {
		err := scanner.SetProjection(nil)
		require.NoError(t, err)
		assert.Nil(t, scanner.Projection())
	})

	t.Run("invalid column index", func(t *testing.T) {
		err := scanner.SetProjection([]int{0, 5})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidProjection)
	})

	t.Run("negative column index", func(t *testing.T) {
		err := scanner.SetProjection([]int{-1})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidProjection)
	})
}


func TestTableScannerReset(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFileWithBlocks(t, tmpFile, 4)
	defer cleanup()

	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	dp := NewDataPointer(0, 3, BlockPointer{BlockID: 1, Offset: 0}, CompressionConstant)
	dpBytes := serializePersistentColumnDataForTest(dp)

	block0 := &Block{
		ID:   0,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	copy(block0.Data, dpBytes)

	block1 := &Block{
		ID:   1,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	binary.LittleEndian.PutUint32(block1.Data, 42)

	require.NoError(t, bm.WriteBlock(block0))
	require.NoError(t, bm.WriteBlock(block1))

	rowGroups := []*RowGroupPointer{
		{
			TableOID:     1,
			RowStart:     0,
			TupleCount:   3,
			DataPointers: []MetaBlockPointer{{BlockID: 0, Offset: 0}},
		},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	// Scan all rows
	count := 0
	for scanner.Next() {
		scanner.Advance()
		count++
	}
	assert.Equal(t, 3, count)
	assert.Equal(t, uint64(3), scanner.TotalRows())

	// Reset and scan again
	scanner.Reset()
	assert.Equal(t, uint64(0), scanner.TotalRows())
	assert.Equal(t, -1, scanner.CurrentRowGroupIndex())

	count = 0
	for scanner.Next() {
		scanner.Advance()
		count++
	}
	assert.Equal(t, 3, count)
}

func TestTableScannerClose(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFile(t, tmpFile)
	defer cleanup()

	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	rowGroups := []*RowGroupPointer{}

	scanner := NewTableScanner(bm, table, rowGroups)
	scanner.Close()

	// Operations should fail after close
	err := scanner.SetProjection([]int{0})
	assert.ErrorIs(t, err, ErrScannerClosed)

	assert.False(t, scanner.Next())

	_, err = scanner.GetRow()
	assert.ErrorIs(t, err, ErrScannerClosed)

	_, err = scanner.GetColumn(0)
	assert.ErrorIs(t, err, ErrScannerClosed)
}


func TestTableScannerScanRowGroupOutOfRange(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFile(t, tmpFile)
	defer cleanup()

	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	rowGroups := []*RowGroupPointer{
		{TableOID: 1, RowStart: 0, TupleCount: 5},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	_, err := scanner.ScanRowGroup(5)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrRowGroupIndexOutOfRange)

	_, err = scanner.ScanRowGroup(-1)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrRowGroupIndexOutOfRange)
}


func TestScanResultMethods(t *testing.T) {
	t.Parallel()

	// Create a ScanResult with test data
	validity := NewValidityMask(3)
	validity.SetInvalid(1) // Mark row 1 as NULL

	colData := &ColumnData{
		Data:       createInt32Array([]int32{10, 0, 30}),
		Validity:   validity,
		TupleCount: 3,
		TypeID:     TypeInteger,
	}

	result := &ScanResult{
		Columns:    []*ColumnData{colData},
		RowCount:   3,
		RowGroupID: 0,
	}

	t.Run("GetValue valid", func(t *testing.T) {
		val, valid := result.GetValue(0, 0)
		assert.True(t, valid)
		assert.Equal(t, int32(10), val)
	})

	t.Run("GetValue NULL", func(t *testing.T) {
		val, valid := result.GetValue(1, 0)
		assert.False(t, valid)
		assert.Nil(t, val)
	})

	t.Run("GetValue invalid column", func(t *testing.T) {
		val, valid := result.GetValue(0, 5)
		assert.False(t, valid)
		assert.Nil(t, val)
	})

	t.Run("IsNull", func(t *testing.T) {
		assert.False(t, result.IsNull(0, 0))
		assert.True(t, result.IsNull(1, 0))
		assert.False(t, result.IsNull(2, 0))
		assert.True(t, result.IsNull(0, 5)) // Invalid column returns true
	})
}


func TestTableScannerEmptyTable(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFile(t, tmpFile)
	defer cleanup()

	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	// No row groups
	rowGroups := []*RowGroupPointer{}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	assert.Equal(t, 0, scanner.RowGroupCount())
	assert.Equal(t, uint64(0), scanner.TotalTableRows())
	assert.False(t, scanner.Next())

	results, err := scanner.ScanAll()
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestTableScannerEmptyRowGroup(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFile(t, tmpFile)
	defer cleanup()

	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	// Row group with zero tuples
	rowGroups := []*RowGroupPointer{
		{
			TableOID:     1,
			RowStart:     0,
			TupleCount:   0,
			DataPointers: []MetaBlockPointer{},
		},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	assert.Equal(t, 1, scanner.RowGroupCount())
	assert.Equal(t, uint64(0), scanner.TotalTableRows())

	// Should not iterate any rows
	assert.False(t, scanner.Next())
}

// Helper functions for test setup

// serializePersistentColumnDataForTest serializes a DataPointer in DuckDB's
// PersistentColumnData format for use in tests. This format is:
// - Field 100: data_pointers count (varint)
// - Then inline DataPointer(s) with fields:
//   - Field 101: tuple_count
//   - Field 102: block_pointer (nested with field 100 for block_id, field 101 for offset)
//   - Field 103: compression
//   - Field 104: statistics (nested)
//   - Terminator 0xFFFF
// - Terminator 0xFFFF
//
// For CONSTANT compression with a valid constant value, the constant should be
// stored in the data block referenced by BlockPointer, not in statistics.
// The decompressor will read from that block.
func serializePersistentColumnDataForTest(dp *DataPointer) []byte {
	var buf []byte

	// Field 100: data_pointers count (always 1 for our tests)
	buf = append(buf, 0x64, 0x00) // Field 100
	buf = appendVarint(buf, 1)     // count = 1

	// DataPointer fields:
	// Field 101: tuple_count
	buf = append(buf, 0x65, 0x00) // Field 101
	buf = appendVarint(buf, dp.TupleCount)

	// Field 102: block_pointer (nested)
	buf = append(buf, 0x66, 0x00) // Field 102
	// BlockPointer field 100: block_id
	buf = append(buf, 0x64, 0x00) // Field 100
	buf = appendVarint(buf, dp.Block.BlockID)
	// BlockPointer field 101: offset
	buf = append(buf, 0x65, 0x00) // Field 101
	buf = appendVarint(buf, uint64(dp.Block.Offset))
	// BlockPointer terminator
	buf = append(buf, 0xff, 0xff)

	// Field 103: compression
	buf = append(buf, 0x67, 0x00) // Field 103
	buf = appendVarint(buf, uint64(dp.Compression))

	// Field 104: statistics (nested BaseStatistics)
	buf = append(buf, 0x68, 0x00) // Field 104
	// BaseStatistics field 100: has_null
	buf = append(buf, 0x64, 0x00) // Field 100
	if dp.Statistics.HasNull {
		buf = appendVarint(buf, 1)
	} else {
		buf = appendVarint(buf, 0)
	}
	// BaseStatistics field 101: has_max
	buf = append(buf, 0x65, 0x00) // Field 101
	if dp.Statistics.HasStats {
		buf = appendVarint(buf, 1)
	} else {
		buf = appendVarint(buf, 0)
	}
	// BaseStatistics field 102: has_min
	buf = append(buf, 0x66, 0x00) // Field 102
	buf = appendVarint(buf, 0)
	// BaseStatistics terminator
	buf = append(buf, 0xff, 0xff)

	// DataPointer terminator
	buf = append(buf, 0xff, 0xff)

	// PersistentColumnData terminator
	buf = append(buf, 0xff, 0xff)

	return buf
}

// appendVarint appends a varint-encoded uint64 to the buffer.
func appendVarint(buf []byte, val uint64) []byte {
	for {
		b := byte(val & 0x7F)
		val >>= 7
		if val == 0 {
			buf = append(buf, b)
			break
		}
		buf = append(buf, b|0x80)
	}
	return buf
}

func setupTestFile(t *testing.T, path string) (*os.File, *BlockManager, func()) {
	t.Helper()

	file, err := os.Create(path)
	require.NoError(t, err)

	// Write file header
	fileHeader := NewFileHeader()
	err = WriteFileHeader(file, fileHeader)
	require.NoError(t, err)

	// Write database headers
	dbHeader := NewDatabaseHeader()
	dbHeader.BlockAllocSize = DefaultBlockSize
	err = WriteDatabaseHeader(file, dbHeader, DatabaseHeader1Offset)
	require.NoError(t, err)
	err = WriteDatabaseHeader(file, dbHeader, DatabaseHeader2Offset)
	require.NoError(t, err)

	require.NoError(t, file.Close())

	// Re-open for read-write
	file, err = os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)

	bm := NewBlockManager(file, DefaultBlockSize, 10)

	cleanup := func() {
		_ = bm.Close()
		_ = file.Close()
	}

	return file, bm, cleanup
}

func setupTestFileWithBlocks(t *testing.T, path string, blockCount int) (*os.File, *BlockManager, func()) {
	t.Helper()

	file, bm, cleanup := setupTestFile(t, path)
	bm.SetBlockCount(uint64(blockCount))

	return file, bm, cleanup
}
