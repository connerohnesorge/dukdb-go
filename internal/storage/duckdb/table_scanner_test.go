package duckdb

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
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

func TestTableScannerRowIteration(t *testing.T) {
	t.Parallel()

	// Create test file with actual data
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFileWithBlocks(t, tmpFile, 4)
	defer cleanup()

	// Create table with integer column
	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	// Create DataPointer for column (constant value 42)
	dp := NewDataPointer(0, 5, BlockPointer{BlockID: 1, Offset: 0}, CompressionConstant)
	dpBytes, _ := dp.SerializeToBytes()

	// Block 0: DataPointer
	block0 := &Block{
		ID:   0,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	copy(block0.Data, dpBytes)

	// Block 1: Column data (constant value 42)
	block1 := &Block{
		ID:   1,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	binary.LittleEndian.PutUint32(block1.Data, 42)

	require.NoError(t, bm.WriteBlock(block0))
	require.NoError(t, bm.WriteBlock(block1))

	// Create row group pointing to our data
	rowGroups := []*RowGroupPointer{
		{
			TableOID:   1,
			RowStart:   0,
			TupleCount: 5,
			DataPointers: []MetaBlockPointer{
				{BlockID: 0, Offset: 0},
			},
		},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	// Test row iteration
	count := 0
	for scanner.Next() {
		row, err := scanner.GetRow()
		require.NoError(t, err)
		require.Len(t, row, 1)
		assert.Equal(t, int32(42), row[0])

		scanner.Advance()
		count++
	}

	assert.Equal(t, 5, count)
	assert.Equal(t, uint64(5), scanner.TotalRows())
}

func TestTableScannerMultipleRowGroups(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFileWithBlocks(t, tmpFile, 6)
	defer cleanup()

	// Create table with integer column
	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	// Row Group 1: constant 10
	dp1 := NewDataPointer(0, 3, BlockPointer{BlockID: 1, Offset: 0}, CompressionConstant)
	dpBytes1, _ := dp1.SerializeToBytes()

	block0 := &Block{
		ID:   0,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	copy(block0.Data, dpBytes1)

	block1 := &Block{
		ID:   1,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	binary.LittleEndian.PutUint32(block1.Data, 10)

	// Row Group 2: constant 20
	dp2 := NewDataPointer(0, 2, BlockPointer{BlockID: 3, Offset: 0}, CompressionConstant)
	dpBytes2, _ := dp2.SerializeToBytes()

	block2 := &Block{
		ID:   2,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	copy(block2.Data, dpBytes2)

	block3 := &Block{
		ID:   3,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	binary.LittleEndian.PutUint32(block3.Data, 20)

	require.NoError(t, bm.WriteBlock(block0))
	require.NoError(t, bm.WriteBlock(block1))
	require.NoError(t, bm.WriteBlock(block2))
	require.NoError(t, bm.WriteBlock(block3))

	rowGroups := []*RowGroupPointer{
		{
			TableOID:     1,
			RowStart:     0,
			TupleCount:   3,
			DataPointers: []MetaBlockPointer{{BlockID: 0, Offset: 0}},
		},
		{
			TableOID:     1,
			RowStart:     3,
			TupleCount:   2,
			DataPointers: []MetaBlockPointer{{BlockID: 2, Offset: 0}},
		},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	// Collect all values
	values := make([]int32, 0)
	for scanner.Next() {
		row, err := scanner.GetRow()
		require.NoError(t, err)
		values = append(values, row[0].(int32))
		scanner.Advance()
	}

	// Should have 3 values of 10, then 2 values of 20
	assert.Equal(t, []int32{10, 10, 10, 20, 20}, values)
	assert.Equal(t, uint64(5), scanner.TotalRows())
}

func TestTableScannerProjectionPushdown(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFileWithBlocks(t, tmpFile, 6)
	defer cleanup()

	// Create table with two columns
	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "value", Type: TypeBigInt})

	// Column 1: int32 constant 42
	dp1 := NewDataPointer(0, 3, BlockPointer{BlockID: 2, Offset: 0}, CompressionConstant)
	dpBytes1, _ := dp1.SerializeToBytes()

	// Column 2: int64 constant 999
	dp2 := NewDataPointer(0, 3, BlockPointer{BlockID: 3, Offset: 0}, CompressionConstant)
	dpBytes2, _ := dp2.SerializeToBytes()

	block0 := &Block{
		ID:   0,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	copy(block0.Data, dpBytes1)

	block1 := &Block{
		ID:   1,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	copy(block1.Data, dpBytes2)

	block2 := &Block{
		ID:   2,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	binary.LittleEndian.PutUint32(block2.Data, 42)

	block3 := &Block{
		ID:   3,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	binary.LittleEndian.PutUint64(block3.Data, 999)

	require.NoError(t, bm.WriteBlock(block0))
	require.NoError(t, bm.WriteBlock(block1))
	require.NoError(t, bm.WriteBlock(block2))
	require.NoError(t, bm.WriteBlock(block3))

	rowGroups := []*RowGroupPointer{
		{
			TableOID:   1,
			RowStart:   0,
			TupleCount: 3,
			DataPointers: []MetaBlockPointer{
				{BlockID: 0, Offset: 0}, // Column 0
				{BlockID: 1, Offset: 0}, // Column 1
			},
		},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	t.Run("project single column", func(t *testing.T) {
		scanner.Reset()
		err := scanner.SetProjection([]int{1}) // Only read "value" column
		require.NoError(t, err)

		assert.True(t, scanner.Next())
		row, err := scanner.GetRow()
		require.NoError(t, err)
		require.Len(t, row, 1)
		assert.Equal(t, int64(999), row[0])
		scanner.Advance()
	})

	t.Run("project multiple columns in different order", func(t *testing.T) {
		scanner.Reset()
		err := scanner.SetProjection([]int{1, 0}) // "value", then "id"
		require.NoError(t, err)

		assert.True(t, scanner.Next())
		row, err := scanner.GetRow()
		require.NoError(t, err)
		require.Len(t, row, 2)
		assert.Equal(t, int64(999), row[0]) // First in projection
		assert.Equal(t, int32(42), row[1])  // Second in projection
		scanner.Advance()
	})
}

func TestTableScannerGetColumn(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFileWithBlocks(t, tmpFile, 4)
	defer cleanup()

	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	dp := NewDataPointer(0, 5, BlockPointer{BlockID: 1, Offset: 0}, CompressionConstant)
	dpBytes, _ := dp.SerializeToBytes()

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
			TupleCount:   5,
			DataPointers: []MetaBlockPointer{{BlockID: 0, Offset: 0}},
		},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	assert.True(t, scanner.Next())

	val, err := scanner.GetColumn(0)
	require.NoError(t, err)
	assert.Equal(t, int32(42), val)

	// Invalid column index
	_, err = scanner.GetColumn(5)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrColumnIndexOutOfRange)
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
	dpBytes, _ := dp.SerializeToBytes()

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

func TestTableScannerScanRowGroup(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFileWithBlocks(t, tmpFile, 4)
	defer cleanup()

	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	dp := NewDataPointer(0, 5, BlockPointer{BlockID: 1, Offset: 0}, CompressionConstant)
	dpBytes, _ := dp.SerializeToBytes()

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
			TupleCount:   5,
			DataPointers: []MetaBlockPointer{{BlockID: 0, Offset: 0}},
		},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	result, err := scanner.ScanRowGroup(0)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, uint64(5), result.RowCount)
	assert.Equal(t, 0, result.RowGroupID)
	assert.Len(t, result.Columns, 1)

	// Verify all values
	for i := uint64(0); i < 5; i++ {
		val, valid := result.GetValue(i, 0)
		assert.True(t, valid)
		assert.Equal(t, int32(42), val)
	}
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

func TestTableScannerScanAll(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFileWithBlocks(t, tmpFile, 6)
	defer cleanup()

	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	// Row Group 1
	dp1 := NewDataPointer(0, 3, BlockPointer{BlockID: 1, Offset: 0}, CompressionConstant)
	dpBytes1, _ := dp1.SerializeToBytes()

	block0 := &Block{
		ID:   0,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	copy(block0.Data, dpBytes1)

	block1 := &Block{
		ID:   1,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	binary.LittleEndian.PutUint32(block1.Data, 10)

	// Row Group 2
	dp2 := NewDataPointer(0, 2, BlockPointer{BlockID: 3, Offset: 0}, CompressionConstant)
	dpBytes2, _ := dp2.SerializeToBytes()

	block2 := &Block{
		ID:   2,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	copy(block2.Data, dpBytes2)

	block3 := &Block{
		ID:   3,
		Data: make([]byte, DefaultBlockSize-BlockChecksumSize),
	}
	binary.LittleEndian.PutUint32(block3.Data, 20)

	require.NoError(t, bm.WriteBlock(block0))
	require.NoError(t, bm.WriteBlock(block1))
	require.NoError(t, bm.WriteBlock(block2))
	require.NoError(t, bm.WriteBlock(block3))

	rowGroups := []*RowGroupPointer{
		{
			TableOID:     1,
			RowStart:     0,
			TupleCount:   3,
			DataPointers: []MetaBlockPointer{{BlockID: 0, Offset: 0}},
		},
		{
			TableOID:     1,
			RowStart:     3,
			TupleCount:   2,
			DataPointers: []MetaBlockPointer{{BlockID: 2, Offset: 0}},
		},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	results, err := scanner.ScanAll()
	require.NoError(t, err)
	require.Len(t, results, 2)

	// First row group
	assert.Equal(t, uint64(3), results[0].RowCount)
	assert.Equal(t, 0, results[0].RowGroupID)
	for i := uint64(0); i < 3; i++ {
		val, valid := results[0].GetValue(i, 0)
		assert.True(t, valid)
		assert.Equal(t, int32(10), val)
	}

	// Second row group
	assert.Equal(t, uint64(2), results[1].RowCount)
	assert.Equal(t, 1, results[1].RowGroupID)
	for i := uint64(0); i < 2; i++ {
		val, valid := results[1].GetValue(i, 0)
		assert.True(t, valid)
		assert.Equal(t, int32(20), val)
	}
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

func TestTableScannerIterator(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFileWithBlocks(t, tmpFile, 4)
	defer cleanup()

	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	dp := NewDataPointer(0, 3, BlockPointer{BlockID: 1, Offset: 0}, CompressionConstant)
	dpBytes, _ := dp.SerializeToBytes()

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
	iter := NewTableScannerIterator(scanner)
	defer iter.Close()

	// Iterate through all rows
	count := 0
	for iter.Next() {
		row := iter.Row()
		require.NotNil(t, row)
		require.Len(t, row, 1)
		assert.Equal(t, int32(42), row[0])
		count++
	}

	assert.Equal(t, 3, count)
	assert.NoError(t, iter.Err())
}

func TestTableScannerConcurrentAccess(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	_, bm, cleanup := setupTestFileWithBlocks(t, tmpFile, 4)
	defer cleanup()

	table := NewTableCatalogEntry("test_table")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})

	dp := NewDataPointer(0, 10, BlockPointer{BlockID: 1, Offset: 0}, CompressionConstant)
	dpBytes, _ := dp.SerializeToBytes()

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
			TupleCount:   10,
			DataPointers: []MetaBlockPointer{{BlockID: 0, Offset: 0}},
		},
	}

	scanner := NewTableScanner(bm, table, rowGroups)
	defer scanner.Close()

	// Concurrent ScanRowGroup calls should be safe
	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				result, err := scanner.ScanRowGroup(0)
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, uint64(10), result.RowCount)
			}
		}()
	}

	wg.Wait()
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
