package duckdb

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnDataGetValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		typeID     LogicalTypeID
		data       []byte
		tupleCount uint64
		rowIdx     uint64
		wantValue  any
		wantValid  bool
	}{
		{
			name:       "boolean true",
			typeID:     TypeBoolean,
			data:       []byte{1, 0, 1},
			tupleCount: 3,
			rowIdx:     0,
			wantValue:  true,
			wantValid:  true,
		},
		{
			name:       "boolean false",
			typeID:     TypeBoolean,
			data:       []byte{1, 0, 1},
			tupleCount: 3,
			rowIdx:     1,
			wantValue:  false,
			wantValid:  true,
		},
		{
			name:       "int32 value",
			typeID:     TypeInteger,
			data:       intToBytes(42, 4),
			tupleCount: 1,
			rowIdx:     0,
			wantValue:  int32(42),
			wantValid:  true,
		},
		{
			name:       "int64 value",
			typeID:     TypeBigInt,
			data:       intToBytes(9999999999, 8),
			tupleCount: 1,
			rowIdx:     0,
			wantValue:  int64(9999999999),
			wantValid:  true,
		},
		{
			name:       "out of range",
			typeID:     TypeInteger,
			data:       intToBytes(42, 4),
			tupleCount: 1,
			rowIdx:     5,
			wantValue:  nil,
			wantValid:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			colData := &ColumnData{
				Data:       tt.data,
				Validity:   nil,
				TupleCount: tt.tupleCount,
				TypeID:     tt.typeID,
			}

			val, valid := colData.GetValue(tt.rowIdx)
			assert.Equal(t, tt.wantValid, valid)
			if valid {
				assert.Equal(t, tt.wantValue, val)
			}
		})
	}
}

func TestColumnDataIsNull(t *testing.T) {
	t.Parallel()

	t.Run("no validity mask means no nulls", func(t *testing.T) {
		t.Parallel()

		colData := &ColumnData{
			Data:       []byte{1, 2, 3, 4},
			Validity:   nil,
			TupleCount: 4,
			TypeID:     TypeTinyInt,
		}

		for i := uint64(0); i < 4; i++ {
			assert.False(t, colData.IsNull(i))
		}
	})

	t.Run("with validity mask", func(t *testing.T) {
		t.Parallel()

		validity := NewValidityMask(4)
		validity.SetInvalid(1) // Mark row 1 as NULL
		validity.SetInvalid(3) // Mark row 3 as NULL

		colData := &ColumnData{
			Data:       []byte{1, 2, 3, 4},
			Validity:   validity,
			TupleCount: 4,
			TypeID:     TypeTinyInt,
		}

		assert.False(t, colData.IsNull(0))
		assert.True(t, colData.IsNull(1))
		assert.False(t, colData.IsNull(2))
		assert.True(t, colData.IsNull(3))
	})
}

func TestGetTypeSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		typeID   LogicalTypeID
		expected int
	}{
		{TypeBoolean, 1},
		{TypeTinyInt, 1},
		{TypeUTinyInt, 1},
		{TypeSmallInt, 2},
		{TypeUSmallInt, 2},
		{TypeInteger, 4},
		{TypeUInteger, 4},
		{TypeDate, 4},
		{TypeFloat, 4},
		{TypeBigInt, 8},
		{TypeUBigInt, 8},
		{TypeDouble, 8},
		{TypeTimestamp, 8},
		{TypeHugeInt, 16},
		{TypeUUID, 16},
		{TypeVarchar, 0}, // Variable size
		{TypeBlob, 0},    // Variable size
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, GetTypeSize(tt.typeID))
		})
	}
}

func TestDecodeValidityMask(t *testing.T) {
	t.Parallel()

	t.Run("all valid", func(t *testing.T) {
		t.Parallel()

		// All bits set to 1
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, ^uint64(0))

		mask, err := DecodeValidityMask(data, 64)
		require.NoError(t, err)
		require.NotNil(t, mask)

		for i := uint64(0); i < 64; i++ {
			assert.True(t, mask.IsValid(i), "row %d should be valid", i)
		}
		assert.True(t, mask.AllValid())
	})

	t.Run("some nulls", func(t *testing.T) {
		t.Parallel()

		// Bits 0, 2 set (rows 0, 2 valid); bits 1, 3 unset (rows 1, 3 null)
		// Binary: ...0101 = 5
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, 5)

		mask, err := DecodeValidityMask(data, 4)
		require.NoError(t, err)
		require.NotNil(t, mask)

		assert.True(t, mask.IsValid(0))
		assert.False(t, mask.IsValid(1))
		assert.True(t, mask.IsValid(2))
		assert.False(t, mask.IsValid(3))
		assert.False(t, mask.AllValid())
	})

	t.Run("insufficient data", func(t *testing.T) {
		t.Parallel()

		data := make([]byte, 4) // Need 8 bytes for 64 rows

		_, err := DecodeValidityMask(data, 64)
		assert.Error(t, err)
	})
}


func TestRowIterator(t *testing.T) {
	t.Parallel()

	// Create a mock RowGroupReader with in-memory data
	colData1 := &ColumnData{
		Data:       createInt32Array([]int32{1, 2, 3, 4, 5}),
		Validity:   nil,
		TupleCount: 5,
		TypeID:     TypeInteger,
	}

	colData2 := &ColumnData{
		Data:       createInt64Array([]int64{10, 20, 30, 40, 50}),
		Validity:   nil,
		TupleCount: 5,
		TypeID:     TypeBigInt,
	}

	// Create a mock reader by directly populating the cache
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")

	file, err := os.Create(tmpFile)
	require.NoError(t, err)

	// Write minimal headers
	fileHeader := NewFileHeader()
	_ = WriteFileHeader(file, fileHeader)
	dbHeader := NewDatabaseHeader()
	_ = WriteDatabaseHeader(file, dbHeader, DatabaseHeader1Offset)
	_ = WriteDatabaseHeader(file, dbHeader, DatabaseHeader2Offset)
	_ = file.Close()

	file, _ = os.Open(tmpFile)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 10)

	rgp := &RowGroupPointer{
		TupleCount: 5,
		DataPointers: []MetaBlockPointer{
			{BlockID: 0, Offset: 0},
			{BlockID: 1, Offset: 0},
		},
	}

	reader := NewRowGroupReader(bm, rgp, []LogicalTypeID{TypeInteger, TypeBigInt})

	// Manually populate cache for testing
	reader.mu.Lock()
	reader.columnCache[0] = colData1
	reader.columnCache[1] = colData2
	reader.mu.Unlock()

	t.Run("iterate through rows", func(t *testing.T) {
		iter, err := NewRowIterator(reader, []int{0, 1})
		require.NoError(t, err)
		defer iter.Close()

		row := 0
		expected1 := []int32{1, 2, 3, 4, 5}
		expected2 := []int64{10, 20, 30, 40, 50}

		for iter.Next() {
			val1, valid1 := iter.GetValue(0)
			assert.True(t, valid1)
			assert.Equal(t, expected1[row], val1)

			val2, valid2 := iter.GetValue(1)
			assert.True(t, valid2)
			assert.Equal(t, expected2[row], val2)

			iter.Advance()
			row++
		}

		assert.Equal(t, 5, row)
	})

	t.Run("reset iterator", func(t *testing.T) {
		iter, err := NewRowIterator(reader, []int{0})
		require.NoError(t, err)
		defer iter.Close()

		// Read all rows
		for iter.Next() {
			iter.Advance()
		}

		// Reset and read again
		iter.Reset()

		assert.True(t, iter.Next())
		val, valid := iter.GetValue(0)
		assert.True(t, valid)
		assert.Equal(t, int32(1), val)
	})
}


// Helper functions for tests

func intToBytes(val int64, size int) []byte {
	buf := make([]byte, size)
	switch size {
	case 1:
		buf[0] = byte(val)
	case 2:
		binary.LittleEndian.PutUint16(buf, uint16(val))
	case 4:
		binary.LittleEndian.PutUint32(buf, uint32(val))
	case 8:
		binary.LittleEndian.PutUint64(buf, uint64(val))
	}
	return buf
}

func createInt32Array(values []int32) []byte {
	buf := new(bytes.Buffer)
	for _, v := range values {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(v))
		buf.Write(b[:])
	}
	return buf.Bytes()
}

func createInt64Array(values []int64) []byte {
	buf := new(bytes.Buffer)
	for _, v := range values {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(v))
		buf.Write(b[:])
	}
	return buf.Bytes()
}
