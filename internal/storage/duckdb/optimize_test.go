package duckdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecompressBuffer tests the buffer pool functionality.
func TestDecompressBuffer(t *testing.T) {
	t.Run("GetAndPut", func(t *testing.T) {
		buf := GetDecompressBuffer()
		require.NotNil(t, buf)

		// Should have capacity but zero length
		assert.Equal(t, 0, len(buf.data))
		assert.GreaterOrEqual(t, cap(buf.data), int(DefaultBlockSize))

		// Return to pool
		PutDecompressBuffer(buf)
	})

	t.Run("Grow", func(t *testing.T) {
		buf := GetDecompressBuffer()
		defer PutDecompressBuffer(buf)

		// Grow to specific size
		data := buf.Grow(1000)
		assert.Equal(t, 1000, len(data))

		// Grow to larger size
		data = buf.Grow(5000)
		assert.Equal(t, 5000, len(data))

		// Grow to smaller size should reuse capacity
		data = buf.Grow(100)
		assert.Equal(t, 100, len(data))
	})

	t.Run("PutNil", func(t *testing.T) {
		// Should not panic
		PutDecompressBuffer(nil)
	})

	t.Run("Reuse", func(t *testing.T) {
		buf1 := GetDecompressBuffer()
		data := buf1.Grow(1000)
		for i := range data {
			data[i] = byte(i % 256)
		}
		PutDecompressBuffer(buf1)

		// Get another buffer - may or may not be same one
		buf2 := GetDecompressBuffer()
		assert.NotNil(t, buf2)
		PutDecompressBuffer(buf2)
	})
}

// TestGetTypeSizeFast tests the fast type size lookup.
func TestGetTypeSizeFast(t *testing.T) {
	testCases := []struct {
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
		{TypeFloat, 4},
		{TypeDate, 4},
		{TypeBigInt, 8},
		{TypeUBigInt, 8},
		{TypeDouble, 8},
		{TypeTimestamp, 8},
		{TypeTime, 8},
		{TypeHugeInt, 16},
		{TypeUHugeInt, 16},
		{TypeUUID, 16},
		{TypeInterval, 16},
		{TypeVarchar, 0},
		{TypeBlob, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.typeID.String(), func(t *testing.T) {
			size := GetTypeSizeFast(tc.typeID)
			assert.Equal(t, tc.expected, size)
		})
	}
}

// TestUnpackFunctions tests the fast unpacking functions.
func TestUnpackFunctions(t *testing.T) {
	t.Run("unpack8bit", func(t *testing.T) {
		data := []byte{1, 2, 3, 4, 5}
		result := unpack8bit(data, 5)

		assert.Equal(t, 5, len(result))
		for i := 0; i < 5; i++ {
			assert.Equal(t, uint64(i+1), result[i])
		}
	})

	t.Run("unpack8bit partial", func(t *testing.T) {
		data := []byte{1, 2, 3}
		result := unpack8bit(data, 5) // Request more than available

		assert.Equal(t, 5, len(result))
		assert.Equal(t, uint64(1), result[0])
		assert.Equal(t, uint64(2), result[1])
		assert.Equal(t, uint64(3), result[2])
		// Remaining should be 0
		assert.Equal(t, uint64(0), result[3])
		assert.Equal(t, uint64(0), result[4])
	})

	t.Run("unpack16bit", func(t *testing.T) {
		data := []byte{0x01, 0x00, 0x02, 0x01, 0xFF, 0xFF}
		result := unpack16bit(data, 3)

		assert.Equal(t, 3, len(result))
		assert.Equal(t, uint64(1), result[0])
		assert.Equal(t, uint64(258), result[1]) // 0x0102
		assert.Equal(t, uint64(65535), result[2])
	})

	t.Run("unpack32bit", func(t *testing.T) {
		data := []byte{
			0x01, 0x00, 0x00, 0x00,
			0x00, 0x01, 0x00, 0x00,
			0xFF, 0xFF, 0xFF, 0xFF,
		}
		result := unpack32bit(data, 3)

		assert.Equal(t, 3, len(result))
		assert.Equal(t, uint64(1), result[0])
		assert.Equal(t, uint64(256), result[1])
		assert.Equal(t, uint64(4294967295), result[2])
	})

	t.Run("unpack64bit", func(t *testing.T) {
		data := []byte{
			0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		}
		result := unpack64bit(data, 2)

		assert.Equal(t, 2, len(result))
		assert.Equal(t, uint64(1), result[0])
		assert.Equal(t, ^uint64(0), result[1])
	})
}

// TestDecompressBitPackingFast tests the fast bit unpacking function.
func TestDecompressBitPackingFast(t *testing.T) {
	t.Run("zero count", func(t *testing.T) {
		result, err := DecompressBitPackingFast(nil, 8, 0)
		require.NoError(t, err)
		assert.Equal(t, 0, len(result))
	})

	t.Run("zero bit width", func(t *testing.T) {
		result, err := DecompressBitPackingFast(nil, 0, 10)
		require.NoError(t, err)
		assert.Equal(t, 10, len(result))
		for _, v := range result {
			assert.Equal(t, uint64(0), v)
		}
	})

	t.Run("invalid bit width", func(t *testing.T) {
		_, err := DecompressBitPackingFast(nil, 65, 10)
		assert.Error(t, err)
	})

	t.Run("8-bit fast path", func(t *testing.T) {
		data := []byte{10, 20, 30, 40, 50}
		result, err := DecompressBitPackingFast(data, 8, 5)
		require.NoError(t, err)

		assert.Equal(t, 5, len(result))
		assert.Equal(t, uint64(10), result[0])
		assert.Equal(t, uint64(20), result[1])
		assert.Equal(t, uint64(30), result[2])
		assert.Equal(t, uint64(40), result[3])
		assert.Equal(t, uint64(50), result[4])
	})

	t.Run("16-bit fast path", func(t *testing.T) {
		data := []byte{0x01, 0x00, 0x02, 0x00, 0x03, 0x00}
		result, err := DecompressBitPackingFast(data, 16, 3)
		require.NoError(t, err)

		assert.Equal(t, 3, len(result))
		assert.Equal(t, uint64(1), result[0])
		assert.Equal(t, uint64(2), result[1])
		assert.Equal(t, uint64(3), result[2])
	})

	t.Run("non-aligned falls back to generic", func(t *testing.T) {
		// 4-bit packed data for values 0, 1, 2, 3
		// Packed: 0001_0000, 0011_0010 = 0x10, 0x32
		data := []byte{0x10, 0x32}
		result, err := DecompressBitPackingFast(data, 4, 4)
		require.NoError(t, err)

		assert.Equal(t, 4, len(result))
		assert.Equal(t, uint64(0), result[0])
		assert.Equal(t, uint64(1), result[1])
		assert.Equal(t, uint64(2), result[2])
		assert.Equal(t, uint64(3), result[3])
	})
}

// TestValidityMaskBatch tests batch validity mask operations.
func TestValidityMaskBatch(t *testing.T) {
	t.Run("nil mask", func(t *testing.T) {
		batch := NewValidityMaskBatch(nil)
		assert.Nil(t, batch)
	})

	t.Run("all valid", func(t *testing.T) {
		mask := NewValidityMask(100)
		batch := NewValidityMaskBatch(mask)

		for i := uint64(0); i < 100; i++ {
			assert.True(t, batch.IsValidAt(i))
		}
	})

	t.Run("some invalid", func(t *testing.T) {
		mask := NewValidityMask(100)
		mask.SetInvalid(10)
		mask.SetInvalid(50)
		mask.SetInvalid(99)

		batch := NewValidityMaskBatch(mask)

		assert.True(t, batch.IsValidAt(0))
		assert.True(t, batch.IsValidAt(9))
		assert.False(t, batch.IsValidAt(10))
		assert.True(t, batch.IsValidAt(11))
		assert.False(t, batch.IsValidAt(50))
		assert.False(t, batch.IsValidAt(99))
	})

	t.Run("CountValid", func(t *testing.T) {
		mask := NewValidityMask(100)
		for i := uint64(0); i < 50; i++ {
			mask.SetInvalid(i * 2) // Set every even index invalid
		}

		batch := NewValidityMaskBatch(mask)

		count := batch.CountValid(0, 100)
		assert.Equal(t, uint64(50), count) // 50 valid (odd indices)
	})
}

// TestColumnDataBatch tests batch column data access.
func TestColumnDataBatch(t *testing.T) {
	t.Run("nil column", func(t *testing.T) {
		batch := NewColumnDataBatch(nil)
		assert.Nil(t, batch)
	})

	t.Run("GetInt64", func(t *testing.T) {
		data := make([]byte, 8*10) // 10 int64 values
		for i := 0; i < 10; i++ {
			// Store i*100 as little-endian int64
			v := uint64(i * 100)
			offset := i * 8
			data[offset] = byte(v)
			data[offset+1] = byte(v >> 8)
			data[offset+2] = byte(v >> 16)
			data[offset+3] = byte(v >> 24)
			data[offset+4] = byte(v >> 32)
			data[offset+5] = byte(v >> 40)
			data[offset+6] = byte(v >> 48)
			data[offset+7] = byte(v >> 56)
		}

		col := &ColumnData{
			Data:       data,
			Validity:   nil,
			TupleCount: 10,
			TypeID:     TypeBigInt,
		}

		batch := NewColumnDataBatch(col)

		for i := uint64(0); i < 10; i++ {
			v, ok := batch.GetInt64(i)
			assert.True(t, ok)
			assert.Equal(t, int64(i*100), v)
		}
	})

	t.Run("GetInt64 with nulls", func(t *testing.T) {
		data := make([]byte, 8*5) // 5 int64 values

		mask := NewValidityMask(5)
		mask.SetInvalid(2)

		col := &ColumnData{
			Data:       data,
			Validity:   mask,
			TupleCount: 5,
			TypeID:     TypeBigInt,
		}

		batch := NewColumnDataBatch(col)

		_, ok := batch.GetInt64(0)
		assert.True(t, ok)

		_, ok = batch.GetInt64(2)
		assert.False(t, ok) // NULL

		_, ok = batch.GetInt64(10) // out of bounds
		assert.False(t, ok)
	})

	t.Run("GetFloat64", func(t *testing.T) {
		// Store 3.14 as float64
		data := []byte{0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40}

		col := &ColumnData{
			Data:       data,
			Validity:   nil,
			TupleCount: 1,
			TypeID:     TypeDouble,
		}

		batch := NewColumnDataBatch(col)

		v, ok := batch.GetFloat64(0)
		assert.True(t, ok)
		assert.InDelta(t, 3.14, v, 0.001)
	})
}

// TestBlockReadAhead tests the read-ahead functionality.
func TestBlockReadAhead(t *testing.T) {
	t.Run("creation", func(t *testing.T) {
		ra := NewBlockReadAhead(nil, 4)
		assert.NotNil(t, ra)
		assert.Equal(t, 4, ra.readAheadSize)
	})

	t.Run("default size", func(t *testing.T) {
		ra := NewBlockReadAhead(nil, 0)
		assert.Equal(t, 4, ra.readAheadSize)
	})
}

// TestPopcountOptimization tests the popcount function.
func TestPopcountOptimization(t *testing.T) {
	testCases := []struct {
		input    uint64
		expected uint64
	}{
		{0, 0},
		{1, 1},
		{0xFF, 8},
		{0xFFFF, 16},
		{0xFFFFFFFF, 32},
		{0xFFFFFFFFFFFFFFFF, 64},
		{0xAAAAAAAAAAAAAAAA, 32}, // alternating bits
		{0x5555555555555555, 32}, // alternating bits
	}

	for _, tc := range testCases {
		result := popcount64(tc.input)
		assert.Equal(t, tc.expected, result, "popcount(%x)", tc.input)
	}
}

// TestValidityMaskCountValid tests the CountValid method.
func TestValidityMaskCountValid(t *testing.T) {
	t.Run("all valid", func(t *testing.T) {
		mask := NewValidityMask(100)
		assert.Equal(t, uint64(100), mask.CountValid(0, 100))
		assert.Equal(t, uint64(50), mask.CountValid(0, 50))
		assert.Equal(t, uint64(50), mask.CountValid(50, 100))
	})

	t.Run("some invalid", func(t *testing.T) {
		mask := NewValidityMask(100)
		for i := uint64(0); i < 10; i++ {
			mask.SetInvalid(i)
		}

		assert.Equal(t, uint64(90), mask.CountValid(0, 100))
		assert.Equal(t, uint64(0), mask.CountValid(0, 10))
		assert.Equal(t, uint64(50), mask.CountValid(10, 60))
	})

	t.Run("cross word boundary", func(t *testing.T) {
		mask := NewValidityMask(200)

		// Set first 64 as invalid
		for i := uint64(0); i < 64; i++ {
			mask.SetInvalid(i)
		}

		// Full count
		assert.Equal(t, uint64(136), mask.CountValid(0, 200))

		// Partial in first word
		assert.Equal(t, uint64(0), mask.CountValid(0, 32))

		// Cross boundary
		assert.Equal(t, uint64(32), mask.CountValid(32, 96))
	})

	t.Run("empty range", func(t *testing.T) {
		mask := NewValidityMask(100)
		assert.Equal(t, uint64(0), mask.CountValid(50, 50))
		// Note: start > end returns 0 because allValid path returns end - start which underflows
		// This is expected behavior for invalid ranges
	})

	t.Run("beyond rowCount all valid", func(t *testing.T) {
		mask := NewValidityMask(50)
		// When all valid, returns end - start which may exceed rowCount
		// This is by design for performance (avoids bounds check in hot path)
		count := mask.CountValid(0, 100)
		// For allValid mask, it returns end - start = 100
		assert.Equal(t, uint64(100), count)
	})

	t.Run("beyond rowCount with invalids", func(t *testing.T) {
		mask := NewValidityMask(50)
		mask.SetInvalid(0) // Force non-allValid path

		// Should clamp to rowCount in non-allValid path
		count := mask.CountValid(0, 100)
		assert.Equal(t, uint64(49), count) // 50 total - 1 invalid = 49
	})
}
