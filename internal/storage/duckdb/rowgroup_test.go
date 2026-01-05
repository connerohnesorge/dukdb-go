package duckdb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidityMask_NewValidityMask(t *testing.T) {
	tests := []struct {
		name     string
		rowCount uint64
	}{
		{"zero rows", 0},
		{"one row", 1},
		{"small", 10},
		{"exactly 64", 64},
		{"65 rows", 65},
		{"128 rows", 128},
		{"default row group size", DefaultRowGroupSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mask := NewValidityMask(tt.rowCount)

			assert.NotNil(t, mask)
			assert.Equal(t, tt.rowCount, mask.RowCount())
			assert.True(t, mask.AllValid())
			assert.Equal(t, uint64(0), mask.NullCount())

			// All rows should be valid
			for i := uint64(0); i < tt.rowCount; i++ {
				assert.True(t, mask.IsValid(i), "row %d should be valid", i)
			}
		})
	}
}

func TestValidityMask_SetInvalid(t *testing.T) {
	mask := NewValidityMask(100)

	// Initially all valid
	assert.True(t, mask.AllValid())
	assert.True(t, mask.IsValid(50))

	// Set row 50 as invalid (NULL)
	mask.SetInvalid(50)

	assert.False(t, mask.AllValid())
	assert.False(t, mask.IsValid(50))
	assert.Equal(t, uint64(1), mask.NullCount())

	// Other rows should still be valid
	assert.True(t, mask.IsValid(49))
	assert.True(t, mask.IsValid(51))
}

func TestValidityMask_SetValid(t *testing.T) {
	mask := NewValidityMask(100)

	// Set row 50 as invalid
	mask.SetInvalid(50)
	assert.False(t, mask.IsValid(50))

	// Set it back to valid
	mask.SetValid(50)
	assert.True(t, mask.IsValid(50))
}

func TestValidityMask_MultipleInvalidRows(t *testing.T) {
	mask := NewValidityMask(100)

	// Set multiple rows as invalid
	invalidRows := []uint64{0, 10, 50, 63, 64, 99}
	for _, row := range invalidRows {
		mask.SetInvalid(row)
	}

	assert.False(t, mask.AllValid())
	assert.Equal(t, uint64(len(invalidRows)), mask.NullCount())

	// Check invalid rows
	for _, row := range invalidRows {
		assert.False(t, mask.IsValid(row), "row %d should be invalid", row)
	}

	// Check some valid rows
	validRows := []uint64{1, 9, 11, 49, 51, 62, 65, 98}
	for _, row := range validRows {
		assert.True(t, mask.IsValid(row), "row %d should be valid", row)
	}
}

func TestValidityMask_BoundaryConditions(t *testing.T) {
	mask := NewValidityMask(64)

	// Test first and last rows
	mask.SetInvalid(0)
	mask.SetInvalid(63)

	assert.False(t, mask.IsValid(0))
	assert.False(t, mask.IsValid(63))
	assert.True(t, mask.IsValid(1))
	assert.True(t, mask.IsValid(62))

	// Out of range should return false
	assert.False(t, mask.IsValid(64))
	assert.False(t, mask.IsValid(100))
}

func TestValidityMask_Clone(t *testing.T) {
	original := NewValidityMask(100)
	original.SetInvalid(25)
	original.SetInvalid(75)

	clone := original.Clone()

	// Clone should have same values
	assert.Equal(t, original.RowCount(), clone.RowCount())
	assert.Equal(t, original.AllValid(), clone.AllValid())
	assert.False(t, clone.IsValid(25))
	assert.False(t, clone.IsValid(75))

	// Modifying clone should not affect original
	clone.SetInvalid(50)
	assert.True(t, original.IsValid(50))
	assert.False(t, clone.IsValid(50))
}

func TestValidityMask_SetAllValid(t *testing.T) {
	mask := NewValidityMask(100)
	mask.SetInvalid(25)
	mask.SetInvalid(50)
	mask.SetInvalid(75)

	assert.False(t, mask.AllValid())
	assert.Equal(t, uint64(3), mask.NullCount())

	mask.SetAllValid()

	assert.True(t, mask.AllValid())
	assert.Equal(t, uint64(0), mask.NullCount())
	assert.True(t, mask.IsValid(25))
	assert.True(t, mask.IsValid(50))
	assert.True(t, mask.IsValid(75))
}

func TestValidityMask_Serialization(t *testing.T) {
	tests := []struct {
		name        string
		rowCount    uint64
		invalidRows []uint64
	}{
		{"all valid", 100, nil},
		{"single null", 100, []uint64{50}},
		{"multiple nulls", 100, []uint64{0, 10, 50, 99}},
		{"large row group", DefaultRowGroupSize, []uint64{0, 1000, 50000, DefaultRowGroupSize - 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mask
			mask := NewValidityMask(tt.rowCount)
			for _, row := range tt.invalidRows {
				mask.SetInvalid(row)
			}

			// Serialize
			data, err := mask.SerializeToBytes()
			require.NoError(t, err)
			require.NotNil(t, data)

			// Deserialize
			restored, err := DeserializeValidityMask(data)
			require.NoError(t, err)
			require.NotNil(t, restored)

			// Verify
			assert.Equal(t, mask.RowCount(), restored.RowCount())
			assert.Equal(t, mask.AllValid(), restored.AllValid())
			assert.Equal(t, mask.NullCount(), restored.NullCount())

			// Check all rows
			for i := uint64(0); i < tt.rowCount; i++ {
				assert.Equal(t, mask.IsValid(i), restored.IsValid(i), "row %d mismatch", i)
			}
		})
	}
}

func TestValidityMask_NewValidityMaskFromData(t *testing.T) {
	// Create some data with specific patterns
	data := []uint64{
		^uint64(0),         // All valid
		^uint64(0) ^ (1 << 10), // Row 74 invalid (64 + 10)
	}

	mask := NewValidityMaskFromData(data, 100)

	assert.Equal(t, uint64(100), mask.RowCount())
	assert.False(t, mask.AllValid())
	assert.True(t, mask.IsValid(0))
	assert.True(t, mask.IsValid(63))
	assert.True(t, mask.IsValid(64))
	assert.False(t, mask.IsValid(74))
	assert.True(t, mask.IsValid(99))
}

func TestRowGroupPointer_Basic(t *testing.T) {
	rg := NewRowGroupPointer(42, 1000, 500, 5)

	assert.Equal(t, uint64(42), rg.TableOID)
	assert.Equal(t, uint64(1000), rg.RowStart)
	assert.Equal(t, uint64(500), rg.TupleCount)
	assert.Equal(t, 5, rg.ColumnCount())
	assert.False(t, rg.IsEmpty())
	assert.Equal(t, uint64(1500), rg.RowEnd())
}

func TestRowGroupPointer_ContainsRow(t *testing.T) {
	rg := NewRowGroupPointer(1, 100, 50, 3)

	// Inside range
	assert.True(t, rg.ContainsRow(100))
	assert.True(t, rg.ContainsRow(125))
	assert.True(t, rg.ContainsRow(149))

	// Outside range
	assert.False(t, rg.ContainsRow(99))
	assert.False(t, rg.ContainsRow(150))
	assert.False(t, rg.ContainsRow(0))
}

func TestRowGroupPointer_Serialization(t *testing.T) {
	rg := &RowGroupPointer{
		TableOID:   123,
		RowStart:   1000,
		TupleCount: 500,
		DataPointers: []MetaBlockPointer{
			{BlockID: 10, Offset: 100},
			{BlockID: 11, Offset: 200},
			{BlockID: 12, Offset: 300},
		},
	}

	// Serialize
	data, err := rg.SerializeToBytes()
	require.NoError(t, err)
	require.NotNil(t, data)

	// Deserialize
	restored, err := DeserializeRowGroupPointer(data)
	require.NoError(t, err)
	require.NotNil(t, restored)

	// Verify
	assert.Equal(t, rg.TableOID, restored.TableOID)
	assert.Equal(t, rg.RowStart, restored.RowStart)
	assert.Equal(t, rg.TupleCount, restored.TupleCount)
	assert.Equal(t, len(rg.DataPointers), len(restored.DataPointers))

	for i, dp := range rg.DataPointers {
		assert.Equal(t, dp.BlockID, restored.DataPointers[i].BlockID)
		assert.Equal(t, dp.Offset, restored.DataPointers[i].Offset)
	}
}

func TestDataPointer_Basic(t *testing.T) {
	block := BlockPointer{BlockID: 5, Offset: 1024}
	dp := NewDataPointer(0, 1000, block, CompressionRLE)

	assert.Equal(t, uint64(0), dp.RowStart)
	assert.Equal(t, uint64(1000), dp.TupleCount)
	assert.Equal(t, uint64(5), dp.Block.BlockID)
	assert.Equal(t, uint32(1024), dp.Block.Offset)
	assert.Equal(t, CompressionRLE, dp.Compression)
	assert.False(t, dp.IsEmpty())
	assert.False(t, dp.HasStatistics())
	assert.False(t, dp.HasNulls())
}

func TestDataPointer_WithStatistics(t *testing.T) {
	dp := &DataPointer{
		RowStart:    0,
		TupleCount:  1000,
		Block:       BlockPointer{BlockID: 5, Offset: 100},
		Compression: CompressionDictionary,
		Statistics: BaseStatistics{
			HasStats:      true,
			HasNull:       true,
			NullCount:     50,
			DistinctCount: 100,
		},
		SegmentState: ColumnSegmentState{
			HasValidityMask: true,
			ValidityBlock:   BlockPointer{BlockID: 6, Offset: 0},
		},
	}

	assert.True(t, dp.HasStatistics())
	assert.True(t, dp.HasNulls())
}

func TestDataPointer_Serialization(t *testing.T) {
	dp := &DataPointer{
		RowStart:    100,
		TupleCount:  500,
		Block:       BlockPointer{BlockID: 10, Offset: 2048},
		Compression: CompressionBitPacking,
		Statistics: BaseStatistics{
			HasStats:      true,
			HasNull:       true,
			NullCount:     25,
			DistinctCount: 50,
			StatData:      []byte{1, 2, 3, 4},
		},
		SegmentState: ColumnSegmentState{
			HasValidityMask: true,
			ValidityBlock:   BlockPointer{BlockID: 11, Offset: 512},
			StateData:       []byte{5, 6, 7},
		},
	}

	// Serialize
	data, err := dp.SerializeToBytes()
	require.NoError(t, err)
	require.NotNil(t, data)

	// Deserialize
	restored, err := DeserializeDataPointer(data)
	require.NoError(t, err)
	require.NotNil(t, restored)

	// Verify
	assert.Equal(t, dp.RowStart, restored.RowStart)
	assert.Equal(t, dp.TupleCount, restored.TupleCount)
	assert.Equal(t, dp.Block.BlockID, restored.Block.BlockID)
	assert.Equal(t, dp.Block.Offset, restored.Block.Offset)
	assert.Equal(t, dp.Compression, restored.Compression)

	// Statistics
	assert.Equal(t, dp.Statistics.HasStats, restored.Statistics.HasStats)
	assert.Equal(t, dp.Statistics.HasNull, restored.Statistics.HasNull)
	assert.Equal(t, dp.Statistics.NullCount, restored.Statistics.NullCount)
	assert.Equal(t, dp.Statistics.DistinctCount, restored.Statistics.DistinctCount)
	assert.Equal(t, dp.Statistics.StatData, restored.Statistics.StatData)

	// Segment state
	assert.Equal(t, dp.SegmentState.HasValidityMask, restored.SegmentState.HasValidityMask)
	assert.Equal(t, dp.SegmentState.ValidityBlock.BlockID, restored.SegmentState.ValidityBlock.BlockID)
	assert.Equal(t, dp.SegmentState.ValidityBlock.Offset, restored.SegmentState.ValidityBlock.Offset)
	assert.Equal(t, dp.SegmentState.StateData, restored.SegmentState.StateData)
}

func TestDataPointer_SerializationNoStats(t *testing.T) {
	dp := &DataPointer{
		RowStart:    0,
		TupleCount:  100,
		Block:       BlockPointer{BlockID: 1, Offset: 0},
		Compression: CompressionUncompressed,
		Statistics: BaseStatistics{
			HasStats: false,
		},
		SegmentState: ColumnSegmentState{
			HasValidityMask: false,
		},
	}

	// Serialize
	data, err := dp.SerializeToBytes()
	require.NoError(t, err)

	// Deserialize
	restored, err := DeserializeDataPointer(data)
	require.NoError(t, err)

	assert.Equal(t, dp.Statistics.HasStats, restored.Statistics.HasStats)
	assert.Equal(t, dp.SegmentState.HasValidityMask, restored.SegmentState.HasValidityMask)
}

func TestBaseStatistics_Serialization(t *testing.T) {
	tests := []struct {
		name  string
		stats BaseStatistics
	}{
		{
			name: "no stats",
			stats: BaseStatistics{
				HasStats: false,
			},
		},
		{
			name: "with stats no nulls",
			stats: BaseStatistics{
				HasStats:      true,
				HasNull:       false,
				NullCount:     0,
				DistinctCount: 100,
			},
		},
		{
			name: "with stats and nulls",
			stats: BaseStatistics{
				HasStats:      true,
				HasNull:       true,
				NullCount:     50,
				DistinctCount: 75,
				StatData:      []byte{1, 2, 3, 4, 5},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewBinaryWriter(&buf)

			err := tt.stats.Serialize(w)
			require.NoError(t, err)

			r := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			var restored BaseStatistics
			err = restored.Deserialize(r)
			require.NoError(t, err)

			assert.Equal(t, tt.stats.HasStats, restored.HasStats)
			if !tt.stats.HasStats {
				return
			}

			assert.Equal(t, tt.stats.HasNull, restored.HasNull)
			assert.Equal(t, tt.stats.NullCount, restored.NullCount)
			assert.Equal(t, tt.stats.DistinctCount, restored.DistinctCount)
			assert.Equal(t, tt.stats.StatData, restored.StatData)
		})
	}
}

func TestColumnSegmentState_Serialization(t *testing.T) {
	tests := []struct {
		name  string
		state ColumnSegmentState
	}{
		{
			name: "no validity mask",
			state: ColumnSegmentState{
				HasValidityMask: false,
			},
		},
		{
			name: "with validity mask",
			state: ColumnSegmentState{
				HasValidityMask: true,
				ValidityBlock:   BlockPointer{BlockID: 10, Offset: 512},
			},
		},
		{
			name: "with state data",
			state: ColumnSegmentState{
				HasValidityMask: true,
				ValidityBlock:   BlockPointer{BlockID: 20, Offset: 1024},
				StateData:       []byte{1, 2, 3, 4, 5, 6, 7, 8},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewBinaryWriter(&buf)

			err := tt.state.Serialize(w)
			require.NoError(t, err)

			r := NewBinaryReader(bytes.NewReader(buf.Bytes()))
			var restored ColumnSegmentState
			err = restored.Deserialize(r)
			require.NoError(t, err)

			assert.Equal(t, tt.state.HasValidityMask, restored.HasValidityMask)
			if tt.state.HasValidityMask {
				assert.Equal(t, tt.state.ValidityBlock.BlockID, restored.ValidityBlock.BlockID)
				assert.Equal(t, tt.state.ValidityBlock.Offset, restored.ValidityBlock.Offset)
			}
			assert.Equal(t, tt.state.StateData, restored.StateData)
		})
	}
}

func TestStringStatistics_Serialization(t *testing.T) {
	tests := []struct {
		name  string
		stats StringStatistics
	}{
		{
			name: "no stats",
			stats: StringStatistics{
				HasStats: false,
			},
		},
		{
			name: "varchar stats",
			stats: StringStatistics{
				HasStats:  true,
				MinLen:    0,
				MaxLen:    100,
				HasMaxLen: false,
			},
		},
		{
			name: "char stats with max",
			stats: StringStatistics{
				HasStats:  true,
				MinLen:    10,
				MaxLen:    10,
				HasMaxLen: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.stats.SerializeToBytes()
			require.NoError(t, err)

			restored, err := DeserializeStringStatistics(data)
			require.NoError(t, err)

			assert.Equal(t, tt.stats.HasStats, restored.HasStats)
			if !tt.stats.HasStats {
				return
			}

			assert.Equal(t, tt.stats.MinLen, restored.MinLen)
			assert.Equal(t, tt.stats.MaxLen, restored.MaxLen)
			assert.Equal(t, tt.stats.HasMaxLen, restored.HasMaxLen)
		})
	}
}

func TestNumericStatistics_Basic(t *testing.T) {
	stats := NewNumericStatistics(int64(10), int64(100))

	assert.True(t, stats.HasMin)
	assert.True(t, stats.HasMax)
	assert.Equal(t, int64(10), stats.Min)
	assert.Equal(t, int64(100), stats.Max)

	// Test with nil values
	nilStats := NewNumericStatistics(nil, nil)
	assert.False(t, nilStats.HasMin)
	assert.False(t, nilStats.HasMax)
}

func TestNewBaseStatistics(t *testing.T) {
	stats := NewBaseStatistics(true, 50, 100)

	assert.True(t, stats.HasStats)
	assert.True(t, stats.HasNull)
	assert.Equal(t, uint64(50), stats.NullCount)
	assert.Equal(t, uint64(100), stats.DistinctCount)
}

func TestNewColumnSegmentState(t *testing.T) {
	validityBlock := BlockPointer{BlockID: 5, Offset: 100}
	state := NewColumnSegmentState(true, validityBlock)

	assert.True(t, state.HasValidityMask)
	assert.Equal(t, uint64(5), state.ValidityBlock.BlockID)
	assert.Equal(t, uint32(100), state.ValidityBlock.Offset)
}

func TestNewStringStatistics(t *testing.T) {
	stats := NewStringStatistics(5, 50, false)

	assert.True(t, stats.HasStats)
	assert.Equal(t, uint32(5), stats.MinLen)
	assert.Equal(t, uint32(50), stats.MaxLen)
	assert.False(t, stats.HasMaxLen)
}

func TestDefaultRowGroupSizeConstant(t *testing.T) {
	// Verify the default row group size matches DuckDB's default
	assert.Equal(t, uint64(122880), DefaultRowGroupSize)
}

func TestRowGroupPointer_EmptyRowGroup(t *testing.T) {
	rg := NewRowGroupPointer(1, 0, 0, 3)

	assert.True(t, rg.IsEmpty())
	assert.Equal(t, uint64(0), rg.RowEnd())
	assert.False(t, rg.ContainsRow(0))
}

func TestDataPointer_EmptyPointer(t *testing.T) {
	dp := NewDataPointer(0, 0, BlockPointer{}, CompressionUncompressed)

	assert.True(t, dp.IsEmpty())
}

func BenchmarkValidityMask_IsValid(b *testing.B) {
	mask := NewValidityMask(DefaultRowGroupSize)
	// Set some random rows as invalid
	for i := uint64(0); i < 1000; i++ {
		mask.SetInvalid(i * 100)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mask.IsValid(uint64(i) % DefaultRowGroupSize)
	}
}

func BenchmarkValidityMask_SetInvalid(b *testing.B) {
	mask := NewValidityMask(DefaultRowGroupSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mask.SetInvalid(uint64(i) % DefaultRowGroupSize)
	}
}

func BenchmarkValidityMask_NullCount(b *testing.B) {
	mask := NewValidityMask(DefaultRowGroupSize)
	// Set some random rows as invalid
	for i := uint64(0); i < 1000; i++ {
		mask.SetInvalid(i * 100)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mask.NullCount()
	}
}
