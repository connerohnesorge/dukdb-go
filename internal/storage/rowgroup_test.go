package storage

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/compression"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDuckDBRowGroup(t *testing.T) {
	rg := NewDuckDBRowGroup(3)

	assert.NotNil(t, rg)
	assert.Equal(t, uint16(3), rg.MetaData.ColumnCount)
	assert.Equal(t, uint64(0), rg.MetaData.RowCount)
	assert.Equal(t, uint32(DuckDBRowGroupFlagNone), rg.MetaData.Flags)
	assert.Len(t, rg.ColumnData, 3)
}

func TestNewDuckDBColumnSegment(t *testing.T) {
	segment := NewDuckDBColumnSegment(compression.LogicalTypeInteger)

	assert.NotNil(t, segment)
	assert.Equal(t, compression.LogicalTypeInteger, segment.MetaData.Type)
	assert.Equal(t, compression.CompressionNone, segment.Compression)
	assert.Nil(t, segment.Data)
	assert.Nil(t, segment.Validity)
}

func TestDuckDBColumnSegment_SetData(t *testing.T) {
	segment := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	data := []byte{1, 2, 3, 4, 5}

	segment.SetData(data, false)

	assert.Equal(t, data, segment.Data)
	assert.Equal(t, uint64(5), segment.MetaData.Length)
	assert.Equal(t, uint64(5), segment.MetaData.SegmentSize)
}

func TestDuckDBColumnSegment_SetDataCompressed(t *testing.T) {
	segment := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	data := []byte{1, 2, 3}

	segment.SetData(data, true)

	assert.Equal(t, data, segment.Data)
	assert.Equal(t, uint64(3), segment.MetaData.Length)
	// SegmentSize should be set separately for compressed data
}

func TestDuckDBColumnSegment_Validity(t *testing.T) {
	segment := NewDuckDBColumnSegment(compression.LogicalTypeInteger)

	// Initially no validity bitmap, all valid
	assert.True(t, segment.IsValid(0))
	assert.True(t, segment.IsValid(10))

	// Set invalid
	segment.SetInvalid(5)
	assert.False(t, segment.IsValid(5))
	assert.True(t, segment.IsValid(4))
	assert.True(t, segment.IsValid(6))

	// Set valid again
	segment.SetValid(5)
	assert.True(t, segment.IsValid(5))

	// Test multiple invalids
	segment.SetInvalid(0)
	segment.SetInvalid(63)
	segment.SetInvalid(64)
	segment.SetInvalid(100)

	assert.False(t, segment.IsValid(0))
	assert.False(t, segment.IsValid(63))
	assert.False(t, segment.IsValid(64))
	assert.False(t, segment.IsValid(100))
	assert.True(t, segment.IsValid(1))
	assert.True(t, segment.IsValid(62))
	assert.True(t, segment.IsValid(65))
}

func TestDuckDBRowGroup_AddAndGetColumn(t *testing.T) {
	rg := NewDuckDBRowGroup(2)

	segment1 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	segment2 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)

	rg.AddColumn(0, segment1)
	rg.AddColumn(1, segment2)

	assert.Equal(t, segment1, rg.GetColumn(0))
	assert.Equal(t, segment2, rg.GetColumn(1))
	assert.Nil(t, rg.GetColumn(2))
	assert.Nil(t, rg.GetColumn(-1))
}

func TestDuckDBRowGroup_SetRowCount(t *testing.T) {
	rg := NewDuckDBRowGroup(1)

	rg.SetRowCount(100)

	assert.Equal(t, uint64(100), rg.MetaData.RowCount)
}

func TestDuckDBRowGroup_SetStartId(t *testing.T) {
	rg := NewDuckDBRowGroup(1)

	rg.SetStartId(42)

	assert.Equal(t, uint64(42), rg.MetaData.StartId)
}

func TestDuckDBRowGroup_Flags(t *testing.T) {
	rg := NewDuckDBRowGroup(1)

	// Initially no flags
	assert.False(t, rg.HasFlag(DuckDBRowGroupFlagSorted))
	assert.False(t, rg.HasFlag(DuckDBRowGroupFlagCompressed))

	// Set sorted flag
	rg.SetFlags(DuckDBRowGroupFlagSorted)
	assert.True(t, rg.HasFlag(DuckDBRowGroupFlagSorted))
	assert.False(t, rg.HasFlag(DuckDBRowGroupFlagCompressed))

	// Set multiple flags
	rg.SetFlags(DuckDBRowGroupFlagSorted | DuckDBRowGroupFlagCompressed)
	assert.True(t, rg.HasFlag(DuckDBRowGroupFlagSorted))
	assert.True(t, rg.HasFlag(DuckDBRowGroupFlagCompressed))
}

func TestDataChunk_ToDuckDBRowGroup(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := NewDataChunk(types)

	// Add some data
	chunk.AppendRow([]any{int32(1), "hello"})
	chunk.AppendRow([]any{int32(2), "world"})
	chunk.AppendRow([]any{nil, "test"})

	// Convert to DuckDB row group
	rg := chunk.ToDuckDBRowGroup()

	require.NotNil(t, rg)
	assert.Equal(t, uint16(2), rg.MetaData.ColumnCount)
	assert.Equal(t, uint64(3), rg.MetaData.RowCount)

	// Check column 0 (INTEGER)
	col0 := rg.GetColumn(0)
	require.NotNil(t, col0)
	assert.Equal(t, compression.LogicalTypeInteger, col0.MetaData.Type)
	assert.True(t, col0.IsValid(0))
	assert.True(t, col0.IsValid(1))
	assert.False(t, col0.IsValid(2)) // nil value

	// Check column 1 (VARCHAR)
	col1 := rg.GetColumn(1)
	require.NotNil(t, col1)
	assert.Equal(t, compression.LogicalTypeVarchar, col1.MetaData.Type)
	assert.True(t, col1.IsValid(0))
	assert.True(t, col1.IsValid(1))
	assert.True(t, col1.IsValid(2))
}

func TestFromDuckDBRowGroup(t *testing.T) {
	// Create a DuckDB row group
	rg := NewDuckDBRowGroup(2)
	rg.SetRowCount(3)

	// Add column segments
	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col0.SetInvalid(2) // Mark row 2 as NULL
	rg.AddColumn(0, col0)

	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	rg.AddColumn(1, col1)

	// Convert to DataChunk
	chunk := FromDuckDBRowGroup(rg)

	require.NotNil(t, chunk)
	assert.Equal(t, 2, chunk.ColumnCount())
	assert.Equal(t, 3, chunk.Count())

	// Check types
	types := chunk.Types()
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0])
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1])

	// Check validity
	vec0 := chunk.GetVector(0)
	require.NotNil(t, vec0)
	assert.True(t, vec0.Validity().IsValid(0))
	assert.True(t, vec0.Validity().IsValid(1))
	assert.False(t, vec0.Validity().IsValid(2)) // NULL value
}

func TestMapTypeToLogicalTypeID(t *testing.T) {
	tests := []struct {
		name     string
		dukdbType dukdb.Type
		expected  compression.LogicalTypeID
	}{
		{"Boolean", dukdb.TYPE_BOOLEAN, compression.LogicalTypeBoolean},
		{"TinyInt", dukdb.TYPE_TINYINT, compression.LogicalTypeTinyInt},
		{"SmallInt", dukdb.TYPE_SMALLINT, compression.LogicalTypeSmallInt},
		{"Integer", dukdb.TYPE_INTEGER, compression.LogicalTypeInteger},
		{"BigInt", dukdb.TYPE_BIGINT, compression.LogicalTypeBigInt},
		{"UTinyInt", dukdb.TYPE_UTINYINT, compression.LogicalTypeUTinyInt},
		{"USmallInt", dukdb.TYPE_USMALLINT, compression.LogicalTypeUSmallInt},
		{"UInteger", dukdb.TYPE_UINTEGER, compression.LogicalTypeUInteger},
		{"UBigInt", dukdb.TYPE_UBIGINT, compression.LogicalTypeUBigInt},
		{"Float", dukdb.TYPE_FLOAT, compression.LogicalTypeFloat},
		{"Double", dukdb.TYPE_DOUBLE, compression.LogicalTypeDouble},
		{"Varchar", dukdb.TYPE_VARCHAR, compression.LogicalTypeVarchar},
		{"Blob", dukdb.TYPE_BLOB, compression.LogicalTypeBlob},
		{"Timestamp", dukdb.TYPE_TIMESTAMP, compression.LogicalTypeTimestamp},
		{"Date", dukdb.TYPE_DATE, compression.LogicalTypeDate},
		{"Time", dukdb.TYPE_TIME, compression.LogicalTypeTime},
		{"Interval", dukdb.TYPE_INTERVAL, compression.LogicalTypeInterval},
		{"Decimal", dukdb.TYPE_DECIMAL, compression.LogicalTypeDecimal},
		{"Invalid", dukdb.TYPE_INVALID, compression.LogicalTypeInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapTypeToLogicalTypeID(tt.dukdbType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMapLogicalTypeIDToType(t *testing.T) {
	tests := []struct {
		name     string
		typeID   compression.LogicalTypeID
		expected dukdb.Type
	}{
		{"Boolean", compression.LogicalTypeBoolean, dukdb.TYPE_BOOLEAN},
		{"TinyInt", compression.LogicalTypeTinyInt, dukdb.TYPE_TINYINT},
		{"SmallInt", compression.LogicalTypeSmallInt, dukdb.TYPE_SMALLINT},
		{"Integer", compression.LogicalTypeInteger, dukdb.TYPE_INTEGER},
		{"BigInt", compression.LogicalTypeBigInt, dukdb.TYPE_BIGINT},
		{"UTinyInt", compression.LogicalTypeUTinyInt, dukdb.TYPE_UTINYINT},
		{"USmallInt", compression.LogicalTypeUSmallInt, dukdb.TYPE_USMALLINT},
		{"UInteger", compression.LogicalTypeUInteger, dukdb.TYPE_UINTEGER},
		{"UBigInt", compression.LogicalTypeUBigInt, dukdb.TYPE_UBIGINT},
		{"Float", compression.LogicalTypeFloat, dukdb.TYPE_FLOAT},
		{"Double", compression.LogicalTypeDouble, dukdb.TYPE_DOUBLE},
		{"Varchar", compression.LogicalTypeVarchar, dukdb.TYPE_VARCHAR},
		{"Char", compression.LogicalTypeChar, dukdb.TYPE_VARCHAR},
		{"Blob", compression.LogicalTypeBlob, dukdb.TYPE_BLOB},
		{"Timestamp", compression.LogicalTypeTimestamp, dukdb.TYPE_TIMESTAMP},
		{"Date", compression.LogicalTypeDate, dukdb.TYPE_DATE},
		{"Time", compression.LogicalTypeTime, dukdb.TYPE_TIME},
		{"Interval", compression.LogicalTypeInterval, dukdb.TYPE_INTERVAL},
		{"Decimal", compression.LogicalTypeDecimal, dukdb.TYPE_DECIMAL},
		{"Invalid", compression.LogicalTypeInvalid, dukdb.TYPE_INVALID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapLogicalTypeIDToType(tt.typeID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRoundTrip_DataChunkToDuckDBRowGroupToDataChunk(t *testing.T) {
	// Create original data chunk
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_BOOLEAN,
	}
	chunk := NewDataChunk(types)

	chunk.AppendRow([]any{int32(1), "hello", float64(1.5), true})
	chunk.AppendRow([]any{int32(2), "world", float64(2.5), false})
	chunk.AppendRow([]any{nil, "test", float64(3.5), true})
	chunk.AppendRow([]any{int32(4), nil, nil, false})

	// Convert to DuckDB row group
	rg := chunk.ToDuckDBRowGroup()

	// Convert back to data chunk
	chunk2 := FromDuckDBRowGroup(rg)

	// Verify structure
	assert.Equal(t, chunk.ColumnCount(), chunk2.ColumnCount())
	assert.Equal(t, chunk.Count(), chunk2.Count())

	// Verify types
	types1 := chunk.Types()
	types2 := chunk2.Types()
	assert.Equal(t, types1, types2)

	// Verify validity masks
	for colIdx := 0; colIdx < chunk.ColumnCount(); colIdx++ {
		vec1 := chunk.GetVector(colIdx)
		vec2 := chunk2.GetVector(colIdx)

		for rowIdx := 0; rowIdx < chunk.Count(); rowIdx++ {
			valid1 := vec1.Validity().IsValid(rowIdx)
			valid2 := vec2.Validity().IsValid(rowIdx)
			assert.Equal(t, valid1, valid2,
				"Validity mismatch at col=%d row=%d", colIdx, rowIdx)
		}
	}
}

// TestSerializeRowGroup_Empty tests serialization of an empty row group
func TestSerializeRowGroup_Empty(t *testing.T) {
	rg := NewDuckDBRowGroup(0)
	rg.SetRowCount(0)

	data, err := SerializeRowGroup(rg)
	require.NoError(t, err)
	require.NotNil(t, data)

	// Should have metadata (22 bytes) + index count (2 bytes) = 24 bytes
	assert.Equal(t, 24, len(data))
}

// TestSerializeRowGroup_Nil tests serialization of a nil row group
func TestSerializeRowGroup_Nil(t *testing.T) {
	data, err := SerializeRowGroup(nil)
	require.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "nil row group")
}

// TestSerializeRowGroup_SingleColumn tests serialization with a single column
func TestSerializeRowGroup_SingleColumn(t *testing.T) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(3)
	rg.SetStartId(0)
	rg.SetFlags(DuckDBRowGroupFlagNone)

	// Create a simple INTEGER column
	col := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col.SetData([]byte{1, 2, 3, 4}, false)
	col.SetValidity([]uint64{0xFFFFFFFFFFFFFFFF}) // All valid
	rg.AddColumn(0, col)

	data, err := SerializeRowGroup(rg)
	require.NoError(t, err)
	require.NotNil(t, data)

	// Verify the data structure
	// Metadata: 22 bytes
	// Column: 4 + 1 + 8 + 4 = 17 bytes header
	// Data: 4 bytes
	// Validity: 8 bytes (1 uint64)
	// Index count: 2 bytes
	expectedSize := 22 + 17 + 4 + 8 + 2
	assert.Equal(t, expectedSize, len(data))
}

// TestSerializeRowGroup_MultipleColumns tests serialization with multiple columns
func TestSerializeRowGroup_MultipleColumns(t *testing.T) {
	rg := NewDuckDBRowGroup(3)
	rg.SetRowCount(2)
	rg.SetStartId(100)
	rg.SetFlags(DuckDBRowGroupFlagCompressed)

	// Column 0: INTEGER
	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col0.SetData([]byte{1, 2, 3, 4}, false)
	col0.Compression = compression.CompressionNone
	rg.AddColumn(0, col0)

	// Column 1: VARCHAR
	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	col1.SetData([]byte{'h', 'e', 'l', 'l', 'o'}, false)
	col1.Compression = compression.CompressionNone
	col1.SetInvalid(1) // Mark row 1 as NULL
	rg.AddColumn(1, col1)

	// Column 2: DOUBLE
	col2 := NewDuckDBColumnSegment(compression.LogicalTypeDouble)
	col2.SetData([]byte{0, 0, 0, 0, 0, 0, 0xF0, 0x3F}, false) // 1.0 in IEEE 754
	col2.Compression = compression.CompressionNone
	rg.AddColumn(2, col2)

	data, err := SerializeRowGroup(rg)
	require.NoError(t, err)
	require.NotNil(t, data)

	// Verify we can read back the flags
	assert.Equal(t, uint32(DuckDBRowGroupFlagCompressed), rg.MetaData.Flags)
}

// TestDeserializeRowGroup_Empty tests deserialization of an empty row group
func TestDeserializeRowGroup_Empty(t *testing.T) {
	// Create and serialize an empty row group
	original := NewDuckDBRowGroup(0)
	original.SetRowCount(0)

	data, err := SerializeRowGroup(original)
	require.NoError(t, err)

	// Deserialize
	rg, err := DeserializeRowGroup(data)
	require.NoError(t, err)
	require.NotNil(t, rg)

	assert.Equal(t, uint64(0), rg.MetaData.RowCount)
	assert.Equal(t, uint16(0), rg.MetaData.ColumnCount)
	assert.Len(t, rg.ColumnData, 0)
}

// TestDeserializeRowGroup_InvalidData tests deserialization with invalid data
func TestDeserializeRowGroup_InvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"too short", []byte{1, 2, 3}},
		{"empty", []byte{}},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rg, err := DeserializeRowGroup(tt.data)
			require.Error(t, err)
			assert.Nil(t, rg)
		})
	}
}

// TestSerializeDeserialize_RoundTrip tests round-trip serialization
func TestSerializeDeserialize_RoundTrip(t *testing.T) {
	// Create a row group with various column types
	rg := NewDuckDBRowGroup(4)
	rg.SetRowCount(5)
	rg.SetStartId(42)
	rg.SetFlags(DuckDBRowGroupFlagSorted | DuckDBRowGroupFlagCompressed)

	// Column 0: INTEGER with some NULL values
	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col0.SetData([]byte{1, 2, 3, 4, 5, 6, 7, 8}, false)
	col0.Compression = compression.CompressionNone
	col0.SetInvalid(2) // Row 2 is NULL
	col0.SetInvalid(4) // Row 4 is NULL
	rg.AddColumn(0, col0)

	// Column 1: VARCHAR
	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	col1.SetData([]byte("hello world"), false)
	col1.Compression = compression.CompressionNone
	rg.AddColumn(1, col1)

	// Column 2: DOUBLE
	col2 := NewDuckDBColumnSegment(compression.LogicalTypeDouble)
	col2.SetData([]byte{0, 0, 0, 0, 0, 0, 0xF0, 0x3F}, false)
	col2.Compression = compression.CompressionRLE
	rg.AddColumn(2, col2)

	// Column 3: BOOLEAN (all valid)
	col3 := NewDuckDBColumnSegment(compression.LogicalTypeBoolean)
	col3.SetData([]byte{1, 0, 1, 0, 1}, false)
	col3.Compression = compression.CompressionNone
	rg.AddColumn(3, col3)

	// Serialize
	data, err := SerializeRowGroup(rg)
	require.NoError(t, err)
	require.NotNil(t, data)

	// Deserialize
	rg2, err := DeserializeRowGroup(data)
	require.NoError(t, err)
	require.NotNil(t, rg2)

	// Verify metadata
	assert.Equal(t, rg.MetaData.RowCount, rg2.MetaData.RowCount)
	assert.Equal(t, rg.MetaData.ColumnCount, rg2.MetaData.ColumnCount)
	assert.Equal(t, rg.MetaData.Flags, rg2.MetaData.Flags)
	assert.Equal(t, rg.MetaData.StartId, rg2.MetaData.StartId)

	// Verify columns
	for i := 0; i < 4; i++ {
		origCol := rg.GetColumn(i)
		deserCol := rg2.GetColumn(i)

		require.NotNil(t, origCol)
		require.NotNil(t, deserCol)

		// Verify metadata
		assert.Equal(t, origCol.MetaData.Type, deserCol.MetaData.Type,
			"Column %d type mismatch", i)
		assert.Equal(t, origCol.Compression, deserCol.Compression,
			"Column %d compression mismatch", i)

		// Verify data
		assert.Equal(t, origCol.Data, deserCol.Data,
			"Column %d data mismatch", i)

		// Verify validity
		assert.Equal(t, origCol.Validity, deserCol.Validity,
			"Column %d validity mismatch", i)
	}

	// Specifically check NULL values in column 0
	col0After := rg2.GetColumn(0)
	assert.True(t, col0After.IsValid(0))
	assert.True(t, col0After.IsValid(1))
	assert.False(t, col0After.IsValid(2)) // NULL
	assert.True(t, col0After.IsValid(3))
	assert.False(t, col0After.IsValid(4)) // NULL
}

// TestSerializeDeserialize_WithNilColumns tests handling of nil columns
func TestSerializeDeserialize_WithNilColumns(t *testing.T) {
	rg := NewDuckDBRowGroup(3)
	rg.SetRowCount(10)

	// Only set column 1, leave 0 and 2 as nil
	col1 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col1.SetData([]byte{1, 2, 3, 4}, false)
	rg.AddColumn(1, col1)

	// Serialize
	data, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	// Deserialize
	rg2, err := DeserializeRowGroup(data)
	require.NoError(t, err)

	// Verify nil columns
	assert.Nil(t, rg2.GetColumn(0))
	assert.NotNil(t, rg2.GetColumn(1))
	assert.Nil(t, rg2.GetColumn(2))

	// Verify non-nil column
	col1After := rg2.GetColumn(1)
	assert.Equal(t, compression.LogicalTypeInteger, col1After.MetaData.Type)
	assert.Equal(t, []byte{1, 2, 3, 4}, col1After.Data)
}

// TestSerializeDeserialize_EmptyColumns tests handling of columns with no data
func TestSerializeDeserialize_EmptyColumns(t *testing.T) {
	rg := NewDuckDBRowGroup(2)
	rg.SetRowCount(0)

	// Create columns with no data
	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	rg.AddColumn(0, col0)
	rg.AddColumn(1, col1)

	// Serialize
	data, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	// Deserialize
	rg2, err := DeserializeRowGroup(data)
	require.NoError(t, err)

	// Verify columns exist but have no data
	col0After := rg2.GetColumn(0)
	col1After := rg2.GetColumn(1)

	require.NotNil(t, col0After)
	require.NotNil(t, col1After)

	assert.Equal(t, compression.LogicalTypeInteger, col0After.MetaData.Type)
	assert.Equal(t, compression.LogicalTypeVarchar, col1After.MetaData.Type)
	assert.Empty(t, col0After.Data)
	assert.Empty(t, col1After.Data)
}

// TestSerializeDeserialize_LargeValidity tests handling of large validity bitmaps
func TestSerializeDeserialize_LargeValidity(t *testing.T) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(200) // More than 64 rows to test multiple uint64 entries

	col := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col.SetData(make([]byte, 800), false) // 200 * 4 bytes per int32

	// Set some values as invalid at various positions
	col.SetInvalid(0)
	col.SetInvalid(63)
	col.SetInvalid(64)
	col.SetInvalid(127)
	col.SetInvalid(128)
	col.SetInvalid(199)

	rg.AddColumn(0, col)

	// Serialize
	data, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	// Deserialize
	rg2, err := DeserializeRowGroup(data)
	require.NoError(t, err)

	col2 := rg2.GetColumn(0)
	require.NotNil(t, col2)

	// Verify the invalid positions
	assert.False(t, col2.IsValid(0))
	assert.False(t, col2.IsValid(63))
	assert.False(t, col2.IsValid(64))
	assert.False(t, col2.IsValid(127))
	assert.False(t, col2.IsValid(128))
	assert.False(t, col2.IsValid(199))

	// Verify some valid positions
	assert.True(t, col2.IsValid(1))
	assert.True(t, col2.IsValid(62))
	assert.True(t, col2.IsValid(65))
	assert.True(t, col2.IsValid(126))
	assert.True(t, col2.IsValid(198))
}

// TestRowGroup_IntegerColumn tests row group with INTEGER columns
func TestRowGroup_IntegerColumn(t *testing.T) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(4)
	rg.SetStartId(0)

	// Create INTEGER column with int32 values
	col := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	// Serialize 4 int32 values: 10, 20, 30, 40
	data := make([]byte, 16)
	writeInt32(data[0:], 10)
	writeInt32(data[4:], 20)
	writeInt32(data[8:], 30)
	writeInt32(data[12:], 40)
	col.SetData(data, false)
	rg.AddColumn(0, col)

	// Serialize and deserialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)
	require.NotNil(t, rg2)

	// Verify metadata
	assert.Equal(t, uint64(4), rg2.MetaData.RowCount)
	assert.Equal(t, uint16(1), rg2.MetaData.ColumnCount)

	// Verify column
	col2 := rg2.GetColumn(0)
	require.NotNil(t, col2)
	assert.Equal(t, compression.LogicalTypeInteger, col2.MetaData.Type)
	assert.Equal(t, data, col2.Data)
}

// TestRowGroup_VarcharColumn tests row group with VARCHAR columns
func TestRowGroup_VarcharColumn(t *testing.T) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(3)

	// Create VARCHAR column
	col := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	strData := []byte("hello world test")
	col.SetData(strData, false)
	rg.AddColumn(0, col)

	// Serialize and deserialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify column
	col2 := rg2.GetColumn(0)
	require.NotNil(t, col2)
	assert.Equal(t, compression.LogicalTypeVarchar, col2.MetaData.Type)
	assert.Equal(t, strData, col2.Data)
}

// TestRowGroup_FloatColumn tests row group with FLOAT columns
func TestRowGroup_FloatColumn(t *testing.T) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(2)

	// Create FLOAT column with float32 values
	col := NewDuckDBColumnSegment(compression.LogicalTypeFloat)
	data := make([]byte, 8) // 2 float32 values
	writeFloat32(data[0:], 1.5)
	writeFloat32(data[4:], 2.5)
	col.SetData(data, false)
	rg.AddColumn(0, col)

	// Serialize and deserialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify column
	col2 := rg2.GetColumn(0)
	require.NotNil(t, col2)
	assert.Equal(t, compression.LogicalTypeFloat, col2.MetaData.Type)
	assert.Equal(t, data, col2.Data)
}

// TestRowGroup_DoubleColumn tests row group with DOUBLE columns
func TestRowGroup_DoubleColumn(t *testing.T) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(2)

	// Create DOUBLE column with float64 values
	col := NewDuckDBColumnSegment(compression.LogicalTypeDouble)
	data := make([]byte, 16) // 2 float64 values
	writeFloat64(data[0:], 3.14159)
	writeFloat64(data[8:], 2.71828)
	col.SetData(data, false)
	rg.AddColumn(0, col)

	// Serialize and deserialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify column
	col2 := rg2.GetColumn(0)
	require.NotNil(t, col2)
	assert.Equal(t, compression.LogicalTypeDouble, col2.MetaData.Type)
	assert.Equal(t, data, col2.Data)
}

// TestRowGroup_DateColumn tests row group with DATE columns
func TestRowGroup_DateColumn(t *testing.T) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(3)

	// Create DATE column (stored as int32 days since epoch)
	col := NewDuckDBColumnSegment(compression.LogicalTypeDate)
	data := make([]byte, 12) // 3 int32 values
	writeInt32(data[0:], 18000)  // 2019-04-11
	writeInt32(data[4:], 18001)  // 2019-04-12
	writeInt32(data[8:], 18002)  // 2019-04-13
	col.SetData(data, false)
	rg.AddColumn(0, col)

	// Serialize and deserialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify column
	col2 := rg2.GetColumn(0)
	require.NotNil(t, col2)
	assert.Equal(t, compression.LogicalTypeDate, col2.MetaData.Type)
	assert.Equal(t, data, col2.Data)
}

// TestRowGroup_TimestampColumn tests row group with TIMESTAMP columns
func TestRowGroup_TimestampColumn(t *testing.T) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(2)

	// Create TIMESTAMP column (stored as int64 microseconds since epoch)
	col := NewDuckDBColumnSegment(compression.LogicalTypeTimestamp)
	data := make([]byte, 16) // 2 int64 values
	writeInt64(data[0:], 1609459200000000)  // 2021-01-01 00:00:00
	writeInt64(data[8:], 1609545600000000)  // 2021-01-02 00:00:00
	col.SetData(data, false)
	rg.AddColumn(0, col)

	// Serialize and deserialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify column
	col2 := rg2.GetColumn(0)
	require.NotNil(t, col2)
	assert.Equal(t, compression.LogicalTypeTimestamp, col2.MetaData.Type)
	assert.Equal(t, data, col2.Data)
}

// TestRowGroup_MixedTypes tests row group with mixed column types
func TestRowGroup_MixedTypes(t *testing.T) {
	rg := NewDuckDBRowGroup(5)
	rg.SetRowCount(2)

	// Column 0: INTEGER
	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	data0 := make([]byte, 8)
	writeInt32(data0[0:], 100)
	writeInt32(data0[4:], 200)
	col0.SetData(data0, false)
	rg.AddColumn(0, col0)

	// Column 1: VARCHAR
	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	col1.SetData([]byte("test"), false)
	rg.AddColumn(1, col1)

	// Column 2: FLOAT
	col2 := NewDuckDBColumnSegment(compression.LogicalTypeFloat)
	data2 := make([]byte, 8)
	writeFloat32(data2[0:], 1.5)
	writeFloat32(data2[4:], 2.5)
	col2.SetData(data2, false)
	rg.AddColumn(2, col2)

	// Column 3: DOUBLE
	col3 := NewDuckDBColumnSegment(compression.LogicalTypeDouble)
	data3 := make([]byte, 16)
	writeFloat64(data3[0:], 3.14)
	writeFloat64(data3[8:], 2.71)
	col3.SetData(data3, false)
	rg.AddColumn(3, col3)

	// Column 4: DATE
	col4 := NewDuckDBColumnSegment(compression.LogicalTypeDate)
	data4 := make([]byte, 8)
	writeInt32(data4[0:], 18000)
	writeInt32(data4[4:], 18001)
	col4.SetData(data4, false)
	rg.AddColumn(4, col4)

	// Serialize and deserialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify all columns
	assert.Equal(t, uint16(5), rg2.MetaData.ColumnCount)
	assert.Equal(t, uint64(2), rg2.MetaData.RowCount)

	assert.Equal(t, compression.LogicalTypeInteger, rg2.GetColumn(0).MetaData.Type)
	assert.Equal(t, compression.LogicalTypeVarchar, rg2.GetColumn(1).MetaData.Type)
	assert.Equal(t, compression.LogicalTypeFloat, rg2.GetColumn(2).MetaData.Type)
	assert.Equal(t, compression.LogicalTypeDouble, rg2.GetColumn(3).MetaData.Type)
	assert.Equal(t, compression.LogicalTypeDate, rg2.GetColumn(4).MetaData.Type)
}

// TestRowGroup_CompressionSelection tests that compression is selected based on column type
func TestRowGroup_CompressionSelection(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_TIMESTAMP,
		dukdb.TYPE_DATE,
	}
	chunk := NewDataChunk(types)

	// Convert to DuckDB row group
	rg := chunk.ToDuckDBRowGroup()

	// Verify compression is selected based on type
	// INTEGER: BitPacking
	assert.Equal(t, compression.CompressionBitPack, rg.GetColumn(0).Compression)

	// VARCHAR: FSST
	assert.Equal(t, compression.CompressionFSST, rg.GetColumn(1).Compression)

	// FLOAT: Chimp
	assert.Equal(t, compression.CompressionChimp, rg.GetColumn(2).Compression)

	// DOUBLE: Chimp
	assert.Equal(t, compression.CompressionChimp, rg.GetColumn(3).Compression)

	// TIMESTAMP: RLE
	assert.Equal(t, compression.CompressionRLE, rg.GetColumn(4).Compression)

	// DATE: RLE
	assert.Equal(t, compression.CompressionRLE, rg.GetColumn(5).Compression)
}

// TestRowGroup_CompressionRoundTrip tests that compression metadata is preserved
func TestRowGroup_CompressionRoundTrip(t *testing.T) {
	rg := NewDuckDBRowGroup(3)
	rg.SetRowCount(2)

	// Column 0: INTEGER with BitPack compression
	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col0.Compression = compression.CompressionBitPack
	col0.SetData([]byte{1, 2, 3, 4}, false)
	rg.AddColumn(0, col0)

	// Column 1: VARCHAR with FSST compression
	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	col1.Compression = compression.CompressionFSST
	col1.SetData([]byte("hello"), false)
	rg.AddColumn(1, col1)

	// Column 2: DOUBLE with Chimp compression
	col2 := NewDuckDBColumnSegment(compression.LogicalTypeDouble)
	col2.Compression = compression.CompressionChimp
	col2.SetData([]byte{0, 0, 0, 0, 0, 0, 0xF0, 0x3F}, false)
	rg.AddColumn(2, col2)

	// Serialize and deserialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify compression types are preserved
	assert.Equal(t, compression.CompressionBitPack, rg2.GetColumn(0).Compression)
	assert.Equal(t, compression.CompressionFSST, rg2.GetColumn(1).Compression)
	assert.Equal(t, compression.CompressionChimp, rg2.GetColumn(2).Compression)
}

// TestRowGroup_CompressionFlags tests that compressed flag is set correctly
func TestRowGroup_CompressionFlags(t *testing.T) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(2)
	rg.SetFlags(DuckDBRowGroupFlagCompressed)

	col := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col.Compression = compression.CompressionBitPack
	col.SetData([]byte{1, 2, 3, 4}, false)
	rg.AddColumn(0, col)

	// Serialize and deserialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify flag is preserved
	assert.True(t, rg2.HasFlag(DuckDBRowGroupFlagCompressed))
}

// TestRowGroup_IndexPersistence tests saving and loading indexes with row groups
func TestRowGroup_IndexPersistence(t *testing.T) {
	rg := NewDuckDBRowGroup(2)
	rg.SetRowCount(10)

	// Add column segments
	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col0.SetData([]byte{1, 2, 3, 4}, false)
	rg.AddColumn(0, col0)

	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	col1.SetData([]byte("test"), false)
	rg.AddColumn(1, col1)

	// Create a simple index for column 0
	// This would normally be serialized ART data
	indexData := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	rg.SetIndex(0, indexData)

	// Verify index is set
	assert.True(t, rg.HasIndex(0))
	assert.False(t, rg.HasIndex(1))
	assert.Equal(t, indexData, rg.GetIndex(0))
	assert.Nil(t, rg.GetIndex(1))

	// Serialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	// Deserialize
	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify index data is preserved
	assert.True(t, rg2.HasIndex(0))
	assert.False(t, rg2.HasIndex(1))
	assert.Equal(t, indexData, rg2.GetIndex(0))
	assert.Nil(t, rg2.GetIndex(1))
}

// TestRowGroup_MultipleIndexes tests multiple indexes on different columns
func TestRowGroup_MultipleIndexes(t *testing.T) {
	rg := NewDuckDBRowGroup(3)
	rg.SetRowCount(5)

	// Add columns
	for i := uint16(0); i < 3; i++ {
		col := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
		col.SetData([]byte{byte(i), byte(i + 1), byte(i + 2)}, false)
		rg.AddColumn(int(i), col)
	}

	// Add indexes for columns 0 and 2
	index0 := []byte("index_col_0")
	index2 := []byte("index_col_2")
	rg.SetIndex(0, index0)
	rg.SetIndex(2, index2)

	// Verify indexes
	assert.True(t, rg.HasIndex(0))
	assert.False(t, rg.HasIndex(1))
	assert.True(t, rg.HasIndex(2))

	// Serialize and deserialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify all indexes are preserved
	assert.True(t, rg2.HasIndex(0))
	assert.False(t, rg2.HasIndex(1))
	assert.True(t, rg2.HasIndex(2))
	assert.Equal(t, index0, rg2.GetIndex(0))
	assert.Nil(t, rg2.GetIndex(1))
	assert.Equal(t, index2, rg2.GetIndex(2))
}

// TestRowGroup_IndexReconstruction tests reconstructing indexes after load
func TestRowGroup_IndexReconstruction(t *testing.T) {
	// Import the index package for ART serialization
	// This test demonstrates how indexes can be reconstructed from serialized data

	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(3)

	// Add a column
	col := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	data := make([]byte, 12)
	writeInt32(data[0:], 100)
	writeInt32(data[4:], 200)
	writeInt32(data[8:], 300)
	col.SetData(data, false)
	rg.AddColumn(0, col)

	// Create a mock index (in real use, this would be ART.Serialize())
	mockIndexData := []byte{
		0x01, // Key type
		0x01, // Has root
		0x00, // Node type (leaf)
		0x00, 0x00, // Prefix length
		0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Value (100)
	}
	rg.SetIndex(0, mockIndexData)

	// Serialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	// Deserialize
	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify index can be retrieved
	indexData := rg2.GetIndex(0)
	require.NotNil(t, indexData)
	assert.Equal(t, mockIndexData, indexData)

	// In real usage, you would deserialize the ART here:
	// art, err := index.DeserializeART(indexData)
	// require.NoError(t, err)
	// ... use the reconstructed index for lookups
}

// TestRowGroup_NoIndexes tests row groups without any indexes
func TestRowGroup_NoIndexes(t *testing.T) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(5)

	col := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col.SetData([]byte{1, 2, 3, 4}, false)
	rg.AddColumn(0, col)

	// No indexes set - verify defaults
	assert.False(t, rg.HasIndex(0))
	assert.Nil(t, rg.GetIndex(0))

	// Serialize and deserialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify no indexes after deserialization
	assert.False(t, rg2.HasIndex(0))
	assert.Nil(t, rg2.GetIndex(0))
	assert.NotNil(t, rg2.IndexData)
	assert.Equal(t, 0, len(rg2.IndexData))
}

// TestRowGroup_LargeIndexData tests handling of large index data
func TestRowGroup_LargeIndexData(t *testing.T) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(1000)

	col := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col.SetData(make([]byte, 4000), false) // 1000 int32 values
	rg.AddColumn(0, col)

	// Create a large index (simulating a large ART)
	largeIndexData := make([]byte, 10000)
	for i := range largeIndexData {
		largeIndexData[i] = byte(i % 256)
	}
	rg.SetIndex(0, largeIndexData)

	// Serialize
	serialized, err := SerializeRowGroup(rg)
	require.NoError(t, err)

	// Deserialize
	rg2, err := DeserializeRowGroup(serialized)
	require.NoError(t, err)

	// Verify large index is preserved
	assert.True(t, rg2.HasIndex(0))
	retrievedIndex := rg2.GetIndex(0)
	require.NotNil(t, retrievedIndex)
	assert.Equal(t, len(largeIndexData), len(retrievedIndex))
	assert.Equal(t, largeIndexData, retrievedIndex)
}

// Helper functions for writing binary data

func writeInt32(buf []byte, v int32) {
	u := uint32(v)
	buf[0] = byte(u)
	buf[1] = byte(u >> 8)
	buf[2] = byte(u >> 16)
	buf[3] = byte(u >> 24)
}

func writeInt64(buf []byte, v int64) {
	u := uint64(v)
	buf[0] = byte(u)
	buf[1] = byte(u >> 8)
	buf[2] = byte(u >> 16)
	buf[3] = byte(u >> 24)
	buf[4] = byte(u >> 32)
	buf[5] = byte(u >> 40)
	buf[6] = byte(u >> 48)
	buf[7] = byte(u >> 56)
}

func writeFloat32(buf []byte, v float32) {
	bits := uint32(0)
	if v != 0 {
		// Simple IEEE 754 encoding for common values
		// This is a simplified version for testing
		if v == 1.5 {
			bits = 0x3FC00000
		} else if v == 2.5 {
			bits = 0x40200000
		}
	}
	buf[0] = byte(bits)
	buf[1] = byte(bits >> 8)
	buf[2] = byte(bits >> 16)
	buf[3] = byte(bits >> 24)
}

func writeFloat64(buf []byte, v float64) {
	bits := uint64(0)
	if v != 0 {
		// Simple IEEE 754 encoding for common values
		// This is a simplified version for testing
		if v == 3.14159 {
			bits = 0x400921FB4D12D84A
		} else if v == 2.71828 {
			bits = 0x4005BF0A8B145769
		} else if v == 3.14 {
			bits = 0x40091EB851EB851F
		} else if v == 2.71 {
			bits = 0x4005AE147AE147AE
		}
	}
	buf[0] = byte(bits)
	buf[1] = byte(bits >> 8)
	buf[2] = byte(bits >> 16)
	buf[3] = byte(bits >> 24)
	buf[4] = byte(bits >> 32)
	buf[5] = byte(bits >> 40)
	buf[6] = byte(bits >> 48)
	buf[7] = byte(bits >> 56)
}

// Benchmarks for row group serialization

func BenchmarkSerializeRowGroup_SingleColumn(b *testing.B) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(100)
	rg.SetStartId(0)

	col := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	data := make([]byte, 400) // 100 int32 values
	col.SetData(data, false)
	rg.AddColumn(0, col)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := SerializeRowGroup(rg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeRowGroup_MultipleColumns(b *testing.B) {
	rg := NewDuckDBRowGroup(5)
	rg.SetRowCount(100)

	// INTEGER column
	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col0.SetData(make([]byte, 400), false)
	rg.AddColumn(0, col0)

	// VARCHAR column
	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	col1.SetData(make([]byte, 1000), false)
	rg.AddColumn(1, col1)

	// DOUBLE column
	col2 := NewDuckDBColumnSegment(compression.LogicalTypeDouble)
	col2.SetData(make([]byte, 800), false)
	rg.AddColumn(2, col2)

	// TIMESTAMP column
	col3 := NewDuckDBColumnSegment(compression.LogicalTypeTimestamp)
	col3.SetData(make([]byte, 800), false)
	rg.AddColumn(3, col3)

	// BOOLEAN column
	col4 := NewDuckDBColumnSegment(compression.LogicalTypeBoolean)
	col4.SetData(make([]byte, 100), false)
	rg.AddColumn(4, col4)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := SerializeRowGroup(rg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeRowGroup_SingleColumn(b *testing.B) {
	rg := NewDuckDBRowGroup(1)
	rg.SetRowCount(100)

	col := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col.SetData(make([]byte, 400), false)
	rg.AddColumn(0, col)

	data, err := SerializeRowGroup(rg)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DeserializeRowGroup(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeRowGroup_MultipleColumns(b *testing.B) {
	rg := NewDuckDBRowGroup(5)
	rg.SetRowCount(100)

	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col0.SetData(make([]byte, 400), false)
	rg.AddColumn(0, col0)

	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	col1.SetData(make([]byte, 1000), false)
	rg.AddColumn(1, col1)

	col2 := NewDuckDBColumnSegment(compression.LogicalTypeDouble)
	col2.SetData(make([]byte, 800), false)
	rg.AddColumn(2, col2)

	col3 := NewDuckDBColumnSegment(compression.LogicalTypeTimestamp)
	col3.SetData(make([]byte, 800), false)
	rg.AddColumn(3, col3)

	col4 := NewDuckDBColumnSegment(compression.LogicalTypeBoolean)
	col4.SetData(make([]byte, 100), false)
	rg.AddColumn(4, col4)

	data, err := SerializeRowGroup(rg)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DeserializeRowGroup(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeRowGroup_RoundTrip(b *testing.B) {
	rg := NewDuckDBRowGroup(3)
	rg.SetRowCount(100)

	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col0.SetData(make([]byte, 400), false)
	col0.SetInvalid(10)
	col0.SetInvalid(50)
	rg.AddColumn(0, col0)

	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	col1.SetData(make([]byte, 500), false)
	rg.AddColumn(1, col1)

	col2 := NewDuckDBColumnSegment(compression.LogicalTypeDouble)
	col2.SetData(make([]byte, 800), false)
	rg.AddColumn(2, col2)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data, err := SerializeRowGroup(rg)
		if err != nil {
			b.Fatal(err)
		}

		_, err = DeserializeRowGroup(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDataChunk_ToDuckDBRowGroup(b *testing.B) {
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_BOOLEAN,
	}
	chunk := NewDataChunk(types)

	// Add some sample data
	for i := 0; i < 100; i++ {
		chunk.AppendRow([]any{int32(i), "test string", float64(i) * 1.5, i%2 == 0})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = chunk.ToDuckDBRowGroup()
	}
}

func BenchmarkFromDuckDBRowGroup(b *testing.B) {
	rg := NewDuckDBRowGroup(4)
	rg.SetRowCount(100)

	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col0.SetData(make([]byte, 400), false)
	rg.AddColumn(0, col0)

	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	col1.SetData(make([]byte, 500), false)
	rg.AddColumn(1, col1)

	col2 := NewDuckDBColumnSegment(compression.LogicalTypeDouble)
	col2.SetData(make([]byte, 800), false)
	rg.AddColumn(2, col2)

	col3 := NewDuckDBColumnSegment(compression.LogicalTypeBoolean)
	col3.SetData(make([]byte, 100), false)
	rg.AddColumn(3, col3)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = FromDuckDBRowGroup(rg)
	}
}

func BenchmarkSerializeRowGroup_WithIndexes(b *testing.B) {
	rg := NewDuckDBRowGroup(2)
	rg.SetRowCount(100)

	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col0.SetData(make([]byte, 400), false)
	rg.AddColumn(0, col0)

	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	col1.SetData(make([]byte, 500), false)
	rg.AddColumn(1, col1)

	// Add indexes
	rg.SetIndex(0, make([]byte, 1000)) // Simulated ART index
	rg.SetIndex(1, make([]byte, 2000)) // Simulated ART index

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := SerializeRowGroup(rg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeRowGroup_WithIndexes(b *testing.B) {
	rg := NewDuckDBRowGroup(2)
	rg.SetRowCount(100)

	col0 := NewDuckDBColumnSegment(compression.LogicalTypeInteger)
	col0.SetData(make([]byte, 400), false)
	rg.AddColumn(0, col0)

	col1 := NewDuckDBColumnSegment(compression.LogicalTypeVarchar)
	col1.SetData(make([]byte, 500), false)
	rg.AddColumn(1, col1)

	rg.SetIndex(0, make([]byte, 1000))
	rg.SetIndex(1, make([]byte, 2000))

	data, err := SerializeRowGroup(rg)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DeserializeRowGroup(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
