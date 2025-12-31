package dukdb

import (
	"math/big"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataChunkToRecordBatch_Primitives(t *testing.T) {
	t.Parallel()

	// Create types for chunk
	intInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	strInfo, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)

	boolInfo, err := NewTypeInfo(TYPE_BOOLEAN)
	require.NoError(t, err)

	// Create chunk
	chunk, err := NewDataChunk([]TypeInfo{intInfo, strInfo, boolInfo})
	require.NoError(t, err)

	// Set values
	require.NoError(t, chunk.SetValue(0, 0, int32(42)))
	require.NoError(t, chunk.SetValue(1, 0, "hello"))
	require.NoError(t, chunk.SetValue(2, 0, true))

	require.NoError(t, chunk.SetValue(0, 1, int32(123)))
	require.NoError(t, chunk.SetValue(1, 1, "world"))
	require.NoError(t, chunk.SetValue(2, 1, false))

	require.NoError(t, chunk.SetSize(2))

	// Create Arrow schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)

	// Convert
	alloc := memory.NewGoAllocator()
	record, err := DataChunkToRecordBatch(chunk, schema, alloc)
	require.NoError(t, err)
	defer record.Release()

	// Verify
	assert.EqualValues(t, 2, record.NumRows())
	assert.EqualValues(t, 3, record.NumCols())

	// Check values
	intCol := record.Column(0).(*array.Int32)
	assert.EqualValues(t, 42, intCol.Value(0))
	assert.EqualValues(t, 123, intCol.Value(1))

	strCol := record.Column(1).(*array.String)
	assert.Equal(t, "hello", strCol.Value(0))
	assert.Equal(t, "world", strCol.Value(1))

	boolCol := record.Column(2).(*array.Boolean)
	assert.True(t, boolCol.Value(0))
	assert.False(t, boolCol.Value(1))
}

func TestDataChunkToRecordBatch_WithNulls(t *testing.T) {
	t.Parallel()

	intInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	chunk, err := NewDataChunk([]TypeInfo{intInfo})
	require.NoError(t, err)

	// Set values with a NULL
	require.NoError(t, chunk.SetValue(0, 0, int32(42)))
	require.NoError(t, chunk.SetValue(0, 1, nil)) // NULL
	require.NoError(t, chunk.SetValue(0, 2, int32(99)))
	require.NoError(t, chunk.SetSize(3))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	alloc := memory.NewGoAllocator()
	record, err := DataChunkToRecordBatch(chunk, schema, alloc)
	require.NoError(t, err)
	defer record.Release()

	intCol := record.Column(0).(*array.Int32)
	assert.False(t, intCol.IsNull(0))
	assert.True(t, intCol.IsNull(1))
	assert.False(t, intCol.IsNull(2))
	assert.EqualValues(t, 42, intCol.Value(0))
	assert.EqualValues(t, 99, intCol.Value(2))
}

func TestDataChunkToRecordBatch_AllNumericTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		duckType  Type
		arrowType arrow.DataType
		value     any
		getValue  func(arr arrow.Array, idx int) any
	}{
		{"INT8", TYPE_TINYINT, arrow.PrimitiveTypes.Int8, int8(-42),
			func(arr arrow.Array, idx int) any { return arr.(*array.Int8).Value(idx) }},
		{"INT16", TYPE_SMALLINT, arrow.PrimitiveTypes.Int16, int16(-1000),
			func(arr arrow.Array, idx int) any { return arr.(*array.Int16).Value(idx) }},
		{"INT32", TYPE_INTEGER, arrow.PrimitiveTypes.Int32, int32(-100000),
			func(arr arrow.Array, idx int) any { return arr.(*array.Int32).Value(idx) }},
		{"INT64", TYPE_BIGINT, arrow.PrimitiveTypes.Int64, int64(-9999999999),
			func(arr arrow.Array, idx int) any { return arr.(*array.Int64).Value(idx) }},
		{"UINT8", TYPE_UTINYINT, arrow.PrimitiveTypes.Uint8, uint8(200),
			func(arr arrow.Array, idx int) any { return arr.(*array.Uint8).Value(idx) }},
		{"UINT16", TYPE_USMALLINT, arrow.PrimitiveTypes.Uint16, uint16(60000),
			func(arr arrow.Array, idx int) any { return arr.(*array.Uint16).Value(idx) }},
		{"UINT32", TYPE_UINTEGER, arrow.PrimitiveTypes.Uint32, uint32(4000000000),
			func(arr arrow.Array, idx int) any { return arr.(*array.Uint32).Value(idx) }},
		{"UINT64", TYPE_UBIGINT, arrow.PrimitiveTypes.Uint64, uint64(18446744073709551000),
			func(arr arrow.Array, idx int) any { return arr.(*array.Uint64).Value(idx) }},
		{"FLOAT32", TYPE_FLOAT, arrow.PrimitiveTypes.Float32, float32(3.14),
			func(arr arrow.Array, idx int) any { return arr.(*array.Float32).Value(idx) }},
		{"FLOAT64", TYPE_DOUBLE, arrow.PrimitiveTypes.Float64, float64(2.718281828),
			func(arr arrow.Array, idx int) any { return arr.(*array.Float64).Value(idx) }},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			typeInfo, err := NewTypeInfo(tc.duckType)
			require.NoError(t, err)

			chunk, err := NewDataChunk([]TypeInfo{typeInfo})
			require.NoError(t, err)

			require.NoError(t, chunk.SetValue(0, 0, tc.value))
			require.NoError(t, chunk.SetSize(1))

			schema := arrow.NewSchema([]arrow.Field{
				{Name: "val", Type: tc.arrowType, Nullable: true},
			}, nil)

			alloc := memory.NewGoAllocator()
			record, err := DataChunkToRecordBatch(chunk, schema, alloc)
			require.NoError(t, err)
			defer record.Release()

			got := tc.getValue(record.Column(0), 0)
			assert.Equal(t, tc.value, got)
		})
	}
}

func TestDataChunkToRecordBatch_Timestamps(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 6, 15, 10, 30, 45, 123456000, time.UTC)

	testCases := []struct {
		name      string
		duckType  Type
		arrowType *arrow.TimestampType
		value     time.Time
	}{
		{"TIMESTAMP_S", TYPE_TIMESTAMP_S, &arrow.TimestampType{Unit: arrow.Second}, now.Truncate(time.Second)},
		{"TIMESTAMP_MS", TYPE_TIMESTAMP_MS, &arrow.TimestampType{Unit: arrow.Millisecond}, now.Truncate(time.Millisecond)},
		{"TIMESTAMP", TYPE_TIMESTAMP, &arrow.TimestampType{Unit: arrow.Microsecond}, now.Truncate(time.Microsecond)},
		{"TIMESTAMP_NS", TYPE_TIMESTAMP_NS, &arrow.TimestampType{Unit: arrow.Nanosecond}, now},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			typeInfo, err := NewTypeInfo(tc.duckType)
			require.NoError(t, err)

			chunk, err := NewDataChunk([]TypeInfo{typeInfo})
			require.NoError(t, err)

			require.NoError(t, chunk.SetValue(0, 0, tc.value))
			require.NoError(t, chunk.SetSize(1))

			schema := arrow.NewSchema([]arrow.Field{
				{Name: "ts", Type: tc.arrowType, Nullable: true},
			}, nil)

			alloc := memory.NewGoAllocator()
			record, err := DataChunkToRecordBatch(chunk, schema, alloc)
			require.NoError(t, err)
			defer record.Release()

			// Retrieve and verify timestamp
			col := record.Column(0).(*array.Timestamp)
			tsVal := col.Value(0)

			var got time.Time
			switch tc.arrowType.Unit {
			case arrow.Second:
				got = time.Unix(int64(tsVal), 0).UTC()
			case arrow.Millisecond:
				got = time.UnixMilli(int64(tsVal)).UTC()
			case arrow.Microsecond:
				got = time.UnixMicro(int64(tsVal)).UTC()
			case arrow.Nanosecond:
				got = time.Unix(0, int64(tsVal)).UTC()
			}

			assert.Equal(t, tc.value.UTC(), got)
		})
	}
}

func TestRecordBatchToDataChunk_Primitives(t *testing.T) {
	t.Parallel()

	alloc := memory.NewGoAllocator()

	// Build Arrow record
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"alice", "bob", "charlie"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{95.5, 87.3, 92.0}, nil)

	record := builder.NewRecord()
	defer record.Release()

	// Create chunk with matching types
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	strInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	floatInfo, _ := NewTypeInfo(TYPE_DOUBLE)

	chunk, err := NewDataChunk([]TypeInfo{intInfo, strInfo, floatInfo})
	require.NoError(t, err)

	// Convert
	err = RecordBatchToDataChunk(record, chunk)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, 3, chunk.GetSize())

	val, err := chunk.GetValue(0, 0)
	require.NoError(t, err)
	assert.EqualValues(t, 1, val)

	val, err = chunk.GetValue(1, 1)
	require.NoError(t, err)
	assert.Equal(t, "bob", val)

	val, err = chunk.GetValue(2, 2)
	require.NoError(t, err)
	assert.EqualValues(t, 92.0, val)
}

func TestRecordBatchToDataChunk_WithNulls(t *testing.T) {
	t.Parallel()

	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	intBuilder := builder.Field(0).(*array.Int32Builder)
	intBuilder.Append(42)
	intBuilder.AppendNull()
	intBuilder.Append(99)

	record := builder.NewRecord()
	defer record.Release()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	chunk, err := NewDataChunk([]TypeInfo{intInfo})
	require.NoError(t, err)

	err = RecordBatchToDataChunk(record, chunk)
	require.NoError(t, err)

	val, err := chunk.GetValue(0, 0)
	require.NoError(t, err)
	assert.EqualValues(t, 42, val)

	val, err = chunk.GetValue(0, 1)
	require.NoError(t, err)
	assert.Nil(t, val)

	val, err = chunk.GetValue(0, 2)
	require.NoError(t, err)
	assert.EqualValues(t, 99, val)
}

func TestRoundTrip_DataChunk_RecordBatch(t *testing.T) {
	t.Parallel()

	// Create types
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	strInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	boolInfo, _ := NewTypeInfo(TYPE_BOOLEAN)

	// Create original chunk
	original, err := NewDataChunk([]TypeInfo{intInfo, strInfo, boolInfo})
	require.NoError(t, err)

	// Set values
	original.SetValue(0, 0, int32(42))
	original.SetValue(1, 0, "hello")
	original.SetValue(2, 0, true)

	original.SetValue(0, 1, nil) // NULL
	original.SetValue(1, 1, "world")
	original.SetValue(2, 1, false)

	original.SetValue(0, 2, int32(99))
	original.SetValue(1, 2, nil) // NULL
	original.SetValue(2, 2, true)

	original.SetSize(3)

	// Convert to Arrow
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)

	alloc := memory.NewGoAllocator()
	record, err := DataChunkToRecordBatch(original, schema, alloc)
	require.NoError(t, err)
	defer record.Release()

	// Convert back to chunk
	roundtrip, err := NewDataChunk([]TypeInfo{intInfo, strInfo, boolInfo})
	require.NoError(t, err)

	err = RecordBatchToDataChunk(record, roundtrip)
	require.NoError(t, err)

	// Verify values match
	assert.Equal(t, original.GetSize(), roundtrip.GetSize())

	for row := range 3 {
		for col := range 3 {
			origVal, err := original.GetValue(col, row)
			require.NoError(t, err)
			rtVal, err := roundtrip.GetValue(col, row)
			require.NoError(t, err)
			assert.Equal(t, origVal, rtVal, "mismatch at row %d, col %d", row, col)
		}
	}
}

func TestSchemaFromTypes(t *testing.T) {
	t.Parallel()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	strInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	boolInfo, _ := NewTypeInfo(TYPE_BOOLEAN)

	schema, err := SchemaFromTypes(
		[]string{"id", "name", "active"},
		[]TypeInfo{intInfo, strInfo, boolInfo},
	)
	require.NoError(t, err)

	require.Len(t, schema.Fields(), 3)
	assert.Equal(t, "id", schema.Field(0).Name)
	assert.Equal(t, arrow.PrimitiveTypes.Int32, schema.Field(0).Type)
	assert.Equal(t, "name", schema.Field(1).Name)
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(1).Type)
	assert.Equal(t, "active", schema.Field(2).Name)
	assert.Equal(t, arrow.FixedWidthTypes.Boolean, schema.Field(2).Type)
}

func TestTypesFromSchema(t *testing.T) {
	t.Parallel()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	types, err := TypesFromSchema(schema)
	require.NoError(t, err)

	require.Len(t, types, 3)
	assert.Equal(t, TYPE_INTEGER, types[0].InternalType())
	assert.Equal(t, TYPE_VARCHAR, types[1].InternalType())
	assert.Equal(t, TYPE_BOOLEAN, types[2].InternalType())
}

func TestDataChunkToRecordBatch_List(t *testing.T) {
	t.Parallel()

	// Create list type
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	listInfo, err := NewListInfo(intInfo)
	require.NoError(t, err)

	chunk, err := NewDataChunk([]TypeInfo{listInfo})
	require.NoError(t, err)

	// Set list values
	require.NoError(t, chunk.SetValue(0, 0, []any{int32(1), int32(2), int32(3)}))
	require.NoError(t, chunk.SetValue(0, 1, []any{int32(4), int32(5)}))
	require.NoError(t, chunk.SetSize(2))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "nums", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	}, nil)

	alloc := memory.NewGoAllocator()
	record, err := DataChunkToRecordBatch(chunk, schema, alloc)
	require.NoError(t, err)
	defer record.Release()

	assert.EqualValues(t, 2, record.NumRows())

	listArr := record.Column(0).(*array.List)
	assert.False(t, listArr.IsNull(0))
	assert.False(t, listArr.IsNull(1))
}

func TestDataChunkToRecordBatch_ErrorCases(t *testing.T) {
	t.Parallel()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	chunk, _ := NewDataChunk([]TypeInfo{intInfo})
	chunk.SetSize(1)
	chunk.SetValue(0, 0, int32(42))

	alloc := memory.NewGoAllocator()

	t.Run("nil chunk", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		}, nil)
		_, err := DataChunkToRecordBatch(nil, schema, alloc)
		assert.Error(t, err)
	})

	t.Run("nil schema", func(t *testing.T) {
		_, err := DataChunkToRecordBatch(chunk, nil, alloc)
		assert.Error(t, err)
	})

	t.Run("column count mismatch", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int32},
			{Name: "b", Type: arrow.PrimitiveTypes.Int32},
		}, nil)
		_, err := DataChunkToRecordBatch(chunk, schema, alloc)
		assert.Error(t, err)
	})
}

func TestRecordBatchToDataChunk_ErrorCases(t *testing.T) {
	t.Parallel()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	chunk, _ := NewDataChunk([]TypeInfo{intInfo})

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	builder := array.NewRecordBuilder(alloc, schema)
	builder.Field(0).(*array.Int32Builder).Append(42)
	record := builder.NewRecord()
	defer record.Release()
	builder.Release()

	t.Run("nil record", func(t *testing.T) {
		err := RecordBatchToDataChunk(nil, chunk)
		assert.Error(t, err)
	})

	t.Run("nil chunk", func(t *testing.T) {
		err := RecordBatchToDataChunk(record, nil)
		assert.Error(t, err)
	})

	t.Run("column count mismatch", func(t *testing.T) {
		intInfo2, _ := NewTypeInfo(TYPE_INTEGER)
		chunk2, _ := NewDataChunk([]TypeInfo{intInfo2, intInfo2})
		err := RecordBatchToDataChunk(record, chunk2)
		assert.Error(t, err)
	})
}

func TestDataChunkToRecordBatch_UUID(t *testing.T) {
	t.Parallel()

	uuidInfo, _ := NewTypeInfo(TYPE_UUID)
	chunk, err := NewDataChunk([]TypeInfo{uuidInfo})
	require.NoError(t, err)

	testUUID := UUID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	require.NoError(t, chunk.SetValue(0, 0, testUUID))
	require.NoError(t, chunk.SetSize(1))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "uuid", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: true},
	}, nil)

	alloc := memory.NewGoAllocator()
	record, err := DataChunkToRecordBatch(chunk, schema, alloc)
	require.NoError(t, err)
	defer record.Release()

	fsbArr := record.Column(0).(*array.FixedSizeBinary)
	gotBytes := fsbArr.Value(0)
	assert.Equal(t, testUUID[:], gotBytes)
}

func TestDataChunkToRecordBatch_Decimal(t *testing.T) {
	t.Parallel()

	decInfo, err := NewDecimalInfo(18, 4)
	require.NoError(t, err)

	chunk, err := NewDataChunk([]TypeInfo{decInfo})
	require.NoError(t, err)

	// Create a decimal value: 123.4567
	decVal := Decimal{
		Width: 18,
		Scale: 4,
		Value: big.NewInt(1234567),
	}
	require.NoError(t, chunk.SetValue(0, 0, decVal))
	require.NoError(t, chunk.SetSize(1))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 4}, Nullable: true},
	}, nil)

	alloc := memory.NewGoAllocator()
	record, err := DataChunkToRecordBatch(chunk, schema, alloc)
	require.NoError(t, err)
	defer record.Release()

	decArr := record.Column(0).(*array.Decimal128)
	gotBigInt := decArr.Value(0).BigInt()
	assert.Equal(t, big.NewInt(1234567), gotBigInt)
}

func TestArrowTypeToDuckDBRoundTrip(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		arrowType arrow.DataType
		duckType  Type
	}{
		{"BOOL", arrow.FixedWidthTypes.Boolean, TYPE_BOOLEAN},
		{"INT8", arrow.PrimitiveTypes.Int8, TYPE_TINYINT},
		{"INT16", arrow.PrimitiveTypes.Int16, TYPE_SMALLINT},
		{"INT32", arrow.PrimitiveTypes.Int32, TYPE_INTEGER},
		{"INT64", arrow.PrimitiveTypes.Int64, TYPE_BIGINT},
		{"UINT8", arrow.PrimitiveTypes.Uint8, TYPE_UTINYINT},
		{"UINT16", arrow.PrimitiveTypes.Uint16, TYPE_USMALLINT},
		{"UINT32", arrow.PrimitiveTypes.Uint32, TYPE_UINTEGER},
		{"UINT64", arrow.PrimitiveTypes.Uint64, TYPE_UBIGINT},
		{"FLOAT32", arrow.PrimitiveTypes.Float32, TYPE_FLOAT},
		{"FLOAT64", arrow.PrimitiveTypes.Float64, TYPE_DOUBLE},
		{"STRING", arrow.BinaryTypes.String, TYPE_VARCHAR},
		{"BINARY", arrow.BinaryTypes.Binary, TYPE_BLOB},
		{"DATE32", arrow.FixedWidthTypes.Date32, TYPE_DATE},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrow -> DuckDB
			typeInfo, err := arrowTypeToDuckDB(tc.arrowType)
			require.NoError(t, err)
			assert.Equal(t, tc.duckType, typeInfo.InternalType())

			// DuckDB -> Arrow
			arrowType, err := duckdbTypeToArrow(typeInfo)
			require.NoError(t, err)
			assert.Equal(t, tc.arrowType.ID(), arrowType.ID())
		})
	}
}
