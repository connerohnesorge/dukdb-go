package arrow

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDuckDBTypeToArrow_AllPrimitiveTypes tests conversion of all primitive DuckDB types to Arrow types.
func TestDuckDBTypeToArrow_AllPrimitiveTypes(t *testing.T) {
	tests := []struct {
		name       string
		duckType   dukdb.Type
		expectErr  bool
		arrowCheck func(t *testing.T, dt arrow.DataType)
	}{
		// Boolean
		{
			name:     "TYPE_BOOLEAN",
			duckType: dukdb.TYPE_BOOLEAN,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.BOOL, dt.ID())
			},
		},
		// Signed integers
		{
			name:     "TYPE_TINYINT",
			duckType: dukdb.TYPE_TINYINT,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.INT8, dt.ID())
			},
		},
		{
			name:     "TYPE_SMALLINT",
			duckType: dukdb.TYPE_SMALLINT,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.INT16, dt.ID())
			},
		},
		{
			name:     "TYPE_INTEGER",
			duckType: dukdb.TYPE_INTEGER,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.INT32, dt.ID())
			},
		},
		{
			name:     "TYPE_BIGINT",
			duckType: dukdb.TYPE_BIGINT,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.INT64, dt.ID())
			},
		},
		// Unsigned integers
		{
			name:     "TYPE_UTINYINT",
			duckType: dukdb.TYPE_UTINYINT,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.UINT8, dt.ID())
			},
		},
		{
			name:     "TYPE_USMALLINT",
			duckType: dukdb.TYPE_USMALLINT,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.UINT16, dt.ID())
			},
		},
		{
			name:     "TYPE_UINTEGER",
			duckType: dukdb.TYPE_UINTEGER,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.UINT32, dt.ID())
			},
		},
		{
			name:     "TYPE_UBIGINT",
			duckType: dukdb.TYPE_UBIGINT,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.UINT64, dt.ID())
			},
		},
		// Floating point
		{
			name:     "TYPE_FLOAT",
			duckType: dukdb.TYPE_FLOAT,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.FLOAT32, dt.ID())
			},
		},
		{
			name:     "TYPE_DOUBLE",
			duckType: dukdb.TYPE_DOUBLE,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.FLOAT64, dt.ID())
			},
		},
		// String/Binary
		{
			name:     "TYPE_VARCHAR",
			duckType: dukdb.TYPE_VARCHAR,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.STRING, dt.ID())
			},
		},
		{
			name:     "TYPE_BLOB",
			duckType: dukdb.TYPE_BLOB,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.BINARY, dt.ID())
			},
		},
		{
			name:     "TYPE_BIT",
			duckType: dukdb.TYPE_BIT,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.BINARY, dt.ID())
			},
		},
		// Date/Time
		{
			name:     "TYPE_DATE",
			duckType: dukdb.TYPE_DATE,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.DATE32, dt.ID())
			},
		},
		{
			name:     "TYPE_TIME",
			duckType: dukdb.TYPE_TIME,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.TIME64, dt.ID())
			},
		},
		{
			name:     "TYPE_TIME_TZ",
			duckType: dukdb.TYPE_TIME_TZ,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.TIME64, dt.ID())
			},
		},
		// Timestamps
		{
			name:     "TYPE_TIMESTAMP",
			duckType: dukdb.TYPE_TIMESTAMP,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.TIMESTAMP, dt.ID())
				ts, ok := dt.(*arrow.TimestampType)
				require.True(t, ok, "expected *arrow.TimestampType")
				assert.Equal(t, arrow.Microsecond, ts.Unit)
			},
		},
		{
			name:     "TYPE_TIMESTAMP_S",
			duckType: dukdb.TYPE_TIMESTAMP_S,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.TIMESTAMP, dt.ID())
				ts, ok := dt.(*arrow.TimestampType)
				require.True(t, ok, "expected *arrow.TimestampType")
				assert.Equal(t, arrow.Second, ts.Unit)
			},
		},
		{
			name:     "TYPE_TIMESTAMP_MS",
			duckType: dukdb.TYPE_TIMESTAMP_MS,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.TIMESTAMP, dt.ID())
				ts, ok := dt.(*arrow.TimestampType)
				require.True(t, ok, "expected *arrow.TimestampType")
				assert.Equal(t, arrow.Millisecond, ts.Unit)
			},
		},
		{
			name:     "TYPE_TIMESTAMP_NS",
			duckType: dukdb.TYPE_TIMESTAMP_NS,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.TIMESTAMP, dt.ID())
				ts, ok := dt.(*arrow.TimestampType)
				require.True(t, ok, "expected *arrow.TimestampType")
				assert.Equal(t, arrow.Nanosecond, ts.Unit)
			},
		},
		{
			name:     "TYPE_TIMESTAMP_TZ",
			duckType: dukdb.TYPE_TIMESTAMP_TZ,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.TIMESTAMP, dt.ID())
				ts, ok := dt.(*arrow.TimestampType)
				require.True(t, ok, "expected *arrow.TimestampType")
				assert.Equal(t, arrow.Microsecond, ts.Unit)
				assert.Equal(t, "UTC", ts.TimeZone)
			},
		},
		// Interval
		{
			name:     "TYPE_INTERVAL",
			duckType: dukdb.TYPE_INTERVAL,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.INTERVAL_MONTH_DAY_NANO, dt.ID())
			},
		},
		// Special numeric types
		{
			name:     "TYPE_HUGEINT",
			duckType: dukdb.TYPE_HUGEINT,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.DECIMAL128, dt.ID())
				dec, ok := dt.(*arrow.Decimal128Type)
				require.True(t, ok, "expected *arrow.Decimal128Type")
				assert.Equal(t, int32(38), dec.Precision)
				assert.Equal(t, int32(0), dec.Scale)
			},
		},
		{
			name:     "TYPE_UHUGEINT",
			duckType: dukdb.TYPE_UHUGEINT,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.DECIMAL128, dt.ID())
			},
		},
		{
			name:     "TYPE_DECIMAL",
			duckType: dukdb.TYPE_DECIMAL,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.DECIMAL128, dt.ID())
			},
		},
		// UUID
		{
			name:     "TYPE_UUID",
			duckType: dukdb.TYPE_UUID,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.FIXED_SIZE_BINARY, dt.ID())
				fsb, ok := dt.(*arrow.FixedSizeBinaryType)
				require.True(t, ok, "expected *arrow.FixedSizeBinaryType")
				assert.Equal(t, 16, fsb.ByteWidth)
			},
		},
		// Complex types
		{
			name:     "TYPE_LIST",
			duckType: dukdb.TYPE_LIST,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.LIST, dt.ID())
			},
		},
		{
			name:     "TYPE_ARRAY",
			duckType: dukdb.TYPE_ARRAY,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.FIXED_SIZE_LIST, dt.ID())
			},
		},
		{
			name:     "TYPE_STRUCT",
			duckType: dukdb.TYPE_STRUCT,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.STRUCT, dt.ID())
			},
		},
		{
			name:     "TYPE_MAP",
			duckType: dukdb.TYPE_MAP,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.MAP, dt.ID())
			},
		},
		{
			name:     "TYPE_ENUM",
			duckType: dukdb.TYPE_ENUM,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.DICTIONARY, dt.ID())
			},
		},
		// NULL type
		{
			name:     "TYPE_SQLNULL",
			duckType: dukdb.TYPE_SQLNULL,
			arrowCheck: func(t *testing.T, dt arrow.DataType) {
				assert.Equal(t, arrow.NULL, dt.ID())
			},
		},
		// Invalid types should error
		{
			name:      "TYPE_INVALID",
			duckType:  dukdb.TYPE_INVALID,
			expectErr: true,
		},
		{
			name:      "TYPE_ANY",
			duckType:  dukdb.TYPE_ANY,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arrowType, err := DuckDBTypeToArrow(tt.duckType)
			if tt.expectErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, arrowType)
			if tt.arrowCheck != nil {
				tt.arrowCheck(t, arrowType)
			}
		})
	}
}

// TestArrowTypeToDuckDB_AllTypes tests conversion of all Arrow types to DuckDB types.
func TestArrowTypeToDuckDB_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		arrowTyp arrow.DataType
		expected dukdb.Type
		wantErr  bool
	}{
		// Boolean
		{"bool", arrow.FixedWidthTypes.Boolean, dukdb.TYPE_BOOLEAN, false},
		// Signed integers
		{"int8", arrow.PrimitiveTypes.Int8, dukdb.TYPE_TINYINT, false},
		{"int16", arrow.PrimitiveTypes.Int16, dukdb.TYPE_SMALLINT, false},
		{"int32", arrow.PrimitiveTypes.Int32, dukdb.TYPE_INTEGER, false},
		{"int64", arrow.PrimitiveTypes.Int64, dukdb.TYPE_BIGINT, false},
		// Unsigned integers
		{"uint8", arrow.PrimitiveTypes.Uint8, dukdb.TYPE_UTINYINT, false},
		{"uint16", arrow.PrimitiveTypes.Uint16, dukdb.TYPE_USMALLINT, false},
		{"uint32", arrow.PrimitiveTypes.Uint32, dukdb.TYPE_UINTEGER, false},
		{"uint64", arrow.PrimitiveTypes.Uint64, dukdb.TYPE_UBIGINT, false},
		// Floating point
		{"float32", arrow.PrimitiveTypes.Float32, dukdb.TYPE_FLOAT, false},
		{"float64", arrow.PrimitiveTypes.Float64, dukdb.TYPE_DOUBLE, false},
		// String types
		{"string", arrow.BinaryTypes.String, dukdb.TYPE_VARCHAR, false},
		{"large_string", arrow.BinaryTypes.LargeString, dukdb.TYPE_VARCHAR, false},
		// Binary types
		{"binary", arrow.BinaryTypes.Binary, dukdb.TYPE_BLOB, false},
		{"large_binary", arrow.BinaryTypes.LargeBinary, dukdb.TYPE_BLOB, false},
		// Date types
		{"date32", arrow.FixedWidthTypes.Date32, dukdb.TYPE_DATE, false},
		{"date64", arrow.FixedWidthTypes.Date64, dukdb.TYPE_DATE, false},
		// Time types
		{"time32s", arrow.FixedWidthTypes.Time32s, dukdb.TYPE_TIME, false},
		{"time32ms", arrow.FixedWidthTypes.Time32ms, dukdb.TYPE_TIME, false},
		{"time64us", arrow.FixedWidthTypes.Time64us, dukdb.TYPE_TIME, false},
		{"time64ns", arrow.FixedWidthTypes.Time64ns, dukdb.TYPE_TIME, false},
		// Timestamp types
		{
			"timestamp_s",
			&arrow.TimestampType{Unit: arrow.Second},
			dukdb.TYPE_TIMESTAMP_S,
			false,
		},
		{
			"timestamp_ms",
			&arrow.TimestampType{Unit: arrow.Millisecond},
			dukdb.TYPE_TIMESTAMP_MS,
			false,
		},
		{
			"timestamp_us",
			&arrow.TimestampType{Unit: arrow.Microsecond},
			dukdb.TYPE_TIMESTAMP,
			false,
		},
		{
			"timestamp_ns",
			&arrow.TimestampType{Unit: arrow.Nanosecond},
			dukdb.TYPE_TIMESTAMP_NS,
			false,
		},
		// Interval types
		{"interval_months", arrow.FixedWidthTypes.MonthInterval, dukdb.TYPE_INTERVAL, false},
		{"interval_day_time", arrow.FixedWidthTypes.DayTimeInterval, dukdb.TYPE_INTERVAL, false},
		{"interval_month_day_nano", arrow.FixedWidthTypes.MonthDayNanoInterval, dukdb.TYPE_INTERVAL, false},
		// Fixed size binary - UUID (16 bytes)
		{
			"uuid_fsb",
			&arrow.FixedSizeBinaryType{ByteWidth: 16},
			dukdb.TYPE_UUID,
			false,
		},
		// Fixed size binary - other sizes
		{
			"fsb_other",
			&arrow.FixedSizeBinaryType{ByteWidth: 32},
			dukdb.TYPE_BLOB,
			false,
		},
		// Decimal
		{
			"decimal128",
			&arrow.Decimal128Type{Precision: 18, Scale: 2},
			dukdb.TYPE_DECIMAL,
			false,
		},
		// Nested types
		{"list", arrow.ListOf(arrow.PrimitiveTypes.Int64), dukdb.TYPE_LIST, false},
		{"large_list", arrow.LargeListOf(arrow.PrimitiveTypes.Int64), dukdb.TYPE_LIST, false},
		{
			"fixed_size_list",
			arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32),
			dukdb.TYPE_ARRAY,
			false,
		},
		{
			"struct",
			arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32}),
			dukdb.TYPE_STRUCT,
			false,
		},
		{
			"map",
			arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32),
			dukdb.TYPE_MAP,
			false,
		},
		// Dictionary (ENUM)
		{
			"dictionary",
			&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String},
			dukdb.TYPE_VARCHAR,
			false,
		},
		// Null
		{"null", arrow.Null, dukdb.TYPE_SQLNULL, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ArrowTypeToDuckDB(tt.arrowTyp)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got, "expected %s, got %s", tt.expected.String(), got.String())
		})
	}
}

// TestSchemaToColumnTypes tests conversion of Arrow schema to DuckDB column types.
func TestSchemaToColumnTypes(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "created", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		},
		nil,
	)

	names, types, err := SchemaToColumnTypes(schema)
	require.NoError(t, err)

	assert.Equal(t, []string{"id", "name", "price", "active", "created"}, names)
	assert.Equal(t, []dukdb.Type{
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_TIMESTAMP,
	}, types)
}

// TestRecordBatchToDataChunk_AllTypes tests conversion of Arrow RecordBatch to DataChunk with various types.
func TestRecordBatchToDataChunk_AllTypes(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "bool_col", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			{Name: "int8_col", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			{Name: "int16_col", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
			{Name: "int32_col", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "int64_col", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "uint8_col", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
			{Name: "uint16_col", Type: arrow.PrimitiveTypes.Uint16, Nullable: true},
			{Name: "uint32_col", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "uint64_col", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
			{Name: "float32_col", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
			{Name: "float64_col", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "string_col", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "binary_col", Type: arrow.BinaryTypes.Binary, Nullable: true},
			{Name: "date32_col", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	// Add values
	bldr.Field(0).(*array.BooleanBuilder).Append(true)
	bldr.Field(1).(*array.Int8Builder).Append(42)
	bldr.Field(2).(*array.Int16Builder).Append(1000)
	bldr.Field(3).(*array.Int32Builder).Append(100000)
	bldr.Field(4).(*array.Int64Builder).Append(1000000000)
	bldr.Field(5).(*array.Uint8Builder).Append(200)
	bldr.Field(6).(*array.Uint16Builder).Append(50000)
	bldr.Field(7).(*array.Uint32Builder).Append(3000000000)
	bldr.Field(8).(*array.Uint64Builder).Append(10000000000000)
	bldr.Field(9).(*array.Float32Builder).Append(3.14)
	bldr.Field(10).(*array.Float64Builder).Append(2.71828)
	bldr.Field(11).(*array.StringBuilder).Append("hello")
	bldr.Field(12).(*array.BinaryBuilder).Append([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	bldr.Field(13).(*array.Date32Builder).Append(arrow.Date32(19000)) // ~2022-01-01

	record := bldr.NewRecord()
	defer record.Release()

	chunk, err := RecordBatchToDataChunk(record)
	require.NoError(t, err)
	require.NotNil(t, chunk)
	assert.Equal(t, 1, chunk.Count())
	assert.Equal(t, 14, chunk.ColumnCount())

	// Verify values
	assert.Equal(t, true, chunk.GetValue(0, 0))
	assert.Equal(t, int8(42), chunk.GetValue(0, 1))
	assert.Equal(t, int16(1000), chunk.GetValue(0, 2))
	assert.Equal(t, int32(100000), chunk.GetValue(0, 3))
	assert.Equal(t, int64(1000000000), chunk.GetValue(0, 4))
	assert.Equal(t, uint8(200), chunk.GetValue(0, 5))
	assert.Equal(t, uint16(50000), chunk.GetValue(0, 6))
	assert.Equal(t, uint32(3000000000), chunk.GetValue(0, 7))
	assert.Equal(t, uint64(10000000000000), chunk.GetValue(0, 8))
	assert.InDelta(t, float32(3.14), chunk.GetValue(0, 9), 0.001)
	assert.InDelta(t, 2.71828, chunk.GetValue(0, 10), 0.00001)
	assert.Equal(t, "hello", chunk.GetValue(0, 11))
	assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, chunk.GetValue(0, 12))
}

// TestRecordBatchToDataChunk_Timestamps tests timestamp conversions.
func TestRecordBatchToDataChunk_Timestamps(t *testing.T) {
	alloc := memory.NewGoAllocator()

	// Test timestamp at 2024-01-15 12:30:45
	testTime := time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "ts_s", Type: &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}, Nullable: true},
			{Name: "ts_ms", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: true},
			{Name: "ts_us", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
			{Name: "ts_ns", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(testTime.Unix()))
	bldr.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(testTime.UnixMilli()))
	bldr.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(testTime.UnixMicro()))
	bldr.Field(3).(*array.TimestampBuilder).Append(arrow.Timestamp(testTime.UnixNano()))

	record := bldr.NewRecord()
	defer record.Release()

	chunk, err := RecordBatchToDataChunk(record)
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Verify all timestamps represent the same time
	for i := 0; i < 4; i++ {
		val, ok := chunk.GetValue(0, i).(time.Time)
		require.True(t, ok, "column %d should be time.Time", i)
		assert.Equal(t, testTime.Year(), val.Year())
		assert.Equal(t, testTime.Month(), val.Month())
		assert.Equal(t, testTime.Day(), val.Day())
		assert.Equal(t, testTime.Hour(), val.Hour())
		assert.Equal(t, testTime.Minute(), val.Minute())
		assert.Equal(t, testTime.Second(), val.Second())
	}
}

// TestRecordBatchToDataChunk_Interval tests interval conversion.
func TestRecordBatchToDataChunk_Interval(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "interval_col", Type: arrow.FixedWidthTypes.MonthDayNanoInterval, Nullable: true},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	// 1 year, 15 days, 3600 seconds (1 hour)
	bldr.Field(0).(*array.MonthDayNanoIntervalBuilder).Append(arrow.MonthDayNanoInterval{
		Months:      12,
		Days:        15,
		Nanoseconds: 3600 * 1000000 * 1000, // 1 hour in nanoseconds
	})

	record := bldr.NewRecord()
	defer record.Release()

	chunk, err := RecordBatchToDataChunk(record)
	require.NoError(t, err)
	require.NotNil(t, chunk)

	val, ok := chunk.GetValue(0, 0).(dukdb.Interval)
	require.True(t, ok)
	assert.Equal(t, int32(12), val.Months)
	assert.Equal(t, int32(15), val.Days)
	assert.Equal(t, int64(3600*1000000), val.Micros) // 1 hour in microseconds
}

// TestRecordBatchToDataChunk_Decimal tests decimal conversion.
func TestRecordBatchToDataChunk_Decimal(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "decimal_col", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}, Nullable: true},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	// Value 12345.67 stored as 1234567 with scale 2
	val := big.NewInt(1234567)
	bldr.Field(0).(*array.Decimal128Builder).Append(decimal128.FromBigInt(val))

	record := bldr.NewRecord()
	defer record.Release()

	chunk, err := RecordBatchToDataChunk(record)
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Result should be *big.Int
	decVal, ok := chunk.GetValue(0, 0).(*big.Int)
	require.True(t, ok)
	assert.Equal(t, int64(1234567), decVal.Int64())
}

// TestRecordBatchToDataChunk_UUID tests UUID conversion.
func TestRecordBatchToDataChunk_UUID(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "uuid_col", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: true},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	// Example UUID bytes
	uuidBytes := []byte{
		0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
		0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
	}
	bldr.Field(0).(*array.FixedSizeBinaryBuilder).Append(uuidBytes)

	record := bldr.NewRecord()
	defer record.Release()

	chunk, err := RecordBatchToDataChunk(record)
	require.NoError(t, err)
	require.NotNil(t, chunk)

	val, ok := chunk.GetValue(0, 0).(dukdb.UUID)
	require.True(t, ok)
	assert.Equal(t, uuidBytes, val[:])
}

// TestRecordBatchToDataChunk_Nulls tests null value handling.
func TestRecordBatchToDataChunk_Nulls(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int_col", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "str_col", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	// Row 0: value, value
	// Row 1: null, value
	// Row 2: value, null
	// Row 3: null, null
	bldr.Field(0).(*array.Int64Builder).Append(100)
	bldr.Field(1).(*array.StringBuilder).Append("hello")

	bldr.Field(0).(*array.Int64Builder).AppendNull()
	bldr.Field(1).(*array.StringBuilder).Append("world")

	bldr.Field(0).(*array.Int64Builder).Append(200)
	bldr.Field(1).(*array.StringBuilder).AppendNull()

	bldr.Field(0).(*array.Int64Builder).AppendNull()
	bldr.Field(1).(*array.StringBuilder).AppendNull()

	record := bldr.NewRecord()
	defer record.Release()

	chunk, err := RecordBatchToDataChunk(record)
	require.NoError(t, err)
	require.NotNil(t, chunk)
	assert.Equal(t, 4, chunk.Count())

	// Row 0
	assert.Equal(t, int64(100), chunk.GetValue(0, 0))
	assert.Equal(t, "hello", chunk.GetValue(0, 1))

	// Row 1 - null int
	assert.False(t, chunk.GetVector(0).Validity().IsValid(1))
	assert.Equal(t, "world", chunk.GetValue(1, 1))

	// Row 2 - null string
	assert.Equal(t, int64(200), chunk.GetValue(2, 0))
	assert.False(t, chunk.GetVector(1).Validity().IsValid(2))

	// Row 3 - both null
	assert.False(t, chunk.GetVector(0).Validity().IsValid(3))
	assert.False(t, chunk.GetVector(1).Validity().IsValid(3))
}

// TestRecordBatchToDataChunk_List tests list/array conversion.
func TestRecordBatchToDataChunk_List(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "list_col", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	listBldr, ok := bldr.Field(0).(*array.ListBuilder)
	require.True(t, ok, "expected *array.ListBuilder")
	valBldr, ok := listBldr.ValueBuilder().(*array.Int64Builder)
	require.True(t, ok, "expected *array.Int64Builder")

	// Row 0: [1, 2, 3]
	listBldr.Append(true)
	valBldr.Append(1)
	valBldr.Append(2)
	valBldr.Append(3)

	// Row 1: [4, 5]
	listBldr.Append(true)
	valBldr.Append(4)
	valBldr.Append(5)

	record := bldr.NewRecord()
	defer record.Release()

	chunk, err := RecordBatchToDataChunk(record)
	require.NoError(t, err)
	require.NotNil(t, chunk)
	assert.Equal(t, 2, chunk.Count())

	// Verify list values
	list0, ok := chunk.GetValue(0, 0).([]any)
	require.True(t, ok)
	assert.Equal(t, 3, len(list0))
	assert.Equal(t, int64(1), list0[0])
	assert.Equal(t, int64(2), list0[1])
	assert.Equal(t, int64(3), list0[2])

	list1, ok := chunk.GetValue(1, 0).([]any)
	require.True(t, ok)
	assert.Equal(t, 2, len(list1))
	assert.Equal(t, int64(4), list1[0])
	assert.Equal(t, int64(5), list1[1])
}

// TestRecordBatchToDataChunk_Struct tests struct conversion.
func TestRecordBatchToDataChunk_Struct(t *testing.T) {
	alloc := memory.NewGoAllocator()

	structType := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "y", Type: arrow.BinaryTypes.String, Nullable: true},
	)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "struct_col", Type: structType, Nullable: true},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	structBldr, ok := bldr.Field(0).(*array.StructBuilder)
	require.True(t, ok, "expected *array.StructBuilder")
	xBldr, ok := structBldr.FieldBuilder(0).(*array.Int32Builder)
	require.True(t, ok, "expected *array.Int32Builder")
	yBldr, ok := structBldr.FieldBuilder(1).(*array.StringBuilder)
	require.True(t, ok, "expected *array.StringBuilder")

	// Row 0: {x: 10, y: "hello"}
	structBldr.Append(true)
	xBldr.Append(10)
	yBldr.Append("hello")

	// Row 1: {x: 20, y: "world"}
	structBldr.Append(true)
	xBldr.Append(20)
	yBldr.Append("world")

	record := bldr.NewRecord()
	defer record.Release()

	chunk, err := RecordBatchToDataChunk(record)
	require.NoError(t, err)
	require.NotNil(t, chunk)
	assert.Equal(t, 2, chunk.Count())

	// Verify struct values
	struct0, ok := chunk.GetValue(0, 0).(map[string]any)
	require.True(t, ok)
	assert.Equal(t, int32(10), struct0["x"])
	assert.Equal(t, "hello", struct0["y"])

	struct1, ok := chunk.GetValue(1, 0).(map[string]any)
	require.True(t, ok)
	assert.Equal(t, int32(20), struct1["x"])
	assert.Equal(t, "world", struct1["y"])
}

// TestRecordBatchToDataChunk_Map tests map conversion.
func TestRecordBatchToDataChunk_Map(t *testing.T) {
	alloc := memory.NewGoAllocator()

	mapType := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "map_col", Type: mapType, Nullable: true},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	mapBldr, ok := bldr.Field(0).(*array.MapBuilder)
	require.True(t, ok, "expected *array.MapBuilder")
	keyBldr, ok := mapBldr.KeyBuilder().(*array.StringBuilder)
	require.True(t, ok, "expected *array.StringBuilder")
	valBldr, ok := mapBldr.ItemBuilder().(*array.Int32Builder)
	require.True(t, ok, "expected *array.Int32Builder")

	// Row 0: {"a": 1, "b": 2}
	mapBldr.Append(true)
	keyBldr.Append("a")
	valBldr.Append(1)
	keyBldr.Append("b")
	valBldr.Append(2)

	// Row 1: {"x": 10}
	mapBldr.Append(true)
	keyBldr.Append("x")
	valBldr.Append(10)

	record := bldr.NewRecord()
	defer record.Release()

	chunk, err := RecordBatchToDataChunk(record)
	require.NoError(t, err)
	require.NotNil(t, chunk)
	assert.Equal(t, 2, chunk.Count())

	// Verify map values
	map0, ok := chunk.GetValue(0, 0).(dukdb.Map)
	require.True(t, ok)
	assert.Equal(t, int32(1), map0["a"])
	assert.Equal(t, int32(2), map0["b"])

	map1, ok := chunk.GetValue(1, 0).(dukdb.Map)
	require.True(t, ok)
	assert.Equal(t, int32(10), map1["x"])
}

// TestDataChunkToRecordBatch_AllTypes tests conversion from DataChunk to RecordBatch.
func TestDataChunkToRecordBatch_AllTypes(t *testing.T) {
	alloc := memory.NewGoAllocator()

	types := []dukdb.Type{
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_UTINYINT,
		dukdb.TYPE_USMALLINT,
		dukdb.TYPE_UINTEGER,
		dukdb.TYPE_UBIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
	}

	chunk := storage.NewDataChunkWithCapacity(types, 2)
	chunk.SetCount(2)

	// Row 0
	chunk.GetVector(0).SetValue(0, true)
	chunk.GetVector(1).SetValue(0, int8(10))
	chunk.GetVector(2).SetValue(0, int16(100))
	chunk.GetVector(3).SetValue(0, int32(1000))
	chunk.GetVector(4).SetValue(0, int64(10000))
	chunk.GetVector(5).SetValue(0, uint8(20))
	chunk.GetVector(6).SetValue(0, uint16(200))
	chunk.GetVector(7).SetValue(0, uint32(2000))
	chunk.GetVector(8).SetValue(0, uint64(20000))
	chunk.GetVector(9).SetValue(0, float32(1.5))
	chunk.GetVector(10).SetValue(0, float64(2.5))
	chunk.GetVector(11).SetValue(0, "hello")

	// Row 1 (with different values)
	chunk.GetVector(0).SetValue(1, false)
	chunk.GetVector(1).SetValue(1, int8(-10))
	chunk.GetVector(2).SetValue(1, int16(-100))
	chunk.GetVector(3).SetValue(1, int32(-1000))
	chunk.GetVector(4).SetValue(1, int64(-10000))
	chunk.GetVector(5).SetValue(1, uint8(30))
	chunk.GetVector(6).SetValue(1, uint16(300))
	chunk.GetVector(7).SetValue(1, uint32(3000))
	chunk.GetVector(8).SetValue(1, uint64(30000))
	chunk.GetVector(9).SetValue(1, float32(-1.5))
	chunk.GetVector(10).SetValue(1, float64(-2.5))
	chunk.GetVector(11).SetValue(1, "world")

	// Build schema
	fields := make([]arrow.Field, len(types))
	for i, typ := range types {
		arrowType, err := DuckDBTypeToArrow(typ)
		require.NoError(t, err)
		fields[i] = arrow.Field{Name: "col" + string(rune('a'+i)), Type: arrowType, Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)

	// Convert
	record, err := DataChunkToRecordBatch(chunk, schema, alloc)
	require.NoError(t, err)
	require.NotNil(t, record)
	defer record.Release()

	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, int64(12), record.NumCols())

	// Verify some values
	boolCol, ok := record.Column(0).(*array.Boolean)
	require.True(t, ok, "expected *array.Boolean")
	assert.Equal(t, true, boolCol.Value(0))
	assert.Equal(t, false, boolCol.Value(1))

	strCol, ok := record.Column(11).(*array.String)
	require.True(t, ok, "expected *array.String")
	assert.Equal(t, "hello", strCol.Value(0))
	assert.Equal(t, "world", strCol.Value(1))
}

// TestDataChunkToRecordBatch_NilHandling tests nil/null value handling.
func TestDataChunkToRecordBatch_NilHandling(t *testing.T) {
	alloc := memory.NewGoAllocator()

	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 3)
	chunk.SetCount(3)

	// Row 0: value, value
	chunk.GetVector(0).SetValue(0, int64(100))
	chunk.GetVector(1).SetValue(0, "hello")

	// Row 1: null, value
	chunk.GetVector(0).SetValue(1, nil)
	chunk.GetVector(1).SetValue(1, "world")

	// Row 2: value, null
	chunk.GetVector(0).SetValue(2, int64(200))
	chunk.GetVector(1).SetValue(2, nil)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	record, err := DataChunkToRecordBatch(chunk, schema, alloc)
	require.NoError(t, err)
	require.NotNil(t, record)
	defer record.Release()

	// Verify nulls
	assert.False(t, record.Column(0).IsNull(0))
	assert.True(t, record.Column(0).IsNull(1))
	assert.False(t, record.Column(0).IsNull(2))

	assert.False(t, record.Column(1).IsNull(0))
	assert.False(t, record.Column(1).IsNull(1))
	assert.True(t, record.Column(1).IsNull(2))
}

// TestRoundTrip_RecordBatch_DataChunk tests full round-trip conversion.
func TestRoundTrip_RecordBatch_DataChunk(t *testing.T) {
	alloc := memory.NewGoAllocator()

	// Create original Arrow record
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	bldr.Field(2).(*array.Float64Builder).AppendValues([]float64{95.5, 88.0, 91.2}, nil)

	originalRecord := bldr.NewRecord()
	defer originalRecord.Release()

	// Convert to DataChunk
	chunk, err := RecordBatchToDataChunk(originalRecord)
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Convert back to RecordBatch
	roundTripRecord, err := DataChunkToRecordBatch(chunk, schema, alloc)
	require.NoError(t, err)
	require.NotNil(t, roundTripRecord)
	defer roundTripRecord.Release()

	// Verify the data matches
	assert.Equal(t, originalRecord.NumRows(), roundTripRecord.NumRows())
	assert.Equal(t, originalRecord.NumCols(), roundTripRecord.NumCols())

	for col := 0; col < int(originalRecord.NumCols()); col++ {
		for row := 0; row < int(originalRecord.NumRows()); row++ {
			assert.Equal(t, originalRecord.Column(col).IsNull(row), roundTripRecord.Column(col).IsNull(row))
			if !originalRecord.Column(col).IsNull(row) {
				assert.Equal(t,
					originalRecord.Column(col).ValueStr(row),
					roundTripRecord.Column(col).ValueStr(row),
					"mismatch at col=%d, row=%d", col, row)
			}
		}
	}
}

// TestRoundTrip_WriteRead_AllTypes tests full write-read round-trip with various types.
func TestRoundTrip_WriteRead_AllTypes(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
	}
	names := []string{"bool", "i8", "i16", "i32", "i64", "f32", "f64", "str"}

	chunk := storage.NewDataChunkWithCapacity(types, 3)
	chunk.SetCount(3)

	// Row 0
	chunk.GetVector(0).SetValue(0, true)
	chunk.GetVector(1).SetValue(0, int8(1))
	chunk.GetVector(2).SetValue(0, int16(10))
	chunk.GetVector(3).SetValue(0, int32(100))
	chunk.GetVector(4).SetValue(0, int64(1000))
	chunk.GetVector(5).SetValue(0, float32(1.1))
	chunk.GetVector(6).SetValue(0, float64(1.11))
	chunk.GetVector(7).SetValue(0, "one")

	// Row 1
	chunk.GetVector(0).SetValue(1, false)
	chunk.GetVector(1).SetValue(1, int8(2))
	chunk.GetVector(2).SetValue(1, int16(20))
	chunk.GetVector(3).SetValue(1, int32(200))
	chunk.GetVector(4).SetValue(1, int64(2000))
	chunk.GetVector(5).SetValue(1, float32(2.2))
	chunk.GetVector(6).SetValue(1, float64(2.22))
	chunk.GetVector(7).SetValue(1, "two")

	// Row 2
	chunk.GetVector(0).SetValue(2, true)
	chunk.GetVector(1).SetValue(2, int8(3))
	chunk.GetVector(2).SetValue(2, int16(30))
	chunk.GetVector(3).SetValue(2, int32(300))
	chunk.GetVector(4).SetValue(2, int64(3000))
	chunk.GetVector(5).SetValue(2, float32(3.3))
	chunk.GetVector(6).SetValue(2, float64(3.33))
	chunk.GetVector(7).SetValue(2, "three")

	// Write to buffer
	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema(names)
	require.NoError(t, err)

	err = writer.SetTypes(types)
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back
	reader, err := NewReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readChunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, readChunk.Count())
	assert.Equal(t, 8, readChunk.ColumnCount())

	// Verify all values
	assert.Equal(t, true, readChunk.GetValue(0, 0))
	assert.Equal(t, int8(1), readChunk.GetValue(0, 1))
	assert.Equal(t, int16(10), readChunk.GetValue(0, 2))
	assert.Equal(t, int32(100), readChunk.GetValue(0, 3))
	assert.Equal(t, int64(1000), readChunk.GetValue(0, 4))
	assert.InDelta(t, float32(1.1), readChunk.GetValue(0, 5), 0.01)
	assert.InDelta(t, 1.11, readChunk.GetValue(0, 6), 0.001)
	assert.Equal(t, "one", readChunk.GetValue(0, 7))

	assert.Equal(t, false, readChunk.GetValue(1, 0))
	assert.Equal(t, "two", readChunk.GetValue(1, 7))

	assert.Equal(t, true, readChunk.GetValue(2, 0))
	assert.Equal(t, "three", readChunk.GetValue(2, 7))
}

// TestRecordBatchToDataChunk_NilRecord tests nil record handling.
func TestRecordBatchToDataChunk_NilRecord(t *testing.T) {
	_, err := RecordBatchToDataChunk(nil)
	assert.Error(t, err)
}

// TestDataChunkToRecordBatch_NilInputs tests nil input handling.
func TestDataChunkToRecordBatch_NilInputs(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{}, nil)
	chunk := storage.NewDataChunkWithCapacity([]dukdb.Type{}, 0)

	_, err := DataChunkToRecordBatch(nil, schema, alloc)
	assert.Error(t, err)

	_, err = DataChunkToRecordBatch(chunk, nil, alloc)
	assert.Error(t, err)

	_, err = DataChunkToRecordBatch(chunk, schema, nil)
	assert.Error(t, err)
}
