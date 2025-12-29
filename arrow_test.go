package dukdb

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/coder/quartz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupArrowTest registers the mock backend for Arrow tests.
func setupArrowTest(t *testing.T) {
	t.Helper()
	setupTestMockBackend()
}

func TestNewArrowFromConnInvalidType(t *testing.T) {
	t.Parallel()

	// Create a mock driver.Conn that's not a *Conn
	_, err := NewArrowFromConn(&fakeConn{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be a *dukdb.Conn")
}

func TestArrowWithClock(t *testing.T) {
	setupArrowTest(t)

	// Create a connection through the connector
	connector, err := NewConnector(":memory:", nil)
	require.NoError(t, err)

	conn, err := connector.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	arrowConn, err := NewArrowFromConn(conn)
	require.NoError(t, err)

	mockClock := quartz.NewMock(t)
	arrowWithClock := arrowConn.WithClock(mockClock)

	assert.NotSame(t, arrowConn, arrowWithClock)
	assert.Equal(t, mockClock, arrowWithClock.Clock())
}

func TestArrowWithAllocator(t *testing.T) {
	setupArrowTest(t)

	connector, err := NewConnector(":memory:", nil)
	require.NoError(t, err)

	conn, err := connector.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	arrowConn, err := NewArrowFromConn(conn)
	require.NoError(t, err)

	customAlloc := memory.NewGoAllocator()
	arrowWithAlloc := arrowConn.WithAllocator(customAlloc)

	assert.NotSame(t, arrowConn, arrowWithAlloc)
}

func TestArrowContextDeadlineExceeded(t *testing.T) {
	setupArrowTest(t)

	connector, err := NewConnector(":memory:", nil)
	require.NoError(t, err)

	conn, err := connector.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	arrowConn, err := NewArrowFromConn(conn)
	require.NoError(t, err)

	// Use mock clock for deterministic testing
	mockClock := quartz.NewMock(t)
	arrowWithClock := arrowConn.WithClock(mockClock)

	// Set deadline in the past
	deadline := mockClock.Now().Add(-1 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	_, err = arrowWithClock.QueryContext(ctx, "SELECT 1")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDuckDBTypeToArrowPrimitives(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		duckdbType Type
		arrowType  arrow.DataType
	}{
		{TYPE_BOOLEAN, arrow.FixedWidthTypes.Boolean},
		{TYPE_TINYINT, arrow.PrimitiveTypes.Int8},
		{TYPE_SMALLINT, arrow.PrimitiveTypes.Int16},
		{TYPE_INTEGER, arrow.PrimitiveTypes.Int32},
		{TYPE_BIGINT, arrow.PrimitiveTypes.Int64},
		{TYPE_UTINYINT, arrow.PrimitiveTypes.Uint8},
		{TYPE_USMALLINT, arrow.PrimitiveTypes.Uint16},
		{TYPE_UINTEGER, arrow.PrimitiveTypes.Uint32},
		{TYPE_UBIGINT, arrow.PrimitiveTypes.Uint64},
		{TYPE_FLOAT, arrow.PrimitiveTypes.Float32},
		{TYPE_DOUBLE, arrow.PrimitiveTypes.Float64},
		{TYPE_VARCHAR, arrow.BinaryTypes.String},
		{TYPE_BLOB, arrow.BinaryTypes.Binary},
		{TYPE_DATE, arrow.FixedWidthTypes.Date32},
		{TYPE_INTERVAL, arrow.FixedWidthTypes.MonthDayNanoInterval},
		{TYPE_SQLNULL, arrow.Null},
	}

	for _, tc := range testCases {
		t.Run(tc.duckdbType.String(), func(t *testing.T) {
			typeInfo := &typeInfo{typ: tc.duckdbType}
			result, err := duckdbTypeToArrow(typeInfo)
			require.NoError(t, err)
			assert.Equal(t, tc.arrowType, result)
		})
	}
}

func TestDuckDBTypeToArrowTimestamps(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		duckdbType Type
		unit       arrow.TimeUnit
	}{
		{"TIMESTAMP", TYPE_TIMESTAMP, arrow.Microsecond},
		{"TIMESTAMP_TZ", TYPE_TIMESTAMP_TZ, arrow.Microsecond},
		{"TIMESTAMP_S", TYPE_TIMESTAMP_S, arrow.Second},
		{"TIMESTAMP_MS", TYPE_TIMESTAMP_MS, arrow.Millisecond},
		{"TIMESTAMP_NS", TYPE_TIMESTAMP_NS, arrow.Nanosecond},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			typeInfo := &typeInfo{typ: tc.duckdbType}
			result, err := duckdbTypeToArrow(typeInfo)
			require.NoError(t, err)

			tsType, ok := result.(*arrow.TimestampType)
			require.True(t, ok, "expected TimestampType")
			assert.Equal(t, tc.unit, tsType.Unit)
			assert.Equal(t, "UTC", tsType.TimeZone)
		})
	}
}

func TestDuckDBTypeToArrowTime(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		duckdbType Type
	}{
		{"TIME", TYPE_TIME},
		{"TIME_TZ", TYPE_TIME_TZ},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			typeInfo := &typeInfo{typ: tc.duckdbType}
			result, err := duckdbTypeToArrow(typeInfo)
			require.NoError(t, err)
			assert.Equal(t, arrow.FixedWidthTypes.Time64us, result)
		})
	}
}

func TestDuckDBTypeToArrowUUID(t *testing.T) {
	t.Parallel()

	typeInfo := &typeInfo{typ: TYPE_UUID}
	result, err := duckdbTypeToArrow(typeInfo)
	require.NoError(t, err)

	fsb, ok := result.(*arrow.FixedSizeBinaryType)
	require.True(t, ok, "expected FixedSizeBinaryType")
	assert.Equal(t, 16, fsb.ByteWidth)
}

func TestDuckDBTypeToArrowHugeInt(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		duckdbType Type
	}{
		{"HUGEINT", TYPE_HUGEINT},
		{"UHUGEINT", TYPE_UHUGEINT},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			typeInfo := &typeInfo{typ: tc.duckdbType}
			result, err := duckdbTypeToArrow(typeInfo)
			require.NoError(t, err)

			dec, ok := result.(*arrow.Decimal128Type)
			require.True(t, ok, "expected Decimal128Type")
			assert.EqualValues(t, 38, dec.Precision)
			assert.EqualValues(t, 0, dec.Scale)
		})
	}
}

func TestDuckDBTypeToArrowDecimal(t *testing.T) {
	t.Parallel()

	// Create a decimal type info with details
	decInfo, err := NewDecimalInfo(18, 4)
	require.NoError(t, err)

	result, err := duckdbTypeToArrow(decInfo)
	require.NoError(t, err)

	dec, ok := result.(*arrow.Decimal128Type)
	require.True(t, ok, "expected Decimal128Type")
	assert.EqualValues(t, 18, dec.Precision)
	assert.EqualValues(t, 4, dec.Scale)
}

func TestDuckDBTypeToArrowList(t *testing.T) {
	t.Parallel()

	// Create a list type info with integer child
	intInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	listInfo, err := NewListInfo(intInfo)
	require.NoError(t, err)

	result, err := duckdbTypeToArrow(listInfo)
	require.NoError(t, err)

	listType, ok := result.(*arrow.ListType)
	require.True(t, ok, "expected ListType")
	assert.Equal(t, arrow.PrimitiveTypes.Int32, listType.Elem())
}

func TestDuckDBTypeToArrowArray(t *testing.T) {
	t.Parallel()

	// Create an array type info with integer child and fixed size
	intInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	arrayInfo, err := NewArrayInfo(intInfo, 5)
	require.NoError(t, err)

	result, err := duckdbTypeToArrow(arrayInfo)
	require.NoError(t, err)

	fslType, ok := result.(*arrow.FixedSizeListType)
	require.True(t, ok, "expected FixedSizeListType")
	assert.Equal(t, arrow.PrimitiveTypes.Int32, fslType.Elem())
	assert.EqualValues(t, 5, fslType.Len())
}

func TestDuckDBTypeToArrowStruct(t *testing.T) {
	t.Parallel()

	// Create a struct type info with named fields
	intInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	strInfo, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)

	idEntry, err := NewStructEntry(intInfo, "id")
	require.NoError(t, err)

	nameEntry, err := NewStructEntry(strInfo, "name")
	require.NoError(t, err)

	structInfo, err := NewStructInfo(idEntry, nameEntry)
	require.NoError(t, err)

	result, err := duckdbTypeToArrow(structInfo)
	require.NoError(t, err)

	structType, ok := result.(*arrow.StructType)
	require.True(t, ok, "expected StructType")
	require.Len(t, structType.Fields(), 2)
	assert.Equal(t, "id", structType.Field(0).Name)
	assert.Equal(t, arrow.PrimitiveTypes.Int32, structType.Field(0).Type)
	assert.Equal(t, "name", structType.Field(1).Name)
	assert.Equal(t, arrow.BinaryTypes.String, structType.Field(1).Type)
}

func TestDuckDBTypeToArrowMap(t *testing.T) {
	t.Parallel()

	// Create a map type info with string keys and integer values
	keyInfo, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)

	valueInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	mapInfo, err := NewMapInfo(keyInfo, valueInfo)
	require.NoError(t, err)

	result, err := duckdbTypeToArrow(mapInfo)
	require.NoError(t, err)

	mapType, ok := result.(*arrow.MapType)
	require.True(t, ok, "expected MapType")
	assert.Equal(t, arrow.BinaryTypes.String, mapType.KeyType())
	assert.Equal(t, arrow.PrimitiveTypes.Int32, mapType.ItemType())
}

func TestDuckDBTypeToArrowEnum(t *testing.T) {
	t.Parallel()

	// Create an enum type info
	enumInfo, err := NewEnumInfo("small", "medium", "large")
	require.NoError(t, err)

	result, err := duckdbTypeToArrow(enumInfo)
	require.NoError(t, err)

	dictType, ok := result.(*arrow.DictionaryType)
	require.True(t, ok, "expected DictionaryType")
	assert.Equal(t, arrow.PrimitiveTypes.Uint32, dictType.IndexType)
	assert.Equal(t, arrow.BinaryTypes.String, dictType.ValueType)
}

func TestDuckDBTypeToArrowBit(t *testing.T) {
	t.Parallel()

	typeInfo := &typeInfo{typ: TYPE_BIT}
	result, err := duckdbTypeToArrow(typeInfo)
	require.NoError(t, err)
	assert.Equal(t, arrow.BinaryTypes.Binary, result)
}

func TestDuckDBTypeToArrowUnion(t *testing.T) {
	t.Parallel()

	// Create union member types
	intInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	strInfo, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)

	unionInfo, err := NewUnionInfo([]TypeInfo{intInfo, strInfo}, []string{"number", "text"})
	require.NoError(t, err)

	result, err := duckdbTypeToArrow(unionInfo)
	require.NoError(t, err)

	unionType, ok := result.(*arrow.DenseUnionType)
	require.True(t, ok, "expected DenseUnionType")
	require.Len(t, unionType.Fields(), 2)
	assert.Equal(t, "number", unionType.Fields()[0].Name)
	assert.Equal(t, arrow.PrimitiveTypes.Int32, unionType.Fields()[0].Type)
	assert.Equal(t, "text", unionType.Fields()[1].Name)
	assert.Equal(t, arrow.BinaryTypes.String, unionType.Fields()[1].Type)
}

func TestDuckDBTypeToArrowUnsupported(t *testing.T) {
	t.Parallel()

	// Use TYPE_INVALID which is not handled in the Arrow conversion
	typeInfo := &typeInfo{typ: TYPE_INVALID}
	_, err := duckdbTypeToArrow(typeInfo)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
}

func TestArrowRecordReaderInterface(t *testing.T) {
	t.Parallel()

	// Verify that arrowRecordReader implements array.RecordReader
	var _ interface {
		Schema() *arrow.Schema
		Next() bool
		RecordBatch() arrow.RecordBatch
		Record() arrow.RecordBatch
		Err() error
		Retain()
		Release()
	} = (*arrowRecordReader)(nil)
}

// fakeConn implements driver.Conn interface for testing
type fakeConn struct{}

func (f *fakeConn) Prepare(query string) (driver.Stmt, error) { return nil, nil }
func (f *fakeConn) Close() error                              { return nil }
func (f *fakeConn) Begin() (driver.Tx, error)                 { return nil, nil }
