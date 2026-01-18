package parquet

import (
	"bytes"
	"io"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// bufferReaderAt and newBufferReaderAt are defined in parquet_test.go

// createTestParquetData creates a test Parquet file in memory with basic types.
func createTestParquetData(t *testing.T) []byte {
	t.Helper()

	type TestRecord struct {
		ID     int64   `parquet:"id"`
		Name   string  `parquet:"name"`
		Value  float64 `parquet:"value"`
		Active bool    `parquet:"active"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[TestRecord](buf)

	records := []TestRecord{
		{ID: 1, Name: "Alice", Value: 100.5, Active: true},
		{ID: 2, Name: "Bob", Value: 200.75, Active: false},
		{ID: 3, Name: "Charlie", Value: 300.0, Active: true},
	}

	n, err := writer.Write(records)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	err = writer.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

// createTestParquetDataWithInts creates a test Parquet file with integer types.
func createTestParquetDataWithInts(t *testing.T) []byte {
	t.Helper()

	type IntRecord struct {
		Int32Col int32 `parquet:"int32_col"`
		Int64Col int64 `parquet:"int64_col"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[IntRecord](buf)

	records := []IntRecord{
		{Int32Col: 10, Int64Col: 100},
		{Int32Col: 20, Int64Col: 200},
		{Int32Col: 30, Int64Col: 300},
	}

	n, err := writer.Write(records)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	err = writer.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

// createLargeTestParquetData creates a Parquet file with many rows for chunking tests.
func createLargeTestParquetData(t *testing.T, numRows int) []byte {
	t.Helper()

	type Record struct {
		ID    int64  `parquet:"id"`
		Value string `parquet:"value"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf)

	records := make([]Record, numRows)
	for i := range numRows {
		records[i] = Record{
			ID:    int64(i),
			Value: "test",
		}
	}

	n, err := writer.Write(records)
	require.NoError(t, err)
	require.Equal(t, numRows, n)

	err = writer.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

func TestReader_NewReader(t *testing.T) {
	data := createTestParquetData(t)
	buf := newBufferReaderAt(data)

	reader, err := NewReader(buf, buf.Size(), nil)
	require.NoError(t, err)
	require.NotNil(t, reader)

	err = reader.Close()
	require.NoError(t, err)
}

func TestReader_Schema(t *testing.T) {
	data := createTestParquetData(t)
	buf := newBufferReaderAt(data)

	reader, err := NewReader(buf, buf.Size(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	require.Len(t, schema, 4)
	require.Contains(t, schema, "id")
	require.Contains(t, schema, "name")
	require.Contains(t, schema, "value")
	require.Contains(t, schema, "active")
}

func TestReader_Types(t *testing.T) {
	data := createTestParquetData(t)
	buf := newBufferReaderAt(data)

	reader, err := NewReader(buf, buf.Size(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	types, err := reader.Types()
	require.NoError(t, err)
	require.Len(t, types, 4)

	// Verify type mapping
	schema, _ := reader.Schema()
	for i, colName := range schema {
		switch colName {
		case "id":
			require.Equal(t, dukdb.TYPE_BIGINT, types[i], "id should be BIGINT")
		case "name":
			require.Equal(t, dukdb.TYPE_VARCHAR, types[i], "name should be VARCHAR")
		case "value":
			require.Equal(t, dukdb.TYPE_DOUBLE, types[i], "value should be DOUBLE")
		case "active":
			require.Equal(t, dukdb.TYPE_BOOLEAN, types[i], "active should be BOOLEAN")
		}
	}
}

func TestReader_ReadChunk(t *testing.T) {
	data := createTestParquetData(t)
	buf := newBufferReaderAt(data)

	reader, err := NewReader(buf, buf.Size(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	require.Equal(t, 3, chunk.Count())
	require.Equal(t, 4, chunk.ColumnCount())
}

func TestReader_ReadChunkValues(t *testing.T) {
	data := createTestParquetData(t)
	buf := newBufferReaderAt(data)

	reader, err := NewReader(buf, buf.Size(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, _ := reader.Schema()
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)

	// Find column indices
	idIdx := -1
	nameIdx := -1
	valueIdx := -1
	activeIdx := -1

	for i, name := range schema {
		switch name {
		case "id":
			idIdx = i
		case "name":
			nameIdx = i
		case "value":
			valueIdx = i
		case "active":
			activeIdx = i
		}
	}

	// Verify first row values
	idVal := chunk.GetValue(0, idIdx)
	require.Equal(t, int64(1), idVal)

	nameVal := chunk.GetValue(0, nameIdx)
	require.Equal(t, "Alice", nameVal)

	valueVal := chunk.GetValue(0, valueIdx)
	require.Equal(t, 100.5, valueVal)

	activeVal := chunk.GetValue(0, activeIdx)
	require.Equal(t, true, activeVal)
}

func TestReader_ReadChunkEOF(t *testing.T) {
	data := createTestParquetData(t)
	buf := newBufferReaderAt(data)

	reader, err := NewReader(buf, buf.Size(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// First read should succeed
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Second read should return EOF
	chunk, err = reader.ReadChunk()
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, chunk)
}

func TestReader_ColumnProjection(t *testing.T) {
	data := createTestParquetData(t)
	buf := newBufferReaderAt(data)

	opts := &ReaderOptions{
		Columns: []string{"id", "name"},
	}

	reader, err := NewReader(buf, buf.Size(), opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	require.Len(t, schema, 2)
	require.Equal(t, "id", schema[0])
	require.Equal(t, "name", schema[1])

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	require.Equal(t, 2, chunk.ColumnCount())
}

func TestReader_ColumnProjectionInvalidColumn(t *testing.T) {
	data := createTestParquetData(t)
	buf := newBufferReaderAt(data)

	opts := &ReaderOptions{
		Columns: []string{"id", "nonexistent"},
	}

	reader, err := NewReader(buf, buf.Size(), opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	_, err = reader.Schema()
	require.Error(t, err)
	require.Contains(t, err.Error(), "nonexistent")
}

func TestReader_MaxRowsPerChunk(t *testing.T) {
	numRows := 100
	data := createLargeTestParquetData(t, numRows)
	buf := newBufferReaderAt(data)

	opts := &ReaderOptions{
		MaxRowsPerChunk: 25,
	}

	reader, err := NewReader(buf, buf.Size(), opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	totalRows := 0
	chunkCount := 0

	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.LessOrEqual(t, chunk.Count(), 25)
		totalRows += chunk.Count()
		chunkCount++
	}

	require.Equal(t, numRows, totalRows)
	require.Equal(t, 4, chunkCount) // 100 rows / 25 per chunk = 4 chunks
}

func TestReader_IntegerTypes(t *testing.T) {
	data := createTestParquetDataWithInts(t)
	buf := newBufferReaderAt(data)

	reader, err := NewReader(buf, buf.Size(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	types, err := reader.Types()
	require.NoError(t, err)

	schema, _ := reader.Schema()
	for i, name := range schema {
		switch name {
		case "int32_col":
			require.Equal(t, dukdb.TYPE_INTEGER, types[i])
		case "int64_col":
			require.Equal(t, dukdb.TYPE_BIGINT, types[i])
		}
	}

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)

	int32Idx := -1
	int64Idx := -1
	for i, name := range schema {
		switch name {
		case "int32_col":
			int32Idx = i
		case "int64_col":
			int64Idx = i
		}
	}

	// Verify values
	require.Equal(t, int32(10), chunk.GetValue(0, int32Idx))
	require.Equal(t, int64(100), chunk.GetValue(0, int64Idx))
}

func TestReader_DefaultOptions(t *testing.T) {
	opts := DefaultReaderOptions()
	require.NotNil(t, opts)
	require.Nil(t, opts.Columns)
	require.Equal(t, DefaultMaxRowsPerChunk, opts.MaxRowsPerChunk)
}

func TestReader_OptionsApplyDefaults(t *testing.T) {
	opts := &ReaderOptions{
		MaxRowsPerChunk: 0, // Should be replaced with default
	}
	opts.applyDefaults()
	require.Equal(t, DefaultMaxRowsPerChunk, opts.MaxRowsPerChunk)
}

func TestParquetTypeToDuckDB_Boolean(t *testing.T) {
	type Record struct {
		Flag bool `parquet:"flag"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, dukdb.TYPE_BOOLEAN, parquetTypeToDuckDB(fields[0]))
}

func TestParquetTypeToDuckDB_Int32(t *testing.T) {
	type Record struct {
		Val int32 `parquet:"val"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, dukdb.TYPE_INTEGER, parquetTypeToDuckDB(fields[0]))
}

func TestParquetTypeToDuckDB_Int64(t *testing.T) {
	type Record struct {
		Val int64 `parquet:"val"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, dukdb.TYPE_BIGINT, parquetTypeToDuckDB(fields[0]))
}

func TestParquetTypeToDuckDB_Float32(t *testing.T) {
	type Record struct {
		Val float32 `parquet:"val"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, dukdb.TYPE_FLOAT, parquetTypeToDuckDB(fields[0]))
}

func TestParquetTypeToDuckDB_Float64(t *testing.T) {
	type Record struct {
		Val float64 `parquet:"val"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, dukdb.TYPE_DOUBLE, parquetTypeToDuckDB(fields[0]))
}

func TestParquetTypeToDuckDB_String(t *testing.T) {
	type Record struct {
		Val string `parquet:"val"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()
	require.Len(t, fields, 1)
	require.Equal(t, dukdb.TYPE_VARCHAR, parquetTypeToDuckDB(fields[0]))
}

func TestParquetTypeToDuckDB_ByteArray(t *testing.T) {
	type Record struct {
		Val []byte `parquet:"val"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()
	require.Len(t, fields, 1)
	// []byte is typically stored as BYTE_ARRAY which maps to BLOB
	typ := parquetTypeToDuckDB(fields[0])
	require.True(t, typ == dukdb.TYPE_BLOB || typ == dukdb.TYPE_VARCHAR,
		"expected BLOB or VARCHAR, got %v", typ)
}

func TestParquetTypeToDuckDB_Timestamp(t *testing.T) {
	type Record struct {
		Val time.Time `parquet:"val,timestamp"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()
	require.Len(t, fields, 1)
	typ := parquetTypeToDuckDB(fields[0])
	// Timestamp can map to various timestamp types
	require.True(t,
		typ == dukdb.TYPE_TIMESTAMP ||
			typ == dukdb.TYPE_TIMESTAMP_MS ||
			typ == dukdb.TYPE_TIMESTAMP_NS ||
			typ == dukdb.TYPE_TIMESTAMP_TZ ||
			typ == dukdb.TYPE_BIGINT, // Some implementations store as int64
		"expected timestamp type, got %v", typ)
}

func TestConvertValueToDuckDB_Boolean(t *testing.T) {
	v := parquet.BooleanValue(true)
	result := convertValueToDuckDB(v, dukdb.TYPE_BOOLEAN)
	require.Equal(t, true, result)
}

func TestConvertValueToDuckDB_Int32(t *testing.T) {
	v := parquet.Int32Value(42)
	result := convertValueToDuckDB(v, dukdb.TYPE_INTEGER)
	require.Equal(t, int32(42), result)
}

func TestConvertValueToDuckDB_Int64(t *testing.T) {
	v := parquet.Int64Value(9223372036854775807)
	result := convertValueToDuckDB(v, dukdb.TYPE_BIGINT)
	require.Equal(t, int64(9223372036854775807), result)
}

func TestConvertValueToDuckDB_Float(t *testing.T) {
	v := parquet.FloatValue(3.14)
	result := convertValueToDuckDB(v, dukdb.TYPE_FLOAT)
	require.InDelta(t, float32(3.14), result, 0.001)
}

func TestConvertValueToDuckDB_Double(t *testing.T) {
	v := parquet.DoubleValue(3.14159265359)
	result := convertValueToDuckDB(v, dukdb.TYPE_DOUBLE)
	require.InDelta(t, 3.14159265359, result, 0.0000001)
}

func TestConvertValueToDuckDB_String(t *testing.T) {
	v := parquet.ByteArrayValue([]byte("hello world"))
	result := convertValueToDuckDB(v, dukdb.TYPE_VARCHAR)
	require.Equal(t, "hello world", result)
}

func TestConvertValueToDuckDB_Blob(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04}
	v := parquet.ByteArrayValue(data)
	result := convertValueToDuckDB(v, dukdb.TYPE_BLOB)
	require.Equal(t, data, result)
}

func TestConvertValueToDuckDB_Null(t *testing.T) {
	v := parquet.NullValue()
	result := convertValueToDuckDB(v, dukdb.TYPE_VARCHAR)
	require.Nil(t, result)
}

func TestConvertValueToDuckDB_TinyInt(t *testing.T) {
	v := parquet.Int32Value(127)
	result := convertValueToDuckDB(v, dukdb.TYPE_TINYINT)
	require.Equal(t, int8(127), result)
}

func TestConvertValueToDuckDB_SmallInt(t *testing.T) {
	v := parquet.Int32Value(32767)
	result := convertValueToDuckDB(v, dukdb.TYPE_SMALLINT)
	require.Equal(t, int16(32767), result)
}

func TestConvertValueToDuckDB_Date(t *testing.T) {
	// Days since epoch: 2024-01-15 = 19737 days
	days := int32(19737)
	v := parquet.Int32Value(days)
	result := convertValueToDuckDB(v, dukdb.TYPE_DATE)
	resultTime, ok := result.(time.Time)
	require.True(t, ok)
	require.Equal(t, 2024, resultTime.Year())
	require.Equal(t, time.January, resultTime.Month())
	require.Equal(t, 15, resultTime.Day())
}

func TestConvertValueToDuckDB_Timestamp(t *testing.T) {
	// Microseconds since epoch: 2024-01-15 12:30:45 UTC
	// = 1705322445 seconds * 1000000 = 1705322445000000 microseconds
	micros := int64(1705322445000000)
	v := parquet.Int64Value(micros)
	result := convertValueToDuckDB(v, dukdb.TYPE_TIMESTAMP)
	resultTime, ok := result.(time.Time)
	require.True(t, ok)
	require.Equal(t, 2024, resultTime.Year())
	require.Equal(t, time.January, resultTime.Month())
	require.Equal(t, 15, resultTime.Day())
}

// ============================================================================
// Nested Type Tests
// ============================================================================

// createTestParquetDataWithList creates a test Parquet file with LIST columns.
func createTestParquetDataWithList(t *testing.T) []byte {
	t.Helper()

	type ListRecord struct {
		ID   int64    `parquet:"id"`
		Tags []string `parquet:"tags"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[ListRecord](buf)

	records := []ListRecord{
		{ID: 1, Tags: []string{"a", "b", "c"}},
		{ID: 2, Tags: []string{"x", "y"}},
		{ID: 3, Tags: []string{}},
	}

	n, err := writer.Write(records)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	err = writer.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

// createTestParquetDataWithStruct creates a test Parquet file with nested STRUCT columns.
func createTestParquetDataWithStruct(t *testing.T) []byte {
	t.Helper()

	type Address struct {
		Street string `parquet:"street"`
		City   string `parquet:"city"`
	}

	type StructRecord struct {
		ID      int64   `parquet:"id"`
		Address Address `parquet:"address"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[StructRecord](buf)

	records := []StructRecord{
		{ID: 1, Address: Address{Street: "123 Main St", City: "NYC"}},
		{ID: 2, Address: Address{Street: "456 Oak Ave", City: "LA"}},
	}

	n, err := writer.Write(records)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	err = writer.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

// createTestParquetDataWithMap creates a test Parquet file with MAP columns.
func createTestParquetDataWithMap(t *testing.T) []byte {
	t.Helper()

	type MapRecord struct {
		ID     int64             `parquet:"id"`
		Labels map[string]string `parquet:"labels"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[MapRecord](buf)

	records := []MapRecord{
		{ID: 1, Labels: map[string]string{"env": "prod", "tier": "web"}},
		{ID: 2, Labels: map[string]string{"env": "dev"}},
	}

	n, err := writer.Write(records)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	err = writer.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

func TestReader_ListColumn(t *testing.T) {
	data := createTestParquetDataWithList(t)
	buf := newBufferReaderAt(data)

	reader, err := NewReader(buf, buf.Size(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Get schema and verify types.
	schema, err := reader.Schema()
	require.NoError(t, err)
	require.Len(t, schema, 2)
	require.Contains(t, schema, "id")
	require.Contains(t, schema, "tags")

	types, err := reader.Types()
	require.NoError(t, err)

	// Find column indices.
	tagsIdx := -1
	for i, name := range schema {
		if name == "tags" {
			tagsIdx = i
		}
	}
	require.NotEqual(t, -1, tagsIdx, "tags column should exist")

	// Tags should be VARCHAR (JSON serialized).
	require.Equal(t, dukdb.TYPE_VARCHAR, types[tagsIdx], "list column should be VARCHAR (JSON)")

	// Read chunk and verify list data.
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	require.Equal(t, 3, chunk.Count())

	// Verify first row's tags value is a string (JSON serialized list).
	tagsVal := chunk.GetValue(0, tagsIdx)
	tagsStr, ok := tagsVal.(string)
	require.True(t, ok, "list value should be a string (JSON)")
	require.NotEmpty(t, tagsStr)
}

func TestReader_StructColumn(t *testing.T) {
	data := createTestParquetDataWithStruct(t)
	buf := newBufferReaderAt(data)

	reader, err := NewReader(buf, buf.Size(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Get schema and verify types.
	schema, err := reader.Schema()
	require.NoError(t, err)
	require.Len(t, schema, 2)
	require.Contains(t, schema, "id")
	require.Contains(t, schema, "address")

	types, err := reader.Types()
	require.NoError(t, err)

	// Find column indices.
	addressIdx := -1
	for i, name := range schema {
		if name == "address" {
			addressIdx = i
		}
	}
	require.NotEqual(t, -1, addressIdx, "address column should exist")

	// Address should be VARCHAR (JSON serialized).
	require.Equal(
		t,
		dukdb.TYPE_VARCHAR,
		types[addressIdx],
		"struct column should be VARCHAR (JSON)",
	)

	// Read chunk and verify struct data.
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	require.Equal(t, 2, chunk.Count())

	// Verify first row's address value is a string (JSON serialized struct).
	addressVal := chunk.GetValue(0, addressIdx)
	addressStr, ok := addressVal.(string)
	require.True(t, ok, "struct value should be a string (JSON)")
	require.NotEmpty(t, addressStr)
}

func TestReader_MapColumn(t *testing.T) {
	data := createTestParquetDataWithMap(t)
	buf := newBufferReaderAt(data)

	reader, err := NewReader(buf, buf.Size(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Get schema and verify types.
	schema, err := reader.Schema()
	require.NoError(t, err)
	require.Len(t, schema, 2)
	require.Contains(t, schema, "id")
	require.Contains(t, schema, "labels")

	types, err := reader.Types()
	require.NoError(t, err)

	// Find column indices.
	labelsIdx := -1
	for i, name := range schema {
		if name == "labels" {
			labelsIdx = i
		}
	}
	require.NotEqual(t, -1, labelsIdx, "labels column should exist")

	// Labels should be VARCHAR (JSON serialized).
	require.Equal(t, dukdb.TYPE_VARCHAR, types[labelsIdx], "map column should be VARCHAR (JSON)")

	// Read chunk and verify map data.
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	require.Equal(t, 2, chunk.Count())

	// Verify first row's labels value is a string (JSON serialized map).
	labelsVal := chunk.GetValue(0, labelsIdx)
	labelsStr, ok := labelsVal.(string)
	require.True(t, ok, "map value should be a string (JSON)")
	require.NotEmpty(t, labelsStr)
}

func TestIsNestedType(t *testing.T) {
	// Test LIST type detection.
	type ListRecord struct {
		Tags []string `parquet:"tags"`
	}
	listSchema := parquet.SchemaOf(new(ListRecord))
	listFields := listSchema.Fields()
	require.Len(t, listFields, 1)
	require.True(t, isNestedType(listFields[0]), "list field should be detected as nested")

	// Test STRUCT type detection.
	type Inner struct {
		Value int32 `parquet:"value"`
	}
	type StructRecord struct {
		Inner Inner `parquet:"inner"`
	}
	structSchema := parquet.SchemaOf(new(StructRecord))
	structFields := structSchema.Fields()
	require.Len(t, structFields, 1)
	require.True(t, isNestedType(structFields[0]), "struct field should be detected as nested")

	// Test MAP type detection.
	type MapRecord struct {
		Labels map[string]string `parquet:"labels"`
	}
	mapSchema := parquet.SchemaOf(new(MapRecord))
	mapFields := mapSchema.Fields()
	require.Len(t, mapFields, 1)
	require.True(t, isNestedType(mapFields[0]), "map field should be detected as nested")

	// Test primitive type is NOT detected as nested.
	type SimpleRecord struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	simpleSchema := parquet.SchemaOf(new(SimpleRecord))
	simpleFields := simpleSchema.Fields()
	require.Len(t, simpleFields, 2)
	require.False(t, isNestedType(simpleFields[0]), "int64 field should NOT be detected as nested")
	require.False(t, isNestedType(simpleFields[1]), "string field should NOT be detected as nested")
}

func TestParquetTypeToDuckDB_ListAsVarchar(t *testing.T) {
	type Record struct {
		Tags []string `parquet:"tags"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()
	require.Len(t, fields, 1)

	// LIST should be mapped to VARCHAR (JSON serialized).
	require.Equal(t, dukdb.TYPE_VARCHAR, parquetTypeToDuckDB(fields[0]))
}

func TestParquetTypeToDuckDB_StructAsVarchar(t *testing.T) {
	type Inner struct {
		Value int32 `parquet:"value"`
	}
	type Record struct {
		Inner Inner `parquet:"inner"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()
	require.Len(t, fields, 1)

	// STRUCT should be mapped to VARCHAR (JSON serialized).
	require.Equal(t, dukdb.TYPE_VARCHAR, parquetTypeToDuckDB(fields[0]))
}

func TestParquetTypeToDuckDB_MapAsVarchar(t *testing.T) {
	type Record struct {
		Labels map[string]string `parquet:"labels"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()
	require.Len(t, fields, 1)

	// MAP should be mapped to VARCHAR (JSON serialized).
	require.Equal(t, dukdb.TYPE_VARCHAR, parquetTypeToDuckDB(fields[0]))
}

func TestExtractBasicValue(t *testing.T) {
	// Test boolean.
	boolVal := parquet.BooleanValue(true)
	require.Equal(t, true, extractBasicValue(boolVal))

	// Test int32.
	int32Val := parquet.Int32Value(42)
	require.Equal(t, int32(42), extractBasicValue(int32Val))

	// Test int64.
	int64Val := parquet.Int64Value(1234567890)
	require.Equal(t, int64(1234567890), extractBasicValue(int64Val))

	// Test float.
	floatVal := parquet.FloatValue(3.14)
	require.InDelta(t, float32(3.14), extractBasicValue(floatVal), 0.001)

	// Test double.
	doubleVal := parquet.DoubleValue(3.14159)
	require.InDelta(t, 3.14159, extractBasicValue(doubleVal), 0.00001)

	// Test string (byte array).
	strVal := parquet.ByteArrayValue([]byte("hello"))
	require.Equal(t, "hello", extractBasicValue(strVal))

	// Test null.
	nullVal := parquet.NullValue()
	require.Nil(t, extractBasicValue(nullVal))
}

func TestConvertNestedValue_Null(t *testing.T) {
	type Record struct {
		Tags []string `parquet:"tags"`
	}
	schema := parquet.SchemaOf(new(Record))
	fields := schema.Fields()

	nullVal := parquet.NullValue()
	result := convertNestedValue(nullVal, fields[0])
	require.Nil(t, result)
}
