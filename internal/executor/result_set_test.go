package executor

import (
	"database/sql/driver"
	"io"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResultSet_Columns tests that Columns() returns the correct column names.
func TestResultSet_Columns(t *testing.T) {
	columnNames := []string{"id", "name", "age"}
	var chunks []*storage.DataChunk
	var types []dukdb.TypeInfo

	rs := NewResultSet(chunks, types, columnNames)

	columns := rs.Columns()
	assert.Equal(t, columnNames, columns)
}

// TestResultSet_Next_SingleChunk tests Next() iterating over rows in a single chunk.
func TestResultSet_Next_SingleChunk(
	t *testing.T,
) {
	// Create a chunk with 3 rows and 2 columns (id, name)
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	chunk := storage.NewDataChunk(types)

	// Add rows: (1, "Alice"), (2, "Bob"), (3, "Charlie")
	chunk.AppendRow([]any{int32(1), "Alice"})
	chunk.AppendRow([]any{int32(2), "Bob"})
	chunk.AppendRow([]any{int32(3), "Charlie"})

	chunks := []*storage.DataChunk{chunk}
	columnNames := []string{"id", "name"}
	var typeInfos []dukdb.TypeInfo

	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Test iterating through all rows
	dest := make([]driver.Value, 2)

	// First row
	err := rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dest[0])
	assert.Equal(t, "Alice", dest[1])

	// Second row
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(2), dest[0])
	assert.Equal(t, "Bob", dest[1])

	// Third row
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(3), dest[0])
	assert.Equal(t, "Charlie", dest[1])

	// Fourth call should return io.EOF
	err = rs.Next(dest)
	assert.Equal(t, io.EOF, err)
}

// TestResultSet_Next_MultipleChunks tests Next() iterating across chunk boundaries.
func TestResultSet_Next_MultipleChunks(
	t *testing.T,
) {
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}

	// First chunk with 2 rows
	chunk1 := storage.NewDataChunk(types)
	chunk1.AppendRow([]any{int32(1), "Alice"})
	chunk1.AppendRow([]any{int32(2), "Bob"})

	// Second chunk with 2 rows
	chunk2 := storage.NewDataChunk(types)
	chunk2.AppendRow([]any{int32(3), "Charlie"})
	chunk2.AppendRow([]any{int32(4), "Diana"})

	chunks := []*storage.DataChunk{chunk1, chunk2}
	columnNames := []string{"id", "name"}
	var typeInfos []dukdb.TypeInfo

	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	dest := make([]driver.Value, 2)

	// Iterate through all 4 rows across 2 chunks
	expected := []struct {
		id   int64
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
		{4, "Diana"},
	}

	for i, exp := range expected {
		err := rs.Next(dest)
		require.NoError(t, err, "row %d", i)
		assert.Equal(
			t,
			exp.id,
			dest[0],
			"row %d id",
			i,
		)
		assert.Equal(
			t,
			exp.name,
			dest[1],
			"row %d name",
			i,
		)
	}

	// Should return io.EOF after all rows
	err := rs.Next(dest)
	assert.Equal(t, io.EOF, err)
}

// TestResultSet_Next_EmptyChunks tests handling of empty result sets.
func TestResultSet_Next_EmptyChunks(
	t *testing.T,
) {
	var chunks []*storage.DataChunk
	columnNames := []string{"id", "name"}
	var typeInfos []dukdb.TypeInfo

	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	dest := make([]driver.Value, 2)
	err := rs.Next(dest)
	assert.Equal(t, io.EOF, err)
}

// TestResultSet_Next_NullValues tests handling of NULL values.
func TestResultSet_Next_NullValues(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	chunk := storage.NewDataChunk(types)

	// Add rows with NULL values: (1, NULL), (NULL, "Bob"), (3, "Charlie")
	chunk.AppendRow([]any{int32(1), nil})
	chunk.AppendRow([]any{nil, "Bob"})
	chunk.AppendRow([]any{int32(3), "Charlie"})

	chunks := []*storage.DataChunk{chunk}
	columnNames := []string{"id", "name"}
	var typeInfos []dukdb.TypeInfo

	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	dest := make([]driver.Value, 2)

	// First row: (1, NULL)
	err := rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dest[0])
	assert.Nil(t, dest[1])

	// Second row: (NULL, "Bob")
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Nil(t, dest[0])
	assert.Equal(t, "Bob", dest[1])

	// Third row: (3, "Charlie")
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(3), dest[0])
	assert.Equal(t, "Charlie", dest[1])

	// Should return io.EOF
	err = rs.Next(dest)
	assert.Equal(t, io.EOF, err)
}

// TestResultSet_Close tests that Close() prevents further iteration.
func TestResultSet_Close(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	chunk.AppendRow([]any{int32(1)})

	chunks := []*storage.DataChunk{chunk}
	columnNames := []string{"id"}
	var typeInfos []dukdb.TypeInfo

	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Close the result set
	err := rs.Close()
	require.NoError(t, err)

	// Attempting to iterate should return an error
	dest := make([]driver.Value, 1)
	err = rs.Next(dest)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// TestResultSet_Next_VariousTypes tests conversion of various data types to driver.Value.
func TestResultSet_Next_VariousTypes(
	t *testing.T,
) {
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

	chunk := storage.NewDataChunk(types)
	chunk.AppendRow([]any{
		true,
		int8(10),
		int16(100),
		int32(1000),
		int64(10000),
		float32(1.5),
		float64(2.5),
		"test",
	})

	chunks := []*storage.DataChunk{chunk}
	columnNames := []string{
		"bool",
		"i8",
		"i16",
		"i32",
		"i64",
		"f32",
		"f64",
		"str",
	}
	var typeInfos []dukdb.TypeInfo

	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	dest := make([]driver.Value, 8)
	err := rs.Next(dest)
	require.NoError(t, err)

	// Verify type conversions
	assert.Equal(t, true, dest[0])
	assert.Equal(t, int64(10), dest[1])
	assert.Equal(t, int64(100), dest[2])
	assert.Equal(t, int64(1000), dest[3])
	assert.Equal(t, int64(10000), dest[4])
	assert.Equal(t, float64(1.5), dest[5])
	assert.Equal(t, float64(2.5), dest[6])
	assert.Equal(t, "test", dest[7])
}

// TestResultSet_ImplementsDriverRows verifies compile-time interface compliance.
func TestResultSet_ImplementsDriverRows(
	_ *testing.T,
) {
	var _ driver.Rows = (*ResultSet)(nil)
}

// TestResultSet_Next_DestinationTooSmall tests error handling for insufficient destination slice.
func TestResultSet_Next_DestinationTooSmall(
	t *testing.T,
) {
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	chunk := storage.NewDataChunk(types)
	chunk.AppendRow([]any{int32(1), "Alice"})

	chunks := []*storage.DataChunk{chunk}
	columnNames := []string{"id", "name"}
	var typeInfos []dukdb.TypeInfo

	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Provide a destination slice that's too small
	dest := make([]driver.Value, 1)
	err := rs.Next(dest)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too small")
}

// TestResultSet_Next_EmptyChunkInMiddle tests handling of an empty chunk between non-empty chunks.
func TestResultSet_Next_EmptyChunkInMiddle(
	t *testing.T,
) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	// First chunk with 1 row
	chunk1 := storage.NewDataChunk(types)
	chunk1.AppendRow([]any{int32(1)})

	// Empty chunk in the middle
	chunk2 := storage.NewDataChunk(types)

	// Third chunk with 1 row
	chunk3 := storage.NewDataChunk(types)
	chunk3.AppendRow([]any{int32(3)})

	chunks := []*storage.DataChunk{
		chunk1,
		chunk2,
		chunk3,
	}
	columnNames := []string{"id"}
	var typeInfos []dukdb.TypeInfo

	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	dest := make([]driver.Value, 1)

	// First row from chunk1
	err := rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dest[0])

	// Should skip empty chunk2 and get row from chunk3
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(3), dest[0])

	// Should return io.EOF
	err = rs.Next(dest)
	assert.Equal(t, io.EOF, err)
}

// TestResultSet_ColumnTypeDatabaseTypeName tests the ColumnTypeDatabaseTypeName method.
func TestResultSet_ColumnTypeDatabaseTypeName(
	t *testing.T,
) {
	// Create TypeInfos for various types
	intType, err := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	require.NoError(t, err)

	varcharType, err := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	require.NoError(t, err)

	decimalType, err := dukdb.NewDecimalInfo(
		10,
		2,
	)
	require.NoError(t, err)

	typeInfos := []dukdb.TypeInfo{
		intType,
		varcharType,
		decimalType,
	}
	columnNames := []string{"id", "name", "price"}

	var chunks []*storage.DataChunk
	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Test valid indices
	assert.Equal(
		t,
		"INTEGER",
		rs.ColumnTypeDatabaseTypeName(0),
	)
	assert.Equal(
		t,
		"VARCHAR",
		rs.ColumnTypeDatabaseTypeName(1),
	)
	assert.Equal(
		t,
		"DECIMAL(10,2)",
		rs.ColumnTypeDatabaseTypeName(2),
	)

	// Test out of bounds - negative index
	assert.Equal(
		t,
		"",
		rs.ColumnTypeDatabaseTypeName(-1),
	)

	// Test out of bounds - index too large
	assert.Equal(
		t,
		"",
		rs.ColumnTypeDatabaseTypeName(3),
	)
	assert.Equal(
		t,
		"",
		rs.ColumnTypeDatabaseTypeName(100),
	)
}

// TestResultSet_ColumnTypeDatabaseTypeName_ComplexTypes tests complex type names.
func TestResultSet_ColumnTypeDatabaseTypeName_ComplexTypes(
	t *testing.T,
) {
	// Create a LIST type
	intType, err := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	require.NoError(t, err)

	listType, err := dukdb.NewListInfo(intType)
	require.NoError(t, err)

	// Create an ARRAY type
	arrayType, err := dukdb.NewArrayInfo(
		intType,
		5,
	)
	require.NoError(t, err)

	// Create a MAP type
	varcharType, err := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	require.NoError(t, err)

	mapType, err := dukdb.NewMapInfo(
		varcharType,
		intType,
	)
	require.NoError(t, err)

	typeInfos := []dukdb.TypeInfo{
		listType,
		arrayType,
		mapType,
	}
	columnNames := []string{
		"int_list",
		"int_array",
		"str_int_map",
	}

	var chunks []*storage.DataChunk
	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Test complex type names
	assert.Equal(
		t,
		"INTEGER[]",
		rs.ColumnTypeDatabaseTypeName(0),
	)
	assert.Equal(
		t,
		"INTEGER[5]",
		rs.ColumnTypeDatabaseTypeName(1),
	)
	assert.Equal(
		t,
		"MAP(VARCHAR, INTEGER)",
		rs.ColumnTypeDatabaseTypeName(2),
	)
}

// TestResultSet_ColumnTypeInfo tests the ColumnTypeInfo method.
func TestResultSet_ColumnTypeInfo(t *testing.T) {
	// Create TypeInfos for various types
	intType, err := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	require.NoError(t, err)

	varcharType, err := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	require.NoError(t, err)

	decimalType, err := dukdb.NewDecimalInfo(
		10,
		2,
	)
	require.NoError(t, err)

	typeInfos := []dukdb.TypeInfo{
		intType,
		varcharType,
		decimalType,
	}
	columnNames := []string{"id", "name", "price"}

	var chunks []*storage.DataChunk
	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Test valid indices - verify we get the correct TypeInfo back
	typeInfo0 := rs.ColumnTypeInfo(0)
	require.NotNil(t, typeInfo0)
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		typeInfo0.InternalType(),
	)
	assert.Equal(
		t,
		"INTEGER",
		typeInfo0.SQLType(),
	)
	assert.Nil(t, typeInfo0.Details())

	typeInfo1 := rs.ColumnTypeInfo(1)
	require.NotNil(t, typeInfo1)
	assert.Equal(
		t,
		dukdb.TYPE_VARCHAR,
		typeInfo1.InternalType(),
	)
	assert.Equal(
		t,
		"VARCHAR",
		typeInfo1.SQLType(),
	)

	typeInfo2 := rs.ColumnTypeInfo(2)
	require.NotNil(t, typeInfo2)
	assert.Equal(
		t,
		dukdb.TYPE_DECIMAL,
		typeInfo2.InternalType(),
	)
	assert.Equal(
		t,
		"DECIMAL(10,2)",
		typeInfo2.SQLType(),
	)

	// Check decimal details
	details := typeInfo2.Details()
	require.NotNil(t, details)
	decimalDetails, ok := details.(*dukdb.DecimalDetails)
	require.True(t, ok)
	assert.Equal(
		t,
		uint8(10),
		decimalDetails.Width,
	)
	assert.Equal(
		t,
		uint8(2),
		decimalDetails.Scale,
	)

	// Test out of bounds - negative index
	assert.Nil(t, rs.ColumnTypeInfo(-1))

	// Test out of bounds - index too large
	assert.Nil(t, rs.ColumnTypeInfo(3))
	assert.Nil(t, rs.ColumnTypeInfo(100))
}

// TestResultSet_ColumnTypeInfo_ComplexTypes tests ColumnTypeInfo with complex types.
func TestResultSet_ColumnTypeInfo_ComplexTypes(
	t *testing.T,
) {
	// Create a LIST type
	intType, err := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	require.NoError(t, err)

	listType, err := dukdb.NewListInfo(intType)
	require.NoError(t, err)

	// Create a STRUCT type
	field1, err := dukdb.NewStructEntry(
		intType,
		"x",
	)
	require.NoError(t, err)

	varcharType, err := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	require.NoError(t, err)

	field2, err := dukdb.NewStructEntry(
		varcharType,
		"y",
	)
	require.NoError(t, err)

	structType, err := dukdb.NewStructInfo(
		field1,
		field2,
	)
	require.NoError(t, err)

	typeInfos := []dukdb.TypeInfo{
		listType,
		structType,
	}
	columnNames := []string{"int_list", "point"}

	var chunks []*storage.DataChunk
	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Test LIST type
	listInfo := rs.ColumnTypeInfo(0)
	require.NotNil(t, listInfo)
	assert.Equal(
		t,
		dukdb.TYPE_LIST,
		listInfo.InternalType(),
	)

	listDetails := listInfo.Details()
	require.NotNil(t, listDetails)
	listDetailsTyped, ok := listDetails.(*dukdb.ListDetails)
	require.True(t, ok)
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		listDetailsTyped.Child.InternalType(),
	)

	// Test STRUCT type
	structInfo := rs.ColumnTypeInfo(1)
	require.NotNil(t, structInfo)
	assert.Equal(
		t,
		dukdb.TYPE_STRUCT,
		structInfo.InternalType(),
	)

	structDetails := structInfo.Details()
	require.NotNil(t, structDetails)
	structDetailsTyped, ok := structDetails.(*dukdb.StructDetails)
	require.True(t, ok)
	require.Len(t, structDetailsTyped.Entries, 2)
	assert.Equal(
		t,
		"x",
		structDetailsTyped.Entries[0].Name(),
	)
	assert.Equal(
		t,
		"y",
		structDetailsTyped.Entries[1].Name(),
	)
}

// TestResultSet_ImplementsRowsColumnTypeDatabaseTypeName verifies compile-time interface compliance.
func TestResultSet_ImplementsRowsColumnTypeDatabaseTypeName(
	_ *testing.T,
) {
	var _ driver.RowsColumnTypeDatabaseTypeName = (*ResultSet)(nil)
}
