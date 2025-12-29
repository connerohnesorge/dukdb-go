package dukdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLType_Primitives(t *testing.T) {
	tests := []struct {
		typ      Type
		expected string
	}{
		{TYPE_BOOLEAN, "BOOLEAN"},
		{TYPE_TINYINT, "TINYINT"},
		{TYPE_SMALLINT, "SMALLINT"},
		{TYPE_INTEGER, "INTEGER"},
		{TYPE_BIGINT, "BIGINT"},
		{TYPE_UTINYINT, "UTINYINT"},
		{TYPE_USMALLINT, "USMALLINT"},
		{TYPE_UINTEGER, "UINTEGER"},
		{TYPE_UBIGINT, "UBIGINT"},
		{TYPE_FLOAT, "FLOAT"},
		{TYPE_DOUBLE, "DOUBLE"},
		{TYPE_HUGEINT, "HUGEINT"},
		{TYPE_UHUGEINT, "UHUGEINT"},
		{TYPE_VARCHAR, "VARCHAR"},
		{TYPE_BLOB, "BLOB"},
		{TYPE_UUID, "UUID"},
		{TYPE_BIT, "BIT"},
		{TYPE_DATE, "DATE"},
		{TYPE_TIME, "TIME"},
		{TYPE_TIME_TZ, "TIMETZ"},
		{TYPE_TIMESTAMP, "TIMESTAMP"},
		{TYPE_TIMESTAMP_S, "TIMESTAMP_S"},
		{TYPE_TIMESTAMP_MS, "TIMESTAMP_MS"},
		{TYPE_TIMESTAMP_NS, "TIMESTAMP_NS"},
		{TYPE_TIMESTAMP_TZ, "TIMESTAMPTZ"},
		{TYPE_INTERVAL, "INTERVAL"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			info, err := NewTypeInfo(tc.typ)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, info.SQLType())
		})
	}
}

func TestSQLType_Decimal(t *testing.T) {
	info, err := NewDecimalInfo(10, 2)
	require.NoError(t, err)
	assert.Equal(t, "DECIMAL(10,2)", info.SQLType())

	info, err = NewDecimalInfo(18, 4)
	require.NoError(t, err)
	assert.Equal(t, "DECIMAL(18,4)", info.SQLType())

	info, err = NewDecimalInfo(38, 0)
	require.NoError(t, err)
	assert.Equal(t, "DECIMAL(38,0)", info.SQLType())
}

func TestSQLType_Enum(t *testing.T) {
	info, err := NewEnumInfo("small", "medium", "large")
	require.NoError(t, err)
	assert.Equal(t, "ENUM('small', 'medium', 'large')", info.SQLType())

	// Test with quotes in values
	info, err = NewEnumInfo("it's", "fine")
	require.NoError(t, err)
	assert.Equal(t, "ENUM('it''s', 'fine')", info.SQLType())
}

func TestSQLType_List(t *testing.T) {
	// Simple list
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	info, err := NewListInfo(intInfo)
	require.NoError(t, err)
	assert.Equal(t, "INTEGER[]", info.SQLType())

	// Nested list (list of lists)
	innerList, _ := NewListInfo(intInfo)
	outerList, err := NewListInfo(innerList)
	require.NoError(t, err)
	assert.Equal(t, "INTEGER[][]", outerList.SQLType())
}

func TestSQLType_Array(t *testing.T) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	info, err := NewArrayInfo(intInfo, 10)
	require.NoError(t, err)
	assert.Equal(t, "INTEGER[10]", info.SQLType())

	varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	info, err = NewArrayInfo(varcharInfo, 5)
	require.NoError(t, err)
	assert.Equal(t, "VARCHAR[5]", info.SQLType())
}

func TestSQLType_Map(t *testing.T) {
	keyInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	valueInfo, _ := NewTypeInfo(TYPE_INTEGER)
	info, err := NewMapInfo(keyInfo, valueInfo)
	require.NoError(t, err)
	assert.Equal(t, "MAP(VARCHAR, INTEGER)", info.SQLType())

	// Nested map value
	innerMap, _ := NewMapInfo(keyInfo, valueInfo)
	outerMap, err := NewMapInfo(keyInfo, innerMap)
	require.NoError(t, err)
	assert.Equal(t, "MAP(VARCHAR, MAP(VARCHAR, INTEGER))", outerMap.SQLType())
}

func TestSQLType_Struct(t *testing.T) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)

	entry1, err := NewStructEntry(intInfo, "id")
	require.NoError(t, err)
	entry2, err := NewStructEntry(varcharInfo, "name")
	require.NoError(t, err)

	info, err := NewStructInfo(entry1, entry2)
	require.NoError(t, err)
	assert.Equal(t, `STRUCT("id" INTEGER, "name" VARCHAR)`, info.SQLType())
}

func TestSQLType_Union(t *testing.T) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)

	info, err := NewUnionInfo(
		[]TypeInfo{intInfo, varcharInfo},
		[]string{"num", "str"},
	)
	require.NoError(t, err)
	assert.Equal(t, `UNION("num" INTEGER, "str" VARCHAR)`, info.SQLType())
}

func TestSQLType_NestedComplex(t *testing.T) {
	// STRUCT(id INTEGER, tags VARCHAR[], metadata MAP(VARCHAR, INTEGER))
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	varcharList, _ := NewListInfo(varcharInfo)
	metadataMap, _ := NewMapInfo(varcharInfo, intInfo)

	entry1, _ := NewStructEntry(intInfo, "id")
	entry2, _ := NewStructEntry(varcharList, "tags")
	entry3, _ := NewStructEntry(metadataMap, "metadata")

	info, err := NewStructInfo(entry1, entry2, entry3)
	require.NoError(t, err)
	assert.Equal(t, `STRUCT("id" INTEGER, "tags" VARCHAR[], "metadata" MAP(VARCHAR, INTEGER))`, info.SQLType())
}
