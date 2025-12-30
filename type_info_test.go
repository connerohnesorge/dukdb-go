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
		// TYPE_UHUGEINT is in unsupportedTypeToStringMap, so it's unsupported via NewTypeInfo
		{TYPE_VARCHAR, "VARCHAR"},
		{TYPE_BLOB, "BLOB"},
		{TYPE_UUID, "UUID"},
		// TYPE_BIT is in unsupportedTypeToStringMap, so it's unsupported via NewTypeInfo
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

// testError is a helper function that checks if an error contains all expected strings.
func testError(t *testing.T, actual error, contains ...string) {
	t.Helper()
	for _, msg := range contains {
		assert.Contains(t, actual.Error(), msg)
	}
}

func TestErrTypeInfo(t *testing.T) {
	// Test complex types require their specific constructors
	incorrectTypes := []Type{
		TYPE_DECIMAL,
		TYPE_ENUM,
		TYPE_LIST,
		TYPE_STRUCT,
		TYPE_MAP,
		TYPE_ARRAY,
		TYPE_UNION,
	}

	for _, incorrect := range incorrectTypes {
		_, err := NewTypeInfo(incorrect)
		testError(t, err, errAPI.Error(), tryOtherFuncErrMsg)
	}

	// Test unsupported types
	var unsupportedTypes []Type
	for k := range unsupportedTypeToStringMap {
		if k != TYPE_ANY {
			unsupportedTypes = append(unsupportedTypes, k)
		}
	}
	unsupportedTypes = append(unsupportedTypes, TYPE_SQLNULL)

	for _, unsupported := range unsupportedTypes {
		_, err := NewTypeInfo(unsupported)
		testError(t, err, errAPI.Error(), unsupportedTypeErrMsg)
	}

	// Test decimal validation
	_, err := NewDecimalInfo(0, 0)
	testError(t, err, errAPI.Error(), errInvalidDecimalWidth.Error())
	_, err = NewDecimalInfo(42, 20)
	testError(t, err, errAPI.Error(), errInvalidDecimalWidth.Error())
	_, err = NewDecimalInfo(5, 6)
	testError(t, err, errAPI.Error(), errInvalidDecimalScale.Error())

	// Test enum duplicate names
	_, err = NewEnumInfo("hello", "hello")
	testError(t, err, errAPI.Error(), duplicateNameErrMsg)
	_, err = NewEnumInfo("hello", "world", "hello")
	testError(t, err, errAPI.Error(), duplicateNameErrMsg)

	validInfo, err := NewTypeInfo(TYPE_FLOAT)
	require.NoError(t, err)

	// Test struct entry with empty name
	_, err = NewStructEntry(validInfo, "")
	testError(t, err, errAPI.Error(), errEmptyName.Error())

	validStructEntry, err := NewStructEntry(validInfo, "hello")
	require.NoError(t, err)
	otherValidStructEntry, err := NewStructEntry(validInfo, "you")
	require.NoError(t, err)
	nilStructEntry, err := NewStructEntry(nil, "hello")
	require.NoError(t, err)

	// Test array with invalid size
	_, err = NewArrayInfo(validInfo, 0)
	testError(t, err, errAPI.Error(), errInvalidArraySize.Error())

	// Test nil parameters
	_, err = NewListInfo(nil)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)

	_, err = NewStructInfo(nil)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
	_, err = NewStructInfo(validStructEntry, nil)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
	_, err = NewStructInfo(nilStructEntry, validStructEntry)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
	_, err = NewStructInfo(validStructEntry, nilStructEntry)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
	_, err = NewStructInfo(validStructEntry, validStructEntry)
	testError(t, err, errAPI.Error(), duplicateNameErrMsg)
	_, err = NewStructInfo(validStructEntry, otherValidStructEntry, validStructEntry)
	testError(t, err, errAPI.Error(), duplicateNameErrMsg)

	_, err = NewMapInfo(nil, validInfo)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
	_, err = NewMapInfo(validInfo, nil)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)

	_, err = NewArrayInfo(nil, 3)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)

	// Test union validation
	unionIntInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)
	unionStringInfo, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)

	_, err = NewUnionInfo([]TypeInfo{}, []string{})
	testError(t, err, errAPI.Error(), "UNION type must have at least one member")

	_, err = NewUnionInfo([]TypeInfo{unionIntInfo, unionStringInfo}, []string{"single_name"})
	testError(t, err, errAPI.Error(), "member types and names must have the same length")

	_, err = NewUnionInfo([]TypeInfo{unionIntInfo}, []string{""})
	testError(t, err, errAPI.Error(), errEmptyName.Error())

	_, err = NewUnionInfo([]TypeInfo{unionIntInfo, unionStringInfo}, []string{"same_name", "same_name"})
	testError(t, err, errAPI.Error(), duplicateNameErrMsg)
}

func TestTypeInfoDetails(t *testing.T) {
	t.Run("PrimitiveTypes", func(t *testing.T) {
		primitiveTypes := []Type{
			TYPE_BOOLEAN, TYPE_INTEGER, TYPE_VARCHAR, TYPE_TIMESTAMP, TYPE_DATE,
		}

		for _, primitiveType := range primitiveTypes {
			info, err := NewTypeInfo(primitiveType)
			require.NoError(t, err)
			require.Nil(t, info.Details())
		}
	})

	t.Run("DecimalDetails", func(t *testing.T) {
		info, err := NewDecimalInfo(10, 3)
		require.NoError(t, err)

		details := info.Details()
		require.NotNil(t, details)

		decimalDetails, ok := details.(*DecimalDetails)
		require.True(t, ok)
		require.Equal(t, uint8(10), decimalDetails.Width)
		require.Equal(t, uint8(3), decimalDetails.Scale)
	})

	t.Run("EnumDetails", func(t *testing.T) {
		info, err := NewEnumInfo("red", "green", "blue")
		require.NoError(t, err)

		details := info.Details()
		require.NotNil(t, details)

		enumDetails, ok := details.(*EnumDetails)
		require.True(t, ok)
		require.Equal(t, []string{"red", "green", "blue"}, enumDetails.Values)

		// Test defensive copy
		enumDetails.Values[0] = "modified"
		details2 := info.Details()
		enumDetails2, ok := details2.(*EnumDetails)
		require.True(t, ok)
		require.Equal(t, "red", enumDetails2.Values[0])
	})

	t.Run("ListDetails", func(t *testing.T) {
		intInfo, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		info, err := NewListInfo(intInfo)
		require.NoError(t, err)

		details := info.Details()
		require.NotNil(t, details)

		listDetails, ok := details.(*ListDetails)
		require.True(t, ok)
		require.Equal(t, TYPE_INTEGER, listDetails.Child.InternalType())
	})

	t.Run("ArrayDetails", func(t *testing.T) {
		varcharInfo, err := NewTypeInfo(TYPE_VARCHAR)
		require.NoError(t, err)

		info, err := NewArrayInfo(varcharInfo, 5)
		require.NoError(t, err)

		details := info.Details()
		require.NotNil(t, details)

		arrayDetails, ok := details.(*ArrayDetails)
		require.True(t, ok)
		require.Equal(t, TYPE_VARCHAR, arrayDetails.Child.InternalType())
		require.Equal(t, uint64(5), arrayDetails.Size)
	})

	t.Run("MapDetails", func(t *testing.T) {
		keyInfo, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)
		valueInfo, err := NewTypeInfo(TYPE_VARCHAR)
		require.NoError(t, err)

		info, err := NewMapInfo(keyInfo, valueInfo)
		require.NoError(t, err)

		details := info.Details()
		require.NotNil(t, details)

		mapDetails, ok := details.(*MapDetails)
		require.True(t, ok)
		require.Equal(t, TYPE_INTEGER, mapDetails.Key.InternalType())
		require.Equal(t, TYPE_VARCHAR, mapDetails.Value.InternalType())
	})

	t.Run("StructDetails", func(t *testing.T) {
		intInfo, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)
		strInfo, err := NewTypeInfo(TYPE_VARCHAR)
		require.NoError(t, err)

		entry1, err := NewStructEntry(intInfo, "id")
		require.NoError(t, err)
		entry2, err := NewStructEntry(strInfo, "name")
		require.NoError(t, err)

		info, err := NewStructInfo(entry1, entry2)
		require.NoError(t, err)

		details := info.Details()
		require.NotNil(t, details)

		structDetails, ok := details.(*StructDetails)
		require.True(t, ok)
		require.Len(t, structDetails.Entries, 2)
		require.Equal(t, "id", structDetails.Entries[0].Name())
		require.Equal(t, TYPE_INTEGER, structDetails.Entries[0].Info().InternalType())
		require.Equal(t, "name", structDetails.Entries[1].Name())
		require.Equal(t, TYPE_VARCHAR, structDetails.Entries[1].Info().InternalType())
	})

	t.Run("UnionDetails", func(t *testing.T) {
		intInfo, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)
		strInfo, err := NewTypeInfo(TYPE_VARCHAR)
		require.NoError(t, err)

		info, err := NewUnionInfo(
			[]TypeInfo{intInfo, strInfo},
			[]string{"num", "text"},
		)
		require.NoError(t, err)

		details := info.Details()
		require.NotNil(t, details)

		unionDetails, ok := details.(*UnionDetails)
		require.True(t, ok)
		require.Len(t, unionDetails.Members, 2)
		require.Equal(t, "num", unionDetails.Members[0].Name)
		require.Equal(t, TYPE_INTEGER, unionDetails.Members[0].Type.InternalType())
		require.Equal(t, "text", unionDetails.Members[1].Name)
		require.Equal(t, TYPE_VARCHAR, unionDetails.Members[1].Type.InternalType())

		// Test defensive copy
		unionDetails.Members[0].Name = "new_name"
		details2 := info.Details()
		require.NotNil(t, details2)
		unionDetails2, ok := details2.(*UnionDetails)
		require.True(t, ok)
		require.Equal(t, "num", unionDetails2.Members[0].Name)
	})

	t.Run("NestedTypeDetails", func(t *testing.T) {
		intInfo, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)
		strInfo, err := NewTypeInfo(TYPE_VARCHAR)
		require.NoError(t, err)

		entry1, err := NewStructEntry(intInfo, "id")
		require.NoError(t, err)
		entry2, err := NewStructEntry(strInfo, "name")
		require.NoError(t, err)

		structInfo, err := NewStructInfo(entry1, entry2)
		require.NoError(t, err)

		listInfo, err := NewListInfo(structInfo)
		require.NoError(t, err)

		details := listInfo.Details()
		require.NotNil(t, details)

		listDetails, ok := details.(*ListDetails)
		require.True(t, ok)
		require.Equal(t, TYPE_STRUCT, listDetails.Child.InternalType())

		structDetails := listDetails.Child.Details()
		require.NotNil(t, structDetails)

		structDetailsTyped, ok := structDetails.(*StructDetails)
		require.True(t, ok)
		require.Len(t, structDetailsTyped.Entries, 2)
	})
}

func TestTypeInfoInternalType(t *testing.T) {
	tests := []struct {
		name     string
		creator  func() (TypeInfo, error)
		expected Type
	}{
		{"Boolean", func() (TypeInfo, error) { return NewTypeInfo(TYPE_BOOLEAN) }, TYPE_BOOLEAN},
		{"Integer", func() (TypeInfo, error) { return NewTypeInfo(TYPE_INTEGER) }, TYPE_INTEGER},
		{"Decimal", func() (TypeInfo, error) { return NewDecimalInfo(10, 2) }, TYPE_DECIMAL},
		{"Enum", func() (TypeInfo, error) { return NewEnumInfo("a", "b") }, TYPE_ENUM},
		{"List", func() (TypeInfo, error) {
			intInfo, _ := NewTypeInfo(TYPE_INTEGER)
			return NewListInfo(intInfo)
		}, TYPE_LIST},
		{"Array", func() (TypeInfo, error) {
			intInfo, _ := NewTypeInfo(TYPE_INTEGER)
			return NewArrayInfo(intInfo, 5)
		}, TYPE_ARRAY},
		{"Map", func() (TypeInfo, error) {
			keyInfo, _ := NewTypeInfo(TYPE_VARCHAR)
			valueInfo, _ := NewTypeInfo(TYPE_INTEGER)
			return NewMapInfo(keyInfo, valueInfo)
		}, TYPE_MAP},
		{"Struct", func() (TypeInfo, error) {
			intInfo, _ := NewTypeInfo(TYPE_INTEGER)
			entry, _ := NewStructEntry(intInfo, "field")
			return NewStructInfo(entry)
		}, TYPE_STRUCT},
		{"Union", func() (TypeInfo, error) {
			intInfo, _ := NewTypeInfo(TYPE_INTEGER)
			return NewUnionInfo([]TypeInfo{intInfo}, []string{"int_val"})
		}, TYPE_UNION},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info, err := tc.creator()
			require.NoError(t, err)
			assert.Equal(t, tc.expected, info.InternalType())
		})
	}
}
