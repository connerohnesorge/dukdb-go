package dukdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypeInfoEndToEnd tests the complete TypeInfo workflow including
// creation, introspection, and SQL generation for all supported types.
func TestTypeInfoEndToEnd(t *testing.T) {
	t.Run("complete workflow for all type categories", func(t *testing.T) {
		// Clear cache to start fresh
		ClearTypeInfoCache()

		// 1. Create TypeInfo for all primitive types
		primitiveTypes := []Type{
			TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INTEGER, TYPE_BIGINT,
			TYPE_UTINYINT, TYPE_USMALLINT, TYPE_UINTEGER, TYPE_UBIGINT,
			TYPE_FLOAT, TYPE_DOUBLE, TYPE_HUGEINT,
			TYPE_VARCHAR, TYPE_BLOB, TYPE_UUID,
			TYPE_DATE, TYPE_TIME, TYPE_TIME_TZ,
			TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ,
			TYPE_INTERVAL, TYPE_ANY,
		}

		for _, typ := range primitiveTypes {
			info, err := NewTypeInfo(typ)
			require.NoError(t, err, "NewTypeInfo(%v) should succeed", typ)
			assert.Equal(t, typ, info.InternalType())
			assert.Nil(t, info.Details(), "Primitive types should have nil Details()")
			assert.NotEmpty(t, info.SQLType(), "SQLType() should return non-empty string")
		}

		// Verify caching worked
		assert.Equal(t, len(primitiveTypes), TypeInfoCacheSize())

		// 2. Create DECIMAL types
		decimalInfo, err := NewDecimalInfo(18, 6)
		require.NoError(t, err)
		assert.Equal(t, TYPE_DECIMAL, decimalInfo.InternalType())
		decDetails := decimalInfo.Details().(*DecimalDetails)
		assert.Equal(t, uint8(18), decDetails.Width)
		assert.Equal(t, uint8(6), decDetails.Scale)
		assert.Equal(t, "DECIMAL(18,6)", decimalInfo.SQLType())

		// 3. Create ENUM type
		enumInfo, err := NewEnumInfo("pending", "active", "completed", "cancelled")
		require.NoError(t, err)
		assert.Equal(t, TYPE_ENUM, enumInfo.InternalType())
		enumDetails := enumInfo.Details().(*EnumDetails)
		assert.Equal(t, []string{"pending", "active", "completed", "cancelled"}, enumDetails.Values)
		assert.Equal(t, "ENUM('pending', 'active', 'completed', 'cancelled')", enumInfo.SQLType())

		// 4. Create LIST type
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		listInfo, err := NewListInfo(intInfo)
		require.NoError(t, err)
		assert.Equal(t, TYPE_LIST, listInfo.InternalType())
		listDetails := listInfo.Details().(*ListDetails)
		assert.Equal(t, TYPE_INTEGER, listDetails.Child.InternalType())
		assert.Equal(t, "INTEGER[]", listInfo.SQLType())

		// 5. Create ARRAY type
		arrayInfo, err := NewArrayInfo(intInfo, 10)
		require.NoError(t, err)
		assert.Equal(t, TYPE_ARRAY, arrayInfo.InternalType())
		arrayDetails := arrayInfo.Details().(*ArrayDetails)
		assert.Equal(t, TYPE_INTEGER, arrayDetails.Child.InternalType())
		assert.Equal(t, uint64(10), arrayDetails.Size)
		assert.Equal(t, "INTEGER[10]", arrayInfo.SQLType())

		// 6. Create MAP type
		strInfo, _ := NewTypeInfo(TYPE_VARCHAR)
		mapInfo, err := NewMapInfo(strInfo, intInfo)
		require.NoError(t, err)
		assert.Equal(t, TYPE_MAP, mapInfo.InternalType())
		mapDetails := mapInfo.Details().(*MapDetails)
		assert.Equal(t, TYPE_VARCHAR, mapDetails.Key.InternalType())
		assert.Equal(t, TYPE_INTEGER, mapDetails.Value.InternalType())
		assert.Equal(t, "MAP(VARCHAR, INTEGER)", mapInfo.SQLType())

		// 7. Create STRUCT type
		idEntry, _ := NewStructEntry(intInfo, "id")
		nameEntry, _ := NewStructEntry(strInfo, "name")
		statusEntry, _ := NewStructEntry(enumInfo, "status")
		structInfo, err := NewStructInfo(idEntry, nameEntry, statusEntry)
		require.NoError(t, err)
		assert.Equal(t, TYPE_STRUCT, structInfo.InternalType())
		structDetails := structInfo.Details().(*StructDetails)
		require.Len(t, structDetails.Entries, 3)
		assert.Equal(t, "id", structDetails.Entries[0].Name())
		assert.Equal(t, "name", structDetails.Entries[1].Name())
		assert.Equal(t, "status", structDetails.Entries[2].Name())

		// 8. Create UNION type
		unionInfo, err := NewUnionInfo(
			[]TypeInfo{intInfo, strInfo},
			[]string{"number", "text"},
		)
		require.NoError(t, err)
		assert.Equal(t, TYPE_UNION, unionInfo.InternalType())
		unionDetails := unionInfo.Details().(*UnionDetails)
		require.Len(t, unionDetails.Members, 2)
		assert.Equal(t, "number", unionDetails.Members[0].Name)
		assert.Equal(t, "text", unionDetails.Members[1].Name)
	})

	t.Run("complex nested type construction", func(t *testing.T) {
		// Build a complex nested type:
		// STRUCT(
		//   id INTEGER,
		//   tags VARCHAR[],
		//   metadata MAP(VARCHAR, STRUCT(key VARCHAR, value INTEGER))
		// )

		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		strInfo, _ := NewTypeInfo(TYPE_VARCHAR)

		// tags: VARCHAR[]
		tagsInfo, err := NewListInfo(strInfo)
		require.NoError(t, err)

		// inner struct: STRUCT(key VARCHAR, value INTEGER)
		keyEntry, _ := NewStructEntry(strInfo, "key")
		valueEntry, _ := NewStructEntry(intInfo, "value")
		innerStructInfo, err := NewStructInfo(keyEntry, valueEntry)
		require.NoError(t, err)

		// metadata: MAP(VARCHAR, STRUCT(key VARCHAR, value INTEGER))
		metadataInfo, err := NewMapInfo(strInfo, innerStructInfo)
		require.NoError(t, err)

		// outer struct
		idEntry, _ := NewStructEntry(intInfo, "id")
		tagsEntry, _ := NewStructEntry(tagsInfo, "tags")
		metadataEntry, _ := NewStructEntry(metadataInfo, "metadata")
		outerStructInfo, err := NewStructInfo(idEntry, tagsEntry, metadataEntry)
		require.NoError(t, err)

		// Verify the SQLType output is valid
		expectedSQL := `STRUCT("id" INTEGER, "tags" VARCHAR[], "metadata" MAP(VARCHAR, STRUCT("key" VARCHAR, "value" INTEGER)))`
		assert.Equal(t, expectedSQL, outerStructInfo.SQLType())

		// Verify we can traverse the structure
		outerDetails := outerStructInfo.Details().(*StructDetails)
		require.Len(t, outerDetails.Entries, 3)

		// Verify tags field
		tagsField := outerDetails.Entries[1]
		assert.Equal(t, "tags", tagsField.Name())
		assert.Equal(t, TYPE_LIST, tagsField.Info().InternalType())

		// Verify metadata field
		metadataField := outerDetails.Entries[2]
		assert.Equal(t, "metadata", metadataField.Name())
		assert.Equal(t, TYPE_MAP, metadataField.Info().InternalType())

		// Traverse into metadata's value type
		metaDetails := metadataField.Info().Details().(*MapDetails)
		assert.Equal(t, TYPE_STRUCT, metaDetails.Value.InternalType())
	})

	t.Run("error handling workflow", func(t *testing.T) {
		// Test that errors are properly returned for invalid inputs

		// Invalid DECIMAL width
		_, err := NewDecimalInfo(0, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "width")

		// Invalid DECIMAL scale
		_, err = NewDecimalInfo(10, 15)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scale")

		// Duplicate ENUM values
		_, err = NewEnumInfo("a", "b", "a")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")

		// Nil childInfo for LIST
		_, err = NewListInfo(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")

		// Empty STRUCT entry name
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		_, err = NewStructEntry(intInfo, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")

		// Duplicate STRUCT field names
		entry1, _ := NewStructEntry(intInfo, "field")
		entry2, _ := NewStructEntry(intInfo, "field")
		_, err = NewStructInfo(entry1, entry2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")

		// UNION with empty members
		_, err = NewUnionInfo([]TypeInfo{}, []string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one")

		// UNION with mismatched lengths
		strInfo, _ := NewTypeInfo(TYPE_VARCHAR)
		_, err = NewUnionInfo([]TypeInfo{intInfo, strInfo}, []string{"a"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "same length")
	})

	t.Run("caching behavior verification", func(t *testing.T) {
		ClearTypeInfoCache()
		assert.Equal(t, 0, TypeInfoCacheSize())

		// Create multiple instances of the same type
		info1, _ := NewTypeInfo(TYPE_INTEGER)
		info2, _ := NewTypeInfo(TYPE_INTEGER)
		info3, _ := NewTypeInfo(TYPE_INTEGER)

		// All should be the same cached instance
		assert.Same(t, info1, info2)
		assert.Same(t, info2, info3)

		// Only one entry in cache
		assert.Equal(t, 1, TypeInfoCacheSize())

		// Different types should be different instances
		varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)
		assert.NotSame(t, info1, varcharInfo)
		assert.Equal(t, 2, TypeInfoCacheSize())

		// Complex types are not cached (new instance each time)
		decimal1, _ := NewDecimalInfo(10, 2)
		decimal2, _ := NewDecimalInfo(10, 2)
		assert.NotSame(t, decimal1, decimal2)

		// Clear and verify
		ClearTypeInfoCache()
		assert.Equal(t, 0, TypeInfoCacheSize())
	})

	t.Run("defensive copy verification", func(t *testing.T) {
		// Verify that Details() returns defensive copies

		// ENUM
		enumInfo, _ := NewEnumInfo("a", "b", "c")
		enumDetails1 := enumInfo.Details().(*EnumDetails)
		enumDetails1.Values[0] = "modified"
		enumDetails2 := enumInfo.Details().(*EnumDetails)
		assert.Equal(t, "a", enumDetails2.Values[0])

		// STRUCT
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		entry1, _ := NewStructEntry(intInfo, "a")
		entry2, _ := NewStructEntry(intInfo, "b")
		structInfo, _ := NewStructInfo(entry1, entry2)
		structDetails1 := structInfo.Details().(*StructDetails)
		originalLen := len(structDetails1.Entries)
		structDetails1.Entries = structDetails1.Entries[:1]
		structDetails2 := structInfo.Details().(*StructDetails)
		assert.Len(t, structDetails2.Entries, originalLen)

		// UNION
		strInfo, _ := NewTypeInfo(TYPE_VARCHAR)
		unionInfo, _ := NewUnionInfo([]TypeInfo{intInfo, strInfo}, []string{"a", "b"})
		unionDetails1 := unionInfo.Details().(*UnionDetails)
		unionDetails1.Members[0].Name = "modified"
		unionDetails2 := unionInfo.Details().(*UnionDetails)
		assert.Equal(t, "a", unionDetails2.Members[0].Name)
	})
}

// TestTypeInfoSQLTypeValidity ensures SQLType() produces valid SQL for all types.
func TestTypeInfoSQLTypeValidity(t *testing.T) {
	testCases := []struct {
		name     string
		setup    func() (TypeInfo, error)
		expected string
	}{
		{
			name:     "INTEGER",
			setup:    func() (TypeInfo, error) { return NewTypeInfo(TYPE_INTEGER) },
			expected: "INTEGER",
		},
		{
			name:     "VARCHAR",
			setup:    func() (TypeInfo, error) { return NewTypeInfo(TYPE_VARCHAR) },
			expected: "VARCHAR",
		},
		{
			name:     "DECIMAL(10,2)",
			setup:    func() (TypeInfo, error) { return NewDecimalInfo(10, 2) },
			expected: "DECIMAL(10,2)",
		},
		{
			name:     "ENUM",
			setup:    func() (TypeInfo, error) { return NewEnumInfo("x", "y") },
			expected: "ENUM('x', 'y')",
		},
		{
			name: "LIST[INTEGER]",
			setup: func() (TypeInfo, error) {
				child, _ := NewTypeInfo(TYPE_INTEGER)

				return NewListInfo(child)
			},
			expected: "INTEGER[]",
		},
		{
			name: "INTEGER[5]",
			setup: func() (TypeInfo, error) {
				child, _ := NewTypeInfo(TYPE_INTEGER)

				return NewArrayInfo(child, 5)
			},
			expected: "INTEGER[5]",
		},
		{
			name: "MAP(VARCHAR, INTEGER)",
			setup: func() (TypeInfo, error) {
				key, _ := NewTypeInfo(TYPE_VARCHAR)
				value, _ := NewTypeInfo(TYPE_INTEGER)

				return NewMapInfo(key, value)
			},
			expected: "MAP(VARCHAR, INTEGER)",
		},
		{
			name: "STRUCT",
			setup: func() (TypeInfo, error) {
				intInfo, _ := NewTypeInfo(TYPE_INTEGER)
				entry, _ := NewStructEntry(intInfo, "field")

				return NewStructInfo(entry)
			},
			expected: `STRUCT("field" INTEGER)`,
		},
		{
			name: "UNION",
			setup: func() (TypeInfo, error) {
				intInfo, _ := NewTypeInfo(TYPE_INTEGER)
				strInfo, _ := NewTypeInfo(TYPE_VARCHAR)

				return NewUnionInfo([]TypeInfo{intInfo, strInfo}, []string{"a", "b"})
			},
			expected: `UNION("a" INTEGER, "b" VARCHAR)`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			info, err := tc.setup()
			require.NoError(t, err)
			assert.Equal(t, tc.expected, info.SQLType())
		})
	}
}
