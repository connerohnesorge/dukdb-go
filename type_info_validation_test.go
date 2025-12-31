package dukdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypeInfoValidation tests all validation constraints for TypeInfo constructors.
func TestTypeInfoValidation(t *testing.T) {
	t.Run(
		"NewTypeInfo rejects complex types",
		func(t *testing.T) {
			complexTypes := []Type{
				TYPE_DECIMAL,
				TYPE_ENUM,
				TYPE_LIST,
				TYPE_STRUCT,
				TYPE_MAP,
				TYPE_ARRAY,
				TYPE_UNION,
			}

			for _, typ := range complexTypes {
				_, err := NewTypeInfo(typ)
				require.Error(
					t,
					err,
					"NewTypeInfo(%v) should return error",
					typ,
				)
				assert.Contains(
					t,
					err.Error(),
					"please try this function instead",
				)
			}
		},
	)

	t.Run(
		"NewTypeInfo rejects unsupported types",
		func(t *testing.T) {
			unsupportedTypes := []Type{
				TYPE_INVALID,
				TYPE_BIGNUM,
				TYPE_SQLNULL,
			}

			for _, typ := range unsupportedTypes {
				_, err := NewTypeInfo(typ)
				require.Error(
					t,
					err,
					"NewTypeInfo(%v) should return error",
					typ,
				)
				assert.Contains(
					t,
					err.Error(),
					"unsupported data type",
				)
			}
		},
	)

	t.Run(
		"NewTypeInfo accepts TYPE_ANY",
		func(t *testing.T) {
			info, err := NewTypeInfo(TYPE_ANY)
			require.NoError(t, err)
			assert.Equal(
				t,
				TYPE_ANY,
				info.InternalType(),
			)
		},
	)

	t.Run(
		"NewDecimalInfo validates width",
		func(t *testing.T) {
			// Width must be 1-38
			_, err := NewDecimalInfo(0, 0)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"DECIMAL width must be between 1 and 38",
			)

			_, err = NewDecimalInfo(39, 0)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"DECIMAL width must be between 1 and 38",
			)

			// Valid widths
			for _, width := range []uint8{1, 10, 18, 38} {
				info, err := NewDecimalInfo(
					width,
					0,
				)
				require.NoError(t, err)
				details := info.Details().(*DecimalDetails)
				assert.Equal(
					t,
					width,
					details.Width,
				)
			}
		},
	)

	t.Run(
		"NewDecimalInfo validates scale",
		func(t *testing.T) {
			// Scale cannot exceed width
			_, err := NewDecimalInfo(5, 6)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"DECIMAL scale must be less than or equal to the width",
			)

			_, err = NewDecimalInfo(10, 11)
			require.Error(t, err)

			// Valid scale
			info, err := NewDecimalInfo(10, 5)
			require.NoError(t, err)
			details := info.Details().(*DecimalDetails)
			assert.Equal(
				t,
				uint8(10),
				details.Width,
			)
			assert.Equal(
				t,
				uint8(5),
				details.Scale,
			)

			// Scale can equal width
			info, err = NewDecimalInfo(10, 10)
			require.NoError(t, err)
			details = info.Details().(*DecimalDetails)
			assert.Equal(
				t,
				uint8(10),
				details.Scale,
			)
		},
	)

	t.Run(
		"NewEnumInfo rejects duplicate values",
		func(t *testing.T) {
			_, err := NewEnumInfo("a", "a")
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"duplicate name",
			)

			_, err = NewEnumInfo("a", "b", "a")
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"duplicate name",
			)

			_, err = NewEnumInfo(
				"a",
				"b",
				"c",
				"b",
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"duplicate name",
			)
		},
	)

	t.Run(
		"NewEnumInfo accepts unique values",
		func(t *testing.T) {
			info, err := NewEnumInfo("a")
			require.NoError(t, err)
			details := info.Details().(*EnumDetails)
			assert.Equal(
				t,
				[]string{"a"},
				details.Values,
			)

			info, err = NewEnumInfo("a", "b", "c")
			require.NoError(t, err)
			details = info.Details().(*EnumDetails)
			assert.Equal(
				t,
				[]string{"a", "b", "c"},
				details.Values,
			)
		},
	)

	t.Run(
		"NewStructEntry rejects empty name",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			_, err := NewStructEntry(intInfo, "")
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"empty name",
			)
		},
	)

	t.Run(
		"NewStructEntry accepts nil TypeInfo",
		func(t *testing.T) {
			// NewStructEntry allows nil TypeInfo (validation happens in NewStructInfo)
			entry, err := NewStructEntry(
				nil,
				"field",
			)
			require.NoError(t, err)
			assert.Nil(t, entry.Info())
		},
	)

	t.Run(
		"NewListInfo rejects nil childInfo",
		func(t *testing.T) {
			_, err := NewListInfo(nil)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"interface is nil",
			)
		},
	)

	t.Run(
		"NewArrayInfo rejects nil childInfo",
		func(t *testing.T) {
			_, err := NewArrayInfo(nil, 10)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"interface is nil",
			)
		},
	)

	t.Run(
		"NewArrayInfo rejects zero size",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			_, err := NewArrayInfo(intInfo, 0)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"invalid ARRAY size",
			)
		},
	)

	t.Run(
		"NewArrayInfo accepts valid size",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)

			for _, size := range []uint64{1, 10, 100, 1000} {
				info, err := NewArrayInfo(
					intInfo,
					size,
				)
				require.NoError(t, err)
				details := info.Details().(*ArrayDetails)
				assert.Equal(
					t,
					size,
					details.Size,
				)
			}
		},
	)

	t.Run(
		"NewMapInfo rejects nil keyInfo",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			_, err := NewMapInfo(nil, intInfo)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"interface is nil",
			)
		},
	)

	t.Run(
		"NewMapInfo rejects nil valueInfo",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			_, err := NewMapInfo(intInfo, nil)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"interface is nil",
			)
		},
	)

	t.Run(
		"NewStructInfo rejects nil firstEntry",
		func(t *testing.T) {
			_, err := NewStructInfo(nil)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"interface is nil",
			)
		},
	)

	t.Run(
		"NewStructInfo rejects nil entry in others",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			entry1, _ := NewStructEntry(
				intInfo,
				"a",
			)

			_, err := NewStructInfo(entry1, nil)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"interface is nil",
			)
		},
	)

	t.Run(
		"NewStructInfo rejects nil TypeInfo in entry",
		func(t *testing.T) {
			nilEntry, _ := NewStructEntry(
				nil,
				"a",
			)
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			validEntry, _ := NewStructEntry(
				intInfo,
				"b",
			)

			_, err := NewStructInfo(nilEntry)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"interface is nil",
			)

			_, err = NewStructInfo(
				validEntry,
				nilEntry,
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"interface is nil",
			)
		},
	)

	t.Run(
		"NewStructInfo rejects duplicate field names",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			entry1, _ := NewStructEntry(
				intInfo,
				"field",
			)
			entry2, _ := NewStructEntry(
				intInfo,
				"field",
			)

			_, err := NewStructInfo(
				entry1,
				entry2,
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"duplicate name",
			)
		},
	)

	t.Run(
		"NewUnionInfo rejects empty members",
		func(t *testing.T) {
			_, err := NewUnionInfo(
				[]TypeInfo{},
				[]string{},
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"at least one member",
			)
		},
	)

	t.Run(
		"NewUnionInfo rejects mismatched lengths",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			strInfo, _ := NewTypeInfo(
				TYPE_VARCHAR,
			)

			_, err := NewUnionInfo(
				[]TypeInfo{intInfo, strInfo},
				[]string{"a"},
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"same length",
			)
		},
	)

	t.Run(
		"NewUnionInfo rejects empty member names",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)

			_, err := NewUnionInfo(
				[]TypeInfo{intInfo},
				[]string{""},
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"empty name",
			)
		},
	)

	t.Run(
		"NewUnionInfo rejects duplicate member names",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			strInfo, _ := NewTypeInfo(
				TYPE_VARCHAR,
			)

			_, err := NewUnionInfo(
				[]TypeInfo{intInfo, strInfo},
				[]string{"a", "a"},
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"duplicate name",
			)
		},
	)
}

// TestTypeInfoCaching tests the caching behavior for primitive types.
func TestTypeInfoCaching(t *testing.T) {
	// Clear cache before test
	ClearTypeInfoCache()
	assert.Equal(t, 0, TypeInfoCacheSize())

	t.Run(
		"same type returns same instance",
		func(t *testing.T) {
			ClearTypeInfoCache()

			info1, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			info2, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			// Should be the same pointer (cached)
			assert.Same(t, info1, info2)
		},
	)

	t.Run(
		"different types return different instances",
		func(t *testing.T) {
			ClearTypeInfoCache()

			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			strInfo, _ := NewTypeInfo(
				TYPE_VARCHAR,
			)

			assert.NotSame(t, intInfo, strInfo)
		},
	)

	t.Run(
		"cache size increases with new types",
		func(t *testing.T) {
			ClearTypeInfoCache()
			assert.Equal(
				t,
				0,
				TypeInfoCacheSize(),
			)

			NewTypeInfo(TYPE_INTEGER)
			assert.Equal(
				t,
				1,
				TypeInfoCacheSize(),
			)

			NewTypeInfo(TYPE_VARCHAR)
			assert.Equal(
				t,
				2,
				TypeInfoCacheSize(),
			)

			// Same type doesn't increase cache size
			NewTypeInfo(TYPE_INTEGER)
			assert.Equal(
				t,
				2,
				TypeInfoCacheSize(),
			)
		},
	)

	t.Run(
		"ClearTypeInfoCache clears all entries",
		func(t *testing.T) {
			NewTypeInfo(TYPE_INTEGER)
			NewTypeInfo(TYPE_VARCHAR)
			NewTypeInfo(TYPE_BOOLEAN)

			assert.Greater(
				t,
				TypeInfoCacheSize(),
				0,
			)

			ClearTypeInfoCache()
			assert.Equal(
				t,
				0,
				TypeInfoCacheSize(),
			)
		},
	)
}

// TestTypeInfoDetailsDefensiveCopy verifies that Details() returns defensive copies.
func TestTypeInfoDetailsDefensiveCopy(
	t *testing.T,
) {
	t.Run(
		"EnumDetails returns defensive copy",
		func(t *testing.T) {
			info, _ := NewEnumInfo("a", "b", "c")

			details1 := info.Details().(*EnumDetails)
			details1.Values[0] = "modified"

			details2 := info.Details().(*EnumDetails)
			assert.Equal(
				t,
				"a",
				details2.Values[0],
				"modification should not affect original",
			)
		},
	)

	t.Run(
		"StructDetails returns defensive copy",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			entry1, _ := NewStructEntry(
				intInfo,
				"a",
			)
			entry2, _ := NewStructEntry(
				intInfo,
				"b",
			)

			info, _ := NewStructInfo(
				entry1,
				entry2,
			)

			details1 := info.Details().(*StructDetails)
			originalLen := len(details1.Entries)

			// Modify the returned slice
			details1.Entries = details1.Entries[:1]

			details2 := info.Details().(*StructDetails)
			assert.Len(
				t,
				details2.Entries,
				originalLen,
				"modification should not affect original",
			)
		},
	)

	t.Run(
		"UnionDetails returns defensive copy",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			strInfo, _ := NewTypeInfo(
				TYPE_VARCHAR,
			)

			info, _ := NewUnionInfo(
				[]TypeInfo{intInfo, strInfo},
				[]string{"a", "b"},
			)

			details1 := info.Details().(*UnionDetails)
			details1.Members[0].Name = "modified"

			details2 := info.Details().(*UnionDetails)
			assert.Equal(
				t,
				"a",
				details2.Members[0].Name,
				"modification should not affect original",
			)
		},
	)
}

// TestNestedTypeInfoConstruction tests construction of deeply nested types.
func TestNestedTypeInfoConstruction(
	t *testing.T,
) {
	t.Run(
		"deeply nested list",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)

			// LIST[LIST[LIST[INTEGER]]]
			list1, _ := NewListInfo(intInfo)
			list2, _ := NewListInfo(list1)
			list3, _ := NewListInfo(list2)

			assert.Equal(
				t,
				TYPE_LIST,
				list3.InternalType(),
			)

			details := list3.Details().(*ListDetails)
			assert.Equal(
				t,
				TYPE_LIST,
				details.Child.InternalType(),
			)

			innerDetails := details.Child.Details().(*ListDetails)
			assert.Equal(
				t,
				TYPE_LIST,
				innerDetails.Child.InternalType(),
			)
		},
	)

	t.Run(
		"struct with nested map",
		func(t *testing.T) {
			strInfo, _ := NewTypeInfo(
				TYPE_VARCHAR,
			)
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)

			// MAP[VARCHAR, INTEGER]
			mapInfo, _ := NewMapInfo(
				strInfo,
				intInfo,
			)

			// STRUCT(data MAP[VARCHAR, INTEGER])
			entry, _ := NewStructEntry(
				mapInfo,
				"data",
			)
			structInfo, _ := NewStructInfo(entry)

			assert.Equal(
				t,
				TYPE_STRUCT,
				structInfo.InternalType(),
			)

			structDetails := structInfo.Details().(*StructDetails)
			assert.Len(
				t,
				structDetails.Entries,
				1,
			)
			assert.Equal(
				t,
				TYPE_MAP,
				structDetails.Entries[0].Info().
					InternalType(),
			)
		},
	)

	t.Run(
		"union with complex members",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			listInfo, _ := NewListInfo(intInfo)

			entry, _ := NewStructEntry(
				intInfo,
				"id",
			)
			structInfo, _ := NewStructInfo(entry)

			unionInfo, _ := NewUnionInfo(
				[]TypeInfo{listInfo, structInfo},
				[]string{
					"list_val",
					"struct_val",
				},
			)

			assert.Equal(
				t,
				TYPE_UNION,
				unionInfo.InternalType(),
			)

			details := unionInfo.Details().(*UnionDetails)
			assert.Len(t, details.Members, 2)
			assert.Equal(
				t,
				TYPE_LIST,
				details.Members[0].Type.InternalType(),
			)
			assert.Equal(
				t,
				TYPE_STRUCT,
				details.Members[1].Type.InternalType(),
			)
		},
	)
}
