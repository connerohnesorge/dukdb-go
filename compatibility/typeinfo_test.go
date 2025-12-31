package compatibility

import (
	"testing"

	"github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypeInfoAPICompatibility verifies that the TypeInfo API matches
// the expected interface from the duckdb-go reference implementation.
func TestTypeInfoAPICompatibility(t *testing.T) {
	t.Run(
		"TypeInfo interface methods",
		func(t *testing.T) {
			info, err := dukdb.NewTypeInfo(
				dukdb.TYPE_INTEGER,
			)
			require.NoError(t, err)

			// Verify interface methods exist and work
			typeInfo := info
			assert.Equal(
				t,
				dukdb.TYPE_INTEGER,
				typeInfo.InternalType(),
			)
			assert.Nil(t, typeInfo.Details())
			assert.Equal(
				t,
				"INTEGER",
				typeInfo.SQLType(),
			)
		},
	)

	t.Run(
		"NewTypeInfo for all primitive types",
		func(t *testing.T) {
			primitives := []struct {
				typ     dukdb.Type
				sqlType string
			}{
				{dukdb.TYPE_BOOLEAN, "BOOLEAN"},
				{dukdb.TYPE_TINYINT, "TINYINT"},
				{dukdb.TYPE_SMALLINT, "SMALLINT"},
				{dukdb.TYPE_INTEGER, "INTEGER"},
				{dukdb.TYPE_BIGINT, "BIGINT"},
				{dukdb.TYPE_UTINYINT, "UTINYINT"},
				{
					dukdb.TYPE_USMALLINT,
					"USMALLINT",
				},
				{dukdb.TYPE_UINTEGER, "UINTEGER"},
				{dukdb.TYPE_UBIGINT, "UBIGINT"},
				{dukdb.TYPE_FLOAT, "FLOAT"},
				{dukdb.TYPE_DOUBLE, "DOUBLE"},
				{dukdb.TYPE_HUGEINT, "HUGEINT"},
				{dukdb.TYPE_VARCHAR, "VARCHAR"},
				{dukdb.TYPE_BLOB, "BLOB"},
				{dukdb.TYPE_UUID, "UUID"},
				{dukdb.TYPE_DATE, "DATE"},
				{dukdb.TYPE_TIME, "TIME"},
				{dukdb.TYPE_TIME_TZ, "TIMETZ"},
				{
					dukdb.TYPE_TIMESTAMP,
					"TIMESTAMP",
				},
				{
					dukdb.TYPE_TIMESTAMP_S,
					"TIMESTAMP_S",
				},
				{
					dukdb.TYPE_TIMESTAMP_MS,
					"TIMESTAMP_MS",
				},
				{
					dukdb.TYPE_TIMESTAMP_NS,
					"TIMESTAMP_NS",
				},
				{
					dukdb.TYPE_TIMESTAMP_TZ,
					"TIMESTAMPTZ",
				},
				{dukdb.TYPE_INTERVAL, "INTERVAL"},
			}

			for _, tc := range primitives {
				info, err := dukdb.NewTypeInfo(
					tc.typ,
				)
				require.NoError(
					t,
					err,
					"NewTypeInfo(%v)",
					tc.typ,
				)
				assert.Equal(
					t,
					tc.typ,
					info.InternalType(),
				)
				assert.Equal(
					t,
					tc.sqlType,
					info.SQLType(),
				)
				assert.Nil(t, info.Details())
			}
		},
	)

	t.Run(
		"NewTypeInfo rejects complex types",
		func(t *testing.T) {
			complexTypes := []dukdb.Type{
				dukdb.TYPE_DECIMAL,
				dukdb.TYPE_ENUM,
				dukdb.TYPE_LIST,
				dukdb.TYPE_STRUCT,
				dukdb.TYPE_MAP,
				dukdb.TYPE_ARRAY,
				dukdb.TYPE_UNION,
			}

			for _, typ := range complexTypes {
				_, err := dukdb.NewTypeInfo(typ)
				assert.Error(
					t,
					err,
					"NewTypeInfo(%v) should error",
					typ,
				)
			}
		},
	)

	t.Run(
		"NewDecimalInfo API",
		func(t *testing.T) {
			info, err := dukdb.NewDecimalInfo(
				10,
				2,
			)
			require.NoError(t, err)

			assert.Equal(
				t,
				dukdb.TYPE_DECIMAL,
				info.InternalType(),
			)
			assert.Equal(
				t,
				"DECIMAL(10,2)",
				info.SQLType(),
			)

			details, ok := info.Details().(*dukdb.DecimalDetails)
			require.True(
				t,
				ok,
				"Details should be *DecimalDetails",
			)
			assert.Equal(
				t,
				uint8(10),
				details.Width,
			)
			assert.Equal(
				t,
				uint8(2),
				details.Scale,
			)
		},
	)

	t.Run("NewEnumInfo API", func(t *testing.T) {
		info, err := dukdb.NewEnumInfo(
			"small",
			"medium",
			"large",
		)
		require.NoError(t, err)

		assert.Equal(
			t,
			dukdb.TYPE_ENUM,
			info.InternalType(),
		)
		assert.Equal(
			t,
			"ENUM('small', 'medium', 'large')",
			info.SQLType(),
		)

		details, ok := info.Details().(*dukdb.EnumDetails)
		require.True(
			t,
			ok,
			"Details should be *EnumDetails",
		)
		assert.Equal(
			t,
			[]string{"small", "medium", "large"},
			details.Values,
		)
	})

	t.Run("NewListInfo API", func(t *testing.T) {
		childInfo, _ := dukdb.NewTypeInfo(
			dukdb.TYPE_INTEGER,
		)
		info, err := dukdb.NewListInfo(childInfo)
		require.NoError(t, err)

		assert.Equal(
			t,
			dukdb.TYPE_LIST,
			info.InternalType(),
		)
		assert.Equal(
			t,
			"INTEGER[]",
			info.SQLType(),
		)

		details, ok := info.Details().(*dukdb.ListDetails)
		require.True(
			t,
			ok,
			"Details should be *ListDetails",
		)
		assert.Equal(
			t,
			dukdb.TYPE_INTEGER,
			details.Child.InternalType(),
		)
	})

	t.Run("NewArrayInfo API", func(t *testing.T) {
		childInfo, _ := dukdb.NewTypeInfo(
			dukdb.TYPE_INTEGER,
		)
		info, err := dukdb.NewArrayInfo(
			childInfo,
			5,
		)
		require.NoError(t, err)

		assert.Equal(
			t,
			dukdb.TYPE_ARRAY,
			info.InternalType(),
		)
		assert.Equal(
			t,
			"INTEGER[5]",
			info.SQLType(),
		)

		details, ok := info.Details().(*dukdb.ArrayDetails)
		require.True(
			t,
			ok,
			"Details should be *ArrayDetails",
		)
		assert.Equal(
			t,
			dukdb.TYPE_INTEGER,
			details.Child.InternalType(),
		)
		assert.Equal(t, uint64(5), details.Size)
	})

	t.Run("NewMapInfo API", func(t *testing.T) {
		keyInfo, _ := dukdb.NewTypeInfo(
			dukdb.TYPE_VARCHAR,
		)
		valueInfo, _ := dukdb.NewTypeInfo(
			dukdb.TYPE_INTEGER,
		)
		info, err := dukdb.NewMapInfo(
			keyInfo,
			valueInfo,
		)
		require.NoError(t, err)

		assert.Equal(
			t,
			dukdb.TYPE_MAP,
			info.InternalType(),
		)
		assert.Equal(
			t,
			"MAP(VARCHAR, INTEGER)",
			info.SQLType(),
		)

		details, ok := info.Details().(*dukdb.MapDetails)
		require.True(
			t,
			ok,
			"Details should be *MapDetails",
		)
		assert.Equal(
			t,
			dukdb.TYPE_VARCHAR,
			details.Key.InternalType(),
		)
		assert.Equal(
			t,
			dukdb.TYPE_INTEGER,
			details.Value.InternalType(),
		)
	})

	t.Run(
		"NewStructEntry and NewStructInfo API",
		func(t *testing.T) {
			intInfo, _ := dukdb.NewTypeInfo(
				dukdb.TYPE_INTEGER,
			)
			strInfo, _ := dukdb.NewTypeInfo(
				dukdb.TYPE_VARCHAR,
			)

			entry1, err := dukdb.NewStructEntry(
				intInfo,
				"id",
			)
			require.NoError(t, err)
			assert.Equal(t, "id", entry1.Name())
			assert.Equal(
				t,
				dukdb.TYPE_INTEGER,
				entry1.Info().InternalType(),
			)

			entry2, _ := dukdb.NewStructEntry(
				strInfo,
				"name",
			)

			info, err := dukdb.NewStructInfo(
				entry1,
				entry2,
			)
			require.NoError(t, err)

			assert.Equal(
				t,
				dukdb.TYPE_STRUCT,
				info.InternalType(),
			)
			assert.Equal(
				t,
				`STRUCT("id" INTEGER, "name" VARCHAR)`,
				info.SQLType(),
			)

			details, ok := info.Details().(*dukdb.StructDetails)
			require.True(
				t,
				ok,
				"Details should be *StructDetails",
			)
			require.Len(t, details.Entries, 2)
			assert.Equal(
				t,
				"id",
				details.Entries[0].Name(),
			)
			assert.Equal(
				t,
				"name",
				details.Entries[1].Name(),
			)
		},
	)

	t.Run("NewUnionInfo API", func(t *testing.T) {
		intInfo, _ := dukdb.NewTypeInfo(
			dukdb.TYPE_INTEGER,
		)
		strInfo, _ := dukdb.NewTypeInfo(
			dukdb.TYPE_VARCHAR,
		)

		info, err := dukdb.NewUnionInfo(
			[]dukdb.TypeInfo{intInfo, strInfo},
			[]string{"num", "str"},
		)
		require.NoError(t, err)

		assert.Equal(
			t,
			dukdb.TYPE_UNION,
			info.InternalType(),
		)
		assert.Equal(
			t,
			`UNION("num" INTEGER, "str" VARCHAR)`,
			info.SQLType(),
		)

		details, ok := info.Details().(*dukdb.UnionDetails)
		require.True(
			t,
			ok,
			"Details should be *UnionDetails",
		)
		require.Len(t, details.Members, 2)
		assert.Equal(
			t,
			"num",
			details.Members[0].Name,
		)
		assert.Equal(
			t,
			dukdb.TYPE_INTEGER,
			details.Members[0].Type.InternalType(),
		)
		assert.Equal(
			t,
			"str",
			details.Members[1].Name,
		)
		assert.Equal(
			t,
			dukdb.TYPE_VARCHAR,
			details.Members[1].Type.InternalType(),
		)
	})
}

// TestTypeDetailsAPICompatibility verifies that TypeDetails implementations
// expose the expected fields.
func TestTypeDetailsAPICompatibility(
	t *testing.T,
) {
	t.Run(
		"DecimalDetails fields",
		func(t *testing.T) {
			info, _ := dukdb.NewDecimalInfo(18, 6)
			details := info.Details().(*dukdb.DecimalDetails)

			// Verify struct has expected fields
			_ = details.Width
			_ = details.Scale
		},
	)

	t.Run(
		"EnumDetails fields",
		func(t *testing.T) {
			info, _ := dukdb.NewEnumInfo("a", "b")
			details := info.Details().(*dukdb.EnumDetails)

			// Verify struct has expected fields
			_ = details.Values
		},
	)

	t.Run(
		"ListDetails fields",
		func(t *testing.T) {
			intInfo, _ := dukdb.NewTypeInfo(
				dukdb.TYPE_INTEGER,
			)
			info, _ := dukdb.NewListInfo(intInfo)
			details := info.Details().(*dukdb.ListDetails)

			// Verify struct has expected fields
			_ = details.Child
		},
	)

	t.Run(
		"ArrayDetails fields",
		func(t *testing.T) {
			intInfo, _ := dukdb.NewTypeInfo(
				dukdb.TYPE_INTEGER,
			)
			info, _ := dukdb.NewArrayInfo(
				intInfo,
				10,
			)
			details := info.Details().(*dukdb.ArrayDetails)

			// Verify struct has expected fields
			_ = details.Child
			_ = details.Size
		},
	)

	t.Run(
		"MapDetails fields",
		func(t *testing.T) {
			keyInfo, _ := dukdb.NewTypeInfo(
				dukdb.TYPE_VARCHAR,
			)
			valueInfo, _ := dukdb.NewTypeInfo(
				dukdb.TYPE_INTEGER,
			)
			info, _ := dukdb.NewMapInfo(
				keyInfo,
				valueInfo,
			)
			details := info.Details().(*dukdb.MapDetails)

			// Verify struct has expected fields
			_ = details.Key
			_ = details.Value
		},
	)

	t.Run(
		"StructDetails fields",
		func(t *testing.T) {
			intInfo, _ := dukdb.NewTypeInfo(
				dukdb.TYPE_INTEGER,
			)
			entry, _ := dukdb.NewStructEntry(
				intInfo,
				"field",
			)
			info, _ := dukdb.NewStructInfo(entry)
			details := info.Details().(*dukdb.StructDetails)

			// Verify struct has expected fields
			_ = details.Entries
		},
	)

	t.Run(
		"UnionDetails and UnionMember fields",
		func(t *testing.T) {
			intInfo, _ := dukdb.NewTypeInfo(
				dukdb.TYPE_INTEGER,
			)
			info, _ := dukdb.NewUnionInfo(
				[]dukdb.TypeInfo{intInfo},
				[]string{"a"},
			)
			details := info.Details().(*dukdb.UnionDetails)

			// Verify struct has expected fields
			_ = details.Members

			// Verify UnionMember fields
			member := details.Members[0]
			_ = member.Name
			_ = member.Type
		},
	)
}

// TestStructEntryAPICompatibility verifies the StructEntry interface.
func TestStructEntryAPICompatibility(
	t *testing.T,
) {
	intInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	entry, err := dukdb.NewStructEntry(
		intInfo,
		"test_field",
	)
	require.NoError(t, err)

	// Verify interface methods
	structEntry := entry
	assert.Equal(
		t,
		"test_field",
		structEntry.Name(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		structEntry.Info().InternalType(),
	)
}

// TestTypeInfoCacheAPICompatibility verifies the cache helper functions.
func TestTypeInfoCacheAPICompatibility(
	t *testing.T,
) {
	// These are utility functions for testing, verify they exist
	dukdb.ClearTypeInfoCache()
	size := dukdb.TypeInfoCacheSize()
	assert.Equal(t, 0, size)

	// Create a type to populate cache
	_, err := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, dukdb.TypeInfoCacheSize())
}
