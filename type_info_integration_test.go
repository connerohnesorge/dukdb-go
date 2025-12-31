package dukdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypeInfoSQLTypeIntegration tests SQLType() output for all supported types.
func TestTypeInfoSQLTypeIntegration(
	t *testing.T,
) {
	t.Run("primitive types", func(t *testing.T) {
		testCases := []struct {
			typ     Type
			sqlType string
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
			{TYPE_VARCHAR, "VARCHAR"},
			{TYPE_BLOB, "BLOB"},
			{TYPE_UUID, "UUID"},
			{TYPE_DATE, "DATE"},
			{TYPE_TIME, "TIME"},
			{TYPE_TIME_TZ, "TIMETZ"},
			{TYPE_TIMESTAMP, "TIMESTAMP"},
			{TYPE_TIMESTAMP_S, "TIMESTAMP_S"},
			{TYPE_TIMESTAMP_MS, "TIMESTAMP_MS"},
			{TYPE_TIMESTAMP_NS, "TIMESTAMP_NS"},
			{TYPE_TIMESTAMP_TZ, "TIMESTAMPTZ"},
			{TYPE_INTERVAL, "INTERVAL"},
			{TYPE_ANY, "ANY"},
		}

		for _, tc := range testCases {
			info, err := NewTypeInfo(tc.typ)
			require.NoError(
				t,
				err,
				"NewTypeInfo(%v) should succeed",
				tc.typ,
			)
			assert.Equal(
				t,
				tc.sqlType,
				info.SQLType(),
				"SQLType for %v",
				tc.typ,
			)
		}
	})

	t.Run("DECIMAL types", func(t *testing.T) {
		testCases := []struct {
			width   uint8
			scale   uint8
			sqlType string
		}{
			{10, 2, "DECIMAL(10,2)"},
			{18, 6, "DECIMAL(18,6)"},
			{38, 0, "DECIMAL(38,0)"},
			{5, 5, "DECIMAL(5,5)"},
		}

		for _, tc := range testCases {
			info, err := NewDecimalInfo(
				tc.width,
				tc.scale,
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				tc.sqlType,
				info.SQLType(),
			)
		}
	})

	t.Run("ENUM types", func(t *testing.T) {
		info, err := NewEnumInfo(
			"red",
			"green",
			"blue",
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			"ENUM('red', 'green', 'blue')",
			info.SQLType(),
		)

		// Test escaping
		info, err = NewEnumInfo(
			"it's",
			"a",
			"test",
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			"ENUM('it''s', 'a', 'test')",
			info.SQLType(),
		)
	})

	t.Run("LIST types", func(t *testing.T) {
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		listInfo, err := NewListInfo(intInfo)
		require.NoError(t, err)
		assert.Equal(
			t,
			"INTEGER[]",
			listInfo.SQLType(),
		)

		// Nested list
		nestedList, _ := NewListInfo(listInfo)
		assert.Equal(
			t,
			"INTEGER[][]",
			nestedList.SQLType(),
		)
	})

	t.Run("ARRAY types", func(t *testing.T) {
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		arrayInfo, err := NewArrayInfo(intInfo, 5)
		require.NoError(t, err)
		assert.Equal(
			t,
			"INTEGER[5]",
			arrayInfo.SQLType(),
		)
	})

	t.Run("MAP types", func(t *testing.T) {
		strInfo, _ := NewTypeInfo(TYPE_VARCHAR)
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		mapInfo, err := NewMapInfo(
			strInfo,
			intInfo,
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			"MAP(VARCHAR, INTEGER)",
			mapInfo.SQLType(),
		)
	})

	t.Run("STRUCT types", func(t *testing.T) {
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		strInfo, _ := NewTypeInfo(TYPE_VARCHAR)

		entry1, _ := NewStructEntry(intInfo, "id")
		entry2, _ := NewStructEntry(
			strInfo,
			"name",
		)

		structInfo, err := NewStructInfo(
			entry1,
			entry2,
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			`STRUCT("id" INTEGER, "name" VARCHAR)`,
			structInfo.SQLType(),
		)
	})

	t.Run("UNION types", func(t *testing.T) {
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		strInfo, _ := NewTypeInfo(TYPE_VARCHAR)

		unionInfo, err := NewUnionInfo(
			[]TypeInfo{intInfo, strInfo},
			[]string{"num", "str"},
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			`UNION("num" INTEGER, "str" VARCHAR)`,
			unionInfo.SQLType(),
		)
	})

	t.Run(
		"complex nested types",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			strInfo, _ := NewTypeInfo(
				TYPE_VARCHAR,
			)

			// MAP(VARCHAR, LIST[INTEGER])
			listInfo, _ := NewListInfo(intInfo)
			mapInfo, _ := NewMapInfo(
				strInfo,
				listInfo,
			)
			assert.Equal(
				t,
				"MAP(VARCHAR, INTEGER[])",
				mapInfo.SQLType(),
			)

			// STRUCT(data MAP(VARCHAR, INTEGER[]))
			entry, _ := NewStructEntry(
				mapInfo,
				"data",
			)
			structInfo, _ := NewStructInfo(entry)
			assert.Equal(
				t,
				`STRUCT("data" MAP(VARCHAR, INTEGER[]))`,
				structInfo.SQLType(),
			)
		},
	)
}

// TestTypeInfoRoundTrip tests that TypeInfo can be created and queried correctly.
func TestTypeInfoRoundTrip(t *testing.T) {
	t.Run(
		"primitive type round trip",
		func(t *testing.T) {
			for _, typ := range []Type{
				TYPE_BOOLEAN, TYPE_INTEGER, TYPE_BIGINT, TYPE_DOUBLE,
				TYPE_VARCHAR, TYPE_DATE, TYPE_TIMESTAMP,
			} {
				info, err := NewTypeInfo(typ)
				require.NoError(t, err)
				assert.Equal(
					t,
					typ,
					info.InternalType(),
				)
				assert.Nil(t, info.Details())
			}
		},
	)

	t.Run(
		"DECIMAL round trip",
		func(t *testing.T) {
			info, err := NewDecimalInfo(18, 6)
			require.NoError(t, err)
			assert.Equal(
				t,
				TYPE_DECIMAL,
				info.InternalType(),
			)

			details := info.Details().(*DecimalDetails)
			assert.Equal(
				t,
				uint8(18),
				details.Width,
			)
			assert.Equal(
				t,
				uint8(6),
				details.Scale,
			)
		},
	)

	t.Run("ENUM round trip", func(t *testing.T) {
		info, err := NewEnumInfo(
			"small",
			"medium",
			"large",
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			TYPE_ENUM,
			info.InternalType(),
		)

		details := info.Details().(*EnumDetails)
		assert.Equal(
			t,
			[]string{"small", "medium", "large"},
			details.Values,
		)
	})

	t.Run("LIST round trip", func(t *testing.T) {
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		info, err := NewListInfo(intInfo)
		require.NoError(t, err)
		assert.Equal(
			t,
			TYPE_LIST,
			info.InternalType(),
		)

		details := info.Details().(*ListDetails)
		assert.Equal(
			t,
			TYPE_INTEGER,
			details.Child.InternalType(),
		)
	})

	t.Run("ARRAY round trip", func(t *testing.T) {
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		info, err := NewArrayInfo(intInfo, 10)
		require.NoError(t, err)
		assert.Equal(
			t,
			TYPE_ARRAY,
			info.InternalType(),
		)

		details := info.Details().(*ArrayDetails)
		assert.Equal(
			t,
			TYPE_INTEGER,
			details.Child.InternalType(),
		)
		assert.Equal(t, uint64(10), details.Size)
	})

	t.Run("MAP round trip", func(t *testing.T) {
		strInfo, _ := NewTypeInfo(TYPE_VARCHAR)
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		info, err := NewMapInfo(strInfo, intInfo)
		require.NoError(t, err)
		assert.Equal(
			t,
			TYPE_MAP,
			info.InternalType(),
		)

		details := info.Details().(*MapDetails)
		assert.Equal(
			t,
			TYPE_VARCHAR,
			details.Key.InternalType(),
		)
		assert.Equal(
			t,
			TYPE_INTEGER,
			details.Value.InternalType(),
		)
	})

	t.Run(
		"STRUCT round trip",
		func(t *testing.T) {
			intInfo, _ := NewTypeInfo(
				TYPE_INTEGER,
			)
			strInfo, _ := NewTypeInfo(
				TYPE_VARCHAR,
			)

			entry1, _ := NewStructEntry(
				intInfo,
				"id",
			)
			entry2, _ := NewStructEntry(
				strInfo,
				"name",
			)

			info, err := NewStructInfo(
				entry1,
				entry2,
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				TYPE_STRUCT,
				info.InternalType(),
			)

			details := info.Details().(*StructDetails)
			require.Len(t, details.Entries, 2)
			assert.Equal(
				t,
				"id",
				details.Entries[0].Name(),
			)
			assert.Equal(
				t,
				TYPE_INTEGER,
				details.Entries[0].Info().
					InternalType(),
			)
			assert.Equal(
				t,
				"name",
				details.Entries[1].Name(),
			)
			assert.Equal(
				t,
				TYPE_VARCHAR,
				details.Entries[1].Info().
					InternalType(),
			)
		},
	)

	t.Run("UNION round trip", func(t *testing.T) {
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		strInfo, _ := NewTypeInfo(TYPE_VARCHAR)

		info, err := NewUnionInfo(
			[]TypeInfo{intInfo, strInfo},
			[]string{"num", "str"},
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			TYPE_UNION,
			info.InternalType(),
		)

		details := info.Details().(*UnionDetails)
		require.Len(t, details.Members, 2)
		assert.Equal(
			t,
			"num",
			details.Members[0].Name,
		)
		assert.Equal(
			t,
			TYPE_INTEGER,
			details.Members[0].Type.InternalType(),
		)
		assert.Equal(
			t,
			"str",
			details.Members[1].Name,
		)
		assert.Equal(
			t,
			TYPE_VARCHAR,
			details.Members[1].Type.InternalType(),
		)
	})
}

// TestTypeInfoConcurrency tests that TypeInfo caching is thread-safe.
func TestTypeInfoConcurrency(t *testing.T) {
	ClearTypeInfoCache()

	const goroutines = 100
	done := make(chan TypeInfo, goroutines)

	for range goroutines {
		go func() {
			info, err := NewTypeInfo(TYPE_INTEGER)
			if err != nil {
				t.Errorf(
					"unexpected error: %v",
					err,
				)
			}
			done <- info
		}()
	}

	// Collect all results
	var results []TypeInfo
	for range goroutines {
		results = append(results, <-done)
	}

	// All should be the same cached instance
	first := results[0]
	for i, info := range results {
		assert.Same(
			t,
			first,
			info,
			"goroutine %d returned different instance",
			i,
		)
	}

	// Only one entry should be in the cache
	assert.Equal(t, 1, TypeInfoCacheSize())
}

// TestTypeInfoEquality tests TypeInfo equality behavior.
func TestTypeInfoEquality(t *testing.T) {
	t.Run(
		"cached primitives are same instance",
		func(t *testing.T) {
			ClearTypeInfoCache()

			info1, _ := NewTypeInfo(TYPE_INTEGER)
			info2, _ := NewTypeInfo(TYPE_INTEGER)
			assert.Same(t, info1, info2)
		},
	)

	t.Run(
		"complex types are different instances",
		func(t *testing.T) {
			info1, _ := NewDecimalInfo(10, 2)
			info2, _ := NewDecimalInfo(10, 2)
			assert.NotSame(t, info1, info2)

			// But they have the same values
			d1 := info1.Details().(*DecimalDetails)
			d2 := info2.Details().(*DecimalDetails)
			assert.Equal(t, d1.Width, d2.Width)
			assert.Equal(t, d1.Scale, d2.Scale)
		},
	)
}
