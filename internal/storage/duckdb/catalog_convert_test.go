package duckdb

import (
	"math"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConvertTypeToDuckDB tests type conversion from dukdb to DuckDB format.
func TestConvertTypeToDuckDB(t *testing.T) {
	tests := []struct {
		name       string
		typ        dukdb.Type
		wantTypeID LogicalTypeID
		wantErr    bool
	}{
		// Boolean
		{"BOOLEAN", dukdb.TYPE_BOOLEAN, TypeBoolean, false},

		// Integer types
		{"TINYINT", dukdb.TYPE_TINYINT, TypeTinyInt, false},
		{"SMALLINT", dukdb.TYPE_SMALLINT, TypeSmallInt, false},
		{"INTEGER", dukdb.TYPE_INTEGER, TypeInteger, false},
		{"BIGINT", dukdb.TYPE_BIGINT, TypeBigInt, false},
		{"HUGEINT", dukdb.TYPE_HUGEINT, TypeHugeInt, false},

		// Unsigned integer types
		{"UTINYINT", dukdb.TYPE_UTINYINT, TypeUTinyInt, false},
		{"USMALLINT", dukdb.TYPE_USMALLINT, TypeUSmallInt, false},
		{"UINTEGER", dukdb.TYPE_UINTEGER, TypeUInteger, false},
		{"UBIGINT", dukdb.TYPE_UBIGINT, TypeUBigInt, false},
		{"UHUGEINT", dukdb.TYPE_UHUGEINT, TypeUHugeInt, false},

		// Floating point types
		{"FLOAT", dukdb.TYPE_FLOAT, TypeFloat, false},
		{"DOUBLE", dukdb.TYPE_DOUBLE, TypeDouble, false},

		// Decimal type
		{"DECIMAL", dukdb.TYPE_DECIMAL, TypeDecimal, false},

		// String types
		{"VARCHAR", dukdb.TYPE_VARCHAR, TypeVarchar, false},
		{"BLOB", dukdb.TYPE_BLOB, TypeBlob, false},
		{"BIT", dukdb.TYPE_BIT, TypeBit, false},

		// Date/Time types
		{"DATE", dukdb.TYPE_DATE, TypeDate, false},
		{"TIME", dukdb.TYPE_TIME, TypeTime, false},
		{"TIME_TZ", dukdb.TYPE_TIME_TZ, TypeTimeTZ, false},
		{"TIMESTAMP", dukdb.TYPE_TIMESTAMP, TypeTimestamp, false},
		{"TIMESTAMP_S", dukdb.TYPE_TIMESTAMP_S, TypeTimestampS, false},
		{"TIMESTAMP_MS", dukdb.TYPE_TIMESTAMP_MS, TypeTimestampMS, false},
		{"TIMESTAMP_NS", dukdb.TYPE_TIMESTAMP_NS, TypeTimestampNS, false},
		{"TIMESTAMP_TZ", dukdb.TYPE_TIMESTAMP_TZ, TypeTimestampTZ, false},
		{"INTERVAL", dukdb.TYPE_INTERVAL, TypeInterval, false},

		// UUID
		{"UUID", dukdb.TYPE_UUID, TypeUUID, false},

		// Nested types
		{"LIST", dukdb.TYPE_LIST, TypeList, false},
		{"STRUCT", dukdb.TYPE_STRUCT, TypeStruct, false},
		{"MAP", dukdb.TYPE_MAP, TypeMap, false},
		{"ARRAY", dukdb.TYPE_ARRAY, TypeArray, false},
		{"UNION", dukdb.TYPE_UNION, TypeUnion, false},

		// Enum
		{"ENUM", dukdb.TYPE_ENUM, TypeEnum, false},

		// Other types
		{"ANY", dukdb.TYPE_ANY, TypeAny, false},
		{"SQLNULL", dukdb.TYPE_SQLNULL, TypeSQLNull, false},
		{"GEOMETRY", dukdb.TYPE_GEOMETRY, TypeGeometry, false},
		{"LAMBDA", dukdb.TYPE_LAMBDA, TypeLambda, false},
		{"VARIANT", dukdb.TYPE_VARIANT, TypeVariant, false},
		{"BIGNUM", dukdb.TYPE_BIGNUM, TypeBigNum, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typeID, mods, err := ConvertTypeToDuckDB(tt.typ)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantTypeID, typeID)
			assert.NotNil(t, mods)
		})
	}
}

// TestConvertTypeFromDuckDB tests type conversion from DuckDB to dukdb format.
func TestConvertTypeFromDuckDB(t *testing.T) {
	tests := []struct {
		name    string
		typeID  LogicalTypeID
		mods    *TypeModifiers
		want    dukdb.Type
		wantErr bool
	}{
		// Special types
		{"INVALID", TypeInvalid, nil, dukdb.TYPE_INVALID, false},
		{"SQLNULL", TypeSQLNull, nil, dukdb.TYPE_SQLNULL, false},
		{"ANY", TypeAny, nil, dukdb.TYPE_ANY, false},

		// Boolean
		{"BOOLEAN", TypeBoolean, nil, dukdb.TYPE_BOOLEAN, false},

		// Integer types
		{"TINYINT", TypeTinyInt, nil, dukdb.TYPE_TINYINT, false},
		{"SMALLINT", TypeSmallInt, nil, dukdb.TYPE_SMALLINT, false},
		{"INTEGER", TypeInteger, nil, dukdb.TYPE_INTEGER, false},
		{"BIGINT", TypeBigInt, nil, dukdb.TYPE_BIGINT, false},
		{"HUGEINT", TypeHugeInt, nil, dukdb.TYPE_HUGEINT, false},

		// Unsigned integer types
		{"UTINYINT", TypeUTinyInt, nil, dukdb.TYPE_UTINYINT, false},
		{"USMALLINT", TypeUSmallInt, nil, dukdb.TYPE_USMALLINT, false},
		{"UINTEGER", TypeUInteger, nil, dukdb.TYPE_UINTEGER, false},
		{"UBIGINT", TypeUBigInt, nil, dukdb.TYPE_UBIGINT, false},
		{"UHUGEINT", TypeUHugeInt, nil, dukdb.TYPE_UHUGEINT, false},

		// Floating point types
		{"FLOAT", TypeFloat, nil, dukdb.TYPE_FLOAT, false},
		{"DOUBLE", TypeDouble, nil, dukdb.TYPE_DOUBLE, false},

		// Decimal type
		{"DECIMAL", TypeDecimal, nil, dukdb.TYPE_DECIMAL, false},

		// String types
		{"CHAR", TypeChar, nil, dukdb.TYPE_VARCHAR, false}, // CHAR maps to VARCHAR
		{"VARCHAR", TypeVarchar, nil, dukdb.TYPE_VARCHAR, false},
		{"BLOB", TypeBlob, nil, dukdb.TYPE_BLOB, false},
		{"BIT", TypeBit, nil, dukdb.TYPE_BIT, false},

		// Date/Time types
		{"DATE", TypeDate, nil, dukdb.TYPE_DATE, false},
		{"TIME", TypeTime, nil, dukdb.TYPE_TIME, false},
		{"TIME_NS", TypeTimeNS, nil, dukdb.TYPE_TIME, false}, // TIME_NS maps to TIME
		{"TIME_TZ", TypeTimeTZ, nil, dukdb.TYPE_TIME_TZ, false},
		{"TIMESTAMP", TypeTimestamp, nil, dukdb.TYPE_TIMESTAMP, false},
		{"TIMESTAMP_S", TypeTimestampS, nil, dukdb.TYPE_TIMESTAMP_S, false},
		{"TIMESTAMP_MS", TypeTimestampMS, nil, dukdb.TYPE_TIMESTAMP_MS, false},
		{"TIMESTAMP_NS", TypeTimestampNS, nil, dukdb.TYPE_TIMESTAMP_NS, false},
		{"TIMESTAMP_TZ", TypeTimestampTZ, nil, dukdb.TYPE_TIMESTAMP_TZ, false},
		{"INTERVAL", TypeInterval, nil, dukdb.TYPE_INTERVAL, false},

		// UUID
		{"UUID", TypeUUID, nil, dukdb.TYPE_UUID, false},

		// Nested types
		{"LIST", TypeList, nil, dukdb.TYPE_LIST, false},
		{"STRUCT", TypeStruct, nil, dukdb.TYPE_STRUCT, false},
		{"MAP", TypeMap, nil, dukdb.TYPE_MAP, false},
		{"ARRAY", TypeArray, nil, dukdb.TYPE_ARRAY, false},
		{"UNION", TypeUnion, nil, dukdb.TYPE_UNION, false},

		// Enum
		{"ENUM", TypeEnum, nil, dukdb.TYPE_ENUM, false},

		// Other types
		{"GEOMETRY", TypeGeometry, nil, dukdb.TYPE_GEOMETRY, false},
		{"LAMBDA", TypeLambda, nil, dukdb.TYPE_LAMBDA, false},
		{"VARIANT", TypeVariant, nil, dukdb.TYPE_VARIANT, false},
		{"BIGNUM", TypeBigNum, nil, dukdb.TYPE_BIGNUM, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertTypeFromDuckDB(tt.typeID, tt.mods)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestTypeRoundTrip tests that types can be converted to DuckDB format and back.
func TestTypeRoundTrip(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_HUGEINT,
		dukdb.TYPE_UTINYINT,
		dukdb.TYPE_USMALLINT,
		dukdb.TYPE_UINTEGER,
		dukdb.TYPE_UBIGINT,
		dukdb.TYPE_UHUGEINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_DECIMAL,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BLOB,
		dukdb.TYPE_BIT,
		dukdb.TYPE_DATE,
		dukdb.TYPE_TIME,
		dukdb.TYPE_TIME_TZ,
		dukdb.TYPE_TIMESTAMP,
		dukdb.TYPE_TIMESTAMP_S,
		dukdb.TYPE_TIMESTAMP_MS,
		dukdb.TYPE_TIMESTAMP_NS,
		dukdb.TYPE_TIMESTAMP_TZ,
		dukdb.TYPE_INTERVAL,
		dukdb.TYPE_UUID,
		dukdb.TYPE_LIST,
		dukdb.TYPE_STRUCT,
		dukdb.TYPE_MAP,
		dukdb.TYPE_ARRAY,
		dukdb.TYPE_UNION,
		dukdb.TYPE_ENUM,
		dukdb.TYPE_ANY,
		dukdb.TYPE_SQLNULL,
		dukdb.TYPE_GEOMETRY,
		dukdb.TYPE_LAMBDA,
		dukdb.TYPE_VARIANT,
		dukdb.TYPE_BIGNUM,
	}

	for _, typ := range types {
		t.Run(typ.String(), func(t *testing.T) {
			// Convert to DuckDB format
			typeID, mods, err := ConvertTypeToDuckDB(typ)
			require.NoError(t, err)

			// Convert back
			got, err := ConvertTypeFromDuckDB(typeID, mods)
			require.NoError(t, err)

			// Verify round trip
			assert.Equal(t, typ, got, "round trip failed for %s", typ.String())
		})
	}
}

// TestConvertColumnToDuckDB tests column conversion.
func TestConvertColumnToDuckDB(t *testing.T) {
	col := catalog.NewColumnDef("test_col", dukdb.TYPE_INTEGER)
	col.Nullable = false
	col.HasDefault = true
	col.DefaultValue = 42

	result, err := ConvertColumnToDuckDB(col)
	require.NoError(t, err)
	assert.Equal(t, "test_col", result.Name)
	assert.Equal(t, TypeInteger, result.Type)
	assert.False(t, result.Nullable)
	assert.True(t, result.HasDefault)
	assert.Equal(t, "42", result.DefaultValue)
}

// TestConvertColumnFromDuckDB tests column conversion from DuckDB format.
func TestConvertColumnFromDuckDB(t *testing.T) {
	col := NewColumnDefinition("test_col", TypeVarchar)
	col.Nullable = true
	col.HasDefault = true
	col.DefaultValue = "'hello'"

	result, err := ConvertColumnFromDuckDB(col)
	require.NoError(t, err)
	assert.Equal(t, "test_col", result.Name)
	assert.Equal(t, dukdb.TYPE_VARCHAR, result.Type)
	assert.True(t, result.Nullable)
	assert.True(t, result.HasDefault)
	assert.Equal(t, "'hello'", result.DefaultValue)
}

// TestConvertTableToDuckDB tests table conversion.
func TestConvertTableToDuckDB(t *testing.T) {
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("value", dukdb.TYPE_DOUBLE),
	}
	table := catalog.NewTableDef("test_table", columns)
	table.Schema = "main"
	table.PrimaryKey = []int{0}

	result, err := ConvertTableToDuckDB(table)
	require.NoError(t, err)
	assert.Equal(t, "test_table", result.Name)
	assert.Equal(t, "main", result.GetSchema())
	assert.Len(t, result.Columns, 3)
	assert.Len(t, result.Constraints, 1)
	assert.Equal(t, ConstraintTypePrimaryKey, result.Constraints[0].Type)
	assert.Equal(t, []uint64{0}, result.Constraints[0].ColumnIndices)
}

// TestConvertTableFromDuckDB tests table conversion from DuckDB format.
func TestConvertTableFromDuckDB(t *testing.T) {
	table := NewTableCatalogEntry("test_table")
	table.CreateInfo.Schema = "main"
	table.AddColumn(ColumnDefinition{
		Name:     "id",
		Type:     TypeInteger,
		Nullable: false,
	})
	table.AddColumn(ColumnDefinition{
		Name:     "name",
		Type:     TypeVarchar,
		Nullable: true,
	})
	table.AddConstraint(Constraint{
		Type:          ConstraintTypePrimaryKey,
		ColumnIndices: []uint64{0},
	})

	result, err := ConvertTableFromDuckDB(table)
	require.NoError(t, err)
	assert.Equal(t, "test_table", result.Name)
	assert.Equal(t, "main", result.Schema)
	assert.Len(t, result.Columns, 2)
	assert.Equal(t, []int{0}, result.PrimaryKey)
}

// TestConvertViewToDuckDB tests view conversion.
func TestConvertViewToDuckDB(t *testing.T) {
	view := catalog.NewViewDefWithDependencies(
		"test_view",
		"main",
		"SELECT * FROM users",
		[]string{"users"},
	)

	result, err := ConvertViewToDuckDB(view)
	require.NoError(t, err)
	assert.Equal(t, "test_view", result.Name)
	assert.Equal(t, "main", result.GetSchema())
	assert.Equal(t, "SELECT * FROM users", result.Query)
	assert.Len(t, result.CreateInfo.Dependencies, 1)
	assert.Equal(t, "users", result.CreateInfo.Dependencies[0].Name)
}

// TestConvertViewFromDuckDB tests view conversion from DuckDB format.
func TestConvertViewFromDuckDB(t *testing.T) {
	view := NewViewCatalogEntry("test_view", "SELECT id, name FROM users")
	view.CreateInfo.Schema = "main"
	view.CreateInfo.Dependencies = []DependencyEntry{
		{Schema: "main", Name: "users", Type: CatalogTableEntry},
	}

	result, err := ConvertViewFromDuckDB(view)
	require.NoError(t, err)
	assert.Equal(t, "test_view", result.Name)
	assert.Equal(t, "main", result.Schema)
	assert.Equal(t, "SELECT id, name FROM users", result.Query)
	assert.Len(t, result.TableDependencies, 1)
}

// TestConvertIndexToDuckDB tests index conversion.
func TestConvertIndexToDuckDB(t *testing.T) {
	index := catalog.NewIndexDef(
		"idx_users_name",
		"main",
		"users",
		[]string{"name", "email"},
		true, // isUnique
	)
	index.IsPrimary = false

	result, err := ConvertIndexToDuckDB(index)
	require.NoError(t, err)
	assert.Equal(t, "idx_users_name", result.Name)
	assert.Equal(t, "users", result.TableName)
	assert.True(t, result.IsUnique())
	assert.False(t, result.IsPrimary())
	assert.Equal(t, []string{"name", "email"}, result.Expressions)
}

// TestConvertIndexFromDuckDB tests index conversion from DuckDB format.
func TestConvertIndexFromDuckDB(t *testing.T) {
	index := NewIndexCatalogEntry("idx_test", "test_table")
	index.CreateInfo.Schema = "main"
	index.Constraint = IndexConstraintPrimary
	index.Expressions = []string{"id"}

	result, err := ConvertIndexFromDuckDB(index)
	require.NoError(t, err)
	assert.Equal(t, "idx_test", result.Name)
	assert.Equal(t, "main", result.Schema)
	assert.Equal(t, "test_table", result.Table)
	assert.True(t, result.IsUnique)
	assert.True(t, result.IsPrimary)
	assert.Equal(t, []string{"id"}, result.Columns)
}

// TestConvertSequenceToDuckDB tests sequence conversion.
func TestConvertSequenceToDuckDB(t *testing.T) {
	seq := catalog.NewSequenceDef("test_seq", "main")
	seq.StartWith = 100
	seq.IncrementBy = 10
	seq.MinValue = 1
	seq.MaxValue = 1000000
	seq.IsCycle = true
	seq.SetCurrentVal(200)

	result, err := ConvertSequenceToDuckDB(seq)
	require.NoError(t, err)
	assert.Equal(t, "test_seq", result.Name)
	assert.Equal(t, "main", result.GetSchema())
	assert.Equal(t, int64(100), result.StartWith)
	assert.Equal(t, int64(10), result.Increment)
	assert.Equal(t, int64(1), result.MinValue)
	assert.Equal(t, int64(1000000), result.MaxValue)
	assert.True(t, result.Cycle)
	assert.Equal(t, int64(200), result.Counter)
}

// TestConvertSequenceFromDuckDB tests sequence conversion from DuckDB format.
func TestConvertSequenceFromDuckDB(t *testing.T) {
	seq := NewSequenceCatalogEntry("test_seq")
	seq.CreateInfo.Schema = "main"
	seq.StartWith = 1
	seq.Increment = 1
	seq.MinValue = 1
	seq.MaxValue = 9223372036854775807
	seq.Cycle = false
	seq.Counter = 50

	result, err := ConvertSequenceFromDuckDB(seq)
	require.NoError(t, err)
	assert.Equal(t, "test_seq", result.Name)
	assert.Equal(t, "main", result.Schema)
	assert.Equal(t, int64(1), result.StartWith)
	assert.Equal(t, int64(1), result.IncrementBy)
	assert.Equal(t, int64(50), result.GetCurrentVal())
	assert.False(t, result.IsCycle)
}

// TestConvertCatalogRoundTrip tests full catalog conversion round trip.
func TestConvertCatalogRoundTrip(t *testing.T) {
	// Create a source catalog with various objects
	cat := catalog.NewCatalog()

	// Add a table
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	}
	table := catalog.NewTableDef("users", columns)
	table.PrimaryKey = []int{0}
	err := cat.CreateTable(table)
	require.NoError(t, err)

	// Add a view
	view := catalog.NewViewDefWithDependencies(
		"user_names",
		"main",
		"SELECT name FROM users",
		[]string{"users"},
	)
	err = cat.CreateView(view)
	require.NoError(t, err)

	// Add an index
	index := catalog.NewIndexDef("idx_users_name", "main", "users", []string{"name"}, false)
	err = cat.CreateIndex(index)
	require.NoError(t, err)

	// Add a sequence
	seq := catalog.NewSequenceDef("user_id_seq", "main")
	seq.StartWith = 1
	seq.IncrementBy = 1
	err = cat.CreateSequence(seq)
	require.NoError(t, err)

	// Convert to DuckDB format
	dcat, err := ConvertCatalogToDuckDB(cat)
	require.NoError(t, err)
	assert.NotNil(t, dcat)
	assert.Len(t, dcat.Tables, 1)
	assert.Len(t, dcat.Views, 1)
	assert.Len(t, dcat.Indexes, 1)
	assert.Len(t, dcat.Sequences, 1)

	// Convert back to dukdb-go format
	result, err := ConvertCatalogFromDuckDB(dcat)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify table
	tbl, ok := result.GetTable("users")
	assert.True(t, ok)
	assert.Equal(t, "users", tbl.Name)
	assert.Len(t, tbl.Columns, 2)
	assert.Equal(t, []int{0}, tbl.PrimaryKey)

	// Verify view
	vw, ok := result.GetView("user_names")
	assert.True(t, ok)
	assert.Equal(t, "user_names", vw.Name)
	assert.Contains(t, vw.Query, "SELECT name FROM users")

	// Verify index
	idx, ok := result.GetIndex("idx_users_name")
	assert.True(t, ok)
	assert.Equal(t, "idx_users_name", idx.Name)
	assert.Equal(t, "users", idx.Table)

	// Verify sequence
	sequence, ok := result.GetSequence("user_id_seq")
	assert.True(t, ok)
	assert.Equal(t, "user_id_seq", sequence.Name)
}

// TestConvertCatalogWithMultipleSchemas tests catalog conversion with multiple schemas.
func TestConvertCatalogWithMultipleSchemas(t *testing.T) {
	cat := catalog.NewCatalog()

	// Create a new schema
	_, err := cat.CreateSchema("other")
	require.NoError(t, err)

	// Add table to main schema
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
	}
	table1 := catalog.NewTableDef("table1", columns)
	err = cat.CreateTable(table1)
	require.NoError(t, err)

	// Add table to other schema
	table2 := catalog.NewTableDef("table2", columns)
	err = cat.CreateTableInSchema("other", table2)
	require.NoError(t, err)

	// Convert to DuckDB format
	dcat, err := ConvertCatalogToDuckDB(cat)
	require.NoError(t, err)
	assert.Len(t, dcat.Schemas, 2)
	assert.Len(t, dcat.Tables, 2)

	// Convert back
	result, err := ConvertCatalogFromDuckDB(dcat)
	require.NoError(t, err)

	// Verify tables in different schemas
	t1, ok := result.GetTableInSchema("main", "table1")
	assert.True(t, ok)
	assert.Equal(t, "table1", t1.Name)

	t2, ok := result.GetTableInSchema("other", "table2")
	assert.True(t, ok)
	assert.Equal(t, "table2", t2.Name)
}

// TestConvertNilInputs tests handling of nil inputs.
func TestConvertNilInputs(t *testing.T) {
	t.Run("nil catalog", func(t *testing.T) {
		_, err := ConvertCatalogToDuckDB(nil)
		assert.Error(t, err)
	})

	t.Run("nil DuckDB catalog", func(t *testing.T) {
		_, err := ConvertCatalogFromDuckDB(nil)
		assert.Error(t, err)
	})

	t.Run("nil table", func(t *testing.T) {
		_, err := ConvertTableToDuckDB(nil)
		assert.Error(t, err)
	})

	t.Run("nil DuckDB table", func(t *testing.T) {
		_, err := ConvertTableFromDuckDB(nil)
		assert.Error(t, err)
	})

	t.Run("nil view", func(t *testing.T) {
		_, err := ConvertViewToDuckDB(nil)
		assert.Error(t, err)
	})

	t.Run("nil DuckDB view", func(t *testing.T) {
		_, err := ConvertViewFromDuckDB(nil)
		assert.Error(t, err)
	})

	t.Run("nil index", func(t *testing.T) {
		_, err := ConvertIndexToDuckDB(nil)
		assert.Error(t, err)
	})

	t.Run("nil DuckDB index", func(t *testing.T) {
		_, err := ConvertIndexFromDuckDB(nil)
		assert.Error(t, err)
	})

	t.Run("nil sequence", func(t *testing.T) {
		_, err := ConvertSequenceToDuckDB(nil)
		assert.Error(t, err)
	})

	t.Run("nil DuckDB sequence", func(t *testing.T) {
		_, err := ConvertSequenceFromDuckDB(nil)
		assert.Error(t, err)
	})

	t.Run("nil column", func(t *testing.T) {
		_, err := ConvertColumnToDuckDB(nil)
		assert.Error(t, err)
	})

	t.Run("nil DuckDB column", func(t *testing.T) {
		_, err := ConvertColumnFromDuckDB(nil)
		assert.Error(t, err)
	})
}

// TestDuckDBCatalogConstructor tests DuckDBCatalog creation.
func TestDuckDBCatalogConstructor(t *testing.T) {
	dcat := NewDuckDBCatalog()
	assert.NotNil(t, dcat)
	assert.NotNil(t, dcat.Schemas)
	assert.NotNil(t, dcat.Tables)
	assert.NotNil(t, dcat.Views)
	assert.NotNil(t, dcat.Indexes)
	assert.NotNil(t, dcat.Sequences)
	assert.NotNil(t, dcat.Types)
	assert.Len(t, dcat.Schemas, 0)
	assert.Len(t, dcat.Tables, 0)
}

// TestGetSequenceMinMaxDefaults tests default sequence min/max values.
func TestGetSequenceMinMaxDefaults(t *testing.T) {
	minVal, maxVal := GetSequenceMinMaxDefaults()
	assert.Equal(t, int64(math.MinInt64), minVal)
	assert.Equal(t, int64(math.MaxInt64), maxVal)
}

// TestValidateTypeConversion tests the type validation function.
func TestValidateTypeConversion(t *testing.T) {
	// Types that should round-trip successfully
	validTypes := []dukdb.Type{
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_TIMESTAMP,
		dukdb.TYPE_UUID,
	}

	for _, typ := range validTypes {
		t.Run(typ.String(), func(t *testing.T) {
			err := ValidateTypeConversion(typ)
			assert.NoError(t, err)
		})
	}
}

// TestConvertTypeToDuckDBWithInfo tests type conversion with TypeInfo.
func TestConvertTypeToDuckDBWithInfo(t *testing.T) {
	t.Run("DECIMAL with precision and scale", func(t *testing.T) {
		info, err := dukdb.NewDecimalInfo(18, 4)
		require.NoError(t, err)

		typeID, mods, err := ConvertTypeToDuckDBWithInfo(dukdb.TYPE_DECIMAL, info)
		require.NoError(t, err)
		assert.Equal(t, TypeDecimal, typeID)
		assert.Equal(t, uint8(18), mods.Width)
		assert.Equal(t, uint8(4), mods.Scale)
	})

	t.Run("LIST with child type", func(t *testing.T) {
		childInfo, err := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
		require.NoError(t, err)
		info, err := dukdb.NewListInfo(childInfo)
		require.NoError(t, err)

		typeID, mods, err := ConvertTypeToDuckDBWithInfo(dukdb.TYPE_LIST, info)
		require.NoError(t, err)
		assert.Equal(t, TypeList, typeID)
		assert.Equal(t, TypeInteger, mods.ChildTypeID)
	})

	t.Run("ENUM with values", func(t *testing.T) {
		info, err := dukdb.NewEnumInfo("small", "medium", "large")
		require.NoError(t, err)

		typeID, mods, err := ConvertTypeToDuckDBWithInfo(dukdb.TYPE_ENUM, info)
		require.NoError(t, err)
		assert.Equal(t, TypeEnum, typeID)
		assert.Equal(t, []string{"small", "medium", "large"}, mods.EnumValues)
	})
}

// TestConvertTypeFromDuckDBWithInfo tests type conversion with TypeInfo creation.
func TestConvertTypeFromDuckDBWithInfo(t *testing.T) {
	t.Run("DECIMAL with precision and scale", func(t *testing.T) {
		mods := &TypeModifiers{Width: 10, Scale: 2}
		typ, info, err := ConvertTypeFromDuckDBWithInfo(TypeDecimal, mods)
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_DECIMAL, typ)
		assert.NotNil(t, info)
		details := info.Details()
		if dd, ok := details.(*dukdb.DecimalDetails); ok {
			assert.Equal(t, uint8(10), dd.Width)
			assert.Equal(t, uint8(2), dd.Scale)
		}
	})

	t.Run("ENUM with values", func(t *testing.T) {
		mods := &TypeModifiers{EnumValues: []string{"a", "b", "c"}}
		typ, info, err := ConvertTypeFromDuckDBWithInfo(TypeEnum, mods)
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_ENUM, typ)
		assert.NotNil(t, info)
		details := info.Details()
		if ed, ok := details.(*dukdb.EnumDetails); ok {
			assert.Equal(t, []string{"a", "b", "c"}, ed.Values)
		}
	})

	t.Run("Simple type", func(t *testing.T) {
		typ, info, err := ConvertTypeFromDuckDBWithInfo(TypeInteger, nil)
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_INTEGER, typ)
		assert.NotNil(t, info)
	})
}
