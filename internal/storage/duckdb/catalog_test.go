package duckdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCatalogEntryInterface(t *testing.T) {
	// Verify all catalog entry types implement CatalogEntry interface.
	t.Run("schema entry implements interface", func(t *testing.T) {
		var entry CatalogEntry = NewSchemaCatalogEntry("test_schema")
		assert.Equal(t, CatalogSchemaEntry, entry.Type())
		assert.Equal(t, "test_schema", entry.GetName())
	})

	t.Run("table entry implements interface", func(t *testing.T) {
		table := NewTableCatalogEntry("test_table")
		table.CreateInfo.Schema = "public"

		var entry CatalogEntry = table
		assert.Equal(t, CatalogTableEntry, entry.Type())
		assert.Equal(t, "test_table", entry.GetName())
		assert.Equal(t, "public", entry.GetSchema())
	})

	t.Run("view entry implements interface", func(t *testing.T) {
		view := NewViewCatalogEntry("test_view", "SELECT * FROM t")
		view.CreateInfo.Schema = "main"

		var entry CatalogEntry = view
		assert.Equal(t, CatalogViewEntry, entry.Type())
		assert.Equal(t, "test_view", entry.GetName())
		assert.Equal(t, "main", entry.GetSchema())
	})

	t.Run("index entry implements interface", func(t *testing.T) {
		index := NewIndexCatalogEntry("test_idx", "test_table")
		index.CreateInfo.Schema = "public"

		var entry CatalogEntry = index
		assert.Equal(t, CatalogIndexEntry, entry.Type())
		assert.Equal(t, "test_idx", entry.GetName())
		assert.Equal(t, "public", entry.GetSchema())
	})

	t.Run("sequence entry implements interface", func(t *testing.T) {
		seq := NewSequenceCatalogEntry("test_seq")
		seq.CreateInfo.Schema = "public"

		var entry CatalogEntry = seq
		assert.Equal(t, CatalogSequenceEntry, entry.Type())
		assert.Equal(t, "test_seq", entry.GetName())
		assert.Equal(t, "public", entry.GetSchema())
	})

	t.Run("type entry implements interface", func(t *testing.T) {
		typeEntry := NewTypeCatalogEntry("mood", TypeEnum)
		typeEntry.CreateInfo.Schema = "public"

		var entry CatalogEntry = typeEntry
		assert.Equal(t, CatalogTypeEntry, entry.Type())
		assert.Equal(t, "mood", entry.GetName())
		assert.Equal(t, "public", entry.GetSchema())
	})
}

func TestSchemaCatalogEntry(t *testing.T) {
	t.Run("new schema entry", func(t *testing.T) {
		schema := NewSchemaCatalogEntry("my_schema")
		assert.Equal(t, "my_schema", schema.Name)
		assert.NotNil(t, schema.Tags)
	})

	t.Run("schema with create info", func(t *testing.T) {
		schema := NewSchemaCatalogEntry("temp_schema")
		schema.CreateInfo.Temporary = true
		schema.CreateInfo.Comment = "A temporary schema"

		assert.True(t, schema.Temporary)
		assert.Equal(t, "A temporary schema", schema.Comment)
	})
}

func TestTableCatalogEntry(t *testing.T) {
	t.Run("new table entry", func(t *testing.T) {
		table := NewTableCatalogEntry("users")
		assert.Equal(t, "users", table.Name)
		assert.Empty(t, table.Columns)
		assert.Empty(t, table.Constraints)
		assert.NotNil(t, table.Tags)
	})

	t.Run("add columns", func(t *testing.T) {
		table := NewTableCatalogEntry("users")

		col1 := ColumnDefinition{
			Name:     "id",
			Type:     TypeInteger,
			Nullable: false,
		}
		col2 := ColumnDefinition{
			Name:       "name",
			Type:       TypeVarchar,
			Nullable:   true,
			HasDefault: true,
		}

		table.AddColumn(col1)
		table.AddColumn(col2)

		assert.Equal(t, 2, table.ColumnCount())
		assert.Equal(t, "id", table.GetColumn(0).Name)
		assert.Equal(t, TypeInteger, table.GetColumn(0).Type)
		assert.False(t, table.GetColumn(0).Nullable)
		assert.Equal(t, "name", table.GetColumn(1).Name)
		assert.True(t, table.GetColumn(1).HasDefault)
	})

	t.Run("get column by name", func(t *testing.T) {
		table := NewTableCatalogEntry("products")
		table.AddColumn(ColumnDefinition{Name: "price", Type: TypeDouble})
		table.AddColumn(ColumnDefinition{Name: "quantity", Type: TypeInteger})

		col := table.GetColumnByName("price")
		assert.NotNil(t, col)
		assert.Equal(t, TypeDouble, col.Type)

		col2 := table.GetColumnByName("nonexistent")
		assert.Nil(t, col2)
	})

	t.Run("get column bounds checking", func(t *testing.T) {
		table := NewTableCatalogEntry("t")
		table.AddColumn(ColumnDefinition{Name: "a", Type: TypeInteger})

		assert.Nil(t, table.GetColumn(-1))
		assert.Nil(t, table.GetColumn(1))
		assert.NotNil(t, table.GetColumn(0))
	})

	t.Run("add constraints", func(t *testing.T) {
		table := NewTableCatalogEntry("orders")
		table.AddColumn(ColumnDefinition{Name: "id", Type: TypeBigInt})
		table.AddColumn(ColumnDefinition{Name: "customer_id", Type: TypeInteger})

		pkConstraint := Constraint{
			Type:          ConstraintTypePrimaryKey,
			Name:          "pk_orders",
			ColumnIndices: []uint64{0},
		}
		fkConstraint := Constraint{
			Type:          ConstraintTypeForeignKey,
			Name:          "fk_customer",
			ColumnIndices: []uint64{1},
			ForeignKey: &ForeignKeyInfo{
				ReferencedSchema:  "public",
				ReferencedTable:   "customers",
				ReferencedColumns: []string{"id"},
				OnDelete:          ForeignKeyActionCascade,
				OnUpdate:          ForeignKeyActionNoAction,
			},
		}

		table.AddConstraint(pkConstraint)
		table.AddConstraint(fkConstraint)

		assert.Len(t, table.Constraints, 2)
		assert.Equal(t, ConstraintTypePrimaryKey, table.Constraints[0].Type)
		assert.Equal(t, ConstraintTypeForeignKey, table.Constraints[1].Type)
		assert.Equal(t, ForeignKeyActionCascade, table.Constraints[1].ForeignKey.OnDelete)
	})
}

func TestViewCatalogEntry(t *testing.T) {
	t.Run("new view entry", func(t *testing.T) {
		view := NewViewCatalogEntry("active_users", "SELECT * FROM users WHERE active = true")
		assert.Equal(t, "active_users", view.Name)
		assert.Equal(t, "SELECT * FROM users WHERE active = true", view.Query)
		assert.Empty(t, view.Aliases)
		assert.Empty(t, view.Types)
	})

	t.Run("view with aliases and types", func(t *testing.T) {
		view := NewViewCatalogEntry("summary", "SELECT id, name FROM items")
		view.Aliases = []string{"item_id", "item_name"}
		view.Types = []LogicalTypeID{TypeBigInt, TypeVarchar}

		assert.Len(t, view.Aliases, 2)
		assert.Len(t, view.Types, 2)
		assert.Equal(t, "item_id", view.Aliases[0])
		assert.Equal(t, TypeBigInt, view.Types[0])
	})
}

func TestIndexCatalogEntry(t *testing.T) {
	t.Run("new index entry", func(t *testing.T) {
		index := NewIndexCatalogEntry("idx_users_email", "users")
		assert.Equal(t, "idx_users_email", index.Name)
		assert.Equal(t, "users", index.TableName)
		assert.Equal(t, IndexTypeART, index.IndexType) // Default
		assert.Equal(t, IndexConstraintNone, index.Constraint)
		assert.Empty(t, index.ColumnIDs)
	})

	t.Run("unique index", func(t *testing.T) {
		index := NewIndexCatalogEntry("idx_unique_email", "users")
		index.Constraint = IndexConstraintUnique
		index.ColumnIDs = []uint64{2}

		assert.True(t, index.IsUnique())
		assert.False(t, index.IsPrimary())
	})

	t.Run("primary key index", func(t *testing.T) {
		index := NewIndexCatalogEntry("pk_users", "users")
		index.Constraint = IndexConstraintPrimary
		index.ColumnIDs = []uint64{0}

		assert.True(t, index.IsUnique())
		assert.True(t, index.IsPrimary())
	})

	t.Run("expression index", func(t *testing.T) {
		index := NewIndexCatalogEntry("idx_lower_name", "users")
		index.IndexType = IndexTypeHash
		index.Expressions = []string{"lower(name)"}

		assert.Equal(t, IndexTypeHash, index.IndexType)
		assert.Len(t, index.Expressions, 1)
	})
}

func TestSequenceCatalogEntry(t *testing.T) {
	t.Run("new sequence entry with defaults", func(t *testing.T) {
		seq := NewSequenceCatalogEntry("user_id_seq")
		assert.Equal(t, "user_id_seq", seq.Name)
		assert.Equal(t, int64(1), seq.StartWith)
		assert.Equal(t, int64(1), seq.Increment)
		assert.Equal(t, int64(1), seq.MinValue)
		assert.Equal(t, int64(9223372036854775807), seq.MaxValue)
		assert.False(t, seq.Cycle)
		assert.Equal(t, int64(1), seq.Counter)
	})

	t.Run("custom sequence", func(t *testing.T) {
		seq := NewSequenceCatalogEntry("order_num")
		seq.StartWith = 1000
		seq.Increment = 10
		seq.MinValue = 1000
		seq.MaxValue = 9999
		seq.Cycle = true
		seq.Counter = 1050

		assert.Equal(t, int64(1000), seq.StartWith)
		assert.Equal(t, int64(10), seq.Increment)
		assert.True(t, seq.Cycle)
	})
}

func TestTypeCatalogEntry(t *testing.T) {
	t.Run("basic type entry", func(t *testing.T) {
		typeEntry := NewTypeCatalogEntry("my_type", TypeStruct)
		assert.Equal(t, "my_type", typeEntry.Name)
		assert.Equal(t, TypeStruct, typeEntry.TypeID)
		assert.False(t, typeEntry.IsEnum())
	})

	t.Run("enum type entry", func(t *testing.T) {
		typeEntry := NewEnumTypeCatalogEntry("mood", []string{"sad", "ok", "happy"})
		assert.Equal(t, "mood", typeEntry.Name)
		assert.Equal(t, TypeEnum, typeEntry.TypeID)
		assert.True(t, typeEntry.IsEnum())
		assert.Equal(t, []string{"sad", "ok", "happy"}, typeEntry.TypeModifiers.EnumValues)
	})
}

func TestColumnDefinition(t *testing.T) {
	t.Run("new column definition", func(t *testing.T) {
		col := NewColumnDefinition("age", TypeInteger)
		assert.Equal(t, "age", col.Name)
		assert.Equal(t, TypeInteger, col.Type)
		assert.True(t, col.Nullable)
		assert.Equal(t, CompressionAuto, col.CompressionType)
	})

	t.Run("column with modifiers", func(t *testing.T) {
		col := NewColumnDefinition("price", TypeDecimal)
		col.TypeModifiers = TypeModifiers{
			Width: 10,
			Scale: 2,
		}
		col.Nullable = false
		col.HasDefault = true
		col.DefaultValue = "0.00"

		assert.Equal(t, uint8(10), col.TypeModifiers.Width)
		assert.Equal(t, uint8(2), col.TypeModifiers.Scale)
		assert.False(t, col.Nullable)
		assert.True(t, col.HasDefault)
	})

	t.Run("generated column", func(t *testing.T) {
		col := NewColumnDefinition("full_name", TypeVarchar)
		col.Generated = true
		col.GeneratedExpression = "first_name || ' ' || last_name"

		assert.True(t, col.Generated)
		assert.Equal(t, "first_name || ' ' || last_name", col.GeneratedExpression)
	})
}

func TestTypeModifiers(t *testing.T) {
	t.Run("decimal modifiers", func(t *testing.T) {
		mods := TypeModifiers{
			Width: 18,
			Scale: 4,
		}
		assert.Equal(t, uint8(18), mods.Width)
		assert.Equal(t, uint8(4), mods.Scale)
	})

	t.Run("list modifiers", func(t *testing.T) {
		childMod := &TypeModifiers{}
		mods := TypeModifiers{
			ChildTypeID: TypeInteger,
			ChildType:   childMod,
		}
		assert.Equal(t, TypeInteger, mods.ChildTypeID)
	})

	t.Run("struct modifiers", func(t *testing.T) {
		mods := TypeModifiers{
			StructFields: []StructField{
				{Name: "x", Type: TypeDouble},
				{Name: "y", Type: TypeDouble},
			},
		}
		assert.Len(t, mods.StructFields, 2)
		assert.Equal(t, "x", mods.StructFields[0].Name)
	})

	t.Run("map modifiers", func(t *testing.T) {
		keyMod := &TypeModifiers{}
		valMod := &TypeModifiers{}
		mods := TypeModifiers{
			KeyTypeID:   TypeVarchar,
			KeyType:     keyMod,
			ValueTypeID: TypeInteger,
			ValueType:   valMod,
		}
		assert.Equal(t, TypeVarchar, mods.KeyTypeID)
		assert.Equal(t, TypeInteger, mods.ValueTypeID)
	})

	t.Run("union modifiers", func(t *testing.T) {
		mods := TypeModifiers{
			UnionMembers: []UnionMember{
				{Tag: "str", Type: TypeVarchar},
				{Tag: "num", Type: TypeInteger},
			},
		}
		assert.Len(t, mods.UnionMembers, 2)
		assert.Equal(t, "str", mods.UnionMembers[0].Tag)
	})
}

func TestEnumStrings(t *testing.T) {
	t.Run("OnCreateConflict", func(t *testing.T) {
		assert.Equal(t, "ERROR", OnCreateConflictError.String())
		assert.Equal(t, "IGNORE", OnCreateConflictIgnore.String())
		assert.Equal(t, "REPLACE", OnCreateConflictReplace.String())
		assert.Equal(t, strUnknown, OnCreateConflict(99).String())
	})

	t.Run("DependencyType", func(t *testing.T) {
		assert.Equal(t, "REGULAR", DependencyTypeRegular.String())
		assert.Equal(t, "AUTOMATIC", DependencyTypeAutomatic.String())
		assert.Equal(t, "OWNERSHIP", DependencyTypeOwnership.String())
		assert.Equal(t, strUnknown, DependencyType(99).String())
	})

	t.Run("ConstraintType", func(t *testing.T) {
		assert.Equal(t, "PRIMARY_KEY", ConstraintTypePrimaryKey.String())
		assert.Equal(t, "FOREIGN_KEY", ConstraintTypeForeignKey.String())
		assert.Equal(t, "UNIQUE", ConstraintTypeUnique.String())
		assert.Equal(t, "CHECK", ConstraintTypeCheck.String())
		assert.Equal(t, "NOT_NULL", ConstraintTypeNotNull.String())
		assert.Equal(t, strUnknown, ConstraintType(99).String())
	})

	t.Run("ForeignKeyAction", func(t *testing.T) {
		assert.Equal(t, "NO_ACTION", ForeignKeyActionNoAction.String())
		assert.Equal(t, "RESTRICT", ForeignKeyActionRestrict.String())
		assert.Equal(t, "CASCADE", ForeignKeyActionCascade.String())
		assert.Equal(t, "SET_NULL", ForeignKeyActionSetNull.String())
		assert.Equal(t, "SET_DEFAULT", ForeignKeyActionSetDefault.String())
		assert.Equal(t, strUnknown, ForeignKeyAction(99).String())
	})

	t.Run("IndexType", func(t *testing.T) {
		assert.Equal(t, strInvalid, IndexTypeInvalid.String())
		assert.Equal(t, "ART", IndexTypeART.String())
		assert.Equal(t, "HASH", IndexTypeHash.String())
		assert.Equal(t, strUnknown, IndexType(99).String())
	})

	t.Run("IndexConstraintType", func(t *testing.T) {
		assert.Equal(t, "NONE", IndexConstraintNone.String())
		assert.Equal(t, "UNIQUE", IndexConstraintUnique.String())
		assert.Equal(t, "PRIMARY", IndexConstraintPrimary.String())
		assert.Equal(t, "FOREIGN", IndexConstraintForeign.String())
		assert.Equal(t, strUnknown, IndexConstraintType(99).String())
	})

	t.Run("SequenceUsage", func(t *testing.T) {
		assert.Equal(t, "NONE", SequenceUsageNone.String())
		assert.Equal(t, "OWNED", SequenceUsageOwned.String())
		assert.Equal(t, strUnknown, SequenceUsage(99).String())
	})
}

func TestCreateInfo(t *testing.T) {
	t.Run("default create info", func(t *testing.T) {
		info := CreateInfo{
			Tags: make(map[string]string),
		}
		assert.Empty(t, info.Catalog)
		assert.Empty(t, info.Schema)
		assert.False(t, info.Temporary)
		assert.False(t, info.Internal)
		assert.Equal(t, OnCreateConflictError, info.OnConflict)
	})

	t.Run("create info with all fields", func(t *testing.T) {
		info := CreateInfo{
			Catalog:      "main",
			Schema:       "public",
			Temporary:    true,
			Internal:     false,
			OnConflict:   OnCreateConflictReplace,
			SQL:          "CREATE TABLE t (id INT)",
			Comment:      "Test table",
			Tags:         map[string]string{"env": "test"},
			Dependencies: []DependencyEntry{},
		}

		assert.Equal(t, "main", info.Catalog)
		assert.Equal(t, "public", info.Schema)
		assert.True(t, info.Temporary)
		assert.Equal(t, OnCreateConflictReplace, info.OnConflict)
		assert.Equal(t, "test", info.Tags["env"])
	})
}

func TestDependencyEntry(t *testing.T) {
	t.Run("create dependency entry", func(t *testing.T) {
		dep := DependencyEntry{
			Catalog:        "main",
			Schema:         "public",
			Name:           "users",
			Type:           CatalogTableEntry,
			DependencyType: DependencyTypeRegular,
		}

		assert.Equal(t, "main", dep.Catalog)
		assert.Equal(t, "users", dep.Name)
		assert.Equal(t, CatalogTableEntry, dep.Type)
	})
}
