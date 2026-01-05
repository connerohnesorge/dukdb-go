package duckdb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDependencyEntryRoundTrip verifies that DependencyEntry serializes and deserializes correctly.
func TestDependencyEntryRoundTrip(t *testing.T) {
	original := DependencyEntry{
		Catalog:        testCatalogMain,
		Schema:         testSchemaPublic,
		Name:           "my_table",
		Type:           CatalogTableEntry,
		DependencyType: DependencyTypeAutomatic,
	}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	var deserialized DependencyEntry
	err = deserialized.Deserialize(r)
	require.NoError(t, err)

	assert.Equal(t, original.Catalog, deserialized.Catalog)
	assert.Equal(t, original.Schema, deserialized.Schema)
	assert.Equal(t, original.Name, deserialized.Name)
	assert.Equal(t, original.Type, deserialized.Type)
	assert.Equal(t, original.DependencyType, deserialized.DependencyType)
}

// TestTypeModifiersRoundTrip verifies that TypeModifiers serializes and deserializes correctly.
func TestTypeModifiersRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		tm   TypeModifiers
	}{
		{
			name: "empty modifiers",
			tm:   TypeModifiers{},
		},
		{
			name: "decimal modifiers",
			tm: TypeModifiers{
				Width: 18,
				Scale: 4,
			},
		},
		{
			name: "char with length",
			tm: TypeModifiers{
				Length: 255,
			},
		},
		{
			name: "list type",
			tm: TypeModifiers{
				ChildTypeID: TypeInteger,
				ChildType:   &TypeModifiers{},
			},
		},
		{
			name: "map type",
			tm: TypeModifiers{
				KeyTypeID:   TypeVarchar,
				KeyType:     &TypeModifiers{},
				ValueTypeID: TypeInteger,
				ValueType:   &TypeModifiers{},
			},
		},
		{
			name: "struct type",
			tm: TypeModifiers{
				StructFields: []StructField{
					{Name: "x", Type: TypeInteger},
					{Name: "y", Type: TypeDouble},
				},
			},
		},
		{
			name: "enum type",
			tm: TypeModifiers{
				EnumValues: []string{"small", "medium", "large"},
			},
		},
		{
			name: "union type",
			tm: TypeModifiers{
				UnionMembers: []UnionMember{
					{Tag: "str", Type: TypeVarchar},
					{Tag: "num", Type: TypeInteger},
				},
			},
		},
		{
			name: "with collation",
			tm: TypeModifiers{
				Collation: "en_US.utf8",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewBinaryWriter(&buf)

			err := tc.tm.Serialize(w)
			require.NoError(t, err)

			r := NewBinaryReader(&buf)
			var deserialized TypeModifiers
			err = deserialized.Deserialize(r)
			require.NoError(t, err)

			assertTypeModifiersEqual(t, &tc.tm, &deserialized)
		})
	}
}

// assertTypeModifiersEqual compares two TypeModifiers for equality.
func assertTypeModifiersEqual(t *testing.T, expected, actual *TypeModifiers) {
	t.Helper()
	assert.Equal(t, expected.Width, actual.Width)
	assert.Equal(t, expected.Scale, actual.Scale)
	assert.Equal(t, expected.Length, actual.Length)
	assert.Equal(t, expected.Collation, actual.Collation)
	assert.Equal(t, expected.ChildTypeID, actual.ChildTypeID)
	assert.Equal(t, expected.KeyTypeID, actual.KeyTypeID)
	assert.Equal(t, expected.ValueTypeID, actual.ValueTypeID)

	// Check child type
	if expected.ChildType != nil {
		require.NotNil(t, actual.ChildType)
		assertTypeModifiersEqual(t, expected.ChildType, actual.ChildType)
	} else {
		assert.Nil(t, actual.ChildType)
	}

	// Check key type
	if expected.KeyType != nil {
		require.NotNil(t, actual.KeyType)
		assertTypeModifiersEqual(t, expected.KeyType, actual.KeyType)
	} else {
		assert.Nil(t, actual.KeyType)
	}

	// Check value type
	if expected.ValueType != nil {
		require.NotNil(t, actual.ValueType)
		assertTypeModifiersEqual(t, expected.ValueType, actual.ValueType)
	} else {
		assert.Nil(t, actual.ValueType)
	}

	// Check struct fields
	require.Equal(t, len(expected.StructFields), len(actual.StructFields))
	for i := range expected.StructFields {
		assert.Equal(t, expected.StructFields[i].Name, actual.StructFields[i].Name)
		assert.Equal(t, expected.StructFields[i].Type, actual.StructFields[i].Type)
		if expected.StructFields[i].TypeModifiers != nil {
			require.NotNil(t, actual.StructFields[i].TypeModifiers)
			assertTypeModifiersEqual(t, expected.StructFields[i].TypeModifiers, actual.StructFields[i].TypeModifiers)
		}
	}

	// Check enum values
	assert.Equal(t, expected.EnumValues, actual.EnumValues)

	// Check union members
	require.Equal(t, len(expected.UnionMembers), len(actual.UnionMembers))
	for i := range expected.UnionMembers {
		assert.Equal(t, expected.UnionMembers[i].Tag, actual.UnionMembers[i].Tag)
		assert.Equal(t, expected.UnionMembers[i].Type, actual.UnionMembers[i].Type)
	}
}

// TestStructFieldRoundTrip verifies that StructField serializes and deserializes correctly.
func TestStructFieldRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		sf   StructField
	}{
		{
			name: "simple field",
			sf: StructField{
				Name: "my_field",
				Type: TypeInteger,
			},
		},
		{
			name: "field with modifiers",
			sf: StructField{
				Name: "my_field",
				Type: TypeVarchar,
				TypeModifiers: &TypeModifiers{
					Collation: "C",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewBinaryWriter(&buf)

			err := tc.sf.Serialize(w)
			require.NoError(t, err)

			r := NewBinaryReader(&buf)
			var deserialized StructField
			err = deserialized.Deserialize(r)
			require.NoError(t, err)

			assert.Equal(t, tc.sf.Name, deserialized.Name)
			assert.Equal(t, tc.sf.Type, deserialized.Type)
			if tc.sf.TypeModifiers != nil {
				require.NotNil(t, deserialized.TypeModifiers)
				assertTypeModifiersEqual(t, tc.sf.TypeModifiers, deserialized.TypeModifiers)
			} else {
				assert.Nil(t, deserialized.TypeModifiers)
			}
		})
	}
}

// TestUnionMemberRoundTrip verifies that UnionMember serializes and deserializes correctly.
func TestUnionMemberRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		um   UnionMember
	}{
		{
			name: "simple member",
			um: UnionMember{
				Tag:  "value",
				Type: TypeDouble,
			},
		},
		{
			name: "member with modifiers",
			um: UnionMember{
				Tag:  "decimal_val",
				Type: TypeDecimal,
				TypeModifiers: &TypeModifiers{
					Width: 18,
					Scale: 2,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewBinaryWriter(&buf)

			err := tc.um.Serialize(w)
			require.NoError(t, err)

			r := NewBinaryReader(&buf)
			var deserialized UnionMember
			err = deserialized.Deserialize(r)
			require.NoError(t, err)

			assert.Equal(t, tc.um.Tag, deserialized.Tag)
			assert.Equal(t, tc.um.Type, deserialized.Type)
			if tc.um.TypeModifiers != nil {
				require.NotNil(t, deserialized.TypeModifiers)
				assertTypeModifiersEqual(t, tc.um.TypeModifiers, deserialized.TypeModifiers)
			} else {
				assert.Nil(t, deserialized.TypeModifiers)
			}
		})
	}
}

// TestColumnDefinitionRoundTrip verifies that ColumnDefinition serializes and deserializes correctly.
func TestColumnDefinitionRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		col  ColumnDefinition
	}{
		{
			name: "simple column",
			col: ColumnDefinition{
				Name:            "id",
				Type:            TypeInteger,
				Nullable:        false,
				CompressionType: CompressionAuto,
			},
		},
		{
			name: "column with default",
			col: ColumnDefinition{
				Name:         "created_at",
				Type:         TypeTimestamp,
				Nullable:     true,
				HasDefault:   true,
				DefaultValue: "current_timestamp",
			},
		},
		{
			name: "generated column",
			col: ColumnDefinition{
				Name:                "full_name",
				Type:                TypeVarchar,
				Nullable:            true,
				Generated:           true,
				GeneratedExpression: "first_name || ' ' || last_name",
			},
		},
		{
			name: "decimal column",
			col: ColumnDefinition{
				Name: "price",
				Type: TypeDecimal,
				TypeModifiers: TypeModifiers{
					Width: 18,
					Scale: 2,
				},
				Nullable:        true,
				CompressionType: CompressionDictionary,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewBinaryWriter(&buf)

			err := tc.col.Serialize(w)
			require.NoError(t, err)

			r := NewBinaryReader(&buf)
			var deserialized ColumnDefinition
			err = deserialized.Deserialize(r)
			require.NoError(t, err)

			assert.Equal(t, tc.col.Name, deserialized.Name)
			assert.Equal(t, tc.col.Type, deserialized.Type)
			assert.Equal(t, tc.col.Nullable, deserialized.Nullable)
			assert.Equal(t, tc.col.HasDefault, deserialized.HasDefault)
			assert.Equal(t, tc.col.DefaultValue, deserialized.DefaultValue)
			assert.Equal(t, tc.col.Generated, deserialized.Generated)
			assert.Equal(t, tc.col.GeneratedExpression, deserialized.GeneratedExpression)
			assert.Equal(t, tc.col.CompressionType, deserialized.CompressionType)
			assertTypeModifiersEqual(t, &tc.col.TypeModifiers, &deserialized.TypeModifiers)
		})
	}
}

// TestConstraintRoundTrip verifies that Constraint serializes and deserializes correctly.
func TestConstraintRoundTrip(t *testing.T) {
	tests := []struct {
		name       string
		constraint Constraint
	}{
		{
			name: "primary key",
			constraint: Constraint{
				Type:          ConstraintTypePrimaryKey,
				Name:          "pk_users",
				ColumnIndices: []uint64{0},
			},
		},
		{
			name: "unique",
			constraint: Constraint{
				Type:          ConstraintTypeUnique,
				Name:          "uq_email",
				ColumnIndices: []uint64{2},
			},
		},
		{
			name: "check",
			constraint: Constraint{
				Type:       ConstraintTypeCheck,
				Name:       "chk_age",
				Expression: "age >= 0",
			},
		},
		{
			name: "not null",
			constraint: Constraint{
				Type:          ConstraintTypeNotNull,
				ColumnIndices: []uint64{1},
			},
		},
		{
			name: "foreign key",
			constraint: Constraint{
				Type:          ConstraintTypeForeignKey,
				Name:          "fk_order_user",
				ColumnIndices: []uint64{1},
				ForeignKey: &ForeignKeyInfo{
					ReferencedSchema:  testSchemaPublic,
					ReferencedTable:   "users",
					ReferencedColumns: []string{"id"},
					OnDelete:          ForeignKeyActionCascade,
					OnUpdate:          ForeignKeyActionNoAction,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewBinaryWriter(&buf)

			err := tc.constraint.Serialize(w)
			require.NoError(t, err)

			r := NewBinaryReader(&buf)
			var deserialized Constraint
			err = deserialized.Deserialize(r)
			require.NoError(t, err)

			assert.Equal(t, tc.constraint.Type, deserialized.Type)
			assert.Equal(t, tc.constraint.Name, deserialized.Name)
			// Use Len check since nil vs empty slice are semantically equal for column indices
			assert.Equal(t, len(tc.constraint.ColumnIndices), len(deserialized.ColumnIndices))
			for i := range tc.constraint.ColumnIndices {
				assert.Equal(t, tc.constraint.ColumnIndices[i], deserialized.ColumnIndices[i])
			}
			assert.Equal(t, tc.constraint.Expression, deserialized.Expression)

			if tc.constraint.ForeignKey != nil {
				require.NotNil(t, deserialized.ForeignKey)
				assert.Equal(t, tc.constraint.ForeignKey.ReferencedSchema, deserialized.ForeignKey.ReferencedSchema)
				assert.Equal(t, tc.constraint.ForeignKey.ReferencedTable, deserialized.ForeignKey.ReferencedTable)
				assert.Equal(t, tc.constraint.ForeignKey.ReferencedColumns, deserialized.ForeignKey.ReferencedColumns)
				assert.Equal(t, tc.constraint.ForeignKey.OnDelete, deserialized.ForeignKey.OnDelete)
				assert.Equal(t, tc.constraint.ForeignKey.OnUpdate, deserialized.ForeignKey.OnUpdate)
			} else {
				assert.Nil(t, deserialized.ForeignKey)
			}
		})
	}
}

// TestForeignKeyInfoRoundTrip verifies that ForeignKeyInfo serializes and deserializes correctly.
func TestForeignKeyInfoRoundTrip(t *testing.T) {
	original := ForeignKeyInfo{
		ReferencedSchema:  testSchemaPublic,
		ReferencedTable:   "orders",
		ReferencedColumns: []string{"id", "version"},
		OnDelete:          ForeignKeyActionSetNull,
		OnUpdate:          ForeignKeyActionRestrict,
	}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	var deserialized ForeignKeyInfo
	err = deserialized.Deserialize(r)
	require.NoError(t, err)

	assert.Equal(t, original.ReferencedSchema, deserialized.ReferencedSchema)
	assert.Equal(t, original.ReferencedTable, deserialized.ReferencedTable)
	assert.Equal(t, original.ReferencedColumns, deserialized.ReferencedColumns)
	assert.Equal(t, original.OnDelete, deserialized.OnDelete)
	assert.Equal(t, original.OnUpdate, deserialized.OnUpdate)
}

// TestSchemaCatalogEntryRoundTrip verifies that SchemaCatalogEntry serializes and deserializes correctly.
func TestSchemaCatalogEntryRoundTrip(t *testing.T) {
	original := NewSchemaCatalogEntry("my_schema")
	original.Catalog = testCatalogMain
	original.Comment = "A test schema"
	original.Tags["owner"] = "test_user"
	original.Dependencies = []DependencyEntry{
		{
			Catalog:        testCatalogMain,
			Schema:         testSchemaPublic,
			Name:           "other_schema",
			Type:           CatalogSchemaEntry,
			DependencyType: DependencyTypeRegular,
		},
	}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	entry, err := DeserializeCatalogEntry(r)
	require.NoError(t, err)

	deserialized, ok := entry.(*SchemaCatalogEntry)
	require.True(t, ok)

	assert.Equal(t, original.Name, deserialized.Name)
	assert.Equal(t, original.Catalog, deserialized.Catalog)
	assert.Equal(t, original.Comment, deserialized.Comment)
	assert.Equal(t, original.Tags, deserialized.Tags)
	require.Equal(t, len(original.Dependencies), len(deserialized.Dependencies))
	assert.Equal(t, original.Dependencies[0].Name, deserialized.Dependencies[0].Name)
}

// TestTableCatalogEntryRoundTrip verifies that TableCatalogEntry serializes and deserializes correctly.
func TestTableCatalogEntryRoundTrip(t *testing.T) {
	original := NewTableCatalogEntry("users")
	original.Schema = testSchemaPublic
	original.Catalog = testCatalogMain
	original.SQL = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR);"

	original.AddColumn(ColumnDefinition{
		Name:     "id",
		Type:     TypeInteger,
		Nullable: false,
	})
	original.AddColumn(ColumnDefinition{
		Name:     "name",
		Type:     TypeVarchar,
		Nullable: true,
	})

	original.AddConstraint(Constraint{
		Type:          ConstraintTypePrimaryKey,
		Name:          "pk_users",
		ColumnIndices: []uint64{0},
	})

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	entry, err := DeserializeCatalogEntry(r)
	require.NoError(t, err)

	deserialized, ok := entry.(*TableCatalogEntry)
	require.True(t, ok)

	assert.Equal(t, original.Name, deserialized.Name)
	assert.Equal(t, original.Schema, deserialized.Schema)
	assert.Equal(t, original.Catalog, deserialized.Catalog)
	assert.Equal(t, original.SQL, deserialized.SQL)

	require.Equal(t, len(original.Columns), len(deserialized.Columns))
	for i := range original.Columns {
		assert.Equal(t, original.Columns[i].Name, deserialized.Columns[i].Name)
		assert.Equal(t, original.Columns[i].Type, deserialized.Columns[i].Type)
		assert.Equal(t, original.Columns[i].Nullable, deserialized.Columns[i].Nullable)
	}

	require.Equal(t, len(original.Constraints), len(deserialized.Constraints))
	assert.Equal(t, original.Constraints[0].Type, deserialized.Constraints[0].Type)
	assert.Equal(t, original.Constraints[0].Name, deserialized.Constraints[0].Name)
	assert.Equal(t, original.Constraints[0].ColumnIndices, deserialized.Constraints[0].ColumnIndices)
}

// TestViewCatalogEntryRoundTrip verifies that ViewCatalogEntry serializes and deserializes correctly.
func TestViewCatalogEntryRoundTrip(t *testing.T) {
	original := NewViewCatalogEntry("active_users", "SELECT * FROM users WHERE active = true")
	original.Schema = testSchemaPublic
	original.Catalog = testCatalogMain
	original.Aliases = []string{"id", "name", "email"}
	original.Types = []LogicalTypeID{TypeInteger, TypeVarchar, TypeVarchar}
	original.TypeModifiers = []TypeModifiers{{}, {}, {}}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	entry, err := DeserializeCatalogEntry(r)
	require.NoError(t, err)

	deserialized, ok := entry.(*ViewCatalogEntry)
	require.True(t, ok)

	assert.Equal(t, original.Name, deserialized.Name)
	assert.Equal(t, original.Schema, deserialized.Schema)
	assert.Equal(t, original.Query, deserialized.Query)
	assert.Equal(t, original.Aliases, deserialized.Aliases)
	assert.Equal(t, original.Types, deserialized.Types)
	require.Equal(t, len(original.TypeModifiers), len(deserialized.TypeModifiers))
}

// TestIndexCatalogEntryRoundTrip verifies that IndexCatalogEntry serializes and deserializes correctly.
func TestIndexCatalogEntryRoundTrip(t *testing.T) {
	original := NewIndexCatalogEntry("idx_users_email", "users")
	original.Schema = testSchemaPublic
	original.IndexType = IndexTypeART
	original.Constraint = IndexConstraintUnique
	original.ColumnIDs = []uint64{2}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	entry, err := DeserializeCatalogEntry(r)
	require.NoError(t, err)

	deserialized, ok := entry.(*IndexCatalogEntry)
	require.True(t, ok)

	assert.Equal(t, original.Name, deserialized.Name)
	assert.Equal(t, original.TableName, deserialized.TableName)
	assert.Equal(t, original.IndexType, deserialized.IndexType)
	assert.Equal(t, original.Constraint, deserialized.Constraint)
	assert.Equal(t, original.ColumnIDs, deserialized.ColumnIDs)
}

// TestSequenceCatalogEntryRoundTrip verifies that SequenceCatalogEntry serializes and deserializes correctly.
func TestSequenceCatalogEntryRoundTrip(t *testing.T) {
	original := NewSequenceCatalogEntry("user_id_seq")
	original.Schema = testSchemaPublic
	original.StartWith = 1
	original.Increment = 1
	original.MinValue = 1
	original.MaxValue = 9223372036854775807
	original.Cycle = false
	original.Counter = 42

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	entry, err := DeserializeCatalogEntry(r)
	require.NoError(t, err)

	deserialized, ok := entry.(*SequenceCatalogEntry)
	require.True(t, ok)

	assert.Equal(t, original.Name, deserialized.Name)
	assert.Equal(t, original.StartWith, deserialized.StartWith)
	assert.Equal(t, original.Increment, deserialized.Increment)
	assert.Equal(t, original.MinValue, deserialized.MinValue)
	assert.Equal(t, original.MaxValue, deserialized.MaxValue)
	assert.Equal(t, original.Cycle, deserialized.Cycle)
	assert.Equal(t, original.Counter, deserialized.Counter)
}

// TestTypeCatalogEntryRoundTrip verifies that TypeCatalogEntry serializes and deserializes correctly.
func TestTypeCatalogEntryRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		entry *TypeCatalogEntry
	}{
		{
			name:  "simple type alias",
			entry: NewTypeCatalogEntry("my_int", TypeInteger),
		},
		{
			name:  "enum type",
			entry: NewEnumTypeCatalogEntry("status", []string{"pending", "active", "completed"}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.entry.Schema = testSchemaPublic

			var buf bytes.Buffer
			w := NewBinaryWriter(&buf)

			err := tc.entry.Serialize(w)
			require.NoError(t, err)

			r := NewBinaryReader(&buf)
			entry, err := DeserializeCatalogEntry(r)
			require.NoError(t, err)

			deserialized, ok := entry.(*TypeCatalogEntry)
			require.True(t, ok)

			assert.Equal(t, tc.entry.Name, deserialized.Name)
			assert.Equal(t, tc.entry.TypeID, deserialized.TypeID)
			assertTypeModifiersEqual(t, &tc.entry.TypeModifiers, &deserialized.TypeModifiers)
		})
	}
}

// TestDeserializeCatalogEntry verifies that DeserializeCatalogEntry correctly dispatches to the right deserializer.
func TestDeserializeCatalogEntry(t *testing.T) {
	entries := []CatalogEntry{
		NewSchemaCatalogEntry("test_schema"),
		NewTableCatalogEntry("test_table"),
		NewViewCatalogEntry("test_view", "SELECT 1"),
		NewIndexCatalogEntry("test_index", "test_table"),
		NewSequenceCatalogEntry("test_seq"),
		NewTypeCatalogEntry("test_type", TypeInteger),
	}

	for _, original := range entries {
		t.Run(original.Type().String(), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewBinaryWriter(&buf)

			err := SerializeCatalogEntry(w, original)
			require.NoError(t, err)

			r := NewBinaryReader(&buf)
			deserialized, err := DeserializeCatalogEntry(r)
			require.NoError(t, err)

			assert.Equal(t, original.Type(), deserialized.Type())
			assert.Equal(t, original.GetName(), deserialized.GetName())
		})
	}
}

// TestNestedTypeModifiersRoundTrip tests deeply nested type modifiers.
func TestNestedTypeModifiersRoundTrip(t *testing.T) {
	// Test deeply nested type: LIST<MAP<VARCHAR, STRUCT<x INTEGER, y DOUBLE>>>
	original := TypeModifiers{
		ChildTypeID: TypeMap,
		ChildType: &TypeModifiers{
			KeyTypeID: TypeVarchar,
			KeyType:   &TypeModifiers{},
			ValueTypeID: TypeStruct,
			ValueType: &TypeModifiers{
				StructFields: []StructField{
					{Name: "x", Type: TypeInteger},
					{Name: "y", Type: TypeDouble},
				},
			},
		},
	}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	var deserialized TypeModifiers
	err = deserialized.Deserialize(r)
	require.NoError(t, err)

	assertTypeModifiersEqual(t, &original, &deserialized)
}

// TestTableWithAllConstraintTypesRoundTrip tests a table with all constraint types.
func TestTableWithAllConstraintTypesRoundTrip(t *testing.T) {
	original := NewTableCatalogEntry("orders")
	original.Schema = testSchemaPublic

	original.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	original.AddColumn(ColumnDefinition{Name: "user_id", Type: TypeInteger})
	original.AddColumn(ColumnDefinition{Name: "email", Type: TypeVarchar})
	original.AddColumn(ColumnDefinition{
		Name: "amount",
		Type: TypeDecimal,
		TypeModifiers: TypeModifiers{
			Width: 18,
			Scale: 2,
		},
	})

	// Add all constraint types
	original.AddConstraint(Constraint{
		Type:          ConstraintTypePrimaryKey,
		Name:          "pk_orders",
		ColumnIndices: []uint64{0},
	})

	original.AddConstraint(Constraint{
		Type:          ConstraintTypeUnique,
		Name:          "uq_email",
		ColumnIndices: []uint64{2},
	})

	original.AddConstraint(Constraint{
		Type:          ConstraintTypeNotNull,
		ColumnIndices: []uint64{1},
	})

	original.AddConstraint(Constraint{
		Type:       ConstraintTypeCheck,
		Name:       "chk_amount",
		Expression: "amount > 0",
	})

	original.AddConstraint(Constraint{
		Type:          ConstraintTypeForeignKey,
		Name:          "fk_user",
		ColumnIndices: []uint64{1},
		ForeignKey: &ForeignKeyInfo{
			ReferencedSchema:  testSchemaPublic,
			ReferencedTable:   "users",
			ReferencedColumns: []string{"id"},
			OnDelete:          ForeignKeyActionCascade,
			OnUpdate:          ForeignKeyActionNoAction,
		},
	})

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	entry, err := DeserializeCatalogEntry(r)
	require.NoError(t, err)

	deserialized, ok := entry.(*TableCatalogEntry)
	require.True(t, ok)

	assert.Equal(t, original.Name, deserialized.Name)
	require.Equal(t, len(original.Columns), len(deserialized.Columns))
	require.Equal(t, len(original.Constraints), len(deserialized.Constraints))

	// Check each constraint
	for i := range original.Constraints {
		assert.Equal(t, original.Constraints[i].Type, deserialized.Constraints[i].Type)
		assert.Equal(t, original.Constraints[i].Name, deserialized.Constraints[i].Name)
	}
}

// TestEmptyCollectionsRoundTrip tests serialization with empty collections.
func TestEmptyCollectionsRoundTrip(t *testing.T) {
	original := NewTableCatalogEntry("empty_table")
	// Columns and Constraints are already empty slices

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	entry, err := DeserializeCatalogEntry(r)
	require.NoError(t, err)

	deserialized, ok := entry.(*TableCatalogEntry)
	require.True(t, ok)

	assert.Equal(t, original.Name, deserialized.Name)
	assert.Empty(t, deserialized.Columns)
	assert.Empty(t, deserialized.Constraints)
}

// TestViewWithNoAliasesRoundTrip tests view serialization with no aliases.
func TestViewWithNoAliasesRoundTrip(t *testing.T) {
	original := NewViewCatalogEntry("simple_view", "SELECT 1 AS val")
	// Aliases, Types, and TypeModifiers are empty

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	entry, err := DeserializeCatalogEntry(r)
	require.NoError(t, err)

	deserialized, ok := entry.(*ViewCatalogEntry)
	require.True(t, ok)

	assert.Equal(t, original.Name, deserialized.Name)
	assert.Equal(t, original.Query, deserialized.Query)
	assert.Empty(t, deserialized.Aliases)
}

// TestIndexWithExpressionsRoundTrip tests index serialization with expressions.
func TestIndexWithExpressionsRoundTrip(t *testing.T) {
	original := NewIndexCatalogEntry("idx_lower_email", "users")
	original.IndexType = IndexTypeHash
	original.Expressions = []string{"lower(email)"}
	// No column IDs when using expressions

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	entry, err := DeserializeCatalogEntry(r)
	require.NoError(t, err)

	deserialized, ok := entry.(*IndexCatalogEntry)
	require.True(t, ok)

	assert.Equal(t, original.Name, deserialized.Name)
	assert.Equal(t, original.IndexType, deserialized.IndexType)
	assert.Equal(t, original.Expressions, deserialized.Expressions)
	assert.Empty(t, deserialized.ColumnIDs)
}

// TestSequenceWithCycleRoundTrip tests sequence serialization with cycle enabled.
func TestSequenceWithCycleRoundTrip(t *testing.T) {
	original := NewSequenceCatalogEntry("cyclic_seq")
	original.StartWith = 1
	original.Increment = 10
	original.MinValue = 1
	original.MaxValue = 100
	original.Cycle = true
	original.Counter = 91

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := original.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	entry, err := DeserializeCatalogEntry(r)
	require.NoError(t, err)

	deserialized, ok := entry.(*SequenceCatalogEntry)
	require.True(t, ok)

	assert.Equal(t, original.StartWith, deserialized.StartWith)
	assert.Equal(t, original.Increment, deserialized.Increment)
	assert.Equal(t, original.MinValue, deserialized.MinValue)
	assert.Equal(t, original.MaxValue, deserialized.MaxValue)
	assert.Equal(t, original.Cycle, deserialized.Cycle)
	assert.Equal(t, original.Counter, deserialized.Counter)
}

// TestDeserializeInvalidCatalogType tests handling of invalid catalog types.
func TestDeserializeInvalidCatalogType(t *testing.T) {
	// Create a buffer with an invalid catalog type byte
	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)
	w.WriteUint8(uint8(CatalogInvalid))

	r := NewBinaryReader(&buf)
	_, err := DeserializeCatalogEntry(r)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid")
}

// TestDeserializeUnsupportedCatalogType tests handling of unsupported catalog types.
func TestDeserializeUnsupportedCatalogType(t *testing.T) {
	// Create a buffer with an unsupported catalog type byte
	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)
	w.WriteUint8(uint8(CatalogPreparedStatement)) // Unsupported type

	r := NewBinaryReader(&buf)
	_, err := DeserializeCatalogEntry(r)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported")
}

// TestCreateInfoRoundTrip tests CreateInfo serialization and deserialization.
func TestCreateInfoRoundTrip(t *testing.T) {
	original := CreateInfo{
		Catalog:    testCatalogMain,
		Schema:     testSchemaPublic,
		Temporary:  true,
		Internal:   false,
		OnConflict: OnCreateConflictReplace,
		SQL:        "CREATE TABLE test (id INTEGER);",
		Comment:    "Test table",
		Tags: map[string]string{
			"owner": "test",
			"env":   "dev",
		},
		Dependencies: []DependencyEntry{
			{
				Catalog:        testCatalogMain,
				Schema:         testSchemaPublic,
				Name:           "other_table",
				Type:           CatalogTableEntry,
				DependencyType: DependencyTypeRegular,
			},
		},
	}

	// Wrap in a schema entry to test round-trip
	schema := &SchemaCatalogEntry{
		CreateInfo: original,
		Name:       "test_schema",
	}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := schema.Serialize(w)
	require.NoError(t, err)

	r := NewBinaryReader(&buf)
	entry, err := DeserializeCatalogEntry(r)
	require.NoError(t, err)

	deserialized, ok := entry.(*SchemaCatalogEntry)
	require.True(t, ok)

	assert.Equal(t, original.Catalog, deserialized.Catalog)
	assert.Equal(t, original.Schema, deserialized.Schema)
	assert.Equal(t, original.Temporary, deserialized.Temporary)
	assert.Equal(t, original.Internal, deserialized.Internal)
	assert.Equal(t, original.OnConflict, deserialized.OnConflict)
	assert.Equal(t, original.SQL, deserialized.SQL)
	assert.Equal(t, original.Comment, deserialized.Comment)
	assert.Equal(t, original.Tags, deserialized.Tags)
	require.Equal(t, len(original.Dependencies), len(deserialized.Dependencies))
	assert.Equal(t, original.Dependencies[0].Name, deserialized.Dependencies[0].Name)
}
