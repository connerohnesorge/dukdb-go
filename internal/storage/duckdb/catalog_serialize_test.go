package duckdb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test constants for common string values.
const (
	testCatalogMain  = "main"
	testSchemaPublic = "public"
)

func TestCreateInfoSerialize(t *testing.T) {
	ci := CreateInfo{
		Catalog:    testCatalogMain,
		Schema:     testSchemaPublic,
		Temporary:  false,
		Internal:   false,
		OnConflict: OnCreateConflictError,
		SQL:        "CREATE TABLE test (id INTEGER);",
		Comment:    "Test table",
		Tags: map[string]string{
			"owner": "test",
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

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := ci.Serialize(w)
	require.NoError(t, err)

	// Verify data was written
	assert.Greater(t, buf.Len(), 0, "Serialization should produce output")

	// Read back and verify property IDs are present
	data := buf.Bytes()
	assert.True(t, len(data) > 4, "Should have at least one property ID")
}

func TestDependencyEntrySerialize(t *testing.T) {
	dep := DependencyEntry{
		Catalog:        testCatalogMain,
		Schema:         testSchemaPublic,
		Name:           "my_table",
		Type:           CatalogTableEntry,
		DependencyType: DependencyTypeAutomatic,
	}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := dep.Serialize(w)
	require.NoError(t, err)

	assert.Greater(t, buf.Len(), 0)
}

func TestTypeModifiersSerialize(t *testing.T) {
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

			assert.Greater(t, buf.Len(), 0, "Serialization should produce output")
		})
	}
}

func TestStructFieldSerialize(t *testing.T) {
	sf := StructField{
		Name: "my_field",
		Type: TypeVarchar,
		TypeModifiers: &TypeModifiers{
			Collation: "C",
		},
	}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := sf.Serialize(w)
	require.NoError(t, err)

	assert.Greater(t, buf.Len(), 0)
}

func TestUnionMemberSerialize(t *testing.T) {
	um := UnionMember{
		Tag:  "value",
		Type: TypeDouble,
	}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := um.Serialize(w)
	require.NoError(t, err)

	assert.Greater(t, buf.Len(), 0)
}

func TestColumnDefinitionSerialize(t *testing.T) {
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

			assert.Greater(t, buf.Len(), 0)
		})
	}
}

func TestConstraintSerialize(t *testing.T) {
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

			assert.Greater(t, buf.Len(), 0)
		})
	}
}

func TestForeignKeyInfoSerialize(t *testing.T) {
	fk := ForeignKeyInfo{
		ReferencedSchema:  testSchemaPublic,
		ReferencedTable:   "orders",
		ReferencedColumns: []string{"id", "version"},
		OnDelete:          ForeignKeyActionSetNull,
		OnUpdate:          ForeignKeyActionRestrict,
	}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := fk.Serialize(w)
	require.NoError(t, err)

	assert.Greater(t, buf.Len(), 0)
}

func TestSchemaCatalogEntrySerialize(t *testing.T) {
	schema := NewSchemaCatalogEntry("my_schema")
	schema.Catalog = testCatalogMain
	schema.Comment = "A test schema"

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := schema.Serialize(w)
	require.NoError(t, err)

	// Verify first byte is the catalog type
	data := buf.Bytes()
	assert.Equal(t, uint8(CatalogSchemaEntry), data[0])
	assert.Greater(t, len(data), 1)
}

func TestTableCatalogEntrySerialize(t *testing.T) {
	table := NewTableCatalogEntry("users")
	table.Schema = testSchemaPublic
	table.Catalog = testCatalogMain
	table.SQL = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR);"

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
		Name:          "pk_users",
		ColumnIndices: []uint64{0},
	})

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := table.Serialize(w)
	require.NoError(t, err)

	data := buf.Bytes()
	assert.Equal(t, uint8(CatalogTableEntry), data[0])
	assert.Greater(t, len(data), 50, "Table should have substantial serialized size")
}

func TestViewCatalogEntrySerialize(t *testing.T) {
	view := NewViewCatalogEntry("active_users", "SELECT * FROM users WHERE active = true")
	view.Schema = testSchemaPublic
	view.Catalog = testCatalogMain
	view.Aliases = []string{"id", "name", "email"}
	view.Types = []LogicalTypeID{TypeInteger, TypeVarchar, TypeVarchar}
	view.TypeModifiers = []TypeModifiers{{}, {}, {}}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := view.Serialize(w)
	require.NoError(t, err)

	data := buf.Bytes()
	assert.Equal(t, uint8(CatalogViewEntry), data[0])
	assert.Greater(t, len(data), 30)
}

func TestIndexCatalogEntrySerialize(t *testing.T) {
	index := NewIndexCatalogEntry("idx_users_email", "users")
	index.Schema = testSchemaPublic
	index.IndexType = IndexTypeART
	index.Constraint = IndexConstraintUnique
	index.ColumnIDs = []uint64{2}

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := index.Serialize(w)
	require.NoError(t, err)

	data := buf.Bytes()
	assert.Equal(t, uint8(CatalogIndexEntry), data[0])
}

func TestSequenceCatalogEntrySerialize(t *testing.T) {
	seq := NewSequenceCatalogEntry("user_id_seq")
	seq.Schema = testSchemaPublic
	seq.StartWith = 1
	seq.Increment = 1
	seq.MinValue = 1
	seq.MaxValue = 9223372036854775807
	seq.Cycle = false
	seq.Counter = 42

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := seq.Serialize(w)
	require.NoError(t, err)

	data := buf.Bytes()
	assert.Equal(t, uint8(CatalogSequenceEntry), data[0])
}

func TestTypeCatalogEntrySerialize(t *testing.T) {
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

			data := buf.Bytes()
			assert.Equal(t, uint8(CatalogTypeEntry), data[0])
		})
	}
}

func TestSerializeCatalogEntry(t *testing.T) {
	entries := []CatalogEntry{
		NewSchemaCatalogEntry("test_schema"),
		NewTableCatalogEntry("test_table"),
		NewViewCatalogEntry("test_view", "SELECT 1"),
		NewIndexCatalogEntry("test_index", "test_table"),
		NewSequenceCatalogEntry("test_seq"),
		NewTypeCatalogEntry("test_type", TypeInteger),
	}

	for _, entry := range entries {
		t.Run(entry.Type().String(), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewBinaryWriter(&buf)

			err := SerializeCatalogEntry(w, entry)
			require.NoError(t, err)

			data := buf.Bytes()
			assert.Equal(t, uint8(entry.Type()), data[0], "First byte should be catalog type")
		})
	}
}

func TestSerializeWithError(t *testing.T) {
	// Create a writer that fails after writing a few bytes
	errWriter := &limitedWriter{limit: 10}
	w := NewBinaryWriter(errWriter)

	table := NewTableCatalogEntry("test")
	table.AddColumn(ColumnDefinition{Name: "col1", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "col2", Type: TypeVarchar})

	// This should fail partway through
	err := table.Serialize(w)
	assert.Error(t, err)
}

// limitedWriter is a test helper that fails after writing a certain number of bytes.
type limitedWriter struct {
	written int
	limit   int
}

func (lw *limitedWriter) Write(p []byte) (n int, err error) {
	if lw.written+len(p) > lw.limit {
		// Write what we can
		remaining := lw.limit - lw.written
		if remaining > 0 {
			lw.written += remaining
			return remaining, errWriteLimitReached
		}

		return 0, errWriteLimitReached
	}

	lw.written += len(p)

	return len(p), nil
}

var errWriteLimitReached = assert.AnError

func TestNestedTypeModifiersSerialize(t *testing.T) {
	// Test deeply nested type: LIST<MAP<VARCHAR, STRUCT<x INTEGER, y DOUBLE>>>
	tm := TypeModifiers{
		ChildTypeID: TypeMap,
		ChildType: &TypeModifiers{
			KeyTypeID:   TypeVarchar,
			KeyType:     &TypeModifiers{},
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

	err := tm.Serialize(w)
	require.NoError(t, err)

	assert.Greater(t, buf.Len(), 20, "Nested types should have substantial serialized size")
}

func TestTableWithAllConstraintTypes(t *testing.T) {
	table := NewTableCatalogEntry("orders")
	table.Schema = testSchemaPublic

	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "user_id", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "email", Type: TypeVarchar})
	table.AddColumn(
		ColumnDefinition{
			Name:          "amount",
			Type:          TypeDecimal,
			TypeModifiers: TypeModifiers{Width: 18, Scale: 2},
		},
	)

	// Add all constraint types
	table.AddConstraint(Constraint{
		Type:          ConstraintTypePrimaryKey,
		Name:          "pk_orders",
		ColumnIndices: []uint64{0},
	})

	table.AddConstraint(Constraint{
		Type:          ConstraintTypeUnique,
		Name:          "uq_email",
		ColumnIndices: []uint64{2},
	})

	table.AddConstraint(Constraint{
		Type:          ConstraintTypeNotNull,
		ColumnIndices: []uint64{1},
	})

	table.AddConstraint(Constraint{
		Type:       ConstraintTypeCheck,
		Name:       "chk_amount",
		Expression: "amount > 0",
	})

	table.AddConstraint(Constraint{
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

	err := table.Serialize(w)
	require.NoError(t, err)

	assert.Greater(
		t,
		buf.Len(),
		100,
		"Table with many constraints should have large serialized size",
	)
}

func TestEmptyCollections(t *testing.T) {
	// Test serialization with empty collections (but non-nil)
	table := NewTableCatalogEntry("empty_table")
	// Columns and Constraints are already empty slices

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := table.Serialize(w)
	require.NoError(t, err)

	assert.Greater(t, buf.Len(), 0)
}

func TestViewWithNoAliases(t *testing.T) {
	view := NewViewCatalogEntry("simple_view", "SELECT 1 AS val")
	// Aliases, Types, and TypeModifiers are empty

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := view.Serialize(w)
	require.NoError(t, err)

	assert.Greater(t, buf.Len(), 0)
}

func TestIndexWithExpressions(t *testing.T) {
	index := NewIndexCatalogEntry("idx_lower_email", "users")
	index.IndexType = IndexTypeHash
	index.Expressions = []string{"lower(email)"}
	// No column IDs when using expressions

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := index.Serialize(w)
	require.NoError(t, err)

	assert.Greater(t, buf.Len(), 0)
}

func TestSequenceWithCycle(t *testing.T) {
	seq := NewSequenceCatalogEntry("cyclic_seq")
	seq.StartWith = 1
	seq.Increment = 10
	seq.MinValue = 1
	seq.MaxValue = 100
	seq.Cycle = true
	seq.Counter = 91

	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	err := seq.Serialize(w)
	require.NoError(t, err)

	assert.Greater(t, buf.Len(), 0)
}
