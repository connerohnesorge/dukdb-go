package duckdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testBlockManagerSetup creates a temporary file and block manager for testing.
// It returns a cleanup function that should be deferred.
func testBlockManagerSetup(t *testing.T) (*BlockManager, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	f, err := os.Create(tmpFile)
	require.NoError(t, err)

	bm := NewBlockManager(f, DefaultBlockSize, DefaultCacheCapacity)

	cleanup := func() {
		_ = bm.Close()
		_ = f.Close()
	}

	return bm, cleanup
}

func TestNewDuckDBCatalog(t *testing.T) {
	catalog := NewDuckDBCatalog()

	assert.NotNil(t, catalog)
	assert.Empty(t, catalog.Schemas)
	assert.Empty(t, catalog.Tables)
	assert.Empty(t, catalog.Views)
	assert.Empty(t, catalog.Indexes)
	assert.Empty(t, catalog.Sequences)
	assert.Empty(t, catalog.Types)
	assert.True(t, catalog.IsEmpty())
	assert.Equal(t, 0, catalog.EntryCount())
}

func TestDuckDBCatalog_AddEntries(t *testing.T) {
	catalog := NewDuckDBCatalog()

	// Add schema
	schema := NewSchemaCatalogEntry("main")
	catalog.AddSchema(schema)
	assert.Equal(t, 1, len(catalog.Schemas))
	assert.Equal(t, 1, catalog.EntryCount())

	// Add table
	table := NewTableCatalogEntry("users")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "name", Type: TypeVarchar})
	catalog.AddTable(table)
	assert.Equal(t, 1, len(catalog.Tables))
	assert.Equal(t, 2, catalog.EntryCount())

	// Add view
	view := NewViewCatalogEntry("active_users", "SELECT * FROM users WHERE active = true")
	catalog.AddView(view)
	assert.Equal(t, 1, len(catalog.Views))
	assert.Equal(t, 3, catalog.EntryCount())

	// Add index
	index := NewIndexCatalogEntry("idx_users_name", "users")
	index.ColumnIDs = []uint64{1}
	catalog.AddIndex(index)
	assert.Equal(t, 1, len(catalog.Indexes))
	assert.Equal(t, 4, catalog.EntryCount())

	// Add sequence
	sequence := NewSequenceCatalogEntry("user_id_seq")
	catalog.AddSequence(sequence)
	assert.Equal(t, 1, len(catalog.Sequences))
	assert.Equal(t, 5, catalog.EntryCount())

	// Add custom type
	enumType := NewEnumTypeCatalogEntry("status", []string{"active", "inactive", "pending"})
	catalog.AddType(enumType)
	assert.Equal(t, 1, len(catalog.Types))
	assert.Equal(t, 6, catalog.EntryCount())

	assert.False(t, catalog.IsEmpty())
}

func TestDuckDBCatalog_GetTable(t *testing.T) {
	catalog := NewDuckDBCatalog()

	// Add tables
	table1 := NewTableCatalogEntry("users")
	table2 := NewTableCatalogEntry("orders")
	catalog.AddTable(table1)
	catalog.AddTable(table2)

	// Test GetTable
	found := catalog.GetTable("users")
	assert.NotNil(t, found)
	assert.Equal(t, "users", found.Name)

	found = catalog.GetTable("orders")
	assert.NotNil(t, found)
	assert.Equal(t, "orders", found.Name)

	found = catalog.GetTable("nonexistent")
	assert.Nil(t, found)

	// Test GetTableByOID
	found = catalog.GetTableByOID(0)
	assert.NotNil(t, found)
	assert.Equal(t, "users", found.Name)

	found = catalog.GetTableByOID(1)
	assert.NotNil(t, found)
	assert.Equal(t, "orders", found.Name)

	found = catalog.GetTableByOID(99)
	assert.Nil(t, found)
}

func TestNewCatalogWriter(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()
	writer := NewCatalogWriter(bm, catalog)

	assert.NotNil(t, writer)
	assert.Equal(t, catalog, writer.Catalog())
	assert.Equal(t, bm, writer.BlockManager())
	assert.False(t, writer.IsClosed())
	assert.Equal(t, 0, writer.MetaBlockCount())
	assert.Equal(t, 0, writer.GetRowGroupCount())
}

func TestCatalogWriter_AddRowGroupPointer(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()
	table := NewTableCatalogEntry("users")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	catalog.AddTable(table)

	writer := NewCatalogWriter(bm, catalog)

	// Add row group pointer for existing table
	rgp := &RowGroupPointer{
		TableOID:   0,
		RowStart:   0,
		TupleCount: 1000,
		DataPointers: []MetaBlockPointer{
			{BlockID: 1, Offset: 0},
		},
	}
	err := writer.AddRowGroupPointer(0, rgp)
	assert.NoError(t, err)
	assert.Equal(t, 1, writer.GetRowGroupCount())
	assert.Equal(t, 1, writer.GetTableRowGroupCount(0))

	// Add another row group to same table
	rgp2 := &RowGroupPointer{
		TableOID:   0,
		RowStart:   1000,
		TupleCount: 500,
		DataPointers: []MetaBlockPointer{
			{BlockID: 2, Offset: 0},
		},
	}
	err = writer.AddRowGroupPointer(0, rgp2)
	assert.NoError(t, err)
	assert.Equal(t, 2, writer.GetRowGroupCount())
	assert.Equal(t, 2, writer.GetTableRowGroupCount(0))

	// Try adding to non-existent table
	err = writer.AddRowGroupPointer(99, rgp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table not found")

	// Verify table OIDs
	oids := writer.GetTableOIDs()
	assert.Equal(t, []uint64{0}, oids)
}

func TestCatalogWriter_Write_EmptyCatalog(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()
	writer := NewCatalogWriter(bm, catalog)

	// Write empty catalog - returns invalid MetaBlockPointer
	// This signals to DuckDB that there is no catalog metadata to load
	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.False(t, mbp.IsValid(), "empty catalog should return invalid MetaBlockPointer for DuckDB compatibility")
}

func TestCatalogWriter_Write_WithSchemas(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()
	catalog.AddSchema(NewSchemaCatalogEntry("main"))
	catalog.AddSchema(NewSchemaCatalogEntry("test"))

	writer := NewCatalogWriter(bm, catalog)

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
	assert.Equal(t, 2, writer.MetaBlockCount()) // 2 schemas
}

func TestCatalogWriter_Write_WithTables(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add table with columns
	table := NewTableCatalogEntry("users")
	table.CreateInfo.Schema = "main"
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger, Nullable: false})
	table.AddColumn(ColumnDefinition{Name: "name", Type: TypeVarchar, Nullable: true})
	table.AddColumn(ColumnDefinition{Name: "email", Type: TypeVarchar, Nullable: true})
	catalog.AddTable(table)

	writer := NewCatalogWriter(bm, catalog)

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
	assert.Equal(t, 1, writer.MetaBlockCount())
}

func TestCatalogWriter_Write_WithRowGroups(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add table
	table := NewTableCatalogEntry("users")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "name", Type: TypeVarchar})
	catalog.AddTable(table)

	writer := NewCatalogWriter(bm, catalog)

	// Add row groups
	rgp1 := &RowGroupPointer{
		TableOID:   0,
		RowStart:   0,
		TupleCount: 1000,
		DataPointers: []MetaBlockPointer{
			{BlockID: 10, Offset: 0},
			{BlockID: 11, Offset: 0},
		},
	}
	rgp2 := &RowGroupPointer{
		TableOID:   0,
		RowStart:   1000,
		TupleCount: 500,
		DataPointers: []MetaBlockPointer{
			{BlockID: 20, Offset: 0},
			{BlockID: 21, Offset: 0},
		},
	}
	require.NoError(t, writer.AddRowGroupPointer(0, rgp1))
	require.NoError(t, writer.AddRowGroupPointer(0, rgp2))

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())

	// Should have 1 table block + 1 row group block = 2 total meta blocks
	// (row groups block is not tracked in metaBlocks as it's internal)
	assert.Equal(t, 1, writer.MetaBlockCount())
}

func TestCatalogWriter_Write_ComplexCatalog(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add schemas
	catalog.AddSchema(NewSchemaCatalogEntry("main"))
	catalog.AddSchema(NewSchemaCatalogEntry("analytics"))

	// Add tables
	users := NewTableCatalogEntry("users")
	users.CreateInfo.Schema = "main"
	users.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	users.AddColumn(ColumnDefinition{Name: "name", Type: TypeVarchar})
	users.AddColumn(ColumnDefinition{Name: "status", Type: TypeVarchar})
	catalog.AddTable(users)

	orders := NewTableCatalogEntry("orders")
	orders.CreateInfo.Schema = "main"
	orders.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	orders.AddColumn(ColumnDefinition{Name: "user_id", Type: TypeInteger})
	orders.AddColumn(ColumnDefinition{Name: "total", Type: TypeDouble})
	catalog.AddTable(orders)

	// Add views
	activeUsers := NewViewCatalogEntry("active_users", "SELECT * FROM users WHERE status = 'active'")
	activeUsers.CreateInfo.Schema = "main"
	catalog.AddView(activeUsers)

	// Add indexes
	idxUserName := NewIndexCatalogEntry("idx_users_name", "users")
	idxUserName.CreateInfo.Schema = "main"
	idxUserName.ColumnIDs = []uint64{1}
	catalog.AddIndex(idxUserName)

	idxOrderUser := NewIndexCatalogEntry("idx_orders_user", "orders")
	idxOrderUser.CreateInfo.Schema = "main"
	idxOrderUser.ColumnIDs = []uint64{1}
	catalog.AddIndex(idxOrderUser)

	// Add sequences
	userSeq := NewSequenceCatalogEntry("user_id_seq")
	userSeq.CreateInfo.Schema = "main"
	catalog.AddSequence(userSeq)

	// Add custom types
	statusEnum := NewEnumTypeCatalogEntry("status_type", []string{"active", "inactive", "pending"})
	statusEnum.CreateInfo.Schema = "main"
	catalog.AddType(statusEnum)

	writer := NewCatalogWriter(bm, catalog)

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())

	// 2 schemas + 2 tables + 1 view + 2 indexes + 1 sequence + 1 type = 9
	assert.Equal(t, 9, writer.MetaBlockCount())

	// Verify blocks can be retrieved
	blocks := writer.GetMetaBlocks()
	assert.Equal(t, 9, len(blocks))
}

func TestCatalogWriter_Close(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()
	writer := NewCatalogWriter(bm, catalog)

	assert.False(t, writer.IsClosed())

	err := writer.Close()
	assert.NoError(t, err)
	assert.True(t, writer.IsClosed())

	// Double close should be safe
	err = writer.Close()
	assert.NoError(t, err)

	// Operations after close should fail
	err = writer.AddRowGroupPointer(0, &RowGroupPointer{})
	assert.Error(t, err)
	assert.Equal(t, ErrCatalogWriterClosed, err)

	_, err = writer.Write()
	assert.Error(t, err)
	assert.Equal(t, ErrCatalogWriterClosed, err)
}

func TestCatalogWriter_Write_WithConstraints(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add table with constraints
	table := NewTableCatalogEntry("users")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "email", Type: TypeVarchar})
	table.AddColumn(ColumnDefinition{Name: "age", Type: TypeInteger})

	// Primary key constraint
	table.AddConstraint(Constraint{
		Type:          ConstraintTypePrimaryKey,
		Name:          "pk_users",
		ColumnIndices: []uint64{0},
	})

	// Unique constraint
	table.AddConstraint(Constraint{
		Type:          ConstraintTypeUnique,
		Name:          "uq_users_email",
		ColumnIndices: []uint64{1},
	})

	// Check constraint
	table.AddConstraint(Constraint{
		Type:       ConstraintTypeCheck,
		Name:       "ck_users_age",
		Expression: "age >= 0",
	})

	catalog.AddTable(table)

	writer := NewCatalogWriter(bm, catalog)

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
}

func TestCatalogWriter_Write_WithForeignKey(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add parent table
	users := NewTableCatalogEntry("users")
	users.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	users.AddColumn(ColumnDefinition{Name: "name", Type: TypeVarchar})
	catalog.AddTable(users)

	// Add child table with foreign key
	orders := NewTableCatalogEntry("orders")
	orders.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	orders.AddColumn(ColumnDefinition{Name: "user_id", Type: TypeInteger})

	orders.AddConstraint(Constraint{
		Type:          ConstraintTypeForeignKey,
		Name:          "fk_orders_user",
		ColumnIndices: []uint64{1},
		ForeignKey: &ForeignKeyInfo{
			ReferencedSchema:  "main",
			ReferencedTable:   "users",
			ReferencedColumns: []string{"id"},
			OnDelete:          ForeignKeyActionCascade,
			OnUpdate:          ForeignKeyActionNoAction,
		},
	})

	catalog.AddTable(orders)

	writer := NewCatalogWriter(bm, catalog)

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
}

func TestCatalogWriter_MultipleTables_WithRowGroups(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add multiple tables
	for i := 0; i < 3; i++ {
		table := NewTableCatalogEntry("table_" + string(rune('a'+i)))
		table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
		table.AddColumn(ColumnDefinition{Name: "data", Type: TypeVarchar})
		catalog.AddTable(table)
	}

	writer := NewCatalogWriter(bm, catalog)

	// Add row groups to each table
	for tableOID := uint64(0); tableOID < 3; tableOID++ {
		for i := 0; i < 2; i++ {
			rgp := &RowGroupPointer{
				TableOID:   tableOID,
				RowStart:   uint64(i) * 1000,
				TupleCount: 1000,
				DataPointers: []MetaBlockPointer{
					{BlockID: tableOID*10 + uint64(i), Offset: 0},
					{BlockID: tableOID*10 + uint64(i) + 100, Offset: 0},
				},
			}
			require.NoError(t, writer.AddRowGroupPointer(tableOID, rgp))
		}
	}

	assert.Equal(t, 6, writer.GetRowGroupCount()) // 3 tables * 2 row groups each

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())

	// Verify table OIDs
	oids := writer.GetTableOIDs()
	assert.Equal(t, []uint64{0, 1, 2}, oids)
}

func TestSerializeCatalogToBytes(t *testing.T) {
	catalog := NewDuckDBCatalog()

	// Add some entries
	catalog.AddSchema(NewSchemaCatalogEntry("main"))

	table := NewTableCatalogEntry("users")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	catalog.AddTable(table)

	view := NewViewCatalogEntry("v_users", "SELECT * FROM users")
	catalog.AddView(view)

	sequence := NewSequenceCatalogEntry("user_seq")
	catalog.AddSequence(sequence)

	// Serialize to bytes
	data, err := SerializeCatalogToBytes(catalog)
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify the data starts with the correct version
	assert.True(t, len(data) >= 4)
}

func TestCatalogWriter_GetMetaBlocks(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()
	catalog.AddSchema(NewSchemaCatalogEntry("main"))
	catalog.AddSchema(NewSchemaCatalogEntry("test"))

	writer := NewCatalogWriter(bm, catalog)

	// Before write, no blocks
	blocks := writer.GetMetaBlocks()
	assert.Empty(t, blocks)

	// After write
	_, err := writer.Write()
	require.NoError(t, err)

	blocks = writer.GetMetaBlocks()
	assert.Equal(t, 2, len(blocks))

	// Verify blocks are unique
	blockSet := make(map[uint64]bool)
	for _, b := range blocks {
		assert.False(t, blockSet[b], "duplicate block ID found")
		blockSet[b] = true
	}
}

func TestCatalogWriter_ViewWithTypes(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add view with column types
	view := NewViewCatalogEntry("typed_view", "SELECT id, name FROM users")
	view.Types = []LogicalTypeID{TypeInteger, TypeVarchar}
	view.Aliases = []string{"user_id", "user_name"}
	catalog.AddView(view)

	writer := NewCatalogWriter(bm, catalog)

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
}

func TestCatalogWriter_SequenceOptions(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add sequence with custom options
	seq := NewSequenceCatalogEntry("custom_seq")
	seq.StartWith = 100
	seq.Increment = 10
	seq.MinValue = 1
	seq.MaxValue = 1000000
	seq.Cycle = true
	seq.Counter = 150
	catalog.AddSequence(seq)

	writer := NewCatalogWriter(bm, catalog)

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
}

func TestCatalogWriter_IndexTypes(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add table
	table := NewTableCatalogEntry("users")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "email", Type: TypeVarchar})
	catalog.AddTable(table)

	// Add ART index
	artIdx := NewIndexCatalogEntry("art_idx", "users")
	artIdx.IndexType = IndexTypeART
	artIdx.ColumnIDs = []uint64{0}
	catalog.AddIndex(artIdx)

	// Add Hash index
	hashIdx := NewIndexCatalogEntry("hash_idx", "users")
	hashIdx.IndexType = IndexTypeHash
	hashIdx.ColumnIDs = []uint64{1}
	catalog.AddIndex(hashIdx)

	// Add unique index
	uniqueIdx := NewIndexCatalogEntry("unique_idx", "users")
	uniqueIdx.Constraint = IndexConstraintUnique
	uniqueIdx.ColumnIDs = []uint64{1}
	catalog.AddIndex(uniqueIdx)

	// Add primary key index
	pkIdx := NewIndexCatalogEntry("pk_idx", "users")
	pkIdx.Constraint = IndexConstraintPrimary
	pkIdx.ColumnIDs = []uint64{0}
	catalog.AddIndex(pkIdx)

	writer := NewCatalogWriter(bm, catalog)

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
}

func TestCatalogWriter_EnumType(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add enum type with many values
	enumType := NewEnumTypeCatalogEntry("color", []string{
		"red", "green", "blue", "yellow", "orange",
		"purple", "pink", "black", "white", "gray",
	})
	catalog.AddType(enumType)

	assert.True(t, enumType.IsEnum())

	writer := NewCatalogWriter(bm, catalog)

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
}

func TestCatalogWriter_TableWithDefaults(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add table with default values
	table := NewTableCatalogEntry("users")
	table.AddColumn(ColumnDefinition{
		Name:         "id",
		Type:         TypeInteger,
		Nullable:     false,
		HasDefault:   false,
		DefaultValue: "",
	})
	table.AddColumn(ColumnDefinition{
		Name:         "created_at",
		Type:         TypeTimestamp,
		Nullable:     false,
		HasDefault:   true,
		DefaultValue: "CURRENT_TIMESTAMP",
	})
	table.AddColumn(ColumnDefinition{
		Name:         "active",
		Type:         TypeBoolean,
		Nullable:     false,
		HasDefault:   true,
		DefaultValue: "true",
	})
	catalog.AddTable(table)

	writer := NewCatalogWriter(bm, catalog)

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
}

func TestCatalogWriter_GeneratedColumn(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add table with generated column
	table := NewTableCatalogEntry("products")
	table.AddColumn(ColumnDefinition{
		Name: "price",
		Type: TypeDouble,
	})
	table.AddColumn(ColumnDefinition{
		Name: "quantity",
		Type: TypeInteger,
	})
	table.AddColumn(ColumnDefinition{
		Name:                "total",
		Type:                TypeDouble,
		Generated:           true,
		GeneratedExpression: "price * quantity",
	})
	catalog.AddTable(table)

	writer := NewCatalogWriter(bm, catalog)

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
}

// Test WriteBinaryFormat with empty catalog
func TestCatalogWriter_WriteBinaryFormat_EmptyCatalog(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()
	writer := NewCatalogWriter(bm, catalog)
	writer.SetDuckDBCompatMode(true) // Enable BinarySerializer

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.False(t, mbp.IsValid(), "empty catalog should return invalid pointer")
	assert.Equal(t, InvalidBlockID, mbp.BlockID)
}

// Test WriteBinaryFormat with schema
func TestCatalogWriter_WriteBinaryFormat_WithSchema(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()
	schema := NewSchemaCatalogEntry("test_schema")
	catalog.AddSchema(schema)

	writer := NewCatalogWriter(bm, catalog)
	writer.SetDuckDBCompatMode(true) // Enable BinarySerializer

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid(), "catalog with entries should return valid pointer")
	assert.NotEqual(t, InvalidBlockID, mbp.BlockID)
}

// Test WriteBinaryFormat with table
func TestCatalogWriter_WriteBinaryFormat_WithTable(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()
	table := NewTableCatalogEntry("users")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "name", Type: TypeVarchar})
	catalog.AddTable(table)

	writer := NewCatalogWriter(bm, catalog)
	writer.SetDuckDBCompatMode(true) // Enable BinarySerializer

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
	assert.NotEqual(t, InvalidBlockID, mbp.BlockID)
}

// Test WriteBinaryFormat with multiple entry types
func TestCatalogWriter_WriteBinaryFormat_MultipleEntries(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add schema
	schema := NewSchemaCatalogEntry("main")
	catalog.AddSchema(schema)

	// Add table
	table := NewTableCatalogEntry("users")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	table.AddColumn(ColumnDefinition{Name: "email", Type: TypeVarchar})
	catalog.AddTable(table)

	// Add view
	view := NewViewCatalogEntry("active_users", "SELECT * FROM users WHERE active = true")
	catalog.AddView(view)

	// Add index
	index := NewIndexCatalogEntry("idx_users_email", "users")
	index.ColumnIDs = []uint64{1}
	catalog.AddIndex(index)

	// Add sequence
	seq := NewSequenceCatalogEntry("user_id_seq")
	catalog.AddSequence(seq)

	// Add type
	enumType := NewEnumTypeCatalogEntry("status", []string{"active", "inactive"})
	catalog.AddType(enumType)

	writer := NewCatalogWriter(bm, catalog)
	writer.SetDuckDBCompatMode(true) // Enable BinarySerializer

	mbp, err := writer.Write()
	assert.NoError(t, err)
	assert.True(t, mbp.IsValid())
	assert.NotEqual(t, InvalidBlockID, mbp.BlockID)
}

// Test SetDuckDBCompatMode enables BinarySerializer
func TestCatalogWriter_SetDuckDBCompatMode_EnablesBinarySerializer(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()
	table := NewTableCatalogEntry("test")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	catalog.AddTable(table)

	writer := NewCatalogWriter(bm, catalog)

	// Initially should not use BinarySerializer
	assert.False(t, writer.useBinarySerializer)

	// Enable compat mode should enable BinarySerializer
	writer.SetDuckDBCompatMode(true)
	assert.True(t, writer.duckdbCompatMode)
	assert.True(t, writer.useBinarySerializer)

	// Disable compat mode should disable BinarySerializer
	writer.SetDuckDBCompatMode(false)
	assert.False(t, writer.duckdbCompatMode)
	assert.False(t, writer.useBinarySerializer)
}

// Test collectCatalogEntries ordering
func TestCatalogWriter_CollectCatalogEntries_Ordering(t *testing.T) {
	bm, cleanup := testBlockManagerSetup(t)
	defer cleanup()

	catalog := NewDuckDBCatalog()

	// Add entries in non-standard order
	table := NewTableCatalogEntry("users")
	table.AddColumn(ColumnDefinition{Name: "id", Type: TypeInteger})
	catalog.AddTable(table)

	schema := NewSchemaCatalogEntry("main")
	catalog.AddSchema(schema)

	view := NewViewCatalogEntry("v1", "SELECT 1")
	catalog.AddView(view)

	index := NewIndexCatalogEntry("idx1", "users")
	catalog.AddIndex(index)

	seq := NewSequenceCatalogEntry("seq1")
	catalog.AddSequence(seq)

	typ := NewEnumTypeCatalogEntry("status", []string{"active"})
	catalog.AddType(typ)

	writer := NewCatalogWriter(bm, catalog)

	// Collect entries
	entries := writer.collectCatalogEntries()

	// Verify order: schemas, tables, views, indexes, sequences, types
	require.Len(t, entries, 6)
	assert.Equal(t, CatalogSchemaEntry, entries[0].Type())
	assert.Equal(t, CatalogTableEntry, entries[1].Type())
	assert.Equal(t, CatalogViewEntry, entries[2].Type())
	assert.Equal(t, CatalogIndexEntry, entries[3].Type())
	assert.Equal(t, CatalogSequenceEntry, entries[4].Type())
	assert.Equal(t, CatalogTypeEntry, entries[5].Type())
}
