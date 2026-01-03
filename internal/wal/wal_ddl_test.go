package wal

import (
	"bytes"
	"math"
	"path/filepath"
	"testing"

	"github.com/coder/quartz"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateViewEntry tests CREATE VIEW WAL entry serialization.
func TestCreateViewEntry(t *testing.T) {
	entry := &CreateViewEntry{
		Schema: "main",
		Name:   "my_view",
		Query:  "SELECT id, name FROM users WHERE active = true",
	}

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	readEntry := &CreateViewEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(t, entry.Schema, readEntry.Schema)
	assert.Equal(t, entry.Name, readEntry.Name)
	assert.Equal(t, entry.Query, readEntry.Query)
}

// TestDropViewEntry tests DROP VIEW WAL entry serialization.
func TestDropViewEntry(t *testing.T) {
	entry := &DropViewEntry{
		Schema: "main",
		Name:   "my_view",
	}

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	readEntry := &DropViewEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(t, entry.Schema, readEntry.Schema)
	assert.Equal(t, entry.Name, readEntry.Name)
}

// TestCreateSequenceEntry tests CREATE SEQUENCE WAL entry serialization.
func TestCreateSequenceEntry(t *testing.T) {
	entry := &CreateSequenceEntry{
		Schema:      "main",
		Name:        "my_seq",
		StartWith:   100,
		IncrementBy: 5,
		MinValue:    0,
		MaxValue:    10000,
		IsCycle:     true,
	}

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	readEntry := &CreateSequenceEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(t, entry.Schema, readEntry.Schema)
	assert.Equal(t, entry.Name, readEntry.Name)
	assert.Equal(t, entry.StartWith, readEntry.StartWith)
	assert.Equal(t, entry.IncrementBy, readEntry.IncrementBy)
	assert.Equal(t, entry.MinValue, readEntry.MinValue)
	assert.Equal(t, entry.MaxValue, readEntry.MaxValue)
	assert.Equal(t, entry.IsCycle, readEntry.IsCycle)
}

// TestDropSequenceEntry tests DROP SEQUENCE WAL entry serialization.
func TestDropSequenceEntry(t *testing.T) {
	entry := &DropSequenceEntry{
		Schema: "main",
		Name:   "my_seq",
	}

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	readEntry := &DropSequenceEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(t, entry.Schema, readEntry.Schema)
	assert.Equal(t, entry.Name, readEntry.Name)
}

// TestAlterTableEntry tests ALTER TABLE WAL entry serialization.
func TestAlterTableEntry(t *testing.T) {
	tests := []struct {
		name  string
		entry *AlterTableEntry
	}{
		{
			name: "rename_table",
			entry: &AlterTableEntry{
				Schema:       "main",
				Table:        "old_table",
				Operation:    0, // AlterTableRenameTo
				NewTableName: "new_table",
			},
		},
		{
			name: "rename_column",
			entry: &AlterTableEntry{
				Schema:    "main",
				Table:     "users",
				Operation: 1, // AlterTableRenameColumn
				OldColumn: "name",
				NewColumn: "full_name",
			},
		},
		{
			name: "drop_column",
			entry: &AlterTableEntry{
				Schema:    "main",
				Table:     "users",
				Operation: 2, // AlterTableDropColumn
				Column:    "deprecated_field",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := tt.entry.Serialize(&buf)
			require.NoError(t, err)

			readEntry := &AlterTableEntry{}
			err = readEntry.Deserialize(&buf)
			require.NoError(t, err)

			assert.Equal(t, tt.entry.Schema, readEntry.Schema)
			assert.Equal(t, tt.entry.Table, readEntry.Table)
			assert.Equal(t, tt.entry.Operation, readEntry.Operation)
			assert.Equal(t, tt.entry.NewTableName, readEntry.NewTableName)
			assert.Equal(t, tt.entry.OldColumn, readEntry.OldColumn)
			assert.Equal(t, tt.entry.NewColumn, readEntry.NewColumn)
			assert.Equal(t, tt.entry.Column, readEntry.Column)
		})
	}
}

// TestRecoveryCreateView tests that CREATE VIEW is properly recovered from WAL.
func TestRecoveryCreateView(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write CREATE VIEW to WAL
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	createViewEntry := &CreateViewEntry{
		Schema: "main",
		Name:   "active_users",
		Query:  "SELECT id, name FROM users WHERE active = true",
	}
	err = writer.WriteEntry(createViewEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify view was created
	view, ok := cat.GetView("active_users")
	require.True(t, ok)
	assert.Equal(t, "active_users", view.Name)
	assert.Equal(t, "main", view.Schema)
	assert.Equal(t, "SELECT id, name FROM users WHERE active = true", view.Query)
}

// TestRecoveryDropView tests that DROP VIEW is properly recovered from WAL.
func TestRecoveryDropView(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write CREATE VIEW then DROP VIEW to WAL
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	createViewEntry := &CreateViewEntry{
		Schema: "main",
		Name:   "temp_view",
		Query:  "SELECT * FROM users",
	}
	err = writer.WriteEntry(createViewEntry)
	require.NoError(t, err)

	dropViewEntry := &DropViewEntry{
		Schema: "main",
		Name:   "temp_view",
	}
	err = writer.WriteEntry(dropViewEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify view was dropped
	_, ok := cat.GetView("temp_view")
	assert.False(t, ok, "View should not exist after DROP")
}

// TestRecoveryCreateIndex tests that CREATE INDEX is properly recovered from WAL.
func TestRecoveryCreateIndex(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write CREATE INDEX to WAL
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	createIndexEntry := &CreateIndexEntry{
		Schema:    "main",
		Table:     "users",
		Name:      "idx_users_email",
		Columns:   []string{"email"},
		IsUnique:  true,
		IsPrimary: false,
	}
	err = writer.WriteEntry(createIndexEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify index was created
	index, ok := cat.GetIndex("idx_users_email")
	require.True(t, ok)
	assert.Equal(t, "idx_users_email", index.Name)
	assert.Equal(t, "main", index.Schema)
	assert.Equal(t, "users", index.Table)
	assert.Equal(t, []string{"email"}, index.Columns)
	assert.True(t, index.IsUnique)
	assert.False(t, index.IsPrimary)
}

// TestRecoveryDropIndex tests that DROP INDEX is properly recovered from WAL.
func TestRecoveryDropIndex(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write CREATE INDEX then DROP INDEX to WAL
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	createIndexEntry := &CreateIndexEntry{
		Schema:   "main",
		Table:    "users",
		Name:     "temp_idx",
		Columns:  []string{"created_at"},
		IsUnique: false,
	}
	err = writer.WriteEntry(createIndexEntry)
	require.NoError(t, err)

	dropIndexEntry := &DropIndexEntry{
		Schema: "main",
		Table:  "users",
		Name:   "temp_idx",
	}
	err = writer.WriteEntry(dropIndexEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify index was dropped
	_, ok := cat.GetIndex("temp_idx")
	assert.False(t, ok, "Index should not exist after DROP")
}

// TestRecoveryCreateSequence tests that CREATE SEQUENCE is properly recovered from WAL.
func TestRecoveryCreateSequence(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write CREATE SEQUENCE to WAL
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	createSeqEntry := &CreateSequenceEntry{
		Schema:      "main",
		Name:        "user_id_seq",
		StartWith:   1000,
		IncrementBy: 1,
		MinValue:    0,
		MaxValue:    math.MaxInt64,
		IsCycle:     false,
	}
	err = writer.WriteEntry(createSeqEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify sequence was created
	seq, ok := cat.GetSequence("user_id_seq")
	require.True(t, ok)
	assert.Equal(t, "user_id_seq", seq.Name)
	assert.Equal(t, "main", seq.Schema)
	assert.Equal(t, int64(1000), seq.StartWith)
	assert.Equal(t, int64(1000), seq.CurrentVal) // Should be initialized to StartWith
	assert.Equal(t, int64(1), seq.IncrementBy)
	assert.Equal(t, int64(0), seq.MinValue)
	assert.Equal(t, int64(math.MaxInt64), seq.MaxValue)
	assert.False(t, seq.IsCycle)
}

// TestRecoveryDropSequence tests that DROP SEQUENCE is properly recovered from WAL.
func TestRecoveryDropSequence(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write CREATE SEQUENCE then DROP SEQUENCE to WAL
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	createSeqEntry := &CreateSequenceEntry{
		Schema:      "main",
		Name:        "temp_seq",
		StartWith:   1,
		IncrementBy: 1,
		MinValue:    math.MinInt64,
		MaxValue:    math.MaxInt64,
		IsCycle:     false,
	}
	err = writer.WriteEntry(createSeqEntry)
	require.NoError(t, err)

	dropSeqEntry := &DropSequenceEntry{
		Schema: "main",
		Name:   "temp_seq",
	}
	err = writer.WriteEntry(dropSeqEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify sequence was dropped
	_, ok := cat.GetSequence("temp_seq")
	assert.False(t, ok, "Sequence should not exist after DROP")
}

// TestRecoveryAlterTableRename tests that ALTER TABLE RENAME is properly recovered from WAL.
func TestRecoveryAlterTableRename(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write CREATE TABLE then ALTER TABLE to WAL
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	createTableEntry := &CreateTableEntry{
		Schema: "main",
		Name:   "old_name",
		Columns: []ColumnDef{
			{Name: "id", Type: 5, Nullable: false},
			{Name: "name", Type: 10, Nullable: true},
		},
	}
	err = writer.WriteEntry(createTableEntry)
	require.NoError(t, err)

	alterEntry := &AlterTableEntry{
		Schema:       "main",
		Table:        "old_name",
		Operation:    0, // AlterTableRenameTo
		NewTableName: "new_name",
	}
	err = writer.WriteEntry(alterEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify table was renamed
	_, ok := cat.GetTable("old_name")
	assert.False(t, ok, "Old table name should not exist")

	newTable, ok := cat.GetTable("new_name")
	require.True(t, ok)
	assert.Equal(t, "new_name", newTable.Name)
	assert.Len(t, newTable.Columns, 2)
}

// TestRecoveryAlterTableRenameColumn tests that ALTER TABLE RENAME COLUMN is properly recovered from WAL.
func TestRecoveryAlterTableRenameColumn(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write CREATE TABLE then ALTER TABLE to WAL
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	createTableEntry := &CreateTableEntry{
		Schema: "main",
		Name:   "users",
		Columns: []ColumnDef{
			{Name: "id", Type: 5, Nullable: false},
			{Name: "old_column", Type: 10, Nullable: true},
		},
	}
	err = writer.WriteEntry(createTableEntry)
	require.NoError(t, err)

	alterEntry := &AlterTableEntry{
		Schema:    "main",
		Table:     "users",
		Operation: 1, // AlterTableRenameColumn
		OldColumn: "old_column",
		NewColumn: "new_column",
	}
	err = writer.WriteEntry(alterEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify column was renamed
	table, ok := cat.GetTable("users")
	require.True(t, ok)
	assert.Len(t, table.Columns, 2)
	assert.Equal(t, "id", table.Columns[0].Name)
	assert.Equal(t, "new_column", table.Columns[1].Name)
}

// TestRecoveryAlterTableDropColumn tests that ALTER TABLE DROP COLUMN is properly recovered from WAL.
func TestRecoveryAlterTableDropColumn(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write CREATE TABLE then ALTER TABLE to WAL
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	createTableEntry := &CreateTableEntry{
		Schema: "main",
		Name:   "users",
		Columns: []ColumnDef{
			{Name: "id", Type: 5, Nullable: false},
			{Name: "name", Type: 10, Nullable: true},
			{Name: "deprecated", Type: 10, Nullable: true},
		},
	}
	err = writer.WriteEntry(createTableEntry)
	require.NoError(t, err)

	alterEntry := &AlterTableEntry{
		Schema:    "main",
		Table:     "users",
		Operation: 2, // AlterTableDropColumn
		Column:    "deprecated",
	}
	err = writer.WriteEntry(alterEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify column was dropped
	table, ok := cat.GetTable("users")
	require.True(t, ok)
	assert.Len(t, table.Columns, 2, "Should have 2 columns after dropping deprecated")
	assert.Equal(t, "id", table.Columns[0].Name)
	assert.Equal(t, "name", table.Columns[1].Name)
}

// TestRecoveryMultipleDDLOperations tests recovery of multiple DDL operations.
func TestRecoveryMultipleDDLOperations(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write multiple DDL operations to WAL
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	// Create schema
	createSchemaEntry := &CreateSchemaEntry{Name: "test_schema"}
	err = writer.WriteEntry(createSchemaEntry)
	require.NoError(t, err)

	// Create table
	createTableEntry := &CreateTableEntry{
		Schema: "test_schema",
		Name:   "users",
		Columns: []ColumnDef{
			{Name: "id", Type: 5, Nullable: false},
			{Name: "name", Type: 10, Nullable: true},
		},
	}
	err = writer.WriteEntry(createTableEntry)
	require.NoError(t, err)

	// Create view
	createViewEntry := &CreateViewEntry{
		Schema: "test_schema",
		Name:   "active_users",
		Query:  "SELECT * FROM users WHERE active = true",
	}
	err = writer.WriteEntry(createViewEntry)
	require.NoError(t, err)

	// Create index
	createIndexEntry := &CreateIndexEntry{
		Schema:   "test_schema",
		Table:    "users",
		Name:     "idx_name",
		Columns:  []string{"name"},
		IsUnique: false,
	}
	err = writer.WriteEntry(createIndexEntry)
	require.NoError(t, err)

	// Create sequence
	createSeqEntry := &CreateSequenceEntry{
		Schema:      "test_schema",
		Name:        "user_id_seq",
		StartWith:   1,
		IncrementBy: 1,
		MinValue:    1,
		MaxValue:    1000000,
		IsCycle:     false,
	}
	err = writer.WriteEntry(createSeqEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify all DDL objects were created
	schema, ok := cat.GetSchema("test_schema")
	require.True(t, ok)
	assert.Equal(t, "test_schema", schema.Name())

	table, ok := cat.GetTableInSchema("test_schema", "users")
	require.True(t, ok)
	assert.Equal(t, "users", table.Name)

	view, ok := cat.GetViewInSchema("test_schema", "active_users")
	require.True(t, ok)
	assert.Equal(t, "active_users", view.Name)

	index, ok := cat.GetIndexInSchema("test_schema", "idx_name")
	require.True(t, ok)
	assert.Equal(t, "idx_name", index.Name)

	seq, ok := cat.GetSequenceInSchema("test_schema", "user_id_seq")
	require.True(t, ok)
	assert.Equal(t, "user_id_seq", seq.Name)
}

// TestDDLEntryTypes tests that all DDL entry types are properly recognized.
func TestDDLEntryTypes(t *testing.T) {
	tests := []struct {
		name  string
		entry Entry
	}{
		{
			"CreateView",
			&CreateViewEntry{
				Schema: "main",
				Name:   "test_view",
				Query:  "SELECT * FROM test",
			},
		},
		{
			"DropView",
			&DropViewEntry{
				Schema: "main",
				Name:   "test_view",
			},
		},
		{
			"CreateSequence",
			&CreateSequenceEntry{
				Schema:      "main",
				Name:        "test_seq",
				StartWith:   1,
				IncrementBy: 1,
				MinValue:    0,
				MaxValue:    1000,
				IsCycle:     false,
			},
		},
		{
			"DropSequence",
			&DropSequenceEntry{
				Schema: "main",
				Name:   "test_seq",
			},
		},
		{
			"AlterTable",
			&AlterTableEntry{
				Schema:       "main",
				Table:        "test",
				Operation:    0,
				NewTableName: "new_test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotEmpty(t, tt.entry.Type().String())
		})
	}
}
