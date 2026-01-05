package duckdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDuckDBWriter(t *testing.T) {
	// Create a temporary directory
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	// Create a new writer
	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Verify file exists
	_, err = os.Stat(dbPath)
	require.NoError(t, err)

	// Check initial state
	assert.Equal(t, dbPath, writer.Path())
	assert.False(t, writer.IsModified())
	assert.False(t, writer.IsClosed())
	assert.Equal(t, 0, writer.TableCount())
	assert.Equal(t, 1, writer.HeaderSlot())
	assert.Equal(t, uint64(1), writer.Iteration())

	// Close the writer
	err = writer.Close()
	require.NoError(t, err)
	assert.True(t, writer.IsClosed())
}

func TestNewDuckDBWriter_FileExists(t *testing.T) {
	// Create a temporary directory
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	// Create the file first
	f, err := os.Create(dbPath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Try to create a new writer - should fail
	_, err = NewDuckDBWriter(dbPath)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrFileExists)
}

func TestOpenDuckDBWriter(t *testing.T) {
	// Create a temporary directory
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	// Create a new file first
	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Open the existing file
	writer, err = OpenDuckDBWriter(dbPath)
	require.NoError(t, err)
	require.NotNil(t, writer)

	assert.Equal(t, dbPath, writer.Path())
	assert.False(t, writer.IsClosed())

	err = writer.Close()
	require.NoError(t, err)
}

func TestOpenDuckDBWriter_FileNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "nonexistent.duckdb")

	_, err := OpenDuckDBWriter(dbPath)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrFileNotFound)
}

func TestDuckDBWriter_CreateTable(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Create a table
	table := NewTableCatalogEntry("users")
	table.AddColumn(*NewColumnDefinition("id", TypeInteger))
	table.AddColumn(*NewColumnDefinition("name", TypeVarchar))

	err = writer.CreateTable(table)
	require.NoError(t, err)

	// Verify table was added
	assert.Equal(t, 1, writer.TableCount())
	assert.True(t, writer.IsModified())

	// Get table by name
	retrieved := writer.GetTable("users")
	require.NotNil(t, retrieved)
	assert.Equal(t, "users", retrieved.Name)
	assert.Len(t, retrieved.Columns, 2)

	// Get table OID
	oid := writer.GetTableOID("", "users")
	assert.Equal(t, 0, oid)
}

func TestDuckDBWriter_CreateTable_Duplicate(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Create first table
	table1 := NewTableCatalogEntry("users")
	err = writer.CreateTable(table1)
	require.NoError(t, err)

	// Try to create duplicate
	table2 := NewTableCatalogEntry("users")
	err = writer.CreateTable(table2)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTableExists)
}

func TestDuckDBWriter_CreateTableSimple(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	columns := []ColumnDefinition{
		*NewColumnDefinition("id", TypeInteger),
		*NewColumnDefinition("name", TypeVarchar),
		*NewColumnDefinition("age", TypeSmallInt),
	}

	err = writer.CreateTableSimple("employees", columns)
	require.NoError(t, err)

	assert.Equal(t, 1, writer.TableCount())
	table := writer.GetTable("employees")
	require.NotNil(t, table)
	assert.Len(t, table.Columns, 3)
}

func TestDuckDBWriter_DropTable(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Create a table
	table := NewTableCatalogEntry("users")
	err = writer.CreateTable(table)
	require.NoError(t, err)
	assert.Equal(t, 1, writer.TableCount())

	// Drop the table
	err = writer.DropTable("", "users")
	require.NoError(t, err)
	assert.Equal(t, 0, writer.TableCount())

	// Verify table is gone
	assert.Nil(t, writer.GetTable("users"))

	// Drop non-existent table
	err = writer.DropTable("", "nonexistent")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTableNotFound)
}

func TestDuckDBWriter_CreateView(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	view := NewViewCatalogEntry("active_users", "SELECT * FROM users WHERE active = true")
	err = writer.CreateView(view)
	require.NoError(t, err)

	assert.True(t, writer.IsModified())
	catalog := writer.Catalog()
	assert.Len(t, catalog.Views, 1)
	assert.Equal(t, "active_users", catalog.Views[0].Name)
}

func TestDuckDBWriter_CreateIndex(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	index := NewIndexCatalogEntry("idx_users_email", "users")
	index.ColumnIDs = []uint64{1}
	err = writer.CreateIndex(index)
	require.NoError(t, err)

	assert.True(t, writer.IsModified())
	catalog := writer.Catalog()
	assert.Len(t, catalog.Indexes, 1)
}

func TestDuckDBWriter_CreateSequence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	seq := NewSequenceCatalogEntry("user_id_seq")
	err = writer.CreateSequence(seq)
	require.NoError(t, err)

	assert.True(t, writer.IsModified())
	catalog := writer.Catalog()
	assert.Len(t, catalog.Sequences, 1)
}

func TestDuckDBWriter_CreateSchema(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	schema := NewSchemaCatalogEntry("analytics")
	err = writer.CreateSchema(schema)
	require.NoError(t, err)

	assert.True(t, writer.IsModified())
	catalog := writer.Catalog()
	assert.Len(t, catalog.Schemas, 1)
}

func TestDuckDBWriter_InsertRows(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Create a table
	columns := []ColumnDefinition{
		*NewColumnDefinition("id", TypeInteger),
		*NewColumnDefinition("name", TypeVarchar),
	}
	err = writer.CreateTableSimple("users", columns)
	require.NoError(t, err)

	// Insert rows
	rows := [][]any{
		{int32(1), "Alice"},
		{int32(2), "Bob"},
		{int32(3), "Charlie"},
	}

	err = writer.InsertRows(0, rows)
	require.NoError(t, err)

	// Verify row count
	assert.Equal(t, uint64(3), writer.TotalRowCount(0))
	assert.True(t, writer.IsModified())
}

func TestDuckDBWriter_InsertRows_InvalidTableOID(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	rows := [][]any{{int32(1), "test"}}
	err = writer.InsertRows(999, rows)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTableNotFound)
}

func TestDuckDBWriter_Checkpoint(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)

	// Create a table and insert data
	columns := []ColumnDefinition{
		*NewColumnDefinition("id", TypeInteger),
		*NewColumnDefinition("value", TypeDouble),
	}
	err = writer.CreateTableSimple("data", columns)
	require.NoError(t, err)

	rows := [][]any{
		{int32(1), float64(1.5)},
		{int32(2), float64(2.5)},
	}
	err = writer.InsertRows(0, rows)
	require.NoError(t, err)

	// Checkpoint
	err = writer.Checkpoint()
	require.NoError(t, err)
	assert.False(t, writer.IsModified())
	assert.Equal(t, 2, writer.HeaderSlot()) // Should have switched slots
	assert.Equal(t, uint64(2), writer.Iteration())

	// Close
	err = writer.Close()
	require.NoError(t, err)

	// Reopen and verify headers
	file, err := os.Open(dbPath)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	fileHeader, err := ReadFileHeader(file)
	require.NoError(t, err)
	assert.NoError(t, ValidateFileHeader(fileHeader))

	dbHeader, slot, err := GetActiveHeader(file)
	require.NoError(t, err)
	assert.Equal(t, 2, slot)
	assert.Equal(t, uint64(2), dbHeader.Iteration)
}

func TestDuckDBWriter_CheckpointNoChanges(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Checkpoint with no changes - should be a no-op
	err = writer.Checkpoint()
	require.NoError(t, err)
	assert.Equal(t, 1, writer.HeaderSlot()) // Should not have switched
	assert.Equal(t, uint64(1), writer.Iteration())
}

func TestDuckDBWriter_ForceCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Force checkpoint with no changes
	err = writer.ForceCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, 2, writer.HeaderSlot()) // Should have switched
	assert.Equal(t, uint64(2), writer.Iteration())
}

func TestDuckDBWriter_CloseWithData(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)

	// Create table and insert data
	columns := []ColumnDefinition{
		*NewColumnDefinition("id", TypeInteger),
	}
	err = writer.CreateTableSimple("test", columns)
	require.NoError(t, err)

	rows := [][]any{{int32(1)}, {int32(2)}, {int32(3)}}
	err = writer.InsertRows(0, rows)
	require.NoError(t, err)

	// Close should checkpoint automatically
	err = writer.Close()
	require.NoError(t, err)

	// Verify file is valid
	file, err := os.Open(dbPath)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	_, _, err = GetActiveHeader(file)
	require.NoError(t, err)
}

func TestDuckDBWriter_CloseMultipleTimes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)

	// Close multiple times should be safe
	err = writer.Close()
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err) // Should not error
}

func TestDuckDBWriter_OperationsAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Operations on closed writer should fail
	err = writer.CreateTable(NewTableCatalogEntry("test"))
	assert.ErrorIs(t, err, ErrDuckDBWriterClosed)

	err = writer.InsertRows(0, [][]any{})
	assert.ErrorIs(t, err, ErrDuckDBWriterClosed)

	err = writer.Checkpoint()
	assert.ErrorIs(t, err, ErrDuckDBWriterClosed)
}

func TestDuckDBWriter_Flush(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Create table and insert data
	columns := []ColumnDefinition{
		*NewColumnDefinition("id", TypeInteger),
	}
	err = writer.CreateTableSimple("test", columns)
	require.NoError(t, err)

	rows := [][]any{{int32(1)}, {int32(2)}}
	err = writer.InsertRows(0, rows)
	require.NoError(t, err)

	// Flush should work
	err = writer.Flush()
	require.NoError(t, err)

	// Data should still be counted
	assert.Equal(t, uint64(2), writer.TotalRowCount(0))
}

func TestDuckDBWriter_MultipleCheckpoints(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// First batch
	columns := []ColumnDefinition{
		*NewColumnDefinition("id", TypeInteger),
	}
	err = writer.CreateTableSimple("test", columns)
	require.NoError(t, err)

	rows1 := [][]any{{int32(1)}, {int32(2)}}
	err = writer.InsertRows(0, rows1)
	require.NoError(t, err)

	err = writer.Checkpoint()
	require.NoError(t, err)
	assert.Equal(t, 2, writer.HeaderSlot())
	assert.Equal(t, uint64(2), writer.Iteration())

	// Second batch
	rows2 := [][]any{{int32(3)}, {int32(4)}}
	err = writer.InsertRows(0, rows2)
	require.NoError(t, err)

	err = writer.Checkpoint()
	require.NoError(t, err)
	assert.Equal(t, 1, writer.HeaderSlot()) // Back to slot 1
	assert.Equal(t, uint64(3), writer.Iteration())

	// Total rows
	assert.Equal(t, uint64(4), writer.TotalRowCount(0))
}

func TestDuckDBWriter_RowGroupCount(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	columns := []ColumnDefinition{
		*NewColumnDefinition("id", TypeInteger),
	}
	err = writer.CreateTableSimple("test", columns)
	require.NoError(t, err)

	// Initially no row groups
	assert.Equal(t, 0, writer.RowGroupCount(0))

	// Insert some data
	rows := [][]any{{int32(1)}}
	err = writer.InsertRows(0, rows)
	require.NoError(t, err)

	// Flush to create row group
	err = writer.Flush()
	require.NoError(t, err)

	// Should have one row group
	assert.Equal(t, 1, writer.RowGroupCount(0))
}

func TestDuckDBWriter_BlockCount(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Initially no blocks
	assert.Equal(t, uint64(0), writer.BlockCount())

	// Create table and insert data
	columns := []ColumnDefinition{
		*NewColumnDefinition("id", TypeInteger),
	}
	err = writer.CreateTableSimple("test", columns)
	require.NoError(t, err)

	rows := [][]any{{int32(1)}}
	err = writer.InsertRows(0, rows)
	require.NoError(t, err)

	// Checkpoint to write blocks
	err = writer.Checkpoint()
	require.NoError(t, err)

	// Should have some blocks now
	assert.Greater(t, writer.BlockCount(), uint64(0))
}

func TestCreate(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := Create(dbPath)
	require.NoError(t, err)
	require.NotNil(t, writer)
	defer func() { _ = writer.Close() }()

	assert.Equal(t, dbPath, writer.Path())
}

func TestOpen(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	// Create first
	writer, err := Create(dbPath)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Open
	writer, err = Open(dbPath)
	require.NoError(t, err)
	require.NotNil(t, writer)
	defer func() { _ = writer.Close() }()

	assert.Equal(t, dbPath, writer.Path())
}

func TestDuckDBWriter_DropOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Create objects
	err = writer.CreateView(NewViewCatalogEntry("test_view", "SELECT 1"))
	require.NoError(t, err)

	err = writer.CreateIndex(NewIndexCatalogEntry("test_idx", "users"))
	require.NoError(t, err)

	err = writer.CreateSequence(NewSequenceCatalogEntry("test_seq"))
	require.NoError(t, err)

	// Drop operations
	err = writer.DropView("", "test_view")
	require.NoError(t, err)
	assert.Len(t, writer.Catalog().Views, 0)

	err = writer.DropIndex("", "test_idx")
	require.NoError(t, err)
	assert.Len(t, writer.Catalog().Indexes, 0)

	err = writer.DropSequence("", "test_seq")
	require.NoError(t, err)
	assert.Len(t, writer.Catalog().Sequences, 0)

	// Drop non-existent objects
	err = writer.DropView("", "nonexistent")
	assert.Error(t, err)

	err = writer.DropIndex("", "nonexistent")
	assert.Error(t, err)

	err = writer.DropSequence("", "nonexistent")
	assert.Error(t, err)
}

func TestDuckDBWriter_DuplicateCatalogEntries(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Create view
	err = writer.CreateView(NewViewCatalogEntry("v1", "SELECT 1"))
	require.NoError(t, err)

	// Duplicate view
	err = writer.CreateView(NewViewCatalogEntry("v1", "SELECT 2"))
	assert.ErrorIs(t, err, ErrViewExists)

	// Create index
	err = writer.CreateIndex(NewIndexCatalogEntry("idx1", "t1"))
	require.NoError(t, err)

	// Duplicate index
	err = writer.CreateIndex(NewIndexCatalogEntry("idx1", "t2"))
	assert.ErrorIs(t, err, ErrIndexExists)

	// Create sequence
	err = writer.CreateSequence(NewSequenceCatalogEntry("seq1"))
	require.NoError(t, err)

	// Duplicate sequence
	err = writer.CreateSequence(NewSequenceCatalogEntry("seq1"))
	assert.ErrorIs(t, err, ErrSequenceExists)

	// Create schema
	err = writer.CreateSchema(NewSchemaCatalogEntry("s1"))
	require.NoError(t, err)

	// Duplicate schema
	err = writer.CreateSchema(NewSchemaCatalogEntry("s1"))
	assert.Error(t, err)
}

func TestDuckDBWriter_MultipleTables(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	writer, err := NewDuckDBWriter(dbPath)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Create multiple tables
	columns1 := []ColumnDefinition{*NewColumnDefinition("id", TypeInteger)}
	err = writer.CreateTableSimple("table1", columns1)
	require.NoError(t, err)

	columns2 := []ColumnDefinition{*NewColumnDefinition("name", TypeVarchar)}
	err = writer.CreateTableSimple("table2", columns2)
	require.NoError(t, err)

	columns3 := []ColumnDefinition{*NewColumnDefinition("value", TypeDouble)}
	err = writer.CreateTableSimple("table3", columns3)
	require.NoError(t, err)

	assert.Equal(t, 3, writer.TableCount())

	// Insert data into each table
	err = writer.InsertRows(0, [][]any{{int32(1)}})
	require.NoError(t, err)

	err = writer.InsertRows(1, [][]any{{"test"}})
	require.NoError(t, err)

	err = writer.InsertRows(2, [][]any{{float64(3.14)}})
	require.NoError(t, err)

	// Checkpoint
	err = writer.Checkpoint()
	require.NoError(t, err)

	// Verify row counts
	assert.Equal(t, uint64(1), writer.TotalRowCount(0))
	assert.Equal(t, uint64(1), writer.TotalRowCount(1))
	assert.Equal(t, uint64(1), writer.TotalRowCount(2))
}
