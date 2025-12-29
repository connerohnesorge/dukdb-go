package wal

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/quartz"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileHeader(t *testing.T) {
	header := &FileHeader{
		Magic:     MagicBytes,
		Version:   CurrentVersion,
		Iteration: 42,
	}

	var buf bytes.Buffer
	err := header.Serialize(&buf)
	require.NoError(t, err)

	// Deserialize
	readHeader := &FileHeader{}
	err = readHeader.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(t, header.Magic, readHeader.Magic)
	assert.Equal(t, header.Version, readHeader.Version)
	assert.Equal(t, header.Iteration, readHeader.Iteration)
}

func TestCreateTableEntry(t *testing.T) {
	entry := &CreateTableEntry{
		Schema: "main",
		Name:   "test_table",
		Columns: []ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
			{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true},
		},
	}

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	// Deserialize
	readEntry := &CreateTableEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(t, entry.Schema, readEntry.Schema)
	assert.Equal(t, entry.Name, readEntry.Name)
	assert.Equal(t, len(entry.Columns), len(readEntry.Columns))
	for i, col := range entry.Columns {
		assert.Equal(t, col.Name, readEntry.Columns[i].Name)
		assert.Equal(t, col.Type, readEntry.Columns[i].Type)
		assert.Equal(t, col.Nullable, readEntry.Columns[i].Nullable)
	}
}

func TestDropTableEntry(t *testing.T) {
	entry := &DropTableEntry{
		Schema: "main",
		Name:   "test_table",
	}

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	readEntry := &DropTableEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(t, entry.Schema, readEntry.Schema)
	assert.Equal(t, entry.Name, readEntry.Name)
}

func TestInsertEntry(t *testing.T) {
	entry := NewInsertEntry(1, "main", "test_table", [][]any{
		{int32(1), "Alice"},
		{int32(2), "Bob"},
	})

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	readEntry := &InsertEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(t, entry.TxnID(), readEntry.TxnID())
	assert.Equal(t, entry.Schema, readEntry.Schema)
	assert.Equal(t, entry.Table, readEntry.Table)
	assert.Equal(t, len(entry.Values), len(readEntry.Values))
}

func TestDeleteEntry(t *testing.T) {
	entry := NewDeleteEntry(1, "main", "test_table", []uint64{1, 2, 3})

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	readEntry := &DeleteEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(t, entry.TxnID(), readEntry.TxnID())
	assert.Equal(t, entry.Schema, readEntry.Schema)
	assert.Equal(t, entry.Table, readEntry.Table)
	assert.Equal(t, entry.RowIDs, readEntry.RowIDs)
}

func TestTxnEntries(t *testing.T) {
	now := time.Now()

	beginEntry := NewTxnBeginEntry(1, now)
	var buf bytes.Buffer
	err := beginEntry.Serialize(&buf)
	require.NoError(t, err)

	readBegin := &TxnBeginEntry{}
	err = readBegin.Deserialize(&buf)
	require.NoError(t, err)
	assert.Equal(t, beginEntry.TxnID(), readBegin.TxnID())

	commitEntry := NewTxnCommitEntry(1, now)
	buf.Reset()
	err = commitEntry.Serialize(&buf)
	require.NoError(t, err)

	readCommit := &TxnCommitEntry{}
	err = readCommit.Deserialize(&buf)
	require.NoError(t, err)
	assert.Equal(t, commitEntry.TxnID(), readCommit.TxnID())
}

func TestCheckpointEntry(t *testing.T) {
	now := time.Now()
	entry := NewCheckpointEntry(42, now)

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	readEntry := &CheckpointEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(t, entry.Iteration, readEntry.Iteration)
}

func TestWriterReader(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Create writer
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	// Write entries
	createEntry := &CreateTableEntry{
		Schema: "main",
		Name:   "users",
		Columns: []ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
			{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true},
		},
	}
	err = writer.WriteEntry(createEntry)
	require.NoError(t, err)

	insertEntry := NewInsertEntry(1, "main", "users", [][]any{
		{int32(1), "Alice"},
	})
	err = writer.WriteEntry(insertEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Create reader
	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Read entries
	entries, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	// Verify first entry
	readCreate, ok := entries[0].(*CreateTableEntry)
	require.True(t, ok)
	assert.Equal(t, createEntry.Name, readCreate.Name)

	// Verify second entry
	readInsert, ok := entries[1].(*InsertEntry)
	require.True(t, ok)
	assert.Equal(t, insertEntry.Table, readInsert.Table)
}

func TestRecovery(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write data
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	// Create table
	createEntry := &CreateTableEntry{
		Schema: "main",
		Name:   "users",
		Columns: []ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
			{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true},
		},
	}
	err = writer.WriteEntry(createEntry)
	require.NoError(t, err)

	// Begin transaction
	beginEntry := NewTxnBeginEntry(1, clock.Now())
	err = writer.WriteEntry(beginEntry)
	require.NoError(t, err)

	// Insert data
	insertEntry := NewInsertEntry(1, "main", "users", [][]any{
		{int32(1), "Alice"},
		{int32(2), "Bob"},
	})
	err = writer.WriteEntry(insertEntry)
	require.NoError(t, err)

	// Commit transaction
	commitEntry := NewTxnCommitEntry(1, clock.Now())
	err = writer.WriteEntry(commitEntry)
	require.NoError(t, err)

	// Write uncommitted transaction (should not be recovered)
	beginEntry2 := NewTxnBeginEntry(2, clock.Now())
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	insertEntry2 := NewInsertEntry(2, "main", "users", [][]any{
		{int32(3), "Charlie"},
	})
	err = writer.WriteEntry(insertEntry2)
	require.NoError(t, err)
	// No commit for transaction 2

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

	// Verify table was created
	tableDef, ok := cat.GetTable("users")
	require.True(t, ok)
	assert.Equal(t, "users", tableDef.Name)
	assert.Len(t, tableDef.Columns, 2)

	// Verify only committed data was recovered
	table, ok := store.GetTable("users")
	require.True(t, ok)
	assert.Equal(t, int64(2), table.RowCount()) // Only Alice and Bob, not Charlie
}

func TestTornWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Write valid entries
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	entry := &CreateTableEntry{
		Schema: "main",
		Name:   "users",
		Columns: []ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
		},
	}
	err = writer.WriteEntry(entry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Append partial entry header to simulate torn write
	// Write a complete size field (8 bytes) but partial checksum (4 bytes)
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	// Write a valid size
	_, err = f.Write([]byte{0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // size = 16
	require.NoError(t, err)
	// Write partial checksum (torn)
	_, err = f.Write([]byte{0x00, 0x01, 0x02, 0x03})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Recovery should succeed, ignoring the torn write
	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	entries, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, entries, 1) // Only the valid entry
}

func TestChecksumValidation(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Write valid entry
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	entry := &CreateTableEntry{
		Schema: "main",
		Name:   "users",
		Columns: []ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
		},
	}
	err = writer.WriteEntry(entry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Corrupt the data
	data, err := os.ReadFile(walPath)
	require.NoError(t, err)

	// Flip a bit in the entry data (after header + entry header)
	if len(data) > HeaderSize+EntryHeaderSize+5 {
		data[HeaderSize+EntryHeaderSize+5] ^= 0xFF
	}
	err = os.WriteFile(walPath, data, 0644)
	require.NoError(t, err)

	// Reading should fail with checksum error
	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	_, err = reader.ReadEntry()
	assert.ErrorIs(t, err, ErrChecksumMismatch)
}

func TestWriterReset(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Create writer and write entry
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	entry := &CreateTableEntry{
		Schema: "main",
		Name:   "users",
		Columns: []ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
		},
	}
	err = writer.WriteEntry(entry)
	require.NoError(t, err)

	oldIteration := writer.Iteration()

	// Reset the writer
	err = writer.Reset()
	require.NoError(t, err)

	// Iteration should be incremented
	assert.Equal(t, oldIteration+1, writer.Iteration())

	// Bytes written should be reset to header size
	assert.Equal(t, uint64(HeaderSize), writer.BytesWritten())

	err = writer.Close()
	require.NoError(t, err)

	// Verify file only contains header
	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	_, err = reader.ReadEntry()
	assert.Equal(t, io.EOF, err)
}

func TestCheckpointManager(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Set up catalog and storage
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	// Create table in catalog
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER).WithNullable(false),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR).WithNullable(true),
	}
	tableDef := catalog.NewTableDef("users", columns)
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	// Create table in storage
	storeTable, err := store.CreateTable("users", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})
	require.NoError(t, err)

	// Insert some data
	err = storeTable.AppendRow([]any{int32(1), "Alice"})
	require.NoError(t, err)
	err = storeTable.AppendRow([]any{int32(2), "Bob"})
	require.NoError(t, err)

	// Create WAL writer and checkpoint manager
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	cm := NewCheckpointManager(writer, cat, store, clock)

	// Perform checkpoint
	err = cm.Checkpoint()
	require.NoError(t, err)

	// Close writer
	err = cm.WAL().Close()
	require.NoError(t, err)

	// Verify checkpoint can be read back
	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	entries, err := reader.ReadAll()
	require.NoError(t, err)

	// Should have: CreateTable, TxnBegin, Insert, TxnCommit, Checkpoint
	assert.GreaterOrEqual(t, len(entries), 4)
}

func TestAutoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	cm := NewCheckpointManager(writer, cat, store, clock)

	// Set a low threshold - but high enough to contain checkpoint data
	cm.SetThreshold(500)

	// Write enough data to trigger auto-checkpoint
	for i := 0; i < 20; i++ {
		entry := &CreateTableEntry{
			Schema: "main",
			Name:   "table_" + string(rune('a'+i)),
			Columns: []ColumnDef{
				{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
			},
		}
		err = cm.WAL().WriteEntry(entry)
		require.NoError(t, err)
	}

	bytesBeforeCheckpoint := cm.WAL().BytesWritten()
	require.GreaterOrEqual(t, bytesBeforeCheckpoint, uint64(500))

	// Auto-checkpoint should trigger
	err = cm.MaybeAutoCheckpoint()
	require.NoError(t, err)

	// Bytes written should be less than before checkpoint (reset to just header + checkpoint marker)
	assert.Less(t, cm.WAL().BytesWritten(), bytesBeforeCheckpoint)

	err = cm.WAL().Close()
	require.NoError(t, err)
}

func TestEntryTypes(t *testing.T) {
	tests := []struct {
		name  string
		entry Entry
	}{
		{"CreateTable", &CreateTableEntry{Schema: "main", Name: "test", Columns: []ColumnDef{{Name: "id", Type: dukdb.TYPE_INTEGER}}}},
		{"DropTable", &DropTableEntry{Schema: "main", Name: "test"}},
		{"CreateSchema", &CreateSchemaEntry{Name: "test_schema"}},
		{"DropSchema", &DropSchemaEntry{Name: "test_schema"}},
		{"CreateView", &CreateViewEntry{Schema: "main", Name: "test_view", Query: "SELECT * FROM test"}},
		{"DropView", &DropViewEntry{Schema: "main", Name: "test_view"}},
		{"CreateIndex", &CreateIndexEntry{Schema: "main", Table: "test", Name: "idx", Columns: []string{"id"}, IsUnique: true}},
		{"DropIndex", &DropIndexEntry{Schema: "main", Table: "test", Name: "idx"}},
		{"UseTable", &UseTableEntry{Schema: "main", Table: "test"}},
		{"TxnBegin", NewTxnBeginEntry(1, time.Now())},
		{"TxnCommit", NewTxnCommitEntry(1, time.Now())},
		{"Checkpoint", NewCheckpointEntry(1, time.Now())},
		{"Flush", NewFlushEntry(time.Now())},
		{"Version", NewVersionEntry(2, 1, time.Now())},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotEmpty(t, tt.entry.Type().String())
		})
	}
}
