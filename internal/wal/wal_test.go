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
		Magic:          MagicBytes,
		Version:        CurrentVersion,
		HeaderSize:     HeaderSize,
		SequenceNumber: 42,
	}

	var buf bytes.Buffer
	err := header.Serialize(&buf)
	require.NoError(t, err)

	// Verify header size
	assert.Equal(t, HeaderSize, buf.Len(), "Header should be exactly 24 bytes")

	// Deserialize
	readHeader := &FileHeader{}
	err = readHeader.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(
		t,
		header.Magic,
		readHeader.Magic,
	)
	assert.Equal(
		t,
		header.Version,
		readHeader.Version,
	)
	assert.Equal(
		t,
		header.HeaderSize,
		readHeader.HeaderSize,
	)
	assert.Equal(
		t,
		header.SequenceNumber,
		readHeader.SequenceNumber,
	)
	// Checksum should be calculated and match
	assert.NotZero(t, readHeader.Checksum, "Checksum should be non-zero")
}

func TestFileHeaderInvalidMagic(t *testing.T) {
	header := &FileHeader{
		Magic:          [4]byte{'B', 'A', 'D', '!'},
		Version:        CurrentVersion,
		HeaderSize:     HeaderSize,
		SequenceNumber: 1,
	}

	var buf bytes.Buffer
	err := header.Serialize(&buf)
	require.NoError(t, err)

	// Try to deserialize with wrong magic
	readHeader := &FileHeader{}
	err = readHeader.Deserialize(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid WAL magic")
}

func TestFileHeaderInvalidVersion(t *testing.T) {
	header := &FileHeader{
		Magic:          MagicBytes,
		Version:        999, // Unsupported version
		HeaderSize:     HeaderSize,
		SequenceNumber: 1,
	}

	var buf bytes.Buffer
	err := header.Serialize(&buf)
	require.NoError(t, err)

	// Try to deserialize with unsupported version
	readHeader := &FileHeader{}
	err = readHeader.Deserialize(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported WAL version")
}

func TestFileHeaderInvalidSize(t *testing.T) {
	header := &FileHeader{
		Magic:          MagicBytes,
		Version:        CurrentVersion,
		HeaderSize:     100, // Wrong size
		SequenceNumber: 1,
	}

	var buf bytes.Buffer
	err := header.Serialize(&buf)
	require.NoError(t, err)

	// Try to deserialize with wrong header size
	readHeader := &FileHeader{}
	err = readHeader.Deserialize(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid WAL header size")
}

func TestFileHeaderChecksumVerification(t *testing.T) {
	header := &FileHeader{
		Magic:          MagicBytes,
		Version:        CurrentVersion,
		HeaderSize:     HeaderSize,
		SequenceNumber: 123,
	}

	var buf bytes.Buffer
	err := header.Serialize(&buf)
	require.NoError(t, err)

	// Corrupt the checksum by modifying the last 4 bytes
	data := buf.Bytes()
	data[len(data)-1] ^= 0xFF // Flip bits in last byte of checksum

	// Try to deserialize with corrupted checksum
	readHeader := &FileHeader{}
	err = readHeader.Deserialize(bytes.NewReader(data))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checksum mismatch")
}

func TestFileHeaderDuckDBFormat(t *testing.T) {
	// Test that header matches DuckDB format specification
	header := &FileHeader{
		Magic:          MagicBytes,
		Version:        CurrentVersion,
		HeaderSize:     HeaderSize,
		SequenceNumber: 0,
	}

	var buf bytes.Buffer
	err := header.Serialize(&buf)
	require.NoError(t, err)

	data := buf.Bytes()

	// Verify magic number "WAL " (0x57414C20)
	assert.Equal(t, byte('W'), data[0])
	assert.Equal(t, byte('A'), data[1])
	assert.Equal(t, byte('L'), data[2])
	assert.Equal(t, byte(' '), data[3])

	// Verify version = 3 (4 bytes, little-endian)
	assert.Equal(t, byte(3), data[4])
	assert.Equal(t, byte(0), data[5])
	assert.Equal(t, byte(0), data[6])
	assert.Equal(t, byte(0), data[7])

	// Verify header size = 24 (4 bytes, little-endian)
	assert.Equal(t, byte(24), data[8])
	assert.Equal(t, byte(0), data[9])
	assert.Equal(t, byte(0), data[10])
	assert.Equal(t, byte(0), data[11])

	// Verify sequence number = 0 (8 bytes, little-endian)
	for i := 12; i < 20; i++ {
		assert.Equal(t, byte(0), data[i])
	}

	// Checksum is last 4 bytes (20-23)
	// We just verify it's present, not the actual value
	assert.Equal(t, 24, len(data))
}

func TestCreateTableEntry(t *testing.T) {
	entry := &CreateTableEntry{
		Schema: "main",
		Name:   "test_table",
		Columns: []ColumnDef{
			{
				Name:     "id",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
			{
				Name:     "name",
				Type:     dukdb.TYPE_VARCHAR,
				Nullable: true,
			},
		},
	}

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	// Deserialize
	readEntry := &CreateTableEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(
		t,
		entry.Schema,
		readEntry.Schema,
	)
	assert.Equal(t, entry.Name, readEntry.Name)
	assert.Equal(
		t,
		len(entry.Columns),
		len(readEntry.Columns),
	)
	for i, col := range entry.Columns {
		assert.Equal(
			t,
			col.Name,
			readEntry.Columns[i].Name,
		)
		assert.Equal(
			t,
			col.Type,
			readEntry.Columns[i].Type,
		)
		assert.Equal(
			t,
			col.Nullable,
			readEntry.Columns[i].Nullable,
		)
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

	assert.Equal(
		t,
		entry.Schema,
		readEntry.Schema,
	)
	assert.Equal(t, entry.Name, readEntry.Name)
}

func TestInsertEntry(t *testing.T) {
	entry := NewInsertEntry(
		1,
		"main",
		"test_table",
		[][]any{
			{int32(1), "Alice"},
			{int32(2), "Bob"},
		},
	)

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	readEntry := &InsertEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(
		t,
		entry.TxnID(),
		readEntry.TxnID(),
	)
	assert.Equal(
		t,
		entry.Schema,
		readEntry.Schema,
	)
	assert.Equal(t, entry.Table, readEntry.Table)
	assert.Equal(
		t,
		len(entry.Values),
		len(readEntry.Values),
	)
}

func TestDeleteEntry(t *testing.T) {
	entry := NewDeleteEntry(
		1,
		"main",
		"test_table",
		[]uint64{1, 2, 3},
	)

	var buf bytes.Buffer
	err := entry.Serialize(&buf)
	require.NoError(t, err)

	readEntry := &DeleteEntry{}
	err = readEntry.Deserialize(&buf)
	require.NoError(t, err)

	assert.Equal(
		t,
		entry.TxnID(),
		readEntry.TxnID(),
	)
	assert.Equal(
		t,
		entry.Schema,
		readEntry.Schema,
	)
	assert.Equal(t, entry.Table, readEntry.Table)
	assert.Equal(
		t,
		entry.RowIDs,
		readEntry.RowIDs,
	)
}

// TestInsertEntrySerializationRoundTrip tests that INSERT entry serialization preserves all data
// This covers task 3.7: Test INSERT entry serialization preserves all data
func TestInsertEntrySerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values [][]any
	}{
		{
			name: "basic_values",
			values: [][]any{
				{int32(1), "Alice", true},
				{int32(2), "Bob", false},
			},
		},
		{
			name: "with_null_values",
			values: [][]any{
				{int32(1), nil, true},
				{nil, "Bob", nil},
			},
		},
		{
			name: "large_batch",
			values: func() [][]any {
				rows := make([][]any, 100)
				for i := range 100 {
					rows[i] = []any{int32(i), "Name" + string(rune('A'+i%26)), i%2 == 0}
				}
				return rows
			}(),
		},
		{
			name: "mixed_types",
			values: [][]any{
				{int32(1), int64(100), float64(3.14), "hello", true},
				{int32(2), int64(200), float64(2.71), "world", false},
			},
		},
		{
			name:   "empty_values",
			values: [][]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := NewInsertEntry(
				42,
				"main",
				"test_table",
				tt.values,
			)

			var buf bytes.Buffer
			err := entry.Serialize(&buf)
			require.NoError(t, err)

			readEntry := &InsertEntry{}
			err = readEntry.Deserialize(&buf)
			require.NoError(t, err)

			// Verify all fields are preserved
			assert.Equal(t, entry.TxnID(), readEntry.TxnID())
			assert.Equal(t, entry.Schema, readEntry.Schema)
			assert.Equal(t, entry.Table, readEntry.Table)
			assert.Equal(t, len(entry.Values), len(readEntry.Values))

			// Deep compare values
			for i, row := range entry.Values {
				require.Len(t, readEntry.Values[i], len(row))
				for j, val := range row {
					assert.Equal(t, val, readEntry.Values[i][j],
						"Mismatch at row %d, col %d", i, j)
				}
			}
		})
	}
}

// TestDeleteEntrySerializationWithData tests DELETE entry serialization with deleted data
// This covers task 3.9: Test DELETE entry serialization preserves deleted data
func TestDeleteEntrySerializationWithData(t *testing.T) {
	tests := []struct {
		name        string
		rowIDs      []uint64
		deletedData [][]any
	}{
		{
			name:   "basic_delete_with_data",
			rowIDs: []uint64{1, 2, 3},
			deletedData: [][]any{
				{int32(1), "Alice"},
				{int32(2), "Bob"},
				{int32(3), "Charlie"},
			},
		},
		{
			name:   "delete_with_null_values",
			rowIDs: []uint64{10, 20},
			deletedData: [][]any{
				{int32(10), nil},
				{nil, "Test"},
			},
		},
		{
			name:   "large_delete_batch",
			rowIDs: func() []uint64 {
				ids := make([]uint64, 100)
				for i := range 100 {
					ids[i] = uint64(i * 10)
				}
				return ids
			}(),
			deletedData: func() [][]any {
				rows := make([][]any, 100)
				for i := range 100 {
					rows[i] = []any{int32(i), "Name" + string(rune('A'+i%26))}
				}
				return rows
			}(),
		},
		{
			name:        "delete_without_data",
			rowIDs:      []uint64{1, 2},
			deletedData: nil,
		},
		{
			name:        "empty_delete",
			rowIDs:      []uint64{},
			deletedData: [][]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := NewDeleteEntryWithData(
				99,
				"main",
				"test_table",
				tt.rowIDs,
				tt.deletedData,
			)

			var buf bytes.Buffer
			err := entry.Serialize(&buf)
			require.NoError(t, err)

			readEntry := &DeleteEntry{}
			err = readEntry.Deserialize(&buf)
			require.NoError(t, err)

			// Verify all fields are preserved
			assert.Equal(t, entry.TxnID(), readEntry.TxnID())
			assert.Equal(t, entry.Schema, readEntry.Schema)
			assert.Equal(t, entry.Table, readEntry.Table)
			assert.Equal(t, entry.RowIDs, readEntry.RowIDs)

			// Verify deleted data is preserved
			assert.Equal(t, len(entry.DeletedData), len(readEntry.DeletedData))
			for i, row := range entry.DeletedData {
				require.Len(t, readEntry.DeletedData[i], len(row))
				for j, val := range row {
					assert.Equal(t, val, readEntry.DeletedData[i][j],
						"Mismatch at row %d, col %d", i, j)
				}
			}
		})
	}
}

// TestUpdateEntrySerializationWithBeforeAfter tests UPDATE entry serialization with before/after values
// This covers task 3.8: Test UPDATE entry serialization preserves before/after values
func TestUpdateEntrySerializationWithBeforeAfter(t *testing.T) {
	tests := []struct {
		name         string
		rowIDs       []uint64
		columnIdxs   []int
		beforeValues [][]any
		afterValues  [][]any
	}{
		{
			name:       "basic_update_with_before_after",
			rowIDs:     []uint64{1, 2},
			columnIdxs: []int{1}, // updating column 1
			beforeValues: [][]any{
				{"Alice"},
				{"Bob"},
			},
			afterValues: [][]any{
				{"ALICE"},
				{"BOB"},
			},
		},
		{
			name:       "update_with_null_handling",
			rowIDs:     []uint64{10, 20},
			columnIdxs: []int{0, 1},
			beforeValues: [][]any{
				{nil, "Test"},
				{int32(20), nil},
			},
			afterValues: [][]any{
				{int32(100), "Test Updated"},
				{int32(200), "Was Null"},
			},
		},
		{
			name:       "large_update_batch",
			rowIDs:     func() []uint64 {
				ids := make([]uint64, 50)
				for i := range 50 {
					ids[i] = uint64(i)
				}
				return ids
			}(),
			columnIdxs: []int{0, 1, 2},
			beforeValues: func() [][]any {
				rows := make([][]any, 50)
				for i := range 50 {
					rows[i] = []any{int32(i), "Before" + string(rune('A'+i%26)), false}
				}
				return rows
			}(),
			afterValues: func() [][]any {
				rows := make([][]any, 50)
				for i := range 50 {
					rows[i] = []any{int32(i * 10), "After" + string(rune('A'+i%26)), true}
				}
				return rows
			}(),
		},
		{
			name:         "update_without_before_values",
			rowIDs:       []uint64{1},
			columnIdxs:   []int{0},
			beforeValues: nil,
			afterValues: [][]any{
				{int32(999)},
			},
		},
		{
			name:         "empty_update",
			rowIDs:       []uint64{},
			columnIdxs:   []int{},
			beforeValues: [][]any{},
			afterValues:  [][]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := NewUpdateEntryWithBeforeValues(
				77,
				"main",
				"test_table",
				tt.rowIDs,
				tt.columnIdxs,
				tt.beforeValues,
				tt.afterValues,
			)

			var buf bytes.Buffer
			err := entry.Serialize(&buf)
			require.NoError(t, err)

			readEntry := &UpdateEntry{}
			err = readEntry.Deserialize(&buf)
			require.NoError(t, err)

			// Verify all fields are preserved
			assert.Equal(t, entry.TxnID(), readEntry.TxnID())
			assert.Equal(t, entry.Schema, readEntry.Schema)
			assert.Equal(t, entry.Table, readEntry.Table)
			assert.Equal(t, entry.RowIDs, readEntry.RowIDs)
			assert.Equal(t, entry.ColumnIdxs, readEntry.ColumnIdxs)

			// Verify before values are preserved
			assert.Equal(t, len(entry.BeforeValues), len(readEntry.BeforeValues))
			for i, row := range entry.BeforeValues {
				require.Len(t, readEntry.BeforeValues[i], len(row))
				for j, val := range row {
					assert.Equal(t, val, readEntry.BeforeValues[i][j],
						"BeforeValues mismatch at row %d, col %d", i, j)
				}
			}

			// Verify after values are preserved
			assert.Equal(t, len(entry.NewValues), len(readEntry.NewValues))
			for i, row := range entry.NewValues {
				require.Len(t, readEntry.NewValues[i], len(row))
				for j, val := range row {
					assert.Equal(t, val, readEntry.NewValues[i][j],
						"NewValues mismatch at row %d, col %d", i, j)
				}
			}

			// Verify the getter methods work
			// Note: Empty slices and nil are semantically equivalent for this comparison
			assert.Len(t, readEntry.GetBeforeValues(), len(entry.GetBeforeValues()))
			assert.Len(t, readEntry.GetAfterValues(), len(entry.GetAfterValues()))
		})
	}
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
	assert.Equal(
		t,
		beginEntry.TxnID(),
		readBegin.TxnID(),
	)

	commitEntry := NewTxnCommitEntry(1, now)
	buf.Reset()
	err = commitEntry.Serialize(&buf)
	require.NoError(t, err)

	readCommit := &TxnCommitEntry{}
	err = readCommit.Deserialize(&buf)
	require.NoError(t, err)
	assert.Equal(
		t,
		commitEntry.TxnID(),
		readCommit.TxnID(),
	)
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

	assert.Equal(
		t,
		entry.Iteration,
		readEntry.Iteration,
	)
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
			{
				Name:     "id",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
			{
				Name:     "name",
				Type:     dukdb.TYPE_VARCHAR,
				Nullable: true,
			},
		},
	}
	err = writer.WriteEntry(createEntry)
	require.NoError(t, err)

	insertEntry := NewInsertEntry(
		1,
		"main",
		"users",
		[][]any{
			{int32(1), "Alice"},
		},
	)
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
	assert.Equal(
		t,
		createEntry.Name,
		readCreate.Name,
	)

	// Verify second entry
	readInsert, ok := entries[1].(*InsertEntry)
	require.True(t, ok)
	assert.Equal(
		t,
		insertEntry.Table,
		readInsert.Table,
	)
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
			{
				Name:     "id",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
			{
				Name:     "name",
				Type:     dukdb.TYPE_VARCHAR,
				Nullable: true,
			},
		},
	}
	err = writer.WriteEntry(createEntry)
	require.NoError(t, err)

	// Begin transaction
	beginEntry := NewTxnBeginEntry(1, clock.Now())
	err = writer.WriteEntry(beginEntry)
	require.NoError(t, err)

	// Insert data
	insertEntry := NewInsertEntry(
		1,
		"main",
		"users",
		[][]any{
			{int32(1), "Alice"},
			{int32(2), "Bob"},
		},
	)
	err = writer.WriteEntry(insertEntry)
	require.NoError(t, err)

	// Commit transaction
	commitEntry := NewTxnCommitEntry(
		1,
		clock.Now(),
	)
	err = writer.WriteEntry(commitEntry)
	require.NoError(t, err)

	// Write uncommitted transaction (should not be recovered)
	beginEntry2 := NewTxnBeginEntry(
		2,
		clock.Now(),
	)
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	insertEntry2 := NewInsertEntry(
		2,
		"main",
		"users",
		[][]any{
			{int32(3), "Charlie"},
		},
	)
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
	assert.Equal(
		t,
		int64(2),
		table.RowCount(),
	) // Only Alice and Bob, not Charlie
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
			{
				Name:     "id",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
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
	f, err := os.OpenFile(
		walPath,
		os.O_APPEND|os.O_WRONLY,
		0644,
	)
	require.NoError(t, err)
	// Write a valid size
	_, err = f.Write(
		[]byte{
			0x10,
			0x00,
			0x00,
			0x00,
			0x00,
			0x00,
			0x00,
			0x00,
		},
	) // size = 16
	require.NoError(t, err)
	// Write partial checksum (torn)
	_, err = f.Write(
		[]byte{0x00, 0x01, 0x02, 0x03},
	)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Recovery should succeed, ignoring the torn write
	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	entries, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(
		t,
		entries,
		1,
	) // Only the valid entry
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
			{
				Name:     "id",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
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
			{
				Name:     "id",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
		},
	}
	err = writer.WriteEntry(entry)
	require.NoError(t, err)

	oldIteration := writer.Iteration()

	// Reset the writer
	err = writer.Reset()
	require.NoError(t, err)

	// Iteration should be incremented
	assert.Equal(
		t,
		oldIteration+1,
		writer.Iteration(),
	)

	// Bytes written should be reset to header size
	assert.Equal(
		t,
		uint64(HeaderSize),
		writer.BytesWritten(),
	)

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
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER).
			WithNullable(false),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR).
			WithNullable(true),
	}
	tableDef := catalog.NewTableDef(
		"users",
		columns,
	)
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	// Create table in storage
	storeTable, err := store.CreateTable(
		"users",
		[]dukdb.Type{
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_VARCHAR,
		},
	)
	require.NoError(t, err)

	// Insert some data
	err = storeTable.AppendRow(
		[]any{int32(1), "Alice"},
	)
	require.NoError(t, err)
	err = storeTable.AppendRow(
		[]any{int32(2), "Bob"},
	)
	require.NoError(t, err)

	// Create WAL writer and checkpoint manager
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	cm := NewCheckpointManager(
		writer,
		cat,
		store,
		clock,
	)

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

	cm := NewCheckpointManager(
		writer,
		cat,
		store,
		clock,
	)

	// Set a low threshold - but high enough to contain checkpoint data
	cm.SetThreshold(500)

	// Write enough data to trigger auto-checkpoint
	for i := range 20 {
		entry := &CreateTableEntry{
			Schema: "main",
			Name: "table_" + string(
				rune('a'+i),
			),
			Columns: []ColumnDef{
				{
					Name:     "id",
					Type:     dukdb.TYPE_INTEGER,
					Nullable: false,
				},
			},
		}
		err = cm.WAL().WriteEntry(entry)
		require.NoError(t, err)
	}

	bytesBeforeCheckpoint := cm.WAL().
		BytesWritten()
	require.GreaterOrEqual(
		t,
		bytesBeforeCheckpoint,
		uint64(500),
	)

	// Auto-checkpoint should trigger
	err = cm.MaybeAutoCheckpoint()
	require.NoError(t, err)

	// Bytes written should be less than before checkpoint (reset to just header + checkpoint marker)
	assert.Less(
		t,
		cm.WAL().BytesWritten(),
		bytesBeforeCheckpoint,
	)

	err = cm.WAL().Close()
	require.NoError(t, err)
}

func TestEntryTypes(t *testing.T) {
	tests := []struct {
		name  string
		entry Entry
	}{
		{
			"CreateTable",
			&CreateTableEntry{
				Schema: "main",
				Name:   "test",
				Columns: []ColumnDef{
					{
						Name: "id",
						Type: dukdb.TYPE_INTEGER,
					},
				},
			},
		},
		{
			"DropTable",
			&DropTableEntry{
				Schema: "main",
				Name:   "test",
			},
		},
		{
			"CreateSchema",
			&CreateSchemaEntry{
				Name: "test_schema",
			},
		},
		{
			"DropSchema",
			&DropSchemaEntry{Name: "test_schema"},
		},
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
			"CreateIndex",
			&CreateIndexEntry{
				Schema:   "main",
				Table:    "test",
				Name:     "idx",
				Columns:  []string{"id"},
				IsUnique: true,
			},
		},
		{
			"DropIndex",
			&DropIndexEntry{
				Schema: "main",
				Table:  "test",
				Name:   "idx",
			},
		},
		{
			"UseTable",
			&UseTableEntry{
				Schema: "main",
				Table:  "test",
			},
		},
		{
			"TxnBegin",
			NewTxnBeginEntry(1, time.Now()),
		},
		{
			"TxnCommit",
			NewTxnCommitEntry(1, time.Now()),
		},
		{
			"Checkpoint",
			NewCheckpointEntry(1, time.Now()),
		},
		{"Flush", NewFlushEntry(time.Now())},
		{
			"Version",
			NewVersionEntry(2, 1, time.Now()),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotEmpty(
				t,
				tt.entry.Type().String(),
			)
		})
	}
}

// TestRecoveryInsert tests that crash after INSERT, recovery restores inserted rows
// This covers task 3.21: Test crash after INSERT, recovery restores inserted rows
func TestRecoveryInsert(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write INSERT to WAL (simulating committed transaction)
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

	// Insert 3 rows
	insertEntry := NewInsertEntry(
		1,
		"main",
		"users",
		[][]any{
			{int32(1), "Alice"},
			{int32(2), "Bob"},
			{int32(3), "Charlie"},
		},
	)
	err = writer.WriteEntry(insertEntry)
	require.NoError(t, err)

	// Commit transaction
	commitEntry := NewTxnCommitEntry(1, clock.Now())
	err = writer.WriteEntry(commitEntry)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery (simulating restart after crash)
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify all 3 rows were recovered
	table, ok := store.GetTable("users")
	require.True(t, ok)
	assert.Equal(t, int64(3), table.RowCount())

	// Verify row values
	row0 := table.GetRow(storage.RowID(0))
	require.NotNil(t, row0)
	assert.Equal(t, int32(1), row0[0])
	assert.Equal(t, "Alice", row0[1])

	row1 := table.GetRow(storage.RowID(1))
	require.NotNil(t, row1)
	assert.Equal(t, int32(2), row1[0])
	assert.Equal(t, "Bob", row1[1])

	row2 := table.GetRow(storage.RowID(2))
	require.NotNil(t, row2)
	assert.Equal(t, int32(3), row2[0])
	assert.Equal(t, "Charlie", row2[1])
}

// TestRecoveryUpdate tests that crash after UPDATE, recovery restores updated values
// This covers task 3.22: Test crash after UPDATE, recovery restores updated values
func TestRecoveryUpdate(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write INSERT + UPDATE to WAL
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

	// Transaction 1: Insert rows
	beginEntry := NewTxnBeginEntry(1, clock.Now())
	err = writer.WriteEntry(beginEntry)
	require.NoError(t, err)

	insertEntry := NewInsertEntry(
		1,
		"main",
		"users",
		[][]any{
			{int32(1), "Alice"},
			{int32(2), "Bob"},
		},
	)
	err = writer.WriteEntry(insertEntry)
	require.NoError(t, err)

	commitEntry := NewTxnCommitEntry(1, clock.Now())
	err = writer.WriteEntry(commitEntry)
	require.NoError(t, err)

	// Transaction 2: Update row 0 (Alice -> ALICE)
	beginEntry2 := NewTxnBeginEntry(2, clock.Now())
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	updateEntry := NewUpdateEntryWithBeforeValues(
		2,
		"main",
		"users",
		[]uint64{0}, // RowID 0
		[]int{1},    // Update column 1 (name)
		[][]any{{"Alice"}},  // Before value
		[][]any{{"ALICE"}},  // After value
	)
	err = writer.WriteEntry(updateEntry)
	require.NoError(t, err)

	commitEntry2 := NewTxnCommitEntry(2, clock.Now())
	err = writer.WriteEntry(commitEntry2)
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

	// Verify the update was applied
	table, ok := store.GetTable("users")
	require.True(t, ok)
	assert.Equal(t, int64(2), table.RowCount())

	row0 := table.GetRow(storage.RowID(0))
	require.NotNil(t, row0)
	assert.Equal(t, int32(1), row0[0])
	assert.Equal(t, "ALICE", row0[1]) // Should be updated

	row1 := table.GetRow(storage.RowID(1))
	require.NotNil(t, row1)
	assert.Equal(t, int32(2), row1[0])
	assert.Equal(t, "Bob", row1[1]) // Should be unchanged
}

// TestRecoveryDelete tests that crash after DELETE, recovery removes deleted rows
// This covers task 3.23: Test crash after DELETE, recovery removes deleted rows
func TestRecoveryDelete(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write INSERT + DELETE to WAL
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

	// Transaction 1: Insert 3 rows
	beginEntry := NewTxnBeginEntry(1, clock.Now())
	err = writer.WriteEntry(beginEntry)
	require.NoError(t, err)

	insertEntry := NewInsertEntry(
		1,
		"main",
		"users",
		[][]any{
			{int32(1), "Alice"},
			{int32(2), "Bob"},
			{int32(3), "Charlie"},
		},
	)
	err = writer.WriteEntry(insertEntry)
	require.NoError(t, err)

	commitEntry := NewTxnCommitEntry(1, clock.Now())
	err = writer.WriteEntry(commitEntry)
	require.NoError(t, err)

	// Transaction 2: Delete row 1 (Bob)
	beginEntry2 := NewTxnBeginEntry(2, clock.Now())
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	deleteEntry := NewDeleteEntryWithData(
		2,
		"main",
		"users",
		[]uint64{1}, // Delete RowID 1 (Bob)
		[][]any{{int32(2), "Bob"}}, // Deleted data for rollback
	)
	err = writer.WriteEntry(deleteEntry)
	require.NoError(t, err)

	commitEntry2 := NewTxnCommitEntry(2, clock.Now())
	err = writer.WriteEntry(commitEntry2)
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

	// Verify the delete was applied
	table, ok := store.GetTable("users")
	require.True(t, ok)

	// Row 0 (Alice) should exist
	row0 := table.GetRow(storage.RowID(0))
	require.NotNil(t, row0)
	assert.Equal(t, int32(1), row0[0])
	assert.Equal(t, "Alice", row0[1])

	// Row 1 (Bob) should be deleted (tombstoned)
	row1 := table.GetRow(storage.RowID(1))
	assert.Nil(t, row1) // GetRow returns nil for deleted rows
	assert.True(t, table.IsTombstoned(storage.RowID(1)))

	// Row 2 (Charlie) should exist
	row2 := table.GetRow(storage.RowID(2))
	require.NotNil(t, row2)
	assert.Equal(t, int32(3), row2[0])
	assert.Equal(t, "Charlie", row2[1])
}

// TestRecoveryIdempotent tests that recovery is idempotent (running twice produces same result)
func TestRecoveryIdempotent(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Write WAL with INSERT, UPDATE, DELETE
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

	// Transaction 1: Insert
	beginEntry := NewTxnBeginEntry(1, clock.Now())
	err = writer.WriteEntry(beginEntry)
	require.NoError(t, err)

	insertEntry := NewInsertEntry(1, "main", "users", [][]any{
		{int32(1), "Alice"},
		{int32(2), "Bob"},
	})
	err = writer.WriteEntry(insertEntry)
	require.NoError(t, err)

	commitEntry := NewTxnCommitEntry(1, clock.Now())
	err = writer.WriteEntry(commitEntry)
	require.NoError(t, err)

	// Transaction 2: Update
	beginEntry2 := NewTxnBeginEntry(2, clock.Now())
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	updateEntry := NewUpdateEntryWithBeforeValues(
		2, "main", "users",
		[]uint64{0}, []int{1},
		[][]any{{"Alice"}},
		[][]any{{"ALICE"}},
	)
	err = writer.WriteEntry(updateEntry)
	require.NoError(t, err)

	commitEntry2 := NewTxnCommitEntry(2, clock.Now())
	err = writer.WriteEntry(commitEntry2)
	require.NoError(t, err)

	// Transaction 3: Delete
	beginEntry3 := NewTxnBeginEntry(3, clock.Now())
	err = writer.WriteEntry(beginEntry3)
	require.NoError(t, err)

	deleteEntry := NewDeleteEntryWithData(
		3, "main", "users",
		[]uint64{1},
		[][]any{{int32(2), "Bob"}},
	)
	err = writer.WriteEntry(deleteEntry)
	require.NoError(t, err)

	commitEntry3 := NewTxnCommitEntry(3, clock.Now())
	err = writer.WriteEntry(commitEntry3)
	require.NoError(t, err)

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// First recovery
	cat1 := catalog.NewCatalog()
	store1 := storage.NewStorage()

	reader1, err := NewReader(walPath, clock)
	require.NoError(t, err)
	err = reader1.Recover(cat1, store1)
	require.NoError(t, err)
	err = reader1.Close()
	require.NoError(t, err)

	// Capture state after first recovery
	table1, ok := store1.GetTable("users")
	require.True(t, ok)
	row0First := table1.GetRow(storage.RowID(0))
	row1First := table1.GetRow(storage.RowID(1))

	// Second recovery (simulating double-recovery scenario)
	cat2 := catalog.NewCatalog()
	store2 := storage.NewStorage()

	reader2, err := NewReader(walPath, clock)
	require.NoError(t, err)
	err = reader2.Recover(cat2, store2)
	require.NoError(t, err)
	err = reader2.Close()
	require.NoError(t, err)

	// Capture state after second recovery
	table2, ok := store2.GetTable("users")
	require.True(t, ok)
	row0Second := table2.GetRow(storage.RowID(0))
	row1Second := table2.GetRow(storage.RowID(1))

	// Verify states are identical
	assert.Equal(t, row0First, row0Second)
	assert.Equal(t, row1First, row1Second)
}

// TestRecoveryUncommittedNotRecovered tests that uncommitted transactions are not recovered
func TestRecoveryUncommittedNotRecovered(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

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

	// Committed transaction
	beginEntry := NewTxnBeginEntry(1, clock.Now())
	err = writer.WriteEntry(beginEntry)
	require.NoError(t, err)

	insertEntry := NewInsertEntry(1, "main", "users", [][]any{
		{int32(1), "Alice"},
	})
	err = writer.WriteEntry(insertEntry)
	require.NoError(t, err)

	commitEntry := NewTxnCommitEntry(1, clock.Now())
	err = writer.WriteEntry(commitEntry)
	require.NoError(t, err)

	// Uncommitted transaction (no commit)
	beginEntry2 := NewTxnBeginEntry(2, clock.Now())
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	insertEntry2 := NewInsertEntry(2, "main", "users", [][]any{
		{int32(2), "Bob"},
	})
	err = writer.WriteEntry(insertEntry2)
	require.NoError(t, err)

	// Uncommitted update
	updateEntry := NewUpdateEntryWithBeforeValues(
		2, "main", "users",
		[]uint64{0}, []int{1},
		[][]any{{"Alice"}},
		[][]any{{"ALICE"}},
	)
	err = writer.WriteEntry(updateEntry)
	require.NoError(t, err)

	// Uncommitted delete
	deleteEntry := NewDeleteEntryWithData(
		2, "main", "users",
		[]uint64{0},
		[][]any{{int32(1), "Alice"}},
	)
	err = writer.WriteEntry(deleteEntry)
	require.NoError(t, err)

	// NO COMMIT for transaction 2!

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Recovery
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify only committed data was recovered
	table, ok := store.GetTable("users")
	require.True(t, ok)
	assert.Equal(t, int64(1), table.RowCount()) // Only Alice, not Bob

	row0 := table.GetRow(storage.RowID(0))
	require.NotNil(t, row0)
	assert.Equal(t, int32(1), row0[0])
	assert.Equal(t, "Alice", row0[1]) // Should NOT be updated to ALICE

	// Alice should NOT be deleted
	assert.False(t, table.IsTombstoned(storage.RowID(0)))
}

// TestCrashMidTransaction_RollsBack tests that crash mid-transaction (before commit) results in rollback on recovery.
// This covers task 3.24: Test: Crash mid-transaction (before commit), recovery rolls back
func TestCrashMidTransaction_RollsBack(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write data - Start transaction but do NOT commit (simulate crash)
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

	// First committed transaction
	beginEntry1 := NewTxnBeginEntry(1, clock.Now())
	err = writer.WriteEntry(beginEntry1)
	require.NoError(t, err)

	insertEntry1 := NewInsertEntry(1, "main", "users", [][]any{
		{int32(1), "CommittedAlice"},
		{int32(2), "CommittedBob"},
	})
	err = writer.WriteEntry(insertEntry1)
	require.NoError(t, err)

	commitEntry1 := NewTxnCommitEntry(1, clock.Now())
	err = writer.WriteEntry(commitEntry1)
	require.NoError(t, err)

	// Second transaction: Insert more rows but DO NOT COMMIT (simulating crash)
	beginEntry2 := NewTxnBeginEntry(2, clock.Now())
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	insertEntry2 := NewInsertEntry(2, "main", "users", [][]any{
		{int32(3), "UncommittedCharlie"},
		{int32(4), "UncommittedDave"},
		{int32(5), "UncommittedEve"},
	})
	err = writer.WriteEntry(insertEntry2)
	require.NoError(t, err)

	// Update in uncommitted transaction
	updateEntry := NewUpdateEntryWithBeforeValues(
		2, "main", "users",
		[]uint64{0}, []int{1},
		[][]any{{"CommittedAlice"}},
		[][]any{{"UNCOMMITTED_UPDATE"}},
	)
	err = writer.WriteEntry(updateEntry)
	require.NoError(t, err)

	// Delete in uncommitted transaction
	deleteEntry := NewDeleteEntryWithData(
		2, "main", "users",
		[]uint64{1},
		[][]any{{int32(2), "CommittedBob"}},
	)
	err = writer.WriteEntry(deleteEntry)
	require.NoError(t, err)

	// NO COMMIT for transaction 2 - simulate crash by just closing the WAL
	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Phase 2: Recovery - uncommitted transaction should be rolled back
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, clock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify only committed transaction was recovered
	table, ok := store.GetTable("users")
	require.True(t, ok)
	assert.Equal(t, int64(2), table.RowCount()) // Only Alice and Bob from committed txn

	// Verify committed rows are present
	row0 := table.GetRow(storage.RowID(0))
	require.NotNil(t, row0)
	assert.Equal(t, int32(1), row0[0])
	assert.Equal(t, "CommittedAlice", row0[1]) // NOT updated to UNCOMMITTED_UPDATE

	row1 := table.GetRow(storage.RowID(1))
	require.NotNil(t, row1) // NOT deleted
	assert.Equal(t, int32(2), row1[0])
	assert.Equal(t, "CommittedBob", row1[1])

	// Verify uncommitted rows do not exist
	row2 := table.GetRow(storage.RowID(2))
	assert.Nil(t, row2) // UncommittedCharlie should not exist

	row3 := table.GetRow(storage.RowID(3))
	assert.Nil(t, row3) // UncommittedDave should not exist

	row4 := table.GetRow(storage.RowID(4))
	assert.Nil(t, row4) // UncommittedEve should not exist
}

// TestCrashAtRow500_RecoverCommitted tests deterministic crash during INSERT of 1000 rows at row 500.
// This covers task 3.25: Add deterministic crash test: INSERT 1000 rows, crash at row 500
func TestCrashAtRow500_RecoverCommitted(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write data
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	// Create table
	createEntry := &CreateTableEntry{
		Schema: "main",
		Name:   "large_table",
		Columns: []ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
			{Name: "value", Type: dukdb.TYPE_VARCHAR, Nullable: true},
		},
	}
	err = writer.WriteEntry(createEntry)
	require.NoError(t, err)

	// Transaction 1: Insert first 500 rows and COMMIT
	beginEntry1 := NewTxnBeginEntry(1, clock.Now())
	err = writer.WriteEntry(beginEntry1)
	require.NoError(t, err)

	rows1 := make([][]any, 500)
	for i := range 500 {
		rows1[i] = []any{int32(i), "committed_" + string(rune('a'+(i%26)))}
	}
	insertEntry1 := NewInsertEntry(1, "main", "large_table", rows1)
	err = writer.WriteEntry(insertEntry1)
	require.NoError(t, err)

	commitEntry1 := NewTxnCommitEntry(1, clock.Now())
	err = writer.WriteEntry(commitEntry1)
	require.NoError(t, err)

	// Transaction 2: Insert next 500 rows but DO NOT COMMIT (simulate crash at row 500)
	beginEntry2 := NewTxnBeginEntry(2, clock.Now())
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	rows2 := make([][]any, 500)
	for i := range 500 {
		rows2[i] = []any{int32(i + 500), "uncommitted_" + string(rune('a'+(i%26)))}
	}
	insertEntry2 := NewInsertEntry(2, "main", "large_table", rows2)
	err = writer.WriteEntry(insertEntry2)
	require.NoError(t, err)

	// NO COMMIT - crash at row 500 (after first 500 committed, before second 500 committed)
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

	// Verify only 500 committed rows were recovered
	table, ok := store.GetTable("large_table")
	require.True(t, ok)
	assert.Equal(t, int64(500), table.RowCount())

	// Verify committed rows
	for i := range 500 {
		row := table.GetRow(storage.RowID(i))
		require.NotNil(t, row, "Row %d should exist", i)
		assert.Equal(t, int32(i), row[0])
		assert.Equal(t, "committed_"+string(rune('a'+(i%26))), row[1])
	}

	// Verify uncommitted rows do not exist
	for i := range 500 {
		row := table.GetRow(storage.RowID(i + 500))
		assert.Nil(t, row, "Row %d should NOT exist (uncommitted)", i+500)
	}
}

// TestCrashMidUpdate_RecoverCommitted tests deterministic crash during UPDATE of 1000 rows.
// This covers task 3.26: Add deterministic crash test: UPDATE 1000 rows, crash mid-operation
func TestCrashMidUpdate_RecoverCommitted(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write data
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	// Create table
	createEntry := &CreateTableEntry{
		Schema: "main",
		Name:   "update_table",
		Columns: []ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
			{Name: "status", Type: dukdb.TYPE_VARCHAR, Nullable: true},
		},
	}
	err = writer.WriteEntry(createEntry)
	require.NoError(t, err)

	// Transaction 1: Insert 1000 rows (committed)
	beginEntry1 := NewTxnBeginEntry(1, clock.Now())
	err = writer.WriteEntry(beginEntry1)
	require.NoError(t, err)

	rows := make([][]any, 1000)
	for i := range 1000 {
		rows[i] = []any{int32(i), "original"}
	}
	insertEntry := NewInsertEntry(1, "main", "update_table", rows)
	err = writer.WriteEntry(insertEntry)
	require.NoError(t, err)

	commitEntry1 := NewTxnCommitEntry(1, clock.Now())
	err = writer.WriteEntry(commitEntry1)
	require.NoError(t, err)

	// Transaction 2: Update first 500 rows (committed)
	beginEntry2 := NewTxnBeginEntry(2, clock.Now())
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	rowIDs2 := make([]uint64, 500)
	beforeValues2 := make([][]any, 500)
	afterValues2 := make([][]any, 500)
	for i := range 500 {
		rowIDs2[i] = uint64(i)
		beforeValues2[i] = []any{"original"}
		afterValues2[i] = []any{"updated_committed"}
	}
	updateEntry2 := NewUpdateEntryWithBeforeValues(
		2, "main", "update_table",
		rowIDs2, []int{1},
		beforeValues2, afterValues2,
	)
	err = writer.WriteEntry(updateEntry2)
	require.NoError(t, err)

	commitEntry2 := NewTxnCommitEntry(2, clock.Now())
	err = writer.WriteEntry(commitEntry2)
	require.NoError(t, err)

	// Transaction 3: Update next 500 rows (NOT committed - crash mid-operation)
	beginEntry3 := NewTxnBeginEntry(3, clock.Now())
	err = writer.WriteEntry(beginEntry3)
	require.NoError(t, err)

	rowIDs3 := make([]uint64, 500)
	beforeValues3 := make([][]any, 500)
	afterValues3 := make([][]any, 500)
	for i := range 500 {
		rowIDs3[i] = uint64(i + 500)
		beforeValues3[i] = []any{"original"}
		afterValues3[i] = []any{"updated_uncommitted"}
	}
	updateEntry3 := NewUpdateEntryWithBeforeValues(
		3, "main", "update_table",
		rowIDs3, []int{1},
		beforeValues3, afterValues3,
	)
	err = writer.WriteEntry(updateEntry3)
	require.NoError(t, err)

	// NO COMMIT for transaction 3 - simulate crash
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

	// Verify state after recovery
	table, ok := store.GetTable("update_table")
	require.True(t, ok)
	assert.Equal(t, int64(1000), table.RowCount())

	// Verify first 500 rows have committed update
	for i := range 500 {
		row := table.GetRow(storage.RowID(i))
		require.NotNil(t, row)
		assert.Equal(t, int32(i), row[0])
		assert.Equal(t, "updated_committed", row[1], "Row %d should have committed update", i)
	}

	// Verify next 500 rows still have original values (uncommitted update rolled back)
	for i := range 500 {
		row := table.GetRow(storage.RowID(i + 500))
		require.NotNil(t, row)
		assert.Equal(t, int32(i+500), row[0])
		assert.Equal(t, "original", row[1], "Row %d should have original value (rollback)", i+500)
	}
}

// TestCrashMidDelete_RecoverCommitted tests deterministic crash during DELETE of 1000 rows.
// This covers task 3.27: Add deterministic crash test: DELETE 1000 rows, crash mid-operation
func TestCrashMidDelete_RecoverCommitted(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write data
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	// Create table
	createEntry := &CreateTableEntry{
		Schema: "main",
		Name:   "delete_table",
		Columns: []ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
			{Name: "data", Type: dukdb.TYPE_VARCHAR, Nullable: true},
		},
	}
	err = writer.WriteEntry(createEntry)
	require.NoError(t, err)

	// Transaction 1: Insert 1000 rows (committed)
	beginEntry1 := NewTxnBeginEntry(1, clock.Now())
	err = writer.WriteEntry(beginEntry1)
	require.NoError(t, err)

	rows := make([][]any, 1000)
	for i := range 1000 {
		rows[i] = []any{int32(i), "data_" + string(rune('a'+(i%26)))}
	}
	insertEntry := NewInsertEntry(1, "main", "delete_table", rows)
	err = writer.WriteEntry(insertEntry)
	require.NoError(t, err)

	commitEntry1 := NewTxnCommitEntry(1, clock.Now())
	err = writer.WriteEntry(commitEntry1)
	require.NoError(t, err)

	// Transaction 2: Delete first 500 rows (committed)
	beginEntry2 := NewTxnBeginEntry(2, clock.Now())
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	rowIDs2 := make([]uint64, 500)
	deletedData2 := make([][]any, 500)
	for i := range 500 {
		rowIDs2[i] = uint64(i)
		deletedData2[i] = []any{int32(i), "data_" + string(rune('a'+(i%26)))}
	}
	deleteEntry2 := NewDeleteEntryWithData(2, "main", "delete_table", rowIDs2, deletedData2)
	err = writer.WriteEntry(deleteEntry2)
	require.NoError(t, err)

	commitEntry2 := NewTxnCommitEntry(2, clock.Now())
	err = writer.WriteEntry(commitEntry2)
	require.NoError(t, err)

	// Transaction 3: Delete next 500 rows (NOT committed - crash mid-operation)
	beginEntry3 := NewTxnBeginEntry(3, clock.Now())
	err = writer.WriteEntry(beginEntry3)
	require.NoError(t, err)

	rowIDs3 := make([]uint64, 500)
	deletedData3 := make([][]any, 500)
	for i := range 500 {
		rowIDs3[i] = uint64(i + 500)
		deletedData3[i] = []any{int32(i + 500), "data_" + string(rune('a'+((i+500)%26)))}
	}
	deleteEntry3 := NewDeleteEntryWithData(3, "main", "delete_table", rowIDs3, deletedData3)
	err = writer.WriteEntry(deleteEntry3)
	require.NoError(t, err)

	// NO COMMIT for transaction 3 - simulate crash
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

	// Verify state after recovery
	table, ok := store.GetTable("delete_table")
	require.True(t, ok)

	// First 500 rows should be deleted (committed)
	for i := range 500 {
		row := table.GetRow(storage.RowID(i))
		assert.Nil(t, row, "Row %d should be deleted (committed)", i)
		assert.True(t, table.IsTombstoned(storage.RowID(i)), "Row %d should be tombstoned", i)
	}

	// Next 500 rows should still exist (uncommitted delete rolled back)
	for i := range 500 {
		row := table.GetRow(storage.RowID(i + 500))
		require.NotNil(t, row, "Row %d should exist (uncommitted delete rolled back)", i+500)
		assert.Equal(t, int32(i+500), row[0])
		assert.False(t, table.IsTombstoned(storage.RowID(i+500)), "Row %d should NOT be tombstoned", i+500)
	}
}

// TestTimeoutDuringDelete tests timeout during long-running DELETE using context cancellation.
// This covers task 3.28: Test: Timeout during long-running DELETE (quartz.Mock clock advance)
// Note: This test simulates context cancellation behavior at the WAL level.
// The actual context propagation through the executor is tested elsewhere.
func TestTimeoutDuringDelete(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	mClock := quartz.NewMock(t)

	// Create WAL writer with mock clock
	writer, err := NewWriter(walPath, mClock)
	require.NoError(t, err)

	// Create table
	createEntry := &CreateTableEntry{
		Schema: "main",
		Name:   "timeout_table",
		Columns: []ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
			{Name: "value", Type: dukdb.TYPE_VARCHAR, Nullable: true},
		},
	}
	err = writer.WriteEntry(createEntry)
	require.NoError(t, err)

	// Transaction 1: Insert rows (committed)
	beginEntry1 := NewTxnBeginEntry(1, mClock.Now())
	err = writer.WriteEntry(beginEntry1)
	require.NoError(t, err)

	rows := make([][]any, 100)
	for i := range 100 {
		rows[i] = []any{int32(i), "value_" + string(rune('a'+(i%26)))}
	}
	insertEntry := NewInsertEntry(1, "main", "timeout_table", rows)
	err = writer.WriteEntry(insertEntry)
	require.NoError(t, err)

	commitEntry1 := NewTxnCommitEntry(1, mClock.Now())
	err = writer.WriteEntry(commitEntry1)
	require.NoError(t, err)

	// Transaction 2: Start delete but "timeout" before commit
	// This simulates what happens when a context is cancelled - the transaction
	// is aborted and no commit is written to the WAL.
	beginEntry2 := NewTxnBeginEntry(2, mClock.Now())
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	rowIDs := make([]uint64, 50)
	deletedData := make([][]any, 50)
	for i := range 50 {
		rowIDs[i] = uint64(i)
		deletedData[i] = []any{int32(i), "value_" + string(rune('a'+(i%26)))}
	}
	deleteEntry := NewDeleteEntryWithData(2, "main", "timeout_table", rowIDs, deletedData)
	err = writer.WriteEntry(deleteEntry)
	require.NoError(t, err)

	// Advance mock clock past the timeout deadline
	mClock.Advance(6 * time.Second)

	// NO COMMIT - simulating timeout/cancellation
	// In a real scenario, context.DeadlineExceeded would be returned and
	// the transaction would abort without writing a commit entry.

	err = writer.Sync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Recovery should restore the table without the timed-out delete
	cat := catalog.NewCatalog()
	store := storage.NewStorage()

	reader, err := NewReader(walPath, mClock)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	err = reader.Recover(cat, store)
	require.NoError(t, err)

	// Verify all 100 rows still exist (timeout delete was not committed)
	table, ok := store.GetTable("timeout_table")
	require.True(t, ok)

	for i := range 100 {
		row := table.GetRow(storage.RowID(i))
		require.NotNil(t, row, "Row %d should exist (timeout delete not committed)", i)
		assert.False(t, table.IsTombstoned(storage.RowID(i)))
	}
}

// TestTransactionRollback_UndoesPartialInsert tests that transaction rollback undoes partial INSERT.
// This covers task 3.29: Test: Transaction rollback undoes partial INSERT
// Note: Rollback is simulated by not writing a commit entry. The WAL recovery
// automatically ignores uncommitted transactions, effectively rolling them back.
func TestTransactionRollback_UndoesPartialInsert(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	clock := quartz.NewReal()

	// Phase 1: Write data with multiple transactions
	writer, err := NewWriter(walPath, clock)
	require.NoError(t, err)

	// Create table
	createEntry := &CreateTableEntry{
		Schema: "main",
		Name:   "rollback_table",
		Columns: []ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
			{Name: "batch", Type: dukdb.TYPE_VARCHAR, Nullable: true},
		},
	}
	err = writer.WriteEntry(createEntry)
	require.NoError(t, err)

	// Transaction 1: Insert first batch (committed)
	beginEntry1 := NewTxnBeginEntry(1, clock.Now())
	err = writer.WriteEntry(beginEntry1)
	require.NoError(t, err)

	batch1Rows := make([][]any, 50)
	for i := range 50 {
		batch1Rows[i] = []any{int32(i), "batch1"}
	}
	insertEntry1 := NewInsertEntry(1, "main", "rollback_table", batch1Rows)
	err = writer.WriteEntry(insertEntry1)
	require.NoError(t, err)

	commitEntry1 := NewTxnCommitEntry(1, clock.Now())
	err = writer.WriteEntry(commitEntry1)
	require.NoError(t, err)

	// Transaction 2: Insert second batch (committed)
	beginEntry2 := NewTxnBeginEntry(2, clock.Now())
	err = writer.WriteEntry(beginEntry2)
	require.NoError(t, err)

	batch2Rows := make([][]any, 50)
	for i := range 50 {
		batch2Rows[i] = []any{int32(i + 50), "batch2"}
	}
	insertEntry2 := NewInsertEntry(2, "main", "rollback_table", batch2Rows)
	err = writer.WriteEntry(insertEntry2)
	require.NoError(t, err)

	commitEntry2 := NewTxnCommitEntry(2, clock.Now())
	err = writer.WriteEntry(commitEntry2)
	require.NoError(t, err)

	// Transaction 3: Insert third batch but ROLLBACK (no commit)
	// This simulates an explicit rollback or application error
	beginEntry3 := NewTxnBeginEntry(3, clock.Now())
	err = writer.WriteEntry(beginEntry3)
	require.NoError(t, err)

	batch3Rows := make([][]any, 100)
	for i := range 100 {
		batch3Rows[i] = []any{int32(i + 100), "batch3_rollback"}
	}
	insertEntry3 := NewInsertEntry(3, "main", "rollback_table", batch3Rows)
	err = writer.WriteEntry(insertEntry3)
	require.NoError(t, err)

	// NO COMMIT - this is a rollback scenario
	// In real usage, this would be a manual rollback or error condition

	// Transaction 4: Insert fourth batch (committed) - to verify interleaving works
	beginEntry4 := NewTxnBeginEntry(4, clock.Now())
	err = writer.WriteEntry(beginEntry4)
	require.NoError(t, err)

	batch4Rows := make([][]any, 25)
	for i := range 25 {
		batch4Rows[i] = []any{int32(i + 200), "batch4"}
	}
	insertEntry4 := NewInsertEntry(4, "main", "rollback_table", batch4Rows)
	err = writer.WriteEntry(insertEntry4)
	require.NoError(t, err)

	commitEntry4 := NewTxnCommitEntry(4, clock.Now())
	err = writer.WriteEntry(commitEntry4)
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

	// Verify state after recovery
	table, ok := store.GetTable("rollback_table")
	require.True(t, ok)

	// Should have 50 (batch1) + 50 (batch2) + 25 (batch4) = 125 rows
	// batch3 was rolled back, so those 100 rows should NOT exist
	assert.Equal(t, int64(125), table.RowCount())

	// Verify batch1 rows (RowID 0-49, id values 0-49)
	for i := range 50 {
		row := table.GetRow(storage.RowID(i))
		require.NotNil(t, row, "Batch1 row %d should exist", i)
		assert.Equal(t, int32(i), row[0])
		assert.Equal(t, "batch1", row[1])
	}

	// Verify batch2 rows (RowID 50-99, id values 50-99)
	for i := range 50 {
		row := table.GetRow(storage.RowID(i + 50))
		require.NotNil(t, row, "Batch2 row %d should exist", i+50)
		assert.Equal(t, int32(i+50), row[0])
		assert.Equal(t, "batch2", row[1])
	}

	// Verify batch4 rows (RowID 100-124, id values 200-224)
	// Note: Batch3 was rolled back, so batch4 gets RowIDs starting at 100
	for i := range 25 {
		row := table.GetRow(storage.RowID(i + 100))
		require.NotNil(t, row, "Batch4 row at RowID %d should exist", i+100)
		assert.Equal(t, int32(i+200), row[0]) // id value is 200+i
		assert.Equal(t, "batch4", row[1])
	}

	// Verify no rows exist beyond RowID 124 (batch3 was rolled back)
	for i := range 25 {
		row := table.GetRow(storage.RowID(i + 125))
		assert.Nil(t, row, "RowID %d should NOT exist", i+125)
	}
}
