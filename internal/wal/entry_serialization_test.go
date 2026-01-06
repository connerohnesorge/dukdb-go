package wal

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dukdb "github.com/dukdb/dukdb-go"
)

// TestWALEntry_InsertRoundTrip tests INSERT entry serialization with DuckDB format.
func TestWALEntry_InsertRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		txnID  uint64
		schema string
		table  string
		values [][]any
	}{
		{
			name:   "single_row_single_column",
			txnID:  1,
			schema: "main",
			table:  "users",
			values: [][]any{
				{int64(42)},
			},
		},
		{
			name:   "single_row_multiple_columns",
			txnID:  2,
			schema: "main",
			table:  "users",
			values: [][]any{
				{int64(1), "Alice", int64(30)},
			},
		},
		{
			name:   "multiple_rows",
			txnID:  3,
			schema: "main",
			table:  "users",
			values: [][]any{
				{int64(1), "Alice", int64(30)},
				{int64(2), "Bob", int64(25)},
				{int64(3), "Charlie", int64(35)},
			},
		},
		{
			name:   "with_null_values",
			txnID:  4,
			schema: "main",
			table:  "users",
			values: [][]any{
				{int64(1), "Alice", nil},
				{int64(2), nil, int64(25)},
			},
		},
		{
			name:   "empty_schema",
			txnID:  5,
			schema: "",
			table:  "users",
			values: [][]any{
				{int64(1), "Alice", int64(30)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create entry
			original := NewInsertEntry(tt.txnID, tt.schema, tt.table, tt.values)

			// Serialize payload
			var payloadBuf bytes.Buffer
			err := original.Serialize(&payloadBuf)
			require.NoError(t, err)
			payload := payloadBuf.Bytes()

			// Create entry header
			header := EntryHeader{
				Type:           EntryInsert,
				Flags:          EntryFlagChecksum,
				Length:         uint32(len(payload)),
				SequenceNumber: 1,
			}

			// Serialize header
			var headerBuf bytes.Buffer
			err = header.Serialize(&headerBuf)
			require.NoError(t, err)
			assert.Equal(t, EntryHeaderSize, headerBuf.Len())

			// Calculate checksum
			checksum := header.CalculateChecksum(payload)
			assert.NotZero(t, checksum)

			// Deserialize header
			var deserializedHeader EntryHeader
			err = deserializedHeader.Deserialize(&headerBuf)
			require.NoError(t, err)
			assert.Equal(t, header.Type, deserializedHeader.Type)
			assert.Equal(t, header.Flags, deserializedHeader.Flags)
			assert.Equal(t, header.Length, deserializedHeader.Length)
			assert.Equal(t, header.SequenceNumber, deserializedHeader.SequenceNumber)

			// Deserialize entry
			deserialized := &InsertEntry{}
			err = deserialized.Deserialize(&payloadBuf)
			require.NoError(t, err)

			// Verify fields
			assert.Equal(t, tt.txnID, deserialized.TxnID())
			assert.Equal(t, tt.schema, deserialized.Schema)
			assert.Equal(t, tt.table, deserialized.Table)
			assert.Equal(t, len(tt.values), len(deserialized.Values))
			for i := range tt.values {
				assert.Equal(t, tt.values[i], deserialized.Values[i])
			}
		})
	}
}

// TestWALEntry_DeleteRoundTrip tests DELETE entry serialization with DuckDB format.
func TestWALEntry_DeleteRoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		txnID       uint64
		schema      string
		table       string
		rowIDs      []uint64
		deletedData [][]any
	}{
		{
			name:   "single_row",
			txnID:  1,
			schema: "main",
			table:  "users",
			rowIDs: []uint64{10},
		},
		{
			name:   "multiple_rows",
			txnID:  2,
			schema: "main",
			table:  "users",
			rowIDs: []uint64{10, 20, 30},
		},
		{
			name:   "with_deleted_data",
			txnID:  3,
			schema: "main",
			table:  "users",
			rowIDs: []uint64{10, 20},
			deletedData: [][]any{
				{int64(1), "Alice", int64(30)},
				{int64(2), "Bob", int64(25)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create entry
			var original *DeleteEntry
			if tt.deletedData != nil {
				original = NewDeleteEntryWithData(tt.txnID, tt.schema, tt.table, tt.rowIDs, tt.deletedData)
			} else {
				original = NewDeleteEntry(tt.txnID, tt.schema, tt.table, tt.rowIDs)
			}

			// Serialize payload
			var payloadBuf bytes.Buffer
			err := original.Serialize(&payloadBuf)
			require.NoError(t, err)
			payload := payloadBuf.Bytes()

			// Create entry header
			header := EntryHeader{
				Type:           EntryDelete,
				Flags:          EntryFlagChecksum,
				Length:         uint32(len(payload)),
				SequenceNumber: 1,
			}

			// Serialize header
			var headerBuf bytes.Buffer
			err = header.Serialize(&headerBuf)
			require.NoError(t, err)
			assert.Equal(t, EntryHeaderSize, headerBuf.Len())

			// Calculate checksum
			checksum := header.CalculateChecksum(payload)
			assert.NotZero(t, checksum)

			// Deserialize entry
			deserialized := &DeleteEntry{}
			err = deserialized.Deserialize(&payloadBuf)
			require.NoError(t, err)

			// Verify fields
			assert.Equal(t, tt.txnID, deserialized.TxnID())
			assert.Equal(t, tt.schema, deserialized.Schema)
			assert.Equal(t, tt.table, deserialized.Table)
			assert.Equal(t, tt.rowIDs, deserialized.RowIDs)
			if tt.deletedData != nil {
				assert.Equal(t, tt.deletedData, deserialized.DeletedData)
			}
		})
	}
}

// TestWALEntry_UpdateRoundTrip tests UPDATE entry serialization with DuckDB format.
func TestWALEntry_UpdateRoundTrip(t *testing.T) {
	tests := []struct {
		name         string
		txnID        uint64
		schema       string
		table        string
		rowIDs       []uint64
		columnIdxs   []int
		beforeValues [][]any
		newValues    [][]any
	}{
		{
			name:       "single_row_single_column",
			txnID:      1,
			schema:     "main",
			table:      "users",
			rowIDs:     []uint64{10},
			columnIdxs: []int{2},
			newValues: [][]any{
				{int64(31)},
			},
		},
		{
			name:       "multiple_rows_multiple_columns",
			txnID:      2,
			schema:     "main",
			table:      "users",
			rowIDs:     []uint64{10, 20},
			columnIdxs: []int{1, 2},
			newValues: [][]any{
				{"Alice2", int64(31)},
				{"Bob2", int64(26)},
			},
		},
		{
			name:       "with_before_values",
			txnID:      3,
			schema:     "main",
			table:      "users",
			rowIDs:     []uint64{10, 20},
			columnIdxs: []int{1, 2},
			beforeValues: [][]any{
				{"Alice", int64(30)},
				{"Bob", int64(25)},
			},
			newValues: [][]any{
				{"Alice2", int64(31)},
				{"Bob2", int64(26)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create entry
			var original *UpdateEntry
			if tt.beforeValues != nil {
				original = NewUpdateEntryWithBeforeValues(
					tt.txnID,
					tt.schema,
					tt.table,
					tt.rowIDs,
					tt.columnIdxs,
					tt.beforeValues,
					tt.newValues,
				)
			} else {
				original = NewUpdateEntry(
					tt.txnID,
					tt.schema,
					tt.table,
					tt.rowIDs,
					tt.columnIdxs,
					tt.newValues,
				)
			}

			// Serialize payload
			var payloadBuf bytes.Buffer
			err := original.Serialize(&payloadBuf)
			require.NoError(t, err)
			payload := payloadBuf.Bytes()

			// Create entry header
			header := EntryHeader{
				Type:           EntryUpdate,
				Flags:          EntryFlagChecksum,
				Length:         uint32(len(payload)),
				SequenceNumber: 1,
			}

			// Serialize header
			var headerBuf bytes.Buffer
			err = header.Serialize(&headerBuf)
			require.NoError(t, err)
			assert.Equal(t, EntryHeaderSize, headerBuf.Len())

			// Calculate checksum
			checksum := header.CalculateChecksum(payload)
			assert.NotZero(t, checksum)

			// Deserialize entry
			deserialized := &UpdateEntry{}
			err = deserialized.Deserialize(&payloadBuf)
			require.NoError(t, err)

			// Verify fields
			assert.Equal(t, tt.txnID, deserialized.TxnID())
			assert.Equal(t, tt.schema, deserialized.Schema)
			assert.Equal(t, tt.table, deserialized.Table)
			assert.Equal(t, tt.rowIDs, deserialized.RowIDs)
			assert.Equal(t, tt.columnIdxs, deserialized.ColumnIdxs)
			assert.Equal(t, tt.newValues, deserialized.NewValues)
			if tt.beforeValues != nil {
				assert.Equal(t, tt.beforeValues, deserialized.BeforeValues)
			}
		})
	}
}

// TestWALEntry_TxnCommitRoundTrip tests COMMIT entry serialization with DuckDB format.
func TestWALEntry_TxnCommitRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		txnID     uint64
		timestamp time.Time
	}{
		{
			name:      "txn_1",
			txnID:     1,
			timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "txn_100",
			txnID:     100,
			timestamp: time.Date(2024, 12, 31, 23, 59, 59, 999999999, time.UTC),
		},
		{
			name:      "txn_max",
			txnID:     ^uint64(0), // max uint64
			timestamp: time.Now(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create entry
			original := NewTxnCommitEntry(tt.txnID, tt.timestamp)

			// Serialize payload
			var payloadBuf bytes.Buffer
			err := original.Serialize(&payloadBuf)
			require.NoError(t, err)
			payload := payloadBuf.Bytes()

			// Create entry header
			header := EntryHeader{
				Type:           EntryTxnCommit,
				Flags:          EntryFlagChecksum,
				Length:         uint32(len(payload)),
				SequenceNumber: 1,
			}

			// Serialize header
			var headerBuf bytes.Buffer
			err = header.Serialize(&headerBuf)
			require.NoError(t, err)
			assert.Equal(t, EntryHeaderSize, headerBuf.Len())

			// Calculate checksum
			checksum := header.CalculateChecksum(payload)
			assert.NotZero(t, checksum)

			// Deserialize entry
			deserialized := &TxnCommitEntry{}
			err = deserialized.Deserialize(&payloadBuf)
			require.NoError(t, err)

			// Verify fields
			assert.Equal(t, tt.txnID, deserialized.TxnID())
			assert.Equal(t, tt.timestamp.UnixNano(), deserialized.Timestamp)
		})
	}
}

// TestWALEntry_CreateTableRoundTrip tests CREATE TABLE entry serialization with DuckDB format.
func TestWALEntry_CreateTableRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		table   string
		columns []ColumnDef
	}{
		{
			name:   "single_column",
			schema: "main",
			table:  "users",
			columns: []ColumnDef{
				{Name: "id", Type: dukdb.TYPE_BIGINT, Nullable: false, HasDefault: false},
			},
		},
		{
			name:   "multiple_columns",
			schema: "main",
			table:  "users",
			columns: []ColumnDef{
				{Name: "id", Type: dukdb.TYPE_BIGINT, Nullable: false, HasDefault: false},
				{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true, HasDefault: false},
				{Name: "age", Type: dukdb.TYPE_INTEGER, Nullable: true, HasDefault: true},
			},
		},
		{
			name:   "various_types",
			schema: "test",
			table:  "data",
			columns: []ColumnDef{
				{Name: "col_int", Type: dukdb.TYPE_INTEGER, Nullable: false, HasDefault: false},
				{Name: "col_bigint", Type: dukdb.TYPE_BIGINT, Nullable: false, HasDefault: false},
				{Name: "col_varchar", Type: dukdb.TYPE_VARCHAR, Nullable: true, HasDefault: false},
				{Name: "col_double", Type: dukdb.TYPE_DOUBLE, Nullable: true, HasDefault: false},
				{Name: "col_bool", Type: dukdb.TYPE_BOOLEAN, Nullable: true, HasDefault: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create entry
			original := &CreateTableEntry{
				Schema:  tt.schema,
				Name:    tt.table,
				Columns: tt.columns,
			}

			// Serialize payload
			var payloadBuf bytes.Buffer
			err := original.Serialize(&payloadBuf)
			require.NoError(t, err)
			payload := payloadBuf.Bytes()

			// Create entry header
			header := EntryHeader{
				Type:           EntryCreateTable,
				Flags:          EntryFlagChecksum,
				Length:         uint32(len(payload)),
				SequenceNumber: 1,
			}

			// Serialize header
			var headerBuf bytes.Buffer
			err = header.Serialize(&headerBuf)
			require.NoError(t, err)
			assert.Equal(t, EntryHeaderSize, headerBuf.Len())

			// Calculate checksum
			checksum := header.CalculateChecksum(payload)
			assert.NotZero(t, checksum)

			// Deserialize entry
			deserialized := &CreateTableEntry{}
			err = deserialized.Deserialize(&payloadBuf)
			require.NoError(t, err)

			// Verify fields
			assert.Equal(t, tt.schema, deserialized.Schema)
			assert.Equal(t, tt.table, deserialized.Name)
			assert.Equal(t, len(tt.columns), len(deserialized.Columns))
			for i := range tt.columns {
				assert.Equal(t, tt.columns[i].Name, deserialized.Columns[i].Name)
				assert.Equal(t, tt.columns[i].Type, deserialized.Columns[i].Type)
				assert.Equal(t, tt.columns[i].Nullable, deserialized.Columns[i].Nullable)
				assert.Equal(t, tt.columns[i].HasDefault, deserialized.Columns[i].HasDefault)
			}
		})
	}
}

// TestWALEntry_CompleteRoundTrip tests all entry types with full serialization.
func TestWALEntry_CompleteRoundTrip(t *testing.T) {
	timestamp := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	entries := []struct {
		name  string
		entry Entry
		eType EntryType
	}{
		{
			name:  "insert",
			entry: NewInsertEntry(1, "main", "users", [][]any{{int64(1), "Alice", int64(30)}}),
			eType: EntryInsert,
		},
		{
			name:  "delete",
			entry: NewDeleteEntry(1, "main", "users", []uint64{10}),
			eType: EntryDelete,
		},
		{
			name:  "update",
			entry: NewUpdateEntry(1, "main", "users", []uint64{10}, []int{1}, [][]any{{"Bob"}}),
			eType: EntryUpdate,
		},
		{
			name:  "txn_begin",
			entry: NewTxnBeginEntry(1, timestamp),
			eType: EntryTxnBegin,
		},
		{
			name:  "txn_commit",
			entry: NewTxnCommitEntry(1, timestamp),
			eType: EntryTxnCommit,
		},
		{
			name:  "create_table",
			entry: &CreateTableEntry{Schema: "main", Name: "users", Columns: []ColumnDef{{Name: "id", Type: dukdb.TYPE_BIGINT}}},
			eType: EntryCreateTable,
		},
		{
			name:  "drop_table",
			entry: &DropTableEntry{Schema: "main", Name: "users"},
			eType: EntryDropTable,
		},
		{
			name:  "create_schema",
			entry: &CreateSchemaEntry{Name: "test"},
			eType: EntryCreateSchema,
		},
		{
			name:  "drop_schema",
			entry: &DropSchemaEntry{Name: "test"},
			eType: EntryDropSchema,
		},
		{
			name:  "checkpoint",
			entry: NewCheckpointEntry(1, timestamp),
			eType: EntryCheckpoint,
		},
		{
			name:  "flush",
			entry: NewFlushEntry(timestamp),
			eType: EntryFlush,
		},
		{
			name:  "version",
			entry: NewVersionEntry(3, 1, timestamp),
			eType: EntryVersion,
		},
		{
			name:  "savepoint",
			entry: NewSavepointEntry(1, "sp1", 5, timestamp),
			eType: EntrySavepoint,
		},
		{
			name:  "release_savepoint",
			entry: NewReleaseSavepointEntry(1, "sp1", timestamp),
			eType: EntryReleaseSavepoint,
		},
		{
			name:  "rollback_savepoint",
			entry: NewRollbackSavepointEntry(1, "sp1", 5, timestamp),
			eType: EntryRollbackSavepoint,
		},
	}

	for _, tt := range entries {
		t.Run(tt.name, func(t *testing.T) {
			// Verify entry type
			assert.Equal(t, tt.eType, tt.entry.Type())

			// Serialize payload
			var payloadBuf bytes.Buffer
			err := tt.entry.Serialize(&payloadBuf)
			require.NoError(t, err)
			payload := payloadBuf.Bytes()

			// Create entry header with checksum flag
			header := EntryHeader{
				Type:           tt.eType,
				Flags:          EntryFlagChecksum,
				Length:         uint32(len(payload)),
				SequenceNumber: 1,
			}

			// Serialize header
			var headerBuf bytes.Buffer
			err = header.Serialize(&headerBuf)
			require.NoError(t, err)
			assert.Equal(t, EntryHeaderSize, headerBuf.Len(), "header should be exactly 16 bytes")

			// Verify checksum calculation
			checksum := header.CalculateChecksum(payload)
			assert.NotZero(t, checksum, "checksum should not be zero")

			// Deserialize header
			var deserializedHeader EntryHeader
			err = deserializedHeader.Deserialize(&headerBuf)
			require.NoError(t, err)
			assert.Equal(t, header.Type, deserializedHeader.Type)
			assert.Equal(t, header.Flags, deserializedHeader.Flags)
			assert.Equal(t, header.Length, deserializedHeader.Length)
			assert.Equal(t, header.SequenceNumber, deserializedHeader.SequenceNumber)

			// Verify checksum matches
			deserializedChecksum := deserializedHeader.CalculateChecksum(payload)
			assert.Equal(t, checksum, deserializedChecksum, "checksums should match")
		})
	}
}

// TestWALEntry_ChecksumValidation tests that checksum detects corruption.
func TestWALEntry_ChecksumValidation(t *testing.T) {
	// Create an INSERT entry
	entry := NewInsertEntry(1, "main", "users", [][]any{{int64(1), "Alice"}})

	// Serialize payload
	var payloadBuf bytes.Buffer
	err := entry.Serialize(&payloadBuf)
	require.NoError(t, err)
	payload := payloadBuf.Bytes()

	// Create header
	header := EntryHeader{
		Type:           EntryInsert,
		Flags:          EntryFlagChecksum,
		Length:         uint32(len(payload)),
		SequenceNumber: 1,
	}

	// Calculate correct checksum
	correctChecksum := header.CalculateChecksum(payload)

	// Corrupt the payload
	corruptedPayload := make([]byte, len(payload))
	copy(corruptedPayload, payload)
	if len(corruptedPayload) > 0 {
		corruptedPayload[0] ^= 0xFF
	}

	// Calculate checksum of corrupted payload
	corruptedChecksum := header.CalculateChecksum(corruptedPayload)

	// Verify checksums are different
	assert.NotEqual(t, correctChecksum, corruptedChecksum, "corrupted payload should have different checksum")
}

// TestWALEntry_SequenceNumbers tests that sequence numbers are preserved.
func TestWALEntry_SequenceNumbers(t *testing.T) {
	entry := NewInsertEntry(1, "main", "users", [][]any{{int64(1)}})

	for seq := uint32(0); seq < 100; seq++ {
		// Serialize payload
		var payloadBuf bytes.Buffer
		err := entry.Serialize(&payloadBuf)
		require.NoError(t, err)
		payload := payloadBuf.Bytes()

		// Create header with specific sequence number
		header := EntryHeader{
			Type:           EntryInsert,
			Flags:          EntryFlagNone,
			Length:         uint32(len(payload)),
			SequenceNumber: seq,
		}

		// Serialize header
		var headerBuf bytes.Buffer
		err = header.Serialize(&headerBuf)
		require.NoError(t, err)

		// Deserialize header
		var deserializedHeader EntryHeader
		err = deserializedHeader.Deserialize(&headerBuf)
		require.NoError(t, err)

		// Verify sequence number
		assert.Equal(t, seq, deserializedHeader.SequenceNumber)
	}
}

// BenchmarkWALEntry_InsertSerialize benchmarks INSERT entry serialization.
func BenchmarkWALEntry_InsertSerialize(b *testing.B) {
	entry := NewInsertEntry(
		1,
		"main",
		"users",
		[][]any{
			{int64(1), "Alice", int64(30)},
			{int64(2), "Bob", int64(25)},
			{int64(3), "Charlie", int64(35)},
		},
	)

	var buf bytes.Buffer
	buf.Grow(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = entry.Serialize(&buf)
	}
}

// BenchmarkWALEntry_InsertDeserialize benchmarks INSERT entry deserialization.
func BenchmarkWALEntry_InsertDeserialize(b *testing.B) {
	entry := NewInsertEntry(
		1,
		"main",
		"users",
		[][]any{
			{int64(1), "Alice", int64(30)},
			{int64(2), "Bob", int64(25)},
			{int64(3), "Charlie", int64(35)},
		},
	)

	var buf bytes.Buffer
	_ = entry.Serialize(&buf)
	data := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		var e InsertEntry
		_ = e.Deserialize(reader)
	}
}

// TestWALEntry_SavepointRoundTrip tests SAVEPOINT entry serialization with DuckDB format.
func TestWALEntry_SavepointRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		txnID     uint64
		spName    string
		undoIndex int
		timestamp time.Time
	}{
		{
			name:      "basic_savepoint",
			txnID:     1,
			spName:    "sp1",
			undoIndex: 0,
			timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "savepoint_with_undo_operations",
			txnID:     2,
			spName:    "my_savepoint",
			undoIndex: 5,
			timestamp: time.Date(2024, 12, 31, 23, 59, 59, 999999999, time.UTC),
		},
		{
			name:      "savepoint_max_txn",
			txnID:     ^uint64(0), // max uint64
			spName:    "sp_max",
			undoIndex: 1000,
			timestamp: time.Now(),
		},
		{
			name:      "savepoint_with_special_chars",
			txnID:     42,
			spName:    "sp_with_underscores_123",
			undoIndex: 10,
			timestamp: time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create entry
			original := NewSavepointEntry(tt.txnID, tt.spName, tt.undoIndex, tt.timestamp)

			// Verify entry type
			assert.Equal(t, EntrySavepoint, original.Type())
			assert.Equal(t, tt.txnID, original.TxnID())

			// Serialize payload
			var payloadBuf bytes.Buffer
			err := original.Serialize(&payloadBuf)
			require.NoError(t, err)
			payload := payloadBuf.Bytes()

			// Create entry header
			header := EntryHeader{
				Type:           EntrySavepoint,
				Flags:          EntryFlagChecksum,
				Length:         uint32(len(payload)),
				SequenceNumber: 1,
			}

			// Serialize header
			var headerBuf bytes.Buffer
			err = header.Serialize(&headerBuf)
			require.NoError(t, err)
			assert.Equal(t, EntryHeaderSize, headerBuf.Len())

			// Calculate checksum
			checksum := header.CalculateChecksum(payload)
			assert.NotZero(t, checksum)

			// Deserialize entry
			deserialized := &SavepointEntry{}
			err = deserialized.Deserialize(bytes.NewReader(payload))
			require.NoError(t, err)

			// Verify fields
			assert.Equal(t, tt.txnID, deserialized.TxnID())
			assert.Equal(t, tt.spName, deserialized.Name)
			assert.Equal(t, tt.undoIndex, deserialized.UndoIndex)
			assert.Equal(t, tt.timestamp.UnixNano(), deserialized.Timestamp)
		})
	}
}

// TestWALEntry_ReleaseSavepointRoundTrip tests RELEASE SAVEPOINT entry serialization.
func TestWALEntry_ReleaseSavepointRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		txnID     uint64
		spName    string
		timestamp time.Time
	}{
		{
			name:      "basic_release",
			txnID:     1,
			spName:    "sp1",
			timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "release_with_long_name",
			txnID:     100,
			spName:    "a_very_long_savepoint_name_that_is_still_valid",
			timestamp: time.Date(2024, 12, 31, 23, 59, 59, 999999999, time.UTC),
		},
		{
			name:      "release_max_txn",
			txnID:     ^uint64(0), // max uint64
			spName:    "sp_max",
			timestamp: time.Now(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create entry
			original := NewReleaseSavepointEntry(tt.txnID, tt.spName, tt.timestamp)

			// Verify entry type
			assert.Equal(t, EntryReleaseSavepoint, original.Type())
			assert.Equal(t, tt.txnID, original.TxnID())

			// Serialize payload
			var payloadBuf bytes.Buffer
			err := original.Serialize(&payloadBuf)
			require.NoError(t, err)
			payload := payloadBuf.Bytes()

			// Create entry header
			header := EntryHeader{
				Type:           EntryReleaseSavepoint,
				Flags:          EntryFlagChecksum,
				Length:         uint32(len(payload)),
				SequenceNumber: 1,
			}

			// Serialize header
			var headerBuf bytes.Buffer
			err = header.Serialize(&headerBuf)
			require.NoError(t, err)
			assert.Equal(t, EntryHeaderSize, headerBuf.Len())

			// Calculate checksum
			checksum := header.CalculateChecksum(payload)
			assert.NotZero(t, checksum)

			// Deserialize entry
			deserialized := &ReleaseSavepointEntry{}
			err = deserialized.Deserialize(bytes.NewReader(payload))
			require.NoError(t, err)

			// Verify fields
			assert.Equal(t, tt.txnID, deserialized.TxnID())
			assert.Equal(t, tt.spName, deserialized.Name)
			assert.Equal(t, tt.timestamp.UnixNano(), deserialized.Timestamp)
		})
	}
}

// TestWALEntry_RollbackSavepointRoundTrip tests ROLLBACK TO SAVEPOINT entry serialization.
func TestWALEntry_RollbackSavepointRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		txnID     uint64
		spName    string
		undoIndex int
		timestamp time.Time
	}{
		{
			name:      "basic_rollback",
			txnID:     1,
			spName:    "sp1",
			undoIndex: 0,
			timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "rollback_with_undo_operations",
			txnID:     2,
			spName:    "my_savepoint",
			undoIndex: 5,
			timestamp: time.Date(2024, 12, 31, 23, 59, 59, 999999999, time.UTC),
		},
		{
			name:      "rollback_max_txn",
			txnID:     ^uint64(0), // max uint64
			spName:    "sp_max",
			undoIndex: 1000,
			timestamp: time.Now(),
		},
		{
			name:      "rollback_partial_undo",
			txnID:     42,
			spName:    "checkpoint_sp",
			undoIndex: 25,
			timestamp: time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create entry
			original := NewRollbackSavepointEntry(tt.txnID, tt.spName, tt.undoIndex, tt.timestamp)

			// Verify entry type
			assert.Equal(t, EntryRollbackSavepoint, original.Type())
			assert.Equal(t, tt.txnID, original.TxnID())

			// Serialize payload
			var payloadBuf bytes.Buffer
			err := original.Serialize(&payloadBuf)
			require.NoError(t, err)
			payload := payloadBuf.Bytes()

			// Create entry header
			header := EntryHeader{
				Type:           EntryRollbackSavepoint,
				Flags:          EntryFlagChecksum,
				Length:         uint32(len(payload)),
				SequenceNumber: 1,
			}

			// Serialize header
			var headerBuf bytes.Buffer
			err = header.Serialize(&headerBuf)
			require.NoError(t, err)
			assert.Equal(t, EntryHeaderSize, headerBuf.Len())

			// Calculate checksum
			checksum := header.CalculateChecksum(payload)
			assert.NotZero(t, checksum)

			// Deserialize entry
			deserialized := &RollbackSavepointEntry{}
			err = deserialized.Deserialize(bytes.NewReader(payload))
			require.NoError(t, err)

			// Verify fields
			assert.Equal(t, tt.txnID, deserialized.TxnID())
			assert.Equal(t, tt.spName, deserialized.Name)
			assert.Equal(t, tt.undoIndex, deserialized.UndoIndex)
			assert.Equal(t, tt.timestamp.UnixNano(), deserialized.Timestamp)
		})
	}
}

// TestWALEntry_SavepointEntryTypes tests that all savepoint entry types are correct.
func TestWALEntry_SavepointEntryTypes(t *testing.T) {
	timestamp := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	// Test EntrySavepoint
	spEntry := NewSavepointEntry(1, "sp1", 0, timestamp)
	assert.Equal(t, EntrySavepoint, spEntry.Type())
	assert.Equal(t, "SAVEPOINT", spEntry.Type().String())

	// Test EntryReleaseSavepoint
	releaseEntry := NewReleaseSavepointEntry(1, "sp1", timestamp)
	assert.Equal(t, EntryReleaseSavepoint, releaseEntry.Type())
	assert.Equal(t, "RELEASE_SAVEPOINT", releaseEntry.Type().String())

	// Test EntryRollbackSavepoint
	rollbackEntry := NewRollbackSavepointEntry(1, "sp1", 0, timestamp)
	assert.Equal(t, EntryRollbackSavepoint, rollbackEntry.Type())
	assert.Equal(t, "ROLLBACK_SAVEPOINT", rollbackEntry.Type().String())
}

// BenchmarkWALEntry_SavepointSerialize benchmarks SAVEPOINT entry serialization.
func BenchmarkWALEntry_SavepointSerialize(b *testing.B) {
	entry := NewSavepointEntry(
		1,
		"my_savepoint_name",
		10,
		time.Now(),
	)

	var buf bytes.Buffer
	buf.Grow(256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = entry.Serialize(&buf)
	}
}

// BenchmarkWALEntry_SavepointDeserialize benchmarks SAVEPOINT entry deserialization.
func BenchmarkWALEntry_SavepointDeserialize(b *testing.B) {
	entry := NewSavepointEntry(
		1,
		"my_savepoint_name",
		10,
		time.Now(),
	)

	var buf bytes.Buffer
	_ = entry.Serialize(&buf)
	data := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		var e SavepointEntry
		_ = e.Deserialize(reader)
	}
}

// BenchmarkWALEntry_FullRoundTrip benchmarks full serialization + deserialization.
func BenchmarkWALEntry_FullRoundTrip(b *testing.B) {
	entry := NewInsertEntry(
		1,
		"main",
		"users",
		[][]any{
			{int64(1), "Alice", int64(30)},
			{int64(2), "Bob", int64(25)},
			{int64(3), "Charlie", int64(35)},
		},
	)

	var buf bytes.Buffer
	buf.Grow(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Serialize
		buf.Reset()
		_ = entry.Serialize(&buf)
		payload := buf.Bytes()

		// Create header
		header := EntryHeader{
			Type:           EntryInsert,
			Flags:          EntryFlagChecksum,
			Length:         uint32(len(payload)),
			SequenceNumber: uint32(i),
		}

		// Calculate checksum
		_ = header.CalculateChecksum(payload)

		// Deserialize
		reader := bytes.NewReader(payload)
		var e InsertEntry
		_ = e.Deserialize(reader)
	}
}
