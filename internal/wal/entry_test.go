package wal

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntryType_String(t *testing.T) {
	tests := []struct {
		name      string
		entryType EntryType
		expected  string
	}{
		// Catalog operations
		{"CREATE_TABLE", EntryCreateTable, "CREATE_TABLE"},
		{"DROP_TABLE", EntryDropTable, "DROP_TABLE"},
		{"CREATE_SCHEMA", EntryCreateSchema, "CREATE_SCHEMA"},
		{"DROP_SCHEMA", EntryDropSchema, "DROP_SCHEMA"},
		{"CREATE_VIEW", EntryCreateView, "CREATE_VIEW"},
		{"DROP_VIEW", EntryDropView, "DROP_VIEW"},
		{"CREATE_SEQUENCE", EntryCreateSequence, "CREATE_SEQUENCE"},
		{"DROP_SEQUENCE", EntryDropSequence, "DROP_SEQUENCE"},
		{"SEQUENCE_VALUE", EntrySequenceValue, "SEQUENCE_VALUE"},
		{"CREATE_MACRO", EntryCreateMacro, "CREATE_MACRO"},
		{"DROP_MACRO", EntryDropMacro, "DROP_MACRO"},
		{"CREATE_TABLE_MACRO", EntryCreateTableMacro, "CREATE_TABLE_MACRO"},
		{"DROP_TABLE_MACRO", EntryDropTableMacro, "DROP_TABLE_MACRO"},
		{"CREATE_TYPE", EntryCreateType, "CREATE_TYPE"},
		{"DROP_TYPE", EntryDropType, "DROP_TYPE"},
		{"ALTER_INFO", EntryAlterInfo, "ALTER_INFO"},
		{"CREATE_INDEX", EntryCreateIndex, "CREATE_INDEX"},
		{"DROP_INDEX", EntryDropIndex, "DROP_INDEX"},

		// Data operations
		{"USE_TABLE", EntryUseTable, "USE_TABLE"},
		{"INSERT", EntryInsert, "INSERT"},
		{"DELETE", EntryDelete, "DELETE"},
		{"UPDATE", EntryUpdate, "UPDATE"},
		{"ROW_GROUP", EntryRowGroup, "ROW_GROUP"},

		// Transaction boundaries
		{"TXN_BEGIN", EntryTxnBegin, "TXN_BEGIN"},
		{"TXN_COMMIT", EntryTxnCommit, "TXN_COMMIT"},

		// Control entries
		{"VERSION", EntryVersion, "VERSION"},
		{"CHECKPOINT", EntryCheckpoint, "CHECKPOINT"},
		{"FLUSH", EntryFlush, "FLUSH"},

		// Unknown
		{"UNKNOWN", EntryType(999), "UNKNOWN(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.entryType.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEntryType_Values(t *testing.T) {
	// Verify entry type values match DuckDB specification
	assert.Equal(t, EntryType(1), EntryCreateTable)
	assert.Equal(t, EntryType(2), EntryDropTable)
	assert.Equal(t, EntryType(23), EntryCreateIndex)
	assert.Equal(t, EntryType(24), EntryDropIndex)
	assert.Equal(t, EntryType(25), EntryUseTable)
	assert.Equal(t, EntryType(26), EntryInsert)
	assert.Equal(t, EntryType(27), EntryDelete)
	assert.Equal(t, EntryType(28), EntryUpdate)
	assert.Equal(t, EntryType(90), EntryTxnBegin)
	assert.Equal(t, EntryType(91), EntryTxnCommit)
	assert.Equal(t, EntryType(98), EntryVersion)
	assert.Equal(t, EntryType(99), EntryCheckpoint)
	assert.Equal(t, EntryType(100), EntryFlush)
}

func TestEntryHeader_Serialize(t *testing.T) {
	tests := []struct {
		name   string
		header EntryHeader
	}{
		{
			name: "basic_entry",
			header: EntryHeader{
				Type:           EntryInsert,
				Flags:          EntryFlagNone,
				Length:         1024,
				SequenceNumber: 1,
			},
		},
		{
			name: "compressed_entry",
			header: EntryHeader{
				Type:           EntryUpdate,
				Flags:          EntryFlagCompressed,
				Length:         512,
				SequenceNumber: 100,
			},
		},
		{
			name: "checksum_flag",
			header: EntryHeader{
				Type:           EntryDelete,
				Flags:          EntryFlagChecksum,
				Length:         256,
				SequenceNumber: 1000,
			},
		},
		{
			name: "multiple_flags",
			header: EntryHeader{
				Type:           EntryCreateTable,
				Flags:          EntryFlagCompressed | EntryFlagChecksum,
				Length:         2048,
				SequenceNumber: 42,
			},
		},
		{
			name: "zero_length",
			header: EntryHeader{
				Type:           EntryCheckpoint,
				Flags:          EntryFlagNone,
				Length:         0,
				SequenceNumber: 5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			// Serialize
			err := tt.header.Serialize(&buf)
			require.NoError(t, err)

			// Verify size
			assert.Equal(
				t,
				EntryHeaderSize,
				buf.Len(),
				"serialized header should be exactly 16 bytes",
			)

			// Deserialize
			var deserialized EntryHeader
			err = deserialized.Deserialize(&buf)
			require.NoError(t, err)

			// Verify fields
			assert.Equal(t, tt.header.Type, deserialized.Type)
			assert.Equal(t, tt.header.Flags, deserialized.Flags)
			assert.Equal(t, tt.header.Length, deserialized.Length)
			assert.Equal(t, tt.header.SequenceNumber, deserialized.SequenceNumber)
		})
	}
}

func TestEntryHeader_Deserialize_Errors(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		expectErr bool
	}{
		{
			name:      "empty_buffer",
			data:      []byte{},
			expectErr: true,
		},
		{
			name:      "partial_header_1byte",
			data:      []byte{0x01},
			expectErr: true,
		},
		{
			name:      "partial_header_8bytes",
			data:      []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectErr: true,
		},
		{
			name: "partial_header_15bytes",
			data: []byte{
				0x01,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
				0x00,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.data)
			var header EntryHeader
			err := header.Deserialize(buf)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEntryHeader_CalculateChecksum(t *testing.T) {
	tests := []struct {
		name    string
		header  EntryHeader
		payload []byte
	}{
		{
			name: "empty_payload",
			header: EntryHeader{
				Type:           EntryInsert,
				Flags:          EntryFlagNone,
				Length:         0,
				SequenceNumber: 1,
			},
			payload: []byte{},
		},
		{
			name: "small_payload",
			header: EntryHeader{
				Type:           EntryUpdate,
				Flags:          EntryFlagChecksum,
				Length:         4,
				SequenceNumber: 10,
			},
			payload: []byte{0x01, 0x02, 0x03, 0x04},
		},
		{
			name: "large_payload",
			header: EntryHeader{
				Type:           EntryDelete,
				Flags:          EntryFlagCompressed,
				Length:         1024,
				SequenceNumber: 100,
			},
			payload: make([]byte, 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate checksum twice - should be deterministic
			checksum1 := tt.header.CalculateChecksum(tt.payload)
			checksum2 := tt.header.CalculateChecksum(tt.payload)
			assert.Equal(t, checksum1, checksum2, "checksum should be deterministic")

			// Modify payload - checksum should change
			if len(tt.payload) > 0 {
				modifiedPayload := make([]byte, len(tt.payload))
				copy(modifiedPayload, tt.payload)
				modifiedPayload[0] ^= 0xFF
				checksum3 := tt.header.CalculateChecksum(modifiedPayload)
				assert.NotEqual(
					t,
					checksum1,
					checksum3,
					"checksum should change with different payload",
				)
			}

			// Modify header - checksum should change
			modifiedHeader := tt.header
			modifiedHeader.SequenceNumber++
			checksum4 := modifiedHeader.CalculateChecksum(tt.payload)
			assert.NotEqual(t, checksum1, checksum4, "checksum should change with different header")
		})
	}
}

func TestEntryHeader_RoundTrip(t *testing.T) {
	// Test multiple round-trips
	for i := 0; i < 100; i++ {
		header := EntryHeader{
			Type:           EntryType(i % 100),
			Flags:          uint32(i % 4),
			Length:         uint32(i * 10),
			SequenceNumber: uint32(i),
		}

		var buf bytes.Buffer
		err := header.Serialize(&buf)
		require.NoError(t, err)

		var deserialized EntryHeader
		err = deserialized.Deserialize(&buf)
		require.NoError(t, err)

		assert.Equal(t, header.Type, deserialized.Type)
		assert.Equal(t, header.Flags, deserialized.Flags)
		assert.Equal(t, header.Length, deserialized.Length)
		assert.Equal(t, header.SequenceNumber, deserialized.SequenceNumber)
	}
}

func TestEntryHeaderSize(t *testing.T) {
	// Verify the constant matches the struct size
	assert.Equal(t, 16, EntryHeaderSize, "EntryHeaderSize should be 16 bytes")

	// Verify by serialization
	header := EntryHeader{
		Type:           EntryInsert,
		Flags:          EntryFlagNone,
		Length:         100,
		SequenceNumber: 1,
	}

	var buf bytes.Buffer
	err := header.Serialize(&buf)
	require.NoError(t, err)
	assert.Equal(t, EntryHeaderSize, buf.Len())
}

func TestEntryFlags(t *testing.T) {
	tests := []struct {
		name     string
		flags    uint32
		expected string
	}{
		{"none", EntryFlagNone, "no flags"},
		{"compressed", EntryFlagCompressed, "compressed"},
		{"checksum", EntryFlagChecksum, "checksum"},
		{"both", EntryFlagCompressed | EntryFlagChecksum, "compressed and checksum"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := EntryHeader{
				Type:           EntryInsert,
				Flags:          tt.flags,
				Length:         100,
				SequenceNumber: 1,
			}

			// Verify flags can be set and read
			assert.Equal(t, tt.flags, header.Flags)

			// Verify individual flag checks
			if tt.flags&EntryFlagCompressed != 0 {
				assert.NotEqual(t, uint32(0), header.Flags&EntryFlagCompressed)
			}
			if tt.flags&EntryFlagChecksum != 0 {
				assert.NotEqual(t, uint32(0), header.Flags&EntryFlagChecksum)
			}
		})
	}
}

func BenchmarkEntryHeader_Serialize(b *testing.B) {
	header := EntryHeader{
		Type:           EntryInsert,
		Flags:          EntryFlagCompressed,
		Length:         1024,
		SequenceNumber: 42,
	}

	var buf bytes.Buffer
	buf.Grow(EntryHeaderSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = header.Serialize(&buf)
	}
}

func BenchmarkEntryHeader_Deserialize(b *testing.B) {
	header := EntryHeader{
		Type:           EntryInsert,
		Flags:          EntryFlagCompressed,
		Length:         1024,
		SequenceNumber: 42,
	}

	var buf bytes.Buffer
	_ = header.Serialize(&buf)
	data := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		var h EntryHeader
		_ = h.Deserialize(reader)
	}
}

func BenchmarkEntryHeader_CalculateChecksum(b *testing.B) {
	header := EntryHeader{
		Type:           EntryInsert,
		Flags:          EntryFlagChecksum,
		Length:         1024,
		SequenceNumber: 42,
	}
	payload := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = header.CalculateChecksum(payload)
	}
}
