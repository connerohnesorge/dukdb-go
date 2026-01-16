package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// LSN TESTS
// =============================================================================

func TestLSNString(t *testing.T) {
	tests := []struct {
		name     string
		lsn      LSN
		expected string
	}{
		{
			name:     "zero LSN",
			lsn:      LSN(0),
			expected: "0/0",
		},
		{
			name:     "simple LSN",
			lsn:      LSN(0x100000001),
			expected: "1/1",
		},
		{
			name:     "high value LSN",
			lsn:      LSN(0xDEADBEEF12345678),
			expected: "DEADBEEF/12345678",
		},
		{
			name:     "max high",
			lsn:      LSN(0xFFFFFFFF00000000),
			expected: "FFFFFFFF/0",
		},
		{
			name:     "max low",
			lsn:      LSN(0x00000000FFFFFFFF),
			expected: "0/FFFFFFFF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.lsn.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseLSN(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  LSN
		expectErr bool
	}{
		{
			name:     "zero LSN",
			input:    "0/0",
			expected: LSN(0),
		},
		{
			name:     "simple LSN",
			input:    "1/1",
			expected: LSN(0x100000001),
		},
		{
			name:     "uppercase hex",
			input:    "DEADBEEF/12345678",
			expected: LSN(0xDEADBEEF12345678),
		},
		{
			name:     "lowercase hex",
			input:    "deadbeef/12345678",
			expected: LSN(0xDEADBEEF12345678),
		},
		{
			name:      "invalid format - no slash",
			input:     "12345678",
			expectErr: true,
		},
		{
			name:      "invalid format - multiple slashes",
			input:     "1/2/3",
			expectErr: true,
		},
		{
			name:      "invalid hex - high",
			input:     "GHIJ/1234",
			expectErr: true,
		},
		{
			name:      "invalid hex - low",
			input:     "1234/GHIJ",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseLSN(tt.input)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestLSNRoundTrip(t *testing.T) {
	testCases := []LSN{
		LSN(0),
		LSN(1),
		LSN(0x100000001),
		LSN(0xDEADBEEF12345678),
		LSN(0xFFFFFFFFFFFFFFFF),
	}

	for _, original := range testCases {
		t.Run(original.String(), func(t *testing.T) {
			str := original.String()
			parsed, err := ParseLSN(str)
			require.NoError(t, err)
			assert.Equal(t, original, parsed)
		})
	}
}

// =============================================================================
// REPLICATION SLOT MANAGER TESTS
// =============================================================================

func TestInMemorySlotManager_CreateSlot(t *testing.T) {
	ctx := context.Background()
	manager := NewInMemorySlotManager()

	// Create a slot
	slot, err := manager.CreateSlot(ctx, "test_slot", "pgoutput", false)
	require.NoError(t, err)
	assert.Equal(t, "test_slot", slot.Name)
	assert.Equal(t, "pgoutput", slot.Plugin)
	assert.Equal(t, SlotTypeLogical, slot.SlotType)
	assert.False(t, slot.Active)
	assert.NotEqual(t, LSN(0), slot.RestartLSN)

	// Cannot create duplicate
	_, err = manager.CreateSlot(ctx, "test_slot", "pgoutput", false)
	assert.Equal(t, ErrSlotAlreadyExists, err)

	// Can create another slot with different name
	slot2, err := manager.CreateSlot(ctx, "test_slot_2", "wal2json", true)
	require.NoError(t, err)
	assert.Equal(t, "test_slot_2", slot2.Name)
	assert.Equal(t, "wal2json", slot2.Plugin)
}

func TestInMemorySlotManager_DropSlot(t *testing.T) {
	ctx := context.Background()
	manager := NewInMemorySlotManager()

	// Create a slot
	_, err := manager.CreateSlot(ctx, "test_slot", "pgoutput", false)
	require.NoError(t, err)

	// Drop it
	err = manager.DropSlot(ctx, "test_slot")
	require.NoError(t, err)

	// Cannot drop again
	err = manager.DropSlot(ctx, "test_slot")
	assert.Equal(t, ErrSlotNotFound, err)

	// Cannot drop non-existent
	err = manager.DropSlot(ctx, "nonexistent")
	assert.Equal(t, ErrSlotNotFound, err)
}

func TestInMemorySlotManager_DropActiveSlot(t *testing.T) {
	ctx := context.Background()
	manager := NewInMemorySlotManager()

	// Create and acquire a slot
	_, err := manager.CreateSlot(ctx, "active_slot", "pgoutput", false)
	require.NoError(t, err)

	err = manager.AcquireSlot(ctx, "active_slot", 123)
	require.NoError(t, err)

	// Cannot drop active slot
	err = manager.DropSlot(ctx, "active_slot")
	assert.Equal(t, ErrSlotInUse, err)

	// Release and then drop
	err = manager.ReleaseSlot(ctx, "active_slot", 123)
	require.NoError(t, err)

	err = manager.DropSlot(ctx, "active_slot")
	require.NoError(t, err)
}

func TestInMemorySlotManager_GetSlot(t *testing.T) {
	ctx := context.Background()
	manager := NewInMemorySlotManager()

	// Create a slot
	created, err := manager.CreateSlot(ctx, "test_slot", "pgoutput", false)
	require.NoError(t, err)

	// Get it
	retrieved, err := manager.GetSlot(ctx, "test_slot")
	require.NoError(t, err)
	assert.Equal(t, created.Name, retrieved.Name)
	assert.Equal(t, created.Plugin, retrieved.Plugin)
	assert.Equal(t, created.RestartLSN, retrieved.RestartLSN)

	// Cannot get non-existent
	_, err = manager.GetSlot(ctx, "nonexistent")
	assert.Equal(t, ErrSlotNotFound, err)
}

func TestInMemorySlotManager_ListSlots(t *testing.T) {
	ctx := context.Background()
	manager := NewInMemorySlotManager()

	// Empty list
	slots, err := manager.ListSlots(ctx)
	require.NoError(t, err)
	assert.Empty(t, slots)

	// Create some slots
	_, err = manager.CreateSlot(ctx, "slot1", "pgoutput", false)
	require.NoError(t, err)
	_, err = manager.CreateSlot(ctx, "slot2", "wal2json", false)
	require.NoError(t, err)

	// List should have 2 slots
	slots, err = manager.ListSlots(ctx)
	require.NoError(t, err)
	assert.Len(t, slots, 2)

	// Verify slot names
	names := make(map[string]bool)
	for _, s := range slots {
		names[s.Name] = true
	}
	assert.True(t, names["slot1"])
	assert.True(t, names["slot2"])
}

func TestInMemorySlotManager_AdvanceSlot(t *testing.T) {
	ctx := context.Background()
	manager := NewInMemorySlotManager()

	// Create a slot
	slot, err := manager.CreateSlot(ctx, "test_slot", "pgoutput", false)
	require.NoError(t, err)

	originalLSN := slot.RestartLSN

	// Advance to a higher LSN
	newLSN := originalLSN + 1000
	err = manager.AdvanceSlot(ctx, "test_slot", newLSN)
	require.NoError(t, err)

	// Verify advancement
	updated, err := manager.GetSlot(ctx, "test_slot")
	require.NoError(t, err)
	assert.Equal(t, newLSN, updated.RestartLSN)

	// Cannot advance to lower LSN
	err = manager.AdvanceSlot(ctx, "test_slot", originalLSN)
	require.NoError(t, err) // No error, just doesn't move backward

	updated, err = manager.GetSlot(ctx, "test_slot")
	require.NoError(t, err)
	assert.Equal(t, newLSN, updated.RestartLSN) // Still at higher LSN
}

func TestInMemorySlotManager_AcquireRelease(t *testing.T) {
	ctx := context.Background()
	manager := NewInMemorySlotManager()

	// Create a slot
	_, err := manager.CreateSlot(ctx, "test_slot", "pgoutput", false)
	require.NoError(t, err)

	// Acquire it
	err = manager.AcquireSlot(ctx, "test_slot", 123)
	require.NoError(t, err)

	// Verify it's active
	slot, err := manager.GetSlot(ctx, "test_slot")
	require.NoError(t, err)
	assert.True(t, slot.Active)

	// Cannot acquire again
	err = manager.AcquireSlot(ctx, "test_slot", 456)
	assert.Equal(t, ErrSlotInUse, err)

	// Different session cannot release
	err = manager.ReleaseSlot(ctx, "test_slot", 456)
	assert.Equal(t, ErrSlotInUse, err)

	// Same session can release
	err = manager.ReleaseSlot(ctx, "test_slot", 123)
	require.NoError(t, err)

	// Verify it's not active
	slot, err = manager.GetSlot(ctx, "test_slot")
	require.NoError(t, err)
	assert.False(t, slot.Active)
}

func TestInMemorySlotManager_UpdateConfirmedFlushLSN(t *testing.T) {
	ctx := context.Background()
	manager := NewInMemorySlotManager()

	// Create a slot
	slot, err := manager.CreateSlot(ctx, "test_slot", "pgoutput", false)
	require.NoError(t, err)

	originalLSN := slot.ConfirmedFlushLSN

	// Update confirmed flush
	newLSN := originalLSN + 500
	err = manager.UpdateConfirmedFlushLSN(ctx, "test_slot", newLSN)
	require.NoError(t, err)

	// Verify
	updated, err := manager.GetSlot(ctx, "test_slot")
	require.NoError(t, err)
	assert.Equal(t, newLSN, updated.ConfirmedFlushLSN)
}

func TestInMemorySlotManager_TemporarySlots(t *testing.T) {
	ctx := context.Background()
	manager := NewInMemorySlotManager()

	// Create temporary slots
	_, err := manager.CreateSlot(ctx, "temp_slot1", "pgoutput", true)
	require.NoError(t, err)
	_, err = manager.CreateSlot(ctx, "temp_slot2", "pgoutput", true)
	require.NoError(t, err)

	// Acquire slots for session 123
	err = manager.AcquireSlot(ctx, "temp_slot1", 123)
	require.NoError(t, err)
	err = manager.AcquireSlot(ctx, "temp_slot2", 456)
	require.NoError(t, err)

	// Cleanup session 123's temporary slots
	manager.CleanupTemporarySlots(ctx, 123)

	// temp_slot1 should be gone
	_, err = manager.GetSlot(ctx, "temp_slot1")
	assert.Equal(t, ErrSlotNotFound, err)

	// temp_slot2 should still exist (different session)
	_, err = manager.GetSlot(ctx, "temp_slot2")
	require.NoError(t, err)
}

// =============================================================================
// PUBLICATION MANAGER TESTS
// =============================================================================

func TestPublicationManager_CreatePublication(t *testing.T) {
	pm := NewPublicationManager()

	// Create publication for specific tables
	err := pm.CreatePublication("pub1", false, []string{"table1", "table2"})
	require.NoError(t, err)

	pub, err := pm.GetPublication("pub1")
	require.NoError(t, err)
	assert.Equal(t, "pub1", pub.Name)
	assert.False(t, pub.AllTables)
	assert.Equal(t, []string{"table1", "table2"}, pub.Tables)
	assert.True(t, pub.PublishInserts)
	assert.True(t, pub.PublishUpdates)
	assert.True(t, pub.PublishDeletes)

	// Create publication for all tables
	err = pm.CreatePublication("pub2", true, nil)
	require.NoError(t, err)

	pub2, err := pm.GetPublication("pub2")
	require.NoError(t, err)
	assert.True(t, pub2.AllTables)

	// Cannot create duplicate
	err = pm.CreatePublication("pub1", false, nil)
	assert.Error(t, err)
}

func TestPublicationManager_DropPublication(t *testing.T) {
	pm := NewPublicationManager()

	// Create and drop
	err := pm.CreatePublication("test_pub", false, []string{"t1"})
	require.NoError(t, err)

	err = pm.DropPublication("test_pub")
	require.NoError(t, err)

	// Cannot get after drop
	_, err = pm.GetPublication("test_pub")
	assert.Error(t, err)

	// Cannot drop non-existent
	err = pm.DropPublication("nonexistent")
	assert.Error(t, err)
}

func TestPublicationManager_AlterPublication(t *testing.T) {
	pm := NewPublicationManager()

	// Create publication
	err := pm.CreatePublication("test_pub", false, []string{"table1"})
	require.NoError(t, err)

	// Add tables
	err = pm.AlterPublication("test_pub", []string{"table2", "table3"}, nil, nil)
	require.NoError(t, err)

	pub, err := pm.GetPublication("test_pub")
	require.NoError(t, err)
	assert.Contains(t, pub.Tables, "table1")
	assert.Contains(t, pub.Tables, "table2")
	assert.Contains(t, pub.Tables, "table3")

	// Drop table
	err = pm.AlterPublication("test_pub", nil, []string{"table2"}, nil)
	require.NoError(t, err)

	pub, err = pm.GetPublication("test_pub")
	require.NoError(t, err)
	assert.NotContains(t, pub.Tables, "table2")
	assert.Contains(t, pub.Tables, "table1")
	assert.Contains(t, pub.Tables, "table3")

	// Set all tables
	allTables := true
	err = pm.AlterPublication("test_pub", nil, nil, &allTables)
	require.NoError(t, err)

	pub, err = pm.GetPublication("test_pub")
	require.NoError(t, err)
	assert.True(t, pub.AllTables)
}

func TestPublicationManager_ListPublications(t *testing.T) {
	pm := NewPublicationManager()

	// Empty list
	pubs := pm.ListPublications()
	assert.Empty(t, pubs)

	// Create some publications
	err := pm.CreatePublication("pub1", false, []string{"t1"})
	require.NoError(t, err)
	err = pm.CreatePublication("pub2", true, nil)
	require.NoError(t, err)

	// List should have 2
	pubs = pm.ListPublications()
	assert.Len(t, pubs, 2)
}

func TestPublicationManager_ShouldReplicate(t *testing.T) {
	pm := NewPublicationManager()

	// No publications - nothing should replicate
	change := &ChangeRecord{
		Type:   ChangeInsert,
		Schema: "public",
		Table:  "users",
	}
	assert.False(t, pm.ShouldReplicate(change))

	// Create publication for specific table
	err := pm.CreatePublication("pub1", false, []string{"users", "public.orders"})
	require.NoError(t, err)

	// Should replicate users table
	assert.True(t, pm.ShouldReplicate(&ChangeRecord{
		Type:   ChangeInsert,
		Schema: "public",
		Table:  "users",
	}))

	// Should replicate orders table (qualified name)
	assert.True(t, pm.ShouldReplicate(&ChangeRecord{
		Type:   ChangeUpdate,
		Schema: "public",
		Table:  "orders",
	}))

	// Should NOT replicate products table
	assert.False(t, pm.ShouldReplicate(&ChangeRecord{
		Type:   ChangeDelete,
		Schema: "public",
		Table:  "products",
	}))

	// Create all-tables publication
	err = pm.CreatePublication("pub_all", true, nil)
	require.NoError(t, err)

	// Now products should replicate
	assert.True(t, pm.ShouldReplicate(&ChangeRecord{
		Type:   ChangeInsert,
		Schema: "public",
		Table:  "products",
	}))
}

// =============================================================================
// PGOUTPUT PLUGIN TESTS
// =============================================================================

func TestPgOutputPlugin_Name(t *testing.T) {
	plugin := NewPgOutputPlugin(nil)
	assert.Equal(t, "pgoutput", plugin.Name())
}

func TestPgOutputPlugin_StartupShutdown(t *testing.T) {
	ctx := context.Background()
	plugin := NewPgOutputPlugin(nil)

	err := plugin.Startup(ctx, map[string]string{
		"proto_version":     "1",
		"publication_names": "pub1,pub2",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, plugin.protoVersion)
	assert.Equal(t, []string{"pub1", "pub2"}, plugin.pubNames)

	err = plugin.Shutdown(ctx)
	require.NoError(t, err)
}

func TestPgOutputPlugin_BeginTransaction(t *testing.T) {
	plugin := NewPgOutputPlugin(nil)

	data, err := plugin.BeginTransaction(12345, time.Now(), LSN(100))
	require.NoError(t, err)
	assert.NotEmpty(t, data)
	assert.Equal(t, byte('B'), data[0]) // Begin message type
}

func TestPgOutputPlugin_CommitTransaction(t *testing.T) {
	plugin := NewPgOutputPlugin(nil)

	data, err := plugin.CommitTransaction(12345, time.Now(), LSN(200))
	require.NoError(t, err)
	assert.NotEmpty(t, data)
	assert.Equal(t, byte('C'), data[0]) // Commit message type
}

func TestPgOutputPlugin_Insert(t *testing.T) {
	plugin := NewPgOutputPlugin(nil)

	row := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	data, err := plugin.Insert("public.users", row)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Should contain both Relation ('R') and Insert ('I') messages
	assert.Equal(t, byte('R'), data[0]) // Relation message first
	// Find Insert message
	hasInsert := false
	for i := 0; i < len(data); i++ {
		if data[i] == 'I' {
			hasInsert = true
			break
		}
	}
	assert.True(t, hasInsert)
}

func TestPgOutputPlugin_Update(t *testing.T) {
	plugin := NewPgOutputPlugin(nil)

	oldRow := map[string]interface{}{
		"id":   1,
		"name": "old",
	}
	newRow := map[string]interface{}{
		"id":   1,
		"name": "new",
	}

	data, err := plugin.Update("public.users", oldRow, newRow)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Should contain both Relation ('R') and Update ('U') messages
	hasUpdate := false
	for i := 0; i < len(data); i++ {
		if data[i] == 'U' {
			hasUpdate = true
			break
		}
	}
	assert.True(t, hasUpdate)
}

func TestPgOutputPlugin_Delete(t *testing.T) {
	plugin := NewPgOutputPlugin(nil)

	oldRow := map[string]interface{}{
		"id":   1,
		"name": "deleted",
	}

	data, err := plugin.Delete("public.users", oldRow)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Should contain both Relation ('R') and Delete ('D') messages
	hasDelete := false
	for i := 0; i < len(data); i++ {
		if data[i] == 'D' {
			hasDelete = true
			break
		}
	}
	assert.True(t, hasDelete)
}

// =============================================================================
// WIRE PROTOCOL MESSAGE TESTS
// =============================================================================

func TestXLogData_Encode(t *testing.T) {
	msg := &XLogData{
		WALStart:   LSN(100),
		WALEnd:     LSN(200),
		ServerTime: time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
		Data:       []byte("test data"),
	}

	encoded := msg.Encode()
	assert.NotEmpty(t, encoded)
	assert.Equal(t, byte('w'), encoded[0]) // XLogData message type
	assert.Len(t, encoded, 1+8+8+8+len(msg.Data))
}

func TestPrimaryKeepalive_Encode(t *testing.T) {
	msg := &PrimaryKeepalive{
		WALEnd:         LSN(500),
		ServerTime:     time.Now(),
		ReplyRequested: true,
	}

	encoded := msg.Encode()
	assert.NotEmpty(t, encoded)
	assert.Equal(t, byte('k'), encoded[0]) // PrimaryKeepalive message type
	assert.Len(t, encoded, 1+8+8+1)
	assert.Equal(t, byte(1), encoded[17]) // Reply requested = true

	msg.ReplyRequested = false
	encoded = msg.Encode()
	assert.Equal(t, byte(0), encoded[17]) // Reply requested = false
}

func TestDecodeStandbyStatusUpdate(t *testing.T) {
	// Build a valid status update message
	data := make([]byte, 33)
	// Write position = 100
	data[0] = 0
	data[1] = 0
	data[2] = 0
	data[3] = 0
	data[4] = 0
	data[5] = 0
	data[6] = 0
	data[7] = 100
	// Flush position = 90
	data[8] = 0
	data[9] = 0
	data[10] = 0
	data[11] = 0
	data[12] = 0
	data[13] = 0
	data[14] = 0
	data[15] = 90
	// Apply position = 80
	data[16] = 0
	data[17] = 0
	data[18] = 0
	data[19] = 0
	data[20] = 0
	data[21] = 0
	data[22] = 0
	data[23] = 80
	// Client time (zeros)
	// Reply requested = true
	data[32] = 1

	msg, err := DecodeStandbyStatusUpdate(data)
	require.NoError(t, err)
	assert.Equal(t, LSN(100), msg.WritePosition)
	assert.Equal(t, LSN(90), msg.FlushPosition)
	assert.Equal(t, LSN(80), msg.ApplyPosition)
	assert.True(t, msg.ReplyRequested)
}

func TestDecodeStandbyStatusUpdate_InvalidLength(t *testing.T) {
	data := make([]byte, 10) // Too short
	_, err := DecodeStandbyStatusUpdate(data)
	assert.Error(t, err)
}

// =============================================================================
// REPLICATION HANDLER TESTS
// =============================================================================

func TestReplicationHandler_HandleIdentifySystem(t *testing.T) {
	handler := NewReplicationHandler()
	ctx := context.Background()

	systemID, timeline, xlogPos, dbName := handler.HandleIdentifySystem(ctx)

	assert.NotEmpty(t, systemID)
	assert.Equal(t, int32(1), timeline)
	assert.NotEqual(t, LSN(0), xlogPos)
	assert.Equal(t, "dukdb", dbName)
}

func TestReplicationHandler_CreateDropSlot(t *testing.T) {
	handler := NewReplicationHandler()
	ctx := context.Background()

	// Create slot
	slot, err := handler.HandleCreateReplicationSlot(ctx, "test_slot", "pgoutput", false)
	require.NoError(t, err)
	assert.Equal(t, "test_slot", slot.Name)

	// Drop slot
	err = handler.HandleDropReplicationSlot(ctx, "test_slot")
	require.NoError(t, err)

	// Verify gone
	_, err = handler.SlotManager().GetSlot(ctx, "test_slot")
	assert.Equal(t, ErrSlotNotFound, err)
}

func TestReplicationHandler_PublicationOperations(t *testing.T) {
	handler := NewReplicationHandler()

	// Create publication
	err := handler.CreatePublication("test_pub", false, []string{"table1"})
	require.NoError(t, err)

	// Get publication
	pub, err := handler.GetPublication("test_pub")
	require.NoError(t, err)
	assert.Equal(t, "test_pub", pub.Name)

	// List publications
	pubs := handler.ListPublications()
	assert.Len(t, pubs, 1)

	// Alter publication
	err = handler.AlterPublication("test_pub", []string{"table2"}, nil, nil)
	require.NoError(t, err)

	pub, _ = handler.GetPublication("test_pub")
	assert.Contains(t, pub.Tables, "table2")

	// Drop publication
	err = handler.DropPublication("test_pub")
	require.NoError(t, err)

	pubs = handler.ListPublications()
	assert.Empty(t, pubs)
}

// =============================================================================
// REPLICATION MANAGER TESTS (UPDATED)
// =============================================================================

func TestReplicationManager_IsSupported(t *testing.T) {
	rm := NewReplicationManager()
	assert.True(t, rm.IsReplicationSupported())
}

func TestReplicationManager_CreatePublication(t *testing.T) {
	rm := NewReplicationManager()

	err := rm.CreatePublication("test_pub", false, []string{"t1", "t2"})
	require.NoError(t, err)

	pub, err := rm.GetPublication("test_pub")
	require.NoError(t, err)
	assert.Equal(t, "test_pub", pub.Name)
	assert.Equal(t, []string{"t1", "t2"}, pub.Tables)

	// Duplicate should fail
	err = rm.CreatePublication("test_pub", false, nil)
	assert.Error(t, err)
}

func TestReplicationManager_DropPublication(t *testing.T) {
	rm := NewReplicationManager()

	err := rm.CreatePublication("test_pub", true, nil)
	require.NoError(t, err)

	err = rm.DropPublication("test_pub")
	require.NoError(t, err)

	_, err = rm.GetPublication("test_pub")
	assert.Error(t, err)

	// Drop non-existent should fail
	err = rm.DropPublication("nonexistent")
	assert.Error(t, err)
}

func TestReplicationManager_ListPublications(t *testing.T) {
	rm := NewReplicationManager()

	_ = rm.CreatePublication("pub1", false, []string{"t1"})
	_ = rm.CreatePublication("pub2", true, nil)

	pubs := rm.ListPublications()
	assert.Len(t, pubs, 2)
}

// =============================================================================
// TIMESTAMP CONVERSION TESTS
// =============================================================================

func TestPgTimestamp(t *testing.T) {
	// PostgreSQL epoch
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	// At epoch, timestamp should be 0
	assert.Equal(t, uint64(0), pgTimestamp(pgEpoch))

	// 1 second after epoch
	oneSecAfter := pgEpoch.Add(time.Second)
	assert.Equal(t, uint64(1000000), pgTimestamp(oneSecAfter)) // 1 million microseconds

	// 1 day after epoch
	oneDayAfter := pgEpoch.Add(24 * time.Hour)
	assert.Equal(t, uint64(86400000000), pgTimestamp(oneDayAfter)) // 86400 seconds * 1000000
}

func TestPgTimestampRoundTrip(t *testing.T) {
	testTimes := []time.Time{
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 6, 15, 12, 30, 45, 0, time.UTC),
		time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
	}

	for _, original := range testTimes {
		t.Run(original.Format(time.RFC3339), func(t *testing.T) {
			ts := pgTimestamp(original)
			recovered := pgTimestampToTime(ts)
			// Compare with second precision since we lose nanoseconds
			assert.Equal(t, original.Unix(), recovered.Unix())
		})
	}
}

// =============================================================================
// CHANGE TYPE TESTS
// =============================================================================

func TestChangeType_String(t *testing.T) {
	assert.Equal(t, "INSERT", ChangeInsert.String())
	assert.Equal(t, "UPDATE", ChangeUpdate.String())
	assert.Equal(t, "DELETE", ChangeDelete.String())
	assert.Equal(t, "UNKNOWN", ChangeType(99).String())
}

// =============================================================================
// DEBEZIUM COMPATIBILITY TESTS
// =============================================================================

// TestDebeziumCompatibility_MessageFormat tests that the wire protocol messages
// are compatible with what Debezium expects.
func TestDebeziumCompatibility_MessageFormat(t *testing.T) {
	// Debezium expects XLogData messages with type 'w'
	xlog := &XLogData{
		WALStart:   LSN(0x100000010),
		WALEnd:     LSN(0x100000020),
		ServerTime: time.Now(),
		Data:       []byte("change data"),
	}
	encoded := xlog.Encode()
	assert.Equal(t, byte('w'), encoded[0], "XLogData should start with 'w'")

	// Debezium sends StandbyStatusUpdate with type 'r' (but we decode without the type byte)
	// Just verify our decode works with proper data
	statusData := make([]byte, 33)
	_, err := DecodeStandbyStatusUpdate(statusData)
	assert.NoError(t, err)

	// Debezium expects PrimaryKeepalive messages with type 'k'
	keepalive := &PrimaryKeepalive{
		WALEnd:         LSN(0x100000030),
		ServerTime:     time.Now(),
		ReplyRequested: true,
	}
	kaEncoded := keepalive.Encode()
	assert.Equal(t, byte('k'), kaEncoded[0], "PrimaryKeepalive should start with 'k'")
}

// TestDebeziumCompatibility_PgOutputFormat tests pgoutput message formats
func TestDebeziumCompatibility_PgOutputFormat(t *testing.T) {
	plugin := NewPgOutputPlugin(nil)

	// Test BEGIN message format
	beginData, err := plugin.BeginTransaction(1, time.Now(), LSN(100))
	require.NoError(t, err)
	assert.Equal(t, byte('B'), beginData[0], "BEGIN should start with 'B'")

	// Test COMMIT message format
	commitData, err := plugin.CommitTransaction(1, time.Now(), LSN(200))
	require.NoError(t, err)
	assert.Equal(t, byte('C'), commitData[0], "COMMIT should start with 'C'")

	// Test that Relation messages start with 'R' and are followed by Insert ('I')
	insertData, err := plugin.Insert("public.test", map[string]interface{}{"id": 1})
	require.NoError(t, err)
	assert.Equal(t, byte('R'), insertData[0], "First message should be Relation ('R')")

	// Find the Insert message
	foundInsert := false
	for i := range insertData {
		if insertData[i] == 'I' {
			foundInsert = true
			break
		}
	}
	assert.True(t, foundInsert, "Should contain Insert message ('I')")
}

// TestDebeziumCompatibility_SlotManagement tests slot operations needed for Debezium
func TestDebeziumCompatibility_SlotManagement(t *testing.T) {
	ctx := context.Background()
	handler := NewReplicationHandler()

	// Debezium creates a logical replication slot with pgoutput
	slot, err := handler.HandleCreateReplicationSlot(ctx, "debezium_slot", "pgoutput", false)
	require.NoError(t, err)
	assert.Equal(t, "debezium_slot", slot.Name)
	assert.Equal(t, "pgoutput", slot.Plugin)
	assert.Equal(t, SlotTypeLogical, slot.SlotType)

	// Slot should have initial LSN
	assert.NotEqual(t, InvalidLSN, slot.RestartLSN)
	assert.NotEqual(t, InvalidLSN, slot.ConfirmedFlushLSN)

	// Debezium needs to query slot information
	slotInfo, err := handler.SlotManager().GetSlot(ctx, "debezium_slot")
	require.NoError(t, err)
	assert.Equal(t, "dukdb", slotInfo.Database)

	// Cleanup
	err = handler.HandleDropReplicationSlot(ctx, "debezium_slot")
	require.NoError(t, err)
}

// TestDebeziumCompatibility_PublicationSetup tests publication setup for Debezium
func TestDebeziumCompatibility_PublicationSetup(t *testing.T) {
	handler := NewReplicationHandler()

	// Debezium creates a publication for all tables by default
	err := handler.CreatePublication("dbz_publication", true, nil)
	require.NoError(t, err)

	pub, err := handler.GetPublication("dbz_publication")
	require.NoError(t, err)
	assert.True(t, pub.AllTables)
	assert.True(t, pub.PublishInserts)
	assert.True(t, pub.PublishUpdates)
	assert.True(t, pub.PublishDeletes)

	// Should replicate any table change
	pm := handler.PublicationManager()
	assert.True(t, pm.ShouldReplicate(&ChangeRecord{
		Type:   ChangeInsert,
		Schema: "public",
		Table:  "any_table",
	}))

	// Debezium can also use specific tables
	err = handler.CreatePublication("dbz_specific", false, []string{"customers", "orders"})
	require.NoError(t, err)

	// Only specified tables should replicate
	assert.True(t, pm.ShouldReplicate(&ChangeRecord{
		Type:  ChangeUpdate,
		Table: "customers",
	}))
}
