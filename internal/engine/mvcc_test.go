package engine

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/coder/quartz"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// MVCCManager Tests
// =============================================================================

// TestMVCCManager_NewMVCCManager tests that the constructor creates a manager with the provided clock.
func TestMVCCManager_NewMVCCManager(t *testing.T) {
	mockClock := quartz.NewMock(t)

	manager := NewMVCCManager(mockClock)

	require.NotNil(t, manager)
	assert.NotNil(t, manager.clock)
	assert.NotNil(t, manager.activeTxns)
	assert.Equal(t, uint64(0), manager.lastTS)
	assert.Empty(t, manager.activeTxns)
}

// TestMVCCManager_NextTimestamp tests that timestamps are monotonically increasing.
func TestMVCCManager_NextTimestamp(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	// Set initial time
	initialTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mockClock.Set(initialTime)

	// Get first timestamp
	ts1 := manager.NextTimestamp()
	assert.Greater(t, ts1, uint64(0))

	// Advance clock and get another timestamp
	mockClock.Advance(time.Millisecond)
	ts2 := manager.NextTimestamp()
	assert.Greater(t, ts2, ts1, "Timestamps should be monotonically increasing")

	// Get multiple timestamps in succession - should still increase
	ts3 := manager.NextTimestamp()
	ts4 := manager.NextTimestamp()
	assert.Greater(t, ts3, ts2)
	assert.Greater(t, ts4, ts3)
}

// TestMVCCManager_NextTimestamp_ClockBackward tests that timestamps remain monotonic even when clock goes backward.
func TestMVCCManager_NextTimestamp_ClockBackward(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	// Set initial time
	initialTime := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	mockClock.Set(initialTime)

	// Get first timestamp
	ts1 := manager.NextTimestamp()

	// Move clock backward (simulating NTP correction)
	mockClock.Set(initialTime.Add(-time.Hour))

	// Get another timestamp - should still be greater than previous
	ts2 := manager.NextTimestamp()
	assert.Greater(t, ts2, ts1, "Timestamps should increase even when clock goes backward")
}

// TestMVCCManager_NextTimestamp_SameTime tests that timestamps increase when clock returns same time.
func TestMVCCManager_NextTimestamp_SameTime(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	// Set a fixed time
	fixedTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	mockClock.Set(fixedTime)

	// Get multiple timestamps at the "same" time
	var timestamps []uint64
	for i := 0; i < 10; i++ {
		timestamps = append(timestamps, manager.NextTimestamp())
	}

	// All timestamps should be unique and increasing
	for i := 1; i < len(timestamps); i++ {
		assert.Greater(t, timestamps[i], timestamps[i-1],
			"Timestamp %d should be greater than timestamp %d", i, i-1)
	}
}

// TestMVCCManager_BeginTransaction tests starting transactions and capturing active snapshots.
func TestMVCCManager_BeginTransaction(t *testing.T) {
	mockClock := quartz.NewMock(t)
	initialTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mockClock.Set(initialTime)

	manager := NewMVCCManager(mockClock)

	// Begin first transaction
	txn1 := manager.BeginTransaction(parser.IsolationLevelSerializable)

	require.NotNil(t, txn1)
	assert.True(t, txn1.IsActive())
	assert.Equal(t, parser.IsolationLevelSerializable, txn1.GetIsolationLevel())
	assert.Greater(t, txn1.ID(), uint64(0))
	assert.Equal(t, txn1.StartTS, txn1.ID())
	assert.Equal(t, uint64(0), txn1.CommitTS, "CommitTS should be 0 for uncommitted transaction")
	assert.Equal(t, initialTime, txn1.GetStartTime())
	assert.Empty(t, txn1.ActiveAtStart, "First transaction should have no active transactions at start")

	// Begin second transaction - should capture first as active
	mockClock.Advance(time.Millisecond)
	txn2 := manager.BeginTransaction(parser.IsolationLevelRepeatableRead)

	require.NotNil(t, txn2)
	assert.True(t, txn2.IsActive())
	assert.Equal(t, parser.IsolationLevelRepeatableRead, txn2.GetIsolationLevel())
	assert.Greater(t, txn2.ID(), txn1.ID())
	assert.Contains(t, txn2.ActiveAtStart, txn1.ID(), "Txn2 should have Txn1 in its active-at-start set")
}

// TestMVCCManager_BeginTransaction_AllIsolationLevels tests beginning transactions with all isolation levels.
func TestMVCCManager_BeginTransaction_AllIsolationLevels(t *testing.T) {
	levels := []parser.IsolationLevel{
		parser.IsolationLevelSerializable,
		parser.IsolationLevelRepeatableRead,
		parser.IsolationLevelReadCommitted,
		parser.IsolationLevelReadUncommitted,
	}

	for _, level := range levels {
		t.Run(level.String(), func(t *testing.T) {
			mockClock := quartz.NewMock(t)
			manager := NewMVCCManager(mockClock)

			txn := manager.BeginTransaction(level)

			require.NotNil(t, txn)
			assert.True(t, txn.IsActive())
			assert.Equal(t, level, txn.GetIsolationLevel())
		})
	}
}

// TestMVCCManager_Commit tests committing transactions and assigning CommitTS.
func TestMVCCManager_Commit(t *testing.T) {
	mockClock := quartz.NewMock(t)
	initialTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mockClock.Set(initialTime)

	manager := NewMVCCManager(mockClock)

	// Begin and commit a transaction
	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)
	startTS := txn.StartTS

	mockClock.Advance(time.Millisecond)

	err := manager.Commit(txn)

	require.NoError(t, err)
	assert.False(t, txn.IsActive(), "Transaction should not be active after commit")
	assert.Greater(t, txn.CommitTS, uint64(0), "CommitTS should be set")
	assert.Greater(t, txn.CommitTS, startTS, "CommitTS should be greater than StartTS")

	// Verify transaction is removed from active set
	activeTxns := manager.GetActiveTransactions()
	assert.Empty(t, activeTxns)
}

// TestMVCCManager_Commit_Errors tests error cases for commit.
func TestMVCCManager_Commit_Errors(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	t.Run("Commit inactive transaction", func(t *testing.T) {
		txn := manager.BeginTransaction(parser.IsolationLevelSerializable)
		err := manager.Commit(txn)
		require.NoError(t, err)

		// Try to commit again
		err = manager.Commit(txn)
		assert.Error(t, err)
		assert.Equal(t, storage.ErrSerializationFailure, err)
	})

	t.Run("Commit transaction from different manager", func(t *testing.T) {
		manager2 := NewMVCCManager(mockClock)
		txn := manager2.BeginTransaction(parser.IsolationLevelSerializable)

		// Try to commit on wrong manager
		err := manager.Commit(txn)
		assert.Error(t, err)
		assert.Equal(t, storage.ErrSerializationFailure, err)
	})
}

// TestMVCCManager_Rollback tests rolling back transactions.
func TestMVCCManager_Rollback(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	// Begin and rollback a transaction
	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	err := manager.Rollback(txn)

	require.NoError(t, err)
	assert.False(t, txn.IsActive(), "Transaction should not be active after rollback")
	assert.Equal(t, uint64(0), txn.CommitTS, "CommitTS should remain 0 after rollback")

	// Verify transaction is removed from active set
	activeTxns := manager.GetActiveTransactions()
	assert.Empty(t, activeTxns)
}

// TestMVCCManager_Rollback_Errors tests error cases for rollback.
func TestMVCCManager_Rollback_Errors(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	t.Run("Rollback inactive transaction", func(t *testing.T) {
		txn := manager.BeginTransaction(parser.IsolationLevelSerializable)
		err := manager.Rollback(txn)
		require.NoError(t, err)

		// Try to rollback again
		err = manager.Rollback(txn)
		assert.Error(t, err)
		assert.Equal(t, storage.ErrSerializationFailure, err)
	})

	t.Run("Rollback transaction from different manager", func(t *testing.T) {
		manager2 := NewMVCCManager(mockClock)
		txn := manager2.BeginTransaction(parser.IsolationLevelSerializable)

		// Try to rollback on wrong manager
		err := manager.Rollback(txn)
		assert.Error(t, err)
		assert.Equal(t, storage.ErrSerializationFailure, err)
	})
}

// TestMVCCManager_GetActiveTransactions tests getting the list of active transactions.
func TestMVCCManager_GetActiveTransactions(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	// Initially empty
	activeTxns := manager.GetActiveTransactions()
	assert.Empty(t, activeTxns)

	// Start some transactions
	txn1 := manager.BeginTransaction(parser.IsolationLevelSerializable)
	txn2 := manager.BeginTransaction(parser.IsolationLevelReadCommitted)
	txn3 := manager.BeginTransaction(parser.IsolationLevelRepeatableRead)

	activeTxns = manager.GetActiveTransactions()
	assert.Len(t, activeTxns, 3)

	// Verify all transactions are in the list
	ids := make(map[uint64]bool)
	for _, txn := range activeTxns {
		ids[txn.ID()] = true
	}
	assert.True(t, ids[txn1.ID()])
	assert.True(t, ids[txn2.ID()])
	assert.True(t, ids[txn3.ID()])

	// Commit one
	_ = manager.Commit(txn1)
	activeTxns = manager.GetActiveTransactions()
	assert.Len(t, activeTxns, 2)

	// Rollback one
	_ = manager.Rollback(txn2)
	activeTxns = manager.GetActiveTransactions()
	assert.Len(t, activeTxns, 1)
	assert.Equal(t, txn3.ID(), activeTxns[0].ID())
}

// TestMVCCManager_GetActiveTransactions_ReturnsCopy tests that the returned slice is a copy.
func TestMVCCManager_GetActiveTransactions_ReturnsCopy(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	_ = manager.BeginTransaction(parser.IsolationLevelSerializable)
	_ = manager.BeginTransaction(parser.IsolationLevelSerializable)

	activeTxns := manager.GetActiveTransactions()
	assert.Len(t, activeTxns, 2)

	// Modify the returned slice
	activeTxns[0] = nil

	// Original should be unaffected
	activeTxns2 := manager.GetActiveTransactions()
	assert.Len(t, activeTxns2, 2)
	assert.NotNil(t, activeTxns2[0])
}

// TestMVCCManager_GetLowWatermark tests getting the minimum timestamp for GC.
func TestMVCCManager_GetLowWatermark(t *testing.T) {
	mockClock := quartz.NewMock(t)
	initialTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mockClock.Set(initialTime)

	manager := NewMVCCManager(mockClock)

	// No active transactions - should return 0
	assert.Equal(t, uint64(0), manager.GetLowWatermark())

	// Start first transaction
	txn1 := manager.BeginTransaction(parser.IsolationLevelSerializable)
	lowWatermark := manager.GetLowWatermark()
	assert.Equal(t, txn1.StartTS, lowWatermark)

	// Start second transaction with later timestamp
	mockClock.Advance(time.Second)
	txn2 := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Low watermark should still be txn1's timestamp
	lowWatermark = manager.GetLowWatermark()
	assert.Equal(t, txn1.StartTS, lowWatermark)
	assert.Less(t, lowWatermark, txn2.StartTS)

	// Commit txn1 - low watermark should now be txn2's timestamp
	_ = manager.Commit(txn1)
	lowWatermark = manager.GetLowWatermark()
	assert.Equal(t, txn2.StartTS, lowWatermark)

	// Rollback txn2 - low watermark should return to 0
	_ = manager.Rollback(txn2)
	assert.Equal(t, uint64(0), manager.GetLowWatermark())
}

// TestMVCCManager_ConcurrentAccess tests thread safety with concurrent operations.
func TestMVCCManager_ConcurrentAccess(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				// Begin a transaction
				txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

				// Get some timestamps
				_ = manager.NextTimestamp()

				// Query active transactions
				_ = manager.GetActiveTransactions()

				// Query low watermark
				_ = manager.GetLowWatermark()

				// Randomly commit or rollback
				if j%2 == 0 {
					_ = manager.Commit(txn)
				} else {
					_ = manager.Rollback(txn)
				}
			}
		}()
	}

	wg.Wait()

	// All transactions should be finished
	activeTxns := manager.GetActiveTransactions()
	assert.Empty(t, activeTxns)
}

// =============================================================================
// MVCCTransaction Tests
// =============================================================================

// TestMVCCTransaction_ID tests that ID returns the correct value.
func TestMVCCTransaction_ID(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// ID should equal StartTS
	assert.Equal(t, txn.StartTS, txn.ID())
	assert.Greater(t, txn.ID(), uint64(0))

	// ID should remain constant
	id1 := txn.ID()
	id2 := txn.ID()
	assert.Equal(t, id1, id2)
}

// TestMVCCTransaction_IsActive tests active state tracking.
func TestMVCCTransaction_IsActive(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Should be active after creation
	assert.True(t, txn.IsActive())

	// Should be inactive after commit
	_ = manager.Commit(txn)
	assert.False(t, txn.IsActive())

	// Test rollback path
	txn2 := manager.BeginTransaction(parser.IsolationLevelSerializable)
	assert.True(t, txn2.IsActive())

	_ = manager.Rollback(txn2)
	assert.False(t, txn2.IsActive())
}

// TestMVCCTransaction_GetIsolationLevel tests that isolation level is set correctly.
func TestMVCCTransaction_GetIsolationLevel(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	levels := []parser.IsolationLevel{
		parser.IsolationLevelSerializable,
		parser.IsolationLevelRepeatableRead,
		parser.IsolationLevelReadCommitted,
		parser.IsolationLevelReadUncommitted,
	}

	for _, level := range levels {
		t.Run(level.String(), func(t *testing.T) {
			txn := manager.BeginTransaction(level)
			assert.Equal(t, level, txn.GetIsolationLevel())
			_ = manager.Rollback(txn)
		})
	}
}

// TestMVCCTransaction_RecordRead tests read set tracking.
func TestMVCCTransaction_RecordRead(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Initially empty
	readSet := txn.GetReadSet()
	assert.Empty(t, readSet)

	// Record some reads
	txn.RecordRead("users", 1)
	txn.RecordRead("users", 2)
	txn.RecordRead("orders", 100)

	readSet = txn.GetReadSet()
	assert.Len(t, readSet, 2, "Should have 2 tables")
	assert.Len(t, readSet["users"], 2, "Should have 2 rows read from users")
	assert.Len(t, readSet["orders"], 1, "Should have 1 row read from orders")

	assert.Contains(t, readSet["users"], uint64(1))
	assert.Contains(t, readSet["users"], uint64(2))
	assert.Contains(t, readSet["orders"], uint64(100))
}

// TestMVCCTransaction_RecordRead_Duplicates tests that duplicate reads are tracked.
func TestMVCCTransaction_RecordRead_Duplicates(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Record same row multiple times
	txn.RecordRead("users", 1)
	txn.RecordRead("users", 1)
	txn.RecordRead("users", 1)

	readSet := txn.GetReadSet()
	// Duplicates are stored (append behavior)
	assert.Len(t, readSet["users"], 3)
}

// TestMVCCTransaction_RecordWrite tests write set tracking.
func TestMVCCTransaction_RecordWrite(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Initially empty
	writeSet := txn.GetWriteSet()
	assert.Empty(t, writeSet)

	// Record some writes
	txn.RecordWrite("users", 1)
	txn.RecordWrite("users", 3)
	txn.RecordWrite("products", 50)

	writeSet = txn.GetWriteSet()
	assert.Len(t, writeSet, 2, "Should have 2 tables")
	assert.Len(t, writeSet["users"], 2, "Should have 2 rows written to users")
	assert.Len(t, writeSet["products"], 1, "Should have 1 row written to products")

	assert.Contains(t, writeSet["users"], uint64(1))
	assert.Contains(t, writeSet["users"], uint64(3))
	assert.Contains(t, writeSet["products"], uint64(50))
}

// TestMVCCTransaction_GetReadSet tests that GetReadSet returns a copy.
func TestMVCCTransaction_GetReadSet(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	txn.RecordRead("users", 1)
	txn.RecordRead("users", 2)

	// Get read set
	readSet1 := txn.GetReadSet()
	assert.Len(t, readSet1["users"], 2)

	// Modify the returned set
	readSet1["users"] = append(readSet1["users"], 999)
	readSet1["newTable"] = []uint64{100}

	// Original should be unaffected
	readSet2 := txn.GetReadSet()
	assert.Len(t, readSet2["users"], 2, "Original read set should be unmodified")
	_, hasNewTable := readSet2["newTable"]
	assert.False(t, hasNewTable, "Original read set should not have new table")
}

// TestMVCCTransaction_GetWriteSet tests that GetWriteSet returns a copy.
func TestMVCCTransaction_GetWriteSet(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	txn.RecordWrite("users", 1)
	txn.RecordWrite("users", 2)

	// Get write set
	writeSet1 := txn.GetWriteSet()
	assert.Len(t, writeSet1["users"], 2)

	// Modify the returned set
	writeSet1["users"] = append(writeSet1["users"], 999)
	writeSet1["newTable"] = []uint64{100}

	// Original should be unaffected
	writeSet2 := txn.GetWriteSet()
	assert.Len(t, writeSet2["users"], 2, "Original write set should be unmodified")
	_, hasNewTable := writeSet2["newTable"]
	assert.False(t, hasNewTable, "Original write set should not have new table")
}

// TestMVCCTransaction_VisibilityChecker tests that the correct visibility checker is returned for each isolation level.
func TestMVCCTransaction_VisibilityChecker(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	testCases := []struct {
		level        parser.IsolationLevel
		expectedType string
	}{
		{parser.IsolationLevelReadUncommitted, "*storage.ReadUncommittedVisibility"},
		{parser.IsolationLevelReadCommitted, "*storage.ReadCommittedVisibility"},
		{parser.IsolationLevelRepeatableRead, "*storage.RepeatableReadVisibility"},
		{parser.IsolationLevelSerializable, "*storage.SerializableVisibility"},
	}

	for _, tc := range testCases {
		t.Run(tc.level.String(), func(t *testing.T) {
			txn := manager.BeginTransaction(tc.level)
			checker := txn.VisibilityChecker()

			require.NotNil(t, checker)

			// Verify the correct type based on isolation level
			switch tc.level {
			case parser.IsolationLevelReadUncommitted:
				_, ok := checker.(*storage.ReadUncommittedVisibility)
				assert.True(t, ok, "Expected ReadUncommittedVisibility")
			case parser.IsolationLevelReadCommitted:
				_, ok := checker.(*storage.ReadCommittedVisibility)
				assert.True(t, ok, "Expected ReadCommittedVisibility")
			case parser.IsolationLevelRepeatableRead:
				_, ok := checker.(*storage.RepeatableReadVisibility)
				assert.True(t, ok, "Expected RepeatableReadVisibility")
			case parser.IsolationLevelSerializable:
				_, ok := checker.(*storage.SerializableVisibility)
				assert.True(t, ok, "Expected SerializableVisibility")
			}

			_ = manager.Rollback(txn)
		})
	}
}

// TestMVCCTransaction_WasActiveAtStart tests active-at-start checking.
func TestMVCCTransaction_WasActiveAtStart(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	// Start first transaction
	txn1 := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Start second transaction
	txn2 := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Start third transaction
	txn3 := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Txn2 should see txn1 as active at start
	assert.True(t, txn2.WasActiveAtStart(txn1.ID()))
	// Txn2 should NOT see itself as active at start
	assert.False(t, txn2.WasActiveAtStart(txn2.ID()))
	// Txn2 should NOT see txn3 (started after)
	assert.False(t, txn2.WasActiveAtStart(txn3.ID()))

	// Txn3 should see both txn1 and txn2 as active at start
	assert.True(t, txn3.WasActiveAtStart(txn1.ID()))
	assert.True(t, txn3.WasActiveAtStart(txn2.ID()))
	assert.False(t, txn3.WasActiveAtStart(txn3.ID()))

	// Txn1 should not see any other transactions as active at start
	assert.False(t, txn1.WasActiveAtStart(txn1.ID()))
	assert.False(t, txn1.WasActiveAtStart(txn2.ID()))
	assert.False(t, txn1.WasActiveAtStart(txn3.ID()))

	// Non-existent transaction ID
	assert.False(t, txn3.WasActiveAtStart(999999))
}

// TestMVCCTransaction_WasActiveAtStart_LazyInit tests that the set is lazily initialized.
func TestMVCCTransaction_WasActiveAtStart_LazyInit(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	txn1 := manager.BeginTransaction(parser.IsolationLevelSerializable)
	txn2 := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Before any call, the set should be nil
	assert.Nil(t, txn2.activeAtStartSet)

	// First call should initialize the set
	_ = txn2.WasActiveAtStart(txn1.ID())
	assert.NotNil(t, txn2.activeAtStartSet)

	// Add a marker to verify the set is not re-created
	// The set should contain txn1.ID() after initialization
	setLenBefore := len(txn2.activeAtStartSet)
	assert.Greater(t, setLenBefore, 0)

	// Subsequent calls should use the cached set (not re-initialize)
	_ = txn2.WasActiveAtStart(txn1.ID())

	// The set length should remain the same (same set, not recreated)
	assert.Equal(t, setLenBefore, len(txn2.activeAtStartSet))
}

// TestMVCCTransaction_GetActiveAtStart tests getting a copy of active-at-start IDs.
func TestMVCCTransaction_GetActiveAtStart(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	// Start several transactions
	txn1 := manager.BeginTransaction(parser.IsolationLevelSerializable)
	txn2 := manager.BeginTransaction(parser.IsolationLevelSerializable)
	txn3 := manager.BeginTransaction(parser.IsolationLevelSerializable)
	txn4 := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Get active at start for txn4
	activeAtStart := txn4.GetActiveAtStart()

	// Should contain txn1, txn2, txn3 (sorted)
	assert.Len(t, activeAtStart, 3)

	// Verify it's sorted
	assert.True(t, sort.SliceIsSorted(activeAtStart, func(i, j int) bool {
		return activeAtStart[i] < activeAtStart[j]
	}))

	// Verify contents
	assert.Contains(t, activeAtStart, txn1.ID())
	assert.Contains(t, activeAtStart, txn2.ID())
	assert.Contains(t, activeAtStart, txn3.ID())
	assert.NotContains(t, activeAtStart, txn4.ID())

	// Verify it returns a copy
	originalLen := len(activeAtStart)
	activeAtStart[0] = 999999
	activeAtStart2 := txn4.GetActiveAtStart()
	assert.Equal(t, originalLen, len(activeAtStart2))
	assert.NotEqual(t, uint64(999999), activeAtStart2[0])
}

// TestMVCCTransaction_ConcurrentReadWriteSets tests thread safety of read/write set operations.
func TestMVCCTransaction_ConcurrentReadWriteSets(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // Half for reads, half for writes

	// Concurrent RecordRead calls
	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				txn.RecordRead("table", uint64(workerID*numOperations+j))
			}
		}(i)
	}

	// Concurrent RecordWrite calls
	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				txn.RecordWrite("table", uint64(workerID*numOperations+j))
			}
		}(i)
	}

	wg.Wait()

	// Verify we have the expected number of entries
	readSet := txn.GetReadSet()
	writeSet := txn.GetWriteSet()

	assert.Len(t, readSet["table"], numGoroutines*numOperations)
	assert.Len(t, writeSet["table"], numGoroutines*numOperations)
}

// TestMVCCTransaction_GetStartTime tests that start time is captured correctly.
func TestMVCCTransaction_GetStartTime(t *testing.T) {
	mockClock := quartz.NewMock(t)
	initialTime := time.Date(2024, 6, 15, 12, 30, 45, 0, time.UTC)
	mockClock.Set(initialTime)

	manager := NewMVCCManager(mockClock)

	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	assert.Equal(t, initialTime, txn.GetStartTime())

	// Advance time and verify transaction's start time is unchanged
	mockClock.Advance(time.Hour)
	assert.Equal(t, initialTime, txn.GetStartTime())
}

// TestMVCCTransaction_GetStartTS tests that start timestamp is captured correctly.
func TestMVCCTransaction_GetStartTS(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	assert.Equal(t, txn.StartTS, txn.GetStartTS())
	assert.Equal(t, txn.ID(), txn.GetStartTS())
}

// TestMVCCTransaction_GetCommitTS tests that commit timestamp is tracked correctly.
func TestMVCCTransaction_GetCommitTS(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	txn := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Before commit, should be 0
	assert.Equal(t, uint64(0), txn.GetCommitTS())

	// After commit, should be set
	_ = manager.Commit(txn)
	assert.Greater(t, txn.GetCommitTS(), uint64(0))
	assert.Greater(t, txn.GetCommitTS(), txn.GetStartTS())
}

// TestMVCCTransaction_CommittedVsRolledBack tests the difference between committed and rolled back transactions.
func TestMVCCTransaction_CommittedVsRolledBack(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	// Committed transaction
	txn1 := manager.BeginTransaction(parser.IsolationLevelSerializable)
	_ = manager.Commit(txn1)

	assert.False(t, txn1.IsActive())
	assert.Greater(t, txn1.CommitTS, uint64(0), "Committed transaction should have CommitTS")

	// Rolled back transaction
	txn2 := manager.BeginTransaction(parser.IsolationLevelSerializable)
	_ = manager.Rollback(txn2)

	assert.False(t, txn2.IsActive())
	assert.Equal(t, uint64(0), txn2.CommitTS, "Rolled back transaction should have CommitTS = 0")
}

// TestMVCCManager_TransactionIDsAreUnique tests that all transaction IDs are unique.
func TestMVCCManager_TransactionIDsAreUnique(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	const numTransactions = 1000
	ids := make(map[uint64]bool)

	for i := 0; i < numTransactions; i++ {
		txn := manager.BeginTransaction(parser.IsolationLevelSerializable)
		assert.False(t, ids[txn.ID()], "Transaction ID should be unique: %d", txn.ID())
		ids[txn.ID()] = true
		_ = manager.Rollback(txn)
	}

	assert.Len(t, ids, numTransactions)
}

// TestMVCCManager_SnapshotExcludesNewTransactions tests that snapshots don't include transactions started after.
func TestMVCCManager_SnapshotExcludesNewTransactions(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	// Start T1
	txn1 := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Start T2 - captures T1 as active
	txn2 := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// Start T3 - captures T1 and T2 as active
	txn3 := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// T2's snapshot should only include T1, not T3
	assert.True(t, txn2.WasActiveAtStart(txn1.ID()))
	assert.False(t, txn2.WasActiveAtStart(txn3.ID()))

	// T3's snapshot should include both T1 and T2
	assert.True(t, txn3.WasActiveAtStart(txn1.ID()))
	assert.True(t, txn3.WasActiveAtStart(txn2.ID()))

	// Cleanup
	_ = manager.Rollback(txn1)
	_ = manager.Rollback(txn2)
	_ = manager.Rollback(txn3)
}

// TestMVCCManager_SnapshotAfterCommit tests that committed transactions are not in new snapshots.
func TestMVCCManager_SnapshotAfterCommit(t *testing.T) {
	mockClock := quartz.NewMock(t)
	manager := NewMVCCManager(mockClock)

	// Start and commit T1
	txn1 := manager.BeginTransaction(parser.IsolationLevelSerializable)
	txn1ID := txn1.ID()
	_ = manager.Commit(txn1)

	// Start T2 after T1 is committed
	txn2 := manager.BeginTransaction(parser.IsolationLevelSerializable)

	// T2's snapshot should NOT include T1 (it was already committed)
	assert.False(t, txn2.WasActiveAtStart(txn1ID))

	_ = manager.Rollback(txn2)
}
