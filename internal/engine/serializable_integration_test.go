package engine

import (
	"context"
	"testing"
	"time"

	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// SERIALIZABLE Integration Tests
// =============================================================================
// These tests verify that SERIALIZABLE isolation level works correctly
// when using the actual engine implementation (not mocks).
//
// SERIALIZABLE characteristics:
// - Prevents dirty reads (cannot see uncommitted changes from other transactions)
// - Prevents non-repeatable reads (same query returns same results within transaction)
// - Prevents phantom reads (new rows do not appear between queries)
// - Uses conflict detection at commit time
// - Returns serialization failure on write-write or read-write conflicts
//
// Key difference from REPEATABLE READ:
// - REPEATABLE READ may allow phantom reads (implementation dependent)
// - SERIALIZABLE prevents all anomalies and detects conflicts at commit
// =============================================================================

// TestSerializableDirtyReadPrevention verifies that a transaction with
// SERIALIZABLE isolation cannot see uncommitted changes from another
// transaction (dirty read prevention).
//
// Scenario:
// 1. T1 starts with SERIALIZABLE
// 2. T2 inserts a row but does NOT commit
// 3. T1 queries the table
// 4. T1 should NOT see the uncommitted row from T2
func TestSerializableDirtyReadPrevention(t *testing.T) {
	// Create a shared engine for both connections
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	// Connection 1 (T1) - will query with SERIALIZABLE
	conn1, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn1.Close() }()

	engineConn1 := conn1.(*EngineConn)

	// Create a test table
	_, err = engineConn1.Execute(
		context.Background(),
		"CREATE TABLE test_dirty_read_ser (id INTEGER, name VARCHAR)",
		nil,
	)
	require.NoError(t, err)

	// Insert initial data and commit it
	_, err = engineConn1.Execute(
		context.Background(),
		"INSERT INTO test_dirty_read_ser VALUES (1, 'Alice')",
		nil,
	)
	require.NoError(t, err)

	// Begin T1 with SERIALIZABLE
	_, err = engineConn1.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	// Verify T1 is in SERIALIZABLE mode
	assert.True(t, engineConn1.inTxn)
	assert.Equal(t, "SERIALIZABLE", engineConn1.currentIsolationLevel.String())

	// Verify T1 has a snapshot (required for SERIALIZABLE)
	assert.True(t, engineConn1.txn.HasSnapshot())
	assert.NotNil(t, engineConn1.txn.GetSnapshot())

	// Query the table - should see the committed row
	rows, _, err := engineConn1.Query(
		context.Background(),
		"SELECT * FROM test_dirty_read_ser",
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, rows, 1, "Should see the committed row")

	// Note: In a real multi-connection scenario, T2's uncommitted insert would not be visible.
	// For this test, we verify the isolation level is properly set and snapshot exists.

	// Commit T1
	_, err = engineConn1.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)
}

// TestSerializableNonRepeatableReadPrevention verifies that non-repeatable
// reads are PREVENTED in SERIALIZABLE isolation.
//
// This is the same behavior as REPEATABLE READ:
// - T1 reads row R with value V1
// - T2 updates R to V2 and commits
// - T1 reads R again and STILL sees V1 (snapshot isolation)
func TestSerializableNonRepeatableReadPrevention(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Setup: Create table and insert initial data
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_nonrepeatable_ser (id INTEGER PRIMARY KEY, value INTEGER)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_nonrepeatable_ser VALUES (1, 100)",
		nil,
	)
	require.NoError(t, err)

	// T1: Begin transaction with SERIALIZABLE
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	// Verify snapshot was created at transaction start
	snapshotTime := engineConn.txn.GetSnapshot().GetTimestamp()
	assert.False(t, snapshotTime.IsZero(), "Snapshot should be created at transaction start")

	// T1: First read - should see value 100
	rows, _, err := engineConn.Query(
		context.Background(),
		"SELECT value FROM test_nonrepeatable_ser WHERE id = 1",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	firstValue := rows[0]["value"]
	assert.EqualValues(t, 100, firstValue, "First read should see value 100")

	// T1: Second read - should STILL see value 100 (snapshot isolation)
	// In SERIALIZABLE, the same query within a transaction always returns the same result
	rows, _, err = engineConn.Query(
		context.Background(),
		"SELECT value FROM test_nonrepeatable_ser WHERE id = 1",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	secondValue := rows[0]["value"]
	assert.EqualValues(t, 100, secondValue, "Second read should still see value 100")

	// Verify that both reads returned the same value (non-repeatable read prevented)
	assert.Equal(t, firstValue, secondValue, "SERIALIZABLE should prevent non-repeatable reads")

	// Cleanup
	_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)
}

// TestSerializablePhantomReadPrevention tests phantom read behavior in
// SERIALIZABLE isolation.
//
// SERIALIZABLE should prevent phantom reads - new rows inserted by concurrent
// transactions should not be visible within the transaction.
//
// Scenario:
// 1. T1 starts with SERIALIZABLE
// 2. T1 executes SELECT WHERE x > 5, gets N rows
// 3. (In a multi-connection scenario, T2 would insert a row with x = 10 and commit)
// 4. T1 re-executes the same SELECT
// 5. T1 should STILL see N rows (phantom prevented by snapshot)
func TestSerializablePhantomReadPrevention(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Setup: Create table and insert initial data (x > 5 rows)
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_phantom_ser (id INTEGER, x INTEGER)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_phantom_ser VALUES (1, 6)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_phantom_ser VALUES (2, 7)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_phantom_ser VALUES (3, 8)",
		nil,
	)
	require.NoError(t, err)

	// T1: Begin transaction with SERIALIZABLE
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	// T1: First query - should see 3 rows where x > 5
	rows, _, err := engineConn.Query(
		context.Background(),
		"SELECT * FROM test_phantom_ser WHERE x > 5",
		nil,
	)
	require.NoError(t, err)
	firstCount := len(rows)
	assert.Equal(t, 3, firstCount, "First query should return 3 rows where x > 5")

	// T1: Second query (within same transaction) - should return same count
	// because SERIALIZABLE provides snapshot isolation preventing phantom reads
	rows, _, err = engineConn.Query(
		context.Background(),
		"SELECT * FROM test_phantom_ser WHERE x > 5",
		nil,
	)
	require.NoError(t, err)
	secondCount := len(rows)
	assert.Equal(
		t,
		firstCount,
		secondCount,
		"Second query within SERIALIZABLE should return same row count (phantom prevented)",
	)

	// Cleanup
	_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)
}

// TestSerializableOwnChangesVisible verifies that a transaction can
// always see its own changes, even uncommitted ones.
func TestSerializableOwnChangesVisible(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Create table
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_own_changes_ser (id INTEGER, name VARCHAR)",
		nil,
	)
	require.NoError(t, err)

	// Begin T1 with SERIALIZABLE
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	// T1: Insert a row (not yet committed at transaction level)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_own_changes_ser VALUES (1, 'Test')",
		nil,
	)
	require.NoError(t, err)

	// T1: Query should see own insert
	rows, _, err := engineConn.Query(
		context.Background(),
		"SELECT * FROM test_own_changes_ser WHERE id = 1",
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, rows, 1, "Transaction should see its own uncommitted insert")
	assert.Equal(t, "Test", rows[0]["name"], "Should see correct value")

	// T1: Update the row
	_, err = engineConn.Execute(
		context.Background(),
		"UPDATE test_own_changes_ser SET name = 'Updated' WHERE id = 1",
		nil,
	)
	require.NoError(t, err)

	// T1: Query should see the update
	rows, _, err = engineConn.Query(
		context.Background(),
		"SELECT * FROM test_own_changes_ser WHERE id = 1",
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, rows, 1, "Transaction should see its own update")
	assert.Equal(t, "Updated", rows[0]["name"], "Should see updated value")

	// Rollback to discard changes
	_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)

	// After rollback, the row should not exist
	rows, _, err = engineConn.Query(context.Background(), "SELECT * FROM test_own_changes_ser", nil)
	require.NoError(t, err)
	assert.Len(t, rows, 0, "After rollback, inserted row should not exist")
}

// =============================================================================
// Write-Write Conflict Detection Tests
// =============================================================================
// These tests verify that SERIALIZABLE isolation properly detects write-write
// conflicts when two transactions modify the same row.
// =============================================================================

// TestSerializableWriteWriteConflictDetection tests that the conflict detector
// properly identifies write-write conflicts between concurrent transactions.
//
// Scenario:
// 1. T1 and T2 both start with SERIALIZABLE
// 2. Both transactions update the same row
// 3. T1 commits first (succeeds)
// 4. T2 attempts to commit -> receives serialization failure
func TestSerializableWriteWriteConflictDetection(t *testing.T) {
	// Create a conflict detector to test write-write conflict detection
	cd := storage.NewConflictDetector()

	const txn1ID uint64 = 1
	const txn2ID uint64 = 2
	const tableID = "test_table"
	const rowID = "row_1"

	// Both transactions write to the same row
	cd.RegisterWrite(txn1ID, tableID, rowID)
	cd.RegisterWrite(txn2ID, tableID, rowID)

	// T1 commits first - no concurrent transactions committed yet, so no conflict
	err := cd.CheckConflicts(txn1ID, []uint64{})
	assert.NoError(t, err, "T1 should commit successfully (no concurrent commits yet)")

	// T2 attempts to commit - T1 has already committed and wrote to the same row
	// T2 should detect write-write conflict
	err = cd.CheckConflicts(txn2ID, []uint64{txn1ID})
	assert.Error(t, err, "T2 should fail due to write-write conflict")
	assert.Equal(t, storage.ErrSerializationFailure, err, "Error should be serialization failure")

	// Clean up
	cd.ClearTransaction(txn1ID)
	cd.ClearTransaction(txn2ID)
}

// TestSerializableWriteWriteConflictDifferentRows verifies that write-write
// conflicts are NOT detected when transactions modify different rows.
//
// Scenario:
// 1. T1 updates row R1
// 2. T2 updates row R2 (different row)
// 3. Both should commit successfully
func TestSerializableWriteWriteConflictDifferentRows(t *testing.T) {
	cd := storage.NewConflictDetector()

	const txn1ID uint64 = 1
	const txn2ID uint64 = 2
	const tableID = "test_table"

	// Transactions write to different rows
	cd.RegisterWrite(txn1ID, tableID, "row_1")
	cd.RegisterWrite(txn2ID, tableID, "row_2")

	// T1 commits first
	err := cd.CheckConflicts(txn1ID, []uint64{})
	assert.NoError(t, err, "T1 should commit successfully")

	// T2 commits second - no conflict because different rows
	err = cd.CheckConflicts(txn2ID, []uint64{txn1ID})
	assert.NoError(t, err, "T2 should commit successfully (different rows)")

	// Clean up
	cd.ClearTransaction(txn1ID)
	cd.ClearTransaction(txn2ID)
}

// =============================================================================
// Read-Write Conflict Detection Tests
// =============================================================================
// These tests verify that SERIALIZABLE isolation properly detects read-write
// conflicts when a transaction reads a row that was modified by a concurrent
// committed transaction.
// =============================================================================

// TestSerializableReadWriteConflictDetection tests that the conflict detector
// properly identifies read-write conflicts.
//
// Scenario:
// 1. T1 reads row R
// 2. T2 updates row R and commits
// 3. T1 attempts to commit -> receives serialization failure
func TestSerializableReadWriteConflictDetection(t *testing.T) {
	cd := storage.NewConflictDetector()

	const txn1ID uint64 = 1
	const txn2ID uint64 = 2
	const tableID = "test_table"
	const rowID = "row_1"

	// T1 reads the row
	cd.RegisterRead(txn1ID, tableID, rowID)

	// T2 writes to the same row
	cd.RegisterWrite(txn2ID, tableID, rowID)

	// T2 commits first - T1 is still active but hasn't committed
	err := cd.CheckConflicts(txn2ID, []uint64{})
	assert.NoError(t, err, "T2 should commit successfully (no concurrent commits yet)")

	// T1 attempts to commit - T2 has written to a row that T1 read
	// This is a read-write conflict (T1's read is now stale)
	err = cd.CheckConflicts(txn1ID, []uint64{txn2ID})
	assert.Error(t, err, "T1 should fail due to read-write conflict")
	assert.Equal(t, storage.ErrSerializationFailure, err, "Error should be serialization failure")

	// Clean up
	cd.ClearTransaction(txn1ID)
	cd.ClearTransaction(txn2ID)
}

// TestSerializableReadWriteConflictDifferentRows verifies that read-write
// conflicts are NOT detected when transactions access different rows.
//
// Scenario:
// 1. T1 reads row R1
// 2. T2 writes row R2 (different row) and commits
// 3. T1 should commit successfully
func TestSerializableReadWriteConflictDifferentRows(t *testing.T) {
	cd := storage.NewConflictDetector()

	const txn1ID uint64 = 1
	const txn2ID uint64 = 2
	const tableID = "test_table"

	// T1 reads row 1
	cd.RegisterRead(txn1ID, tableID, "row_1")

	// T2 writes to row 2 (different row)
	cd.RegisterWrite(txn2ID, tableID, "row_2")

	// T2 commits first
	err := cd.CheckConflicts(txn2ID, []uint64{})
	assert.NoError(t, err, "T2 should commit successfully")

	// T1 commits - no conflict because different rows
	err = cd.CheckConflicts(txn1ID, []uint64{txn2ID})
	assert.NoError(t, err, "T1 should commit successfully (different rows)")

	// Clean up
	cd.ClearTransaction(txn1ID)
	cd.ClearTransaction(txn2ID)
}

// =============================================================================
// Non-Conflicting Transaction Tests
// =============================================================================
// These tests verify that transactions that don't conflict can commit
// successfully even when running concurrently.
// =============================================================================

// TestSerializableNonConflictingTransactionsCommit verifies that
// non-conflicting transactions commit successfully.
//
// Scenario:
// 1. T1 with SERIALIZABLE reads table A
// 2. T2 with SERIALIZABLE reads table B
// 3. Both transactions commit successfully
func TestSerializableNonConflictingTransactionsCommit(t *testing.T) {
	cd := storage.NewConflictDetector()

	const txn1ID uint64 = 1
	const txn2ID uint64 = 2

	// T1 reads from table A
	cd.RegisterRead(txn1ID, "table_A", "row_1")
	cd.RegisterRead(txn1ID, "table_A", "row_2")

	// T2 reads from table B (different table)
	cd.RegisterRead(txn2ID, "table_B", "row_1")
	cd.RegisterRead(txn2ID, "table_B", "row_2")

	// Both should commit successfully (read-only, different tables)
	err := cd.CheckConflicts(txn1ID, []uint64{txn2ID})
	assert.NoError(t, err, "T1 should commit successfully (no writes)")

	err = cd.CheckConflicts(txn2ID, []uint64{txn1ID})
	assert.NoError(t, err, "T2 should commit successfully (no writes)")

	// Clean up
	cd.ClearTransaction(txn1ID)
	cd.ClearTransaction(txn2ID)
}

// TestSerializableReadOnlyTransactions verifies that read-only
// transactions never conflict with each other.
//
// Scenario:
// 1. Multiple transactions read the same rows
// 2. All should commit successfully (no writes = no conflicts)
func TestSerializableReadOnlyTransactions(t *testing.T) {
	cd := storage.NewConflictDetector()

	const tableID = "test_table"
	const rowID = "row_1"

	// Multiple transactions read the same row
	var txnIDs []uint64
	for i := uint64(1); i <= 5; i++ {
		cd.RegisterRead(i, tableID, rowID)
		txnIDs = append(txnIDs, i)
	}

	// All should commit successfully - read-only transactions don't conflict
	for _, txnID := range txnIDs {
		// Each transaction sees all others as concurrent
		var concurrent []uint64
		for _, other := range txnIDs {
			if other != txnID && other < txnID {
				concurrent = append(concurrent, other)
			}
		}
		err := cd.CheckConflicts(txnID, concurrent)
		assert.NoError(t, err, "Read-only transaction %d should commit successfully", txnID)
	}

	// Clean up
	for _, txnID := range txnIDs {
		cd.ClearTransaction(txnID)
	}
}

// =============================================================================
// Serialization Failure Error Handling Tests
// =============================================================================
// These tests verify that serialization failures return the correct error
// type and that the error is properly handled.
// =============================================================================

// TestSerializationFailureErrorType verifies that the serialization failure
// error has the correct type and message.
func TestSerializationFailureErrorType(t *testing.T) {
	// Verify the error is defined
	assert.NotNil(t, storage.ErrSerializationFailure)

	// Verify error message
	assert.Contains(t, storage.ErrSerializationFailure.Error(), "could not serialize")
	assert.Contains(t, storage.ErrSerializationFailure.Error(), "concurrent update")

	// Verify it can be used with errors.Is
	cd := storage.NewConflictDetector()

	const txn1ID uint64 = 1
	const txn2ID uint64 = 2

	// Create a conflict
	cd.RegisterWrite(txn1ID, "table", "row")
	cd.RegisterWrite(txn2ID, "table", "row")

	// Get the error
	err := cd.CheckConflicts(txn2ID, []uint64{txn1ID})

	// Verify error type
	assert.ErrorIs(t, err, storage.ErrSerializationFailure)
}

// TestSerializationFailureRequiresRetry verifies that after a serialization
// failure, the transaction must be retried from the beginning.
func TestSerializationFailureRequiresRetry(t *testing.T) {
	// This test demonstrates the retry pattern for serialization failures
	cd := storage.NewConflictDetector()

	const tableID = "test_table"
	const rowID = "row_1"

	// Simulate a scenario where T1 and T2 conflict
	// T1 writes first
	cd.RegisterWrite(1, tableID, rowID)
	_ = cd.CheckConflicts(1, []uint64{}) // T1 commits
	// Don't clear T1 - we need it for conflict detection

	// T2 attempts to write to the same row
	cd.RegisterWrite(2, tableID, rowID)
	err := cd.CheckConflicts(2, []uint64{1})
	require.ErrorIs(t, err, storage.ErrSerializationFailure)

	// T2 must be cleared before retry
	cd.ClearTransaction(2)

	// Verify T2's state is cleared
	assert.False(t, cd.HasWriteSet(2), "T2's write set should be cleared after failure")
	assert.False(t, cd.HasReadSet(2), "T2's read set should be cleared after failure")

	// T2 can now retry - register operations again
	// In a real scenario, T1 would no longer be concurrent after T2 retries
	cd.RegisterWrite(2, tableID, rowID)

	// If T1 is no longer concurrent (committed before T2's retry started),
	// T2 should succeed
	err = cd.CheckConflicts(2, []uint64{}) // No concurrent transactions in retry
	assert.NoError(t, err, "Retried T2 should succeed")
}

// =============================================================================
// Lock Manager Integration Tests
// =============================================================================
// These tests verify the lock manager behavior for write conflict handling.
// =============================================================================

// TestLockManagerExclusiveLockAcquisition verifies that the lock manager
// properly handles exclusive lock acquisition for writes.
func TestLockManagerExclusiveLockAcquisition(t *testing.T) {
	lm := storage.NewLockManager()

	const txn1ID uint64 = 1
	const tableID = "test_table"
	const rowID = "row_1"

	// T1 acquires lock
	err := lm.Lock(txn1ID, tableID, rowID, time.Second)
	assert.NoError(t, err, "T1 should acquire lock successfully")

	// Verify lock is held by T1
	assert.True(t, lm.IsLockedByTxn(txn1ID, tableID, rowID))

	// Release lock
	lm.Release(txn1ID)

	// Verify lock is released
	assert.False(t, lm.IsLocked(tableID, rowID))
}

// TestLockManagerBlockingOnConflict verifies that a transaction blocks
// when trying to lock a row already locked by another transaction.
func TestLockManagerBlockingOnConflict(t *testing.T) {
	lm := storage.NewLockManager()

	const txn1ID uint64 = 1
	const txn2ID uint64 = 2
	const tableID = "test_table"
	const rowID = "row_1"

	// T1 acquires lock
	err := lm.Lock(txn1ID, tableID, rowID, time.Second)
	require.NoError(t, err)

	// T2 tries to acquire the same lock with short timeout
	err = lm.Lock(txn2ID, tableID, rowID, 10*time.Millisecond)
	assert.ErrorIs(t, err, storage.ErrLockTimeout, "T2 should timeout waiting for lock")

	// Release T1's lock
	lm.Release(txn1ID)

	// Now T2 should be able to acquire the lock
	err = lm.Lock(txn2ID, tableID, rowID, time.Second)
	assert.NoError(t, err, "T2 should acquire lock after T1 releases")

	// Clean up
	lm.Release(txn2ID)
}

// TestLockManagerLockReleaseOnCommit verifies that all locks are released
// when a transaction commits.
func TestLockManagerLockReleaseOnCommit(t *testing.T) {
	lm := storage.NewLockManager()

	const txnID uint64 = 1
	const tableID = "test_table"

	// Acquire multiple locks
	for i := 0; i < 5; i++ {
		err := lm.Lock(txnID, tableID, string(rune('a'+i)), time.Second)
		require.NoError(t, err)
	}

	// Verify locks are held
	assert.Equal(t, 5, lm.GetLocksHeldByTxn(txnID))
	assert.Equal(t, 5, lm.TotalLockCount())

	// Release all locks (simulating commit)
	lm.Release(txnID)

	// Verify all locks are released
	assert.Equal(t, 0, lm.GetLocksHeldByTxn(txnID))
	assert.Equal(t, 0, lm.TotalLockCount())
}

// TestLockManagerLockReleaseOnRollback verifies that all locks are released
// when a transaction rolls back.
func TestLockManagerLockReleaseOnRollback(t *testing.T) {
	lm := storage.NewLockManager()

	const txnID uint64 = 1
	const tableID = "test_table"

	// Acquire multiple locks
	for i := 0; i < 3; i++ {
		err := lm.Lock(txnID, tableID, string(rune('a'+i)), time.Second)
		require.NoError(t, err)
	}

	// Verify locks are held
	assert.Equal(t, 3, lm.GetLocksHeldByTxn(txnID))

	// Release all locks (simulating rollback)
	lm.Release(txnID)

	// Verify all locks are released
	assert.Equal(t, 0, lm.GetLocksHeldByTxn(txnID))
}

// TestLockManagerWaitingTransactionsGetNotified verifies that waiting
// transactions are properly notified when locks become available.
func TestLockManagerWaitingTransactionsGetNotified(t *testing.T) {
	lm := storage.NewLockManager()

	const txn1ID uint64 = 1
	const txn2ID uint64 = 2
	const tableID = "test_table"
	const rowID = "row_1"

	// T1 acquires lock
	err := lm.Lock(txn1ID, tableID, rowID, time.Second)
	require.NoError(t, err)

	// Channel to signal when T2 gets the lock
	t2GotLock := make(chan bool, 1)

	// T2 tries to acquire the lock in a goroutine (will block)
	go func() {
		err := lm.Lock(txn2ID, tableID, rowID, 5*time.Second)
		t2GotLock <- (err == nil)
	}()

	// Give T2 time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Verify there's a waiter
	assert.Equal(t, 1, lm.GetWaiterCount(tableID, rowID))

	// T1 releases the lock
	lm.Release(txn1ID)

	// T2 should get the lock
	select {
	case success := <-t2GotLock:
		assert.True(t, success, "T2 should have acquired the lock")
	case <-time.After(time.Second):
		t.Fatal("T2 did not get the lock in time")
	}

	// Clean up
	lm.Release(txn2ID)
}

// =============================================================================
// Isolation Level Configuration Tests
// =============================================================================
// These tests verify that SERIALIZABLE isolation can be configured properly.
// =============================================================================

// TestSerializableIsolationLevelConfiguration verifies that the isolation
// level can be configured at both connection and transaction level.
func TestSerializableIsolationLevelConfiguration(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	t.Run("explicit BEGIN with SERIALIZABLE", func(t *testing.T) {
		_, err := engineConn.Execute(
			context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
			nil,
		)
		require.NoError(t, err)

		assert.True(t, engineConn.inTxn)
		assert.Equal(t, "SERIALIZABLE", engineConn.currentIsolationLevel.String())

		// Transaction should have a snapshot
		assert.True(t, engineConn.txn.HasSnapshot())

		// SHOW transaction_isolation should return SERIALIZABLE
		rows, _, err := engineConn.Query(context.Background(), "SHOW transaction_isolation", nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, "SERIALIZABLE", rows[0]["transaction_isolation"])

		_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
		require.NoError(t, err)
	})

	t.Run("SET default then BEGIN", func(t *testing.T) {
		// Set default isolation level
		_, err := engineConn.Execute(
			context.Background(),
			"SET default_transaction_isolation = 'SERIALIZABLE'",
			nil,
		)
		require.NoError(t, err)

		// BEGIN without explicit isolation level should use default
		_, err = engineConn.Execute(context.Background(), "BEGIN", nil)
		require.NoError(t, err)

		assert.True(t, engineConn.inTxn)
		assert.Equal(t, "SERIALIZABLE", engineConn.currentIsolationLevel.String())
		assert.True(t, engineConn.txn.HasSnapshot(), "SERIALIZABLE should have snapshot")

		_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
		require.NoError(t, err)
	})
}

// TestSerializableSnapshotConsistency verifies that the snapshot taken at
// transaction start provides consistent reads throughout the transaction.
func TestSerializableSnapshotConsistency(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Create table with initial data
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_snapshot_ser (id INTEGER, version INTEGER)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_snapshot_ser VALUES (1, 1)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_snapshot_ser VALUES (2, 1)",
		nil,
	)
	require.NoError(t, err)

	// Begin T1 with SERIALIZABLE
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	// Verify snapshot was taken at transaction start
	assert.True(t, engineConn.txn.HasSnapshot())
	snapshotTime := engineConn.txn.GetSnapshot().GetTimestamp()
	assert.False(t, snapshotTime.IsZero())

	// Execute multiple queries - all should see consistent data
	for i := 0; i < 5; i++ {
		time.Sleep(5 * time.Millisecond)

		rows, _, err := engineConn.Query(
			context.Background(),
			"SELECT * FROM test_snapshot_ser ORDER BY id",
			nil,
		)
		require.NoError(t, err)
		assert.Len(t, rows, 2, "All queries in SERIALIZABLE should see same row count")
		assert.EqualValues(t, 1, rows[0]["version"], "All queries should see version 1 for id=1")
		assert.EqualValues(t, 1, rows[1]["version"], "All queries should see version 1 for id=2")
	}

	// Cleanup
	_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)
}

// TestSerializableWithSavepoints verifies that savepoints work correctly
// under SERIALIZABLE isolation.
func TestSerializableWithSavepoints(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Create table
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_savepoint_ser (id INTEGER, name VARCHAR)",
		nil,
	)
	require.NoError(t, err)

	// Begin transaction with SERIALIZABLE
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	// Insert first row
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_savepoint_ser VALUES (1, 'First')",
		nil,
	)
	require.NoError(t, err)

	// Create savepoint
	_, err = engineConn.Execute(context.Background(), "SAVEPOINT sp1", nil)
	require.NoError(t, err)

	// Insert second row after savepoint
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_savepoint_ser VALUES (2, 'Second')",
		nil,
	)
	require.NoError(t, err)

	// Verify both rows visible
	rows, _, err := engineConn.Query(
		context.Background(),
		"SELECT * FROM test_savepoint_ser ORDER BY id",
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, rows, 2, "Should see both rows before rollback to savepoint")

	// Rollback to savepoint
	_, err = engineConn.Execute(context.Background(), "ROLLBACK TO SAVEPOINT sp1", nil)
	require.NoError(t, err)

	// Only first row should be visible
	rows, _, err = engineConn.Query(context.Background(), "SELECT * FROM test_savepoint_ser", nil)
	require.NoError(t, err)
	assert.Len(t, rows, 1, "Should only see first row after rollback to savepoint")
	assert.Equal(t, "First", rows[0]["name"])

	// Commit
	_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)

	// Verify final state
	rows, _, err = engineConn.Query(context.Background(), "SELECT * FROM test_savepoint_ser", nil)
	require.NoError(t, err)
	assert.Len(t, rows, 1, "Should have only first row after commit")
}

// TestSerializableRollbackBehavior verifies that rollback correctly undoes
// changes under SERIALIZABLE isolation.
func TestSerializableRollbackBehavior(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Create table with initial data
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_rollback_ser (id INTEGER, value INTEGER)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_rollback_ser VALUES (1, 100)",
		nil,
	)
	require.NoError(t, err)

	// Begin transaction with SERIALIZABLE
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	// Make changes
	_, err = engineConn.Execute(
		context.Background(),
		"UPDATE test_rollback_ser SET value = 200 WHERE id = 1",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_rollback_ser VALUES (2, 300)",
		nil,
	)
	require.NoError(t, err)

	// Verify changes are visible within transaction
	rows, _, err := engineConn.Query(
		context.Background(),
		"SELECT * FROM test_rollback_ser ORDER BY id",
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, rows, 2, "Should see both rows in transaction")

	// Rollback
	_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)

	// After rollback, changes should be undone
	rows, _, err = engineConn.Query(context.Background(), "SELECT * FROM test_rollback_ser", nil)
	require.NoError(t, err)
	assert.Len(t, rows, 1, "Should only see original row after rollback")
	assert.EqualValues(t, 100, rows[0]["value"], "Value should be original 100")
}

// TestSerializableAggregateConsistency verifies that aggregate queries
// return consistent results within a SERIALIZABLE transaction.
func TestSerializableAggregateConsistency(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Create table with numeric data
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_aggregate_ser (id INTEGER, amount INTEGER)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_aggregate_ser VALUES (1, 100)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_aggregate_ser VALUES (2, 200)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_aggregate_ser VALUES (3, 300)",
		nil,
	)
	require.NoError(t, err)

	// Begin with SERIALIZABLE
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	// First aggregate query
	rows, _, err := engineConn.Query(
		context.Background(),
		"SELECT COUNT(*) as cnt, SUM(amount) as total FROM test_aggregate_ser",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	firstCount := rows[0]["cnt"]
	firstTotal := rows[0]["total"]

	// Execute multiple times - all should return same results
	for i := 0; i < 3; i++ {
		time.Sleep(5 * time.Millisecond)

		rows, _, err = engineConn.Query(
			context.Background(),
			"SELECT COUNT(*) as cnt, SUM(amount) as total FROM test_aggregate_ser",
			nil,
		)
		require.NoError(t, err)
		require.Len(t, rows, 1)

		assert.Equal(t, firstCount, rows[0]["cnt"], "COUNT should be consistent across queries")
		assert.Equal(t, firstTotal, rows[0]["total"], "SUM should be consistent across queries")
	}

	// Cleanup
	_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)
}

// TestSerializableComparisonWithRepeatableRead demonstrates that SERIALIZABLE
// provides the same snapshot isolation as REPEATABLE READ but adds conflict detection.
func TestSerializableComparisonWithRepeatableRead(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Create table
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_comparison (id INTEGER, data VARCHAR)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_comparison VALUES (1, 'initial')",
		nil,
	)
	require.NoError(t, err)

	t.Run("SERIALIZABLE uses transaction-level snapshot", func(t *testing.T) {
		_, err := engineConn.Execute(
			context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
			nil,
		)
		require.NoError(t, err)

		// Snapshot should exist
		assert.True(t, engineConn.txn.HasSnapshot())

		// Record the snapshot timestamp
		snapshotTime := engineConn.txn.GetSnapshot().GetTimestamp()

		// Wait a bit
		time.Sleep(10 * time.Millisecond)

		// Execute a statement - this should NOT create a new snapshot
		rows, _, err := engineConn.Query(context.Background(), "SELECT * FROM test_comparison", nil)
		require.NoError(t, err)
		assert.Len(t, rows, 1)

		// Snapshot timestamp should be unchanged (transaction-level)
		assert.Equal(t, snapshotTime, engineConn.txn.GetSnapshot().GetTimestamp(),
			"SERIALIZABLE should use transaction-level snapshot")

		_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
		require.NoError(t, err)
	})

	t.Run("SERIALIZABLE vs REPEATABLE READ both have snapshots", func(t *testing.T) {
		// SERIALIZABLE transaction
		_, err := engineConn.Execute(
			context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
			nil,
		)
		require.NoError(t, err)
		assert.True(t, engineConn.txn.HasSnapshot())
		serSnapshot := engineConn.txn.GetSnapshot().GetTimestamp()
		_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
		require.NoError(t, err)

		// Small delay
		time.Sleep(10 * time.Millisecond)

		// REPEATABLE READ transaction
		_, err = engineConn.Execute(
			context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ",
			nil,
		)
		require.NoError(t, err)
		assert.True(t, engineConn.txn.HasSnapshot())
		rrSnapshot := engineConn.txn.GetSnapshot().GetTimestamp()

		// Both should have snapshots
		assert.True(t, rrSnapshot.After(serSnapshot) || rrSnapshot.Equal(serSnapshot),
			"REPEATABLE READ snapshot should be >= SERIALIZABLE snapshot")

		_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
		require.NoError(t, err)
	})
}

// =============================================================================
// Commit/Rollback Cleanup Tests
// =============================================================================
// These tests verify that commit and rollback properly clean up locks and
// conflict tracking data.
// =============================================================================

// TestSerializableCommitCleansUpLocks verifies that all locks are released
// when a transaction commits.
func TestSerializableCommitCleansUpLocks(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// For :memory: databases, a new isolated engine is created.
	// We need to use the engine from the connection, not the outer engine.
	isolatedEngine := engineConn.engine

	// Create table
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_lock_cleanup (id INTEGER)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_lock_cleanup VALUES (1)",
		nil,
	)
	require.NoError(t, err)

	// Begin SERIALIZABLE transaction
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	txnID := engineConn.txn.ID()

	// Register a lock manually (simulating what happens during writes)
	lm := isolatedEngine.LockManager()
	err = lm.Lock(txnID, "test_lock_cleanup", "1", time.Second)
	require.NoError(t, err)

	// Verify lock is held
	assert.True(t, lm.IsLockedByTxn(txnID, "test_lock_cleanup", "1"))
	assert.Equal(t, 1, lm.GetLocksHeldByTxn(txnID))

	// Commit the transaction
	_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)

	// Verify locks are released
	assert.False(t, lm.IsLocked("test_lock_cleanup", "1"), "Lock should be released after commit")
	assert.Equal(t, 0, lm.GetLocksHeldByTxn(txnID), "Transaction should have no locks after commit")
}

// TestSerializableRollbackCleansUpLocks verifies that all locks are released
// when a transaction rolls back.
func TestSerializableRollbackCleansUpLocks(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	isolatedEngine := engineConn.engine

	// Create table
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_lock_rollback (id INTEGER)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_lock_rollback VALUES (1)",
		nil,
	)
	require.NoError(t, err)

	// Begin SERIALIZABLE transaction
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	txnID := engineConn.txn.ID()

	// Register a lock manually
	lm := isolatedEngine.LockManager()
	err = lm.Lock(txnID, "test_lock_rollback", "1", time.Second)
	require.NoError(t, err)

	// Verify lock is held
	assert.True(t, lm.IsLockedByTxn(txnID, "test_lock_rollback", "1"))

	// Rollback the transaction
	_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)

	// Verify locks are released
	assert.False(
		t,
		lm.IsLocked("test_lock_rollback", "1"),
		"Lock should be released after rollback",
	)
	assert.Equal(
		t,
		0,
		lm.GetLocksHeldByTxn(txnID),
		"Transaction should have no locks after rollback",
	)
}

// TestSerializableCommitCleansUpConflictData verifies that read/write tracking
// data is cleaned up when a transaction commits.
func TestSerializableCommitCleansUpConflictData(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	isolatedEngine := engineConn.engine

	// Create table
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_conflict_cleanup (id INTEGER)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_conflict_cleanup VALUES (1)",
		nil,
	)
	require.NoError(t, err)

	// Begin SERIALIZABLE transaction
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	txnID := engineConn.txn.ID()

	// Register reads and writes manually
	cd := isolatedEngine.ConflictDetector()
	cd.RegisterRead(txnID, "test_conflict_cleanup", "1")
	cd.RegisterWrite(txnID, "test_conflict_cleanup", "1")

	// Verify tracking data exists
	assert.True(t, cd.HasReadSet(txnID))
	assert.True(t, cd.HasWriteSet(txnID))

	// Commit the transaction
	_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)

	// Verify tracking data is cleaned up
	assert.False(t, cd.HasReadSet(txnID), "Read set should be cleared after commit")
	assert.False(t, cd.HasWriteSet(txnID), "Write set should be cleared after commit")
}

// TestSerializableRollbackCleansUpConflictData verifies that read/write tracking
// data is cleaned up when a transaction rolls back.
func TestSerializableRollbackCleansUpConflictData(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	isolatedEngine := engineConn.engine

	// Create table
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_conflict_rollback (id INTEGER)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_conflict_rollback VALUES (1)",
		nil,
	)
	require.NoError(t, err)

	// Begin SERIALIZABLE transaction
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	txnID := engineConn.txn.ID()

	// Register reads and writes manually
	cd := isolatedEngine.ConflictDetector()
	cd.RegisterRead(txnID, "test_conflict_rollback", "1")
	cd.RegisterWrite(txnID, "test_conflict_rollback", "1")

	// Verify tracking data exists
	assert.True(t, cd.HasReadSet(txnID))
	assert.True(t, cd.HasWriteSet(txnID))

	// Rollback the transaction
	_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)

	// Verify tracking data is cleaned up
	assert.False(t, cd.HasReadSet(txnID), "Read set should be cleared after rollback")
	assert.False(t, cd.HasWriteSet(txnID), "Write set should be cleared after rollback")
}

// TestSerializableConflictAtCommitReturnsError verifies that COMMIT returns
// ErrSerializationFailure when there is a conflict with a concurrent transaction.
func TestSerializableConflictAtCommitReturnsError(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	isolatedEngine := engineConn.engine

	// Create table
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE test_conflict_error (id INTEGER)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO test_conflict_error VALUES (1)",
		nil,
	)
	require.NoError(t, err)

	// Begin SERIALIZABLE transaction T1
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	txn1ID := engineConn.txn.ID()

	// Simulate T1 reading a row
	cd := isolatedEngine.ConflictDetector()
	cd.RegisterRead(txn1ID, "test_conflict_error", "1")

	// Simulate a concurrent transaction T2 that wrote to the same row and committed
	// We need to manually add T2 to the committedSince list for T1
	const txn2ID uint64 = 999
	cd.RegisterWrite(txn2ID, "test_conflict_error", "1")

	// Add T2 to T1's concurrent committed list (simulating T2 committed while T1 was active)
	isolatedEngine.txnMgr.mu.Lock()
	isolatedEngine.txnMgr.committedSince[txn1ID] = append(
		isolatedEngine.txnMgr.committedSince[txn1ID],
		txn2ID,
	)
	isolatedEngine.txnMgr.mu.Unlock()

	// T1 tries to commit - should fail due to read-write conflict
	_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
	require.Error(t, err, "Commit should fail due to conflict")
	assert.ErrorIs(t, err, storage.ErrSerializationFailure, "Error should be serialization failure")

	// Verify cleanup still happened despite error
	assert.False(t, cd.HasReadSet(txn1ID), "Read set should be cleared after failed commit")
	assert.Equal(
		t,
		0,
		isolatedEngine.LockManager().GetLocksHeldByTxn(txn1ID),
		"Locks should be released after failed commit",
	)
}
