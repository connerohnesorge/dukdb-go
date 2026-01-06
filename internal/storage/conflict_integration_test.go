package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/coder/quartz"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// End-to-End Integration Tests for Write-Write Conflict Detection
// =============================================================================
//
// These tests verify the conflict detection mechanism for SERIALIZABLE isolation
// level. They use the ConflictDetector to track read/write sets and verify that
// conflicts are properly detected at commit time.
//
// The tests simulate real-world scenarios using:
//   - ConflictDetector for tracking read/write sets
//   - testMVCCManager (from snapshot_isolation_test.go) for transaction management
//   - Versioned table operations for simulating database operations
//
// Key scenarios tested:
//   1. No conflict with disjoint writes (different rows)
//   2. Write-write conflict detection (same row)
//   3. First committer wins semantics
//   4. Read-only transaction no conflict
//   5. Read-write conflict detection

// =============================================================================
// Test 1: No Conflict With Disjoint Writes
// =============================================================================

// TestConflict_NoConflictWithDisjointWrites verifies that transactions updating
// different rows can both commit successfully without conflict.
//
// Scenario:
//   - T1 updates row A
//   - T2 updates row B
//   - Both T1 and T2 commit
//   - Both commits should succeed (no conflict on different rows)
func TestConflict_NoConflictWithDisjointWrites(t *testing.T) {
	// Setup: Create mock clock, MVCC manager, and conflict detector
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)
	conflictDetector := NewConflictDetector()

	// Create a test table with two rows
	table := NewTable("test_disjoint", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Pre-populate table with two rows (committed by a setup transaction)
	mockClock.Advance(time.Millisecond)
	t0 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	rowA, err := table.InsertVersioned(t0, []any{int32(1), "A"})
	require.NoError(t, err)
	rowB, err := table.InsertVersioned(t0, []any{int32(2), "B"})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t0)
	require.NoError(t, err)
	table.CommitVersions(t0, t0.GetCommitTS())

	// Step 1: T1 starts
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t1ID := t1.ID()

	// Step 2: T2 starts
	mockClock.Advance(time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t2ID := t2.ID()

	// Step 3: T1 updates row A
	err = table.UpdateVersioned(t1, rowA, []any{int32(10), "A_updated"})
	require.NoError(t, err)
	// Register write in conflict detector
	conflictDetector.RegisterWrite(t1ID, "test_disjoint", fmt.Sprintf("%d", rowA))

	// Step 4: T2 updates row B (different row)
	err = table.UpdateVersioned(t2, rowB, []any{int32(20), "B_updated"})
	require.NoError(t, err)
	// Register write in conflict detector
	conflictDetector.RegisterWrite(t2ID, "test_disjoint", fmt.Sprintf("%d", rowB))

	// Step 5: T1 commits - should succeed
	// No concurrent committed transactions yet
	err = conflictDetector.CheckConflicts(t1ID, []uint64{})
	assert.NoError(t, err, "T1 should commit without conflict")

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t1)
	require.NoError(t, err)
	table.CommitVersions(t1, t1.GetCommitTS())

	// Step 6: T2 commits - should succeed (T1 committed but touched different row)
	// T1 is now a concurrent committed transaction
	err = conflictDetector.CheckConflicts(t2ID, []uint64{t1ID})
	assert.NoError(t, err, "T2 should commit without conflict (disjoint writes)")

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t2)
	require.NoError(t, err)
	table.CommitVersions(t2, t2.GetCommitTS())

	// Verify both updates are visible in a new transaction
	mockClock.Advance(time.Millisecond)
	t3 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)

	valuesA, err := table.ReadVersioned(t3, rowA)
	require.NoError(t, err)
	assert.Equal(t, int32(10), valuesA[0], "Row A should have T1's update")
	assert.Equal(t, "A_updated", valuesA[1])

	valuesB, err := table.ReadVersioned(t3, rowB)
	require.NoError(t, err)
	assert.Equal(t, int32(20), valuesB[0], "Row B should have T2's update")
	assert.Equal(t, "B_updated", valuesB[1])

	// Cleanup
	conflictDetector.ClearTransaction(t1ID)
	conflictDetector.ClearTransaction(t2ID)
	err = mvccMgr.Commit(t3)
	require.NoError(t, err)
}

// =============================================================================
// Test 2: Write-Write Conflict Detected
// =============================================================================

// TestConflict_WriteWriteConflictDetected verifies that a transaction attempting
// to commit after another transaction has modified the same row is rejected.
//
// Scenario:
//   - T1 starts at time 100
//   - T2 starts at time 110
//   - T2 updates row A and commits at time 120
//   - T1 updates row A and attempts to commit at time 130
//   - T1's commit should fail with ErrSerializationFailure
func TestConflict_WriteWriteConflictDetected(t *testing.T) {
	// Setup
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)
	conflictDetector := NewConflictDetector()

	// Create a test table with one row
	table := NewTable("test_ww_conflict", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Pre-populate table with row A
	mockClock.Advance(time.Millisecond)
	t0 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	rowA, err := table.InsertVersioned(t0, []any{int32(1), "initial"})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t0)
	require.NoError(t, err)
	table.CommitVersions(t0, t0.GetCommitTS())

	// Step 1: T1 starts at time ~100 (simulated by advancing clock)
	mockClock.Advance(100 * time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t1ID := t1.ID()

	// Step 2: T2 starts at time ~110
	mockClock.Advance(10 * time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t2ID := t2.ID()

	// Step 3: T2 updates row A and commits at time ~120
	err = table.UpdateVersioned(t2, rowA, []any{int32(2), "T2_update"})
	require.NoError(t, err)
	conflictDetector.RegisterWrite(t2ID, "test_ww_conflict", fmt.Sprintf("%d", rowA))

	// T2 commits - no concurrent committed transactions
	err = conflictDetector.CheckConflicts(t2ID, []uint64{})
	assert.NoError(t, err, "T2 should commit first without conflict")

	mockClock.Advance(10 * time.Millisecond) // time ~120
	err = mvccMgr.Commit(t2)
	require.NoError(t, err)
	table.CommitVersions(t2, t2.GetCommitTS())

	// Step 4: T1 updates row A (same row as T2)
	// Note: In a real system, T1 would not be able to create a version here because
	// T2 already committed changes to this row. But for testing conflict detection,
	// we register the write intent and check conflicts before committing.
	conflictDetector.RegisterWrite(t1ID, "test_ww_conflict", fmt.Sprintf("%d", rowA))

	// Step 5: T1 attempts to commit at time ~130
	// T2 is now a concurrent committed transaction that modified the same row
	mockClock.Advance(10 * time.Millisecond) // time ~130
	err = conflictDetector.CheckConflicts(t1ID, []uint64{t2ID})
	assert.Error(t, err, "T1 should fail with serialization error")
	assert.ErrorIs(t, err, ErrSerializationFailure, "Error should be ErrSerializationFailure")

	// T1 must rollback due to conflict - verified by error above
	// The key assertion is that CheckConflicts properly detected the conflict

	// Cleanup
	conflictDetector.ClearTransaction(t1ID)
	conflictDetector.ClearTransaction(t2ID)
}

// =============================================================================
// Test 3: First Committer Wins
// =============================================================================

// TestConflict_FirstCommitterWins verifies that when two transactions update
// the same row, the first one to commit succeeds and the second one fails.
//
// Scenario:
//   - T1 and T2 both start
//   - Both T1 and T2 want to update row A
//   - T1 commits first - should succeed
//   - T2's commit should fail
func TestConflict_FirstCommitterWins(t *testing.T) {
	// Setup
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)
	conflictDetector := NewConflictDetector()

	// Create a test table with one row
	table := NewTable("test_first_wins", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Pre-populate table with row A
	mockClock.Advance(time.Millisecond)
	t0 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	rowA, err := table.InsertVersioned(t0, []any{int32(0), "original"})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t0)
	require.NoError(t, err)
	table.CommitVersions(t0, t0.GetCommitTS())

	// Step 1: T1 and T2 start concurrently
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t1ID := t1.ID()

	mockClock.Advance(time.Microsecond) // T2 starts slightly after T1
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t2ID := t2.ID()

	// Step 2: Both T1 and T2 register write intents for row A
	// (In a real system, this would be tracked when they attempt to update)
	conflictDetector.RegisterWrite(t1ID, "test_first_wins", fmt.Sprintf("%d", rowA))
	conflictDetector.RegisterWrite(t2ID, "test_first_wins", fmt.Sprintf("%d", rowA))

	// Step 3: T1 commits first - should succeed (no concurrent committed yet)
	err = conflictDetector.CheckConflicts(t1ID, []uint64{})
	assert.NoError(t, err, "T1 should commit successfully (first committer)")

	// T1 actually performs the update and commits
	err = table.UpdateVersioned(t1, rowA, []any{int32(1), "T1_value"})
	require.NoError(t, err)

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t1)
	require.NoError(t, err)
	table.CommitVersions(t1, t1.GetCommitTS())

	// Step 4: T2's commit should fail (T1 committed first and modified same row)
	err = conflictDetector.CheckConflicts(t2ID, []uint64{t1ID})
	assert.Error(t, err, "T2 should fail (conflict with T1)")
	assert.ErrorIs(t, err, ErrSerializationFailure)

	// T2 must rollback - verified by the error above
	// The key assertion is that the first committer wins, second fails

	// Cleanup
	conflictDetector.ClearTransaction(t1ID)
	conflictDetector.ClearTransaction(t2ID)
}

// =============================================================================
// Test 4: Read-Only Transaction No Conflict
// =============================================================================

// TestConflict_ReadOnlyTransactionNoConflict verifies that a read-only transaction
// does not conflict with write transactions, even if they modify the same rows.
//
// Scenario:
//   - T1 starts (read-only, only reads rows)
//   - T2 modifies those rows and commits
//   - T1 commits - should succeed (read-only has no write conflicts)
func TestConflict_ReadOnlyTransactionNoConflict(t *testing.T) {
	// Setup
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)
	conflictDetector := NewConflictDetector()

	// Create a test table with one row
	table := NewTable("test_readonly", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Pre-populate table with row A
	mockClock.Advance(time.Millisecond)
	t0 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	rowA, err := table.InsertVersioned(t0, []any{int32(100), "initial"})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t0)
	require.NoError(t, err)
	table.CommitVersions(t0, t0.GetCommitTS())

	// Step 1: T1 starts (will be read-only)
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t1ID := t1.ID()

	// Step 2: T1 reads row A (only reads, no writes)
	values, err := table.ReadVersioned(t1, rowA)
	require.NoError(t, err)
	assert.Equal(t, int32(100), values[0])
	// Note: A read-only transaction does NOT register reads in conflict detector
	// because it has no writes to conflict with.
	// The conflict detector only checks writes against writes.

	// Step 3: T2 starts and modifies row A
	mockClock.Advance(time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t2ID := t2.ID()

	err = table.UpdateVersioned(t2, rowA, []any{int32(200), "T2_modified"})
	require.NoError(t, err)
	conflictDetector.RegisterWrite(t2ID, "test_readonly", fmt.Sprintf("%d", rowA))

	// T2 commits
	err = conflictDetector.CheckConflicts(t2ID, []uint64{})
	assert.NoError(t, err)

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t2)
	require.NoError(t, err)
	table.CommitVersions(t2, t2.GetCommitTS())

	// Step 4: T1 commits - should succeed
	// T1 is read-only (no write set), so no write-write conflicts possible
	err = conflictDetector.CheckConflicts(t1ID, []uint64{t2ID})
	assert.NoError(t, err, "Read-only T1 should commit without conflict")

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t1)
	require.NoError(t, err)

	// Cleanup
	conflictDetector.ClearTransaction(t1ID)
	conflictDetector.ClearTransaction(t2ID)
}

// =============================================================================
// Test 5: Read-Write Conflict
// =============================================================================

// TestConflict_ReadWriteConflict verifies that when a transaction reads a row
// and another concurrent transaction modifies it, the reading transaction
// fails at commit time due to read-write conflict.
//
// Scenario:
//   - T1 starts and reads row A
//   - T2 modifies row A and commits
//   - T1 modifies row A and tries to commit
//   - T1's commit should fail (read-write conflict)
func TestConflict_ReadWriteConflict(t *testing.T) {
	// Setup
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)
	conflictDetector := NewConflictDetector()

	// Create a test table with one row
	table := NewTable("test_rw_conflict", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Pre-populate table with row A
	mockClock.Advance(time.Millisecond)
	t0 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	rowA, err := table.InsertVersioned(t0, []any{int32(1), "initial"})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t0)
	require.NoError(t, err)
	table.CommitVersions(t0, t0.GetCommitTS())

	// Step 1: T1 starts and reads row A
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t1ID := t1.ID()

	// T1 reads row A - register the read
	_, err = table.ReadVersioned(t1, rowA)
	require.NoError(t, err)
	conflictDetector.RegisterRead(t1ID, "test_rw_conflict", fmt.Sprintf("%d", rowA))

	// Step 2: T2 starts and modifies row A
	mockClock.Advance(time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t2ID := t2.ID()

	err = table.UpdateVersioned(t2, rowA, []any{int32(2), "T2_update"})
	require.NoError(t, err)
	conflictDetector.RegisterWrite(t2ID, "test_rw_conflict", fmt.Sprintf("%d", rowA))

	// T2 commits (no concurrent committed transactions)
	err = conflictDetector.CheckConflicts(t2ID, []uint64{})
	assert.NoError(t, err)

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t2)
	require.NoError(t, err)
	table.CommitVersions(t2, t2.GetCommitTS())

	// Step 3: T1 wants to modify row A (after reading it earlier)
	// Register the write intent
	conflictDetector.RegisterWrite(t1ID, "test_rw_conflict", fmt.Sprintf("%d", rowA))

	// Step 4: T1 attempts to commit - should fail with read-write conflict
	// T1 read row A, but T2 (which committed) also wrote to row A
	err = conflictDetector.CheckConflicts(t1ID, []uint64{t2ID})
	assert.Error(t, err, "T1 should fail with read-write conflict")
	assert.ErrorIs(t, err, ErrSerializationFailure)

	// T1 must rollback - verified by the error above
	// The key assertion is that the read-write conflict was properly detected

	// Cleanup
	conflictDetector.ClearTransaction(t1ID)
	conflictDetector.ClearTransaction(t2ID)
}

// =============================================================================
// Additional Integration Tests for Edge Cases
// =============================================================================

// TestConflict_MultipleRowsPartialConflict tests that a conflict is detected
// when transactions have partial overlap in their write sets.
func TestConflict_MultipleRowsPartialConflict(t *testing.T) {
	// Setup
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)
	conflictDetector := NewConflictDetector()

	// Create a test table with multiple rows
	table := NewTable("test_partial", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Pre-populate table with rows A, B, C
	mockClock.Advance(time.Millisecond)
	t0 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	rowA, _ := table.InsertVersioned(t0, []any{int32(1)})
	rowB, _ := table.InsertVersioned(t0, []any{int32(2)})
	rowC, _ := table.InsertVersioned(t0, []any{int32(3)})
	mockClock.Advance(time.Millisecond)
	err := mvccMgr.Commit(t0)
	require.NoError(t, err)
	table.CommitVersions(t0, t0.GetCommitTS())

	// T1 and T2 start
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t1ID := t1.ID()

	mockClock.Advance(time.Microsecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t2ID := t2.ID()

	// T1 updates rows A and B - register write intents
	conflictDetector.RegisterWrite(t1ID, "test_partial", fmt.Sprintf("%d", rowA))
	conflictDetector.RegisterWrite(t1ID, "test_partial", fmt.Sprintf("%d", rowB))

	// T2 updates rows B and C (overlap on B) - register write intents
	conflictDetector.RegisterWrite(t2ID, "test_partial", fmt.Sprintf("%d", rowB))
	conflictDetector.RegisterWrite(t2ID, "test_partial", fmt.Sprintf("%d", rowC))

	// T1 commits first - success
	err = conflictDetector.CheckConflicts(t1ID, []uint64{})
	assert.NoError(t, err)

	// T1 actually performs updates and commits
	err = table.UpdateVersioned(t1, rowA, []any{int32(10)})
	require.NoError(t, err)
	err = table.UpdateVersioned(t1, rowB, []any{int32(20)})
	require.NoError(t, err)

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t1)
	require.NoError(t, err)
	table.CommitVersions(t1, t1.GetCommitTS())

	// T2 tries to commit - should fail on row B conflict
	err = conflictDetector.CheckConflicts(t2ID, []uint64{t1ID})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrSerializationFailure)

	// Cleanup
	conflictDetector.ClearTransaction(t1ID)
	conflictDetector.ClearTransaction(t2ID)
}

// TestConflict_ReadSetOnlyNoWriteConflict tests that a transaction with only
// reads does not have write-write conflicts (only read-write if applicable).
func TestConflict_ReadSetOnlyNoWriteConflict(t *testing.T) {
	// Setup
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)
	conflictDetector := NewConflictDetector()

	// Create a test table
	table := NewTable("test_read_only", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Pre-populate
	mockClock.Advance(time.Millisecond)
	t0 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	rowA, err := table.InsertVersioned(t0, []any{int32(1)})
	require.NoError(t, err)
	rowB, err := table.InsertVersioned(t0, []any{int32(2)})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t0)
	require.NoError(t, err)
	table.CommitVersions(t0, t0.GetCommitTS())

	// T1 starts and reads both rows
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t1ID := t1.ID()

	_, err = table.ReadVersioned(t1, rowA)
	require.NoError(t, err)
	conflictDetector.RegisterRead(t1ID, "test_read_only", fmt.Sprintf("%d", rowA))
	_, err = table.ReadVersioned(t1, rowB)
	require.NoError(t, err)
	conflictDetector.RegisterRead(t1ID, "test_read_only", fmt.Sprintf("%d", rowB))

	// T2 starts and writes to a different row (rowC)
	mockClock.Advance(time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t2ID := t2.ID()

	rowC, err := table.InsertVersioned(t2, []any{int32(3)})
	require.NoError(t, err)
	conflictDetector.RegisterWrite(t2ID, "test_read_only", fmt.Sprintf("%d", rowC))

	// T2 commits
	err = conflictDetector.CheckConflicts(t2ID, []uint64{})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t2)
	require.NoError(t, err)
	table.CommitVersions(t2, t2.GetCommitTS())

	// T1 commits - should succeed (read set only, no overlap with T2's writes)
	err = conflictDetector.CheckConflicts(t1ID, []uint64{t2ID})
	assert.NoError(t, err, "T1 with only reads should not conflict with T2's writes to different rows")

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t1)
	require.NoError(t, err)

	// Cleanup
	conflictDetector.ClearTransaction(t1ID)
	conflictDetector.ClearTransaction(t2ID)
}

// TestConflict_SerializableRetryScenario tests the full retry scenario for
// serialization failures, simulating how an application would handle conflicts.
func TestConflict_SerializableRetryScenario(t *testing.T) {
	// Setup
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)
	conflictDetector := NewConflictDetector()

	// Create a test table
	table := NewTable("test_retry", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Pre-populate
	mockClock.Advance(time.Millisecond)
	t0 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	rowA, err := table.InsertVersioned(t0, []any{int32(100)})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t0)
	require.NoError(t, err)
	table.CommitVersions(t0, t0.GetCommitTS())

	// Simulate concurrent update scenario with retry

	// First attempt: T1 and T2 both try to update the same row
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t1ID := t1.ID()

	mockClock.Advance(time.Microsecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t2ID := t2.ID()

	// Both read the current value and register reads
	_, err = table.ReadVersioned(t1, rowA)
	require.NoError(t, err)
	conflictDetector.RegisterRead(t1ID, "test_retry", fmt.Sprintf("%d", rowA))

	_, err = table.ReadVersioned(t2, rowA)
	require.NoError(t, err)
	conflictDetector.RegisterRead(t2ID, "test_retry", fmt.Sprintf("%d", rowA))

	// Both register write intents
	conflictDetector.RegisterWrite(t1ID, "test_retry", fmt.Sprintf("%d", rowA))
	conflictDetector.RegisterWrite(t2ID, "test_retry", fmt.Sprintf("%d", rowA))

	// T1 passes conflict check (no concurrent committed transactions)
	err = conflictDetector.CheckConflicts(t1ID, []uint64{})
	require.NoError(t, err, "T1 should pass conflict check (first committer)")

	// T1 actually updates and commits
	err = table.UpdateVersioned(t1, rowA, []any{int32(101)})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t1)
	require.NoError(t, err)
	table.CommitVersions(t1, t1.GetCommitTS())

	// T2 fails conflict check
	err = conflictDetector.CheckConflicts(t2ID, []uint64{t1ID})
	assert.ErrorIs(t, err, ErrSerializationFailure, "T2 should fail (conflict with T1)")
	conflictDetector.ClearTransaction(t2ID)

	// Retry: T2 starts again (new transaction)
	mockClock.Advance(time.Millisecond)
	t2Retry := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t2RetryID := t2Retry.ID()

	// Register read and write intents for the retry
	conflictDetector.RegisterRead(t2RetryID, "test_retry", fmt.Sprintf("%d", rowA))
	conflictDetector.RegisterWrite(t2RetryID, "test_retry", fmt.Sprintf("%d", rowA))

	// Retry should succeed (T1 is no longer concurrent - it committed before T2Retry started)
	// Note: T1 is NOT passed in as a concurrent transaction because T2Retry started AFTER T1 committed
	err = conflictDetector.CheckConflicts(t2RetryID, []uint64{})
	assert.NoError(t, err, "Retried transaction should succeed (no concurrent transactions)")

	// T2Retry actually updates and commits
	err = table.UpdateVersioned(t2Retry, rowA, []any{int32(102)})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t2Retry)
	require.NoError(t, err)
	table.CommitVersions(t2Retry, t2Retry.GetCommitTS())

	// Cleanup
	conflictDetector.ClearTransaction(t1ID)
	conflictDetector.ClearTransaction(t2RetryID)
}
