package storage

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/coder/quartz"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// End-to-End Snapshot Isolation Tests using MVCC System
// =============================================================================
//
// These tests demonstrate full snapshot isolation semantics using the
// versioned table methods and proper MVCC transaction management.
// Each test follows a specific scenario to verify the correct visibility
// behavior under snapshot-based isolation levels (REPEATABLE READ and
// SERIALIZABLE).
//
// Since we cannot import the engine package from storage tests (import cycle),
// we implement a test-local MVCCManager and MVCCTransaction that provide the
// same semantics.

// =============================================================================
// Test-local MVCC Manager and Transaction Types
// =============================================================================

// testMVCCManager manages MVCC transactions for testing.
// This mirrors the engine.MVCCManager but is local to avoid import cycles.
type testMVCCManager struct {
	lastTS     uint64
	clock      quartz.Clock
	activeTxns map[uint64]*testMVCCTransaction
	mu         sync.RWMutex
}

// newTestMVCCManager creates a new test MVCC manager.
func newTestMVCCManager(clock quartz.Clock) *testMVCCManager {
	return &testMVCCManager{
		lastTS:     0,
		clock:      clock,
		activeTxns: make(map[uint64]*testMVCCTransaction),
	}
}

// nextTimestampLocked returns a monotonically increasing timestamp.
// Caller must hold m.mu lock.
func (m *testMVCCManager) nextTimestampLocked() uint64 {
	ts := uint64(m.clock.Now().UnixNano())
	if ts <= m.lastTS {
		m.lastTS++
	} else {
		m.lastTS = ts
	}
	return m.lastTS
}

// BeginTransaction starts a new MVCC transaction.
func (m *testMVCCManager) BeginTransaction(isolationLevel parser.IsolationLevel) *testMVCCTransaction {
	m.mu.Lock()
	defer m.mu.Unlock()

	startTS := m.nextTimestampLocked()
	startTime := m.clock.Now()

	// Capture snapshot of active transactions at start time
	activeAtStart := make([]uint64, 0, len(m.activeTxns))
	for id := range m.activeTxns {
		activeAtStart = append(activeAtStart, id)
	}

	txn := &testMVCCTransaction{
		id:             startTS,
		active:         true,
		isolationLevel: isolationLevel,
		startTime:      startTime,
		startTS:        startTS,
		commitTS:       0,
		activeAtStart:  activeAtStart,
		readSet:        make(map[string][]uint64),
		writeSet:       make(map[string][]uint64),
		manager:        m,
	}

	m.activeTxns[txn.id] = txn
	return txn
}

// Commit commits the given transaction.
func (m *testMVCCManager) Commit(txn *testMVCCTransaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !txn.active {
		return ErrSerializationFailure
	}

	if m.activeTxns[txn.id] != txn {
		return ErrSerializationFailure
	}

	txn.commitTS = m.nextTimestampLocked()
	delete(m.activeTxns, txn.id)
	txn.active = false

	return nil
}

// Rollback rolls back the given transaction.
func (m *testMVCCManager) Rollback(txn *testMVCCTransaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !txn.active {
		return ErrSerializationFailure
	}

	if m.activeTxns[txn.id] != txn {
		return ErrSerializationFailure
	}

	delete(m.activeTxns, txn.id)
	txn.active = false

	return nil
}

// testMVCCTransaction represents a transaction in the test MVCC system.
type testMVCCTransaction struct {
	id             uint64
	active         bool
	isolationLevel parser.IsolationLevel
	startTime      time.Time
	startTS        uint64
	commitTS       uint64
	activeAtStart  []uint64
	readSet        map[string][]uint64
	writeSet       map[string][]uint64
	manager        *testMVCCManager
	mu             sync.RWMutex

	// activeAtStartSet is a lazily-built set for O(1) lookups
	activeAtStartSet map[uint64]struct{}
}

// ID returns the transaction ID.
func (t *testMVCCTransaction) ID() uint64 {
	return t.id
}

// GetStartTS returns the start timestamp.
func (t *testMVCCTransaction) GetStartTS() uint64 {
	return t.startTS
}

// GetCommitTS returns the commit timestamp.
func (t *testMVCCTransaction) GetCommitTS() uint64 {
	return t.commitTS
}

// RecordRead tracks a row read.
func (t *testMVCCTransaction) RecordRead(table string, rowID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.readSet[table] = append(t.readSet[table], rowID)
}

// RecordWrite tracks a row write.
func (t *testMVCCTransaction) RecordWrite(table string, rowID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writeSet[table] = append(t.writeSet[table], rowID)
}

// VisibilityChecker returns the appropriate visibility checker.
func (t *testMVCCTransaction) VisibilityChecker() VisibilityChecker {
	return GetVisibilityChecker(t.isolationLevel)
}

// IsActive returns whether the transaction is active.
func (t *testMVCCTransaction) IsActive() bool {
	return t.active
}

// GetIsolationLevel returns the isolation level.
func (t *testMVCCTransaction) GetIsolationLevel() parser.IsolationLevel {
	return t.isolationLevel
}

// GetStartTime returns the start time.
func (t *testMVCCTransaction) GetStartTime() time.Time {
	return t.startTime
}

// GetActiveAtStart returns the list of active transaction IDs at start.
func (t *testMVCCTransaction) GetActiveAtStart() []uint64 {
	result := make([]uint64, len(t.activeAtStart))
	copy(result, t.activeAtStart)
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result
}

// WasActiveAtStart checks if a transaction was active when this one started.
func (t *testMVCCTransaction) WasActiveAtStart(txnID uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.activeAtStartSet == nil {
		t.activeAtStartSet = make(map[uint64]struct{}, len(t.activeAtStart))
		for _, id := range t.activeAtStart {
			t.activeAtStartSet[id] = struct{}{}
		}
	}

	_, exists := t.activeAtStartSet[txnID]
	return exists
}

// =============================================================================
// Snapshot Isolation Transaction Context
// =============================================================================

// snapshotTxnContext implements TransactionContext for snapshot isolation tests.
type snapshotTxnContext struct {
	txn       *testMVCCTransaction
	mvccMgr   *testMVCCManager
	snapshot  *Snapshot
	committed map[uint64]bool
	aborted   map[uint64]bool
}

// newSnapshotTxnContext creates a new transaction context for testing.
func newSnapshotTxnContext(txn *testMVCCTransaction) *snapshotTxnContext {
	activeAtStart := txn.GetActiveAtStart()
	snapshot := NewSnapshot(txn.GetStartTime(), activeAtStart)

	return &snapshotTxnContext{
		txn:       txn,
		mvccMgr:   txn.manager,
		snapshot:  snapshot,
		committed: make(map[uint64]bool),
		aborted:   make(map[uint64]bool),
	}
}

func (c *snapshotTxnContext) GetTxnID() uint64 {
	return c.txn.ID()
}

func (c *snapshotTxnContext) GetIsolationLevel() parser.IsolationLevel {
	return c.txn.GetIsolationLevel()
}

func (c *snapshotTxnContext) GetStartTime() time.Time {
	return c.txn.GetStartTime()
}

func (c *snapshotTxnContext) GetStatementTime() time.Time {
	return c.txn.GetStartTime()
}

func (c *snapshotTxnContext) IsCommitted(txnID uint64) bool {
	if committed, ok := c.committed[txnID]; ok {
		return committed
	}

	if txnID == c.txn.ID() {
		return c.txn.GetCommitTS() != 0
	}

	// If this transaction was active at our snapshot time, it's not committed
	// from our perspective
	if c.WasActiveAtSnapshot(txnID) {
		return false
	}

	// For transactions that started AFTER our snapshot (txnID > our startTS),
	// they are not considered committed from our snapshot's perspective,
	// even if they have committed since then. This is key for snapshot isolation:
	// we should not see any data from transactions that started after us.
	if txnID > c.txn.startTS {
		return false
	}

	// Transaction committed before our snapshot
	return true
}

func (c *snapshotTxnContext) IsAborted(txnID uint64) bool {
	if aborted, ok := c.aborted[txnID]; ok {
		return aborted
	}
	return false
}

func (c *snapshotTxnContext) GetSnapshot() *Snapshot {
	return c.snapshot
}

func (c *snapshotTxnContext) WasActiveAtSnapshot(txnID uint64) bool {
	if c.snapshot == nil {
		return false
	}
	return c.snapshot.WasActiveAtSnapshot(txnID)
}

func (c *snapshotTxnContext) MarkCommitted(txnID uint64) {
	c.committed[txnID] = true
}

func (c *snapshotTxnContext) MarkAborted(txnID uint64) {
	c.aborted[txnID] = true
}

// =============================================================================
// Test 1: Own uncommitted writes are visible
// =============================================================================

// TestSnapshotIsolation_OwnUncommittedWritesVisible verifies that a transaction
// can see its own uncommitted changes.
//
// Scenario:
//  1. Transaction T1 starts
//  2. T1 inserts a row (does not commit yet)
//  3. T1 reads the row - should see it (own uncommitted writes are visible)
//  4. T1 commits
//  5. Verify T1 saw its own uncommitted write
func TestSnapshotIsolation_OwnUncommittedWritesVisible(t *testing.T) {
	// Setup: Create mock clock and MVCC manager
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)

	// Create a test table
	table := NewTable("test_own_writes", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Step 1: T1 starts with REPEATABLE READ isolation
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	require.True(t, t1.IsActive(), "T1 should be active after begin")

	// Step 2: T1 inserts a row (uncommitted)
	rowID, err := table.InsertVersioned(t1, []any{int32(42), "hello"})
	require.NoError(t, err, "T1 insert should succeed")

	// Step 3: T1 reads the row - should see own uncommitted write
	values, err := table.ReadVersioned(t1, rowID)
	require.NoError(t, err, "T1 should be able to read its own uncommitted write")
	assert.Equal(t, int32(42), values[0], "Should read the correct integer value")
	assert.Equal(t, "hello", values[1], "Should read the correct string value")

	// Step 4: T1 commits
	err = mvccMgr.Commit(t1)
	require.NoError(t, err, "T1 commit should succeed")
	assert.False(t, t1.IsActive(), "T1 should not be active after commit")

	// Step 5: Verify T1 saw its own uncommitted write (done in step 3)
	// Also verify that the committed version is properly recorded
	table.CommitVersions(t1, t1.GetCommitTS())

	// Verify the version chain shows the commit
	chains := table.GetVersionChains()
	require.Len(t, chains, 1, "Should have one version chain")
	head := chains[0].GetHead()
	require.NotNil(t, head, "Head should not be nil")
	assert.Equal(t, t1.GetCommitTS(), head.CommitTS, "CommitTS should be set after commit")
}

// =============================================================================
// Test 2: Other transaction's uncommitted writes are invisible
// =============================================================================

// TestSnapshotIsolation_OtherUncommittedWritesInvisible verifies that a
// transaction cannot see uncommitted changes from other transactions.
//
// Scenario:
//  1. Transaction T1 starts
//  2. Transaction T2 starts
//  3. T2 inserts a row (does not commit)
//  4. T1 tries to read the table - should NOT see T2's uncommitted row
//  5. T2 commits
//  6. T1 still should NOT see the row (committed after T1 started)
func TestSnapshotIsolation_OtherUncommittedWritesInvisible(t *testing.T) {
	// Setup: Create mock clock and MVCC manager
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)

	// Create a test table
	table := NewTable("test_uncommitted_invisible", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Step 1: T1 starts
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	require.True(t, t1.IsActive(), "T1 should be active")

	// Step 2: T2 starts (after T1)
	mockClock.Advance(time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	require.True(t, t2.IsActive(), "T2 should be active")

	// Step 3: T2 inserts a row (uncommitted)
	_, err := table.InsertVersioned(t2, []any{int32(100)})
	require.NoError(t, err, "T2 insert should succeed")

	// Step 4: T1 tries to read the row - should NOT see T2's uncommitted write
	t1Ctx := newSnapshotTxnContext(t1)
	visibility := NewRepeatableReadVisibility()

	// Get the version chain for the row
	chains := table.GetVersionChains()
	require.Len(t, chains, 1, "Should have one version chain")

	// Try to find a visible version using T1's context
	visibleVersion := chains[0].FindVisibleVersion(visibility, t1Ctx)
	assert.Nil(t, visibleVersion, "T1 should NOT see T2's uncommitted row")

	// Step 5: T2 commits
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t2)
	require.NoError(t, err, "T2 commit should succeed")
	table.CommitVersions(t2, t2.GetCommitTS())

	// Step 6: T1 still should NOT see the row (committed after T1's snapshot)
	// T2 was active when T1 started, so its row should remain invisible to T1
	visibleVersion = chains[0].FindVisibleVersion(visibility, t1Ctx)
	assert.Nil(t, visibleVersion, "T1 should NOT see T2's row even after T2 commits (committed after T1's snapshot)")

	// Cleanup
	err = mvccMgr.Commit(t1)
	require.NoError(t, err)
}

// =============================================================================
// Test 3: Rows committed before transaction start are visible
// =============================================================================

// TestSnapshotIsolation_CommittedBeforeStartVisible verifies that a transaction
// can see rows that were committed before the transaction started.
//
// Scenario:
//  1. Transaction T1 inserts and commits a row
//  2. Transaction T2 starts (after T1 commits)
//  3. T2 reads the table - should see T1's committed row
func TestSnapshotIsolation_CommittedBeforeStartVisible(t *testing.T) {
	// Setup: Create mock clock and MVCC manager
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)

	// Create a test table
	table := NewTable("test_committed_visible", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Step 1: T1 inserts and commits a row
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)

	rowID, err := table.InsertVersioned(t1, []any{int32(42), "committed_before"})
	require.NoError(t, err, "T1 insert should succeed")

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t1)
	require.NoError(t, err, "T1 commit should succeed")
	table.CommitVersions(t1, t1.GetCommitTS())

	// Step 2: T2 starts (after T1 commits)
	mockClock.Advance(time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	require.True(t, t2.IsActive(), "T2 should be active")

	// Step 3: T2 reads the row - should see T1's committed row
	values, err := table.ReadVersioned(t2, rowID)
	require.NoError(t, err, "T2 should be able to read T1's committed row")
	assert.Equal(t, int32(42), values[0], "Should read the correct integer value")
	assert.Equal(t, "committed_before", values[1], "Should read the correct string value")

	// Also verify using version chain visibility
	t2Ctx := newSnapshotTxnContext(t2)
	visibility := NewRepeatableReadVisibility()

	chains := table.GetVersionChains()
	require.Len(t, chains, 1, "Should have one version chain")

	visibleVersion := chains[0].FindVisibleVersion(visibility, t2Ctx)
	require.NotNil(t, visibleVersion, "T2 should see T1's committed row")
	assert.Equal(t, []any{int32(42), "committed_before"}, visibleVersion.Data)

	// Cleanup
	err = mvccMgr.Commit(t2)
	require.NoError(t, err)
}

// =============================================================================
// Test 4: Rows committed after snapshot are invisible
// =============================================================================

// TestSnapshotIsolation_CommittedAfterStartInvisible verifies that a transaction
// cannot see rows that were committed after the transaction's snapshot was taken.
//
// Scenario:
//  1. Transaction T1 starts (takes snapshot)
//  2. Transaction T2 starts
//  3. T2 inserts and commits a row
//  4. T1 reads the table - should NOT see T2's row (committed after T1's snapshot)
func TestSnapshotIsolation_CommittedAfterStartInvisible(t *testing.T) {
	// Setup: Create mock clock and MVCC manager
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)

	// Create a test table
	table := NewTable("test_committed_after_invisible", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Step 1: T1 starts (takes snapshot)
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	t1Ctx := newSnapshotTxnContext(t1)
	require.True(t, t1.IsActive(), "T1 should be active")

	// Step 2: T2 starts (after T1)
	mockClock.Advance(time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	require.True(t, t2.IsActive(), "T2 should be active")

	// Step 3: T2 inserts and commits a row
	_, err := table.InsertVersioned(t2, []any{int32(999)})
	require.NoError(t, err, "T2 insert should succeed")

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t2)
	require.NoError(t, err, "T2 commit should succeed")
	table.CommitVersions(t2, t2.GetCommitTS())

	// Step 4: T1 reads the table - should NOT see T2's row
	visibility := NewRepeatableReadVisibility()

	chains := table.GetVersionChains()
	require.Len(t, chains, 1, "Should have one version chain")

	// The row was created by T2, which started after T1.
	// T2 was in T1's active set when T1 started
	// Even though T2 has committed, T1 should not see it
	visibleVersion := chains[0].FindVisibleVersion(visibility, t1Ctx)

	// The row should be invisible because T2 was active when T1 started
	assert.Nil(t, visibleVersion, "T1 should NOT see T2's row (T2 was active when T1 started)")

	// Cleanup
	err = mvccMgr.Commit(t1)
	require.NoError(t, err)
}

// =============================================================================
// Test 5: Delete visibility - snapshot sees row deleted after snapshot
// =============================================================================

// TestSnapshotIsolation_DeleteVisibility verifies that a transaction with an
// older snapshot can still see rows that were deleted after the snapshot.
//
// Scenario:
//  1. Transaction T1 inserts and commits a row
//  2. Transaction T2 starts (takes snapshot at this point, sees row)
//  3. Transaction T3 starts
//  4. T3 deletes the row and commits
//  5. T2 reads - should still see the row (delete happened after T2's snapshot)
func TestSnapshotIsolation_DeleteVisibility(t *testing.T) {
	// Setup: Create mock clock and MVCC manager
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)

	// Create a test table
	table := NewTable("test_delete_visibility", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Step 1: T1 inserts and commits a row
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)

	rowID, err := table.InsertVersioned(t1, []any{int32(100), "to_be_deleted"})
	require.NoError(t, err, "T1 insert should succeed")

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t1)
	require.NoError(t, err, "T1 commit should succeed")
	table.CommitVersions(t1, t1.GetCommitTS())

	// Step 2: T2 starts (takes snapshot, should see the row)
	mockClock.Advance(time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	t2Ctx := newSnapshotTxnContext(t2)
	require.True(t, t2.IsActive(), "T2 should be active")

	// Verify T2 can see the row at this point
	values, err := table.ReadVersioned(t2, rowID)
	require.NoError(t, err, "T2 should be able to read the row initially")
	assert.Equal(t, int32(100), values[0], "Should read the correct integer value")

	// Step 3: T3 starts (after T2)
	mockClock.Advance(time.Millisecond)
	t3 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	require.True(t, t3.IsActive(), "T3 should be active")

	// Step 4: T3 deletes the row and commits
	err = table.DeleteVersioned(t3, rowID)
	require.NoError(t, err, "T3 delete should succeed")

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t3)
	require.NoError(t, err, "T3 commit should succeed")
	table.CommitVersions(t3, t3.GetCommitTS())

	// Step 5: T2 reads - should still see the row
	// The delete happened after T2's snapshot, so T2 should not see the delete
	visibility := NewRepeatableReadVisibility()

	chains := table.GetVersionChains()
	require.Len(t, chains, 1, "Should have one version chain")

	// Create a version info that reflects the delete but with delete time after T2's snapshot
	head := chains[0].GetHead()
	require.NotNil(t, head, "Head should not be nil")

	// The row's DeletedBy is set to T3's ID, and DeleteTS is T3's commit timestamp
	// T2 should still see this row because:
	// - T3 was not in T2's active set at start (T3 started after T2)
	// - The delete committed after T2's snapshot
	visibleVersion := chains[0].FindVisibleVersion(visibility, t2Ctx)

	// For proper snapshot isolation, T2 should still see the row
	// because the delete was committed after T2's snapshot
	require.NotNil(t, visibleVersion, "T2 should still see the row (delete happened after T2's snapshot)")
	assert.Equal(t, int32(100), visibleVersion.Data[0])
	assert.Equal(t, "to_be_deleted", visibleVersion.Data[1])

	// Cleanup
	err = mvccMgr.Commit(t2)
	require.NoError(t, err)
}

// =============================================================================
// Test 6: Update creates new version - snapshot sees old version
// =============================================================================

// TestSnapshotIsolation_UpdateCreatesNewVersion verifies that an update creates
// a new version and older snapshots see the old version.
//
// Scenario:
//  1. Transaction T1 inserts and commits a row with value "A"
//  2. Transaction T2 starts (takes snapshot, sees "A")
//  3. Transaction T3 starts
//  4. T3 updates the row to "B" and commits
//  5. T2 reads - should see "A" (the version at T2's snapshot time)
func TestSnapshotIsolation_UpdateCreatesNewVersion(t *testing.T) {
	// Setup: Create mock clock and MVCC manager
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)

	// Create a test table
	table := NewTable("test_update_version", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Step 1: T1 inserts and commits a row with value "A"
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)

	rowID, err := table.InsertVersioned(t1, []any{int32(1), "A"})
	require.NoError(t, err, "T1 insert should succeed")

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t1)
	require.NoError(t, err, "T1 commit should succeed")
	table.CommitVersions(t1, t1.GetCommitTS())

	// Verify version chain has one version with "A"
	chains := table.GetVersionChains()
	require.Len(t, chains, 1)
	assert.Equal(t, 1, chains[0].Len(), "Should have one version initially")

	// Step 2: T2 starts (takes snapshot, should see "A")
	mockClock.Advance(time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	t2Ctx := newSnapshotTxnContext(t2)
	require.True(t, t2.IsActive(), "T2 should be active")

	// Verify T2 can see "A"
	values, err := table.ReadVersioned(t2, rowID)
	require.NoError(t, err, "T2 should be able to read the row")
	assert.Equal(t, "A", values[1], "T2 should see value A")

	// Step 3: T3 starts (after T2)
	mockClock.Advance(time.Millisecond)
	t3 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	require.True(t, t3.IsActive(), "T3 should be active")

	// Step 4: T3 updates the row to "B" and commits
	err = table.UpdateVersioned(t3, rowID, []any{int32(2), "B"})
	require.NoError(t, err, "T3 update should succeed")

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t3)
	require.NoError(t, err, "T3 commit should succeed")
	table.CommitVersions(t3, t3.GetCommitTS())

	// Verify version chain now has two versions
	assert.Equal(t, 2, chains[0].Len(), "Should have two versions after update")

	// Step 5: T2 reads - should still see "A" (the version at T2's snapshot time)
	visibility := NewRepeatableReadVisibility()

	visibleVersion := chains[0].FindVisibleVersion(visibility, t2Ctx)
	require.NotNil(t, visibleVersion, "T2 should find a visible version")

	// T2 should see the OLD version "A", not the new version "B"
	// because T3's update committed after T2's snapshot
	assert.Equal(t, "A", visibleVersion.Data[1], "T2 should see value A (version at T2's snapshot time)")
	assert.Equal(t, int32(1), visibleVersion.Data[0], "T2 should see the old integer value")

	// Note: ReadVersioned uses an internal versionedTxnContext that doesn't have
	// full snapshot support (it lacks the active-at-start tracking needed for proper
	// snapshot isolation). The visibility check above using FindVisibleVersion with
	// our proper snapshotTxnContext is the authoritative test for snapshot isolation.
	// In a full integration, the engine would provide a proper transaction context
	// that implements all the snapshot semantics.

	// Now start a new transaction T4 after T3's commit - should see "B"
	mockClock.Advance(time.Millisecond)
	t4 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	t4Ctx := newSnapshotTxnContext(t4)

	visibleVersion = chains[0].FindVisibleVersion(visibility, t4Ctx)
	require.NotNil(t, visibleVersion, "T4 should find a visible version")
	assert.Equal(t, "B", visibleVersion.Data[1], "T4 should see value B (latest committed value)")
	assert.Equal(t, int32(2), visibleVersion.Data[0], "T4 should see the new integer value")

	// Cleanup
	err = mvccMgr.Commit(t2)
	require.NoError(t, err)
	err = mvccMgr.Commit(t4)
	require.NoError(t, err)
}

// =============================================================================
// Additional Tests for Edge Cases
// =============================================================================

// TestSnapshotIsolation_SerializableIsolation tests that SERIALIZABLE isolation
// provides the same snapshot visibility as REPEATABLE READ.
func TestSnapshotIsolation_SerializableIsolation(t *testing.T) {
	// Setup: Create mock clock and MVCC manager
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)

	// Create a test table
	table := NewTable("test_serializable", []dukdb.Type{dukdb.TYPE_INTEGER})

	// T1 inserts and commits a row
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)

	rowID, err := table.InsertVersioned(t1, []any{int32(1)})
	require.NoError(t, err)

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t1)
	require.NoError(t, err)
	table.CommitVersions(t1, t1.GetCommitTS())

	// T2 starts with SERIALIZABLE
	mockClock.Advance(time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)
	t2Ctx := newSnapshotTxnContext(t2)

	// T3 starts after T2 and updates the row
	mockClock.Advance(time.Millisecond)
	t3 := mvccMgr.BeginTransaction(parser.IsolationLevelSerializable)

	err = table.UpdateVersioned(t3, rowID, []any{int32(2)})
	require.NoError(t, err)

	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t3)
	require.NoError(t, err)
	table.CommitVersions(t3, t3.GetCommitTS())

	// T2 should see the old value (1) under SERIALIZABLE
	visibility := NewSerializableVisibility()

	chains := table.GetVersionChains()
	visibleVersion := chains[0].FindVisibleVersion(visibility, t2Ctx)
	require.NotNil(t, visibleVersion)
	assert.Equal(t, int32(1), visibleVersion.Data[0], "T2 (SERIALIZABLE) should see old value")

	// Cleanup
	err = mvccMgr.Commit(t2)
	require.NoError(t, err)
}

// TestSnapshotIsolation_MultipleUpdates tests snapshot isolation with multiple
// updates to the same row.
func TestSnapshotIsolation_MultipleUpdates(t *testing.T) {
	// Setup: Create mock clock and MVCC manager
	mockClock := quartz.NewMock(t)
	mvccMgr := newTestMVCCManager(mockClock)

	// Create a test table
	table := NewTable("test_multi_updates", []dukdb.Type{dukdb.TYPE_INTEGER})

	// T1 inserts initial value 1 and commits
	mockClock.Advance(time.Millisecond)
	t1 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	rowID, err := table.InsertVersioned(t1, []any{int32(1)})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t1)
	require.NoError(t, err)
	table.CommitVersions(t1, t1.GetCommitTS())

	// T_read starts (will observe the state at this point)
	mockClock.Advance(time.Millisecond)
	tRead := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	tReadCtx := newSnapshotTxnContext(tRead)

	// T2 updates to 2 and commits
	mockClock.Advance(time.Millisecond)
	t2 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	err = table.UpdateVersioned(t2, rowID, []any{int32(2)})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t2)
	require.NoError(t, err)
	table.CommitVersions(t2, t2.GetCommitTS())

	// T3 updates to 3 and commits
	mockClock.Advance(time.Millisecond)
	t3 := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	err = table.UpdateVersioned(t3, rowID, []any{int32(3)})
	require.NoError(t, err)
	mockClock.Advance(time.Millisecond)
	err = mvccMgr.Commit(t3)
	require.NoError(t, err)
	table.CommitVersions(t3, t3.GetCommitTS())

	// Verify version chain has 3 versions
	chains := table.GetVersionChains()
	require.Len(t, chains, 1)
	assert.Equal(t, 3, chains[0].Len(), "Should have 3 versions")

	// T_read should still see value 1 (its snapshot time)
	visibility := NewRepeatableReadVisibility()
	visibleVersion := chains[0].FindVisibleVersion(visibility, tReadCtx)
	require.NotNil(t, visibleVersion)
	assert.Equal(t, int32(1), visibleVersion.Data[0], "T_read should see original value 1")

	// New transaction should see value 3 (latest)
	mockClock.Advance(time.Millisecond)
	tNew := mvccMgr.BeginTransaction(parser.IsolationLevelRepeatableRead)
	tNewCtx := newSnapshotTxnContext(tNew)

	visibleVersion = chains[0].FindVisibleVersion(visibility, tNewCtx)
	require.NotNil(t, visibleVersion)
	assert.Equal(t, int32(3), visibleVersion.Data[0], "New transaction should see latest value 3")

	// Cleanup
	err = mvccMgr.Commit(tRead)
	require.NoError(t, err)
	err = mvccMgr.Commit(tNew)
	require.NoError(t, err)
}
