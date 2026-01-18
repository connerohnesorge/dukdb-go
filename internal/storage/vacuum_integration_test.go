package storage

import (
	"testing"

	"github.com/coder/quartz"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Simulated MVCC Manager for Integration Tests
// =============================================================================

// simulatedMVCCManager simulates an MVCC manager that tracks active transactions
// and provides a low watermark for vacuum operations.
type simulatedMVCCManager struct {
	activeTransactions map[uint64]*simulatedTransaction
	nextTxnID          uint64
	nextTS             uint64
}

// simulatedTransaction represents a transaction in the simulated MVCC system.
type simulatedTransaction struct {
	id             uint64
	startTS        uint64
	commitTS       uint64
	isolationLevel parser.IsolationLevel
	reads          map[string][]uint64
	writes         map[string][]uint64
}

// newSimulatedMVCCManager creates a new simulated MVCC manager.
func newSimulatedMVCCManager() *simulatedMVCCManager {
	return &simulatedMVCCManager{
		activeTransactions: make(map[uint64]*simulatedTransaction),
		nextTxnID:          1,
		nextTS:             100, // Start at a reasonable timestamp
	}
}

// BeginTransaction starts a new transaction and returns it.
func (m *simulatedMVCCManager) BeginTransaction(level parser.IsolationLevel) *simulatedTransaction {
	txn := &simulatedTransaction{
		id:             m.nextTxnID,
		startTS:        m.nextTS,
		commitTS:       0,
		isolationLevel: level,
		reads:          make(map[string][]uint64),
		writes:         make(map[string][]uint64),
	}
	m.activeTransactions[txn.id] = txn
	m.nextTxnID++
	m.nextTS += 10
	return txn
}

// Commit commits a transaction and removes it from active transactions.
func (m *simulatedMVCCManager) Commit(txn *simulatedTransaction) uint64 {
	commitTS := m.nextTS
	txn.commitTS = commitTS
	delete(m.activeTransactions, txn.id)
	m.nextTS += 10
	return commitTS
}

// Abort aborts a transaction and removes it from active transactions.
func (m *simulatedMVCCManager) Abort(txn *simulatedTransaction) {
	delete(m.activeTransactions, txn.id)
}

// GetLowWatermark returns the minimum StartTS of all active transactions.
// If no transactions are active, returns max uint64 (everything can be vacuumed).
func (m *simulatedMVCCManager) GetLowWatermark() uint64 {
	if len(m.activeTransactions) == 0 {
		return ^uint64(0) // Max uint64 - everything can be vacuumed
	}

	minTS := ^uint64(0)
	for _, txn := range m.activeTransactions {
		if txn.startTS < minTS {
			minTS = txn.startTS
		}
	}
	return minTS
}

// AdvanceTime advances the internal timestamp by the given amount.
func (m *simulatedMVCCManager) AdvanceTime(delta uint64) {
	m.nextTS += delta
}

// Implement MVCCTransactionContext interface for simulatedTransaction

func (t *simulatedTransaction) ID() uint64 {
	return t.id
}

func (t *simulatedTransaction) GetStartTS() uint64 {
	return t.startTS
}

func (t *simulatedTransaction) GetCommitTS() uint64 {
	return t.commitTS
}

func (t *simulatedTransaction) RecordRead(table string, rowID uint64) {
	t.reads[table] = append(t.reads[table], rowID)
}

func (t *simulatedTransaction) RecordWrite(table string, rowID uint64) {
	t.writes[table] = append(t.writes[table], rowID)
}

func (t *simulatedTransaction) VisibilityChecker() VisibilityChecker {
	switch t.isolationLevel {
	case parser.IsolationLevelReadUncommitted:
		return NewReadUncommittedVisibility()
	case parser.IsolationLevelReadCommitted:
		return NewReadCommittedVisibility()
	case parser.IsolationLevelRepeatableRead:
		return NewRepeatableReadVisibility()
	default:
		return NewSerializableVisibility()
	}
}

// =============================================================================
// TestVacuum_BasicVersionCleanup
// =============================================================================
// Scenario:
// - T1 inserts a row and commits
// - T2 updates the row and commits
// - T3 updates the row and commits
// - No active transactions (all committed)
// - Run vacuum - should remove old versions, keeping only the latest committed

func TestVacuum_BasicVersionCleanup(t *testing.T) {
	mockClock := quartz.NewMock(t)
	mvccManager := newSimulatedMVCCManager()

	// Create a table
	table := NewTable("test_table", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// T1: Insert a row and commit
	t1 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	rowID, err := table.InsertVersioned(t1, []any{int32(1), "original"})
	require.NoError(t, err)
	commitTS1 := mvccManager.Commit(t1)
	table.CommitVersions(t1, commitTS1)

	// Verify initial state: 1 version in chain
	chains := table.GetVersionChains()
	require.Len(t, chains, 1)
	assert.Equal(t, 1, chains[0].Len(), "should have 1 version after insert")

	// T2: Update the row and commit
	t2 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.UpdateVersioned(t2, rowID, []any{int32(2), "update1"})
	require.NoError(t, err)
	commitTS2 := mvccManager.Commit(t2)
	table.CommitVersions(t2, commitTS2)

	// Verify: 2 versions in chain
	assert.Equal(t, 2, chains[0].Len(), "should have 2 versions after first update")

	// T3: Update the row again and commit
	t3 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.UpdateVersioned(t3, rowID, []any{int32(3), "update2"})
	require.NoError(t, err)
	commitTS3 := mvccManager.Commit(t3)
	table.CommitVersions(t3, commitTS3)

	// Verify: 3 versions in chain
	assert.Equal(t, 3, chains[0].Len(), "should have 3 versions after second update")

	// Now run vacuum - no active transactions, all old versions should be removed
	vacuum := NewVacuum(mvccManager.GetLowWatermark, mockClock)

	versionsRemoved := vacuum.VacuumChains(chains)

	// Should remove 2 old versions (keeping only the head with commitTS3)
	assert.Equal(t, 2, versionsRemoved, "should remove 2 old versions")

	// Verify: only 1 version remains (the head)
	assert.Equal(t, 1, chains[0].Len(), "should have 1 version after vacuum")

	// Verify the remaining version is the latest one
	head := chains[0].GetHead()
	require.NotNil(t, head)
	assert.Equal(t, []any{int32(3), "update2"}, head.Data)
	assert.Equal(t, commitTS3, head.CommitTS)

	// Verify statistics
	stats := vacuum.GetStatistics()
	assert.Equal(t, uint64(2), stats.VersionsRemoved)
	assert.Equal(t, uint64(1), stats.ChainsProcessed)
}

// =============================================================================
// TestVacuum_LongRunningTransactionBlocksGC
// =============================================================================
// Scenario:
// - T1 starts (long-running, takes snapshot at time 100)
// - T2 inserts and commits a row
// - T3 updates the row and commits
// - T4 updates the row again and commits
// - Run vacuum - old versions needed by T1 should NOT be removed
// - T1 should still be able to read the version from its snapshot

func TestVacuum_LongRunningTransactionBlocksGC(t *testing.T) {
	mockClock := quartz.NewMock(t)
	mvccManager := newSimulatedMVCCManager()

	// Create a table
	table := NewTable("test_table", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// T1 starts but doesn't do anything yet (long-running transaction)
	t1 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	t1SnapshotTS := t1.startTS // T1's snapshot timestamp

	// T2: Insert a row and commit
	t2 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	rowID, err := table.InsertVersioned(t2, []any{int32(1), "original"})
	require.NoError(t, err)
	commitTS2 := mvccManager.Commit(t2)
	table.CommitVersions(t2, commitTS2)

	// T3: Update the row and commit
	t3 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.UpdateVersioned(t3, rowID, []any{int32(2), "update1"})
	require.NoError(t, err)
	commitTS3 := mvccManager.Commit(t3)
	table.CommitVersions(t3, commitTS3)

	// T4: Update the row again and commit
	t4 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.UpdateVersioned(t4, rowID, []any{int32(3), "update2"})
	require.NoError(t, err)
	commitTS4 := mvccManager.Commit(t4)
	table.CommitVersions(t4, commitTS4)

	// Verify: 3 versions in chain
	chains := table.GetVersionChains()
	require.Len(t, chains, 1)
	assert.Equal(t, 3, chains[0].Len(), "should have 3 versions before vacuum")

	// Run vacuum - T1 is still active, so low watermark is T1's startTS
	vacuum := NewVacuum(mvccManager.GetLowWatermark, mockClock)

	// Get low watermark before vacuum
	lowWatermark := mvccManager.GetLowWatermark()
	assert.Equal(t, t1SnapshotTS, lowWatermark, "low watermark should be T1's startTS")

	versionsRemoved := vacuum.VacuumChains(chains)

	// Since T1 started before any commits, its startTS (low watermark) is before
	// all commit timestamps. Versions with CommitTS >= lowWatermark cannot be removed.
	// In this scenario, all versions have CommitTS > T1's startTS, so they all need
	// to be preserved for potential visibility to T1.
	// However, the head is never removed, and versions with CommitTS < lowWatermark
	// can be removed. Since all CommitTS > lowWatermark (T1's startTS), no versions
	// should be removed (except the head is always preserved regardless).

	// The versions:
	// - v3 (head): commitTS4 > t1SnapshotTS - cannot remove, is head
	// - v2: commitTS3 > t1SnapshotTS - cannot remove
	// - v1: commitTS2 > t1SnapshotTS - cannot remove

	assert.Equal(t, 0, versionsRemoved, "no versions should be removed while T1 is active")
	assert.Equal(t, 3, chains[0].Len(), "should still have 3 versions after vacuum")

	// Verify T1 can still read (would see nothing since insert was after T1 started)
	// In a real snapshot isolation implementation, T1 would not see the row at all
	// since it was inserted after T1's snapshot was taken.

	// Verify statistics
	stats := vacuum.GetStatistics()
	assert.Equal(t, uint64(0), stats.VersionsRemoved)
	assert.Equal(t, uint64(1), stats.ChainsProcessed)

	// Now commit T1 and run vacuum again
	mvccManager.Commit(t1)

	// Reset statistics
	vacuum.ResetStatistics()

	versionsRemoved = vacuum.VacuumChains(chains)

	// Now all old versions can be removed (keeping only head)
	assert.Equal(t, 2, versionsRemoved, "should remove 2 old versions after T1 commits")
	assert.Equal(t, 1, chains[0].Len(), "should have 1 version after final vacuum")
}

// =============================================================================
// TestVacuum_NoActiveTransactionsAllowsFullCleanup
// =============================================================================
// Scenario:
// - Insert several rows, update them multiple times, commit all
// - No active transactions remaining
// - Run vacuum - all old versions should be removed
// - Latest committed version for each row should remain

func TestVacuum_NoActiveTransactionsAllowsFullCleanup(t *testing.T) {
	mockClock := quartz.NewMock(t)
	mvccManager := newSimulatedMVCCManager()

	// Create a table
	table := NewTable("test_table", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Insert and update multiple rows
	numRows := 5
	numUpdates := 3
	rowIDs := make([]RowID, numRows)

	// Insert all rows
	for i := 0; i < numRows; i++ {
		txn := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
		rowID, err := table.InsertVersioned(txn, []any{int32(i * 10), "initial"})
		require.NoError(t, err)
		rowIDs[i] = rowID
		commitTS := mvccManager.Commit(txn)
		table.CommitVersions(txn, commitTS)
	}

	// Update each row multiple times
	for update := 0; update < numUpdates; update++ {
		for i := 0; i < numRows; i++ {
			txn := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
			err := table.UpdateVersioned(txn, rowIDs[i], []any{int32(i*10 + update + 1), "update"})
			require.NoError(t, err)
			commitTS := mvccManager.Commit(txn)
			table.CommitVersions(txn, commitTS)
		}
	}

	// Verify each chain has 1 (insert) + numUpdates versions
	chains := table.GetVersionChains()
	require.Len(t, chains, numRows)

	totalVersionsBefore := 0
	for _, chain := range chains {
		chainLen := chain.Len()
		totalVersionsBefore += chainLen
		assert.Equal(t, 1+numUpdates, chainLen, "each chain should have insert + updates versions")
	}

	expectedVersionsBefore := numRows * (1 + numUpdates)
	assert.Equal(t, expectedVersionsBefore, totalVersionsBefore)

	// Verify no active transactions
	lowWatermark := mvccManager.GetLowWatermark()
	assert.Equal(
		t,
		^uint64(0),
		lowWatermark,
		"low watermark should be max uint64 with no active transactions",
	)

	// Run vacuum - all old versions should be removed
	vacuum := NewVacuum(mvccManager.GetLowWatermark, mockClock)
	versionsRemoved := vacuum.VacuumChains(chains)

	// Should remove (numUpdates) versions per row (keeping only head)
	expectedRemoved := numRows * numUpdates
	assert.Equal(t, expectedRemoved, versionsRemoved, "should remove all old versions")

	// Each chain should now have exactly 1 version (the head)
	for i, chain := range chains {
		assert.Equal(t, 1, chain.Len(), "chain %d should have 1 version after vacuum", i)
	}

	// Verify the remaining versions are the latest ones
	// Note: Map iteration order is not guaranteed, so we use RowID to determine expected value
	for _, chain := range chains {
		head := chain.GetHead()
		require.NotNil(t, head, "chain should have a head")
		// Get the row index from the RowID (rowIDs[i] = RowID(i))
		rowIdx := int(chain.RowID)
		// Final value should be rowIdx*10 + numUpdates
		expectedValue := int32(rowIdx*10 + numUpdates)
		assert.Equal(
			t,
			expectedValue,
			head.Data[0].(int32),
			"row %d should have final value",
			rowIdx,
		)
	}

	// Verify statistics
	stats := vacuum.GetStatistics()
	assert.Equal(t, uint64(expectedRemoved), stats.VersionsRemoved)
	assert.Equal(t, uint64(numRows), stats.ChainsProcessed)
}

// =============================================================================
// TestVacuum_DeletedVersionsCleanup
// =============================================================================
// Scenario:
// - T1 inserts a row and commits
// - T2 deletes the row and commits
// - No active transactions
// - Run vacuum - the deleted version chain should be cleaned

func TestVacuum_DeletedVersionsCleanup(t *testing.T) {
	mockClock := quartz.NewMock(t)
	mvccManager := newSimulatedMVCCManager()

	// Create a table
	table := NewTable("test_table", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// T1: Insert a row and commit
	t1 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	rowID, err := table.InsertVersioned(t1, []any{int32(42), "to_be_deleted"})
	require.NoError(t, err)
	commitTS1 := mvccManager.Commit(t1)
	table.CommitVersions(t1, commitTS1)

	// Verify initial state
	chains := table.GetVersionChains()
	require.Len(t, chains, 1)
	assert.Equal(t, 1, chains[0].Len())

	// T2: Delete the row and commit
	t2 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.DeleteVersioned(t2, rowID)
	require.NoError(t, err)
	commitTS2 := mvccManager.Commit(t2)
	table.CommitVersions(t2, commitTS2)

	// Verify the row is marked as deleted
	head := chains[0].GetHead()
	require.NotNil(t, head)
	assert.Equal(t, t2.id, head.DeletedBy, "DeletedBy should be T2's ID")
	assert.Equal(t, commitTS2, head.DeleteTS, "DeleteTS should be T2's commit timestamp")

	// Run vacuum
	vacuum := NewVacuum(mvccManager.GetLowWatermark, mockClock)
	versionsRemoved := vacuum.VacuumChains(chains)

	// The chain has only 1 version (the head with delete marker)
	// Head is never removed, so no versions should be removed
	assert.Equal(t, 0, versionsRemoved, "head with delete marker should not be removed")
	assert.Equal(t, 1, chains[0].Len(), "chain should still have 1 version")

	// Verify the version still has the delete marker
	head = chains[0].GetHead()
	require.NotNil(t, head)
	assert.Equal(t, t2.id, head.DeletedBy)
	assert.Equal(t, commitTS2, head.DeleteTS)

	// Verify statistics
	stats := vacuum.GetStatistics()
	assert.Equal(t, uint64(0), stats.VersionsRemoved)
	assert.Equal(t, uint64(1), stats.ChainsProcessed)
}

// =============================================================================
// TestVacuum_DeletedVersionsCleanup_WithMultipleVersions
// =============================================================================
// Extended scenario with multiple versions before delete:
// - T1 inserts a row and commits
// - T2 updates the row and commits
// - T3 updates the row and commits
// - T4 deletes the row and commits
// - No active transactions
// - Run vacuum - old versions should be cleaned, head (with delete) remains

func TestVacuum_DeletedVersionsCleanup_WithMultipleVersions(t *testing.T) {
	mockClock := quartz.NewMock(t)
	mvccManager := newSimulatedMVCCManager()

	// Create a table
	table := NewTable("test_table", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// T1: Insert a row and commit
	t1 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	rowID, err := table.InsertVersioned(t1, []any{int32(1), "v1"})
	require.NoError(t, err)
	commitTS1 := mvccManager.Commit(t1)
	table.CommitVersions(t1, commitTS1)

	// T2: Update and commit
	t2 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.UpdateVersioned(t2, rowID, []any{int32(2), "v2"})
	require.NoError(t, err)
	commitTS2 := mvccManager.Commit(t2)
	table.CommitVersions(t2, commitTS2)

	// T3: Update and commit
	t3 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.UpdateVersioned(t3, rowID, []any{int32(3), "v3"})
	require.NoError(t, err)
	commitTS3 := mvccManager.Commit(t3)
	table.CommitVersions(t3, commitTS3)

	// Verify chain has 3 versions
	chains := table.GetVersionChains()
	require.Len(t, chains, 1)
	assert.Equal(t, 3, chains[0].Len())

	// T4: Delete and commit
	t4 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.DeleteVersioned(t4, rowID)
	require.NoError(t, err)
	commitTS4 := mvccManager.Commit(t4)
	table.CommitVersions(t4, commitTS4)

	// Delete marks the head version - chain still has 3 versions
	assert.Equal(t, 3, chains[0].Len())

	head := chains[0].GetHead()
	require.NotNil(t, head)
	assert.Equal(t, t4.id, head.DeletedBy)
	assert.Equal(t, commitTS4, head.DeleteTS)

	// Run vacuum
	vacuum := NewVacuum(mvccManager.GetLowWatermark, mockClock)
	versionsRemoved := vacuum.VacuumChains(chains)

	// Should remove 2 old versions, keeping only the head (which has the delete marker)
	assert.Equal(t, 2, versionsRemoved, "should remove 2 old versions")
	assert.Equal(t, 1, chains[0].Len(), "should have 1 version after vacuum")

	// Verify the remaining version has the delete marker and latest data
	head = chains[0].GetHead()
	require.NotNil(t, head)
	assert.Equal(t, []any{int32(3), "v3"}, head.Data)
	assert.Equal(t, commitTS3, head.CommitTS)
	assert.Equal(t, t4.id, head.DeletedBy)
	assert.Equal(t, commitTS4, head.DeleteTS)

	// Verify statistics
	stats := vacuum.GetStatistics()
	assert.Equal(t, uint64(2), stats.VersionsRemoved)
	assert.Equal(t, uint64(1), stats.ChainsProcessed)
}

// =============================================================================
// TestVacuum_StatisticsTracking
// =============================================================================
// Scenario:
// - Perform multiple inserts/updates/commits on multiple tables
// - Run vacuum
// - Verify statistics (VersionsRemoved, ChainsProcessed) are correct

func TestVacuum_StatisticsTracking(t *testing.T) {
	mockClock := quartz.NewMock(t)
	mvccManager := newSimulatedMVCCManager()

	// Create multiple tables
	table1 := NewTable("table1", []dukdb.Type{dukdb.TYPE_INTEGER})
	table2 := NewTable("table2", []dukdb.Type{dukdb.TYPE_VARCHAR})

	// Table 1: Insert 3 rows, update each twice
	table1RowIDs := make([]RowID, 3)
	for i := 0; i < 3; i++ {
		txn := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
		rowID, err := table1.InsertVersioned(txn, []any{int32(i)})
		require.NoError(t, err)
		table1RowIDs[i] = rowID
		commitTS := mvccManager.Commit(txn)
		table1.CommitVersions(txn, commitTS)
	}

	for update := 0; update < 2; update++ {
		for i := 0; i < 3; i++ {
			txn := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
			err := table1.UpdateVersioned(txn, table1RowIDs[i], []any{int32(i*10 + update)})
			require.NoError(t, err)
			commitTS := mvccManager.Commit(txn)
			table1.CommitVersions(txn, commitTS)
		}
	}

	// Table 2: Insert 2 rows, update each 3 times
	table2RowIDs := make([]RowID, 2)
	for i := 0; i < 2; i++ {
		txn := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
		rowID, err := table2.InsertVersioned(txn, []any{"row"})
		require.NoError(t, err)
		table2RowIDs[i] = rowID
		commitTS := mvccManager.Commit(txn)
		table2.CommitVersions(txn, commitTS)
	}

	for update := 0; update < 3; update++ {
		for i := 0; i < 2; i++ {
			txn := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
			err := table2.UpdateVersioned(txn, table2RowIDs[i], []any{"updated"})
			require.NoError(t, err)
			commitTS := mvccManager.Commit(txn)
			table2.CommitVersions(txn, commitTS)
		}
	}

	// Get all chains
	table1Chains := table1.GetVersionChains()
	table2Chains := table2.GetVersionChains()

	// Verify chain counts
	require.Len(t, table1Chains, 3)
	require.Len(t, table2Chains, 2)

	// Calculate expected versions before vacuum
	// Table 1: 3 rows * (1 insert + 2 updates) = 3 rows * 3 versions = 9 total versions
	// Table 2: 2 rows * (1 insert + 3 updates) = 2 rows * 4 versions = 8 total versions

	for _, chain := range table1Chains {
		assert.Equal(t, 3, chain.Len(), "table1 chains should have 3 versions each")
	}
	for _, chain := range table2Chains {
		assert.Equal(t, 4, chain.Len(), "table2 chains should have 4 versions each")
	}

	// Create vacuum and process all chains
	vacuum := NewVacuum(mvccManager.GetLowWatermark, mockClock)

	// Process table 1
	removed1 := vacuum.VacuumChains(table1Chains)
	// Table 1: 3 rows * 2 old versions removed = 6
	assert.Equal(t, 6, removed1)

	// Check intermediate statistics
	stats := vacuum.GetStatistics()
	assert.Equal(t, uint64(6), stats.VersionsRemoved)
	assert.Equal(t, uint64(3), stats.ChainsProcessed)

	// Process table 2
	removed2 := vacuum.VacuumChains(table2Chains)
	// Table 2: 2 rows * 3 old versions removed = 6
	assert.Equal(t, 6, removed2)

	// Check final statistics
	stats = vacuum.GetStatistics()
	assert.Equal(t, uint64(12), stats.VersionsRemoved, "total versions removed")
	assert.Equal(t, uint64(5), stats.ChainsProcessed, "total chains processed")

	// Verify all chains now have exactly 1 version
	for i, chain := range table1Chains {
		assert.Equal(t, 1, chain.Len(), "table1 chain %d should have 1 version", i)
	}
	for i, chain := range table2Chains {
		assert.Equal(t, 1, chain.Len(), "table2 chain %d should have 1 version", i)
	}

	// Test statistics reset
	vacuum.ResetStatistics()
	stats = vacuum.GetStatistics()
	assert.Equal(t, uint64(0), stats.VersionsRemoved)
	assert.Equal(t, uint64(0), stats.ChainsProcessed)
	assert.True(t, stats.LastRunTime.IsZero())

	// Running vacuum again should report 0 removed (already cleaned)
	removed := vacuum.VacuumChains(table1Chains)
	assert.Equal(t, 0, removed, "second vacuum should remove nothing")

	stats = vacuum.GetStatistics()
	assert.Equal(t, uint64(0), stats.VersionsRemoved)
	assert.Equal(t, uint64(3), stats.ChainsProcessed)
}

// =============================================================================
// TestVacuum_MixedScenario_ActiveAndCompletedTransactions
// =============================================================================
// Complex scenario with both active and completed transactions:
// - T1 starts (remains active throughout)
// - T2 inserts row A, commits
// - T3 updates row A, commits
// - T4 inserts row B, commits
// - T5 updates row B twice, commits each
// - Run vacuum - row A versions needed by T1 preserved, row B can be cleaned

func TestVacuum_MixedScenario_ActiveAndCompletedTransactions(t *testing.T) {
	mockClock := quartz.NewMock(t)
	mvccManager := newSimulatedMVCCManager()

	// Create a table
	table := NewTable("test_table", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// T1 starts and remains active
	t1 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	t1StartTS := t1.startTS

	// T2: Insert row A and commit (after T1 started)
	t2 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	rowA, err := table.InsertVersioned(t2, []any{int32(1), "A-v1"})
	require.NoError(t, err)
	commitTS2 := mvccManager.Commit(t2)
	table.CommitVersions(t2, commitTS2)
	assert.True(t, commitTS2 > t1StartTS, "T2 committed after T1 started")

	// T3: Update row A and commit
	t3 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.UpdateVersioned(t3, rowA, []any{int32(2), "A-v2"})
	require.NoError(t, err)
	commitTS3 := mvccManager.Commit(t3)
	table.CommitVersions(t3, commitTS3)

	// Advance time significantly
	mvccManager.AdvanceTime(1000)

	// T4: Insert row B and commit (much later)
	t4 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	rowB, err := table.InsertVersioned(t4, []any{int32(10), "B-v1"})
	require.NoError(t, err)
	commitTS4 := mvccManager.Commit(t4)
	table.CommitVersions(t4, commitTS4)

	// T5: Update row B and commit
	t5 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.UpdateVersioned(t5, rowB, []any{int32(20), "B-v2"})
	require.NoError(t, err)
	commitTS5 := mvccManager.Commit(t5)
	table.CommitVersions(t5, commitTS5)

	// T6: Update row B again and commit
	t6 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.UpdateVersioned(t6, rowB, []any{int32(30), "B-v3"})
	require.NoError(t, err)
	commitTS6 := mvccManager.Commit(t6)
	table.CommitVersions(t6, commitTS6)

	// Get chains
	chains := table.GetVersionChains()
	require.Len(t, chains, 2)

	// Find chains for row A and row B
	var chainA, chainB *VersionChain
	for _, chain := range chains {
		if chain.RowID == uint64(rowA) {
			chainA = chain
		} else if chain.RowID == uint64(rowB) {
			chainB = chain
		}
	}
	require.NotNil(t, chainA, "should find chain for row A")
	require.NotNil(t, chainB, "should find chain for row B")

	// Verify chain lengths before vacuum
	assert.Equal(t, 2, chainA.Len(), "row A should have 2 versions")
	assert.Equal(t, 3, chainB.Len(), "row B should have 3 versions")

	// Run vacuum - T1 is still active with startTS before all commits
	lowWatermark := mvccManager.GetLowWatermark()
	assert.Equal(t, t1StartTS, lowWatermark, "low watermark should be T1's startTS")

	vacuum := NewVacuum(mvccManager.GetLowWatermark, mockClock)
	versionsRemoved := vacuum.VacuumChains(chains)

	// All commits are after T1's startTS, so no versions can be removed
	assert.Equal(t, 0, versionsRemoved, "no versions should be removed while T1 is active")
	assert.Equal(t, 2, chainA.Len(), "row A should still have 2 versions")
	assert.Equal(t, 3, chainB.Len(), "row B should still have 3 versions")

	// Now commit T1
	mvccManager.Commit(t1)

	// Run vacuum again
	vacuum.ResetStatistics()
	versionsRemoved = vacuum.VacuumChains(chains)

	// Now all old versions can be removed
	// Row A: 1 old version removed (keeping head)
	// Row B: 2 old versions removed (keeping head)
	assert.Equal(t, 3, versionsRemoved, "should remove 3 old versions after T1 commits")
	assert.Equal(t, 1, chainA.Len(), "row A should have 1 version after vacuum")
	assert.Equal(t, 1, chainB.Len(), "row B should have 1 version after vacuum")

	// Verify final data
	headA := chainA.GetHead()
	require.NotNil(t, headA)
	assert.Equal(t, []any{int32(2), "A-v2"}, headA.Data)

	headB := chainB.GetHead()
	require.NotNil(t, headB)
	assert.Equal(t, []any{int32(30), "B-v3"}, headB.Data)
}

// =============================================================================
// TestVacuum_UncommittedVersionsPreserved
// =============================================================================
// Scenario:
// - T1 inserts a row and commits
// - T2 updates the row (uncommitted)
// - Run vacuum - uncommitted head should not be removed
//
// Note: The committed version (v1) may be removed if its CommitTS < low watermark.
// The uncommitted version at head is never removed.

func TestVacuum_UncommittedVersionsPreserved(t *testing.T) {
	mockClock := quartz.NewMock(t)
	mvccManager := newSimulatedMVCCManager()

	// Create a table
	table := NewTable("test_table", []dukdb.Type{dukdb.TYPE_INTEGER})

	// T1: Insert and commit
	t1 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	rowID, err := table.InsertVersioned(t1, []any{int32(1)})
	require.NoError(t, err)
	commitTS1 := mvccManager.Commit(t1)
	table.CommitVersions(t1, commitTS1)

	// T2: Update but don't commit
	t2 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.UpdateVersioned(t2, rowID, []any{int32(2)})
	require.NoError(t, err)
	// T2 remains uncommitted

	// Get chain
	chains := table.GetVersionChains()
	require.Len(t, chains, 1)
	assert.Equal(t, 2, chains[0].Len())

	// Verify head is uncommitted
	head := chains[0].GetHead()
	require.NotNil(t, head)
	assert.Equal(t, uint64(0), head.CommitTS, "head should be uncommitted")

	// Run vacuum
	vacuum := NewVacuum(mvccManager.GetLowWatermark, mockClock)
	versionsRemoved := vacuum.VacuumChains(chains)

	// T2 is active, low watermark is T2's startTS (which is after T1's commit)
	// v1 (committed): commitTS1 < T2's startTS - CAN be removed (if not the head)
	// v2 (uncommitted, head): CommitTS == 0 - cannot be removed (head is never removed)
	//
	// The vacuum removes v1 because:
	// - v1 is not the head (v2 is the head)
	// - v1's CommitTS < low watermark
	//
	// Expected: 1 version removed (v1), 1 version remaining (v2 - head)

	assert.Equal(t, 1, versionsRemoved, "committed old version should be removed")
	assert.Equal(t, 1, chains[0].Len(), "only uncommitted head should remain")

	// Head should still be uncommitted
	head = chains[0].GetHead()
	require.NotNil(t, head)
	assert.Equal(t, uint64(0), head.CommitTS)
	assert.Equal(t, []any{int32(2)}, head.Data)

	// Previous pointer should be nil (v1 was removed)
	assert.Nil(t, head.PrevPtr, "old version should have been removed")
}

// =============================================================================
// TestVacuum_PendingDeletePreserved
// =============================================================================
// Scenario:
// - T1 inserts a row and commits
// - T2 marks the row for deletion (uncommitted)
// - Run vacuum - pending delete should be preserved

func TestVacuum_PendingDeletePreserved(t *testing.T) {
	mockClock := quartz.NewMock(t)
	mvccManager := newSimulatedMVCCManager()

	// Create a table
	table := NewTable("test_table", []dukdb.Type{dukdb.TYPE_INTEGER})

	// T1: Insert and commit
	t1 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	rowID, err := table.InsertVersioned(t1, []any{int32(1)})
	require.NoError(t, err)
	commitTS1 := mvccManager.Commit(t1)
	table.CommitVersions(t1, commitTS1)

	// T2: Delete but don't commit (pending delete)
	t2 := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	err = table.DeleteVersioned(t2, rowID)
	require.NoError(t, err)
	// T2 remains uncommitted

	// Get chain
	chains := table.GetVersionChains()
	require.Len(t, chains, 1)

	// Verify delete is pending
	head := chains[0].GetHead()
	require.NotNil(t, head)
	assert.Equal(t, t2.id, head.DeletedBy, "DeletedBy should be T2")
	assert.Equal(t, uint64(0), head.DeleteTS, "DeleteTS should be 0 (uncommitted)")

	// Run vacuum
	vacuum := NewVacuum(mvccManager.GetLowWatermark, mockClock)
	versionsRemoved := vacuum.VacuumChains(chains)

	// Version with pending delete should be preserved (only 1 version anyway - head)
	assert.Equal(t, 0, versionsRemoved, "head with pending delete should not be removed")
	assert.Equal(t, 1, chains[0].Len())

	// Verify pending delete is preserved
	head = chains[0].GetHead()
	require.NotNil(t, head)
	assert.Equal(t, t2.id, head.DeletedBy)
	assert.Equal(t, uint64(0), head.DeleteTS)
}

// =============================================================================
// TestVacuum_MultipleVacuumRuns
// =============================================================================
// Scenario:
// - Insert and update rows
// - Run vacuum multiple times
// - Verify vacuum is idempotent and statistics accumulate correctly

func TestVacuum_MultipleVacuumRuns(t *testing.T) {
	mockClock := quartz.NewMock(t)
	mvccManager := newSimulatedMVCCManager()

	// Create a table
	table := NewTable("test_table", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Insert and update a row
	txn := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
	rowID, err := table.InsertVersioned(txn, []any{int32(1)})
	require.NoError(t, err)
	commitTS := mvccManager.Commit(txn)
	table.CommitVersions(txn, commitTS)

	for i := 0; i < 5; i++ {
		txn := mvccManager.BeginTransaction(parser.IsolationLevelSerializable)
		err := table.UpdateVersioned(txn, rowID, []any{int32(i + 2)})
		require.NoError(t, err)
		commitTS := mvccManager.Commit(txn)
		table.CommitVersions(txn, commitTS)
	}

	chains := table.GetVersionChains()
	require.Len(t, chains, 1)
	assert.Equal(t, 6, chains[0].Len(), "should have 6 versions")

	// Create vacuum
	vacuum := NewVacuum(mvccManager.GetLowWatermark, mockClock)

	// First vacuum run
	removed1 := vacuum.VacuumChains(chains)
	assert.Equal(t, 5, removed1, "first vacuum should remove 5 versions")
	assert.Equal(t, 1, chains[0].Len(), "should have 1 version after first vacuum")

	stats1 := vacuum.GetStatistics()
	assert.Equal(t, uint64(5), stats1.VersionsRemoved)
	assert.Equal(t, uint64(1), stats1.ChainsProcessed)

	// Second vacuum run - should be idempotent
	removed2 := vacuum.VacuumChains(chains)
	assert.Equal(t, 0, removed2, "second vacuum should remove 0 versions")
	assert.Equal(t, 1, chains[0].Len(), "should still have 1 version")

	stats2 := vacuum.GetStatistics()
	assert.Equal(t, uint64(5), stats2.VersionsRemoved, "total should still be 5")
	assert.Equal(t, uint64(2), stats2.ChainsProcessed, "should have processed chain twice")

	// Third vacuum run
	removed3 := vacuum.VacuumChains(chains)
	assert.Equal(t, 0, removed3, "third vacuum should remove 0 versions")

	stats3 := vacuum.GetStatistics()
	assert.Equal(t, uint64(5), stats3.VersionsRemoved, "total should still be 5")
	assert.Equal(t, uint64(3), stats3.ChainsProcessed, "should have processed chain three times")
}
