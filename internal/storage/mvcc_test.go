package storage

import (
	"testing"
	"time"

	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/assert"
)

// mockTransactionContext implements TransactionContext for testing.
type mockTransactionContext struct {
	txnID                uint64
	isolationLevel       parser.IsolationLevel
	startTime            time.Time
	statementTime        time.Time
	committedTxns        map[uint64]bool
	abortedTxns          map[uint64]bool
	snapshot             *Snapshot
	activeTxnsAtSnapshot map[uint64]bool
}

func newMockTransactionContext(
	txnID uint64,
	isolation parser.IsolationLevel,
) *mockTransactionContext {
	return &mockTransactionContext{
		txnID:                txnID,
		isolationLevel:       isolation,
		startTime:            time.Now(),
		statementTime:        time.Now(),
		committedTxns:        make(map[uint64]bool),
		abortedTxns:          make(map[uint64]bool),
		activeTxnsAtSnapshot: make(map[uint64]bool),
	}
}

func (m *mockTransactionContext) GetTxnID() uint64 {
	return m.txnID
}

func (m *mockTransactionContext) GetIsolationLevel() parser.IsolationLevel {
	return m.isolationLevel
}

func (m *mockTransactionContext) GetStartTime() time.Time {
	return m.startTime
}

func (m *mockTransactionContext) GetStatementTime() time.Time {
	return m.statementTime
}

func (m *mockTransactionContext) IsCommitted(txnID uint64) bool {
	return m.committedTxns[txnID]
}

func (m *mockTransactionContext) IsAborted(txnID uint64) bool {
	return m.abortedTxns[txnID]
}

func (m *mockTransactionContext) setCommitted(txnID uint64) {
	m.committedTxns[txnID] = true
}

func (m *mockTransactionContext) setAborted(txnID uint64) {
	m.abortedTxns[txnID] = true
}

func (m *mockTransactionContext) GetSnapshot() *Snapshot {
	return m.snapshot
}

func (m *mockTransactionContext) WasActiveAtSnapshot(txnID uint64) bool {
	if m.snapshot != nil {
		return m.snapshot.WasActiveAtSnapshot(txnID)
	}
	return m.activeTxnsAtSnapshot[txnID]
}

func (m *mockTransactionContext) setSnapshot(snapshot *Snapshot) {
	m.snapshot = snapshot
}

// TestVersionInfoIsDeleted tests the IsDeleted method.
func TestVersionInfoIsDeleted(t *testing.T) {
	tests := []struct {
		name     string
		version  VersionInfo
		expected bool
	}{
		{
			name: "not deleted",
			version: VersionInfo{
				CreatedTxnID: 1,
				DeletedTxnID: 0,
			},
			expected: false,
		},
		{
			name: "deleted",
			version: VersionInfo{
				CreatedTxnID: 1,
				DeletedTxnID: 2,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.version.IsDeleted())
		})
	}
}

// TestVersionInfoIsActive tests the IsActive method.
func TestVersionInfoIsActive(t *testing.T) {
	tests := []struct {
		name     string
		version  VersionInfo
		expected bool
	}{
		{
			name: "active row",
			version: VersionInfo{
				CreatedTxnID: 1,
				DeletedTxnID: 0,
			},
			expected: true,
		},
		{
			name: "deleted row",
			version: VersionInfo{
				CreatedTxnID: 1,
				DeletedTxnID: 2,
			},
			expected: false,
		},
		{
			name: "never created",
			version: VersionInfo{
				CreatedTxnID: 0,
				DeletedTxnID: 0,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.version.IsActive())
		})
	}
}

// TestReadUncommittedVisibility_OwnChangesVisible tests that a transaction
// can see its own uncommitted changes.
func TestReadUncommittedVisibility_OwnChangesVisible(t *testing.T) {
	checker := NewReadUncommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)

	// Row created by own transaction
	version := VersionInfo{
		CreatedTxnID: 100, // Same as txn.GetTxnID()
		DeletedTxnID: 0,
		Committed:    false,
	}

	assert.True(t, checker.IsVisible(version, txn),
		"Transaction should see its own uncommitted rows")
}

// TestReadUncommittedVisibility_DirtyReadAllowed tests that READ UNCOMMITTED
// allows dirty reads from other uncommitted transactions.
func TestReadUncommittedVisibility_DirtyReadAllowed(t *testing.T) {
	checker := NewReadUncommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)

	// Row created by another uncommitted transaction (T2)
	version := VersionInfo{
		CreatedTxnID: 200, // Different transaction
		DeletedTxnID: 0,
		Committed:    false, // T2 has not committed
	}
	// T2 is not committed
	// T2 is not aborted (default)

	assert.True(t, checker.IsVisible(version, txn),
		"READ UNCOMMITTED should allow dirty reads from uncommitted transactions")
}

// TestReadUncommittedVisibility_UncommittedDeleteHidesRow tests that a row
// deleted by an uncommitted transaction is not visible.
func TestReadUncommittedVisibility_UncommittedDeleteHidesRow(t *testing.T) {
	checker := NewReadUncommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)

	// Row deleted by another uncommitted transaction (T2)
	version := VersionInfo{
		CreatedTxnID: 50,  // Created by committed transaction
		DeletedTxnID: 200, // Deleted by T2 (uncommitted)
		Committed:    true,
	}
	txn.setCommitted(50)
	// T2 (200) is not committed yet

	assert.False(t, checker.IsVisible(version, txn),
		"Row deleted by uncommitted transaction should not be visible")
}

// TestReadUncommittedVisibility_AbortedTransactionNotVisible tests that rows
// from aborted transactions are not visible.
func TestReadUncommittedVisibility_AbortedTransactionNotVisible(t *testing.T) {
	checker := NewReadUncommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)

	// Row created by an aborted transaction (T2)
	version := VersionInfo{
		CreatedTxnID: 200, // Created by T2
		DeletedTxnID: 0,
		Committed:    false,
	}
	txn.setAborted(200) // T2 has aborted

	assert.False(t, checker.IsVisible(version, txn),
		"Rows from aborted transactions should not be visible")
}

// TestReadUncommittedVisibility_CommittedRowVisible tests that committed
// rows are visible.
func TestReadUncommittedVisibility_CommittedRowVisible(t *testing.T) {
	checker := NewReadUncommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)

	// Row created by a committed transaction
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 0,
		Committed:    true,
	}
	txn.setCommitted(50)

	assert.True(t, checker.IsVisible(version, txn),
		"Committed rows should be visible")
}

// TestReadUncommittedVisibility_DeletedRowNotVisible tests that deleted
// rows are not visible even if the delete is committed.
func TestReadUncommittedVisibility_DeletedRowNotVisible(t *testing.T) {
	checker := NewReadUncommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)

	// Row that has been deleted
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 60,
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setCommitted(60)

	assert.False(t, checker.IsVisible(version, txn),
		"Deleted rows should not be visible")
}

// TestReadUncommittedVisibility_NeverCreatedRowNotVisible tests that a row
// with CreatedTxnID of 0 is not visible.
func TestReadUncommittedVisibility_NeverCreatedRowNotVisible(t *testing.T) {
	checker := NewReadUncommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)

	// Row that was never created (invalid state)
	version := VersionInfo{
		CreatedTxnID: 0,
		DeletedTxnID: 0,
	}

	assert.False(t, checker.IsVisible(version, txn),
		"Row with no creator should not be visible")
}

// TestReadUncommittedVisibility_OwnDeletedRowNotVisible tests that a row
// deleted by the current transaction is not visible.
func TestReadUncommittedVisibility_OwnDeletedRowNotVisible(t *testing.T) {
	checker := NewReadUncommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)

	// Row deleted by own transaction
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 100, // Deleted by current transaction
		Committed:    true,
	}
	txn.setCommitted(50)

	assert.False(t, checker.IsVisible(version, txn),
		"Row deleted by own transaction should not be visible")
}

// TestReadUncommittedVisibility_MultipleScenarios tests multiple visibility
// scenarios in sequence simulating a more realistic workflow.
func TestReadUncommittedVisibility_MultipleScenarios(t *testing.T) {
	checker := NewReadUncommittedVisibility()

	// Setup: Transaction T1 (100) with READ UNCOMMITTED
	t1 := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)

	// Scenario 1: T1 sees committed data from old transaction
	t.Run("sees committed data", func(t *testing.T) {
		t1.setCommitted(10) // Old committed transaction
		version := VersionInfo{
			CreatedTxnID: 10,
			DeletedTxnID: 0,
			Committed:    true,
		}
		assert.True(t, checker.IsVisible(version, t1))
	})

	// Scenario 2: T1 sees uncommitted data from active T2 (dirty read)
	t.Run("dirty read from active T2", func(t *testing.T) {
		// T2 (200) is active, not committed, not aborted
		version := VersionInfo{
			CreatedTxnID: 200,
			DeletedTxnID: 0,
			Committed:    false,
		}
		assert.True(t, checker.IsVisible(version, t1))
	})

	// Scenario 3: T2 deletes a row (uncommitted) - T1 doesn't see it
	t.Run("uncommitted delete hides row", func(t *testing.T) {
		t1.setCommitted(10)
		version := VersionInfo{
			CreatedTxnID: 10,
			DeletedTxnID: 200, // T2 deleted it
			Committed:    true,
		}
		assert.False(t, checker.IsVisible(version, t1))
	})

	// Scenario 4: T2 aborts - T1 doesn't see T2's rows
	t.Run("aborted transaction rows hidden", func(t *testing.T) {
		t1.setAborted(200) // T2 aborted
		version := VersionInfo{
			CreatedTxnID: 200,
			DeletedTxnID: 0,
			Committed:    false,
		}
		assert.False(t, checker.IsVisible(version, t1))
	})
}

// TestNewReadUncommittedVisibility tests the constructor.
func TestNewReadUncommittedVisibility(t *testing.T) {
	checker := NewReadUncommittedVisibility()
	assert.NotNil(t, checker)

	// Verify it implements VisibilityChecker
	var _ VisibilityChecker = checker
}

// =============================================================================
// READ COMMITTED Visibility Tests
// =============================================================================

// TestNewReadCommittedVisibility tests the constructor.
func TestNewReadCommittedVisibility(t *testing.T) {
	checker := NewReadCommittedVisibility()
	assert.NotNil(t, checker)

	// Verify it implements VisibilityChecker
	var _ VisibilityChecker = checker
}

// TestReadCommittedVisibility_OwnChangesVisible tests that a transaction
// can see its own uncommitted changes.
func TestReadCommittedVisibility_OwnChangesVisible(t *testing.T) {
	checker := NewReadCommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)

	// Row created by own transaction (uncommitted)
	version := VersionInfo{
		CreatedTxnID: 100, // Same as txn.GetTxnID()
		DeletedTxnID: 0,
		Committed:    false,
	}

	assert.True(t, checker.IsVisible(version, txn),
		"Transaction should see its own uncommitted rows in READ COMMITTED")
}

// TestReadCommittedVisibility_DirtyReadPrevented tests that READ COMMITTED
// prevents dirty reads from other uncommitted transactions.
func TestReadCommittedVisibility_DirtyReadPrevented(t *testing.T) {
	checker := NewReadCommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)

	// Row created by another uncommitted transaction (T2)
	version := VersionInfo{
		CreatedTxnID: 200, // Different transaction
		DeletedTxnID: 0,
		Committed:    false, // T2 has not committed
	}
	// T2 is not committed (dirty read should be prevented)

	assert.False(t, checker.IsVisible(version, txn),
		"READ COMMITTED should prevent dirty reads from uncommitted transactions")
}

// TestReadCommittedVisibility_CommittedDataVisible tests that committed
// data from other transactions is visible.
func TestReadCommittedVisibility_CommittedDataVisible(t *testing.T) {
	checker := NewReadCommittedVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	txn.statementTime = baseTime

	// Row created by another committed transaction before statement start
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(-1 * time.Hour), // Created 1 hour before statement
		Committed:    true,
	}
	txn.setCommitted(50)

	assert.True(t, checker.IsVisible(version, txn),
		"READ COMMITTED should see committed data from other transactions")
}

// TestReadCommittedVisibility_CommittedAfterStatementNotVisible tests that
// data committed after the statement started is not visible.
func TestReadCommittedVisibility_CommittedAfterStatementNotVisible(t *testing.T) {
	checker := NewReadCommittedVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	txn.statementTime = baseTime

	// Row created by another transaction after statement start
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(1 * time.Second), // Created after statement
		Committed:    true,
	}
	txn.setCommitted(50)

	assert.False(t, checker.IsVisible(version, txn),
		"READ COMMITTED should not see data committed after statement start")
}

// TestReadCommittedVisibility_NonRepeatableReadAllowed tests that non-repeatable
// reads are allowed (different values on subsequent reads within same transaction).
func TestReadCommittedVisibility_NonRepeatableReadAllowed(t *testing.T) {
	checker := NewReadCommittedVisibility()

	// Scenario: T1 reads, T2 updates and commits, T1 reads again
	// First statement at time T
	t1Read1Time := time.Now()
	t1 := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	t1.statementTime = t1Read1Time

	// Original row - visible in first read
	originalVersion := VersionInfo{
		CreatedTxnID: 10,
		DeletedTxnID: 0,
		CreatedTime:  t1Read1Time.Add(-1 * time.Hour),
		Committed:    true,
	}
	t1.setCommitted(10)

	assert.True(t, checker.IsVisible(originalVersion, t1),
		"First read should see original row")

	// T2 updates the row (creates new version, marks old as deleted)
	t2CommitTime := t1Read1Time.Add(1 * time.Second)

	// Update T1's statement time for second read
	t1.statementTime = t1Read1Time.Add(2 * time.Second)

	// Original row is now deleted by T2
	deletedOriginal := VersionInfo{
		CreatedTxnID: 10,
		DeletedTxnID: 200, // T2 deleted it
		CreatedTime:  t1Read1Time.Add(-1 * time.Hour),
		DeletedTime:  t2CommitTime,
		Committed:    true,
	}
	t1.setCommitted(200)

	// New version created by T2
	newVersion := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		CreatedTime:  t2CommitTime,
		Committed:    true,
	}

	// In second read (after T2 committed), T1 should NOT see deleted original
	assert.False(t, checker.IsVisible(deletedOriginal, t1),
		"Second read should not see deleted original row")

	// T1 should see the new version (non-repeatable read)
	assert.True(t, checker.IsVisible(newVersion, t1),
		"Second read should see new value (non-repeatable read allowed)")
}

// TestReadCommittedVisibility_AbortedTransactionNotVisible tests that rows
// from aborted transactions are not visible.
func TestReadCommittedVisibility_AbortedTransactionNotVisible(t *testing.T) {
	checker := NewReadCommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)

	// Row created by an aborted transaction (T2)
	version := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		Committed:    false,
	}
	txn.setAborted(200)

	assert.False(t, checker.IsVisible(version, txn),
		"Rows from aborted transactions should not be visible")
}

// TestReadCommittedVisibility_NeverCreatedRowNotVisible tests that a row
// with CreatedTxnID of 0 is not visible.
func TestReadCommittedVisibility_NeverCreatedRowNotVisible(t *testing.T) {
	checker := NewReadCommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)

	// Row that was never created (invalid state)
	version := VersionInfo{
		CreatedTxnID: 0,
		DeletedTxnID: 0,
	}

	assert.False(t, checker.IsVisible(version, txn),
		"Row with no creator should not be visible")
}

// TestReadCommittedVisibility_OwnDeletedRowNotVisible tests that a row
// deleted by the current transaction is not visible.
func TestReadCommittedVisibility_OwnDeletedRowNotVisible(t *testing.T) {
	checker := NewReadCommittedVisibility()
	txn := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)

	// Row created by us and deleted by us
	version := VersionInfo{
		CreatedTxnID: 100, // Created by current transaction
		DeletedTxnID: 100, // Deleted by current transaction
		Committed:    false,
	}

	assert.False(t, checker.IsVisible(version, txn),
		"Row deleted by own transaction should not be visible")
}

// TestReadCommittedVisibility_CommittedDeleteHidesRow tests that a row
// deleted and committed before statement start is not visible.
func TestReadCommittedVisibility_CommittedDeleteHidesRow(t *testing.T) {
	checker := NewReadCommittedVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	txn.statementTime = baseTime

	// Row deleted by T2, which committed before statement start
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 60,
		CreatedTime:  baseTime.Add(-2 * time.Hour),
		DeletedTime:  baseTime.Add(-1 * time.Hour), // Deleted before statement start
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setCommitted(60)

	assert.False(t, checker.IsVisible(version, txn),
		"Row deleted and committed before statement start should not be visible")
}

// TestReadCommittedVisibility_UncommittedDeleteDoesNotHideRow tests that a row
// deleted by an uncommitted transaction is still visible.
func TestReadCommittedVisibility_UncommittedDeleteDoesNotHideRow(t *testing.T) {
	checker := NewReadCommittedVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	txn.statementTime = baseTime

	// Row created by committed transaction, deleted by uncommitted T2
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200, // T2 deleted it but hasn't committed
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  baseTime.Add(-1 * time.Minute),
		Committed:    true,
	}
	txn.setCommitted(50)
	// T2 (200) is NOT committed

	assert.True(t, checker.IsVisible(version, txn),
		"Row deleted by uncommitted transaction should still be visible in READ COMMITTED")
}

// TestReadCommittedVisibility_DeleteCommittedAfterStatementStillVisible tests that
// a row deleted by a transaction that committed after statement start is still visible.
func TestReadCommittedVisibility_DeleteCommittedAfterStatementStillVisible(t *testing.T) {
	checker := NewReadCommittedVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	txn.statementTime = baseTime

	// Row deleted by T2, which committed after statement start
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200,
		CreatedTime:  baseTime.Add(-2 * time.Hour),
		DeletedTime:  baseTime.Add(1 * time.Second), // Committed after statement start
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setCommitted(200)

	assert.True(t, checker.IsVisible(version, txn),
		"Row deleted after statement start should still be visible in this statement")
}

// TestReadCommittedVisibility_AbortedDeleteDoesNotHideRow tests that a row
// deleted by an aborted transaction is still visible.
func TestReadCommittedVisibility_AbortedDeleteDoesNotHideRow(t *testing.T) {
	checker := NewReadCommittedVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	txn.statementTime = baseTime

	// Row deleted by an aborted transaction
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200,
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  baseTime.Add(-30 * time.Minute),
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setAborted(200) // T2 aborted

	assert.True(t, checker.IsVisible(version, txn),
		"Row deleted by aborted transaction should still be visible")
}

// TestReadCommittedVisibility_StatementLevelSnapshot tests the statement-level
// snapshot behavior where each statement gets a fresh view.
func TestReadCommittedVisibility_StatementLevelSnapshot(t *testing.T) {
	checker := NewReadCommittedVisibility()

	// Scenario: Transaction T1 executes two statements
	// T2 commits between the two statements
	// Second statement should see T2's changes

	baseTime := time.Now()

	// First statement
	t1 := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	t1.startTime = baseTime
	t1.statementTime = baseTime

	// Row from T2 that will be committed between statements
	t2Row := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(500 * time.Millisecond),
		Committed:    true,
	}

	// First statement - T2 not yet committed
	// (not calling setCommitted yet)
	assert.False(t, checker.IsVisible(t2Row, t1),
		"First statement should not see uncommitted T2 data")

	// T2 commits
	t1.setCommitted(200)

	// T1 executes second statement (new statement time)
	t1.statementTime = baseTime.Add(1 * time.Second)

	// Second statement should see T2's committed data
	assert.True(t, checker.IsVisible(t2Row, t1),
		"Second statement should see T2's committed data (statement-level snapshot)")
}

// TestReadCommittedVisibility_MultipleScenarios tests multiple visibility
// scenarios simulating a realistic workflow.
func TestReadCommittedVisibility_MultipleScenarios(t *testing.T) {
	checker := NewReadCommittedVisibility()
	baseTime := time.Now()

	// Setup: Transaction T1 (100) with READ COMMITTED
	t1 := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	t1.statementTime = baseTime

	// Scenario 1: T1 sees committed data from old transaction
	t.Run("sees committed data", func(t *testing.T) {
		t1.setCommitted(10)
		version := VersionInfo{
			CreatedTxnID: 10,
			DeletedTxnID: 0,
			CreatedTime:  baseTime.Add(-1 * time.Hour),
			Committed:    true,
		}
		assert.True(t, checker.IsVisible(version, t1))
	})

	// Scenario 2: T1 does NOT see uncommitted data from active T2 (no dirty read)
	t.Run("no dirty read from active T2", func(t *testing.T) {
		// T2 (200) is active, not committed
		version := VersionInfo{
			CreatedTxnID: 200,
			DeletedTxnID: 0,
			Committed:    false,
		}
		assert.False(t, checker.IsVisible(version, t1))
	})

	// Scenario 3: T2 deletes a row (uncommitted) - T1 STILL sees it
	t.Run("uncommitted delete does not hide row", func(t *testing.T) {
		t1.setCommitted(10)
		version := VersionInfo{
			CreatedTxnID: 10,
			DeletedTxnID: 200, // T2 deleted it but not committed
			CreatedTime:  baseTime.Add(-1 * time.Hour),
			DeletedTime:  baseTime.Add(-1 * time.Minute),
			Committed:    true,
		}
		assert.True(t, checker.IsVisible(version, t1),
			"Uncommitted delete should not hide row in READ COMMITTED")
	})

	// Scenario 4: T2 commits - now in next statement, T1 can see T2's data
	t.Run("committed data visible in next statement", func(t *testing.T) {
		t1.setCommitted(200)
		t1.statementTime = baseTime.Add(1 * time.Second) // New statement

		version := VersionInfo{
			CreatedTxnID: 200,
			DeletedTxnID: 0,
			CreatedTime:  baseTime.Add(500 * time.Millisecond),
			Committed:    true,
		}
		assert.True(t, checker.IsVisible(version, t1))
	})

	// Scenario 5: T2 aborts - T1 doesn't see T2's rows
	t.Run("aborted transaction rows hidden", func(t *testing.T) {
		t1.setAborted(300) // T3 aborted
		version := VersionInfo{
			CreatedTxnID: 300,
			DeletedTxnID: 0,
			Committed:    false,
		}
		assert.False(t, checker.IsVisible(version, t1))
	})
}

// TestReadCommittedVisibility_PhantomReadAllowed tests that phantom reads
// are allowed (new rows appear in subsequent queries).
func TestReadCommittedVisibility_PhantomReadAllowed(t *testing.T) {
	checker := NewReadCommittedVisibility()
	baseTime := time.Now()

	// Transaction T1 with READ COMMITTED
	t1 := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	t1.startTime = baseTime
	t1.statementTime = baseTime

	// First query: No row from T2
	t2Row := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(1 * time.Second), // Will be created after first statement
		Committed:    true,
	}

	// T2 not committed yet in first statement
	assert.False(t, checker.IsVisible(t2Row, t1),
		"First query should not see T2's row")

	// T2 commits and T1 executes second statement
	t1.setCommitted(200)
	t1.statementTime = baseTime.Add(2 * time.Second)

	// Second query: T2's row appears (phantom read)
	assert.True(t, checker.IsVisible(t2Row, t1),
		"Second query should see T2's new row (phantom read allowed)")
}

// TestReadCommittedVisibility_OwnRowDeletedByOtherCommittedTransaction tests
// the edge case where our own row is deleted by another committed transaction.
func TestReadCommittedVisibility_OwnRowDeletedByOtherCommittedTransaction(t *testing.T) {
	checker := NewReadCommittedVisibility()
	baseTime := time.Now()

	t1 := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	t1.statementTime = baseTime

	// Row created by us, but deleted by T2 which committed before our statement
	version := VersionInfo{
		CreatedTxnID: 100, // Created by us
		DeletedTxnID: 200, // Deleted by T2
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  baseTime.Add(-30 * time.Minute), // Before statement
		Committed:    false,                           // Our create is not committed
	}
	t1.setCommitted(200)

	// Even though we created it, T2's committed delete should hide it
	assert.False(t, checker.IsVisible(version, t1),
		"Our row deleted by committed transaction should not be visible")
}

// TestReadCommittedVisibility_OwnRowDeletedByOtherUncommittedTransaction tests
// the edge case where our own row is deleted by another uncommitted transaction.
func TestReadCommittedVisibility_OwnRowDeletedByOtherUncommittedTransaction(t *testing.T) {
	checker := NewReadCommittedVisibility()
	baseTime := time.Now()

	t1 := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
	t1.statementTime = baseTime

	// Row created by us, but T2 is trying to delete it (uncommitted)
	version := VersionInfo{
		CreatedTxnID: 100, // Created by us
		DeletedTxnID: 200, // T2 trying to delete (uncommitted)
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  baseTime.Add(-1 * time.Minute),
		Committed:    false,
	}
	// T2 (200) is NOT committed

	// Our row, T2's delete is uncommitted - we should still see it
	assert.True(t, checker.IsVisible(version, t1),
		"Our row should be visible when delete is uncommitted")
}

// =============================================================================
// REPEATABLE READ Visibility Tests
// =============================================================================

// TestNewRepeatableReadVisibility tests the constructor.
func TestNewRepeatableReadVisibility(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	assert.NotNil(t, checker)

	// Verify it implements VisibilityChecker
	var _ VisibilityChecker = checker
}

// TestRepeatableReadVisibility_OwnChangesVisible tests that a transaction
// can see its own uncommitted changes.
func TestRepeatableReadVisibility_OwnChangesVisible(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by own transaction (uncommitted)
	version := VersionInfo{
		CreatedTxnID: 100, // Same as txn.GetTxnID()
		DeletedTxnID: 0,
		Committed:    false,
	}

	assert.True(t, checker.IsVisible(version, txn),
		"Transaction should see its own uncommitted rows in REPEATABLE READ")
}

// TestRepeatableReadVisibility_DirtyReadPrevented tests that REPEATABLE READ
// prevents dirty reads from other uncommitted transactions.
func TestRepeatableReadVisibility_DirtyReadPrevented(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{200})) // T2 was active at snapshot

	// Row created by another uncommitted transaction (T2)
	version := VersionInfo{
		CreatedTxnID: 200, // Different transaction
		DeletedTxnID: 0,
		Committed:    false, // T2 has not committed
	}
	// T2 is not committed (dirty read should be prevented)

	assert.False(t, checker.IsVisible(version, txn),
		"REPEATABLE READ should prevent dirty reads from uncommitted transactions")
}

// TestRepeatableReadVisibility_CommittedBeforeSnapshotVisible tests that
// data committed before the snapshot is visible.
func TestRepeatableReadVisibility_CommittedBeforeSnapshotVisible(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by another committed transaction before snapshot
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(-1 * time.Hour), // Created 1 hour before snapshot
		Committed:    true,
	}
	txn.setCommitted(50)

	assert.True(t, checker.IsVisible(version, txn),
		"REPEATABLE READ should see data committed before snapshot")
}

// TestRepeatableReadVisibility_CommittedAfterSnapshotNotVisible tests that
// data committed after the snapshot is NOT visible (key REPEATABLE READ behavior).
func TestRepeatableReadVisibility_CommittedAfterSnapshotNotVisible(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by another transaction after snapshot
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(1 * time.Second), // Created after snapshot
		Committed:    true,
	}
	txn.setCommitted(50)

	assert.False(t, checker.IsVisible(version, txn),
		"REPEATABLE READ should NOT see data committed after snapshot")
}

// TestRepeatableReadVisibility_ActiveAtSnapshotNotVisible tests that rows
// from transactions that were active at snapshot time are not visible.
func TestRepeatableReadVisibility_ActiveAtSnapshotNotVisible(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	// T1 creates snapshot when T2 is active
	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{200})) // T2 was active at snapshot

	// Row created by T2 which was active at snapshot time
	// Even if T2 commits later, T1 should NOT see T2's rows
	version := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(-1 * time.Minute), // T2 created before T1's snapshot
		Committed:    true,
	}
	txn.setCommitted(200) // T2 committed after T1's snapshot

	assert.False(t, checker.IsVisible(version, txn),
		"REPEATABLE READ should NOT see rows from transactions active at snapshot time")
}

// TestRepeatableReadVisibility_NonRepeatableReadPrevented tests the key
// REPEATABLE READ behavior: re-reading data returns the same values.
func TestRepeatableReadVisibility_NonRepeatableReadPrevented(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	// Scenario: T1 reads at snapshot time, T2 updates and commits,
	// T1 reads again and should still see original values

	// T1 with REPEATABLE READ, snapshot at baseTime
	t1 := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	t1.startTime = baseTime
	t1.setSnapshot(NewSnapshot(baseTime, []uint64{}))
	t1.setCommitted(10) // Old transaction that created original data

	// First read: T1 sees original data (committed before snapshot)
	originalVersion := VersionInfo{
		CreatedTxnID: 10,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		Committed:    true,
	}

	assert.True(t, checker.IsVisible(originalVersion, t1),
		"First read should see original row")

	// T2 updates the row: deletes old version, creates new version
	// T2 commits AFTER T1's snapshot
	t2CommitTime := baseTime.Add(1 * time.Second)

	// Original row is now deleted by T2
	deletedOriginal := VersionInfo{
		CreatedTxnID: 10,
		DeletedTxnID: 200, // T2 deleted it
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  t2CommitTime,
		Committed:    true,
	}
	t1.setCommitted(200)

	// New version created by T2
	newVersion := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		CreatedTime:  t2CommitTime,
		Committed:    true,
	}

	// In REPEATABLE READ, T1 should still see the original (delete was after snapshot)
	assert.True(t, checker.IsVisible(deletedOriginal, t1),
		"REPEATABLE READ should still see original row (delete after snapshot)")

	// T1 should NOT see the new version (created after snapshot)
	assert.False(t, checker.IsVisible(newVersion, t1),
		"REPEATABLE READ should NOT see new value committed after snapshot")
}

// TestRepeatableReadVisibility_TransactionSnapshotVsStatementSnapshot tests
// that REPEATABLE READ uses transaction-level snapshot, not statement-level.
func TestRepeatableReadVisibility_TransactionSnapshotVsStatementSnapshot(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	// T1 starts at baseTime
	t1 := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	t1.startTime = baseTime
	t1.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// First statement at baseTime
	t1.statementTime = baseTime

	// Row from T2 that commits between statements
	t2Row := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(500 * time.Millisecond), // After transaction start
		Committed:    true,
	}
	t1.setCommitted(200)

	// First statement - T2's row created after snapshot, should not be visible
	assert.False(t, checker.IsVisible(t2Row, t1),
		"First statement should not see T2's row (created after snapshot)")

	// Update statement time to after T2's commit
	// In READ COMMITTED this would make T2's row visible
	// In REPEATABLE READ it should STILL not be visible
	t1.statementTime = baseTime.Add(1 * time.Second)

	// Second statement - STILL should NOT see T2's data
	// This is the key difference from READ COMMITTED!
	assert.False(
		t,
		checker.IsVisible(t2Row, t1),
		"Second statement should STILL not see T2's data (REPEATABLE READ uses transaction snapshot)",
	)
}

// TestRepeatableReadVisibility_AbortedTransactionNotVisible tests that rows
// from aborted transactions are not visible.
func TestRepeatableReadVisibility_AbortedTransactionNotVisible(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by an aborted transaction (T2)
	version := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		Committed:    false,
	}
	txn.setAborted(200)

	assert.False(t, checker.IsVisible(version, txn),
		"Rows from aborted transactions should not be visible")
}

// TestRepeatableReadVisibility_NeverCreatedRowNotVisible tests that a row
// with CreatedTxnID of 0 is not visible.
func TestRepeatableReadVisibility_NeverCreatedRowNotVisible(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row that was never created (invalid state)
	version := VersionInfo{
		CreatedTxnID: 0,
		DeletedTxnID: 0,
	}

	assert.False(t, checker.IsVisible(version, txn),
		"Row with no creator should not be visible")
}

// TestRepeatableReadVisibility_OwnDeletedRowNotVisible tests that a row
// deleted by the current transaction is not visible.
func TestRepeatableReadVisibility_OwnDeletedRowNotVisible(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by us and deleted by us
	version := VersionInfo{
		CreatedTxnID: 100, // Created by current transaction
		DeletedTxnID: 100, // Deleted by current transaction
		Committed:    false,
	}

	assert.False(t, checker.IsVisible(version, txn),
		"Row deleted by own transaction should not be visible")
}

// TestRepeatableReadVisibility_DeletedBeforeSnapshotNotVisible tests that
// rows deleted and committed before snapshot are not visible.
func TestRepeatableReadVisibility_DeletedBeforeSnapshotNotVisible(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row deleted by T2, which committed before snapshot
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 60,
		CreatedTime:  baseTime.Add(-2 * time.Hour),
		DeletedTime:  baseTime.Add(-1 * time.Hour), // Deleted before snapshot
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setCommitted(60)

	assert.False(t, checker.IsVisible(version, txn),
		"Row deleted before snapshot should not be visible")
}

// TestRepeatableReadVisibility_DeletedAfterSnapshotStillVisible tests that
// rows deleted after the snapshot are still visible.
func TestRepeatableReadVisibility_DeletedAfterSnapshotStillVisible(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row deleted by T2 after snapshot
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200,
		CreatedTime:  baseTime.Add(-2 * time.Hour),
		DeletedTime:  baseTime.Add(1 * time.Second), // Deleted after snapshot
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setCommitted(200)

	assert.True(t, checker.IsVisible(version, txn),
		"Row deleted after snapshot should still be visible")
}

// TestRepeatableReadVisibility_DeletedByActiveAtSnapshotStillVisible tests that
// rows deleted by transactions active at snapshot time are still visible.
func TestRepeatableReadVisibility_DeletedByActiveAtSnapshotStillVisible(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	// T2 was active at snapshot time
	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{200})) // T2 was active

	// Row deleted by T2 (which was active at snapshot)
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200,
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  baseTime.Add(1 * time.Second),
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setCommitted(200) // T2 commits after snapshot

	// Row should be visible because T2's delete was not committed at snapshot time
	assert.True(t, checker.IsVisible(version, txn),
		"Row deleted by transaction active at snapshot should still be visible")
}

// TestRepeatableReadVisibility_UncommittedDeleteDoesNotHideRow tests that
// a delete by an uncommitted transaction does not hide the row.
func TestRepeatableReadVisibility_UncommittedDeleteDoesNotHideRow(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by committed transaction, deleted by uncommitted T2
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200, // T2 deleted it but hasn't committed
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  baseTime.Add(-1 * time.Minute),
		Committed:    true,
	}
	txn.setCommitted(50)
	// T2 (200) is NOT committed

	assert.True(t, checker.IsVisible(version, txn),
		"Row deleted by uncommitted transaction should still be visible")
}

// TestRepeatableReadVisibility_AbortedDeleteDoesNotHideRow tests that
// a delete by an aborted transaction does not hide the row.
func TestRepeatableReadVisibility_AbortedDeleteDoesNotHideRow(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row deleted by an aborted transaction
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200,
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  baseTime.Add(-30 * time.Minute),
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setAborted(200) // T2 aborted

	assert.True(t, checker.IsVisible(version, txn),
		"Row deleted by aborted transaction should still be visible")
}

// TestRepeatableReadVisibility_WithoutSnapshot tests fallback to startTime
// when no snapshot is provided.
func TestRepeatableReadVisibility_WithoutSnapshot(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	// No snapshot set - should fall back to startTime
	txn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	txn.startTime = baseTime
	// No snapshot!

	// Row committed before start time - visible
	version1 := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		Committed:    true,
	}
	txn.setCommitted(50)

	assert.True(t, checker.IsVisible(version1, txn),
		"Row committed before start time should be visible even without snapshot")

	// Row committed after start time - NOT visible
	version2 := VersionInfo{
		CreatedTxnID: 60,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(1 * time.Second),
		Committed:    true,
	}
	txn.setCommitted(60)

	assert.False(t, checker.IsVisible(version2, txn),
		"Row committed after start time should not be visible")
}

// TestRepeatableReadVisibility_MultipleScenarios tests multiple visibility
// scenarios simulating a realistic REPEATABLE READ workflow.
func TestRepeatableReadVisibility_MultipleScenarios(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	// Setup: Transaction T1 (100) with REPEATABLE READ
	// T2 (200) is active when T1 starts
	t1 := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	t1.startTime = baseTime
	t1.setSnapshot(NewSnapshot(baseTime, []uint64{200})) // T2 was active at snapshot
	t1.setCommitted(10)                                  // Old committed transaction

	// Scenario 1: T1 sees data committed before snapshot
	t.Run("sees data committed before snapshot", func(t *testing.T) {
		version := VersionInfo{
			CreatedTxnID: 10,
			DeletedTxnID: 0,
			CreatedTime:  baseTime.Add(-1 * time.Hour),
			Committed:    true,
		}
		assert.True(t, checker.IsVisible(version, t1))
	})

	// Scenario 2: T1 does NOT see data from T2 (was active at snapshot)
	t.Run("does not see data from active-at-snapshot transaction", func(t *testing.T) {
		t1.setCommitted(200) // T2 committed after snapshot
		version := VersionInfo{
			CreatedTxnID: 200,
			DeletedTxnID: 0,
			CreatedTime:  baseTime.Add(-1 * time.Minute), // Created before snapshot
			Committed:    true,
		}
		assert.False(t, checker.IsVisible(version, t1),
			"Should not see T2's data even though T2 committed")
	})

	// Scenario 3: T1 does NOT see data committed after snapshot
	t.Run("does not see data committed after snapshot", func(t *testing.T) {
		t1.setCommitted(300)
		version := VersionInfo{
			CreatedTxnID: 300,
			DeletedTxnID: 0,
			CreatedTime:  baseTime.Add(1 * time.Second), // After snapshot
			Committed:    true,
		}
		assert.False(t, checker.IsVisible(version, t1),
			"Should not see data committed after snapshot")
	})

	// Scenario 4: Row deleted by T2 (active at snapshot) is still visible
	t.Run("row deleted by active-at-snapshot transaction still visible", func(t *testing.T) {
		version := VersionInfo{
			CreatedTxnID: 10,
			DeletedTxnID: 200, // T2 deleted it
			CreatedTime:  baseTime.Add(-1 * time.Hour),
			DeletedTime:  baseTime.Add(1 * time.Second),
			Committed:    true,
		}
		assert.True(t, checker.IsVisible(version, t1),
			"Row deleted by T2 (active at snapshot) should still be visible")
	})

	// Scenario 5: Own changes are visible
	t.Run("own changes visible", func(t *testing.T) {
		version := VersionInfo{
			CreatedTxnID: 100, // T1's own row
			DeletedTxnID: 0,
			Committed:    false,
		}
		assert.True(t, checker.IsVisible(version, t1))
	})

	// Scenario 6: T1 does NOT see uncommitted data from T3
	t.Run("no dirty read from uncommitted transaction", func(t *testing.T) {
		// T3 (300) started after T1's snapshot, not committed
		version := VersionInfo{
			CreatedTxnID: 400, // T3's row
			DeletedTxnID: 0,
			Committed:    false,
		}
		assert.False(t, checker.IsVisible(version, t1),
			"Should not see uncommitted data from T3")
	})
}

// TestRepeatableReadVisibility_SnapshotTimingScenario tests the precise
// snapshot timing behavior described in the spec.
func TestRepeatableReadVisibility_SnapshotTimingScenario(t *testing.T) {
	checker := NewRepeatableReadVisibility()

	// Timeline:
	// T0: Data D1 created and committed (by T0)
	// T1: T1 starts with REPEATABLE READ (snapshot at T1_start)
	// T2: T2 starts, creates D2, commits
	// T3: T1 queries - should see D1, NOT D2

	t0Time := time.Now().Add(-1 * time.Hour)
	t1StartTime := time.Now()
	t2CommitTime := t1StartTime.Add(500 * time.Millisecond)

	// T1 with REPEATABLE READ
	t1 := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	t1.startTime = t1StartTime
	t1.setSnapshot(NewSnapshot(t1StartTime, []uint64{}))
	t1.setCommitted(10)  // T0
	t1.setCommitted(200) // T2

	// D1: Created by T0, committed before T1's snapshot
	d1 := VersionInfo{
		CreatedTxnID: 10, // T0
		DeletedTxnID: 0,
		CreatedTime:  t0Time,
		Committed:    true,
	}

	// D2: Created by T2, committed AFTER T1's snapshot
	d2 := VersionInfo{
		CreatedTxnID: 200, // T2
		DeletedTxnID: 0,
		CreatedTime:  t2CommitTime,
		Committed:    true,
	}

	// T1 should see D1 (committed before snapshot)
	assert.True(t, checker.IsVisible(d1, t1),
		"T1 should see D1 (committed before snapshot)")

	// T1 should NOT see D2 (committed after snapshot)
	assert.False(t, checker.IsVisible(d2, t1),
		"T1 should NOT see D2 (committed after snapshot)")
}

// TestRepeatableReadVisibility_OwnRowDeletedByOtherCommittedBeforeSnapshot tests
// the edge case where our own row is deleted by a transaction that committed
// before our snapshot (unusual but possible with concurrent transactions).
func TestRepeatableReadVisibility_OwnRowDeletedByOtherCommittedBeforeSnapshot(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	t1 := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	t1.startTime = baseTime
	t1.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by us, but deleted by T2 which committed before our snapshot
	version := VersionInfo{
		CreatedTxnID: 100, // Created by us
		DeletedTxnID: 200, // Deleted by T2
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  baseTime.Add(-30 * time.Minute), // Before snapshot
		Committed:    false,                           // Our create is not committed
	}
	t1.setCommitted(200)

	// T2's committed delete before our snapshot should hide the row
	assert.False(t, checker.IsVisible(version, t1),
		"Our row deleted by committed transaction before snapshot should not be visible")
}

// TestRepeatableReadVisibility_PhantomReadScenario demonstrates that phantom reads
// are prevented (new rows don't appear in subsequent queries within same transaction).
func TestRepeatableReadVisibility_PhantomReadPrevented(t *testing.T) {
	checker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	// T1 starts with REPEATABLE READ
	t1 := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	t1.startTime = baseTime
	t1.setSnapshot(NewSnapshot(baseTime, []uint64{}))
	t1.setCommitted(10)

	// First query: Existing row is visible
	existingRow := VersionInfo{
		CreatedTxnID: 10,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		Committed:    true,
	}
	assert.True(t, checker.IsVisible(existingRow, t1), "Existing row visible")

	// T2 inserts a new row and commits
	t1.setCommitted(200)
	newRow := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(1 * time.Second), // After T1's snapshot
		Committed:    true,
	}

	// Second query: New row should NOT appear (phantom prevented)
	// This is the same statement time to simulate re-executing the same query
	assert.False(t, checker.IsVisible(newRow, t1),
		"New row should NOT appear in subsequent query (phantom prevented)")
}

// =============================================================================
// SERIALIZABLE Visibility Tests
// =============================================================================

// TestNewSerializableVisibility tests the constructor.
func TestNewSerializableVisibility(t *testing.T) {
	checker := NewSerializableVisibility()
	assert.NotNil(t, checker)

	// Verify it implements VisibilityChecker
	var _ VisibilityChecker = checker
}

// TestSerializableVisibility_OwnChangesVisible tests that a transaction
// can see its own uncommitted changes.
func TestSerializableVisibility_OwnChangesVisible(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by own transaction (uncommitted)
	version := VersionInfo{
		CreatedTxnID: 100, // Same as txn.GetTxnID()
		DeletedTxnID: 0,
		Committed:    false,
	}

	assert.True(t, checker.IsVisible(version, txn),
		"Transaction should see its own uncommitted rows in SERIALIZABLE")
}

// TestSerializableVisibility_DirtyReadPrevented tests that SERIALIZABLE
// prevents dirty reads from other uncommitted transactions.
func TestSerializableVisibility_DirtyReadPrevented(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{200})) // T2 was active at snapshot

	// Row created by another uncommitted transaction (T2)
	version := VersionInfo{
		CreatedTxnID: 200, // Different transaction
		DeletedTxnID: 0,
		Committed:    false, // T2 has not committed
	}
	// T2 is not committed (dirty read should be prevented)

	assert.False(t, checker.IsVisible(version, txn),
		"SERIALIZABLE should prevent dirty reads from uncommitted transactions")
}

// TestSerializableVisibility_CommittedBeforeSnapshotVisible tests that
// data committed before the snapshot is visible.
func TestSerializableVisibility_CommittedBeforeSnapshotVisible(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by another committed transaction before snapshot
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(-1 * time.Hour), // Created 1 hour before snapshot
		Committed:    true,
	}
	txn.setCommitted(50)

	assert.True(t, checker.IsVisible(version, txn),
		"SERIALIZABLE should see data committed before snapshot")
}

// TestSerializableVisibility_CommittedAfterSnapshotNotVisible tests that
// data committed after the snapshot is NOT visible.
func TestSerializableVisibility_CommittedAfterSnapshotNotVisible(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by another transaction after snapshot
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(1 * time.Second), // Created after snapshot
		Committed:    true,
	}
	txn.setCommitted(50)

	assert.False(t, checker.IsVisible(version, txn),
		"SERIALIZABLE should NOT see data committed after snapshot")
}

// TestSerializableVisibility_ActiveAtSnapshotNotVisible tests that rows
// from transactions that were active at snapshot time are not visible.
func TestSerializableVisibility_ActiveAtSnapshotNotVisible(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	// T1 creates snapshot when T2 is active
	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{200})) // T2 was active at snapshot

	// Row created by T2 which was active at snapshot time
	// Even if T2 commits later, T1 should NOT see T2's rows
	version := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(-1 * time.Minute), // T2 created before T1's snapshot
		Committed:    true,
	}
	txn.setCommitted(200) // T2 committed after T1's snapshot

	assert.False(t, checker.IsVisible(version, txn),
		"SERIALIZABLE should NOT see rows from transactions active at snapshot time")
}

// TestSerializableVisibility_NonRepeatableReadPrevented tests the key
// SERIALIZABLE behavior: re-reading data returns the same values.
func TestSerializableVisibility_NonRepeatableReadPrevented(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	// Scenario: T1 reads at snapshot time, T2 updates and commits,
	// T1 reads again and should still see original values

	// T1 with SERIALIZABLE, snapshot at baseTime
	t1 := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	t1.startTime = baseTime
	t1.setSnapshot(NewSnapshot(baseTime, []uint64{}))
	t1.setCommitted(10) // Old transaction that created original data

	// First read: T1 sees original data (committed before snapshot)
	originalVersion := VersionInfo{
		CreatedTxnID: 10,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		Committed:    true,
	}

	assert.True(t, checker.IsVisible(originalVersion, t1),
		"First read should see original row")

	// T2 updates the row: deletes old version, creates new version
	// T2 commits AFTER T1's snapshot
	t2CommitTime := baseTime.Add(1 * time.Second)

	// Original row is now deleted by T2
	deletedOriginal := VersionInfo{
		CreatedTxnID: 10,
		DeletedTxnID: 200, // T2 deleted it
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  t2CommitTime,
		Committed:    true,
	}
	t1.setCommitted(200)

	// New version created by T2
	newVersion := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		CreatedTime:  t2CommitTime,
		Committed:    true,
	}

	// In SERIALIZABLE, T1 should still see the original (delete was after snapshot)
	assert.True(t, checker.IsVisible(deletedOriginal, t1),
		"SERIALIZABLE should still see original row (delete after snapshot)")

	// T1 should NOT see the new version (created after snapshot)
	assert.False(t, checker.IsVisible(newVersion, t1),
		"SERIALIZABLE should NOT see new value committed after snapshot")
}

// TestSerializableVisibility_TransactionSnapshotVsStatementSnapshot tests
// that SERIALIZABLE uses transaction-level snapshot, not statement-level.
func TestSerializableVisibility_TransactionSnapshotVsStatementSnapshot(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	// T1 starts at baseTime
	t1 := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	t1.startTime = baseTime
	t1.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// First statement at baseTime
	t1.statementTime = baseTime

	// Row from T2 that commits between statements
	t2Row := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(500 * time.Millisecond), // After transaction start
		Committed:    true,
	}
	t1.setCommitted(200)

	// First statement - T2's row created after snapshot, should not be visible
	assert.False(t, checker.IsVisible(t2Row, t1),
		"First statement should not see T2's row (created after snapshot)")

	// Update statement time to after T2's commit
	// In READ COMMITTED this would make T2's row visible
	// In SERIALIZABLE it should STILL not be visible
	t1.statementTime = baseTime.Add(1 * time.Second)

	// Second statement - STILL should NOT see T2's data
	// This is the key difference from READ COMMITTED!
	assert.False(t, checker.IsVisible(t2Row, t1),
		"Second statement should STILL not see T2's data (SERIALIZABLE uses transaction snapshot)")
}

// TestSerializableVisibility_AbortedTransactionNotVisible tests that rows
// from aborted transactions are not visible.
func TestSerializableVisibility_AbortedTransactionNotVisible(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by an aborted transaction (T2)
	version := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		Committed:    false,
	}
	txn.setAborted(200)

	assert.False(t, checker.IsVisible(version, txn),
		"Rows from aborted transactions should not be visible")
}

// TestSerializableVisibility_NeverCreatedRowNotVisible tests that a row
// with CreatedTxnID of 0 is not visible.
func TestSerializableVisibility_NeverCreatedRowNotVisible(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row that was never created (invalid state)
	version := VersionInfo{
		CreatedTxnID: 0,
		DeletedTxnID: 0,
	}

	assert.False(t, checker.IsVisible(version, txn),
		"Row with no creator should not be visible")
}

// TestSerializableVisibility_OwnDeletedRowNotVisible tests that a row
// deleted by the current transaction is not visible.
func TestSerializableVisibility_OwnDeletedRowNotVisible(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by us and deleted by us
	version := VersionInfo{
		CreatedTxnID: 100, // Created by current transaction
		DeletedTxnID: 100, // Deleted by current transaction
		Committed:    false,
	}

	assert.False(t, checker.IsVisible(version, txn),
		"Row deleted by own transaction should not be visible")
}

// TestSerializableVisibility_DeletedBeforeSnapshotNotVisible tests that
// rows deleted and committed before snapshot are not visible.
func TestSerializableVisibility_DeletedBeforeSnapshotNotVisible(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row deleted by T2, which committed before snapshot
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 60,
		CreatedTime:  baseTime.Add(-2 * time.Hour),
		DeletedTime:  baseTime.Add(-1 * time.Hour), // Deleted before snapshot
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setCommitted(60)

	assert.False(t, checker.IsVisible(version, txn),
		"Row deleted before snapshot should not be visible")
}

// TestSerializableVisibility_DeletedAfterSnapshotStillVisible tests that
// rows deleted after the snapshot are still visible.
func TestSerializableVisibility_DeletedAfterSnapshotStillVisible(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row deleted by T2 after snapshot
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200,
		CreatedTime:  baseTime.Add(-2 * time.Hour),
		DeletedTime:  baseTime.Add(1 * time.Second), // Deleted after snapshot
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setCommitted(200)

	assert.True(t, checker.IsVisible(version, txn),
		"Row deleted after snapshot should still be visible")
}

// TestSerializableVisibility_DeletedByActiveAtSnapshotStillVisible tests that
// rows deleted by transactions active at snapshot time are still visible.
func TestSerializableVisibility_DeletedByActiveAtSnapshotStillVisible(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	// T2 was active at snapshot time
	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{200})) // T2 was active

	// Row deleted by T2 (which was active at snapshot)
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200,
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  baseTime.Add(1 * time.Second),
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setCommitted(200) // T2 commits after snapshot

	// Row should be visible because T2's delete was not committed at snapshot time
	assert.True(t, checker.IsVisible(version, txn),
		"Row deleted by transaction active at snapshot should still be visible")
}

// TestSerializableVisibility_UncommittedDeleteDoesNotHideRow tests that
// a delete by an uncommitted transaction does not hide the row.
func TestSerializableVisibility_UncommittedDeleteDoesNotHideRow(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row created by committed transaction, deleted by uncommitted T2
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200, // T2 deleted it but hasn't committed
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  baseTime.Add(-1 * time.Minute),
		Committed:    true,
	}
	txn.setCommitted(50)
	// T2 (200) is NOT committed

	assert.True(t, checker.IsVisible(version, txn),
		"Row deleted by uncommitted transaction should still be visible")
}

// TestSerializableVisibility_AbortedDeleteDoesNotHideRow tests that
// a delete by an aborted transaction does not hide the row.
func TestSerializableVisibility_AbortedDeleteDoesNotHideRow(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	txn.setSnapshot(NewSnapshot(baseTime, []uint64{}))

	// Row deleted by an aborted transaction
	version := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200,
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		DeletedTime:  baseTime.Add(-30 * time.Minute),
		Committed:    true,
	}
	txn.setCommitted(50)
	txn.setAborted(200) // T2 aborted

	assert.True(t, checker.IsVisible(version, txn),
		"Row deleted by aborted transaction should still be visible")
}

// TestSerializableVisibility_WithoutSnapshot tests fallback to startTime
// when no snapshot is provided.
func TestSerializableVisibility_WithoutSnapshot(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	// No snapshot set - should fall back to startTime
	txn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	txn.startTime = baseTime
	// No snapshot!

	// Row committed before start time - visible
	version1 := VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		Committed:    true,
	}
	txn.setCommitted(50)

	assert.True(t, checker.IsVisible(version1, txn),
		"Row committed before start time should be visible even without snapshot")

	// Row committed after start time - NOT visible
	version2 := VersionInfo{
		CreatedTxnID: 60,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(1 * time.Second),
		Committed:    true,
	}
	txn.setCommitted(60)

	assert.False(t, checker.IsVisible(version2, txn),
		"Row committed after start time should not be visible")
}

// TestSerializableVisibility_PhantomReadPrevented demonstrates that phantom reads
// are prevented (new rows don't appear in subsequent queries within same transaction).
func TestSerializableVisibility_PhantomReadPrevented(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	// T1 starts with SERIALIZABLE
	t1 := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	t1.startTime = baseTime
	t1.setSnapshot(NewSnapshot(baseTime, []uint64{}))
	t1.setCommitted(10)

	// First query: Existing row is visible
	existingRow := VersionInfo{
		CreatedTxnID: 10,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(-1 * time.Hour),
		Committed:    true,
	}
	assert.True(t, checker.IsVisible(existingRow, t1), "Existing row visible")

	// T2 inserts a new row and commits
	t1.setCommitted(200)
	newRow := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		CreatedTime:  baseTime.Add(1 * time.Second), // After T1's snapshot
		Committed:    true,
	}

	// Second query: New row should NOT appear (phantom prevented)
	assert.False(t, checker.IsVisible(newRow, t1),
		"New row should NOT appear in subsequent query (phantom prevented)")
}

// TestSerializableVisibility_MultipleScenarios tests multiple visibility
// scenarios simulating a realistic SERIALIZABLE workflow.
func TestSerializableVisibility_MultipleScenarios(t *testing.T) {
	checker := NewSerializableVisibility()
	baseTime := time.Now()

	// Setup: Transaction T1 (100) with SERIALIZABLE
	// T2 (200) is active when T1 starts
	t1 := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	t1.startTime = baseTime
	t1.setSnapshot(NewSnapshot(baseTime, []uint64{200})) // T2 was active at snapshot
	t1.setCommitted(10)                                  // Old committed transaction

	// Scenario 1: T1 sees data committed before snapshot
	t.Run("sees data committed before snapshot", func(t *testing.T) {
		version := VersionInfo{
			CreatedTxnID: 10,
			DeletedTxnID: 0,
			CreatedTime:  baseTime.Add(-1 * time.Hour),
			Committed:    true,
		}
		assert.True(t, checker.IsVisible(version, t1))
	})

	// Scenario 2: T1 does NOT see data from T2 (was active at snapshot)
	t.Run("does not see data from active-at-snapshot transaction", func(t *testing.T) {
		t1.setCommitted(200) // T2 committed after snapshot
		version := VersionInfo{
			CreatedTxnID: 200,
			DeletedTxnID: 0,
			CreatedTime:  baseTime.Add(-1 * time.Minute), // Created before snapshot
			Committed:    true,
		}
		assert.False(t, checker.IsVisible(version, t1),
			"Should not see T2's data even though T2 committed")
	})

	// Scenario 3: T1 does NOT see data committed after snapshot
	t.Run("does not see data committed after snapshot", func(t *testing.T) {
		t1.setCommitted(300)
		version := VersionInfo{
			CreatedTxnID: 300,
			DeletedTxnID: 0,
			CreatedTime:  baseTime.Add(1 * time.Second), // After snapshot
			Committed:    true,
		}
		assert.False(t, checker.IsVisible(version, t1),
			"Should not see data committed after snapshot")
	})

	// Scenario 4: Row deleted by T2 (active at snapshot) is still visible
	t.Run("row deleted by active-at-snapshot transaction still visible", func(t *testing.T) {
		version := VersionInfo{
			CreatedTxnID: 10,
			DeletedTxnID: 200, // T2 deleted it
			CreatedTime:  baseTime.Add(-1 * time.Hour),
			DeletedTime:  baseTime.Add(1 * time.Second),
			Committed:    true,
		}
		assert.True(t, checker.IsVisible(version, t1),
			"Row deleted by T2 (active at snapshot) should still be visible")
	})

	// Scenario 5: Own changes are visible
	t.Run("own changes visible", func(t *testing.T) {
		version := VersionInfo{
			CreatedTxnID: 100, // T1's own row
			DeletedTxnID: 0,
			Committed:    false,
		}
		assert.True(t, checker.IsVisible(version, t1))
	})

	// Scenario 6: T1 does NOT see uncommitted data from T3
	t.Run("no dirty read from uncommitted transaction", func(t *testing.T) {
		// T3 (400) started after T1's snapshot, not committed
		version := VersionInfo{
			CreatedTxnID: 400, // T3's row
			DeletedTxnID: 0,
			Committed:    false,
		}
		assert.False(t, checker.IsVisible(version, t1),
			"Should not see uncommitted data from T3")
	})
}

// TestSerializableVisibility_IdenticalToRepeatableRead verifies that SERIALIZABLE
// visibility rules are identical to REPEATABLE READ (the difference is in conflict
// detection at commit time, not visibility).
func TestSerializableVisibility_IdenticalToRepeatableRead(t *testing.T) {
	serializableChecker := NewSerializableVisibility()
	repeatableReadChecker := NewRepeatableReadVisibility()
	baseTime := time.Now()

	// Create identical transaction contexts for both isolation levels
	serializableTxn := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	serializableTxn.startTime = baseTime
	serializableTxn.setSnapshot(NewSnapshot(baseTime, []uint64{200}))
	serializableTxn.setCommitted(10)
	serializableTxn.setCommitted(50)
	serializableTxn.setAborted(300)

	repeatableReadTxn := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
	repeatableReadTxn.startTime = baseTime
	repeatableReadTxn.setSnapshot(NewSnapshot(baseTime, []uint64{200}))
	repeatableReadTxn.setCommitted(10)
	repeatableReadTxn.setCommitted(50)
	repeatableReadTxn.setAborted(300)

	// Test cases that should have identical visibility
	testCases := []struct {
		name    string
		version VersionInfo
	}{
		{
			name: "committed before snapshot",
			version: VersionInfo{
				CreatedTxnID: 10,
				DeletedTxnID: 0,
				CreatedTime:  baseTime.Add(-1 * time.Hour),
				Committed:    true,
			},
		},
		{
			name: "committed after snapshot",
			version: VersionInfo{
				CreatedTxnID: 50,
				DeletedTxnID: 0,
				CreatedTime:  baseTime.Add(1 * time.Second),
				Committed:    true,
			},
		},
		{
			name: "active at snapshot",
			version: VersionInfo{
				CreatedTxnID: 200,
				DeletedTxnID: 0,
				CreatedTime:  baseTime.Add(-1 * time.Minute),
				Committed:    true,
			},
		},
		{
			name: "own row",
			version: VersionInfo{
				CreatedTxnID: 100,
				DeletedTxnID: 0,
				Committed:    false,
			},
		},
		{
			name: "aborted transaction",
			version: VersionInfo{
				CreatedTxnID: 300,
				DeletedTxnID: 0,
				Committed:    false,
			},
		},
		{
			name: "deleted before snapshot",
			version: VersionInfo{
				CreatedTxnID: 10,
				DeletedTxnID: 50,
				CreatedTime:  baseTime.Add(-2 * time.Hour),
				DeletedTime:  baseTime.Add(-1 * time.Hour),
				Committed:    true,
			},
		},
		{
			name: "deleted after snapshot",
			version: VersionInfo{
				CreatedTxnID: 10,
				DeletedTxnID: 50,
				CreatedTime:  baseTime.Add(-2 * time.Hour),
				DeletedTime:  baseTime.Add(1 * time.Second),
				Committed:    true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			serializableResult := serializableChecker.IsVisible(tc.version, serializableTxn)
			repeatableReadResult := repeatableReadChecker.IsVisible(tc.version, repeatableReadTxn)

			assert.Equal(
				t,
				repeatableReadResult,
				serializableResult,
				"SERIALIZABLE and REPEATABLE READ should have identical visibility for: %s",
				tc.name,
			)
		})
	}
}

// TestSerializableVisibility_SnapshotTimingScenario tests the precise
// snapshot timing behavior described in the spec.
func TestSerializableVisibility_SnapshotTimingScenario(t *testing.T) {
	checker := NewSerializableVisibility()

	// Timeline:
	// T0: Data D1 created and committed (by T0)
	// T1: T1 starts with SERIALIZABLE (snapshot at T1_start)
	// T2: T2 starts, creates D2, commits
	// T3: T1 queries - should see D1, NOT D2

	t0Time := time.Now().Add(-1 * time.Hour)
	t1StartTime := time.Now()
	t2CommitTime := t1StartTime.Add(500 * time.Millisecond)

	// T1 with SERIALIZABLE
	t1 := newMockTransactionContext(100, parser.IsolationLevelSerializable)
	t1.startTime = t1StartTime
	t1.setSnapshot(NewSnapshot(t1StartTime, []uint64{}))
	t1.setCommitted(10)  // T0
	t1.setCommitted(200) // T2

	// D1: Created by T0, committed before T1's snapshot
	d1 := VersionInfo{
		CreatedTxnID: 10, // T0
		DeletedTxnID: 0,
		CreatedTime:  t0Time,
		Committed:    true,
	}

	// D2: Created by T2, committed AFTER T1's snapshot
	d2 := VersionInfo{
		CreatedTxnID: 200, // T2
		DeletedTxnID: 0,
		CreatedTime:  t2CommitTime,
		Committed:    true,
	}

	// T1 should see D1 (committed before snapshot)
	assert.True(t, checker.IsVisible(d1, t1),
		"T1 should see D1 (committed before snapshot)")

	// T1 should NOT see D2 (committed after snapshot)
	assert.False(t, checker.IsVisible(d2, t1),
		"T1 should NOT see D2 (committed after snapshot)")
}

// TestGetVisibilityChecker_FactoryFunction tests that GetVisibilityChecker
// returns the correct VisibilityChecker implementation for each isolation level.
func TestGetVisibilityChecker_FactoryFunction(t *testing.T) {
	tests := []struct {
		name         string
		level        parser.IsolationLevel
		expectedType string
	}{
		{
			name:         "READ_UNCOMMITTED returns ReadUncommittedVisibility",
			level:        parser.IsolationLevelReadUncommitted,
			expectedType: "*storage.ReadUncommittedVisibility",
		},
		{
			name:         "READ_COMMITTED returns ReadCommittedVisibility",
			level:        parser.IsolationLevelReadCommitted,
			expectedType: "*storage.ReadCommittedVisibility",
		},
		{
			name:         "REPEATABLE_READ returns RepeatableReadVisibility",
			level:        parser.IsolationLevelRepeatableRead,
			expectedType: "*storage.RepeatableReadVisibility",
		},
		{
			name:         "SERIALIZABLE returns SerializableVisibility",
			level:        parser.IsolationLevelSerializable,
			expectedType: "*storage.SerializableVisibility",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := GetVisibilityChecker(tt.level)
			assert.NotNil(t, checker, "GetVisibilityChecker should not return nil")

			// Verify correct type by checking the type name
			actualType := visibilityTypeName(checker)
			assert.Equal(t, tt.expectedType, actualType,
				"GetVisibilityChecker(%v) returned wrong type", tt.level)
		})
	}
}

// TestGetVisibilityChecker_UnknownLevel tests that unknown isolation levels
// default to SERIALIZABLE (the safest option).
func TestGetVisibilityChecker_UnknownLevel(t *testing.T) {
	// Use an invalid isolation level value
	unknownLevel := parser.IsolationLevel(99)
	checker := GetVisibilityChecker(unknownLevel)

	assert.NotNil(t, checker, "GetVisibilityChecker should not return nil for unknown level")

	// Should default to SerializableVisibility
	actualType := visibilityTypeName(checker)
	assert.Equal(t, "*storage.SerializableVisibility", actualType,
		"GetVisibilityChecker should default to SerializableVisibility for unknown levels")
}

// TestGetVisibilityChecker_BehaviorVerification tests that each visibility
// checker returned by the factory behaves correctly for its isolation level.
func TestGetVisibilityChecker_BehaviorVerification(t *testing.T) {
	baseTime := time.Now()

	// Create a version from an uncommitted transaction
	uncommittedVersion := VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		Committed:    false,
	}

	t.Run("READ_UNCOMMITTED allows dirty reads", func(t *testing.T) {
		checker := GetVisibilityChecker(parser.IsolationLevelReadUncommitted)
		txnCtx := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)
		// Read uncommitted should see uncommitted data from other transactions
		assert.True(t, checker.IsVisible(uncommittedVersion, txnCtx),
			"READ UNCOMMITTED should allow dirty reads")
	})

	t.Run("READ_COMMITTED prevents dirty reads", func(t *testing.T) {
		checker := GetVisibilityChecker(parser.IsolationLevelReadCommitted)
		txnCtx := newMockTransactionContext(100, parser.IsolationLevelReadCommitted)
		txnCtx.statementTime = baseTime
		// Read committed should not see uncommitted data
		assert.False(t, checker.IsVisible(uncommittedVersion, txnCtx),
			"READ COMMITTED should prevent dirty reads")
	})

	t.Run("REPEATABLE_READ prevents dirty reads", func(t *testing.T) {
		checker := GetVisibilityChecker(parser.IsolationLevelRepeatableRead)
		txnCtx := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)
		txnCtx.startTime = baseTime
		txnCtx.setSnapshot(NewSnapshot(baseTime, []uint64{}))
		// Repeatable read should not see uncommitted data
		assert.False(t, checker.IsVisible(uncommittedVersion, txnCtx),
			"REPEATABLE READ should prevent dirty reads")
	})

	t.Run("SERIALIZABLE prevents dirty reads", func(t *testing.T) {
		checker := GetVisibilityChecker(parser.IsolationLevelSerializable)
		txnCtx := newMockTransactionContext(100, parser.IsolationLevelSerializable)
		txnCtx.startTime = baseTime
		txnCtx.setSnapshot(NewSnapshot(baseTime, []uint64{}))
		// Serializable should not see uncommitted data
		assert.False(t, checker.IsVisible(uncommittedVersion, txnCtx),
			"SERIALIZABLE should prevent dirty reads")
	})
}

// visibilityTypeName returns the type name of the given visibility checker.
// Used for verifying the correct visibility checker type is returned.
func visibilityTypeName(v VisibilityChecker) string {
	switch v.(type) {
	case *ReadUncommittedVisibility:
		return "*storage.ReadUncommittedVisibility"
	case *ReadCommittedVisibility:
		return "*storage.ReadCommittedVisibility"
	case *RepeatableReadVisibility:
		return "*storage.RepeatableReadVisibility"
	case *SerializableVisibility:
		return "*storage.SerializableVisibility"
	default:
		return "unknown"
	}
}
