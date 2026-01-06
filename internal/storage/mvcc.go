// Package storage provides columnar storage for the native Go DuckDB implementation.
package storage

import (
	"time"

	"github.com/dukdb/dukdb-go/internal/parser"
)

// VersionInfo contains MVCC version metadata for a row.
// This information is used by VisibilityChecker implementations to determine
// whether a row version is visible to a particular transaction.
type VersionInfo struct {
	// CreatedTxnID is the ID of the transaction that created this row version.
	CreatedTxnID uint64

	// DeletedTxnID is the ID of the transaction that deleted this row version.
	// A value of 0 indicates the row has not been deleted.
	DeletedTxnID uint64

	// CreatedTime is the timestamp when this row version was created.
	// Used for time-based visibility checks in some isolation levels.
	CreatedTime time.Time

	// DeletedTime is the timestamp when this row version was deleted.
	// Zero value indicates the row has not been deleted.
	DeletedTime time.Time

	// Committed indicates whether the creating transaction has committed.
	// This is essential for determining visibility in most isolation levels.
	Committed bool
}

// IsDeleted returns true if this row version has been marked as deleted.
func (v VersionInfo) IsDeleted() bool {
	return v.DeletedTxnID != 0
}

// IsActive returns true if this row version was created but not yet deleted.
func (v VersionInfo) IsActive() bool {
	return v.CreatedTxnID != 0 && v.DeletedTxnID == 0
}

// TransactionContext provides the transaction state needed for visibility checks.
// Implementations of this interface represent the current transaction's view
// of the database state.
type TransactionContext interface {
	// GetTxnID returns the unique identifier for this transaction.
	GetTxnID() uint64

	// GetIsolationLevel returns the isolation level for this transaction.
	GetIsolationLevel() parser.IsolationLevel

	// GetStartTime returns the timestamp when this transaction started.
	// Used for snapshot-based visibility in REPEATABLE READ.
	GetStartTime() time.Time

	// GetStatementTime returns the timestamp when the current statement started.
	// Used for statement-level visibility in READ COMMITTED.
	GetStatementTime() time.Time

	// IsCommitted checks if another transaction has committed.
	// This is used to determine visibility of rows created by other transactions.
	IsCommitted(txnID uint64) bool

	// IsAborted checks if another transaction has been aborted.
	// This is used to exclude rows from aborted transactions in READ UNCOMMITTED.
	// Returns true if the transaction has been aborted, false otherwise.
	IsAborted(txnID uint64) bool

	// GetSnapshot returns the snapshot taken at transaction start.
	// This is only non-nil for REPEATABLE READ and SERIALIZABLE isolation levels.
	// The snapshot contains the timestamp and active transaction IDs at that time.
	GetSnapshot() *Snapshot

	// WasActiveAtSnapshot returns true if the given transaction ID was active
	// (uncommitted) when this transaction's snapshot was taken.
	// This is used for REPEATABLE READ and SERIALIZABLE visibility checks.
	// Returns false if this transaction has no snapshot.
	WasActiveAtSnapshot(txnID uint64) bool
}

// VisibilityChecker determines whether a row version is visible to a transaction.
// Different isolation levels implement different visibility rules:
//   - READ UNCOMMITTED: Can see uncommitted changes from other transactions
//   - READ COMMITTED: Can only see committed changes, snapshot per statement
//   - REPEATABLE READ: Can only see committed changes, snapshot at transaction start
//   - SERIALIZABLE: Like REPEATABLE READ but with conflict detection
type VisibilityChecker interface {
	// IsVisible determines if the given row version is visible to the transaction.
	// Returns true if the row should be visible, false otherwise.
	//
	// The visibility rules depend on the isolation level:
	//   - The row must have been created before/by the visible snapshot
	//   - The row must not have been deleted in the visible snapshot
	//   - For READ UNCOMMITTED, uncommitted rows are visible
	//   - For READ COMMITTED, only rows committed before statement start are visible
	//   - For REPEATABLE READ, only rows committed before transaction start are visible
	//   - For SERIALIZABLE, same as REPEATABLE READ with conflict tracking
	IsVisible(version VersionInfo, transaction TransactionContext) bool
}

// ReadUncommittedVisibility implements VisibilityChecker for READ UNCOMMITTED isolation.
// This is the weakest isolation level, allowing dirty reads from uncommitted transactions.
//
// Visibility rules for READ UNCOMMITTED:
//   - All non-deleted rows are visible (even uncommitted ones)
//   - Own transaction's changes are visible
//   - Rows created by other uncommitted transactions are visible (dirty reads allowed)
//   - Rows marked as deleted by ANY transaction (including uncommitted) are NOT visible
//   - Aborted transactions' rows should not be visible
type ReadUncommittedVisibility struct{}

// NewReadUncommittedVisibility creates a new ReadUncommittedVisibility checker.
func NewReadUncommittedVisibility() *ReadUncommittedVisibility {
	return &ReadUncommittedVisibility{}
}

// IsVisible determines if a row version is visible under READ UNCOMMITTED isolation.
//
// A row is visible if:
//  1. The row was created by our own transaction, OR
//  2. The row was created by a committed transaction, OR
//  3. The row was created by another active (uncommitted, non-aborted) transaction (dirty read)
//
// AND:
//  1. The row has not been deleted (DeletedTxnID == 0)
//
// Note: READ UNCOMMITTED allows dirty reads from other active transactions,
// but rows from aborted transactions are not visible.
func (*ReadUncommittedVisibility) IsVisible(version VersionInfo, txn TransactionContext) bool {
	myTxnID := txn.GetTxnID()

	// Check if the row has been created at all
	if version.CreatedTxnID == 0 {
		return false
	}

	// Check if the creating transaction has been aborted
	// Aborted transactions' rows should never be visible
	if txn.IsAborted(version.CreatedTxnID) {
		return false
	}

	// Check deletion status
	// In READ UNCOMMITTED, deleted rows are not visible regardless of who deleted them
	// or whether the delete is committed
	if version.IsDeleted() {
		return false
	}

	// At this point:
	// - Row was created by a non-aborted transaction
	// - Row has not been deleted
	// For READ UNCOMMITTED, this row is visible (dirty reads allowed)
	//
	// Cases covered:
	// 1. Own transaction's row: visible
	// 2. Committed transaction's row: visible
	// 3. Other active uncommitted transaction's row: visible (dirty read)
	_ = myTxnID // Acknowledge that we could use this for additional checks if needed

	return true
}

// ReadCommittedVisibility implements VisibilityChecker for READ COMMITTED isolation.
// READ COMMITTED prevents dirty reads by only showing rows that have been committed.
// It uses statement-level snapshots, meaning each statement sees a consistent view
// of data as of the statement start time.
//
// Visibility rules for READ COMMITTED:
//   - Own transaction's changes are always visible (even uncommitted)
//   - Only rows created by committed transactions are visible (no dirty reads)
//   - Rows must be committed before the current statement's start time
//   - Rows deleted and committed before statement start are NOT visible
//   - Non-repeatable reads are allowed (re-reading may see different values)
//   - Phantom reads are allowed (new rows may appear in subsequent queries)
type ReadCommittedVisibility struct{}

// NewReadCommittedVisibility creates a new ReadCommittedVisibility checker.
func NewReadCommittedVisibility() *ReadCommittedVisibility {
	return &ReadCommittedVisibility{}
}

// IsVisible determines if a row version is visible under READ COMMITTED isolation.
//
// A row is visible if:
//  1. The row was created by our own transaction (own changes visible), OR
//  2. The row was created by a transaction that:
//     a. Has committed (no dirty reads)
//     b. Committed before the current statement started (statement-level snapshot)
//
// AND:
//  1. The row has not been deleted, OR
//  2. The row was deleted by:
//     a. A different transaction that has NOT committed, OR
//     b. A transaction that committed AFTER the current statement started
//
// Note: READ COMMITTED uses statement-level snapshots, not transaction-level.
// Each new statement in the transaction gets a fresh view of committed data.
func (*ReadCommittedVisibility) IsVisible(version VersionInfo, txn TransactionContext) bool {
	myTxnID := txn.GetTxnID()
	statementTime := txn.GetStatementTime()

	// Check if the row has been created at all
	if version.CreatedTxnID == 0 {
		return false
	}

	// Check if the creating transaction has been aborted
	// Aborted transactions' rows should never be visible
	if txn.IsAborted(version.CreatedTxnID) {
		return false
	}

	// Case 1: Own transaction's row - always visible (if not deleted by self)
	if version.CreatedTxnID == myTxnID {
		// Check if we deleted it ourselves
		if version.IsDeleted() && version.DeletedTxnID == myTxnID {
			return false
		}
		// Own row, not deleted by self - visible
		// Note: If deleted by another transaction, we still see our version
		if version.IsDeleted() && version.DeletedTxnID != myTxnID {
			// Another transaction marked it deleted
			// If that transaction committed before our statement, it's not visible
			if txn.IsCommitted(version.DeletedTxnID) && !version.DeletedTime.IsZero() &&
				!version.DeletedTime.After(statementTime) {
				return false
			}
		}
		return true
	}

	// Case 2: Row created by another transaction
	// Must be committed to be visible (no dirty reads)
	if !txn.IsCommitted(version.CreatedTxnID) {
		return false
	}

	// Row must have been committed before or at the statement start time
	// If CreatedTime is zero, we rely solely on commit status
	if !version.CreatedTime.IsZero() && version.CreatedTime.After(statementTime) {
		return false
	}

	// Check deletion status for rows from other committed transactions
	if version.IsDeleted() {
		// If deleted by our own transaction, not visible
		if version.DeletedTxnID == myTxnID {
			return false
		}

		// If deleted by an aborted transaction, the delete doesn't count
		// Row is still visible
		if txn.IsAborted(version.DeletedTxnID) {
			return true
		}

		// If deleted by another committed transaction
		if txn.IsCommitted(version.DeletedTxnID) {
			// Check if delete was committed before statement start
			if !version.DeletedTime.IsZero() && !version.DeletedTime.After(statementTime) {
				// Delete was committed before/at statement start - row not visible
				return false
			}
			// Delete committed after statement start - row still visible in this statement
			return true
		}

		// Deleted by uncommitted transaction (not our own)
		// In READ COMMITTED, we don't see uncommitted deletes from other transactions
		// So the row IS visible
		return true
	}

	// Row is not deleted, created by committed transaction before statement start
	return true
}

// RepeatableReadVisibility implements VisibilityChecker for REPEATABLE READ isolation.
// REPEATABLE READ prevents both dirty reads and non-repeatable reads by using a
// transaction-level snapshot. All reads within the transaction see a consistent
// view of data as of the transaction start time.
//
// Key difference from READ COMMITTED:
//   - READ COMMITTED uses statement-level snapshots (each statement gets fresh view)
//   - REPEATABLE READ uses transaction-level snapshot (entire transaction sees same view)
//
// Visibility rules for REPEATABLE READ:
//   - Own transaction's changes are always visible (even uncommitted)
//   - Only rows created by committed transactions are visible (no dirty reads)
//   - Rows must be committed BEFORE the transaction's snapshot time
//   - Rows created by transactions that were active at snapshot time are NOT visible
//   - Rows deleted and committed before snapshot are NOT visible
//   - Non-repeatable reads are prevented (re-reading always sees same values)
//   - Phantom reads may still occur (depends on implementation of range queries)
type RepeatableReadVisibility struct{}

// NewRepeatableReadVisibility creates a new RepeatableReadVisibility checker.
func NewRepeatableReadVisibility() *RepeatableReadVisibility {
	return &RepeatableReadVisibility{}
}

// IsVisible determines if a row version is visible under REPEATABLE READ isolation.
//
// A row is visible if:
//  1. The row was created by our own transaction (own changes visible), OR
//  2. The row was created by a transaction that:
//     a. Has committed (no dirty reads)
//     b. Committed BEFORE the transaction's snapshot (transaction-level snapshot)
//     c. Was NOT active at snapshot time (committed before we started)
//
// AND:
//  1. The row has not been deleted, OR
//  2. The row was deleted by:
//     a. A different transaction that has NOT committed, OR
//     b. A transaction that was still active at our snapshot time, OR
//     c. A transaction that committed AFTER our snapshot time
//
// Note: REPEATABLE READ uses transaction-level snapshots, not statement-level.
// The snapshot is taken at transaction start, ensuring consistent reads throughout.
func (*RepeatableReadVisibility) IsVisible(version VersionInfo, txn TransactionContext) bool {
	myTxnID := txn.GetTxnID()
	snapshot := txn.GetSnapshot()

	// Get snapshot timestamp - if no snapshot, fall back to transaction start time
	var snapshotTime time.Time
	if snapshot != nil {
		snapshotTime = snapshot.GetTimestamp()
	} else {
		snapshotTime = txn.GetStartTime()
	}

	// Check if the row has been created at all
	if version.CreatedTxnID == 0 {
		return false
	}

	// Check if the creating transaction has been aborted
	// Aborted transactions' rows should never be visible
	if txn.IsAborted(version.CreatedTxnID) {
		return false
	}

	// Case 1: Own transaction's row - always visible (if not deleted by self)
	if version.CreatedTxnID == myTxnID {
		// Check if we deleted it ourselves
		if version.IsDeleted() && version.DeletedTxnID == myTxnID {
			return false
		}
		// Own row, not deleted by self - visible
		// Note: If deleted by another transaction, we still see our version
		// unless that delete was committed before our snapshot
		if version.IsDeleted() && version.DeletedTxnID != myTxnID {
			// Another transaction marked it deleted
			// Check if that transaction committed before our snapshot
			if txn.IsCommitted(version.DeletedTxnID) {
				// Was it active at snapshot time? If so, delete doesn't count yet
				if txn.WasActiveAtSnapshot(version.DeletedTxnID) {
					return true
				}
				// Check if delete was committed before snapshot
				if !version.DeletedTime.IsZero() && !version.DeletedTime.After(snapshotTime) {
					return false
				}
			}
		}
		return true
	}

	// Case 2: Row created by another transaction
	// For REPEATABLE READ, check if the creating transaction was active at snapshot time
	// If it was active, its rows are NOT visible (uncommitted at snapshot time)
	if txn.WasActiveAtSnapshot(version.CreatedTxnID) {
		return false
	}

	// Must be committed to be visible (no dirty reads)
	if !txn.IsCommitted(version.CreatedTxnID) {
		return false
	}

	// Row must have been committed before or at the snapshot time
	// This is the key difference from READ COMMITTED:
	// - READ COMMITTED: checks against statement start time (can change between statements)
	// - REPEATABLE READ: checks against transaction snapshot time (fixed at BEGIN)
	if !version.CreatedTime.IsZero() && version.CreatedTime.After(snapshotTime) {
		return false
	}

	// Check deletion status for rows from other committed transactions
	if version.IsDeleted() {
		// If deleted by our own transaction, not visible
		if version.DeletedTxnID == myTxnID {
			return false
		}

		// If deleted by an aborted transaction, the delete doesn't count
		// Row is still visible
		if txn.IsAborted(version.DeletedTxnID) {
			return true
		}

		// If the deleting transaction was active at our snapshot time,
		// the delete is not visible yet - row IS visible
		if txn.WasActiveAtSnapshot(version.DeletedTxnID) {
			return true
		}

		// If deleted by another committed transaction
		if txn.IsCommitted(version.DeletedTxnID) {
			// Check if delete was committed before snapshot time
			if !version.DeletedTime.IsZero() && !version.DeletedTime.After(snapshotTime) {
				// Delete was committed before/at snapshot time - row not visible
				return false
			}
			// Delete committed after snapshot time - row still visible in our snapshot
			return true
		}

		// Deleted by uncommitted transaction (not our own, not active at snapshot)
		// This means transaction started after our snapshot and hasn't committed
		// In REPEATABLE READ, we don't see uncommitted deletes
		// So the row IS visible
		return true
	}

	// Row is not deleted, created by committed transaction before snapshot
	return true
}

// GetVisibilityChecker returns the appropriate VisibilityChecker for the given isolation level.
// This factory function centralizes the creation of visibility checkers and ensures
// the correct implementation is used for each isolation level.
//
// Parameters:
//   - level: The isolation level from parser.IsolationLevel
//
// Returns:
//   - VisibilityChecker: The appropriate visibility checker for the isolation level
//
// Supported isolation levels:
//   - IsolationLevelReadUncommitted: Returns ReadUncommittedVisibility (dirty reads allowed)
//   - IsolationLevelReadCommitted: Returns ReadCommittedVisibility (statement-level snapshots)
//   - IsolationLevelRepeatableRead: Returns RepeatableReadVisibility (transaction-level snapshot)
//   - IsolationLevelSerializable: Returns SerializableVisibility (same as REPEATABLE READ with conflict detection)
func GetVisibilityChecker(level parser.IsolationLevel) VisibilityChecker {
	switch level {
	case parser.IsolationLevelReadUncommitted:
		return NewReadUncommittedVisibility()
	case parser.IsolationLevelReadCommitted:
		return NewReadCommittedVisibility()
	case parser.IsolationLevelRepeatableRead:
		return NewRepeatableReadVisibility()
	case parser.IsolationLevelSerializable:
		return NewSerializableVisibility()
	default:
		// Default to SERIALIZABLE for unknown levels (safest option)
		return NewSerializableVisibility()
	}
}

// SerializableVisibility implements VisibilityChecker for SERIALIZABLE isolation.
// SERIALIZABLE provides the strictest isolation level, preventing dirty reads,
// non-repeatable reads, and phantom reads. It uses the same snapshot-based
// visibility as REPEATABLE READ, but additionally performs conflict detection
// at commit time to ensure serializable execution.
//
// Key characteristics:
//   - Uses transaction-level snapshot (same as REPEATABLE READ)
//   - Visibility rules are identical to REPEATABLE READ
//   - Conflict detection is performed at commit time (handled by ConflictDetector)
//   - If conflicts are detected, the transaction is aborted
//
// Visibility rules for SERIALIZABLE (same as REPEATABLE READ):
//   - Own transaction's changes are always visible (even uncommitted)
//   - Only rows created by committed transactions are visible (no dirty reads)
//   - Rows must be committed BEFORE the transaction's snapshot time
//   - Rows created by transactions that were active at snapshot time are NOT visible
//   - Rows deleted and committed before snapshot are NOT visible
//   - Non-repeatable reads are prevented (re-reading always sees same values)
//   - Phantom reads are prevented (new rows don't appear in subsequent queries)
//
// Note: The key difference from REPEATABLE READ is not in visibility rules but
// in the conflict detection that happens at commit time. This ensures that
// concurrent transactions appear to execute in some serial order.
type SerializableVisibility struct{}

// NewSerializableVisibility creates a new SerializableVisibility checker.
func NewSerializableVisibility() *SerializableVisibility {
	return &SerializableVisibility{}
}

// IsVisible determines if a row version is visible under SERIALIZABLE isolation.
// The visibility rules are identical to REPEATABLE READ - the difference is in
// conflict detection at commit time.
//
// A row is visible if:
//  1. The row was created by our own transaction (own changes visible), OR
//  2. The row was created by a transaction that:
//     a. Has committed (no dirty reads)
//     b. Committed BEFORE the transaction's snapshot (transaction-level snapshot)
//     c. Was NOT active at snapshot time (committed before we started)
//
// AND:
//  1. The row has not been deleted, OR
//  2. The row was deleted by:
//     a. A different transaction that has NOT committed, OR
//     b. A transaction that was still active at our snapshot time, OR
//     c. A transaction that committed AFTER our snapshot time
//
// Note: SERIALIZABLE uses transaction-level snapshots, identical to REPEATABLE READ.
// The conflict detection that ensures serializability is handled externally by
// the ConflictDetector during commit.
func (*SerializableVisibility) IsVisible(version VersionInfo, txn TransactionContext) bool {
	myTxnID := txn.GetTxnID()
	snapshot := txn.GetSnapshot()

	// Get snapshot timestamp - if no snapshot, fall back to transaction start time
	var snapshotTime time.Time
	if snapshot != nil {
		snapshotTime = snapshot.GetTimestamp()
	} else {
		snapshotTime = txn.GetStartTime()
	}

	// Check if the row has been created at all
	if version.CreatedTxnID == 0 {
		return false
	}

	// Check if the creating transaction has been aborted
	// Aborted transactions' rows should never be visible
	if txn.IsAborted(version.CreatedTxnID) {
		return false
	}

	// Case 1: Own transaction's row - always visible (if not deleted by self)
	if version.CreatedTxnID == myTxnID {
		// Check if we deleted it ourselves
		if version.IsDeleted() && version.DeletedTxnID == myTxnID {
			return false
		}
		// Own row, not deleted by self - visible
		// Note: If deleted by another transaction, we still see our version
		// unless that delete was committed before our snapshot
		if version.IsDeleted() && version.DeletedTxnID != myTxnID {
			// Another transaction marked it deleted
			// Check if that transaction committed before our snapshot
			if txn.IsCommitted(version.DeletedTxnID) {
				// Was it active at snapshot time? If so, delete doesn't count yet
				if txn.WasActiveAtSnapshot(version.DeletedTxnID) {
					return true
				}
				// Check if delete was committed before snapshot
				if !version.DeletedTime.IsZero() && !version.DeletedTime.After(snapshotTime) {
					return false
				}
			}
		}
		return true
	}

	// Case 2: Row created by another transaction
	// For SERIALIZABLE, check if the creating transaction was active at snapshot time
	// If it was active, its rows are NOT visible (uncommitted at snapshot time)
	if txn.WasActiveAtSnapshot(version.CreatedTxnID) {
		return false
	}

	// Must be committed to be visible (no dirty reads)
	if !txn.IsCommitted(version.CreatedTxnID) {
		return false
	}

	// Row must have been committed before or at the snapshot time
	// This is the same check as REPEATABLE READ:
	// - Checks against transaction snapshot time (fixed at BEGIN)
	if !version.CreatedTime.IsZero() && version.CreatedTime.After(snapshotTime) {
		return false
	}

	// Check deletion status for rows from other committed transactions
	if version.IsDeleted() {
		// If deleted by our own transaction, not visible
		if version.DeletedTxnID == myTxnID {
			return false
		}

		// If deleted by an aborted transaction, the delete doesn't count
		// Row is still visible
		if txn.IsAborted(version.DeletedTxnID) {
			return true
		}

		// If the deleting transaction was active at our snapshot time,
		// the delete is not visible yet - row IS visible
		if txn.WasActiveAtSnapshot(version.DeletedTxnID) {
			return true
		}

		// If deleted by another committed transaction
		if txn.IsCommitted(version.DeletedTxnID) {
			// Check if delete was committed before snapshot time
			if !version.DeletedTime.IsZero() && !version.DeletedTime.After(snapshotTime) {
				// Delete was committed before/at snapshot time - row not visible
				return false
			}
			// Delete committed after snapshot time - row still visible in our snapshot
			return true
		}

		// Deleted by uncommitted transaction (not our own, not active at snapshot)
		// This means transaction started after our snapshot and hasn't committed
		// In SERIALIZABLE, we don't see uncommitted deletes
		// So the row IS visible
		return true
	}

	// Row is not deleted, created by committed transaction before snapshot
	return true
}
