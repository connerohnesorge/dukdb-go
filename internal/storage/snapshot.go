// Package storage provides columnar storage for the native Go DuckDB implementation.
package storage

import (
	"time"
)

// Snapshot represents a point-in-time view of the database state.
// It captures the necessary information to determine row visibility
// for REPEATABLE READ and SERIALIZABLE isolation levels.
//
// A snapshot is taken at transaction start for REPEATABLE READ/SERIALIZABLE,
// and all reads within the transaction see a consistent view as of that snapshot.
// This prevents non-repeatable reads (seeing different values on re-read)
// and phantom reads (seeing new rows appear in subsequent queries).
//
// The snapshot contains:
//   - Timestamp: When the snapshot was taken
//   - ActiveTxnIDs: Which transactions were active (uncommitted) at snapshot time
//
// Visibility rules using snapshot:
//   - Rows committed before snapshot timestamp are visible (if not deleted)
//   - Rows created by transactions in ActiveTxnIDs are NOT visible (uncommitted at snapshot time)
//   - Rows deleted by transactions in ActiveTxnIDs ARE visible (delete not yet committed)
type Snapshot struct {
	// Timestamp is when this snapshot was taken.
	// Used to determine if a row was committed before or after the snapshot.
	Timestamp time.Time

	// ActiveTxnIDs is the list of transaction IDs that were active
	// (not yet committed) when this snapshot was taken.
	// Rows created by these transactions are not visible in this snapshot.
	ActiveTxnIDs []uint64

	// activeTxnSet is a set representation of ActiveTxnIDs for O(1) lookups.
	// This is populated lazily on first call to WasActiveAtSnapshot.
	activeTxnSet map[uint64]struct{}
}

// NewSnapshot creates a new snapshot with the given timestamp and active transaction IDs.
// The activeTxnIDs slice is copied to prevent external modification.
func NewSnapshot(timestamp time.Time, activeTxnIDs []uint64) *Snapshot {
	// Copy the slice to prevent external modification
	ids := make([]uint64, len(activeTxnIDs))
	copy(ids, activeTxnIDs)

	return &Snapshot{
		Timestamp:    timestamp,
		ActiveTxnIDs: ids,
	}
}

// WasActiveAtSnapshot returns true if the given transaction ID was active
// (not yet committed) when this snapshot was taken.
//
// This is used for visibility checks:
//   - If a row was created by a transaction that was active at snapshot time,
//     that row is NOT visible (the create had not committed yet).
//   - If a row was deleted by a transaction that was active at snapshot time,
//     that row IS still visible (the delete had not committed yet).
func (s *Snapshot) WasActiveAtSnapshot(txnID uint64) bool {
	if s == nil {
		return false
	}

	// Lazily build the set for O(1) lookups
	if s.activeTxnSet == nil {
		s.activeTxnSet = make(map[uint64]struct{}, len(s.ActiveTxnIDs))
		for _, id := range s.ActiveTxnIDs {
			s.activeTxnSet[id] = struct{}{}
		}
	}

	_, exists := s.activeTxnSet[txnID]
	return exists
}

// GetTimestamp returns the timestamp when this snapshot was taken.
// Returns zero time if the snapshot is nil.
func (s *Snapshot) GetTimestamp() time.Time {
	if s == nil {
		return time.Time{}
	}
	return s.Timestamp
}

// GetActiveTransactionCount returns the number of transactions that were
// active when this snapshot was taken.
func (s *Snapshot) GetActiveTransactionCount() int {
	if s == nil {
		return 0
	}
	return len(s.ActiveTxnIDs)
}

// IsAfterSnapshot returns true if the given time is after the snapshot timestamp.
// This is used to check if a commit/create happened after the snapshot was taken.
func (s *Snapshot) IsAfterSnapshot(t time.Time) bool {
	if s == nil {
		return false
	}
	return t.After(s.Timestamp)
}

// IsBeforeOrAtSnapshot returns true if the given time is before or at the snapshot timestamp.
// This is used to check if a commit/create happened before or at the snapshot time.
func (s *Snapshot) IsBeforeOrAtSnapshot(t time.Time) bool {
	if s == nil {
		return true
	}
	return !t.After(s.Timestamp)
}
