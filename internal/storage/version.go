// Package storage provides columnar storage for the native Go DuckDB implementation.
package storage

import (
	"sync"
)

// VersionedRow represents a single version of a row in the MVCC system.
// Each modification to a row creates a new VersionedRow, linked to the previous
// version through the PrevPtr pointer, forming a version chain.
//
// The version chain allows transactions at different isolation levels to see
// different versions of the same logical row based on their visibility rules.
//
// Lifecycle:
//   - Created: A new VersionedRow is created with TxnID set to the creating transaction.
//     CommitTS is 0 until the transaction commits.
//   - Committed: When the creating transaction commits, CommitTS is set to the commit timestamp.
//   - Deleted: When a transaction deletes the row, DeletedBy is set to that transaction's ID.
//     DeleteTS is set when the deleting transaction commits.
type VersionedRow struct {
	// Data contains the column values for this row version.
	// The slice order corresponds to the table's column order.
	Data []any

	// RowID is the stable identifier for this logical row across all versions.
	// All versions in a chain share the same RowID.
	RowID uint64

	// TxnID is the ID of the transaction that created this version.
	// This is set when the version is created and never changes.
	TxnID uint64

	// CommitTS is the commit timestamp of the creating transaction.
	// A value of 0 indicates the creating transaction has not yet committed.
	// Once set to a non-zero value, this version is considered committed.
	CommitTS uint64

	// DeletedBy is the ID of the transaction that deleted this row version.
	// A value of 0 indicates the row has not been deleted.
	DeletedBy uint64

	// DeleteTS is the commit timestamp of the deleting transaction.
	// A value of 0 indicates either:
	//   - The row has not been deleted (DeletedBy == 0), or
	//   - The delete has not been committed yet (DeletedBy != 0 but transaction uncommitted)
	DeleteTS uint64

	// PrevPtr points to the previous version of this row.
	// A nil value indicates this is the oldest version in the chain.
	// Version chains are traversed from newest (Head) to oldest (PrevPtr == nil).
	PrevPtr *VersionedRow
}

// IsCommitted returns true if the creating transaction has committed.
// A committed version has a non-zero CommitTS.
//
// Note: This only indicates that the version was created by a committed transaction.
// Use IsDeleted to check if the row has been subsequently deleted.
func (v *VersionedRow) IsCommitted() bool {
	return v.CommitTS != 0
}

// IsDeleted returns true if this row version has been deleted.
// A row is considered deleted if DeletedBy is non-zero AND the delete
// has been committed (DeleteTS != 0).
//
// If DeletedBy is non-zero but DeleteTS is 0, the delete is pending
// (the deleting transaction has not yet committed).
//
// Visibility semantics for pending deletes depend on the isolation level:
//   - READ UNCOMMITTED: May see the delete immediately
//   - READ COMMITTED: Does not see the delete until it commits
//   - REPEATABLE READ/SERIALIZABLE: Sees the delete only if committed before snapshot
func (v *VersionedRow) IsDeleted() bool {
	// A row is considered deleted if:
	// 1. Someone has marked it for deletion (DeletedBy != 0)
	// 2. That deletion has been committed (DeleteTS != 0)
	return v.DeletedBy != 0 && v.DeleteTS != 0
}

// IsPendingDelete returns true if this row has been marked for deletion
// but the deleting transaction has not yet committed.
// This is useful for handling uncommitted deletes in different isolation levels.
func (v *VersionedRow) IsPendingDelete() bool {
	return v.DeletedBy != 0 && v.DeleteTS == 0
}

// Clone creates a deep copy of the VersionedRow.
// The Data slice is copied to prevent shared state between the original
// and the clone. However, note that if Data contains reference types
// (slices, maps, pointers), those inner references are not deep-copied.
//
// The PrevPtr is NOT copied - the clone will have a nil PrevPtr.
// This is intentional as cloning is typically used to create a new version
// that will be linked into a different position in a chain.
//
// Returns nil if called on a nil receiver.
func (v *VersionedRow) Clone() *VersionedRow {
	if v == nil {
		return nil
	}

	// Deep copy the Data slice
	var dataCopy []any
	if v.Data != nil {
		dataCopy = make([]any, len(v.Data))
		copy(dataCopy, v.Data)
	}

	return &VersionedRow{
		Data:      dataCopy,
		RowID:     v.RowID,
		TxnID:     v.TxnID,
		CommitTS:  v.CommitTS,
		DeletedBy: v.DeletedBy,
		DeleteTS:  v.DeleteTS,
		PrevPtr:   nil, // Clone does not copy PrevPtr
	}
}

// ToVersionInfo converts the VersionedRow metadata to a VersionInfo struct.
// This is useful for visibility checks that use the VersionInfo interface.
//
// Note: The CreatedTime and DeletedTime fields in VersionInfo will be zero
// since VersionedRow uses timestamps (uint64) instead of time.Time.
// The Committed field is derived from CommitTS.
func (v *VersionedRow) ToVersionInfo() VersionInfo {
	return VersionInfo{
		CreatedTxnID: v.TxnID,
		DeletedTxnID: v.DeletedBy,
		Committed:    v.IsCommitted(),
		// CreatedTime and DeletedTime are left as zero values
		// since VersionedRow uses uint64 timestamps instead of time.Time
	}
}

// VersionChain manages a linked list of VersionedRow instances for a single logical row.
// The chain is ordered from newest (Head) to oldest, with each version's PrevPtr
// pointing to the previous version.
//
// Version chains enable MVCC by allowing different transactions to see different
// versions of the same row based on their visibility rules. When a row is updated,
// a new version is added at the head of the chain rather than modifying in place.
//
// Thread Safety:
// All public methods on VersionChain are thread-safe. The internal mutex protects
// concurrent access to the chain. For complex operations requiring multiple method
// calls to be atomic, use the Lock/Unlock methods to hold the lock externally.
//
// Memory Management:
// Old versions that are no longer visible to any active transaction can be
// garbage collected. This is typically handled by a separate vacuum process
// that identifies and removes obsolete versions.
type VersionChain struct {
	// Head points to the newest version in the chain.
	// A nil Head indicates an empty chain (should not normally occur for active rows).
	Head *VersionedRow

	// RowID is the stable identifier for this logical row.
	// All versions in the chain share this RowID.
	RowID uint64

	// mu protects all operations on the chain.
	// Use RLock/RUnlock for read-only operations, Lock/Unlock for modifications.
	mu sync.RWMutex
}

// NewVersionChain creates a new empty VersionChain with the given row ID.
// The chain starts with no versions (Head is nil).
//
// Typically, AddVersion should be called immediately after creating the chain
// to add the initial version of the row.
func NewVersionChain(rowID uint64) *VersionChain {
	return &VersionChain{
		Head:  nil,
		RowID: rowID,
	}
}

// AddVersion adds a new version at the head of the chain.
// The new version's PrevPtr is set to the current head before updating.
//
// This method is thread-safe and acquires the write lock.
//
// The version's RowID should match the chain's RowID, but this is not
// enforced to allow flexibility in certain operations.
//
// Parameters:
//   - version: The new VersionedRow to add. Must not be nil.
//
// Usage:
//
//	chain.AddVersion(&VersionedRow{
//	    Data:    []any{1, "value"},
//	    RowID:   chain.RowID,
//	    TxnID:   txn.GetTxnID(),
//	    CommitTS: 0, // Will be set when transaction commits
//	})
func (vc *VersionChain) AddVersion(version *VersionedRow) {
	if version == nil {
		return
	}

	vc.mu.Lock()
	defer vc.mu.Unlock()

	// Link the new version to the current head
	version.PrevPtr = vc.Head

	// Update head to point to the new version
	vc.Head = version
}

// GetHead returns the current head (newest version) of the chain.
// This method is thread-safe and acquires a read lock.
//
// Returns nil if the chain is empty.
//
// Note: The returned pointer is to the actual head, not a copy.
// Modifications to the returned VersionedRow should be done carefully
// and typically require holding the chain's lock.
func (vc *VersionChain) GetHead() *VersionedRow {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	return vc.Head
}

// FindVisibleVersion traverses the chain to find the first version visible
// to the given transaction context according to the visibility checker's rules.
//
// The traversal starts at Head (newest) and follows PrevPtr links until:
//   - A visible version is found (returned)
//   - The end of the chain is reached (returns nil)
//
// This method is thread-safe and acquires a read lock for the duration
// of the traversal.
//
// Parameters:
//   - checker: The VisibilityChecker implementing the isolation level's visibility rules
//   - txnCtx: The transaction context containing the current transaction's state
//
// Returns:
//   - The first visible VersionedRow, or nil if no visible version exists
//
// Example:
//
//	checker := GetVisibilityChecker(parser.IsolationLevelRepeatableRead)
//	visible := chain.FindVisibleVersion(checker, txnCtx)
//	if visible != nil {
//	    // Use visible.Data
//	}
func (vc *VersionChain) FindVisibleVersion(
	checker VisibilityChecker,
	txnCtx TransactionContext,
) *VersionedRow {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	// Traverse from head (newest) to oldest
	current := vc.Head
	for current != nil {
		// Convert to VersionInfo for visibility check
		versionInfo := current.ToVersionInfo()

		if checker.IsVisible(versionInfo, txnCtx) {
			return current
		}

		current = current.PrevPtr
	}

	// No visible version found
	return nil
}

// Len returns the number of versions in the chain.
// This method is thread-safe and acquires a read lock.
//
// Note: This traverses the entire chain, so it has O(n) complexity.
// Use sparingly in performance-critical code.
func (vc *VersionChain) Len() int {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	count := 0
	current := vc.Head
	for current != nil {
		count++
		current = current.PrevPtr
	}

	return count
}

// Lock acquires the write lock on the chain.
// Use this for external operations that require exclusive access across
// multiple method calls.
//
// Must be paired with Unlock. Consider using defer:
//
//	chain.Lock()
//	defer chain.Unlock()
//	// ... multiple operations on chain ...
func (vc *VersionChain) Lock() {
	vc.mu.Lock()
}

// Unlock releases the write lock on the chain.
// Must be called after Lock to release the lock.
func (vc *VersionChain) Unlock() {
	vc.mu.Unlock()
}

// RLock acquires the read lock on the chain.
// Use this for external read-only operations that require consistent access
// across multiple method calls.
//
// Must be paired with RUnlock. Consider using defer:
//
//	chain.RLock()
//	defer chain.RUnlock()
//	// ... multiple read operations on chain ...
func (vc *VersionChain) RLock() {
	vc.mu.RLock()
}

// RUnlock releases the read lock on the chain.
// Must be called after RLock to release the lock.
func (vc *VersionChain) RUnlock() {
	vc.mu.RUnlock()
}

// IsEmpty returns true if the chain has no versions.
// This method is thread-safe and acquires a read lock.
func (vc *VersionChain) IsEmpty() bool {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	return vc.Head == nil
}

// GetAllVersions returns a slice of all versions in the chain, ordered from
// newest to oldest. This is primarily useful for debugging and testing.
//
// This method is thread-safe and acquires a read lock.
// The returned slice contains pointers to the actual versions, not copies.
func (vc *VersionChain) GetAllVersions() []*VersionedRow {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	var versions []*VersionedRow
	current := vc.Head
	for current != nil {
		versions = append(versions, current)
		current = current.PrevPtr
	}

	return versions
}
