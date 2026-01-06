// Package storage provides columnar storage for the native Go DuckDB implementation.
package storage

import (
	"errors"
	"fmt"
	"sync"
)

// ErrSerializationFailure is returned when a transaction cannot be committed
// due to a conflict with another concurrent transaction. This occurs when:
//   - A transaction reads a row that was modified by a concurrent committed transaction (read-write conflict)
//   - A transaction modifies a row that was also modified by a concurrent committed transaction (write-write conflict)
//
// When this error is received, the transaction must be retried from the beginning.
var ErrSerializationFailure = errors.New("could not serialize access due to concurrent update")

// ConflictDetector tracks read and write sets for SERIALIZABLE isolation level.
// It provides the infrastructure for detecting conflicts between concurrent transactions
// at commit time.
//
// For SERIALIZABLE isolation, transactions must appear to execute serially.
// This is achieved by tracking which rows each transaction reads and writes,
// and detecting conflicts when:
//   - A transaction reads a row that was written by another concurrent transaction
//   - A transaction writes a row that was read or written by another concurrent transaction
//
// The ConflictDetector stores sets of "tableID:rowID" strings for each transaction.
// At commit time, the CheckConflicts method (to be implemented in a future task)
// will use these sets to detect serialization conflicts.
//
// Thread Safety:
// All methods are thread-safe and can be called concurrently from multiple goroutines.
type ConflictDetector struct {
	// mu protects all internal state
	mu sync.RWMutex

	// readSets maps transaction ID to the set of rows read by that transaction.
	// Each row is identified by a "tableID:rowID" string.
	readSets map[uint64]map[string]struct{}

	// writeSets maps transaction ID to the set of rows written by that transaction.
	// Each row is identified by a "tableID:rowID" string.
	writeSets map[uint64]map[string]struct{}
}

// NewConflictDetector creates a new ConflictDetector instance.
func NewConflictDetector() *ConflictDetector {
	return &ConflictDetector{
		readSets:  make(map[uint64]map[string]struct{}),
		writeSets: make(map[uint64]map[string]struct{}),
	}
}

// makeRowKey creates a unique key for a row based on table and row IDs.
// The key format is "tableID:rowID".
func makeRowKey(tableID, rowID string) string {
	return fmt.Sprintf("%s:%s", tableID, rowID)
}

// RegisterRead records that a transaction has read a specific row.
// This is used to track the read set for SERIALIZABLE isolation.
//
// Parameters:
//   - txnID: The unique identifier of the transaction
//   - tableID: The identifier of the table containing the row
//   - rowID: The identifier of the specific row being read
//
// The read set is used at commit time to detect write-after-read conflicts
// (another transaction modified a row that this transaction read).
func (cd *ConflictDetector) RegisterRead(txnID uint64, tableID, rowID string) {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	// Ensure the transaction's read set exists
	if cd.readSets[txnID] == nil {
		cd.readSets[txnID] = make(map[string]struct{})
	}

	// Add the row to the read set
	key := makeRowKey(tableID, rowID)
	cd.readSets[txnID][key] = struct{}{}
}

// RegisterWrite records that a transaction has written a specific row.
// This is used to track the write set for SERIALIZABLE isolation.
//
// Parameters:
//   - txnID: The unique identifier of the transaction
//   - tableID: The identifier of the table containing the row
//   - rowID: The identifier of the specific row being written
//
// The write set is used at commit time to detect:
//   - Read-after-write conflicts (another transaction read a row that this transaction modified)
//   - Write-after-write conflicts (another transaction also modified the same row)
func (cd *ConflictDetector) RegisterWrite(txnID uint64, tableID, rowID string) {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	// Ensure the transaction's write set exists
	if cd.writeSets[txnID] == nil {
		cd.writeSets[txnID] = make(map[string]struct{})
	}

	// Add the row to the write set
	key := makeRowKey(tableID, rowID)
	cd.writeSets[txnID][key] = struct{}{}
}

// GetReadSet returns a copy of the read set for a transaction.
// Returns nil if the transaction has no read set.
//
// This method is primarily used for testing and debugging.
// The returned map is a copy, so modifications will not affect the detector.
func (cd *ConflictDetector) GetReadSet(txnID uint64) map[string]struct{} {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	readSet := cd.readSets[txnID]
	if readSet == nil {
		return nil
	}

	// Return a copy to prevent external modification
	result := make(map[string]struct{}, len(readSet))
	for k, v := range readSet {
		result[k] = v
	}

	return result
}

// GetWriteSet returns a copy of the write set for a transaction.
// Returns nil if the transaction has no write set.
//
// This method is primarily used for testing and debugging.
// The returned map is a copy, so modifications will not affect the detector.
func (cd *ConflictDetector) GetWriteSet(txnID uint64) map[string]struct{} {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	writeSet := cd.writeSets[txnID]
	if writeSet == nil {
		return nil
	}

	// Return a copy to prevent external modification
	result := make(map[string]struct{}, len(writeSet))
	for k, v := range writeSet {
		result[k] = v
	}

	return result
}

// ClearTransaction removes all read and write set data for a transaction.
// This should be called when a transaction commits or aborts to free memory.
//
// Parameters:
//   - txnID: The unique identifier of the transaction to clear
func (cd *ConflictDetector) ClearTransaction(txnID uint64) {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	delete(cd.readSets, txnID)
	delete(cd.writeSets, txnID)
}

// HasReadSet returns true if the transaction has a non-empty read set.
func (cd *ConflictDetector) HasReadSet(txnID uint64) bool {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	return len(cd.readSets[txnID]) > 0
}

// HasWriteSet returns true if the transaction has a non-empty write set.
func (cd *ConflictDetector) HasWriteSet(txnID uint64) bool {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	return len(cd.writeSets[txnID]) > 0
}

// ReadSetSize returns the number of rows in the transaction's read set.
// Returns 0 if the transaction has no read set.
func (cd *ConflictDetector) ReadSetSize(txnID uint64) int {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	return len(cd.readSets[txnID])
}

// WriteSetSize returns the number of rows in the transaction's write set.
// Returns 0 if the transaction has no write set.
func (cd *ConflictDetector) WriteSetSize(txnID uint64) int {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	return len(cd.writeSets[txnID])
}

// ActiveTransactionCount returns the number of transactions currently being tracked.
// A transaction is considered active if it has either a read set or write set.
func (cd *ConflictDetector) ActiveTransactionCount() int {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	// Use a set to count unique transaction IDs
	txnIDs := make(map[uint64]struct{})
	for txnID := range cd.readSets {
		txnIDs[txnID] = struct{}{}
	}
	for txnID := range cd.writeSets {
		txnIDs[txnID] = struct{}{}
	}

	return len(txnIDs)
}

// CheckConflicts checks for serialization conflicts between the given transaction
// and a list of concurrent transactions that have already committed.
//
// This method implements conflict detection for SERIALIZABLE isolation:
//
//  1. Read-Write Conflict: If this transaction read a row that was written by
//     a concurrent committed transaction, there is a conflict. This ensures
//     serializable ordering - if T1 reads R, and concurrent T2 writes R and commits
//     before T1 tries to commit, T1 must abort because its view is stale.
//
//  2. Write-Write Conflict: If this transaction wrote a row that was also written
//     by a concurrent committed transaction, there is a conflict. This ensures
//     that concurrent writes to the same row are serialized.
//
// Parameters:
//   - txnID: The unique identifier of the transaction attempting to commit
//   - committedConcurrentTxnIDs: List of transaction IDs that were concurrent with
//     txnID and have already successfully committed. These represent transactions
//     that started after txnID started but committed before txnID tries to commit.
//
// Returns:
//   - nil if no conflicts were detected and the transaction can safely commit
//   - ErrSerializationFailure if a conflict was detected
//
// Note: This method should be called during commit validation, after the transaction
// has completed all its operations but before making changes permanent.
func (cd *ConflictDetector) CheckConflicts(txnID uint64, committedConcurrentTxnIDs []uint64) error {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	// Get this transaction's read and write sets
	myReadSet := cd.readSets[txnID]
	myWriteSet := cd.writeSets[txnID]

	// If transaction has neither reads nor writes, no conflicts possible
	if len(myReadSet) == 0 && len(myWriteSet) == 0 {
		return nil
	}

	// Check against each concurrent committed transaction
	for _, concurrentTxnID := range committedConcurrentTxnIDs {
		concurrentWriteSet := cd.writeSets[concurrentTxnID]

		// Check for read-write conflicts:
		// If this transaction read a row that a concurrent transaction wrote, conflict!
		// This catches cases where we read stale data.
		if len(myReadSet) > 0 && len(concurrentWriteSet) > 0 {
			for rowKey := range myReadSet {
				if _, exists := concurrentWriteSet[rowKey]; exists {
					return ErrSerializationFailure
				}
			}
		}

		// Check for write-write conflicts:
		// If this transaction wrote a row that a concurrent transaction also wrote, conflict!
		// This ensures writes are properly serialized.
		if len(myWriteSet) > 0 && len(concurrentWriteSet) > 0 {
			for rowKey := range myWriteSet {
				if _, exists := concurrentWriteSet[rowKey]; exists {
					return ErrSerializationFailure
				}
			}
		}
	}

	return nil
}
