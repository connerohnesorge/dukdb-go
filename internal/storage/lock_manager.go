// Package storage provides columnar storage for the native Go DuckDB implementation.
package storage

import (
	"errors"
	"sync"
	"time"
)

// ErrLockTimeout is returned when a lock acquisition times out waiting for
// another transaction to release its lock on a row.
var ErrLockTimeout = errors.New("lock acquisition timed out waiting for concurrent transaction")

// LockManager manages row-level exclusive locks for write conflict handling.
// It provides the infrastructure for blocking concurrent writes to the same row
// in SERIALIZABLE isolation level.
//
// The LockManager implements a wait/notify pattern using channels:
//   - When a transaction wants to lock a row already locked by another transaction, it waits
//   - When a transaction releases its locks (commit/rollback), waiting transactions are notified
//   - Deadlock avoidance is achieved through timeout-based acquisition
//
// Thread Safety:
// All methods are thread-safe and can be called concurrently from multiple goroutines.
type LockManager struct {
	// mu protects all internal state
	mu sync.Mutex

	// locks maps row keys ("tableID:rowID") to lock entries
	locks map[string]*lockEntry

	// txnLocks maps transaction ID to list of row keys locked by that transaction
	txnLocks map[uint64][]string
}

// lockEntry represents a lock on a specific row.
type lockEntry struct {
	// holderTxnID is the transaction ID currently holding the lock
	holderTxnID uint64

	// waiters is the list of transactions waiting to acquire this lock
	waiters []*lockWaiter
}

// lockWaiter represents a transaction waiting to acquire a lock.
type lockWaiter struct {
	// txnID is the ID of the waiting transaction
	txnID uint64

	// notify is the channel to signal when the lock becomes available
	notify chan struct{}
}

// NewLockManager creates a new LockManager instance.
func NewLockManager() *LockManager {
	return &LockManager{
		locks:    make(map[string]*lockEntry),
		txnLocks: make(map[uint64][]string),
	}
}

// Lock attempts to acquire an exclusive lock on a row for the given transaction.
// If the row is already locked by another transaction, this method blocks until:
//   - The lock becomes available (returns nil)
//   - The timeout expires (returns ErrLockTimeout)
//
// Parameters:
//   - txnID: The unique identifier of the transaction requesting the lock
//   - tableID: The identifier of the table containing the row
//   - rowID: The identifier of the specific row to lock
//   - timeout: Maximum time to wait for lock acquisition (0 means no waiting)
//
// Returns:
//   - nil if the lock was successfully acquired
//   - ErrLockTimeout if the timeout expired while waiting
//
// Note: If the transaction already holds the lock on this row, this is a no-op
// and returns nil immediately.
func (lm *LockManager) Lock(txnID uint64, tableID, rowID string, timeout time.Duration) error {
	key := makeRowKey(tableID, rowID)

	lm.mu.Lock()

	// Check if lock exists
	entry, exists := lm.locks[key]

	if !exists {
		// No lock exists - acquire it immediately
		lm.locks[key] = &lockEntry{
			holderTxnID: txnID,
			waiters:     nil,
		}
		lm.recordLockForTxn(txnID, key)
		lm.mu.Unlock()
		return nil
	}

	// Lock exists - check if we already hold it
	if entry.holderTxnID == txnID {
		// We already hold this lock - no-op
		lm.mu.Unlock()
		return nil
	}

	// Lock held by another transaction - need to wait
	if timeout == 0 {
		// No waiting allowed - immediate timeout
		lm.mu.Unlock()
		return ErrLockTimeout
	}

	// Create a waiter entry
	waiter := &lockWaiter{
		txnID:  txnID,
		notify: make(chan struct{}),
	}
	entry.waiters = append(entry.waiters, waiter)
	lm.mu.Unlock()

	// Wait for lock to become available or timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-waiter.notify:
		// Lock became available - now try to acquire it
		// Note: We need to re-check and acquire under lock
		return lm.tryAcquireAfterNotify(txnID, key, waiter)

	case <-timer.C:
		// Timeout expired - remove ourselves from waiters and return error
		lm.removeWaiter(key, waiter)
		return ErrLockTimeout
	}
}

// tryAcquireAfterNotify attempts to acquire a lock after being notified.
// This handles the race condition where multiple waiters are notified simultaneously.
func (lm *LockManager) tryAcquireAfterNotify(txnID uint64, key string, waiter *lockWaiter) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[key]
	if !exists {
		// Lock was released and no one else took it - acquire it
		lm.locks[key] = &lockEntry{
			holderTxnID: txnID,
			waiters:     nil,
		}
		lm.recordLockForTxn(txnID, key)
		return nil
	}

	// Check if we already hold it (could happen if notified correctly)
	if entry.holderTxnID == txnID {
		return nil
	}

	// Check if we're the designated holder after release
	// The release function should have set us as the holder
	// If someone else got it, we lost the race
	// In this implementation, the releaser picks the first waiter, so if we were
	// notified, we should be the holder
	// This is a defensive check
	if entry.holderTxnID != 0 && entry.holderTxnID != txnID {
		// Someone else got the lock - this shouldn't normally happen
		// with our implementation, but handle it gracefully
		// Remove ourselves from waiters if still there
		lm.removeWaiterUnlocked(key, waiter)
		return ErrLockTimeout
	}

	return nil
}

// removeWaiter removes a waiter from the waiters list for a lock.
func (lm *LockManager) removeWaiter(key string, waiter *lockWaiter) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.removeWaiterUnlocked(key, waiter)
}

// removeWaiterUnlocked removes a waiter without holding the lock.
// Caller must hold lm.mu.
func (lm *LockManager) removeWaiterUnlocked(key string, waiter *lockWaiter) {
	entry, exists := lm.locks[key]
	if !exists {
		return
	}

	for i, w := range entry.waiters {
		if w == waiter {
			// Remove waiter from slice
			entry.waiters = append(entry.waiters[:i], entry.waiters[i+1:]...)
			break
		}
	}
}

// recordLockForTxn records that a transaction holds a lock on a key.
// Caller must hold lm.mu.
func (lm *LockManager) recordLockForTxn(txnID uint64, key string) {
	lm.txnLocks[txnID] = append(lm.txnLocks[txnID], key)
}

// Release releases all locks held by the given transaction.
// This should be called when a transaction commits or rolls back.
//
// For each lock released, if there are waiting transactions, the first
// waiter is notified and given the lock.
//
// Parameters:
//   - txnID: The unique identifier of the transaction releasing locks
func (lm *LockManager) Release(txnID uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Get all keys locked by this transaction
	keys, exists := lm.txnLocks[txnID]
	if !exists {
		return
	}

	// Release each lock
	for _, key := range keys {
		entry, lockExists := lm.locks[key]
		if !lockExists {
			continue
		}

		// Verify we hold this lock
		if entry.holderTxnID != txnID {
			continue
		}

		// Check if there are waiters
		if len(entry.waiters) > 0 {
			// Give lock to first waiter
			nextWaiter := entry.waiters[0]
			entry.waiters = entry.waiters[1:]
			entry.holderTxnID = nextWaiter.txnID

			// Record lock for new holder
			lm.txnLocks[nextWaiter.txnID] = append(lm.txnLocks[nextWaiter.txnID], key)

			// Notify the waiter
			close(nextWaiter.notify)
		} else {
			// No waiters - delete the lock entry
			delete(lm.locks, key)
		}
	}

	// Remove transaction from tracking
	delete(lm.txnLocks, txnID)
}

// TryLock attempts to acquire a lock without waiting.
// This is equivalent to Lock with timeout=0.
//
// Parameters:
//   - txnID: The unique identifier of the transaction requesting the lock
//   - tableID: The identifier of the table containing the row
//   - rowID: The identifier of the specific row to lock
//
// Returns:
//   - true if the lock was acquired
//   - false if the lock is held by another transaction
func (lm *LockManager) TryLock(txnID uint64, tableID, rowID string) bool {
	return lm.Lock(txnID, tableID, rowID, 0) == nil
}

// IsLocked returns whether a row is currently locked by any transaction.
//
// Parameters:
//   - tableID: The identifier of the table containing the row
//   - rowID: The identifier of the specific row to check
//
// Returns:
//   - true if the row is locked
//   - false if the row is not locked
func (lm *LockManager) IsLocked(tableID, rowID string) bool {
	key := makeRowKey(tableID, rowID)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	_, exists := lm.locks[key]
	return exists
}

// IsLockedByTxn returns whether a row is currently locked by a specific transaction.
//
// Parameters:
//   - txnID: The transaction ID to check
//   - tableID: The identifier of the table containing the row
//   - rowID: The identifier of the specific row to check
//
// Returns:
//   - true if the row is locked by the specified transaction
//   - false otherwise
func (lm *LockManager) IsLockedByTxn(txnID uint64, tableID, rowID string) bool {
	key := makeRowKey(tableID, rowID)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[key]
	if !exists {
		return false
	}
	return entry.holderTxnID == txnID
}

// GetLockHolder returns the transaction ID holding the lock on a row.
//
// Parameters:
//   - tableID: The identifier of the table containing the row
//   - rowID: The identifier of the specific row to check
//
// Returns:
//   - The transaction ID holding the lock
//   - exists is false if no lock exists on this row
func (lm *LockManager) GetLockHolder(tableID, rowID string) (txnID uint64, exists bool) {
	key := makeRowKey(tableID, rowID)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, entryExists := lm.locks[key]
	if !entryExists {
		return 0, false
	}
	return entry.holderTxnID, true
}

// GetLocksHeldByTxn returns the number of locks held by a transaction.
//
// Parameters:
//   - txnID: The transaction ID to check
//
// Returns:
//   - The number of locks held by the transaction
func (lm *LockManager) GetLocksHeldByTxn(txnID uint64) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	return len(lm.txnLocks[txnID])
}

// GetWaiterCount returns the number of transactions waiting for a lock on a row.
//
// Parameters:
//   - tableID: The identifier of the table containing the row
//   - rowID: The identifier of the specific row to check
//
// Returns:
//   - The number of waiting transactions (0 if no lock or no waiters)
func (lm *LockManager) GetWaiterCount(tableID, rowID string) int {
	key := makeRowKey(tableID, rowID)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[key]
	if !exists {
		return 0
	}
	return len(entry.waiters)
}

// TotalLockCount returns the total number of locks currently held.
// This is primarily for monitoring and debugging.
func (lm *LockManager) TotalLockCount() int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	return len(lm.locks)
}

// TotalWaiterCount returns the total number of transactions waiting across all locks.
// This is primarily for monitoring and debugging.
func (lm *LockManager) TotalWaiterCount() int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	count := 0
	for _, entry := range lm.locks {
		count += len(entry.waiters)
	}
	return count
}
