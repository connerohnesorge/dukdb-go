package storage

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Constructor Tests
// =============================================================================

func TestNewLockManager(t *testing.T) {
	lm := NewLockManager()
	require.NotNil(t, lm)
	assert.NotNil(t, lm.locks)
	assert.NotNil(t, lm.txnLocks)
	assert.Equal(t, 0, lm.TotalLockCount())
}

// =============================================================================
// ErrLockTimeout Tests
// =============================================================================

func TestErrLockTimeout_Exists(t *testing.T) {
	require.NotNil(t, ErrLockTimeout)
	assert.Contains(t, ErrLockTimeout.Error(), "lock acquisition timed out")
}

func TestErrLockTimeout_IsComparable(t *testing.T) {
	err := ErrLockTimeout
	assert.True(t, errors.Is(err, ErrLockTimeout))
}

func TestErrLockTimeout_WrappedError(t *testing.T) {
	wrapped := fmt.Errorf("transaction 100 failed: %w", ErrLockTimeout)
	assert.True(t, errors.Is(wrapped, ErrLockTimeout))
}

// =============================================================================
// Lock Acquisition Tests - Basic
// =============================================================================

func TestLockManager_Lock_SingleLock(t *testing.T) {
	lm := NewLockManager()

	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	assert.True(t, lm.IsLocked("users", "row1"))
	assert.True(t, lm.IsLockedByTxn(100, "users", "row1"))
	assert.Equal(t, 1, lm.TotalLockCount())
}

func TestLockManager_Lock_MultipleLocksSameTransaction(t *testing.T) {
	lm := NewLockManager()

	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	err = lm.Lock(100, "users", "row2", time.Second)
	require.NoError(t, err)

	err = lm.Lock(100, "orders", "order1", time.Second)
	require.NoError(t, err)

	assert.Equal(t, 3, lm.TotalLockCount())
	assert.Equal(t, 3, lm.GetLocksHeldByTxn(100))
}

func TestLockManager_Lock_ReentrantLock(t *testing.T) {
	lm := NewLockManager()

	// Acquire lock
	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	// Same transaction locking same row should succeed immediately (idempotent)
	err = lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	// Should still be only one lock
	assert.Equal(t, 1, lm.TotalLockCount())
}

func TestLockManager_Lock_DifferentTransactionsDifferentRows(t *testing.T) {
	lm := NewLockManager()

	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	err = lm.Lock(200, "users", "row2", time.Second)
	require.NoError(t, err)

	assert.True(t, lm.IsLockedByTxn(100, "users", "row1"))
	assert.True(t, lm.IsLockedByTxn(200, "users", "row2"))
	assert.Equal(t, 2, lm.TotalLockCount())
}

// =============================================================================
// Lock Timeout Tests
// =============================================================================

func TestLockManager_Lock_ImmediateTimeout(t *testing.T) {
	lm := NewLockManager()

	// T1 acquires lock
	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	// T2 tries to lock same row with zero timeout
	err = lm.Lock(200, "users", "row1", 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrLockTimeout))
}

func TestLockManager_Lock_ShortTimeout(t *testing.T) {
	lm := NewLockManager()

	// T1 acquires lock
	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	// T2 tries to lock same row with short timeout
	start := time.Now()
	err = lm.Lock(200, "users", "row1", 50*time.Millisecond)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrLockTimeout))
	// Should have waited approximately the timeout duration
	assert.True(t, elapsed >= 40*time.Millisecond, "should wait at least near timeout")
	assert.True(t, elapsed < 200*time.Millisecond, "should not wait much longer than timeout")
}

// =============================================================================
// Lock Blocking and Release Tests
// =============================================================================

func TestLockManager_Lock_BlocksUntilRelease(t *testing.T) {
	lm := NewLockManager()

	// T1 acquires lock
	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	var wg sync.WaitGroup
	var t2Err error
	var t2AcquireTime time.Time

	// T2 tries to acquire lock (will block)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t2Err = lm.Lock(200, "users", "row1", 5*time.Second)
		t2AcquireTime = time.Now()
	}()

	// Small delay to ensure T2 is waiting
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, lm.GetWaiterCount("users", "row1"))

	// Release T1's lock
	releaseTime := time.Now()
	lm.Release(100)

	// Wait for T2 to complete
	wg.Wait()

	// T2 should have acquired the lock
	require.NoError(t, t2Err)
	assert.True(t, lm.IsLockedByTxn(200, "users", "row1"))
	// T2 should have acquired shortly after release
	assert.True(t, t2AcquireTime.After(releaseTime) || t2AcquireTime.Equal(releaseTime))
}

func TestLockManager_Release_NoLocksHeld(t *testing.T) {
	lm := NewLockManager()

	// Should not panic when releasing with no locks
	assert.NotPanics(t, func() {
		lm.Release(999)
	})
}

func TestLockManager_Release_ReleasesAllLocks(t *testing.T) {
	lm := NewLockManager()

	// T1 acquires multiple locks
	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)
	err = lm.Lock(100, "users", "row2", time.Second)
	require.NoError(t, err)
	err = lm.Lock(100, "orders", "order1", time.Second)
	require.NoError(t, err)

	assert.Equal(t, 3, lm.TotalLockCount())

	// Release all locks
	lm.Release(100)

	assert.Equal(t, 0, lm.TotalLockCount())
	assert.False(t, lm.IsLocked("users", "row1"))
	assert.False(t, lm.IsLocked("users", "row2"))
	assert.False(t, lm.IsLocked("orders", "order1"))
}

// =============================================================================
// Waiter Notification Tests
// =============================================================================

func TestLockManager_Release_NotifiesFirstWaiter(t *testing.T) {
	lm := NewLockManager()

	// T1 acquires lock
	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	var wg sync.WaitGroup
	results := make([]uint64, 0, 3)
	var resultsMu sync.Mutex

	// T2, T3, T4 all try to acquire (will wait in order)
	for _, txnID := range []uint64{200, 300, 400} {
		wg.Add(1)
		go func(tid uint64) {
			defer wg.Done()
			time.Sleep(time.Duration(tid-199) * 10 * time.Millisecond) // Stagger start
			lockErr := lm.Lock(tid, "users", "row1", 5*time.Second)
			if lockErr == nil {
				resultsMu.Lock()
				results = append(results, tid)
				resultsMu.Unlock()
				// Release after acquiring
				time.Sleep(10 * time.Millisecond)
				lm.Release(tid)
			}
		}(txnID)
	}

	// Wait for waiters to queue
	time.Sleep(100 * time.Millisecond)

	// Release T1
	lm.Release(100)

	// Wait for all to complete
	wg.Wait()

	// All three should have acquired the lock
	assert.Len(t, results, 3)
}

func TestLockManager_TimeoutRemovesWaiter(t *testing.T) {
	lm := NewLockManager()

	// T1 acquires lock
	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	// T2 tries with short timeout
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = lm.Lock(200, "users", "row1", 50*time.Millisecond)
	}()

	// Wait a bit for T2 to start waiting
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 1, lm.GetWaiterCount("users", "row1"))

	// Wait for timeout
	wg.Wait()

	// Waiter should be removed after timeout
	// Allow some time for cleanup
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0, lm.GetWaiterCount("users", "row1"))
}

// =============================================================================
// TryLock Tests
// =============================================================================

func TestLockManager_TryLock_Success(t *testing.T) {
	lm := NewLockManager()

	success := lm.TryLock(100, "users", "row1")
	assert.True(t, success)
	assert.True(t, lm.IsLockedByTxn(100, "users", "row1"))
}

func TestLockManager_TryLock_Failure(t *testing.T) {
	lm := NewLockManager()

	// T1 acquires lock
	success := lm.TryLock(100, "users", "row1")
	assert.True(t, success)

	// T2 tries - should fail immediately
	success = lm.TryLock(200, "users", "row1")
	assert.False(t, success)
}

// =============================================================================
// Query Methods Tests
// =============================================================================

func TestLockManager_IsLocked(t *testing.T) {
	lm := NewLockManager()

	assert.False(t, lm.IsLocked("users", "row1"))

	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	assert.True(t, lm.IsLocked("users", "row1"))

	lm.Release(100)
	assert.False(t, lm.IsLocked("users", "row1"))
}

func TestLockManager_IsLockedByTxn(t *testing.T) {
	lm := NewLockManager()

	assert.False(t, lm.IsLockedByTxn(100, "users", "row1"))

	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	assert.True(t, lm.IsLockedByTxn(100, "users", "row1"))
	assert.False(t, lm.IsLockedByTxn(200, "users", "row1"))
}

func TestLockManager_GetLockHolder(t *testing.T) {
	lm := NewLockManager()

	// No lock
	holder, exists := lm.GetLockHolder("users", "row1")
	assert.False(t, exists)
	assert.Equal(t, uint64(0), holder)

	// Acquire lock
	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	holder, exists = lm.GetLockHolder("users", "row1")
	assert.True(t, exists)
	assert.Equal(t, uint64(100), holder)
}

func TestLockManager_GetLocksHeldByTxn(t *testing.T) {
	lm := NewLockManager()

	assert.Equal(t, 0, lm.GetLocksHeldByTxn(100))

	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)
	assert.Equal(t, 1, lm.GetLocksHeldByTxn(100))

	err = lm.Lock(100, "users", "row2", time.Second)
	require.NoError(t, err)
	assert.Equal(t, 2, lm.GetLocksHeldByTxn(100))
}

func TestLockManager_GetWaiterCount(t *testing.T) {
	lm := NewLockManager()

	assert.Equal(t, 0, lm.GetWaiterCount("users", "row1"))

	// T1 acquires lock
	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, lm.GetWaiterCount("users", "row1"))

	// T2 starts waiting
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = lm.Lock(200, "users", "row1", 2*time.Second)
	}()

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, lm.GetWaiterCount("users", "row1"))

	// Release to let T2 complete
	lm.Release(100)
	wg.Wait()
}

func TestLockManager_TotalLockCount(t *testing.T) {
	lm := NewLockManager()

	assert.Equal(t, 0, lm.TotalLockCount())

	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)
	assert.Equal(t, 1, lm.TotalLockCount())

	err = lm.Lock(200, "users", "row2", time.Second)
	require.NoError(t, err)
	assert.Equal(t, 2, lm.TotalLockCount())

	lm.Release(100)
	assert.Equal(t, 1, lm.TotalLockCount())
}

func TestLockManager_TotalWaiterCount(t *testing.T) {
	lm := NewLockManager()

	assert.Equal(t, 0, lm.TotalWaiterCount())

	// Acquire two locks by T1
	_ = lm.Lock(100, "users", "row1", time.Second)
	_ = lm.Lock(100, "users", "row2", time.Second)

	var wg sync.WaitGroup

	// Start waiters on both rows
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = lm.Lock(200, "users", "row1", 2*time.Second)
	}()
	go func() {
		defer wg.Done()
		_ = lm.Lock(300, "users", "row2", 2*time.Second)
	}()

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 2, lm.TotalWaiterCount())

	// Release to clean up
	lm.Release(100)
	wg.Wait()
}

// =============================================================================
// Spec Scenario Tests
// =============================================================================

func TestLockManager_SpecScenario_ExclusiveLockAcquisition(t *testing.T) {
	// From spec:
	// Scenario: Exclusive lock acquisition for writes
	// - GIVEN Transaction T1 performing UPDATE on row R
	// - WHEN Transaction T2 attempts UPDATE on same row R
	// - THEN T2 blocks until T1 commits or rolls back

	lm := NewLockManager()

	// T1 acquires lock for UPDATE
	err := lm.Lock(1, "users", "row1", time.Second)
	require.NoError(t, err)

	var t2Started atomic.Bool
	var t2Acquired atomic.Bool
	var wg sync.WaitGroup

	// T2 attempts lock (should block)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t2Started.Store(true)
		lockErr := lm.Lock(2, "users", "row1", 5*time.Second)
		if lockErr == nil {
			t2Acquired.Store(true)
		}
	}()

	// Wait for T2 to start blocking
	time.Sleep(50 * time.Millisecond)
	assert.True(t, t2Started.Load())
	assert.False(t, t2Acquired.Load())

	// T1 commits (releases locks)
	lm.Release(1)

	// Wait for T2
	wg.Wait()

	// T2 should now hold the lock
	assert.True(t, t2Acquired.Load())
	assert.True(t, lm.IsLockedByTxn(2, "users", "row1"))
}

func TestLockManager_SpecScenario_LockTimeout(t *testing.T) {
	// From spec:
	// Scenario: Lock timeout
	// - GIVEN Transaction T1 holds lock on row R
	// - AND T1 does not commit within lock timeout period
	// - WHEN Transaction T2 waits for lock on row R
	// - AND timeout expires
	// - THEN T2 receives lock timeout error

	lm := NewLockManager()

	// T1 acquires lock
	err := lm.Lock(1, "users", "row1", time.Second)
	require.NoError(t, err)

	// T2 waits with timeout
	err = lm.Lock(2, "users", "row1", 100*time.Millisecond)

	// T2 should timeout
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrLockTimeout))

	// T1 still holds the lock
	assert.True(t, lm.IsLockedByTxn(1, "users", "row1"))
}

func TestLockManager_SpecScenario_LockReleaseOnCommit(t *testing.T) {
	// From spec:
	// Scenario: Lock release on commit
	// - GIVEN Transaction T1 holds locks on multiple rows
	// - WHEN T1 commits
	// - THEN all locks held by T1 are released
	// - AND waiting transactions can proceed

	lm := NewLockManager()

	// T1 acquires multiple locks
	_ = lm.Lock(1, "users", "row1", time.Second)
	_ = lm.Lock(1, "users", "row2", time.Second)
	_ = lm.Lock(1, "orders", "order1", time.Second)

	var wg sync.WaitGroup
	var t2Acquired, t3Acquired atomic.Bool

	// T2 waits for row1
	wg.Add(1)
	go func() {
		defer wg.Done()
		if lm.Lock(2, "users", "row1", 5*time.Second) == nil {
			t2Acquired.Store(true)
		}
	}()

	// T3 waits for row2
	wg.Add(1)
	go func() {
		defer wg.Done()
		if lm.Lock(3, "users", "row2", 5*time.Second) == nil {
			t3Acquired.Store(true)
		}
	}()

	time.Sleep(50 * time.Millisecond)

	// T1 commits (releases all locks)
	lm.Release(1)

	wg.Wait()

	// Both T2 and T3 should have acquired their locks
	assert.True(t, t2Acquired.Load())
	assert.True(t, t3Acquired.Load())
}

func TestLockManager_SpecScenario_LockReleaseOnRollback(t *testing.T) {
	// From spec:
	// Scenario: Lock release on rollback
	// - GIVEN Transaction T1 holds locks on multiple rows
	// - WHEN T1 rolls back
	// - THEN all locks held by T1 are released
	// - AND waiting transactions can proceed

	// Note: Release() is used for both commit and rollback
	// The behavior is identical from lock manager's perspective

	lm := NewLockManager()

	// T1 acquires locks
	_ = lm.Lock(1, "users", "row1", time.Second)

	var wg sync.WaitGroup
	var t2Acquired atomic.Bool

	// T2 waits
	wg.Add(1)
	go func() {
		defer wg.Done()
		if lm.Lock(2, "users", "row1", 5*time.Second) == nil {
			t2Acquired.Store(true)
		}
	}()

	time.Sleep(50 * time.Millisecond)

	// T1 rolls back (also calls Release)
	lm.Release(1)

	wg.Wait()

	assert.True(t, t2Acquired.Load())
}

// =============================================================================
// Thread Safety Tests
// =============================================================================

func TestLockManager_ConcurrentLockAcquisition(t *testing.T) {
	t.Parallel()
	lm := NewLockManager()
	var wg sync.WaitGroup

	// Multiple transactions lock different rows concurrently
	for i := range uint64(100) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			err := lm.Lock(txnID, "users", fmt.Sprintf("row%d", txnID), time.Second)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()
	assert.Equal(t, 100, lm.TotalLockCount())
}

func TestLockManager_ConcurrentLockAndRelease(t *testing.T) {
	t.Parallel()
	lm := NewLockManager()
	var wg sync.WaitGroup

	// Goroutines acquiring and releasing locks
	for i := range uint64(50) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			err := lm.Lock(txnID, "users", fmt.Sprintf("row%d", txnID%10), time.Second)
			if err == nil {
				time.Sleep(10 * time.Millisecond)
				lm.Release(txnID)
			}
		}(i)
	}

	wg.Wait()
}

func TestLockManager_ConcurrentContention(t *testing.T) {
	t.Parallel()
	lm := NewLockManager()
	var wg sync.WaitGroup
	var successCount atomic.Int32

	// Many transactions compete for the same row
	for i := range uint64(20) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			err := lm.Lock(txnID, "users", "hot_row", 2*time.Second)
			if err == nil {
				successCount.Add(1)
				time.Sleep(10 * time.Millisecond)
				lm.Release(txnID)
			}
		}(i)
	}

	wg.Wait()

	// All should eventually succeed
	assert.Equal(t, int32(20), successCount.Load())
}

func TestLockManager_ConcurrentQueryMethods(t *testing.T) {
	t.Parallel()
	lm := NewLockManager()
	var wg sync.WaitGroup

	// Pre-populate some locks
	for i := range uint64(50) {
		_ = lm.Lock(i, "users", fmt.Sprintf("row%d", i), time.Second)
	}

	// Concurrent queries
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = lm.IsLocked("users", "row1")
			_ = lm.TotalLockCount()
			_ = lm.GetWaiterCount("users", "row1")
		}()
	}

	wg.Wait()
}

// =============================================================================
// Edge Cases
// =============================================================================

func TestLockManager_ZeroTransactionID(t *testing.T) {
	lm := NewLockManager()

	err := lm.Lock(0, "users", "row1", time.Second)
	require.NoError(t, err)

	assert.True(t, lm.IsLockedByTxn(0, "users", "row1"))

	lm.Release(0)
	assert.False(t, lm.IsLocked("users", "row1"))
}

func TestLockManager_EmptyTableOrRowID(t *testing.T) {
	lm := NewLockManager()

	// Empty table ID
	err := lm.Lock(100, "", "row1", time.Second)
	require.NoError(t, err)
	assert.True(t, lm.IsLocked("", "row1"))

	// Empty row ID
	err = lm.Lock(100, "users", "", time.Second)
	require.NoError(t, err)
	assert.True(t, lm.IsLocked("users", ""))
}

func TestLockManager_SpecialCharactersInIDs(t *testing.T) {
	lm := NewLockManager()

	err := lm.Lock(100, "schema.table", "uuid-1234-5678", time.Second)
	require.NoError(t, err)
	assert.True(t, lm.IsLocked("schema.table", "uuid-1234-5678"))
}

func TestLockManager_LargeLockCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large lock test in short mode")
	}

	lm := NewLockManager()

	// Single transaction acquires many locks
	for i := range 10000 {
		err := lm.Lock(100, "large_table", fmt.Sprintf("row%d", i), time.Second)
		require.NoError(t, err)
	}

	assert.Equal(t, 10000, lm.TotalLockCount())
	assert.Equal(t, 10000, lm.GetLocksHeldByTxn(100))

	// Release should handle large lock counts efficiently
	start := time.Now()
	lm.Release(100)
	elapsed := time.Since(start)

	assert.Equal(t, 0, lm.TotalLockCount())
	assert.Less(t, elapsed, time.Second, "release should be fast")
}

// =============================================================================
// Waiter Queue Order Tests
// =============================================================================

func TestLockManager_WaiterQueueFIFO(t *testing.T) {
	lm := NewLockManager()

	// T1 acquires lock
	err := lm.Lock(1, "users", "row1", time.Second)
	require.NoError(t, err)

	var acquireOrder []uint64
	var orderMu sync.Mutex
	var wg sync.WaitGroup

	// Start waiters in order with delays to ensure ordering
	for _, txnID := range []uint64{2, 3, 4, 5} {
		wg.Add(1)
		go func(tid uint64) {
			defer wg.Done()
			// Stagger start times to ensure queue order
			time.Sleep(time.Duration(tid-1) * 20 * time.Millisecond)
			lockErr := lm.Lock(tid, "users", "row1", 10*time.Second)
			if lockErr == nil {
				orderMu.Lock()
				acquireOrder = append(acquireOrder, tid)
				orderMu.Unlock()
				// Release immediately
				lm.Release(tid)
			}
		}(txnID)
	}

	// Wait for all waiters to queue
	time.Sleep(200 * time.Millisecond)

	// Release T1 to let waiters proceed
	lm.Release(1)

	wg.Wait()

	// Should have acquired in FIFO order
	assert.Len(t, acquireOrder, 4)
	assert.Equal(t, []uint64{2, 3, 4, 5}, acquireOrder)
}

// =============================================================================
// Multiple Releases Tests
// =============================================================================

func TestLockManager_DoubleRelease(t *testing.T) {
	lm := NewLockManager()

	// Acquire lock
	err := lm.Lock(100, "users", "row1", time.Second)
	require.NoError(t, err)

	// Release once
	lm.Release(100)

	// Second release should be safe (no-op)
	assert.NotPanics(t, func() {
		lm.Release(100)
	})
}

// =============================================================================
// Integration-like Tests
// =============================================================================

func TestLockManager_SimulatedTransactionWorkflow(t *testing.T) {
	lm := NewLockManager()
	var wg sync.WaitGroup
	completedTxns := make(chan uint64, 100)

	// Simulate multiple transactions
	for i := range uint64(10) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()

			// Each transaction locks multiple rows
			rows := []string{
				fmt.Sprintf("row%d", txnID%5),
				fmt.Sprintf("row%d", (txnID+1)%5),
			}

			// Acquire all locks (could timeout)
			acquired := make([]bool, len(rows))
			for j, row := range rows {
				if err := lm.Lock(txnID, "users", row, 500*time.Millisecond); err == nil {
					acquired[j] = true
				}
			}

			// Simulate work
			time.Sleep(50 * time.Millisecond)

			// Release all held locks
			lm.Release(txnID)

			completedTxns <- txnID
		}(i)
	}

	wg.Wait()
	close(completedTxns)

	// Count completed transactions
	completed := 0
	for range completedTxns {
		completed++
	}
	assert.Equal(t, 10, completed)

	// All locks should be released
	assert.Equal(t, 0, lm.TotalLockCount())
}
