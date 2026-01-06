package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSnapshot(t *testing.T) {
	now := time.Now()
	activeIDs := []uint64{1, 2, 3}

	snapshot := NewSnapshot(now, activeIDs)

	require.NotNil(t, snapshot)
	assert.Equal(t, now, snapshot.Timestamp)
	assert.Equal(t, activeIDs, snapshot.ActiveTxnIDs)
}

func TestNewSnapshot_CopiesSlice(t *testing.T) {
	now := time.Now()
	activeIDs := []uint64{1, 2, 3}

	snapshot := NewSnapshot(now, activeIDs)

	// Modify the original slice
	activeIDs[0] = 999

	// Snapshot should not be affected
	assert.Equal(t, uint64(1), snapshot.ActiveTxnIDs[0])
}

func TestNewSnapshot_EmptyActiveIDs(t *testing.T) {
	now := time.Now()
	snapshot := NewSnapshot(now, []uint64{})

	require.NotNil(t, snapshot)
	assert.Equal(t, now, snapshot.Timestamp)
	assert.Empty(t, snapshot.ActiveTxnIDs)
}

func TestNewSnapshot_NilActiveIDs(t *testing.T) {
	now := time.Now()
	snapshot := NewSnapshot(now, nil)

	require.NotNil(t, snapshot)
	assert.Equal(t, now, snapshot.Timestamp)
	assert.Empty(t, snapshot.ActiveTxnIDs)
}

func TestSnapshot_WasActiveAtSnapshot(t *testing.T) {
	now := time.Now()
	activeIDs := []uint64{10, 20, 30}
	snapshot := NewSnapshot(now, activeIDs)

	// Transaction IDs that were active at snapshot time
	assert.True(t, snapshot.WasActiveAtSnapshot(10))
	assert.True(t, snapshot.WasActiveAtSnapshot(20))
	assert.True(t, snapshot.WasActiveAtSnapshot(30))

	// Transaction IDs that were NOT active at snapshot time
	assert.False(t, snapshot.WasActiveAtSnapshot(1))
	assert.False(t, snapshot.WasActiveAtSnapshot(15))
	assert.False(t, snapshot.WasActiveAtSnapshot(100))
}

func TestSnapshot_WasActiveAtSnapshot_EmptySnapshot(t *testing.T) {
	now := time.Now()
	snapshot := NewSnapshot(now, []uint64{})

	// No transactions were active
	assert.False(t, snapshot.WasActiveAtSnapshot(1))
	assert.False(t, snapshot.WasActiveAtSnapshot(100))
}

func TestSnapshot_WasActiveAtSnapshot_NilSnapshot(t *testing.T) {
	var snapshot *Snapshot = nil

	// Nil snapshot should return false
	assert.False(t, snapshot.WasActiveAtSnapshot(1))
	assert.False(t, snapshot.WasActiveAtSnapshot(100))
}

func TestSnapshot_WasActiveAtSnapshot_LazySetInit(t *testing.T) {
	now := time.Now()
	activeIDs := []uint64{10, 20, 30}
	snapshot := NewSnapshot(now, activeIDs)

	// First call should initialize the set
	assert.Nil(t, snapshot.activeTxnSet)
	snapshot.WasActiveAtSnapshot(10)
	assert.NotNil(t, snapshot.activeTxnSet)
	assert.Len(t, snapshot.activeTxnSet, 3)

	// Subsequent calls should reuse the set (check by length staying the same)
	snapshot.WasActiveAtSnapshot(20)
	assert.Len(t, snapshot.activeTxnSet, 3, "Set should not be recreated on subsequent calls")
}

func TestSnapshot_GetTimestamp(t *testing.T) {
	now := time.Now()
	snapshot := NewSnapshot(now, []uint64{1, 2, 3})

	assert.Equal(t, now, snapshot.GetTimestamp())
}

func TestSnapshot_GetTimestamp_NilSnapshot(t *testing.T) {
	var snapshot *Snapshot = nil

	assert.True(t, snapshot.GetTimestamp().IsZero())
}

func TestSnapshot_GetActiveTransactionCount(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name          string
		activeIDs     []uint64
		expectedCount int
	}{
		{"no active transactions", []uint64{}, 0},
		{"one active transaction", []uint64{1}, 1},
		{"multiple active transactions", []uint64{1, 2, 3, 4, 5}, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := NewSnapshot(now, tt.activeIDs)
			assert.Equal(t, tt.expectedCount, snapshot.GetActiveTransactionCount())
		})
	}
}

func TestSnapshot_GetActiveTransactionCount_NilSnapshot(t *testing.T) {
	var snapshot *Snapshot = nil

	assert.Equal(t, 0, snapshot.GetActiveTransactionCount())
}

func TestSnapshot_IsAfterSnapshot(t *testing.T) {
	baseTime := time.Now()
	snapshot := NewSnapshot(baseTime, []uint64{})

	// Time before snapshot
	assert.False(t, snapshot.IsAfterSnapshot(baseTime.Add(-1*time.Hour)))

	// Time exactly at snapshot
	assert.False(t, snapshot.IsAfterSnapshot(baseTime))

	// Time after snapshot
	assert.True(t, snapshot.IsAfterSnapshot(baseTime.Add(1*time.Nanosecond)))
	assert.True(t, snapshot.IsAfterSnapshot(baseTime.Add(1*time.Hour)))
}

func TestSnapshot_IsAfterSnapshot_NilSnapshot(t *testing.T) {
	var snapshot *Snapshot = nil

	// Nil snapshot should return false for any time
	assert.False(t, snapshot.IsAfterSnapshot(time.Now()))
	assert.False(t, snapshot.IsAfterSnapshot(time.Time{}))
}

func TestSnapshot_IsBeforeOrAtSnapshot(t *testing.T) {
	baseTime := time.Now()
	snapshot := NewSnapshot(baseTime, []uint64{})

	// Time before snapshot
	assert.True(t, snapshot.IsBeforeOrAtSnapshot(baseTime.Add(-1*time.Hour)))

	// Time exactly at snapshot
	assert.True(t, snapshot.IsBeforeOrAtSnapshot(baseTime))

	// Time after snapshot
	assert.False(t, snapshot.IsBeforeOrAtSnapshot(baseTime.Add(1*time.Nanosecond)))
	assert.False(t, snapshot.IsBeforeOrAtSnapshot(baseTime.Add(1*time.Hour)))
}

func TestSnapshot_IsBeforeOrAtSnapshot_NilSnapshot(t *testing.T) {
	var snapshot *Snapshot = nil

	// Nil snapshot should return true for any time
	assert.True(t, snapshot.IsBeforeOrAtSnapshot(time.Now()))
	assert.True(t, snapshot.IsBeforeOrAtSnapshot(time.Time{}))
}

// TestSnapshot_ScenarioRepeatableRead tests a realistic REPEATABLE READ scenario.
func TestSnapshot_ScenarioRepeatableRead(t *testing.T) {
	// Scenario:
	// - T1 (txn ID 100) starts at time T with active transactions: T2 (200), T3 (300)
	// - T1 should NOT see uncommitted data from T2 or T3
	// - T1 SHOULD see committed data from T4 (400) which committed before T1 started

	snapshotTime := time.Now()
	activeTxnsAtStart := []uint64{200, 300} // T2 and T3 were active when T1 started
	snapshot := NewSnapshot(snapshotTime, activeTxnsAtStart)

	// T2 (200) was active at snapshot - its data should NOT be visible
	assert.True(t, snapshot.WasActiveAtSnapshot(200),
		"T2 was active at snapshot time")

	// T3 (300) was active at snapshot - its data should NOT be visible
	assert.True(t, snapshot.WasActiveAtSnapshot(300),
		"T3 was active at snapshot time")

	// T4 (400) was NOT active at snapshot (presumably committed) - its data SHOULD be visible
	assert.False(t, snapshot.WasActiveAtSnapshot(400),
		"T4 was not active at snapshot time (should be visible)")

	// T0 (50) was NOT active at snapshot (presumably committed long ago)
	assert.False(t, snapshot.WasActiveAtSnapshot(50),
		"Old committed transaction should not be in active list")

	// Data created before snapshot should be considered visible (time-based check)
	dataCreatedBeforeSnapshot := snapshotTime.Add(-1 * time.Hour)
	assert.True(t, snapshot.IsBeforeOrAtSnapshot(dataCreatedBeforeSnapshot),
		"Data created before snapshot should be visible")

	// Data created after snapshot should NOT be visible (time-based check)
	dataCreatedAfterSnapshot := snapshotTime.Add(1 * time.Second)
	assert.True(t, snapshot.IsAfterSnapshot(dataCreatedAfterSnapshot),
		"Data created after snapshot should not be visible")
}

// TestSnapshot_LargeActiveTransactionSet tests performance with many active transactions.
func TestSnapshot_LargeActiveTransactionSet(t *testing.T) {
	now := time.Now()

	// Create a snapshot with 1000 active transactions
	activeIDs := make([]uint64, 1000)
	for i := range activeIDs {
		activeIDs[i] = uint64(i + 1)
	}

	snapshot := NewSnapshot(now, activeIDs)

	// Check that all active transactions are properly tracked
	for i := uint64(1); i <= 1000; i++ {
		assert.True(t, snapshot.WasActiveAtSnapshot(i),
			"Transaction %d should be in active set", i)
	}

	// Check that non-active transactions are not tracked
	assert.False(t, snapshot.WasActiveAtSnapshot(0))
	assert.False(t, snapshot.WasActiveAtSnapshot(1001))
	assert.False(t, snapshot.WasActiveAtSnapshot(9999))
}
