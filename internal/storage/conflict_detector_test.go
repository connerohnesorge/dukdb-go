package storage

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Constructor Tests
// =============================================================================

func TestNewConflictDetector(t *testing.T) {
	cd := NewConflictDetector()
	require.NotNil(t, cd)
	assert.NotNil(t, cd.readSets)
	assert.NotNil(t, cd.writeSets)
	assert.Equal(t, 0, cd.ActiveTransactionCount())
}

// =============================================================================
// makeRowKey Tests
// =============================================================================

func TestMakeRowKey(t *testing.T) {
	tests := []struct {
		tableID  string
		rowID    string
		expected string
	}{
		{"users", "123", "users:123"},
		{"orders", "abc-def", "orders:abc-def"},
		{"", "row1", ":row1"},
		{"table", "", "table:"},
		{"schema.table", "uuid-1234", "schema.table:uuid-1234"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := makeRowKey(tt.tableID, tt.rowID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// RegisterRead Tests
// =============================================================================

func TestConflictDetector_RegisterRead_SingleRow(t *testing.T) {
	cd := NewConflictDetector()

	cd.RegisterRead(100, "users", "row1")

	readSet := cd.GetReadSet(100)
	require.NotNil(t, readSet)
	assert.Len(t, readSet, 1)
	_, exists := readSet["users:row1"]
	assert.True(t, exists, "row should be in read set")
}

func TestConflictDetector_RegisterRead_MultipleRows(t *testing.T) {
	cd := NewConflictDetector()

	cd.RegisterRead(100, "users", "row1")
	cd.RegisterRead(100, "users", "row2")
	cd.RegisterRead(100, "orders", "order1")

	readSet := cd.GetReadSet(100)
	require.NotNil(t, readSet)
	assert.Len(t, readSet, 3)
	assert.Contains(t, readSet, "users:row1")
	assert.Contains(t, readSet, "users:row2")
	assert.Contains(t, readSet, "orders:order1")
}

func TestConflictDetector_RegisterRead_DuplicateRow(t *testing.T) {
	cd := NewConflictDetector()

	// Register the same row twice
	cd.RegisterRead(100, "users", "row1")
	cd.RegisterRead(100, "users", "row1")

	readSet := cd.GetReadSet(100)
	require.NotNil(t, readSet)
	// Should only have one entry (sets don't allow duplicates)
	assert.Len(t, readSet, 1)
}

func TestConflictDetector_RegisterRead_MultipleTransactions(t *testing.T) {
	cd := NewConflictDetector()

	cd.RegisterRead(100, "users", "row1")
	cd.RegisterRead(200, "users", "row2")
	cd.RegisterRead(300, "orders", "order1")

	// Each transaction should have its own read set
	assert.Equal(t, 1, cd.ReadSetSize(100))
	assert.Equal(t, 1, cd.ReadSetSize(200))
	assert.Equal(t, 1, cd.ReadSetSize(300))
	assert.Equal(t, 3, cd.ActiveTransactionCount())
}

func TestConflictDetector_RegisterRead_SameRowDifferentTransactions(t *testing.T) {
	cd := NewConflictDetector()

	// Same row read by multiple transactions
	cd.RegisterRead(100, "users", "row1")
	cd.RegisterRead(200, "users", "row1")

	readSet100 := cd.GetReadSet(100)
	readSet200 := cd.GetReadSet(200)

	require.NotNil(t, readSet100)
	require.NotNil(t, readSet200)
	assert.Contains(t, readSet100, "users:row1")
	assert.Contains(t, readSet200, "users:row1")
}

// =============================================================================
// RegisterWrite Tests
// =============================================================================

func TestConflictDetector_RegisterWrite_SingleRow(t *testing.T) {
	cd := NewConflictDetector()

	cd.RegisterWrite(100, "users", "row1")

	writeSet := cd.GetWriteSet(100)
	require.NotNil(t, writeSet)
	assert.Len(t, writeSet, 1)
	_, exists := writeSet["users:row1"]
	assert.True(t, exists, "row should be in write set")
}

func TestConflictDetector_RegisterWrite_MultipleRows(t *testing.T) {
	cd := NewConflictDetector()

	cd.RegisterWrite(100, "users", "row1")
	cd.RegisterWrite(100, "users", "row2")
	cd.RegisterWrite(100, "orders", "order1")

	writeSet := cd.GetWriteSet(100)
	require.NotNil(t, writeSet)
	assert.Len(t, writeSet, 3)
	assert.Contains(t, writeSet, "users:row1")
	assert.Contains(t, writeSet, "users:row2")
	assert.Contains(t, writeSet, "orders:order1")
}

func TestConflictDetector_RegisterWrite_DuplicateRow(t *testing.T) {
	cd := NewConflictDetector()

	// Register the same row twice
	cd.RegisterWrite(100, "users", "row1")
	cd.RegisterWrite(100, "users", "row1")

	writeSet := cd.GetWriteSet(100)
	require.NotNil(t, writeSet)
	// Should only have one entry (sets don't allow duplicates)
	assert.Len(t, writeSet, 1)
}

func TestConflictDetector_RegisterWrite_MultipleTransactions(t *testing.T) {
	cd := NewConflictDetector()

	cd.RegisterWrite(100, "users", "row1")
	cd.RegisterWrite(200, "users", "row2")
	cd.RegisterWrite(300, "orders", "order1")

	// Each transaction should have its own write set
	assert.Equal(t, 1, cd.WriteSetSize(100))
	assert.Equal(t, 1, cd.WriteSetSize(200))
	assert.Equal(t, 1, cd.WriteSetSize(300))
}

// =============================================================================
// Mixed Read and Write Tests
// =============================================================================

func TestConflictDetector_MixedReadWrite_SameTransaction(t *testing.T) {
	cd := NewConflictDetector()

	// Transaction reads and writes different rows
	cd.RegisterRead(100, "users", "row1")
	cd.RegisterWrite(100, "users", "row2")

	assert.Equal(t, 1, cd.ReadSetSize(100))
	assert.Equal(t, 1, cd.WriteSetSize(100))

	readSet := cd.GetReadSet(100)
	writeSet := cd.GetWriteSet(100)
	assert.Contains(t, readSet, "users:row1")
	assert.Contains(t, writeSet, "users:row2")
}

func TestConflictDetector_MixedReadWrite_SameRowSameTransaction(t *testing.T) {
	cd := NewConflictDetector()

	// Transaction reads and writes the same row
	cd.RegisterRead(100, "users", "row1")
	cd.RegisterWrite(100, "users", "row1")

	// Row should be in both sets
	readSet := cd.GetReadSet(100)
	writeSet := cd.GetWriteSet(100)
	assert.Contains(t, readSet, "users:row1")
	assert.Contains(t, writeSet, "users:row1")
}

// =============================================================================
// GetReadSet and GetWriteSet Tests
// =============================================================================

func TestConflictDetector_GetReadSet_NonExistent(t *testing.T) {
	cd := NewConflictDetector()

	readSet := cd.GetReadSet(999)
	assert.Nil(t, readSet)
}

func TestConflictDetector_GetWriteSet_NonExistent(t *testing.T) {
	cd := NewConflictDetector()

	writeSet := cd.GetWriteSet(999)
	assert.Nil(t, writeSet)
}

func TestConflictDetector_GetReadSet_ReturnsCopy(t *testing.T) {
	cd := NewConflictDetector()
	cd.RegisterRead(100, "users", "row1")

	// Get the read set and modify it
	readSet := cd.GetReadSet(100)
	readSet["users:row2"] = struct{}{}

	// Original should be unchanged
	originalSet := cd.GetReadSet(100)
	assert.Len(t, originalSet, 1)
	assert.NotContains(t, originalSet, "users:row2")
}

func TestConflictDetector_GetWriteSet_ReturnsCopy(t *testing.T) {
	cd := NewConflictDetector()
	cd.RegisterWrite(100, "users", "row1")

	// Get the write set and modify it
	writeSet := cd.GetWriteSet(100)
	writeSet["users:row2"] = struct{}{}

	// Original should be unchanged
	originalSet := cd.GetWriteSet(100)
	assert.Len(t, originalSet, 1)
	assert.NotContains(t, originalSet, "users:row2")
}

// =============================================================================
// ClearTransaction Tests
// =============================================================================

func TestConflictDetector_ClearTransaction_RemovesBothSets(t *testing.T) {
	cd := NewConflictDetector()

	cd.RegisterRead(100, "users", "row1")
	cd.RegisterWrite(100, "users", "row2")

	// Verify transaction exists
	assert.True(t, cd.HasReadSet(100))
	assert.True(t, cd.HasWriteSet(100))

	// Clear the transaction
	cd.ClearTransaction(100)

	// Verify transaction is cleared
	assert.False(t, cd.HasReadSet(100))
	assert.False(t, cd.HasWriteSet(100))
	assert.Nil(t, cd.GetReadSet(100))
	assert.Nil(t, cd.GetWriteSet(100))
}

func TestConflictDetector_ClearTransaction_OnlyAffectsTarget(t *testing.T) {
	cd := NewConflictDetector()

	cd.RegisterRead(100, "users", "row1")
	cd.RegisterRead(200, "users", "row2")

	// Clear only transaction 100
	cd.ClearTransaction(100)

	// Transaction 100 should be cleared
	assert.False(t, cd.HasReadSet(100))

	// Transaction 200 should be unaffected
	assert.True(t, cd.HasReadSet(200))
	readSet := cd.GetReadSet(200)
	assert.Contains(t, readSet, "users:row2")
}

func TestConflictDetector_ClearTransaction_NonExistent(t *testing.T) {
	cd := NewConflictDetector()

	// Should not panic when clearing non-existent transaction
	assert.NotPanics(t, func() {
		cd.ClearTransaction(999)
	})
}

// =============================================================================
// HasReadSet and HasWriteSet Tests
// =============================================================================

func TestConflictDetector_HasReadSet(t *testing.T) {
	cd := NewConflictDetector()

	assert.False(t, cd.HasReadSet(100))

	cd.RegisterRead(100, "users", "row1")
	assert.True(t, cd.HasReadSet(100))

	cd.ClearTransaction(100)
	assert.False(t, cd.HasReadSet(100))
}

func TestConflictDetector_HasWriteSet(t *testing.T) {
	cd := NewConflictDetector()

	assert.False(t, cd.HasWriteSet(100))

	cd.RegisterWrite(100, "users", "row1")
	assert.True(t, cd.HasWriteSet(100))

	cd.ClearTransaction(100)
	assert.False(t, cd.HasWriteSet(100))
}

// =============================================================================
// ReadSetSize and WriteSetSize Tests
// =============================================================================

func TestConflictDetector_ReadSetSize(t *testing.T) {
	cd := NewConflictDetector()

	assert.Equal(t, 0, cd.ReadSetSize(100))

	cd.RegisterRead(100, "users", "row1")
	assert.Equal(t, 1, cd.ReadSetSize(100))

	cd.RegisterRead(100, "users", "row2")
	assert.Equal(t, 2, cd.ReadSetSize(100))

	// Duplicate should not increase size
	cd.RegisterRead(100, "users", "row1")
	assert.Equal(t, 2, cd.ReadSetSize(100))
}

func TestConflictDetector_WriteSetSize(t *testing.T) {
	cd := NewConflictDetector()

	assert.Equal(t, 0, cd.WriteSetSize(100))

	cd.RegisterWrite(100, "users", "row1")
	assert.Equal(t, 1, cd.WriteSetSize(100))

	cd.RegisterWrite(100, "users", "row2")
	assert.Equal(t, 2, cd.WriteSetSize(100))

	// Duplicate should not increase size
	cd.RegisterWrite(100, "users", "row1")
	assert.Equal(t, 2, cd.WriteSetSize(100))
}

// =============================================================================
// ActiveTransactionCount Tests
// =============================================================================

func TestConflictDetector_ActiveTransactionCount(t *testing.T) {
	cd := NewConflictDetector()

	assert.Equal(t, 0, cd.ActiveTransactionCount())

	// Add read set for transaction 100
	cd.RegisterRead(100, "users", "row1")
	assert.Equal(t, 1, cd.ActiveTransactionCount())

	// Add write set for same transaction (should still be 1)
	cd.RegisterWrite(100, "users", "row2")
	assert.Equal(t, 1, cd.ActiveTransactionCount())

	// Add another transaction
	cd.RegisterRead(200, "users", "row3")
	assert.Equal(t, 2, cd.ActiveTransactionCount())

	// Add write-only transaction
	cd.RegisterWrite(300, "orders", "order1")
	assert.Equal(t, 3, cd.ActiveTransactionCount())

	// Clear one transaction
	cd.ClearTransaction(100)
	assert.Equal(t, 2, cd.ActiveTransactionCount())
}

// =============================================================================
// Thread Safety Tests
// =============================================================================

func TestConflictDetector_ConcurrentReads(t *testing.T) {
	t.Parallel()
	cd := NewConflictDetector()
	var wg sync.WaitGroup

	// Pre-populate some data
	for i := range uint64(100) {
		cd.RegisterRead(i, "users", fmt.Sprintf("row%d", i))
	}

	// Concurrent reads should not cause races
	for i := range uint64(100) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			_ = cd.GetReadSet(txnID)
			_ = cd.ReadSetSize(txnID)
			_ = cd.HasReadSet(txnID)
		}(i)
	}

	wg.Wait()
}

func TestConflictDetector_ConcurrentWrites(t *testing.T) {
	cd := NewConflictDetector()
	var wg sync.WaitGroup

	// Concurrent writes to different transactions
	for i := range uint64(100) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			cd.RegisterRead(txnID, "users", fmt.Sprintf("row%d", txnID))
			cd.RegisterWrite(txnID, "users", fmt.Sprintf("row%d", txnID))
		}(i)
	}

	wg.Wait()

	// Verify all transactions were recorded
	assert.Equal(t, 100, cd.ActiveTransactionCount())
}

func TestConflictDetector_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()
	cd := NewConflictDetector()
	var wg sync.WaitGroup

	// Some goroutines write
	for i := range uint64(50) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			cd.RegisterRead(txnID, "users", "row1")
			cd.RegisterWrite(txnID, "orders", "order1")
		}(i)
	}

	// Some goroutines read
	for i := range uint64(50) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			_ = cd.GetReadSet(txnID)
			_ = cd.GetWriteSet(txnID)
			_ = cd.ActiveTransactionCount()
		}(i)
	}

	wg.Wait()
}

func TestConflictDetector_ConcurrentClear(t *testing.T) {
	cd := NewConflictDetector()
	var wg sync.WaitGroup

	// Create transactions
	for i := range uint64(100) {
		cd.RegisterRead(i, "users", "row1")
		cd.RegisterWrite(i, "users", "row2")
	}

	// Concurrent clears
	for i := range uint64(100) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			cd.ClearTransaction(txnID)
		}(i)
	}

	wg.Wait()

	// All transactions should be cleared
	assert.Equal(t, 0, cd.ActiveTransactionCount())
}

// =============================================================================
// Realistic Usage Scenario Tests
// =============================================================================

func TestConflictDetector_RealisticReadOnlyTransaction(t *testing.T) {
	cd := NewConflictDetector()

	// Simulate a read-only transaction scanning a table
	txnID := uint64(100)
	for i := range 100 {
		cd.RegisterRead(txnID, "users", fmt.Sprintf("row%d", i))
	}

	assert.Equal(t, 100, cd.ReadSetSize(txnID))
	assert.Equal(t, 0, cd.WriteSetSize(txnID))
	assert.True(t, cd.HasReadSet(txnID))
	assert.False(t, cd.HasWriteSet(txnID))
}

func TestConflictDetector_RealisticWriteTransaction(t *testing.T) {
	cd := NewConflictDetector()

	// Simulate a transaction that reads then writes
	txnID := uint64(100)

	// Read existing data
	cd.RegisterRead(txnID, "users", "row1")
	cd.RegisterRead(txnID, "users", "row2")

	// Write updates
	cd.RegisterWrite(txnID, "users", "row1")
	cd.RegisterWrite(txnID, "users", "row2")

	// Write new rows
	cd.RegisterWrite(txnID, "users", "row3")

	assert.Equal(t, 2, cd.ReadSetSize(txnID))
	assert.Equal(t, 3, cd.WriteSetSize(txnID))
}

func TestConflictDetector_RealisticConcurrentTransactions(t *testing.T) {
	cd := NewConflictDetector()

	// T1: Reads row1, row2
	cd.RegisterRead(1, "users", "row1")
	cd.RegisterRead(1, "users", "row2")

	// T2: Reads row2, row3, writes row2
	cd.RegisterRead(2, "users", "row2")
	cd.RegisterRead(2, "users", "row3")
	cd.RegisterWrite(2, "users", "row2")

	// T3: Reads row3, writes row4
	cd.RegisterRead(3, "users", "row3")
	cd.RegisterWrite(3, "users", "row4")

	assert.Equal(t, 3, cd.ActiveTransactionCount())

	// T2 has both reads and writes
	readSet2 := cd.GetReadSet(2)
	writeSet2 := cd.GetWriteSet(2)
	assert.Contains(t, readSet2, "users:row2")
	assert.Contains(t, readSet2, "users:row3")
	assert.Contains(t, writeSet2, "users:row2")

	// Commit T1 (clear its tracking)
	cd.ClearTransaction(1)
	assert.Equal(t, 2, cd.ActiveTransactionCount())
	assert.False(t, cd.HasReadSet(1))
}

func TestConflictDetector_MultipleTables(t *testing.T) {
	cd := NewConflictDetector()

	txnID := uint64(100)

	// Transaction accesses multiple tables
	cd.RegisterRead(txnID, "users", "user1")
	cd.RegisterRead(txnID, "orders", "order1")
	cd.RegisterRead(txnID, "products", "prod1")

	cd.RegisterWrite(txnID, "orders", "order1")
	cd.RegisterWrite(txnID, "audit_log", "log1")

	readSet := cd.GetReadSet(txnID)
	writeSet := cd.GetWriteSet(txnID)

	assert.Contains(t, readSet, "users:user1")
	assert.Contains(t, readSet, "orders:order1")
	assert.Contains(t, readSet, "products:prod1")
	assert.Contains(t, writeSet, "orders:order1")
	assert.Contains(t, writeSet, "audit_log:log1")
}

// =============================================================================
// Edge Cases
// =============================================================================

func TestConflictDetector_ZeroTransactionID(t *testing.T) {
	cd := NewConflictDetector()

	// Transaction ID 0 should work (though unusual)
	cd.RegisterRead(0, "users", "row1")
	cd.RegisterWrite(0, "users", "row2")

	assert.Equal(t, 1, cd.ReadSetSize(0))
	assert.Equal(t, 1, cd.WriteSetSize(0))
}

func TestConflictDetector_EmptyTableOrRowID(t *testing.T) {
	cd := NewConflictDetector()

	// Empty table ID
	cd.RegisterRead(100, "", "row1")
	readSet := cd.GetReadSet(100)
	assert.Contains(t, readSet, ":row1")

	// Empty row ID
	cd.RegisterWrite(100, "users", "")
	writeSet := cd.GetWriteSet(100)
	assert.Contains(t, writeSet, "users:")
}

func TestConflictDetector_LargeTransaction(t *testing.T) {
	cd := NewConflictDetector()

	txnID := uint64(100)

	// Large number of reads
	for i := range 10000 {
		cd.RegisterRead(txnID, "large_table", fmt.Sprintf("row%d", i))
	}

	assert.Equal(t, 10000, cd.ReadSetSize(txnID))

	// Clear should work efficiently
	cd.ClearTransaction(txnID)
	assert.Equal(t, 0, cd.ReadSetSize(txnID))
}

// =============================================================================
// ErrSerializationFailure Tests
// =============================================================================

func TestErrSerializationFailure_Exists(t *testing.T) {
	// Verify the error exists and has expected message
	require.NotNil(t, ErrSerializationFailure)
	assert.Contains(t, ErrSerializationFailure.Error(), "could not serialize access")
}

func TestErrSerializationFailure_IsComparable(t *testing.T) {
	// Verify we can use errors.Is to compare
	err := ErrSerializationFailure
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

func TestErrSerializationFailure_WrappedError(t *testing.T) {
	// Verify that a wrapped error still matches
	wrapped := fmt.Errorf("transaction 100 failed: %w", ErrSerializationFailure)
	assert.True(t, errors.Is(wrapped, ErrSerializationFailure))
}

// =============================================================================
// CheckConflicts Tests - Basic Cases
// =============================================================================

func TestConflictDetector_CheckConflicts_NoConflicts(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads table A
	cd.RegisterRead(1, "tableA", "row1")

	// T2 writes to table B (concurrent, already committed)
	cd.RegisterWrite(2, "tableB", "row1")

	// No conflict because they access different tables
	err := cd.CheckConflicts(1, []uint64{2})
	assert.NoError(t, err)
}

func TestConflictDetector_CheckConflicts_EmptyConcurrentList(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads and writes
	cd.RegisterRead(1, "users", "row1")
	cd.RegisterWrite(1, "users", "row1")

	// No concurrent transactions to check against
	err := cd.CheckConflicts(1, []uint64{})
	assert.NoError(t, err)
}

func TestConflictDetector_CheckConflicts_NilConcurrentList(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads and writes
	cd.RegisterRead(1, "users", "row1")
	cd.RegisterWrite(1, "users", "row1")

	// Nil concurrent transactions list
	err := cd.CheckConflicts(1, nil)
	assert.NoError(t, err)
}

func TestConflictDetector_CheckConflicts_TransactionWithNoActivity(t *testing.T) {
	cd := NewConflictDetector()

	// T2 writes something
	cd.RegisterWrite(2, "users", "row1")

	// T1 has no reads or writes
	err := cd.CheckConflicts(1, []uint64{2})
	assert.NoError(t, err)
}

func TestConflictDetector_CheckConflicts_ConcurrentWithNoActivity(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads and writes
	cd.RegisterRead(1, "users", "row1")
	cd.RegisterWrite(1, "users", "row2")

	// T2 has no activity (read-only transaction that didn't read anything?)
	err := cd.CheckConflicts(1, []uint64{2})
	assert.NoError(t, err)
}

// =============================================================================
// CheckConflicts Tests - Read-Write Conflicts
// =============================================================================

func TestConflictDetector_CheckConflicts_ReadWriteConflict(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads row1
	cd.RegisterRead(1, "users", "row1")

	// T2 writes row1 (concurrent, already committed)
	cd.RegisterWrite(2, "users", "row1")

	// Conflict: T1 read a row that T2 wrote
	err := cd.CheckConflicts(1, []uint64{2})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

func TestConflictDetector_CheckConflicts_ReadWriteConflict_MultipleReads(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads multiple rows
	cd.RegisterRead(1, "users", "row1")
	cd.RegisterRead(1, "users", "row2")
	cd.RegisterRead(1, "users", "row3")

	// T2 writes only row2 (concurrent, already committed)
	cd.RegisterWrite(2, "users", "row2")

	// Conflict: T1 read row2 that T2 wrote
	err := cd.CheckConflicts(1, []uint64{2})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

func TestConflictDetector_CheckConflicts_ReadWriteConflict_DifferentTable(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads from users
	cd.RegisterRead(1, "users", "row1")

	// T2 writes to orders (different table)
	cd.RegisterWrite(2, "orders", "row1")

	// No conflict: different tables
	err := cd.CheckConflicts(1, []uint64{2})
	assert.NoError(t, err)
}

func TestConflictDetector_CheckConflicts_ReadWriteConflict_SameTableDifferentRow(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads row1
	cd.RegisterRead(1, "users", "row1")

	// T2 writes row2 (same table, different row)
	cd.RegisterWrite(2, "users", "row2")

	// No conflict: different rows
	err := cd.CheckConflicts(1, []uint64{2})
	assert.NoError(t, err)
}

// =============================================================================
// CheckConflicts Tests - Write-Write Conflicts
// =============================================================================

func TestConflictDetector_CheckConflicts_WriteWriteConflict(t *testing.T) {
	cd := NewConflictDetector()

	// T1 writes row1
	cd.RegisterWrite(1, "users", "row1")

	// T2 also writes row1 (concurrent, already committed)
	cd.RegisterWrite(2, "users", "row1")

	// Conflict: both wrote the same row
	err := cd.CheckConflicts(1, []uint64{2})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

func TestConflictDetector_CheckConflicts_WriteWriteConflict_MultipleWrites(t *testing.T) {
	cd := NewConflictDetector()

	// T1 writes multiple rows
	cd.RegisterWrite(1, "users", "row1")
	cd.RegisterWrite(1, "users", "row2")
	cd.RegisterWrite(1, "users", "row3")

	// T2 writes row2 (concurrent, already committed)
	cd.RegisterWrite(2, "users", "row2")

	// Conflict: both wrote row2
	err := cd.CheckConflicts(1, []uint64{2})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

func TestConflictDetector_CheckConflicts_WriteWriteConflict_DifferentRows(t *testing.T) {
	cd := NewConflictDetector()

	// T1 writes row1
	cd.RegisterWrite(1, "users", "row1")

	// T2 writes row2
	cd.RegisterWrite(2, "users", "row2")

	// No conflict: different rows
	err := cd.CheckConflicts(1, []uint64{2})
	assert.NoError(t, err)
}

// =============================================================================
// CheckConflicts Tests - Multiple Concurrent Transactions
// =============================================================================

func TestConflictDetector_CheckConflicts_MultipleConcurrent_NoConflict(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads row1
	cd.RegisterRead(1, "users", "row1")

	// T2, T3, T4 write different rows
	cd.RegisterWrite(2, "users", "row2")
	cd.RegisterWrite(3, "users", "row3")
	cd.RegisterWrite(4, "users", "row4")

	// No conflicts
	err := cd.CheckConflicts(1, []uint64{2, 3, 4})
	assert.NoError(t, err)
}

func TestConflictDetector_CheckConflicts_MultipleConcurrent_ConflictWithSecond(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads row3
	cd.RegisterRead(1, "users", "row3")

	// T2 writes row2, T3 writes row3, T4 writes row4
	cd.RegisterWrite(2, "users", "row2")
	cd.RegisterWrite(3, "users", "row3")
	cd.RegisterWrite(4, "users", "row4")

	// Conflict with T3
	err := cd.CheckConflicts(1, []uint64{2, 3, 4})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

func TestConflictDetector_CheckConflicts_MultipleConcurrent_ConflictWithLast(t *testing.T) {
	cd := NewConflictDetector()

	// T1 writes row4
	cd.RegisterWrite(1, "users", "row4")

	// T2 writes row2, T3 writes row3, T4 writes row4
	cd.RegisterWrite(2, "users", "row2")
	cd.RegisterWrite(3, "users", "row3")
	cd.RegisterWrite(4, "users", "row4")

	// Conflict with T4
	err := cd.CheckConflicts(1, []uint64{2, 3, 4})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

// =============================================================================
// CheckConflicts Tests - Spec Scenarios
// =============================================================================

func TestConflictDetector_CheckConflicts_SpecScenario_WriteWriteConflict(t *testing.T) {
	// From spec:
	// Scenario: Write-write conflict detection
	// - GIVEN Transaction T1 with SERIALIZABLE isolation
	// - AND Transaction T2 with SERIALIZABLE isolation
	// - AND both transactions update the same row
	// - WHEN T1 commits first
	// - AND T2 attempts to commit
	// - THEN T2 receives serialization failure error

	cd := NewConflictDetector()

	// Both T1 and T2 update the same row
	cd.RegisterWrite(1, "users", "row1")
	cd.RegisterWrite(2, "users", "row1")

	// T1 commits first - no conflict check needed since no concurrent committed
	err := cd.CheckConflicts(1, []uint64{})
	assert.NoError(t, err)

	// T2 attempts to commit, T1 was concurrent and already committed
	err = cd.CheckConflicts(2, []uint64{1})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

func TestConflictDetector_CheckConflicts_SpecScenario_ReadWriteConflict(t *testing.T) {
	// From spec:
	// Scenario: Read-write conflict detection
	// - GIVEN Transaction T1 with SERIALIZABLE isolation reads row R
	// - AND Transaction T2 with SERIALIZABLE isolation updates row R and commits
	// - WHEN T1 attempts to commit
	// - THEN T1 receives serialization failure error
	// - AND T1 must retry the transaction

	cd := NewConflictDetector()

	// T1 reads row R
	cd.RegisterRead(1, "users", "row1")

	// T2 updates row R and commits
	cd.RegisterWrite(2, "users", "row1")

	// T2 commits first (no concurrent transactions when it started)
	err := cd.CheckConflicts(2, []uint64{})
	assert.NoError(t, err)

	// T1 attempts to commit, T2 was concurrent and committed
	err = cd.CheckConflicts(1, []uint64{2})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

func TestConflictDetector_CheckConflicts_SpecScenario_NonConflicting(t *testing.T) {
	// From spec:
	// Scenario: Non-conflicting transactions commit successfully
	// - GIVEN Transaction T1 with SERIALIZABLE isolation reads table A
	// - AND Transaction T2 with SERIALIZABLE isolation reads table B
	// - WHEN both transactions commit
	// - THEN both commits succeed

	cd := NewConflictDetector()

	// T1 reads from table A
	cd.RegisterRead(1, "tableA", "row1")

	// T2 reads from table B (note: reads don't conflict with reads)
	cd.RegisterRead(2, "tableB", "row1")

	// Both should commit successfully
	err := cd.CheckConflicts(1, []uint64{2})
	assert.NoError(t, err)

	err = cd.CheckConflicts(2, []uint64{1})
	assert.NoError(t, err)
}

// =============================================================================
// CheckConflicts Tests - Complex Scenarios
// =============================================================================

func TestConflictDetector_CheckConflicts_ReadThenWrite_ConflictOnRead(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads row1, then writes row2
	cd.RegisterRead(1, "users", "row1")
	cd.RegisterWrite(1, "users", "row2")

	// T2 writes row1
	cd.RegisterWrite(2, "users", "row1")

	// Conflict: T1 read row1 that T2 wrote
	err := cd.CheckConflicts(1, []uint64{2})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

func TestConflictDetector_CheckConflicts_ReadThenWrite_ConflictOnWrite(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads row1, then writes row2
	cd.RegisterRead(1, "users", "row1")
	cd.RegisterWrite(1, "users", "row2")

	// T2 writes row2
	cd.RegisterWrite(2, "users", "row2")

	// Conflict: T1 wrote row2 that T2 also wrote
	err := cd.CheckConflicts(1, []uint64{2})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

func TestConflictDetector_CheckConflicts_MixedReadWrite_NoConflict(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads row1, writes row2
	cd.RegisterRead(1, "users", "row1")
	cd.RegisterWrite(1, "users", "row2")

	// T2 reads row2, writes row3
	cd.RegisterRead(2, "users", "row2")
	cd.RegisterWrite(2, "users", "row3")

	// No conflict: disjoint access patterns
	err := cd.CheckConflicts(1, []uint64{2})
	assert.NoError(t, err)
}

func TestConflictDetector_CheckConflicts_MultipleTables(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads from users and orders
	cd.RegisterRead(1, "users", "user1")
	cd.RegisterRead(1, "orders", "order1")
	cd.RegisterWrite(1, "audit", "log1")

	// T2 writes to orders (conflict!)
	cd.RegisterWrite(2, "orders", "order1")

	err := cd.CheckConflicts(1, []uint64{2})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

// =============================================================================
// CheckConflicts Tests - Thread Safety
// =============================================================================

func TestConflictDetector_CheckConflicts_Concurrent(t *testing.T) {
	t.Parallel()
	cd := NewConflictDetector()
	var wg sync.WaitGroup

	// Set up transactions
	for i := range uint64(100) {
		cd.RegisterRead(i, "users", fmt.Sprintf("row%d", i%10))
		cd.RegisterWrite(i, "orders", fmt.Sprintf("order%d", i))
	}

	// Concurrent conflict checks
	for i := range uint64(100) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			// Check against some concurrent transactions
			concurrent := []uint64{(txnID + 1) % 100, (txnID + 2) % 100}
			_ = cd.CheckConflicts(txnID, concurrent)
		}(i)
	}

	wg.Wait()
	// Test passes if no race conditions detected
}

func TestConflictDetector_CheckConflicts_ConcurrentWithRegistrations(t *testing.T) {
	t.Parallel()
	cd := NewConflictDetector()
	var wg sync.WaitGroup

	// Some goroutines register reads/writes
	for i := range uint64(50) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			cd.RegisterRead(txnID, "users", fmt.Sprintf("row%d", txnID))
			cd.RegisterWrite(txnID, "orders", fmt.Sprintf("order%d", txnID))
		}(i)
	}

	// Some goroutines check conflicts
	for i := range uint64(50) {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			_ = cd.CheckConflicts(txnID, []uint64{(txnID + 10) % 50})
		}(i)
	}

	wg.Wait()
}

// =============================================================================
// CheckConflicts Tests - Edge Cases
// =============================================================================

func TestConflictDetector_CheckConflicts_SelfReference(t *testing.T) {
	cd := NewConflictDetector()

	// T1 reads and writes
	cd.RegisterRead(1, "users", "row1")
	cd.RegisterWrite(1, "users", "row1")

	// Check against itself (should not conflict - checking for committed concurrent transactions)
	// In practice, a transaction won't be in its own concurrent committed list
	err := cd.CheckConflicts(1, []uint64{1})
	// This will detect a conflict since T1's write intersects with T1's read
	// However, in real usage, we wouldn't include the transaction in its own concurrent list
	assert.Error(t, err)
}

func TestConflictDetector_CheckConflicts_NonExistentConcurrent(t *testing.T) {
	cd := NewConflictDetector()

	cd.RegisterRead(1, "users", "row1")

	// Check against a transaction that doesn't exist
	err := cd.CheckConflicts(1, []uint64{999})
	assert.NoError(t, err) // No conflict since concurrent txn has no write set
}

func TestConflictDetector_CheckConflicts_NonExistentCommitting(t *testing.T) {
	cd := NewConflictDetector()

	cd.RegisterWrite(2, "users", "row1")

	// Transaction that doesn't exist tries to check conflicts
	err := cd.CheckConflicts(999, []uint64{2})
	assert.NoError(t, err) // No conflict since txn 999 has no read/write set
}

func TestConflictDetector_CheckConflicts_EmptyRowKey(t *testing.T) {
	cd := NewConflictDetector()

	cd.RegisterRead(1, "", "")
	cd.RegisterWrite(2, "", "")

	err := cd.CheckConflicts(1, []uint64{2})
	assert.Error(t, err) // Should still detect conflict on ":" key
	assert.True(t, errors.Is(err, ErrSerializationFailure))
}

// =============================================================================
// CheckConflicts Tests - Read-only and Write-only Transactions
// =============================================================================

func TestConflictDetector_CheckConflicts_ReadOnlyTransaction(t *testing.T) {
	cd := NewConflictDetector()

	// T1 only reads
	cd.RegisterRead(1, "users", "row1")
	cd.RegisterRead(1, "users", "row2")

	// T2 only reads (concurrent)
	cd.RegisterRead(2, "users", "row1")

	// No conflict: both are read-only
	err := cd.CheckConflicts(1, []uint64{2})
	assert.NoError(t, err)
}

func TestConflictDetector_CheckConflicts_WriteOnlyTransaction(t *testing.T) {
	cd := NewConflictDetector()

	// T1 only writes
	cd.RegisterWrite(1, "users", "row1")

	// T2 only reads row1
	cd.RegisterRead(2, "users", "row1")

	// No conflict: T1 wrote, but T2 only read (T2's reads don't conflict with T1's writes)
	err := cd.CheckConflicts(1, []uint64{2})
	assert.NoError(t, err)
}
