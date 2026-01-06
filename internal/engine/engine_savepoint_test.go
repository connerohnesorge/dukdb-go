package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransaction_CreateSavepoint(t *testing.T) {
	t.Run("create savepoint in active transaction", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		now := time.Now()
		err := txn.CreateSavepoint("sp1", now)
		require.NoError(t, err)

		sp, found := txn.GetSavepoint("sp1")
		require.True(t, found)
		assert.Equal(t, "sp1", sp.Name)
		assert.Equal(t, 0, sp.UndoIndex)
		assert.Equal(t, now, sp.CreatedAt)
		assert.Equal(t, 1, txn.SavepointCount())
	})

	t.Run("create multiple savepoints", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		// Record some undo operations
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{1}})
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{2}})

		// Create first savepoint after 2 operations
		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		// Record more operations
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{3}})

		// Create second savepoint after 3 operations
		err = txn.CreateSavepoint("sp2", time.Now())
		require.NoError(t, err)

		assert.Equal(t, 2, txn.SavepointCount())

		sp1, found := txn.GetSavepoint("sp1")
		require.True(t, found)
		assert.Equal(t, 2, sp1.UndoIndex) // Should point to position 2

		sp2, found := txn.GetSavepoint("sp2")
		require.True(t, found)
		assert.Equal(t, 3, sp2.UndoIndex) // Should point to position 3
	})

	t.Run("create savepoint with empty name fails", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		err := txn.CreateSavepoint("", time.Now())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
	})

	t.Run("create savepoint in inactive transaction fails", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		err := tm.Commit(txn)
		require.NoError(t, err)

		err = txn.CreateSavepoint("sp1", time.Now())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not active")
	})

	t.Run("duplicate savepoint name replaces existing", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		// Create first savepoint
		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		// Record some operations
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{1}})
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{2}})

		// Create another savepoint with same name
		err = txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		// Should still have only 1 savepoint
		assert.Equal(t, 1, txn.SavepointCount())

		// The savepoint should now point to the new position
		sp, found := txn.GetSavepoint("sp1")
		require.True(t, found)
		assert.Equal(t, 2, sp.UndoIndex)
	})
}

func TestTransaction_RollbackToSavepoint(t *testing.T) {
	t.Run("rollback to savepoint undoes operations in reverse order", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		// Record initial operations
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{1}})
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{2}})

		// Create savepoint
		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		// Record more operations
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{3}})
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{4}})
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{5}})

		// Track which operations were undone and in what order
		var undoneOps []UndoOperation
		undoFunc := func(op UndoOperation) error {
			undoneOps = append(undoneOps, op)
			return nil
		}

		// Rollback to savepoint
		err = txn.RollbackToSavepoint("sp1", undoFunc)
		require.NoError(t, err)

		// Should have undone 3 operations (indices 4, 3, 2 in that order)
		assert.Len(t, undoneOps, 3)

		// Verify reverse order (newest first)
		assert.Equal(t, []uint64{5}, undoneOps[0].RowIDs)
		assert.Equal(t, []uint64{4}, undoneOps[1].RowIDs)
		assert.Equal(t, []uint64{3}, undoneOps[2].RowIDs)

		// Undo log should be truncated to savepoint position
		undoLog := txn.GetUndoLog()
		assert.Len(t, undoLog, 2)
		assert.Equal(t, []uint64{1}, undoLog[0].RowIDs)
		assert.Equal(t, []uint64{2}, undoLog[1].RowIDs)

		// Savepoint should be removed
		_, found := txn.GetSavepoint("sp1")
		assert.False(t, found)
	})

	t.Run("rollback to savepoint with nil undoFunc skips undo but truncates log", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{1}})

		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{2}})
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{3}})

		// Rollback with nil undoFunc
		err = txn.RollbackToSavepoint("sp1", nil)
		require.NoError(t, err)

		// Undo log should still be truncated
		undoLog := txn.GetUndoLog()
		assert.Len(t, undoLog, 1)
	})

	t.Run("rollback to non-existent savepoint fails", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		err := txn.RollbackToSavepoint("nonexistent", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("rollback in inactive transaction fails", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		err = tm.Commit(txn)
		require.NoError(t, err)

		err = txn.RollbackToSavepoint("sp1", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not active")
	})

	t.Run("rollback removes nested savepoints", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		// Create savepoints
		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{1}})

		err = txn.CreateSavepoint("sp2", time.Now())
		require.NoError(t, err)

		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{2}})

		err = txn.CreateSavepoint("sp3", time.Now())
		require.NoError(t, err)

		assert.Equal(t, 3, txn.SavepointCount())

		// Rollback to sp1 should remove sp1, sp2, and sp3
		err = txn.RollbackToSavepoint("sp1", nil)
		require.NoError(t, err)

		assert.Equal(t, 0, txn.SavepointCount())
	})

	t.Run("rollback to middle savepoint keeps older ones", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{1}})

		err = txn.CreateSavepoint("sp2", time.Now())
		require.NoError(t, err)

		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{2}})

		err = txn.CreateSavepoint("sp3", time.Now())
		require.NoError(t, err)

		// Rollback to sp2 should keep sp1
		err = txn.RollbackToSavepoint("sp2", nil)
		require.NoError(t, err)

		assert.Equal(t, 1, txn.SavepointCount())

		_, found := txn.GetSavepoint("sp1")
		assert.True(t, found)

		_, found = txn.GetSavepoint("sp2")
		assert.False(t, found)
	})

	t.Run("undo function error stops rollback", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{1}})
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{2}})

		// Undo function that fails on the second call
		callCount := 0
		undoFunc := func(_ UndoOperation) error {
			callCount++
			if callCount == 2 {
				return errors.New("undo failed")
			}
			return nil
		}

		err = txn.RollbackToSavepoint("sp1", undoFunc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to undo operation")
	})
}

func TestTransaction_ReleaseSavepoint(t *testing.T) {
	t.Run("release savepoint keeps undo log intact", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{1}})

		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{2}})
		txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{3}})

		// Release savepoint
		err = txn.ReleaseSavepoint("sp1")
		require.NoError(t, err)

		// Savepoint should be gone
		_, found := txn.GetSavepoint("sp1")
		assert.False(t, found)

		// Undo log should be intact
		undoLog := txn.GetUndoLog()
		assert.Len(t, undoLog, 3)
	})

	t.Run("release non-existent savepoint fails", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		err := txn.ReleaseSavepoint("nonexistent")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("release in inactive transaction fails", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		err = tm.Commit(txn)
		require.NoError(t, err)

		err = txn.ReleaseSavepoint("sp1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not active")
	})

	t.Run("release removes nested savepoints", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		err = txn.CreateSavepoint("sp2", time.Now())
		require.NoError(t, err)

		err = txn.CreateSavepoint("sp3", time.Now())
		require.NoError(t, err)

		assert.Equal(t, 3, txn.SavepointCount())

		// Release sp1 should remove sp1, sp2, and sp3
		err = txn.ReleaseSavepoint("sp1")
		require.NoError(t, err)

		assert.Equal(t, 0, txn.SavepointCount())
	})
}

func TestTransaction_GetSavepoint(t *testing.T) {
	t.Run("get existing savepoint", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		now := time.Now()
		err := txn.CreateSavepoint("sp1", now)
		require.NoError(t, err)

		sp, found := txn.GetSavepoint("sp1")
		require.True(t, found)
		assert.Equal(t, "sp1", sp.Name)
		assert.Equal(t, now, sp.CreatedAt)
	})

	t.Run("get non-existent savepoint", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		sp, found := txn.GetSavepoint("nonexistent")
		assert.False(t, found)
		assert.Nil(t, sp)
	})

	t.Run("get savepoint from transaction with nil savepoints", func(t *testing.T) {
		// Create transaction directly without using Begin (to test nil savepoints handling)
		txn := &Transaction{
			id:         1,
			active:     true,
			savepoints: nil,
		}

		sp, found := txn.GetSavepoint("sp1")
		assert.False(t, found)
		assert.Nil(t, sp)
	})
}

func TestTransaction_SavepointCount(t *testing.T) {
	t.Run("count starts at zero", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		assert.Equal(t, 0, txn.SavepointCount())
	})

	t.Run("count increases with savepoints", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)
		assert.Equal(t, 1, txn.SavepointCount())

		err = txn.CreateSavepoint("sp2", time.Now())
		require.NoError(t, err)
		assert.Equal(t, 2, txn.SavepointCount())

		err = txn.CreateSavepoint("sp3", time.Now())
		require.NoError(t, err)
		assert.Equal(t, 3, txn.SavepointCount())
	})

	t.Run("count decreases with release", func(t *testing.T) {
		tm := NewTransactionManager()
		txn := tm.Begin()

		err := txn.CreateSavepoint("sp1", time.Now())
		require.NoError(t, err)

		err = txn.CreateSavepoint("sp2", time.Now())
		require.NoError(t, err)

		err = txn.ReleaseSavepoint("sp2")
		require.NoError(t, err)
		assert.Equal(t, 1, txn.SavepointCount())
	})

	t.Run("count is zero for nil savepoints", func(t *testing.T) {
		txn := &Transaction{
			id:         1,
			active:     true,
			savepoints: nil,
		}

		assert.Equal(t, 0, txn.SavepointCount())
	})
}

func TestTransaction_ClearUndoLog_ClearsSavepoints(t *testing.T) {
	tm := NewTransactionManager()
	txn := tm.Begin()

	// Create some savepoints
	err := txn.CreateSavepoint("sp1", time.Now())
	require.NoError(t, err)

	err = txn.CreateSavepoint("sp2", time.Now())
	require.NoError(t, err)

	// Record some undo operations
	txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{1}})
	txn.RecordUndo(UndoOperation{TableName: "t1", OpType: UndoInsert, RowIDs: []uint64{2}})

	assert.Equal(t, 2, txn.SavepointCount())
	assert.Len(t, txn.GetUndoLog(), 2)

	// Clear undo log
	txn.ClearUndoLog()

	// Both undo log and savepoints should be cleared
	assert.Equal(t, 0, txn.SavepointCount())
	assert.Len(t, txn.GetUndoLog(), 0)
}

func TestTransactionManager_Commit_ClearsSavepoints(t *testing.T) {
	tm := NewTransactionManager()
	txn := tm.Begin()

	// Create savepoints
	err := txn.CreateSavepoint("sp1", time.Now())
	require.NoError(t, err)

	err = txn.CreateSavepoint("sp2", time.Now())
	require.NoError(t, err)

	assert.Equal(t, 2, txn.SavepointCount())

	// Commit transaction
	err = tm.Commit(txn)
	require.NoError(t, err)

	// Savepoints should be cleared
	assert.Equal(t, 0, txn.SavepointCount())
}

func TestTransactionManager_Rollback_ClearsSavepoints(t *testing.T) {
	tm := NewTransactionManager()
	txn := tm.Begin()

	// Create savepoints
	err := txn.CreateSavepoint("sp1", time.Now())
	require.NoError(t, err)

	err = txn.CreateSavepoint("sp2", time.Now())
	require.NoError(t, err)

	assert.Equal(t, 2, txn.SavepointCount())

	// Rollback transaction
	err = tm.Rollback(txn)
	require.NoError(t, err)

	// Savepoints should be cleared
	assert.Equal(t, 0, txn.SavepointCount())
}

func TestTransactionManager_Begin_InitializesSavepoints(t *testing.T) {
	tm := NewTransactionManager()
	txn := tm.Begin()

	// Savepoints should be initialized (not nil)
	assert.Equal(t, 0, txn.SavepointCount())

	// Should be able to create savepoints without error
	err := txn.CreateSavepoint("sp1", time.Now())
	require.NoError(t, err)
	assert.Equal(t, 1, txn.SavepointCount())
}

func TestTransaction_CreateSavepoint_InitializesSavepointsIfNil(t *testing.T) {
	// Create transaction directly without using Begin
	txn := &Transaction{
		id:         1,
		active:     true,
		savepoints: nil, // Explicitly nil
	}

	// CreateSavepoint should initialize savepoints
	err := txn.CreateSavepoint("sp1", time.Now())
	require.NoError(t, err)

	assert.Equal(t, 1, txn.SavepointCount())

	sp, found := txn.GetSavepoint("sp1")
	require.True(t, found)
	assert.Equal(t, "sp1", sp.Name)
}
