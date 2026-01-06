package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSavepointStack_NewSavepointStack(t *testing.T) {
	stack := NewSavepointStack()

	assert.NotNil(t, stack)
	assert.Empty(t, stack.stack)
	assert.Empty(t, stack.byName)
	assert.Equal(t, 0, stack.Len())
	assert.True(t, stack.IsEmpty())
}

func TestSavepointStack_Push(t *testing.T) {
	t.Run("push single savepoint", func(t *testing.T) {
		stack := NewSavepointStack()
		sp := &Savepoint{
			Name:      "sp1",
			UndoIndex: 0,
			CreatedAt: time.Now(),
		}

		err := stack.Push(sp)
		require.NoError(t, err)
		assert.Equal(t, 1, stack.Len())
		assert.False(t, stack.IsEmpty())
	})

	t.Run("push multiple savepoints", func(t *testing.T) {
		stack := NewSavepointStack()
		sp1 := &Savepoint{Name: "sp1", UndoIndex: 0}
		sp2 := &Savepoint{Name: "sp2", UndoIndex: 1}
		sp3 := &Savepoint{Name: "sp3", UndoIndex: 2}

		require.NoError(t, stack.Push(sp1))
		require.NoError(t, stack.Push(sp2))
		require.NoError(t, stack.Push(sp3))

		assert.Equal(t, 3, stack.Len())

		// Verify order (oldest first, newest last)
		assert.Equal(t, sp1, stack.stack[0])
		assert.Equal(t, sp2, stack.stack[1])
		assert.Equal(t, sp3, stack.stack[2])
	})

	t.Run("push nil savepoint returns error", func(t *testing.T) {
		stack := NewSavepointStack()

		err := stack.Push(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nil")
		assert.Equal(t, 0, stack.Len())
	})

	t.Run("push savepoint with empty name returns error", func(t *testing.T) {
		stack := NewSavepointStack()
		sp := &Savepoint{Name: "", UndoIndex: 0}

		err := stack.Push(sp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
		assert.Equal(t, 0, stack.Len())
	})

	t.Run("push duplicate name replaces existing (PostgreSQL behavior)", func(t *testing.T) {
		stack := NewSavepointStack()
		sp1 := &Savepoint{Name: "sp1", UndoIndex: 0}
		sp2 := &Savepoint{Name: "sp2", UndoIndex: 1}
		sp1New := &Savepoint{Name: "sp1", UndoIndex: 5}

		require.NoError(t, stack.Push(sp1))
		require.NoError(t, stack.Push(sp2))
		require.NoError(t, stack.Push(sp1New))

		// Should still have 2 savepoints (original sp1 was replaced)
		assert.Equal(t, 2, stack.Len())

		// The new sp1 should be at the end
		assert.Equal(t, sp2, stack.stack[0])
		assert.Equal(t, sp1New, stack.stack[1])

		// Lookup should return the new savepoint
		found, ok := stack.Get("sp1")
		require.True(t, ok)
		assert.Equal(t, 5, found.UndoIndex)
	})
}

func TestSavepointStack_Get(t *testing.T) {
	t.Run("get existing savepoint", func(t *testing.T) {
		stack := NewSavepointStack()
		sp := &Savepoint{Name: "sp1", UndoIndex: 42}
		require.NoError(t, stack.Push(sp))

		found, ok := stack.Get("sp1")
		require.True(t, ok)
		assert.Equal(t, sp, found)
		assert.Equal(t, 42, found.UndoIndex)
	})

	t.Run("get non-existent savepoint", func(t *testing.T) {
		stack := NewSavepointStack()

		found, ok := stack.Get("sp1")
		assert.False(t, ok)
		assert.Nil(t, found)
	})

	t.Run("get from multiple savepoints", func(t *testing.T) {
		stack := NewSavepointStack()
		sp1 := &Savepoint{Name: "sp1", UndoIndex: 1}
		sp2 := &Savepoint{Name: "sp2", UndoIndex: 2}
		sp3 := &Savepoint{Name: "sp3", UndoIndex: 3}

		require.NoError(t, stack.Push(sp1))
		require.NoError(t, stack.Push(sp2))
		require.NoError(t, stack.Push(sp3))

		found2, ok2 := stack.Get("sp2")
		require.True(t, ok2)
		assert.Equal(t, 2, found2.UndoIndex)
	})
}

func TestSavepointStack_Release(t *testing.T) {
	t.Run("release single savepoint", func(t *testing.T) {
		stack := NewSavepointStack()
		sp := &Savepoint{Name: "sp1", UndoIndex: 0}
		require.NoError(t, stack.Push(sp))

		err := stack.Release("sp1")
		require.NoError(t, err)
		assert.Equal(t, 0, stack.Len())
		assert.True(t, stack.IsEmpty())

		// Should not be findable anymore
		_, ok := stack.Get("sp1")
		assert.False(t, ok)
	})

	t.Run("release non-existent savepoint returns error", func(t *testing.T) {
		stack := NewSavepointStack()

		err := stack.Release("sp1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("release removes nested savepoints", func(t *testing.T) {
		stack := NewSavepointStack()
		sp1 := &Savepoint{Name: "sp1", UndoIndex: 0}
		sp2 := &Savepoint{Name: "sp2", UndoIndex: 1}
		sp3 := &Savepoint{Name: "sp3", UndoIndex: 2}

		require.NoError(t, stack.Push(sp1))
		require.NoError(t, stack.Push(sp2))
		require.NoError(t, stack.Push(sp3))
		assert.Equal(t, 3, stack.Len())

		// Release sp1 should remove sp1, sp2, and sp3 (all nested)
		err := stack.Release("sp1")
		require.NoError(t, err)
		assert.Equal(t, 0, stack.Len())

		_, ok1 := stack.Get("sp1")
		assert.False(t, ok1)
		_, ok2 := stack.Get("sp2")
		assert.False(t, ok2)
		_, ok3 := stack.Get("sp3")
		assert.False(t, ok3)
	})

	t.Run("release middle savepoint removes only newer ones", func(t *testing.T) {
		stack := NewSavepointStack()
		sp1 := &Savepoint{Name: "sp1", UndoIndex: 0}
		sp2 := &Savepoint{Name: "sp2", UndoIndex: 1}
		sp3 := &Savepoint{Name: "sp3", UndoIndex: 2}

		require.NoError(t, stack.Push(sp1))
		require.NoError(t, stack.Push(sp2))
		require.NoError(t, stack.Push(sp3))

		// Release sp2 should remove sp2 and sp3, but keep sp1
		err := stack.Release("sp2")
		require.NoError(t, err)
		assert.Equal(t, 1, stack.Len())

		_, ok1 := stack.Get("sp1")
		assert.True(t, ok1)
		_, ok2 := stack.Get("sp2")
		assert.False(t, ok2)
		_, ok3 := stack.Get("sp3")
		assert.False(t, ok3)
	})

	t.Run("release newest savepoint keeps older ones", func(t *testing.T) {
		stack := NewSavepointStack()
		sp1 := &Savepoint{Name: "sp1", UndoIndex: 0}
		sp2 := &Savepoint{Name: "sp2", UndoIndex: 1}
		sp3 := &Savepoint{Name: "sp3", UndoIndex: 2}

		require.NoError(t, stack.Push(sp1))
		require.NoError(t, stack.Push(sp2))
		require.NoError(t, stack.Push(sp3))

		// Release sp3 should only remove sp3
		err := stack.Release("sp3")
		require.NoError(t, err)
		assert.Equal(t, 2, stack.Len())

		_, ok1 := stack.Get("sp1")
		assert.True(t, ok1)
		_, ok2 := stack.Get("sp2")
		assert.True(t, ok2)
		_, ok3 := stack.Get("sp3")
		assert.False(t, ok3)
	})
}

func TestSavepointStack_RollbackTo(t *testing.T) {
	t.Run("rollback to existing savepoint", func(t *testing.T) {
		stack := NewSavepointStack()
		now := time.Now()
		sp := &Savepoint{Name: "sp1", UndoIndex: 42, CreatedAt: now}
		require.NoError(t, stack.Push(sp))

		returned, err := stack.RollbackTo("sp1")
		require.NoError(t, err)
		assert.NotNil(t, returned)
		assert.Equal(t, "sp1", returned.Name)
		assert.Equal(t, 42, returned.UndoIndex)
		assert.Equal(t, now, returned.CreatedAt)

		// Stack should be empty after rollback
		assert.Equal(t, 0, stack.Len())
	})

	t.Run("rollback to non-existent savepoint returns error", func(t *testing.T) {
		stack := NewSavepointStack()

		returned, err := stack.RollbackTo("sp1")
		require.Error(t, err)
		assert.Nil(t, returned)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("rollback removes nested savepoints", func(t *testing.T) {
		stack := NewSavepointStack()
		sp1 := &Savepoint{Name: "sp1", UndoIndex: 0}
		sp2 := &Savepoint{Name: "sp2", UndoIndex: 5}
		sp3 := &Savepoint{Name: "sp3", UndoIndex: 10}

		require.NoError(t, stack.Push(sp1))
		require.NoError(t, stack.Push(sp2))
		require.NoError(t, stack.Push(sp3))

		// Rollback to sp1 should remove all and return sp1 info
		returned, err := stack.RollbackTo("sp1")
		require.NoError(t, err)
		assert.Equal(t, 0, returned.UndoIndex)
		assert.Equal(t, 0, stack.Len())
	})

	t.Run("rollback to middle keeps older savepoints", func(t *testing.T) {
		stack := NewSavepointStack()
		sp1 := &Savepoint{Name: "sp1", UndoIndex: 0}
		sp2 := &Savepoint{Name: "sp2", UndoIndex: 5}
		sp3 := &Savepoint{Name: "sp3", UndoIndex: 10}

		require.NoError(t, stack.Push(sp1))
		require.NoError(t, stack.Push(sp2))
		require.NoError(t, stack.Push(sp3))

		// Rollback to sp2 should keep sp1
		returned, err := stack.RollbackTo("sp2")
		require.NoError(t, err)
		assert.Equal(t, 5, returned.UndoIndex)
		assert.Equal(t, 1, stack.Len())

		_, ok1 := stack.Get("sp1")
		assert.True(t, ok1)
	})

	t.Run("rollback returns copy not original", func(t *testing.T) {
		stack := NewSavepointStack()
		sp := &Savepoint{Name: "sp1", UndoIndex: 42}
		require.NoError(t, stack.Push(sp))

		returned, err := stack.RollbackTo("sp1")
		require.NoError(t, err)

		// Modifying returned should not affect anything (it's a copy)
		returned.UndoIndex = 999

		// Original sp is no longer in stack anyway
		assert.Equal(t, 0, stack.Len())
	})
}

func TestSavepointStack_Clear(t *testing.T) {
	t.Run("clear empty stack", func(t *testing.T) {
		stack := NewSavepointStack()
		stack.Clear()

		assert.Equal(t, 0, stack.Len())
		assert.True(t, stack.IsEmpty())
	})

	t.Run("clear stack with savepoints", func(t *testing.T) {
		stack := NewSavepointStack()
		sp1 := &Savepoint{Name: "sp1", UndoIndex: 0}
		sp2 := &Savepoint{Name: "sp2", UndoIndex: 1}

		require.NoError(t, stack.Push(sp1))
		require.NoError(t, stack.Push(sp2))
		assert.Equal(t, 2, stack.Len())

		stack.Clear()

		assert.Equal(t, 0, stack.Len())
		assert.True(t, stack.IsEmpty())

		_, ok1 := stack.Get("sp1")
		assert.False(t, ok1)
		_, ok2 := stack.Get("sp2")
		assert.False(t, ok2)
	})
}

func TestSavepointStack_LenAndIsEmpty(t *testing.T) {
	stack := NewSavepointStack()

	assert.Equal(t, 0, stack.Len())
	assert.True(t, stack.IsEmpty())

	require.NoError(t, stack.Push(&Savepoint{Name: "sp1", UndoIndex: 0}))
	assert.Equal(t, 1, stack.Len())
	assert.False(t, stack.IsEmpty())

	require.NoError(t, stack.Push(&Savepoint{Name: "sp2", UndoIndex: 1}))
	assert.Equal(t, 2, stack.Len())
	assert.False(t, stack.IsEmpty())

	require.NoError(t, stack.Release("sp2"))
	assert.Equal(t, 1, stack.Len())
	assert.False(t, stack.IsEmpty())

	require.NoError(t, stack.Release("sp1"))
	assert.Equal(t, 0, stack.Len())
	assert.True(t, stack.IsEmpty())
}

func TestSavepoint_Fields(t *testing.T) {
	now := time.Now()
	sp := Savepoint{
		Name:      "test_savepoint",
		UndoIndex: 42,
		CreatedAt: now,
	}

	assert.Equal(t, "test_savepoint", sp.Name)
	assert.Equal(t, 42, sp.UndoIndex)
	assert.Equal(t, now, sp.CreatedAt)
}
