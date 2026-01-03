package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHashIndex(t *testing.T) {
	t.Run("creates non-unique index", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)
		assert.NotNil(t, idx)
		assert.Equal(t, "idx_users_age", idx.Name)
		assert.Equal(t, "users", idx.TableName)
		assert.Equal(t, []string{"age"}, idx.Columns)
		assert.False(t, idx.IsUnique)
		assert.Equal(t, 0, idx.Count())
	})

	t.Run("creates unique index", func(t *testing.T) {
		idx := NewHashIndex("idx_users_email", "users", []string{"email"}, true)
		assert.NotNil(t, idx)
		assert.Equal(t, "idx_users_email", idx.Name)
		assert.Equal(t, "users", idx.TableName)
		assert.Equal(t, []string{"email"}, idx.Columns)
		assert.True(t, idx.IsUnique)
		assert.Equal(t, 0, idx.Count())
	})

	t.Run("creates multi-column index", func(t *testing.T) {
		idx := NewHashIndex("idx_users_name", "users", []string{"first_name", "last_name"}, false)
		assert.NotNil(t, idx)
		assert.Equal(t, []string{"first_name", "last_name"}, idx.Columns)
	})
}

func makeRowID(id uint64) RowID {
	return RowID(id)
}

func TestHashIndexInsert(t *testing.T) {
	t.Run("inserts single value into non-unique index", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)
		rowID := makeRowID(0)

		err := idx.Insert([]any{int32(25)}, rowID)
		require.NoError(t, err)

		assert.Equal(t, 1, idx.Count())
		rowIDs := idx.Lookup([]any{int32(25)})
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, rowID, rowIDs[0])
	})

	t.Run("inserts multiple values with same key into non-unique index", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)
		rowID1 := makeRowID(0)
		rowID2 := makeRowID(1)
		rowID3 := makeRowID(2)

		err := idx.Insert([]any{int32(25)}, rowID1)
		require.NoError(t, err)
		err = idx.Insert([]any{int32(25)}, rowID2)
		require.NoError(t, err)
		err = idx.Insert([]any{int32(25)}, rowID3)
		require.NoError(t, err)

		assert.Equal(t, 1, idx.Count()) // One unique key
		rowIDs := idx.Lookup([]any{int32(25)})
		assert.Len(t, rowIDs, 3)
		assert.Contains(t, rowIDs, rowID1)
		assert.Contains(t, rowIDs, rowID2)
		assert.Contains(t, rowIDs, rowID3)
	})

	t.Run("inserts into unique index succeeds for different keys", func(t *testing.T) {
		idx := NewHashIndex("idx_users_email", "users", []string{"email"}, true)
		rowID1 := makeRowID(0)
		rowID2 := makeRowID(1)

		err := idx.Insert([]any{"alice@example.com"}, rowID1)
		require.NoError(t, err)
		err = idx.Insert([]any{"bob@example.com"}, rowID2)
		require.NoError(t, err)

		assert.Equal(t, 2, idx.Count())
	})

	t.Run("inserts into unique index fails for duplicate key", func(t *testing.T) {
		idx := NewHashIndex("idx_users_email", "users", []string{"email"}, true)
		rowID1 := makeRowID(0)
		rowID2 := makeRowID(1)

		err := idx.Insert([]any{"alice@example.com"}, rowID1)
		require.NoError(t, err)

		err = idx.Insert([]any{"alice@example.com"}, rowID2)
		require.Error(t, err)
		assert.Equal(t, 1, idx.Count())

		// Original entry should still be there
		rowIDs := idx.Lookup([]any{"alice@example.com"})
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, rowID1, rowIDs[0])
	})

	t.Run("inserts with wrong key length returns error", func(t *testing.T) {
		idx := NewHashIndex("idx_users_name", "users", []string{"first_name", "last_name"}, false)
		rowID := makeRowID(0)

		err := idx.Insert([]any{"Alice"}, rowID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "key length")
	})

	t.Run("inserts multi-column key", func(t *testing.T) {
		idx := NewHashIndex("idx_users_name", "users", []string{"first_name", "last_name"}, false)
		rowID1 := makeRowID(0)
		rowID2 := makeRowID(1)

		err := idx.Insert([]any{"Alice", "Smith"}, rowID1)
		require.NoError(t, err)
		err = idx.Insert([]any{"Alice", "Jones"}, rowID2)
		require.NoError(t, err)

		assert.Equal(t, 2, idx.Count())

		rowIDs := idx.Lookup([]any{"Alice", "Smith"})
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, rowID1, rowIDs[0])

		rowIDs = idx.Lookup([]any{"Alice", "Jones"})
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, rowID2, rowIDs[0])
	})

	t.Run("inserts with NULL values", func(t *testing.T) {
		idx := NewHashIndex("idx_users_middle_name", "users", []string{"middle_name"}, false)
		rowID1 := makeRowID(0)
		rowID2 := makeRowID(1)

		err := idx.Insert([]any{nil}, rowID1)
		require.NoError(t, err)
		err = idx.Insert([]any{nil}, rowID2)
		require.NoError(t, err)

		rowIDs := idx.Lookup([]any{nil})
		assert.Len(t, rowIDs, 2)
	})

	t.Run("inserts various data types", func(t *testing.T) {
		idx := NewHashIndex("idx_test", "test", []string{"col"}, false)

		// Integer types
		err := idx.Insert([]any{int8(1)}, makeRowID(0))
		require.NoError(t, err)
		err = idx.Insert([]any{int16(2)}, makeRowID(1))
		require.NoError(t, err)
		err = idx.Insert([]any{int32(3)}, makeRowID(2))
		require.NoError(t, err)
		err = idx.Insert([]any{int64(4)}, makeRowID(3))
		require.NoError(t, err)

		// Unsigned integer types
		err = idx.Insert([]any{uint8(5)}, makeRowID(4))
		require.NoError(t, err)
		err = idx.Insert([]any{uint16(6)}, makeRowID(5))
		require.NoError(t, err)
		err = idx.Insert([]any{uint32(7)}, makeRowID(6))
		require.NoError(t, err)
		err = idx.Insert([]any{uint64(8)}, makeRowID(7))
		require.NoError(t, err)

		// Float types
		err = idx.Insert([]any{float32(9.5)}, makeRowID(8))
		require.NoError(t, err)
		err = idx.Insert([]any{float64(10.5)}, makeRowID(9))
		require.NoError(t, err)

		// String and byte array
		err = idx.Insert([]any{"test"}, makeRowID(10))
		require.NoError(t, err)
		err = idx.Insert([]any{[]byte{1, 2, 3}}, makeRowID(11))
		require.NoError(t, err)

		// Boolean
		err = idx.Insert([]any{true}, makeRowID(12))
		require.NoError(t, err)

		assert.Equal(t, 13, idx.Count())
	})
}

func TestHashIndexLookup(t *testing.T) {
	t.Run("lookup existing key", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)
		rowID := makeRowID(0)

		err := idx.Insert([]any{int32(25)}, rowID)
		require.NoError(t, err)

		rowIDs := idx.Lookup([]any{int32(25)})
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, rowID, rowIDs[0])
	})

	t.Run("lookup non-existing key", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)

		rowIDs := idx.Lookup([]any{int32(25)})
		assert.Len(t, rowIDs, 0)
	})

	t.Run("lookup with wrong key length", func(t *testing.T) {
		idx := NewHashIndex("idx_users_name", "users", []string{"first_name", "last_name"}, false)

		rowIDs := idx.Lookup([]any{"Alice"})
		assert.Nil(t, rowIDs)
	})

	t.Run("lookup returns copy of row IDs", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)
		rowID1 := makeRowID(0)
		rowID2 := makeRowID(1)

		err := idx.Insert([]any{int32(25)}, rowID1)
		require.NoError(t, err)
		err = idx.Insert([]any{int32(25)}, rowID2)
		require.NoError(t, err)

		rowIDs1 := idx.Lookup([]any{int32(25)})
		rowIDs2 := idx.Lookup([]any{int32(25)})

		// Modify the first result
		rowIDs1[0] = makeRowID(9999)

		// Second result should be unchanged
		assert.NotEqual(t, rowIDs1[0], rowIDs2[0])
		assert.Equal(t, rowID1, rowIDs2[0])
	})
}

func TestHashIndexDelete(t *testing.T) {
	t.Run("deletes existing entry", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)
		rowID := makeRowID(0)

		err := idx.Insert([]any{int32(25)}, rowID)
		require.NoError(t, err)

		err = idx.Delete([]any{int32(25)}, rowID)
		require.NoError(t, err)

		assert.Equal(t, 0, idx.Count())
		rowIDs := idx.Lookup([]any{int32(25)})
		assert.Len(t, rowIDs, 0)
	})

	t.Run("deletes one of multiple entries for same key", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)
		rowID1 := makeRowID(0)
		rowID2 := makeRowID(1)
		rowID3 := makeRowID(2)

		err := idx.Insert([]any{int32(25)}, rowID1)
		require.NoError(t, err)
		err = idx.Insert([]any{int32(25)}, rowID2)
		require.NoError(t, err)
		err = idx.Insert([]any{int32(25)}, rowID3)
		require.NoError(t, err)

		err = idx.Delete([]any{int32(25)}, rowID2)
		require.NoError(t, err)

		assert.Equal(t, 1, idx.Count()) // Key still exists
		rowIDs := idx.Lookup([]any{int32(25)})
		assert.Len(t, rowIDs, 2)
		assert.Contains(t, rowIDs, rowID1)
		assert.Contains(t, rowIDs, rowID3)
		assert.NotContains(t, rowIDs, rowID2)
	})

	t.Run("deletes non-existing entry does not error", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)
		rowID := makeRowID(0)

		err := idx.Delete([]any{int32(25)}, rowID)
		require.NoError(t, err)
	})

	t.Run("deletes with wrong key length returns error", func(t *testing.T) {
		idx := NewHashIndex("idx_users_name", "users", []string{"first_name", "last_name"}, false)
		rowID := makeRowID(0)

		err := idx.Delete([]any{"Alice"}, rowID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "key length")
	})

	t.Run("deletes last entry for key removes key from index", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)
		rowID1 := makeRowID(0)
		rowID2 := makeRowID(1)

		err := idx.Insert([]any{int32(25)}, rowID1)
		require.NoError(t, err)
		err = idx.Insert([]any{int32(30)}, rowID2)
		require.NoError(t, err)

		assert.Equal(t, 2, idx.Count())

		err = idx.Delete([]any{int32(25)}, rowID1)
		require.NoError(t, err)

		assert.Equal(t, 1, idx.Count())
		rowIDs := idx.Lookup([]any{int32(25)})
		assert.Len(t, rowIDs, 0)

		// Other key should still be there
		rowIDs = idx.Lookup([]any{int32(30)})
		assert.Len(t, rowIDs, 1)
	})
}

func TestHashIndexClear(t *testing.T) {
	t.Run("clears all entries", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)

		err := idx.Insert([]any{int32(25)}, makeRowID(0))
		require.NoError(t, err)
		err = idx.Insert([]any{int32(30)}, makeRowID(1))
		require.NoError(t, err)
		err = idx.Insert([]any{int32(35)}, makeRowID(2))
		require.NoError(t, err)

		assert.Equal(t, 3, idx.Count())

		idx.Clear()

		assert.Equal(t, 0, idx.Count())
		rowIDs := idx.Lookup([]any{int32(25)})
		assert.Len(t, rowIDs, 0)
		rowIDs = idx.Lookup([]any{int32(30)})
		assert.Len(t, rowIDs, 0)
		rowIDs = idx.Lookup([]any{int32(35)})
		assert.Len(t, rowIDs, 0)
	})
}

func TestHashIndexConcurrency(t *testing.T) {
	t.Run("concurrent inserts and lookups", func(t *testing.T) {
		idx := NewHashIndex("idx_users_age", "users", []string{"age"}, false)

		// Use channels to coordinate goroutines
		done := make(chan bool)

		// Insert goroutine
		go func() {
			for i := 0; i < 100; i++ {
				rowID := makeRowID(uint64(i))
				err := idx.Insert([]any{int32(i % 10)}, rowID)
				require.NoError(t, err)
			}
			done <- true
		}()

		// Lookup goroutine
		go func() {
			for i := 0; i < 100; i++ {
				idx.Lookup([]any{int32(i % 10)})
			}
			done <- true
		}()

		// Wait for both goroutines
		<-done
		<-done

		// Verify final state
		assert.LessOrEqual(t, idx.Count(), 10) // At most 10 unique keys
	})
}
