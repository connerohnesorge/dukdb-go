package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/assert"
)

// ============================================================================
// VersionedRow Tests
// ============================================================================

// TestVersionedRow_IsCommitted tests that IsCommitted returns true when CommitTS != 0.
func TestVersionedRow_IsCommitted(t *testing.T) {
	tests := []struct {
		name     string
		row      *VersionedRow
		expected bool
	}{
		{
			name: "committed row (CommitTS != 0)",
			row: &VersionedRow{
				Data:     []any{1, "test"},
				RowID:    1,
				TxnID:    100,
				CommitTS: 12345, // Non-zero means committed
			},
			expected: true,
		},
		{
			name: "uncommitted row (CommitTS == 0)",
			row: &VersionedRow{
				Data:     []any{1, "test"},
				RowID:    1,
				TxnID:    100,
				CommitTS: 0, // Zero means not committed
			},
			expected: false,
		},
		{
			name: "row with only CommitTS set",
			row: &VersionedRow{
				CommitTS: 1,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.row.IsCommitted()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestVersionedRow_IsDeleted tests that IsDeleted returns true only when both
// DeletedBy != 0 AND DeleteTS != 0.
func TestVersionedRow_IsDeleted(t *testing.T) {
	tests := []struct {
		name     string
		row      *VersionedRow
		expected bool
	}{
		{
			name: "fully deleted (DeletedBy != 0 AND DeleteTS != 0)",
			row: &VersionedRow{
				Data:      []any{1, "test"},
				RowID:     1,
				TxnID:     100,
				CommitTS:  12345,
				DeletedBy: 200,   // Non-zero
				DeleteTS:  12346, // Non-zero
			},
			expected: true,
		},
		{
			name: "not deleted (DeletedBy == 0, DeleteTS == 0)",
			row: &VersionedRow{
				Data:      []any{1, "test"},
				RowID:     1,
				TxnID:     100,
				CommitTS:  12345,
				DeletedBy: 0,
				DeleteTS:  0,
			},
			expected: false,
		},
		{
			name: "pending delete (DeletedBy != 0, DeleteTS == 0)",
			row: &VersionedRow{
				Data:      []any{1, "test"},
				RowID:     1,
				TxnID:     100,
				CommitTS:  12345,
				DeletedBy: 200, // Non-zero
				DeleteTS:  0,   // Zero - delete not committed
			},
			expected: false,
		},
		{
			name: "invalid state (DeletedBy == 0, DeleteTS != 0)",
			row: &VersionedRow{
				Data:      []any{1, "test"},
				RowID:     1,
				TxnID:     100,
				CommitTS:  12345,
				DeletedBy: 0,     // Zero
				DeleteTS:  12346, // Non-zero - unusual state
			},
			expected: false, // DeletedBy is 0, so not deleted
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.row.IsDeleted()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestVersionedRow_IsPendingDelete tests that IsPendingDelete returns true when
// DeletedBy != 0 but DeleteTS == 0.
func TestVersionedRow_IsPendingDelete(t *testing.T) {
	tests := []struct {
		name     string
		row      *VersionedRow
		expected bool
	}{
		{
			name: "pending delete (DeletedBy != 0, DeleteTS == 0)",
			row: &VersionedRow{
				Data:      []any{1, "test"},
				RowID:     1,
				TxnID:     100,
				CommitTS:  12345,
				DeletedBy: 200, // Non-zero - marked for deletion
				DeleteTS:  0,   // Zero - delete not yet committed
			},
			expected: true,
		},
		{
			name: "not pending (DeletedBy == 0)",
			row: &VersionedRow{
				Data:      []any{1, "test"},
				RowID:     1,
				TxnID:     100,
				CommitTS:  12345,
				DeletedBy: 0,
				DeleteTS:  0,
			},
			expected: false,
		},
		{
			name: "fully deleted (not pending)",
			row: &VersionedRow{
				Data:      []any{1, "test"},
				RowID:     1,
				TxnID:     100,
				CommitTS:  12345,
				DeletedBy: 200,
				DeleteTS:  12346, // Non-zero - delete committed
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.row.IsPendingDelete()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestVersionedRow_Clone tests that Clone creates a proper deep copy.
// Modifying the clone should not affect the original.
func TestVersionedRow_Clone(t *testing.T) {
	t.Run("clone creates independent copy", func(t *testing.T) {
		original := &VersionedRow{
			Data:      []any{1, "test", 3.14},
			RowID:     42,
			TxnID:     100,
			CommitTS:  12345,
			DeletedBy: 200,
			DeleteTS:  12346,
			PrevPtr:   &VersionedRow{RowID: 41}, // Has a previous version
		}

		clone := original.Clone()

		// Verify clone has same values
		assert.Equal(t, original.RowID, clone.RowID)
		assert.Equal(t, original.TxnID, clone.TxnID)
		assert.Equal(t, original.CommitTS, clone.CommitTS)
		assert.Equal(t, original.DeletedBy, clone.DeletedBy)
		assert.Equal(t, original.DeleteTS, clone.DeleteTS)
		assert.Equal(t, original.Data, clone.Data)

		// Verify PrevPtr is NOT copied (should be nil)
		assert.Nil(t, clone.PrevPtr, "Clone should have nil PrevPtr")
		assert.NotNil(t, original.PrevPtr, "Original PrevPtr should be unchanged")
	})

	t.Run("modifying clone does not affect original data slice", func(t *testing.T) {
		original := &VersionedRow{
			Data:  []any{1, "test", 3.14},
			RowID: 42,
			TxnID: 100,
		}

		clone := original.Clone()

		// Modify the clone's data
		clone.Data[0] = 999
		clone.Data[1] = "modified"

		// Original should be unchanged
		assert.Equal(t, 1, original.Data[0])
		assert.Equal(t, "test", original.Data[1])
	})

	t.Run("modifying clone scalar fields does not affect original", func(t *testing.T) {
		original := &VersionedRow{
			Data:      []any{1},
			RowID:     42,
			TxnID:     100,
			CommitTS:  12345,
			DeletedBy: 0,
			DeleteTS:  0,
		}

		clone := original.Clone()

		// Modify all scalar fields on clone
		clone.RowID = 999
		clone.TxnID = 888
		clone.CommitTS = 777
		clone.DeletedBy = 666
		clone.DeleteTS = 555

		// Original should be unchanged
		assert.Equal(t, uint64(42), original.RowID)
		assert.Equal(t, uint64(100), original.TxnID)
		assert.Equal(t, uint64(12345), original.CommitTS)
		assert.Equal(t, uint64(0), original.DeletedBy)
		assert.Equal(t, uint64(0), original.DeleteTS)
	})

	t.Run("clone of nil returns nil", func(t *testing.T) {
		var nilRow *VersionedRow
		clone := nilRow.Clone()
		assert.Nil(t, clone)
	})

	t.Run("clone with nil Data", func(t *testing.T) {
		original := &VersionedRow{
			Data:  nil,
			RowID: 42,
			TxnID: 100,
		}

		clone := original.Clone()

		assert.Nil(t, clone.Data)
		assert.Equal(t, original.RowID, clone.RowID)
	})

	t.Run("clone with empty Data slice", func(t *testing.T) {
		original := &VersionedRow{
			Data:  []any{},
			RowID: 42,
			TxnID: 100,
		}

		clone := original.Clone()

		assert.NotNil(t, clone.Data)
		assert.Len(t, clone.Data, 0)
	})
}

// TestVersionedRow_ToVersionInfo tests conversion to VersionInfo.
func TestVersionedRow_ToVersionInfo(t *testing.T) {
	tests := []struct {
		name        string
		row         *VersionedRow
		expectedVI  VersionInfo
	}{
		{
			name: "committed row without deletion",
			row: &VersionedRow{
				Data:      []any{1, "test"},
				RowID:     42,
				TxnID:     100,
				CommitTS:  12345,
				DeletedBy: 0,
				DeleteTS:  0,
			},
			expectedVI: VersionInfo{
				CreatedTxnID: 100,
				DeletedTxnID: 0,
				Committed:    true,
			},
		},
		{
			name: "uncommitted row",
			row: &VersionedRow{
				Data:      []any{1, "test"},
				RowID:     42,
				TxnID:     100,
				CommitTS:  0, // Not committed
				DeletedBy: 0,
				DeleteTS:  0,
			},
			expectedVI: VersionInfo{
				CreatedTxnID: 100,
				DeletedTxnID: 0,
				Committed:    false,
			},
		},
		{
			name: "committed and deleted row",
			row: &VersionedRow{
				Data:      []any{1, "test"},
				RowID:     42,
				TxnID:     100,
				CommitTS:  12345,
				DeletedBy: 200,
				DeleteTS:  12346,
			},
			expectedVI: VersionInfo{
				CreatedTxnID: 100,
				DeletedTxnID: 200,
				Committed:    true,
			},
		},
		{
			name: "row with pending delete",
			row: &VersionedRow{
				Data:      []any{1, "test"},
				RowID:     42,
				TxnID:     100,
				CommitTS:  12345,
				DeletedBy: 200,
				DeleteTS:  0, // Delete not committed
			},
			expectedVI: VersionInfo{
				CreatedTxnID: 100,
				DeletedTxnID: 200, // DeletedTxnID is set even if delete not committed
				Committed:    true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.row.ToVersionInfo()

			assert.Equal(t, tt.expectedVI.CreatedTxnID, result.CreatedTxnID)
			assert.Equal(t, tt.expectedVI.DeletedTxnID, result.DeletedTxnID)
			assert.Equal(t, tt.expectedVI.Committed, result.Committed)
			// CreatedTime and DeletedTime should be zero values
			assert.True(t, result.CreatedTime.IsZero())
			assert.True(t, result.DeletedTime.IsZero())
		})
	}
}

// ============================================================================
// VersionChain Tests
// ============================================================================

// TestVersionChain_NewVersionChain tests the constructor creates an empty chain
// with the correct RowID.
func TestVersionChain_NewVersionChain(t *testing.T) {
	tests := []struct {
		name   string
		rowID  uint64
	}{
		{"row ID 0", 0},
		{"row ID 1", 1},
		{"row ID large", 999999999},
		{"row ID max uint64", ^uint64(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain := NewVersionChain(tt.rowID)

			assert.NotNil(t, chain)
			assert.Equal(t, tt.rowID, chain.RowID)
			assert.Nil(t, chain.Head, "New chain should have nil Head")
			assert.True(t, chain.IsEmpty(), "New chain should be empty")
			assert.Equal(t, 0, chain.Len(), "New chain should have length 0")
		})
	}
}

// TestVersionChain_AddVersion tests adding versions to the chain and verifies
// PrevPtr linking.
func TestVersionChain_AddVersion(t *testing.T) {
	t.Run("add single version", func(t *testing.T) {
		chain := NewVersionChain(1)

		v1 := &VersionedRow{
			Data:  []any{"first"},
			RowID: 1,
			TxnID: 100,
		}

		chain.AddVersion(v1)

		assert.Equal(t, 1, chain.Len())
		assert.Equal(t, v1, chain.GetHead())
		assert.Nil(t, v1.PrevPtr, "First version should have nil PrevPtr")
	})

	t.Run("add multiple versions creates proper chain", func(t *testing.T) {
		chain := NewVersionChain(1)

		v1 := &VersionedRow{
			Data:  []any{"first"},
			RowID: 1,
			TxnID: 100,
		}
		v2 := &VersionedRow{
			Data:  []any{"second"},
			RowID: 1,
			TxnID: 101,
		}
		v3 := &VersionedRow{
			Data:  []any{"third"},
			RowID: 1,
			TxnID: 102,
		}

		chain.AddVersion(v1)
		chain.AddVersion(v2)
		chain.AddVersion(v3)

		assert.Equal(t, 3, chain.Len())

		// Head should be the last added (newest)
		assert.Equal(t, v3, chain.GetHead())

		// Check PrevPtr chain: v3 -> v2 -> v1 -> nil
		assert.Equal(t, v2, v3.PrevPtr)
		assert.Equal(t, v1, v2.PrevPtr)
		assert.Nil(t, v1.PrevPtr)
	})

	t.Run("add nil version does nothing", func(t *testing.T) {
		chain := NewVersionChain(1)

		v1 := &VersionedRow{
			Data:  []any{"first"},
			RowID: 1,
			TxnID: 100,
		}

		chain.AddVersion(v1)
		chain.AddVersion(nil)

		assert.Equal(t, 1, chain.Len())
		assert.Equal(t, v1, chain.GetHead())
	})
}

// TestVersionChain_GetHead tests that GetHead returns the latest version.
func TestVersionChain_GetHead(t *testing.T) {
	t.Run("empty chain returns nil", func(t *testing.T) {
		chain := NewVersionChain(1)
		assert.Nil(t, chain.GetHead())
	})

	t.Run("returns newest version", func(t *testing.T) {
		chain := NewVersionChain(1)

		v1 := &VersionedRow{Data: []any{"first"}, RowID: 1, TxnID: 100}
		v2 := &VersionedRow{Data: []any{"second"}, RowID: 1, TxnID: 101}
		v3 := &VersionedRow{Data: []any{"third"}, RowID: 1, TxnID: 102}

		chain.AddVersion(v1)
		assert.Equal(t, v1, chain.GetHead())

		chain.AddVersion(v2)
		assert.Equal(t, v2, chain.GetHead())

		chain.AddVersion(v3)
		assert.Equal(t, v3, chain.GetHead())
	})
}

// TestVersionChain_Len tests that Len returns the correct count.
func TestVersionChain_Len(t *testing.T) {
	tests := []struct {
		name     string
		numItems int
	}{
		{"empty chain", 0},
		{"one item", 1},
		{"five items", 5},
		{"ten items", 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain := NewVersionChain(1)

			for i := 0; i < tt.numItems; i++ {
				chain.AddVersion(&VersionedRow{
					Data:  []any{i},
					RowID: 1,
					TxnID: uint64(100 + i),
				})
			}

			assert.Equal(t, tt.numItems, chain.Len())
		})
	}
}

// TestVersionChain_IsEmpty tests that IsEmpty returns the correct value.
func TestVersionChain_IsEmpty(t *testing.T) {
	t.Run("new chain is empty", func(t *testing.T) {
		chain := NewVersionChain(1)
		assert.True(t, chain.IsEmpty())
	})

	t.Run("chain with versions is not empty", func(t *testing.T) {
		chain := NewVersionChain(1)
		chain.AddVersion(&VersionedRow{Data: []any{"test"}, RowID: 1, TxnID: 100})
		assert.False(t, chain.IsEmpty())
	})

	t.Run("empty after adding nil", func(t *testing.T) {
		chain := NewVersionChain(1)
		chain.AddVersion(nil)
		assert.True(t, chain.IsEmpty())
	})
}

// TestVersionChain_GetAllVersions tests getting all versions in order.
func TestVersionChain_GetAllVersions(t *testing.T) {
	t.Run("empty chain returns nil slice", func(t *testing.T) {
		chain := NewVersionChain(1)
		versions := chain.GetAllVersions()
		// Implementation returns nil slice for empty chain
		assert.Nil(t, versions)
		assert.Len(t, versions, 0) // Len(nil) == 0
	})

	t.Run("returns versions from newest to oldest", func(t *testing.T) {
		chain := NewVersionChain(1)

		v1 := &VersionedRow{Data: []any{"first"}, RowID: 1, TxnID: 100}
		v2 := &VersionedRow{Data: []any{"second"}, RowID: 1, TxnID: 101}
		v3 := &VersionedRow{Data: []any{"third"}, RowID: 1, TxnID: 102}

		chain.AddVersion(v1)
		chain.AddVersion(v2)
		chain.AddVersion(v3)

		versions := chain.GetAllVersions()

		assert.Len(t, versions, 3)
		// Order should be newest to oldest: v3, v2, v1
		assert.Equal(t, v3, versions[0], "First should be newest (v3)")
		assert.Equal(t, v2, versions[1], "Second should be v2")
		assert.Equal(t, v1, versions[2], "Last should be oldest (v1)")
	})

	t.Run("returns actual pointers not copies", func(t *testing.T) {
		chain := NewVersionChain(1)

		v1 := &VersionedRow{Data: []any{"first"}, RowID: 1, TxnID: 100}
		chain.AddVersion(v1)

		versions := chain.GetAllVersions()

		// Should be the same pointer, not a copy
		assert.Same(t, v1, versions[0])
	})
}

// mockVisibilityChecker is a mock implementation for testing FindVisibleVersion.
type mockVisibilityChecker struct {
	visibleTxnIDs map[uint64]bool
}

func newMockVisibilityChecker(visibleTxnIDs ...uint64) *mockVisibilityChecker {
	m := &mockVisibilityChecker{
		visibleTxnIDs: make(map[uint64]bool),
	}
	for _, id := range visibleTxnIDs {
		m.visibleTxnIDs[id] = true
	}
	return m
}

func (m *mockVisibilityChecker) IsVisible(version VersionInfo, _ TransactionContext) bool {
	return m.visibleTxnIDs[version.CreatedTxnID]
}

// TestVersionChain_FindVisibleVersion tests finding visible version with a mock
// visibility checker.
func TestVersionChain_FindVisibleVersion(t *testing.T) {
	t.Run("empty chain returns nil", func(t *testing.T) {
		chain := NewVersionChain(1)
		checker := newMockVisibilityChecker(100)
		txnCtx := newMockTransactionContext(50, parser.IsolationLevelRepeatableRead)

		result := chain.FindVisibleVersion(checker, txnCtx)
		assert.Nil(t, result)
	})

	t.Run("finds first visible version from head", func(t *testing.T) {
		chain := NewVersionChain(1)

		v1 := &VersionedRow{Data: []any{"first"}, RowID: 1, TxnID: 100, CommitTS: 1}
		v2 := &VersionedRow{Data: []any{"second"}, RowID: 1, TxnID: 101, CommitTS: 2}
		v3 := &VersionedRow{Data: []any{"third"}, RowID: 1, TxnID: 102, CommitTS: 3}

		chain.AddVersion(v1)
		chain.AddVersion(v2)
		chain.AddVersion(v3)

		// Make only v2 visible
		checker := newMockVisibilityChecker(101)
		txnCtx := newMockTransactionContext(50, parser.IsolationLevelRepeatableRead)

		result := chain.FindVisibleVersion(checker, txnCtx)

		assert.NotNil(t, result)
		assert.Equal(t, v2, result)
	})

	t.Run("returns head if head is visible", func(t *testing.T) {
		chain := NewVersionChain(1)

		v1 := &VersionedRow{Data: []any{"first"}, RowID: 1, TxnID: 100, CommitTS: 1}
		v2 := &VersionedRow{Data: []any{"second"}, RowID: 1, TxnID: 101, CommitTS: 2}

		chain.AddVersion(v1)
		chain.AddVersion(v2)

		// Make head (v2) visible
		checker := newMockVisibilityChecker(101)
		txnCtx := newMockTransactionContext(50, parser.IsolationLevelRepeatableRead)

		result := chain.FindVisibleVersion(checker, txnCtx)

		assert.Equal(t, v2, result)
	})

	t.Run("returns nil if no version is visible", func(t *testing.T) {
		chain := NewVersionChain(1)

		v1 := &VersionedRow{Data: []any{"first"}, RowID: 1, TxnID: 100, CommitTS: 1}
		v2 := &VersionedRow{Data: []any{"second"}, RowID: 1, TxnID: 101, CommitTS: 2}

		chain.AddVersion(v1)
		chain.AddVersion(v2)

		// Make no versions visible
		checker := newMockVisibilityChecker(999) // Non-existent TxnID
		txnCtx := newMockTransactionContext(50, parser.IsolationLevelRepeatableRead)

		result := chain.FindVisibleVersion(checker, txnCtx)

		assert.Nil(t, result)
	})

	t.Run("finds oldest visible if only oldest is visible", func(t *testing.T) {
		chain := NewVersionChain(1)

		v1 := &VersionedRow{Data: []any{"first"}, RowID: 1, TxnID: 100, CommitTS: 1}
		v2 := &VersionedRow{Data: []any{"second"}, RowID: 1, TxnID: 101, CommitTS: 2}
		v3 := &VersionedRow{Data: []any{"third"}, RowID: 1, TxnID: 102, CommitTS: 3}

		chain.AddVersion(v1)
		chain.AddVersion(v2)
		chain.AddVersion(v3)

		// Make only v1 (oldest) visible
		checker := newMockVisibilityChecker(100)
		txnCtx := newMockTransactionContext(50, parser.IsolationLevelRepeatableRead)

		result := chain.FindVisibleVersion(checker, txnCtx)

		assert.Equal(t, v1, result)
	})

	t.Run("multiple visible returns newest visible", func(t *testing.T) {
		chain := NewVersionChain(1)

		v1 := &VersionedRow{Data: []any{"first"}, RowID: 1, TxnID: 100, CommitTS: 1}
		v2 := &VersionedRow{Data: []any{"second"}, RowID: 1, TxnID: 101, CommitTS: 2}
		v3 := &VersionedRow{Data: []any{"third"}, RowID: 1, TxnID: 102, CommitTS: 3}

		chain.AddVersion(v1)
		chain.AddVersion(v2)
		chain.AddVersion(v3)

		// Make v1 and v2 visible (but not v3)
		checker := newMockVisibilityChecker(100, 101)
		txnCtx := newMockTransactionContext(50, parser.IsolationLevelRepeatableRead)

		result := chain.FindVisibleVersion(checker, txnCtx)

		// Should return v2 as it's the newest visible
		assert.Equal(t, v2, result)
	})
}

// TestVersionChain_ConcurrentAccess tests that concurrent access is safe using goroutines.
func TestVersionChain_ConcurrentAccess(t *testing.T) {
	t.Run("concurrent reads are safe", func(t *testing.T) {
		chain := NewVersionChain(1)

		// Pre-populate chain
		for i := 0; i < 100; i++ {
			chain.AddVersion(&VersionedRow{
				Data:  []any{i},
				RowID: 1,
				TxnID: uint64(i),
			})
		}

		var wg sync.WaitGroup
		numGoroutines := 50
		numReads := 100

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < numReads; i++ {
					_ = chain.GetHead()
					_ = chain.Len()
					_ = chain.IsEmpty()
					_ = chain.GetAllVersions()
				}
			}()
		}

		wg.Wait()
		// If we reach here without deadlock or panic, the test passes
	})

	t.Run("concurrent writes are safe", func(t *testing.T) {
		chain := NewVersionChain(1)

		var wg sync.WaitGroup
		numGoroutines := 50
		numWrites := 10

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < numWrites; i++ {
					chain.AddVersion(&VersionedRow{
						Data:  []any{goroutineID, i},
						RowID: 1,
						TxnID: uint64(goroutineID*1000 + i),
					})
				}
			}(g)
		}

		wg.Wait()

		// Verify final count
		expectedCount := numGoroutines * numWrites
		assert.Equal(t, expectedCount, chain.Len())
	})

	t.Run("concurrent reads and writes are safe", func(t *testing.T) {
		chain := NewVersionChain(1)

		// Pre-populate with some versions
		for i := 0; i < 10; i++ {
			chain.AddVersion(&VersionedRow{
				Data:  []any{i},
				RowID: 1,
				TxnID: uint64(i),
			})
		}

		var wg sync.WaitGroup

		// Start readers
		numReaders := 20
		numReads := 50
		for g := 0; g < numReaders; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < numReads; i++ {
					_ = chain.GetHead()
					_ = chain.Len()
					_ = chain.GetAllVersions()
				}
			}()
		}

		// Start writers
		numWriters := 10
		numWrites := 20
		for g := 0; g < numWriters; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < numWrites; i++ {
					chain.AddVersion(&VersionedRow{
						Data:  []any{goroutineID, i},
						RowID: 1,
						TxnID: uint64(1000 + goroutineID*100 + i),
					})
				}
			}(g)
		}

		wg.Wait()
		// If we reach here without deadlock or panic, the test passes
	})

	t.Run("concurrent FindVisibleVersion is safe", func(t *testing.T) {
		chain := NewVersionChain(1)

		// Pre-populate chain
		for i := 0; i < 50; i++ {
			chain.AddVersion(&VersionedRow{
				Data:     []any{i},
				RowID:    1,
				TxnID:    uint64(i),
				CommitTS: uint64(i + 1),
			})
		}

		var wg sync.WaitGroup
		numGoroutines := 30
		numFinds := 50

		checker := newMockVisibilityChecker(25, 30, 35) // Some visible TxnIDs
		txnCtx := newMockTransactionContext(100, parser.IsolationLevelRepeatableRead)

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < numFinds; i++ {
					_ = chain.FindVisibleVersion(checker, txnCtx)
				}
			}()
		}

		wg.Wait()
		// If we reach here without deadlock or panic, the test passes
	})

	t.Run("Lock and Unlock work correctly", func(t *testing.T) {
		chain := NewVersionChain(1)

		var wg sync.WaitGroup
		numGoroutines := 10
		counter := 0

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				chain.Lock()
				defer chain.Unlock()
				// Critical section
				counter++
				// Small work to increase chance of race if locking is broken
				temp := counter
				for i := 0; i < 100; i++ {
					temp++
				}
				_ = temp
			}()
		}

		wg.Wait()
		assert.Equal(t, numGoroutines, counter)
	})

	t.Run("RLock allows concurrent reads", func(t *testing.T) {
		chain := NewVersionChain(1)

		// Add some data
		chain.AddVersion(&VersionedRow{Data: []any{"test"}, RowID: 1, TxnID: 100})

		var wg sync.WaitGroup
		numGoroutines := 50

		startSignal := make(chan struct{})

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startSignal // Wait for start signal
				chain.RLock()
				defer chain.RUnlock()
				// All goroutines should be able to read concurrently
				_ = chain.Head
			}()
		}

		close(startSignal) // Start all goroutines
		wg.Wait()

		// If we reach here without deadlock or panic, RLock allows concurrent reads
		// The test passes if all goroutines complete successfully
	})
}

// TestVersionChain_LockMethods tests the Lock/Unlock and RLock/RUnlock methods.
func TestVersionChain_LockMethods(t *testing.T) {
	t.Run("Lock blocks other locks", func(t *testing.T) {
		chain := NewVersionChain(1)

		chain.Lock()

		lockAcquired := make(chan bool, 1)

		go func() {
			chain.Lock()
			lockAcquired <- true
			chain.Unlock()
		}()

		// Give the goroutine time to try to acquire the lock
		time.Sleep(50 * time.Millisecond)

		select {
		case <-lockAcquired:
			t.Fatal("Second Lock should have blocked")
		default:
			// Expected - the second lock is blocked
		}

		chain.Unlock()

		// Now the second lock should be acquired
		select {
		case <-lockAcquired:
			// Expected
		case <-time.After(time.Second):
			t.Fatal("Second Lock should have been acquired after Unlock")
		}
	})

	t.Run("RLock allows multiple readers", func(t *testing.T) {
		chain := NewVersionChain(1)

		chain.RLock()
		chain.RLock() // Should not block

		// Both read locks acquired
		chain.RUnlock()
		chain.RUnlock()
	})

	t.Run("Lock blocks RLock", func(t *testing.T) {
		chain := NewVersionChain(1)

		chain.Lock()

		lockAcquired := make(chan bool, 1)

		go func() {
			chain.RLock()
			lockAcquired <- true
			chain.RUnlock()
		}()

		// Give the goroutine time to try to acquire the lock
		time.Sleep(50 * time.Millisecond)

		select {
		case <-lockAcquired:
			t.Fatal("RLock should have blocked when Lock is held")
		default:
			// Expected
		}

		chain.Unlock()

		select {
		case <-lockAcquired:
			// Expected
		case <-time.After(time.Second):
			t.Fatal("RLock should have been acquired after Unlock")
		}
	})
}
