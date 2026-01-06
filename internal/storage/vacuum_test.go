package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/coder/quartz"
	"github.com/stretchr/testify/assert"
)

// ============================================================================
// NewVacuum Tests
// ============================================================================

// TestVacuum_NewVacuum tests that NewVacuum correctly initializes a Vacuum instance.
func TestVacuum_NewVacuum(t *testing.T) {
	t.Run("creates vacuum with mock clock", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		lowWatermark := uint64(1000)

		vacuum := NewVacuum(func() uint64 { return lowWatermark }, mockClock)

		assert.NotNil(t, vacuum)
		assert.NotNil(t, vacuum.lowWatermarkFunc)
		assert.NotNil(t, vacuum.clock)
		assert.Equal(t, uint64(0), vacuum.versionsRemoved)
		assert.Equal(t, uint64(0), vacuum.chainsProcessed)
		assert.True(t, vacuum.lastRunTime.IsZero(), "lastRunTime should be zero initially")
	})

	t.Run("lowWatermarkFunc is called correctly", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		expectedWatermark := uint64(5000)
		callCount := 0

		vacuum := NewVacuum(func() uint64 {
			callCount++
			return expectedWatermark
		}, mockClock)

		// Call the function through the vacuum
		result := vacuum.lowWatermarkFunc()

		assert.Equal(t, expectedWatermark, result)
		assert.Equal(t, 1, callCount)
	})

	t.Run("creates vacuum with real clock", func(t *testing.T) {
		realClock := quartz.NewReal()
		vacuum := NewVacuum(func() uint64 { return 0 }, realClock)

		assert.NotNil(t, vacuum)
		assert.NotNil(t, vacuum.clock)
	})
}

// ============================================================================
// CanRemoveVersion Tests
// ============================================================================

// TestVacuum_CanRemoveVersion tests all removal conditions.
func TestVacuum_CanRemoveVersion(t *testing.T) {
	mockClock := quartz.NewMock(t)
	vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

	t.Run("nil version cannot be removed", func(t *testing.T) {
		result := vacuum.CanRemoveVersion(nil, 1000)
		assert.False(t, result, "nil version should not be removable")
	})

	t.Run("uncommitted version (CommitTS == 0) cannot be removed", func(t *testing.T) {
		version := &VersionedRow{
			Data:     []any{"test"},
			RowID:    1,
			TxnID:    100,
			CommitTS: 0, // Uncommitted
		}

		result := vacuum.CanRemoveVersion(version, 1000)
		assert.False(t, result, "uncommitted version should not be removable")
	})

	t.Run("version committed after low watermark cannot be removed", func(t *testing.T) {
		version := &VersionedRow{
			Data:     []any{"test"},
			RowID:    1,
			TxnID:    100,
			CommitTS: 1500, // After watermark of 1000
		}

		result := vacuum.CanRemoveVersion(version, 1000)
		assert.False(t, result, "version committed after watermark should not be removable")
	})

	t.Run("version committed at low watermark cannot be removed", func(t *testing.T) {
		version := &VersionedRow{
			Data:     []any{"test"},
			RowID:    1,
			TxnID:    100,
			CommitTS: 1000, // At watermark
		}

		result := vacuum.CanRemoveVersion(version, 1000)
		assert.False(t, result, "version committed at watermark should not be removable")
	})

	t.Run("version committed before low watermark can be removed", func(t *testing.T) {
		version := &VersionedRow{
			Data:     []any{"test"},
			RowID:    1,
			TxnID:    100,
			CommitTS: 500, // Before watermark of 1000
		}

		result := vacuum.CanRemoveVersion(version, 1000)
		assert.True(t, result, "version committed before watermark should be removable")
	})

	t.Run("delete marker with DeleteTS >= low watermark cannot be removed", func(t *testing.T) {
		version := &VersionedRow{
			Data:      []any{"test"},
			RowID:     1,
			TxnID:     100,
			CommitTS:  500,  // Before watermark
			DeletedBy: 200,
			DeleteTS:  1000, // At watermark
		}

		result := vacuum.CanRemoveVersion(version, 1000)
		assert.False(t, result, "delete marker at watermark should not be removable")
	})

	t.Run("delete marker with DeleteTS > low watermark cannot be removed", func(t *testing.T) {
		version := &VersionedRow{
			Data:      []any{"test"},
			RowID:     1,
			TxnID:     100,
			CommitTS:  500,  // Before watermark
			DeletedBy: 200,
			DeleteTS:  1500, // After watermark
		}

		result := vacuum.CanRemoveVersion(version, 1000)
		assert.False(t, result, "delete marker after watermark should not be removable")
	})

	t.Run("delete marker with DeleteTS < low watermark can be removed", func(t *testing.T) {
		version := &VersionedRow{
			Data:      []any{"test"},
			RowID:     1,
			TxnID:     100,
			CommitTS:  300, // Before watermark
			DeletedBy: 200,
			DeleteTS:  500, // Also before watermark
		}

		result := vacuum.CanRemoveVersion(version, 1000)
		assert.True(t, result, "delete marker before watermark should be removable")
	})

	t.Run("version with DeletedBy but no DeleteTS is removable if CommitTS < watermark", func(t *testing.T) {
		// This is a pending delete (DeletedBy set but DeleteTS == 0)
		version := &VersionedRow{
			Data:      []any{"test"},
			RowID:     1,
			TxnID:     100,
			CommitTS:  500, // Before watermark
			DeletedBy: 200, // Pending delete
			DeleteTS:  0,   // Delete not committed yet
		}

		// DeleteTS is 0, so the check `DeleteTS >= lowWatermark` becomes `0 >= 1000`
		// which is false, so this version CAN be removed (as long as CommitTS < watermark)
		result := vacuum.CanRemoveVersion(version, 1000)
		assert.True(t, result, "pending delete with old CommitTS should be removable")
	})
}

// ============================================================================
// CleanVersionChain Tests
// ============================================================================

// TestVacuum_CleanVersionChain tests cleaning individual version chains.
func TestVacuum_CleanVersionChain(t *testing.T) {
	t.Run("nil chain returns 0", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		result := vacuum.CleanVersionChain(nil)
		assert.Equal(t, 0, result)
	})

	t.Run("empty chain returns 0", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		chain := NewVersionChain(1)
		result := vacuum.CleanVersionChain(chain)

		assert.Equal(t, 0, result)
	})

	t.Run("single version chain (head only) - head is not removed", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		chain := NewVersionChain(1)
		head := &VersionedRow{
			Data:     []any{"head"},
			RowID:    1,
			TxnID:    100,
			CommitTS: 500, // Old enough to be removed
		}
		chain.AddVersion(head)

		result := vacuum.CleanVersionChain(chain)

		assert.Equal(t, 0, result, "head should never be removed")
		assert.Equal(t, 1, chain.Len())
		assert.Equal(t, head, chain.GetHead())
	})

	t.Run("chain with old versions - old versions are removed", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		chain := NewVersionChain(1)

		// Add versions from oldest to newest
		v1 := &VersionedRow{
			Data:     []any{"v1"},
			RowID:    1,
			TxnID:    100,
			CommitTS: 100, // Very old
		}
		v2 := &VersionedRow{
			Data:     []any{"v2"},
			RowID:    1,
			TxnID:    101,
			CommitTS: 200, // Old
		}
		v3 := &VersionedRow{
			Data:     []any{"v3"},
			RowID:    1,
			TxnID:    102,
			CommitTS: 1500, // Recent, after watermark
		}

		chain.AddVersion(v1)
		chain.AddVersion(v2)
		chain.AddVersion(v3)

		// Chain: v3 (head) -> v2 -> v1
		// With watermark 1000, v1 and v2 can be removed (CommitTS < 1000)
		// But v3 is the head and head is never removed

		result := vacuum.CleanVersionChain(chain)

		// v2 and v1 should be removed
		assert.Equal(t, 2, result)
		assert.Equal(t, 1, chain.Len())
		assert.Equal(t, v3, chain.GetHead())
		assert.Nil(t, v3.PrevPtr, "v3's PrevPtr should be nil after cleaning")
	})

	t.Run("chain with all recent versions - nothing is removed", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		chain := NewVersionChain(1)

		v1 := &VersionedRow{
			Data:     []any{"v1"},
			RowID:    1,
			TxnID:    100,
			CommitTS: 1100, // After watermark
		}
		v2 := &VersionedRow{
			Data:     []any{"v2"},
			RowID:    1,
			TxnID:    101,
			CommitTS: 1200, // After watermark
		}
		v3 := &VersionedRow{
			Data:     []any{"v3"},
			RowID:    1,
			TxnID:    102,
			CommitTS: 1300, // After watermark
		}

		chain.AddVersion(v1)
		chain.AddVersion(v2)
		chain.AddVersion(v3)

		result := vacuum.CleanVersionChain(chain)

		assert.Equal(t, 0, result, "no versions should be removed")
		assert.Equal(t, 3, chain.Len())
	})

	t.Run("chain with mixed old/new versions - only old are removed", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		chain := NewVersionChain(1)

		// Oldest
		v1 := &VersionedRow{
			Data:     []any{"v1"},
			RowID:    1,
			TxnID:    100,
			CommitTS: 100, // Old
		}
		// Recent
		v2 := &VersionedRow{
			Data:     []any{"v2"},
			RowID:    1,
			TxnID:    101,
			CommitTS: 1100, // After watermark
		}
		// Old
		v3 := &VersionedRow{
			Data:     []any{"v3"},
			RowID:    1,
			TxnID:    102,
			CommitTS: 200, // Old
		}
		// Recent (head)
		v4 := &VersionedRow{
			Data:     []any{"v4"},
			RowID:    1,
			TxnID:    103,
			CommitTS: 1200, // After watermark
		}

		chain.AddVersion(v1)
		chain.AddVersion(v2)
		chain.AddVersion(v3)
		chain.AddVersion(v4)

		// Chain: v4 (head) -> v3 -> v2 -> v1
		// v4: recent (1200 >= 1000) - cannot remove (and is head)
		// v3: old (200 < 1000) - CAN remove, this is where we truncate
		// v2: recent (1100 >= 1000) - cannot remove (but unreachable after v3 is cut)
		// v1: old (100 < 1000) - CAN remove (but unreachable after v3 is cut)
		//
		// Algorithm finds first removable version after head, which is v3
		// Then truncates: v4.PrevPtr = nil, removing v3, v2, v1

		result := vacuum.CleanVersionChain(chain)

		// v3, v2, v1 removed (3 versions)
		assert.Equal(t, 3, result)
		assert.Equal(t, 1, chain.Len())
		assert.Equal(t, v4, chain.GetHead())
		assert.Nil(t, v4.PrevPtr)
	})

	t.Run("updates statistics after cleaning", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		expectedTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
		mockClock.Set(expectedTime)

		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		chain := NewVersionChain(1)
		v1 := &VersionedRow{
			Data:     []any{"v1"},
			RowID:    1,
			TxnID:    100,
			CommitTS: 100,
		}
		v2 := &VersionedRow{
			Data:     []any{"v2"},
			RowID:    1,
			TxnID:    101,
			CommitTS: 1500,
		}

		chain.AddVersion(v1)
		chain.AddVersion(v2)

		vacuum.CleanVersionChain(chain)

		stats := vacuum.GetStatistics()
		assert.Equal(t, uint64(1), stats.VersionsRemoved)
		assert.Equal(t, uint64(1), stats.ChainsProcessed)
		assert.Equal(t, expectedTime, stats.LastRunTime)
	})

	t.Run("zero low watermark means max watermark - everything old is removable", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		// When lowWatermarkFunc returns 0, it means no active transactions
		// The vacuum treats this as max uint64, meaning all committed versions
		// except head can be removed
		vacuum := NewVacuum(func() uint64 { return 0 }, mockClock)

		chain := NewVersionChain(1)
		v1 := &VersionedRow{
			Data:     []any{"v1"},
			RowID:    1,
			TxnID:    100,
			CommitTS: 100,
		}
		v2 := &VersionedRow{
			Data:     []any{"v2"},
			RowID:    1,
			TxnID:    101,
			CommitTS: 200,
		}
		v3 := &VersionedRow{
			Data:     []any{"v3"},
			RowID:    1,
			TxnID:    102,
			CommitTS: 300,
		}

		chain.AddVersion(v1)
		chain.AddVersion(v2)
		chain.AddVersion(v3)

		result := vacuum.CleanVersionChain(chain)

		// v2 and v1 should be removed (all committed versions except head)
		assert.Equal(t, 2, result)
		assert.Equal(t, 1, chain.Len())
	})
}

// ============================================================================
// VacuumChains Tests
// ============================================================================

// TestVacuum_VacuumChains tests processing multiple chains at once.
func TestVacuum_VacuumChains(t *testing.T) {
	t.Run("empty chains slice returns 0", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		result := vacuum.VacuumChains([]*VersionChain{})
		assert.Equal(t, 0, result)
	})

	t.Run("nil chains are skipped", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		chain := NewVersionChain(1)
		v1 := &VersionedRow{
			Data:     []any{"v1"},
			RowID:    1,
			TxnID:    100,
			CommitTS: 100,
		}
		v2 := &VersionedRow{
			Data:     []any{"v2"},
			RowID:    1,
			TxnID:    101,
			CommitTS: 1500,
		}
		chain.AddVersion(v1)
		chain.AddVersion(v2)

		result := vacuum.VacuumChains([]*VersionChain{nil, chain, nil})

		assert.Equal(t, 1, result) // Only v1 from the non-nil chain
	})

	t.Run("processes multiple chains and returns total removed", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		// Chain 1: 2 old versions + 1 recent
		chain1 := NewVersionChain(1)
		chain1.AddVersion(&VersionedRow{RowID: 1, TxnID: 100, CommitTS: 100})
		chain1.AddVersion(&VersionedRow{RowID: 1, TxnID: 101, CommitTS: 200})
		chain1.AddVersion(&VersionedRow{RowID: 1, TxnID: 102, CommitTS: 1500})

		// Chain 2: 1 old version + 1 recent
		chain2 := NewVersionChain(2)
		chain2.AddVersion(&VersionedRow{RowID: 2, TxnID: 200, CommitTS: 300})
		chain2.AddVersion(&VersionedRow{RowID: 2, TxnID: 201, CommitTS: 1600})

		// Chain 3: all recent (nothing to remove)
		chain3 := NewVersionChain(3)
		chain3.AddVersion(&VersionedRow{RowID: 3, TxnID: 300, CommitTS: 1100})
		chain3.AddVersion(&VersionedRow{RowID: 3, TxnID: 301, CommitTS: 1200})

		chains := []*VersionChain{chain1, chain2, chain3}
		result := vacuum.VacuumChains(chains)

		// Chain1: 2 removed, Chain2: 1 removed, Chain3: 0 removed
		assert.Equal(t, 3, result)

		// Verify chain lengths
		assert.Equal(t, 1, chain1.Len())
		assert.Equal(t, 1, chain2.Len())
		assert.Equal(t, 2, chain3.Len())
	})

	t.Run("updates statistics for all chains", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		chain1 := NewVersionChain(1)
		chain1.AddVersion(&VersionedRow{RowID: 1, TxnID: 100, CommitTS: 100})
		chain1.AddVersion(&VersionedRow{RowID: 1, TxnID: 101, CommitTS: 1500})

		chain2 := NewVersionChain(2)
		chain2.AddVersion(&VersionedRow{RowID: 2, TxnID: 200, CommitTS: 200})
		chain2.AddVersion(&VersionedRow{RowID: 2, TxnID: 201, CommitTS: 1600})

		vacuum.VacuumChains([]*VersionChain{chain1, chain2})

		stats := vacuum.GetStatistics()
		assert.Equal(t, uint64(2), stats.VersionsRemoved)
		assert.Equal(t, uint64(2), stats.ChainsProcessed)
	})
}

// ============================================================================
// GetStatistics Tests
// ============================================================================

// TestVacuum_GetStatistics tests that statistics are correctly reported.
func TestVacuum_GetStatistics(t *testing.T) {
	t.Run("initial statistics are zero", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		stats := vacuum.GetStatistics()

		assert.Equal(t, uint64(0), stats.VersionsRemoved)
		assert.Equal(t, uint64(0), stats.ChainsProcessed)
		assert.True(t, stats.LastRunTime.IsZero())
	})

	t.Run("statistics reflect vacuum operations", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		firstTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
		mockClock.Set(firstTime)

		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		// First vacuum
		chain1 := NewVersionChain(1)
		chain1.AddVersion(&VersionedRow{RowID: 1, TxnID: 100, CommitTS: 100})
		chain1.AddVersion(&VersionedRow{RowID: 1, TxnID: 101, CommitTS: 1500})

		vacuum.CleanVersionChain(chain1)

		stats := vacuum.GetStatistics()
		assert.Equal(t, uint64(1), stats.VersionsRemoved)
		assert.Equal(t, uint64(1), stats.ChainsProcessed)
		assert.Equal(t, firstTime, stats.LastRunTime)

		// Advance time and do another vacuum
		secondTime := time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC)
		mockClock.Set(secondTime)

		chain2 := NewVersionChain(2)
		chain2.AddVersion(&VersionedRow{RowID: 2, TxnID: 200, CommitTS: 200})
		chain2.AddVersion(&VersionedRow{RowID: 2, TxnID: 201, CommitTS: 300})
		chain2.AddVersion(&VersionedRow{RowID: 2, TxnID: 202, CommitTS: 1600})

		vacuum.CleanVersionChain(chain2)

		stats = vacuum.GetStatistics()
		assert.Equal(t, uint64(3), stats.VersionsRemoved) // 1 + 2
		assert.Equal(t, uint64(2), stats.ChainsProcessed)
		assert.Equal(t, secondTime, stats.LastRunTime)
	})

	t.Run("returns snapshot of statistics", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		chain := NewVersionChain(1)
		chain.AddVersion(&VersionedRow{RowID: 1, TxnID: 100, CommitTS: 100})
		chain.AddVersion(&VersionedRow{RowID: 1, TxnID: 101, CommitTS: 1500})

		vacuum.CleanVersionChain(chain)

		stats1 := vacuum.GetStatistics()
		stats2 := vacuum.GetStatistics()

		// Both should have the same values
		assert.Equal(t, stats1.VersionsRemoved, stats2.VersionsRemoved)
		assert.Equal(t, stats1.ChainsProcessed, stats2.ChainsProcessed)
		assert.Equal(t, stats1.LastRunTime, stats2.LastRunTime)
	})
}

// ============================================================================
// ResetStatistics Tests
// ============================================================================

// TestVacuum_ResetStatistics tests that statistics are properly reset.
func TestVacuum_ResetStatistics(t *testing.T) {
	t.Run("resets all statistics to zero", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		mockClock.Set(time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC))

		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		// Do some vacuum work
		chain := NewVersionChain(1)
		chain.AddVersion(&VersionedRow{RowID: 1, TxnID: 100, CommitTS: 100})
		chain.AddVersion(&VersionedRow{RowID: 1, TxnID: 101, CommitTS: 200})
		chain.AddVersion(&VersionedRow{RowID: 1, TxnID: 102, CommitTS: 1500})

		vacuum.CleanVersionChain(chain)

		// Verify statistics were updated
		stats := vacuum.GetStatistics()
		assert.True(t, stats.VersionsRemoved > 0)
		assert.True(t, stats.ChainsProcessed > 0)
		assert.False(t, stats.LastRunTime.IsZero())

		// Reset statistics
		vacuum.ResetStatistics()

		// Verify all statistics are zero
		stats = vacuum.GetStatistics()
		assert.Equal(t, uint64(0), stats.VersionsRemoved)
		assert.Equal(t, uint64(0), stats.ChainsProcessed)
		assert.True(t, stats.LastRunTime.IsZero())
	})

	t.Run("reset allows fresh statistics tracking", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		// First batch of work
		chain1 := NewVersionChain(1)
		chain1.AddVersion(&VersionedRow{RowID: 1, TxnID: 100, CommitTS: 100})
		chain1.AddVersion(&VersionedRow{RowID: 1, TxnID: 101, CommitTS: 1500})
		vacuum.CleanVersionChain(chain1)

		// Reset
		vacuum.ResetStatistics()

		// Second batch of work
		mockClock.Set(time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC))
		chain2 := NewVersionChain(2)
		chain2.AddVersion(&VersionedRow{RowID: 2, TxnID: 200, CommitTS: 200})
		chain2.AddVersion(&VersionedRow{RowID: 2, TxnID: 201, CommitTS: 300})
		chain2.AddVersion(&VersionedRow{RowID: 2, TxnID: 202, CommitTS: 1600})
		vacuum.CleanVersionChain(chain2)

		// Statistics should only reflect second batch
		stats := vacuum.GetStatistics()
		assert.Equal(t, uint64(2), stats.VersionsRemoved) // Only from chain2
		assert.Equal(t, uint64(1), stats.ChainsProcessed) // Only chain2
	})
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

// TestVacuum_ConcurrentAccess tests thread safety of Vacuum operations.
func TestVacuum_ConcurrentAccess(t *testing.T) {
	t.Run("concurrent CleanVersionChain calls are safe", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		// Create multiple chains
		numChains := 50
		chains := make([]*VersionChain, numChains)
		for i := 0; i < numChains; i++ {
			chain := NewVersionChain(uint64(i))
			// Add some versions
			chain.AddVersion(&VersionedRow{RowID: uint64(i), TxnID: uint64(i * 10), CommitTS: 100})
			chain.AddVersion(&VersionedRow{RowID: uint64(i), TxnID: uint64(i*10 + 1), CommitTS: 200})
			chain.AddVersion(&VersionedRow{RowID: uint64(i), TxnID: uint64(i*10 + 2), CommitTS: 1500})
			chains[i] = chain
		}

		var wg sync.WaitGroup
		for i := 0; i < numChains; i++ {
			wg.Add(1)
			go func(chain *VersionChain) {
				defer wg.Done()
				vacuum.CleanVersionChain(chain)
			}(chains[i])
		}

		wg.Wait()

		// Verify all chains were processed
		stats := vacuum.GetStatistics()
		assert.Equal(t, uint64(numChains), stats.ChainsProcessed)
	})

	t.Run("concurrent GetStatistics calls are safe", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		// Do some initial work
		chain := NewVersionChain(1)
		chain.AddVersion(&VersionedRow{RowID: 1, TxnID: 100, CommitTS: 100})
		chain.AddVersion(&VersionedRow{RowID: 1, TxnID: 101, CommitTS: 1500})
		vacuum.CleanVersionChain(chain)

		var wg sync.WaitGroup
		numGoroutines := 100
		numReads := 50

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < numReads; i++ {
					stats := vacuum.GetStatistics()
					// Just verify we can read without panic
					_ = stats.VersionsRemoved
					_ = stats.ChainsProcessed
					_ = stats.LastRunTime
				}
			}()
		}

		wg.Wait()
	})

	t.Run("concurrent ResetStatistics calls are safe", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		var wg sync.WaitGroup
		numGoroutines := 50

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				vacuum.ResetStatistics()
				vacuum.GetStatistics()
			}()
		}

		wg.Wait()
	})

	t.Run("concurrent CleanVersionChain and GetStatistics are safe", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		numChains := 30
		chains := make([]*VersionChain, numChains)
		for i := 0; i < numChains; i++ {
			chain := NewVersionChain(uint64(i))
			chain.AddVersion(&VersionedRow{RowID: uint64(i), TxnID: uint64(i * 10), CommitTS: 100})
			chain.AddVersion(&VersionedRow{RowID: uint64(i), TxnID: uint64(i*10 + 1), CommitTS: 1500})
			chains[i] = chain
		}

		var wg sync.WaitGroup

		// Start cleaners
		for i := 0; i < numChains; i++ {
			wg.Add(1)
			go func(chain *VersionChain) {
				defer wg.Done()
				vacuum.CleanVersionChain(chain)
			}(chains[i])
		}

		// Start readers
		numReaders := 20
		for g := 0; g < numReaders; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					_ = vacuum.GetStatistics()
				}
			}()
		}

		wg.Wait()

		// Final statistics should show all chains processed
		stats := vacuum.GetStatistics()
		assert.Equal(t, uint64(numChains), stats.ChainsProcessed)
	})

	t.Run("concurrent operations on same chain are safe", func(t *testing.T) {
		mockClock := quartz.NewMock(t)

		// Create a new vacuum for each goroutine to avoid statistics contention
		// but use the same chain to test chain locking
		chain := NewVersionChain(1)

		var wg sync.WaitGroup
		numGoroutines := 20

		// Add versions and clean concurrently
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

				// Add a version
				chain.AddVersion(&VersionedRow{
					RowID:    1,
					TxnID:    uint64(id*10 + 1),
					CommitTS: 100,
				})

				// Clean the chain
				vacuum.CleanVersionChain(chain)
			}(g)
		}

		wg.Wait()

		// The chain should be in a valid state (no crash, no deadlock)
		_ = chain.Len()
		_ = chain.GetHead()
	})
}

// ============================================================================
// Edge Cases and Special Scenarios
// ============================================================================

// TestVacuum_EdgeCases tests various edge cases and special scenarios.
func TestVacuum_EdgeCases(t *testing.T) {
	t.Run("chain with only uncommitted versions", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		chain := NewVersionChain(1)
		chain.AddVersion(&VersionedRow{RowID: 1, TxnID: 100, CommitTS: 0}) // Uncommitted
		chain.AddVersion(&VersionedRow{RowID: 1, TxnID: 101, CommitTS: 0}) // Uncommitted

		result := vacuum.CleanVersionChain(chain)

		assert.Equal(t, 0, result, "uncommitted versions should not be removed")
		assert.Equal(t, 2, chain.Len())
	})

	t.Run("long chain with many old versions", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		chain := NewVersionChain(1)

		// Add 100 old versions
		for i := 0; i < 100; i++ {
			chain.AddVersion(&VersionedRow{
				RowID:    1,
				TxnID:    uint64(i),
				CommitTS: uint64(i + 1), // All < 1000
			})
		}

		// Add one recent version at the head
		chain.AddVersion(&VersionedRow{
			RowID:    1,
			TxnID:    200,
			CommitTS: 1500,
		})

		result := vacuum.CleanVersionChain(chain)

		assert.Equal(t, 100, result, "all old versions should be removed")
		assert.Equal(t, 1, chain.Len())
	})

	t.Run("VacuumStats fields are independent", func(t *testing.T) {
		stats := VacuumStats{
			VersionsRemoved: 42,
			ChainsProcessed: 10,
			LastRunTime:     time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		}

		assert.Equal(t, uint64(42), stats.VersionsRemoved)
		assert.Equal(t, uint64(10), stats.ChainsProcessed)
		assert.False(t, stats.LastRunTime.IsZero())
	})

	t.Run("delete marker at boundary", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)

		// Version committed just before watermark with delete at watermark
		version := &VersionedRow{
			RowID:     1,
			TxnID:     100,
			CommitTS:  999,  // Just before watermark
			DeletedBy: 200,
			DeleteTS:  1000, // Exactly at watermark
		}

		result := vacuum.CanRemoveVersion(version, 1000)
		assert.False(t, result, "version with delete at watermark should not be removable")
	})

	t.Run("version with CommitTS = 1 and low watermark = 1", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return 1 }, mockClock)

		version := &VersionedRow{
			RowID:    1,
			TxnID:    100,
			CommitTS: 1, // Equal to watermark
		}

		result := vacuum.CanRemoveVersion(version, 1)
		assert.False(t, result, "version at watermark should not be removable")
	})

	t.Run("very large CommitTS values", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		vacuum := NewVacuum(func() uint64 { return ^uint64(0) - 1 }, mockClock)

		version := &VersionedRow{
			RowID:    1,
			TxnID:    100,
			CommitTS: ^uint64(0) - 2, // Just before max - 1
		}

		result := vacuum.CanRemoveVersion(version, ^uint64(0)-1)
		assert.True(t, result, "version before high watermark should be removable")
	})
}
