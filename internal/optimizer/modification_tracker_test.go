package optimizer

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestRecordInsert verifies that inserts are tracked correctly.
func TestRecordInsert(t *testing.T) {
	tracker := NewModificationTracker()

	// Record single insert
	tracker.RecordInsert("users", 10)

	mod := tracker.GetTableModification("users")
	assert.NotNil(t, mod)
	assert.Equal(t, int64(10), mod.InsertCount)
	assert.Equal(t, int64(0), mod.UpdateCount)
	assert.Equal(t, int64(0), mod.DeleteCount)

	// Record another insert
	tracker.RecordInsert("users", 5)
	mod = tracker.GetTableModification("users")
	assert.Equal(t, int64(15), mod.InsertCount)
}

// TestRecordUpdate verifies that updates are tracked correctly.
func TestRecordUpdate(t *testing.T) {
	tracker := NewModificationTracker()

	// Record updates
	tracker.RecordUpdate("products", 20)
	tracker.RecordUpdate("products", 10)

	mod := tracker.GetTableModification("products")
	assert.NotNil(t, mod)
	assert.Equal(t, int64(0), mod.InsertCount)
	assert.Equal(t, int64(30), mod.UpdateCount)
	assert.Equal(t, int64(0), mod.DeleteCount)
}

// TestRecordDelete verifies that deletes are tracked correctly.
func TestRecordDelete(t *testing.T) {
	tracker := NewModificationTracker()

	// Record deletes
	tracker.RecordDelete("orders", 5)
	tracker.RecordDelete("orders", 3)

	mod := tracker.GetTableModification("orders")
	assert.NotNil(t, mod)
	assert.Equal(t, int64(0), mod.InsertCount)
	assert.Equal(t, int64(0), mod.UpdateCount)
	assert.Equal(t, int64(8), mod.DeleteCount)
}

// TestGetModificationRatio verifies correct ratio calculation.
func TestGetModificationRatio(t *testing.T) {
	tracker := NewModificationTracker()

	// Initialize table with 1000 rows
	tracker.InitializeTable("customers", 1000)

	// No modifications yet
	ratio := tracker.GetModificationRatio("customers")
	assert.Equal(t, 0.0, ratio)

	// 100 inserts = 10% modification
	tracker.RecordInsert("customers", 100)
	ratio = tracker.GetModificationRatio("customers")
	assert.InDelta(t, 0.1, ratio, 0.001)

	// Add 50 updates = 15% total
	tracker.RecordUpdate("customers", 50)
	ratio = tracker.GetModificationRatio("customers")
	assert.InDelta(t, 0.15, ratio, 0.001)

	// Add 50 deletes = 20% total
	tracker.RecordDelete("customers", 50)
	ratio = tracker.GetModificationRatio("customers")
	assert.InDelta(t, 0.2, ratio, 0.001)
}

// TestRatioWithZeroOriginalCount verifies edge case handling.
func TestRatioWithZeroOriginalCount(t *testing.T) {
	tracker := NewModificationTracker()

	// Track modifications on uninitialized table (OriginalCount = 0)
	tracker.RecordInsert("newTable", 100)

	// Should return 0.0 when OriginalCount is 0 (avoid division by zero)
	ratio := tracker.GetModificationRatio("newTable")
	assert.Equal(t, 0.0, ratio)
}

// TestRatioNonexistentTable verifies behavior for tables never tracked.
func TestRatioNonexistentTable(t *testing.T) {
	tracker := NewModificationTracker()

	// Query ratio for table never seen
	ratio := tracker.GetModificationRatio("unknownTable")
	assert.Equal(t, 0.0, ratio)
}

// TestReset verifies that reset clears modification counts.
func TestReset(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("data", 500)

	// Add modifications
	tracker.RecordInsert("data", 50)
	tracker.RecordUpdate("data", 25)

	// Verify modifications are tracked
	ratio := tracker.GetModificationRatio("data")
	assert.InDelta(t, 0.15, ratio, 0.001)

	// Reset the table
	tracker.Reset("data")

	// Modifications should be cleared, but OriginalCount preserved
	ratio = tracker.GetModificationRatio("data")
	assert.Equal(t, 0.0, ratio)

	mod := tracker.GetTableModification("data")
	assert.Equal(t, int64(0), mod.InsertCount)
	assert.Equal(t, int64(0), mod.UpdateCount)
	assert.Equal(t, int64(0), mod.DeleteCount)
	assert.Equal(t, int64(500), mod.OriginalCount)
}

// TestInitializeTable verifies initialization sets correct baseline.
func TestInitializeTable(t *testing.T) {
	tracker := NewModificationTracker()

	// Initialize with row count
	tracker.InitializeTable("table1", 1000)

	mod := tracker.GetTableModification("table1")
	assert.NotNil(t, mod)
	assert.Equal(t, int64(1000), mod.OriginalCount)
	assert.Equal(t, int64(0), mod.InsertCount)
	assert.Equal(t, int64(0), mod.UpdateCount)
	assert.Equal(t, int64(0), mod.DeleteCount)
	assert.NotZero(t, mod.LastUpdated)
}

// TestMultipleTables verifies independent tracking per table.
func TestMultipleTables(t *testing.T) {
	tracker := NewModificationTracker()

	// Initialize different tables
	tracker.InitializeTable("users", 1000)
	tracker.InitializeTable("posts", 5000)
	tracker.InitializeTable("comments", 10000)

	// Modify each table differently
	tracker.RecordInsert("users", 50)
	tracker.RecordUpdate("posts", 500)
	tracker.RecordDelete("comments", 1000)

	// Verify each table has correct ratio
	usersRatio := tracker.GetModificationRatio("users")
	assert.InDelta(t, 0.05, usersRatio, 0.001)

	postsRatio := tracker.GetModificationRatio("posts")
	assert.InDelta(t, 0.1, postsRatio, 0.001)

	commentsRatio := tracker.GetModificationRatio("comments")
	assert.InDelta(t, 0.1, commentsRatio, 0.001)
}

// TestConcurrentAccess verifies thread-safety with multiple goroutines.
func TestConcurrentAccess(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("concurrent", 10000)

	var wg sync.WaitGroup
	numGoroutines := 100

	// Spawn multiple goroutines doing concurrent modifications
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Each goroutine records different operations
			for j := 0; j < 10; j++ {
				tracker.RecordInsert("concurrent", 1)
				tracker.RecordUpdate("concurrent", 1)
				tracker.RecordDelete("concurrent", 1)
			}

			// Also read the ratio
			_ = tracker.GetModificationRatio("concurrent")
		}(i)
	}

	wg.Wait()

	// Verify final counts are correct (100 goroutines * 10 iterations * 3 ops each)
	mod := tracker.GetTableModification("concurrent")
	assert.Equal(t, int64(1000), mod.InsertCount)
	assert.Equal(t, int64(1000), mod.UpdateCount)
	assert.Equal(t, int64(1000), mod.DeleteCount)

	// Verify ratio is correct
	ratio := tracker.GetModificationRatio("concurrent")
	expectedRatio := 3000.0 / 10000.0 // 0.3
	assert.InDelta(t, expectedRatio, ratio, 0.001)
}

// TestNegativeRowCounts verifies that negative counts are ignored.
func TestNegativeRowCounts(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("table", 100)

	// Attempt to record negative counts (should be ignored)
	tracker.RecordInsert("table", -10)
	tracker.RecordUpdate("table", -5)
	tracker.RecordDelete("table", -3)

	mod := tracker.GetTableModification("table")
	assert.Equal(t, int64(0), mod.InsertCount)
	assert.Equal(t, int64(0), mod.UpdateCount)
	assert.Equal(t, int64(0), mod.DeleteCount)
}

// TestLargeNumbers verifies handling of very large row counts.
func TestLargeNumbers(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("bigTable", 1000000000) // 1 billion rows

	// Record large modifications
	tracker.RecordInsert("bigTable", 100000000)  // 100 million inserts
	tracker.RecordUpdate("bigTable", 50000000)   // 50 million updates
	tracker.RecordDelete("bigTable", 50000000)   // 50 million deletes

	// Verify calculations with large numbers
	mod := tracker.GetTableModification("bigTable")
	assert.Equal(t, int64(100000000), mod.InsertCount)
	assert.Equal(t, int64(50000000), mod.UpdateCount)
	assert.Equal(t, int64(50000000), mod.DeleteCount)

	ratio := tracker.GetModificationRatio("bigTable")
	expectedRatio := 200000000.0 / 1000000000.0 // 0.2
	assert.InDelta(t, expectedRatio, ratio, 0.0001)
}

// TestZeroRowInsert verifies that zero-row modifications are ignored.
func TestZeroRowInsert(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("empty", 100)

	// Record zero modifications
	tracker.RecordInsert("empty", 0)
	tracker.RecordUpdate("empty", 0)
	tracker.RecordDelete("empty", 0)

	mod := tracker.GetTableModification("empty")
	assert.Equal(t, int64(0), mod.InsertCount)
	assert.Equal(t, int64(0), mod.UpdateCount)
	assert.Equal(t, int64(0), mod.DeleteCount)
}

// TestGetAllModifications verifies snapshot functionality.
func TestGetAllModifications(t *testing.T) {
	tracker := NewModificationTracker()

	// Set up multiple tables
	tracker.InitializeTable("t1", 100)
	tracker.InitializeTable("t2", 200)
	tracker.InitializeTable("t3", 300)

	// Add modifications
	tracker.RecordInsert("t1", 10)
	tracker.RecordUpdate("t2", 20)
	tracker.RecordDelete("t3", 30)

	// Get all modifications
	all := tracker.GetAllModifications()

	assert.Equal(t, 3, len(all))
	assert.Equal(t, int64(10), all["t1"].InsertCount)
	assert.Equal(t, int64(20), all["t2"].UpdateCount)
	assert.Equal(t, int64(30), all["t3"].DeleteCount)
}

// TestResetAll verifies complete reset of all tables.
func TestResetAll(t *testing.T) {
	tracker := NewModificationTracker()

	// Set up multiple tables with modifications
	tracker.InitializeTable("t1", 100)
	tracker.InitializeTable("t2", 200)
	tracker.RecordInsert("t1", 50)
	tracker.RecordUpdate("t2", 100)

	// Verify modifications exist
	all := tracker.GetAllModifications()
	assert.Equal(t, 2, len(all))

	// Reset all
	tracker.ResetAll()

	// Verify everything is cleared
	all = tracker.GetAllModifications()
	assert.Equal(t, 0, len(all))

	// Verify ratios for cleared tables
	ratio1 := tracker.GetModificationRatio("t1")
	ratio2 := tracker.GetModificationRatio("t2")
	assert.Equal(t, 0.0, ratio1)
	assert.Equal(t, 0.0, ratio2)
}

// TestLastUpdatedTime verifies that LastUpdated is set correctly.
func TestLastUpdatedTime(t *testing.T) {
	tracker := NewModificationTracker()

	before := time.Now()
	tracker.InitializeTable("timed", 100)
	after := time.Now()

	mod := tracker.GetTableModification("timed")
	assert.NotNil(t, mod)
	assert.True(t, mod.LastUpdated.After(before) || mod.LastUpdated.Equal(before))
	assert.True(t, mod.LastUpdated.Before(after) || mod.LastUpdated.Equal(after))

	// Reset should update LastUpdated
	time.Sleep(10 * time.Millisecond)
	tracker.RecordInsert("timed", 10)
	oldTime := mod.LastUpdated

	tracker.Reset("timed")
	mod = tracker.GetTableModification("timed")
	assert.True(t, mod.LastUpdated.After(oldTime))
}

// TestSequentialOperations verifies complex scenarios.
func TestSequentialOperations(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("app", 1000)

	// Simulate an application's workflow
	// Initial insert of batch data: 500 rows
	tracker.RecordInsert("app", 500)
	ratio := tracker.GetModificationRatio("app")
	assert.InDelta(t, 0.5, ratio, 0.001)

	// Some updates and deletes
	tracker.RecordUpdate("app", 100)
	tracker.RecordDelete("app", 50)
	ratio = tracker.GetModificationRatio("app")
	assert.InDelta(t, 0.65, ratio, 0.001)

	// Now ANALYZE happens - reset tracking
	tracker.InitializeTable("app", 1450) // New row count after modifications

	// New cycle starts
	ratio = tracker.GetModificationRatio("app")
	assert.Equal(t, 0.0, ratio)

	// Small modifications
	tracker.RecordInsert("app", 10)
	ratio = tracker.GetModificationRatio("app")
	assert.InDelta(t, 10.0/1450.0, ratio, 0.001)
}
