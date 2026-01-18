package optimizer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestAutoUpdateThreshold verifies that ANALYZE is triggered at 10% modification threshold.
func TestAutoUpdateThreshold(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("test_table", 1000) // 1000 rows baseline

	// Track ANALYZE invocations
	analyzeCount := 0
	var analyzeMutex sync.Mutex
	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		analyzeMutex.Lock()
		analyzeCount++
		analyzeMutex.Unlock()
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 1000, nil
	}

	// Create auto-update manager
	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)
	aum.SetBatchInterval(50 * time.Millisecond) // Use shorter interval for testing

	// Add 50 rows (5% modification - should NOT trigger)
	tracker.RecordInsert("test_table", 50)
	aum.CheckAndQueueAutoAnalyze("test_table")
	time.Sleep(100 * time.Millisecond) // Wait for batch processing

	analyzeMutex.Lock()
	count1 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 0, count1, "ANALYZE should not trigger at 5% modification")

	// Add another 50 rows (10% total - SHOULD trigger)
	tracker.RecordInsert("test_table", 50)
	aum.CheckAndQueueAutoAnalyze("test_table")
	time.Sleep(150 * time.Millisecond) // Wait for batch processing

	analyzeMutex.Lock()
	count2 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 1, count2, "ANALYZE should trigger at or exceeding 10% modification")

	aum.Stop()
}

// TestAutoUpdateWithDeleteAndUpdate verifies threshold with mixed operations.
func TestAutoUpdateWithDeleteAndUpdate(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("products", 1000)

	analyzeCount := 0
	var analyzeMutex sync.Mutex
	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		analyzeMutex.Lock()
		analyzeCount++
		analyzeMutex.Unlock()
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 1000, nil
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)
	aum.SetBatchInterval(50 * time.Millisecond)

	// DELETE 30 rows (3%)
	tracker.RecordDelete("products", 30)
	aum.CheckAndQueueAutoAnalyze("products")
	time.Sleep(100 * time.Millisecond)

	analyzeMutex.Lock()
	count1 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 0, count1, "ANALYZE should not trigger at 3% deletion")

	// UPDATE 35 rows (3% + 3.5% = 6.5%)
	tracker.RecordUpdate("products", 35)
	aum.CheckAndQueueAutoAnalyze("products")
	time.Sleep(100 * time.Millisecond)

	analyzeMutex.Lock()
	count2 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 0, count2, "ANALYZE should not trigger at 6.5% combined")

	// INSERT 35 more rows (3% + 3.5% + 3.5% = 10%)
	tracker.RecordInsert("products", 35)
	aum.CheckAndQueueAutoAnalyze("products")
	time.Sleep(150 * time.Millisecond)

	analyzeMutex.Lock()
	count3 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 1, count3, "ANALYZE should trigger at 10% combined modifications")

	aum.Stop()
}

// TestModificationCounterResetAfterAnalyze verifies that counters reset after ANALYZE.
func TestModificationCounterResetAfterAnalyze(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("users", 5000)

	analyzeCount := 0
	var analyzeMutex sync.Mutex
	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		analyzeMutex.Lock()
		analyzeCount++
		analyzeMutex.Unlock()

		// Simulate ANALYZE: get new row count and reset tracker
		if tableName == "users" {
			tracker.InitializeTable("users", 5600)
		}
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 5600, nil
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)
	aum.SetBatchInterval(50 * time.Millisecond)

	// Insert 600 rows (12% of 5000 - triggers ANALYZE)
	tracker.RecordInsert("users", 600)
	aum.CheckAndQueueAutoAnalyze("users")
	time.Sleep(150 * time.Millisecond)

	analyzeMutex.Lock()
	count1 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 1, count1, "First ANALYZE should have triggered")

	// Verify modification counter was reset
	mod := tracker.GetTableModification("users")
	assert.Equal(t, int64(0), mod.InsertCount, "Modification counter should be reset after ANALYZE")
	assert.Equal(t, int64(5600), mod.OriginalCount, "OriginalCount should be updated")

	// Insert 200 more rows (3.6% of 5600 - should NOT trigger)
	tracker.RecordInsert("users", 200)
	aum.CheckAndQueueAutoAnalyze("users")
	time.Sleep(100 * time.Millisecond)

	analyzeMutex.Lock()
	count2 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 1, count2, "Second ANALYZE should NOT trigger at 3.6%")

	// Insert 330 more rows (9.6% total - still under 10%)
	tracker.RecordInsert("users", 330)
	aum.CheckAndQueueAutoAnalyze("users")
	time.Sleep(100 * time.Millisecond)

	analyzeMutex.Lock()
	count3 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 1, count3, "Second ANALYZE should NOT trigger at 9.6% total")

	// Insert 29 more rows (9.96% total - still just under 10%)
	tracker.RecordInsert("users", 29)
	aum.CheckAndQueueAutoAnalyze("users")
	time.Sleep(100 * time.Millisecond)

	analyzeMutex.Lock()
	count4 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 1, count4, "Second ANALYZE should NOT trigger at 9.96% total")

	// Insert 6 more rows (10.05% total - SHOULD trigger at or above 10%)
	tracker.RecordInsert("users", 6)
	aum.CheckAndQueueAutoAnalyze("users")
	time.Sleep(150 * time.Millisecond)

	analyzeMutex.Lock()
	count5 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 2, count5, "Second ANALYZE should trigger at 10.05% total")

	aum.Stop()
}

// TestBatchingPreventsExcessiveAnalyze verifies batching reduces overhead.
func TestBatchingPreventsExcessiveAnalyze(t *testing.T) {
	tracker := NewModificationTracker()

	// Initialize multiple tables
	for i := 1; i <= 5; i++ {
		tableName := "table_" + string(rune(i+'0'))
		tracker.InitializeTable(tableName, 1000)
	}

	analyzeCount := 0
	var analyzeMutex sync.Mutex
	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		analyzeMutex.Lock()
		analyzeCount++
		analyzeMutex.Unlock()
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 1000, nil
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)
	aum.SetBatchInterval(50 * time.Millisecond)

	// Trigger auto-analyze on all 5 tables at roughly the same time
	for i := 1; i <= 5; i++ {
		tableName := "table_" + string(rune(i+'0'))
		tracker.RecordInsert(tableName, 150) // 15% modification on each
		aum.CheckAndQueueAutoAnalyze(tableName)
	}

	// Wait for batch execution
	time.Sleep(150 * time.Millisecond)

	analyzeMutex.Lock()
	finalCount := analyzeCount
	analyzeMutex.Unlock()

	// Should have 5 ANALYZE calls (one per table), not 5+ from multiple batches
	assert.Equal(t, 5, finalCount, "Should execute exactly 5 ANALYZE operations (batched)")

	aum.Stop()
}

// TestIncrementalAnalyzeForLargeTables verifies incremental flag for large tables.
func TestIncrementalAnalyzeForLargeTables(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("big_table", 2000000) // 2 million rows

	var incrementalFlags []bool
	var flagsMutex sync.Mutex

	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		flagsMutex.Lock()
		incrementalFlags = append(incrementalFlags, incremental)
		flagsMutex.Unlock()
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 2000000, nil
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)
	aum.SetBatchInterval(50 * time.Millisecond)

	// Trigger auto-analyze on large table
	tracker.RecordInsert("big_table", 250000) // 12.5% modification
	aum.CheckAndQueueAutoAnalyze("big_table")
	time.Sleep(150 * time.Millisecond)

	flagsMutex.Lock()
	assert.Len(t, incrementalFlags, 1)
	assert.True(t, incrementalFlags[0], "Large table (>1M rows) should use incremental ANALYZE")
	flagsMutex.Unlock()

	aum.Stop()
}

// TestNoAnalyzeForSmallModifications verifies threshold boundary.
func TestNoAnalyzeForSmallModifications(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("data", 10000)

	analyzeCount := 0
	var analyzeMutex sync.Mutex
	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		analyzeMutex.Lock()
		analyzeCount++
		analyzeMutex.Unlock()
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 10000, nil
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)
	aum.SetBatchInterval(50 * time.Millisecond)

	// Insert 999 rows (9.99% - just under 10%)
	tracker.RecordInsert("data", 999)
	aum.CheckAndQueueAutoAnalyze("data")
	time.Sleep(100 * time.Millisecond)

	analyzeMutex.Lock()
	count1 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 0, count1, "Should not trigger at 9.99%")

	// Insert 1 more row (10.0% - should trigger)
	tracker.RecordInsert("data", 1)
	aum.CheckAndQueueAutoAnalyze("data")
	time.Sleep(150 * time.Millisecond)

	analyzeMutex.Lock()
	count2 := analyzeCount
	analyzeMutex.Unlock()
	assert.Equal(t, 1, count2, "Should trigger at 10.0%")

	aum.Stop()
}

// TestDuplicateQueueingPrevention verifies that duplicate table queuing is prevented.
func TestDuplicateQueueingPrevention(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("queue_test", 1000)

	analyzeCount := 0
	var analyzeMutex sync.Mutex
	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		analyzeMutex.Lock()
		analyzeCount++
		analyzeMutex.Unlock()
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 1000, nil
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)
	aum.SetBatchInterval(50 * time.Millisecond)

	// Trigger the same table multiple times before batch executes
	for i := 0; i < 10; i++ {
		tracker.RecordInsert("queue_test", 15) // 1.5% each
		aum.CheckAndQueueAutoAnalyze("queue_test")
	}

	// After 10 calls with 1.5% each, we're at 15% total
	// But the table should only be queued once
	time.Sleep(150 * time.Millisecond)

	analyzeMutex.Lock()
	finalCount := analyzeCount
	analyzeMutex.Unlock()

	assert.Equal(
		t,
		1,
		finalCount,
		"Table should be ANALYZE'd only once despite multiple trigger calls",
	)

	aum.Stop()
}

// TestMetricsTracking verifies metrics are collected correctly.
func TestMetricsTracking(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("metrics_test", 1000)

	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		// Reset after ANALYZE so subsequent triggers can happen
		tracker.InitializeTable(tableName, 1000)
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 1000, nil
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)
	aum.SetBatchInterval(50 * time.Millisecond)

	// Trigger multiple times with resets between
	for i := 0; i < 3; i++ {
		tracker.RecordInsert("metrics_test", 100)
		aum.CheckAndQueueAutoAnalyze("metrics_test")
		time.Sleep(150 * time.Millisecond) // Wait for batch to complete
	}

	metrics := aum.GetMetrics()
	assert.Equal(t, int64(3), metrics["triggers"], "Should have 3 triggers")
	assert.Equal(
		t,
		int64(3),
		metrics["analyzes"],
		"Should have 3 ANALYZE operations (one per trigger)",
	)
	assert.True(t, metrics["batches"] >= 1, "Should have at least 1 batch")

	aum.Stop()
}

// TestAutoUpdateWithMultipleTables verifies independent tracking per table.
func TestAutoUpdateWithMultipleTables(t *testing.T) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("t1", 1000)
	tracker.InitializeTable("t2", 2000)
	tracker.InitializeTable("t3", 5000)

	analyzedTables := make(map[string]int)
	var tablesMutex sync.Mutex

	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		tablesMutex.Lock()
		analyzedTables[tableName]++
		tablesMutex.Unlock()
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		switch tableName {
		case "t1":
			return 1000, nil
		case "t2":
			return 2000, nil
		case "t3":
			return 5000, nil
		default:
			return 0, nil
		}
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)
	aum.SetBatchInterval(50 * time.Millisecond)

	// Trigger on different tables at different times
	// t1: 150 rows (15%)
	tracker.RecordInsert("t1", 150)
	aum.CheckAndQueueAutoAnalyze("t1")

	// t2: 150 rows (7.5%) - not enough
	tracker.RecordInsert("t2", 150)
	aum.CheckAndQueueAutoAnalyze("t2")

	// t3: 600 rows (12%)
	tracker.RecordInsert("t3", 600)
	aum.CheckAndQueueAutoAnalyze("t3")

	time.Sleep(150 * time.Millisecond)

	tablesMutex.Lock()
	assert.Equal(t, 1, analyzedTables["t1"], "t1 should be analyzed")
	assert.Equal(t, 0, analyzedTables["t2"], "t2 should not be analyzed")
	assert.Equal(t, 1, analyzedTables["t3"], "t3 should be analyzed")
	tablesMutex.Unlock()

	aum.Stop()
}
