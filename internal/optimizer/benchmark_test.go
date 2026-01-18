// Package optimizer provides cost-based query optimization for dukdb-go.
//
// # Benchmark Tests
//
// This file contains benchmarks to verify optimizer overhead is acceptable.
// Task 9.3 requires that optimizer overhead is < 5% (< 5ms) for simple queries.
// These benchmarks measure:
//   - Simple query optimization time (single table, no joins)
//   - Complex query optimization time (multi-table joins)
//   - Cardinality estimation overhead
//   - Cost model computation overhead
package optimizer

import (
	"context"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSimpleQueryOptimizationOverhead verifies that optimizer overhead
// for simple queries (single table, no joins) is minimal.
// This is task 9.3: Verify optimizer overhead < 5% on simple queries.
func TestSimpleQueryOptimizationOverhead(t *testing.T) {
	catalog := newMockCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{
		stats: &TableStatistics{
			RowCount:  10000,
			PageCount: 100,
			Columns: []ColumnStatistics{
				{ColumnName: "id", ColumnType: dukdb.TYPE_INTEGER, DistinctCount: 10000},
				{ColumnName: "name", ColumnType: dukdb.TYPE_VARCHAR, DistinctCount: 9000},
			},
		},
	})

	optimizer := NewCostBasedOptimizer(catalog)

	// Simple scan query
	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "u", Column: "name", Type: dukdb.TYPE_VARCHAR},
		},
	}

	// Measure optimization time over multiple iterations
	const iterations = 1000
	start := time.Now()
	for range iterations {
		_, err := optimizer.Optimize(scan)
		require.NoError(t, err)
	}
	elapsed := time.Since(start)
	avgTime := elapsed / iterations

	// Average time should be well under 5ms for simple queries
	// We expect sub-millisecond performance for simple scans
	t.Logf(
		"Simple scan optimization: avg=%v (total %v for %d iterations)",
		avgTime,
		elapsed,
		iterations,
	)
	assert.Less(t, avgTime, 5*time.Millisecond, "Simple query optimization should be < 5ms")

	// Actually for a single table scan, we expect sub-100 microsecond performance
	assert.Less(t, avgTime, 1*time.Millisecond, "Simple scan should be < 1ms")
}

// TestSimpleFilterQueryOverhead tests optimization overhead for simple filter queries.
func TestSimpleFilterQueryOverhead(t *testing.T) {
	catalog := newMockCatalog()
	catalog.AddTable("main", "orders", &mockTableInfo{
		stats: &TableStatistics{
			RowCount:  100000,
			PageCount: 1000,
			Columns: []ColumnStatistics{
				{ColumnName: "id", ColumnType: dukdb.TYPE_INTEGER, DistinctCount: 100000},
				{ColumnName: "status", ColumnType: dukdb.TYPE_VARCHAR, DistinctCount: 5},
			},
		},
	})

	optimizer := NewCostBasedOptimizer(catalog)

	// Filter query: SELECT * FROM orders WHERE status = 'active'
	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "orders",
		alias:     "o",
		columns: []OutputColumn{
			{Table: "o", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "o", Column: "status", Type: dukdb.TYPE_VARCHAR},
		},
	}

	filter := &mockLogicalFilter{
		child: scan,
		condition: &mockBinaryExpr{
			left:    &mockColumnRef{table: "o", column: "status", colType: dukdb.TYPE_VARCHAR},
			op:      OpEq,
			right:   &mockLiteral{value: "active", valType: dukdb.TYPE_VARCHAR},
			resType: dukdb.TYPE_BOOLEAN,
		},
	}

	const iterations = 1000
	start := time.Now()
	for range iterations {
		_, err := optimizer.Optimize(filter)
		require.NoError(t, err)
	}
	elapsed := time.Since(start)
	avgTime := elapsed / iterations

	t.Logf(
		"Filter query optimization: avg=%v (total %v for %d iterations)",
		avgTime,
		elapsed,
		iterations,
	)
	assert.Less(t, avgTime, 5*time.Millisecond, "Simple filter query optimization should be < 5ms")
}

// TestSimpleProjectionQueryOverhead tests optimization overhead for projection queries.
func TestSimpleProjectionQueryOverhead(t *testing.T) {
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "data",
		alias:     "d",
		columns: []OutputColumn{
			{Table: "d", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "d", Column: "name", Type: dukdb.TYPE_VARCHAR},
			{Table: "d", Column: "value", Type: dukdb.TYPE_DOUBLE},
		},
	}

	project := &mockLogicalProject{
		child:   scan,
		columns: scan.columns,
	}

	const iterations = 10000
	start := time.Now()
	for range iterations {
		_, err := optimizer.Optimize(project)
		require.NoError(t, err)
	}
	elapsed := time.Since(start)
	avgTime := elapsed / iterations

	t.Logf(
		"Projection optimization: avg=%v (total %v for %d iterations)",
		avgTime,
		elapsed,
		iterations,
	)
	assert.Less(t, avgTime, 5*time.Millisecond, "Simple projection optimization should be < 5ms")
}

// ============================================================================
// Task 3.9: Auto-Update Performance Benchmarks
// ============================================================================

// BenchmarkModificationTracking benchmarks the performance of recording modifications.
// Goal: <1 microsecond per operation (non-blocking, just counter increment)
func BenchmarkModificationTracking(b *testing.B) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("test", 10000)

	b.ResetTimer()
	for range b.N {
		tracker.RecordInsert("test", 1)
	}

	// Result should be extremely fast: sub-microsecond
	// Each call is just a lock + counter increment
	b.ReportAllocs()
}

// BenchmarkThresholdCheck benchmarks ratio calculation and threshold comparison.
// Goal: <10 microseconds per check (calculate ratio + compare)
func BenchmarkThresholdCheck(b *testing.B) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("test", 100000)

	// Add some modifications
	tracker.RecordInsert("test", 5000)

	b.ResetTimer()
	for range b.N {
		ratio := tracker.GetModificationRatio("test")
		if ratio >= 0.10 {
			// Threshold exceeded
		}
	}

	b.ReportAllocs()
}

// BenchmarkQueueing benchmarks the overhead of queuing a table for ANALYZE.
// Goal: <100 microseconds per queue operation (lock + map insertion)
func BenchmarkQueueing(b *testing.B) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("test", 10000)

	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 10000, nil
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)
	aum.SetBatchInterval(1 * time.Second) // Long interval so batch doesn't execute during benchmark

	tracker.RecordInsert("test", 1100) // Trigger threshold

	b.ResetTimer()
	for range b.N {
		aum.CheckAndQueueAutoAnalyze("test")
	}

	b.ReportAllocs()
}

// BenchmarkAutoUpdateWithoutTrigger benchmarks the performance of modification tracking
// when auto-update is NOT triggered (modifications stay below 10%).
// Goal: <2 microseconds total overhead per operation
func BenchmarkAutoUpdateWithoutTrigger(b *testing.B) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("test", 100000)

	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 100000, nil
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)

	b.ResetTimer()
	for range b.N {
		// Record insert below threshold (< 0.05%)
		tracker.RecordInsert("test", 1)
		aum.CheckAndQueueAutoAnalyze("test")
	}

	b.ReportAllocs()
}

// BenchmarkAutoUpdateBatchExecution benchmarks the time to execute a batch of ANALYZE operations.
// This includes actual ANALYZE calls (stubbed) plus tracking updates.
// Goal: ~1ms per table in batch
func BenchmarkAutoUpdateBatchExecution(b *testing.B) {
	tracker := NewModificationTracker()

	// Set up 10 tables
	tableCount := 10
	for i := 0; i < tableCount; i++ {
		tableName := "table_" + string(rune('0'+i))
		tracker.InitializeTable(tableName, 100000)
		tracker.RecordInsert(tableName, 15000) // 15% modification
	}

	analyzeCount := 0
	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		analyzeCount++
		// Simulate some ANALYZE work (but keep it minimal for benchmark)
		time.Sleep(100 * time.Microsecond)
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 100000, nil
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)

	// Manually trigger analyze for all tables
	b.ResetTimer()
	for range b.N {
		for i := 0; i < tableCount; i++ {
			tableName := "table_" + string(rune('0'+i))
			aum.CheckAndQueueAutoAnalyze(tableName)
		}

		// Wait for batch to complete
		time.Sleep(200 * time.Millisecond)
	}

	b.ReportAllocs()
}

// BenchmarkAutoUpdateOverheadComparison benchmarks the total overhead of auto-update
// in a realistic scenario: 1000 single-row inserts.
// This measures: modification tracking + queuing + batching
// Goal: <1ms total for 1000 operations (including batch execution)
func BenchmarkAutoUpdateOverheadComparison(b *testing.B) {
	tracker := NewModificationTracker()
	tracker.InitializeTable("busy_table", 1000000)

	batchCount := 0
	analyzeFunc := func(ctx context.Context, tableName string, incremental bool) error {
		batchCount++
		return nil
	}

	getRowCountFunc := func(tableName string) (int64, error) {
		return 1000000, nil
	}

	aum := NewAutoUpdateManager(tracker, analyzeFunc, getRowCountFunc)
	aum.SetBatchInterval(10 * time.Millisecond) // Short batch interval

	b.ResetTimer()
	b.StopTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		start := time.Now()

		// Record 1000 single-row inserts
		for i := 0; i < 1000; i++ {
			tracker.RecordInsert("busy_table", 1)
			aum.CheckAndQueueAutoAnalyze("busy_table")
		}

		// Wait for batch to complete
		time.Sleep(50 * time.Millisecond)

		elapsed := time.Since(start)
		b.ReportMetric(float64(elapsed.Microseconds()), "microseconds_per_1000_ops")
	}

	b.ReportAllocs()
}
