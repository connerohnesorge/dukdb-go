// Package optimizer provides cost-based query optimization for dukdb-go.
//
// Benchmark Tests
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
	t.Logf("Simple scan optimization: avg=%v (total %v for %d iterations)", avgTime, elapsed, iterations)
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

	t.Logf("Filter query optimization: avg=%v (total %v for %d iterations)", avgTime, elapsed, iterations)
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

	const iterations = 1000
	start := time.Now()
	for range iterations {
		_, err := optimizer.Optimize(project)
		require.NoError(t, err)
	}
	elapsed := time.Since(start)
	avgTime := elapsed / iterations

	t.Logf("Projection query optimization: avg=%v (total %v for %d iterations)", avgTime, elapsed, iterations)
	assert.Less(t, avgTime, 5*time.Millisecond, "Simple projection optimization should be < 5ms")
}

// TestSimpleSortQueryOverhead tests optimization overhead for sort queries.
func TestSimpleSortQueryOverhead(t *testing.T) {
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "data",
		alias:     "d",
		columns: []OutputColumn{
			{Table: "d", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	sort := &mockLogicalSort{child: scan}

	const iterations = 1000
	start := time.Now()
	for range iterations {
		_, err := optimizer.Optimize(sort)
		require.NoError(t, err)
	}
	elapsed := time.Since(start)
	avgTime := elapsed / iterations

	t.Logf("Sort query optimization: avg=%v (total %v for %d iterations)", avgTime, elapsed, iterations)
	assert.Less(t, avgTime, 5*time.Millisecond, "Simple sort optimization should be < 5ms")
}

// TestSimpleLimitQueryOverhead tests optimization overhead for limit queries.
func TestSimpleLimitQueryOverhead(t *testing.T) {
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "data",
		alias:     "d",
		columns: []OutputColumn{
			{Table: "d", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	limit := &mockLogicalLimit{
		child:  scan,
		limit:  100,
		offset: 0,
	}

	const iterations = 1000
	start := time.Now()
	for range iterations {
		_, err := optimizer.Optimize(limit)
		require.NoError(t, err)
	}
	elapsed := time.Since(start)
	avgTime := elapsed / iterations

	t.Logf("Limit query optimization: avg=%v (total %v for %d iterations)", avgTime, elapsed, iterations)
	assert.Less(t, avgTime, 5*time.Millisecond, "Simple limit optimization should be < 5ms")
}

// TestSimpleAggregateQueryOverhead tests optimization overhead for aggregate queries.
func TestSimpleAggregateQueryOverhead(t *testing.T) {
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "data",
		alias:     "d",
		columns: []OutputColumn{
			{Table: "d", Column: "category", Type: dukdb.TYPE_VARCHAR},
			{Table: "d", Column: "value", Type: dukdb.TYPE_DOUBLE},
		},
	}

	aggregate := &mockLogicalAggregate{
		child: scan,
		groupBy: []ExprNode{
			&mockColumnRef{table: "d", column: "category", colType: dukdb.TYPE_VARCHAR},
		},
		columns: []OutputColumn{
			{Table: "d", Column: "category", Type: dukdb.TYPE_VARCHAR},
			{Column: "sum", Type: dukdb.TYPE_DOUBLE},
		},
	}

	const iterations = 1000
	start := time.Now()
	for range iterations {
		_, err := optimizer.Optimize(aggregate)
		require.NoError(t, err)
	}
	elapsed := time.Since(start)
	avgTime := elapsed / iterations

	t.Logf("Aggregate query optimization: avg=%v (total %v for %d iterations)", avgTime, elapsed, iterations)
	assert.Less(t, avgTime, 5*time.Millisecond, "Simple aggregate optimization should be < 5ms")
}

// TestTwoTableJoinOverhead tests optimization overhead for 2-table joins.
func TestTwoTableJoinOverhead(t *testing.T) {
	catalog := newMockCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{
		stats: &TableStatistics{RowCount: 10000, PageCount: 100},
	})
	catalog.AddTable("main", "orders", &mockTableInfo{
		stats: &TableStatistics{RowCount: 50000, PageCount: 500},
	})

	optimizer := NewCostBasedOptimizer(catalog)

	usersScan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	ordersScan := &mockLogicalScan{
		schema:    "main",
		tableName: "orders",
		alias:     "o",
		columns: []OutputColumn{
			{Table: "o", Column: "user_id", Type: dukdb.TYPE_INTEGER},
		},
	}

	join := &mockLogicalJoin{
		left:     usersScan,
		right:    ordersScan,
		joinType: JoinTypeInner,
		condition: &mockBinaryExpr{
			left:    &mockColumnRef{table: "u", column: "id", colType: dukdb.TYPE_INTEGER},
			op:      OpEq,
			right:   &mockColumnRef{table: "o", column: "user_id", colType: dukdb.TYPE_INTEGER},
			resType: dukdb.TYPE_BOOLEAN,
		},
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "o", Column: "user_id", Type: dukdb.TYPE_INTEGER},
		},
	}

	const iterations = 1000
	start := time.Now()
	for range iterations {
		_, err := optimizer.Optimize(join)
		require.NoError(t, err)
	}
	elapsed := time.Since(start)
	avgTime := elapsed / iterations

	t.Logf("2-table join optimization: avg=%v (total %v for %d iterations)", avgTime, elapsed, iterations)
	// Joins are more complex, but should still be fast
	assert.Less(t, avgTime, 10*time.Millisecond, "2-table join optimization should be < 10ms")
}

// TestCardinalityEstimatorOverhead measures cardinality estimation overhead.
func TestCardinalityEstimatorOverhead(t *testing.T) {
	catalog := newMockCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{
		stats: &TableStatistics{
			RowCount:  100000,
			PageCount: 1000,
			Columns: []ColumnStatistics{
				{ColumnName: "id", ColumnType: dukdb.TYPE_INTEGER, DistinctCount: 100000},
				{ColumnName: "status", ColumnType: dukdb.TYPE_VARCHAR, DistinctCount: 10},
			},
		},
	})

	stats := NewStatisticsManager(catalog)
	estimator := NewCardinalityEstimator(stats)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	filter := &mockLogicalFilter{
		child: scan,
		condition: &mockBinaryExpr{
			left:    &mockColumnRef{table: "u", column: "status", colType: dukdb.TYPE_VARCHAR},
			op:      OpEq,
			right:   &mockLiteral{value: "active", valType: dukdb.TYPE_VARCHAR},
			resType: dukdb.TYPE_BOOLEAN,
		},
	}

	const iterations = 10000
	start := time.Now()
	for range iterations {
		_ = estimator.EstimateCardinality(filter)
	}
	elapsed := time.Since(start)
	avgTime := elapsed / iterations

	t.Logf("Cardinality estimation: avg=%v (total %v for %d iterations)", avgTime, elapsed, iterations)
	// Cardinality estimation should be very fast (sub-microsecond for simple cases)
	assert.Less(t, avgTime, 100*time.Microsecond, "Cardinality estimation should be < 100us")
}

// TestCostModelOverhead measures cost model computation overhead.
func TestCostModelOverhead(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	// Create a mock physical scan node
	mockScan := &mockPhysicalScan{
		schema:    "main",
		tableName: "users",
		rowCount:  10000,
		pageCount: 100,
	}

	const iterations = 10000
	start := time.Now()
	for range iterations {
		_ = costModel.EstimateCost(mockScan)
	}
	elapsed := time.Since(start)
	avgTime := elapsed / iterations

	t.Logf("Cost model estimation: avg=%v (total %v for %d iterations)", avgTime, elapsed, iterations)
	assert.Less(t, avgTime, 100*time.Microsecond, "Cost model estimation should be < 100us")
}

// mockPhysicalScan implements PhysicalScanNode for testing.
type mockPhysicalScan struct {
	schema    string
	tableName string
	rowCount  float64
	pageCount float64
}

func (s *mockPhysicalScan) PhysicalPlanType() string { return "PhysicalScan" }
func (s *mockPhysicalScan) PhysicalChildren() []PhysicalPlanNode {
	return nil
}
func (s *mockPhysicalScan) PhysicalOutputColumns() []PhysicalOutputColumn {
	return []PhysicalOutputColumn{
		{Table: s.tableName, Column: "id", Type: dukdb.TYPE_INTEGER},
	}
}
func (s *mockPhysicalScan) ScanSchema() string      { return s.schema }
func (s *mockPhysicalScan) ScanTableName() string   { return s.tableName }
func (s *mockPhysicalScan) ScanAlias() string       { return s.tableName }
func (s *mockPhysicalScan) ScanRowCount() float64   { return s.rowCount }
func (s *mockPhysicalScan) ScanPageCount() float64  { return s.pageCount }

// BenchmarkCardinalityEstimation benchmarks cardinality estimation.
func BenchmarkCardinalityEstimation(b *testing.B) {
	catalog := newMockCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{
		stats: &TableStatistics{
			RowCount: 100000,
			Columns: []ColumnStatistics{
				{ColumnName: "status", ColumnType: dukdb.TYPE_VARCHAR, DistinctCount: 10},
			},
		},
	})

	stats := NewStatisticsManager(catalog)
	estimator := NewCardinalityEstimator(stats)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
	}

	filter := &mockLogicalFilter{
		child: scan,
		condition: &mockBinaryExpr{
			left:  &mockColumnRef{table: "u", column: "status", colType: dukdb.TYPE_VARCHAR},
			op:    OpEq,
			right: &mockLiteral{value: "active", valType: dukdb.TYPE_VARCHAR},
		},
	}

	b.ResetTimer()
	for range b.N {
		_ = estimator.EstimateCardinality(filter)
	}
}

// BenchmarkCostModelScan benchmarks cost model for scan operations.
func BenchmarkCostModelScan(b *testing.B) {
	costModel := NewCostModel(DefaultCostConstants(), nil)

	mockScan := &mockPhysicalScan{
		schema:    "main",
		tableName: "users",
		rowCount:  10000,
		pageCount: 100,
	}

	b.ResetTimer()
	for range b.N {
		_ = costModel.EstimateCost(mockScan)
	}
}

// BenchmarkOptimizerSimpleScan benchmarks optimizer for simple scan.
func BenchmarkOptimizerSimpleScan(b *testing.B) {
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	b.ResetTimer()
	for range b.N {
		_, _ = optimizer.Optimize(scan)
	}
}

// BenchmarkOptimizerSimpleFilter benchmarks optimizer for simple filter.
func BenchmarkOptimizerSimpleFilter(b *testing.B) {
	catalog := newMockCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{
		stats: &TableStatistics{
			RowCount: 10000,
			Columns: []ColumnStatistics{
				{ColumnName: "status", ColumnType: dukdb.TYPE_VARCHAR, DistinctCount: 5},
			},
		},
	})

	optimizer := NewCostBasedOptimizer(catalog)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	filter := &mockLogicalFilter{
		child: scan,
		condition: &mockBinaryExpr{
			left:  &mockColumnRef{table: "u", column: "status", colType: dukdb.TYPE_VARCHAR},
			op:    OpEq,
			right: &mockLiteral{value: "active", valType: dukdb.TYPE_VARCHAR},
		},
	}

	b.ResetTimer()
	for range b.N {
		_, _ = optimizer.Optimize(filter)
	}
}

// BenchmarkOptimizerSimpleAggregate benchmarks optimizer for simple aggregate.
func BenchmarkOptimizerSimpleAggregate(b *testing.B) {
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "category", Type: dukdb.TYPE_VARCHAR},
		},
	}

	aggregate := &mockLogicalAggregate{
		child: scan,
		groupBy: []ExprNode{
			&mockColumnRef{table: "u", column: "category", colType: dukdb.TYPE_VARCHAR},
		},
		columns: []OutputColumn{
			{Column: "count", Type: dukdb.TYPE_BIGINT},
		},
	}

	b.ResetTimer()
	for range b.N {
		_, _ = optimizer.Optimize(aggregate)
	}
}

// BenchmarkOptimizerSortLimit benchmarks optimizer for sort with limit.
func BenchmarkOptimizerSortLimit(b *testing.B) {
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	sort := &mockLogicalSort{child: scan}
	limit := &mockLogicalLimit{child: sort, limit: 10, offset: 0}

	b.ResetTimer()
	for range b.N {
		_, _ = optimizer.Optimize(limit)
	}
}

// BenchmarkOptimizerTwoTableJoin benchmarks optimizer for 2-table join.
func BenchmarkOptimizerTwoTableJoin(b *testing.B) {
	catalog := newMockCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{
		stats: &TableStatistics{RowCount: 10000, PageCount: 100},
	})
	catalog.AddTable("main", "orders", &mockTableInfo{
		stats: &TableStatistics{RowCount: 50000, PageCount: 500},
	})

	optimizer := NewCostBasedOptimizer(catalog)

	usersScan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	ordersScan := &mockLogicalScan{
		schema:    "main",
		tableName: "orders",
		alias:     "o",
		columns: []OutputColumn{
			{Table: "o", Column: "user_id", Type: dukdb.TYPE_INTEGER},
		},
	}

	join := &mockLogicalJoin{
		left:     usersScan,
		right:    ordersScan,
		joinType: JoinTypeInner,
		condition: &mockBinaryExpr{
			left:  &mockColumnRef{table: "u", column: "id", colType: dukdb.TYPE_INTEGER},
			op:    OpEq,
			right: &mockColumnRef{table: "o", column: "user_id", colType: dukdb.TYPE_INTEGER},
		},
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "o", Column: "user_id", Type: dukdb.TYPE_INTEGER},
		},
	}

	b.ResetTimer()
	for range b.N {
		_, _ = optimizer.Optimize(join)
	}
}

// BenchmarkOptimizerThreeTableJoin benchmarks optimizer for 3-table join.
func BenchmarkOptimizerThreeTableJoin(b *testing.B) {
	catalog := newMockCatalog()
	catalog.AddTable("main", "a", &mockTableInfo{
		stats: &TableStatistics{RowCount: 1000, PageCount: 10},
	})
	catalog.AddTable("main", "b", &mockTableInfo{
		stats: &TableStatistics{RowCount: 5000, PageCount: 50},
	})
	catalog.AddTable("main", "c", &mockTableInfo{
		stats: &TableStatistics{RowCount: 10000, PageCount: 100},
	})

	optimizer := NewCostBasedOptimizer(catalog)

	scanA := &mockLogicalScan{schema: "main", tableName: "a", alias: "a",
		columns: []OutputColumn{{Table: "a", Column: "id", Type: dukdb.TYPE_INTEGER}}}
	scanB := &mockLogicalScan{schema: "main", tableName: "b", alias: "b",
		columns: []OutputColumn{{Table: "b", Column: "a_id", Type: dukdb.TYPE_INTEGER}}}
	scanC := &mockLogicalScan{schema: "main", tableName: "c", alias: "c",
		columns: []OutputColumn{{Table: "c", Column: "b_id", Type: dukdb.TYPE_INTEGER}}}

	joinAB := &mockLogicalJoin{
		left: scanA, right: scanB, joinType: JoinTypeInner,
		condition: &mockBinaryExpr{
			left:  &mockColumnRef{table: "a", column: "id", colType: dukdb.TYPE_INTEGER},
			op:    OpEq,
			right: &mockColumnRef{table: "b", column: "a_id", colType: dukdb.TYPE_INTEGER},
		},
		columns: []OutputColumn{
			{Table: "a", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "b", Column: "a_id", Type: dukdb.TYPE_INTEGER},
		},
	}

	joinABC := &mockLogicalJoin{
		left: joinAB, right: scanC, joinType: JoinTypeInner,
		condition: &mockBinaryExpr{
			left:  &mockColumnRef{table: "b", column: "a_id", colType: dukdb.TYPE_INTEGER},
			op:    OpEq,
			right: &mockColumnRef{table: "c", column: "b_id", colType: dukdb.TYPE_INTEGER},
		},
		columns: []OutputColumn{
			{Table: "a", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "b", Column: "a_id", Type: dukdb.TYPE_INTEGER},
			{Table: "c", Column: "b_id", Type: dukdb.TYPE_INTEGER},
		},
	}

	b.ResetTimer()
	for range b.N {
		_, _ = optimizer.Optimize(joinABC)
	}
}
