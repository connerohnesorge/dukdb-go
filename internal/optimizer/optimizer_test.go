package optimizer

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Note: Mock types (mockCatalog, mockTableInfo, mockColumnInfo, mockLogicalScan,
// mockLogicalJoin, etc.) are defined in cardinality_test.go and reused here.

func TestNewCostBasedOptimizer(t *testing.T) {
	catalog := newMockCatalog()
	optimizer := NewCostBasedOptimizer(catalog)

	require.NotNil(t, optimizer)
	assert.True(t, optimizer.IsEnabled())
	assert.NotNil(t, optimizer.GetStatisticsManager())
	assert.NotNil(t, optimizer.GetCardinalityEstimator())
	assert.NotNil(t, optimizer.GetCostModel())
	assert.NotNil(t, optimizer.GetJoinOrderOptimizer())
	assert.NotNil(t, optimizer.GetPlanEnumerator())
}

func TestOptimizerEnableDisable(t *testing.T) {
	optimizer := NewCostBasedOptimizer(nil)

	assert.True(t, optimizer.IsEnabled())

	optimizer.SetEnabled(false)
	assert.False(t, optimizer.IsEnabled())

	optimizer.SetEnabled(true)
	assert.True(t, optimizer.IsEnabled())
}

func TestOptimizeNilPlan(t *testing.T) {
	optimizer := NewCostBasedOptimizer(nil)

	result, err := optimizer.Optimize(nil)

	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestOptimizeDisabled(t *testing.T) {
	catalog := newMockCatalog()
	optimizer := NewCostBasedOptimizer(catalog)
	optimizer.SetEnabled(false)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "u", Column: "name", Type: dukdb.TYPE_VARCHAR},
		},
	}

	result, err := optimizer.Optimize(scan)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, scan, result.Plan)
	assert.NotNil(t, result.JoinHints)
	assert.NotNil(t, result.AccessHints)
}

func TestOptimizeSimpleQuery(t *testing.T) {
	catalog := newMockCatalog()

	// Add table with statistics
	stats := &TableStatistics{
		RowCount:  1000,
		PageCount: 10,
	}
	catalog.AddTable("main", "users", &mockTableInfo{
		stats: stats,
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

	result, err := optimizer.Optimize(scan)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, scan, result.Plan)
	assert.Greater(t, result.EstimatedCost.TotalCost, 0.0)
	assert.Greater(t, result.EstimatedCost.OutputRows, 0.0)
	assert.Empty(t, result.JoinHints) // No joins in simple query
	assert.Len(t, result.AccessHints, 1)
	assert.Equal(t, PlanTypeSeqScan, result.AccessHints["users"].Method)
}

func TestOptimizeQueryWithJoin(t *testing.T) {
	catalog := newMockCatalog()

	// Add tables with statistics
	usersStats := &TableStatistics{
		RowCount:  1000,
		PageCount: 10,
	}
	ordersStats := &TableStatistics{
		RowCount:  5000,
		PageCount: 50,
	}
	catalog.AddTable("main", "users", &mockTableInfo{stats: usersStats})
	catalog.AddTable("main", "orders", &mockTableInfo{stats: ordersStats})

	optimizer := NewCostBasedOptimizer(catalog)

	// Create a join plan
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
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "o", Column: "user_id", Type: dukdb.TYPE_INTEGER},
		},
	}

	result, err := optimizer.Optimize(join)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, join, result.Plan)
	assert.Greater(t, result.EstimatedCost.TotalCost, 0.0)
	// Access hints should contain both tables
	assert.Len(t, result.AccessHints, 2)
}

func TestCountJoins(t *testing.T) {
	testCases := []struct {
		name     string
		plan     LogicalPlanNode
		expected int
	}{
		{
			name:     "nil plan",
			plan:     nil,
			expected: 0,
		},
		{
			name: "simple scan",
			plan: &mockLogicalScan{
				tableName: "users",
			},
			expected: 0,
		},
		{
			name: "single join",
			plan: &mockLogicalJoin{
				left:  &mockLogicalScan{tableName: "users"},
				right: &mockLogicalScan{tableName: "orders"},
			},
			expected: 1,
		},
		{
			name: "nested joins",
			plan: &mockLogicalJoin{
				left: &mockLogicalJoin{
					left:  &mockLogicalScan{tableName: "users"},
					right: &mockLogicalScan{tableName: "orders"},
				},
				right: &mockLogicalScan{tableName: "products"},
			},
			expected: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			count := countJoins(tc.plan)
			assert.Equal(t, tc.expected, count)
		})
	}
}

func TestExtractTables(t *testing.T) {
	scan1 := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
	}

	scan2 := &mockLogicalScan{
		schema:    "main",
		tableName: "orders",
		alias:     "o",
	}

	join := &mockLogicalJoin{
		left:  scan1,
		right: scan2,
	}

	tables := extractTables(join)

	assert.Len(t, tables, 2)

	// Check that both tables are present
	tableNames := make(map[string]bool)
	for _, tbl := range tables {
		tableNames[tbl.Table] = true
	}
	assert.True(t, tableNames["users"])
	assert.True(t, tableNames["orders"])
}

func TestExtractTablesSkipsTableFunctions(t *testing.T) {
	regularScan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
	}

	tableFuncScan := &mockLogicalScan{
		schema:          "",
		tableName:       "read_csv",
		alias:           "csv",
		isTableFunction: true,
	}

	join := &mockLogicalJoin{
		left:  regularScan,
		right: tableFuncScan,
	}

	tables := extractTables(join)

	// Should only contain the regular table, not the table function
	assert.Len(t, tables, 1)
	assert.Equal(t, "users", tables[0].Table)
}

func TestLog2(t *testing.T) {
	testCases := []struct {
		input    float64
		expected float64
		delta    float64
	}{
		{1, 0, 0.01},
		{2, 1, 0.01},
		{4, 2, 0.01},
		{8, 3, 0.01},
		{16, 4, 0.01},
		{1024, 10, 0.01},
		{0.5, -1, 0.01},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			result := log2(tc.input)
			assert.InDelta(t, tc.expected, result, tc.delta)
		})
	}
}

func TestLog2EdgeCases(t *testing.T) {
	// Zero and negative values should return 0
	assert.Equal(t, 0.0, log2(0))
	assert.Equal(t, 0.0, log2(-1))
}

func TestOptimizedPlanTypes(t *testing.T) {
	// Test JoinHint struct
	hint := JoinHint{
		Method:    PlanTypeHashJoin,
		BuildSide: "right",
	}
	assert.Equal(t, PlanTypeHashJoin, hint.Method)
	assert.Equal(t, "right", hint.BuildSide)

	// Test AccessHint struct
	accessHint := AccessHint{
		Method:    PlanTypeIndexScan,
		IndexName: "users_pkey",
	}
	assert.Equal(t, PlanTypeIndexScan, accessHint.Method)
	assert.Equal(t, "users_pkey", accessHint.IndexName)
}

func TestEstimatePlanCost(t *testing.T) {
	catalog := newMockCatalog()

	stats := &TableStatistics{
		RowCount:  1000,
		PageCount: 10,
	}
	catalog.AddTable("main", "users", &mockTableInfo{stats: stats})

	optimizer := NewCostBasedOptimizer(catalog)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	cost := optimizer.EstimatePlanCost(scan)

	assert.Greater(t, cost.TotalCost, 0.0)
	assert.Greater(t, cost.OutputRows, 0.0)
	assert.Greater(t, cost.OutputWidth, int32(0))
}

func TestOptimizeWithNoStatistics(t *testing.T) {
	// Test with nil catalog provider
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	result, err := optimizer.Optimize(scan)

	require.NoError(t, err)
	require.NotNil(t, result)
	// Should still produce valid results using defaults
	assert.Greater(t, result.EstimatedCost.TotalCost, 0.0)
}

func TestOptimizeComplexPlan(t *testing.T) {
	catalog := newMockCatalog()

	// Add table with statistics
	stats := &TableStatistics{
		RowCount:  10000,
		PageCount: 100,
	}
	catalog.AddTable("main", "users", &mockTableInfo{stats: stats})

	optimizer := NewCostBasedOptimizer(catalog)

	// Create a complex plan: Project -> Filter -> Scan
	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "u", Column: "name", Type: dukdb.TYPE_VARCHAR},
		},
	}

	filter := &mockLogicalFilter{
		child: scan,
	}

	project := &mockLogicalProject{
		child:   filter,
		columns: scan.columns,
	}

	result, err := optimizer.Optimize(project)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Greater(t, result.EstimatedCost.TotalCost, 0.0)
}

func TestOptimizeWithSort(t *testing.T) {
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	sort := &mockLogicalSort{
		child: scan,
	}

	result, err := optimizer.Optimize(sort)

	require.NoError(t, err)
	require.NotNil(t, result)
	// Sort cost should be higher than just scan
	scanResult, _ := optimizer.Optimize(scan)
	assert.Greater(t, result.EstimatedCost.TotalCost, scanResult.EstimatedCost.TotalCost)
}

func TestOptimizeWithLimit(t *testing.T) {
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	limit := &mockLogicalLimit{
		child:  scan,
		limit:  10,
		offset: 0,
	}

	result, err := optimizer.Optimize(limit)

	require.NoError(t, err)
	require.NotNil(t, result)
	// Limit should reduce estimated cost
	scanResult, _ := optimizer.Optimize(scan)
	assert.LessOrEqual(t, result.EstimatedCost.TotalCost, scanResult.EstimatedCost.TotalCost)
}

func TestOptimizeWithAggregate(t *testing.T) {
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	agg := &mockLogicalAggregate{
		child:   scan,
		columns: []OutputColumn{{Column: "count", Type: dukdb.TYPE_BIGINT}},
	}

	result, err := optimizer.Optimize(agg)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Greater(t, result.EstimatedCost.TotalCost, 0.0)
}

func TestOptimizeWithDistinct(t *testing.T) {
	optimizer := NewCostBasedOptimizer(nil)

	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
		},
	}

	distinct := &mockLogicalDistinct{
		child: scan,
	}

	result, err := optimizer.Optimize(distinct)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Greater(t, result.EstimatedCost.TotalCost, 0.0)
}

// Benchmark optimization
func BenchmarkOptimizeSimpleQuery(b *testing.B) {
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

func BenchmarkOptimizeJoinQuery(b *testing.B) {
	optimizer := NewCostBasedOptimizer(nil)

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

// =============================================================================
// Range Scan Integration Tests
// =============================================================================

// TestExtractPredicatesFromCondition_RangePredicate tests predicate extraction.
func TestExtractPredicatesFromCondition_RangePredicate(t *testing.T) {
	optimizer := NewCostBasedOptimizer(nil)

	// Create a simple range predicate: age > 25
	condition := &mockExprBinaryPredicate{
		left:  &mockExprColumnRef{table: "users", column: "age"},
		right: &mockExprLiteral{value: 25},
		op:    OpGt,
	}

	predicates := optimizer.extractPredicatesFromCondition(condition)
	require.Len(t, predicates, 1)

	// Verify the predicate implements the right interfaces
	pred := predicates[0]
	assert.NotNil(t, pred)

	// Check it implements BinaryPredicateExpr
	binPred, ok := pred.(BinaryPredicateExpr)
	require.True(t, ok, "predicate should implement BinaryPredicateExpr")
	assert.Equal(t, OpGt, binPred.PredicateOperator())

	// Check left is column reference
	colRef, ok := binPred.PredicateLeft().(ColumnRefPredicateExpr)
	require.True(t, ok, "left should be ColumnRefPredicateExpr")
	assert.Equal(t, "age", colRef.PredicateColumn())
}

// TestSelectAccessHintForFilteredScan_RangePredicate tests that range predicates
// produce AccessHints with IsRangeScan and RangeBounds populated.
func TestSelectAccessHintForFilteredScan_RangePredicate(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{
		stats: &TableStatistics{
			RowCount:  10000,
			PageCount: 100,
		},
	})

	// Create an index on the 'age' column
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_age",
		table:    "users",
		columns:  []string{"age"},
		isUnique: false,
	})

	optimizer := NewCostBasedOptimizer(catalog)

	// Create a scan with a filter: age > 25
	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "u", Column: "age", Type: dukdb.TYPE_INTEGER},
		},
	}

	filter := &mockLogicalFilterWithCondition{
		child: scan,
		condition: &mockExprBinaryPredicate{
			left:  &mockExprColumnRef{table: "users", column: "age"},
			right: &mockExprLiteral{value: 25},
			op:    OpGt,
		},
	}

	hint := optimizer.selectAccessHintForFilteredScan(filter, scan)

	// Should select index range scan
	assert.Equal(t, PlanTypeIndexRangeScan, hint.Method)
	assert.Equal(t, "idx_users_age", hint.IndexName)
	assert.True(t, hint.IsRangeScan)

	// Should have range bounds
	require.NotNil(t, hint.RangeBounds)
	assert.NotNil(t, hint.RangeBounds.LowerBound)
	assert.Nil(t, hint.RangeBounds.UpperBound)
	assert.False(t, hint.RangeBounds.LowerInclusive) // > is exclusive
	assert.Equal(t, 0, hint.RangeBounds.RangeColumnIndex)
}

// TestSelectAccessHintForFilteredScan_BetweenPredicate tests BETWEEN predicate handling.
func TestSelectAccessHintForFilteredScan_BetweenPredicate(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "products", &mockTableInfo{
		stats: &TableStatistics{
			RowCount:  5000,
			PageCount: 50,
		},
	})

	catalog.AddIndex("main", "products", &mockIndexDef{
		name:     "idx_products_price",
		table:    "products",
		columns:  []string{"price"},
		isUnique: false,
	})

	optimizer := NewCostBasedOptimizer(catalog)

	// Create a scan with a filter: price BETWEEN 100 AND 500
	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "products",
		alias:     "p",
		columns: []OutputColumn{
			{Table: "p", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "p", Column: "price", Type: dukdb.TYPE_DOUBLE},
		},
	}

	filter := &mockLogicalFilterWithCondition{
		child: scan,
		condition: &mockExprBetween{
			expr:       &mockExprColumnRef{table: "products", column: "price"},
			low:        &mockExprLiteral{value: 100},
			high:       &mockExprLiteral{value: 500},
			notBetween: false,
		},
	}

	hint := optimizer.selectAccessHintForFilteredScan(filter, scan)

	// Should select index range scan
	assert.Equal(t, PlanTypeIndexRangeScan, hint.Method)
	assert.Equal(t, "idx_products_price", hint.IndexName)
	assert.True(t, hint.IsRangeScan)

	// Should have both lower and upper bounds
	require.NotNil(t, hint.RangeBounds)
	assert.NotNil(t, hint.RangeBounds.LowerBound)
	assert.NotNil(t, hint.RangeBounds.UpperBound)
	assert.True(t, hint.RangeBounds.LowerInclusive) // BETWEEN is inclusive
	assert.True(t, hint.RangeBounds.UpperInclusive)
}

// TestSelectAccessHintForFilteredScan_CompositeIndexWithRange tests composite index
// with equality prefix and range on the next column.
func TestSelectAccessHintForFilteredScan_CompositeIndexWithRange(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "orders", &mockTableInfo{
		stats: &TableStatistics{
			RowCount:  50000,
			PageCount: 500,
		},
	})

	// Composite index on (customer_id, order_date)
	catalog.AddIndex("main", "orders", &mockIndexDef{
		name:     "idx_orders_customer_date",
		table:    "orders",
		columns:  []string{"customer_id", "order_date"},
		isUnique: false,
	})

	optimizer := NewCostBasedOptimizer(catalog)

	// Create a scan with filter: customer_id = 123 AND order_date > '2024-01-01'
	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "orders",
		alias:     "o",
		columns: []OutputColumn{
			{Table: "o", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "o", Column: "customer_id", Type: dukdb.TYPE_INTEGER},
			{Table: "o", Column: "order_date", Type: dukdb.TYPE_DATE},
		},
	}

	// AND condition: customer_id = 123 AND order_date > '2024-01-01'
	filter := &mockLogicalFilterWithCondition{
		child: scan,
		condition: &mockExprBinaryPredicate{
			left: &mockExprBinaryPredicate{
				left:  &mockExprColumnRef{table: "orders", column: "customer_id"},
				right: &mockExprLiteral{value: 123},
				op:    OpEq,
			},
			right: &mockExprBinaryPredicate{
				left:  &mockExprColumnRef{table: "orders", column: "order_date"},
				right: &mockExprLiteral{value: "2024-01-01"},
				op:    OpGt,
			},
			op: OpAnd,
		},
	}

	hint := optimizer.selectAccessHintForFilteredScan(filter, scan)

	// Should select index range scan
	assert.Equal(t, PlanTypeIndexRangeScan, hint.Method)
	assert.Equal(t, "idx_orders_customer_date", hint.IndexName)
	assert.True(t, hint.IsRangeScan)

	// Should have 2 matched columns (customer_id equality + order_date range)
	assert.Equal(t, 2, hint.MatchedColumns)

	// Should have range bounds with column index 1 (order_date)
	require.NotNil(t, hint.RangeBounds)
	assert.Equal(t, 1, hint.RangeBounds.RangeColumnIndex)
	assert.NotNil(t, hint.RangeBounds.LowerBound)
	assert.False(t, hint.RangeBounds.LowerInclusive) // > is exclusive
}

// TestSelectAccessHintForFilteredScan_EqualityPreferredOverRange tests that
// equality predicates are preferred over range predicates.
func TestSelectAccessHintForFilteredScan_EqualityPreferredOverRange(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{
		stats: &TableStatistics{
			RowCount:  10000,
			PageCount: 100,
		},
	})

	// Index on 'status' column
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_status",
		table:    "users",
		columns:  []string{"status"},
		isUnique: false,
	})

	optimizer := NewCostBasedOptimizer(catalog)

	// Create a scan with an equality predicate
	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "u", Column: "status", Type: dukdb.TYPE_VARCHAR},
		},
	}

	filter := &mockLogicalFilterWithCondition{
		child: scan,
		condition: &mockExprBinaryPredicate{
			left:  &mockExprColumnRef{table: "users", column: "status"},
			right: &mockExprLiteral{value: "active"},
			op:    OpEq,
		},
	}

	hint := optimizer.selectAccessHintForFilteredScan(filter, scan)

	// Should select regular index scan (point lookup), not range scan
	assert.Equal(t, PlanTypeIndexScan, hint.Method)
	assert.Equal(t, "idx_users_status", hint.IndexName)
	assert.False(t, hint.IsRangeScan)
	assert.Nil(t, hint.RangeBounds)
	assert.Len(t, hint.LookupKeys, 1)
}

// TestSelectAccessHintForFilteredScan_NoApplicableIndex tests fallback to seq scan.
func TestSelectAccessHintForFilteredScan_NoApplicableIndex(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{
		stats: &TableStatistics{
			RowCount:  10000,
			PageCount: 100,
		},
	})

	// Index on 'status' column only
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_status",
		table:    "users",
		columns:  []string{"status"},
		isUnique: false,
	})

	optimizer := NewCostBasedOptimizer(catalog)

	// Create a scan with a filter on a column without an index
	scan := &mockLogicalScan{
		schema:    "main",
		tableName: "users",
		alias:     "u",
		columns: []OutputColumn{
			{Table: "u", Column: "id", Type: dukdb.TYPE_INTEGER},
			{Table: "u", Column: "name", Type: dukdb.TYPE_VARCHAR},
		},
	}

	filter := &mockLogicalFilterWithCondition{
		child: scan,
		condition: &mockExprBinaryPredicate{
			left:  &mockExprColumnRef{table: "users", column: "name"},
			right: &mockExprLiteral{value: "John"},
			op:    OpEq,
		},
	}

	hint := optimizer.selectAccessHintForFilteredScan(filter, scan)

	// Should fallback to sequential scan
	assert.Equal(t, PlanTypeSeqScan, hint.Method)
	assert.Empty(t, hint.IndexName)
	assert.False(t, hint.IsRangeScan)
	assert.Nil(t, hint.RangeBounds)
}

// TestConvertIndexMatchRangeBounds tests the bounds conversion function.
func TestConvertIndexMatchRangeBounds(t *testing.T) {
	t.Run("nil bounds", func(t *testing.T) {
		result := convertIndexMatchRangeBounds(nil)
		assert.Nil(t, result)
	})

	t.Run("full bounds", func(t *testing.T) {
		bounds := &RangeScanBounds{
			LowerBound:       &mockLiteralPredicate{value: 10},
			UpperBound:       &mockLiteralPredicate{value: 20},
			LowerInclusive:   true,
			UpperInclusive:   false,
			RangeColumnIndex: 2,
		}

		result := convertIndexMatchRangeBounds(bounds)

		require.NotNil(t, result)
		assert.NotNil(t, result.LowerBound)
		assert.NotNil(t, result.UpperBound)
		assert.True(t, result.LowerInclusive)
		assert.False(t, result.UpperInclusive)
		assert.Equal(t, 2, result.RangeColumnIndex)
	})

	t.Run("lower bound only", func(t *testing.T) {
		bounds := &RangeScanBounds{
			LowerBound:       &mockLiteralPredicate{value: 10},
			LowerInclusive:   false,
			RangeColumnIndex: 0,
		}

		result := convertIndexMatchRangeBounds(bounds)

		require.NotNil(t, result)
		assert.NotNil(t, result.LowerBound)
		assert.Nil(t, result.UpperBound)
		assert.False(t, result.LowerInclusive)
		assert.Equal(t, 0, result.RangeColumnIndex)
	})

	t.Run("upper bound only", func(t *testing.T) {
		bounds := &RangeScanBounds{
			UpperBound:       &mockLiteralPredicate{value: 100},
			UpperInclusive:   true,
			RangeColumnIndex: 1,
		}

		result := convertIndexMatchRangeBounds(bounds)

		require.NotNil(t, result)
		assert.Nil(t, result.LowerBound)
		assert.NotNil(t, result.UpperBound)
		assert.True(t, result.UpperInclusive)
		assert.Equal(t, 1, result.RangeColumnIndex)
	})
}

// mockLogicalFilterWithCondition is a mock filter that supports FilterCondition().
type mockLogicalFilterWithCondition struct {
	child     LogicalPlanNode
	condition ExprNode
}

func (f *mockLogicalFilterWithCondition) PlanType() string { return "LogicalFilter" }
func (f *mockLogicalFilterWithCondition) PlanChildren() []LogicalPlanNode {
	return []LogicalPlanNode{f.child}
}
func (f *mockLogicalFilterWithCondition) PlanOutputColumns() []OutputColumn {
	if f.child != nil {
		return f.child.PlanOutputColumns()
	}
	return nil
}
func (f *mockLogicalFilterWithCondition) FilterChild() LogicalPlanNode { return f.child }
func (f *mockLogicalFilterWithCondition) FilterCondition() ExprNode    { return f.condition }

// mockExprBinaryPredicate implements both ExprNode, BinaryExprNode, and PredicateExpr for testing.
// This is a dual-interface mock needed for selectAccessHintForFilteredScan tests.
type mockExprBinaryPredicate struct {
	left  ExprNode
	right ExprNode
	op    BinaryOp
}

func (m *mockExprBinaryPredicate) ExprType() string           { return "BinaryExpr" }
func (m *mockExprBinaryPredicate) ExprResultType() dukdb.Type { return dukdb.TYPE_BOOLEAN }
func (m *mockExprBinaryPredicate) Left() ExprNode             { return m.left }
func (m *mockExprBinaryPredicate) Right() ExprNode            { return m.right }
func (m *mockExprBinaryPredicate) Operator() BinaryOp         { return m.op }

// PredicateExpr interface
func (m *mockExprBinaryPredicate) PredicateType() string          { return "BinaryPredicate" }
func (m *mockExprBinaryPredicate) PredicateLeft() PredicateExpr   { return m.left.(PredicateExpr) }
func (m *mockExprBinaryPredicate) PredicateRight() PredicateExpr  { return m.right.(PredicateExpr) }
func (m *mockExprBinaryPredicate) PredicateOperator() BinaryOp    { return m.op }

// mockExprColumnRef implements both ExprNode and ColumnRefPredicateExpr for testing.
type mockExprColumnRef struct {
	table  string
	column string
}

func (m *mockExprColumnRef) ExprType() string           { return "ColumnRef" }
func (m *mockExprColumnRef) ExprResultType() dukdb.Type { return dukdb.TYPE_INTEGER }
func (m *mockExprColumnRef) PredicateType() string      { return "ColumnRef" }
func (m *mockExprColumnRef) PredicateTable() string     { return m.table }
func (m *mockExprColumnRef) PredicateColumn() string    { return m.column }

// mockExprLiteral implements both ExprNode and PredicateExpr for testing.
type mockExprLiteral struct {
	value any
}

func (m *mockExprLiteral) ExprType() string           { return "Literal" }
func (m *mockExprLiteral) ExprResultType() dukdb.Type { return dukdb.TYPE_INTEGER }
func (m *mockExprLiteral) PredicateType() string      { return "Literal" }

// mockExprBetween implements both ExprNode and BetweenPredicateExpr for testing.
type mockExprBetween struct {
	expr       ExprNode
	low        ExprNode
	high       ExprNode
	notBetween bool
}

func (m *mockExprBetween) ExprType() string                { return "Between" }
func (m *mockExprBetween) ExprResultType() dukdb.Type      { return dukdb.TYPE_BOOLEAN }
func (m *mockExprBetween) PredicateType() string           { return "Between" }
func (m *mockExprBetween) PredicateBetweenExpr() PredicateExpr { return m.expr.(PredicateExpr) }
func (m *mockExprBetween) PredicateLowBound() PredicateExpr    { return m.low.(PredicateExpr) }
func (m *mockExprBetween) PredicateHighBound() PredicateExpr   { return m.high.(PredicateExpr) }
func (m *mockExprBetween) PredicateIsNotBetween() bool         { return m.notBetween }
