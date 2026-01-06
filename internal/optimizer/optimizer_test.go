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
