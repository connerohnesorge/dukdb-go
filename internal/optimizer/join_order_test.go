package optimizer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewJoinOrderOptimizer(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	assert.NotNil(t, optimizer)
	assert.Equal(t, DefaultDPThreshold, optimizer.dpThreshold)
	assert.Equal(t, DefaultPairLimit, optimizer.pairLimit)
}

func TestJoinOrderOptimizer_SetThresholds(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	optimizer.SetDPThreshold(8)
	assert.Equal(t, 8, optimizer.dpThreshold)

	optimizer.SetPairLimit(5000)
	assert.Equal(t, 5000, optimizer.pairLimit)

	// Invalid values should not change thresholds
	optimizer.SetDPThreshold(0)
	assert.Equal(t, 8, optimizer.dpThreshold)

	optimizer.SetPairLimit(-1)
	assert.Equal(t, 5000, optimizer.pairLimit)
}

func TestJoinOrderOptimizer_EmptyTables(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	plan, err := optimizer.OptimizeJoinOrder([]TableRef{}, []JoinPredicate{})

	require.NoError(t, err)
	assert.NotNil(t, plan)
	assert.Empty(t, plan.Tables)
	assert.Empty(t, plan.JoinOrder)
}

func TestJoinOrderOptimizer_SingleTable(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	tables := []TableRef{
		{Table: "orders", Cardinality: 1000, Width: 50},
	}

	plan, err := optimizer.OptimizeJoinOrder(tables, []JoinPredicate{})

	require.NoError(t, err)
	assert.NotNil(t, plan)
	assert.Equal(t, []string{"orders"}, plan.Tables)
	assert.Empty(t, plan.JoinOrder)
	assert.Equal(t, float64(1000), plan.TotalCost.OutputRows)
}

func TestJoinOrderOptimizer_TwoTableJoin(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	tables := []TableRef{
		{Table: "orders", Cardinality: 10000, Width: 50},
		{Table: "customers", Cardinality: 1000, Width: 100},
	}

	predicates := []JoinPredicate{
		{
			LeftTable:   "orders",
			LeftColumn:  "customer_id",
			RightTable:  "customers",
			RightColumn: "id",
			IsEquality:  true,
		},
	}

	plan, err := optimizer.OptimizeJoinOrder(tables, predicates)

	require.NoError(t, err)
	assert.NotNil(t, plan)
	assert.Len(t, plan.Tables, 2)
	assert.Len(t, plan.JoinOrder, 1)

	// Verify the join step has the predicate
	step := plan.JoinOrder[0]
	assert.NotNil(t, step.Predicate)
	assert.True(t, step.Predicate.IsEquality)
}

func TestJoinOrderOptimizer_ThreeTableJoin(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	// Setup: orders (10000) -> customers (1000) -> regions (50)
	// Optimal order should start with smallest tables
	tables := []TableRef{
		{Table: "orders", Cardinality: 10000, Width: 50},
		{Table: "customers", Cardinality: 1000, Width: 100},
		{Table: "regions", Cardinality: 50, Width: 30},
	}

	predicates := []JoinPredicate{
		{
			LeftTable:   "orders",
			LeftColumn:  "customer_id",
			RightTable:  "customers",
			RightColumn: "id",
			IsEquality:  true,
		},
		{
			LeftTable:   "customers",
			LeftColumn:  "region_id",
			RightTable:  "regions",
			RightColumn: "id",
			IsEquality:  true,
		},
	}

	plan, err := optimizer.OptimizeJoinOrder(tables, predicates)

	require.NoError(t, err)
	assert.NotNil(t, plan)
	assert.Len(t, plan.Tables, 3)
	assert.Len(t, plan.JoinOrder, 2)

	// The cost should be positive
	assert.Greater(t, plan.TotalCost.TotalCost, float64(0))
}

func TestJoinOrderOptimizer_GreedyFallback(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)
	optimizer.SetDPThreshold(2) // Force greedy for 3+ tables

	tables := []TableRef{
		{Table: "t1", Cardinality: 100, Width: 20},
		{Table: "t2", Cardinality: 200, Width: 30},
		{Table: "t3", Cardinality: 50, Width: 10},
	}

	predicates := []JoinPredicate{
		{LeftTable: "t1", LeftColumn: "id", RightTable: "t2", RightColumn: "t1_id", IsEquality: true},
		{LeftTable: "t2", LeftColumn: "id", RightTable: "t3", RightColumn: "t2_id", IsEquality: true},
	}

	plan, err := optimizer.OptimizeJoinOrder(tables, predicates)

	require.NoError(t, err)
	assert.NotNil(t, plan)
	// Greedy should produce a valid plan
	assert.NotEmpty(t, plan.Tables)
}

func TestJoinOrderOptimizer_BuildSideSelection(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	// Left relation is smaller - should be build side
	left := &JoinRelation{
		Tables:      []string{"small"},
		Cardinality: 100,
		Width:       20,
	}
	right := &JoinRelation{
		Tables:      []string{"large"},
		Cardinality: 10000,
		Width:       50,
	}

	buildSide := optimizer.selectBuildSide(left, right)
	assert.Equal(t, "left", buildSide)

	// Now right is smaller
	left.Cardinality = 10000
	left.Width = 50
	right.Cardinality = 100
	right.Width = 20

	buildSide = optimizer.selectBuildSide(left, right)
	assert.Equal(t, "right", buildSide)
}

func TestSelectBuildSide(t *testing.T) {
	tests := []struct {
		name        string
		leftRows    float64
		leftWidth   float64
		rightRows   float64
		rightWidth  float64
		buildIsLeft bool
	}{
		{
			name:        "left smaller by rows",
			leftRows:    100,
			leftWidth:   20,
			rightRows:   1000,
			rightWidth:  20,
			buildIsLeft: true,
		},
		{
			name:        "right smaller by rows",
			leftRows:    1000,
			leftWidth:   20,
			rightRows:   100,
			rightWidth:  20,
			buildIsLeft: false,
		},
		{
			name:        "left smaller by width",
			leftRows:    100,
			leftWidth:   10,
			rightRows:   100,
			rightWidth:  100,
			buildIsLeft: true,
		},
		{
			name:        "equal memory",
			leftRows:    100,
			leftWidth:   20,
			rightRows:   100,
			rightWidth:  20,
			buildIsLeft: true,
		},
		{
			name:        "right smaller by memory product",
			leftRows:    1000,
			leftWidth:   10,
			rightRows:   100,
			rightWidth:  50,
			buildIsLeft: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SelectBuildSide(tt.leftRows, tt.leftWidth, tt.rightRows, tt.rightWidth)
			assert.Equal(t, tt.buildIsLeft, result)
		})
	}
}

func TestJoinOrderOptimizer_OuterJoinConstraints(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	// Tables with outer join constraint
	tables := []TableRef{
		{Table: "left_table", Cardinality: 1000, Width: 50, JoinType: JoinTypeInner},
		{Table: "right_table", Cardinality: 500, Width: 30, JoinType: JoinTypeLeft},
	}

	predicates := []JoinPredicate{
		{
			LeftTable:   "left_table",
			LeftColumn:  "id",
			RightTable:  "right_table",
			RightColumn: "left_id",
			IsEquality:  true,
		},
	}

	// Should use greedy due to outer join
	plan, err := optimizer.OptimizeJoinOrder(tables, predicates)

	require.NoError(t, err)
	assert.NotNil(t, plan)
}

func TestCanReorderOuterJoin(t *testing.T) {
	tests := []struct {
		name         string
		joinType     JoinType
		isMovingLeft bool
		canReorder   bool
	}{
		{"inner join - left", JoinTypeInner, true, true},
		{"inner join - right", JoinTypeInner, false, true},
		{"cross join - left", JoinTypeCross, true, true},
		{"cross join - right", JoinTypeCross, false, true},
		{"left join - moving left", JoinTypeLeft, true, false},
		{"left join - moving right", JoinTypeLeft, false, true},
		{"right join - moving left", JoinTypeRight, true, true},
		{"right join - moving right", JoinTypeRight, false, false},
		{"full join - left", JoinTypeFull, true, false},
		{"full join - right", JoinTypeFull, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CanReorderOuterJoin(tt.joinType, tt.isMovingLeft)
			assert.Equal(t, tt.canReorder, result)
		})
	}
}

func TestJoinOrderOptimizer_JoinGraph(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	tables := []TableRef{
		{Table: "a", Cardinality: 100, Width: 10},
		{Table: "b", Cardinality: 200, Width: 20},
		{Table: "c", Cardinality: 300, Width: 30},
	}

	predicates := []JoinPredicate{
		{LeftTable: "a", LeftColumn: "id", RightTable: "b", RightColumn: "a_id", IsEquality: true},
		{LeftTable: "b", LeftColumn: "id", RightTable: "c", RightColumn: "b_id", IsEquality: true},
	}

	graph := optimizer.buildJoinGraph(tables, predicates)

	assert.Equal(t, []string{"a", "b", "c"}, graph.Tables)
	assert.Len(t, graph.Predicates, 2)

	// Check edges
	assert.Contains(t, graph.Edges["a"], "b")
	assert.Contains(t, graph.Edges["b"], "a")
	assert.Contains(t, graph.Edges["b"], "c")
	assert.Contains(t, graph.Edges["c"], "b")
}

func TestJoinOrderOptimizer_FindConnectingPredicate(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	predicates := []JoinPredicate{
		{LeftTable: "a", LeftColumn: "id", RightTable: "b", RightColumn: "a_id", IsEquality: true},
		{LeftTable: "b", LeftColumn: "id", RightTable: "c", RightColumn: "b_id", IsEquality: false},
	}

	// Find predicate between a and b
	pred := optimizer.findConnectingPredicate([]string{"a"}, []string{"b"}, predicates)
	assert.NotNil(t, pred)
	assert.Equal(t, "a", pred.LeftTable)
	assert.True(t, pred.IsEquality)

	// Find predicate between b and c (non-equality)
	pred = optimizer.findConnectingPredicate([]string{"b"}, []string{"c"}, predicates)
	assert.NotNil(t, pred)
	assert.Equal(t, "b", pred.LeftTable)
	assert.False(t, pred.IsEquality)

	// No predicate between a and c
	pred = optimizer.findConnectingPredicate([]string{"a"}, []string{"c"}, predicates)
	assert.Nil(t, pred)
}

func TestJoinOrderOptimizer_CrossJoin(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	tables := []TableRef{
		{Table: "a", Cardinality: 100, Width: 10},
		{Table: "b", Cardinality: 50, Width: 20},
	}

	// No predicates = cross join
	plan, err := optimizer.OptimizeJoinOrder(tables, []JoinPredicate{})

	require.NoError(t, err)
	assert.NotNil(t, plan)
	assert.Len(t, plan.Tables, 2)
	assert.Len(t, plan.JoinOrder, 1)

	// Cross join step should have nil predicate
	assert.Nil(t, plan.JoinOrder[0].Predicate)
}

func TestJoinOrderOptimizer_TableWithAlias(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	tables := []TableRef{
		{Table: "orders", Alias: "o", Cardinality: 10000, Width: 50},
		{Table: "customers", Alias: "c", Cardinality: 1000, Width: 100},
	}

	predicates := []JoinPredicate{
		{
			LeftTable:   "o",
			LeftColumn:  "customer_id",
			RightTable:  "c",
			RightColumn: "id",
			IsEquality:  true,
		},
	}

	plan, err := optimizer.OptimizeJoinOrder(tables, predicates)

	require.NoError(t, err)
	assert.NotNil(t, plan)
	// Tables should use aliases
	assert.Contains(t, plan.Tables, "o")
	assert.Contains(t, plan.Tables, "c")
}

func TestJoinOrderOptimizer_EnumerateSubsets(t *testing.T) {
	optimizer := &JoinOrderOptimizer{}

	// C(4, 2) = 6
	subsets := optimizer.enumerateSubsets(4, 2)
	assert.Len(t, subsets, 6)

	// C(5, 3) = 10
	subsets = optimizer.enumerateSubsets(5, 3)
	assert.Len(t, subsets, 10)

	// C(3, 1) = 3
	subsets = optimizer.enumerateSubsets(3, 1)
	assert.Len(t, subsets, 3)
}

func TestJoinOrderOptimizer_CalculateJoinCost(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	left := &JoinRelation{
		Tables:      []string{"left"},
		Cardinality: 1000,
		Width:       50,
		Cost: PlanCost{
			TotalCost: 100,
		},
	}

	right := &JoinRelation{
		Tables:      []string{"right"},
		Cardinality: 500,
		Width:       30,
		Cost: PlanCost{
			TotalCost: 50,
		},
	}

	// With equality predicate
	pred := &JoinPredicate{IsEquality: true}
	cost := optimizer.calculateJoinCost(left, right, pred)

	assert.Greater(t, cost.TotalCost, float64(0))
	assert.Equal(t, int32(80), cost.OutputWidth) // 50 + 30

	// Without predicate (cross join)
	cost = optimizer.calculateJoinCost(left, right, nil)
	assert.Equal(t, float64(1000*500), cost.OutputRows) // Cross join
}

func TestJoinOrderOptimizer_LargeQuery(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)
	optimizer.SetDPThreshold(3) // Force greedy for 4+ tables

	// Create a chain of 5 tables
	tables := []TableRef{
		{Table: "t1", Cardinality: 1000, Width: 20},
		{Table: "t2", Cardinality: 500, Width: 30},
		{Table: "t3", Cardinality: 200, Width: 10},
		{Table: "t4", Cardinality: 100, Width: 15},
		{Table: "t5", Cardinality: 50, Width: 25},
	}

	predicates := []JoinPredicate{
		{LeftTable: "t1", LeftColumn: "id", RightTable: "t2", RightColumn: "t1_id", IsEquality: true},
		{LeftTable: "t2", LeftColumn: "id", RightTable: "t3", RightColumn: "t2_id", IsEquality: true},
		{LeftTable: "t3", LeftColumn: "id", RightTable: "t4", RightColumn: "t3_id", IsEquality: true},
		{LeftTable: "t4", LeftColumn: "id", RightTable: "t5", RightColumn: "t4_id", IsEquality: true},
	}

	plan, err := optimizer.OptimizeJoinOrder(tables, predicates)

	require.NoError(t, err)
	assert.NotNil(t, plan)
	assert.Len(t, plan.Tables, 5)
	assert.Len(t, plan.JoinOrder, 4) // 5 tables = 4 joins
}

func TestJoinOrderOptimizer_StarSchema(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	optimizer := NewJoinOrderOptimizer(estimator, costModel)

	// Star schema: fact table joins to multiple dimension tables
	tables := []TableRef{
		{Table: "fact_sales", Cardinality: 1000000, Width: 100},
		{Table: "dim_product", Cardinality: 1000, Width: 50},
		{Table: "dim_customer", Cardinality: 5000, Width: 80},
		{Table: "dim_time", Cardinality: 365, Width: 30},
	}

	predicates := []JoinPredicate{
		{LeftTable: "fact_sales", LeftColumn: "product_id", RightTable: "dim_product", RightColumn: "id", IsEquality: true},
		{LeftTable: "fact_sales", LeftColumn: "customer_id", RightTable: "dim_customer", RightColumn: "id", IsEquality: true},
		{LeftTable: "fact_sales", LeftColumn: "time_id", RightTable: "dim_time", RightColumn: "id", IsEquality: true},
	}

	plan, err := optimizer.OptimizeJoinOrder(tables, predicates)

	require.NoError(t, err)
	assert.NotNil(t, plan)
	assert.Len(t, plan.Tables, 4)

	// The optimizer should find a reasonable order
	assert.Greater(t, plan.TotalCost.TotalCost, float64(0))
}

func TestTableRef_Name(t *testing.T) {
	tests := []struct {
		name     string
		ref      TableRef
		expected string
	}{
		{
			name:     "table without alias",
			ref:      TableRef{Table: "orders"},
			expected: "orders",
		},
		{
			name:     "table with alias",
			ref:      TableRef{Table: "orders", Alias: "o"},
			expected: "o",
		},
		{
			name:     "empty alias uses table name",
			ref:      TableRef{Table: "customers", Alias: ""},
			expected: "customers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.ref.Name()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestJoinPredicate(t *testing.T) {
	pred := JoinPredicate{
		LeftTable:   "orders",
		LeftColumn:  "customer_id",
		RightTable:  "customers",
		RightColumn: "id",
		IsEquality:  true,
	}

	assert.Equal(t, "orders", pred.LeftTable)
	assert.Equal(t, "customer_id", pred.LeftColumn)
	assert.Equal(t, "customers", pred.RightTable)
	assert.Equal(t, "id", pred.RightColumn)
	assert.True(t, pred.IsEquality)
}

func TestJoinPlan(t *testing.T) {
	plan := JoinPlan{
		Tables: []string{"a", "b", "c"},
		JoinOrder: []JoinStep{
			{LeftIdx: 0, RightIdx: 1, BuildSide: "right", JoinType: JoinTypeInner},
			{LeftIdx: 0, RightIdx: 2, BuildSide: "right", JoinType: JoinTypeInner},
		},
		TotalCost: PlanCost{TotalCost: 100, OutputRows: 500},
	}

	assert.Len(t, plan.Tables, 3)
	assert.Len(t, plan.JoinOrder, 2)
	assert.Equal(t, float64(100), plan.TotalCost.TotalCost)
}

func TestValidateJoinOrder(t *testing.T) {
	tables := []TableRef{
		{Table: "a", JoinType: JoinTypeInner},
		{Table: "b", JoinType: JoinTypeLeft},
		{Table: "c", JoinType: JoinTypeInner},
	}

	// Any order is valid in this simple implementation
	assert.True(t, ValidateJoinOrder(tables, []int{0, 1, 2}))
	assert.True(t, ValidateJoinOrder(tables, []int{2, 0, 1}))
}
