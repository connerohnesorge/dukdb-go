package optimizer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPlanEnumerator(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	require.NotNil(t, enumerator)
	assert.NotNil(t, enumerator.estimator)
	assert.NotNil(t, enumerator.costModel)
	assert.NotNil(t, enumerator.stats)
}

func TestEnumerateJoinMethods_EquiJoin(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	// Standard equi-join scenario
	alternatives := enumerator.EnumerateJoinMethods(
		1000, 500,   // left and right cardinality
		50, 30,      // left and right width
		true,        // has equi-join
		nil, nil,    // not sorted
		nil,         // join keys
	)

	// Should have hash join alternatives and NLJ alternatives
	require.NotEmpty(t, alternatives)

	// Hash join should be present (at least 2 options: build left/right)
	hashJoinCount := 0
	nljCount := 0
	for _, alt := range alternatives {
		//nolint:exhaustive // We only care about these two types in this test
		switch alt.PlanType {
		case PlanTypeHashJoin:
			hashJoinCount++
		case PlanTypeNestedLoopJoin:
			nljCount++
		}
	}

	assert.GreaterOrEqual(t, hashJoinCount, 2, "should have at least 2 hash join alternatives")
	assert.GreaterOrEqual(t, nljCount, 2, "should have at least 2 NLJ alternatives")

	// Alternatives should be sorted by cost
	for i := 1; i < len(alternatives); i++ {
		assert.LessOrEqual(t, alternatives[i-1].Cost.TotalCost, alternatives[i].Cost.TotalCost,
			"alternatives should be sorted by cost")
	}
}

func TestEnumerateJoinMethods_NoEquiJoin(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	// Non-equi join (no hash join possible)
	alternatives := enumerator.EnumerateJoinMethods(
		100, 50,     // small tables
		20, 10,      // widths
		false,       // no equi-join
		nil, nil,    // not sorted
		nil,         // no join keys
	)

	require.NotEmpty(t, alternatives)

	// Should only have NLJ alternatives (no hash join without equi-join)
	for _, alt := range alternatives {
		if alt.PlanType == PlanTypeHashJoin {
			t.Error("hash join should not be available without equi-join condition")
		}
	}
}

func TestEnumerateJoinMethods_SortMergeJoin(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	// Both inputs sorted on join key
	joinKeys := []string{"id"}
	alternatives := enumerator.EnumerateJoinMethods(
		1000, 500,
		50, 30,
		true,                   // has equi-join
		[]string{"id"},         // left sorted
		[]string{"id"},         // right sorted
		joinKeys,
	)

	require.NotEmpty(t, alternatives)

	// Should include sort-merge join
	hasSMJ := false
	for _, alt := range alternatives {
		if alt.PlanType != PlanTypeSortMergeJoin {
			continue
		}
		hasSMJ = true
		// SMJ should preserve sort order
		assert.NotEmpty(t, alt.Properties.SortedBy)
		break
	}
	assert.True(t, hasSMJ, "should include sort-merge join when both inputs are sorted")
}

func TestEnumerateJoinMethods_BuildSideSelection(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	// Left side much smaller than right
	alternatives := enumerator.EnumerateJoinMethods(
		100, 10000,  // left small, right large
		20, 50,      // widths
		true,        // has equi-join
		nil, nil,
		nil,
	)

	require.NotEmpty(t, alternatives)

	// Find the best hash join alternative
	var bestHashJoin *PhysicalAlternative
	for i := range alternatives {
		if alternatives[i].PlanType != PlanTypeHashJoin {
			continue
		}
		if bestHashJoin == nil || alternatives[i].Cost.TotalCost < bestHashJoin.Cost.TotalCost {
			bestHashJoin = &alternatives[i]
		}
	}

	require.NotNil(t, bestHashJoin)
	// Smaller side should be build side
	assert.Equal(t, "left", bestHashJoin.BuildSide, "smaller side should be build side")
}

func TestEnumerateJoinMethods_SmallInner(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	// Inner side below NLJ threshold
	alternatives := enumerator.EnumerateJoinMethods(
		1000, 50,    // right < NestedLoopThreshold
		50, 20,
		true,        // has equi-join
		nil, nil,
		nil,
	)

	require.NotEmpty(t, alternatives)

	// NLJ should be among the alternatives
	hasNLJ := false
	for _, alt := range alternatives {
		if alt.PlanType == PlanTypeNestedLoopJoin {
			hasNLJ = true
			break
		}
	}
	assert.True(t, hasNLJ, "NLJ should be available for small inner side")
}

func TestSelectBestJoin(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	alternatives := []PhysicalAlternative{
		{PlanType: PlanTypeHashJoin, Cost: PlanCost{TotalCost: 100}},
		{PlanType: PlanTypeNestedLoopJoin, Cost: PlanCost{TotalCost: 500}},
		{PlanType: PlanTypeSortMergeJoin, Cost: PlanCost{TotalCost: 150}},
	}

	best := enumerator.SelectBestJoin(alternatives)

	require.NotNil(t, best)
	assert.Equal(t, PlanTypeHashJoin, best.PlanType)
	assert.Equal(t, 100.0, best.Cost.TotalCost)
}

func TestSelectBestJoin_Empty(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	best := enumerator.SelectBestJoin(nil)
	assert.Nil(t, best)

	best = enumerator.SelectBestJoin([]PhysicalAlternative{})
	assert.Nil(t, best)
}

func TestEnumerateAccessMethods_SeqScan(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	// No index available
	alternatives := enumerator.EnumerateAccessMethods(
		"orders",
		"main",
		0.5,   // high selectivity
		false, // no index
		nil,
	)

	require.Len(t, alternatives, 1)
	assert.Equal(t, PlanTypeSeqScan, alternatives[0].PlanType)
}

func TestEnumerateAccessMethods_IndexScan(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	// Index available with low selectivity
	alternatives := enumerator.EnumerateAccessMethods(
		"orders",
		"main",
		0.05,                   // low selectivity (5%)
		true,                   // has index
		[]string{"customer_id"},
	)

	require.GreaterOrEqual(t, len(alternatives), 2)

	// Should have both seq scan and index scan
	hasSeqScan := false
	hasIndexScan := false
	for _, alt := range alternatives {
		//nolint:exhaustive // We only care about these two types in this test
		switch alt.PlanType {
		case PlanTypeSeqScan:
			hasSeqScan = true
		case PlanTypeIndexScan:
			hasIndexScan = true
			// Index scan should preserve sort order
			assert.NotEmpty(t, alt.Properties.SortedBy)
		}
	}

	assert.True(t, hasSeqScan, "should include sequential scan")
	assert.True(t, hasIndexScan, "should include index scan for low selectivity")
}

func TestEnumerateAccessMethods_HighSelectivity(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	// Index available but high selectivity
	alternatives := enumerator.EnumerateAccessMethods(
		"orders",
		"main",
		0.5,                    // high selectivity (50%)
		true,                   // has index
		[]string{"customer_id"},
	)

	// Should only have seq scan (index scan not beneficial for high selectivity)
	assert.Len(t, alternatives, 1)
	assert.Equal(t, PlanTypeSeqScan, alternatives[0].PlanType)
}

func TestSelectBestAccess(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	alternatives := []PhysicalAlternative{
		{PlanType: PlanTypeSeqScan, Cost: PlanCost{TotalCost: 100}},
		{PlanType: PlanTypeIndexScan, Cost: PlanCost{TotalCost: 50}},
	}

	best := enumerator.SelectBestAccess(alternatives)

	require.NotNil(t, best)
	assert.Equal(t, PlanTypeIndexScan, best.PlanType)
	assert.Equal(t, 50.0, best.Cost.TotalCost)
}

func TestSelectBestAccess_Empty(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	best := enumerator.SelectBestAccess(nil)
	assert.Nil(t, best)

	best = enumerator.SelectBestAccess([]PhysicalAlternative{})
	assert.Nil(t, best)
}

func TestShouldUseSortMergeJoin(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	tests := []struct {
		name        string
		leftSorted  []string
		rightSorted []string
		joinKeys    []string
		expected    bool
	}{
		{
			name:        "both sorted on join key",
			leftSorted:  []string{"id"},
			rightSorted: []string{"id"},
			joinKeys:    []string{"id"},
			expected:    true,
		},
		{
			name:        "left not sorted",
			leftSorted:  nil,
			rightSorted: []string{"id"},
			joinKeys:    []string{"id"},
			expected:    false,
		},
		{
			name:        "right not sorted",
			leftSorted:  []string{"id"},
			rightSorted: nil,
			joinKeys:    []string{"id"},
			expected:    false,
		},
		{
			name:        "neither sorted",
			leftSorted:  nil,
			rightSorted: nil,
			joinKeys:    []string{"id"},
			expected:    false,
		},
		{
			name:        "sorted but not on join key",
			leftSorted:  []string{"name"},
			rightSorted: []string{"name"},
			joinKeys:    []string{"id"},
			expected:    false,
		},
		{
			name:        "no join keys",
			leftSorted:  []string{"id"},
			rightSorted: []string{"id"},
			joinKeys:    nil,
			expected:    false,
		},
		{
			name:        "multiple sorted columns including join key",
			leftSorted:  []string{"id", "name"},
			rightSorted: []string{"id"},
			joinKeys:    []string{"id"},
			expected:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := enumerator.ShouldUseSortMergeJoin(tt.leftSorted, tt.rightSorted, tt.joinKeys)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldUseNestedLoopJoin(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	tests := []struct {
		name        string
		innerCard   float64
		hasEquiJoin bool
		expected    bool
	}{
		{
			name:        "small inner with equi-join",
			innerCard:   50,
			hasEquiJoin: true,
			expected:    true,
		},
		{
			name:        "large inner with equi-join",
			innerCard:   1000,
			hasEquiJoin: true,
			expected:    false,
		},
		{
			name:        "small inner without equi-join",
			innerCard:   50,
			hasEquiJoin: false,
			expected:    true,
		},
		{
			name:        "large inner without equi-join",
			innerCard:   1000,
			hasEquiJoin: false,
			expected:    true, // NLJ required when no equi-join
		},
		{
			name:        "boundary case at threshold",
			innerCard:   NestedLoopThreshold,
			hasEquiJoin: true,
			expected:    false, // exactly at threshold, not below
		},
		{
			name:        "just below threshold",
			innerCard:   NestedLoopThreshold - 1,
			hasEquiJoin: true,
			expected:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := enumerator.ShouldUseNestedLoopJoin(tt.innerCard, tt.hasEquiJoin)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSelectBuildSideForHashJoin(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	tests := []struct {
		name       string
		leftCard   float64
		rightCard  float64
		leftWidth  int32
		rightWidth int32
		expected   string
	}{
		{
			name:       "left smaller by rows",
			leftCard:   100,
			rightCard:  1000,
			leftWidth:  20,
			rightWidth: 20,
			expected:   "left",
		},
		{
			name:       "right smaller by rows",
			leftCard:   1000,
			rightCard:  100,
			leftWidth:  20,
			rightWidth: 20,
			expected:   "right",
		},
		{
			name:       "left smaller by width",
			leftCard:   100,
			rightCard:  100,
			leftWidth:  10,
			rightWidth: 100,
			expected:   "left",
		},
		{
			name:       "right smaller by width",
			leftCard:   100,
			rightCard:  100,
			leftWidth:  100,
			rightWidth: 10,
			expected:   "right",
		},
		{
			name:       "left smaller by memory product",
			leftCard:   100,
			rightCard:  1000,
			leftWidth:  10,
			rightWidth: 10,
			expected:   "left",
		},
		{
			name:       "right smaller by memory product",
			leftCard:   1000,
			rightCard:  100,
			leftWidth:  50,
			rightWidth: 50,
			expected:   "right",
		},
		{
			name:       "equal memory",
			leftCard:   100,
			rightCard:  100,
			leftWidth:  50,
			rightWidth: 50,
			expected:   "left", // tie goes to left
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := enumerator.SelectBuildSideForHashJoin(tt.leftCard, tt.rightCard, tt.leftWidth, tt.rightWidth)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEnumerateJoinMethods_CostRanking(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	// Large tables - hash join should generally be cheaper than NLJ
	alternatives := enumerator.EnumerateJoinMethods(
		10000, 5000,  // large tables
		50, 30,
		true,         // has equi-join
		nil, nil,
		nil,
	)

	require.NotEmpty(t, alternatives)

	// Find best hash join and best NLJ
	var bestHashJoin, bestNLJ *PhysicalAlternative
	for i := range alternatives {
		//nolint:exhaustive // We only care about these two types in this test
		switch alternatives[i].PlanType {
		case PlanTypeHashJoin:
			if bestHashJoin == nil || alternatives[i].Cost.TotalCost < bestHashJoin.Cost.TotalCost {
				bestHashJoin = &alternatives[i]
			}
		case PlanTypeNestedLoopJoin:
			if bestNLJ == nil || alternatives[i].Cost.TotalCost < bestNLJ.Cost.TotalCost {
				bestNLJ = &alternatives[i]
			}
		}
	}

	require.NotNil(t, bestHashJoin)
	require.NotNil(t, bestNLJ)

	// For large tables, hash join should be cheaper
	assert.Less(t, bestHashJoin.Cost.TotalCost, bestNLJ.Cost.TotalCost,
		"hash join should be cheaper than NLJ for large tables")
}

func TestIndexScanVsSeqScan_LowSelectivity(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	// Very low selectivity - index scan should be better
	alternatives := enumerator.EnumerateAccessMethods(
		"orders",
		"main",
		0.01,                   // 1% selectivity
		true,                   // has index
		[]string{"customer_id"},
	)

	require.GreaterOrEqual(t, len(alternatives), 2)

	// First should be the best (index scan for low selectivity)
	assert.Equal(t, PlanTypeIndexScan, alternatives[0].PlanType)
}

func TestPhysicalAlternative_Properties(t *testing.T) {
	alt := PhysicalAlternative{
		PlanType:  PlanTypeSortMergeJoin,
		Cost:      PlanCost{TotalCost: 100},
		BuildSide: "right",
		Properties: PlanProperties{
			SortedBy:    []string{"id"},
			Partitioned: false,
		},
	}

	assert.Equal(t, PlanTypeSortMergeJoin, alt.PlanType)
	assert.Equal(t, 100.0, alt.Cost.TotalCost)
	assert.Equal(t, "right", alt.BuildSide)
	assert.Equal(t, []string{"id"}, alt.Properties.SortedBy)
	assert.False(t, alt.Properties.Partitioned)
}

func TestEnumerateJoinMethods_ZeroCardinality(t *testing.T) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	// Edge case: zero cardinality (should be treated as 1)
	alternatives := enumerator.EnumerateJoinMethods(
		0, 0,        // zero cardinality
		50, 30,
		true,
		nil, nil,
		nil,
	)

	require.NotEmpty(t, alternatives)

	// All costs should be positive and reasonable
	for _, alt := range alternatives {
		assert.Greater(t, alt.Cost.TotalCost, 0.0, "cost should be positive")
		assert.GreaterOrEqual(t, alt.Cost.OutputRows, 1.0, "output rows should be at least 1")
	}
}

// Benchmark tests

func BenchmarkEnumerateJoinMethods(b *testing.B) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	b.ResetTimer()
	for range b.N {
		_ = enumerator.EnumerateJoinMethods(
			10000, 5000,
			50, 30,
			true,
			nil, nil,
			nil,
		)
	}
}

func BenchmarkEnumerateAccessMethods(b *testing.B) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	b.ResetTimer()
	for range b.N {
		_ = enumerator.EnumerateAccessMethods(
			"orders",
			"main",
			0.05,
			true,
			[]string{"customer_id"},
		)
	}
}

func BenchmarkSelectBestJoin(b *testing.B) {
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	alternatives := []PhysicalAlternative{
		{PlanType: PlanTypeHashJoin, Cost: PlanCost{TotalCost: 100}},
		{PlanType: PlanTypeNestedLoopJoin, Cost: PlanCost{TotalCost: 500}},
		{PlanType: PlanTypeSortMergeJoin, Cost: PlanCost{TotalCost: 150}},
		{PlanType: PlanTypeHashJoin, Cost: PlanCost{TotalCost: 120}},
	}

	b.ResetTimer()
	for range b.N {
		_ = enumerator.SelectBestJoin(alternatives)
	}
}
