package optimizer

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultCostConstants(t *testing.T) {
	constants := DefaultCostConstants()

	assert.Equal(t, 1.0, constants.SeqPageCost)
	assert.Equal(t, 1.1, constants.RandomPageCost) // Low for in-memory operations
	assert.Equal(t, 0.01, constants.CPUTupleCost)
	assert.Equal(t, 0.0025, constants.CPUOperatorCost)
	assert.Equal(t, 0.02, constants.HashBuildCost)
	assert.Equal(t, 0.01, constants.HashProbeCost)
	assert.Equal(t, 0.05, constants.SortCost)
	assert.Equal(t, 0.005, constants.IndexLookupCost)
	assert.Equal(t, 0.005, constants.IndexTupleCost)
}

func TestPlanCostAdd(t *testing.T) {
	cost1 := PlanCost{
		StartupCost: 10,
		TotalCost:   100,
		OutputRows:  1000,
		OutputWidth: 50,
	}

	cost2 := PlanCost{
		StartupCost: 5,
		TotalCost:   50,
		OutputRows:  500,
		OutputWidth: 25,
	}

	result := cost1.Add(cost2)

	assert.Equal(t, 15.0, result.StartupCost)
	assert.Equal(t, 150.0, result.TotalCost)
	// OutputRows should stay from the original plan
	assert.Equal(t, 1000.0, result.OutputRows)
	assert.Equal(t, int32(50), result.OutputWidth)
}

func TestPlanCostLess(t *testing.T) {
	cheaper := PlanCost{TotalCost: 100}
	expensive := PlanCost{TotalCost: 200}
	equal := PlanCost{TotalCost: 100}

	assert.True(t, cheaper.Less(expensive))
	assert.False(t, expensive.Less(cheaper))
	assert.False(t, cheaper.Less(equal))
}

func TestNewCostModel(t *testing.T) {
	constants := DefaultCostConstants()
	stats := NewStatisticsManager(nil)
	estimator := NewCardinalityEstimator(stats)

	model := NewCostModel(constants, estimator)

	require.NotNil(t, model)
	assert.Equal(t, constants, model.GetConstants())
}

func TestCostScan(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	testCases := []struct {
		name       string
		rows       float64
		pages      float64
		wantTotal  float64
		wantOutput float64
	}{
		{
			name:       "small table",
			rows:       100,
			pages:      1,
			wantTotal:  1.0 + 100*0.01,
			wantOutput: 100,
		},
		{
			name:       "medium table",
			rows:       10000,
			pages:      100,
			wantTotal:  100*1.0 + 10000*0.01,
			wantOutput: 10000,
		},
		{
			name:       "large table",
			rows:       1000000,
			pages:      10000,
			wantTotal:  10000*1.0 + 1000000*0.01,
			wantOutput: 1000000,
		},
		{
			name:       "minimum values",
			rows:       0,
			pages:      0,
			wantTotal:  1.0 + 1*0.01,
			wantOutput: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cost := model.costScan(tc.rows, tc.pages)

			assert.Equal(t, 0.0, cost.StartupCost)
			assert.InDelta(t, tc.wantTotal, cost.TotalCost, 0.001)
			assert.Equal(t, tc.wantOutput, cost.OutputRows)
		})
	}
}

func TestCostFilter(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	childCost := PlanCost{
		StartupCost: 0,
		TotalCost:   100,
		OutputRows:  10000,
		OutputWidth: 50,
	}

	cost := model.costFilter(childCost, 10000)

	// Startup should be same as child
	assert.Equal(t, childCost.StartupCost, cost.StartupCost)
	// Total should be child + filter cost
	expectedFilterCost := 10000 * 0.0025 // rows * CPUOperatorCost
	assert.InDelta(t, childCost.TotalCost+expectedFilterCost, cost.TotalCost, 0.001)
	// Output rows reduced by default selectivity
	assert.InDelta(t, 10000*DefaultSelectivity, cost.OutputRows, 0.001)
}

func TestCostFilterWithSelectivity(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	childCost := PlanCost{
		StartupCost: 0,
		TotalCost:   100,
		OutputRows:  10000,
		OutputWidth: 50,
	}

	// Test with 10% selectivity
	cost := model.costFilterWithSelectivity(childCost, 10000, 0.1)

	assert.InDelta(t, 1000.0, cost.OutputRows, 0.001) // 10000 * 0.1
}

func TestCostProject(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	childCost := PlanCost{
		StartupCost: 10,
		TotalCost:   100,
		OutputRows:  1000,
		OutputWidth: 50,
	}

	testCases := []struct {
		name      string
		numExprs  int
		wantExtra float64
	}{
		{
			name:      "single expression",
			numExprs:  1,
			wantExtra: 1000 * 1 * 0.0025,
		},
		{
			name:      "multiple expressions",
			numExprs:  5,
			wantExtra: 1000 * 5 * 0.0025,
		},
		{
			name:      "many expressions",
			numExprs:  20,
			wantExtra: 1000 * 20 * 0.0025,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cost := model.costProject(childCost, 1000, tc.numExprs)

			assert.Equal(t, childCost.StartupCost, cost.StartupCost)
			assert.InDelta(t, childCost.TotalCost+tc.wantExtra, cost.TotalCost, 0.001)
			// Projection doesn't change row count
			assert.Equal(t, 1000.0, cost.OutputRows)
		})
	}
}

func TestCostHashJoin(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	leftCost := PlanCost{
		StartupCost: 0,
		TotalCost:   100,
		OutputRows:  1000,
		OutputWidth: 50,
	}

	rightCost := PlanCost{
		StartupCost: 0,
		TotalCost:   50,
		OutputRows:  500,
		OutputWidth: 30,
	}

	// Build on right (500 rows), probe on left (1000 rows), output 800 rows
	params := HashJoinParams{
		Left:       leftCost,
		Right:      rightCost,
		BuildRows:  500,
		ProbeRows:  1000,
		OutputRows: 800,
	}
	cost := model.costHashJoin(params)

	// Build cost: 500 * 0.02 = 10
	buildCost := 500 * 0.02
	// Startup: left.StartupCost + right.TotalCost + buildCost = 0 + 50 + 10 = 60
	expectedStartup := leftCost.StartupCost + rightCost.TotalCost + buildCost

	// Probe cost: 1000 * 0.01 = 10
	probeCost := 1000 * 0.01
	// Total: startup + probeCost + left run cost = 60 + 10 + 100 = 170
	expectedTotal := expectedStartup + probeCost + (leftCost.TotalCost - leftCost.StartupCost)

	assert.InDelta(t, expectedStartup, cost.StartupCost, 0.001)
	assert.InDelta(t, expectedTotal, cost.TotalCost, 0.001)
	assert.Equal(t, 800.0, cost.OutputRows)
	assert.Equal(t, int32(80), cost.OutputWidth) // 50 + 30
}

func TestCostNestedLoopJoin(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	outerCost := PlanCost{
		StartupCost: 5,
		TotalCost:   100,
		OutputRows:  100,
		OutputWidth: 50,
	}

	innerCost := PlanCost{
		StartupCost: 0,
		TotalCost:   10,
		OutputRows:  10,
		OutputWidth: 30,
	}

	cost := model.costNestedLoopJoin(outerCost, innerCost, 100, 500)

	// Startup = outer startup
	assert.Equal(t, outerCost.StartupCost, cost.StartupCost)
	// Total = outer + (outer_rows * inner) = 100 + (100 * 10) = 1100
	expectedTotal := outerCost.TotalCost + (100 * innerCost.TotalCost)
	assert.InDelta(t, expectedTotal, cost.TotalCost, 0.001)
	assert.Equal(t, 500.0, cost.OutputRows)
	assert.Equal(t, int32(80), cost.OutputWidth)
}

func TestCostSort(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	childCost := PlanCost{
		StartupCost: 0,
		TotalCost:   100,
		OutputRows:  1000,
		OutputWidth: 50,
	}

	cost := model.costSort(childCost, 1000)

	// Sort cost: rows * log2(rows) * SortCost = 1000 * log2(1000) * 0.05
	logRows := math.Log2(1000)
	sortCost := 1000 * logRows * 0.05

	// Startup = child.Total + sortCost = 100 + sortCost
	expectedStartup := childCost.TotalCost + sortCost

	// Output cost: rows * CPUTupleCost = 1000 * 0.01 = 10
	outputCost := 1000 * 0.01

	// Total = startup + outputCost
	expectedTotal := expectedStartup + outputCost

	assert.InDelta(t, expectedStartup, cost.StartupCost, 0.001)
	assert.InDelta(t, expectedTotal, cost.TotalCost, 0.001)
	assert.Equal(t, 1000.0, cost.OutputRows)
}

func TestCostHashAggregate(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	childCost := PlanCost{
		StartupCost: 0,
		TotalCost:   100,
		OutputRows:  10000,
		OutputWidth: 50,
	}

	// 10000 input rows, 100 groups
	cost := model.costHashAggregate(childCost, 10000, 100)

	// Hash build cost: rows * HashBuildCost = 10000 * 0.02 = 200
	hashCost := 10000 * 0.02

	// Startup = child.Total + hashCost = 100 + 200 = 300
	expectedStartup := childCost.TotalCost + hashCost

	// Output cost: groups * CPUTupleCost = 100 * 0.01 = 1
	outputCost := 100 * 0.01

	// Total = startup + outputCost = 301
	expectedTotal := expectedStartup + outputCost

	assert.InDelta(t, expectedStartup, cost.StartupCost, 0.001)
	assert.InDelta(t, expectedTotal, cost.TotalCost, 0.001)
	assert.Equal(t, 100.0, cost.OutputRows)
}

func TestCostLimit(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	childCost := PlanCost{
		StartupCost: 10,
		TotalCost:   110,
		OutputRows:  1000,
		OutputWidth: 50,
	}

	testCases := []struct {
		name       string
		limit      int64
		offset     int64
		wantRows   float64
		wantFaster bool // Should total cost be less than child?
	}{
		{
			name:       "small limit",
			limit:      10,
			offset:     0,
			wantRows:   10,
			wantFaster: true,
		},
		{
			name:       "limit with offset",
			limit:      10,
			offset:     5,
			wantRows:   10, // Still 10 rows output
			wantFaster: true,
		},
		{
			name:       "limit exceeds rows",
			limit:      2000,
			offset:     0,
			wantRows:   1000, // Can't output more than available
			wantFaster: false,
		},
		{
			name:       "zero limit",
			limit:      0,
			offset:     0,
			wantRows:   1000, // No limit applied
			wantFaster: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cost := model.costLimit(childCost, tc.limit, tc.offset)

			assert.InDelta(t, tc.wantRows, cost.OutputRows, 1.0)
			if tc.wantFaster {
				assert.Less(t, cost.TotalCost, childCost.TotalCost)
			}
		})
	}
}

func TestComparePlans(t *testing.T) {
	cheaper := PlanCost{TotalCost: 100}
	expensive := PlanCost{TotalCost: 200}
	equal := PlanCost{TotalCost: 100}

	assert.Equal(t, -1, ComparePlans(cheaper, expensive))
	assert.Equal(t, 1, ComparePlans(expensive, cheaper))
	assert.Equal(t, 0, ComparePlans(cheaper, equal))
}

func TestCostModelSetConstants(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	newConstants := CostConstants{
		SeqPageCost:     2.0,
		RandomPageCost:  8.0,
		CPUTupleCost:    0.02,
		CPUOperatorCost: 0.005,
		HashBuildCost:   0.04,
		HashProbeCost:   0.02,
		SortCost:        0.1,
	}

	model.SetConstants(newConstants)

	assert.Equal(t, newConstants, model.GetConstants())

	// Verify costs change accordingly
	scanCost := model.costScan(100, 10)
	expectedTotal := 10*2.0 + 100*0.02 // New constants
	assert.InDelta(t, expectedTotal, scanCost.TotalCost, 0.001)
}

// Test that relative costs make sense
func TestCostModelRelativeRanking(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	// Scanning 1000 rows
	scanCost := model.costScan(1000, 10)

	// Filter should add cost
	filterCost := model.costFilter(scanCost, 1000)
	assert.Greater(t, filterCost.TotalCost, scanCost.TotalCost)

	// Sorting should be more expensive than filtering
	sortCost := model.costSort(scanCost, 1000)
	assert.Greater(t, sortCost.TotalCost, filterCost.TotalCost)

	// NLJ should be more expensive than hash join for same cardinalities
	smallTable := PlanCost{TotalCost: 10, OutputRows: 100, OutputWidth: 50}
	largeTable := PlanCost{TotalCost: 100, OutputRows: 1000, OutputWidth: 50}

	hashJoinParams := HashJoinParams{
		Left:       largeTable,
		Right:      smallTable,
		BuildRows:  100,
		ProbeRows:  1000,
		OutputRows: 500,
	}
	hashJoinCost := model.costHashJoin(hashJoinParams)
	nljCost := model.costNestedLoopJoin(largeTable, smallTable, 1000, 500)

	// Hash join should generally be cheaper than nested loop for larger tables
	assert.Less(t, hashJoinCost.TotalCost, nljCost.TotalCost)
}

// Test edge cases
func TestCostModelEdgeCases(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	t.Run("zero or negative rows handled", func(t *testing.T) {
		cost := model.costScan(0, 0)
		assert.Greater(t, cost.TotalCost, 0.0)
		assert.GreaterOrEqual(t, cost.OutputRows, 1.0)
	})

	t.Run("very large values", func(t *testing.T) {
		cost := model.costScan(1e9, 1e7)
		assert.Greater(t, cost.TotalCost, 0.0)
		assert.False(t, math.IsInf(cost.TotalCost, 0))
		assert.False(t, math.IsNaN(cost.TotalCost))
	})

	t.Run("sort single row", func(t *testing.T) {
		child := PlanCost{TotalCost: 1, OutputRows: 1, OutputWidth: 10}
		cost := model.costSort(child, 1)
		assert.Greater(t, cost.TotalCost, 0.0)
		assert.False(t, math.IsNaN(cost.TotalCost))
	})
}

// Benchmark cost calculations
func BenchmarkCostScan(b *testing.B) {
	model := NewCostModel(DefaultCostConstants(), nil)
	b.ResetTimer()

	for range b.N {
		_ = model.costScan(10000, 100)
	}
}

func BenchmarkCostHashJoin(b *testing.B) {
	model := NewCostModel(DefaultCostConstants(), nil)
	left := PlanCost{TotalCost: 100, OutputRows: 1000, OutputWidth: 50}
	right := PlanCost{TotalCost: 50, OutputRows: 500, OutputWidth: 30}
	params := HashJoinParams{
		Left:       left,
		Right:      right,
		BuildRows:  500,
		ProbeRows:  1000,
		OutputRows: 800,
	}
	b.ResetTimer()

	for range b.N {
		_ = model.costHashJoin(params)
	}
}

func BenchmarkCostSort(b *testing.B) {
	model := NewCostModel(DefaultCostConstants(), nil)
	child := PlanCost{TotalCost: 100, OutputRows: 10000, OutputWidth: 50}
	b.ResetTimer()

	for range b.N {
		_ = model.costSort(child, 10000)
	}
}

// Tests for index scan cost estimation (Phase 3)

func TestEstimateIndexScanCost(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	testCases := []struct {
		name          string
		selectivity   float64
		tableRows     int64
		tablePages    int64
		isIndexOnly   bool
		wantStartup   float64
		wantTotalMin  float64 // Minimum expected total cost
		wantTotalMax  float64 // Maximum expected total cost
		wantOutputMin float64
		wantOutputMax float64
	}{
		{
			name:          "high selectivity index scan",
			selectivity:   0.01, // 1% of rows
			tableRows:     10000,
			tablePages:    100,
			isIndexOnly:   false,
			wantStartup:   0.005, // IndexLookupCost
			wantTotalMin:  1.0,   // Should have non-trivial cost
			wantTotalMax:  500.0, // But not too expensive
			wantOutputMin: 100,   // 10000 * 0.01 = 100
			wantOutputMax: 100,
		},
		{
			name:          "index-only scan",
			selectivity:   0.01,
			tableRows:     10000,
			tablePages:    100,
			isIndexOnly:   true,
			wantStartup:   0.005,
			wantTotalMin:  0.005, // Just startup + index scan
			wantTotalMax:  10.0,  // Much cheaper than regular index scan
			wantOutputMin: 100,
			wantOutputMax: 100,
		},
		{
			name:          "low selectivity (many rows)",
			selectivity:   0.5, // 50% of rows
			tableRows:     10000,
			tablePages:    100,
			isIndexOnly:   false,
			wantStartup:   0.005,
			wantTotalMin:  100.0,  // Higher cost due to more rows
			wantTotalMax:  1000.0, // Expensive but bounded
			wantOutputMin: 5000,
			wantOutputMax: 5000,
		},
		{
			name:          "small table",
			selectivity:   0.1,
			tableRows:     100,
			tablePages:    1,
			isIndexOnly:   false,
			wantStartup:   0.005,
			wantTotalMin:  0.01,
			wantTotalMax:  100.0,
			wantOutputMin: 10,
			wantOutputMax: 10,
		},
		{
			name:          "minimum values handled",
			selectivity:   0.001,
			tableRows:     0, // Should be treated as 1
			tablePages:    0, // Should be treated as 1
			isIndexOnly:   false,
			wantStartup:   0.005,
			wantTotalMin:  0.0,
			wantTotalMax:  100.0,
			wantOutputMin: 1, // Minimum 1 row
			wantOutputMax: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cost := model.EstimateIndexScanCost(
				tc.selectivity,
				tc.tableRows,
				tc.tablePages,
				tc.isIndexOnly,
			)

			assert.InDelta(t, tc.wantStartup, cost.StartupCost, 0.001, "startup cost mismatch")
			assert.GreaterOrEqual(t, cost.TotalCost, tc.wantTotalMin, "total cost below minimum")
			assert.LessOrEqual(t, cost.TotalCost, tc.wantTotalMax, "total cost above maximum")
			assert.GreaterOrEqual(t, cost.OutputRows, tc.wantOutputMin, "output rows below minimum")
			assert.LessOrEqual(t, cost.OutputRows, tc.wantOutputMax, "output rows above maximum")
		})
	}
}

func TestEstimateIndexScanCostFormula(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)
	constants := model.GetConstants()

	// Test index-only scan formula explicitly
	t.Run("index-only scan formula", func(t *testing.T) {
		selectivity := 0.1
		tableRows := int64(1000)
		tablePages := int64(10)

		cost := model.EstimateIndexScanCost(selectivity, tableRows, tablePages, true)

		estimatedRows := float64(tableRows) * selectivity
		expectedStartup := constants.IndexLookupCost
		expectedTotal := expectedStartup + (estimatedRows * constants.IndexTupleCost)

		assert.InDelta(t, expectedStartup, cost.StartupCost, 0.001)
		assert.InDelta(t, expectedTotal, cost.TotalCost, 0.001)
		assert.InDelta(t, estimatedRows, cost.OutputRows, 0.001)
	})

	// Test regular index scan formula explicitly
	t.Run("regular index scan formula", func(t *testing.T) {
		selectivity := 0.1
		tableRows := int64(1000)
		tablePages := int64(10)

		cost := model.EstimateIndexScanCost(selectivity, tableRows, tablePages, false)

		estimatedRows := float64(tableRows) * selectivity
		expectedStartup := constants.IndexLookupCost
		indexScanCost := estimatedRows * constants.IndexTupleCost
		randomPages := math.Min(estimatedRows, float64(tablePages))
		heapCost := randomPages * constants.RandomPageCost
		tupleCost := estimatedRows * constants.CPUTupleCost
		expectedTotal := expectedStartup + indexScanCost + heapCost + tupleCost

		assert.InDelta(t, expectedStartup, cost.StartupCost, 0.001)
		assert.InDelta(t, expectedTotal, cost.TotalCost, 0.001)
		assert.InDelta(t, estimatedRows, cost.OutputRows, 0.001)
	})
}

func TestEstimateSeqScanCost(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)
	constants := model.GetConstants()

	testCases := []struct {
		name        string
		tableRows   int64
		tablePages  int64
		selectivity float64
	}{
		{
			name:        "medium table",
			tableRows:   10000,
			tablePages:  100,
			selectivity: 0.1,
		},
		{
			name:        "small table",
			tableRows:   100,
			tablePages:  1,
			selectivity: 0.5,
		},
		{
			name:        "large table",
			tableRows:   1000000,
			tablePages:  10000,
			selectivity: 0.01,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cost := model.EstimateSeqScanCost(tc.tableRows, tc.tablePages, tc.selectivity)

			// Verify the formula
			expectedTotal := float64(tc.tablePages)*constants.SeqPageCost +
				float64(tc.tableRows)*constants.CPUTupleCost
			expectedOutput := float64(tc.tableRows) * tc.selectivity

			assert.Equal(t, 0.0, cost.StartupCost, "seq scan has no startup cost")
			assert.InDelta(t, expectedTotal, cost.TotalCost, 0.001)
			assert.InDelta(t, expectedOutput, cost.OutputRows, 0.001)
		})
	}
}

func TestShouldUseIndexScan(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	// Note: With RandomPageCost=1.1 (optimized for in-memory), index scans are
	// cheaper than with RandomPageCost=4.0 (disk-based). This means index scans
	// are preferred at higher selectivities than traditional disk-based systems.
	testCases := []struct {
		name        string
		selectivity float64
		tableRows   int64
		tablePages  int64
		isIndexOnly bool
		wantIndex   bool
	}{
		{
			name:        "very selective - should use index",
			selectivity: 0.001, // 0.1% of rows
			tableRows:   100000,
			tablePages:  1000,
			isIndexOnly: false,
			wantIndex:   true, // Index should win for very selective queries
		},
		{
			name:        "index-only scan - should use index",
			selectivity: 0.01,
			tableRows:   100000,
			tablePages:  1000,
			isIndexOnly: true,
			wantIndex:   true, // Index-only is much cheaper
		},
		{
			name:        "low selectivity - should use seq scan",
			selectivity: 0.9, // 90% of rows
			tableRows:   100000,
			tablePages:  1000,
			isIndexOnly: false,
			wantIndex:   false, // Seq scan better for most rows
		},
		{
			name:        "small table medium selectivity - index wins in-memory",
			selectivity: 0.3,
			tableRows:   100,
			tablePages:  1,
			isIndexOnly: false,
			wantIndex:   true, // With low RandomPageCost, index is viable even at 30%
		},
		{
			name:        "medium selectivity large table - index wins in-memory",
			selectivity: 0.1,
			tableRows:   1000000,
			tablePages:  10000,
			isIndexOnly: false,
			wantIndex:   true, // With low RandomPageCost, 10% is still good for index
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := model.ShouldUseIndexScan(
				tc.selectivity,
				tc.tableRows,
				tc.tablePages,
				tc.isIndexOnly,
			)
			assert.Equal(t, tc.wantIndex, result)
		})
	}
}

func TestIndexScanVsSeqScanCrossover(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	// Test the crossover point where seq scan becomes cheaper than index scan
	// For a large table, index scan should be cheaper at low selectivity
	tableRows := int64(100000)
	tablePages := int64(1000)

	// Find approximate crossover point
	var crossoverSelectivity float64
	for sel := 0.001; sel <= 1.0; sel += 0.001 {
		indexCost := model.EstimateIndexScanCost(sel, tableRows, tablePages, false)
		seqCost := model.EstimateSeqScanCost(tableRows, tablePages, sel)

		if seqCost.TotalCost < indexCost.TotalCost {
			crossoverSelectivity = sel
			break
		}
	}

	// Crossover should happen somewhere between 0.1% and 90%
	// With RandomPageCost = 1.1 (in-memory), the crossover is higher (~60%)
	// compared to disk-based RandomPageCost = 4.0 where crossover is ~1%
	assert.GreaterOrEqual(t, crossoverSelectivity, 0.001, "crossover should be at or above 0.1%")
	assert.Less(t, crossoverSelectivity, 0.90, "crossover should be below 90%")

	t.Logf("Crossover selectivity: %.2f%%", crossoverSelectivity*100)
}

func TestIndexScanCostEdgeCases(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	t.Run("zero selectivity", func(t *testing.T) {
		cost := model.EstimateIndexScanCost(0, 10000, 100, false)
		assert.GreaterOrEqual(t, cost.OutputRows, 1.0, "should have minimum 1 row")
		assert.Greater(t, cost.TotalCost, 0.0, "should have positive cost")
	})

	t.Run("selectivity above 1 is capped", func(t *testing.T) {
		cost := model.EstimateIndexScanCost(2.0, 1000, 10, false)
		assert.LessOrEqual(t, cost.OutputRows, 1000.0, "output rows should not exceed table rows")
	})

	t.Run("negative selectivity is capped", func(t *testing.T) {
		cost := model.EstimateIndexScanCost(-0.5, 1000, 10, false)
		assert.GreaterOrEqual(t, cost.OutputRows, 1.0, "should have minimum 1 row")
	})

	t.Run("very large table", func(t *testing.T) {
		cost := model.EstimateIndexScanCost(0.001, 1e9, 1e7, false)
		assert.Greater(t, cost.TotalCost, 0.0)
		assert.False(t, math.IsInf(cost.TotalCost, 0))
		assert.False(t, math.IsNaN(cost.TotalCost))
	})
}

// Tests for index range scan cost estimation (Task 6.2)

func TestEstimateIndexRangeScanCost(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	testCases := []struct {
		name          string
		selectivity   float64
		tableRows     int64
		tablePages    int64
		isIndexOnly   bool
		wantStartup   float64
		wantTotalMin  float64 // Minimum expected total cost
		wantTotalMax  float64 // Maximum expected total cost
		wantOutputMin float64
		wantOutputMax float64
	}{
		{
			name:          "high selectivity range scan",
			selectivity:   0.01, // 1% of rows
			tableRows:     10000,
			tablePages:    100,
			isIndexOnly:   false,
			wantStartup:   0.005, // IndexLookupCost
			wantTotalMin:  1.0,   // Should have non-trivial cost
			wantTotalMax:  500.0, // But not too expensive
			wantOutputMin: 100,   // 10000 * 0.01 = 100
			wantOutputMax: 100,
		},
		{
			name:          "index-only range scan",
			selectivity:   0.01,
			tableRows:     10000,
			tablePages:    100,
			isIndexOnly:   true,
			wantStartup:   0.005,
			wantTotalMin:  0.005, // Just startup + index scan
			wantTotalMax:  10.0,  // Much cheaper than regular index scan
			wantOutputMin: 100,
			wantOutputMax: 100,
		},
		{
			name:          "low selectivity range scan (many rows)",
			selectivity:   0.5, // 50% of rows
			tableRows:     10000,
			tablePages:    100,
			isIndexOnly:   false,
			wantStartup:   0.005,
			wantTotalMin:  100.0,  // Higher cost due to more rows
			wantTotalMax:  1000.0, // Expensive but bounded
			wantOutputMin: 5000,
			wantOutputMax: 5000,
		},
		{
			name:          "small table range scan",
			selectivity:   0.1,
			tableRows:     100,
			tablePages:    1,
			isIndexOnly:   false,
			wantStartup:   0.005,
			wantTotalMin:  0.01,
			wantTotalMax:  100.0,
			wantOutputMin: 10,
			wantOutputMax: 10,
		},
		{
			name:          "minimum values handled",
			selectivity:   0.001,
			tableRows:     0, // Should be treated as 1
			tablePages:    0, // Should be treated as 1
			isIndexOnly:   false,
			wantStartup:   0.005,
			wantTotalMin:  0.0,
			wantTotalMax:  100.0,
			wantOutputMin: 1, // Minimum 1 row
			wantOutputMax: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cost := model.EstimateIndexRangeScanCost(
				tc.selectivity,
				tc.tableRows,
				tc.tablePages,
				tc.isIndexOnly,
			)

			assert.InDelta(t, tc.wantStartup, cost.StartupCost, 0.001, "startup cost mismatch")
			assert.GreaterOrEqual(t, cost.TotalCost, tc.wantTotalMin, "total cost below minimum")
			assert.LessOrEqual(t, cost.TotalCost, tc.wantTotalMax, "total cost above maximum")
			assert.GreaterOrEqual(t, cost.OutputRows, tc.wantOutputMin, "output rows below minimum")
			assert.LessOrEqual(t, cost.OutputRows, tc.wantOutputMax, "output rows above maximum")
		})
	}
}

func TestEstimateIndexRangeScanCostFormula(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)
	constants := model.GetConstants()

	// Test index-only range scan formula explicitly
	t.Run("index-only range scan formula", func(t *testing.T) {
		selectivity := 0.1
		tableRows := int64(1000)
		tablePages := int64(10)

		cost := model.EstimateIndexRangeScanCost(selectivity, tableRows, tablePages, true)

		estimatedRows := float64(tableRows) * selectivity
		expectedStartup := constants.IndexLookupCost
		indexIterationCost := estimatedRows * constants.IndexTupleCost
		boundCheckCost := estimatedRows * constants.CPUOperatorCost
		expectedTotal := expectedStartup + indexIterationCost + boundCheckCost

		assert.InDelta(t, expectedStartup, cost.StartupCost, 0.001)
		assert.InDelta(t, expectedTotal, cost.TotalCost, 0.001)
		assert.InDelta(t, estimatedRows, cost.OutputRows, 0.001)
	})

	// Test regular range scan formula explicitly
	t.Run("regular range scan formula", func(t *testing.T) {
		selectivity := 0.1
		tableRows := int64(1000)
		tablePages := int64(10)

		cost := model.EstimateIndexRangeScanCost(selectivity, tableRows, tablePages, false)

		estimatedRows := float64(tableRows) * selectivity
		expectedStartup := constants.IndexLookupCost
		indexIterationCost := estimatedRows * constants.IndexTupleCost
		boundCheckCost := estimatedRows * constants.CPUOperatorCost

		// Range scan heap cost calculation with correlation factor
		const correlationFactor = 0.8
		maxRandomPages := math.Min(estimatedRows, float64(tablePages))
		effectiveRandomPages := maxRandomPages * correlationFactor
		sequentialPages := maxRandomPages * (1 - correlationFactor)
		heapCost := effectiveRandomPages*constants.RandomPageCost +
			sequentialPages*constants.SeqPageCost
		tupleCost := estimatedRows * constants.CPUTupleCost
		expectedTotal := expectedStartup + indexIterationCost + boundCheckCost + heapCost + tupleCost

		assert.InDelta(t, expectedStartup, cost.StartupCost, 0.001)
		assert.InDelta(t, expectedTotal, cost.TotalCost, 0.001)
		assert.InDelta(t, estimatedRows, cost.OutputRows, 0.001)
	})
}

func TestRangeScanVsPointLookupCostComparison(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	// Compare range scan and point lookup costs. The cost model has different
	// characteristics for each:
	//
	// Point lookup:
	//   - Random page cost for each estimated row (assuming worst-case random access)
	//   - No bound checking overhead
	//
	// Range scan:
	//   - Uses correlation factor (0.8 random, 0.2 sequential) for heap access
	//   - Additional bound checking cost (CPUOperatorCost per row)
	//
	// For index-only scans, range scan is MORE expensive due to bound checking.
	// For regular scans, the comparison depends on table characteristics.

	t.Run("index-only: range scan more expensive due to bound checking", func(t *testing.T) {
		// For index-only scans, the only difference is bound checking overhead
		// in range scans, making them more expensive
		pointLookupCost := model.EstimateIndexScanCost(0.01, 10000, 100, true)
		rangeScanCost := model.EstimateIndexRangeScanCost(0.01, 10000, 100, true)

		assert.Greater(t, rangeScanCost.TotalCost, pointLookupCost.TotalCost,
			"index-only range scan should be more expensive due to bound checking")
	})

	t.Run("regular scan: costs differ based on correlation model", func(t *testing.T) {
		// For regular scans, the heap access cost model differs:
		// - Point lookup assumes 100% random access
		// - Range scan uses correlation factor (80% random, 20% sequential)
		// The additional bound checking in range scan may or may not offset
		// the better heap access model, depending on table characteristics.
		pointLookupCost := model.EstimateIndexScanCost(0.1, 10000, 100, false)
		rangeScanCost := model.EstimateIndexRangeScanCost(0.1, 10000, 100, false)

		// Both should have reasonable costs
		assert.Greater(t, pointLookupCost.TotalCost, 0.0)
		assert.Greater(t, rangeScanCost.TotalCost, 0.0)

		// Log the costs for analysis
		t.Logf("Point lookup cost: %.2f, Range scan cost: %.2f (selectivity 10%%)",
			pointLookupCost.TotalCost, rangeScanCost.TotalCost)
	})

	t.Run("same output rows for same selectivity", func(t *testing.T) {
		// Both methods should estimate the same output rows
		pointLookupCost := model.EstimateIndexScanCost(0.1, 10000, 100, false)
		rangeScanCost := model.EstimateIndexRangeScanCost(0.1, 10000, 100, false)

		assert.InDelta(t, pointLookupCost.OutputRows, rangeScanCost.OutputRows, 0.001,
			"both methods should estimate same output rows")
	})
}

func TestShouldUseIndexRangeScan(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	testCases := []struct {
		name        string
		selectivity float64
		tableRows   int64
		tablePages  int64
		isIndexOnly bool
		wantIndex   bool
	}{
		{
			name:        "very selective - should use range scan",
			selectivity: 0.001, // 0.1% of rows
			tableRows:   100000,
			tablePages:  1000,
			isIndexOnly: false,
			wantIndex:   true, // Range scan should win for very selective queries
		},
		{
			name:        "index-only range scan - should use range scan",
			selectivity: 0.01,
			tableRows:   100000,
			tablePages:  1000,
			isIndexOnly: true,
			wantIndex:   true, // Index-only is much cheaper
		},
		{
			name:        "low selectivity - should use seq scan",
			selectivity: 0.9, // 90% of rows
			tableRows:   100000,
			tablePages:  1000,
			isIndexOnly: false,
			wantIndex:   false, // Seq scan better for most rows
		},
		{
			name:        "medium selectivity - depends on table size",
			selectivity: 0.3,
			tableRows:   100,
			tablePages:  1,
			isIndexOnly: false,
			wantIndex:   true, // With low RandomPageCost, range scan is viable
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := model.ShouldUseIndexRangeScan(
				tc.selectivity,
				tc.tableRows,
				tc.tablePages,
				tc.isIndexOnly,
			)
			assert.Equal(t, tc.wantIndex, result)
		})
	}
}

func TestRangeScanVsSeqScanCrossover(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	// Test the crossover point where seq scan becomes cheaper than range scan
	// Range scans should have a lower crossover threshold than point lookups
	// because they are more expensive
	tableRows := int64(100000)
	tablePages := int64(1000)

	// Find approximate crossover point for range scan
	var rangeScanCrossover float64
	for sel := 0.001; sel <= 1.0; sel += 0.001 {
		rangeCost := model.EstimateIndexRangeScanCost(sel, tableRows, tablePages, false)
		seqCost := model.EstimateSeqScanCost(tableRows, tablePages, sel)

		if seqCost.TotalCost < rangeCost.TotalCost {
			rangeScanCrossover = sel
			break
		}
	}

	// Find approximate crossover point for point lookup
	var pointLookupCrossover float64
	for sel := 0.001; sel <= 1.0; sel += 0.001 {
		indexCost := model.EstimateIndexScanCost(sel, tableRows, tablePages, false)
		seqCost := model.EstimateSeqScanCost(tableRows, tablePages, sel)

		if seqCost.TotalCost < indexCost.TotalCost {
			pointLookupCrossover = sel
			break
		}
	}

	// Range scan crossover should be at a lower selectivity than point lookup
	// because range scan is more expensive (worse locality, bound checking)
	assert.Less(t, rangeScanCrossover, pointLookupCrossover,
		"range scan crossover (%.2f%%) should be lower than point lookup crossover (%.2f%%)",
		rangeScanCrossover*100, pointLookupCrossover*100)

	// Crossover should happen at reasonable thresholds
	assert.GreaterOrEqual(t, rangeScanCrossover, 0.001, "crossover should be at or above 0.1%")
	assert.Less(t, rangeScanCrossover, 0.90, "crossover should be below 90%")

	t.Logf("Range scan crossover: %.2f%%, Point lookup crossover: %.2f%%",
		rangeScanCrossover*100, pointLookupCrossover*100)
}

func TestRangeScanCostEdgeCases(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	t.Run("zero selectivity", func(t *testing.T) {
		cost := model.EstimateIndexRangeScanCost(0, 10000, 100, false)
		assert.GreaterOrEqual(t, cost.OutputRows, 1.0, "should have minimum 1 row")
		assert.Greater(t, cost.TotalCost, 0.0, "should have positive cost")
	})

	t.Run("selectivity above 1 is capped", func(t *testing.T) {
		cost := model.EstimateIndexRangeScanCost(2.0, 1000, 10, false)
		assert.LessOrEqual(t, cost.OutputRows, 1000.0, "output rows should not exceed table rows")
	})

	t.Run("negative selectivity is capped", func(t *testing.T) {
		cost := model.EstimateIndexRangeScanCost(-0.5, 1000, 10, false)
		assert.GreaterOrEqual(t, cost.OutputRows, 1.0, "should have minimum 1 row")
	})

	t.Run("very large table", func(t *testing.T) {
		cost := model.EstimateIndexRangeScanCost(0.001, 1e9, 1e7, false)
		assert.Greater(t, cost.TotalCost, 0.0)
		assert.False(t, math.IsInf(cost.TotalCost, 0))
		assert.False(t, math.IsNaN(cost.TotalCost))
	})
}

func TestCompareAccessMethods(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)

	t.Run("highly selective - index wins", func(t *testing.T) {
		method, cost := model.CompareAccessMethods(0.001, 100000, 1000, false, false)
		assert.Equal(t, "IndexScan", method)
		assert.Greater(t, cost.TotalCost, 0.0)
	})

	t.Run("highly selective range - range scan wins", func(t *testing.T) {
		method, cost := model.CompareAccessMethods(0.001, 100000, 1000, false, true)
		assert.Equal(t, "IndexRangeScan", method)
		assert.Greater(t, cost.TotalCost, 0.0)
	})

	t.Run("low selectivity - seq scan wins", func(t *testing.T) {
		method, cost := model.CompareAccessMethods(0.9, 100000, 1000, false, false)
		assert.Equal(t, "SeqScan", method)
		assert.Greater(t, cost.TotalCost, 0.0)
	})

	t.Run("low selectivity range - seq scan wins", func(t *testing.T) {
		method, cost := model.CompareAccessMethods(0.9, 100000, 1000, false, true)
		assert.Equal(t, "SeqScan", method)
		assert.Greater(t, cost.TotalCost, 0.0)
	})

	t.Run("index-only gives lower cost", func(t *testing.T) {
		_, costRegular := model.CompareAccessMethods(0.01, 100000, 1000, false, false)
		_, costIndexOnly := model.CompareAccessMethods(0.01, 100000, 1000, true, false)
		assert.Less(t, costIndexOnly.TotalCost, costRegular.TotalCost,
			"index-only should have lower cost than regular index scan")
	})
}

func TestOptimizerChoosesCorrectAccessMethod(t *testing.T) {
	// This tests that the cost model produces costs that lead to correct
	// access method selection by the optimizer
	model := NewCostModel(DefaultCostConstants(), nil)

	// Test different scenarios
	scenarios := []struct {
		name          string
		selectivity   float64
		tableRows     int64
		tablePages    int64
		isIndexOnly   bool
		isRangeScan   bool
		expectedBest  string
		expectedWorst string
	}{
		{
			name:          "point lookup beats range scan for equality",
			selectivity:   0.01,
			tableRows:     10000,
			tablePages:    100,
			isIndexOnly:   false,
			isRangeScan:   false,
			expectedBest:  "IndexScan",
			expectedWorst: "SeqScan", // At 1% selectivity, seq scan is worst
		},
		{
			name:          "range scan beats seq scan for selective range",
			selectivity:   0.01,
			tableRows:     100000,
			tablePages:    1000,
			isIndexOnly:   false,
			isRangeScan:   true,
			expectedBest:  "IndexRangeScan",
			expectedWorst: "SeqScan",
		},
		{
			name:          "seq scan beats range scan for unselective range",
			selectivity:   0.9,
			tableRows:     100000,
			tablePages:    1000,
			isIndexOnly:   false,
			isRangeScan:   true,
			expectedBest:  "SeqScan",
			expectedWorst: "IndexRangeScan",
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			method, _ := model.CompareAccessMethods(
				sc.selectivity, sc.tableRows, sc.tablePages,
				sc.isIndexOnly, sc.isRangeScan,
			)
			assert.Equal(t, sc.expectedBest, method)
		})
	}
}

func BenchmarkEstimateIndexScanCost(b *testing.B) {
	model := NewCostModel(DefaultCostConstants(), nil)
	b.ResetTimer()

	for range b.N {
		_ = model.EstimateIndexScanCost(0.01, 100000, 1000, false)
	}
}

func BenchmarkEstimateIndexRangeScanCost(b *testing.B) {
	model := NewCostModel(DefaultCostConstants(), nil)
	b.ResetTimer()

	for range b.N {
		_ = model.EstimateIndexRangeScanCost(0.01, 100000, 1000, false)
	}
}

func BenchmarkShouldUseIndexScan(b *testing.B) {
	model := NewCostModel(DefaultCostConstants(), nil)
	b.ResetTimer()

	for range b.N {
		_ = model.ShouldUseIndexScan(0.01, 100000, 1000, false)
	}
}

func BenchmarkShouldUseIndexRangeScan(b *testing.B) {
	model := NewCostModel(DefaultCostConstants(), nil)
	b.ResetTimer()

	for range b.N {
		_ = model.ShouldUseIndexRangeScan(0.01, 100000, 1000, false)
	}
}

func BenchmarkCompareAccessMethods(b *testing.B) {
	model := NewCostModel(DefaultCostConstants(), nil)
	b.ResetTimer()

	for range b.N {
		_, _ = model.CompareAccessMethods(0.01, 100000, 1000, false, true)
	}
}
