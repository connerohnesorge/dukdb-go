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
	assert.Equal(t, 4.0, constants.RandomPageCost)
	assert.Equal(t, 0.01, constants.CPUTupleCost)
	assert.Equal(t, 0.0025, constants.CPUOperatorCost)
	assert.Equal(t, 0.02, constants.HashBuildCost)
	assert.Equal(t, 0.01, constants.HashProbeCost)
	assert.Equal(t, 0.05, constants.SortCost)
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
