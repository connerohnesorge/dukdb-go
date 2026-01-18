package optimizer

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiColumnStatsManager tests the manager for storing and retrieving multi-column statistics.
func TestMultiColumnStatsManager(t *testing.T) {
	t.Run("AddStats and GetStats", func(t *testing.T) {
		mgr := NewMultiColumnStatsManager("test_table")

		stats := &MultiColumnStats{
			Columns:           [2]string{"col1", "col2"},
			JointNDV:          100,
			Correlation:       0.85,
			SampleSize:        1000,
			RecommendedForUse: true,
		}

		result := mgr.AddStats(stats)
		assert.True(t, result, "Should successfully add stats to empty manager")

		retrieved := mgr.GetStats("col1", "col2")
		require.NotNil(t, retrieved)
		assert.Equal(t, int64(100), retrieved.JointNDV)
		assert.InDelta(t, 0.85, retrieved.Correlation, 0.001)
	})

	t.Run("Key normalization (alphabetical order)", func(t *testing.T) {
		mgr := NewMultiColumnStatsManager("test_table")

		stats := &MultiColumnStats{
			Columns:           [2]string{"zebra", "apple"},
			JointNDV:          50,
			Correlation:       0.5,
			SampleSize:        500,
			RecommendedForUse: false,
		}

		mgr.AddStats(stats)

		// Should be retrievable with columns in either order
		retrieved1 := mgr.GetStats("zebra", "apple")
		retrieved2 := mgr.GetStats("apple", "zebra")

		require.NotNil(t, retrieved1)
		require.NotNil(t, retrieved2)
		assert.Equal(t, int64(50), retrieved1.JointNDV)
		assert.Equal(t, int64(50), retrieved2.JointNDV)
	})

	t.Run("Capacity enforcement", func(t *testing.T) {
		mgr := NewMultiColumnStatsManager("test_table")
		mgr.MaxPairs = 2 // Small capacity for testing

		// Add first pair
		stats1 := &MultiColumnStats{
			Columns:           [2]string{"col1", "col2"},
			JointNDV:          100,
			Correlation:       0.5,
			SampleSize:        500,
			RecommendedForUse: false,
		}
		assert.True(t, mgr.AddStats(stats1))

		// Add second pair
		stats2 := &MultiColumnStats{
			Columns:           [2]string{"col3", "col4"},
			JointNDV:          200,
			Correlation:       0.6,
			SampleSize:        600,
			RecommendedForUse: false,
		}
		assert.True(t, mgr.AddStats(stats2))

		// Try to add third pair with low correlation (should fail)
		stats3 := &MultiColumnStats{
			Columns:           [2]string{"col5", "col6"},
			JointNDV:          300,
			Correlation:       0.3, // Low correlation
			SampleSize:        700,
			RecommendedForUse: false,
		}
		assert.False(t, mgr.AddStats(stats3), "Should reject low-correlation pair when at capacity")

		// Try to add third pair with high correlation (should replace weakest)
		stats4 := &MultiColumnStats{
			Columns:           [2]string{"col7", "col8"},
			JointNDV:          400,
			Correlation:       0.9, // Strong correlation
			SampleSize:        1000,
			RecommendedForUse: true,
		}
		assert.True(
			t,
			mgr.AddStats(stats4),
			"Should add strong-correlation pair, replacing weaker pair",
		)
		assert.Equal(t, 2, len(mgr.Stats))
	})
}

// TestCorrelationDetector tests computation of Pearson correlation coefficients.
func TestCorrelationDetector(t *testing.T) {
	t.Run("Perfect positive correlation", func(t *testing.T) {
		detector := NewCorrelationDetector()

		// x = [1, 2, 3, 4, 5], y = [2, 4, 6, 8, 10]
		// y = 2*x (perfect correlation)
		for i := 1.0; i <= 5; i += 1 {
			detector.AddSample("x", i)
			detector.AddSample("y", 2*i)
		}

		correlation := detector.ComputeCorrelation("x", "y")
		assert.InDelta(t, 1.0, correlation, 0.01, "Should have correlation close to 1.0")
	})

	t.Run("Perfect negative correlation", func(t *testing.T) {
		detector := NewCorrelationDetector()

		// x = [1, 2, 3, 4, 5], y = [5, 4, 3, 2, 1]
		// y = 6 - x (perfect negative correlation)
		valuesX := []float64{1, 2, 3, 4, 5}
		valuesY := []float64{5, 4, 3, 2, 1}

		for i := 0; i < len(valuesX); i++ {
			detector.AddSample("x", valuesX[i])
			detector.AddSample("y", valuesY[i])
		}

		correlation := detector.ComputeCorrelation("x", "y")
		assert.InDelta(t, -1.0, correlation, 0.01, "Should have correlation close to -1.0")
	})

	t.Run("No correlation (independent)", func(t *testing.T) {
		detector := NewCorrelationDetector()

		// x and y are independent random values
		xVals := []float64{1, 2, 3, 4, 5, 10, 20, 30}
		yVals := []float64{5, 10, 2, 15, 3, 1, 25, 8}

		for i := 0; i < len(xVals); i++ {
			detector.AddSample("x", xVals[i])
			detector.AddSample("y", yVals[i])
		}

		correlation := detector.ComputeCorrelation("x", "y")
		// Should be close to 0 (independent)
		assert.Less(
			t,
			math.Abs(correlation),
			0.5,
			"Should have low correlation for independent data",
		)
	})

	t.Run("Missing column", func(t *testing.T) {
		detector := NewCorrelationDetector()
		detector.AddSample("x", 1)
		detector.AddSample("x", 2)

		correlation := detector.ComputeCorrelation("x", "y")
		assert.Equal(t, 0.0, correlation, "Should return 0 if column is missing")
	})

	t.Run("Insufficient samples", func(t *testing.T) {
		detector := NewCorrelationDetector()
		detector.AddSample("x", 1)
		detector.AddSample("y", 2)

		correlation := detector.ComputeCorrelation("x", "y")
		assert.Equal(t, 0.0, correlation, "Should return 0 with less than 2 samples")
	})

	t.Run("Zero variance (constant column)", func(t *testing.T) {
		detector := NewCorrelationDetector()

		// x is constant, y varies
		for i := 0; i < 5; i++ {
			detector.AddSample("x", 1.0) // Constant
			detector.AddSample("y", float64(i))
		}

		correlation := detector.ComputeCorrelation("x", "y")
		assert.Equal(t, 0.0, correlation, "Should return 0 if a column has zero variance")
	})
}

// TestJointNDVEstimator tests Good-Turing estimation for joint distinct values.
func TestJointNDVEstimator(t *testing.T) {
	t.Run("Simple case: all combinations seen", func(t *testing.T) {
		estimator := NewJointNDVEstimator()

		// Add samples: (a, 1), (a, 2), (b, 1), (b, 2)
		estimator.AddSample("col1", "col2", "a", "1")
		estimator.AddSample("col1", "col2", "a", "2")
		estimator.AddSample("col1", "col2", "b", "1")
		estimator.AddSample("col1", "col2", "b", "2")

		// With 4 samples and 4 unique combinations, estimate should be close to 4
		// Good-Turing: u=4, s=4, estimate = 4 + (4/4)²*4 / 4 * 0 = 4
		estimate := estimator.EstimateJointNDV("col1", "col2", 4)
		assert.Equal(t, int64(4), estimate, "Should estimate exactly 4 combinations (seen all)")
	})

	t.Run("Extrapolation: sample to full table", func(t *testing.T) {
		estimator := NewJointNDVEstimator()

		// Simulate: table has 10000 rows, sample 100 rows
		// In sample: see 50 unique combinations
		for i := 0; i < 50; i++ {
			estimator.AddSample("city", "state",
				string(rune(65+i%26)), // A-Z cycle for city
				string(rune(97+i%10))) // a-j cycle for state
		}

		// Add padding samples to reach 100 total samples
		for i := 50; i < 100; i++ {
			estimator.AddSample("city", "state",
				string(rune(65+i%26)), // Repeat some combinations
				string(rune(97+i%10)))
		}

		estimate := estimator.EstimateJointNDV("city", "state", 10000)

		// Should estimate more than 50 (accounting for unseen combinations)
		// but less than 10000 (not every row is unique)
		assert.Greater(
			t,
			estimate,
			int64(50),
			"Should estimate more combinations than seen in sample",
		)
		assert.LessOrEqual(t, estimate, int64(10000), "Should not exceed total row count")
	})

	t.Run("Column order normalization", func(t *testing.T) {
		estimator := NewJointNDVEstimator()

		// Add with col1, col2 order
		estimator.AddSample("col1", "col2", "a", "1")
		estimator.AddSample("col1", "col2", "b", "2")

		// Estimate with reversed column order
		estimate := estimator.EstimateJointNDV("col2", "col1", 2)
		assert.Greater(t, estimate, int64(0), "Should handle reversed column order")
	})

	t.Run("Empty estimator", func(t *testing.T) {
		estimator := NewJointNDVEstimator()
		estimate := estimator.EstimateJointNDV("col1", "col2", 100)
		assert.Equal(t, int64(1), estimate, "Should return 1 for missing data")
	})
}

// TestColumnPairSelector tests heuristics for selecting important column pairs.
func TestColumnPairSelector(t *testing.T) {
	t.Run("Strong correlation heuristic", func(t *testing.T) {
		selector := NewColumnPairSelector()

		col1 := &ColumnStatistics{
			ColumnName:    "col1",
			DistinctCount: 100,
		}
		col2 := &ColumnStatistics{
			ColumnName:    "col2",
			DistinctCount: 50,
		}

		// Strong correlation (0.85 > 0.7)
		should := selector.ShouldTrack(col1, col2, 0.85, 5000, 10000)
		assert.True(t, should, "Should track pair with strong correlation")

		// Weak correlation (0.5 < 0.7)
		should = selector.ShouldTrack(col1, col2, 0.5, 5000, 10000)
		assert.False(t, should, "Should not track pair with weak correlation")
	})

	t.Run("Functional dependency heuristic", func(t *testing.T) {
		selector := NewColumnPairSelector()

		col1 := &ColumnStatistics{
			ColumnName:    "state",
			DistinctCount: 50,
		}
		col2 := &ColumnStatistics{
			ColumnName:    "city",
			DistinctCount: 5000,
		}

		// Functional dependency: joint NDV much less than product
		// Expected: 50 * 5000 = 250,000
		// Actual: 5,000 (city determines state, not independent)
		should := selector.ShouldTrack(col1, col2, 0.5, 5000, 100000)
		assert.True(
			t,
			should,
			"Should track pair with functional dependency (joint NDV << independent)",
		)
	})

	t.Run("Low cardinality categorical pairing", func(t *testing.T) {
		selector := NewColumnPairSelector()

		col1 := &ColumnStatistics{
			ColumnName:    "dept",
			DistinctCount: 10,
		}
		col2 := &ColumnStatistics{
			ColumnName:    "job",
			DistinctCount: 15,
		}

		// Low cardinality, many rows per combination
		// Expected: 10 * 15 = 150
		// Actual: 40 (50% of 150, categories form pairs, not all combinations exist)
		// This triggers heuristic 3: minCardinality=10, expectedIndependent=100
		// jointNDV=40 < 50 (50% of 100)
		should := selector.ShouldTrack(col1, col2, 0.3, 40, 10000)
		assert.True(t, should, "Should track low-cardinality categorical pairing")
	})

	t.Run("Independent columns not tracked", func(t *testing.T) {
		selector := NewColumnPairSelector()

		col1 := &ColumnStatistics{
			ColumnName:    "salary",
			DistinctCount: 1000,
		}
		col2 := &ColumnStatistics{
			ColumnName:    "city",
			DistinctCount: 50,
		}

		// Independent columns: correlation ~ 0, joint NDV ≈ 1000*50
		should := selector.ShouldTrack(col1, col2, 0.1, 50000, 100000)
		assert.False(t, should, "Should not track independent columns")
	})
}

// TestCardinalityWithMultiColumnStats tests selectivity calculation with multi-column stats.
func TestCardinalityWithMultiColumnStats(t *testing.T) {
	t.Run("With multi-column stats (more accurate)", func(t *testing.T) {
		stats := &MultiColumnStats{
			Columns:           [2]string{"city", "state"},
			JointNDV:          5000,
			Correlation:       0.8,
			SampleSize:        50000,
			RecommendedForUse: true,
		}

		// Table with 100,000 rows
		totalRows := int64(100000)
		col1Sel := 0.02 // city='NY' (1/50 cities)
		col2Sel := 0.01 // state='NY' (1/100 states)

		cardWithMultiStats := CardinalityWithMultiColumnStats(totalRows, col1Sel, col2Sel, stats)

		// With multi-column stats: selectivity = 1/5000 ≈ 0.0002
		expectedRowCount := float64(totalRows) * cardWithMultiStats
		assert.Greater(t, expectedRowCount, 0.0, "Should return positive selectivity")
		assert.LessOrEqual(t, expectedRowCount, float64(totalRows), "Should not exceed total rows")
	})

	t.Run("Fallback to independence without multi-column stats", func(t *testing.T) {
		totalRows := int64(100000)
		col1Sel := 0.02
		col2Sel := 0.01

		// No multi-column stats: uses independence assumption
		selectivity := CardinalityWithMultiColumnStats(totalRows, col1Sel, col2Sel, nil)

		// Independence: selectivity = 0.02 * 0.01 = 0.0002
		expectedRowCount := float64(totalRows) * selectivity
		assert.InDelta(
			t,
			20.0,
			expectedRowCount,
			1.0,
			"Should estimate ~20 rows with independence (100000 * 0.02 * 0.01)",
		)
	})

	t.Run("Avoid divide by zero", func(t *testing.T) {
		stats := &MultiColumnStats{
			Columns:           [2]string{"col1", "col2"},
			JointNDV:          0,
			Correlation:       0.0,
			SampleSize:        0,
			RecommendedForUse: false,
		}

		cardWithBadStats := CardinalityWithMultiColumnStats(100000, 0.1, 0.1, stats)
		// Should fallback to independence
		assert.InDelta(t, 0.01, cardWithBadStats, 0.001, "Should fallback when JointNDV is 0")
	})
}

// TestSelectTopColumnPairs tests selection of important column pairs.
func TestSelectTopColumnPairs(t *testing.T) {
	t.Run("Select top pairs by importance", func(t *testing.T) {
		candidates := []*MultiColumnStats{
			{
				Columns:           [2]string{"a", "b"},
				JointNDV:          100,
				Correlation:       0.5,
				SampleSize:        100,
				RecommendedForUse: true,
			},
			{
				Columns:           [2]string{"c", "d"},
				JointNDV:          200,
				Correlation:       0.9,  // High correlation
				SampleSize:        5000, // Large sample
				RecommendedForUse: true,
			},
			{
				Columns:           [2]string{"e", "f"},
				JointNDV:          300,
				Correlation:       0.2,
				SampleSize:        50,
				RecommendedForUse: false,
			},
		}

		// Score: abs(correlation) * sqrt(sample_size)
		// c,d: 0.9 * sqrt(5000) ≈ 63.6 (highest)
		// a,b: 0.5 * sqrt(100) = 5 (middle)
		// e,f: 0.2 * sqrt(50) ≈ 1.4 (lowest)

		selected := SelectTopColumnPairs(candidates, 2)

		require.Equal(t, 2, len(selected), "Should select exactly 2 pairs")
		assert.Equal(t, [2]string{"c", "d"}, selected[0].Columns, "Highest importance pair first")
		assert.Equal(
			t,
			[2]string{"a", "b"},
			selected[1].Columns,
			"Second highest importance pair second",
		)
	})

	t.Run("Return all if fewer than max", func(t *testing.T) {
		candidates := []*MultiColumnStats{
			{Columns: [2]string{"a", "b"}, Correlation: 0.5, SampleSize: 100},
			{Columns: [2]string{"c", "d"}, Correlation: 0.8, SampleSize: 200},
		}

		selected := SelectTopColumnPairs(candidates, 10) // Request more than available
		assert.Equal(t, 2, len(selected), "Should return all candidates")
	})
}

// BenchmarkCorrelationDetector benchmarks correlation computation.
func BenchmarkCorrelationDetector(b *testing.B) {
	detector := NewCorrelationDetector()

	// Pre-populate with samples
	for i := 0; i < 10000; i++ {
		detector.AddSample("x", float64(i))
		detector.AddSample("y", float64(i)*2) // Strong correlation
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = detector.ComputeCorrelation("x", "y")
	}
}

// BenchmarkJointNDVEstimator benchmarks joint NDV estimation.
func BenchmarkJointNDVEstimator(b *testing.B) {
	estimator := NewJointNDVEstimator()

	// Pre-populate with combinations
	for i := 0; i < 10000; i++ {
		estimator.AddSample("col1", "col2",
			string(rune(65+i%26)),
			string(rune(97+i%26)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = estimator.EstimateJointNDV("col1", "col2", 100000)
	}
}

// Helper function for test data generation.
// Returns x and y where y = 2*x (perfect positive correlation).
func generateCorrelatedData(n int) (xValues, yValues []float64) {
	x := make([]float64, n)
	y := make([]float64, n)

	for i := 0; i < n; i++ {
		x[i] = float64(i)
		y[i] = float64(i) * 2.0 // Perfect correlation
	}

	return x, y
}
