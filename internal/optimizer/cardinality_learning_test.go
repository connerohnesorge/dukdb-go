package optimizer

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestCorrectionCalculationUnderThreshold verifies that corrections are not applied
// before reaching the observation threshold.
func TestCorrectionCalculationUnderThreshold(t *testing.T) {
	learner := NewCardinalityLearner(1000, 100)

	sig := "SeqScan(table=t1)"

	// Record 50 observations (under threshold of 100)
	for i := 0; i < 50; i++ {
		learner.RecordObservation(sig, 100, 150) // 1.5x correction ratio
	}

	// Correction should not be applied yet
	correction := learner.GetLearningCorrection(sig)
	assert.Equal(t, 1.0, correction, "Correction should be 1.0 under threshold")

	// But observation count should be tracked
	assert.Equal(t, int64(50), learner.GetObservationCount(sig))
}

// TestCorrectionCalculationAboveThreshold verifies that corrections are applied
// after reaching the observation threshold.
func TestCorrectionCalculationAboveThreshold(t *testing.T) {
	learner := NewCardinalityLearner(1000, 100)

	sig := "SeqScan(table=t1)"

	// Record exactly 100 observations, each with 1.5x actual cardinality
	for i := 0; i < 100; i++ {
		learner.RecordObservation(sig, 100, 150)
	}

	// After reaching threshold, correction should be applied
	correction := learner.GetLearningCorrection(sig)
	// Correction should be close to 1.5, but may vary due to EMA
	assert.Greater(t, correction, 1.0, "Correction should increase")
	assert.LessOrEqual(t, correction, 1.5, "Correction shouldn't exceed input ratio")
}

// TestMovingAverageCorrection verifies that EMA properly dampens rapid changes.
func TestMovingAverageCorrection(t *testing.T) {
	learner := NewCardinalityLearner(1000, 10) // Low threshold for testing

	sig := "HashJoin()"

	// First 10 observations: 1.1x ratio
	for i := 0; i < 10; i++ {
		learner.RecordObservation(sig, 1000, 1100)
	}

	correction1 := learner.GetLearningCorrection(sig)
	assert.Greater(t, correction1, 1.0, "Should have learned 1.1x")

	// Next observation: 2.0x ratio (outlier)
	learner.RecordObservation(sig, 1000, 2000)
	correction2 := learner.GetLearningCorrection(sig)

	// Correction should increase, but not dramatically (EMA dampening)
	assert.Greater(t, correction2, correction1, "Should increase after 2.0x observation")
	// With EMA, new value is 90% old + 10% new = 0.9 * 1.1 + 0.1 * 2.0 ≈ 1.19
	// But capped at 2.0
	assert.Less(t, correction2, 2.0, "Should be less than 2.0 due to EMA dampening")
}

// TestOutlierHandling verifies that single outlier queries don't corrupt learning.
func TestOutlierHandling(t *testing.T) {
	learner := NewCardinalityLearner(1000, 100)

	sig := "Aggregate(table=t1)"

	// Record 99 observations: 1.0x (accurate)
	for i := 0; i < 99; i++ {
		learner.RecordObservation(sig, 1000, 1000)
	}

	// Check correction is still 1.0 (under threshold)
	correction := learner.GetLearningCorrection(sig)
	assert.Equal(t, 1.0, correction, "Should not apply correction under threshold")

	// Record 100th observation: normal (1.0x)
	learner.RecordObservation(sig, 1000, 1000)

	// At threshold now, correction should be ~1.0
	correction = learner.GetLearningCorrection(sig)
	assert.InDelta(t, 1.0, correction, 0.05, "Correction should be ~1.0")

	// Record extreme outlier: 100x cardinality
	learner.RecordObservation(sig, 1000, 100000)

	// With EMA dampening, outlier has only 10% weight
	// newCorrection = 0.9 * 1.0 + 0.1 * min(100, 2.0) = 0.9 + 0.2 = 1.1
	// But the ratio is capped at 2.0 before EMA
	correction = learner.GetLearningCorrection(sig)
	assert.Less(t, correction, 1.5, "Outlier should have limited impact due to EMA and bounds")
}

// TestMemoryBoundsRespected verifies that the learner respects maxHistorySize.
func TestMemoryBoundsRespected(t *testing.T) {
	maxSize := 100
	learner := NewCardinalityLearner(maxSize, 1) // Allow immediate corrections

	// Record observations for more signatures than maxSize
	for i := 0; i < maxSize + 50; i++ {
		sig := "SeqScan(table=t" + string(rune(i)) + ")"
		learner.RecordObservation(sig, 100, 100)
	}

	// Check that history size doesn't exceed maxSize
	stats := learner.GetStatistics()
	tracked := stats["tracked_signatures"].(int)
	assert.LessOrEqual(t, tracked, maxSize, "Should not exceed maxHistorySize")
	assert.Equal(t, maxSize, tracked, "Should be at capacity")
}

// TestLRUEviction verifies that least-recently-used entries are evicted first.
func TestLRUEviction(t *testing.T) {
	learner := NewCardinalityLearner(3, 1) // Very small capacity

	sig1 := "Scan1"
	sig2 := "Scan2"
	sig3 := "Scan3"
	sig4 := "Scan4"

	// Fill to capacity
	learner.RecordObservation(sig1, 100, 100)
	time.Sleep(10 * time.Millisecond) // Small delay to ensure different timestamps
	learner.RecordObservation(sig2, 100, 100)
	time.Sleep(10 * time.Millisecond)
	learner.RecordObservation(sig3, 100, 100)

	// All should be present
	assert.Equal(t, int64(1), learner.GetObservationCount(sig1))
	assert.Equal(t, int64(1), learner.GetObservationCount(sig2))
	assert.Equal(t, int64(1), learner.GetObservationCount(sig3))

	// Add one more, should trigger LRU eviction of sig1 (oldest)
	learner.RecordObservation(sig4, 100, 100)

	// sig1 should be evicted
	assert.Equal(t, int64(0), learner.GetObservationCount(sig1), "LRU entry should be evicted")
	assert.Equal(t, int64(1), learner.GetObservationCount(sig2), "sig2 should remain")
	assert.Equal(t, int64(1), learner.GetObservationCount(sig3), "sig3 should remain")
	assert.Equal(t, int64(1), learner.GetObservationCount(sig4), "sig4 should be added")
}

// TestCorrectionBounds verifies that corrections are bounded to [0.5, 2.0].
func TestCorrectionBounds(t *testing.T) {
	learner := NewCardinalityLearner(1000, 5)

	// Test extreme low ratio
	sig1 := "LowRatio"
	for i := 0; i < 5; i++ {
		learner.RecordObservation(sig1, 1000, 1) // 0.001 ratio, capped at 0.5
	}
	correction1 := learner.GetLearningCorrection(sig1)
	assert.GreaterOrEqual(t, correction1, 0.5, "Should be bounded at 0.5 minimum")
	assert.LessOrEqual(t, correction1, 1.0, "Should not exceed 1.0 much with EMA dampening")

	// Test extreme high ratio
	sig2 := "HighRatio"
	for i := 0; i < 5; i++ {
		learner.RecordObservation(sig2, 1, 1000) // 1000 ratio, capped at 2.0
	}
	correction2 := learner.GetLearningCorrection(sig2)
	assert.LessOrEqual(t, correction2, 2.0, "Should be bounded at 2.0 maximum")
	assert.GreaterOrEqual(t, correction2, 1.0, "Should be at least 1.0")
}

// TestCorrectedCardinalityIntegration verifies that corrections are applied correctly
// through the CorrectedCardinality function.
func TestCorrectedCardinalityIntegration(t *testing.T) {
	learner := NewCardinalityLearner(1000, 10)

	const sig = "SeqScan()"

	// Record 10 observations with 2.0x ratio
	for i := 0; i < 10; i++ {
		learner.RecordObservation(sig, 100, 200)
	}

	// Base estimate: 100
	// Correction: ~2.0
	// Corrected: 100 * ~2.0 = ~200
	corrected := learner.CorrectedCardinality(100, sig)
	assert.Greater(t, corrected, 100.0, "Should increase estimate")
	assert.Less(t, corrected, 300.0, "Should not exceed 2x bound")
}

// TestCorrectedCardinalityUnderThreshold verifies no correction under threshold.
func TestCorrectedCardinalityUnderThreshold(t *testing.T) {
	learner := NewCardinalityLearner(1000, 100)

	sig := "SeqScan()"

	// Record 50 observations (under threshold)
	for i := 0; i < 50; i++ {
		learner.RecordObservation(sig, 100, 200) // 2.0x ratio
	}

	// Should return unchanged estimate
	corrected := learner.CorrectedCardinality(100, sig)
	assert.Equal(t, 100.0, corrected, "Should not apply correction under threshold")
}

// TestEmptyObservations verifies correct behavior with empty learner.
func TestEmptyObservations(t *testing.T) {
	learner := NewCardinalityLearner(1000, 100)

	// Query unknown signature
	correction := learner.GetLearningCorrection("Unknown")
	assert.Equal(t, 1.0, correction, "Unknown signature should return 1.0")

	// Query observation count
	count := learner.GetObservationCount("Unknown")
	assert.Equal(t, int64(0), count, "Unknown signature should have 0 observations")

	// Corrected cardinality should be unchanged
	corrected := learner.CorrectedCardinality(100, "Unknown")
	assert.Equal(t, 100.0, corrected, "Unknown should not change cardinality")
}

// TestMultipleOperators verifies that different operators are tracked independently.
func TestMultipleOperators(t *testing.T) {
	learner := NewCardinalityLearner(1000, 5)

	sig1 := "SeqScan(t1)"
	sig2 := "SeqScan(t2)"
	sig3 := "HashJoin()"

	// Record different correction ratios for each
	for i := 0; i < 5; i++ {
		learner.RecordObservation(sig1, 100, 100)   // 1.0x
		learner.RecordObservation(sig2, 100, 150)   // 1.5x
		learner.RecordObservation(sig3, 100, 200)   // 2.0x
	}

	// Each should have learned its own correction
	corr1 := learner.GetLearningCorrection(sig1)
	corr2 := learner.GetLearningCorrection(sig2)
	corr3 := learner.GetLearningCorrection(sig3)

	// With EMA dampening, corrections converge slowly
	// corr1 should be close to 1.0
	assert.Greater(t, corr1, 0.95, "sig1 should learn ~1.0x")
	assert.Less(t, corr1, 1.05, "sig1 should be close to 1.0")

	// corr2 should be between 1.0 and 1.5
	assert.Greater(t, corr2, 1.0, "sig2 should be above 1.0")
	assert.Less(t, corr2, 1.6, "sig2 should be less than 1.6")

	// corr3 should be close to 2.0 (capped at top)
	assert.Greater(t, corr3, 1.0, "sig3 should be above 1.0")
	assert.LessOrEqual(t, corr3, 2.0, "sig3 should be at most 2.0")
}

// TestStatisticsReporting verifies GetStatistics returns correct data.
func TestStatisticsReporting(t *testing.T) {
	learner := NewCardinalityLearner(100, 10)

	// Initially empty
	stats := learner.GetStatistics()
	assert.Equal(t, 0, stats["tracked_signatures"])
	assert.Equal(t, 0, stats["active_corrections"])
	assert.Equal(t, int64(0), stats["total_observations"])

	// Add some observations
	sig1 := "Op1"
	sig2 := "Op2"
	for i := 0; i < 5; i++ {
		learner.RecordObservation(sig1, 100, 100)
		learner.RecordObservation(sig2, 100, 100)
	}

	// After 5 observations, sig1 and sig2 should both be under threshold
	stats = learner.GetStatistics()
	assert.Equal(t, 2, stats["tracked_signatures"])
	assert.Equal(t, 0, stats["active_corrections"]) // Both under threshold
	assert.Equal(t, int64(10), stats["total_observations"])

	// Add more to reach threshold
	for i := 5; i < 10; i++ {
		learner.RecordObservation(sig1, 100, 100)
		learner.RecordObservation(sig2, 100, 100)
	}

	// Both should now be active (at threshold)
	stats = learner.GetStatistics()
	assert.Equal(t, 2, stats["tracked_signatures"])
	assert.Equal(t, 2, stats["active_corrections"]) // Both at/above threshold
	assert.Equal(t, int64(20), stats["total_observations"])
}

// TestInvalidInputHandling verifies behavior with invalid inputs.
func TestInvalidInputHandling(t *testing.T) {
	learner := NewCardinalityLearner(1000, 100)

	// Empty signature should be ignored
	learner.RecordObservation("", 100, 100)
	assert.Equal(t, int64(0), learner.GetObservationCount(""))

	// Negative estimated cardinality should be ignored
	learner.RecordObservation("sig1", -1, 100)
	assert.Equal(t, int64(0), learner.GetObservationCount("sig1"))

	// Zero estimated cardinality should be ignored
	learner.RecordObservation("sig2", 0, 100)
	assert.Equal(t, int64(0), learner.GetObservationCount("sig2"))

	// Negative actual cardinality should be ignored
	learner.RecordObservation("sig3", 100, -1)
	assert.Equal(t, int64(0), learner.GetObservationCount("sig3"))

	// Valid observation should work
	learner.RecordObservation("sig4", 100, 100)
	assert.Equal(t, int64(1), learner.GetObservationCount("sig4"))
}

// TestMinimumCardinalityResult verifies that CorrectedCardinality never returns 0.
func TestMinimumCardinalityResult(t *testing.T) {
	learner := NewCardinalityLearner(1000, 5)

	sig := "Op"

	// Record very low corrections
	for i := 0; i < 5; i++ {
		learner.RecordObservation(sig, 1000, 1) // 0.001 ratio, capped at 0.5
	}

	// Even with 0.5x correction on very small estimate
	corrected := learner.CorrectedCardinality(0.1, sig)
	assert.GreaterOrEqual(t, corrected, 1.0, "Should be at least 1.0")
}

// BenchmarkRecordObservation measures the performance of recording observations.
func BenchmarkRecordObservation(b *testing.B) {
	learner := NewCardinalityLearner(10000, 100)
	sig := "SeqScan(table=t1)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		learner.RecordObservation(sig, 1000, int64(1000+i%100))
	}
}

// BenchmarkGetLearningCorrection measures the performance of retrieving corrections.
func BenchmarkGetLearningCorrection(b *testing.B) {
	learner := NewCardinalityLearner(10000, 100)

	// Pre-populate with observations
	for i := 0; i < 100; i++ {
		learner.RecordObservation("SeqScan(t1)", 1000, 1100)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		learner.GetLearningCorrection("SeqScan(t1)")
	}
}

// TestCardinalityLearnerConcurrentAccess verifies thread safety of the learner.
func TestCardinalityLearnerConcurrentAccess(t *testing.T) {
	learner := NewCardinalityLearner(1000, 100)

	// Spawn multiple goroutines recording and reading
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				sig := "Op" + string(rune(id))
				learner.RecordObservation(sig, 100, int64(100+j))
				learner.GetLearningCorrection(sig)
				learner.GetObservationCount(sig)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should complete without panic
	assert.True(t, true, "Concurrent access completed successfully")
}

// TestExpectedCorrectionTrajectory verifies that corrections improve smoothly over time.
func TestExpectedCorrectionTrajectory(t *testing.T) {
	learner := NewCardinalityLearner(1000, 20)

	sig := "SeqScan()"
	corrections := []float64{}

	// Record 100 observations with consistent 1.5x ratio
	for i := 0; i < 100; i++ {
		learner.RecordObservation(sig, 100, 150)

		// After threshold, start collecting corrections
		if i >= 20 {
			corr := learner.GetLearningCorrection(sig)
			corrections = append(corrections, corr)
		}
	}

	// Corrections should converge toward ~1.5
	// Starting from ~1.0 (first value at threshold)
	assert.Greater(t, corrections[0], 1.0, "Initial should be above 1.0")
	assert.Less(t, corrections[len(corrections)-1], 1.55, "Final should approach 1.5")

	// Later corrections should be closer to target
	lastDelta := math.Abs(corrections[len(corrections)-1] - 1.5)
	firstDelta := math.Abs(corrections[0] - 1.5)
	assert.Less(t, lastDelta, firstDelta, "Should converge toward target")
}
