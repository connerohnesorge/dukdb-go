package adaptive

import "math"

// Reoptimizer decides when to trigger re-optimization based on runtime anomalies.
type Reoptimizer struct {
	RatioThreshold float64
	MinimumRows    int64
}

// NewReoptimizer creates a new reoptimization trigger.
func NewReoptimizer() *Reoptimizer {
	return &Reoptimizer{
		RatioThreshold: 10.0,
		MinimumRows:    100,
	}
}

// ShouldReoptimize returns true if the deviation exceeds thresholds.
func (r *Reoptimizer) ShouldReoptimize(estimatedRows, actualRows int64) bool {
	if r == nil {
		return false
	}
	if estimatedRows <= 0 || actualRows <= 0 {
		return false
	}
	if actualRows < r.MinimumRows && estimatedRows < r.MinimumRows {
		return false
	}
	ratio := math.Max(float64(actualRows)/float64(estimatedRows), float64(estimatedRows)/float64(actualRows))
	return ratio >= r.RatioThreshold
}
