// Package optimizer provides cost-based query optimization for dukdb-go.
// This file implements multi-column statistics tracking, which DuckDB v1.4.3 does not have.
// Multi-column statistics track the joint number of distinct values (NDV) for selected
// column pairs, improving cardinality estimation for correlated predicates.
//
// Reference: DuckDB v1.4.3 only tracks per-column statistics in:
// references/duckdb/src/storage/statistics/distinct_statistics.cpp
//
// dukdb-go Extension: This implements a conservative multi-column statistics system:
// - Joint NDV collection for selected column pairs (limited to avoid memory bloat)
// - Correlation detection during ANALYZE (Pearson correlation coefficient)
// - Heuristics for selecting which pairs to track:
//  1. Join columns (used together in predicates or joins)
//  2. Highly correlated columns (r > 0.7)
//
// - Integration with cardinality estimation (use joint NDV instead of independence assumption)
package optimizer

import (
	"math"
	"sort"
)

// MultiColumnStats captures statistics for a pair of columns.
// This structure enables more accurate selectivity estimation for correlated predicates
// by tracking the actual number of distinct value combinations instead of assuming
// independence (which would multiply individual NDVs).
type MultiColumnStats struct {
	// Columns are the column names in alphabetical order (for consistency)
	Columns [2]string

	// JointNDV is the estimated number of distinct value combinations for this column pair.
	// Calculated using Good-Turing estimation (same as DuckDB uses for individual columns).
	// If JointNDV is much less than col1.NDV * col2.NDV, the columns are correlated.
	JointNDV int64

	// Correlation is the Pearson correlation coefficient between the columns.
	// Range: [-1.0, 1.0]
	// Interpretation:
	//   > 0.7:   Strong positive correlation (city and state)
	//  -0.7:   Strong negative correlation (price and discount)
	//   ~0:    No linear correlation (independent columns)
	// Only computed for numeric columns; 0 for mixed types.
	Correlation float64

	// SampleSize is the number of rows sampled to collect these statistics.
	// Used to understand confidence in the estimates.
	SampleSize int64

	// RecommendedForUse indicates if this stat should be used in selectivity estimation.
	// Set to true if JointNDV deviates significantly from the independence assumption
	// (e.g., JointNDV < 0.5 * col1.NDV * col2.NDV).
	RecommendedForUse bool
}

// MultiColumnStatsManager manages multi-column statistics for a table.
// This manager is responsible for collecting, storing, and querying multi-column statistics.
type MultiColumnStatsManager struct {
	// TableName is the name of the table these statistics describe
	TableName string

	// Stats maps from column pair keys (e.g., "col1|col2") to their statistics.
	// Pairs are stored in alphabetical order for consistent keys.
	Stats map[string]*MultiColumnStats

	// MaxPairs limits the number of column pairs tracked to avoid memory bloat.
	// Conservative: max 10 pairs per table (most tables don't benefit from more).
	MaxPairs int
}

// NewMultiColumnStatsManager creates a new manager for multi-column statistics.
func NewMultiColumnStatsManager(tableName string) *MultiColumnStatsManager {
	return &MultiColumnStatsManager{
		TableName: tableName,
		Stats:     make(map[string]*MultiColumnStats),
		MaxPairs:  10, // Conservative limit
	}
}

// AddStats adds or updates statistics for a column pair.
// Returns true if the stats were added, false if the manager was at capacity
// and this pair wasn't deemed important enough to replace an existing one.
func (m *MultiColumnStatsManager) AddStats(stats *MultiColumnStats) bool {
	if len(m.Stats) < m.MaxPairs {
		key := m.makeKey(stats.Columns[0], stats.Columns[1])
		m.Stats[key] = stats
		return true
	}

	// At capacity: only add if this stat is more important than an existing one
	// Importance is judged by: stronger correlation + higher sample size
	key := m.makeKey(stats.Columns[0], stats.Columns[1])
	if _, exists := m.Stats[key]; exists {
		// Update existing pair
		m.Stats[key] = stats
		return true
	}

	// Find least important existing stat and potentially replace it
	if m.shouldReplace(stats) {
		worstKey := m.findLeastImportant()
		delete(m.Stats, worstKey)
		m.Stats[key] = stats
		return true
	}

	return false
}

// makeKey creates a consistent key for a column pair.
// Columns are ordered alphabetically for consistency.
func (m *MultiColumnStatsManager) makeKey(col1, col2 string) string {
	if col1 <= col2 {
		return col1 + "|" + col2
	}
	return col2 + "|" + col1
}

// findLeastImportant finds the key of the least important (least correlated, smallest sample)
// column pair currently stored.
func (m *MultiColumnStatsManager) findLeastImportant() string {
	var worstKey string
	worstScore := math.MaxFloat64

	for key, stats := range m.Stats {
		// Importance score: abs(correlation) * sqrt(sample_size)
		// Highly correlated pairs with large samples are more important
		score := math.Abs(stats.Correlation) * math.Sqrt(float64(stats.SampleSize))
		if score < worstScore {
			worstScore = score
			worstKey = key
		}
	}

	return worstKey
}

// shouldReplace determines if a new stat is important enough to replace
// an existing one when at capacity.
func (m *MultiColumnStatsManager) shouldReplace(newStats *MultiColumnStats) bool {
	// Only replace if the new stat is notably important
	// Threshold: correlation > 0.75 indicates strong correlation
	return newStats.RecommendedForUse && math.Abs(newStats.Correlation) > 0.75
}

// GetStats retrieves statistics for a column pair.
// Returns nil if no statistics exist for this pair.
func (m *MultiColumnStatsManager) GetStats(col1, col2 string) *MultiColumnStats {
	key := m.makeKey(col1, col2)
	return m.Stats[key]
}

// CorrelationDetector computes correlation coefficients between columns.
// Used during ANALYZE to identify which column pairs should have multi-column statistics.
type CorrelationDetector struct {
	columnSamples map[string][]float64
	sampleCount   int64
}

// NewCorrelationDetector creates a new correlation detector.
func NewCorrelationDetector() *CorrelationDetector {
	return &CorrelationDetector{
		columnSamples: make(map[string][]float64),
	}
}

// AddSample adds a sample point for correlation detection.
// Only numeric columns can participate in correlation analysis.
func (cd *CorrelationDetector) AddSample(columnName string, numericValue float64) {
	if cd.columnSamples[columnName] == nil {
		cd.columnSamples[columnName] = make([]float64, 0)
	}
	cd.columnSamples[columnName] = append(cd.columnSamples[columnName], numericValue)
	cd.sampleCount++
}

// ComputeCorrelation computes the Pearson correlation coefficient between two columns.
// Returns the correlation in range [-1, 1], or 0 if either column is missing data.
//
// Formula (Pearson correlation):
//
//	r = Σ((x_i - mean_x) * (y_i - mean_y)) / (sqrt(Σ(x_i - mean_x)²) * sqrt(Σ(y_i - mean_y)²))
//
// This measures linear correlation. Strong values (|r| > 0.7) indicate dependencies
// that multi-column statistics should capture.
//
// Algorithm Steps:
// 1. Extract sample values for both columns
// 2. Compute means: mean_x = Σx_i / n, mean_y = Σy_i / n
// 3. Compute covariance numerator: Σ((x_i - mean_x) * (y_i - mean_y))
// 4. Compute standard deviations for denominator
// 5. Divide to get correlation coefficient
//
// Interpretation:
//
//	r > 0.7: Strong positive correlation (e.g., city_id and state_id in customer data)
//	r < -0.7: Strong negative correlation (e.g., price and discount)
//	-0.3 < r < 0.3: Weak/no linear correlation (independent columns)
//	r = 0: No linear correlation (may have non-linear relationship)
//
// Use in Multi-Column Stats:
// High correlation (|r| > 0.7) indicates JointNDV should differ significantly from
// independence assumption. These column pairs are selected for multi-column tracking
// to improve cardinality estimation accuracy.
//
// Edge Cases:
// - If either column is missing: return 0
// - If only <2 samples: return 0 (not enough for correlation)
// - If column values are all identical: return 0 (zero variance)
// - If one column has zero variance, other has non-zero: return 0
func (cd *CorrelationDetector) ComputeCorrelation(col1, col2 string) float64 {
	samples1, ok1 := cd.columnSamples[col1]
	samples2, ok2 := cd.columnSamples[col2]

	if !ok1 || !ok2 || len(samples1) != len(samples2) || len(samples1) == 0 {
		return 0
	}

	n := float64(len(samples1))
	if n < 2 {
		return 0 // Need at least 2 points for correlation
	}

	// Compute means
	mean1 := computeMean(samples1)
	mean2 := computeMean(samples2)

	// Compute standard deviations and covariance
	var sumProd, sumDev1Sq, sumDev2Sq float64
	for i := range samples1 {
		dev1 := samples1[i] - mean1
		dev2 := samples2[i] - mean2
		sumProd += dev1 * dev2
		sumDev1Sq += dev1 * dev1
		sumDev2Sq += dev2 * dev2
	}

	// Avoid division by zero
	if sumDev1Sq == 0 || sumDev2Sq == 0 {
		return 0
	}

	correlation := sumProd / math.Sqrt(sumDev1Sq*sumDev2Sq)

	// Clamp to [-1, 1] due to floating point precision
	if correlation > 1 {
		correlation = 1
	} else if correlation < -1 {
		correlation = -1
	}

	return correlation
}

// computeMean computes the arithmetic mean of a slice of floats.
func computeMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// JointNDVEstimator computes joint number of distinct values for column pairs.
// Uses Good-Turing estimation (same as DuckDB) based on sampled data.
type JointNDVEstimator struct {
	// pairs maps from "col1|col2" to a set of observed combinations
	// We use map[string]bool as a set (values are always true)
	pairs      map[string]map[string]bool
	sampleSize int64
}

// NewJointNDVEstimator creates a new joint NDV estimator.
func NewJointNDVEstimator() *JointNDVEstimator {
	return &JointNDVEstimator{
		pairs: make(map[string]map[string]bool),
	}
}

// AddSample records a combination of values for a column pair.
// col1Value and col2Value should be string representations of the values.
func (je *JointNDVEstimator) AddSample(col1, col2, col1Value, col2Value string) {
	// Normalize column names (alphabetical order)
	var colPair string
	var val1, val2 string
	if col1 <= col2 {
		colPair = col1 + "|" + col2
		val1, val2 = col1Value, col2Value
	} else {
		colPair = col2 + "|" + col1
		val1, val2 = col2Value, col1Value
	}

	if je.pairs[colPair] == nil {
		je.pairs[colPair] = make(map[string]bool)
	}

	// Create a unique key for this combination
	combination := val1 + "||" + val2
	je.pairs[colPair][combination] = true

	je.sampleSize++
}

// EstimateJointNDV estimates the joint number of distinct values using Good-Turing estimation.
// This matches DuckDB's approach for single-column distinct counts.
//
// Algorithm (Good-Turing estimation):
//  1. In sample: u unique combinations observed
//  2. Assume proportion of unseen combinations: (u/s)² where s = sample size
//  3. Estimate unseen: u1 = (u/s)² * u
//  4. Extrapolate to full table: estimate = u + (u1/s) * (n - s)
//     where n = total rows (if known, else use sample size)
//
// This is conservative and matches DuckDB's proven estimation approach.
func (je *JointNDVEstimator) EstimateJointNDV(col1, col2 string, totalRows int64) int64 {
	colPair := col1 + "|" + col2
	if col1 > col2 {
		colPair = col2 + "|" + col1
	}

	combinationSet, ok := je.pairs[colPair]
	if !ok || len(combinationSet) == 0 {
		return 1 // Conservative: at least 1 distinct combination
	}

	u := float64(len(combinationSet)) // Unique combinations in sample
	s := float64(je.sampleSize)
	n := float64(totalRows)

	if n == 0 {
		n = s // Use sample size if total is unknown
	}

	if s == 0 {
		return 1
	}

	// Good-Turing estimation
	// Proportion of values appearing only once (assumes seen once = pattern)
	u1 := (u / s) * (u / s) * u

	// Extrapolate to full table
	estimate := u + (u1/s)*(n-s)

	// Cap at total rows (can't have more distinct combinations than rows)
	if estimate > n {
		estimate = n
	}

	return int64(math.Round(estimate))
}

// ColumnPairSelector heuristics for selecting which column pairs should be tracked.
// Conservative approach: only track pairs that are clearly correlated or frequently used together.
type ColumnPairSelector struct {
	correlationThreshold float64
}

// NewColumnPairSelector creates a new selector with default thresholds.
func NewColumnPairSelector() *ColumnPairSelector {
	return &ColumnPairSelector{
		// Conservative threshold: only track strong correlations
		// DuckDB doesn't track multi-column stats, but if it did, 0.7 is a proven threshold
		// for identifying practically significant correlations
		correlationThreshold: 0.7,
	}
}

// ShouldTrack determines if a column pair should have multi-column statistics.
// Uses heuristics: strong correlation or functional dependency indicators.
func (s *ColumnPairSelector) ShouldTrack(
	col1Stats, col2Stats *ColumnStatistics,
	correlation float64,
	jointNDV, totalRows int64,
) bool {
	// Heuristic 1: Strong linear correlation (|r| > threshold)
	if math.Abs(correlation) > s.correlationThreshold {
		return true
	}

	// Heuristic 2: Functional dependency indicator
	// If joint NDV << individual NDVs, columns are highly dependent
	// Example: city + state usually have city determining state
	expectedIndependent := col1Stats.DistinctCount * col2Stats.DistinctCount
	if expectedIndependent > 0 && jointNDV < int64(float64(expectedIndependent)*0.3) {
		// Joint NDV is less than 30% of what independence would suggest
		return true
	}

	// Heuristic 3: Low distinct count suggests categorical pairing
	// (many rows per combination, not just random correlation)
	minCardinality := col1Stats.DistinctCount
	if col2Stats.DistinctCount < minCardinality {
		minCardinality = col2Stats.DistinctCount
	}
	if minCardinality > 0 && minCardinality < 100 {
		expectedIndependent := minCardinality * minCardinality
		if jointNDV < int64(float64(expectedIndependent)*0.5) {
			return true
		}
	}

	return false
}

// CardinalityWithMultiColumnStats estimates cardinality of a predicate using multi-column stats.
// This replaces the independence assumption with actual data when multi-column statistics are available.
//
// Example:
//
//	Query: WHERE city = 'NY' AND state = 'NY'
//	Without multi-column stats (independence assumption):
//	  rows = total * P(city='NY') * P(state='NY') = total * (1/50) * (1/51) ≈ total/2550
//	With multi-column stats:
//	  rows = total * (distinct_pairs_with_ny_ny / joint_ndv) ≈ more accurate
//
// Formula:
//
//	If multi-column stats available:
//	  selectivity = 1.0 / jointNDV  (assuming uniform distribution)
//	Else (fallback):
//	  selectivity = P(col1) * P(col2)  (independence assumption)
func CardinalityWithMultiColumnStats(
	totalRows int64,
	col1Selectivity, col2Selectivity float64,
	multiStats *MultiColumnStats,
) float64 {
	if multiStats == nil {
		// No multi-column stats: use independence assumption
		return col1Selectivity * col2Selectivity
	}

	// Use joint NDV for more accurate selectivity
	// Assume uniform distribution across joint values
	if multiStats.JointNDV <= 0 {
		return col1Selectivity * col2Selectivity // Fallback
	}

	// Joint selectivity: 1 / JointNDV (assuming uniform distribution)
	return 1.0 / float64(multiStats.JointNDV)
}

// SelectTopColumnPairs selects the most important column pairs from a set of candidates.
// Uses correlation strength and sample size as importance metrics.
// Returns up to maxPairs pairs, sorted by importance.
func SelectTopColumnPairs(
	candidates []*MultiColumnStats,
	maxPairs int,
) []*MultiColumnStats {
	if len(candidates) <= maxPairs {
		return candidates
	}

	// Sort by importance: abs(correlation) * sqrt(sample_size)
	sort.Slice(candidates, func(i, j int) bool {
		scoreI := math.Abs(candidates[i].Correlation) * math.Sqrt(float64(candidates[i].SampleSize))
		scoreJ := math.Abs(candidates[j].Correlation) * math.Sqrt(float64(candidates[j].SampleSize))
		return scoreI > scoreJ // Descending order (most important first)
	})

	return candidates[:maxPairs]
}
