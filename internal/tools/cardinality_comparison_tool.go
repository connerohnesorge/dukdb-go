// Package tools provides utility tools for testing and development.
//
// Cardinality Estimate Comparison Tool
//
// Task 9.15: Extracts cardinality estimates from EXPLAIN ANALYZE output
// and compares them with DuckDB estimates to measure estimation accuracy.
package tools

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// CardinalityEstimate represents a cardinality estimate for a query
type CardinalityEstimate struct {
	QueryID              string
	QueryName            string
	EstimatedCardinality int64
	ActualCardinality    int64
	EstimationError      float64 // (Estimated - Actual) / Actual
	IsWithinTolerance    bool    // true if within 2x of actual
}

// CardinalityComparisonReport contains analysis of cardinality estimates
type CardinalityComparisonReport struct {
	TotalQueries             int
	QueriesWithinTolerance   int
	MaxError                 float64
	MinError                 float64
	MeanAbsoluteError        float64
	MedianAbsoluteError      float64
	WorstPerformingQueries   []CardinalityEstimate
	BestPerformingQueries    []CardinalityEstimate
	Summary                  string
}

// CardinalityComparator analyzes cardinality estimation accuracy
type CardinalityComparator struct {
	toleranceMultiplier float64 // e.g., 2.0 for 2x tolerance
	estimates          []CardinalityEstimate
}

// NewCardinalityComparator creates a new cardinality comparison tool
// toleranceMultiplier specifies acceptable range (e.g., 2.0 = within 2x)
func NewCardinalityComparator(toleranceMultiplier float64) *CardinalityComparator {
	return &CardinalityComparator{
		toleranceMultiplier: toleranceMultiplier,
		estimates:          []CardinalityEstimate{},
	}
}

// AddEstimate adds a cardinality estimate to the analysis
func (cc *CardinalityComparator) AddEstimate(estimate CardinalityEstimate) {
	// Calculate estimation error
	if estimate.ActualCardinality > 0 {
		estimate.EstimationError = float64(estimate.EstimatedCardinality-estimate.ActualCardinality) / float64(estimate.ActualCardinality)
	}

	// Check if within tolerance
	if estimate.ActualCardinality > 0 {
		ratio := float64(estimate.EstimatedCardinality) / float64(estimate.ActualCardinality)
		if ratio < 0 {
			ratio = -ratio
		}
		estimate.IsWithinTolerance = ratio <= cc.toleranceMultiplier && ratio >= (1.0/cc.toleranceMultiplier)
	}

	cc.estimates = append(cc.estimates, estimate)
}

// GenerateReport generates a comprehensive comparison report
func (cc *CardinalityComparator) GenerateReport() *CardinalityComparisonReport {
	report := &CardinalityComparisonReport{
		TotalQueries: len(cc.estimates),
	}

	if report.TotalQueries == 0 {
		return report
	}

	// Calculate statistics
	var errors []float64
	var absoluteErrors []float64

	for _, est := range cc.estimates {
		if est.IsWithinTolerance {
			report.QueriesWithinTolerance++
		}

		error := est.EstimationError
		errors = append(errors, error)

		absError := error
		if absError < 0 {
			absError = -absError
		}
		absoluteErrors = append(absoluteErrors, absError)
	}

	// Calculate metrics
	if len(errors) > 0 {
		report.MaxError = cc.calculateMax(absoluteErrors)
		report.MinError = cc.calculateMin(absoluteErrors)
		report.MeanAbsoluteError = cc.calculateMean(absoluteErrors)
		report.MedianAbsoluteError = cc.calculateMedian(absoluteErrors)
	}

	// Find worst and best performing queries
	report.WorstPerformingQueries = cc.getWorstQueries(5)
	report.BestPerformingQueries = cc.getBestQueries(5)

	// Generate summary
	report.Summary = cc.generateSummary(report)

	return report
}

// calculateMax returns the maximum value in a slice
func (cc *CardinalityComparator) calculateMax(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	return max
}

// calculateMin returns the minimum value in a slice
func (cc *CardinalityComparator) calculateMin(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
	}
	return min
}

// calculateMean returns the mean of values in a slice
func (cc *CardinalityComparator) calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// calculateMedian returns the median of values in a slice
func (cc *CardinalityComparator) calculateMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	// Make a copy and sort
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2.0
	}
	return sorted[mid]
}

// getWorstQueries returns the N queries with largest estimation errors
func (cc *CardinalityComparator) getWorstQueries(n int) []CardinalityEstimate {
	// Sort by absolute estimation error (descending)
	sorted := make([]CardinalityEstimate, len(cc.estimates))
	copy(sorted, cc.estimates)

	sort.Slice(sorted, func(i, j int) bool {
		errI := sorted[i].EstimationError
		if errI < 0 {
			errI = -errI
		}
		errJ := sorted[j].EstimationError
		if errJ < 0 {
			errJ = -errJ
		}
		return errI > errJ
	})

	if n > len(sorted) {
		n = len(sorted)
	}

	return sorted[:n]
}

// getBestQueries returns the N queries with smallest estimation errors
func (cc *CardinalityComparator) getBestQueries(n int) []CardinalityEstimate {
	// Sort by absolute estimation error (ascending)
	sorted := make([]CardinalityEstimate, len(cc.estimates))
	copy(sorted, cc.estimates)

	sort.Slice(sorted, func(i, j int) bool {
		errI := sorted[i].EstimationError
		if errI < 0 {
			errI = -errI
		}
		errJ := sorted[j].EstimationError
		if errJ < 0 {
			errJ = -errJ
		}
		return errI < errJ
	})

	if n > len(sorted) {
		n = len(sorted)
	}

	return sorted[:n]
}

// generateSummary creates a human-readable summary
func (cc *CardinalityComparator) generateSummary(report *CardinalityComparisonReport) string {
	var sb strings.Builder

	sb.WriteString("Cardinality Estimation Analysis Report\n")
	sb.WriteString("=======================================\n\n")

	sb.WriteString(fmt.Sprintf("Total Queries Analyzed: %d\n", report.TotalQueries))
	sb.WriteString(fmt.Sprintf("Queries Within %.1fx Tolerance: %d (%.1f%%)\n",
		cc.toleranceMultiplier,
		report.QueriesWithinTolerance,
		float64(report.QueriesWithinTolerance)/float64(report.TotalQueries)*100))

	sb.WriteString(fmt.Sprintf("\nEstimation Error Statistics:\n"))
	sb.WriteString(fmt.Sprintf("  Max Error: %.2f%%\n", report.MaxError*100))
	sb.WriteString(fmt.Sprintf("  Min Error: %.2f%%\n", report.MinError*100))
	sb.WriteString(fmt.Sprintf("  Mean Absolute Error: %.2f%%\n", report.MeanAbsoluteError*100))
	sb.WriteString(fmt.Sprintf("  Median Absolute Error: %.2f%%\n", report.MedianAbsoluteError*100))

	if len(report.WorstPerformingQueries) > 0 {
		sb.WriteString("\nWorst Performing Queries:\n")
		for i, est := range report.WorstPerformingQueries {
			sb.WriteString(fmt.Sprintf("  %d. %s: Est=%d, Actual=%d (Error: %.2f%%)\n",
				i+1, est.QueryName, est.EstimatedCardinality, est.ActualCardinality,
				est.EstimationError*100))
		}
	}

	return sb.String()
}

// GetAccuracyPercentage returns the percentage of estimates within tolerance
func (cc *CardinalityComparator) GetAccuracyPercentage() float64 {
	if len(cc.estimates) == 0 {
		return 0
	}
	return float64(cc.GetWithinToleranceCount()) / float64(len(cc.estimates)) * 100
}

// GetWithinToleranceCount returns the number of estimates within tolerance
func (cc *CardinalityComparator) GetWithinToleranceCount() int {
	count := 0
	for _, est := range cc.estimates {
		if est.IsWithinTolerance {
			count++
		}
	}
	return count
}

// GetQuadraticMeanError returns the root mean squared error
func (cc *CardinalityComparator) GetQuadraticMeanError() float64 {
	if len(cc.estimates) == 0 {
		return 0
	}

	sumOfSquares := 0.0
	for _, est := range cc.estimates {
		err := est.EstimationError
		sumOfSquares += err * err
	}

	return math.Sqrt(sumOfSquares / float64(len(cc.estimates)))
}
