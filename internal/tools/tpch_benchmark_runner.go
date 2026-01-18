// Package tools provides utility tools for testing and development.
//
// # TPC-H Benchmark Runner
//
// Task 9.16: Runs TPC-H benchmark queries and compares performance metrics
// between dukdb-go and baseline expectations.
package tools

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// TPCHQueryResult holds the result of running a single TPC-H query
type TPCHQueryResult struct {
	QueryNumber      int
	QueryName        string
	ExecutionTimeMs  int64
	RowsReturned     int64
	EstimatedTimeMs  int64
	BaselineTimeMs   int64
	PerformanceRatio float64 // Actual / Baseline
	Status           string  // "PASS", "WARNING", "FAIL"
}

// TPCHBenchmarkReport contains aggregated results from TPC-H benchmarking
type TPCHBenchmarkReport struct {
	ScaleFactor             float64
	TotalQueriesRun         int
	QueriesPassed           int
	QueriesWarning          int
	QueriesFailed           int
	TotalExecutionTimeMs    int64
	AverageExecutionTimeMs  int64
	MedianExecutionTimeMs   int64
	MaxExecutionTimeMs      int64
	MinExecutionTimeMs      int64
	AveragePerformanceRatio float64
	WorstPerformingQueries  []TPCHQueryResult
	BestPerformingQueries   []TPCHQueryResult
	Summary                 string
}

// TPCHBenchmarkRunner runs TPC-H benchmark queries
type TPCHBenchmarkRunner struct {
	results              []TPCHQueryResult
	performanceThreshold float64 // queries > this ratio are failures
	warningThreshold     float64 // queries > this ratio are warnings
}

// NewTPCHBenchmarkRunner creates a new TPC-H benchmark runner
func NewTPCHBenchmarkRunner() *TPCHBenchmarkRunner {
	return &TPCHBenchmarkRunner{
		results:              []TPCHQueryResult{},
		performanceThreshold: 2.0, // Fail if > 2x baseline
		warningThreshold:     1.2, // Warn if > 1.2x baseline
	}
}

// AddResult adds a query result to the benchmark
func (tr *TPCHBenchmarkRunner) AddResult(result TPCHQueryResult) {
	// Calculate performance ratio
	if result.BaselineTimeMs > 0 {
		result.PerformanceRatio = float64(result.ExecutionTimeMs) / float64(result.BaselineTimeMs)
	}

	// Determine status
	if result.PerformanceRatio > tr.performanceThreshold {
		result.Status = "FAIL"
	} else if result.PerformanceRatio > tr.warningThreshold {
		result.Status = "WARNING"
	} else {
		result.Status = "PASS"
	}

	tr.results = append(tr.results, result)
}

// GenerateReport generates a comprehensive benchmark report
func (tr *TPCHBenchmarkRunner) GenerateReport(scaleFactor float64) *TPCHBenchmarkReport {
	report := &TPCHBenchmarkReport{
		ScaleFactor:     scaleFactor,
		TotalQueriesRun: len(tr.results),
	}

	if report.TotalQueriesRun == 0 {
		return report
	}

	// Calculate pass/fail/warning counts
	var executionTimes []int64
	var performanceRatios []float64

	for _, result := range tr.results {
		executionTimes = append(executionTimes, result.ExecutionTimeMs)
		performanceRatios = append(performanceRatios, result.PerformanceRatio)

		switch result.Status {
		case "PASS":
			report.QueriesPassed++
		case "WARNING":
			report.QueriesWarning++
		case "FAIL":
			report.QueriesFailed++
		}

		report.TotalExecutionTimeMs += result.ExecutionTimeMs
	}

	// Calculate metrics
	if len(executionTimes) > 0 {
		report.AverageExecutionTimeMs = report.TotalExecutionTimeMs / int64(len(executionTimes))
		report.MedianExecutionTimeMs = tr.calculateMedianInt64(executionTimes)
		report.MaxExecutionTimeMs = tr.calculateMaxInt64(executionTimes)
		report.MinExecutionTimeMs = tr.calculateMinInt64(executionTimes)
	}

	if len(performanceRatios) > 0 {
		report.AveragePerformanceRatio = tr.calculateMeanFloat64(performanceRatios)
	}

	// Find best and worst queries
	report.WorstPerformingQueries = tr.getWorstQueries(5)
	report.BestPerformingQueries = tr.getBestQueries(5)

	// Generate summary
	report.Summary = tr.generateSummary(report)

	return report
}

// calculateMedianInt64 returns the median of a slice of int64
func (tr *TPCHBenchmarkRunner) calculateMedianInt64(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}

	sorted := make([]int64, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

// calculateMaxInt64 returns the maximum value in a slice
func (tr *TPCHBenchmarkRunner) calculateMaxInt64(values []int64) int64 {
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

// calculateMinInt64 returns the minimum value in a slice
func (tr *TPCHBenchmarkRunner) calculateMinInt64(values []int64) int64 {
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

// calculateMeanFloat64 returns the mean of a slice of float64
func (tr *TPCHBenchmarkRunner) calculateMeanFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// getWorstQueries returns the N queries with worst performance ratio
func (tr *TPCHBenchmarkRunner) getWorstQueries(n int) []TPCHQueryResult {
	sorted := make([]TPCHQueryResult, len(tr.results))
	copy(sorted, tr.results)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].PerformanceRatio > sorted[j].PerformanceRatio
	})

	if n > len(sorted) {
		n = len(sorted)
	}

	return sorted[:n]
}

// getBestQueries returns the N queries with best performance ratio
func (tr *TPCHBenchmarkRunner) getBestQueries(n int) []TPCHQueryResult {
	sorted := make([]TPCHQueryResult, len(tr.results))
	copy(sorted, tr.results)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].PerformanceRatio < sorted[j].PerformanceRatio
	})

	if n > len(sorted) {
		n = len(sorted)
	}

	return sorted[:n]
}

// generateSummary creates a human-readable summary
func (tr *TPCHBenchmarkRunner) generateSummary(report *TPCHBenchmarkReport) string {
	var sb strings.Builder

	sb.WriteString("TPC-H Benchmark Report\n")
	sb.WriteString("======================\n\n")

	sb.WriteString(fmt.Sprintf("Scale Factor: %.1f\n", report.ScaleFactor))
	sb.WriteString(fmt.Sprintf("Total Queries: %d\n\n", report.TotalQueriesRun))

	sb.WriteString("Results:\n")
	sb.WriteString(fmt.Sprintf("  Passed: %d (%.1f%%)\n",
		report.QueriesPassed,
		float64(report.QueriesPassed)/float64(report.TotalQueriesRun)*100))
	sb.WriteString(fmt.Sprintf("  Warning: %d (%.1f%%)\n",
		report.QueriesWarning,
		float64(report.QueriesWarning)/float64(report.TotalQueriesRun)*100))
	sb.WriteString(fmt.Sprintf("  Failed: %d (%.1f%%)\n\n",
		report.QueriesFailed,
		float64(report.QueriesFailed)/float64(report.TotalQueriesRun)*100))

	sb.WriteString("Execution Time Statistics:\n")
	sb.WriteString(fmt.Sprintf("  Total: %dms\n", report.TotalExecutionTimeMs))
	sb.WriteString(fmt.Sprintf("  Average: %dms\n", report.AverageExecutionTimeMs))
	sb.WriteString(fmt.Sprintf("  Median: %dms\n", report.MedianExecutionTimeMs))
	sb.WriteString(fmt.Sprintf("  Min: %dms\n", report.MinExecutionTimeMs))
	sb.WriteString(fmt.Sprintf("  Max: %dms\n\n", report.MaxExecutionTimeMs))

	sb.WriteString(
		fmt.Sprintf("Performance Ratio (vs Baseline): %.2fx\n", report.AveragePerformanceRatio),
	)

	if len(report.WorstPerformingQueries) > 0 {
		sb.WriteString("\nWorst Performing Queries:\n")
		for i, result := range report.WorstPerformingQueries {
			sb.WriteString(fmt.Sprintf(
				"  %d. Q%d (%s): %.2fx baseline (%dms)\n",
				i+1,
				result.QueryNumber,
				result.Status,
				result.PerformanceRatio,
				result.ExecutionTimeMs,
			))
		}
	}

	return sb.String()
}

// GetPassRate returns the percentage of queries that passed
func (tr *TPCHBenchmarkRunner) GetPassRate() float64 {
	if len(tr.results) == 0 {
		return 0
	}
	passCount := 0
	for _, result := range tr.results {
		if result.Status == "PASS" {
			passCount++
		}
	}
	return float64(passCount) / float64(len(tr.results)) * 100
}

// GetSuccessRate returns the percentage of queries that passed or warned
func (tr *TPCHBenchmarkRunner) GetSuccessRate() float64 {
	if len(tr.results) == 0 {
		return 0
	}
	successCount := 0
	for _, result := range tr.results {
		if result.Status == "PASS" || result.Status == "WARNING" {
			successCount++
		}
	}
	return float64(successCount) / float64(len(tr.results)) * 100
}

// GetStandardDeviation returns the standard deviation of performance ratios
func (tr *TPCHBenchmarkRunner) GetStandardDeviation() float64 {
	if len(tr.results) < 2 {
		return 0
	}

	// Calculate mean
	mean := 0.0
	for _, result := range tr.results {
		mean += result.PerformanceRatio
	}
	mean /= float64(len(tr.results))

	// Calculate variance
	variance := 0.0
	for _, result := range tr.results {
		diff := result.PerformanceRatio - mean
		variance += diff * diff
	}
	variance /= float64(len(tr.results) - 1)

	return math.Sqrt(variance)
}

// TimeSince formats a duration in a human-readable format
func TimeSince(start time.Time) string {
	duration := time.Since(start)
	switch {
	case duration < time.Second:
		return fmt.Sprintf("%dms", duration.Milliseconds())
	case duration < time.Minute:
		return fmt.Sprintf("%.1fs", duration.Seconds())
	default:
		return fmt.Sprintf("%.1fm", duration.Minutes())
	}
}
