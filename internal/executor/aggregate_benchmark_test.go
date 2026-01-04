// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"fmt"
	"math"
	"runtime"
	"testing"
)

// ============================================================================
// Benchmark: MEDIAN on various dataset sizes
// Task 9.1.1: Add benchmark for MEDIAN on large datasets
// ============================================================================

// BenchmarkMedian_100 benchmarks MEDIAN computation on 100 values.
func BenchmarkMedian_100(b *testing.B) {
	benchmarkMedian(b, 100)
}

// BenchmarkMedian_1000 benchmarks MEDIAN computation on 1,000 values.
func BenchmarkMedian_1000(b *testing.B) {
	benchmarkMedian(b, 1000)
}

// BenchmarkMedian_10000 benchmarks MEDIAN computation on 10,000 values.
func BenchmarkMedian_10000(b *testing.B) {
	benchmarkMedian(b, 10000)
}

// BenchmarkMedian_100000 benchmarks MEDIAN computation on 100,000 values.
func BenchmarkMedian_100000(b *testing.B) {
	benchmarkMedian(b, 100000)
}

// benchmarkMedian is the internal benchmark helper for MEDIAN computation.
func benchmarkMedian(b *testing.B, size int) {
	b.Helper()

	// Pre-generate test data outside the benchmark loop
	values := generateBenchmarkValues(size)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := computeMedian(values)
		if err != nil {
			b.Fatalf("computeMedian failed: %v", err)
		}
		if result == nil {
			b.Fatal("computeMedian returned nil for non-empty input")
		}
	}

	// Report the number of values processed per operation
	b.ReportMetric(float64(size), "values/op")
}

// ============================================================================
// Benchmark: APPROX_COUNT_DISTINCT vs exact COUNT(DISTINCT)
// Task 9.1.2: Add benchmark for APPROX_COUNT_DISTINCT vs COUNT(DISTINCT)
// ============================================================================

// BenchmarkApproxCountDistinct_100 benchmarks APPROX_COUNT_DISTINCT on 100 values.
func BenchmarkApproxCountDistinct_100(b *testing.B) {
	benchmarkApproxCountDistinct(b, 100)
}

// BenchmarkApproxCountDistinct_1000 benchmarks APPROX_COUNT_DISTINCT on 1,000 values.
func BenchmarkApproxCountDistinct_1000(b *testing.B) {
	benchmarkApproxCountDistinct(b, 1000)
}

// BenchmarkApproxCountDistinct_10000 benchmarks APPROX_COUNT_DISTINCT on 10,000 values.
func BenchmarkApproxCountDistinct_10000(b *testing.B) {
	benchmarkApproxCountDistinct(b, 10000)
}

// BenchmarkApproxCountDistinct_100000 benchmarks APPROX_COUNT_DISTINCT on 100,000 values.
func BenchmarkApproxCountDistinct_100000(b *testing.B) {
	benchmarkApproxCountDistinct(b, 100000)
}

// benchmarkApproxCountDistinct is the internal benchmark helper for APPROX_COUNT_DISTINCT.
func benchmarkApproxCountDistinct(b *testing.B, size int) {
	b.Helper()

	// Generate test data with distinct integer values
	values := make([]any, size)
	for i := 0; i < size; i++ {
		values[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := computeApproxCountDistinct(values)
		if err != nil {
			b.Fatalf("computeApproxCountDistinct failed: %v", err)
		}
		if result == nil {
			b.Fatal("computeApproxCountDistinct returned nil for non-empty input")
		}
	}

	b.ReportMetric(float64(size), "values/op")
}

// BenchmarkExactCountDistinct_100 benchmarks exact COUNT(DISTINCT) on 100 values.
func BenchmarkExactCountDistinct_100(b *testing.B) {
	benchmarkExactCountDistinct(b, 100)
}

// BenchmarkExactCountDistinct_1000 benchmarks exact COUNT(DISTINCT) on 1,000 values.
func BenchmarkExactCountDistinct_1000(b *testing.B) {
	benchmarkExactCountDistinct(b, 1000)
}

// BenchmarkExactCountDistinct_10000 benchmarks exact COUNT(DISTINCT) on 10,000 values.
func BenchmarkExactCountDistinct_10000(b *testing.B) {
	benchmarkExactCountDistinct(b, 10000)
}

// BenchmarkExactCountDistinct_100000 benchmarks exact COUNT(DISTINCT) on 100,000 values.
func BenchmarkExactCountDistinct_100000(b *testing.B) {
	benchmarkExactCountDistinct(b, 100000)
}

// benchmarkExactCountDistinct is the internal benchmark helper for exact COUNT(DISTINCT).
// This simulates exact distinct counting using a map.
func benchmarkExactCountDistinct(b *testing.B, size int) {
	b.Helper()

	// Generate test data with distinct integer values
	values := make([]any, size)
	for i := 0; i < size; i++ {
		values[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Simulate exact COUNT(DISTINCT) using a map
		seen := make(map[any]struct{})
		for _, v := range values {
			if v != nil {
				seen[v] = struct{}{}
			}
		}
		count := int64(len(seen))
		if count == 0 {
			b.Fatal("exact count returned 0 for non-empty input")
		}
	}

	b.ReportMetric(float64(size), "values/op")
}

// BenchmarkApproxVsExactCountDistinct runs both methods for direct comparison.
func BenchmarkApproxVsExactCountDistinct(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Approx/%d", size), func(b *testing.B) {
			benchmarkApproxCountDistinct(b, size)
		})
		b.Run(fmt.Sprintf("Exact/%d", size), func(b *testing.B) {
			benchmarkExactCountDistinct(b, size)
		})
	}
}

// ============================================================================
// Benchmark: Variance/Stddev computations
// Task 9.1.3: Add benchmark for variance/stddev vs naive computation
// ============================================================================

// BenchmarkVarPop_100 benchmarks VAR_POP on 100 values.
func BenchmarkVarPop_100(b *testing.B) {
	benchmarkVarPop(b, 100)
}

// BenchmarkVarPop_1000 benchmarks VAR_POP on 1,000 values.
func BenchmarkVarPop_1000(b *testing.B) {
	benchmarkVarPop(b, 1000)
}

// BenchmarkVarPop_10000 benchmarks VAR_POP on 10,000 values.
func BenchmarkVarPop_10000(b *testing.B) {
	benchmarkVarPop(b, 10000)
}

// BenchmarkVarPop_100000 benchmarks VAR_POP on 100,000 values.
func BenchmarkVarPop_100000(b *testing.B) {
	benchmarkVarPop(b, 100000)
}

// benchmarkVarPop is the internal benchmark helper for VAR_POP computation.
func benchmarkVarPop(b *testing.B, size int) {
	b.Helper()

	values := generateBenchmarkValues(size)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := computeVarPop(values)
		if err != nil {
			b.Fatalf("computeVarPop failed: %v", err)
		}
		if result == nil {
			b.Fatal("computeVarPop returned nil for non-empty input")
		}
	}

	b.ReportMetric(float64(size), "values/op")
}

// BenchmarkStddevPop_100 benchmarks STDDEV_POP on 100 values.
func BenchmarkStddevPop_100(b *testing.B) {
	benchmarkStddevPop(b, 100)
}

// BenchmarkStddevPop_1000 benchmarks STDDEV_POP on 1,000 values.
func BenchmarkStddevPop_1000(b *testing.B) {
	benchmarkStddevPop(b, 1000)
}

// BenchmarkStddevPop_10000 benchmarks STDDEV_POP on 10,000 values.
func BenchmarkStddevPop_10000(b *testing.B) {
	benchmarkStddevPop(b, 10000)
}

// BenchmarkStddevPop_100000 benchmarks STDDEV_POP on 100,000 values.
func BenchmarkStddevPop_100000(b *testing.B) {
	benchmarkStddevPop(b, 100000)
}

// benchmarkStddevPop is the internal benchmark helper for STDDEV_POP computation.
func benchmarkStddevPop(b *testing.B, size int) {
	b.Helper()

	values := generateBenchmarkValues(size)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := computeStddevPop(values)
		if err != nil {
			b.Fatalf("computeStddevPop failed: %v", err)
		}
		if result == nil {
			b.Fatal("computeStddevPop returned nil for non-empty input")
		}
	}

	b.ReportMetric(float64(size), "values/op")
}

// BenchmarkVarianceStateStreaming benchmarks streaming variance using VarianceState.
func BenchmarkVarianceStateStreaming_100(b *testing.B) {
	benchmarkVarianceStateStreaming(b, 100)
}

// BenchmarkVarianceStateStreaming_1000 benchmarks streaming variance using VarianceState.
func BenchmarkVarianceStateStreaming_1000(b *testing.B) {
	benchmarkVarianceStateStreaming(b, 1000)
}

// BenchmarkVarianceStateStreaming_10000 benchmarks streaming variance using VarianceState.
func BenchmarkVarianceStateStreaming_10000(b *testing.B) {
	benchmarkVarianceStateStreaming(b, 10000)
}

// BenchmarkVarianceStateStreaming_100000 benchmarks streaming variance using VarianceState.
func BenchmarkVarianceStateStreaming_100000(b *testing.B) {
	benchmarkVarianceStateStreaming(b, 100000)
}

// benchmarkVarianceStateStreaming benchmarks the streaming VarianceState implementation.
func benchmarkVarianceStateStreaming(b *testing.B, size int) {
	b.Helper()

	// Pre-generate float64 values for streaming
	floats := make([]float64, size)
	for i := 0; i < size; i++ {
		floats[i] = float64(i) * 1.5
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		vs := NewVarianceState()
		for _, f := range floats {
			vs.Update(f)
		}
		variance := vs.VariancePop()
		if variance < 0 {
			b.Fatal("variance cannot be negative")
		}
	}

	b.ReportMetric(float64(size), "values/op")
}

// ============================================================================
// Benchmark: String Aggregation (memory patterns)
// Task 9.1.4: Profile memory usage for string aggregation
// ============================================================================

// BenchmarkStringAgg_100 benchmarks STRING_AGG on 100 string values.
func BenchmarkStringAgg_100(b *testing.B) {
	benchmarkStringAgg(b, 100)
}

// BenchmarkStringAgg_1000 benchmarks STRING_AGG on 1,000 string values.
func BenchmarkStringAgg_1000(b *testing.B) {
	benchmarkStringAgg(b, 1000)
}

// BenchmarkStringAgg_10000 benchmarks STRING_AGG on 10,000 string values.
func BenchmarkStringAgg_10000(b *testing.B) {
	benchmarkStringAgg(b, 10000)
}

// BenchmarkStringAgg_100000 benchmarks STRING_AGG on 100,000 string values.
func BenchmarkStringAgg_100000(b *testing.B) {
	benchmarkStringAgg(b, 100000)
}

// benchmarkStringAgg is the internal benchmark helper for STRING_AGG computation.
func benchmarkStringAgg(b *testing.B, size int) {
	b.Helper()

	// Generate string values of varying lengths
	values := make([]any, size)
	for i := 0; i < size; i++ {
		values[i] = fmt.Sprintf("string_value_%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := computeStringAgg(values, ", ")
		if err != nil {
			b.Fatalf("computeStringAgg failed: %v", err)
		}
		if result == nil {
			b.Fatal("computeStringAgg returned nil for non-empty input")
		}
	}

	b.ReportMetric(float64(size), "values/op")
}

// BenchmarkStringAgg_VaryingDelimiter benchmarks STRING_AGG with different delimiters.
func BenchmarkStringAgg_VaryingDelimiter(b *testing.B) {
	delimiters := []string{",", ", ", " | ", "  --  ", ""}
	size := 1000

	values := make([]any, size)
	for i := 0; i < size; i++ {
		values[i] = fmt.Sprintf("value_%d", i)
	}

	for _, delim := range delimiters {
		name := fmt.Sprintf("delim_len_%d", len(delim))
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := computeStringAgg(values, delim)
				if err != nil {
					b.Fatalf("computeStringAgg failed: %v", err)
				}
				if result == nil {
					b.Fatal("computeStringAgg returned nil for non-empty input")
				}
			}
		})
	}
}

// BenchmarkStringAgg_LargeStrings benchmarks STRING_AGG with larger individual strings.
func BenchmarkStringAgg_LargeStrings(b *testing.B) {
	stringSizes := []int{10, 100, 1000}
	numStrings := 100

	for _, strSize := range stringSizes {
		b.Run(fmt.Sprintf("str_len_%d", strSize), func(b *testing.B) {
			// Generate strings of specified length
			values := make([]any, numStrings)
			baseStr := make([]byte, strSize)
			for j := 0; j < strSize; j++ {
				baseStr[j] = 'a' + byte(j%26)
			}
			strVal := string(baseStr)

			for i := 0; i < numStrings; i++ {
				values[i] = fmt.Sprintf("%s_%d", strVal, i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := computeStringAgg(values, ", ")
				if err != nil {
					b.Fatalf("computeStringAgg failed: %v", err)
				}
				if result == nil {
					b.Fatal("computeStringAgg returned nil")
				}
			}

			// Report total bytes processed
			totalBytes := numStrings * (strSize + 10) // approximate
			b.ReportMetric(float64(totalBytes), "bytes/op")
		})
	}
}

// ============================================================================
// Memory profiling tests for string aggregation
// ============================================================================

// TestStringAggMemoryUsage measures memory allocation patterns for STRING_AGG.
// This is a test (not benchmark) that logs memory statistics.
func TestStringAggMemoryUsage(t *testing.T) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			// Generate string values
			values := make([]any, size)
			for i := 0; i < size; i++ {
				values[i] = fmt.Sprintf("string_value_%d", i)
			}

			// Force GC before measuring
			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			// Perform aggregation
			result, err := computeStringAgg(values, ", ")
			if err != nil {
				t.Fatalf("computeStringAgg failed: %v", err)
			}
			if result == nil {
				t.Fatal("result is nil")
			}

			// Force GC and measure after
			runtime.GC()
			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)

			// Calculate memory used
			memUsed := memAfter.TotalAlloc - memBefore.TotalAlloc
			memUsedKB := float64(memUsed) / 1024

			// Calculate result string size
			resultStr := result.(string)
			resultSizeKB := float64(len(resultStr)) / 1024

			t.Logf("Size: %d strings", size)
			t.Logf("  Result length: %.2f KB", resultSizeKB)
			t.Logf("  Memory allocated: %.2f KB", memUsedKB)
			t.Logf("  Overhead ratio: %.2fx", memUsedKB/resultSizeKB)
		})
	}
}

// ============================================================================
// Additional aggregate benchmarks for completeness
// ============================================================================

// BenchmarkMode_100 benchmarks MODE computation on 100 values.
func BenchmarkMode_100(b *testing.B) {
	benchmarkMode(b, 100)
}

// BenchmarkMode_1000 benchmarks MODE computation on 1,000 values.
func BenchmarkMode_1000(b *testing.B) {
	benchmarkMode(b, 1000)
}

// BenchmarkMode_10000 benchmarks MODE computation on 10,000 values.
func BenchmarkMode_10000(b *testing.B) {
	benchmarkMode(b, 10000)
}

// benchmarkMode is the internal benchmark helper for MODE computation.
func benchmarkMode(b *testing.B, size int) {
	b.Helper()

	// Generate values with some repetition to make mode meaningful
	values := make([]any, size)
	for i := 0; i < size; i++ {
		// Use modulo to create repeated values
		values[i] = float64(i % (size / 10))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := computeMode(values)
		if err != nil {
			b.Fatalf("computeMode failed: %v", err)
		}
		if result == nil {
			b.Fatal("computeMode returned nil for non-empty input")
		}
	}

	b.ReportMetric(float64(size), "values/op")
}

// BenchmarkEntropy_100 benchmarks ENTROPY computation on 100 values.
func BenchmarkEntropy_100(b *testing.B) {
	benchmarkEntropy(b, 100)
}

// BenchmarkEntropy_1000 benchmarks ENTROPY computation on 1,000 values.
func BenchmarkEntropy_1000(b *testing.B) {
	benchmarkEntropy(b, 1000)
}

// BenchmarkEntropy_10000 benchmarks ENTROPY computation on 10,000 values.
func BenchmarkEntropy_10000(b *testing.B) {
	benchmarkEntropy(b, 10000)
}

// benchmarkEntropy is the internal benchmark helper for ENTROPY computation.
func benchmarkEntropy(b *testing.B, size int) {
	b.Helper()

	// Generate values with some repetition
	values := make([]any, size)
	for i := 0; i < size; i++ {
		values[i] = float64(i % (size / 5))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := computeEntropy(values)
		if err != nil {
			b.Fatalf("computeEntropy failed: %v", err)
		}
		if result == nil {
			b.Fatal("computeEntropy returned nil for non-empty input")
		}
	}

	b.ReportMetric(float64(size), "values/op")
}

// BenchmarkQuantile_100 benchmarks QUANTILE computation on 100 values.
func BenchmarkQuantile_100(b *testing.B) {
	benchmarkQuantile(b, 100)
}

// BenchmarkQuantile_1000 benchmarks QUANTILE computation on 1,000 values.
func BenchmarkQuantile_1000(b *testing.B) {
	benchmarkQuantile(b, 1000)
}

// BenchmarkQuantile_10000 benchmarks QUANTILE computation on 10,000 values.
func BenchmarkQuantile_10000(b *testing.B) {
	benchmarkQuantile(b, 10000)
}

// BenchmarkQuantile_100000 benchmarks QUANTILE computation on 100,000 values.
func BenchmarkQuantile_100000(b *testing.B) {
	benchmarkQuantile(b, 100000)
}

// benchmarkQuantile is the internal benchmark helper for QUANTILE computation.
func benchmarkQuantile(b *testing.B, size int) {
	b.Helper()

	values := generateBenchmarkValues(size)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := computeQuantile(values, 0.75)
		if err != nil {
			b.Fatalf("computeQuantile failed: %v", err)
		}
		if result == nil {
			b.Fatal("computeQuantile returned nil for non-empty input")
		}
	}

	b.ReportMetric(float64(size), "values/op")
}

// BenchmarkApproxQuantile_100 benchmarks APPROX_QUANTILE on 100 values.
func BenchmarkApproxQuantile_100(b *testing.B) {
	benchmarkApproxQuantile(b, 100)
}

// BenchmarkApproxQuantile_1000 benchmarks APPROX_QUANTILE on 1,000 values.
func BenchmarkApproxQuantile_1000(b *testing.B) {
	benchmarkApproxQuantile(b, 1000)
}

// BenchmarkApproxQuantile_10000 benchmarks APPROX_QUANTILE on 10,000 values.
func BenchmarkApproxQuantile_10000(b *testing.B) {
	benchmarkApproxQuantile(b, 10000)
}

// BenchmarkApproxQuantile_100000 benchmarks APPROX_QUANTILE on 100,000 values.
func BenchmarkApproxQuantile_100000(b *testing.B) {
	benchmarkApproxQuantile(b, 100000)
}

// benchmarkApproxQuantile is the internal benchmark helper for APPROX_QUANTILE.
func benchmarkApproxQuantile(b *testing.B, size int) {
	b.Helper()

	values := generateBenchmarkValues(size)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := computeApproxQuantile(values, 0.75)
		if err != nil {
			b.Fatalf("computeApproxQuantile failed: %v", err)
		}
		if result == nil {
			b.Fatal("computeApproxQuantile returned nil for non-empty input")
		}
	}

	b.ReportMetric(float64(size), "values/op")
}

// BenchmarkList_100 benchmarks LIST aggregation on 100 values.
func BenchmarkList_100(b *testing.B) {
	benchmarkList(b, 100)
}

// BenchmarkList_1000 benchmarks LIST aggregation on 1,000 values.
func BenchmarkList_1000(b *testing.B) {
	benchmarkList(b, 1000)
}

// BenchmarkList_10000 benchmarks LIST aggregation on 10,000 values.
func BenchmarkList_10000(b *testing.B) {
	benchmarkList(b, 10000)
}

// benchmarkList is the internal benchmark helper for LIST aggregation.
func benchmarkList(b *testing.B, size int) {
	b.Helper()

	values := generateBenchmarkValues(size)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := computeList(values)
		if err != nil {
			b.Fatalf("computeList failed: %v", err)
		}
		if result == nil {
			b.Fatal("computeList returned nil")
		}
	}

	b.ReportMetric(float64(size), "values/op")
}

// ============================================================================
// Accuracy comparison test for APPROX_COUNT_DISTINCT
// ============================================================================

// TestApproxCountDistinctAccuracy compares APPROX_COUNT_DISTINCT accuracy at various sizes.
func TestApproxCountDistinctAccuracy(t *testing.T) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			// Generate distinct values
			values := make([]any, size)
			for i := 0; i < size; i++ {
				values[i] = i
			}

			// Compute approximate count
			approxResult, err := computeApproxCountDistinct(values)
			if err != nil {
				t.Fatalf("computeApproxCountDistinct failed: %v", err)
			}
			approxCount := approxResult.(int64)

			// Compute exact count
			seen := make(map[any]struct{})
			for _, v := range values {
				if v != nil {
					seen[v] = struct{}{}
				}
			}
			exactCount := int64(len(seen))

			// Calculate error percentage
			errorPct := math.Abs(float64(approxCount-exactCount)) / float64(exactCount) * 100

			t.Logf("Size: %d, Exact: %d, Approx: %d, Error: %.2f%%",
				size, exactCount, approxCount, errorPct)

			// HyperLogLog with precision 14 should have ~0.8% standard error
			// Allow up to 5% error for this test
			if errorPct > 5.0 {
				t.Errorf("Error %.2f%% exceeds 5%% threshold", errorPct)
			}
		})
	}
}

// ============================================================================
// Helper functions
// ============================================================================

// generateBenchmarkValues creates a slice of numeric values for benchmarking.
func generateBenchmarkValues(size int) []any {
	values := make([]any, size)
	for i := 0; i < size; i++ {
		values[i] = float64(i) * 1.5
	}
	return values
}
