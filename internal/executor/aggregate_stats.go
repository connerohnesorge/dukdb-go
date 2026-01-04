// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"math"
	"sort"
)

// Statistical aggregate computation functions.
// These functions implement statistical aggregate operations for dukdb-go.
// All functions handle NULL values by skipping them and return nil for edge cases.

// Quickselect algorithm for O(n) median/quantile computation.
// This is an optimization over O(n log n) sorting for single position selection.

// quickselect finds the k-th smallest element in O(n) average time.
// It modifies the slice in place using partitioning.
// Uses median-of-three pivot selection for better average performance.
func quickselect(slice []float64, k int) float64 {
	if len(slice) == 0 {
		return 0
	}
	if len(slice) == 1 {
		return slice[0]
	}

	left, right := 0, len(slice)-1
	for {
		if left == right {
			return slice[left]
		}

		// Select pivot using median-of-three
		pivotIndex := medianOfThreePivot(slice, left, right)
		pivotIndex = partitionSlice(slice, left, right, pivotIndex)

		if k == pivotIndex {
			return slice[k]
		} else if k < pivotIndex {
			right = pivotIndex - 1
		} else {
			left = pivotIndex + 1
		}
	}
}

// medianOfThreePivot selects a pivot using median of first, middle, and last elements.
// This helps avoid worst-case O(n^2) behavior on sorted or nearly-sorted data.
func medianOfThreePivot(slice []float64, left, right int) int {
	mid := left + (right-left)/2
	// Sort the three elements to find median
	if slice[right] < slice[left] {
		slice[left], slice[right] = slice[right], slice[left]
	}
	if slice[mid] < slice[left] {
		slice[mid], slice[left] = slice[left], slice[mid]
	}
	if slice[right] < slice[mid] {
		slice[right], slice[mid] = slice[mid], slice[right]
	}
	return mid
}

// partitionSlice partitions the slice around the pivot and returns new pivot position.
// All elements < pivot are moved to the left, all elements >= pivot to the right.
func partitionSlice(slice []float64, left, right, pivotIndex int) int {
	pivotValue := slice[pivotIndex]
	// Move pivot to end
	slice[pivotIndex], slice[right] = slice[right], slice[pivotIndex]
	storeIndex := left

	for i := left; i < right; i++ {
		if slice[i] < pivotValue {
			slice[storeIndex], slice[i] = slice[i], slice[storeIndex]
			storeIndex++
		}
	}
	// Move pivot to final position
	slice[storeIndex], slice[right] = slice[right], slice[storeIndex]
	return storeIndex
}

// toFloat64ForStats converts a value to float64 for statistical computations.
// Returns the float64 value and a boolean indicating success.
// nil values return (0, false) to allow proper NULL handling.
func toFloat64ForStats(v any) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int16:
		return float64(val), true
	case int8:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint8:
		return float64(val), true
	default:
		return 0, false
	}
}

// collectNonNullFloats filters NULL values and converts to float64 slice.
// Returns the non-NULL values as a float64 slice.
func collectNonNullFloats(values []any) []float64 {
	result := make([]float64, 0, len(values))
	for _, v := range values {
		if f, ok := toFloat64ForStats(v); ok {
			result = append(result, f)
		}
	}
	return result
}

// computeMedian calculates the median of numeric values.
// Median is the middle value when values are sorted.
// For even number of values, returns the average of the two middle values.
// Returns nil for empty input.
// Uses quickselect algorithm for O(n) average time complexity.
func computeMedian(values []any) (any, error) {
	floats := collectNonNullFloats(values)
	if len(floats) == 0 {
		return nil, nil
	}

	n := len(floats)

	if n%2 == 1 {
		// Odd number of elements - use quickselect for middle value
		return quickselect(floats, n/2), nil
	}

	// Even number of elements - find n/2 position using quickselect
	// After quickselect(n/2), all elements < n/2 index are smaller than element at n/2
	// So we just need to find the max of elements [0..n/2-1] for the lower median
	upper := quickselect(floats, n/2)

	// Find the maximum of the left partition (which is the lower median)
	// After quickselect, everything at index < n/2 is smaller than floats[n/2]
	lower := floats[0]
	for i := 1; i < n/2; i++ {
		if floats[i] > lower {
			lower = floats[i]
		}
	}

	return (lower + upper) / 2, nil
}

// computeQuantile calculates the quantile at position q (0 <= q <= 1).
// Uses linear interpolation between adjacent values.
// Returns nil for empty input or invalid q.
// Uses quickselect algorithm for O(n) average time complexity.
func computeQuantile(values []any, q float64) (any, error) {
	if q < 0 || q > 1 {
		return nil, nil
	}

	floats := collectNonNullFloats(values)
	if len(floats) == 0 {
		return nil, nil
	}

	n := len(floats)
	if n == 1 {
		return floats[0], nil
	}

	// Calculate the index for the quantile using R-7 method
	index := q * float64(n-1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))

	if lower == upper {
		// Exact index - use quickselect
		return quickselect(floats, lower), nil
	}

	// Need interpolation between two adjacent values
	// After quickselect(upper), everything at index < upper is smaller
	// So lower value is max of elements [0..upper-1] which includes position 'lower'
	upperVal := quickselect(floats, upper)

	// Find max of left partition to get the value at position 'lower'
	lowerVal := floats[0]
	for i := 1; i < upper; i++ {
		if floats[i] > lowerVal {
			lowerVal = floats[i]
		}
	}

	// Linear interpolation
	frac := index - float64(lower)
	return lowerVal*(1-frac) + upperVal*frac, nil
}

// computeQuantileArray calculates multiple quantiles at once.
// Takes a slice of quantile positions (e.g., [0.25, 0.5, 0.75]).
// Returns a slice of the computed quantiles.
// Returns nil for empty input or if any quantile is invalid.
func computeQuantileArray(values []any, quantiles []float64) (any, error) {
	// Validate all quantile values first
	for _, q := range quantiles {
		if q < 0 || q > 1 {
			return nil, nil
		}
	}

	floats := collectNonNullFloats(values)
	if len(floats) == 0 {
		return nil, nil
	}

	sort.Float64s(floats)
	n := len(floats)

	results := make([]float64, len(quantiles))

	for i, q := range quantiles {
		if n == 1 {
			results[i] = floats[0]
			continue
		}

		// Calculate the index for the quantile
		// Using the R-7 method (default in many systems)
		index := q * float64(n-1)
		lower := int(math.Floor(index))
		upper := int(math.Ceil(index))

		if lower == upper {
			results[i] = floats[lower]
		} else {
			// Linear interpolation
			frac := index - float64(lower)
			results[i] = floats[lower]*(1-frac) + floats[upper]*frac
		}
	}

	// Convert to []any for consistent return type
	result := make([]any, len(results))
	for i, r := range results {
		result[i] = r
	}
	return result, nil
}

// computePercentileCont calculates the continuous percentile with interpolation.
// This is equivalent to computeQuantile with p/100.
// Percentile should be between 0 and 1 (representing 0% to 100%).
// Returns nil for empty input.
func computePercentileCont(values []any, p float64) (any, error) {
	return computeQuantile(values, p)
}

// computePercentileDisc calculates the discrete percentile (nearest value).
// Returns the first value whose cumulative distribution is >= p.
// Percentile should be between 0 and 1 (representing 0% to 100%).
// Returns nil for empty input.
func computePercentileDisc(values []any, p float64) (any, error) {
	if p < 0 || p > 1 {
		return nil, nil
	}

	floats := collectNonNullFloats(values)
	if len(floats) == 0 {
		return nil, nil
	}

	sort.Float64s(floats)
	n := len(floats)

	// Calculate the index for the discrete percentile
	// PERCENTILE_DISC returns the first value whose cumulative distribution >= p
	index := int(math.Ceil(p * float64(n)))
	if index <= 0 {
		return floats[0], nil
	}
	if index >= n {
		return floats[n-1], nil
	}
	return floats[index-1], nil
}

// computeMode calculates the most frequent value.
// If multiple values have the same highest frequency, returns the smallest one.
// Returns nil for empty input.
func computeMode(values []any) (any, error) {
	floats := collectNonNullFloats(values)
	if len(floats) == 0 {
		return nil, nil
	}

	// Count frequencies
	freq := make(map[float64]int)
	for _, v := range floats {
		freq[v]++
	}

	// Find mode (highest frequency, smallest value for ties)
	var mode float64
	maxCount := 0
	first := true

	// Sort unique values to get deterministic result for ties
	uniqueVals := make([]float64, 0, len(freq))
	for v := range freq {
		uniqueVals = append(uniqueVals, v)
	}
	sort.Float64s(uniqueVals)

	for _, v := range uniqueVals {
		count := freq[v]
		if first || count > maxCount {
			mode = v
			maxCount = count
			first = false
		}
	}

	return mode, nil
}

// computeEntropy calculates Shannon entropy (information entropy) in bits.
// H = -SUM(p_i * log2(p_i)) for all distinct values.
// Returns nil for empty input.
// Returns 0 for single unique value.
func computeEntropy(values []any) (any, error) {
	floats := collectNonNullFloats(values)
	if len(floats) == 0 {
		return nil, nil
	}

	// Count frequencies
	freq := make(map[float64]int)
	for _, v := range floats {
		freq[v]++
	}

	n := float64(len(floats))
	if n == 0 {
		return nil, nil
	}

	// Calculate entropy
	var entropy float64
	for _, count := range freq {
		if count > 0 {
			p := float64(count) / n
			entropy -= p * math.Log2(p)
		}
	}

	return entropy, nil
}

// computeSkewness calculates Fisher-Pearson skewness coefficient.
// Skewness = E[(X - mean)^3] / stddev^3
// Returns nil for empty input or when N < 3.
// Uses sample skewness formula with bias correction.
func computeSkewness(values []any) (any, error) {
	floats := collectNonNullFloats(values)
	n := len(floats)
	if n < 3 {
		return nil, nil
	}

	// Calculate mean
	var sum float64
	for _, v := range floats {
		sum += v
	}
	mean := sum / float64(n)

	// Calculate second and third moments
	var m2, m3 float64
	for _, v := range floats {
		diff := v - mean
		diff2 := diff * diff
		m2 += diff2
		m3 += diff2 * diff
	}

	// Sample variance (with Bessel's correction)
	variance := m2 / float64(n-1)
	if variance == 0 {
		return 0.0, nil
	}

	// Sample standard deviation
	stddev := math.Sqrt(variance)

	// Fisher-Pearson sample skewness with bias correction
	// g1 = (n / ((n-1)(n-2))) * SUM((x - mean)^3) / s^3
	skewness := (float64(n) / (float64(n-1) * float64(n-2))) * (m3 / (stddev * stddev * stddev))

	return skewness, nil
}

// computeKurtosis calculates excess kurtosis.
// Kurtosis = E[(X - mean)^4] / variance^2 - 3
// Returns nil for empty input or when N < 4.
// Uses sample kurtosis formula with bias correction.
func computeKurtosis(values []any) (any, error) {
	floats := collectNonNullFloats(values)
	n := len(floats)
	if n < 4 {
		return nil, nil
	}

	// Calculate mean
	var sum float64
	for _, v := range floats {
		sum += v
	}
	mean := sum / float64(n)

	// Calculate second and fourth moments
	var m2, m4 float64
	for _, v := range floats {
		diff := v - mean
		diff2 := diff * diff
		m2 += diff2
		m4 += diff2 * diff2
	}

	// Sample variance
	variance := m2 / float64(n-1)
	if variance == 0 {
		return 0.0, nil
	}

	// Excess kurtosis with bias correction (Fisher's definition)
	// G2 = ((n+1)*n / ((n-1)(n-2)(n-3))) * (SUM((x-mean)^4) / variance^2) - 3*(n-1)^2 / ((n-2)(n-3))
	nf := float64(n)
	term1 := ((nf + 1) * nf) / ((nf - 1) * (nf - 2) * (nf - 3))
	term2 := m4 / (variance * variance)
	term3 := 3 * (nf - 1) * (nf - 1) / ((nf - 2) * (nf - 3))

	kurtosis := term1*term2 - term3

	return kurtosis, nil
}

// computeVarPop calculates population variance.
// VAR_POP = SUM((x - mean)^2) / N
// Returns nil for empty input.
func computeVarPop(values []any) (any, error) {
	floats := collectNonNullFloats(values)
	n := len(floats)
	if n == 0 {
		return nil, nil
	}

	// Calculate mean
	var sum float64
	for _, v := range floats {
		sum += v
	}
	mean := sum / float64(n)

	// Calculate sum of squared differences
	var sumSq float64
	for _, v := range floats {
		diff := v - mean
		sumSq += diff * diff
	}

	// Population variance (divide by N)
	return sumSq / float64(n), nil
}

// computeVarSamp calculates sample variance.
// VAR_SAMP = SUM((x - mean)^2) / (N - 1)
// Returns nil for empty input or when N < 2.
func computeVarSamp(values []any) (any, error) {
	floats := collectNonNullFloats(values)
	n := len(floats)
	if n < 2 {
		return nil, nil
	}

	// Calculate mean
	var sum float64
	for _, v := range floats {
		sum += v
	}
	mean := sum / float64(n)

	// Calculate sum of squared differences
	var sumSq float64
	for _, v := range floats {
		diff := v - mean
		sumSq += diff * diff
	}

	// Sample variance (divide by N-1, Bessel's correction)
	return sumSq / float64(n-1), nil
}

// computeStddevPop calculates population standard deviation.
// STDDEV_POP = SQRT(VAR_POP)
// Returns nil for empty input.
func computeStddevPop(values []any) (any, error) {
	variance, err := computeVarPop(values)
	if err != nil {
		return nil, err
	}
	if variance == nil {
		return nil, nil
	}

	return math.Sqrt(variance.(float64)), nil
}

// computeStddevSamp calculates sample standard deviation.
// STDDEV_SAMP = SQRT(VAR_SAMP)
// Returns nil for empty input or when N < 2.
func computeStddevSamp(values []any) (any, error) {
	variance, err := computeVarSamp(values)
	if err != nil {
		return nil, err
	}
	if variance == nil {
		return nil, nil
	}

	return math.Sqrt(variance.(float64)), nil
}

// VarianceState implements Welford's online algorithm for streaming variance computation.
// This allows computing variance, standard deviation, and related statistics
// in a single pass without storing all values in memory.
//
// The algorithm maintains:
//   - count: number of values seen
//   - mean: running mean
//   - m2: sum of squared differences from the mean
//
// Reference: Welford, B. P. (1962). "Note on a method for calculating corrected sums
// of squares and products". Technometrics. 4 (3): 419-420.
type VarianceState struct {
	count int64   // number of values seen
	mean  float64 // running mean
	m2    float64 // sum of squared differences from the mean
}

// NewVarianceState creates a new VarianceState initialized to zero.
func NewVarianceState() *VarianceState {
	return &VarianceState{
		count: 0,
		mean:  0.0,
		m2:    0.0,
	}
}

// Update adds a new value to the running variance computation using Welford's algorithm.
// This method implements the online update formula:
//
//	count++
//	delta := value - mean
//	mean += delta / count
//	delta2 := value - mean
//	m2 += delta * delta2
func (vs *VarianceState) Update(value float64) {
	vs.count++
	delta := value - vs.mean
	vs.mean += delta / float64(vs.count)
	delta2 := value - vs.mean
	vs.m2 += delta * delta2
}

// Count returns the number of values that have been added.
func (vs *VarianceState) Count() int64 {
	return vs.count
}

// Mean returns the running mean of all values added.
func (vs *VarianceState) Mean() float64 {
	return vs.mean
}

// VariancePop returns the population variance.
// Population variance = M2 / N
// Returns 0 if no values have been added.
func (vs *VarianceState) VariancePop() float64 {
	if vs.count == 0 {
		return 0.0
	}
	return vs.m2 / float64(vs.count)
}

// VarianceSamp returns the sample variance (with Bessel's correction).
// Sample variance = M2 / (N - 1)
// Returns 0 if fewer than 2 values have been added.
func (vs *VarianceState) VarianceSamp() float64 {
	if vs.count < 2 {
		return 0.0
	}
	return vs.m2 / float64(vs.count-1)
}

// StdDevPop returns the population standard deviation.
// Population stddev = sqrt(VariancePop)
func (vs *VarianceState) StdDevPop() float64 {
	return math.Sqrt(vs.VariancePop())
}

// StdDevSamp returns the sample standard deviation.
// Sample stddev = sqrt(VarianceSamp)
func (vs *VarianceState) StdDevSamp() float64 {
	return math.Sqrt(vs.VarianceSamp())
}
