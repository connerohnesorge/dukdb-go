// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file implements compression selection heuristics
// for choosing the optimal compression algorithm based on data characteristics.
package duckdb

import (
	"math"
	"math/bits"
)

// CompressionStats holds statistics about a data segment used for
// selecting the optimal compression algorithm.
type CompressionStats struct {
	// TotalCount is the total number of values in the data.
	TotalCount int

	// UniqueCount is the number of unique (distinct) values.
	UniqueCount int

	// NullCount is the number of NULL values.
	NullCount int

	// RunCount is the number of consecutive runs of identical values.
	// A run is a sequence of one or more identical adjacent values.
	RunCount int

	// MinValue is the minimum non-NULL value (for numeric types).
	MinValue any

	// MaxValue is the maximum non-NULL value (for numeric types).
	MaxValue any

	// BitWidth is the minimum number of bits needed to represent
	// the range of integer values (for integer types).
	BitWidth uint8

	// IsMonotonic indicates whether the values are monotonically
	// increasing or decreasing (for numeric types).
	IsMonotonic bool

	// ConstantDelta indicates whether all consecutive differences
	// are the same (for delta encoding).
	ConstantDelta bool

	// AvgDelta is the average delta between consecutive values
	// (for numeric types).
	AvgDelta float64
}

// SelectCompression analyzes data and selects the best compression algorithm.
// It uses heuristics based on data characteristics to choose an algorithm
// that balances compression ratio and encoding/decoding speed.
//
// The selection logic follows these priorities:
//  1. CONSTANT: If all values are identical
//  2. RLE: If there are few runs relative to total count (< 30%)
//  3. DICTIONARY: If there are few unique values relative to total count (< 50%)
//  4. PFOR_DELTA: For monotonic integer sequences
//  5. BITPACKING: For integers with small bit width relative to full width (< 70%)
//  6. UNCOMPRESSED: Default fallback
//
// Parameters:
//   - data: The data values as []any
//   - typeID: The logical type of the data
//
// Returns the recommended CompressionType for the data.
func SelectCompression(data []any, typeID LogicalTypeID) CompressionType {
	if len(data) == 0 {
		return CompressionUncompressed
	}

	stats := AnalyzeData(data, typeID)

	// Check for constant (all same value)
	if stats.UniqueCount == 1 {
		return CompressionConstant
	}

	// Check for good RLE ratio (few runs relative to total)
	// RLE is efficient when there are long runs of repeated values
	if stats.TotalCount > 0 {
		rleRatio := float64(stats.RunCount) / float64(stats.TotalCount)
		if rleRatio < 0.3 {
			return CompressionRLE
		}
	}

	// Check for good dictionary ratio (few unique values)
	// Dictionary encoding is efficient when the cardinality is low
	if stats.TotalCount > 0 {
		dictRatio := float64(stats.UniqueCount) / float64(stats.TotalCount)
		if dictRatio < 0.5 {
			return CompressionDictionary
		}
	}

	// For integers, check if bit-packing helps
	if isIntegerType(typeID) {
		fullWidth := getTypeWidth(typeID) * 8
		if fullWidth > 0 && stats.BitWidth > 0 {
			bitRatio := float64(stats.BitWidth) / float64(fullWidth)
			if bitRatio < 0.7 {
				// PFOR_DELTA is better for sorted/monotonic data
				if stats.IsMonotonic {
					return CompressionPFORDelta
				}
				return CompressionBitPacking
			}
		}
	}

	return CompressionUncompressed
}

// AnalyzeData computes statistics about the data for compression selection.
// It analyzes the data to determine characteristics like uniqueness, runs,
// value ranges, and monotonicity.
//
// Parameters:
//   - data: The data values as []any
//   - typeID: The logical type of the data
//
// Returns a CompressionStats struct with the analysis results.
func AnalyzeData(data []any, typeID LogicalTypeID) *CompressionStats {
	stats := &CompressionStats{
		TotalCount: len(data),
	}

	if len(data) == 0 {
		return stats
	}

	// Count nulls, unique values, and runs
	uniqueSet := make(map[any]struct{})
	runCount := 0
	var prevValue any
	isFirst := true

	for i, v := range data {
		if v == nil {
			stats.NullCount++
			// A nil is considered a different value for run counting
			if !isFirst && (prevValue != nil) {
				runCount++
			}
			if isFirst {
				runCount = 1
				isFirst = false
			}
			prevValue = v
			continue
		}

		// Track unique values
		// For comparable types, use the value directly
		// For non-comparable types (slices, maps), use index as proxy
		switch vt := v.(type) {
		case []byte:
			// Use string conversion for byte slices
			uniqueSet[string(vt)] = struct{}{}
		default:
			uniqueSet[v] = struct{}{}
		}

		// Count runs (a run starts when value changes)
		if isFirst {
			runCount = 1
			isFirst = false
		} else if !valuesEqual(v, prevValue) {
			runCount++
		}

		// Update min/max for numeric types
		if i == 0 || prevValue == nil {
			stats.MinValue = v
			stats.MaxValue = v
		} else {
			stats.MinValue = minValue(stats.MinValue, v, typeID)
			stats.MaxValue = maxValue(stats.MaxValue, v, typeID)
		}

		prevValue = v
	}

	stats.UniqueCount = len(uniqueSet)
	stats.RunCount = runCount

	// Calculate bit width for integer types
	if isIntegerType(typeID) {
		stats.BitWidth = requiredBitWidth(data)
	}

	// Analyze deltas for numeric types
	if isNumericType(typeID) {
		isMonotonic, constantDelta, avgDelta := analyzeDeltas(data)
		stats.IsMonotonic = isMonotonic
		stats.ConstantDelta = constantDelta
		stats.AvgDelta = avgDelta
	}

	return stats
}

// isConstant checks if all values in the data are identical.
// NULL values are considered equal to each other.
//
// Parameters:
//   - data: The data values as []any
//
// Returns true if all values are the same, false otherwise.
func isConstant(data []any) bool {
	if len(data) == 0 {
		return true
	}

	first := data[0]
	for i := 1; i < len(data); i++ {
		if !valuesEqual(data[i], first) {
			return false
		}
	}
	return true
}

// countRuns counts the number of consecutive runs of identical values.
// A run is a sequence of one or more adjacent values that are the same.
// For example, [1, 1, 2, 2, 2, 1] has 3 runs: [1,1], [2,2,2], [1].
//
// Parameters:
//   - data: The data values as []any
//
// Returns the number of runs in the data.
func countRuns(data []any) int {
	if len(data) == 0 {
		return 0
	}

	runs := 1
	for i := 1; i < len(data); i++ {
		if !valuesEqual(data[i], data[i-1]) {
			runs++
		}
	}
	return runs
}

// countUnique counts the number of unique (distinct) values in the data.
// NULL values are counted as a single unique value.
//
// Parameters:
//   - data: The data values as []any
//
// Returns the number of unique values.
func countUnique(data []any) int {
	if len(data) == 0 {
		return 0
	}

	uniqueSet := make(map[any]struct{})
	hasNull := false

	for _, v := range data {
		if v == nil {
			hasNull = true
			continue
		}
		switch vt := v.(type) {
		case []byte:
			uniqueSet[string(vt)] = struct{}{}
		default:
			uniqueSet[v] = struct{}{}
		}
	}

	count := len(uniqueSet)
	if hasNull {
		count++
	}
	return count
}

// requiredBitWidth calculates the minimum number of bits needed to represent
// the range of integer values in the data.
//
// For unsigned integers, this is the number of bits needed to represent
// the maximum value. For signed integers, this accounts for both positive
// and negative values.
//
// Parameters:
//   - data: The data values as []any (expected to be integers)
//
// Returns the minimum bit width (0-64).
func requiredBitWidth(data []any) uint8 {
	if len(data) == 0 {
		return 0
	}

	var minVal, maxVal int64
	hasValue := false

	for _, v := range data {
		if v == nil {
			continue
		}

		val := toInt64(v)
		if !hasValue {
			minVal = val
			maxVal = val
			hasValue = true
		} else {
			if val < minVal {
				minVal = val
			}
			if val > maxVal {
				maxVal = val
			}
		}
	}

	if !hasValue {
		return 0
	}

	// Calculate the range
	// For signed integers, we need to handle negative values
	if minVal < 0 {
		// For signed representation, we need bits for the magnitude plus sign
		negBits := bitsNeeded(uint64(-minVal))
		posBits := bitsNeeded(uint64(maxVal))
		// Need one extra bit for sign
		maxBits := negBits
		if posBits > negBits {
			maxBits = posBits
		}
		return maxBits + 1
	}

	// For non-negative values, just calculate bits for max
	return bitsNeeded(uint64(maxVal))
}

// analyzeDeltas analyzes the delta (difference) patterns between consecutive values.
// This is useful for determining if PFOR_DELTA compression would be effective.
//
// Parameters:
//   - data: The data values as []any (expected to be numeric)
//
// Returns:
//   - isMonotonic: true if all deltas are >= 0 (increasing) or all <= 0 (decreasing)
//   - constantDelta: true if all deltas are the same
//   - avgDelta: the average delta value
func analyzeDeltas(data []any) (isMonotonic bool, constantDelta bool, avgDelta float64) {
	if len(data) < 2 {
		return true, true, 0
	}

	var deltas []float64
	var prevVal float64
	hasFirst := false
	allPositive := true
	allNegative := true
	allSame := true
	var firstDelta float64
	firstDeltaSet := false

	for _, v := range data {
		if v == nil {
			continue
		}

		val := toFloat64(v)
		if !hasFirst {
			prevVal = val
			hasFirst = true
			continue
		}

		delta := val - prevVal
		deltas = append(deltas, delta)

		if delta < 0 {
			allPositive = false
		}
		if delta > 0 {
			allNegative = false
		}

		if !firstDeltaSet {
			firstDelta = delta
			firstDeltaSet = true
		} else if delta != firstDelta {
			allSame = false
		}

		prevVal = val
	}

	if len(deltas) == 0 {
		return true, true, 0
	}

	// Calculate average delta
	sum := 0.0
	for _, d := range deltas {
		sum += d
	}
	avgDelta = sum / float64(len(deltas))

	isMonotonic = allPositive || allNegative
	constantDelta = allSame

	return isMonotonic, constantDelta, avgDelta
}

// SelectCompressionForStrings applies string-specific heuristics for compression selection.
// Strings have different characteristics than numeric types, so specialized
// heuristics can improve compression efficiency.
//
// Parameters:
//   - data: The string values
//
// Returns the recommended CompressionType for the string data.
func SelectCompressionForStrings(data []string) CompressionType {
	if len(data) == 0 {
		return CompressionUncompressed
	}

	// Convert to []any and analyze
	anyData := make([]any, len(data))
	for i, s := range data {
		anyData[i] = s
	}

	stats := AnalyzeData(anyData, TypeVarchar)

	// Check for constant (all same value)
	if stats.UniqueCount == 1 {
		return CompressionConstant
	}

	// Check for good RLE ratio
	if stats.TotalCount > 0 {
		rleRatio := float64(stats.RunCount) / float64(stats.TotalCount)
		if rleRatio < 0.3 {
			return CompressionRLE
		}
	}

	// Check for good dictionary ratio
	// Strings often benefit from dictionary encoding even at higher ratios
	if stats.TotalCount > 0 {
		dictRatio := float64(stats.UniqueCount) / float64(stats.TotalCount)
		if dictRatio < 0.7 { // Higher threshold for strings
			return CompressionDictionary
		}
	}

	return CompressionUncompressed
}

// SelectCompressionForIntegers applies integer-specific heuristics for compression selection.
// Integer types can benefit from bit-packing and delta encoding in addition
// to the general compression algorithms.
//
// Parameters:
//   - data: The integer values as []int64
//
// Returns the recommended CompressionType for the integer data.
func SelectCompressionForIntegers(data []int64) CompressionType {
	if len(data) == 0 {
		return CompressionUncompressed
	}

	// Convert to []any and analyze
	anyData := make([]any, len(data))
	for i, v := range data {
		anyData[i] = v
	}

	stats := AnalyzeData(anyData, TypeBigInt)

	// Check for constant (all same value)
	if stats.UniqueCount == 1 {
		return CompressionConstant
	}

	// Check for good RLE ratio
	if stats.TotalCount > 0 {
		rleRatio := float64(stats.RunCount) / float64(stats.TotalCount)
		if rleRatio < 0.3 {
			return CompressionRLE
		}
	}

	// Check for good dictionary ratio
	if stats.TotalCount > 0 {
		dictRatio := float64(stats.UniqueCount) / float64(stats.TotalCount)
		if dictRatio < 0.5 {
			return CompressionDictionary
		}
	}

	// Check bit width for bit-packing/FOR
	fullWidth := uint8(64) // int64 is 64 bits
	if stats.BitWidth > 0 && stats.BitWidth < fullWidth {
		bitRatio := float64(stats.BitWidth) / float64(fullWidth)
		if bitRatio < 0.7 {
			// PFOR_DELTA is better for sorted/monotonic data
			if stats.IsMonotonic {
				return CompressionPFORDelta
			}
			return CompressionBitPacking
		}
	}

	return CompressionUncompressed
}

// isIntegerType returns true if the type ID represents an integer type.
func isIntegerType(typeID LogicalTypeID) bool {
	switch typeID {
	case TypeTinyInt, TypeSmallInt, TypeInteger, TypeBigInt,
		TypeUTinyInt, TypeUSmallInt, TypeUInteger, TypeUBigInt,
		TypeHugeInt, TypeUHugeInt:
		return true
	default:
		return false
	}
}

// isNumericType returns true if the type ID represents a numeric type
// (integer or floating point).
func isNumericType(typeID LogicalTypeID) bool {
	if isIntegerType(typeID) {
		return true
	}
	switch typeID {
	case TypeFloat, TypeDouble, TypeDecimal:
		return true
	default:
		return false
	}
}

// getTypeWidth returns the byte width for integer types.
func getTypeWidth(typeID LogicalTypeID) uint8 {
	switch typeID {
	case TypeTinyInt, TypeUTinyInt:
		return 1
	case TypeSmallInt, TypeUSmallInt:
		return 2
	case TypeInteger, TypeUInteger:
		return 4
	case TypeBigInt, TypeUBigInt:
		return 8
	case TypeHugeInt, TypeUHugeInt:
		return 16
	default:
		return 0
	}
}

// valuesEqual compares two values for equality, handling special cases
// like nil values and byte slices.
func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle byte slices specially
	aBytes, aIsBytes := a.([]byte)
	bBytes, bIsBytes := b.([]byte)
	if aIsBytes && bIsBytes {
		if len(aBytes) != len(bBytes) {
			return false
		}
		for i := range aBytes {
			if aBytes[i] != bBytes[i] {
				return false
			}
		}
		return true
	}

	return a == b
}

// minValue returns the minimum of two values based on the type.
func minValue(a, b any, typeID LogicalTypeID) any {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}

	switch typeID {
	case TypeTinyInt:
		if toInt64(a) < toInt64(b) {
			return a
		}
		return b
	case TypeSmallInt:
		if toInt64(a) < toInt64(b) {
			return a
		}
		return b
	case TypeInteger:
		if toInt64(a) < toInt64(b) {
			return a
		}
		return b
	case TypeBigInt:
		if toInt64(a) < toInt64(b) {
			return a
		}
		return b
	case TypeUTinyInt, TypeUSmallInt, TypeUInteger, TypeUBigInt:
		if toUint64(a) < toUint64(b) {
			return a
		}
		return b
	case TypeFloat, TypeDouble:
		if toFloat64(a) < toFloat64(b) {
			return a
		}
		return b
	default:
		// For non-comparable types, return a
		return a
	}
}

// maxValue returns the maximum of two values based on the type.
func maxValue(a, b any, typeID LogicalTypeID) any {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}

	switch typeID {
	case TypeTinyInt:
		if toInt64(a) > toInt64(b) {
			return a
		}
		return b
	case TypeSmallInt:
		if toInt64(a) > toInt64(b) {
			return a
		}
		return b
	case TypeInteger:
		if toInt64(a) > toInt64(b) {
			return a
		}
		return b
	case TypeBigInt:
		if toInt64(a) > toInt64(b) {
			return a
		}
		return b
	case TypeUTinyInt, TypeUSmallInt, TypeUInteger, TypeUBigInt:
		if toUint64(a) > toUint64(b) {
			return a
		}
		return b
	case TypeFloat, TypeDouble:
		if toFloat64(a) > toFloat64(b) {
			return a
		}
		return b
	default:
		// For non-comparable types, return a
		return a
	}
}

// toInt64 converts a value to int64.
func toInt64(v any) int64 {
	switch vt := v.(type) {
	case int8:
		return int64(vt)
	case int16:
		return int64(vt)
	case int32:
		return int64(vt)
	case int64:
		return vt
	case int:
		return int64(vt)
	case uint8:
		return int64(vt)
	case uint16:
		return int64(vt)
	case uint32:
		return int64(vt)
	case uint64:
		return int64(vt)
	case uint:
		return int64(vt)
	default:
		return 0
	}
}

// toUint64 converts a value to uint64.
func toUint64(v any) uint64 {
	switch vt := v.(type) {
	case uint8:
		return uint64(vt)
	case uint16:
		return uint64(vt)
	case uint32:
		return uint64(vt)
	case uint64:
		return vt
	case uint:
		return uint64(vt)
	case int8:
		return uint64(vt)
	case int16:
		return uint64(vt)
	case int32:
		return uint64(vt)
	case int64:
		return uint64(vt)
	case int:
		return uint64(vt)
	default:
		return 0
	}
}

// toFloat64 converts a value to float64.
func toFloat64(v any) float64 {
	switch vt := v.(type) {
	case float32:
		return float64(vt)
	case float64:
		return vt
	case int8:
		return float64(vt)
	case int16:
		return float64(vt)
	case int32:
		return float64(vt)
	case int64:
		return float64(vt)
	case int:
		return float64(vt)
	case uint8:
		return float64(vt)
	case uint16:
		return float64(vt)
	case uint32:
		return float64(vt)
	case uint64:
		return float64(vt)
	case uint:
		return float64(vt)
	default:
		return 0
	}
}

// bitsNeeded returns the number of bits needed to represent a value.
// Returns 0 for the value 0.
func bitsNeeded(v uint64) uint8 {
	if v == 0 {
		return 0
	}
	return uint8(bits.Len64(v))
}

// CompressionEstimate provides an estimate of the compressed size for each
// compression algorithm, helping choose the best compression.
type CompressionEstimate struct {
	// Algorithm is the compression type.
	Algorithm CompressionType

	// EstimatedSize is the estimated compressed size in bytes.
	EstimatedSize int

	// CompressionRatio is the ratio of original size to compressed size.
	CompressionRatio float64
}

// EstimateCompressionSizes estimates the compressed size for different
// compression algorithms. This can be used for more accurate compression
// selection when compression ratio is more important than speed.
//
// Parameters:
//   - data: The data values as []any
//   - typeID: The logical type of the data
//   - valueSize: The size in bytes of each value
//
// Returns a slice of CompressionEstimate sorted by estimated size (smallest first).
func EstimateCompressionSizes(data []any, typeID LogicalTypeID, valueSize int) []CompressionEstimate {
	if len(data) == 0 || valueSize <= 0 {
		return nil
	}

	stats := AnalyzeData(data, typeID)
	originalSize := len(data) * valueSize

	estimates := make([]CompressionEstimate, 0, 6)

	// UNCOMPRESSED: original size
	estimates = append(estimates, CompressionEstimate{
		Algorithm:        CompressionUncompressed,
		EstimatedSize:    originalSize,
		CompressionRatio: 1.0,
	})

	// CONSTANT: just store one value
	if stats.UniqueCount == 1 {
		estimates = append(estimates, CompressionEstimate{
			Algorithm:        CompressionConstant,
			EstimatedSize:    valueSize,
			CompressionRatio: float64(originalSize) / float64(valueSize),
		})
	}

	// RLE: estimate based on run count
	// Each run needs: varint count (1-10 bytes) + value (valueSize bytes)
	// Estimate varint as average 2 bytes
	rleSize := stats.RunCount * (2 + valueSize)
	if rleSize < originalSize {
		estimates = append(estimates, CompressionEstimate{
			Algorithm:        CompressionRLE,
			EstimatedSize:    rleSize,
			CompressionRatio: float64(originalSize) / float64(rleSize),
		})
	}

	// DICTIONARY: dictionary + indices
	// Dictionary: uniqueCount * valueSize
	// Indices: totalCount * ceil(log2(uniqueCount)) / 8 bytes
	indexBits := bitsNeeded(uint64(stats.UniqueCount))
	if indexBits == 0 {
		indexBits = 1
	}
	dictSize := stats.UniqueCount*valueSize + 4 + // dictionary + size header
		8 + // index count
		(len(data)*int(indexBits)+7)/8 // bit-packed indices
	if dictSize < originalSize {
		estimates = append(estimates, CompressionEstimate{
			Algorithm:        CompressionDictionary,
			EstimatedSize:    dictSize,
			CompressionRatio: float64(originalSize) / float64(dictSize),
		})
	}

	// BITPACKING: for integers
	if isIntegerType(typeID) && stats.BitWidth > 0 {
		// Header (9 bytes) + packed data
		bpSize := 9 + (len(data)*int(stats.BitWidth)+7)/8
		if bpSize < originalSize {
			estimates = append(estimates, CompressionEstimate{
				Algorithm:        CompressionBitPacking,
				EstimatedSize:    bpSize,
				CompressionRatio: float64(originalSize) / float64(bpSize),
			})
		}

		// PFOR_DELTA: reference + header + delta bits
		// Deltas might need fewer bits than the values themselves
		deltaBitWidth := stats.BitWidth
		if stats.IsMonotonic && stats.ConstantDelta {
			// Constant delta needs very few bits
			deltaBitWidth = bitsNeeded(uint64(math.Abs(stats.AvgDelta)))
			if deltaBitWidth == 0 {
				deltaBitWidth = 1
			}
		}
		pforSize := 17 + ((len(data)-1)*int(deltaBitWidth)+7)/8 // header + packed deltas
		if pforSize < originalSize {
			estimates = append(estimates, CompressionEstimate{
				Algorithm:        CompressionPFORDelta,
				EstimatedSize:    pforSize,
				CompressionRatio: float64(originalSize) / float64(pforSize),
			})
		}
	}

	// Sort by estimated size
	for i := 1; i < len(estimates); i++ {
		for j := i; j > 0 && estimates[j].EstimatedSize < estimates[j-1].EstimatedSize; j-- {
			estimates[j], estimates[j-1] = estimates[j-1], estimates[j]
		}
	}

	return estimates
}

// SelectBestCompression analyzes data and selects the compression algorithm
// with the best estimated compression ratio. This is more thorough than
// SelectCompression but requires more computation.
//
// Parameters:
//   - data: The data values as []any
//   - typeID: The logical type of the data
//   - valueSize: The size in bytes of each value
//
// Returns the CompressionType with the best estimated compression ratio.
func SelectBestCompression(data []any, typeID LogicalTypeID, valueSize int) CompressionType {
	estimates := EstimateCompressionSizes(data, typeID, valueSize)
	if len(estimates) == 0 {
		return CompressionUncompressed
	}
	// Return the algorithm with smallest estimated size
	return estimates[0].Algorithm
}
