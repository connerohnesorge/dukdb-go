// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"hash/fnv"
	"math"
	"sort"
)

// Approximate aggregate computation functions.
// These functions implement probabilistic aggregate operations for dukdb-go.
// APPROX_COUNT_DISTINCT uses HyperLogLog for cardinality estimation.
// APPROX_QUANTILE and APPROX_MEDIAN use T-Digest for approximate quantile estimation.

// ============================================================================
// HyperLogLog Implementation
// ============================================================================

// HyperLogLog implements the HyperLogLog algorithm for approximate cardinality estimation.
// It provides space-efficient counting of distinct elements with configurable precision.
type HyperLogLog struct {
	registers []uint8
	precision uint8   // Number of bits to use for register index (p)
	m         uint32  // Number of registers (2^p)
	alpha     float64 // Bias correction constant
}

// NewHyperLogLog creates a new HyperLogLog with the given precision.
// Precision should be between 4 and 18. Default is 14 (~16K registers).
// Higher precision means more accurate estimates but more memory usage.
// Memory usage is 2^precision bytes.
func NewHyperLogLog(precision uint8) *HyperLogLog {
	if precision < 4 {
		precision = 4
	}
	if precision > 18 {
		precision = 18
	}

	m := uint32(1) << precision
	registers := make([]uint8, m)

	// Compute alpha (bias correction constant)
	var alpha float64
	switch m {
	case 16:
		alpha = 0.673
	case 32:
		alpha = 0.697
	case 64:
		alpha = 0.709
	default:
		alpha = 0.7213 / (1 + 1.079/float64(m))
	}

	return &HyperLogLog{
		registers: registers,
		precision: precision,
		m:         m,
		alpha:     alpha,
	}
}

// Add adds a value to the HyperLogLog.
func (hll *HyperLogLog) Add(value any) {
	if value == nil {
		return
	}

	// Hash the value using fnv64
	hash := hashValueForHLL(value)

	// Use the first p bits as the register index
	index := hash >> (64 - hll.precision)

	// Count leading zeros in the remaining bits + 1
	w := hash << hll.precision
	leadingZeros := uint8(countLeadingZeros(w) + 1)

	// Update register if new value is larger
	if leadingZeros > hll.registers[index] {
		hll.registers[index] = leadingZeros
	}
}

// Estimate returns the estimated cardinality.
func (hll *HyperLogLog) Estimate() float64 {
	// Compute the raw estimate using harmonic mean
	sum := 0.0
	for _, val := range hll.registers {
		sum += math.Pow(2, -float64(val))
	}

	estimate := hll.alpha * float64(hll.m) * float64(hll.m) / sum

	// Apply bias corrections
	estimate = hll.applyBiasCorrection(estimate)

	return estimate
}

// applyBiasCorrection applies corrections for small and large cardinalities.
func (hll *HyperLogLog) applyBiasCorrection(estimate float64) float64 {
	m := float64(hll.m)

	// Small range correction using linear counting
	if estimate <= 2.5*m {
		// Count empty registers
		zeros := 0
		for _, val := range hll.registers {
			if val == 0 {
				zeros++
			}
		}

		if zeros > 0 {
			// Linear counting estimate
			return m * math.Log(m/float64(zeros))
		}
	}

	// Large range correction (for very large cardinalities close to 2^64)
	const twoTo64 = float64(1<<63) * 2
	if estimate > twoTo64/30 {
		return -twoTo64 * math.Log(1-estimate/twoTo64)
	}

	return estimate
}

// hashValueForHLL hashes an arbitrary value to a uint64 for HyperLogLog.
// Uses FNV-1a followed by a finalization mixing step (similar to MurmurHash3).
func hashValueForHLL(value any) uint64 {
	h := fnv.New64a()
	// Convert value to string representation for hashing
	_, _ = h.Write([]byte(formatValue(value)))
	hash := h.Sum64()

	// Apply finalization mixing (from MurmurHash3)
	// This spreads the entropy across all bits
	hash ^= hash >> 33
	hash *= 0xff51afd7ed558ccd
	hash ^= hash >> 33
	hash *= 0xc4ceb9fe1a85ec53
	hash ^= hash >> 33

	return hash
}

// countLeadingZeros counts leading zeros in a uint64 using bit manipulation.
// This uses the standard technique of binary search for the leading bit.
func countLeadingZeros(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	// Check upper 32 bits
	if (x & 0xFFFFFFFF00000000) == 0 {
		n += 32
		x <<= 32
	}
	// Check upper 16 bits of remaining
	if (x & 0xFFFF000000000000) == 0 {
		n += 16
		x <<= 16
	}
	// Check upper 8 bits of remaining
	if (x & 0xFF00000000000000) == 0 {
		n += 8
		x <<= 8
	}
	// Check upper 4 bits of remaining
	if (x & 0xF000000000000000) == 0 {
		n += 4
		x <<= 4
	}
	// Check upper 2 bits of remaining
	if (x & 0xC000000000000000) == 0 {
		n += 2
		x <<= 2
	}
	// Check upper bit of remaining
	if (x & 0x8000000000000000) == 0 {
		n++
	}
	return n
}

// ============================================================================
// HyperLogLog Bit-Packed Implementation (Memory Optimized)
// ============================================================================

// HyperLogLogPacked uses bit-packed registers for memory efficiency.
// Each register stores a value 0-64 (leading zeros count + 1), requiring only 6 bits.
// We pack 10 registers per uint64 (60 bits used out of 64).
// Memory reduction: ~20% (16,384 bytes → 13,112 bytes at precision 14).
type HyperLogLogPacked struct {
	registers []uint64 // Packed registers, 10 per uint64
	precision uint8
	m         uint32 // Number of logical registers
	numWords  int    // Number of uint64 words
	alpha     float64
}

const (
	bitsPerRegister  = 6    // Each register needs 6 bits (max value 64)
	registersPerWord = 10   // 10 * 6 = 60 bits per uint64
	registerMask     = 0x3F // 6 bits = 0b111111
)

// NewHyperLogLogPacked creates a bit-packed HyperLogLog for memory efficiency.
func NewHyperLogLogPacked(precision uint8) *HyperLogLogPacked {
	if precision < 4 {
		precision = 4
	}
	if precision > 18 {
		precision = 18
	}

	m := uint32(1) << precision
	numWords := (int(m) + registersPerWord - 1) / registersPerWord

	// Compute alpha (bias correction constant)
	var alpha float64
	switch m {
	case 16:
		alpha = 0.673
	case 32:
		alpha = 0.697
	case 64:
		alpha = 0.709
	default:
		alpha = 0.7213 / (1 + 1.079/float64(m))
	}

	return &HyperLogLogPacked{
		registers: make([]uint64, numWords),
		precision: precision,
		m:         m,
		numWords:  numWords,
		alpha:     alpha,
	}
}

// getRegister extracts the 6-bit register value at the given index.
func (hll *HyperLogLogPacked) getRegister(index uint32) uint8 {
	wordIndex := int(index) / registersPerWord
	bitOffset := (int(index) % registersPerWord) * bitsPerRegister
	return uint8((hll.registers[wordIndex] >> bitOffset) & registerMask)
}

// setRegister sets the 6-bit register value at the given index.
func (hll *HyperLogLogPacked) setRegister(index uint32, value uint8) {
	wordIndex := int(index) / registersPerWord
	bitOffset := (int(index) % registersPerWord) * bitsPerRegister

	// Clear existing bits and set new value
	mask := ^(uint64(registerMask) << bitOffset)
	hll.registers[wordIndex] = (hll.registers[wordIndex] & mask) | (uint64(value&registerMask) << bitOffset)
}

// Add adds a value to the HyperLogLog.
func (hll *HyperLogLogPacked) Add(value any) {
	if value == nil {
		return
	}

	hash := hashValueForHLL(value)
	index := uint32(hash >> (64 - hll.precision))
	w := hash << hll.precision
	leadingZeros := uint8(countLeadingZeros(w) + 1)

	// Update register if new value is larger
	if leadingZeros > hll.getRegister(index) {
		hll.setRegister(index, leadingZeros)
	}
}

// Estimate returns the estimated cardinality.
func (hll *HyperLogLogPacked) Estimate() float64 {
	sum := 0.0
	for i := uint32(0); i < hll.m; i++ {
		sum += math.Pow(2, -float64(hll.getRegister(i)))
	}

	estimate := hll.alpha * float64(hll.m) * float64(hll.m) / sum
	return hll.applyBiasCorrection(estimate)
}

// applyBiasCorrection applies corrections for small and large cardinalities.
func (hll *HyperLogLogPacked) applyBiasCorrection(estimate float64) float64 {
	m := float64(hll.m)

	// Small range correction using linear counting
	if estimate <= 2.5*m {
		zeros := 0
		for i := uint32(0); i < hll.m; i++ {
			if hll.getRegister(i) == 0 {
				zeros++
			}
		}
		if zeros > 0 {
			return m * math.Log(m/float64(zeros))
		}
	}

	// Large range correction
	const twoTo64 = float64(1<<63) * 2
	if estimate > twoTo64/30 {
		return -twoTo64 * math.Log(1-estimate/twoTo64)
	}

	return estimate
}

// ============================================================================
// T-Digest Implementation
// ============================================================================

// Centroid represents a cluster in the T-Digest.
type Centroid struct {
	Mean   float64
	Weight float64 // Number of values in this centroid
}

// TDigest implements the T-Digest algorithm for approximate quantile estimation.
// It provides accurate quantile estimates especially at the tails of the distribution.
// Uses lazy compression: values are buffered and only compressed when queried.
type TDigest struct {
	centroids        []Centroid
	compression      float64 // Compression factor (default 100)
	totalWeight      float64
	maxSize          int
	unmerged         []Centroid // Buffer for unmerged centroids
	needsCompression bool       // Track if compression is needed (lazy compression)
}

// lazyMaxBufferMultiplier controls when safety compression is triggered.
// Compression happens when unmerged buffer exceeds maxSize * lazyMaxBufferMultiplier.
const lazyMaxBufferMultiplier = 4

// NewTDigest creates a new T-Digest with the given compression factor.
// Higher compression means more accurate estimates but more memory usage.
// Recommended values are between 100 and 500. Default is 100.
func NewTDigest(compression float64) *TDigest {
	if compression <= 0 {
		compression = 100
	}

	// Maximum number of centroids is proportional to compression
	maxSize := int(math.Ceil(compression * math.Pi))

	return &TDigest{
		centroids:   make([]Centroid, 0, maxSize),
		compression: compression,
		totalWeight: 0,
		maxSize:     maxSize,
		unmerged:    make([]Centroid, 0, maxSize),
	}
}

// Add adds a value to the T-Digest.
func (td *TDigest) Add(value float64) {
	td.AddWeighted(value, 1)
}

// AddWeighted adds a weighted value to the T-Digest.
// Uses lazy compression: values are buffered and only compressed when queried.
func (td *TDigest) AddWeighted(value float64, weight float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) || weight <= 0 {
		return
	}

	td.unmerged = append(td.unmerged, Centroid{Mean: value, Weight: weight})
	td.totalWeight += weight
	td.needsCompression = true

	// Safety check: compress if buffer grows too large (4x maxSize)
	// This prevents unbounded memory growth while still being lazy
	if len(td.unmerged) >= td.maxSize*lazyMaxBufferMultiplier {
		td.Compress()
	}
}

// Compress merges the centroids to reduce memory usage while maintaining accuracy.
func (td *TDigest) Compress() {
	if len(td.unmerged) == 0 && len(td.centroids) <= td.maxSize {
		td.needsCompression = false
		return
	}

	// Merge unmerged into centroids
	allCentroids := make([]Centroid, 0, len(td.centroids)+len(td.unmerged))
	allCentroids = append(allCentroids, td.centroids...)
	allCentroids = append(allCentroids, td.unmerged...)
	td.unmerged = td.unmerged[:0]

	if len(allCentroids) == 0 {
		td.centroids = td.centroids[:0]
		return
	}

	// Sort by mean
	sort.Slice(allCentroids, func(i, j int) bool {
		return allCentroids[i].Mean < allCentroids[j].Mean
	})

	// If we have fewer centroids than maxSize, don't compress
	if len(allCentroids) <= td.maxSize {
		td.centroids = allCentroids
		return
	}

	// Merge centroids using the k-size function
	merged := make([]Centroid, 0, td.maxSize)
	current := allCentroids[0]
	weightSoFar := 0.0

	for i := 1; i < len(allCentroids); i++ {
		next := allCentroids[i]
		proposedWeight := current.Weight + next.Weight

		// Calculate quantile position for the proposed merged centroid
		q := (weightSoFar + current.Weight + proposedWeight/2) / td.totalWeight

		// Calculate the maximum size for this quantile
		kLimit := td.kSize(q)

		if proposedWeight <= kLimit {
			// Merge into current centroid
			newWeight := current.Weight + next.Weight
			current.Mean = (current.Mean*current.Weight + next.Mean*next.Weight) / newWeight
			current.Weight = newWeight
		} else {
			// Start new centroid
			merged = append(merged, current)
			weightSoFar += current.Weight
			current = next
		}
	}
	merged = append(merged, current)

	td.centroids = merged
	td.needsCompression = false
}

// kSize returns the maximum size for a centroid at quantile q.
// This uses the k1 scaling function from the T-Digest paper.
func (td *TDigest) kSize(q float64) float64 {
	// k1 scaling function: k(q) = delta/2 * (sin^(-1)(2q-1) / pi + 1/2)
	// Derivative: dk/dq = delta / (pi * sqrt(q * (1-q)))
	// Size limit is delta / (pi * sqrt(q * (1-q)))
	if q <= 0 || q >= 1 {
		return 0
	}
	return td.compression / (math.Pi * math.Sqrt(q*(1-q)))
}

// Quantile returns the approximate quantile at position q (0 <= q <= 1).
// Returns NaN if the digest is empty.
// Triggers lazy compression if needed.
func (td *TDigest) Quantile(q float64) float64 {
	// Lazy compression: only compress when needed
	if td.needsCompression {
		td.Compress()
	}

	if len(td.centroids) == 0 && len(td.unmerged) == 0 {
		return math.NaN()
	}
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}

	// For single centroid, return its mean
	if len(td.centroids) == 1 {
		return td.centroids[0].Mean
	}

	// Edge cases for q=0 and q=1
	if q == 0 {
		return td.centroids[0].Mean
	}
	if q == 1 {
		return td.centroids[len(td.centroids)-1].Mean
	}

	// Target weight position
	target := q * td.totalWeight

	// Compute cumulative weight for each centroid
	cumulative := make([]float64, len(td.centroids)+1)
	cumulative[0] = 0
	for i, c := range td.centroids {
		cumulative[i+1] = cumulative[i] + c.Weight
	}

	// Find the centroid that contains the target
	for i := 0; i < len(td.centroids); i++ {
		// Compute the "center of mass" position for this centroid
		// Each centroid represents values from (cumulative[i] to cumulative[i+1])
		// The center is at cumulative[i] + weight/2
		centerWeight := cumulative[i] + td.centroids[i].Weight/2

		if i == 0 && target < centerWeight {
			// Before the center of the first centroid - return first centroid's value
			return td.centroids[0].Mean
		}

		if i == len(td.centroids)-1 && target >= centerWeight {
			// After the center of the last centroid - return last centroid's value
			return td.centroids[len(td.centroids)-1].Mean
		}

		// Check if target falls between this centroid's center and the next
		if i < len(td.centroids)-1 {
			nextCenterWeight := cumulative[i+1] + td.centroids[i+1].Weight/2
			if target >= centerWeight && target < nextCenterWeight {
				// Interpolate between this centroid and the next
				t := (target - centerWeight) / (nextCenterWeight - centerWeight)
				return td.centroids[i].Mean + t*(td.centroids[i+1].Mean-td.centroids[i].Mean)
			}
		}
	}

	// Fallback (should not reach here)
	return td.centroids[len(td.centroids)-1].Mean
}

// ============================================================================
// Approximate Aggregate Functions
// ============================================================================

// computeApproxCountDistinct computes approximate count distinct using HyperLogLog.
// Returns the estimated number of distinct values in the input.
// Uses bit-packed HyperLogLog with precision 14 (~16K registers, ~0.8% standard error).
// Memory optimized: uses 13KB instead of 16KB through 6-bit register packing.
// Returns nil for empty input.
func computeApproxCountDistinct(values []any) (any, error) {
	// Filter out NULLs
	nonNull := make([]any, 0, len(values))
	for _, v := range values {
		if v != nil {
			nonNull = append(nonNull, v)
		}
	}

	if len(nonNull) == 0 {
		return nil, nil
	}

	// Create bit-packed HyperLogLog with default precision (14)
	hll := NewHyperLogLogPacked(14)

	// Add all values
	for _, v := range nonNull {
		hll.Add(v)
	}

	// Return the estimate as int64
	estimate := hll.Estimate()
	return int64(math.Round(estimate)), nil
}

// computeApproxQuantile computes approximate quantile using T-Digest.
// Parameter q should be between 0 and 1 (e.g., 0.5 for median).
// Returns nil for empty input or invalid q.
func computeApproxQuantile(values []any, q float64) (any, error) {
	if q < 0 || q > 1 {
		return nil, nil
	}

	// Collect non-NULL numeric values
	floats := collectNonNullFloats(values)
	if len(floats) == 0 {
		return nil, nil
	}

	// Create T-Digest with default compression (100)
	td := NewTDigest(100)

	// Add all values
	for _, f := range floats {
		td.Add(f)
	}

	// Return the quantile
	result := td.Quantile(q)
	if math.IsNaN(result) {
		return nil, nil
	}

	return result, nil
}

// computeApproxMedian computes approximate median using T-Digest.
// This is equivalent to computeApproxQuantile(values, 0.5).
// Returns nil for empty input.
func computeApproxMedian(values []any) (any, error) {
	return computeApproxQuantile(values, 0.5)
}
