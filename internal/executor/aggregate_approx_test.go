package executor

import (
	"math"
	"testing"
)

// TestHyperLogLog tests the HyperLogLog cardinality estimation.
func TestHyperLogLog(t *testing.T) {
	tests := []struct {
		name          string
		values        []any
		expectedMin   float64
		expectedMax   float64
	}{
		{
			name:        "empty input",
			values:      []any{},
			expectedMin: 0,
			expectedMax: 0,
		},
		{
			name:        "single value",
			values:      []any{"a"},
			expectedMin: 1,
			expectedMax: 2,
		},
		{
			name:        "duplicate values",
			values:      []any{"a", "a", "a", "a", "a"},
			expectedMin: 1,
			expectedMax: 2,
		},
		{
			name:        "ten distinct values",
			values:      []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectedMin: 8,
			expectedMax: 12,
		},
		{
			name:        "hundred distinct values",
			values:      generateIntValues(100),
			expectedMin: 90,
			expectedMax: 110,
		},
		{
			name:        "thousand distinct values",
			values:      generateIntValues(1000),
			expectedMin: 950,
			expectedMax: 1050,
		},
		{
			name:        "mixed types",
			values:      []any{"a", 1, 2.5, "b", true, nil, "c"},
			expectedMin: 5,  // nil is skipped
			expectedMax: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hll := NewHyperLogLog(14)
			for _, v := range tt.values {
				hll.Add(v)
			}
			estimate := hll.Estimate()

			if estimate < tt.expectedMin || estimate > tt.expectedMax {
				t.Errorf("HyperLogLog estimate = %v, want between %v and %v",
					estimate, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

// TestTDigest tests the T-Digest quantile estimation.
func TestTDigest(t *testing.T) {
	tests := []struct {
		name      string
		values    []float64
		quantile  float64
		expectedMin float64
		expectedMax float64
	}{
		{
			name:      "single value median",
			values:    []float64{5.0},
			quantile:  0.5,
			expectedMin: 5.0,
			expectedMax: 5.0,
		},
		{
			name:      "two values median",
			values:    []float64{1.0, 9.0},
			quantile:  0.5,
			expectedMin: 4.0,
			expectedMax: 6.0,
		},
		{
			name:      "uniform 1-10 median",
			values:    []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			quantile:  0.5,
			expectedMin: 5.0,
			expectedMax: 6.0,
		},
		{
			name:      "uniform 1-10 p25",
			values:    []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			quantile:  0.25,
			expectedMin: 2.5,
			expectedMax: 3.5,
		},
		{
			name:      "uniform 1-10 p75",
			values:    []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			quantile:  0.75,
			expectedMin: 7.5,
			expectedMax: 8.5,
		},
		{
			name:      "uniform 1-10 p0",
			values:    []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			quantile:  0.0,
			expectedMin: 1.0,
			expectedMax: 1.5,
		},
		{
			name:      "uniform 1-10 p100",
			values:    []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			quantile:  1.0,
			expectedMin: 9.5,
			expectedMax: 10.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := NewTDigest(100)
			for _, v := range tt.values {
				td.Add(v)
			}
			result := td.Quantile(tt.quantile)

			if result < tt.expectedMin || result > tt.expectedMax {
				t.Errorf("TDigest quantile(%v) = %v, want between %v and %v",
					tt.quantile, result, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

// TestComputeApproxCountDistinct tests the approximate count distinct function.
func TestComputeApproxCountDistinct(t *testing.T) {
	tests := []struct {
		name        string
		values      []any
		expectedMin int64
		expectedMax int64
		expectNil   bool
	}{
		{
			name:      "empty input",
			values:    []any{},
			expectNil: true,
		},
		{
			name:      "all nulls",
			values:    []any{nil, nil, nil},
			expectNil: true,
		},
		{
			name:        "single value",
			values:      []any{"hello"},
			expectedMin: 1,
			expectedMax: 2,
		},
		{
			name:        "five distinct",
			values:      []any{1, 2, 3, 4, 5},
			expectedMin: 4,
			expectedMax: 6,
		},
		{
			name:        "with nulls",
			values:      []any{1, nil, 2, nil, 3},
			expectedMin: 2,
			expectedMax: 4,
		},
		{
			name:        "duplicates",
			values:      []any{1, 1, 2, 2, 3, 3},
			expectedMin: 2,
			expectedMax: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeApproxCountDistinct(tt.values)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.expectNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}

			count, ok := result.(int64)
			if !ok {
				t.Fatalf("expected int64, got %T", result)
			}

			if count < tt.expectedMin || count > tt.expectedMax {
				t.Errorf("count = %d, want between %d and %d",
					count, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

// TestComputeApproxQuantile tests the approximate quantile function.
func TestComputeApproxQuantile(t *testing.T) {
	tests := []struct {
		name        string
		values      []any
		q           float64
		expectedMin float64
		expectedMax float64
		expectNil   bool
	}{
		{
			name:      "empty input",
			values:    []any{},
			q:         0.5,
			expectNil: true,
		},
		{
			name:      "all nulls",
			values:    []any{nil, nil, nil},
			q:         0.5,
			expectNil: true,
		},
		{
			name:      "invalid q < 0",
			values:    []any{1, 2, 3},
			q:         -0.1,
			expectNil: true,
		},
		{
			name:      "invalid q > 1",
			values:    []any{1, 2, 3},
			q:         1.1,
			expectNil: true,
		},
		{
			name:        "single value",
			values:      []any{5.0},
			q:           0.5,
			expectedMin: 5.0,
			expectedMax: 5.0,
		},
		{
			name:        "median of 1-10",
			values:      []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			q:           0.5,
			expectedMin: 5.0,
			expectedMax: 6.0,
		},
		{
			name:        "p25 of 1-10",
			values:      []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			q:           0.25,
			expectedMin: 2.0,
			expectedMax: 4.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeApproxQuantile(tt.values, tt.q)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.expectNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}

			val, ok := result.(float64)
			if !ok {
				t.Fatalf("expected float64, got %T", result)
			}

			if val < tt.expectedMin || val > tt.expectedMax {
				t.Errorf("quantile = %v, want between %v and %v",
					val, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

// TestComputeApproxMedian tests the approximate median function.
func TestComputeApproxMedian(t *testing.T) {
	tests := []struct {
		name        string
		values      []any
		expectedMin float64
		expectedMax float64
		expectNil   bool
	}{
		{
			name:      "empty input",
			values:    []any{},
			expectNil: true,
		},
		{
			name:        "single value",
			values:      []any{42.0},
			expectedMin: 42.0,
			expectedMax: 42.0,
		},
		{
			name:        "odd count",
			values:      []any{1, 2, 3, 4, 5},
			expectedMin: 2.5,
			expectedMax: 3.5,
		},
		{
			name:        "even count",
			values:      []any{1, 2, 3, 4},
			expectedMin: 2.0,
			expectedMax: 3.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeApproxMedian(tt.values)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.expectNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}

			val, ok := result.(float64)
			if !ok {
				t.Fatalf("expected float64, got %T", result)
			}

			if val < tt.expectedMin || val > tt.expectedMax {
				t.Errorf("median = %v, want between %v and %v",
					val, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

// TestTDigestCompression tests that T-Digest compression works correctly.
func TestTDigestCompression(t *testing.T) {
	td := NewTDigest(100)

	// Add many values to trigger compression
	for i := 0; i < 10000; i++ {
		td.Add(float64(i))
	}

	td.Compress()

	// Check that centroids were compressed
	if len(td.centroids) > td.maxSize {
		t.Errorf("too many centroids: %d > %d", len(td.centroids), td.maxSize)
	}

	// Check that median is approximately correct
	median := td.Quantile(0.5)
	expectedMedian := 4999.5
	tolerance := 100.0

	if math.Abs(median-expectedMedian) > tolerance {
		t.Errorf("median = %v, want approximately %v", median, expectedMedian)
	}
}

// TestHyperLogLogPrecision tests different precision levels.
func TestHyperLogLogPrecision(t *testing.T) {
	values := generateIntValues(1000)

	precisions := []uint8{4, 8, 12, 16}

	for _, p := range precisions {
		t.Run("precision_"+string(rune('0'+p)), func(t *testing.T) {
			hll := NewHyperLogLog(p)
			for _, v := range values {
				hll.Add(v)
			}
			estimate := hll.Estimate()

			// With 1000 distinct values, estimate should be within reasonable range
			// Lower precision means larger error
			var tolerance float64
			switch p {
			case 4:
				tolerance = 500 // ~50% error
			case 8:
				tolerance = 200 // ~20% error
			case 12:
				tolerance = 100 // ~10% error
			case 16:
				tolerance = 50  // ~5% error
			}

			diff := math.Abs(estimate - 1000)
			if diff > tolerance {
				t.Errorf("precision %d: estimate = %v, expected 1000 +/- %v", p, estimate, tolerance)
			}
		})
	}
}

// generateIntValues generates a slice of distinct integer values.
func generateIntValues(n int) []any {
	values := make([]any, n)
	for i := 0; i < n; i++ {
		values[i] = i
	}
	return values
}
