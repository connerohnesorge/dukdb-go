package executor

import (
	"math"
	"testing"
)

const epsilon = 1e-9

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) < epsilon
}

func TestComputeMedian(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single value",
			values:   []any{5.0},
			expected: 5.0,
		},
		{
			name:     "odd number of values",
			values:   []any{1.0, 2.0, 3.0, 4.0, 5.0},
			expected: 3.0,
		},
		{
			name:     "even number of values",
			values:   []any{1.0, 2.0, 3.0, 4.0},
			expected: 2.5,
		},
		{
			name:     "with nulls",
			values:   []any{1.0, nil, 3.0, nil, 5.0},
			expected: 3.0,
		},
		{
			name:     "all nulls",
			values:   []any{nil, nil, nil},
			expected: nil,
		},
		{
			name:     "integer values",
			values:   []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
			expected: 3.0,
		},
		{
			name:     "unsorted values",
			values:   []any{5.0, 1.0, 3.0, 2.0, 4.0},
			expected: 3.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeMedian(tt.values)
			if err != nil {
				t.Errorf("computeMedian() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeMedian() = %v, want nil", result)
				}
				return
			}
			if !almostEqual(result.(float64), tt.expected.(float64)) {
				t.Errorf("computeMedian() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeQuantile(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		q        float64
		expected any
	}{
		{
			name:     "empty input",
			values:   []any{},
			q:        0.5,
			expected: nil,
		},
		{
			name:     "0th quantile (min)",
			values:   []any{1.0, 2.0, 3.0, 4.0, 5.0},
			q:        0.0,
			expected: 1.0,
		},
		{
			name:     "100th quantile (max)",
			values:   []any{1.0, 2.0, 3.0, 4.0, 5.0},
			q:        1.0,
			expected: 5.0,
		},
		{
			name:     "50th quantile (median)",
			values:   []any{1.0, 2.0, 3.0, 4.0, 5.0},
			q:        0.5,
			expected: 3.0,
		},
		{
			name:     "25th quantile",
			values:   []any{1.0, 2.0, 3.0, 4.0, 5.0},
			q:        0.25,
			expected: 2.0,
		},
		{
			name:     "75th quantile",
			values:   []any{1.0, 2.0, 3.0, 4.0, 5.0},
			q:        0.75,
			expected: 4.0,
		},
		{
			name:     "invalid q < 0",
			values:   []any{1.0, 2.0, 3.0},
			q:        -0.1,
			expected: nil,
		},
		{
			name:     "invalid q > 1",
			values:   []any{1.0, 2.0, 3.0},
			q:        1.1,
			expected: nil,
		},
		{
			name:     "single value",
			values:   []any{5.0},
			q:        0.5,
			expected: 5.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeQuantile(tt.values, tt.q)
			if err != nil {
				t.Errorf("computeQuantile() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeQuantile() = %v, want nil", result)
				}
				return
			}
			if !almostEqual(result.(float64), tt.expected.(float64)) {
				t.Errorf("computeQuantile() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeQuantileArray(t *testing.T) {
	tests := []struct {
		name      string
		values    []any
		quantiles []float64
		expected  []float64
	}{
		{
			name:      "empty input",
			values:    []any{},
			quantiles: []float64{0.25, 0.5, 0.75},
			expected:  nil,
		},
		{
			name:      "single value",
			values:    []any{5.0},
			quantiles: []float64{0.25, 0.5, 0.75},
			expected:  []float64{5.0, 5.0, 5.0},
		},
		{
			name:      "quartiles",
			values:    []any{1.0, 2.0, 3.0, 4.0, 5.0},
			quantiles: []float64{0.25, 0.5, 0.75},
			expected:  []float64{2.0, 3.0, 4.0},
		},
		{
			name:      "min and max",
			values:    []any{1.0, 2.0, 3.0, 4.0, 5.0},
			quantiles: []float64{0.0, 1.0},
			expected:  []float64{1.0, 5.0},
		},
		{
			name:      "deciles",
			values:    []any{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0},
			quantiles: []float64{0.1, 0.2, 0.5, 0.9},
			expected:  []float64{1.9, 2.8, 5.5, 9.1},
		},
		{
			name:      "with nulls",
			values:    []any{1.0, nil, 3.0, nil, 5.0},
			quantiles: []float64{0.0, 0.5, 1.0},
			expected:  []float64{1.0, 3.0, 5.0},
		},
		{
			name:      "all nulls",
			values:    []any{nil, nil, nil},
			quantiles: []float64{0.5},
			expected:  nil,
		},
		{
			name:      "invalid quantile < 0",
			values:    []any{1.0, 2.0, 3.0},
			quantiles: []float64{0.5, -0.1},
			expected:  nil,
		},
		{
			name:      "invalid quantile > 1",
			values:    []any{1.0, 2.0, 3.0},
			quantiles: []float64{0.5, 1.1},
			expected:  nil,
		},
		{
			name:      "empty quantiles array",
			values:    []any{1.0, 2.0, 3.0},
			quantiles: []float64{},
			expected:  []float64{},
		},
		{
			name:      "single quantile in array",
			values:    []any{1.0, 2.0, 3.0, 4.0, 5.0},
			quantiles: []float64{0.5},
			expected:  []float64{3.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeQuantileArray(tt.values, tt.quantiles)
			if err != nil {
				t.Errorf("computeQuantileArray() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeQuantileArray() = %v, want nil", result)
				}
				return
			}
			resultSlice, ok := result.([]any)
			if !ok {
				t.Errorf("computeQuantileArray() returned non-slice type: %T", result)
				return
			}
			if len(resultSlice) != len(tt.expected) {
				t.Errorf("computeQuantileArray() returned %d values, want %d", len(resultSlice), len(tt.expected))
				return
			}
			for i, exp := range tt.expected {
				got, ok := resultSlice[i].(float64)
				if !ok {
					t.Errorf("computeQuantileArray()[%d] is not float64: %T", i, resultSlice[i])
					continue
				}
				if !almostEqual(got, exp) {
					t.Errorf("computeQuantileArray()[%d] = %v, want %v", i, got, exp)
				}
			}
		})
	}
}

func TestComputePercentileDisc(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		p        float64
		expected any
	}{
		{
			name:     "empty input",
			values:   []any{},
			p:        0.5,
			expected: nil,
		},
		{
			name:     "0th percentile (min)",
			values:   []any{1.0, 2.0, 3.0, 4.0, 5.0},
			p:        0.0,
			expected: 1.0,
		},
		{
			name:     "100th percentile (max)",
			values:   []any{1.0, 2.0, 3.0, 4.0, 5.0},
			p:        1.0,
			expected: 5.0,
		},
		{
			name:     "50th percentile (discrete)",
			values:   []any{1.0, 2.0, 3.0, 4.0, 5.0},
			p:        0.5,
			expected: 3.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computePercentileDisc(tt.values, tt.p)
			if err != nil {
				t.Errorf("computePercentileDisc() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computePercentileDisc() = %v, want nil", result)
				}
				return
			}
			if !almostEqual(result.(float64), tt.expected.(float64)) {
				t.Errorf("computePercentileDisc() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeMode(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single value",
			values:   []any{5.0},
			expected: 5.0,
		},
		{
			name:     "clear mode",
			values:   []any{1.0, 2.0, 2.0, 3.0, 2.0},
			expected: 2.0,
		},
		{
			name:     "multiple modes - smallest wins",
			values:   []any{1.0, 1.0, 2.0, 2.0, 3.0},
			expected: 1.0,
		},
		{
			name:     "with nulls",
			values:   []any{1.0, nil, 2.0, 2.0, nil, 3.0},
			expected: 2.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeMode(tt.values)
			if err != nil {
				t.Errorf("computeMode() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeMode() = %v, want nil", result)
				}
				return
			}
			if !almostEqual(result.(float64), tt.expected.(float64)) {
				t.Errorf("computeMode() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeEntropy(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single value",
			values:   []any{5.0},
			expected: 0.0,
		},
		{
			name:     "all same",
			values:   []any{1.0, 1.0, 1.0, 1.0},
			expected: 0.0,
		},
		{
			name:     "two equal values - binary entropy",
			values:   []any{0.0, 1.0},
			expected: 1.0, // log2(2) = 1
		},
		{
			name:     "four equal categories",
			values:   []any{1.0, 2.0, 3.0, 4.0},
			expected: 2.0, // log2(4) = 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeEntropy(tt.values)
			if err != nil {
				t.Errorf("computeEntropy() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeEntropy() = %v, want nil", result)
				}
				return
			}
			if !almostEqual(result.(float64), tt.expected.(float64)) {
				t.Errorf("computeEntropy() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeSkewness(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "less than 3 values",
			values:   []any{1.0, 2.0},
			expected: nil,
		},
		{
			name:     "symmetric distribution",
			values:   []any{1.0, 2.0, 3.0, 4.0, 5.0},
			expected: 0.0,
		},
		{
			name:     "all same values",
			values:   []any{5.0, 5.0, 5.0, 5.0},
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeSkewness(tt.values)
			if err != nil {
				t.Errorf("computeSkewness() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeSkewness() = %v, want nil", result)
				}
				return
			}
			if !almostEqual(result.(float64), tt.expected.(float64)) {
				t.Errorf("computeSkewness() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeKurtosis(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "less than 4 values",
			values:   []any{1.0, 2.0, 3.0},
			expected: nil,
		},
		{
			name:     "all same values",
			values:   []any{5.0, 5.0, 5.0, 5.0, 5.0},
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeKurtosis(tt.values)
			if err != nil {
				t.Errorf("computeKurtosis() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeKurtosis() = %v, want nil", result)
				}
				return
			}
			if !almostEqual(result.(float64), tt.expected.(float64)) {
				t.Errorf("computeKurtosis() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeVarPop(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single value",
			values:   []any{5.0},
			expected: 0.0,
		},
		{
			name:     "known variance",
			values:   []any{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0},
			expected: 4.0,
		},
		{
			name:     "all same values",
			values:   []any{5.0, 5.0, 5.0, 5.0},
			expected: 0.0,
		},
		{
			name:     "with nulls",
			values:   []any{1.0, nil, 3.0},
			expected: 1.0, // (1-2)^2 + (3-2)^2 = 2/2 = 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeVarPop(tt.values)
			if err != nil {
				t.Errorf("computeVarPop() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeVarPop() = %v, want nil", result)
				}
				return
			}
			if !almostEqual(result.(float64), tt.expected.(float64)) {
				t.Errorf("computeVarPop() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeVarSamp(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single value",
			values:   []any{5.0},
			expected: nil,
		},
		{
			name:     "two values",
			values:   []any{1.0, 3.0},
			expected: 2.0, // (1-2)^2 + (3-2)^2 = 2/1 = 2
		},
		{
			name:     "all same values",
			values:   []any{5.0, 5.0, 5.0, 5.0},
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeVarSamp(tt.values)
			if err != nil {
				t.Errorf("computeVarSamp() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeVarSamp() = %v, want nil", result)
				}
				return
			}
			if !almostEqual(result.(float64), tt.expected.(float64)) {
				t.Errorf("computeVarSamp() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeStddevPop(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single value",
			values:   []any{5.0},
			expected: 0.0,
		},
		{
			name:     "known stddev",
			values:   []any{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0},
			expected: 2.0, // sqrt(4) = 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeStddevPop(tt.values)
			if err != nil {
				t.Errorf("computeStddevPop() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeStddevPop() = %v, want nil", result)
				}
				return
			}
			if !almostEqual(result.(float64), tt.expected.(float64)) {
				t.Errorf("computeStddevPop() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeStddevSamp(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single value",
			values:   []any{5.0},
			expected: nil,
		},
		{
			name:     "two values",
			values:   []any{1.0, 3.0},
			expected: math.Sqrt(2.0), // sqrt(2) ~ 1.414
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeStddevSamp(tt.values)
			if err != nil {
				t.Errorf("computeStddevSamp() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeStddevSamp() = %v, want nil", result)
				}
				return
			}
			if !almostEqual(result.(float64), tt.expected.(float64)) {
				t.Errorf("computeStddevSamp() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestToFloat64ForStats(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected float64
		ok       bool
	}{
		{name: "nil", value: nil, expected: 0, ok: false},
		{name: "float64", value: 3.14, expected: 3.14, ok: true},
		{name: "float32", value: float32(3.14), expected: float64(float32(3.14)), ok: true},
		{name: "int64", value: int64(42), expected: 42.0, ok: true},
		{name: "int", value: 42, expected: 42.0, ok: true},
		{name: "int32", value: int32(42), expected: 42.0, ok: true},
		{name: "int16", value: int16(42), expected: 42.0, ok: true},
		{name: "int8", value: int8(42), expected: 42.0, ok: true},
		{name: "uint64", value: uint64(42), expected: 42.0, ok: true},
		{name: "uint32", value: uint32(42), expected: 42.0, ok: true},
		{name: "uint16", value: uint16(42), expected: 42.0, ok: true},
		{name: "uint8", value: uint8(42), expected: 42.0, ok: true},
		{name: "string", value: "not a number", expected: 0, ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toFloat64ForStats(tt.value)
			if ok != tt.ok {
				t.Errorf("toFloat64ForStats() ok = %v, want %v", ok, tt.ok)
			}
			if ok && !almostEqual(result, tt.expected) {
				t.Errorf("toFloat64ForStats() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCollectNonNullFloats(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected []float64
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: []float64{},
		},
		{
			name:     "all nulls",
			values:   []any{nil, nil, nil},
			expected: []float64{},
		},
		{
			name:     "mixed values",
			values:   []any{1.0, nil, 3.0, nil, 5.0},
			expected: []float64{1.0, 3.0, 5.0},
		},
		{
			name:     "no nulls",
			values:   []any{1.0, 2.0, 3.0},
			expected: []float64{1.0, 2.0, 3.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := collectNonNullFloats(tt.values)
			if len(result) != len(tt.expected) {
				t.Errorf("collectNonNullFloats() len = %d, want %d", len(result), len(tt.expected))
				return
			}
			for i := range result {
				if !almostEqual(result[i], tt.expected[i]) {
					t.Errorf("collectNonNullFloats()[%d] = %v, want %v", i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestToFloat64Slice(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected []float64
		ok       bool
	}{
		{
			name:     "nil",
			value:    nil,
			expected: nil,
			ok:       false,
		},
		{
			name:     "[]float64",
			value:    []float64{1.0, 2.0, 3.0},
			expected: []float64{1.0, 2.0, 3.0},
			ok:       true,
		},
		{
			name:     "[]any with floats",
			value:    []any{1.0, 2.0, 3.0},
			expected: []float64{1.0, 2.0, 3.0},
			ok:       true,
		},
		{
			name:     "[]any with mixed numeric",
			value:    []any{int64(1), 2.0, float32(3.0)},
			expected: []float64{1.0, 2.0, 3.0},
			ok:       true,
		},
		{
			name:     "[]int64",
			value:    []int64{1, 2, 3},
			expected: []float64{1.0, 2.0, 3.0},
			ok:       true,
		},
		{
			name:     "[]int",
			value:    []int{1, 2, 3},
			expected: []float64{1.0, 2.0, 3.0},
			ok:       true,
		},
		{
			name:     "[]int32",
			value:    []int32{1, 2, 3},
			expected: []float64{1.0, 2.0, 3.0},
			ok:       true,
		},
		{
			name:     "[]float32",
			value:    []float32{1.0, 2.0, 3.0},
			expected: []float64{1.0, 2.0, 3.0},
			ok:       true,
		},
		{
			name:     "empty []any",
			value:    []any{},
			expected: []float64{},
			ok:       true,
		},
		{
			name:     "single value (not a slice)",
			value:    42.0,
			expected: nil,
			ok:       false,
		},
		{
			name:     "string (not a slice)",
			value:    "hello",
			expected: nil,
			ok:       false,
		},
		{
			name:     "[]string (wrong element type)",
			value:    []string{"a", "b", "c"},
			expected: nil,
			ok:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toFloat64Slice(tt.value)
			if ok != tt.ok {
				t.Errorf("toFloat64Slice() ok = %v, want %v", ok, tt.ok)
				return
			}
			if !ok {
				return
			}
			if len(result) != len(tt.expected) {
				t.Errorf("toFloat64Slice() len = %d, want %d", len(result), len(tt.expected))
				return
			}
			for i := range result {
				if !almostEqual(result[i], tt.expected[i]) {
					t.Errorf("toFloat64Slice()[%d] = %v, want %v", i, result[i], tt.expected[i])
				}
			}
		})
	}
}

// TestNewVarianceState tests the constructor for VarianceState.
func TestNewVarianceState(t *testing.T) {
	vs := NewVarianceState()
	if vs == nil {
		t.Fatal("NewVarianceState() returned nil")
	}
	if vs.count != 0 {
		t.Errorf("NewVarianceState().count = %d, want 0", vs.count)
	}
	if vs.mean != 0.0 {
		t.Errorf("NewVarianceState().mean = %v, want 0.0", vs.mean)
	}
	if vs.m2 != 0.0 {
		t.Errorf("NewVarianceState().m2 = %v, want 0.0", vs.m2)
	}
}

// TestVarianceStateUpdate tests Welford's algorithm update method.
func TestVarianceStateUpdate(t *testing.T) {
	tests := []struct {
		name            string
		values          []float64
		expectedCount   int64
		expectedMean    float64
		expectedVarPop  float64
		expectedVarSamp float64
	}{
		{
			name:            "single value",
			values:          []float64{5.0},
			expectedCount:   1,
			expectedMean:    5.0,
			expectedVarPop:  0.0,
			expectedVarSamp: 0.0, // N<2, returns 0
		},
		{
			name:            "two values",
			values:          []float64{1.0, 3.0},
			expectedCount:   2,
			expectedMean:    2.0,
			expectedVarPop:  1.0,  // ((1-2)^2 + (3-2)^2) / 2 = 2/2 = 1
			expectedVarSamp: 2.0, // ((1-2)^2 + (3-2)^2) / 1 = 2/1 = 2
		},
		{
			name:            "known dataset - population variance 4",
			values:          []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0},
			expectedCount:   8,
			expectedMean:    5.0,
			expectedVarPop:  4.0,
			expectedVarSamp: 4.571428571428571, // 32/7
		},
		{
			name:            "identical values",
			values:          []float64{5.0, 5.0, 5.0, 5.0},
			expectedCount:   4,
			expectedMean:    5.0,
			expectedVarPop:  0.0,
			expectedVarSamp: 0.0,
		},
		{
			name:            "sequential integers",
			values:          []float64{1.0, 2.0, 3.0, 4.0, 5.0},
			expectedCount:   5,
			expectedMean:    3.0,
			expectedVarPop:  2.0, // ((1-3)^2 + (2-3)^2 + (3-3)^2 + (4-3)^2 + (5-3)^2) / 5 = 10/5 = 2
			expectedVarSamp: 2.5, // 10/4 = 2.5
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vs := NewVarianceState()
			for _, v := range tt.values {
				vs.Update(v)
			}

			if vs.Count() != tt.expectedCount {
				t.Errorf("Count() = %d, want %d", vs.Count(), tt.expectedCount)
			}
			if !almostEqual(vs.Mean(), tt.expectedMean) {
				t.Errorf("Mean() = %v, want %v", vs.Mean(), tt.expectedMean)
			}
			if !almostEqual(vs.VariancePop(), tt.expectedVarPop) {
				t.Errorf("VariancePop() = %v, want %v", vs.VariancePop(), tt.expectedVarPop)
			}
			if !almostEqual(vs.VarianceSamp(), tt.expectedVarSamp) {
				t.Errorf("VarianceSamp() = %v, want %v", vs.VarianceSamp(), tt.expectedVarSamp)
			}
		})
	}
}

// TestVarianceStateStdDev tests the standard deviation methods.
func TestVarianceStateStdDev(t *testing.T) {
	tests := []struct {
		name              string
		values            []float64
		expectedStdDevPop float64
		expectedStdDevSamp float64
	}{
		{
			name:              "no values",
			values:            []float64{},
			expectedStdDevPop: 0.0,
			expectedStdDevSamp: 0.0,
		},
		{
			name:              "single value",
			values:            []float64{5.0},
			expectedStdDevPop: 0.0,
			expectedStdDevSamp: 0.0, // N<2, returns 0
		},
		{
			name:              "two values",
			values:            []float64{1.0, 3.0},
			expectedStdDevPop: 1.0,            // sqrt(1)
			expectedStdDevSamp: math.Sqrt(2.0), // sqrt(2)
		},
		{
			name:              "known dataset - stddev pop = 2",
			values:            []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0},
			expectedStdDevPop: 2.0,
			expectedStdDevSamp: math.Sqrt(32.0 / 7.0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vs := NewVarianceState()
			for _, v := range tt.values {
				vs.Update(v)
			}

			if !almostEqual(vs.StdDevPop(), tt.expectedStdDevPop) {
				t.Errorf("StdDevPop() = %v, want %v", vs.StdDevPop(), tt.expectedStdDevPop)
			}
			if !almostEqual(vs.StdDevSamp(), tt.expectedStdDevSamp) {
				t.Errorf("StdDevSamp() = %v, want %v", vs.StdDevSamp(), tt.expectedStdDevSamp)
			}
		})
	}
}

// TestVarianceStateEmptyState tests behavior with no values added.
func TestVarianceStateEmptyState(t *testing.T) {
	vs := NewVarianceState()

	if vs.Count() != 0 {
		t.Errorf("Empty VarianceState Count() = %d, want 0", vs.Count())
	}
	if vs.Mean() != 0.0 {
		t.Errorf("Empty VarianceState Mean() = %v, want 0.0", vs.Mean())
	}
	if vs.VariancePop() != 0.0 {
		t.Errorf("Empty VarianceState VariancePop() = %v, want 0.0", vs.VariancePop())
	}
	if vs.VarianceSamp() != 0.0 {
		t.Errorf("Empty VarianceState VarianceSamp() = %v, want 0.0", vs.VarianceSamp())
	}
	if vs.StdDevPop() != 0.0 {
		t.Errorf("Empty VarianceState StdDevPop() = %v, want 0.0", vs.StdDevPop())
	}
	if vs.StdDevSamp() != 0.0 {
		t.Errorf("Empty VarianceState StdDevSamp() = %v, want 0.0", vs.StdDevSamp())
	}
}

// TestVarianceStateNumericalStability tests Welford's algorithm for numerical stability
// with values that would cause precision issues with naive two-pass algorithms.
func TestVarianceStateNumericalStability(t *testing.T) {
	// Test with large values that have small differences
	// This would cause precision issues with (sum of squares - square of sums / n)
	vs := NewVarianceState()
	baseValue := 1e9
	for i := 0; i < 100; i++ {
		vs.Update(baseValue + float64(i))
	}

	// Mean should be baseValue + 49.5
	expectedMean := baseValue + 49.5
	if !almostEqual(vs.Mean(), expectedMean) {
		t.Errorf("Numerical stability test: Mean() = %v, want %v", vs.Mean(), expectedMean)
	}

	// Population variance of 0, 1, 2, ..., 99 is (99^2 - 0) / 12 = 833.25
	// More precisely: sum((i - 49.5)^2) / 100 for i in 0..99
	expectedVarPop := 833.25
	if !almostEqual(vs.VariancePop(), expectedVarPop) {
		t.Errorf("Numerical stability test: VariancePop() = %v, want %v", vs.VariancePop(), expectedVarPop)
	}
}

// TestVarianceStateCompareWithBatchComputation compares streaming results
// with batch computation to ensure correctness.
func TestVarianceStateCompareWithBatchComputation(t *testing.T) {
	testCases := [][]float64{
		{1.0, 2.0, 3.0, 4.0, 5.0},
		{-5.0, -3.0, 0.0, 3.0, 5.0},
		{0.1, 0.2, 0.3, 0.4, 0.5},
		{100.0, 200.0, 300.0, 400.0, 500.0},
		{-1e6, 0, 1e6},
	}

	for i, values := range testCases {
		// Streaming computation
		vs := NewVarianceState()
		for _, v := range values {
			vs.Update(v)
		}

		// Batch computation for comparison
		anyValues := make([]any, len(values))
		for j, v := range values {
			anyValues[j] = v
		}

		batchVarPop, _ := computeVarPop(anyValues)
		batchVarSamp, _ := computeVarSamp(anyValues)

		if batchVarPop != nil && !almostEqual(vs.VariancePop(), batchVarPop.(float64)) {
			t.Errorf("Test case %d: VariancePop() = %v, batch computation = %v", i, vs.VariancePop(), batchVarPop)
		}

		if batchVarSamp != nil && !almostEqual(vs.VarianceSamp(), batchVarSamp.(float64)) {
			t.Errorf("Test case %d: VarianceSamp() = %v, batch computation = %v", i, vs.VarianceSamp(), batchVarSamp)
		}
	}
}
