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
