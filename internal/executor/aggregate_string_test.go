package executor

import (
	"reflect"
	"testing"
)

func TestComputeStringAgg(t *testing.T) {
	tests := []struct {
		name      string
		values    []any
		delimiter string
		expected  any
	}{
		{
			name:      "empty input",
			values:    []any{},
			delimiter: ",",
			expected:  nil,
		},
		{
			name:      "single value",
			values:    []any{"hello"},
			delimiter: ",",
			expected:  "hello",
		},
		{
			name:      "multiple values with comma",
			values:    []any{"apple", "banana", "cherry"},
			delimiter: ", ",
			expected:  "apple, banana, cherry",
		},
		{
			name:      "multiple values with custom delimiter",
			values:    []any{"a", "b", "c"},
			delimiter: " | ",
			expected:  "a | b | c",
		},
		{
			name:      "with null values",
			values:    []any{"hello", nil, "world"},
			delimiter: " ",
			expected:  "hello world",
		},
		{
			name:      "all null values",
			values:    []any{nil, nil, nil},
			delimiter: ",",
			expected:  nil,
		},
		{
			name:      "empty delimiter",
			values:    []any{"a", "b", "c"},
			delimiter: "",
			expected:  "abc",
		},
		{
			name:      "numeric values converted to string",
			values:    []any{int64(1), int64(2), int64(3)},
			delimiter: "-",
			expected:  "1-2-3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeStringAgg(tt.values, tt.delimiter)
			if err != nil {
				t.Errorf("computeStringAgg() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeStringAgg() = %v, want nil", result)
				}
				return
			}
			if result != tt.expected {
				t.Errorf("computeStringAgg() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeGroupConcat(t *testing.T) {
	tests := []struct {
		name      string
		values    []any
		delimiter string
		expected  any
	}{
		{
			name:      "empty input",
			values:    []any{},
			delimiter: ",",
			expected:  nil,
		},
		{
			name:      "single value",
			values:    []any{"hello"},
			delimiter: ",",
			expected:  "hello",
		},
		{
			name:      "multiple values",
			values:    []any{"apple", "banana", "cherry"},
			delimiter: ",",
			expected:  "apple,banana,cherry",
		},
		{
			name:      "with null values",
			values:    []any{"hello", nil, "world"},
			delimiter: ",",
			expected:  "hello,world",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeGroupConcat(tt.values, tt.delimiter)
			if err != nil {
				t.Errorf("computeGroupConcat() error = %v", err)
				return
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("computeGroupConcat() = %v, want nil", result)
				}
				return
			}
			if result != tt.expected {
				t.Errorf("computeGroupConcat() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeList(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected []any
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: []any{},
		},
		{
			name:     "single value",
			values:   []any{"hello"},
			expected: []any{"hello"},
		},
		{
			name:     "multiple values",
			values:   []any{"apple", "banana", "cherry"},
			expected: []any{"apple", "banana", "cherry"},
		},
		{
			name:     "with null values",
			values:   []any{"hello", nil, "world"},
			expected: []any{"hello", "world"},
		},
		{
			name:     "all null values",
			values:   []any{nil, nil, nil},
			expected: []any{},
		},
		{
			name:     "duplicate values preserved",
			values:   []any{"a", "b", "a", "c", "b"},
			expected: []any{"a", "b", "a", "c", "b"},
		},
		{
			name:     "numeric values",
			values:   []any{int64(1), int64(2), int64(3)},
			expected: []any{int64(1), int64(2), int64(3)},
		},
		{
			name:     "mixed types",
			values:   []any{"hello", int64(42), 3.14},
			expected: []any{"hello", int64(42), 3.14},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeList(tt.values)
			if err != nil {
				t.Errorf("computeList() error = %v", err)
				return
			}
			resultSlice, ok := result.([]any)
			if !ok {
				t.Errorf("computeList() returned non-slice type: %T", result)
				return
			}
			if !reflect.DeepEqual(resultSlice, tt.expected) {
				t.Errorf("computeList() = %v, want %v", resultSlice, tt.expected)
			}
		})
	}
}

func TestComputeListDistinct(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected []any
	}{
		{
			name:     "empty input",
			values:   []any{},
			expected: []any{},
		},
		{
			name:     "single value",
			values:   []any{"hello"},
			expected: []any{"hello"},
		},
		{
			name:     "no duplicates",
			values:   []any{"apple", "banana", "cherry"},
			expected: []any{"apple", "banana", "cherry"},
		},
		{
			name:     "with duplicates",
			values:   []any{"a", "b", "a", "c", "b"},
			expected: []any{"a", "b", "c"},
		},
		{
			name:     "with null values",
			values:   []any{"hello", nil, "world", nil},
			expected: []any{"hello", "world"},
		},
		{
			name:     "all null values",
			values:   []any{nil, nil, nil},
			expected: []any{},
		},
		{
			name:     "all same value",
			values:   []any{"a", "a", "a"},
			expected: []any{"a"},
		},
		{
			name:     "numeric duplicates",
			values:   []any{int64(1), int64(2), int64(1), int64(3), int64(2)},
			expected: []any{int64(1), int64(2), int64(3)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeListDistinct(tt.values)
			if err != nil {
				t.Errorf("computeListDistinct() error = %v", err)
				return
			}
			resultSlice, ok := result.([]any)
			if !ok {
				t.Errorf("computeListDistinct() returned non-slice type: %T", result)
				return
			}
			if len(resultSlice) != len(tt.expected) {
				t.Errorf("computeListDistinct() returned %d items, want %d", len(resultSlice), len(tt.expected))
				return
			}
			// Check all expected items are present (order may vary due to map iteration)
			for _, exp := range tt.expected {
				found := false
				for _, res := range resultSlice {
					if formatValue(res) == formatValue(exp) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("computeListDistinct() missing expected value %v", exp)
				}
			}
		})
	}
}
