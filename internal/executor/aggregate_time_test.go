// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeCountIf(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected int64
	}{
		{
			name:     "all true",
			values:   []any{true, true, true},
			expected: 3,
		},
		{
			name:     "all false",
			values:   []any{false, false, false},
			expected: 0,
		},
		{
			name:     "mixed",
			values:   []any{true, false, true, false, true},
			expected: 3,
		},
		{
			name:     "with nulls",
			values:   []any{true, nil, true, nil, false},
			expected: 2,
		},
		{
			name:     "all nulls",
			values:   []any{nil, nil, nil},
			expected: 0,
		},
		{
			name:     "empty input",
			values:   []any{},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeCountIf(tt.values)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeFirst(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "all non-null",
			values:   []any{1, 2, 3},
			expected: 1,
		},
		{
			name:     "first is null",
			values:   []any{nil, 2, 3},
			expected: 2,
		},
		{
			name:     "first two are null",
			values:   []any{nil, nil, 3},
			expected: 3,
		},
		{
			name:     "all nulls",
			values:   []any{nil, nil, nil},
			expected: nil,
		},
		{
			name:     "empty input",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "string values",
			values:   []any{"a", "b", "c"},
			expected: "a",
		},
		{
			name:     "float values",
			values:   []any{1.5, 2.5, 3.5},
			expected: 1.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeFirst(tt.values)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeLast(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "all non-null",
			values:   []any{1, 2, 3},
			expected: 3,
		},
		{
			name:     "last is null",
			values:   []any{1, 2, nil},
			expected: 2,
		},
		{
			name:     "last two are null",
			values:   []any{1, nil, nil},
			expected: 1,
		},
		{
			name:     "all nulls",
			values:   []any{nil, nil, nil},
			expected: nil,
		},
		{
			name:     "empty input",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "string values",
			values:   []any{"a", "b", "c"},
			expected: "c",
		},
		{
			name:     "float values",
			values:   []any{1.5, 2.5, 3.5},
			expected: 3.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeLast(tt.values)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeArgmin(t *testing.T) {
	tests := []struct {
		name      string
		argValues []any
		valValues []any
		expected  any
	}{
		{
			name:      "basic integers",
			argValues: []any{"a", "b", "c"},
			valValues: []any{3, 1, 2},
			expected:  "b",
		},
		{
			name:      "with null arg",
			argValues: []any{"a", nil, "c"},
			valValues: []any{3, 1, 2},
			expected:  "c",
		},
		{
			name:      "with null val",
			argValues: []any{"a", "b", "c"},
			valValues: []any{3, nil, 2},
			expected:  "c",
		},
		{
			name:      "with both null",
			argValues: []any{"a", nil, "c"},
			valValues: []any{3, nil, 2},
			expected:  "c",
		},
		{
			name:      "all nulls",
			argValues: []any{nil, nil, nil},
			valValues: []any{nil, nil, nil},
			expected:  nil,
		},
		{
			name:      "empty input",
			argValues: []any{},
			valValues: []any{},
			expected:  nil,
		},
		{
			name:      "single element",
			argValues: []any{"only"},
			valValues: []any{42},
			expected:  "only",
		},
		{
			name:      "float values",
			argValues: []any{"a", "b", "c"},
			valValues: []any{3.5, 1.5, 2.5},
			expected:  "b",
		},
		{
			name:      "string values",
			argValues: []any{100, 200, 300},
			valValues: []any{"c", "a", "b"},
			expected:  200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeArgmin(tt.argValues, tt.valValues)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeArgmax(t *testing.T) {
	tests := []struct {
		name      string
		argValues []any
		valValues []any
		expected  any
	}{
		{
			name:      "basic integers",
			argValues: []any{"a", "b", "c"},
			valValues: []any{3, 1, 2},
			expected:  "a",
		},
		{
			name:      "with null arg",
			argValues: []any{nil, "b", "c"},
			valValues: []any{3, 1, 2},
			expected:  "c",
		},
		{
			name:      "with null val",
			argValues: []any{"a", "b", "c"},
			valValues: []any{nil, 1, 2},
			expected:  "c",
		},
		{
			name:      "with both null",
			argValues: []any{nil, "b", "c"},
			valValues: []any{nil, 1, 2},
			expected:  "c",
		},
		{
			name:      "all nulls",
			argValues: []any{nil, nil, nil},
			valValues: []any{nil, nil, nil},
			expected:  nil,
		},
		{
			name:      "empty input",
			argValues: []any{},
			valValues: []any{},
			expected:  nil,
		},
		{
			name:      "single element",
			argValues: []any{"only"},
			valValues: []any{42},
			expected:  "only",
		},
		{
			name:      "float values",
			argValues: []any{"a", "b", "c"},
			valValues: []any{3.5, 1.5, 2.5},
			expected:  "a",
		},
		{
			name:      "string values",
			argValues: []any{100, 200, 300},
			valValues: []any{"c", "a", "b"},
			expected:  100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeArgmax(tt.argValues, tt.valValues)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeArgminArgmaxMismatchedLengths(t *testing.T) {
	// Test with mismatched slice lengths
	argValues := []any{"a", "b", "c"}
	valValues := []any{1, 2}

	result, err := computeArgmin(argValues, valValues)
	require.NoError(t, err)
	assert.Nil(t, result)

	result, err = computeArgmax(argValues, valValues)
	require.NoError(t, err)
	assert.Nil(t, result)
}
