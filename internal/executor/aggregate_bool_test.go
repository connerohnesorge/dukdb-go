// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeBoolAnd(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "all true",
			values:   []any{true, true, true},
			expected: true,
		},
		{
			name:     "one false",
			values:   []any{true, false, true},
			expected: false,
		},
		{
			name:     "all false",
			values:   []any{false, false, false},
			expected: false,
		},
		{
			name:     "with nulls - all true",
			values:   []any{true, nil, true},
			expected: true,
		},
		{
			name:     "with nulls - one false",
			values:   []any{true, nil, false},
			expected: false,
		},
		{
			name:     "all nulls",
			values:   []any{nil, nil, nil},
			expected: nil,
		},
		{
			name:     "empty",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single true",
			values:   []any{true},
			expected: true,
		},
		{
			name:     "single false",
			values:   []any{false},
			expected: false,
		},
		{
			name:     "integers as bools - all non-zero",
			values:   []any{1, 2, 3},
			expected: true,
		},
		{
			name:     "integers as bools - one zero",
			values:   []any{1, 0, 3},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeBoolAnd(tt.values)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeBoolOr(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "all true",
			values:   []any{true, true, true},
			expected: true,
		},
		{
			name:     "one true",
			values:   []any{false, true, false},
			expected: true,
		},
		{
			name:     "all false",
			values:   []any{false, false, false},
			expected: false,
		},
		{
			name:     "with nulls - one true",
			values:   []any{false, nil, true},
			expected: true,
		},
		{
			name:     "with nulls - all false",
			values:   []any{false, nil, false},
			expected: false,
		},
		{
			name:     "all nulls",
			values:   []any{nil, nil, nil},
			expected: nil,
		},
		{
			name:     "empty",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single true",
			values:   []any{true},
			expected: true,
		},
		{
			name:     "single false",
			values:   []any{false},
			expected: false,
		},
		{
			name:     "integers as bools - one non-zero",
			values:   []any{0, 5, 0},
			expected: true,
		},
		{
			name:     "integers as bools - all zero",
			values:   []any{0, 0, 0},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeBoolOr(tt.values)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeBitAnd(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "all same",
			values:   []any{int64(7), int64(7), int64(7)}, // 0111 & 0111 & 0111 = 0111
			expected: int64(7),
		},
		{
			name:     "different values",
			values:   []any{int64(7), int64(3), int64(1)}, // 0111 & 0011 & 0001 = 0001
			expected: int64(1),
		},
		{
			name:     "with zero",
			values:   []any{int64(7), int64(0), int64(3)}, // any & 0 = 0
			expected: int64(0),
		},
		{
			name:     "with nulls",
			values:   []any{int64(7), nil, int64(3)}, // 0111 & 0011 = 0011
			expected: int64(3),
		},
		{
			name:     "all nulls",
			values:   []any{nil, nil, nil},
			expected: nil,
		},
		{
			name:     "empty",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single value",
			values:   []any{int64(42)},
			expected: int64(42),
		},
		{
			name:     "mixed int types",
			values:   []any{int(7), int32(3), int64(1)}, // all should be converted
			expected: int64(1),
		},
		{
			name:     "0xFF and 0x0F",
			values:   []any{int64(255), int64(15)}, // 11111111 & 00001111 = 00001111
			expected: int64(15),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeBitAnd(tt.values)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeBitOr(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "all same",
			values:   []any{int64(7), int64(7), int64(7)}, // 0111 | 0111 | 0111 = 0111
			expected: int64(7),
		},
		{
			name:     "different values",
			values:   []any{int64(1), int64(2), int64(4)}, // 001 | 010 | 100 = 111
			expected: int64(7),
		},
		{
			name:     "with zero",
			values:   []any{int64(7), int64(0), int64(3)}, // 0111 | 0 | 0011 = 0111
			expected: int64(7),
		},
		{
			name:     "with nulls",
			values:   []any{int64(1), nil, int64(2)}, // 001 | 010 = 011
			expected: int64(3),
		},
		{
			name:     "all nulls",
			values:   []any{nil, nil, nil},
			expected: nil,
		},
		{
			name:     "empty",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single value",
			values:   []any{int64(42)},
			expected: int64(42),
		},
		{
			name:     "mixed int types",
			values:   []any{int(1), int32(2), int64(4)},
			expected: int64(7),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeBitOr(tt.values)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeBitXor(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{
			name:     "same values cancel out",
			values:   []any{int64(7), int64(7)}, // 0111 ^ 0111 = 0000
			expected: int64(0),
		},
		{
			name:     "different values",
			values:   []any{int64(1), int64(2), int64(4)}, // 001 ^ 010 ^ 100 = 111
			expected: int64(7),
		},
		{
			name:     "triple same value",
			values:   []any{int64(5), int64(5), int64(5)}, // 5 ^ 5 ^ 5 = 5
			expected: int64(5),
		},
		{
			name:     "with nulls",
			values:   []any{int64(5), nil, int64(3)}, // 101 ^ 011 = 110
			expected: int64(6),
		},
		{
			name:     "all nulls",
			values:   []any{nil, nil, nil},
			expected: nil,
		},
		{
			name:     "empty",
			values:   []any{},
			expected: nil,
		},
		{
			name:     "single value",
			values:   []any{int64(42)},
			expected: int64(42),
		},
		{
			name:     "xor with zero",
			values:   []any{int64(42), int64(0)}, // 42 ^ 0 = 42
			expected: int64(42),
		},
		{
			name:     "mixed int types",
			values:   []any{int(1), int32(2), int64(4)},
			expected: int64(7),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeBitXor(tt.values)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToBoolForAggregate(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected bool
		ok       bool
	}{
		{name: "bool true", value: true, expected: true, ok: true},
		{name: "bool false", value: false, expected: false, ok: true},
		{name: "int non-zero", value: 42, expected: true, ok: true},
		{name: "int zero", value: 0, expected: false, ok: true},
		{name: "int64 non-zero", value: int64(42), expected: true, ok: true},
		{name: "int64 zero", value: int64(0), expected: false, ok: true},
		{name: "float64 non-zero", value: float64(3.14), expected: true, ok: true},
		{name: "float64 zero", value: float64(0), expected: false, ok: true},
		{name: "nil", value: nil, expected: false, ok: false},
		{name: "string", value: "true", expected: false, ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toBoolForAggregate(tt.value)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.ok, ok)
		})
	}
}

func TestToInt64ForBitwise(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected int64
		ok       bool
	}{
		{name: "int", value: 42, expected: 42, ok: true},
		{name: "int8", value: int8(42), expected: 42, ok: true},
		{name: "int16", value: int16(42), expected: 42, ok: true},
		{name: "int32", value: int32(42), expected: 42, ok: true},
		{name: "int64", value: int64(42), expected: 42, ok: true},
		{name: "uint", value: uint(42), expected: 42, ok: true},
		{name: "uint8", value: uint8(42), expected: 42, ok: true},
		{name: "uint16", value: uint16(42), expected: 42, ok: true},
		{name: "uint32", value: uint32(42), expected: 42, ok: true},
		{name: "uint64", value: uint64(42), expected: 42, ok: true},
		{name: "nil", value: nil, expected: 0, ok: false},
		{name: "float64", value: float64(42.0), expected: 0, ok: false},
		{name: "string", value: "42", expected: 0, ok: false},
		{name: "bool", value: true, expected: 0, ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toInt64ForBitwise(tt.value)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.ok, ok)
		})
	}
}
