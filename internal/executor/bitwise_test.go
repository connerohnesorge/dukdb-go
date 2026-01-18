package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBitwiseAndValue tests the bitwiseAndValue function.
func TestBitwiseAndValue(t *testing.T) {
	tests := []struct {
		name     string
		left     any
		right    any
		expected any
		hasError bool
	}{
		// Basic cases
		{"12 & 10", int64(12), int64(10), int64(8), false},
		{"0xFF & 0x0F", int64(0xFF), int64(0x0F), int64(0x0F), false},
		{"255 & 128", int64(255), int64(128), int64(128), false},
		{"7 & 3", int64(7), int64(3), int64(3), false},

		// Edge cases
		{"0 & x", int64(0), int64(12345), int64(0), false},
		{"x & 0", int64(12345), int64(0), int64(0), false},
		{"-1 & x", int64(-1), int64(0xFF), int64(0xFF), false},
		{"x & -1", int64(0xFF), int64(-1), int64(0xFF), false},

		// NULL handling
		{"NULL & 5", nil, int64(5), nil, false},
		{"5 & NULL", int64(5), nil, nil, false},
		{"NULL & NULL", nil, nil, nil, false},

		// Type conversions
		{"int32 & int64", int32(12), int64(10), int64(8), false},
		{"float64 & int", 12.0, int64(10), int64(8), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseAndValue(tt.left, tt.right)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestBitwiseOrValue tests the bitwiseOrValue function.
func TestBitwiseOrValue(t *testing.T) {
	tests := []struct {
		name     string
		left     any
		right    any
		expected any
		hasError bool
	}{
		// Basic cases
		{"12 | 10", int64(12), int64(10), int64(14), false},
		{"0xF0 | 0x0F", int64(0xF0), int64(0x0F), int64(0xFF), false},
		{"4 | 2", int64(4), int64(2), int64(6), false},

		// Edge cases
		{"0 | x", int64(0), int64(12345), int64(12345), false},
		{"x | 0", int64(12345), int64(0), int64(12345), false},
		{"-1 | x", int64(-1), int64(0xFF), int64(-1), false},

		// NULL handling
		{"NULL | 5", nil, int64(5), nil, false},
		{"5 | NULL", int64(5), nil, nil, false},
		{"NULL | NULL", nil, nil, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseOrValue(tt.left, tt.right)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestBitwiseXorValue tests the bitwiseXorValue function.
func TestBitwiseXorValue(t *testing.T) {
	tests := []struct {
		name     string
		left     any
		right    any
		expected any
		hasError bool
	}{
		// Basic cases
		{"12 ^ 10", int64(12), int64(10), int64(6), false},
		{"0xFF ^ 0x0F", int64(0xFF), int64(0x0F), int64(0xF0), false},
		{"5 ^ 3", int64(5), int64(3), int64(6), false},

		// Edge cases
		{"x ^ x", int64(12345), int64(12345), int64(0), false},
		{"x ^ 0", int64(12345), int64(0), int64(12345), false},
		{"0 ^ x", int64(0), int64(12345), int64(12345), false},

		// NULL handling
		{"NULL ^ 5", nil, int64(5), nil, false},
		{"5 ^ NULL", int64(5), nil, nil, false},
		{"NULL ^ NULL", nil, nil, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseXorValue(tt.left, tt.right)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestBitwiseNotValue tests the bitwiseNotValue function.
func TestBitwiseNotValue(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected any
		hasError bool
	}{
		// Basic cases
		{"~0", int64(0), int64(-1), false},
		{"~1", int64(1), int64(-2), false},
		{"~-1", int64(-1), int64(0), false},
		{"~255", int64(255), int64(-256), false},

		// NULL handling
		{"~NULL", nil, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseNotValue(tt.value)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestBitwiseShiftLeftValue tests the bitwiseShiftLeftValue function.
func TestBitwiseShiftLeftValue(t *testing.T) {
	tests := []struct {
		name     string
		left     any
		right    any
		expected any
		hasError bool
	}{
		// Basic cases
		{"1 << 4", int64(1), int64(4), int64(16), false},
		{"5 << 2", int64(5), int64(2), int64(20), false},
		{"0xFF << 8", int64(0xFF), int64(8), int64(0xFF00), false},

		// Edge cases
		{"x << 0", int64(12345), int64(0), int64(12345), false},
		{"0 << x", int64(0), int64(10), int64(0), false},
		{"x << 64", int64(1), int64(64), int64(0), false},
		{"x << 100", int64(1), int64(100), int64(0), false},

		// NULL handling
		{"NULL << 5", nil, int64(5), nil, false},
		{"5 << NULL", int64(5), nil, nil, false},
		{"NULL << NULL", nil, nil, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseShiftLeftValue(tt.left, tt.right)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestBitwiseShiftRightValue tests the bitwiseShiftRightValue function.
func TestBitwiseShiftRightValue(t *testing.T) {
	tests := []struct {
		name     string
		left     any
		right    any
		expected any
		hasError bool
	}{
		// Basic cases
		{"16 >> 4", int64(16), int64(4), int64(1), false},
		{"20 >> 2", int64(20), int64(2), int64(5), false},
		{"0xFF00 >> 8", int64(0xFF00), int64(8), int64(0xFF), false},

		// Edge cases
		{"x >> 0", int64(12345), int64(0), int64(12345), false},
		{"0 >> x", int64(0), int64(10), int64(0), false},
		{"positive >> 64", int64(1), int64(64), int64(0), false},
		{"negative >> 64", int64(-1), int64(64), int64(-1), false},
		{"positive >> 100", int64(12345), int64(100), int64(0), false},
		{"negative >> 100", int64(-12345), int64(100), int64(-1), false},

		// Arithmetic shift preserves sign
		{"-8 >> 1", int64(-8), int64(1), int64(-4), false},
		{"-16 >> 2", int64(-16), int64(2), int64(-4), false},

		// NULL handling
		{"NULL >> 5", nil, int64(5), nil, false},
		{"5 >> NULL", int64(5), nil, nil, false},
		{"NULL >> NULL", nil, nil, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseShiftRightValue(tt.left, tt.right)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestBitCountValue tests the bitCountValue function.
func TestBitCountValue(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected any
		hasError bool
	}{
		// Basic cases
		{"BIT_COUNT(0)", int64(0), int64(0), false},
		{"BIT_COUNT(1)", int64(1), int64(1), false},
		{"BIT_COUNT(7)", int64(7), int64(3), false},     // 111 in binary
		{"BIT_COUNT(255)", int64(255), int64(8), false}, // 11111111 in binary
		{"BIT_COUNT(256)", int64(256), int64(1), false}, // 100000000 in binary
		{"BIT_COUNT(15)", int64(15), int64(4), false},   // 1111 in binary

		// Negative numbers (two's complement)
		{"BIT_COUNT(-1)", int64(-1), int64(64), false}, // All bits set
		{"BIT_COUNT(-2)", int64(-2), int64(63), false}, // All bits except last

		// NULL handling
		{"BIT_COUNT(NULL)", nil, nil, false},

		// Type conversions
		{"BIT_COUNT(int32)", int32(7), int64(3), false},
		{"BIT_COUNT(float)", 7.0, int64(3), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitCountValue(tt.value)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestBitwiseOperatorEdgeCases tests various edge cases for bitwise operators.
func TestBitwiseOperatorEdgeCases(t *testing.T) {
	t.Run("Combined operations", func(t *testing.T) {
		// (5 & 3) | 8 = 1 | 8 = 9
		step1, err := bitwiseAndValue(int64(5), int64(3))
		require.NoError(t, err)
		step2, err := bitwiseOrValue(step1, int64(8))
		require.NoError(t, err)
		assert.Equal(t, int64(9), step2)
	})

	t.Run("NOT and AND identity", func(t *testing.T) {
		// x & ~x = 0
		x := int64(12345)
		notX, err := bitwiseNotValue(x)
		require.NoError(t, err)
		result, err := bitwiseAndValue(x, notX)
		require.NoError(t, err)
		assert.Equal(t, int64(0), result)
	})

	t.Run("XOR self identity", func(t *testing.T) {
		// x ^ x = 0
		x := int64(12345)
		result, err := bitwiseXorValue(x, x)
		require.NoError(t, err)
		assert.Equal(t, int64(0), result)
	})

	t.Run("Shift and unshift", func(t *testing.T) {
		// (x << n) >> n = x (for small n and x fitting in remaining bits)
		x := int64(5)
		shifted, err := bitwiseShiftLeftValue(x, int64(4))
		require.NoError(t, err)
		unshifted, err := bitwiseShiftRightValue(shifted, int64(4))
		require.NoError(t, err)
		assert.Equal(t, x, unshifted)
	})

	t.Run("Large values", func(t *testing.T) {
		// Test with large int64 values
		large := int64(0x7FFFFFFFFFFFFFFF) // Max int64
		result, err := bitwiseAndValue(large, int64(0xFF))
		require.NoError(t, err)
		assert.Equal(t, int64(0xFF), result)
	})
}
