package executor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestToFloat64 tests the toFloat64 helper function.
func TestToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected float64
		ok       bool
	}{
		// Valid conversions
		{"float64", 3.14, 3.14, true},
		{"float32", float32(2.5), 2.5, true},
		{"int64", int64(42), 42.0, true},
		{"int", 42, 42.0, true},
		{"int32", int32(42), 42.0, true},
		{"int16", int16(42), 42.0, true},
		{"int8", int8(42), 42.0, true},
		{"uint64", uint64(42), 42.0, true},
		{"uint32", uint32(42), 42.0, true},
		{"uint16", uint16(42), 42.0, true},
		{"uint8", uint8(42), 42.0, true},

		// Invalid conversions
		{"nil", nil, 0, false},
		{"string", "42", 0, false},
		{"bool", true, 0, false},
		{"slice", []int{1, 2}, 0, false},

		// Edge cases
		{"negative int", -42, -42.0, true},
		{"zero", 0, 0.0, true},
		{"max int64", int64(math.MaxInt64), float64(math.MaxInt64), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toFloat64(tt.input)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestToInt64 tests the toInt64 helper function.
func TestToInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected int64
		ok       bool
	}{
		// Valid conversions
		{"int64", int64(42), 42, true},
		{"int", 42, 42, true},
		{"int32", int32(42), 42, true},
		{"int16", int16(42), 42, true},
		{"int8", int8(42), 42, true},
		{"uint64", uint64(42), 42, true},
		{"uint32", uint32(42), 42, true},
		{"uint16", uint16(42), 42, true},
		{"uint8", uint8(42), 42, true},
		{"float64", 3.7, 3, true},
		{"float32", float32(2.5), 2, true},

		// Invalid conversions
		{"nil", nil, 0, false},
		{"string", "42", 0, false},
		{"bool", true, 0, false},

		// Edge cases
		{"negative", -42, -42, true},
		{"zero", 0, 0, true},
		{"float truncation", 3.99, 3, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toInt64(tt.input)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestToUint64 tests the toUint64 helper function.
func TestToUint64(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected uint64
		ok       bool
	}{
		// Valid conversions
		{"uint64", uint64(42), 42, true},
		{"uint32", uint32(42), 42, true},
		{"uint16", uint16(42), 42, true},
		{"uint8", uint8(42), 42, true},
		{"int64", int64(42), 42, true},
		{"int", 42, 42, true},
		{"int32", int32(42), 42, true},
		{"int16", int16(42), 42, true},
		{"int8", int8(42), 42, true},
		{"float64", 3.7, 3, true},
		{"float32", float32(2.5), 2, true},

		// Invalid conversions
		{"nil", nil, 0, false},
		{"string", "42", 0, false},
		{"bool", true, 0, false},

		// Edge cases
		{"zero", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toUint64(tt.input)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestRoundingFunctions tests the rounding math functions.
func TestRoundingFunctions(t *testing.T) {
	t.Run("roundValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			decimals any
			expected any
		}{
			{"basic round", 3.7, int64(0), 4.0},
			{"round down", 3.2, int64(0), 3.0},
			{"round half up", 2.5, int64(0), 3.0},
			{"negative round", -2.5, int64(0), -3.0},
			{"with decimals", 3.14159, int64(2), 3.14},
			{"with decimals 3", 3.14159, int64(3), 3.142},
			{"negative decimals", 12345.0, int64(-2), 12300.0},
			{"nil value", nil, int64(0), nil},
			{"nil decimals default 0", 3.7, nil, 4.0},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := roundValue(tt.value, tt.decimals)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("ceilValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected any
		}{
			{"positive", 3.1, 4.0},
			{"negative", -3.1, -3.0},
			{"integer", 5.0, 5.0},
			{"nil", nil, nil},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := ceilValue(tt.value)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("floorValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected any
		}{
			{"positive", 3.9, 3.0},
			{"negative", -3.1, -4.0},
			{"integer", 5.0, 5.0},
			{"nil", nil, nil},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := floorValue(tt.value)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("truncValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected any
		}{
			{"positive", 3.9, 3.0},
			{"negative", -3.9, -3.0},
			{"integer", 5.0, 5.0},
			{"nil", nil, nil},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := truncValue(tt.value)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("roundEvenValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			decimals any
			expected any
		}{
			{"round to even - 2.5", 2.5, int64(0), 2.0}, // Banker's rounding
			{"round to even - 3.5", 3.5, int64(0), 4.0}, // Banker's rounding
			{"round to even - 4.5", 4.5, int64(0), 4.0}, // Banker's rounding
			{"round to even - 5.5", 5.5, int64(0), 6.0}, // Banker's rounding
			{"with decimals", 3.145, int64(2), 3.14},    // Banker's rounding
			{"nil", nil, int64(0), nil},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := roundEvenValue(tt.value, tt.decimals)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("evenValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected any
		}{
			{"even input", 4.0, 4.0},
			{"odd input", 3.0, 4.0},
			{"round to even up", 2.5, 4.0},   // Rounds to 3, which is odd, so +1 = 4
			{"round to even down", 1.5, 2.0}, // Rounds to 2, which is even
			{"negative even", -4.0, -4.0},
			{"negative odd", -3.0, -4.0},
			{"nil", nil, nil},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := evenValue(tt.value)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})
}

// TestScientificFunctions tests the scientific math functions.
func TestScientificFunctions(t *testing.T) {
	t.Run("sqrtValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected any
			hasError bool
		}{
			{"positive", 4.0, 2.0, false},
			{"zero", 0.0, 0.0, false},
			{"negative", -1.0, nil, true},
			{"nil", nil, nil, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := sqrtValue(tt.value)
				if tt.hasError {
					require.Error(t, err)
					assert.Contains(t, err.Error(), "SQRT of negative number")
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})

	t.Run("cbrtValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected float64
		}{
			{"positive", 8.0, 2.0},
			{"negative", -8.0, -2.0},
			{"zero", 0.0, 0.0},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := cbrtValue(tt.value)
				require.NoError(t, err)
				assert.InDelta(t, tt.expected, result, 1e-10)
			})
		}
	})

	t.Run("powValue", func(t *testing.T) {
		tests := []struct {
			name     string
			base     any
			exp      any
			expected float64
		}{
			{"basic", 2.0, 3.0, 8.0},
			{"fractional exp", 4.0, 0.5, 2.0},
			{"zero exp", 5.0, 0.0, 1.0},
			{"negative exp", 2.0, -1.0, 0.5},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := powValue(tt.base, tt.exp)
				require.NoError(t, err)
				assert.InDelta(t, tt.expected, result, 1e-10)
			})
		}

		t.Run("nil propagation", func(t *testing.T) {
			result, err := powValue(nil, 2.0)
			require.NoError(t, err)
			assert.Nil(t, result)

			result, err = powValue(2.0, nil)
			require.NoError(t, err)
			assert.Nil(t, result)
		})
	})

	t.Run("expValue", func(t *testing.T) {
		result, err := expValue(0.0)
		require.NoError(t, err)
		assert.Equal(t, 1.0, result)

		result, err = expValue(1.0)
		require.NoError(t, err)
		assert.InDelta(t, math.E, result, 1e-10)
	})

	t.Run("lnValue", func(t *testing.T) {
		result, err := lnValue(math.E)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, result, 1e-10)

		_, err = lnValue(0.0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")

		_, err = lnValue(-1.0)
		require.Error(t, err)
	})

	t.Run("log10Value", func(t *testing.T) {
		result, err := log10Value(100.0)
		require.NoError(t, err)
		assert.InDelta(t, 2.0, result, 1e-10)

		_, err = log10Value(0.0)
		require.Error(t, err)
	})

	t.Run("log2Value", func(t *testing.T) {
		result, err := log2Value(8.0)
		require.NoError(t, err)
		assert.InDelta(t, 3.0, result, 1e-10)

		_, err = log2Value(0.0)
		require.Error(t, err)
	})

	t.Run("gammaValue", func(t *testing.T) {
		result, err := gammaValue(5.0)
		require.NoError(t, err)
		// Gamma(5) = 4! = 24
		assert.InDelta(t, 24.0, result, 1e-10)
	})

	t.Run("lgammaValue", func(t *testing.T) {
		result, err := lgammaValue(5.0)
		require.NoError(t, err)
		// lgamma(5) = ln(24) = 3.178...
		assert.InDelta(t, math.Log(24), result, 1e-10)
	})

	t.Run("factorialValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected any
			hasError bool
		}{
			{"0!", int64(0), int64(1), false},
			{"1!", int64(1), int64(1), false},
			{"5!", int64(5), int64(120), false},
			{"20!", int64(20), int64(2432902008176640000), false},
			{"negative", int64(-1), nil, true},
			{"overflow", int64(21), nil, true},
			{"nil", nil, nil, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := factorialValue(tt.value)
				if tt.hasError {
					require.Error(t, err)
					assert.Contains(t, err.Error(), "FACTORIAL")
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})
}

// TestTrigonometricFunctions tests the trigonometric math functions.
func TestTrigonometricFunctions(t *testing.T) {
	t.Run("sinValue", func(t *testing.T) {
		result, err := sinValue(0.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)

		result, err = sinValue(math.Pi / 2)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, result, 1e-10)
	})

	t.Run("cosValue", func(t *testing.T) {
		result, err := cosValue(0.0)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, result, 1e-10)

		result, err = cosValue(math.Pi)
		require.NoError(t, err)
		assert.InDelta(t, -1.0, result, 1e-10)
	})

	t.Run("tanValue", func(t *testing.T) {
		result, err := tanValue(0.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)

		result, err = tanValue(math.Pi / 4)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, result, 1e-10)
	})

	t.Run("cotValue", func(t *testing.T) {
		result, err := cotValue(math.Pi / 4)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, result, 1e-10)
	})

	t.Run("asinValue", func(t *testing.T) {
		result, err := asinValue(0.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)

		result, err = asinValue(1.0)
		require.NoError(t, err)
		assert.InDelta(t, math.Pi/2, result, 1e-10)

		_, err = asinValue(2.0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ASIN domain error")

		_, err = asinValue(-2.0)
		require.Error(t, err)
	})

	t.Run("acosValue", func(t *testing.T) {
		result, err := acosValue(1.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)

		result, err = acosValue(0.0)
		require.NoError(t, err)
		assert.InDelta(t, math.Pi/2, result, 1e-10)

		_, err = acosValue(2.0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOS domain error")
	})

	t.Run("atanValue", func(t *testing.T) {
		result, err := atanValue(0.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)

		result, err = atanValue(1.0)
		require.NoError(t, err)
		assert.InDelta(t, math.Pi/4, result, 1e-10)
	})

	t.Run("atan2Value", func(t *testing.T) {
		result, err := atan2Value(1.0, 1.0)
		require.NoError(t, err)
		assert.InDelta(t, math.Pi/4, result, 1e-10)

		result, err = atan2Value(0.0, 1.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)

		// Nil propagation
		result, err = atan2Value(nil, 1.0)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("degreesValue", func(t *testing.T) {
		result, err := degreesValue(math.Pi)
		require.NoError(t, err)
		assert.InDelta(t, 180.0, result, 1e-10)

		result, err = degreesValue(math.Pi / 2)
		require.NoError(t, err)
		assert.InDelta(t, 90.0, result, 1e-10)
	})

	t.Run("radiansValue", func(t *testing.T) {
		result, err := radiansValue(180.0)
		require.NoError(t, err)
		assert.InDelta(t, math.Pi, result, 1e-10)

		result, err = radiansValue(90.0)
		require.NoError(t, err)
		assert.InDelta(t, math.Pi/2, result, 1e-10)
	})
}

// TestHyperbolicFunctions tests the hyperbolic math functions.
func TestHyperbolicFunctions(t *testing.T) {
	t.Run("sinhValue", func(t *testing.T) {
		result, err := sinhValue(0.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)
	})

	t.Run("coshValue", func(t *testing.T) {
		result, err := coshValue(0.0)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, result, 1e-10)
	})

	t.Run("tanhValue", func(t *testing.T) {
		result, err := tanhValue(0.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)
	})

	t.Run("asinhValue", func(t *testing.T) {
		result, err := asinhValue(0.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)
	})

	t.Run("acoshValue", func(t *testing.T) {
		result, err := acoshValue(1.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)

		_, err = acoshValue(0.5)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOSH domain error")
	})

	t.Run("atanhValue", func(t *testing.T) {
		result, err := atanhValue(0.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)

		_, err = atanhValue(1.0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ATANH domain error")

		_, err = atanhValue(-1.0)
		require.Error(t, err)
	})
}

// TestUtilityFunctions tests the utility math functions.
func TestUtilityFunctions(t *testing.T) {
	t.Run("piValue", func(t *testing.T) {
		result, err := piValue()
		require.NoError(t, err)
		assert.Equal(t, math.Pi, result)
	})

	t.Run("randomValue", func(t *testing.T) {
		// Test that RANDOM returns values in [0, 1)
		for i := 0; i < 100; i++ {
			result, err := randomValue()
			require.NoError(t, err)
			val, ok := result.(float64)
			require.True(t, ok, "RANDOM should return float64")
			assert.GreaterOrEqual(t, val, 0.0, "RANDOM should return >= 0")
			assert.Less(t, val, 1.0, "RANDOM should return < 1")
		}

		// Test that RANDOM returns different values (non-deterministic)
		result1, _ := randomValue()
		result2, _ := randomValue()
		result3, _ := randomValue()
		// It's extremely unlikely all three would be equal
		allSame := result1 == result2 && result2 == result3
		assert.False(t, allSame, "RANDOM should return different values")
	})

	t.Run("signValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected any
		}{
			{"positive", 5.0, int64(1)},
			{"negative", -5.0, int64(-1)},
			{"zero", 0.0, int64(0)},
			{"nil", nil, nil},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := signValue(tt.value)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("gcdValue", func(t *testing.T) {
		tests := []struct {
			name     string
			a        any
			b        any
			expected int64
		}{
			{"basic", int64(12), int64(8), 4},
			{"coprime", int64(17), int64(13), 1},
			{"same", int64(15), int64(15), 15},
			{"zero", int64(0), int64(5), 5},
			{"negative", int64(-12), int64(8), 4},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := gcdValue(tt.a, tt.b)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}

		t.Run("nil propagation", func(t *testing.T) {
			result, err := gcdValue(nil, int64(5))
			require.NoError(t, err)
			assert.Nil(t, result)
		})
	})

	t.Run("lcmValue", func(t *testing.T) {
		tests := []struct {
			name     string
			a        any
			b        any
			expected int64
		}{
			{"basic", int64(4), int64(6), 12},
			{"coprime", int64(3), int64(5), 15},
			{"same", int64(7), int64(7), 7},
			{"zero", int64(0), int64(5), 0},
			{"negative", int64(-4), int64(6), 12},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := lcmValue(tt.a, tt.b)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("isnanValue", func(t *testing.T) {
		result, err := isnanValue(math.NaN())
		require.NoError(t, err)
		assert.Equal(t, true, result)

		result, err = isnanValue(1.0)
		require.NoError(t, err)
		assert.Equal(t, false, result)

		result, err = isnanValue(nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("isinfValue", func(t *testing.T) {
		result, err := isinfValue(math.Inf(1))
		require.NoError(t, err)
		assert.Equal(t, true, result)

		result, err = isinfValue(math.Inf(-1))
		require.NoError(t, err)
		assert.Equal(t, true, result)

		result, err = isinfValue(1.0)
		require.NoError(t, err)
		assert.Equal(t, false, result)
	})

	t.Run("isfiniteValue", func(t *testing.T) {
		result, err := isfiniteValue(1.0)
		require.NoError(t, err)
		assert.Equal(t, true, result)

		result, err = isfiniteValue(math.NaN())
		require.NoError(t, err)
		assert.Equal(t, false, result)

		result, err = isfiniteValue(math.Inf(1))
		require.NoError(t, err)
		assert.Equal(t, false, result)
	})
}

// TestNullPropagation tests NULL handling across all math functions.
func TestNullPropagation(t *testing.T) {
	// Unary functions that should return nil for nil input
	unaryFuncs := []struct {
		name string
		fn   func(any) (any, error)
	}{
		{"ceil", ceilValue},
		{"floor", floorValue},
		{"trunc", truncValue},
		{"sqrt", sqrtValue},
		{"cbrt", cbrtValue},
		{"exp", expValue},
		{"ln", lnValue},
		{"log10", log10Value},
		{"log2", log2Value},
		{"gamma", gammaValue},
		{"lgamma", lgammaValue},
		{"factorial", factorialValue},
		{"sin", sinValue},
		{"cos", cosValue},
		{"tan", tanValue},
		{"cot", cotValue},
		{"asin", asinValue},
		{"acos", acosValue},
		{"atan", atanValue},
		{"degrees", degreesValue},
		{"radians", radiansValue},
		{"sinh", sinhValue},
		{"cosh", coshValue},
		{"tanh", tanhValue},
		{"asinh", asinhValue},
		{"acosh", acoshValue},
		{"atanh", atanhValue},
		{"sign", signValue},
		{"isnan", isnanValue},
		{"isinf", isinfValue},
		{"isfinite", isfiniteValue},
	}

	for _, tc := range unaryFuncs {
		t.Run(tc.name+"_nil", func(t *testing.T) {
			result, err := tc.fn(nil)
			require.NoError(t, err)
			assert.Nil(t, result, "%s should return nil for nil input", tc.name)
		})
	}

	// Binary functions that should return nil if either input is nil
	t.Run("pow_nil_first", func(t *testing.T) {
		result, err := powValue(nil, 2.0)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("pow_nil_second", func(t *testing.T) {
		result, err := powValue(2.0, nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("atan2_nil_first", func(t *testing.T) {
		result, err := atan2Value(nil, 1.0)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("atan2_nil_second", func(t *testing.T) {
		result, err := atan2Value(1.0, nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("gcd_nil", func(t *testing.T) {
		result, err := gcdValue(nil, int64(5))
		require.NoError(t, err)
		assert.Nil(t, result)

		result, err = gcdValue(int64(5), nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("lcm_nil", func(t *testing.T) {
		result, err := lcmValue(nil, int64(5))
		require.NoError(t, err)
		assert.Nil(t, result)

		result, err = lcmValue(int64(5), nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("round_nil", func(t *testing.T) {
		result, err := roundValue(nil, int64(0))
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("roundEven_nil", func(t *testing.T) {
		result, err := roundEvenValue(nil, int64(0))
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("even_nil", func(t *testing.T) {
		result, err := evenValue(nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

// TestBitwiseOperators tests all bitwise operator functions.
func TestBitwiseOperators(t *testing.T) {
	t.Run("bitwiseAndValue", func(t *testing.T) {
		tests := []struct {
			name     string
			left     any
			right    any
			expected any
		}{
			{"basic AND", int64(0b1100), int64(0b1010), int64(0b1000)},
			{"all ones", int64(0xFF), int64(0xFF), int64(0xFF)},
			{"all zeros", int64(0), int64(0xFF), int64(0)},
			{"zero left", int64(0), int64(42), int64(0)},
			{"zero right", int64(42), int64(0), int64(0)},
			{"negative", int64(-1), int64(0xFF), int64(0xFF)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := bitwiseAndValue(tt.left, tt.right)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("bitwiseOrValue", func(t *testing.T) {
		tests := []struct {
			name     string
			left     any
			right    any
			expected any
		}{
			{"basic OR", int64(0b1100), int64(0b1010), int64(0b1110)},
			{"all zeros", int64(0), int64(0), int64(0)},
			{"identity", int64(42), int64(0), int64(42)},
			{"negative", int64(-1), int64(0), int64(-1)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := bitwiseOrValue(tt.left, tt.right)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("bitwiseXorValue", func(t *testing.T) {
		tests := []struct {
			name     string
			left     any
			right    any
			expected any
		}{
			{"basic XOR", int64(0b1100), int64(0b1010), int64(0b0110)},
			{"same value", int64(42), int64(42), int64(0)},
			{"identity", int64(42), int64(0), int64(42)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := bitwiseXorValue(tt.left, tt.right)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("bitwiseNotValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected any
		}{
			{"zero", int64(0), int64(-1)},
			{"positive", int64(1), int64(-2)},
			{"negative one", int64(-1), int64(0)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := bitwiseNotValue(tt.value)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("bitwiseShiftLeftValue", func(t *testing.T) {
		tests := []struct {
			name     string
			left     any
			right    any
			expected any
		}{
			{"basic shift", int64(1), uint64(4), int64(16)},
			{"shift by 0", int64(42), uint64(0), int64(42)},
			{
				"shift by 63",
				int64(1),
				uint64(63),
				int64(-9223372036854775808),
			}, // 1 << 63 = min int64
			{
				"shift by 64",
				int64(1),
				uint64(64),
				int64(0),
			}, // Overflow returns 0
			{
				"shift by 100",
				int64(1),
				uint64(100),
				int64(0),
			}, // Overflow returns 0
			{"negative value", int64(-1), uint64(4), int64(-16)}, // Arithmetic shift
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := bitwiseShiftLeftValue(tt.left, tt.right)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("bitwiseShiftRightValue", func(t *testing.T) {
		tests := []struct {
			name     string
			left     any
			right    any
			expected any
		}{
			{"basic shift", int64(16), uint64(4), int64(1)},
			{"shift by 0", int64(42), uint64(0), int64(42)},
			{"shift by 63", int64(1 << 62), uint64(62), int64(1)},
			{"shift by 64 positive", int64(1), uint64(64), int64(0)}, // Returns 0 for positive
			{
				"shift by 64 negative",
				int64(-1),
				uint64(64),
				int64(-1),
			}, // Returns -1 for negative (sign extension)
			{
				"negative value",
				int64(-16),
				uint64(4),
				int64(-1),
			}, // Arithmetic shift preserves sign
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := bitwiseShiftRightValue(tt.left, tt.right)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("bitCountValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected any
		}{
			{"zero", int64(0), int64(0)},
			{"one", int64(1), int64(1)},
			{"power of 2", int64(8), int64(1)},       // 0b1000 = 1 bit
			{"all ones byte", int64(0xFF), int64(8)}, // 0b11111111 = 8 bits
			{"mixed", int64(0b10101010), int64(4)},   // 4 bits set
			{"negative", int64(-1), int64(64)},       // All bits set in two's complement
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := bitCountValue(tt.value)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})
}

// TestBitwiseNullPropagation tests NULL handling for all bitwise operators.
func TestBitwiseNullPropagation(t *testing.T) {
	t.Run("bitwiseAnd_nil_left", func(t *testing.T) {
		result, err := bitwiseAndValue(nil, int64(5))
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseAnd should return nil when left is nil")
	})

	t.Run("bitwiseAnd_nil_right", func(t *testing.T) {
		result, err := bitwiseAndValue(int64(5), nil)
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseAnd should return nil when right is nil")
	})

	t.Run("bitwiseAnd_nil_both", func(t *testing.T) {
		result, err := bitwiseAndValue(nil, nil)
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseAnd should return nil when both are nil")
	})

	t.Run("bitwiseOr_nil_left", func(t *testing.T) {
		result, err := bitwiseOrValue(nil, int64(5))
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseOr should return nil when left is nil")
	})

	t.Run("bitwiseOr_nil_right", func(t *testing.T) {
		result, err := bitwiseOrValue(int64(5), nil)
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseOr should return nil when right is nil")
	})

	t.Run("bitwiseXor_nil_left", func(t *testing.T) {
		result, err := bitwiseXorValue(nil, int64(5))
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseXor should return nil when left is nil")
	})

	t.Run("bitwiseXor_nil_right", func(t *testing.T) {
		result, err := bitwiseXorValue(int64(5), nil)
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseXor should return nil when right is nil")
	})

	t.Run("bitwiseNot_nil", func(t *testing.T) {
		result, err := bitwiseNotValue(nil)
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseNot should return nil for nil input")
	})

	t.Run("bitwiseShiftLeft_nil_left", func(t *testing.T) {
		result, err := bitwiseShiftLeftValue(nil, uint64(5))
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseShiftLeft should return nil when left is nil")
	})

	t.Run("bitwiseShiftLeft_nil_right", func(t *testing.T) {
		result, err := bitwiseShiftLeftValue(int64(5), nil)
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseShiftLeft should return nil when right is nil")
	})

	t.Run("bitwiseShiftRight_nil_left", func(t *testing.T) {
		result, err := bitwiseShiftRightValue(nil, uint64(5))
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseShiftRight should return nil when left is nil")
	})

	t.Run("bitwiseShiftRight_nil_right", func(t *testing.T) {
		result, err := bitwiseShiftRightValue(int64(5), nil)
		require.NoError(t, err)
		assert.Nil(t, result, "bitwiseShiftRight should return nil when right is nil")
	})

	t.Run("bitCount_nil", func(t *testing.T) {
		result, err := bitCountValue(nil)
		require.NoError(t, err)
		assert.Nil(t, result, "bitCount should return nil for nil input")
	})
}

// TestComprehensiveNullPropagation tests NULL handling comprehensively across all functions.
func TestComprehensiveNullPropagation(t *testing.T) {
	// Test all unary functions with nil input
	unaryFunctions := []struct {
		name string
		fn   func(any) (any, error)
	}{
		// Rounding functions
		{"ceil", ceilValue},
		{"floor", floorValue},
		{"trunc", truncValue},
		{"even", evenValue},

		// Scientific functions
		{"sqrt", sqrtValue},
		{"cbrt", cbrtValue},
		{"exp", expValue},
		{"ln", lnValue},
		{"log10", log10Value},
		{"log2", log2Value},
		{"gamma", gammaValue},
		{"lgamma", lgammaValue},
		{"factorial", factorialValue},

		// Trigonometric functions
		{"sin", sinValue},
		{"cos", cosValue},
		{"tan", tanValue},
		{"cot", cotValue},
		{"asin", asinValue},
		{"acos", acosValue},
		{"atan", atanValue},
		{"degrees", degreesValue},
		{"radians", radiansValue},

		// Hyperbolic functions
		{"sinh", sinhValue},
		{"cosh", coshValue},
		{"tanh", tanhValue},
		{"asinh", asinhValue},
		{"acosh", acoshValue},
		{"atanh", atanhValue},

		// Utility functions
		{"sign", signValue},
		{"isnan", isnanValue},
		{"isinf", isinfValue},
		{"isfinite", isfiniteValue},

		// Bitwise functions
		{"bitwiseNot", bitwiseNotValue},
		{"bitCount", bitCountValue},
	}

	for _, tc := range unaryFunctions {
		t.Run(tc.name+"_returns_nil_for_nil_input", func(t *testing.T) {
			result, err := tc.fn(nil)
			require.NoError(t, err, "%s should not error for nil input", tc.name)
			assert.Nil(t, result, "%s should return nil for nil input", tc.name)
		})
	}

	// Test all binary functions with nil in first position
	binaryFunctionsLeftNil := []struct {
		name  string
		fn    func(any, any) (any, error)
		right any
	}{
		{"pow", powValue, float64(2)},
		{"atan2", atan2Value, float64(1)},
		{"gcd", gcdValue, int64(12)},
		{"lcm", lcmValue, int64(12)},
		{"bitwiseAnd", bitwiseAndValue, int64(5)},
		{"bitwiseOr", bitwiseOrValue, int64(5)},
		{"bitwiseXor", bitwiseXorValue, int64(5)},
		{"bitwiseShiftLeft", bitwiseShiftLeftValue, uint64(2)},
		{"bitwiseShiftRight", bitwiseShiftRightValue, uint64(2)},
	}

	for _, tc := range binaryFunctionsLeftNil {
		t.Run(tc.name+"_nil_left", func(t *testing.T) {
			result, err := tc.fn(nil, tc.right)
			require.NoError(t, err, "%s should not error when left is nil", tc.name)
			assert.Nil(t, result, "%s should return nil when left is nil", tc.name)
		})
	}

	// Test all binary functions with nil in second position
	binaryFunctionsRightNil := []struct {
		name string
		fn   func(any, any) (any, error)
		left any
	}{
		{"pow", powValue, float64(2)},
		{"atan2", atan2Value, float64(1)},
		{"gcd", gcdValue, int64(12)},
		{"lcm", lcmValue, int64(12)},
		{"bitwiseAnd", bitwiseAndValue, int64(5)},
		{"bitwiseOr", bitwiseOrValue, int64(5)},
		{"bitwiseXor", bitwiseXorValue, int64(5)},
		{"bitwiseShiftLeft", bitwiseShiftLeftValue, int64(5)},
		{"bitwiseShiftRight", bitwiseShiftRightValue, int64(5)},
	}

	for _, tc := range binaryFunctionsRightNil {
		t.Run(tc.name+"_nil_right", func(t *testing.T) {
			result, err := tc.fn(tc.left, nil)
			require.NoError(t, err, "%s should not error when right is nil", tc.name)
			assert.Nil(t, result, "%s should return nil when right is nil", tc.name)
		})
	}

	// Test binary functions with both nil
	binaryFunctionsBothNil := []struct {
		name string
		fn   func(any, any) (any, error)
	}{
		{"pow", powValue},
		{"atan2", atan2Value},
		{"gcd", gcdValue},
		{"lcm", lcmValue},
		{"bitwiseAnd", bitwiseAndValue},
		{"bitwiseOr", bitwiseOrValue},
		{"bitwiseXor", bitwiseXorValue},
		{"bitwiseShiftLeft", bitwiseShiftLeftValue},
		{"bitwiseShiftRight", bitwiseShiftRightValue},
	}

	for _, tc := range binaryFunctionsBothNil {
		t.Run(tc.name+"_nil_both", func(t *testing.T) {
			result, err := tc.fn(nil, nil)
			require.NoError(t, err, "%s should not error when both are nil", tc.name)
			assert.Nil(t, result, "%s should return nil when both are nil", tc.name)
		})
	}

	// Test round and roundEven with nil first argument (decimals can be nil)
	t.Run("round_nil_value", func(t *testing.T) {
		result, err := roundValue(nil, int64(2))
		require.NoError(t, err)
		assert.Nil(t, result, "round should return nil when value is nil")
	})

	t.Run("roundEven_nil_value", func(t *testing.T) {
		result, err := roundEvenValue(nil, int64(2))
		require.NoError(t, err)
		assert.Nil(t, result, "roundEven should return nil when value is nil")
	})
}

// TestErrorTypes tests that the correct error types are returned.
func TestErrorTypes(t *testing.T) {
	// Test that domain errors contain the expected messages
	_, err := sqrtValue(-1.0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), ErrMathDomainSqrt.Error())

	_, err = lnValue(0.0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), ErrMathDomainLog.Error())

	_, err = asinValue(2.0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), ErrMathDomainAsin.Error())

	_, err = acosValue(2.0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), ErrMathDomainAcos.Error())

	_, err = atanhValue(1.0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), ErrMathDomainAtanh.Error())

	_, err = acoshValue(0.5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), ErrMathDomainAcosh.Error())

	_, err = factorialValue(int64(21))
	require.Error(t, err)
	assert.Contains(t, err.Error(), ErrMathDomainFactorial.Error())
}

// =============================================================================
// Task 11.6: Comprehensive Unit Tests for All Error Cases
// =============================================================================

// TestSQRTDomainErrors tests SQRT domain error for negative numbers.
func TestSQRTDomainErrors(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"negative -1", float64(-1)},
		{"negative -0.001", float64(-0.001)},
		{"negative int", int64(-5)},
		{"large negative", float64(-1000000)},
		{"small negative", float64(-1e-10)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sqrtValue(tt.input)
			require.Error(t, err, "SQRT of negative number should return error")
			assert.Nil(t, result, "Result should be nil for error case")
			assert.Contains(t, err.Error(), "SQRT of negative number not allowed")
		})
	}
}

// TestLNDomainErrors tests LN domain error for non-positive numbers.
func TestLNDomainErrors(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"zero", float64(0)},
		{"negative -1", float64(-1)},
		{"negative -0.001", float64(-0.001)},
		{"large negative", float64(-1000000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := lnValue(tt.input)
			require.Error(t, err, "LN of non-positive number should return error")
			assert.Nil(t, result, "Result should be nil for error case")
			assert.Contains(t, err.Error(), "cannot take logarithm of non-positive number")
		})
	}
}

// TestLOG10DomainErrors tests LOG10 domain error for non-positive numbers.
func TestLOG10DomainErrors(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"zero", float64(0)},
		{"negative -1", float64(-1)},
		{"negative -100", float64(-100)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := log10Value(tt.input)
			require.Error(t, err, "LOG10 of non-positive number should return error")
			assert.Nil(t, result, "Result should be nil for error case")
			assert.Contains(t, err.Error(), "cannot take logarithm of non-positive number")
		})
	}
}

// TestLOG2DomainErrors tests LOG2 domain error for non-positive numbers.
func TestLOG2DomainErrors(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"zero", float64(0)},
		{"negative -1", float64(-1)},
		{"negative -8", float64(-8)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := log2Value(tt.input)
			require.Error(t, err, "LOG2 of non-positive number should return error")
			assert.Nil(t, result, "Result should be nil for error case")
			assert.Contains(t, err.Error(), "cannot take logarithm of non-positive number")
		})
	}
}

// TestASINDomainErrors tests ASIN domain error for values outside [-1, 1].
func TestASINDomainErrors(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"greater than 1", float64(1.1)},
		{"much greater than 1", float64(2.0)},
		{"less than -1", float64(-1.1)},
		{"much less than -1", float64(-2.0)},
		{"large positive", float64(100)},
		{"large negative", float64(-100)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := asinValue(tt.input)
			require.Error(t, err, "ASIN outside [-1, 1] should return error")
			assert.Nil(t, result, "Result should be nil for error case")
			assert.Contains(t, err.Error(), "ASIN domain error: input must be in [-1, 1]")
		})
	}
}

// TestACOSDomainErrors tests ACOS domain error for values outside [-1, 1].
func TestACOSDomainErrors(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"greater than 1", float64(1.1)},
		{"much greater than 1", float64(2.0)},
		{"less than -1", float64(-1.1)},
		{"much less than -1", float64(-2.0)},
		{"large positive", float64(100)},
		{"large negative", float64(-100)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := acosValue(tt.input)
			require.Error(t, err, "ACOS outside [-1, 1] should return error")
			assert.Nil(t, result, "Result should be nil for error case")
			assert.Contains(t, err.Error(), "ACOS domain error: input must be in [-1, 1]")
		})
	}
}

// TestFACTORIALDomainErrors tests FACTORIAL domain error for negative and overflow.
func TestFACTORIALDomainErrors(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"negative -1", int64(-1)},
		{"negative -10", int64(-10)},
		{"overflow 21", int64(21)},
		{"overflow 22", int64(22)},
		{"overflow 100", int64(100)},
		{"large overflow", int64(1000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := factorialValue(tt.input)
			require.Error(t, err, "FACTORIAL with invalid input should return error")
			assert.Nil(t, result, "Result should be nil for error case")
			assert.Contains(
				t,
				err.Error(),
				"FACTORIAL domain error: input must be non-negative and <= 20",
			)
		})
	}
}

// TestACOSHDomainErrors tests ACOSH domain error for values less than 1.
func TestACOSHDomainErrors(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"less than 1", float64(0.9)},
		{"zero", float64(0)},
		{"negative", float64(-1)},
		{"just below 1", float64(0.9999)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := acoshValue(tt.input)
			require.Error(t, err, "ACOSH with input < 1 should return error")
			assert.Nil(t, result, "Result should be nil for error case")
			assert.Contains(t, err.Error(), "ACOSH domain error: input must be >= 1")
		})
	}
}

// TestATANHDomainErrors tests ATANH domain error for values outside (-1, 1).
func TestATANHDomainErrors(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"equals 1", float64(1)},
		{"equals -1", float64(-1)},
		{"greater than 1", float64(1.5)},
		{"less than -1", float64(-1.5)},
		{"large positive", float64(100)},
		{"large negative", float64(-100)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := atanhValue(tt.input)
			require.Error(t, err, "ATANH outside (-1, 1) should return error")
			assert.Nil(t, result, "Result should be nil for error case")
			assert.Contains(t, err.Error(), "ATANH domain error: input must be in (-1, 1)")
		})
	}
}

// TestErrorMessagesClarity tests that all error messages are clear and descriptive.
func TestErrorMessagesClarity(t *testing.T) {
	// Test that each error message clearly describes the problem and constraint
	t.Run("SQRT error message clarity", func(t *testing.T) {
		_, err := sqrtValue(-1.0)
		require.Error(t, err)
		msg := err.Error()
		assert.Contains(t, msg, "SQRT")
		assert.Contains(t, msg, "negative")
	})

	t.Run("LN error message clarity", func(t *testing.T) {
		_, err := lnValue(0.0)
		require.Error(t, err)
		msg := err.Error()
		assert.Contains(t, msg, "logarithm")
		assert.Contains(t, msg, "non-positive")
	})

	t.Run("ASIN error message clarity", func(t *testing.T) {
		_, err := asinValue(2.0)
		require.Error(t, err)
		msg := err.Error()
		assert.Contains(t, msg, "ASIN")
		assert.Contains(t, msg, "[-1, 1]")
	})

	t.Run("ACOS error message clarity", func(t *testing.T) {
		_, err := acosValue(2.0)
		require.Error(t, err)
		msg := err.Error()
		assert.Contains(t, msg, "ACOS")
		assert.Contains(t, msg, "[-1, 1]")
	})

	t.Run("FACTORIAL error message clarity", func(t *testing.T) {
		_, err := factorialValue(int64(21))
		require.Error(t, err)
		msg := err.Error()
		assert.Contains(t, msg, "FACTORIAL")
		assert.Contains(t, msg, "<= 20")
	})

	t.Run("ACOSH error message clarity", func(t *testing.T) {
		_, err := acoshValue(0.5)
		require.Error(t, err)
		msg := err.Error()
		assert.Contains(t, msg, "ACOSH")
		assert.Contains(t, msg, ">= 1")
	})

	t.Run("ATANH error message clarity", func(t *testing.T) {
		_, err := atanhValue(1.0)
		require.Error(t, err)
		msg := err.Error()
		assert.Contains(t, msg, "ATANH")
		assert.Contains(t, msg, "(-1, 1)")
	})
}

// TestBoundaryConditions tests edge cases at domain boundaries.
func TestBoundaryConditions(t *testing.T) {
	// SQRT boundary - just above and below zero
	t.Run("SQRT at boundary", func(t *testing.T) {
		// Zero should work
		result, err := sqrtValue(0.0)
		require.NoError(t, err)
		assert.Equal(t, 0.0, result)

		// Just below zero should fail
		_, err = sqrtValue(-1e-15)
		require.Error(t, err)
	})

	// ASIN/ACOS boundaries
	t.Run("ASIN at boundaries", func(t *testing.T) {
		// Exactly -1 should work
		result, err := asinValue(-1.0)
		require.NoError(t, err)
		assert.InDelta(t, -math.Pi/2, result, 1e-10)

		// Exactly 1 should work
		result, err = asinValue(1.0)
		require.NoError(t, err)
		assert.InDelta(t, math.Pi/2, result, 1e-10)

		// Just outside should fail
		_, err = asinValue(1.0000001)
		require.Error(t, err)

		_, err = asinValue(-1.0000001)
		require.Error(t, err)
	})

	t.Run("ACOS at boundaries", func(t *testing.T) {
		// Exactly -1 should work
		result, err := acosValue(-1.0)
		require.NoError(t, err)
		assert.InDelta(t, math.Pi, result, 1e-10)

		// Exactly 1 should work
		result, err = acosValue(1.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)

		// Just outside should fail
		_, err = acosValue(1.0000001)
		require.Error(t, err)

		_, err = acosValue(-1.0000001)
		require.Error(t, err)
	})

	// FACTORIAL boundaries
	t.Run("FACTORIAL at boundaries", func(t *testing.T) {
		// 0 should work
		result, err := factorialValue(int64(0))
		require.NoError(t, err)
		assert.Equal(t, int64(1), result)

		// 20 should work (max)
		result, err = factorialValue(int64(20))
		require.NoError(t, err)
		assert.Equal(t, int64(2432902008176640000), result)

		// 21 should fail (overflow)
		_, err = factorialValue(int64(21))
		require.Error(t, err)

		// -1 should fail (negative)
		_, err = factorialValue(int64(-1))
		require.Error(t, err)
	})

	// LN boundary
	t.Run("LN at boundary", func(t *testing.T) {
		// Very small positive should work
		result, err := lnValue(1e-15)
		require.NoError(t, err)
		assert.Less(t, result.(float64), 0.0) // ln of small positive is large negative

		// Zero should fail
		_, err = lnValue(0.0)
		require.Error(t, err)
	})

	// ACOSH boundary
	t.Run("ACOSH at boundary", func(t *testing.T) {
		// Exactly 1 should work
		result, err := acoshValue(1.0)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, result, 1e-10)

		// Just below 1 should fail
		_, err = acoshValue(0.9999999)
		require.Error(t, err)
	})

	// ATANH boundaries - open interval (-1, 1)
	t.Run("ATANH at boundaries", func(t *testing.T) {
		// Values very close to -1 and 1 should work
		result, err := atanhValue(0.9999)
		require.NoError(t, err)
		assert.Greater(t, result.(float64), 0.0)

		result, err = atanhValue(-0.9999)
		require.NoError(t, err)
		assert.Less(t, result.(float64), 0.0)

		// Exactly 1 and -1 should fail
		_, err = atanhValue(1.0)
		require.Error(t, err)

		_, err = atanhValue(-1.0)
		require.Error(t, err)
	})
}
