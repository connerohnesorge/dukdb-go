package executor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Math Scientific Functions Integration Tests
// Tests for tasks 3.11-3.16 of the essential math functions implementation
// =============================================================================

// =============================================================================
// Task 3.11: Test SQRT, CBRT, POW/POWER via SQL
// =============================================================================

func TestIntegration_SQRT_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"sqrt of 4", "SELECT SQRT(4)", 2.0},
		{"sqrt of 9", "SELECT SQRT(9)", 3.0},
		{"sqrt of 2", "SELECT SQRT(2)", math.Sqrt(2)},
		{"sqrt of 0", "SELECT SQRT(0)", 0.0},
		{"sqrt of 0.25", "SELECT SQRT(0.25)", 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 1e-10)
		})
	}
}

func TestIntegration_SQRT_ErrorCase(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// SQRT of negative number should return an error
	_, err := executeMathQuery(t, exec, cat, "SELECT SQRT(-1)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SQRT of negative number")
}

func TestIntegration_CBRT_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"cbrt of 8", "SELECT CBRT(8)", 2.0},
		{"cbrt of 27", "SELECT CBRT(27)", 3.0},
		{"cbrt of -8", "SELECT CBRT(-8)", -2.0},
		{"cbrt of 0", "SELECT CBRT(0)", 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 1e-10)
		})
	}
}

func TestIntegration_POW_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"2^3", "SELECT POW(2, 3)", 8.0},
		{"3^2", "SELECT POW(3, 2)", 9.0},
		{"2^0", "SELECT POW(2, 0)", 1.0},
		{"2^-1", "SELECT POW(2, -1)", 0.5},
		{"4^0.5", "SELECT POW(4, 0.5)", 2.0},
		{"POWER alias", "SELECT POWER(2, 3)", 8.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 1e-10)
		})
	}
}

// =============================================================================
// Task 3.12: Test EXP, LN, LOG/LOG10, LOG2 via SQL
// =============================================================================

func TestIntegration_EXP_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"exp(0)", "SELECT EXP(0)", 1.0},
		{"exp(1)", "SELECT EXP(1)", math.E},
		{"exp(2)", "SELECT EXP(2)", math.Exp(2)},
		{"exp(-1)", "SELECT EXP(-1)", math.Exp(-1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 1e-10)
		})
	}
}

func TestIntegration_LN_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"ln(1)", "SELECT LN(1)", 0.0},
		{"ln(e)", "SELECT LN(2.718281828459045)", 1.0},
		{"ln(10)", "SELECT LN(10)", math.Log(10)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 1e-6)
		})
	}
}

func TestIntegration_LN_ErrorCases(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("LN(0)", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT LN(0)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})

	t.Run("LN(-1)", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT LN(-1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})
}

func TestIntegration_LOG10_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"log10(1)", "SELECT LOG10(1)", 0.0},
		{"log10(10)", "SELECT LOG10(10)", 1.0},
		{"log10(100)", "SELECT LOG10(100)", 2.0},
		{"log10(1000)", "SELECT LOG10(1000)", 3.0},
		{"LOG alias", "SELECT LOG(100)", 2.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 1e-10)
		})
	}
}

func TestIntegration_LOG2_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"log2(1)", "SELECT LOG2(1)", 0.0},
		{"log2(2)", "SELECT LOG2(2)", 1.0},
		{"log2(4)", "SELECT LOG2(4)", 2.0},
		{"log2(8)", "SELECT LOG2(8)", 3.0},
		{"log2(16)", "SELECT LOG2(16)", 4.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 1e-10)
		})
	}
}

// =============================================================================
// Task 3.13: Test GAMMA, LGAMMA, FACTORIAL via SQL
// =============================================================================

func TestIntegration_GAMMA_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"gamma(1)", "SELECT GAMMA(1)", 1.0},                 // 0! = 1
		{"gamma(2)", "SELECT GAMMA(2)", 1.0},                 // 1! = 1
		{"gamma(3)", "SELECT GAMMA(3)", 2.0},                 // 2! = 2
		{"gamma(4)", "SELECT GAMMA(4)", 6.0},                 // 3! = 6
		{"gamma(5)", "SELECT GAMMA(5)", 24.0},                // 4! = 24
		{"gamma(0.5)", "SELECT GAMMA(0.5)", math.Gamma(0.5)}, // sqrt(pi)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 1e-10)
		})
	}
}

func TestIntegration_LGAMMA_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"lgamma(1)", "SELECT LGAMMA(1)", 0.0},          // ln(1) = 0
		{"lgamma(2)", "SELECT LGAMMA(2)", 0.0},          // ln(1) = 0
		{"lgamma(5)", "SELECT LGAMMA(5)", math.Log(24)}, // ln(24)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 1e-10)
		})
	}
}

func TestIntegration_FACTORIAL_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{"0!", "SELECT FACTORIAL(0)", 1},
		{"1!", "SELECT FACTORIAL(1)", 1},
		{"2!", "SELECT FACTORIAL(2)", 2},
		{"3!", "SELECT FACTORIAL(3)", 6},
		{"4!", "SELECT FACTORIAL(4)", 24},
		{"5!", "SELECT FACTORIAL(5)", 120},
		{"10!", "SELECT FACTORIAL(10)", 3628800},
		{"20!", "SELECT FACTORIAL(20)", 2432902008176640000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.Equal(t, tt.expected, val)
		})
	}
}

// =============================================================================
// Task 3.15: Test error cases - SQRT(-1), LN(0), LN(-1), FACTORIAL(25)
// =============================================================================

func TestIntegration_ScientificFunctions_ErrorCases(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("SQRT(-1)", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT SQRT(-1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})

	t.Run("LN(0)", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT LN(0)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})

	t.Run("LN(-1)", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT LN(-1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})

	t.Run("FACTORIAL(25) overflow", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT FACTORIAL(25)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FACTORIAL")
	})

	t.Run("FACTORIAL(-1) negative", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT FACTORIAL(-1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FACTORIAL")
	})

	t.Run("LOG10(0)", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT LOG10(0)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})

	t.Run("LOG2(-1)", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT LOG2(-1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})
}

// =============================================================================
// Task 3.16: Integration test - Combined scientific functions in queries
// =============================================================================

func TestIntegration_ScientificFunctions_Combined(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("Pythagorean distance", func(t *testing.T) {
		// SQRT(POW(x, 2) + POW(y, 2)) where x=3, y=4 should give 5
		result, err := executeMathQuery(t, exec, cat, "SELECT SQRT(POW(3, 2) + POW(4, 2))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 5.0, val, 1e-10)
	})

	t.Run("Exponential and logarithm inverse", func(t *testing.T) {
		// LN(EXP(x)) should equal x
		result, err := executeMathQuery(t, exec, cat, "SELECT LN(EXP(5))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 5.0, val, 1e-10)
	})

	t.Run("Power and root inverse", func(t *testing.T) {
		// CBRT(POW(x, 3)) should equal x
		result, err := executeMathQuery(t, exec, cat, "SELECT CBRT(POW(7, 3))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 7.0, val, 1e-10)
	})

	t.Run("Compound interest formula", func(t *testing.T) {
		// A = P * EXP(r * t) where P=1000, r=0.05, t=10
		// Should be approximately 1000 * e^0.5 = 1648.72
		result, err := executeMathQuery(t, exec, cat, "SELECT 1000 * EXP(0.05 * 10)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		expected := 1000 * math.Exp(0.5)
		assert.InDelta(t, expected, val, 1e-6)
	})

	t.Run("Logarithmic scale calculation", func(t *testing.T) {
		// LOG2(1024) / LOG2(2) should equal 10
		result, err := executeMathQuery(t, exec, cat, "SELECT LOG2(1024)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 10.0, val, 1e-10)
	})
}

func TestIntegration_ScientificFunctions_WithTable(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// Create a table with values to test
	_, err := executeMathQuery(t, exec, cat, "CREATE TABLE numbers (x DOUBLE, y DOUBLE)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO numbers VALUES (3, 4)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO numbers VALUES (5, 12)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO numbers VALUES (8, 15)")
	require.NoError(t, err)

	t.Run("SQRT in SELECT with column", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT SQRT(x) FROM numbers")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("POW with columns", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT POW(x, 2) FROM numbers")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Combined functions with columns", func(t *testing.T) {
		// Calculate hypotenuse for each row
		result, err := executeMathQuery(
			t,
			exec,
			cat,
			"SELECT SQRT(POW(x, 2) + POW(y, 2)) FROM numbers",
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)

		// First row: sqrt(9 + 16) = 5
		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 5.0, val, 1e-10)

		// Second row: sqrt(25 + 144) = 13
		for _, v := range result.Rows[1] {
			val = v
			break
		}
		assert.InDelta(t, 13.0, val, 1e-10)

		// Third row: sqrt(64 + 225) = 17
		for _, v := range result.Rows[2] {
			val = v
			break
		}
		assert.InDelta(t, 17.0, val, 1e-10)
	})

	t.Run("Scientific functions in WHERE clause", func(t *testing.T) {
		// Select rows where SQRT(x) >= 2
		result, err := executeMathQuery(t, exec, cat, "SELECT x FROM numbers WHERE SQRT(x) >= 2")
		require.NoError(t, err)
		// Should return x=5 (sqrt=2.236) and x=8 (sqrt=2.828), but not x=3 (sqrt=1.73)
		require.Len(t, result.Rows, 2)
	})

	t.Run("LOG functions with columns", func(t *testing.T) {
		result, err := executeMathQuery(
			t,
			exec,
			cat,
			"SELECT LN(x), LOG10(x), LOG2(x) FROM numbers",
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})
}

func TestIntegration_ScientificFunctions_NullHandling(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// Create a table with NULL values
	_, err := executeMathQuery(t, exec, cat, "CREATE TABLE test_nulls (val DOUBLE)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO test_nulls VALUES (NULL)")
	require.NoError(t, err)

	tests := []struct {
		name  string
		query string
	}{
		{"SQRT NULL", "SELECT SQRT(val) FROM test_nulls"},
		{"CBRT NULL", "SELECT CBRT(val) FROM test_nulls"},
		{"EXP NULL", "SELECT EXP(val) FROM test_nulls"},
		{"LN NULL", "SELECT LN(val) FROM test_nulls"},
		{"LOG10 NULL", "SELECT LOG10(val) FROM test_nulls"},
		{"LOG2 NULL", "SELECT LOG2(val) FROM test_nulls"},
		{"GAMMA NULL", "SELECT GAMMA(val) FROM test_nulls"},
		{"LGAMMA NULL", "SELECT LGAMMA(val) FROM test_nulls"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			// The result should be NULL
			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.Nil(t, val, "Function should return NULL for NULL input")
		})
	}
}
