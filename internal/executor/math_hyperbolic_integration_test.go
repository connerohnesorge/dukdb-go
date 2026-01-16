package executor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Math Hyperbolic Functions Integration Tests
// Tests for tasks 5.7-5.11 of the essential math functions implementation
// =============================================================================

// =============================================================================
// Task 5.7: Test SINH, COSH, TANH via SQL
// =============================================================================

func TestIntegration_SINH_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"sinh(0)", "SELECT SINH(0)", 0.0},
		{"sinh(1)", "SELECT SINH(1)", math.Sinh(1)},
		{"sinh(-1)", "SELECT SINH(-1)", math.Sinh(-1)},
		{"sinh(2)", "SELECT SINH(2)", math.Sinh(2)},
		{"sinh(0.5)", "SELECT SINH(0.5)", math.Sinh(0.5)},
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

func TestIntegration_COSH_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"cosh(0)", "SELECT COSH(0)", 1.0},
		{"cosh(1)", "SELECT COSH(1)", math.Cosh(1)},
		{"cosh(-1)", "SELECT COSH(-1)", math.Cosh(-1)}, // cosh is even: cosh(-x) = cosh(x)
		{"cosh(2)", "SELECT COSH(2)", math.Cosh(2)},
		{"cosh(0.5)", "SELECT COSH(0.5)", math.Cosh(0.5)},
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

func TestIntegration_TANH_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"tanh(0)", "SELECT TANH(0)", 0.0},
		{"tanh(1)", "SELECT TANH(1)", math.Tanh(1)},
		{"tanh(-1)", "SELECT TANH(-1)", math.Tanh(-1)},
		{"tanh(2)", "SELECT TANH(2)", math.Tanh(2)},
		{"tanh(0.5)", "SELECT TANH(0.5)", math.Tanh(0.5)},
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
// Task 5.8: Test ASINH, ACOSH, ATANH via SQL
// =============================================================================

func TestIntegration_ASINH_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"asinh(0)", "SELECT ASINH(0)", 0.0},
		{"asinh(1)", "SELECT ASINH(1)", math.Asinh(1)},
		{"asinh(-1)", "SELECT ASINH(-1)", math.Asinh(-1)},
		{"asinh(2)", "SELECT ASINH(2)", math.Asinh(2)},
		{"asinh(0.5)", "SELECT ASINH(0.5)", math.Asinh(0.5)},
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

func TestIntegration_ACOSH_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"acosh(1)", "SELECT ACOSH(1)", 0.0},
		{"acosh(2)", "SELECT ACOSH(2)", math.Acosh(2)},
		{"acosh(3)", "SELECT ACOSH(3)", math.Acosh(3)},
		{"acosh(10)", "SELECT ACOSH(10)", math.Acosh(10)},
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

func TestIntegration_ATANH_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"atanh(0)", "SELECT ATANH(0)", 0.0},
		{"atanh(0.5)", "SELECT ATANH(0.5)", math.Atanh(0.5)},
		{"atanh(-0.5)", "SELECT ATANH(-0.5)", math.Atanh(-0.5)},
		{"atanh(0.9)", "SELECT ATANH(0.9)", math.Atanh(0.9)},
		{"atanh(-0.9)", "SELECT ATANH(-0.9)", math.Atanh(-0.9)},
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
// Task 5.9: Write unit tests for all hyperbolic functions
// (Additional tests beyond those in math_test.go)
// =============================================================================

func TestHyperbolicFunctions_Extended(t *testing.T) {
	t.Run("sinh properties", func(t *testing.T) {
		// sinh is odd: sinh(-x) = -sinh(x)
		result1, err := sinhValue(1.5)
		require.NoError(t, err)
		result2, err := sinhValue(-1.5)
		require.NoError(t, err)
		assert.InDelta(t, -result1.(float64), result2.(float64), 1e-10)
	})

	t.Run("cosh properties", func(t *testing.T) {
		// cosh is even: cosh(-x) = cosh(x)
		result1, err := coshValue(1.5)
		require.NoError(t, err)
		result2, err := coshValue(-1.5)
		require.NoError(t, err)
		assert.InDelta(t, result1.(float64), result2.(float64), 1e-10)

		// cosh(x) >= 1 for all x
		result, err := coshValue(0.0)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, result.(float64), 1.0)
	})

	t.Run("tanh bounds", func(t *testing.T) {
		// tanh(x) is bounded: -1 <= tanh(x) <= 1
		// Due to floating-point precision, tanh(100) rounds to exactly 1.0
		result, err := tanhValue(100.0)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, result.(float64), -1.0)
		assert.LessOrEqual(t, result.(float64), 1.0)

		result, err = tanhValue(-100.0)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, result.(float64), -1.0)
		assert.LessOrEqual(t, result.(float64), 1.0)
	})

	t.Run("asinh inverse", func(t *testing.T) {
		// asinh(sinh(x)) = x
		val := 2.5
		sinhVal, err := sinhValue(val)
		require.NoError(t, err)
		result, err := asinhValue(sinhVal)
		require.NoError(t, err)
		assert.InDelta(t, val, result.(float64), 1e-10)
	})

	t.Run("acosh inverse", func(t *testing.T) {
		// acosh(cosh(x)) = x for x >= 0
		val := 2.5
		coshVal, err := coshValue(val)
		require.NoError(t, err)
		result, err := acoshValue(coshVal)
		require.NoError(t, err)
		assert.InDelta(t, val, result.(float64), 1e-10)
	})

	t.Run("atanh inverse", func(t *testing.T) {
		// atanh(tanh(x)) = x
		val := 0.5
		tanhVal, err := tanhValue(val)
		require.NoError(t, err)
		result, err := atanhValue(tanhVal)
		require.NoError(t, err)
		assert.InDelta(t, val, result.(float64), 1e-10)
	})

	t.Run("hyperbolic identity", func(t *testing.T) {
		// cosh^2(x) - sinh^2(x) = 1
		val := 1.5
		sinhVal, err := sinhValue(val)
		require.NoError(t, err)
		coshVal, err := coshValue(val)
		require.NoError(t, err)

		sinh2 := sinhVal.(float64) * sinhVal.(float64)
		cosh2 := coshVal.(float64) * coshVal.(float64)
		assert.InDelta(t, 1.0, cosh2-sinh2, 1e-10)
	})
}

// =============================================================================
// Task 5.10: Test special values - SINH(0), COSH(0), TANH(0)
// =============================================================================

func TestIntegration_HyperbolicFunctions_SpecialValues(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("SINH(0) = 0", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT SINH(0)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 0.0, val, 1e-10)
	})

	t.Run("COSH(0) = 1", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT COSH(0)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 1.0, val, 1e-10)
	})

	t.Run("TANH(0) = 0", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT TANH(0)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 0.0, val, 1e-10)
	})

	t.Run("ASINH(0) = 0", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT ASINH(0)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 0.0, val, 1e-10)
	})

	t.Run("ACOSH(1) = 0", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT ACOSH(1)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 0.0, val, 1e-10)
	})

	t.Run("ATANH(0) = 0", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT ATANH(0)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 0.0, val, 1e-10)
	})
}

// =============================================================================
// Task 5.11: Integration test - Hyperbolic calculations
// =============================================================================

func TestIntegration_HyperbolicFunctions_Combined(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("Hyperbolic identity: cosh^2 - sinh^2 = 1", func(t *testing.T) {
		// cosh^2(x) - sinh^2(x) = 1 for any x
		result, err := executeMathQuery(t, exec, cat, "SELECT POW(COSH(1.5), 2) - POW(SINH(1.5), 2)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 1.0, val, 1e-10)
	})

	t.Run("tanh = sinh/cosh identity", func(t *testing.T) {
		// tanh(x) = sinh(x) / cosh(x)
		result, err := executeMathQuery(t, exec, cat, "SELECT TANH(0.5) - SINH(0.5) / COSH(0.5)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 0.0, val, 1e-10)
	})

	t.Run("Inverse function: ASINH(SINH(x)) = x", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT ASINH(SINH(2.0))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 2.0, val, 1e-10)
	})

	t.Run("Inverse function: ACOSH(COSH(x)) = x for x >= 0", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT ACOSH(COSH(2.0))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 2.0, val, 1e-10)
	})

	t.Run("Inverse function: ATANH(TANH(x)) = x", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT ATANH(TANH(0.5))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 0.5, val, 1e-10)
	})

	t.Run("Complex hyperbolic calculation", func(t *testing.T) {
		// Calculate (e^x - e^(-x)) / 2 which equals sinh(x)
		// Using x = 1: sinh(1) = (e - e^(-1)) / 2
		result, err := executeMathQuery(t, exec, cat, "SELECT (EXP(1) - EXP(-1)) / 2")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, math.Sinh(1), val, 1e-10)
	})
}

func TestIntegration_HyperbolicFunctions_WithTable(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// Create a table with test values
	_, err := executeMathQuery(t, exec, cat, "CREATE TABLE hyperbolic_test (val DOUBLE)")
	require.NoError(t, err)

	// Insert some test values
	_, err = executeMathQuery(t, exec, cat, "INSERT INTO hyperbolic_test VALUES (0)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO hyperbolic_test VALUES (0.5)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO hyperbolic_test VALUES (1.0)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO hyperbolic_test VALUES (2.0)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO hyperbolic_test VALUES (-1.0)")
	require.NoError(t, err)

	t.Run("SINH in SELECT with column", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT SINH(val) FROM hyperbolic_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 5)
	})

	t.Run("COSH in SELECT with column", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT COSH(val) FROM hyperbolic_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 5)
	})

	t.Run("TANH in SELECT with column", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT TANH(val) FROM hyperbolic_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 5)
	})

	t.Run("Hyperbolic functions in WHERE clause", func(t *testing.T) {
		// Select values where SINH(val) > 0
		result, err := executeMathQuery(t, exec, cat, "SELECT val FROM hyperbolic_test WHERE SINH(val) > 0")
		require.NoError(t, err)
		// Should match 0.5, 1.0, 2.0 (where val > 0)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Combined hyperbolic functions with columns", func(t *testing.T) {
		// Calculate cosh^2(val) - sinh^2(val) for each value (should always be 1)
		result, err := executeMathQuery(t, exec, cat, "SELECT POW(COSH(val), 2) - POW(SINH(val), 2) FROM hyperbolic_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 5)

		// All values should be 1
		for _, row := range result.Rows {
			var val any
			for _, v := range row {
				val = v
				break
			}
			assert.InDelta(t, 1.0, val, 1e-10)
		}
	})
}

func TestIntegration_HyperbolicFunctions_NullHandling(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// Create a table with NULL values
	_, err := executeMathQuery(t, exec, cat, "CREATE TABLE test_hyper_nulls (val DOUBLE)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO test_hyper_nulls VALUES (NULL)")
	require.NoError(t, err)

	tests := []struct {
		name  string
		query string
	}{
		{"SINH NULL", "SELECT SINH(val) FROM test_hyper_nulls"},
		{"COSH NULL", "SELECT COSH(val) FROM test_hyper_nulls"},
		{"TANH NULL", "SELECT TANH(val) FROM test_hyper_nulls"},
		{"ASINH NULL", "SELECT ASINH(val) FROM test_hyper_nulls"},
		{"ACOSH NULL", "SELECT ACOSH(val) FROM test_hyper_nulls"},
		{"ATANH NULL", "SELECT ATANH(val) FROM test_hyper_nulls"},
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

func TestIntegration_HyperbolicFunctions_DomainErrors(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("ACOSH(0.5) domain error", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ACOSH(0.5)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOSH domain error")
	})

	t.Run("ACOSH(0) domain error", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ACOSH(0)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOSH domain error")
	})

	t.Run("ACOSH(-1) domain error", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ACOSH(-1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOSH domain error")
	})

	t.Run("ATANH(1) domain error", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ATANH(1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ATANH domain error")
	})

	t.Run("ATANH(-1) domain error", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ATANH(-1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ATANH domain error")
	})

	t.Run("ATANH(2) domain error", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ATANH(2)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ATANH domain error")
	})
}

func TestIntegration_HyperbolicFunctions_ErrorCases(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("SINH wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT SINH(1, 2)")
		require.Error(t, err)
	})

	t.Run("COSH wrong arg count - none", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT COSH()")
		require.Error(t, err)
	})

	t.Run("TANH wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT TANH()")
		require.Error(t, err)
	})

	t.Run("ASINH wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ASINH(1, 2)")
		require.Error(t, err)
	})

	t.Run("ACOSH wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ACOSH()")
		require.Error(t, err)
	})

	t.Run("ATANH wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ATANH(1, 2)")
		require.Error(t, err)
	})
}
