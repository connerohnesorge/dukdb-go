package executor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Math Trigonometric Functions Integration Tests
// Tests for tasks 4.9-4.15 of the essential math functions implementation
// =============================================================================

// =============================================================================
// Task 4.9: Test SIN, COS, TAN, COT via SQL
// =============================================================================

func TestIntegration_SIN_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"sin(0)", "SELECT SIN(0)", 0.0},
		{"sin(pi/2)", "SELECT SIN(1.5707963267948966)", 1.0}, // pi/2
		{"sin(pi)", "SELECT SIN(3.141592653589793)", 0.0},    // pi
		{"sin(pi/6)", "SELECT SIN(0.5235987755982988)", 0.5}, // pi/6 = 30 degrees
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

func TestIntegration_COS_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"cos(0)", "SELECT COS(0)", 1.0},
		{"cos(pi/2)", "SELECT COS(1.5707963267948966)", 0.0},  // pi/2
		{"cos(pi)", "SELECT COS(3.141592653589793)", -1.0},    // pi
		{"cos(pi/3)", "SELECT COS(1.0471975511965976)", 0.5},  // pi/3 = 60 degrees
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

func TestIntegration_TAN_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"tan(0)", "SELECT TAN(0)", 0.0},
		{"tan(pi/4)", "SELECT TAN(0.7853981633974483)", 1.0}, // pi/4 = 45 degrees
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

func TestIntegration_COT_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"cot(pi/4)", "SELECT COT(0.7853981633974483)", 1.0}, // pi/4 = 45 degrees
		{"cot(pi/2)", "SELECT COT(1.5707963267948966)", 0.0}, // pi/2 = 90 degrees
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
// Task 4.10: Test ASIN, ACOS, ATAN, ATAN2 via SQL
// =============================================================================

func TestIntegration_ASIN_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"asin(0)", "SELECT ASIN(0)", 0.0},
		{"asin(1)", "SELECT ASIN(1)", math.Pi / 2},
		{"asin(-1)", "SELECT ASIN(-1)", -math.Pi / 2},
		{"asin(0.5)", "SELECT ASIN(0.5)", math.Pi / 6}, // 30 degrees
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

func TestIntegration_ACOS_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"acos(1)", "SELECT ACOS(1)", 0.0},
		{"acos(0)", "SELECT ACOS(0)", math.Pi / 2},
		{"acos(-1)", "SELECT ACOS(-1)", math.Pi},
		{"acos(0.5)", "SELECT ACOS(0.5)", math.Pi / 3}, // 60 degrees
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

func TestIntegration_ATAN_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"atan(0)", "SELECT ATAN(0)", 0.0},
		{"atan(1)", "SELECT ATAN(1)", math.Pi / 4}, // 45 degrees
		{"atan(-1)", "SELECT ATAN(-1)", -math.Pi / 4},
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

func TestIntegration_ATAN2_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"atan2(0, 1)", "SELECT ATAN2(0, 1)", 0.0},            // 0 degrees
		{"atan2(1, 1)", "SELECT ATAN2(1, 1)", math.Pi / 4},   // 45 degrees
		{"atan2(1, 0)", "SELECT ATAN2(1, 0)", math.Pi / 2},   // 90 degrees
		{"atan2(-1, 0)", "SELECT ATAN2(-1, 0)", -math.Pi / 2}, // -90 degrees
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
// Task 4.11: Test DEGREES, RADIANS via SQL
// =============================================================================

func TestIntegration_DEGREES_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"degrees(pi)", "SELECT DEGREES(3.141592653589793)", 180.0},
		{"degrees(pi/2)", "SELECT DEGREES(1.5707963267948966)", 90.0},
		{"degrees(pi/4)", "SELECT DEGREES(0.7853981633974483)", 45.0},
		{"degrees(0)", "SELECT DEGREES(0)", 0.0},
		{"degrees(2*pi)", "SELECT DEGREES(6.283185307179586)", 360.0},
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

func TestIntegration_RADIANS_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"radians(180)", "SELECT RADIANS(180)", math.Pi},
		{"radians(90)", "SELECT RADIANS(90)", math.Pi / 2},
		{"radians(45)", "SELECT RADIANS(45)", math.Pi / 4},
		{"radians(0)", "SELECT RADIANS(0)", 0.0},
		{"radians(360)", "SELECT RADIANS(360)", 2 * math.Pi},
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

func TestIntegration_PI_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	result, err := executeMathQuery(t, exec, cat, "SELECT PI()")
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	var val any
	for _, v := range result.Rows[0] {
		val = v
		break
	}
	assert.InDelta(t, math.Pi, val, 1e-10)
}

// =============================================================================
// Task 4.13: Test domain errors - ASIN(2), ACOS(-2)
// =============================================================================

func TestIntegration_TrigFunctions_DomainErrors(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("ASIN(2) domain error", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ASIN(2)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ASIN domain error")
	})

	t.Run("ASIN(-2) domain error", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ASIN(-2)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ASIN domain error")
	})

	t.Run("ACOS(2) domain error", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ACOS(2)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOS domain error")
	})

	t.Run("ACOS(-2) domain error", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ACOS(-2)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOS domain error")
	})
}

// =============================================================================
// Task 4.14: Test special values - SIN(0), COS(0), TAN(PI/4)
// =============================================================================

func TestIntegration_TrigFunctions_SpecialValues(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("SIN(0) = 0", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT SIN(0)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 0.0, val, 1e-10)
	})

	t.Run("COS(0) = 1", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT COS(0)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 1.0, val, 1e-10)
	})

	t.Run("TAN(PI/4) = 1", func(t *testing.T) {
		// PI/4 = 0.7853981633974483
		result, err := executeMathQuery(t, exec, cat, "SELECT TAN(0.7853981633974483)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 1.0, val, 1e-10)
	})

	t.Run("SIN(PI/2) = 1", func(t *testing.T) {
		// PI/2 = 1.5707963267948966
		result, err := executeMathQuery(t, exec, cat, "SELECT SIN(1.5707963267948966)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 1.0, val, 1e-10)
	})

	t.Run("COS(PI) = -1", func(t *testing.T) {
		// PI = 3.141592653589793
		result, err := executeMathQuery(t, exec, cat, "SELECT COS(3.141592653589793)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, -1.0, val, 1e-10)
	})
}

// =============================================================================
// Task 4.15: Integration test - Trigonometric calculations in queries
// =============================================================================

func TestIntegration_TrigFunctions_Combined(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("Pythagorean identity: sin^2 + cos^2 = 1", func(t *testing.T) {
		// sin^2(x) + cos^2(x) = 1 for any x
		// Using x = 0.5
		result, err := executeMathQuery(t, exec, cat, "SELECT POW(SIN(0.5), 2) + POW(COS(0.5), 2)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 1.0, val, 1e-10)
	})

	t.Run("ATAN2 for angle calculation", func(t *testing.T) {
		// ATAN2(3, 4) should give the angle of a 3-4-5 triangle
		result, err := executeMathQuery(t, exec, cat, "SELECT ATAN2(3, 4)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		expected := math.Atan2(3, 4)
		assert.InDelta(t, expected, val, 1e-10)
	})

	t.Run("Convert between degrees and radians", func(t *testing.T) {
		// DEGREES(RADIANS(x)) should equal x
		result, err := executeMathQuery(t, exec, cat, "SELECT DEGREES(RADIANS(45))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 45.0, val, 1e-10)
	})

	t.Run("Inverse function: ASIN(SIN(x)) = x for small x", func(t *testing.T) {
		// ASIN(SIN(x)) should equal x for x in [-pi/2, pi/2]
		result, err := executeMathQuery(t, exec, cat, "SELECT ASIN(SIN(0.5))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 0.5, val, 1e-10)
	})

	t.Run("Inverse function: ACOS(COS(x)) = x for x in [0, pi]", func(t *testing.T) {
		// ACOS(COS(x)) should equal x for x in [0, pi]
		result, err := executeMathQuery(t, exec, cat, "SELECT ACOS(COS(1.0))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		assert.InDelta(t, 1.0, val, 1e-10)
	})

	t.Run("TAN = SIN/COS identity", func(t *testing.T) {
		// TAN(x) = SIN(x) / COS(x)
		result, err := executeMathQuery(t, exec, cat, "SELECT TAN(0.5) - SIN(0.5) / COS(0.5)")
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

func TestIntegration_TrigFunctions_WithTable(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// Create a table with angle values
	_, err := executeMathQuery(t, exec, cat, "CREATE TABLE angles (radians DOUBLE, degrees DOUBLE)")
	require.NoError(t, err)

	// Insert some test angles
	_, err = executeMathQuery(t, exec, cat, "INSERT INTO angles VALUES (0, 0)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO angles VALUES (0.7853981633974483, 45)") // pi/4
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO angles VALUES (1.5707963267948966, 90)") // pi/2
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO angles VALUES (3.141592653589793, 180)") // pi
	require.NoError(t, err)

	t.Run("SIN in SELECT with column", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT SIN(radians) FROM angles")
		require.NoError(t, err)
		require.Len(t, result.Rows, 4)
	})

	t.Run("COS in SELECT with column", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT COS(radians) FROM angles")
		require.NoError(t, err)
		require.Len(t, result.Rows, 4)
	})

	t.Run("RADIANS conversion on degrees column", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT RADIANS(degrees), radians FROM angles")
		require.NoError(t, err)
		require.Len(t, result.Rows, 4)
	})

	t.Run("Trig functions in WHERE clause", func(t *testing.T) {
		// Select angles where SIN(radians) > 0.5
		result, err := executeMathQuery(t, exec, cat, "SELECT degrees FROM angles WHERE SIN(radians) > 0.5")
		require.NoError(t, err)
		// Should match 45 and 90 degrees (sin(45) = 0.707, sin(90) = 1)
		require.Len(t, result.Rows, 2)
	})

	t.Run("Combined trig functions with columns", func(t *testing.T) {
		// Calculate sin^2 + cos^2 for each angle (should always be 1)
		result, err := executeMathQuery(t, exec, cat, "SELECT POW(SIN(radians), 2) + POW(COS(radians), 2) FROM angles")
		require.NoError(t, err)
		require.Len(t, result.Rows, 4)

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

func TestIntegration_TrigFunctions_NullHandling(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// Create a table with NULL values
	_, err := executeMathQuery(t, exec, cat, "CREATE TABLE test_trig_nulls (val DOUBLE)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO test_trig_nulls VALUES (NULL)")
	require.NoError(t, err)

	tests := []struct {
		name  string
		query string
	}{
		{"SIN NULL", "SELECT SIN(val) FROM test_trig_nulls"},
		{"COS NULL", "SELECT COS(val) FROM test_trig_nulls"},
		{"TAN NULL", "SELECT TAN(val) FROM test_trig_nulls"},
		{"COT NULL", "SELECT COT(val) FROM test_trig_nulls"},
		{"ASIN NULL", "SELECT ASIN(val) FROM test_trig_nulls"},
		{"ACOS NULL", "SELECT ACOS(val) FROM test_trig_nulls"},
		{"ATAN NULL", "SELECT ATAN(val) FROM test_trig_nulls"},
		{"DEGREES NULL", "SELECT DEGREES(val) FROM test_trig_nulls"},
		{"RADIANS NULL", "SELECT RADIANS(val) FROM test_trig_nulls"},
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

func TestIntegration_TrigFunctions_ErrorCases(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("SIN wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT SIN(1, 2)")
		require.Error(t, err)
	})

	t.Run("COS wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT COS()")
		require.Error(t, err)
	})

	t.Run("TAN wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT TAN()")
		require.Error(t, err)
	})

	t.Run("ATAN2 wrong arg count - too few", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ATAN2(1)")
		require.Error(t, err)
	})

	t.Run("ATAN2 wrong arg count - too many", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ATAN2(1, 2, 3)")
		require.Error(t, err)
	})

	t.Run("DEGREES wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT DEGREES()")
		require.Error(t, err)
	})

	t.Run("RADIANS wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT RADIANS(1, 2)")
		require.Error(t, err)
	})

	t.Run("PI wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT PI(1)")
		require.Error(t, err)
	})
}
