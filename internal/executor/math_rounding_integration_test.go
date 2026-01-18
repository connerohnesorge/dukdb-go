package executor

import (
	"context"
	"math"
	"testing"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Math Rounding Functions Integration Tests
// Tests for tasks 2.7-2.11 of the essential math functions implementation
// =============================================================================

// setupMathTestExecutor creates an executor for math function testing
func setupMathTestExecutor() (*Executor, *catalog.Catalog) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	return exec, cat
}

// executeMathQuery executes a SQL query and returns the result
func executeMathQuery(
	t *testing.T,
	exec *Executor,
	cat *catalog.Catalog,
	sql string,
) (*ExecutionResult, error) {
	t.Helper()

	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	b := binder.NewBinder(cat)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return nil, err
	}

	p := planner.NewPlanner(cat)
	plan, err := p.Plan(boundStmt)
	if err != nil {
		return nil, err
	}

	return exec.Execute(context.Background(), plan, nil)
}

// =============================================================================
// Task 2.7: Test ROUND, CEIL/CEILING, FLOOR, TRUNC via SQL
// =============================================================================

func TestIntegration_ROUND_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"basic round up", "SELECT ROUND(3.7)", 4.0},
		{"basic round down", "SELECT ROUND(3.2)", 3.0},
		{"round half away from zero positive", "SELECT ROUND(2.5)", 3.0},
		{"round half away from zero negative", "SELECT ROUND(-2.5)", -3.0},
		{"round with 2 decimals", "SELECT ROUND(3.14159, 2)", 3.14},
		{"round with 3 decimals", "SELECT ROUND(3.14159, 3)", 3.142},
		{"round integer", "SELECT ROUND(5)", 5.0},
		{"round negative", "SELECT ROUND(-3.7)", -4.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Len(t, result.Rows[0], 1)

			val, ok := result.Rows[0]["ROUND"]
			if !ok {
				// Try getting the first column value
				for _, v := range result.Rows[0] {
					val = v
					break
				}
			}
			assert.InDelta(t, tt.expected, val, 0.0001)
		})
	}
}

func TestIntegration_CEIL_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"ceil positive", "SELECT CEIL(3.1)", 4.0},
		{"ceil negative", "SELECT CEIL(-3.1)", -3.0},
		{"ceil exact", "SELECT CEIL(5.0)", 5.0},
		{"ceiling alias", "SELECT CEILING(3.1)", 4.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			// Get first column value
			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 0.0001)
		})
	}
}

func TestIntegration_FLOOR_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"floor positive", "SELECT FLOOR(3.9)", 3.0},
		{"floor negative", "SELECT FLOOR(-3.1)", -4.0},
		{"floor exact", "SELECT FLOOR(5.0)", 5.0},
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
			assert.InDelta(t, tt.expected, val, 0.0001)
		})
	}
}

func TestIntegration_TRUNC_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"trunc positive", "SELECT TRUNC(3.9)", 3.0},
		{"trunc negative", "SELECT TRUNC(-3.9)", -3.0},
		{"truncate alias", "SELECT TRUNCATE(3.9)", 3.0},
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
			assert.InDelta(t, tt.expected, val, 0.0001)
		})
	}
}

// =============================================================================
// Task 2.8: Test ROUND_EVEN, EVEN via SQL
// =============================================================================

func TestIntegration_ROUND_EVEN_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"banker's round 2.5 to even", "SELECT ROUND_EVEN(2.5)", 2.0},
		{"banker's round 3.5 to even", "SELECT ROUND_EVEN(3.5)", 4.0},
		{"banker's round 4.5 to even", "SELECT ROUND_EVEN(4.5)", 4.0},
		{"banker's round 5.5 to even", "SELECT ROUND_EVEN(5.5)", 6.0},
		{"with decimals", "SELECT ROUND_EVEN(3.145, 2)", 3.14},
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
			assert.InDelta(t, tt.expected, val, 0.0001)
		})
	}
}

func TestIntegration_EVEN_Function(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"even input", "SELECT EVEN(4.0)", 4.0},
		{"odd input rounds up", "SELECT EVEN(3.0)", 4.0},
		{"negative even", "SELECT EVEN(-4.0)", -4.0},
		{"negative odd", "SELECT EVEN(-3.0)", -4.0},
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
			assert.InDelta(t, tt.expected, val, 0.0001)
		})
	}
}

// =============================================================================
// Task 2.10: Test edge cases - NULL inputs, negative precision, INTEGER vs DOUBLE
// =============================================================================

func TestIntegration_RoundingFunctions_NullInputs(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// Create a table with NULL values for testing
	_, err := executeMathQuery(t, exec, cat, "CREATE TABLE test_nulls (val DOUBLE)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO test_nulls VALUES (NULL)")
	require.NoError(t, err)

	tests := []struct {
		name  string
		query string
	}{
		{"ROUND NULL", "SELECT ROUND(val) FROM test_nulls"},
		{"CEIL NULL", "SELECT CEIL(val) FROM test_nulls"},
		{"FLOOR NULL", "SELECT FLOOR(val) FROM test_nulls"},
		{"TRUNC NULL", "SELECT TRUNC(val) FROM test_nulls"},
		{"ROUND_EVEN NULL", "SELECT ROUND_EVEN(val) FROM test_nulls"},
		{"EVEN NULL", "SELECT EVEN(val) FROM test_nulls"},
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

func TestIntegration_ROUND_NegativePrecision(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"round to tens", "SELECT ROUND(12345.0, -1)", 12350.0},
		{"round to hundreds", "SELECT ROUND(12345.0, -2)", 12300.0},
		{"round to thousands", "SELECT ROUND(12345.0, -3)", 12000.0},
		{"round small number", "SELECT ROUND(123.0, -2)", 100.0},
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
			assert.InDelta(t, tt.expected, val, 0.0001)
		})
	}
}

func TestIntegration_RoundingFunctions_IntegerVsDouble(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// Create a table with integer and double columns
	_, err := executeMathQuery(
		t,
		exec,
		cat,
		"CREATE TABLE test_types (int_val INTEGER, dbl_val DOUBLE)",
	)
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO test_types VALUES (5, 5.7)")
	require.NoError(t, err)

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"ROUND integer", "SELECT ROUND(int_val) FROM test_types", 5.0},
		{"ROUND double", "SELECT ROUND(dbl_val) FROM test_types", 6.0},
		{"CEIL integer", "SELECT CEIL(int_val) FROM test_types", 5.0},
		{"CEIL double", "SELECT CEIL(dbl_val) FROM test_types", 6.0},
		{"FLOOR integer", "SELECT FLOOR(int_val) FROM test_types", 5.0},
		{"FLOOR double", "SELECT FLOOR(dbl_val) FROM test_types", 5.0},
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
			assert.InDelta(t, tt.expected, val, 0.0001)
		})
	}
}

// =============================================================================
// Task 2.11: Integration test - ROUND with positive, zero, and negative precision
// =============================================================================

func TestIntegration_ROUND_AllPrecisionTypes(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("positive precision", func(t *testing.T) {
		tests := []struct {
			query    string
			expected float64
		}{
			{"SELECT ROUND(3.14159, 1)", 3.1},
			{"SELECT ROUND(3.14159, 2)", 3.14},
			{"SELECT ROUND(3.14159, 3)", 3.142},
			{"SELECT ROUND(3.14159, 4)", 3.1416},
		}

		for _, tt := range tests {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 0.00001)
		}
	})

	t.Run("zero precision", func(t *testing.T) {
		tests := []struct {
			query    string
			expected float64
		}{
			{"SELECT ROUND(3.14159, 0)", 3.0},
			{"SELECT ROUND(3.5, 0)", 4.0},
			{"SELECT ROUND(-3.5, 0)", -4.0},
			{"SELECT ROUND(3.14159)", 3.0}, // Default precision is 0
		}

		for _, tt := range tests {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 0.0001)
		}
	})

	t.Run("negative precision", func(t *testing.T) {
		tests := []struct {
			query    string
			expected float64
		}{
			{"SELECT ROUND(12345.6789, -1)", 12350.0},
			{"SELECT ROUND(12345.6789, -2)", 12300.0},
			{"SELECT ROUND(12345.6789, -3)", 12000.0},
			{"SELECT ROUND(12345.6789, -4)", 10000.0},
		}

		for _, tt := range tests {
			result, err := executeMathQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			var val any
			for _, v := range result.Rows[0] {
				val = v
				break
			}
			assert.InDelta(t, tt.expected, val, 0.0001)
		}
	})
}

// =============================================================================
// Combined integration tests - rounding functions in complex queries
// =============================================================================

func TestIntegration_RoundingFunctions_InExpressions(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// Create and populate a table
	_, err := executeMathQuery(t, exec, cat, "CREATE TABLE prices (price DOUBLE)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO prices VALUES (19.99)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO prices VALUES (25.50)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO prices VALUES (9.75)")
	require.NoError(t, err)

	t.Run("CEIL in SELECT", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT CEIL(price) FROM prices")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("FLOOR in WHERE", func(t *testing.T) {
		result, err := executeMathQuery(
			t,
			exec,
			cat,
			"SELECT price FROM prices WHERE FLOOR(price) = 25",
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("ROUND for currency", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT ROUND(price, 1) FROM prices")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})
}

// TestIntegration_RoundingFunctions_SpecialValues tests behavior with special floating point values
func TestIntegration_RoundingFunctions_SpecialValues(t *testing.T) {
	// These are direct function tests since SQL doesn't easily support Inf/NaN literals

	t.Run("ROUND infinity", func(t *testing.T) {
		result, err := roundValue(math.Inf(1), nil)
		require.NoError(t, err)
		assert.True(t, math.IsInf(result.(float64), 1))
	})

	t.Run("CEIL infinity", func(t *testing.T) {
		result, err := ceilValue(math.Inf(1))
		require.NoError(t, err)
		assert.True(t, math.IsInf(result.(float64), 1))
	})

	t.Run("FLOOR negative infinity", func(t *testing.T) {
		result, err := floorValue(math.Inf(-1))
		require.NoError(t, err)
		assert.True(t, math.IsInf(result.(float64), -1))
	})

	t.Run("ROUND NaN", func(t *testing.T) {
		result, err := roundValue(math.NaN(), nil)
		require.NoError(t, err)
		assert.True(t, math.IsNaN(result.(float64)))
	})
}

// TestIntegration_RoundingFunctions_ErrorCases tests error handling
func TestIntegration_RoundingFunctions_ErrorCases(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("ROUND wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ROUND(1, 2, 3)")
		require.Error(t, err)
	})

	t.Run("CEIL wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT CEIL(1, 2)")
		require.Error(t, err)
	})

	t.Run("FLOOR wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT FLOOR()")
		require.Error(t, err)
	})

	t.Run("TRUNC wrong arg count", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT TRUNC()")
		require.Error(t, err)
	})
}
