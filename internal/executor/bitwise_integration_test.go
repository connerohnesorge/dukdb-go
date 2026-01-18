package executor

import (
	"context"
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
// Bitwise Operators Integration Tests
// Tests for tasks 8.8-8.12 of the essential math functions implementation
// =============================================================================

// setupBitwiseTestExecutor creates an executor for bitwise operator testing
func setupBitwiseTestExecutor() (*Executor, *catalog.Catalog) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	return exec, cat
}

// executeBitwiseQuery executes a SQL query and returns the result
func executeBitwiseQuery(
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

	ctx := context.Background()
	return exec.Execute(ctx, plan, nil)
}

// TestBitwiseOperatorsInSelect tests bitwise operators in SELECT clauses.
func TestBitwiseOperatorsInSelect(t *testing.T) {
	exec, cat := setupBitwiseTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		// Bitwise AND
		{"12 & 10", "SELECT 12 & 10", 8},
		{"255 & 128", "SELECT 255 & 128", 128},
		{"0 & 12345", "SELECT 0 & 12345", 0},

		// Bitwise OR
		{"12 | 10", "SELECT 12 | 10", 14},
		{"4 | 2", "SELECT 4 | 2", 6},
		{"0 | 12345", "SELECT 0 | 12345", 12345},

		// Bitwise XOR
		{"12 ^ 10", "SELECT 12 ^ 10", 6},
		{"5 ^ 3", "SELECT 5 ^ 3", 6},
		{"12345 ^ 12345", "SELECT 12345 ^ 12345", 0},

		// Bitwise NOT
		{"~0", "SELECT ~0", -1},
		{"~1", "SELECT ~1", -2},
		{"~(-1)", "SELECT ~(-1)", 0},

		// Bitwise shift left
		{"1 << 4", "SELECT 1 << 4", 16},
		{"5 << 2", "SELECT 5 << 2", 20},

		// Bitwise shift right
		{"16 >> 4", "SELECT 16 >> 4", 1},
		{"20 >> 2", "SELECT 20 >> 2", 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeBitwiseQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Len(t, result.Rows[0], 1)

			// Get the result value (may be int64 or float64 depending on parsing)
			val := result.Rows[0][result.Columns[0]]
			var actual int64
			switch v := val.(type) {
			case int64:
				actual = v
			case float64:
				actual = int64(v)
			default:
				t.Fatalf("unexpected type %T for result", val)
			}
			assert.Equal(t, tt.expected, actual)
		})
	}
}

// TestBitCountFunctionIntegration tests the BIT_COUNT function.
func TestBitCountFunctionIntegration(t *testing.T) {
	exec, cat := setupBitwiseTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{"BIT_COUNT(0)", "SELECT BIT_COUNT(0)", 0},
		{"BIT_COUNT(1)", "SELECT BIT_COUNT(1)", 1},
		{"BIT_COUNT(7)", "SELECT BIT_COUNT(7)", 3},
		{"BIT_COUNT(255)", "SELECT BIT_COUNT(255)", 8},
		{"BIT_COUNT(15)", "SELECT BIT_COUNT(15)", 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeBitwiseQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Len(t, result.Rows[0], 1)

			val := result.Rows[0][result.Columns[0]]
			var actual int64
			switch v := val.(type) {
			case int64:
				actual = v
			case float64:
				actual = int64(v)
			default:
				t.Fatalf("unexpected type %T for result", val)
			}
			assert.Equal(t, tt.expected, actual)
		})
	}
}

// TestBitwiseOperatorsInWhere tests bitwise operators in WHERE clauses.
// These tests verify that bitwise operators work correctly in filter expressions.
func TestBitwiseOperatorsInWhere(t *testing.T) {
	exec, cat := setupBitwiseTestExecutor()

	// Test bitwise expressions in WHERE using simple conditions
	// We test that bitwise operations produce correct boolean conditions
	t.Run("Bitwise AND comparison in WHERE", func(t *testing.T) {
		// Test that (7 & 3) = 3 evaluates correctly in WHERE
		result, err := executeBitwiseQuery(t, exec, cat,
			"SELECT 1 AS result WHERE (7 & 3) = 3")
		require.NoError(t, err)
		// Should return 1 row since 7 & 3 = 3 is true
		assert.Equal(t, 1, len(result.Rows))
	})

	t.Run("Bitwise AND false condition", func(t *testing.T) {
		// Test that (8 & 3) = 0 evaluates correctly
		result, err := executeBitwiseQuery(t, exec, cat,
			"SELECT 1 AS result WHERE (8 & 3) = 0")
		require.NoError(t, err)
		// Should return 1 row since 8 & 3 = 0 is true
		assert.Equal(t, 1, len(result.Rows))
	})

	t.Run("Bitwise OR in WHERE", func(t *testing.T) {
		// Test that (4 | 2) = 6 evaluates correctly
		result, err := executeBitwiseQuery(t, exec, cat,
			"SELECT 1 AS result WHERE (4 | 2) = 6")
		require.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))
	})

	t.Run("Bitwise XOR in WHERE", func(t *testing.T) {
		// Test that (5 ^ 3) = 6 evaluates correctly
		result, err := executeBitwiseQuery(t, exec, cat,
			"SELECT 1 AS result WHERE (5 ^ 3) = 6")
		require.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))
	})

	t.Run("BIT_COUNT in WHERE", func(t *testing.T) {
		// Test that BIT_COUNT(7) = 3 evaluates correctly
		result, err := executeBitwiseQuery(t, exec, cat,
			"SELECT 1 AS result WHERE BIT_COUNT(7) = 3")
		require.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))
	})

	t.Run("BIT_COUNT greater than in WHERE", func(t *testing.T) {
		// Test that BIT_COUNT(255) > 5 evaluates correctly
		result, err := executeBitwiseQuery(t, exec, cat,
			"SELECT 1 AS result WHERE BIT_COUNT(255) > 5")
		require.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))
	})

	t.Run("Complex bitwise in WHERE", func(t *testing.T) {
		// Test complex expression: ((15 & 8) >> 3) = 1
		result, err := executeBitwiseQuery(t, exec, cat,
			"SELECT 1 AS result WHERE ((15 & 8) >> 3) = 1")
		require.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))
	})
}

// TestBitwiseOperatorsComplex tests more complex bitwise expressions.
func TestBitwiseOperatorsComplex(t *testing.T) {
	exec, cat := setupBitwiseTestExecutor()

	t.Run("Combined operators", func(t *testing.T) {
		// (5 & 3) | 8 = 1 | 8 = 9
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT (5 & 3) | 8")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		val := result.Rows[0][result.Columns[0]]
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(9), actual)
	})

	t.Run("XOR swap", func(t *testing.T) {
		// XOR swap: a = a ^ b, b = a ^ b, a = a ^ b
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 5 ^ 3, (5 ^ 3) ^ 3")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var a, b int64
		for colName, val := range result.Rows[0] {
			var v int64
			switch tv := val.(type) {
			case int64:
				v = tv
			case float64:
				v = int64(tv)
			}
			if colName == result.Columns[0] {
				a = v
			} else {
				b = v
			}
		}
		assert.Equal(t, int64(6), a) // 5 ^ 3
		assert.Equal(t, int64(5), b) // (5 ^ 3) ^ 3 = 5
	})

	t.Run("Mask extraction", func(t *testing.T) {
		// Extract bits 4-7 from 0xABCD
		// (0xABCD >> 4) & 0xF = 0xC = 12
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT (43981 >> 4) & 15")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		val := result.Rows[0][result.Columns[0]]
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(12), actual) // 0xC = 12
	})

	t.Run("Set bit", func(t *testing.T) {
		// Set bit 3 in 5: 5 | (1 << 3) = 5 | 8 = 13
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 5 | (1 << 3)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		val := result.Rows[0][result.Columns[0]]
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(13), actual)
	})

	t.Run("Clear bit", func(t *testing.T) {
		// Clear bit 1 in 7: 7 & ~(1 << 1) = 7 & ~2 = 7 & -3 = 5
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 7 & ~(1 << 1)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		val := result.Rows[0][result.Columns[0]]
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(5), actual)
	})

	t.Run("Toggle bit", func(t *testing.T) {
		// Toggle bit 1 in 5: 5 ^ (1 << 1) = 5 ^ 2 = 7
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 5 ^ (1 << 1)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		val := result.Rows[0][result.Columns[0]]
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(7), actual)
	})
}

// TestBitwiseOperatorsNULL tests NULL handling with bitwise operators.
func TestBitwiseOperatorsNULL(t *testing.T) {
	exec, cat := setupBitwiseTestExecutor()

	tests := []struct {
		name  string
		query string
	}{
		{"NULL & 5", "SELECT NULL & 5"},
		{"5 & NULL", "SELECT 5 & NULL"},
		{"NULL | 5", "SELECT NULL | 5"},
		{"NULL ^ 5", "SELECT NULL ^ 5"},
		{"~NULL", "SELECT ~NULL"},
		{"NULL << 5", "SELECT NULL << 5"},
		{"5 >> NULL", "SELECT 5 >> NULL"},
		{"BIT_COUNT(NULL)", "SELECT BIT_COUNT(NULL)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeBitwiseQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			val := result.Rows[0][result.Columns[0]]
			assert.Nil(t, val, "Expected NULL result for %s", tt.name)
		})
	}
}

// TestBitwiseOperatorPrecedence tests operator precedence.
func TestBitwiseOperatorPrecedence(t *testing.T) {
	exec, cat := setupBitwiseTestExecutor()

	t.Run("AND has higher precedence than OR", func(t *testing.T) {
		// 4 | 3 & 2 should be 4 | (3 & 2) = 4 | 2 = 6
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 4 | 3 & 2")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		val := result.Rows[0][result.Columns[0]]
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(6), actual)
	})

	t.Run("XOR between AND and OR", func(t *testing.T) {
		// 8 | 5 ^ 4 & 6 should be 8 | (5 ^ (4 & 6)) = 8 | (5 ^ 4) = 8 | 1 = 9
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 8 | 5 ^ 4 & 6")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		val := result.Rows[0][result.Columns[0]]
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(9), actual)
	})

	t.Run("Parentheses override precedence", func(t *testing.T) {
		// (4 | 3) & 2 = 7 & 2 = 2
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT (4 | 3) & 2")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		val := result.Rows[0][result.Columns[0]]
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(2), actual)
	})
}

// TestBitwiseOperatorsWithSubqueries tests bitwise operators with subquery expressions.
// Note: Table-based tests require full table integration which is tested elsewhere.
func TestBitwiseOperatorsWithSubqueries(t *testing.T) {
	exec, cat := setupBitwiseTestExecutor()

	// Test bitwise operations in computed expressions
	t.Run("Bitwise AND computed", func(t *testing.T) {
		// 12 & 10 = 8
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 12 & 10 AS result")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(8), actual)
	})

	t.Run("Bitwise OR computed", func(t *testing.T) {
		// 4 | 2 = 6
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 4 | 2 AS result")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(6), actual)
	})

	t.Run("BIT_COUNT computed", func(t *testing.T) {
		// BIT_COUNT(255) = 8
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT BIT_COUNT(255) AS bits")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		var val any
		for _, v := range result.Rows[0] {
			val = v
			break
		}
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(8), actual)
	})
}

// TestBitwiseEdgeCasesIntegration tests edge cases for bitwise operations.
func TestBitwiseEdgeCasesIntegration(t *testing.T) {
	exec, cat := setupBitwiseTestExecutor()

	t.Run("0 & x = 0", func(t *testing.T) {
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 0 & 12345")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		val := result.Rows[0][result.Columns[0]]
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(0), actual)
	})

	t.Run("x | 0 = x", func(t *testing.T) {
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 12345 | 0")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		val := result.Rows[0][result.Columns[0]]
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(12345), actual)
	})

	t.Run("shift by 0", func(t *testing.T) {
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 12345 << 0, 12345 >> 0")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		for _, val := range result.Rows[0] {
			var actual int64
			switch v := val.(type) {
			case int64:
				actual = v
			case float64:
				actual = int64(v)
			}
			assert.Equal(t, int64(12345), actual)
		}
	})

	t.Run("shift by 64", func(t *testing.T) {
		result, err := executeBitwiseQuery(t, exec, cat, "SELECT 1 << 64")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		val := result.Rows[0][result.Columns[0]]
		var actual int64
		switch v := val.(type) {
		case int64:
			actual = v
		case float64:
			actual = int64(v)
		}
		assert.Equal(t, int64(0), actual)
	})
}
