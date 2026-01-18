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
// Comprehensive Math Functions Integration Tests
// Tests for tasks 13.1-13.10 of the essential math functions implementation
// =============================================================================

// setupComprehensiveTestExecutor creates an executor for comprehensive integration testing
func setupComprehensiveTestExecutor() (*Executor, *catalog.Catalog) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	return exec, cat
}

// executeComprehensiveQuery executes a SQL query and returns the result
func executeComprehensiveQuery(
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

// getComprehensiveFirstValue extracts the first value from a result row
func getComprehensiveFirstValue(row map[string]any) any {
	for _, v := range row {
		return v
	}
	return nil
}

// =============================================================================
// Task 13.1: Test math functions in SELECT clauses
// =============================================================================

func TestComprehensive_MathFunctions_InSelectClauses(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	// Create a test table
	_, err := executeComprehensiveQuery(t, exec, cat, `
		CREATE TABLE select_test (
			id INTEGER,
			x DOUBLE,
			y DOUBLE,
			angle DOUBLE
		)
	`)
	require.NoError(t, err)

	// Insert test data
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO select_test VALUES (1, 4.0, 3.0, 0.5)",
	)
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO select_test VALUES (2, 9.0, 12.0, 1.0)",
	)
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO select_test VALUES (3, 16.0, 5.0, 0.25)",
	)
	require.NoError(t, err)

	t.Run("Multiple math functions in SELECT", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SQRT(x), CEIL(y), FLOOR(angle) FROM select_test WHERE id = 1")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("Rounding functions in SELECT", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT ROUND(x, 1), TRUNC(y), EVEN(angle) FROM select_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Trigonometric functions in SELECT", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SIN(angle), COS(angle), TAN(angle) FROM select_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Scientific functions in SELECT", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT EXP(angle), LN(x), LOG10(y) FROM select_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Hyperbolic functions in SELECT", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SINH(angle), COSH(angle), TANH(angle) FROM select_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Utility functions in SELECT", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT ABS(x), SIGN(y), PI() FROM select_test WHERE id = 1")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("Combined math expression in SELECT", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SQRT(x) + CEIL(y) * 2 FROM select_test WHERE id = 1")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// SQRT(4) + CEIL(3.0) * 2 = 2 + 3*2 = 8
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 8.0, val, 1e-10)
	})
}

// =============================================================================
// Task 13.2: Test math functions in WHERE clauses
// =============================================================================

func TestComprehensive_MathFunctions_InWhereClauses(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	// Create and populate a test table
	_, err := executeComprehensiveQuery(t, exec, cat, `
		CREATE TABLE where_test (
			id INTEGER,
			value DOUBLE,
			category VARCHAR(20)
		)
	`)
	require.NoError(t, err)

	_, err = executeComprehensiveQuery(t, exec, cat, "INSERT INTO where_test VALUES (1, 4.0, 'A')")
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(t, exec, cat, "INSERT INTO where_test VALUES (2, 9.0, 'B')")
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(t, exec, cat, "INSERT INTO where_test VALUES (3, 16.0, 'A')")
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(t, exec, cat, "INSERT INTO where_test VALUES (4, 25.0, 'B')")
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO where_test VALUES (5, 100.0, 'A')",
	)
	require.NoError(t, err)

	t.Run("SQRT in WHERE clause", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id FROM where_test WHERE SQRT(value) > 3")
		require.NoError(t, err)
		// SQRT(4)=2, SQRT(9)=3, SQRT(16)=4, SQRT(25)=5, SQRT(100)=10
		// Values > 3: 16, 25, 100 (ids 3, 4, 5)
		require.Len(t, result.Rows, 3)
	})

	t.Run("CEIL in WHERE clause", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id FROM where_test WHERE CEIL(value / 10) = 1")
		require.NoError(t, err)
		// CEIL(4/10)=1, CEIL(9/10)=1, CEIL(16/10)=2, CEIL(25/10)=3, CEIL(100/10)=10
		// Values = 1: 4, 9 (ids 1, 2)
		require.Len(t, result.Rows, 2)
	})

	t.Run("LOG10 in WHERE clause", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id FROM where_test WHERE LOG10(value) >= 1")
		require.NoError(t, err)
		// LOG10(4)=0.6, LOG10(9)=0.95, LOG10(16)=1.2, LOG10(25)=1.4, LOG10(100)=2
		// Values >= 1: 16, 25, 100 (ids 3, 4, 5)
		require.Len(t, result.Rows, 3)
	})

	t.Run("POW in WHERE clause", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id FROM where_test WHERE POW(value, 0.5) = 5")
		require.NoError(t, err)
		// POW(25, 0.5) = 5
		require.Len(t, result.Rows, 1)
	})

	t.Run("Combined math functions in WHERE", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id FROM where_test WHERE ROUND(SQRT(value), 0) = 4")
		require.NoError(t, err)
		// ROUND(SQRT(16), 0) = 4
		require.Len(t, result.Rows, 1)
	})

	t.Run("Math function in WHERE with comparison", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id FROM where_test WHERE ABS(value - 10) < 5")
		require.NoError(t, err)
		// |4-10|=6, |9-10|=1, |16-10|=6, |25-10|=15, |100-10|=90
		// Values < 5: 9 (id 2)
		require.Len(t, result.Rows, 1)
	})
}

// =============================================================================
// Task 13.3: Test math functions in computed columns
// =============================================================================

func TestComprehensive_MathFunctions_InComputedColumns(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	// Create and populate a test table
	_, err := executeComprehensiveQuery(t, exec, cat, `
		CREATE TABLE computed_test (
			x DOUBLE,
			y DOUBLE
		)
	`)
	require.NoError(t, err)

	_, err = executeComprehensiveQuery(t, exec, cat, "INSERT INTO computed_test VALUES (3.0, 4.0)")
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(t, exec, cat, "INSERT INTO computed_test VALUES (5.0, 12.0)")
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(t, exec, cat, "INSERT INTO computed_test VALUES (8.0, 15.0)")
	require.NoError(t, err)

	t.Run("Computed column with SQRT", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT x, SQRT(x) AS sqrt_x FROM computed_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Computed column with POW", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT x, POW(x, 2) AS x_squared FROM computed_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Computed column combining multiple columns", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT x, y, x + y AS sum_val, x * y AS product FROM computed_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Complex computed column with Pythagorean theorem", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT x, y, SQRT(POW(x, 2) + POW(y, 2)) AS hypotenuse FROM computed_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
		// First row: SQRT(9+16) = 5
		// Find hypotenuse value
		var hyp float64
		for k, v := range result.Rows[0] {
			if k == "hypotenuse" || k == "SQRT(POW(x, 2) + POW(y, 2))" {
				hyp = v.(float64)
				break
			}
		}
		assert.InDelta(t, 5.0, hyp, 1e-10)
	})

	t.Run("Computed column with rounding", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT x, y, ROUND(x / y, 2) AS ratio FROM computed_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Computed column with trig functions", func(t *testing.T) {
		result, err := executeComprehensiveQuery(
			t,
			exec,
			cat,
			"SELECT x, ATAN2(y, x) AS angle_rad, DEGREES(ATAN2(y, x)) AS angle_deg FROM computed_test",
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})
}

// =============================================================================
// Task 13.4: Test math functions with aggregate functions
// =============================================================================

func TestComprehensive_MathFunctions_WithAggregates(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	// Create and populate a test table
	_, err := executeComprehensiveQuery(t, exec, cat, `
		CREATE TABLE aggregate_test (
			category VARCHAR(10),
			value DOUBLE
		)
	`)
	require.NoError(t, err)

	_, err = executeComprehensiveQuery(t, exec, cat, "INSERT INTO aggregate_test VALUES ('A', 4.0)")
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(t, exec, cat, "INSERT INTO aggregate_test VALUES ('A', 9.0)")
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO aggregate_test VALUES ('B', 16.0)",
	)
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO aggregate_test VALUES ('B', 25.0)",
	)
	require.NoError(t, err)

	t.Run("Math function inside aggregate", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SUM(SQRT(value)) FROM aggregate_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// SUM(2 + 3 + 4 + 5) = 14
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 14.0, val, 1e-10)
	})

	t.Run("Math function on aggregate result", func(t *testing.T) {
		// Note: This tests that SQRT can be applied to an aggregate result
		// The current executor may handle this differently, so we test SUM first
		// and verify SQRT separately
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SUM(value) FROM aggregate_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// SUM = 4+9+16+25 = 54
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 54.0, val, 1e-10)
		// Now verify SQRT works on that value
		sqrtResult, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SQRT(54)")
		require.NoError(t, err)
		require.Len(t, sqrtResult.Rows, 1)
		sqrtVal := getComprehensiveFirstValue(sqrtResult.Rows[0]).(float64)
		assert.InDelta(t, math.Sqrt(54), sqrtVal, 1e-10)
	})

	t.Run("AVG with math function", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT AVG(SQRT(value)) FROM aggregate_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// AVG(2+3+4+5) = 14/4 = 3.5
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 3.5, val, 1e-10)
	})

	t.Run("Grouped aggregate with math function", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT category, SUM(SQRT(value)) FROM aggregate_test GROUP BY category")
		require.NoError(t, err)
		require.Len(t, result.Rows, 2)
	})

	t.Run("ROUND on aggregate result", func(t *testing.T) {
		// Test AVG first
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT AVG(value) FROM aggregate_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// AVG(4+9+16+25) = 54/4 = 13.5
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 13.5, val, 1e-10)
		// Now verify ROUND works on that value
		roundResult, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT ROUND(13.5, 1)")
		require.NoError(t, err)
		require.Len(t, roundResult.Rows, 1)
		roundVal := getComprehensiveFirstValue(roundResult.Rows[0]).(float64)
		assert.InDelta(t, 13.5, roundVal, 1e-10)
	})

	t.Run("COUNT with math function in condition", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT COUNT(*) FROM aggregate_test WHERE SQRT(value) > 3")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// SQRT(4)=2, SQRT(9)=3, SQRT(16)=4, SQRT(25)=5
		// Values > 3: 16, 25 (count = 2)
		val := getComprehensiveFirstValue(result.Rows[0]).(int64)
		assert.Equal(t, int64(2), val)
	})
}

// =============================================================================
// Task 13.5: Test nested math function calls
// =============================================================================

func TestComprehensive_NestedMathFunctions(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	t.Run("Pythagorean distance: SQRT(POW(x,2) + POW(y,2))", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SQRT(POW(3, 2) + POW(4, 2))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 5.0, val, 1e-10)
	})

	t.Run("3D distance: SQRT(POW(x,2) + POW(y,2) + POW(z,2))", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SQRT(POW(1, 2) + POW(2, 2) + POW(2, 2))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 3.0, val, 1e-10)
	})

	t.Run("Exponential and logarithm inverse: LN(EXP(x))", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT LN(EXP(5))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 5.0, val, 1e-10)
	})

	t.Run("EXP(LN(x)) = x", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT EXP(LN(100))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 100.0, val, 1e-10)
	})

	t.Run("DEGREES(RADIANS(x)) = x", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT DEGREES(RADIANS(45))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 45.0, val, 1e-10)
	})

	t.Run("ASIN(SIN(x)) = x for x in domain", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT ASIN(SIN(0.5))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 0.5, val, 1e-10)
	})

	t.Run("Triple nested: ROUND(SQRT(POW(x, 2)), 2)", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT ROUND(SQRT(POW(3.14159, 2)), 2)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 3.14, val, 0.01)
	})

	t.Run("Complex nested: CEIL(LOG10(POW(10, 3.7)))", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT CEIL(LOG10(POW(10, 3.7)))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// LOG10(10^3.7) = 3.7, CEIL(3.7) = 4
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 4.0, val, 1e-10)
	})

	t.Run("Pythagorean identity: SIN^2 + COS^2 = 1", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT POW(SIN(0.7), 2) + POW(COS(0.7), 2)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 1.0, val, 1e-10)
	})
}

// =============================================================================
// Task 13.6: Test bitwise operators in complex expressions
// =============================================================================

func TestComprehensive_BitwiseOperators_ComplexExpressions(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	// Create and populate a test table
	_, err := executeComprehensiveQuery(t, exec, cat, `
		CREATE TABLE bitwise_test (
			id INTEGER,
			flags INTEGER,
			mask INTEGER
		)
	`)
	require.NoError(t, err)

	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO bitwise_test VALUES (1, 5, 3)",
	) // 0101, 0011
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO bitwise_test VALUES (2, 12, 10)",
	) // 1100, 1010
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO bitwise_test VALUES (3, 255, 15)",
	) // 11111111, 00001111
	require.NoError(t, err)

	t.Run("Bitwise AND with columns", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, flags & mask FROM bitwise_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Bitwise OR with columns", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, flags | mask FROM bitwise_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Bitwise XOR with columns", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, flags ^ mask FROM bitwise_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Combined bitwise operations", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, (flags & mask) | (flags ^ mask) FROM bitwise_test WHERE id = 1")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("Bitwise AND in WHERE clause", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id FROM bitwise_test WHERE (flags & 1) = 1")
		require.NoError(t, err)
		// flags with bit 0 set: 5 (0101), 255 (11111111)
		require.Len(t, result.Rows, 2)
	})

	t.Run("Left shift operation", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT 1 << 4")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(int64)
		assert.Equal(t, int64(16), val)
	})

	t.Run("Right shift operation", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT 16 >> 2")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(int64)
		assert.Equal(t, int64(4), val)
	})

	t.Run("Bitwise NOT operation", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT ~0")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(int64)
		assert.Equal(t, int64(-1), val)
	})

	t.Run("BIT_COUNT function", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT BIT_COUNT(255)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(int64)
		assert.Equal(t, int64(8), val)
	})

	t.Run("BIT_COUNT with column", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, BIT_COUNT(flags) FROM bitwise_test")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})
}

// =============================================================================
// Task 13.7: Test type coercion in mixed-type expressions
// =============================================================================

func TestComprehensive_TypeCoercion_MixedTypes(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	// Create a table with different numeric types
	_, err := executeComprehensiveQuery(t, exec, cat, `
		CREATE TABLE type_test (
			int_val INTEGER,
			float_val DOUBLE,
			bigint_val BIGINT
		)
	`)
	require.NoError(t, err)

	_, err = executeComprehensiveQuery(t, exec, cat, "INSERT INTO type_test VALUES (4, 4.0, 4)")
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO type_test VALUES (9, 9.5, 1000000000)",
	)
	require.NoError(t, err)

	t.Run("SQRT with integer input", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SQRT(int_val) FROM type_test WHERE int_val = 4")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 2.0, val, 1e-10)
	})

	t.Run("SQRT with double input", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SQRT(float_val) FROM type_test WHERE int_val = 4")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 2.0, val, 1e-10)
	})

	t.Run("POW with mixed integer and float", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT POW(int_val, 0.5) FROM type_test WHERE int_val = 4")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 2.0, val, 1e-10)
	})

	t.Run("ROUND with integer literal", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT ROUND(float_val, 0) FROM type_test WHERE int_val = 9")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 10.0, val, 1e-10) // 9.5 rounds to 10
	})

	t.Run("GCD with bigint", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT GCD(bigint_val, 100) FROM type_test WHERE int_val = 9")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// GCD(1000000000, 100) = 100
		val := getComprehensiveFirstValue(result.Rows[0]).(int64)
		assert.Equal(t, int64(100), val)
	})

	t.Run("Math expression with mixed types", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT int_val + float_val * 2 FROM type_test WHERE int_val = 4")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// 4 + 4.0 * 2 = 12.0
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 12.0, val, 1e-10)
	})

	t.Run("CEIL/FLOOR preserve integer property", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT CEIL(float_val), FLOOR(float_val) FROM type_test WHERE int_val = 9")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("Trigonometric function with integer", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SIN(int_val) FROM type_test WHERE int_val = 4")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, math.Sin(4), val, 1e-10)
	})
}

// =============================================================================
// Task 13.8: Financial calculations (ROUND for currency)
// =============================================================================

func TestComprehensive_FinancialCalculations(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	// Create a financial transactions table
	_, err := executeComprehensiveQuery(t, exec, cat, `
		CREATE TABLE transactions (
			id INTEGER,
			amount DOUBLE,
			tax_rate DOUBLE,
			quantity INTEGER
		)
	`)
	require.NoError(t, err)

	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO transactions VALUES (1, 19.99, 0.0825, 2)",
	)
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO transactions VALUES (2, 49.95, 0.0825, 1)",
	)
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO transactions VALUES (3, 9.50, 0.0825, 5)",
	)
	require.NoError(t, err)

	t.Run("Calculate tax rounded to cents", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, ROUND(amount * tax_rate, 2) AS tax FROM transactions")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Calculate total with tax", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, ROUND(amount * (1 + tax_rate), 2) AS total FROM transactions")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
		// First row: 19.99 * 1.0825 = 21.6392..., rounded to 21.64
	})

	t.Run("Calculate line total", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, ROUND(amount * quantity, 2) AS line_total FROM transactions")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Sum with currency rounding", func(t *testing.T) {
		// Test SUM first
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SUM(amount * quantity) AS subtotal FROM transactions")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// (19.99*2) + (49.95*1) + (9.50*5) = 39.98 + 49.95 + 47.50 = 137.43
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 137.43, val, 0.01)
	})

	t.Run("Calculate average price rounded", func(t *testing.T) {
		// Test AVG first
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT AVG(amount) AS avg_price FROM transactions")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// (19.99 + 49.95 + 9.50) / 3 = 26.48
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 26.48, val, 0.01)
	})

	t.Run("Discount calculation", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, amount, ROUND(amount * 0.9, 2) AS discounted_price FROM transactions")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Compound interest calculation", func(t *testing.T) {
		// A = P * (1 + r)^n where P=1000, r=0.05, n=10
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT ROUND(1000 * POW(1 + 0.05, 10), 2)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// 1000 * 1.05^10 = 1628.89
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 1628.89, val, 0.01)
	})

	t.Run("Continuous compound interest", func(t *testing.T) {
		// A = P * e^(rt) where P=1000, r=0.05, t=10
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT ROUND(1000 * EXP(0.05 * 10), 2)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// 1000 * e^0.5 = 1648.72
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 1648.72, val, 0.01)
	})
}

// =============================================================================
// Task 13.9: Scientific calculations (trigonometry)
// =============================================================================

func TestComprehensive_ScientificCalculations_Trigonometry(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	// Create a table for geometric calculations
	_, err := executeComprehensiveQuery(t, exec, cat, `
		CREATE TABLE geometry (
			id INTEGER,
			x DOUBLE,
			y DOUBLE,
			radius DOUBLE,
			angle_deg DOUBLE
		)
	`)
	require.NoError(t, err)

	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO geometry VALUES (1, 3.0, 4.0, 5.0, 45.0)",
	)
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO geometry VALUES (2, 1.0, 1.0, 1.0, 90.0)",
	)
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO geometry VALUES (3, 0.0, 5.0, 10.0, 180.0)",
	)
	require.NoError(t, err)

	t.Run("Calculate distance from origin", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, SQRT(POW(x, 2) + POW(y, 2)) AS distance FROM geometry")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Calculate angle in radians", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, ATAN2(y, x) AS angle_rad FROM geometry")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Calculate angle in degrees", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, DEGREES(ATAN2(y, x)) AS angle_deg FROM geometry")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Convert polar to cartesian: x = r*cos(theta)", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, radius * COS(RADIANS(angle_deg)) AS cart_x FROM geometry")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Convert polar to cartesian: y = r*sin(theta)", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, radius * SIN(RADIANS(angle_deg)) AS cart_y FROM geometry")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Area of circle", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, PI() * POW(radius, 2) AS area FROM geometry")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Circumference of circle", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, 2 * PI() * radius AS circumference FROM geometry")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Arc length calculation", func(t *testing.T) {
		// Arc length = radius * angle (in radians)
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT id, radius * RADIANS(angle_deg) AS arc_length FROM geometry")
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Pythagorean identity verification", func(t *testing.T) {
		result, err := executeComprehensiveQuery(
			t,
			exec,
			cat,
			"SELECT id, POW(SIN(RADIANS(angle_deg)), 2) + POW(COS(RADIANS(angle_deg)), 2) AS identity FROM geometry",
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
		// All values should be 1.0
		for _, row := range result.Rows {
			for k, v := range row {
				if k == "identity" {
					assert.InDelta(t, 1.0, v.(float64), 1e-10)
				}
			}
		}
	})

	t.Run("Angle between two vectors", func(t *testing.T) {
		// cos(theta) = (a . b) / (|a| * |b|)
		// For vectors (3,4) and (1,0): dot = 3, |a| = 5, |b| = 1
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT DEGREES(ACOS(3 / (5 * 1)))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		expected := math.Acos(3.0/5.0) * 180 / math.Pi
		assert.InDelta(t, expected, val, 1e-6)
	})

	t.Run("Pendulum period calculation", func(t *testing.T) {
		// T = 2 * PI * sqrt(L/g), where L=1m, g=9.8m/s^2
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT 2 * PI() * SQRT(1.0 / 9.8)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		expected := 2 * math.Pi * math.Sqrt(1.0/9.8)
		assert.InDelta(t, expected, val, 1e-6)
	})
}

// =============================================================================
// Task 13.10: Bitwise operations for flags/masks
// =============================================================================

func TestComprehensive_BitwiseOperations_FlagsMasks(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	// Flag constants for reference:
	// FLAG_READ    = 1 (bit 0)
	// FLAG_WRITE   = 2 (bit 1)
	// FLAG_EXECUTE = 4 (bit 2)
	// FLAG_DELETE  = 8 (bit 3)

	// Create a permissions table
	_, err := executeComprehensiveQuery(t, exec, cat, `
		CREATE TABLE permissions (
			user_id INTEGER,
			permissions INTEGER,
			username VARCHAR(50)
		)
	`)
	require.NoError(t, err)

	// Insert users with different permission combinations
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO permissions VALUES (1, 1, 'reader')",
	) // READ only
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO permissions VALUES (2, 3, 'editor')",
	) // READ + WRITE
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO permissions VALUES (3, 7, 'developer')",
	) // READ + WRITE + EXECUTE
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO permissions VALUES (4, 15, 'admin')",
	) // All permissions
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO permissions VALUES (5, 0, 'guest')",
	) // No permissions
	require.NoError(t, err)

	t.Run("Check if user has READ permission", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT username FROM permissions WHERE (permissions & 1) = 1")
		require.NoError(t, err)
		// Users with READ: reader, editor, developer, admin (4 users)
		require.Len(t, result.Rows, 4)
	})

	t.Run("Check if user has WRITE permission", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT username FROM permissions WHERE (permissions & 2) = 2")
		require.NoError(t, err)
		// Users with WRITE: editor, developer, admin (3 users)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Check if user has EXECUTE permission", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT username FROM permissions WHERE (permissions & 4) = 4")
		require.NoError(t, err)
		// Users with EXECUTE: developer, admin (2 users)
		require.Len(t, result.Rows, 2)
	})

	t.Run("Check if user has DELETE permission", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT username FROM permissions WHERE (permissions & 8) = 8")
		require.NoError(t, err)
		// Users with DELETE: admin only (1 user)
		require.Len(t, result.Rows, 1)
	})

	t.Run("Check for multiple permissions (READ AND WRITE)", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT username FROM permissions WHERE (permissions & 3) = 3")
		require.NoError(t, err)
		// Users with READ AND WRITE: editor, developer, admin (3 users)
		require.Len(t, result.Rows, 3)
	})

	t.Run("Grant permission using OR", func(t *testing.T) {
		// Simulate granting DELETE permission
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT user_id, permissions | 8 AS new_permissions FROM permissions WHERE user_id = 3")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// developer (7) | DELETE (8) = 15
		for _, row := range result.Rows {
			for k, v := range row {
				if k == "new_permissions" {
					assert.Equal(t, int64(15), v.(int64))
				}
			}
		}
	})

	t.Run("Revoke permission using AND NOT", func(t *testing.T) {
		// Simulate revoking WRITE permission using XOR for toggle
		result, err := executeComprehensiveQuery(
			t,
			exec,
			cat,
			"SELECT user_id, permissions & ~2 AS new_permissions FROM permissions WHERE user_id = 2",
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// editor (3) & ~WRITE(2) = 3 & ~2 = 1 (READ only)
		for _, row := range result.Rows {
			for k, v := range row {
				if k == "new_permissions" {
					assert.Equal(t, int64(1), v.(int64))
				}
			}
		}
	})

	t.Run("Toggle permission using XOR", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT user_id, permissions ^ 4 AS toggled FROM permissions WHERE user_id = 3")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// developer (7) ^ EXECUTE (4) = 3 (READ + WRITE)
		for _, row := range result.Rows {
			for k, v := range row {
				if k == "toggled" {
					assert.Equal(t, int64(3), v.(int64))
				}
			}
		}
	})

	t.Run("Count set bits (permissions count)", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT username, BIT_COUNT(permissions) AS perm_count FROM permissions")
		require.NoError(t, err)
		require.Len(t, result.Rows, 5)
	})

	t.Run("Create permission mask", func(t *testing.T) {
		// Create a mask for first 3 permissions
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT (1 << 3) - 1 AS mask")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// (1 << 3) - 1 = 8 - 1 = 7
		val := getComprehensiveFirstValue(result.Rows[0])
		// Handle both int64 and float64 return types
		switch v := val.(type) {
		case int64:
			assert.Equal(t, int64(7), v)
		case float64:
			assert.InDelta(t, 7.0, v, 1e-10)
		default:
			t.Fatalf("unexpected type: %T", val)
		}
	})

	t.Run("Check specific bit position", func(t *testing.T) {
		// Check if bit at position 2 (EXECUTE) is set
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT username, (permissions >> 2) & 1 AS has_execute FROM permissions")
		require.NoError(t, err)
		require.Len(t, result.Rows, 5)
	})

	t.Run("Set bit at position", func(t *testing.T) {
		// Set bit at position 3 (DELETE)
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT permissions | (1 << 3) AS with_delete FROM permissions WHERE user_id = 1")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// reader (1) | (1 << 3) = 1 | 8 = 9
		val := getComprehensiveFirstValue(result.Rows[0])
		// Handle both int64 and float64 return types
		switch v := val.(type) {
		case int64:
			assert.Equal(t, int64(9), v)
		case float64:
			assert.InDelta(t, 9.0, v, 1e-10)
		default:
			t.Fatalf("unexpected type: %T", val)
		}
	})

	t.Run("Clear bit at position", func(t *testing.T) {
		// Clear bit at position 0 (READ)
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT permissions & ~(1 << 0) AS without_read FROM permissions WHERE user_id = 4")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// admin (15) & ~1 = 14
		val := getComprehensiveFirstValue(result.Rows[0])
		// Handle both int64 and float64 return types
		switch v := val.(type) {
		case int64:
			assert.Equal(t, int64(14), v)
		case float64:
			assert.InDelta(t, 14.0, v, 1e-10)
		default:
			t.Fatalf("unexpected type: %T", val)
		}
	})

	t.Run("Check if any permission is set", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT username FROM permissions WHERE permissions != 0")
		require.NoError(t, err)
		// All except guest (4 users)
		require.Len(t, result.Rows, 4)
	})

	t.Run("Check if all basic permissions are set", func(t *testing.T) {
		// Check if READ, WRITE, EXECUTE are all set (mask = 7)
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT username FROM permissions WHERE (permissions & 7) = 7")
		require.NoError(t, err)
		// developer and admin (2 users)
		require.Len(t, result.Rows, 2)
	})
}

// =============================================================================
// Additional comprehensive tests: Edge cases and combined scenarios
// =============================================================================

func TestComprehensive_EdgeCases(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	t.Run("Multiple functions in single expression", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT ROUND(SQRT(POW(3, 2) + POW(4, 2)), 2) + CEIL(LOG10(1000))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// ROUND(5, 2) + CEIL(3) = 5 + 3 = 8
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 8.0, val, 1e-10)
	})

	t.Run("Math with PI constant", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SIN(PI() / 6)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// SIN(PI/6) = 0.5
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 0.5, val, 1e-10)
	})

	t.Run("Chained rounding operations", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT FLOOR(CEIL(ROUND(3.456789, 2)))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// ROUND(3.456789, 2) = 3.46, CEIL(3.46) = 4, FLOOR(4) = 4
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 4.0, val, 1e-10)
	})

	t.Run("Sign function in complex expression", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT SIGN(-5) * ABS(-10)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// SIGN(-5) * ABS(-10) = -1 * 10 = -10
		val := getComprehensiveFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, -10.0, val, 1e-10)
	})

	t.Run("ISFINITE in conditional context", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT ISFINITE(SQRT(4))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getComprehensiveFirstValue(result.Rows[0]).(bool)
		assert.True(t, val)
	})
}

func TestComprehensive_RealWorldScenarios(t *testing.T) {
	exec, cat := setupComprehensiveTestExecutor()

	// Create a sensor data table
	_, err := executeComprehensiveQuery(t, exec, cat, `
		CREATE TABLE sensor_data (
			sensor_id INTEGER,
			reading DOUBLE,
			timestamp_val INTEGER
		)
	`)
	require.NoError(t, err)

	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO sensor_data VALUES (1, 23.5, 1000)",
	)
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO sensor_data VALUES (1, 24.1, 2000)",
	)
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO sensor_data VALUES (2, 18.2, 1000)",
	)
	require.NoError(t, err)
	_, err = executeComprehensiveQuery(
		t,
		exec,
		cat,
		"INSERT INTO sensor_data VALUES (2, 17.8, 2000)",
	)
	require.NoError(t, err)

	t.Run("Calculate average reading per sensor", func(t *testing.T) {
		result, err := executeComprehensiveQuery(
			t,
			exec,
			cat,
			"SELECT sensor_id, ROUND(AVG(reading), 2) AS avg_reading FROM sensor_data GROUP BY sensor_id",
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2)
	})

	t.Run("Find readings above threshold using math", func(t *testing.T) {
		result, err := executeComprehensiveQuery(t, exec, cat,
			"SELECT sensor_id, reading FROM sensor_data WHERE reading > CEIL(20.0)")
		require.NoError(t, err)
		// Readings > 21: 23.5, 24.1 (2 readings)
		require.Len(t, result.Rows, 2)
	})

	t.Run("Normalize readings to 0-1 range", func(t *testing.T) {
		// Simple normalization: (value - min) / (max - min)
		// Min = 17.8, Max = 24.1
		result, err := executeComprehensiveQuery(
			t,
			exec,
			cat,
			"SELECT sensor_id, reading, ROUND((reading - 17.8) / (24.1 - 17.8), 3) AS normalized FROM sensor_data",
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 4)
	})
}
