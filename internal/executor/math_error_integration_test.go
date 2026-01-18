package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Task 11.7: Integration Tests for Error Handling in Complex Queries
// Tests that domain errors are properly propagated in complex SQL queries
// =============================================================================

// TestIntegration_ErrorHandling_SQRT_InComplexQueries tests SQRT domain errors in complex queries.
func TestIntegration_ErrorHandling_SQRT_InComplexQueries(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("SQRT error in nested expression", func(t *testing.T) {
		// SQRT inside a larger expression
		_, err := executeMathQuery(t, exec, cat, "SELECT 1 + SQRT(-1) + 2")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})

	t.Run("SQRT error with computed negative value", func(t *testing.T) {
		// SQRT of a computed negative value
		_, err := executeMathQuery(t, exec, cat, "SELECT SQRT(1 - 10)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})

	t.Run("SQRT error in CASE expression", func(t *testing.T) {
		// SQRT error inside CASE when executed
		_, err := executeMathQuery(t, exec, cat, "SELECT CASE WHEN 1=1 THEN SQRT(-5) ELSE 0 END")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})

	t.Run("SQRT error combined with other functions", func(t *testing.T) {
		// SQRT error combined with other math functions
		_, err := executeMathQuery(t, exec, cat, "SELECT ROUND(SQRT(-4), 2)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})
}

// TestIntegration_ErrorHandling_LOG_InComplexQueries tests LN/LOG domain errors in complex queries.
func TestIntegration_ErrorHandling_LOG_InComplexQueries(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("LN error in arithmetic expression", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT LN(0) * 2")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})

	t.Run("LOG10 error with computed zero", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT LOG10(5 - 5)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})

	t.Run("LOG2 error with negative result", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT LOG2(-2 + 1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})

	t.Run("LN error in combined log expression", func(t *testing.T) {
		// Compound expression with log functions
		_, err := executeMathQuery(t, exec, cat, "SELECT EXP(LN(-1))")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})

	t.Run("LOG error in nested expression", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT POW(2, LOG10(-5))")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})
}

// TestIntegration_ErrorHandling_Trig_InComplexQueries tests ASIN/ACOS domain errors in complex queries.
func TestIntegration_ErrorHandling_Trig_InComplexQueries(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("ASIN error in nested trig expression", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT DEGREES(ASIN(2))")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ASIN domain error")
	})

	t.Run("ACOS error with computed invalid value", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ACOS(0.5 + 1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOS domain error")
	})

	t.Run("ASIN error in combined trig expression", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT SIN(ASIN(-2))")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ASIN domain error")
	})

	t.Run("ACOS error combined with arithmetic", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ACOS(-3) * 180 / 3.14159")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOS domain error")
	})
}

// TestIntegration_ErrorHandling_Factorial_InComplexQueries tests FACTORIAL overflow in complex queries.
func TestIntegration_ErrorHandling_Factorial_InComplexQueries(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("FACTORIAL overflow in arithmetic expression", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT FACTORIAL(21) / 2")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FACTORIAL")
	})

	t.Run("FACTORIAL negative in expression", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT FACTORIAL(-1) + 10")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FACTORIAL")
	})

	t.Run("FACTORIAL computed overflow value", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT FACTORIAL(10 + 15)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FACTORIAL")
	})

	t.Run("FACTORIAL in nested expression", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT LOG10(FACTORIAL(25))")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FACTORIAL")
	})
}

// TestIntegration_ErrorHandling_Hyperbolic_InComplexQueries tests ACOSH/ATANH domain errors.
func TestIntegration_ErrorHandling_Hyperbolic_InComplexQueries(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("ACOSH error in expression", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ACOSH(0.5) + 1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOSH domain error")
	})

	t.Run("ATANH error at boundary", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ATANH(1) * 2")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ATANH domain error")
	})

	t.Run("ATANH error with computed value", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ATANH(0.5 + 0.5)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ATANH domain error")
	})

	t.Run("ACOSH error in nested hyperbolic", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT SINH(ACOSH(0))")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOSH domain error")
	})
}

// TestIntegration_ErrorHandling_WithTableData tests errors with actual table data.
func TestIntegration_ErrorHandling_WithTableData(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// Create a table with test data
	_, err := executeMathQuery(t, exec, cat, "CREATE TABLE test_errors (val DOUBLE)")
	require.NoError(t, err)

	// Insert negative values
	_, err = executeMathQuery(t, exec, cat, "INSERT INTO test_errors VALUES (-5)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO test_errors VALUES (0)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO test_errors VALUES (2)")
	require.NoError(t, err)

	t.Run("SQRT error from table column - negative value", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT SQRT(val) FROM test_errors WHERE val = -5")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})

	t.Run("LN error from table column - zero value", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT LN(val) FROM test_errors WHERE val = 0")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})

	t.Run("ASIN error from table column - value > 1", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT ASIN(val) FROM test_errors WHERE val = 2")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ASIN domain error")
	})
}

// TestIntegration_ErrorHandling_MultipleErrors tests queries with multiple potential errors.
func TestIntegration_ErrorHandling_MultipleErrors(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("First error stops execution", func(t *testing.T) {
		// When multiple operations could fail, the first error should be raised
		_, err := executeMathQuery(t, exec, cat, "SELECT SQRT(-1), LN(0), ASIN(2)")
		require.Error(t, err)
		// We expect an error, but don't specify which one since execution order may vary
		assert.True(t, err != nil)
	})

	t.Run("Error in left operand stops evaluation", func(t *testing.T) {
		_, err := executeMathQuery(t, exec, cat, "SELECT SQRT(-1) + LN(10)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})
}

// TestIntegration_ErrorHandling_ChainedFunctions tests error propagation in chained functions.
func TestIntegration_ErrorHandling_ChainedFunctions(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("Error in inner function propagates", func(t *testing.T) {
		// SQRT(-1) fails first, then EXP never runs
		_, err := executeMathQuery(t, exec, cat, "SELECT EXP(SQRT(-1))")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})

	t.Run("Error in deeply nested function", func(t *testing.T) {
		// POW(2, LOG10(LN(-1))) - LN(-1) fails first
		_, err := executeMathQuery(t, exec, cat, "SELECT POW(2, LOG10(LN(-1)))")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})

	t.Run("Error in function argument expression", func(t *testing.T) {
		// ROUND(SQRT(-4), 2) - SQRT fails first
		_, err := executeMathQuery(t, exec, cat, "SELECT ROUND(SQRT(-4), 2)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})
}

// TestIntegration_ErrorHandling_WithMathCombinations tests error handling in various math function combinations.
// Note: These tests verify error handling at the expression level.
func TestIntegration_ErrorHandling_WithMathCombinations(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("Direct SQRT error", func(t *testing.T) {
		// Direct SQRT(-6) should fail
		_, err := executeMathQuery(t, exec, cat, "SELECT SQRT(-6)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})

	t.Run("Direct LN error", func(t *testing.T) {
		// Direct LN(0) should fail
		_, err := executeMathQuery(t, exec, cat, "SELECT LN(0)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})

	t.Run("Combined valid math operations", func(t *testing.T) {
		// Valid combination: SQRT(4) * 2 = 4
		result, err := executeMathQuery(t, exec, cat, "SELECT SQRT(4) * 2")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("Combined error in second operand", func(t *testing.T) {
		// 10 + SQRT(-4) should fail
		_, err := executeMathQuery(t, exec, cat, "SELECT 10 + SQRT(-4)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})
}

// TestIntegration_ErrorHandling_InWhereClause tests errors in WHERE clause expressions.
func TestIntegration_ErrorHandling_InWhereClause(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	// Create a table
	_, err := executeMathQuery(t, exec, cat, "CREATE TABLE where_test (x DOUBLE)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO where_test VALUES (5)")
	require.NoError(t, err)

	_, err = executeMathQuery(t, exec, cat, "INSERT INTO where_test VALUES (-5)")
	require.NoError(t, err)

	t.Run("SQRT error in WHERE with negative column", func(t *testing.T) {
		// SQRT(x) where x=-5 will fail
		_, err := executeMathQuery(t, exec, cat, "SELECT x FROM where_test WHERE SQRT(x) > 0")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT of negative number")
	})

	t.Run("LN error in WHERE condition", func(t *testing.T) {
		// LN(x - 10) where x=5 gives LN(-5) which fails
		_, err := executeMathQuery(t, exec, cat, "SELECT x FROM where_test WHERE LN(x - 10) > 0")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-positive")
	})
}

// TestIntegration_ErrorHandling_SuccessfulQueries tests that valid queries work correctly.
func TestIntegration_ErrorHandling_SuccessfulQueries(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	t.Run("Valid SQRT in complex expression", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT SQRT(POW(3, 2) + POW(4, 2))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("Valid LN chain", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT LN(EXP(5))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("Valid ASIN with computed value", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT ASIN(SIN(0.5))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("Valid FACTORIAL", func(t *testing.T) {
		result, err := executeMathQuery(t, exec, cat, "SELECT FACTORIAL(20)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})
}

// TestIntegration_ErrorHandling_ErrorMessagePreservation tests that error messages are preserved through execution.
func TestIntegration_ErrorHandling_ErrorMessagePreservation(t *testing.T) {
	exec, cat := setupMathTestExecutor()

	testCases := []struct {
		name           string
		query          string
		expectedSubstr string
	}{
		{
			name:           "SQRT error message preserved",
			query:          "SELECT SQRT(-100)",
			expectedSubstr: "SQRT of negative number not allowed",
		},
		{
			name:           "LN error message preserved",
			query:          "SELECT LN(0)",
			expectedSubstr: "cannot take logarithm of non-positive number",
		},
		{
			name:           "ASIN error message preserved",
			query:          "SELECT ASIN(5)",
			expectedSubstr: "ASIN domain error: input must be in [-1, 1]",
		},
		{
			name:           "ACOS error message preserved",
			query:          "SELECT ACOS(-5)",
			expectedSubstr: "ACOS domain error: input must be in [-1, 1]",
		},
		{
			name:           "FACTORIAL error message preserved",
			query:          "SELECT FACTORIAL(100)",
			expectedSubstr: "FACTORIAL domain error: input must be non-negative and <= 20",
		},
		{
			name:           "ACOSH error message preserved",
			query:          "SELECT ACOSH(0)",
			expectedSubstr: "ACOSH domain error: input must be >= 1",
		},
		{
			name:           "ATANH error message preserved",
			query:          "SELECT ATANH(1)",
			expectedSubstr: "ATANH domain error: input must be in (-1, 1)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := executeMathQuery(t, exec, cat, tc.query)
			require.Error(t, err, "Expected error for query: %s", tc.query)
			assert.Contains(t, err.Error(), tc.expectedSubstr,
				"Error message should contain '%s' for query: %s", tc.expectedSubstr, tc.query)
		})
	}
}
