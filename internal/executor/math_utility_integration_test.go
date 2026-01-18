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
// Math Utility Functions Integration Tests
// Tests for tasks 6.8-6.13 of the essential math functions implementation
// =============================================================================

// setupUtilityTestExecutor creates an executor for utility function testing
func setupUtilityTestExecutor() (*Executor, *catalog.Catalog) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	return exec, cat
}

// executeUtilityQuery executes a SQL query and returns the result
func executeUtilityQuery(
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

// getUtilityFirstValue extracts the first value from a result row
func getUtilityFirstValue(row map[string]any) any {
	for _, v := range row {
		return v
	}
	return nil
}

// =============================================================================
// Task 6.8: Test PI, RANDOM cases in expression evaluator
// =============================================================================

func TestUtilityIntegration_PI_Extended(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	t.Run("PI in arithmetic expression", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT 2 * PI()")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.InDelta(t, 2*math.Pi, val, 1e-10, "2 * PI() should return 2*Pi")
	})

	t.Run("PI in circumference calculation", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT 2 * PI() * 5")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.InDelta(t, 2*math.Pi*5, val, 1e-10)
	})
}

func TestUtilityIntegration_RANDOM_Function(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	t.Run("RANDOM returns value in range [0, 1)", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			result, err := executeUtilityQuery(t, exec, cat, "SELECT RANDOM()")
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			val := getUtilityFirstValue(result.Rows[0])
			f, ok := val.(float64)
			require.True(t, ok, "RANDOM should return float64")
			assert.GreaterOrEqual(t, f, 0.0, "RANDOM() should return >= 0")
			assert.Less(t, f, 1.0, "RANDOM() should return < 1")
		}
	})

	t.Run("RAND alias works", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT RAND()")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		f, ok := val.(float64)
		require.True(t, ok, "RAND should return float64")
		assert.GreaterOrEqual(t, f, 0.0, "RAND() should return >= 0")
		assert.Less(t, f, 1.0, "RAND() should return < 1")
	})
}

func TestUtilityIntegration_SIGN_Function(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{"positive integer", "SELECT SIGN(5)", 1},
		{"negative integer", "SELECT SIGN(-5)", -1},
		{"zero", "SELECT SIGN(0)", 0},
		{"positive float", "SELECT SIGN(3.14)", 1},
		{"negative float", "SELECT SIGN(-2.7)", -1},
		{"very small positive", "SELECT SIGN(0.001)", 1},
		{"very small negative", "SELECT SIGN(-0.001)", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeUtilityQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			val := getUtilityFirstValue(result.Rows[0])
			assert.Equal(t, tt.expected, val, "Query: %s", tt.query)
		})
	}
}

// =============================================================================
// Task 6.9: Test GCD, LCM cases in expression evaluator
// =============================================================================

func TestUtilityIntegration_GCD_Function(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{"basic GCD", "SELECT GCD(12, 8)", 4},
		{"coprime numbers", "SELECT GCD(17, 13)", 1},
		{"same numbers", "SELECT GCD(15, 15)", 15},
		{"with zero first", "SELECT GCD(0, 5)", 5},
		{"with zero second", "SELECT GCD(5, 0)", 5},
		{"both zero", "SELECT GCD(0, 0)", 0},
		{"negative first", "SELECT GCD(-12, 8)", 4},
		{"negative second", "SELECT GCD(12, -8)", 4},
		{"both negative", "SELECT GCD(-12, -8)", 4},
		{"large coprime", "SELECT GCD(97, 89)", 1},
		{"one divides other", "SELECT GCD(10, 5)", 5},
		{"Fibonacci consecutive", "SELECT GCD(21, 13)", 1},
		{"large numbers", "SELECT GCD(1000000, 999999)", 1},
		{"half relationship", "SELECT GCD(1000000, 500000)", 500000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeUtilityQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			val := getUtilityFirstValue(result.Rows[0])
			assert.Equal(t, tt.expected, val, "Query: %s", tt.query)
		})
	}
}

func TestUtilityIntegration_LCM_Function(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{"basic LCM", "SELECT LCM(4, 6)", 12},
		{"coprime numbers", "SELECT LCM(3, 5)", 15},
		{"same numbers", "SELECT LCM(7, 7)", 7},
		{"with zero first", "SELECT LCM(0, 5)", 0},
		{"with zero second", "SELECT LCM(5, 0)", 0},
		{"negative first", "SELECT LCM(-4, 6)", 12},
		{"negative second", "SELECT LCM(4, -6)", 12},
		{"both negative", "SELECT LCM(-4, -6)", 12},
		{"one divides other", "SELECT LCM(10, 5)", 10},
		{"larger numbers", "SELECT LCM(12, 18)", 36},
		{"prime numbers", "SELECT LCM(7, 11)", 77},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeUtilityQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			val := getUtilityFirstValue(result.Rows[0])
			assert.Equal(t, tt.expected, val, "Query: %s", tt.query)
		})
	}
}

// Task 6.12: Test GCD/LCM relationship property
func TestUtilityIntegration_GCD_LCM_Relationship(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	// For any two positive integers a and b: a * b = GCD(a, b) * LCM(a, b)
	tests := []struct {
		a, b int64
	}{
		{12, 8},
		{15, 25},
		{21, 14},
		{100, 35},
		{17, 13}, // coprime
		{7, 7},   // same
	}

	for _, tt := range tests {
		t.Run("relationship_check", func(t *testing.T) {
			// Get GCD
			gcdQuery := "SELECT GCD(" + itoa(tt.a) + ", " + itoa(tt.b) + ")"
			gcdResult, err := executeUtilityQuery(t, exec, cat, gcdQuery)
			require.NoError(t, err)
			gcd := getUtilityFirstValue(gcdResult.Rows[0]).(int64)

			// Get LCM
			lcmQuery := "SELECT LCM(" + itoa(tt.a) + ", " + itoa(tt.b) + ")"
			lcmResult, err := executeUtilityQuery(t, exec, cat, lcmQuery)
			require.NoError(t, err)
			lcm := getUtilityFirstValue(lcmResult.Rows[0]).(int64)

			// Verify: a * b = gcd * lcm
			assert.Equal(
				t,
				tt.a*tt.b,
				gcd*lcm,
				"GCD * LCM should equal a * b for a=%d, b=%d",
				tt.a,
				tt.b,
			)
		})
	}
}

// itoa is a simple int64 to string helper
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// =============================================================================
// Task 6.10: Test ISNAN, ISINF, ISFINITE cases in expression evaluator
// =============================================================================

func TestUtilityIntegration_ISNAN_Function(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{"normal positive", "SELECT ISNAN(1.0)", false},
		{"normal negative", "SELECT ISNAN(-1.0)", false},
		{"zero", "SELECT ISNAN(0.0)", false},
		{"large number", "SELECT ISNAN(1e308)", false},
		{"integer", "SELECT ISNAN(42)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeUtilityQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			val := getUtilityFirstValue(result.Rows[0])
			assert.Equal(t, tt.expected, val, "Query: %s", tt.query)
		})
	}
}

func TestUtilityIntegration_ISINF_Function(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{"normal positive", "SELECT ISINF(1.0)", false},
		{"normal negative", "SELECT ISINF(-1.0)", false},
		{"zero", "SELECT ISINF(0.0)", false},
		{"large number", "SELECT ISINF(1e308)", false},
		{"integer", "SELECT ISINF(42)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeUtilityQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			val := getUtilityFirstValue(result.Rows[0])
			assert.Equal(t, tt.expected, val, "Query: %s", tt.query)
		})
	}
}

func TestUtilityIntegration_ISFINITE_Function(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{"normal positive", "SELECT ISFINITE(1.0)", true},
		{"normal negative", "SELECT ISFINITE(-1.0)", true},
		{"zero", "SELECT ISFINITE(0.0)", true},
		{"large number", "SELECT ISFINITE(1e308)", true},
		{"small number", "SELECT ISFINITE(-1e308)", true},
		{"integer", "SELECT ISFINITE(42)", true},
		{"negative integer", "SELECT ISFINITE(-42)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeUtilityQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			val := getUtilityFirstValue(result.Rows[0])
			assert.Equal(t, tt.expected, val, "Query: %s", tt.query)
		})
	}
}

// Task 6.13: Test ISNAN/ISINF with special values via computation
func TestUtilityIntegration_FloatingPointValidation_Computations(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	// Test ISFINITE with normal computed values
	t.Run("ISFINITE with PI", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT ISFINITE(PI())")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.Equal(t, true, val, "PI() should be finite")
	})

	t.Run("ISFINITE with SQRT", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT ISFINITE(SQRT(2))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.Equal(t, true, val, "SQRT(2) should be finite")
	})

	t.Run("ISNAN with normal value", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT ISNAN(EXP(1))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.Equal(t, false, val, "EXP(1) should not be NaN")
	})

	t.Run("ISINF with normal value", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT ISINF(LN(100))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.Equal(t, false, val, "LN(100) should not be infinite")
	})
}

// =============================================================================
// Task 6.11: Comprehensive unit tests for utility functions (via SQL)
// =============================================================================

func TestUtilityIntegration_NULL_Handling(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	t.Run("SIGN with NULL", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT SIGN(NULL)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.Nil(t, val, "SIGN(NULL) should return NULL")
	})

	t.Run("GCD with NULL first", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT GCD(NULL, 12)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.Nil(t, val, "GCD(NULL, 12) should return NULL")
	})

	t.Run("GCD with NULL second", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT GCD(12, NULL)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.Nil(t, val, "GCD(12, NULL) should return NULL")
	})

	t.Run("LCM with NULL", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT LCM(4, NULL)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.Nil(t, val, "LCM(4, NULL) should return NULL")
	})

	t.Run("ISNAN with NULL", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT ISNAN(NULL)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.Nil(t, val, "ISNAN(NULL) should return NULL")
	})

	t.Run("ISINF with NULL", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT ISINF(NULL)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.Nil(t, val, "ISINF(NULL) should return NULL")
	})

	t.Run("ISFINITE with NULL", func(t *testing.T) {
		result, err := executeUtilityQuery(t, exec, cat, "SELECT ISFINITE(NULL)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		val := getUtilityFirstValue(result.Rows[0])
		assert.Nil(t, val, "ISFINITE(NULL) should return NULL")
	})
}

func TestUtilityIntegration_With_Table(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	// Create a table for testing
	_, err := executeUtilityQuery(t, exec, cat, `
		CREATE TABLE utility_numbers (
			a INTEGER,
			b INTEGER,
			val DOUBLE
		)
	`)
	require.NoError(t, err)

	// Insert test data
	_, err = executeUtilityQuery(t, exec, cat, "INSERT INTO utility_numbers VALUES (12, 8, 3.14)")
	require.NoError(t, err)
	_, err = executeUtilityQuery(t, exec, cat, "INSERT INTO utility_numbers VALUES (24, 36, -2.7)")
	require.NoError(t, err)
	_, err = executeUtilityQuery(t, exec, cat, "INSERT INTO utility_numbers VALUES (15, 25, 0.0)")
	require.NoError(t, err)

	t.Run("GCD with column values", func(t *testing.T) {
		result, err := executeUtilityQuery(
			t,
			exec,
			cat,
			"SELECT a, b, GCD(a, b) AS gcd_result FROM utility_numbers ORDER BY a",
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)

		expected := []int64{4, 5, 12}
		for i, row := range result.Rows {
			// Find the GCD column regardless of its name
			var gcd int64
			for k, v := range row {
				if k == "gcd_result" || k == "GCD(a, b)" {
					if val, ok := v.(int64); ok {
						gcd = val
						break
					}
				}
			}
			assert.Equal(t, expected[i], gcd, "Row %d: GCD should match", i)
		}
	})

	t.Run("SIGN with column values", func(t *testing.T) {
		result, err := executeUtilityQuery(
			t,
			exec,
			cat,
			"SELECT val, SIGN(val) AS sign_result FROM utility_numbers ORDER BY val",
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)

		// Ordered by val: -2.7, 0.0, 3.14
		expected := []int64{-1, 0, 1}
		for i, row := range result.Rows {
			// Find the SIGN column regardless of its name
			var sign int64
			for k, v := range row {
				if k == "sign_result" || k == "SIGN(val)" {
					if val, ok := v.(int64); ok {
						sign = val
						break
					}
				}
			}
			assert.Equal(t, expected[i], sign, "Row %d: SIGN should match", i)
		}
	})

	t.Run("ISFINITE with column values", func(t *testing.T) {
		result, err := executeUtilityQuery(
			t,
			exec,
			cat,
			"SELECT val, ISFINITE(val) AS is_finite FROM utility_numbers ORDER BY val",
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)

		// All values should be finite
		for i, row := range result.Rows {
			// Find the ISFINITE column regardless of its name
			var isFinite bool
			for k, v := range row {
				if k == "is_finite" || k == "ISFINITE(val)" {
					if val, ok := v.(bool); ok {
						isFinite = val
						break
					}
				}
			}
			assert.True(t, isFinite, "Row %d: value should be finite", i)
		}
	})
}

func TestUtilityIntegration_In_Expressions(t *testing.T) {
	exec, cat := setupUtilityTestExecutor()

	t.Run("GCD in arithmetic", func(t *testing.T) {
		// GCD can be used in arithmetic
		result, err := executeUtilityQuery(t, exec, cat, "SELECT 100 / GCD(20, 15)")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// GCD(20, 15) = 5, so 100 / 5 = 20
		val := getUtilityFirstValue(result.Rows[0]).(float64)
		assert.InDelta(t, 20.0, val, 1e-10)
	})

	t.Run("Nested utility functions", func(t *testing.T) {
		// SIGN of a GCD result
		result, err := executeUtilityQuery(t, exec, cat, "SELECT SIGN(GCD(-12, 8))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// GCD(-12, 8) = 4, SIGN(4) = 1
		val := getUtilityFirstValue(result.Rows[0]).(int64)
		assert.Equal(t, int64(1), val)
	})
}
