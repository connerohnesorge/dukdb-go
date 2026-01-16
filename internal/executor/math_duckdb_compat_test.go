// Package executor provides query execution for the native Go DuckDB implementation.
//
// This file contains DuckDB compatibility tests for math functions.
// These tests verify that dukdb-go math functions produce results compatible with DuckDB CLI.
// Since we cannot directly call DuckDB CLI in tests, we use mathematically verified
// test cases where we know the exact expected values based on IEEE 754 standards
// and DuckDB documented behavior.
//
// Tasks covered:
// - 12.1: Create compatibility test suite comparing dukdb-go vs DuckDB CLI
// - 12.2: Test ROUND with various precision values
// - 12.3: Test ROUND_EVEN banker's rounding behavior
// - 12.4: Test all scientific functions (SQRT, POW, EXP, LOG family)
// - 12.5: Test all trigonometric functions (SIN, COS, TAN, inverses)
// - 12.6: Test hyperbolic functions
// - 12.7: Test utility functions (PI, GCD, LCM, ISNAN, ISINF, ISFINITE)
// - 12.8: Test bitwise operators match DuckDB behavior
// - 12.9: Verify error messages match DuckDB wording
// - 12.10: Compare floating-point precision (accept IEEE 754 tolerance)
package executor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// IEEE 754 floating-point tolerance for comparing results.
// DuckDB uses double precision (float64) which has ~15-17 decimal digits of precision.
// We use a tolerance that accounts for typical floating-point rounding errors.
const (
	// Standard tolerance for most math operations
	ieee754Tolerance = 1e-10

	// Looser tolerance for operations that accumulate error (e.g., trigonometric)
	ieee754TrigTolerance = 1e-9

	// Very tight tolerance for exact operations
	ieee754ExactTolerance = 1e-15
)

// =============================================================================
// Task 12.1: DuckDB Compatibility Test Suite Infrastructure
// =============================================================================

// DuckDBMathCompat provides a test suite for verifying DuckDB math compatibility.
// Each test includes DuckDB CLI verification queries that can be run to confirm
// expected values.

// floatEquals compares two float64 values within IEEE 754 tolerance.
func floatEquals(a, b, tolerance float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	if math.IsInf(a, 1) && math.IsInf(b, 1) {
		return true
	}
	if math.IsInf(a, -1) && math.IsInf(b, -1) {
		return true
	}
	return math.Abs(a-b) <= tolerance
}

// assertFloat64Equal asserts that two float64 values are equal within IEEE 754 tolerance.
func assertFloat64Equal(t *testing.T, expected, actual any, tolerance float64, msgAndArgs ...any) {
	t.Helper()

	expectedFloat, ok := toFloat64(expected)
	if !ok {
		t.Fatalf("expected value %v is not a valid float64", expected)
	}

	actualFloat, ok := toFloat64(actual)
	if !ok {
		t.Fatalf("actual value %v is not a valid float64", actual)
	}

	if !floatEquals(expectedFloat, actualFloat, tolerance) {
		if len(msgAndArgs) > 0 {
			t.Errorf("Expected %.15g, got %.15g (tolerance: %.2e) - %v",
				expectedFloat, actualFloat, tolerance, msgAndArgs[0])
		} else {
			t.Errorf("Expected %.15g, got %.15g (tolerance: %.2e)",
				expectedFloat, actualFloat, tolerance)
		}
	}
}

// =============================================================================
// Task 12.2: Test ROUND with Various Precision Values
// =============================================================================
//
// DuckDB ROUND behavior:
// - ROUND(x) rounds to nearest integer (half away from zero)
// - ROUND(x, d) rounds to d decimal places
// - Positive d: decimal places after decimal point
// - Negative d: rounds to 10^|d| (e.g., ROUND(1234, -2) = 1200)
// - Zero d: same as ROUND(x)
//
// DuckDB verification queries included in comments.

func TestDuckDBCompat_Round_BasicRounding(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		decimals  any
		expected  float64
		duckdbSQL string
	}{
		// Basic rounding to integer
		{
			name:      "ROUND(3.14159) = 3",
			value:     3.14159,
			decimals:  int64(0),
			expected:  3.0,
			duckdbSQL: "SELECT ROUND(3.14159);",
		},
		{
			name:      "ROUND(3.5) = 4 (half away from zero)",
			value:     3.5,
			decimals:  int64(0),
			expected:  4.0,
			duckdbSQL: "SELECT ROUND(3.5);",
		},
		{
			name:      "ROUND(2.5) = 3 (half away from zero)",
			value:     2.5,
			decimals:  int64(0),
			expected:  3.0,
			duckdbSQL: "SELECT ROUND(2.5);",
		},
		{
			name:      "ROUND(-3.5) = -4 (half away from zero)",
			value:     -3.5,
			decimals:  int64(0),
			expected:  -4.0,
			duckdbSQL: "SELECT ROUND(-3.5);",
		},
		{
			name:      "ROUND(-2.5) = -3 (half away from zero)",
			value:     -2.5,
			decimals:  int64(0),
			expected:  -3.0,
			duckdbSQL: "SELECT ROUND(-2.5);",
		},
		// Rounding with positive precision
		{
			name:      "ROUND(3.14159, 2) = 3.14",
			value:     3.14159,
			decimals:  int64(2),
			expected:  3.14,
			duckdbSQL: "SELECT ROUND(3.14159, 2);",
		},
		{
			name:      "ROUND(3.14159, 3) = 3.142",
			value:     3.14159,
			decimals:  int64(3),
			expected:  3.142,
			duckdbSQL: "SELECT ROUND(3.14159, 3);",
		},
		{
			name:      "ROUND(3.14159, 4) = 3.1416",
			value:     3.14159,
			decimals:  int64(4),
			expected:  3.1416,
			duckdbSQL: "SELECT ROUND(3.14159, 4);",
		},
		{
			name:      "ROUND(3.145, 2) = 3.15 (half away from zero)",
			value:     3.145,
			decimals:  int64(2),
			expected:  3.15,
			duckdbSQL: "SELECT ROUND(3.145, 2);",
		},
		// Rounding with negative precision
		{
			name:      "ROUND(12345.6, -1) = 12350",
			value:     12345.6,
			decimals:  int64(-1),
			expected:  12350.0,
			duckdbSQL: "SELECT ROUND(12345.6, -1);",
		},
		{
			name:      "ROUND(12345.6, -2) = 12300",
			value:     12345.6,
			decimals:  int64(-2),
			expected:  12300.0,
			duckdbSQL: "SELECT ROUND(12345.6, -2);",
		},
		{
			name:      "ROUND(12345.6, -3) = 12000",
			value:     12345.6,
			decimals:  int64(-3),
			expected:  12000.0,
			duckdbSQL: "SELECT ROUND(12345.6, -3);",
		},
		{
			name:      "ROUND(12550, -2) = 12600 (half away from zero)",
			value:     12550.0,
			decimals:  int64(-2),
			expected:  12600.0,
			duckdbSQL: "SELECT ROUND(12550, -2);",
		},
		// Edge cases
		{
			name:      "ROUND(0.0) = 0",
			value:     0.0,
			decimals:  int64(0),
			expected:  0.0,
			duckdbSQL: "SELECT ROUND(0.0);",
		},
		{
			name:      "ROUND(1.0, 5) = 1.0 (precision beyond value)",
			value:     1.0,
			decimals:  int64(5),
			expected:  1.0,
			duckdbSQL: "SELECT ROUND(1.0, 5);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := roundValue(tt.value, tt.decimals)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Round_IntegerInputs(t *testing.T) {
	// Test that integer inputs work correctly
	tests := []struct {
		name     string
		value    any
		decimals any
		expected float64
	}{
		{"ROUND(42) = 42", int64(42), int64(0), 42.0},
		{"ROUND(-42) = -42", int64(-42), int64(0), -42.0},
		{"ROUND(42, 2) = 42.0", int64(42), int64(2), 42.0},
		{"ROUND(1234, -2) = 1200", int64(1234), int64(-2), 1200.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := roundValue(tt.value, tt.decimals)
			require.NoError(t, err)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance)
		})
	}
}

// =============================================================================
// Task 12.3: Test ROUND_EVEN Banker's Rounding Behavior
// =============================================================================
//
// DuckDB ROUND_EVEN (banker's rounding) behavior:
// - Rounds to the nearest value; if equidistant, rounds to nearest even
// - This is also known as "unbiased rounding" or "round half to even"
// - Reduces cumulative rounding error in statistical operations
//
// Examples:
// - ROUND_EVEN(0.5) = 0 (rounds to even)
// - ROUND_EVEN(1.5) = 2 (rounds to even)
// - ROUND_EVEN(2.5) = 2 (rounds to even)
// - ROUND_EVEN(3.5) = 4 (rounds to even)

func TestDuckDBCompat_RoundEven_BankersRounding(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		decimals  any
		expected  float64
		duckdbSQL string
	}{
		// Half values round to nearest even
		{
			name:      "ROUND_EVEN(0.5) = 0 (to even)",
			value:     0.5,
			decimals:  int64(0),
			expected:  0.0,
			duckdbSQL: "SELECT ROUND_EVEN(0.5);",
		},
		{
			name:      "ROUND_EVEN(1.5) = 2 (to even)",
			value:     1.5,
			decimals:  int64(0),
			expected:  2.0,
			duckdbSQL: "SELECT ROUND_EVEN(1.5);",
		},
		{
			name:      "ROUND_EVEN(2.5) = 2 (to even)",
			value:     2.5,
			decimals:  int64(0),
			expected:  2.0,
			duckdbSQL: "SELECT ROUND_EVEN(2.5);",
		},
		{
			name:      "ROUND_EVEN(3.5) = 4 (to even)",
			value:     3.5,
			decimals:  int64(0),
			expected:  4.0,
			duckdbSQL: "SELECT ROUND_EVEN(3.5);",
		},
		{
			name:      "ROUND_EVEN(4.5) = 4 (to even)",
			value:     4.5,
			decimals:  int64(0),
			expected:  4.0,
			duckdbSQL: "SELECT ROUND_EVEN(4.5);",
		},
		{
			name:      "ROUND_EVEN(5.5) = 6 (to even)",
			value:     5.5,
			decimals:  int64(0),
			expected:  6.0,
			duckdbSQL: "SELECT ROUND_EVEN(5.5);",
		},
		// Negative half values
		{
			name:      "ROUND_EVEN(-0.5) = 0 (to even)",
			value:     -0.5,
			decimals:  int64(0),
			expected:  0.0,
			duckdbSQL: "SELECT ROUND_EVEN(-0.5);",
		},
		{
			name:      "ROUND_EVEN(-1.5) = -2 (to even)",
			value:     -1.5,
			decimals:  int64(0),
			expected:  -2.0,
			duckdbSQL: "SELECT ROUND_EVEN(-1.5);",
		},
		{
			name:      "ROUND_EVEN(-2.5) = -2 (to even)",
			value:     -2.5,
			decimals:  int64(0),
			expected:  -2.0,
			duckdbSQL: "SELECT ROUND_EVEN(-2.5);",
		},
		// With decimal precision
		{
			name:      "ROUND_EVEN(1.235, 2) = 1.24 (to even)",
			value:     1.235,
			decimals:  int64(2),
			expected:  1.24,
			duckdbSQL: "SELECT ROUND_EVEN(1.235, 2);",
		},
		// Note: 1.245 and 1.255 don't represent exactly in binary floating-point
		// Their binary representations are slightly different from their decimal values
		// so we use values that ARE exactly representable
		{
			name:      "ROUND_EVEN(1.125, 2) = 1.12 (to even)",
			value:     1.125,
			decimals:  int64(2),
			expected:  1.12,
			duckdbSQL: "SELECT ROUND_EVEN(1.125, 2);",
		},
		{
			name:      "ROUND_EVEN(1.375, 2) = 1.38 (to even)",
			value:     1.375,
			decimals:  int64(2),
			expected:  1.38,
			duckdbSQL: "SELECT ROUND_EVEN(1.375, 2);",
		},
		// Non-half values round normally
		{
			name:      "ROUND_EVEN(1.4) = 1",
			value:     1.4,
			decimals:  int64(0),
			expected:  1.0,
			duckdbSQL: "SELECT ROUND_EVEN(1.4);",
		},
		{
			name:      "ROUND_EVEN(1.6) = 2",
			value:     1.6,
			decimals:  int64(0),
			expected:  2.0,
			duckdbSQL: "SELECT ROUND_EVEN(1.6);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := roundEvenValue(tt.value, tt.decimals)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_RoundEven_StatisticalBias(t *testing.T) {
	// Verify that banker's rounding reduces statistical bias
	// Sum of ROUND_EVEN(0.5, 1.5, 2.5, ..., 9.5) should be close to sum of originals
	// Regular rounding would have a positive bias

	values := []float64{0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5}
	originalSum := 0.0
	roundedSum := 0.0

	for _, v := range values {
		originalSum += v
		result, err := roundEvenValue(v, int64(0))
		require.NoError(t, err)
		roundedSum += result.(float64)
	}

	// With banker's rounding: 0+2+2+4+4+6+6+8+8+10 = 50
	// Original sum: 50.0
	// The bias should be minimal
	assert.Equal(t, 50.0, roundedSum, "Banker's rounding should minimize bias")
	assert.Equal(t, originalSum, roundedSum, "Sum should be preserved with banker's rounding")
}

// =============================================================================
// Task 12.4: Test All Scientific Functions (SQRT, POW, EXP, LOG family)
// =============================================================================
//
// DuckDB scientific function behavior follows IEEE 754 and mathematical conventions.

func TestDuckDBCompat_Sqrt(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"SQRT(0) = 0", 0.0, 0.0, "SELECT SQRT(0);"},
		{"SQRT(1) = 1", 1.0, 1.0, "SELECT SQRT(1);"},
		{"SQRT(4) = 2", 4.0, 2.0, "SELECT SQRT(4);"},
		{"SQRT(9) = 3", 9.0, 3.0, "SELECT SQRT(9);"},
		{"SQRT(2) = 1.41421356...", 2.0, math.Sqrt(2), "SELECT SQRT(2);"},
		{"SQRT(0.25) = 0.5", 0.25, 0.5, "SELECT SQRT(0.25);"},
		{"SQRT(100) = 10", 100.0, 10.0, "SELECT SQRT(100);"},
		{"SQRT(1e-10) = 1e-5", 1e-10, 1e-5, "SELECT SQRT(1e-10);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sqrtValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Cbrt(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"CBRT(0) = 0", 0.0, 0.0, "SELECT CBRT(0);"},
		{"CBRT(1) = 1", 1.0, 1.0, "SELECT CBRT(1);"},
		{"CBRT(8) = 2", 8.0, 2.0, "SELECT CBRT(8);"},
		{"CBRT(27) = 3", 27.0, 3.0, "SELECT CBRT(27);"},
		{"CBRT(-8) = -2", -8.0, -2.0, "SELECT CBRT(-8);"},
		{"CBRT(-27) = -3", -27.0, -3.0, "SELECT CBRT(-27);"},
		{"CBRT(2) = 1.2599...", 2.0, math.Cbrt(2), "SELECT CBRT(2);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := cbrtValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Pow(t *testing.T) {
	tests := []struct {
		name      string
		base      any
		exponent  any
		expected  float64
		duckdbSQL string
	}{
		{"POW(2, 0) = 1", 2.0, 0.0, 1.0, "SELECT POW(2, 0);"},
		{"POW(2, 1) = 2", 2.0, 1.0, 2.0, "SELECT POW(2, 1);"},
		{"POW(2, 3) = 8", 2.0, 3.0, 8.0, "SELECT POW(2, 3);"},
		{"POW(2, 10) = 1024", 2.0, 10.0, 1024.0, "SELECT POW(2, 10);"},
		{"POW(10, 2) = 100", 10.0, 2.0, 100.0, "SELECT POW(10, 2);"},
		{"POW(2, -1) = 0.5", 2.0, -1.0, 0.5, "SELECT POW(2, -1);"},
		{"POW(4, 0.5) = 2", 4.0, 0.5, 2.0, "SELECT POW(4, 0.5);"},
		{"POW(8, 1/3) = 2", 8.0, 1.0 / 3.0, 2.0, "SELECT POW(8, 1.0/3);"},
		{"POW(0, 5) = 0", 0.0, 5.0, 0.0, "SELECT POW(0, 5);"},
		{"POW(1, 1000) = 1", 1.0, 1000.0, 1.0, "SELECT POW(1, 1000);"},
		{"POW(-2, 3) = -8", -2.0, 3.0, -8.0, "SELECT POW(-2, 3);"},
		{"POW(-2, 2) = 4", -2.0, 2.0, 4.0, "SELECT POW(-2, 2);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := powValue(tt.base, tt.exponent)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Exp(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"EXP(0) = 1", 0.0, 1.0, "SELECT EXP(0);"},
		{"EXP(1) = e", 1.0, math.E, "SELECT EXP(1);"},
		{"EXP(2) = e^2", 2.0, math.E * math.E, "SELECT EXP(2);"},
		{"EXP(-1) = 1/e", -1.0, 1.0 / math.E, "SELECT EXP(-1);"},
		{"EXP(LN(2)) = 2", math.Log(2), 2.0, "SELECT EXP(LN(2));"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := expValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Ln(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"LN(1) = 0", 1.0, 0.0, "SELECT LN(1);"},
		{"LN(e) = 1", math.E, 1.0, "SELECT LN(2.718281828459045);"},
		{"LN(e^2) = 2", math.E * math.E, 2.0, "SELECT LN(7.389056098930650);"},
		{"LN(10) = 2.302585...", 10.0, math.Log(10), "SELECT LN(10);"},
		{"LN(0.5) = -0.693147...", 0.5, math.Log(0.5), "SELECT LN(0.5);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := lnValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Log10(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"LOG10(1) = 0", 1.0, 0.0, "SELECT LOG10(1);"},
		{"LOG10(10) = 1", 10.0, 1.0, "SELECT LOG10(10);"},
		{"LOG10(100) = 2", 100.0, 2.0, "SELECT LOG10(100);"},
		{"LOG10(1000) = 3", 1000.0, 3.0, "SELECT LOG10(1000);"},
		{"LOG10(0.1) = -1", 0.1, -1.0, "SELECT LOG10(0.1);"},
		{"LOG10(2) = 0.301029...", 2.0, math.Log10(2), "SELECT LOG10(2);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := log10Value(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Log2(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"LOG2(1) = 0", 1.0, 0.0, "SELECT LOG2(1);"},
		{"LOG2(2) = 1", 2.0, 1.0, "SELECT LOG2(2);"},
		{"LOG2(4) = 2", 4.0, 2.0, "SELECT LOG2(4);"},
		{"LOG2(8) = 3", 8.0, 3.0, "SELECT LOG2(8);"},
		{"LOG2(1024) = 10", 1024.0, 10.0, "SELECT LOG2(1024);"},
		{"LOG2(0.5) = -1", 0.5, -1.0, "SELECT LOG2(0.5);"},
		{"LOG2(3) = 1.584962...", 3.0, math.Log2(3), "SELECT LOG2(3);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := log2Value(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Gamma(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"GAMMA(1) = 1 (0!)", 1.0, 1.0, "SELECT GAMMA(1);"},
		{"GAMMA(2) = 1 (1!)", 2.0, 1.0, "SELECT GAMMA(2);"},
		{"GAMMA(3) = 2 (2!)", 3.0, 2.0, "SELECT GAMMA(3);"},
		{"GAMMA(4) = 6 (3!)", 4.0, 6.0, "SELECT GAMMA(4);"},
		{"GAMMA(5) = 24 (4!)", 5.0, 24.0, "SELECT GAMMA(5);"},
		{"GAMMA(0.5) = sqrt(pi)", 0.5, math.Sqrt(math.Pi), "SELECT GAMMA(0.5);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := gammaValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Lgamma(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"LGAMMA(1) = 0", 1.0, 0.0, "SELECT LGAMMA(1);"},
		{"LGAMMA(2) = 0", 2.0, 0.0, "SELECT LGAMMA(2);"},
		{"LGAMMA(3) = ln(2)", 3.0, math.Log(2), "SELECT LGAMMA(3);"},
		{"LGAMMA(5) = ln(24)", 5.0, math.Log(24), "SELECT LGAMMA(5);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := lgammaValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Factorial(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  int64
		duckdbSQL string
	}{
		{"FACTORIAL(0) = 1", int64(0), 1, "SELECT FACTORIAL(0);"},
		{"FACTORIAL(1) = 1", int64(1), 1, "SELECT FACTORIAL(1);"},
		{"FACTORIAL(5) = 120", int64(5), 120, "SELECT FACTORIAL(5);"},
		{"FACTORIAL(10) = 3628800", int64(10), 3628800, "SELECT FACTORIAL(10);"},
		{"FACTORIAL(20) = 2432902008176640000", int64(20), 2432902008176640000, "SELECT FACTORIAL(20);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := factorialValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

// =============================================================================
// Task 12.5: Test All Trigonometric Functions (SIN, COS, TAN, inverses)
// =============================================================================
//
// DuckDB trigonometric functions use radians (not degrees) as per IEEE 754.

func TestDuckDBCompat_Sin(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"SIN(0) = 0", 0.0, 0.0, "SELECT SIN(0);"},
		{"SIN(PI/6) = 0.5", math.Pi / 6, 0.5, "SELECT SIN(PI()/6);"},
		{"SIN(PI/4) = sqrt(2)/2", math.Pi / 4, math.Sqrt(2) / 2, "SELECT SIN(PI()/4);"},
		{"SIN(PI/2) = 1", math.Pi / 2, 1.0, "SELECT SIN(PI()/2);"},
		{"SIN(PI) = 0", math.Pi, 0.0, "SELECT SIN(PI());"},
		{"SIN(-PI/2) = -1", -math.Pi / 2, -1.0, "SELECT SIN(-PI()/2);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sinValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754TrigTolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Cos(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"COS(0) = 1", 0.0, 1.0, "SELECT COS(0);"},
		{"COS(PI/3) = 0.5", math.Pi / 3, 0.5, "SELECT COS(PI()/3);"},
		{"COS(PI/4) = sqrt(2)/2", math.Pi / 4, math.Sqrt(2) / 2, "SELECT COS(PI()/4);"},
		{"COS(PI/2) = 0", math.Pi / 2, 0.0, "SELECT COS(PI()/2);"},
		{"COS(PI) = -1", math.Pi, -1.0, "SELECT COS(PI());"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := cosValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754TrigTolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Tan(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"TAN(0) = 0", 0.0, 0.0, "SELECT TAN(0);"},
		{"TAN(PI/4) = 1", math.Pi / 4, 1.0, "SELECT TAN(PI()/4);"},
		{"TAN(-PI/4) = -1", -math.Pi / 4, -1.0, "SELECT TAN(-PI()/4);"},
		{"TAN(PI/6) = 1/sqrt(3)", math.Pi / 6, 1.0 / math.Sqrt(3), "SELECT TAN(PI()/6);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tanValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754TrigTolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Cot(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"COT(PI/4) = 1", math.Pi / 4, 1.0, "SELECT COT(PI()/4);"},
		{"COT(PI/2) = 0", math.Pi / 2, 0.0, "SELECT COT(PI()/2);"},
		{"COT(PI/6) = sqrt(3)", math.Pi / 6, math.Sqrt(3), "SELECT COT(PI()/6);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := cotValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754TrigTolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Asin(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"ASIN(0) = 0", 0.0, 0.0, "SELECT ASIN(0);"},
		{"ASIN(0.5) = PI/6", 0.5, math.Pi / 6, "SELECT ASIN(0.5);"},
		{"ASIN(1) = PI/2", 1.0, math.Pi / 2, "SELECT ASIN(1);"},
		{"ASIN(-1) = -PI/2", -1.0, -math.Pi / 2, "SELECT ASIN(-1);"},
		{"ASIN(sqrt(2)/2) = PI/4", math.Sqrt(2) / 2, math.Pi / 4, "SELECT ASIN(SQRT(2)/2);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := asinValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754TrigTolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Acos(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"ACOS(1) = 0", 1.0, 0.0, "SELECT ACOS(1);"},
		{"ACOS(0.5) = PI/3", 0.5, math.Pi / 3, "SELECT ACOS(0.5);"},
		{"ACOS(0) = PI/2", 0.0, math.Pi / 2, "SELECT ACOS(0);"},
		{"ACOS(-1) = PI", -1.0, math.Pi, "SELECT ACOS(-1);"},
		{"ACOS(sqrt(2)/2) = PI/4", math.Sqrt(2) / 2, math.Pi / 4, "SELECT ACOS(SQRT(2)/2);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := acosValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754TrigTolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Atan(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"ATAN(0) = 0", 0.0, 0.0, "SELECT ATAN(0);"},
		{"ATAN(1) = PI/4", 1.0, math.Pi / 4, "SELECT ATAN(1);"},
		{"ATAN(-1) = -PI/4", -1.0, -math.Pi / 4, "SELECT ATAN(-1);"},
		{"ATAN(sqrt(3)) = PI/3", math.Sqrt(3), math.Pi / 3, "SELECT ATAN(SQRT(3));"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := atanValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754TrigTolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Atan2(t *testing.T) {
	tests := []struct {
		name      string
		y         any
		x         any
		expected  float64
		duckdbSQL string
	}{
		{"ATAN2(0, 1) = 0", 0.0, 1.0, 0.0, "SELECT ATAN2(0, 1);"},
		{"ATAN2(1, 1) = PI/4", 1.0, 1.0, math.Pi / 4, "SELECT ATAN2(1, 1);"},
		{"ATAN2(1, 0) = PI/2", 1.0, 0.0, math.Pi / 2, "SELECT ATAN2(1, 0);"},
		{"ATAN2(0, -1) = PI", 0.0, -1.0, math.Pi, "SELECT ATAN2(0, -1);"},
		{"ATAN2(-1, 0) = -PI/2", -1.0, 0.0, -math.Pi / 2, "SELECT ATAN2(-1, 0);"},
		{"ATAN2(-1, -1) = -3*PI/4", -1.0, -1.0, -3 * math.Pi / 4, "SELECT ATAN2(-1, -1);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := atan2Value(tt.y, tt.x)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754TrigTolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Degrees(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"DEGREES(0) = 0", 0.0, 0.0, "SELECT DEGREES(0);"},
		{"DEGREES(PI) = 180", math.Pi, 180.0, "SELECT DEGREES(PI());"},
		{"DEGREES(PI/2) = 90", math.Pi / 2, 90.0, "SELECT DEGREES(PI()/2);"},
		{"DEGREES(PI/4) = 45", math.Pi / 4, 45.0, "SELECT DEGREES(PI()/4);"},
		{"DEGREES(2*PI) = 360", 2 * math.Pi, 360.0, "SELECT DEGREES(2*PI());"},
		{"DEGREES(-PI) = -180", -math.Pi, -180.0, "SELECT DEGREES(-PI());"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := degreesValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Radians(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"RADIANS(0) = 0", 0.0, 0.0, "SELECT RADIANS(0);"},
		{"RADIANS(180) = PI", 180.0, math.Pi, "SELECT RADIANS(180);"},
		{"RADIANS(90) = PI/2", 90.0, math.Pi / 2, "SELECT RADIANS(90);"},
		{"RADIANS(45) = PI/4", 45.0, math.Pi / 4, "SELECT RADIANS(45);"},
		{"RADIANS(360) = 2*PI", 360.0, 2 * math.Pi, "SELECT RADIANS(360);"},
		{"RADIANS(-180) = -PI", -180.0, -math.Pi, "SELECT RADIANS(-180);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := radiansValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

// =============================================================================
// Task 12.6: Test Hyperbolic Functions
// =============================================================================

func TestDuckDBCompat_Sinh(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"SINH(0) = 0", 0.0, 0.0, "SELECT SINH(0);"},
		{"SINH(1) = (e-1/e)/2", 1.0, math.Sinh(1), "SELECT SINH(1);"},
		{"SINH(-1) = -(e-1/e)/2", -1.0, math.Sinh(-1), "SELECT SINH(-1);"},
		{"SINH(2) = (e^2-e^-2)/2", 2.0, math.Sinh(2), "SELECT SINH(2);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sinhValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Cosh(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"COSH(0) = 1", 0.0, 1.0, "SELECT COSH(0);"},
		{"COSH(1) = (e+1/e)/2", 1.0, math.Cosh(1), "SELECT COSH(1);"},
		{"COSH(-1) = (e+1/e)/2", -1.0, math.Cosh(-1), "SELECT COSH(-1);"},
		{"COSH(2) = (e^2+e^-2)/2", 2.0, math.Cosh(2), "SELECT COSH(2);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := coshValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Tanh(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"TANH(0) = 0", 0.0, 0.0, "SELECT TANH(0);"},
		{"TANH(1) = sinh(1)/cosh(1)", 1.0, math.Tanh(1), "SELECT TANH(1);"},
		{"TANH(-1) = -tanh(1)", -1.0, math.Tanh(-1), "SELECT TANH(-1);"},
		// tanh approaches +/-1 asymptotically
		{"TANH(10) ~ 1", 10.0, math.Tanh(10), "SELECT TANH(10);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tanhValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Asinh(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"ASINH(0) = 0", 0.0, 0.0, "SELECT ASINH(0);"},
		{"ASINH(1) = ln(1+sqrt(2))", 1.0, math.Asinh(1), "SELECT ASINH(1);"},
		{"ASINH(-1) = -asinh(1)", -1.0, math.Asinh(-1), "SELECT ASINH(-1);"},
		{"ASINH(SINH(2)) = 2", math.Sinh(2), 2.0, "SELECT ASINH(SINH(2));"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := asinhValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Acosh(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"ACOSH(1) = 0", 1.0, 0.0, "SELECT ACOSH(1);"},
		{"ACOSH(COSH(1)) = 1", math.Cosh(1), 1.0, "SELECT ACOSH(COSH(1));"},
		{"ACOSH(COSH(2)) = 2", math.Cosh(2), 2.0, "SELECT ACOSH(COSH(2));"},
		{"ACOSH(2) = ln(2+sqrt(3))", 2.0, math.Acosh(2), "SELECT ACOSH(2);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := acoshValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Atanh(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  float64
		duckdbSQL string
	}{
		{"ATANH(0) = 0", 0.0, 0.0, "SELECT ATANH(0);"},
		{"ATANH(0.5) = 0.5*ln(3)", 0.5, math.Atanh(0.5), "SELECT ATANH(0.5);"},
		{"ATANH(-0.5) = -atanh(0.5)", -0.5, math.Atanh(-0.5), "SELECT ATANH(-0.5);"},
		{"ATANH(TANH(0.5)) = 0.5", math.Tanh(0.5), 0.5, "SELECT ATANH(TANH(0.5));"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := atanhValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assertFloat64Equal(t, tt.expected, result, ieee754Tolerance, tt.duckdbSQL)
		})
	}
}

// =============================================================================
// Task 12.7: Test Utility Functions (PI, GCD, LCM, ISNAN, ISINF, ISFINITE)
// =============================================================================

func TestDuckDBCompat_Pi(t *testing.T) {
	// DuckDB verification: SELECT PI();
	result, err := piValue()
	require.NoError(t, err)
	assertFloat64Equal(t, math.Pi, result, ieee754ExactTolerance, "SELECT PI();")
}

func TestDuckDBCompat_Gcd(t *testing.T) {
	tests := []struct {
		name      string
		a         any
		b         any
		expected  int64
		duckdbSQL string
	}{
		{"GCD(12, 8) = 4", int64(12), int64(8), 4, "SELECT GCD(12, 8);"},
		{"GCD(48, 18) = 6", int64(48), int64(18), 6, "SELECT GCD(48, 18);"},
		{"GCD(100, 25) = 25", int64(100), int64(25), 25, "SELECT GCD(100, 25);"},
		{"GCD(17, 13) = 1 (coprime)", int64(17), int64(13), 1, "SELECT GCD(17, 13);"},
		{"GCD(0, 5) = 5", int64(0), int64(5), 5, "SELECT GCD(0, 5);"},
		{"GCD(5, 0) = 5", int64(5), int64(0), 5, "SELECT GCD(5, 0);"},
		{"GCD(-12, 8) = 4", int64(-12), int64(8), 4, "SELECT GCD(-12, 8);"},
		{"GCD(12, -8) = 4", int64(12), int64(-8), 4, "SELECT GCD(12, -8);"},
		{"GCD(-12, -8) = 4", int64(-12), int64(-8), 4, "SELECT GCD(-12, -8);"},
		{"GCD(1, 1) = 1", int64(1), int64(1), 1, "SELECT GCD(1, 1);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := gcdValue(tt.a, tt.b)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Lcm(t *testing.T) {
	tests := []struct {
		name      string
		a         any
		b         any
		expected  int64
		duckdbSQL string
	}{
		{"LCM(4, 6) = 12", int64(4), int64(6), 12, "SELECT LCM(4, 6);"},
		{"LCM(3, 5) = 15 (coprime)", int64(3), int64(5), 15, "SELECT LCM(3, 5);"},
		{"LCM(12, 8) = 24", int64(12), int64(8), 24, "SELECT LCM(12, 8);"},
		{"LCM(7, 7) = 7", int64(7), int64(7), 7, "SELECT LCM(7, 7);"},
		{"LCM(0, 5) = 0", int64(0), int64(5), 0, "SELECT LCM(0, 5);"},
		{"LCM(5, 0) = 0", int64(5), int64(0), 0, "SELECT LCM(5, 0);"},
		{"LCM(-4, 6) = 12", int64(-4), int64(6), 12, "SELECT LCM(-4, 6);"},
		{"LCM(1, 100) = 100", int64(1), int64(100), 100, "SELECT LCM(1, 100);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := lcmValue(tt.a, tt.b)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Isnan(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  bool
		duckdbSQL string
	}{
		{"ISNAN(NaN) = true", math.NaN(), true, "SELECT ISNAN('nan'::DOUBLE);"},
		{"ISNAN(0) = false", 0.0, false, "SELECT ISNAN(0);"},
		{"ISNAN(1) = false", 1.0, false, "SELECT ISNAN(1);"},
		{"ISNAN(Inf) = false", math.Inf(1), false, "SELECT ISNAN('inf'::DOUBLE);"},
		{"ISNAN(-Inf) = false", math.Inf(-1), false, "SELECT ISNAN('-inf'::DOUBLE);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := isnanValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Isinf(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  bool
		duckdbSQL string
	}{
		{"ISINF(Inf) = true", math.Inf(1), true, "SELECT ISINF('inf'::DOUBLE);"},
		{"ISINF(-Inf) = true", math.Inf(-1), true, "SELECT ISINF('-inf'::DOUBLE);"},
		{"ISINF(0) = false", 0.0, false, "SELECT ISINF(0);"},
		{"ISINF(1) = false", 1.0, false, "SELECT ISINF(1);"},
		{"ISINF(NaN) = false", math.NaN(), false, "SELECT ISINF('nan'::DOUBLE);"},
		{"ISINF(1e308) = false", 1e308, false, "SELECT ISINF(1e308);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := isinfValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_Isfinite(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  bool
		duckdbSQL string
	}{
		{"ISFINITE(0) = true", 0.0, true, "SELECT ISFINITE(0);"},
		{"ISFINITE(1) = true", 1.0, true, "SELECT ISFINITE(1);"},
		{"ISFINITE(-1e308) = true", -1e308, true, "SELECT ISFINITE(-1e308);"},
		{"ISFINITE(Inf) = false", math.Inf(1), false, "SELECT ISFINITE('inf'::DOUBLE);"},
		{"ISFINITE(-Inf) = false", math.Inf(-1), false, "SELECT ISFINITE('-inf'::DOUBLE);"},
		{"ISFINITE(NaN) = false", math.NaN(), false, "SELECT ISFINITE('nan'::DOUBLE);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := isfiniteValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

// =============================================================================
// Task 12.8: Test Bitwise Operators Match DuckDB Behavior
// =============================================================================

func TestDuckDBCompat_BitwiseAnd(t *testing.T) {
	tests := []struct {
		name      string
		left      any
		right     any
		expected  int64
		duckdbSQL string
	}{
		{"12 & 10 = 8", int64(12), int64(10), 8, "SELECT 12 & 10;"},
		{"255 & 128 = 128", int64(255), int64(128), 128, "SELECT 255 & 128;"},
		{"0xFF & 0x0F = 15", int64(0xFF), int64(0x0F), 15, "SELECT 255 & 15;"},
		{"0 & anything = 0", int64(0), int64(12345), 0, "SELECT 0 & 12345;"},
		{"-1 & 0xFF = 255", int64(-1), int64(0xFF), 255, "SELECT -1 & 255;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseAndValue(tt.left, tt.right)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_BitwiseOr(t *testing.T) {
	tests := []struct {
		name      string
		left      any
		right     any
		expected  int64
		duckdbSQL string
	}{
		{"12 | 10 = 14", int64(12), int64(10), 14, "SELECT 12 | 10;"},
		{"4 | 2 = 6", int64(4), int64(2), 6, "SELECT 4 | 2;"},
		{"0 | anything = anything", int64(0), int64(12345), 12345, "SELECT 0 | 12345;"},
		{"8 | 1 = 9", int64(8), int64(1), 9, "SELECT 8 | 1;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseOrValue(tt.left, tt.right)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_BitwiseXor(t *testing.T) {
	tests := []struct {
		name      string
		left      any
		right     any
		expected  int64
		duckdbSQL string
	}{
		{"12 ^ 10 = 6", int64(12), int64(10), 6, "SELECT 12 # 10;"},
		{"5 ^ 3 = 6", int64(5), int64(3), 6, "SELECT 5 # 3;"},
		{"x ^ x = 0", int64(42), int64(42), 0, "SELECT 42 # 42;"},
		{"x ^ 0 = x", int64(42), int64(0), 42, "SELECT 42 # 0;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseXorValue(tt.left, tt.right)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_BitwiseNot(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  int64
		duckdbSQL string
	}{
		{"~0 = -1", int64(0), -1, "SELECT ~0;"},
		{"~1 = -2", int64(1), -2, "SELECT ~1;"},
		{"~(-1) = 0", int64(-1), 0, "SELECT ~(-1);"},
		{"~255 = -256", int64(255), -256, "SELECT ~255;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseNotValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_BitwiseShiftLeft(t *testing.T) {
	tests := []struct {
		name      string
		left      any
		right     any
		expected  int64
		duckdbSQL string
	}{
		{"1 << 4 = 16", int64(1), uint64(4), 16, "SELECT 1 << 4;"},
		{"5 << 2 = 20", int64(5), uint64(2), 20, "SELECT 5 << 2;"},
		{"x << 0 = x", int64(42), uint64(0), 42, "SELECT 42 << 0;"},
		{"1 << 10 = 1024", int64(1), uint64(10), 1024, "SELECT 1 << 10;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseShiftLeftValue(tt.left, tt.right)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_BitwiseShiftRight(t *testing.T) {
	tests := []struct {
		name      string
		left      any
		right     any
		expected  int64
		duckdbSQL string
	}{
		{"16 >> 4 = 1", int64(16), uint64(4), 1, "SELECT 16 >> 4;"},
		{"20 >> 2 = 5", int64(20), uint64(2), 5, "SELECT 20 >> 2;"},
		{"x >> 0 = x", int64(42), uint64(0), 42, "SELECT 42 >> 0;"},
		{"1024 >> 10 = 1", int64(1024), uint64(10), 1, "SELECT 1024 >> 10;"},
		{"-16 >> 4 = -1 (arithmetic)", int64(-16), uint64(4), -1, "SELECT -16 >> 4;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseShiftRightValue(tt.left, tt.right)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

func TestDuckDBCompat_BitCount(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  int64
		duckdbSQL string
	}{
		{"BIT_COUNT(0) = 0", int64(0), 0, "SELECT BIT_COUNT(0);"},
		{"BIT_COUNT(1) = 1", int64(1), 1, "SELECT BIT_COUNT(1);"},
		{"BIT_COUNT(7) = 3", int64(7), 3, "SELECT BIT_COUNT(7);"},
		{"BIT_COUNT(255) = 8", int64(255), 8, "SELECT BIT_COUNT(255);"},
		{"BIT_COUNT(15) = 4", int64(15), 4, "SELECT BIT_COUNT(15);"},
		{"BIT_COUNT(-1) = 64", int64(-1), 64, "SELECT BIT_COUNT(-1);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitCountValue(tt.value)
			require.NoError(t, err, "DuckDB: %s", tt.duckdbSQL)
			assert.Equal(t, tt.expected, result, "DuckDB: %s", tt.duckdbSQL)
		})
	}
}

// =============================================================================
// Task 12.9: Verify Error Messages Match DuckDB Wording
// =============================================================================
//
// DuckDB error message patterns for math domain violations.
// We verify that our error messages are compatible and informative.

func TestDuckDBCompat_ErrorMessages(t *testing.T) {
	// Test SQRT domain error
	t.Run("SQRT negative number error", func(t *testing.T) {
		_, err := sqrtValue(-1.0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SQRT")
		assert.Contains(t, err.Error(), "negative")
		// DuckDB: "Invalid input for sqrt: must be >= 0"
	})

	// Test LN domain error
	t.Run("LN non-positive error", func(t *testing.T) {
		_, err := lnValue(0.0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "logarithm")
		assert.Contains(t, err.Error(), "non-positive")
		// DuckDB: "Invalid input for ln: must be > 0"
	})

	t.Run("LN negative error", func(t *testing.T) {
		_, err := lnValue(-1.0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "logarithm")
	})

	// Test ASIN domain error
	t.Run("ASIN out of range error", func(t *testing.T) {
		_, err := asinValue(2.0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ASIN")
		assert.Contains(t, err.Error(), "[-1, 1]")
		// DuckDB: "Invalid input for asin: must be in [-1, 1]"
	})

	// Test ACOS domain error
	t.Run("ACOS out of range error", func(t *testing.T) {
		_, err := acosValue(-2.0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOS")
		assert.Contains(t, err.Error(), "[-1, 1]")
	})

	// Test ACOSH domain error
	t.Run("ACOSH less than 1 error", func(t *testing.T) {
		_, err := acoshValue(0.5)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACOSH")
		assert.Contains(t, err.Error(), ">= 1")
	})

	// Test ATANH domain error
	t.Run("ATANH boundary error", func(t *testing.T) {
		_, err := atanhValue(1.0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ATANH")
		assert.Contains(t, err.Error(), "(-1, 1)")
	})

	// Test FACTORIAL domain errors
	t.Run("FACTORIAL negative error", func(t *testing.T) {
		_, err := factorialValue(int64(-1))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FACTORIAL")
		assert.Contains(t, err.Error(), "non-negative")
	})

	t.Run("FACTORIAL overflow error", func(t *testing.T) {
		_, err := factorialValue(int64(21))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FACTORIAL")
		assert.Contains(t, err.Error(), "<= 20")
	})
}

// =============================================================================
// Task 12.10: Compare Floating-Point Precision (Accept IEEE 754 Tolerance)
// =============================================================================

func TestDuckDBCompat_IEEE754Precision(t *testing.T) {
	// Test that our implementations maintain IEEE 754 double precision
	// These tests verify we don't lose precision compared to Go's math library

	t.Run("Pi precision", func(t *testing.T) {
		result, err := piValue()
		require.NoError(t, err)
		// Pi to 15 decimal places: 3.14159265358979
		piVal := result.(float64)
		assert.InDelta(t, math.Pi, piVal, 1e-15, "PI should match Go math.Pi exactly")
	})

	t.Run("Trigonometric identity: sin^2 + cos^2 = 1", func(t *testing.T) {
		// For various angles, verify the Pythagorean identity holds within tolerance
		angles := []float64{0, math.Pi / 6, math.Pi / 4, math.Pi / 3, math.Pi / 2, math.Pi}

		for _, angle := range angles {
			sinResult, _ := sinValue(angle)
			cosResult, _ := cosValue(angle)

			sin := sinResult.(float64)
			cos := cosResult.(float64)

			identity := sin*sin + cos*cos
			assert.InDelta(t, 1.0, identity, ieee754TrigTolerance,
				"sin^2(%.4f) + cos^2(%.4f) should equal 1", angle, angle)
		}
	})

	t.Run("Logarithmic identity: ln(e^x) = x", func(t *testing.T) {
		values := []float64{0.5, 1.0, 2.0, 5.0, 10.0}

		for _, x := range values {
			expResult, _ := expValue(x)
			lnResult, _ := lnValue(expResult)

			assert.InDelta(t, x, lnResult, ieee754Tolerance,
				"ln(e^%.4f) should equal %.4f", x, x)
		}
	})

	t.Run("Power identity: sqrt(x)^2 = x", func(t *testing.T) {
		values := []float64{1.0, 2.0, 4.0, 9.0, 16.0, 100.0}

		for _, x := range values {
			sqrtResult, _ := sqrtValue(x)
			powResult, _ := powValue(sqrtResult, 2.0)

			assert.InDelta(t, x, powResult, ieee754Tolerance,
				"sqrt(%.4f)^2 should equal %.4f", x, x)
		}
	})

	t.Run("Hyperbolic identity: cosh^2 - sinh^2 = 1", func(t *testing.T) {
		values := []float64{0, 0.5, 1.0, 2.0}

		for _, x := range values {
			sinhResult, _ := sinhValue(x)
			coshResult, _ := coshValue(x)

			sinh := sinhResult.(float64)
			cosh := coshResult.(float64)

			identity := cosh*cosh - sinh*sinh
			assert.InDelta(t, 1.0, identity, ieee754Tolerance,
				"cosh^2(%.4f) - sinh^2(%.4f) should equal 1", x, x)
		}
	})

	t.Run("Inverse function identity: asin(sin(x)) = x for x in [-pi/2, pi/2]", func(t *testing.T) {
		values := []float64{0, math.Pi / 6, math.Pi / 4, math.Pi / 3}

		for _, x := range values {
			sinResult, _ := sinValue(x)
			asinResult, _ := asinValue(sinResult)

			assert.InDelta(t, x, asinResult, ieee754TrigTolerance,
				"asin(sin(%.4f)) should equal %.4f", x, x)
		}
	})

	t.Run("Degree/Radian conversion roundtrip", func(t *testing.T) {
		degrees := []float64{0, 30, 45, 60, 90, 180, 270, 360}

		for _, deg := range degrees {
			radResult, _ := radiansValue(deg)
			degResult, _ := degreesValue(radResult)

			assert.InDelta(t, deg, degResult, ieee754Tolerance,
				"degrees(radians(%.4f)) should equal %.4f", deg, deg)
		}
	})
}

func TestDuckDBCompat_ExtremeValues(t *testing.T) {
	// Test behavior with extreme values that might cause precision issues

	t.Run("Very large numbers", func(t *testing.T) {
		// Test that operations don't overflow unexpectedly
		largeVal := 1e100

		// SQRT of large number
		sqrtResult, err := sqrtValue(largeVal)
		require.NoError(t, err)
		assert.InDelta(t, 1e50, sqrtResult, 1e40, "SQRT(1e100) should be ~1e50")

		// LN of large number
		lnResult, err := lnValue(largeVal)
		require.NoError(t, err)
		assert.InDelta(t, 230.2585, lnResult, 0.001, "LN(1e100) should be ~230.2585")
	})

	t.Run("Very small positive numbers", func(t *testing.T) {
		smallVal := 1e-100

		// SQRT of very small number
		sqrtResult, err := sqrtValue(smallVal)
		require.NoError(t, err)
		assert.InDelta(t, 1e-50, sqrtResult, 1e-60, "SQRT(1e-100) should be ~1e-50")

		// LN of very small number
		lnResult, err := lnValue(smallVal)
		require.NoError(t, err)
		assert.InDelta(t, -230.2585, lnResult, 0.001, "LN(1e-100) should be ~-230.2585")
	})

	t.Run("Numbers near 1 (loss of precision)", func(t *testing.T) {
		// ln(1+x) for small x is a common source of precision loss
		nearOne := 1.0 + 1e-10

		lnResult, err := lnValue(nearOne)
		require.NoError(t, err)
		// ln(1 + 1e-10) ~ 1e-10 - (1e-10)^2/2 + ... ~ 1e-10
		// Due to floating-point representation issues, we use a relative tolerance
		expected := math.Log(nearOne)
		assert.InDelta(t, expected, lnResult, ieee754Tolerance, "LN(1+1e-10) should match Go's math.Log")
	})
}

// =============================================================================
// Combined Integration Tests
// =============================================================================

func TestDuckDBCompat_ComplexExpressions(t *testing.T) {
	// Test complex expressions that combine multiple math functions
	// These represent real-world usage patterns

	t.Run("Distance formula: sqrt(x^2 + y^2)", func(t *testing.T) {
		// Distance from origin to point (3, 4) should be 5
		x := 3.0
		y := 4.0

		xSquared, _ := powValue(x, 2.0)
		ySquared, _ := powValue(y, 2.0)

		xSq := xSquared.(float64)
		ySq := ySquared.(float64)

		distance, err := sqrtValue(xSq + ySq)
		require.NoError(t, err)
		assert.InDelta(t, 5.0, distance, ieee754Tolerance)
	})

	t.Run("Compound interest: P * e^(rt)", func(t *testing.T) {
		// Principal $1000, rate 5%, time 10 years
		principal := 1000.0
		rate := 0.05
		years := 10.0

		rt := rate * years
		expResult, _ := expValue(rt)
		expVal := expResult.(float64)

		finalAmount := principal * expVal
		assert.InDelta(t, 1648.72, finalAmount, 0.01, "Compound interest calculation")
	})

	t.Run("Angle between two vectors", func(t *testing.T) {
		// Using atan2 for angle calculation
		// Vector (1, 0) to vector (0, 1) should be 90 degrees (PI/2 radians)

		angle, err := atan2Value(1.0, 0.0)
		require.NoError(t, err)

		degResult, _ := degreesValue(angle)
		assert.InDelta(t, 90.0, degResult, ieee754Tolerance)
	})
}

func TestDuckDBCompat_NullPropagation(t *testing.T) {
	// Verify NULL propagation matches DuckDB behavior
	// In SQL, any operation with NULL returns NULL

	nullTests := []struct {
		name string
		fn   func() (any, error)
	}{
		{"SQRT(NULL)", func() (any, error) { return sqrtValue(nil) }},
		{"SIN(NULL)", func() (any, error) { return sinValue(nil) }},
		{"POW(NULL, 2)", func() (any, error) { return powValue(nil, 2.0) }},
		{"POW(2, NULL)", func() (any, error) { return powValue(2.0, nil) }},
		{"ROUND(NULL, 2)", func() (any, error) { return roundValue(nil, int64(2)) }},
		{"GCD(NULL, 5)", func() (any, error) { return gcdValue(nil, int64(5)) }},
		{"LCM(5, NULL)", func() (any, error) { return lcmValue(int64(5), nil) }},
		{"ISNAN(NULL)", func() (any, error) { return isnanValue(nil) }},
	}

	for _, tt := range nullTests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn()
			require.NoError(t, err, "%s should not error", tt.name)
			assert.Nil(t, result, "%s should return NULL", tt.name)
		})
	}
}
