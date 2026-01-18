package binder

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
)

// TestInferCoalesceResultType tests the type inference for COALESCE and similar functions.
func TestInferCoalesceResultType(t *testing.T) {
	tests := []struct {
		name     string
		argTypes []dukdb.Type
		expected dukdb.Type
	}{
		{
			name:     "empty args",
			argTypes: []dukdb.Type{},
			expected: dukdb.TYPE_ANY,
		},
		{
			name:     "single INT",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "single VARCHAR",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "NULL then INT",
			argTypes: []dukdb.Type{dukdb.TYPE_SQLNULL, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "INT then NULL",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_SQLNULL},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "INT then BIGINT",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "SMALLINT then INTEGER then BIGINT",
			argTypes: []dukdb.Type{dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "INT then DOUBLE",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "FLOAT then DOUBLE",
			argTypes: []dukdb.Type{dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "VARCHAR then INT promotes to VARCHAR",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "INT then VARCHAR promotes to VARCHAR",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "ANY then INT yields INT",
			argTypes: []dukdb.Type{dukdb.TYPE_ANY, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "INT then ANY yields INT",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_ANY},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "multiple NULLs then INT",
			argTypes: []dukdb.Type{dukdb.TYPE_SQLNULL, dukdb.TYPE_SQLNULL, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock bound expressions with the specified types
			args := make([]BoundExpr, len(tt.argTypes))
			for i, typ := range tt.argTypes {
				args[i] = &BoundLiteral{ValType: typ}
			}

			result := inferCoalesceResultType(args)
			assert.Equal(t, tt.expected, result, "type mismatch for %s", tt.name)
		})
	}
}

// TestPromoteType tests the type promotion logic.
func TestPromoteType(t *testing.T) {
	tests := []struct {
		name     string
		t1       dukdb.Type
		t2       dukdb.Type
		expected dukdb.Type
	}{
		{"same type", dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
		{"ANY yields to concrete", dukdb.TYPE_ANY, dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
		{"concrete yields to ANY", dukdb.TYPE_INTEGER, dukdb.TYPE_ANY, dukdb.TYPE_INTEGER},
		{"NULL yields to INT", dukdb.TYPE_SQLNULL, dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
		{"INT yields to NULL", dukdb.TYPE_INTEGER, dukdb.TYPE_SQLNULL, dukdb.TYPE_INTEGER},
		{"INT to FLOAT promotes to FLOAT", dukdb.TYPE_INTEGER, dukdb.TYPE_FLOAT, dukdb.TYPE_FLOAT},
		{"FLOAT to INT promotes to FLOAT", dukdb.TYPE_FLOAT, dukdb.TYPE_INTEGER, dukdb.TYPE_FLOAT},
		{
			"INT to DOUBLE promotes to DOUBLE",
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_DOUBLE,
			dukdb.TYPE_DOUBLE,
		},
		{
			"FLOAT to DOUBLE promotes to DOUBLE",
			dukdb.TYPE_FLOAT,
			dukdb.TYPE_DOUBLE,
			dukdb.TYPE_DOUBLE,
		},
		{
			"SMALLINT to BIGINT promotes to BIGINT",
			dukdb.TYPE_SMALLINT,
			dukdb.TYPE_BIGINT,
			dukdb.TYPE_BIGINT,
		},
		{
			"VARCHAR mixed with INT yields VARCHAR",
			dukdb.TYPE_VARCHAR,
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_VARCHAR,
		},
		{
			"INT mixed with VARCHAR yields VARCHAR",
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_VARCHAR,
			dukdb.TYPE_VARCHAR,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := promoteType(tt.t1, tt.t2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestTimestampTypePromotion tests timestamp type promotion.
func TestTimestampTypePromotion(t *testing.T) {
	tests := []struct {
		name     string
		t1       dukdb.Type
		t2       dukdb.Type
		expected dukdb.Type
	}{
		{
			"TIMESTAMP_S to TIMESTAMP promotes to TIMESTAMP",
			dukdb.TYPE_TIMESTAMP_S,
			dukdb.TYPE_TIMESTAMP,
			dukdb.TYPE_TIMESTAMP,
		},
		{
			"TIMESTAMP to TIMESTAMP_NS promotes to TIMESTAMP_NS",
			dukdb.TYPE_TIMESTAMP,
			dukdb.TYPE_TIMESTAMP_NS,
			dukdb.TYPE_TIMESTAMP_NS,
		},
		{
			"TIMESTAMP_MS to TIMESTAMP_TZ promotes to TIMESTAMP_TZ",
			dukdb.TYPE_TIMESTAMP_MS,
			dukdb.TYPE_TIMESTAMP_TZ,
			dukdb.TYPE_TIMESTAMP_TZ,
		},
		{"same TIMESTAMP types", dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := promoteType(tt.t1, tt.t2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestInferCaseResultType tests CASE expression type inference.
func TestInferCaseResultType(t *testing.T) {
	tests := []struct {
		name      string
		whenTypes []dukdb.Type
		elseType  *dukdb.Type
		expected  dukdb.Type
	}{
		{
			name:      "no branches, no else",
			whenTypes: []dukdb.Type{},
			elseType:  nil,
			expected:  dukdb.TYPE_SQLNULL,
		},
		{
			name:      "single INT branch",
			whenTypes: []dukdb.Type{dukdb.TYPE_INTEGER},
			elseType:  nil,
			expected:  dukdb.TYPE_INTEGER,
		},
		{
			name:      "INT branch with BIGINT else",
			whenTypes: []dukdb.Type{dukdb.TYPE_INTEGER},
			elseType:  ptrType(dukdb.TYPE_BIGINT),
			expected:  dukdb.TYPE_BIGINT,
		},
		{
			name:      "multiple INT branches",
			whenTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
			elseType:  nil,
			expected:  dukdb.TYPE_INTEGER,
		},
		{
			name:      "INT and DOUBLE branches",
			whenTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE},
			elseType:  nil,
			expected:  dukdb.TYPE_DOUBLE,
		},
		{
			name:      "VARCHAR and INT branches promote to VARCHAR",
			whenTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER},
			elseType:  nil,
			expected:  dukdb.TYPE_VARCHAR,
		},
		{
			name:      "only else branch",
			whenTypes: []dukdb.Type{},
			elseType:  ptrType(dukdb.TYPE_DOUBLE),
			expected:  dukdb.TYPE_DOUBLE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock when clauses
			whens := make([]*BoundWhenClause, len(tt.whenTypes))
			for i, typ := range tt.whenTypes {
				whens[i] = &BoundWhenClause{
					Result: &BoundLiteral{ValType: typ},
				}
			}

			// Create mock else expression
			var elseExpr BoundExpr
			if tt.elseType != nil {
				elseExpr = &BoundLiteral{ValType: *tt.elseType}
			}

			result := inferCaseResultType(whens, elseExpr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func ptrType(t dukdb.Type) *dukdb.Type {
	return &t
}

// TestInferFunctionResultType tests function result type inference.
func TestInferFunctionResultType(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		argTypes []dukdb.Type
		expected dukdb.Type
	}{
		{
			name:     "COALESCE with INT and BIGINT",
			funcName: "COALESCE",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "COALESCE with NULL and INT",
			funcName: "COALESCE",
			argTypes: []dukdb.Type{dukdb.TYPE_SQLNULL, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "NULLIF with INT args",
			funcName: "NULLIF",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "NULLIF with VARCHAR args",
			funcName: "NULLIF",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "GREATEST with multiple INTs",
			funcName: "GREATEST",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "GREATEST with INT and DOUBLE",
			funcName: "GREATEST",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "LEAST with VARCHAR values",
			funcName: "LEAST",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "LEAST with mixed numeric types",
			funcName: "LEAST",
			argTypes: []dukdb.Type{dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT},
			expected: dukdb.TYPE_BIGINT,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock bound expressions
			args := make([]BoundExpr, len(tt.argTypes))
			for i, typ := range tt.argTypes {
				args[i] = &BoundLiteral{ValType: typ}
			}

			result := inferFunctionResultType(tt.funcName, args)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestInferMathFunctionResultType tests type inference for math functions.
func TestInferMathFunctionResultType(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		argTypes []dukdb.Type
		expected dukdb.Type
	}{
		// Rounding functions - preserve integer types
		{
			name:     "ROUND with INTEGER preserves INTEGER",
			funcName: "ROUND",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "ROUND with BIGINT preserves BIGINT",
			funcName: "ROUND",
			argTypes: []dukdb.Type{dukdb.TYPE_BIGINT},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "ROUND with SMALLINT preserves SMALLINT",
			funcName: "ROUND",
			argTypes: []dukdb.Type{dukdb.TYPE_SMALLINT},
			expected: dukdb.TYPE_SMALLINT,
		},
		{
			name:     "ROUND with DOUBLE returns DOUBLE",
			funcName: "ROUND",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "ROUND with FLOAT returns DOUBLE",
			funcName: "ROUND",
			argTypes: []dukdb.Type{dukdb.TYPE_FLOAT},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "ROUND with INTEGER and decimals preserves INTEGER",
			funcName: "ROUND",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "ROUND_EVEN with INTEGER preserves INTEGER",
			funcName: "ROUND_EVEN",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "ROUND_EVEN with DOUBLE returns DOUBLE",
			funcName: "ROUND_EVEN",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "CEIL with INTEGER preserves INTEGER",
			funcName: "CEIL",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "CEILING with DOUBLE returns DOUBLE",
			funcName: "CEILING",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "FLOOR with BIGINT preserves BIGINT",
			funcName: "FLOOR",
			argTypes: []dukdb.Type{dukdb.TYPE_BIGINT},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "FLOOR with DOUBLE returns DOUBLE",
			funcName: "FLOOR",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "TRUNC with INTEGER preserves INTEGER",
			funcName: "TRUNC",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "TRUNCATE with DOUBLE returns DOUBLE",
			funcName: "TRUNCATE",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "EVEN with INTEGER preserves INTEGER",
			funcName: "EVEN",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "EVEN with DOUBLE returns DOUBLE",
			funcName: "EVEN",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},

		// Scientific functions - always return DOUBLE
		{
			name:     "SQRT returns DOUBLE",
			funcName: "SQRT",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "SQRT with INTEGER input returns DOUBLE",
			funcName: "SQRT",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "CBRT returns DOUBLE",
			funcName: "CBRT",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "POW returns DOUBLE",
			funcName: "POW",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE, dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "POWER returns DOUBLE",
			funcName: "POWER",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "EXP returns DOUBLE",
			funcName: "EXP",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "LN returns DOUBLE",
			funcName: "LN",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "LOG returns DOUBLE",
			funcName: "LOG",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "LOG10 returns DOUBLE",
			funcName: "LOG10",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "LOG2 returns DOUBLE",
			funcName: "LOG2",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "GAMMA returns DOUBLE",
			funcName: "GAMMA",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "LGAMMA returns DOUBLE",
			funcName: "LGAMMA",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},

		// FACTORIAL returns BIGINT
		{
			name:     "FACTORIAL returns BIGINT",
			funcName: "FACTORIAL",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_BIGINT,
		},

		// Trigonometric functions - always return DOUBLE
		{
			name:     "SIN returns DOUBLE",
			funcName: "SIN",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "COS returns DOUBLE",
			funcName: "COS",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "TAN returns DOUBLE",
			funcName: "TAN",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "COT returns DOUBLE",
			funcName: "COT",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "ASIN returns DOUBLE",
			funcName: "ASIN",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "ACOS returns DOUBLE",
			funcName: "ACOS",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "ATAN returns DOUBLE",
			funcName: "ATAN",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "ATAN2 returns DOUBLE",
			funcName: "ATAN2",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE, dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "DEGREES returns DOUBLE",
			funcName: "DEGREES",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "RADIANS returns DOUBLE",
			funcName: "RADIANS",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},

		// Hyperbolic functions - always return DOUBLE
		{
			name:     "SINH returns DOUBLE",
			funcName: "SINH",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "COSH returns DOUBLE",
			funcName: "COSH",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "TANH returns DOUBLE",
			funcName: "TANH",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "ASINH returns DOUBLE",
			funcName: "ASINH",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "ACOSH returns DOUBLE",
			funcName: "ACOSH",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "ATANH returns DOUBLE",
			funcName: "ATANH",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_DOUBLE,
		},

		// Utility functions
		{
			name:     "PI returns DOUBLE",
			funcName: "PI",
			argTypes: []dukdb.Type{},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "RANDOM returns DOUBLE",
			funcName: "RANDOM",
			argTypes: []dukdb.Type{},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "RAND returns DOUBLE",
			funcName: "RAND",
			argTypes: []dukdb.Type{},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "SIGN returns INTEGER",
			funcName: "SIGN",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_INTEGER,
		},
		{
			name:     "GCD returns BIGINT",
			funcName: "GCD",
			argTypes: []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_BIGINT},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "LCM returns BIGINT",
			funcName: "LCM",
			argTypes: []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_BIGINT},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "ISNAN returns BOOLEAN",
			funcName: "ISNAN",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_BOOLEAN,
		},
		{
			name:     "ISINF returns BOOLEAN",
			funcName: "ISINF",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_BOOLEAN,
		},
		{
			name:     "ISFINITE returns BOOLEAN",
			funcName: "ISFINITE",
			argTypes: []dukdb.Type{dukdb.TYPE_DOUBLE},
			expected: dukdb.TYPE_BOOLEAN,
		},

		// Bitwise function
		{
			name:     "BIT_COUNT returns INTEGER",
			funcName: "BIT_COUNT",
			argTypes: []dukdb.Type{dukdb.TYPE_BIGINT},
			expected: dukdb.TYPE_INTEGER,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock bound expressions
			args := make([]BoundExpr, len(tt.argTypes))
			for i, typ := range tt.argTypes {
				args[i] = &BoundLiteral{ValType: typ}
			}

			result := inferFunctionResultType(tt.funcName, args)
			assert.Equal(t, tt.expected, result, "type mismatch for %s", tt.funcName)
		})
	}
}

// TestRoundingFunctionsTypePreservation tests that rounding functions preserve input types correctly.
func TestRoundingFunctionsTypePreservation(t *testing.T) {
	integerTypes := []dukdb.Type{
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_UTINYINT,
		dukdb.TYPE_USMALLINT,
		dukdb.TYPE_UINTEGER,
		dukdb.TYPE_UBIGINT,
	}

	floatTypes := []dukdb.Type{
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
	}

	roundingFuncs := []string{
		"ROUND",
		"ROUND_EVEN",
		"CEIL",
		"CEILING",
		"FLOOR",
		"TRUNC",
		"TRUNCATE",
		"EVEN",
	}

	// Test that integer types are preserved
	for _, funcName := range roundingFuncs {
		for _, intType := range integerTypes {
			t.Run(funcName+"_preserves_"+intType.String(), func(t *testing.T) {
				args := []BoundExpr{&BoundLiteral{ValType: intType}}
				result := inferFunctionResultType(funcName, args)
				assert.Equal(t, intType, result, "%s should preserve %s", funcName, intType)
			})
		}
	}

	// Test that float types return DOUBLE
	for _, funcName := range roundingFuncs {
		for _, floatType := range floatTypes {
			t.Run(funcName+"_float_to_double_"+floatType.String(), func(t *testing.T) {
				args := []BoundExpr{&BoundLiteral{ValType: floatType}}
				result := inferFunctionResultType(funcName, args)
				assert.Equal(
					t,
					dukdb.TYPE_DOUBLE,
					result,
					"%s with %s should return DOUBLE",
					funcName,
					floatType,
				)
			})
		}
	}
}

// TestScientificFunctionsAlwaysReturnDouble tests that scientific functions always return DOUBLE.
func TestScientificFunctionsAlwaysReturnDouble(t *testing.T) {
	scientificFuncs := []string{
		"SQRT", "CBRT", "EXP", "LN", "LOG", "LOG10", "LOG2", "GAMMA", "LGAMMA",
	}

	allNumericTypes := []dukdb.Type{
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
	}

	for _, funcName := range scientificFuncs {
		for _, inputType := range allNumericTypes {
			t.Run(funcName+"_with_"+inputType.String(), func(t *testing.T) {
				args := []BoundExpr{&BoundLiteral{ValType: inputType}}
				result := inferFunctionResultType(funcName, args)
				assert.Equal(t, dukdb.TYPE_DOUBLE, result,
					"%s with %s input should return DOUBLE", funcName, inputType)
			})
		}
	}
}

// TestTrigFunctionsAlwaysReturnDouble tests that trig functions always return DOUBLE.
func TestTrigFunctionsAlwaysReturnDouble(t *testing.T) {
	trigFuncs := []string{
		"SIN", "COS", "TAN", "COT",
		"ASIN", "ACOS", "ATAN",
		"DEGREES", "RADIANS",
	}

	allNumericTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
	}

	for _, funcName := range trigFuncs {
		for _, inputType := range allNumericTypes {
			t.Run(funcName+"_with_"+inputType.String(), func(t *testing.T) {
				args := []BoundExpr{&BoundLiteral{ValType: inputType}}
				result := inferFunctionResultType(funcName, args)
				assert.Equal(t, dukdb.TYPE_DOUBLE, result,
					"%s with %s input should return DOUBLE", funcName, inputType)
			})
		}
	}

	// Test ATAN2 separately since it takes 2 args
	t.Run("ATAN2_returns_DOUBLE", func(t *testing.T) {
		args := []BoundExpr{
			&BoundLiteral{ValType: dukdb.TYPE_DOUBLE},
			&BoundLiteral{ValType: dukdb.TYPE_DOUBLE},
		}
		result := inferFunctionResultType("ATAN2", args)
		assert.Equal(t, dukdb.TYPE_DOUBLE, result)
	})
}

// TestHyperbolicFunctionsAlwaysReturnDouble tests that hyperbolic functions always return DOUBLE.
func TestHyperbolicFunctionsAlwaysReturnDouble(t *testing.T) {
	hyperbolicFuncs := []string{
		"SINH", "COSH", "TANH",
		"ASINH", "ACOSH", "ATANH",
	}

	allNumericTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
	}

	for _, funcName := range hyperbolicFuncs {
		for _, inputType := range allNumericTypes {
			t.Run(funcName+"_with_"+inputType.String(), func(t *testing.T) {
				args := []BoundExpr{&BoundLiteral{ValType: inputType}}
				result := inferFunctionResultType(funcName, args)
				assert.Equal(t, dukdb.TYPE_DOUBLE, result,
					"%s with %s input should return DOUBLE", funcName, inputType)
			})
		}
	}
}

// TestBooleanPredicates tests ISNAN, ISINF, ISFINITE return BOOLEAN.
func TestBooleanPredicates(t *testing.T) {
	predicates := []string{"ISNAN", "ISINF", "ISFINITE"}

	for _, funcName := range predicates {
		t.Run(funcName+"_returns_BOOLEAN", func(t *testing.T) {
			args := []BoundExpr{&BoundLiteral{ValType: dukdb.TYPE_DOUBLE}}
			result := inferFunctionResultType(funcName, args)
			assert.Equal(t, dukdb.TYPE_BOOLEAN, result)
		})
	}
}

// =============================================================================
// String Function Type Inference Tests (Task 10.8)
// Tests that inferFunctionResultType returns correct types for string functions
// =============================================================================

// TestInferStringFunctionReturnType_Boolean tests string functions that return BOOLEAN.
func TestInferStringFunctionReturnType_Boolean(t *testing.T) {
	booleanFuncs := []string{
		"REGEXP_MATCHES",
		"CONTAINS",
		"PREFIX",
		"SUFFIX",
		"STARTS_WITH",
		"ENDS_WITH",
	}

	for _, funcName := range booleanFuncs {
		t.Run(funcName+"_returns_BOOLEAN", func(t *testing.T) {
			args := []BoundExpr{
				&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
				&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
			}
			result := inferFunctionResultType(funcName, args)
			assert.Equal(t, dukdb.TYPE_BOOLEAN, result, "%s should return BOOLEAN", funcName)
		})
	}
}

// TestInferStringFunctionReturnType_BIGINT tests string functions that return BIGINT.
func TestInferStringFunctionReturnType_BIGINT(t *testing.T) {
	bigintFuncs := []string{
		"POSITION",
		"STRPOS",
		"INSTR",
		"ASCII",
		"UNICODE",
		"LEVENSHTEIN",
		"DAMERAU_LEVENSHTEIN",
		"HAMMING",
		"HASH",
	}

	for _, funcName := range bigintFuncs {
		t.Run(funcName+"_returns_BIGINT", func(t *testing.T) {
			// Create appropriate args based on function
			var args []BoundExpr
			switch funcName {
			case "ASCII", "UNICODE", "HASH":
				args = []BoundExpr{&BoundLiteral{ValType: dukdb.TYPE_VARCHAR}}
			default:
				args = []BoundExpr{
					&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
					&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
				}
			}
			result := inferFunctionResultType(funcName, args)
			assert.Equal(t, dukdb.TYPE_BIGINT, result, "%s should return BIGINT", funcName)
		})
	}
}

// TestInferStringFunctionReturnType_DOUBLE tests string functions that return DOUBLE.
func TestInferStringFunctionReturnType_DOUBLE(t *testing.T) {
	doubleFuncs := []string{
		"JACCARD",
		"JARO_SIMILARITY",
		"JARO_WINKLER_SIMILARITY",
	}

	for _, funcName := range doubleFuncs {
		t.Run(funcName+"_returns_DOUBLE", func(t *testing.T) {
			args := []BoundExpr{
				&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
				&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
			}
			result := inferFunctionResultType(funcName, args)
			assert.Equal(t, dukdb.TYPE_DOUBLE, result, "%s should return DOUBLE", funcName)
		})
	}
}

// TestInferStringFunctionReturnType_VARCHAR tests string functions that return VARCHAR.
func TestInferStringFunctionReturnType_VARCHAR(t *testing.T) {
	varcharFuncs := []string{
		"REGEXP_REPLACE",
		"REGEXP_EXTRACT",
		"CONCAT_WS",
		"LPAD",
		"RPAD",
		"REVERSE",
		"REPEAT",
		"LEFT",
		"RIGHT",
		"STRIP",
		"LSTRIP",
		"RSTRIP",
		"CHR",
		"MD5",
		"SHA256",
	}

	for _, funcName := range varcharFuncs {
		t.Run(funcName+"_returns_VARCHAR", func(t *testing.T) {
			// Create appropriate args based on function
			var args []BoundExpr
			switch funcName {
			case "REVERSE", "STRIP", "LSTRIP", "RSTRIP", "MD5", "SHA256":
				args = []BoundExpr{&BoundLiteral{ValType: dukdb.TYPE_VARCHAR}}
			case "CHR":
				args = []BoundExpr{&BoundLiteral{ValType: dukdb.TYPE_INTEGER}}
			case "REPEAT", "LEFT", "RIGHT":
				args = []BoundExpr{
					&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
					&BoundLiteral{ValType: dukdb.TYPE_INTEGER},
				}
			case "LPAD", "RPAD":
				args = []BoundExpr{
					&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
					&BoundLiteral{ValType: dukdb.TYPE_INTEGER},
					&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
				}
			default:
				// REGEXP_REPLACE, REGEXP_EXTRACT, CONCAT_WS
				args = []BoundExpr{
					&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
					&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
				}
			}
			result := inferFunctionResultType(funcName, args)
			assert.Equal(t, dukdb.TYPE_VARCHAR, result, "%s should return VARCHAR", funcName)
		})
	}
}

// TestInferStringFunctionReturnType_LIST tests string functions that return LIST (TYPE_ANY).
func TestInferStringFunctionReturnType_LIST(t *testing.T) {
	listFuncs := []string{
		"REGEXP_EXTRACT_ALL",
		"REGEXP_SPLIT_TO_ARRAY",
		"STRING_SPLIT",
		"STRING_SPLIT_REGEX",
	}

	for _, funcName := range listFuncs {
		t.Run(funcName+"_returns_TYPE_ANY_for_LIST", func(t *testing.T) {
			args := []BoundExpr{
				&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
				&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
			}
			result := inferFunctionResultType(funcName, args)
			// LIST types are represented as TYPE_ANY since there's no dedicated LIST type constant
			assert.Equal(
				t,
				dukdb.TYPE_ANY,
				result,
				"%s should return TYPE_ANY (representing LIST)",
				funcName,
			)
		})
	}
}

// TestInferStringFunctionReturnType_Comprehensive tests all string functions with proper argument types.
func TestInferStringFunctionReturnType_Comprehensive(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		argTypes []dukdb.Type
		expected dukdb.Type
	}{
		// Boolean-returning functions
		{
			name:     "REGEXP_MATCHES returns BOOLEAN",
			funcName: "REGEXP_MATCHES",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BOOLEAN,
		},
		{
			name:     "CONTAINS returns BOOLEAN",
			funcName: "CONTAINS",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BOOLEAN,
		},
		{
			name:     "PREFIX returns BOOLEAN",
			funcName: "PREFIX",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BOOLEAN,
		},
		{
			name:     "SUFFIX returns BOOLEAN",
			funcName: "SUFFIX",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BOOLEAN,
		},
		{
			name:     "STARTS_WITH returns BOOLEAN",
			funcName: "STARTS_WITH",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BOOLEAN,
		},
		{
			name:     "ENDS_WITH returns BOOLEAN",
			funcName: "ENDS_WITH",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BOOLEAN,
		},

		// BIGINT-returning functions (position, encoding, distance)
		{
			name:     "POSITION returns BIGINT",
			funcName: "POSITION",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "STRPOS returns BIGINT",
			funcName: "STRPOS",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "INSTR returns BIGINT",
			funcName: "INSTR",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "ASCII returns BIGINT",
			funcName: "ASCII",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "UNICODE returns BIGINT",
			funcName: "UNICODE",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "LEVENSHTEIN returns BIGINT",
			funcName: "LEVENSHTEIN",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "DAMERAU_LEVENSHTEIN returns BIGINT",
			funcName: "DAMERAU_LEVENSHTEIN",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "HAMMING returns BIGINT",
			funcName: "HAMMING",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BIGINT,
		},
		{
			name:     "HASH returns BIGINT",
			funcName: "HASH",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_BIGINT,
		},

		// DOUBLE-returning functions (similarity scores)
		{
			name:     "JACCARD returns DOUBLE",
			funcName: "JACCARD",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "JARO_SIMILARITY returns DOUBLE",
			funcName: "JARO_SIMILARITY",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_DOUBLE,
		},
		{
			name:     "JARO_WINKLER_SIMILARITY returns DOUBLE",
			funcName: "JARO_WINKLER_SIMILARITY",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_DOUBLE,
		},

		// VARCHAR-returning functions (hash, manipulation)
		{
			name:     "MD5 returns VARCHAR",
			funcName: "MD5",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "SHA256 returns VARCHAR",
			funcName: "SHA256",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "REVERSE returns VARCHAR",
			funcName: "REVERSE",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "REPEAT returns VARCHAR",
			funcName: "REPEAT",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "LEFT returns VARCHAR",
			funcName: "LEFT",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "RIGHT returns VARCHAR",
			funcName: "RIGHT",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "LPAD returns VARCHAR",
			funcName: "LPAD",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "RPAD returns VARCHAR",
			funcName: "RPAD",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "CHR returns VARCHAR",
			funcName: "CHR",
			argTypes: []dukdb.Type{dukdb.TYPE_INTEGER},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "CONCAT_WS returns VARCHAR",
			funcName: "CONCAT_WS",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "REGEXP_REPLACE returns VARCHAR",
			funcName: "REGEXP_REPLACE",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "REGEXP_EXTRACT returns VARCHAR",
			funcName: "REGEXP_EXTRACT",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "STRIP returns VARCHAR",
			funcName: "STRIP",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "LSTRIP returns VARCHAR",
			funcName: "LSTRIP",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},
		{
			name:     "RSTRIP returns VARCHAR",
			funcName: "RSTRIP",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_VARCHAR,
		},

		// LIST-returning functions (represented as TYPE_ANY)
		{
			name:     "REGEXP_EXTRACT_ALL returns TYPE_ANY for LIST",
			funcName: "REGEXP_EXTRACT_ALL",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_ANY,
		},
		{
			name:     "REGEXP_SPLIT_TO_ARRAY returns TYPE_ANY for LIST",
			funcName: "REGEXP_SPLIT_TO_ARRAY",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_ANY,
		},
		{
			name:     "STRING_SPLIT returns TYPE_ANY for LIST",
			funcName: "STRING_SPLIT",
			argTypes: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
			expected: dukdb.TYPE_ANY,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := make([]BoundExpr, len(tt.argTypes))
			for i, typ := range tt.argTypes {
				args[i] = &BoundLiteral{ValType: typ}
			}
			result := inferFunctionResultType(tt.funcName, args)
			assert.Equal(t, tt.expected, result, "type mismatch for %s", tt.funcName)
		})
	}
}

// TestInferStringFunctionReturnType_CaseInsensitive tests that function names are case-insensitive.
func TestInferStringFunctionReturnType_CaseInsensitive(t *testing.T) {
	tests := []struct {
		funcNameUpper string
		funcNameLower string
		funcNameMixed string
		expected      dukdb.Type
	}{
		{"MD5", "md5", "Md5", dukdb.TYPE_VARCHAR},
		{"SHA256", "sha256", "Sha256", dukdb.TYPE_VARCHAR},
		{"LEVENSHTEIN", "levenshtein", "Levenshtein", dukdb.TYPE_BIGINT},
		{"CONTAINS", "contains", "Contains", dukdb.TYPE_BOOLEAN},
		{"JACCARD", "jaccard", "Jaccard", dukdb.TYPE_DOUBLE},
	}

	for _, tt := range tests {
		t.Run(tt.funcNameUpper+"_case_insensitive", func(t *testing.T) {
			args := []BoundExpr{
				&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
				&BoundLiteral{ValType: dukdb.TYPE_VARCHAR},
			}

			resultUpper := inferFunctionResultType(tt.funcNameUpper, args)
			resultLower := inferFunctionResultType(tt.funcNameLower, args)
			resultMixed := inferFunctionResultType(tt.funcNameMixed, args)

			assert.Equal(t, tt.expected, resultUpper, "upper case mismatch")
			assert.Equal(t, tt.expected, resultLower, "lower case mismatch")
			assert.Equal(t, tt.expected, resultMixed, "mixed case mismatch")
		})
	}
}

// TestGetFunctionArgTypes_StringFunctions tests getFunctionArgTypes for string functions.
func TestGetFunctionArgTypes_StringFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		argCount int
		expected []dukdb.Type
	}{
		// Regex functions
		{
			name:     "REGEXP_MATCHES expects 2 VARCHAR args",
			funcName: "REGEXP_MATCHES",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "REGEXP_REPLACE expects 3+ VARCHAR args",
			funcName: "REGEXP_REPLACE",
			argCount: 3,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "REGEXP_EXTRACT expects 2 VARCHAR, optional INTEGER",
			funcName: "REGEXP_EXTRACT",
			argCount: 3,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER},
		},

		// String manipulation functions
		{
			name:     "REVERSE expects 1 VARCHAR arg",
			funcName: "REVERSE",
			argCount: 1,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR},
		},
		{
			name:     "REPEAT expects VARCHAR and INTEGER",
			funcName: "REPEAT",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER},
		},
		{
			name:     "LEFT expects VARCHAR and INTEGER",
			funcName: "LEFT",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER},
		},
		{
			name:     "RIGHT expects VARCHAR and INTEGER",
			funcName: "RIGHT",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER},
		},
		{
			name:     "LPAD expects VARCHAR, INTEGER, optional VARCHAR",
			funcName: "LPAD",
			argCount: 3,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "RPAD expects VARCHAR, INTEGER, optional VARCHAR",
			funcName: "RPAD",
			argCount: 3,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
		},

		// Position functions
		{
			name:     "POSITION expects 2 VARCHAR args",
			funcName: "POSITION",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "STRPOS expects 2 VARCHAR args",
			funcName: "STRPOS",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "CONTAINS expects 2 VARCHAR args",
			funcName: "CONTAINS",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "PREFIX expects 2 VARCHAR args",
			funcName: "PREFIX",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},

		// Encoding functions
		{
			name:     "ASCII expects 1 VARCHAR arg",
			funcName: "ASCII",
			argCount: 1,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR},
		},
		{
			name:     "UNICODE expects 1 VARCHAR arg",
			funcName: "UNICODE",
			argCount: 1,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR},
		},
		{
			name:     "CHR expects 1 INTEGER arg",
			funcName: "CHR",
			argCount: 1,
			expected: []dukdb.Type{dukdb.TYPE_INTEGER},
		},

		// Hash functions
		{
			name:     "MD5 expects 1 VARCHAR arg",
			funcName: "MD5",
			argCount: 1,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR},
		},
		{
			name:     "SHA256 expects 1 VARCHAR arg",
			funcName: "SHA256",
			argCount: 1,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR},
		},
		{
			name:     "HASH expects 1 VARCHAR arg",
			funcName: "HASH",
			argCount: 1,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR},
		},

		// Distance functions
		{
			name:     "LEVENSHTEIN expects 2 VARCHAR args",
			funcName: "LEVENSHTEIN",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "DAMERAU_LEVENSHTEIN expects 2 VARCHAR args",
			funcName: "DAMERAU_LEVENSHTEIN",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "HAMMING expects 2 VARCHAR args",
			funcName: "HAMMING",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "JACCARD expects 2 VARCHAR args",
			funcName: "JACCARD",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "JARO_SIMILARITY expects 2 VARCHAR args",
			funcName: "JARO_SIMILARITY",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "JARO_WINKLER_SIMILARITY expects 2 VARCHAR args",
			funcName: "JARO_WINKLER_SIMILARITY",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},

		// Split functions
		{
			name:     "STRING_SPLIT expects 2 VARCHAR args",
			funcName: "STRING_SPLIT",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
		{
			name:     "REGEXP_SPLIT_TO_ARRAY expects 2 VARCHAR args",
			funcName: "REGEXP_SPLIT_TO_ARRAY",
			argCount: 2,
			expected: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFunctionArgTypes(tt.funcName, tt.argCount)
			assert.Equal(t, tt.expected, result, "arg types mismatch for %s", tt.funcName)
		})
	}
}
