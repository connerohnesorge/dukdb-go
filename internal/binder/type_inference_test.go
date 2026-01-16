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
		{"INT to DOUBLE promotes to DOUBLE", dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE, dukdb.TYPE_DOUBLE},
		{"FLOAT to DOUBLE promotes to DOUBLE", dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE, dukdb.TYPE_DOUBLE},
		{"SMALLINT to BIGINT promotes to BIGINT", dukdb.TYPE_SMALLINT, dukdb.TYPE_BIGINT, dukdb.TYPE_BIGINT},
		{"VARCHAR mixed with INT yields VARCHAR", dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
		{"INT mixed with VARCHAR yields VARCHAR", dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
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
		{"TIMESTAMP_S to TIMESTAMP promotes to TIMESTAMP", dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP},
		{"TIMESTAMP to TIMESTAMP_NS promotes to TIMESTAMP_NS", dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_NS, dukdb.TYPE_TIMESTAMP_NS},
		{"TIMESTAMP_MS to TIMESTAMP_TZ promotes to TIMESTAMP_TZ", dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_TZ, dukdb.TYPE_TIMESTAMP_TZ},
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
		name       string
		whenTypes  []dukdb.Type
		elseType   *dukdb.Type
		expected   dukdb.Type
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

	roundingFuncs := []string{"ROUND", "ROUND_EVEN", "CEIL", "CEILING", "FLOOR", "TRUNC", "TRUNCATE", "EVEN"}

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
				assert.Equal(t, dukdb.TYPE_DOUBLE, result, "%s with %s should return DOUBLE", funcName, floatType)
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
