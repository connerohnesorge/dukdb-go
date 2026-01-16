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
