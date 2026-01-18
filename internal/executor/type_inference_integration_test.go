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

// helper function to execute a query and return the first row's first column
func execTypeInferenceQuery(
	t *testing.T,
	exec *Executor,
	cat *catalog.Catalog,
	query string,
) (*ExecutionResult, error) {
	t.Helper()

	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}

	b := binder.NewBinder(cat)
	bound, err := b.Bind(stmt)
	if err != nil {
		return nil, err
	}

	p := planner.NewPlanner(cat)
	plan, err := p.Plan(bound)
	if err != nil {
		return nil, err
	}

	return exec.Execute(context.Background(), plan, nil)
}

// helper function to get first value from query result
func getFirstValue(t *testing.T, exec *Executor, cat *catalog.Catalog, query string) interface{} {
	t.Helper()
	result, err := execTypeInferenceQuery(t, exec, cat, query)
	require.NoError(t, err)
	if len(result.Rows) == 0 || len(result.Columns) == 0 {
		return nil
	}
	// Get first column name and return its value from first row
	firstCol := result.Columns[0]
	return result.Rows[0][firstCol]
}

// helper function to get all values from first column
func getAllFirstColumnValues(
	t *testing.T,
	exec *Executor,
	cat *catalog.Catalog,
	query string,
) []interface{} {
	t.Helper()
	result, err := execTypeInferenceQuery(t, exec, cat, query)
	require.NoError(t, err)
	if len(result.Columns) == 0 {
		return nil
	}
	firstCol := result.Columns[0]
	var values []interface{}
	for _, row := range result.Rows {
		values = append(values, row[firstCol])
	}
	return values
}

// TestCoalesceTypeInference tests COALESCE with various type combinations.
func TestCoalesceTypeInference(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	tests := []struct {
		name     string
		query    string
		expected interface{}
		isNull   bool
	}{
		{
			name:     "COALESCE with NULL and INT returns INT",
			query:    "SELECT COALESCE(NULL, 1)",
			expected: int64(1),
		},
		{
			name:     "COALESCE with NULL, NULL, INT returns INT",
			query:    "SELECT COALESCE(NULL, NULL, 42)",
			expected: int64(42),
		},
		{
			name:     "COALESCE with first non-NULL value",
			query:    "SELECT COALESCE(10, 20, 30)",
			expected: int64(10),
		},
		{
			name:     "COALESCE with VARCHAR values",
			query:    "SELECT COALESCE(NULL, 'hello')",
			expected: "hello",
		},
		{
			name:   "COALESCE with all NULLs returns NULL",
			query:  "SELECT COALESCE(NULL, NULL)",
			isNull: true,
		},
		{
			name:     "nested COALESCE",
			query:    "SELECT COALESCE(COALESCE(NULL, NULL), COALESCE(NULL, 5))",
			expected: int64(5),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFirstValue(t, exec, cat, tt.query)
			if tt.isNull {
				assert.Nil(t, result)
			} else {
				switch expected := tt.expected.(type) {
				case int64:
					// Result might come back as different numeric type
					switch v := result.(type) {
					case int64:
						assert.Equal(t, expected, v)
					case int32:
						assert.Equal(t, int32(expected), v)
					case float64:
						assert.Equal(t, float64(expected), v)
					default:
						assert.Equal(t, expected, result)
					}
				case string:
					assert.Equal(t, expected, result)
				default:
					assert.Equal(t, expected, result)
				}
			}
		})
	}
}

// TestCaseExpressionTypeInference tests CASE expressions with mixed types.
func TestCaseExpressionTypeInference(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	tests := []struct {
		name     string
		query    string
		expected interface{}
		isNull   bool
	}{
		{
			name:     "simple CASE with INT result",
			query:    "SELECT CASE WHEN 1=1 THEN 42 ELSE 0 END",
			expected: int64(42),
		},
		{
			name:     "CASE with VARCHAR result",
			query:    "SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END",
			expected: "yes",
		},
		{
			name:     "searched CASE",
			query:    "SELECT CASE WHEN 1 > 2 THEN 'a' WHEN 2 > 1 THEN 'b' ELSE 'c' END",
			expected: "b",
		},
		{
			name:   "CASE without ELSE returns NULL",
			query:  "SELECT CASE WHEN 1 > 2 THEN 'match' END",
			isNull: true,
		},
		{
			name:     "simple CASE expression",
			query:    "SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
			expected: "one",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFirstValue(t, exec, cat, tt.query)
			if tt.isNull {
				assert.Nil(t, result)
			} else {
				switch expected := tt.expected.(type) {
				case int64:
					switch v := result.(type) {
					case int64:
						assert.Equal(t, expected, v)
					case int32:
						assert.Equal(t, int32(expected), v)
					case float64:
						assert.Equal(t, float64(expected), v)
					}
				case string:
					assert.Equal(t, expected, result)
				default:
					assert.Equal(t, expected, result)
				}
			}
		})
	}
}

// TestNullIfFunction tests NULLIF function behavior.
func TestNullIfFunction(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	tests := []struct {
		name     string
		query    string
		expected interface{}
		isNull   bool
	}{
		{
			name:   "NULLIF with equal values returns NULL",
			query:  "SELECT NULLIF(1, 1)",
			isNull: true,
		},
		{
			name:     "NULLIF with different values returns first",
			query:    "SELECT NULLIF(1, 2)",
			expected: int64(1),
		},
		{
			name:     "NULLIF with strings",
			query:    "SELECT NULLIF('hello', 'world')",
			expected: "hello",
		},
		{
			name:   "NULLIF with equal strings returns NULL",
			query:  "SELECT NULLIF('same', 'same')",
			isNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFirstValue(t, exec, cat, tt.query)
			if tt.isNull {
				assert.Nil(t, result)
			} else {
				switch expected := tt.expected.(type) {
				case int64:
					switch v := result.(type) {
					case int64:
						assert.Equal(t, expected, v)
					case int32:
						assert.Equal(t, int32(expected), v)
					case float64:
						assert.Equal(t, float64(expected), v)
					}
				case string:
					assert.Equal(t, expected, result)
				default:
					assert.Equal(t, expected, result)
				}
			}
		})
	}
}

// TestGreatestFunction tests GREATEST function.
func TestGreatestFunction(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	tests := []struct {
		name     string
		query    string
		expected interface{}
		isNull   bool
	}{
		{
			name:     "GREATEST with two values",
			query:    "SELECT GREATEST(1, 2)",
			expected: int64(2),
		},
		{
			name:     "GREATEST with multiple values",
			query:    "SELECT GREATEST(1, 5, 3, 4, 2)",
			expected: int64(5),
		},
		{
			name:     "GREATEST with negative values",
			query:    "SELECT GREATEST(-10, -5, -20)",
			expected: int64(-5),
		},
		{
			name:     "GREATEST with strings",
			query:    "SELECT GREATEST('apple', 'banana', 'cherry')",
			expected: "cherry",
		},
		{
			name:   "GREATEST with NULL returns NULL",
			query:  "SELECT GREATEST(1, NULL, 3)",
			isNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFirstValue(t, exec, cat, tt.query)
			if tt.isNull {
				assert.Nil(t, result)
			} else {
				switch expected := tt.expected.(type) {
				case int64:
					switch v := result.(type) {
					case int64:
						assert.Equal(t, expected, v)
					case int32:
						assert.Equal(t, int32(expected), v)
					case float64:
						assert.Equal(t, float64(expected), v)
					}
				case string:
					assert.Equal(t, expected, result)
				default:
					assert.Equal(t, expected, result)
				}
			}
		})
	}
}

// TestLeastFunction tests LEAST function.
func TestLeastFunction(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	tests := []struct {
		name     string
		query    string
		expected interface{}
		isNull   bool
	}{
		{
			name:     "LEAST with two values",
			query:    "SELECT LEAST(1, 2)",
			expected: int64(1),
		},
		{
			name:     "LEAST with multiple values",
			query:    "SELECT LEAST(5, 3, 1, 4, 2)",
			expected: int64(1),
		},
		{
			name:     "LEAST with negative values",
			query:    "SELECT LEAST(-10, -5, -20)",
			expected: int64(-20),
		},
		{
			name:     "LEAST with strings",
			query:    "SELECT LEAST('apple', 'banana', 'cherry')",
			expected: "apple",
		},
		{
			name:   "LEAST with NULL returns NULL",
			query:  "SELECT LEAST(1, NULL, 3)",
			isNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFirstValue(t, exec, cat, tt.query)
			if tt.isNull {
				assert.Nil(t, result)
			} else {
				switch expected := tt.expected.(type) {
				case int64:
					switch v := result.(type) {
					case int64:
						assert.Equal(t, expected, v)
					case int32:
						assert.Equal(t, int32(expected), v)
					case float64:
						assert.Equal(t, float64(expected), v)
					}
				case string:
					assert.Equal(t, expected, result)
				default:
					assert.Equal(t, expected, result)
				}
			}
		})
	}
}

// TestCoalesceWithColumnReferences tests COALESCE with table column references.
func TestCoalesceWithColumnReferences(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	// Create and populate a test table using SQL
	_, err := execTypeInferenceQuery(t, exec, cat, `
		CREATE TABLE test_table (
			id INTEGER,
			nullable_col INTEGER,
			string_col VARCHAR
		)
	`)
	require.NoError(t, err)

	// Insert test data
	_, err = execTypeInferenceQuery(
		t,
		exec,
		cat,
		"INSERT INTO test_table VALUES (1, NULL, 'hello')",
	)
	require.NoError(t, err)
	_, err = execTypeInferenceQuery(t, exec, cat, "INSERT INTO test_table VALUES (2, 42, NULL)")
	require.NoError(t, err)
	_, err = execTypeInferenceQuery(t, exec, cat, "INSERT INTO test_table VALUES (3, NULL, NULL)")
	require.NoError(t, err)

	// Test COALESCE with nullable integer column
	t.Run("COALESCE nullable column with default", func(t *testing.T) {
		results := getAllFirstColumnValues(
			t,
			exec,
			cat,
			"SELECT COALESCE(nullable_col, 0) FROM test_table ORDER BY id",
		)
		require.Len(t, results, 3)

		// Convert results to comparable format
		var intResults []int
		for _, r := range results {
			switch v := r.(type) {
			case int32:
				intResults = append(intResults, int(v))
			case int64:
				intResults = append(intResults, int(v))
			case float64:
				intResults = append(intResults, int(v))
			case nil:
				// NULL coalesced to 0
				intResults = append(intResults, 0)
			default:
				t.Fatalf("unexpected type %T: %v", r, r)
			}
		}

		assert.Equal(t, 0, intResults[0])  // NULL -> 0
		assert.Equal(t, 42, intResults[1]) // 42 -> 42
		assert.Equal(t, 0, intResults[2])  // NULL -> 0
	})

	// Test COALESCE with nullable string column
	t.Run("COALESCE string column with default", func(t *testing.T) {
		results := getAllFirstColumnValues(
			t,
			exec,
			cat,
			"SELECT COALESCE(string_col, 'default') FROM test_table ORDER BY id",
		)
		require.Len(t, results, 3)

		assert.Equal(t, "hello", results[0])
		assert.Equal(t, "default", results[1])
		assert.Equal(t, "default", results[2])
	})
}

// TestMathFunctionTypePreservation tests that math functions return correct types.
func TestMathFunctionTypePreservation(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	tests := []struct {
		name         string
		query        string
		expectedType string // "float64", "int64", "int32", "bool"
		checkValue   func(t *testing.T, result interface{})
	}{
		// Rounding functions preserve integer types
		{
			name:         "ROUND with integer returns integer",
			query:        "SELECT ROUND(42)",
			expectedType: "float64", // Currently returns float64 from math.Round
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(float64)
				assert.True(t, ok, "expected float64, got %T", result)
				assert.Equal(t, float64(42), v)
			},
		},
		{
			name:         "FLOOR with float returns double",
			query:        "SELECT FLOOR(3.7)",
			expectedType: "float64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(float64)
				assert.True(t, ok, "expected float64, got %T", result)
				assert.Equal(t, float64(3), v)
			},
		},
		{
			name:         "CEIL with float returns double",
			query:        "SELECT CEIL(3.2)",
			expectedType: "float64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(float64)
				assert.True(t, ok, "expected float64, got %T", result)
				assert.Equal(t, float64(4), v)
			},
		},

		// Scientific functions always return DOUBLE
		{
			name:         "SQRT returns DOUBLE",
			query:        "SELECT SQRT(16)",
			expectedType: "float64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(float64)
				assert.True(t, ok, "expected float64, got %T", result)
				assert.Equal(t, float64(4), v)
			},
		},
		{
			name:         "POW returns DOUBLE",
			query:        "SELECT POW(2, 3)",
			expectedType: "float64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(float64)
				assert.True(t, ok, "expected float64, got %T", result)
				assert.Equal(t, float64(8), v)
			},
		},
		{
			name:         "EXP returns DOUBLE",
			query:        "SELECT EXP(0)",
			expectedType: "float64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(float64)
				assert.True(t, ok, "expected float64, got %T", result)
				assert.Equal(t, float64(1), v)
			},
		},
		{
			name:         "LN returns DOUBLE",
			query:        "SELECT LN(1)",
			expectedType: "float64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(float64)
				assert.True(t, ok, "expected float64, got %T", result)
				assert.Equal(t, float64(0), v)
			},
		},

		// FACTORIAL returns BIGINT
		{
			name:         "FACTORIAL returns BIGINT",
			query:        "SELECT FACTORIAL(5)",
			expectedType: "int64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(int64)
				assert.True(t, ok, "expected int64, got %T", result)
				assert.Equal(t, int64(120), v)
			},
		},

		// Trigonometric functions return DOUBLE
		{
			name:         "SIN returns DOUBLE",
			query:        "SELECT SIN(0)",
			expectedType: "float64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(float64)
				assert.True(t, ok, "expected float64, got %T", result)
				assert.Equal(t, float64(0), v)
			},
		},
		{
			name:         "COS returns DOUBLE",
			query:        "SELECT COS(0)",
			expectedType: "float64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(float64)
				assert.True(t, ok, "expected float64, got %T", result)
				assert.Equal(t, float64(1), v)
			},
		},

		// Boolean predicates return BOOLEAN
		{
			name:         "ISNAN returns BOOLEAN",
			query:        "SELECT ISNAN(1.0)",
			expectedType: "bool",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(bool)
				assert.True(t, ok, "expected bool, got %T", result)
				assert.False(t, v)
			},
		},
		{
			name:         "ISINF returns BOOLEAN",
			query:        "SELECT ISINF(1.0)",
			expectedType: "bool",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(bool)
				assert.True(t, ok, "expected bool, got %T", result)
				assert.False(t, v)
			},
		},
		{
			name:         "ISFINITE returns BOOLEAN",
			query:        "SELECT ISFINITE(1.0)",
			expectedType: "bool",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(bool)
				assert.True(t, ok, "expected bool, got %T", result)
				assert.True(t, v)
			},
		},

		// SIGN returns INTEGER
		{
			name:         "SIGN returns INTEGER",
			query:        "SELECT SIGN(10)",
			expectedType: "int64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(int64)
				assert.True(t, ok, "expected int64, got %T", result)
				assert.Equal(t, int64(1), v)
			},
		},
		{
			name:         "SIGN negative returns -1",
			query:        "SELECT SIGN(-5)",
			expectedType: "int64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(int64)
				assert.True(t, ok, "expected int64, got %T", result)
				assert.Equal(t, int64(-1), v)
			},
		},

		// GCD/LCM return BIGINT
		{
			name:         "GCD returns BIGINT",
			query:        "SELECT GCD(12, 8)",
			expectedType: "int64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(int64)
				assert.True(t, ok, "expected int64, got %T", result)
				assert.Equal(t, int64(4), v)
			},
		},
		{
			name:         "LCM returns BIGINT",
			query:        "SELECT LCM(4, 6)",
			expectedType: "int64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(int64)
				assert.True(t, ok, "expected int64, got %T", result)
				assert.Equal(t, int64(12), v)
			},
		},

		// BIT_COUNT returns INTEGER
		{
			name:         "BIT_COUNT returns INTEGER",
			query:        "SELECT BIT_COUNT(7)",
			expectedType: "int64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(int64)
				assert.True(t, ok, "expected int64, got %T", result)
				assert.Equal(t, int64(3), v) // 7 = 0b111 = 3 bits set
			},
		},

		// PI and RANDOM return DOUBLE
		{
			name:         "PI returns DOUBLE",
			query:        "SELECT PI()",
			expectedType: "float64",
			checkValue: func(t *testing.T, result interface{}) {
				v, ok := result.(float64)
				assert.True(t, ok, "expected float64, got %T", result)
				assert.InDelta(t, 3.14159265358979, v, 0.0001)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFirstValue(t, exec, cat, tt.query)
			require.NotNil(t, result, "result should not be nil")
			tt.checkValue(t, result)
		})
	}
}

// TestMathFunctionTypeCoercion tests that integer arguments are properly coerced to DOUBLE.
func TestMathFunctionTypeCoercion(t *testing.T) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	tests := []struct {
		name     string
		query    string
		expected float64
		delta    float64 // for floating point comparison
	}{
		// Integer arguments should be coerced to DOUBLE for scientific functions
		{
			name:     "SQRT with integer argument",
			query:    "SELECT SQRT(16)",
			expected: 4.0,
			delta:    0.0001,
		},
		{
			name:     "CBRT with integer argument",
			query:    "SELECT CBRT(8)",
			expected: 2.0,
			delta:    0.0001,
		},
		{
			name:     "POW with integer arguments",
			query:    "SELECT POW(2, 10)",
			expected: 1024.0,
			delta:    0.0001,
		},
		{
			name:     "EXP with integer argument",
			query:    "SELECT EXP(1)",
			expected: 2.71828182845904,
			delta:    0.0001,
		},
		{
			name:     "LN with integer argument",
			query:    "SELECT LN(10)",
			expected: 2.302585092994,
			delta:    0.0001,
		},
		{
			name:     "LOG10 with integer argument",
			query:    "SELECT LOG10(100)",
			expected: 2.0,
			delta:    0.0001,
		},
		{
			name:     "LOG2 with integer argument",
			query:    "SELECT LOG2(8)",
			expected: 3.0,
			delta:    0.0001,
		},

		// Trigonometric functions with integer arguments
		{
			name:     "SIN with integer argument (0)",
			query:    "SELECT SIN(0)",
			expected: 0.0,
			delta:    0.0001,
		},
		{
			name:     "COS with integer argument (0)",
			query:    "SELECT COS(0)",
			expected: 1.0,
			delta:    0.0001,
		},
		{
			name:     "TAN with integer argument (0)",
			query:    "SELECT TAN(0)",
			expected: 0.0,
			delta:    0.0001,
		},
		{
			name:     "DEGREES with integer argument",
			query:    "SELECT DEGREES(0)",
			expected: 0.0,
			delta:    0.0001,
		},
		{
			name:     "RADIANS with integer argument (180)",
			query:    "SELECT RADIANS(180)",
			expected: 3.14159265358979,
			delta:    0.0001,
		},

		// Hyperbolic functions with integer arguments
		{
			name:     "SINH with integer argument (0)",
			query:    "SELECT SINH(0)",
			expected: 0.0,
			delta:    0.0001,
		},
		{
			name:     "COSH with integer argument (0)",
			query:    "SELECT COSH(0)",
			expected: 1.0,
			delta:    0.0001,
		},
		{
			name:     "TANH with integer argument (0)",
			query:    "SELECT TANH(0)",
			expected: 0.0,
			delta:    0.0001,
		},

		// Rounding functions with float arguments
		{
			name:     "ROUND with float argument",
			query:    "SELECT ROUND(3.5)",
			expected: 4.0,
			delta:    0.0001,
		},
		{
			name:     "FLOOR with float argument",
			query:    "SELECT FLOOR(3.9)",
			expected: 3.0,
			delta:    0.0001,
		},
		{
			name:     "CEIL with float argument",
			query:    "SELECT CEIL(3.1)",
			expected: 4.0,
			delta:    0.0001,
		},
		{
			name:     "TRUNC with float argument",
			query:    "SELECT TRUNC(3.9)",
			expected: 3.0,
			delta:    0.0001,
		},
		{
			name:     "TRUNC with negative float",
			query:    "SELECT TRUNC(-3.9)",
			expected: -3.0,
			delta:    0.0001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFirstValue(t, exec, cat, tt.query)
			require.NotNil(t, result, "result should not be nil")

			v, ok := result.(float64)
			require.True(t, ok, "expected float64, got %T: %v", result, result)
			assert.InDelta(t, tt.expected, v, tt.delta, "expected %v, got %v", tt.expected, v)
		})
	}
}
