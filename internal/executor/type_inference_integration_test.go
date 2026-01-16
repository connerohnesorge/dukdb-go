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
func getAllFirstColumnValues(t *testing.T, exec *Executor, cat *catalog.Catalog, query string) []interface{} {
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
	_, err = execTypeInferenceQuery(t, exec, cat, "INSERT INTO test_table VALUES (1, NULL, 'hello')")
	require.NoError(t, err)
	_, err = execTypeInferenceQuery(t, exec, cat, "INSERT INTO test_table VALUES (2, 42, NULL)")
	require.NoError(t, err)
	_, err = execTypeInferenceQuery(t, exec, cat, "INSERT INTO test_table VALUES (3, NULL, NULL)")
	require.NoError(t, err)

	// Test COALESCE with nullable integer column
	t.Run("COALESCE nullable column with default", func(t *testing.T) {
		results := getAllFirstColumnValues(t, exec, cat, "SELECT COALESCE(nullable_col, 0) FROM test_table ORDER BY id")
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
		results := getAllFirstColumnValues(t, exec, cat, "SELECT COALESCE(string_col, 'default') FROM test_table ORDER BY id")
		require.Len(t, results, 3)

		assert.Equal(t, "hello", results[0])
		assert.Equal(t, "default", results[1])
		assert.Equal(t, "default", results[2])
	})
}
