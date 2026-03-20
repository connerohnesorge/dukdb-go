package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConstantFoldingE2E verifies that constant expressions are evaluated at plan time.
func TestConstantFoldingE2E(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		{"add_integers", "SELECT 1 + 2", int64(3)},
		{"multiply_and_add", "SELECT 10 * 5 + 3", int64(53)},
		{"subtract", "SELECT 100 - 42", int64(58)},
		{"nested_arithmetic", "SELECT (2 + 3) * (4 + 1)", int64(25)},
		{"string_concat", "SELECT 'hello' || ' ' || 'world'", "hello world"},
		{"boolean_and_literals", "SELECT TRUE AND TRUE", true},
		{"boolean_or_literals", "SELECT FALSE OR TRUE", true},
		{"comparison_literals", "SELECT 3 > 2", true},
		{"equality_literals", "SELECT 5 = 5", true},
		{"inequality_literals", "SELECT 5 != 4", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result any
			err := db.QueryRow(tt.query).Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestBooleanSimplificationE2E verifies that boolean simplification applies through SQL queries.
func TestBooleanSimplificationE2E(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE rw_bool_test(x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO rw_bool_test VALUES (1), (2), (3)")
	require.NoError(t, err)

	t.Run("and_true", func(t *testing.T) {
		// x > 1 AND TRUE should simplify to x > 1
		rows, err := db.Query("SELECT x FROM rw_bool_test WHERE x > 1 AND TRUE ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{2, 3}, results)
	})

	t.Run("or_false", func(t *testing.T) {
		// FALSE OR x > 2 should simplify to x > 2
		rows, err := db.Query("SELECT x FROM rw_bool_test WHERE FALSE OR x > 2 ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{3}, results)
	})

	t.Run("and_false", func(t *testing.T) {
		// x > 0 AND FALSE should simplify to FALSE (no rows returned)
		rows, err := db.Query("SELECT x FROM rw_bool_test WHERE x > 0 AND FALSE")
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 0, count)
	})

	t.Run("or_true", func(t *testing.T) {
		// x > 100 OR TRUE should simplify to TRUE (all rows returned)
		rows, err := db.Query("SELECT x FROM rw_bool_test WHERE x > 100 OR TRUE ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{1, 2, 3}, results)
	})
}

// TestArithmeticIdentityE2E verifies arithmetic identity simplifications in SQL.
func TestArithmeticIdentityE2E(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE rw_arith_test(x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO rw_arith_test VALUES (10), (20), (30)")
	require.NoError(t, err)

	t.Run("multiply_by_one", func(t *testing.T) {
		// x * 1 should simplify to x
		rows, err := db.Query("SELECT x * 1 FROM rw_arith_test ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{10, 20, 30}, results)
	})

	t.Run("add_zero", func(t *testing.T) {
		// x + 0 should simplify to x
		rows, err := db.Query("SELECT x + 0 FROM rw_arith_test ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{10, 20, 30}, results)
	})

	t.Run("subtract_zero", func(t *testing.T) {
		// x - 0 should simplify to x
		rows, err := db.Query("SELECT x - 0 FROM rw_arith_test ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{10, 20, 30}, results)
	})

	t.Run("divide_by_one", func(t *testing.T) {
		// x / 1 should simplify to x
		rows, err := db.Query("SELECT x / 1 FROM rw_arith_test ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{10, 20, 30}, results)
	})

	t.Run("multiply_by_zero", func(t *testing.T) {
		// x * 0 should simplify to 0
		rows, err := db.Query("SELECT x * 0 FROM rw_arith_test ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{0, 0, 0}, results)
	})
}

// TestInListSimplificationE2E verifies IN list simplification in SQL.
func TestInListSimplificationE2E(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE rw_in_test(x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO rw_in_test VALUES (1), (2), (3), (4), (5)")
	require.NoError(t, err)

	t.Run("single_value_in", func(t *testing.T) {
		// x IN (3) should simplify to x = 3
		rows, err := db.Query("SELECT x FROM rw_in_test WHERE x IN (3)")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{3}, results)
	})

	t.Run("single_value_not_in", func(t *testing.T) {
		// x NOT IN (3) should simplify to x != 3
		rows, err := db.Query("SELECT x FROM rw_in_test WHERE x NOT IN (3) ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{1, 2, 4, 5}, results)
	})

	t.Run("multi_value_in", func(t *testing.T) {
		// Multi-value IN should still work normally
		rows, err := db.Query("SELECT x FROM rw_in_test WHERE x IN (2, 4) ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{2, 4}, results)
	})
}

// TestFilterCombinationE2E verifies that adjacent filters are combined by the filter pushdown rule.
func TestFilterCombinationE2E(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE rw_filter_test(x INTEGER, y INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO rw_filter_test VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
	require.NoError(t, err)

	t.Run("combined_where_conditions", func(t *testing.T) {
		// Multiple WHERE conditions combined with AND
		rows, err := db.Query("SELECT x, y FROM rw_filter_test WHERE x > 1 AND x < 5 AND y > 15 ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		type result struct{ x, y int }
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.x, &r.y))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []result{{2, 20}, {3, 30}, {4, 40}}, results)
	})

	t.Run("subquery_with_filter", func(t *testing.T) {
		// Filter on a subquery result - tests filter combination with subqueries
		rows, err := db.Query("SELECT x FROM (SELECT x, y FROM rw_filter_test WHERE x > 1) AS sub WHERE sub.x < 5 ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{2, 3, 4}, results)
	})
}

// TestCombinedRewritesE2E verifies that multiple rewrite rules work together.
func TestCombinedRewritesE2E(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE rw_combined_test(x INTEGER, name VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO rw_combined_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	require.NoError(t, err)

	t.Run("constant_fold_in_where", func(t *testing.T) {
		// Constant folding combined with filter: WHERE x > 1 + 1 should fold to WHERE x > 2
		rows, err := db.Query("SELECT x FROM rw_combined_test WHERE x > 1 + 1 ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{3}, results)
	})

	t.Run("arithmetic_identity_in_select", func(t *testing.T) {
		// Arithmetic identity in SELECT: x * 1 + 0 should simplify to x
		rows, err := db.Query("SELECT x * 1 + 0 FROM rw_combined_test ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{1, 2, 3}, results)
	})

	t.Run("boolean_and_constant_fold", func(t *testing.T) {
		// Boolean simplification with constant folding: WHERE (2 > 1) AND x > 1
		// 2 > 1 folds to TRUE, then TRUE AND x > 1 simplifies to x > 1
		rows, err := db.Query("SELECT x FROM rw_combined_test WHERE (2 > 1) AND x > 1 ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{2, 3}, results)
	})
}
