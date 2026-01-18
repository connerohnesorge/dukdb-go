package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLambdaColumnIntegration tests LAMBDA column operations end-to-end with database/sql.
func TestLambdaColumnIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with LAMBDA column
	_, err = db.Exec(`CREATE TABLE lambda_test (id INTEGER, expr LAMBDA)`)
	require.NoError(t, err)

	t.Run("Insert and select simple lambda expression", func(t *testing.T) {
		lambdaExpr := "x -> x + 1"
		_, err := db.Exec(`INSERT INTO lambda_test VALUES (1, $1)`, lambdaExpr)
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_test WHERE id = 1`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "x -> x + 1", expr)
	})

	t.Run("Insert and select multi-parameter lambda expression", func(t *testing.T) {
		lambdaExpr := "(x, y) -> x * y"
		_, err := db.Exec(`INSERT INTO lambda_test VALUES (2, $1)`, lambdaExpr)
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_test WHERE id = 2`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 2, id)
		assert.Equal(t, "(x, y) -> x * y", expr)
	})

	t.Run("Insert and select complex lambda expression", func(t *testing.T) {
		lambdaExpr := "(a, b, c) -> a * b + c"
		_, err := db.Exec(`INSERT INTO lambda_test VALUES (3, $1)`, lambdaExpr)
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_test WHERE id = 3`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 3, id)
		assert.Equal(t, "(a, b, c) -> a * b + c", expr)
	})

	t.Run("Insert and select lambda with nested function calls", func(t *testing.T) {
		lambdaExpr := "x -> upper(trim(x))"
		_, err := db.Exec(`INSERT INTO lambda_test VALUES (4, $1)`, lambdaExpr)
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_test WHERE id = 4`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 4, id)
		assert.Equal(t, "x -> upper(trim(x))", expr)
	})

	t.Run("Insert and select lambda with special characters", func(t *testing.T) {
		lambdaExpr := "(x, y) -> x >= y AND x <= 100"
		_, err := db.Exec(`INSERT INTO lambda_test VALUES (5, $1)`, lambdaExpr)
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_test WHERE id = 5`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 5, id)
		assert.Equal(t, "(x, y) -> x >= y AND x <= 100", expr)
	})
}

// TestLambdaColumnNullHandling tests NULL handling in LAMBDA columns.
func TestLambdaColumnNullHandling(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with LAMBDA column
	_, err = db.Exec(`CREATE TABLE lambda_null_test (id INTEGER, expr LAMBDA)`)
	require.NoError(t, err)

	t.Run("Insert SQL NULL into LAMBDA column", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO lambda_null_test VALUES (1, NULL)`)
		require.NoError(t, err)

		var id int
		var expr sql.NullString
		err = db.QueryRow(`SELECT id, expr FROM lambda_null_test WHERE id = 1`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.False(t, expr.Valid, "SQL NULL should result in NullString.Valid = false")
	})

	t.Run("Insert SQL NULL using parameter", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO lambda_null_test VALUES (2, $1)`, nil)
		require.NoError(t, err)

		var id int
		var expr sql.NullString
		err = db.QueryRow(`SELECT id, expr FROM lambda_null_test WHERE id = 2`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 2, id)
		assert.False(
			t,
			expr.Valid,
			"SQL NULL via parameter should result in NullString.Valid = false",
		)
	})

	t.Run("Mix NULL and non-NULL values", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO lambda_null_test VALUES (3, $1)`, "x -> x")
		require.NoError(t, err)

		// Query non-NULL value
		var id int
		var expr sql.NullString
		err = db.QueryRow(`SELECT id, expr FROM lambda_null_test WHERE id = 3`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 3, id)
		assert.True(t, expr.Valid, "Non-NULL lambda should be valid")
		assert.Equal(t, "x -> x", expr.String)
	})

	t.Run("Query NULL with IS NULL", func(t *testing.T) {
		rows, err := db.Query(`SELECT id FROM lambda_null_test WHERE expr IS NULL ORDER BY id`)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())

		// IDs 1 and 2 have SQL NULL
		assert.Equal(t, []int{1, 2}, ids)
	})

	t.Run("Query non-NULL with IS NOT NULL", func(t *testing.T) {
		rows, err := db.Query(`SELECT id FROM lambda_null_test WHERE expr IS NOT NULL ORDER BY id`)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())

		// ID 3 has a non-NULL lambda expression
		assert.Equal(t, []int{3}, ids)
	})
}

// TestLambdaColumnEdgeCases tests edge cases for LAMBDA columns.
func TestLambdaColumnEdgeCases(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with LAMBDA column
	_, err = db.Exec(`CREATE TABLE lambda_edge_test (id INTEGER, expr LAMBDA)`)
	require.NoError(t, err)

	t.Run("Empty string expression", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO lambda_edge_test VALUES (1, $1)`, "")
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_edge_test WHERE id = 1`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "", expr)
	})

	t.Run("Unicode lambda expression", func(t *testing.T) {
		lambdaExpr := "x -> x + 'hello'"
		_, err := db.Exec(`INSERT INTO lambda_edge_test VALUES (2, $1)`, lambdaExpr)
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_edge_test WHERE id = 2`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 2, id)
		assert.Equal(t, lambdaExpr, expr)
	})

	t.Run("Lambda with Chinese characters", func(t *testing.T) {
		lambdaExpr := "x -> x || ' World'"
		_, err := db.Exec(`INSERT INTO lambda_edge_test VALUES (3, $1)`, lambdaExpr)
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_edge_test WHERE id = 3`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 3, id)
		assert.Equal(t, lambdaExpr, expr)
	})

	t.Run("Lambda with Japanese characters", func(t *testing.T) {
		lambdaExpr := "x -> x || ' World'"
		_, err := db.Exec(`INSERT INTO lambda_edge_test VALUES (4, $1)`, lambdaExpr)
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_edge_test WHERE id = 4`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 4, id)
		assert.Equal(t, lambdaExpr, expr)
	})

	t.Run("Lambda with emoji", func(t *testing.T) {
		lambdaExpr := "x -> x || ' World'"
		_, err := db.Exec(`INSERT INTO lambda_edge_test VALUES (5, $1)`, lambdaExpr)
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_edge_test WHERE id = 5`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 5, id)
		assert.Equal(t, lambdaExpr, expr)
	})

	t.Run("Lambda with newlines and tabs", func(t *testing.T) {
		lambdaExpr := "x -> x\n\t+ 1"
		_, err := db.Exec(`INSERT INTO lambda_edge_test VALUES (6, $1)`, lambdaExpr)
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_edge_test WHERE id = 6`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 6, id)
		assert.Equal(t, lambdaExpr, expr)
	})

	t.Run("Very long lambda expression", func(t *testing.T) {
		// Create a long lambda expression with many parameters and a long body
		longExpr := "(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9) -> x0 + x1 + x2 + x3 + x4 + x5 + x6 + x7 + x8 + x9"
		_, err := db.Exec(`INSERT INTO lambda_edge_test VALUES (7, $1)`, longExpr)
		require.NoError(t, err)

		var id int
		var expr string
		err = db.QueryRow(`SELECT id, expr FROM lambda_edge_test WHERE id = 7`).Scan(&id, &expr)
		require.NoError(t, err)
		assert.Equal(t, 7, id)
		assert.Equal(t, longExpr, expr)
	})
}

// TestLambdaColumnMultipleRows tests LAMBDA column with multiple rows.
func TestLambdaColumnMultipleRows(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with LAMBDA column
	_, err = db.Exec(`CREATE TABLE lambda_multi_test (id INTEGER, expr LAMBDA)`)
	require.NoError(t, err)

	// Insert multiple rows with different lambda expressions
	testData := []struct {
		id   int
		expr string
	}{
		{1, "x -> x"},
		{2, "x -> x + 1"},
		{3, "(x, y) -> x + y"},
		{4, "(a, b, c) -> a * b + c"},
		{5, "n -> n * n"},
		{6, "str -> upper(str)"},
	}

	for _, td := range testData {
		_, err := db.Exec(`INSERT INTO lambda_multi_test VALUES ($1, $2)`, td.id, td.expr)
		require.NoError(t, err)
	}

	// Query all rows
	rows, err := db.Query(`SELECT id, expr FROM lambda_multi_test ORDER BY id`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	var results []struct {
		id   int
		expr string
	}
	for rows.Next() {
		var id int
		var expr string
		require.NoError(t, rows.Scan(&id, &expr))
		results = append(results, struct {
			id   int
			expr string
		}{id, expr})
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 6)

	// Verify each row
	for i, td := range testData {
		assert.Equal(t, td.id, results[i].id)
		assert.Equal(t, td.expr, results[i].expr)
	}
}

// TestLambdaColumnPreparedStatement tests LAMBDA column with prepared statements.
func TestLambdaColumnPreparedStatement(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with LAMBDA column
	_, err = db.Exec(`CREATE TABLE lambda_prep_test (id INTEGER, expr LAMBDA)`)
	require.NoError(t, err)

	// Prepare insert statement
	insertStmt, err := db.Prepare(`INSERT INTO lambda_prep_test VALUES ($1, $2)`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, insertStmt.Close())
	}()

	// Insert multiple rows using prepared statement
	testData := []struct {
		id   int
		expr string
	}{
		{1, "x -> x + 1"},
		{2, "x -> x * 2"},
		{3, "x -> x - 3"},
	}

	for _, td := range testData {
		_, err := insertStmt.Exec(td.id, td.expr)
		require.NoError(t, err)
	}

	// Prepare select statement
	selectStmt, err := db.Prepare(`SELECT expr FROM lambda_prep_test WHERE id = $1`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, selectStmt.Close())
	}()

	// Query using prepared statement
	for _, td := range testData {
		var expr string
		err := selectStmt.QueryRow(td.id).Scan(&expr)
		require.NoError(t, err)
		assert.Equal(t, td.expr, expr)
	}
}

// TestLambdaColumnTransaction tests LAMBDA column operations within a transaction.
func TestLambdaColumnTransaction(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with LAMBDA column
	_, err = db.Exec(`CREATE TABLE lambda_tx_test (id INTEGER, expr LAMBDA)`)
	require.NoError(t, err)

	t.Run("Commit transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		_, err = tx.Exec(`INSERT INTO lambda_tx_test VALUES (1, $1)`, "x -> x + 1")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		// Verify data persisted
		var expr string
		err = db.QueryRow(`SELECT expr FROM lambda_tx_test WHERE id = 1`).Scan(&expr)
		require.NoError(t, err)
		assert.Equal(t, "x -> x + 1", expr)
	})

	t.Run("Rollback transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		_, err = tx.Exec(`INSERT INTO lambda_tx_test VALUES (2, $1)`, "y -> y * 2")
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		// Verify data was not persisted
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM lambda_tx_test WHERE id = 2`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

// TestLambdaColumnUpdate tests updating LAMBDA column values.
func TestLambdaColumnUpdate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table and insert initial data
	_, err = db.Exec(`CREATE TABLE lambda_update_test (id INTEGER PRIMARY KEY, expr LAMBDA)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO lambda_update_test VALUES (1, $1)`, "x -> x + 1")
	require.NoError(t, err)

	// Update the LAMBDA data
	result, err := db.Exec(
		`UPDATE lambda_update_test SET expr = $1 WHERE id = 1`,
		"(x, y) -> x * y",
	)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify the update
	var expr string
	err = db.QueryRow(`SELECT expr FROM lambda_update_test WHERE id = 1`).Scan(&expr)
	require.NoError(t, err)
	assert.Equal(t, "(x, y) -> x * y", expr)
}

// TestLambdaColumnDelete tests deleting rows with LAMBDA columns.
func TestLambdaColumnDelete(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table and insert data
	_, err = db.Exec(`CREATE TABLE lambda_delete_test (id INTEGER, expr LAMBDA)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO lambda_delete_test VALUES (1, $1)`, "x -> x + 1")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO lambda_delete_test VALUES (2, $1)`, "y -> y * 2")
	require.NoError(t, err)

	// Delete one row
	result, err := db.Exec(`DELETE FROM lambda_delete_test WHERE id = 1`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify only one row remains
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM lambda_delete_test`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify the correct row remains
	var expr string
	err = db.QueryRow(`SELECT expr FROM lambda_delete_test WHERE id = 2`).Scan(&expr)
	require.NoError(t, err)
	assert.Equal(t, "y -> y * 2", expr)
}

// TestLambdaColumnRoundtrip tests that lambda expressions roundtrip correctly.
func TestLambdaColumnRoundtrip(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with LAMBDA column
	_, err = db.Exec(`CREATE TABLE lambda_roundtrip_test (id INTEGER, expr LAMBDA)`)
	require.NoError(t, err)

	testCases := []struct {
		name string
		expr string
	}{
		{"identity", "x -> x"},
		{"increment", "x -> x + 1"},
		{"multiply", "(x, y) -> x * y"},
		{"triple_params", "(a, b, c) -> a + b + c"},
		{"nested_call", "x -> length(x)"},
		{"conditional", "x -> CASE WHEN x > 0 THEN x ELSE -x END"},
		{"arithmetic", "x -> (x * 2 + 3) / 4"},
		{"string_concat", "s -> s || '_suffix'"},
		{"with_quotes", "x -> x || '\"quoted\"'"},
		{"with_backslash", "x -> x || '\\\\'"},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			id := i + 1
			_, err := db.Exec(`INSERT INTO lambda_roundtrip_test VALUES ($1, $2)`, id, tc.expr)
			require.NoError(t, err)

			var expr string
			err = db.QueryRow(`SELECT expr FROM lambda_roundtrip_test WHERE id = $1`, id).
				Scan(&expr)
			require.NoError(t, err)
			assert.Equal(t, tc.expr, expr, "lambda expression should roundtrip correctly")
		})
	}
}
