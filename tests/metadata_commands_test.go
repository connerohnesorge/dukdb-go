package tests

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scanRows is a helper that scans all rows from a query into a slice of maps.
func scanRows(t *testing.T, rows *sql.Rows) []map[string]any {
	t.Helper()
	cols, err := rows.Columns()
	require.NoError(t, err)

	var results []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		err := rows.Scan(ptrs...)
		require.NoError(t, err)
		row := make(map[string]any)
		for i, col := range cols {
			row[col] = vals[i]
		}
		results = append(results, row)
	}
	require.NoError(t, rows.Err())

	return results
}

// TestMetadataCommands exercises DESCRIBE, SHOW TABLES, SHOW COLUMNS,
// SUMMARIZE, and CALL metadata commands as an integration suite.
func TestMetadataCommands(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck // test helper, error not meaningful

	// Create the shared employees table used by several subtests.
	_, err = db.Exec(`CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		name VARCHAR NOT NULL,
		salary DOUBLE,
		hired_at DATE
	)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO employees VALUES (1, 'Alice', 75000.0, '2023-01-15')`)
	require.NoError(t, err)

	// ---------------------------------------------------------------
	// DESCRIBE tests
	// ---------------------------------------------------------------

	t.Run("DescribeTable", func(t *testing.T) {
		rows, err := db.Query("DESCRIBE employees")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck // test helper, error not meaningful

		results := scanRows(t, rows)
		require.Len(t, results, 4, "DESCRIBE should return one row per column")

		// Verify column names
		assert.Equal(t, "id", fmt.Sprintf("%v", results[0]["column_name"]))
		assert.Equal(t, "name", fmt.Sprintf("%v", results[1]["column_name"]))
		assert.Equal(t, "salary", fmt.Sprintf("%v", results[2]["column_name"]))
		assert.Equal(t, "hired_at", fmt.Sprintf("%v", results[3]["column_name"]))

		// Verify column types
		assert.Equal(t, "INTEGER", fmt.Sprintf("%v", results[0]["column_type"]))
		assert.Equal(t, "VARCHAR", fmt.Sprintf("%v", results[1]["column_type"]))
		assert.Equal(t, "DOUBLE", fmt.Sprintf("%v", results[2]["column_type"]))
		assert.Equal(t, "DATE", fmt.Sprintf("%v", results[3]["column_type"]))

		// Verify null: id and name are NOT NULL, salary and hired_at are nullable
		assert.Equal(t, "NO", fmt.Sprintf("%v", results[0]["null"]))
		assert.Equal(t, "NO", fmt.Sprintf("%v", results[1]["null"]))
		assert.Equal(t, "YES", fmt.Sprintf("%v", results[2]["null"]))
		assert.Equal(t, "YES", fmt.Sprintf("%v", results[3]["null"]))

		// Verify key: id is PRIMARY KEY
		assert.Equal(t, "YES", fmt.Sprintf("%v", results[0]["key"]))
		assert.Equal(t, "NO", fmt.Sprintf("%v", results[1]["key"]))
		assert.Equal(t, "NO", fmt.Sprintf("%v", results[2]["key"]))
		assert.Equal(t, "NO", fmt.Sprintf("%v", results[3]["key"]))
	})

	t.Run("DescribeNonExistentTable", func(t *testing.T) {
		rows, qErr := db.Query("DESCRIBE no_such_table")
		if qErr != nil {
			assert.Contains(t, qErr.Error(), "does not exist")

			return
		}
		defer rows.Close() //nolint:errcheck // test cleanup, error not meaningful
		t.Fatal("expected error for DESCRIBE of non-existent table")
	})

	t.Run("DescUsesDESCKeyword", func(t *testing.T) {
		rows, err := db.Query("DESC employees")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck // test helper, error not meaningful

		results := scanRows(t, rows)
		assert.Greater(t, len(results), 0, "DESC should return rows like DESCRIBE")
	})

	// ---------------------------------------------------------------
	// SHOW tests
	// ---------------------------------------------------------------

	t.Run("ShowTables", func(t *testing.T) {
		// Create a second table so we can verify both appear.
		_, err := db.Exec(`CREATE TABLE departments (id INTEGER, dept_name VARCHAR)`)
		require.NoError(t, err)

		rows, err := db.Query("SHOW TABLES")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck // test helper, error not meaningful

		results := scanRows(t, rows)
		names := make(map[string]bool)
		for _, r := range results {
			names[fmt.Sprintf("%v", r["name"])] = true
		}
		assert.True(t, names["employees"], "employees should appear in SHOW TABLES")
		assert.True(t, names["departments"], "departments should appear in SHOW TABLES")

		// Clean up the extra table.
		_, err = db.Exec(`DROP TABLE departments`)
		require.NoError(t, err)
	})

	t.Run("ShowAllTables", func(t *testing.T) {
		_, err := db.Exec(`CREATE SCHEMA test_schema`)
		require.NoError(t, err)

		_, err = db.Exec(`CREATE TABLE test_schema.schema_tbl (x INTEGER)`)
		require.NoError(t, err)

		rows, err := db.Query("SHOW ALL TABLES")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck // test helper, error not meaningful

		results := scanRows(t, rows)

		// Collect schema.name pairs
		found := make(map[string]bool)
		for _, r := range results {
			key := fmt.Sprintf("%v.%v", r["schema"], r["name"])
			found[key] = true
		}
		assert.True(t, found["main.employees"], "main.employees should appear in SHOW ALL TABLES")
		assert.True(t, found["test_schema.schema_tbl"], "test_schema.schema_tbl should appear in SHOW ALL TABLES")

		// Clean up
		_, err = db.Exec(`DROP TABLE test_schema.schema_tbl`)
		require.NoError(t, err)
		_, err = db.Exec(`DROP SCHEMA test_schema`)
		require.NoError(t, err)
	})

	t.Run("ShowColumnsFromTable", func(t *testing.T) {
		rows, err := db.Query("SHOW COLUMNS FROM employees")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck // test helper, error not meaningful

		results := scanRows(t, rows)
		require.Len(t, results, 4, "SHOW COLUMNS should return one row per column")

		// Verify first column info matches DESCRIBE output
		assert.Equal(t, "id", fmt.Sprintf("%v", results[0]["column_name"]))
		assert.Equal(t, "INTEGER", fmt.Sprintf("%v", results[0]["column_type"]))
		assert.Equal(t, "YES", fmt.Sprintf("%v", results[0]["key"]))
	})

	t.Run("ShowColumnsNonExistentTable", func(t *testing.T) {
		rows, qErr := db.Query("SHOW COLUMNS FROM ghost_table")
		if qErr != nil {
			assert.Contains(t, qErr.Error(), "does not exist")

			return
		}
		defer rows.Close() //nolint:errcheck // test cleanup, error not meaningful
		t.Fatal("expected error for SHOW COLUMNS FROM non-existent table")
	})

	// ---------------------------------------------------------------
	// SUMMARIZE tests
	// ---------------------------------------------------------------

	t.Run("SummarizeWithData", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE stats_data (id INTEGER, value DOUBLE, label VARCHAR)`)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO stats_data VALUES
			(1, 10.5, 'a'),
			(2, 20.0, 'b'),
			(3, NULL, 'c'),
			(4, 30.5, NULL),
			(5, 40.0, 'a')`)
		require.NoError(t, err)

		rows, err := db.Query("SUMMARIZE stats_data")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck // test helper, error not meaningful

		results := scanRows(t, rows)
		require.Len(t, results, 3, "SUMMARIZE should return one row per column")

		// Verify column_name values
		assert.Equal(t, "id", fmt.Sprintf("%v", results[0]["column_name"]))
		assert.Equal(t, "value", fmt.Sprintf("%v", results[1]["column_name"]))
		assert.Equal(t, "label", fmt.Sprintf("%v", results[2]["column_name"]))

		// Verify column_type values
		assert.Equal(t, "INTEGER", fmt.Sprintf("%v", results[0]["column_type"]))
		assert.Equal(t, "DOUBLE", fmt.Sprintf("%v", results[1]["column_type"]))
		assert.Equal(t, "VARCHAR", fmt.Sprintf("%v", results[2]["column_type"]))

		// Verify count is non-null count (new schema: count = non-null count)
		assert.Equal(t, int64(5), results[0]["count"], "id has 5 non-null values")
		assert.Equal(t, int64(4), results[1]["count"], "value has 4 non-null values")
		assert.Equal(t, int64(4), results[2]["count"], "label has 4 non-null values")

		// Verify null_percentage > 0 for value (1 NULL) and label (1 NULL)
		assert.InDelta(t, 0.0, results[0]["null_percentage"], 0.01, "id has no NULLs")
		assert.InDelta(t, 20.0, results[1]["null_percentage"], 0.01, "value has 1/5 NULLs = 20%%")
		assert.InDelta(t, 20.0, results[2]["null_percentage"], 0.01, "label has 1/5 NULLs = 20%%")

		// Clean up
		_, err = db.Exec(`DROP TABLE stats_data`)
		require.NoError(t, err)
	})

	t.Run("SummarizeEmptyTable", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE empty_tbl (a INTEGER, b VARCHAR)`)
		require.NoError(t, err)

		rows, err := db.Query("SUMMARIZE empty_tbl")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck // test helper, error not meaningful

		results := scanRows(t, rows)
		require.Len(t, results, 2, "SUMMARIZE on empty table should return one row per column")

		for _, r := range results {
			assert.Equal(t, int64(0), r["count"], "count should be 0 for empty table")
			assert.InDelta(t, 0.0, r["null_percentage"], 0.01, "null_percentage should be 0 for empty table")
		}

		// Clean up
		_, err = db.Exec(`DROP TABLE empty_tbl`)
		require.NoError(t, err)
	})

	t.Run("SummarizeNonExistentTable", func(t *testing.T) {
		rows, qErr := db.Query("SUMMARIZE ghost")
		if qErr != nil {
			assert.Contains(t, qErr.Error(), "does not exist")

			return
		}
		defer rows.Close() //nolint:errcheck // test cleanup, error not meaningful
		t.Fatal("expected error for SUMMARIZE of non-existent table")
	})

	// ---------------------------------------------------------------
	// CALL tests
	// ---------------------------------------------------------------

	t.Run("CallGenerateSeries", func(t *testing.T) {
		rows, err := db.Query("CALL generate_series(1, 5)")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck // test cleanup, error not meaningful

		var values []int64
		for rows.Next() {
			var v int64
			err := rows.Scan(&v)
			require.NoError(t, err)
			values = append(values, v)
		}
		require.NoError(t, rows.Err())
		require.Len(t, values, 5, "generate_series(1, 5) should return 5 rows")
		assert.Equal(t, []int64{1, 2, 3, 4, 5}, values)
	})

	t.Run("CallGenerateSeriesWithStep", func(t *testing.T) {
		rows, err := db.Query("CALL generate_series(0, 10, 2)")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck // test cleanup, error not meaningful

		var values []int64
		for rows.Next() {
			var v int64
			err := rows.Scan(&v)
			require.NoError(t, err)
			values = append(values, v)
		}
		require.NoError(t, rows.Err())
		require.Len(t, values, 6, "generate_series(0, 10, 2) should return 6 rows")
		assert.Equal(t, []int64{0, 2, 4, 6, 8, 10}, values)
	})
}
