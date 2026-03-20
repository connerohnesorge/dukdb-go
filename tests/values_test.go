package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStandaloneValues verifies that standalone VALUES clause works.
// VALUES (1, 'a'), (2, 'b') should return two rows.
func TestStandaloneValues(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("VALUES (1, 'hello'), (2, 'world')")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, 2, len(cols), "should have 2 columns")

	var results []struct {
		col1 int64
		col2 string
	}
	for rows.Next() {
		var r struct {
			col1 int64
			col2 string
		}
		err := rows.Scan(&r.col1, &r.col2)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Equal(t, 2, len(results), "should have 2 rows")
	assert.Equal(t, int64(1), results[0].col1)
	assert.Equal(t, "hello", results[0].col2)
	assert.Equal(t, int64(2), results[1].col1)
	assert.Equal(t, "world", results[1].col2)
}

// TestValuesSingleRow verifies VALUES with a single row.
func TestValuesSingleRow(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("VALUES (42, 'only')")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var col1 int64
	var col2 string
	err = rows.Scan(&col1, &col2)
	require.NoError(t, err)
	assert.Equal(t, int64(42), col1)
	assert.Equal(t, "only", col2)
	assert.False(t, rows.Next())
}

// TestValuesInFromClause verifies VALUES used in FROM clause.
func TestValuesInFromClause(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t")
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		col1 int64
		col2 string
	}
	for rows.Next() {
		var r struct {
			col1 int64
			col2 string
		}
		err := rows.Scan(&r.col1, &r.col2)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Equal(t, 2, len(results))
	assert.Equal(t, int64(1), results[0].col1)
	assert.Equal(t, "a", results[0].col2)
	assert.Equal(t, int64(2), results[1].col1)
	assert.Equal(t, "b", results[1].col2)
}

// TestValuesInCTE verifies VALUES used in a CTE (WITH clause).
func TestValuesInCTE(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query(`
		WITH data AS (
			SELECT * FROM (VALUES (10, 'x'), (20, 'y'), (30, 'z')) AS t
		)
		SELECT * FROM data
	`)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
		var col1 int64
		var col2 string
		err := rows.Scan(&col1, &col2)
		require.NoError(t, err)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, count, "should have 3 rows from CTE")
}

// TestValuesWithExpressions verifies VALUES with computed expressions.
func TestValuesWithExpressions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("VALUES (1 + 1, 'computed')")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var col1 int64
	var col2 string
	err = rows.Scan(&col1, &col2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), col1)
	assert.Equal(t, "computed", col2)
}

// TestValuesWithNull verifies VALUES with NULL values.
func TestValuesWithNull(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("VALUES (1, NULL), (2, 'b')")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var col1 int64
	var col2 sql.NullString
	err = rows.Scan(&col1, &col2)
	require.NoError(t, err)
	assert.Equal(t, int64(1), col1)
	assert.False(t, col2.Valid, "should be NULL")

	require.True(t, rows.Next())
	err = rows.Scan(&col1, &col2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), col1)
	assert.True(t, col2.Valid)
	assert.Equal(t, "b", col2.String)
}

// TestValuesThreeRows verifies VALUES with three rows.
func TestValuesThreeRows(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("VALUES (1), (2), (3)")
	require.NoError(t, err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		err := rows.Scan(&v)
		require.NoError(t, err)
		vals = append(vals, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{1, 2, 3}, vals)
}

// TestValuesColumnNames verifies that VALUES produces default column names.
func TestValuesColumnNames(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("VALUES (1, 'a', true)")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, 3, len(cols), "should have 3 columns")
	// Default column names should be column1, column2, column3
	assert.Equal(t, "column1", cols[0])
	assert.Equal(t, "column2", cols[1])
	assert.Equal(t, "column3", cols[2])
}
