package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helperScanInts scans all rows from the given *sql.Rows into an int64 slice.
func helperScanInts(t *testing.T, rows *sql.Rows) []int64 {
	t.Helper()
	var vals []int64
	for rows.Next() {
		var v int64
		err := rows.Scan(&v)
		require.NoError(t, err)
		vals = append(vals, v)
	}
	require.NoError(t, rows.Err())

	return vals
}

func TestFetchFirst(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create and populate a test table with values 1..10
	_, err = db.Exec("CREATE TABLE nums (id INTEGER)")
	require.NoError(t, err)
	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO nums VALUES (?)", i)
		require.NoError(t, err)
	}

	t.Run("FETCH FIRST 3 ROWS ONLY equals LIMIT 3", func(t *testing.T) {
		rowsFetch, err := db.Query("SELECT id FROM nums ORDER BY id FETCH FIRST 3 ROWS ONLY")
		require.NoError(t, err)
		defer rowsFetch.Close()
		fetchVals := helperScanInts(t, rowsFetch)

		rowsLimit, err := db.Query("SELECT id FROM nums ORDER BY id LIMIT 3")
		require.NoError(t, err)
		defer rowsLimit.Close()
		limitVals := helperScanInts(t, rowsLimit)

		assert.Equal(t, limitVals, fetchVals)
		assert.Equal(t, []int64{1, 2, 3}, fetchVals)
	})

	t.Run("FETCH FIRST ROW ONLY implicit count 1", func(t *testing.T) {
		rows, err := db.Query("SELECT id FROM nums ORDER BY id FETCH FIRST ROW ONLY")
		require.NoError(t, err)
		defer rows.Close()
		vals := helperScanInts(t, rows)
		assert.Equal(t, []int64{1}, vals)
	})

	t.Run("FETCH NEXT 5 ROWS ONLY", func(t *testing.T) {
		rows, err := db.Query("SELECT id FROM nums ORDER BY id FETCH NEXT 5 ROWS ONLY")
		require.NoError(t, err)
		defer rows.Close()
		vals := helperScanInts(t, rows)
		assert.Equal(t, []int64{1, 2, 3, 4, 5}, vals)
	})

	t.Run("OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY", func(t *testing.T) {
		rows, err := db.Query("SELECT id FROM nums ORDER BY id OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY")
		require.NoError(t, err)
		defer rows.Close()
		vals := helperScanInts(t, rows)
		assert.Equal(t, []int64{3, 4, 5}, vals)
	})

	t.Run("FETCH FIRST 0 ROWS ONLY returns empty", func(t *testing.T) {
		rows, err := db.Query("SELECT id FROM nums ORDER BY id FETCH FIRST 0 ROWS ONLY")
		require.NoError(t, err)
		defer rows.Close()
		vals := helperScanInts(t, rows)
		assert.Empty(t, vals)
	})

	t.Run("FETCH FIRST on empty table", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE empty_tbl (id INTEGER)")
		require.NoError(t, err)

		rows, err := db.Query("SELECT id FROM empty_tbl FETCH FIRST 5 ROWS ONLY")
		require.NoError(t, err)
		defer rows.Close()
		vals := helperScanInts(t, rows)
		assert.Empty(t, vals)
	})

	t.Run("WITH TIES includes tied rows", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE scores (name VARCHAR, score INTEGER)")
		require.NoError(t, err)
		for _, row := range []struct {
			name  string
			score int
		}{
			{"alice", 100},
			{"bob", 90},
			{"carol", 90},
			{"dave", 80},
			{"eve", 70},
		} {
			_, err = db.Exec("INSERT INTO scores VALUES (?, ?)", row.name, row.score)
			require.NoError(t, err)
		}

		// FETCH FIRST 2 ROWS WITH TIES on ORDER BY score DESC:
		// Top 2 by score are alice(100) and bob(90). carol also has 90 which ties
		// with the 2nd row, so she should be included.
		rows, err := db.Query("SELECT score FROM scores ORDER BY score DESC FETCH FIRST 2 ROWS WITH TIES")
		require.NoError(t, err)
		defer rows.Close()
		vals := helperScanInts(t, rows)
		assert.Equal(t, []int64{100, 90, 90}, vals)
	})

	t.Run("WITH TIES with NULLs in ORDER BY column", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE scores_null (name VARCHAR, score INTEGER)")
		require.NoError(t, err)
		// Insert rows including NULLs. DuckDB sorts NULLs last by default in DESC.
		_, err = db.Exec("INSERT INTO scores_null VALUES ('a', 100)")
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO scores_null VALUES ('b', 90)")
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO scores_null VALUES ('c', 90)")
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO scores_null VALUES ('d', NULL)")
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO scores_null VALUES ('e', NULL)")
		require.NoError(t, err)

		// ORDER BY score DESC NULLS LAST: 100, 90, 90, NULL, NULL
		// FETCH FIRST 3 ROWS WITH TIES: row 3 has score=90 which ties with row 2,
		// so we get rows with scores 100, 90, 90 (3 rows, no extra ties beyond existing).
		rows, err := db.Query("SELECT score FROM scores_null ORDER BY score DESC NULLS LAST FETCH FIRST 3 ROWS WITH TIES")
		require.NoError(t, err)
		defer rows.Close()

		var vals []sql.NullInt64
		for rows.Next() {
			var v sql.NullInt64
			err := rows.Scan(&v)
			require.NoError(t, err)
			vals = append(vals, v)
		}
		require.NoError(t, rows.Err())

		// Should return exactly 3 rows: 100, 90, 90
		require.Len(t, vals, 3)
		assert.True(t, vals[0].Valid)
		assert.Equal(t, int64(100), vals[0].Int64)
		assert.True(t, vals[1].Valid)
		assert.Equal(t, int64(90), vals[1].Int64)
		assert.True(t, vals[2].Valid)
		assert.Equal(t, int64(90), vals[2].Int64)
	})

	t.Run("large offset beyond available rows", func(t *testing.T) {
		rows, err := db.Query("SELECT id FROM nums ORDER BY id OFFSET 100 ROWS FETCH FIRST 5 ROWS ONLY")
		require.NoError(t, err)
		defer rows.Close()
		vals := helperScanInts(t, rows)
		assert.Empty(t, vals)
	})
}
