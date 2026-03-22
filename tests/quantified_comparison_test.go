package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuantifiedComparison(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("ANY with equality match", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 = ANY(SELECT 1 UNION ALL SELECT 2)").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("ANY with no match", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 3 = ANY(SELECT 1 UNION ALL SELECT 2)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("ALL with all matching", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 3 > ALL(SELECT 1 UNION ALL SELECT 2)").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("ALL with not all matching", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 > ALL(SELECT 0 UNION ALL SELECT 2)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("SOME is alias for ANY", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 = SOME(SELECT 1 UNION ALL SELECT 2)").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("ANY with empty subquery", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE empty_t(x INTEGER)")
		require.NoError(t, err)

		var result bool
		err = db.QueryRow("SELECT 1 = ANY(SELECT x FROM empty_t)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("ALL with empty subquery vacuous truth", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 = ALL(SELECT x FROM empty_t)").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("NULL left operand returns NULL", func(t *testing.T) {
		var result sql.NullBool
		err := db.QueryRow("SELECT NULL = ANY(SELECT 1)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid) // NULL result
	})

	t.Run("ANY with greater than", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 5 > ANY(SELECT 3 UNION ALL SELECT 7)").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result) // 5 > 3 is true
	})

	t.Run("ALL with less than or equal", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 <= ALL(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3)").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result) // 1 <= all of {1,2,3}
	})

	t.Run("ANY in WHERE clause", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE vals(x INTEGER)")
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO vals VALUES (1), (2), (3), (4), (5)")
		require.NoError(t, err)
		_, err = db.Exec("CREATE TABLE thresholds(t INTEGER)")
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO thresholds VALUES (3), (4)")
		require.NoError(t, err)

		rows, err := db.Query("SELECT x FROM vals WHERE x > ALL(SELECT t FROM thresholds) ORDER BY x")
		require.NoError(t, err)
		defer rows.Close()

		var results []int
		for rows.Next() {
			var x int
			require.NoError(t, rows.Scan(&x))
			results = append(results, x)
		}
		assert.Equal(t, []int{5}, results) // only 5 > all of {3,4}
	})

	t.Run("not equal ALL", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 3 <> ALL(SELECT 1 UNION ALL SELECT 2)").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result) // 3 is not equal to any of {1,2}
	})
}
