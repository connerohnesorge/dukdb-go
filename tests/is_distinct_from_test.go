package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsDistinctFrom(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// IS DISTINCT FROM tests

	t.Run("both non-NULL equal", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 IS DISTINCT FROM 1").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, false, result)
	})

	t.Run("both non-NULL different", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 IS DISTINCT FROM 2").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	t.Run("left NULL", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT NULL IS DISTINCT FROM 1").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	t.Run("right NULL", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 IS DISTINCT FROM NULL").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	t.Run("both NULL", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT NULL IS DISTINCT FROM NULL").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, false, result)
	})

	// IS NOT DISTINCT FROM tests

	t.Run("IS NOT DISTINCT FROM - equal", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 IS NOT DISTINCT FROM 1").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	t.Run("IS NOT DISTINCT FROM - different", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 IS NOT DISTINCT FROM 2").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, false, result)
	})

	t.Run("IS NOT DISTINCT FROM - left NULL", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT NULL IS NOT DISTINCT FROM 1").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, false, result)
	})

	t.Run("IS NOT DISTINCT FROM - right NULL", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 IS NOT DISTINCT FROM NULL").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, false, result)
	})

	t.Run("IS NOT DISTINCT FROM - both NULL", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT NULL IS NOT DISTINCT FROM NULL").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	// String comparisons

	t.Run("string comparison", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 'hello' IS DISTINCT FROM 'world'").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	t.Run("string equal", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 'hello' IS NOT DISTINCT FROM 'hello'").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	// WHERE clause with table

	t.Run("WHERE clause", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE test_distinct (id INTEGER, val INTEGER)")
		require.NoError(t, err)

		_, err = db.Exec("INSERT INTO test_distinct VALUES (1, 10), (2, NULL), (3, 10), (4, NULL), (5, 20)")
		require.NoError(t, err)

		// WHERE val IS DISTINCT FROM 10 should return rows where val != 10 or val is NULL
		// That means rows: (2, NULL), (4, NULL), (5, 20) => count 3
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_distinct WHERE val IS DISTINCT FROM 10").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		// WHERE val IS NOT DISTINCT FROM NULL should return rows where val IS NULL
		// That means rows: (2, NULL), (4, NULL) => count 2
		err = db.QueryRow("SELECT COUNT(*) FROM test_distinct WHERE val IS NOT DISTINCT FROM NULL").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)

		_, err = db.Exec("DROP TABLE test_distinct")
		require.NoError(t, err)
	})

	// Regression checks for IS NULL / IS NOT NULL

	t.Run("IS NULL still works", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT NULL IS NULL").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	t.Run("IS NOT NULL still works", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT 1 IS NOT NULL").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})
}
