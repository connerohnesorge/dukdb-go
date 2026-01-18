package executor_test

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnnestBasicIntArray tests UNNEST with a basic integer array.
func TestUnnestBasicIntArray(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Test UNNEST with integer array
	rows, err := db.Query("SELECT * FROM UNNEST([1, 2, 3])")
	require.NoError(t, err)
	defer rows.Close()

	var results []int64
	for rows.Next() {
		var val int64
		err := rows.Scan(&val)
		require.NoError(t, err)
		results = append(results, val)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int64{1, 2, 3}, results)
}

// TestUnnestStringArray tests UNNEST with a string array.
func TestUnnestStringArray(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Test UNNEST with string array
	rows, err := db.Query("SELECT * FROM UNNEST(['a', 'b', 'c'])")
	require.NoError(t, err)
	defer rows.Close()

	var results []string
	for rows.Next() {
		var val string
		err := rows.Scan(&val)
		require.NoError(t, err)
		results = append(results, val)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []string{"a", "b", "c"}, results)
}

// TestUnnestEmptyArray tests UNNEST with an empty array.
func TestUnnestEmptyArray(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Test UNNEST with empty array - should return 0 rows
	rows, err := db.Query("SELECT * FROM UNNEST([])")
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, 0, count)
}

// TestUnnestWithAlias tests UNNEST with a table alias.
func TestUnnestWithAlias(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Test UNNEST with alias
	rows, err := db.Query("SELECT * FROM UNNEST([10, 20, 30]) AS t")
	require.NoError(t, err)
	defer rows.Close()

	var results []int64
	for rows.Next() {
		var val int64
		err := rows.Scan(&val)
		require.NoError(t, err)
		results = append(results, val)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int64{10, 20, 30}, results)
}

// TestUnnestSingleElement tests UNNEST with a single element array.
func TestUnnestSingleElement(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Test UNNEST with single element
	rows, err := db.Query("SELECT * FROM UNNEST([42])")
	require.NoError(t, err)
	defer rows.Close()

	var results []int64
	for rows.Next() {
		var val int64
		err := rows.Scan(&val)
		require.NoError(t, err)
		results = append(results, val)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int64{42}, results)
}

// TestUnnestColumnName tests that UNNEST produces the correct column name.
func TestUnnestColumnName(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Get column names
	rows, err := db.Query("SELECT * FROM UNNEST([1, 2])")
	require.NoError(t, err)
	defer rows.Close()

	columns, err := rows.Columns()
	require.NoError(t, err)

	// Default column name should be "unnest"
	assert.Equal(t, []string{"unnest"}, columns)
}

// TestUnnestWithMixedTypes tests UNNEST with arrays containing different value types.
func TestUnnestWithMixedTypes(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Test with floats
	rows, err := db.Query("SELECT * FROM UNNEST([1.5, 2.5, 3.5])")
	require.NoError(t, err)
	defer rows.Close()

	var results []float64
	for rows.Next() {
		var val float64
		err := rows.Scan(&val)
		require.NoError(t, err)
		results = append(results, val)
	}
	require.NoError(t, rows.Err())

	assert.InDelta(t, 1.5, results[0], 0.01)
	assert.InDelta(t, 2.5, results[1], 0.01)
	assert.InDelta(t, 3.5, results[2], 0.01)
}

// TestUnnestNoArgError tests that UNNEST without arguments returns an error.
func TestUnnestNoArgError(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// UNNEST without arguments should fail
	_, err = db.Query("SELECT * FROM UNNEST()")
	assert.Error(t, err)
}

// TestUnnestInSelectList tests UNNEST used in WHERE with a join.
func TestUnnestInJoin(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Create a simple table
	_, err = db.Exec("CREATE TABLE nums (id INTEGER)")
	require.NoError(t, err)
	defer db.Exec("DROP TABLE nums")

	_, err = db.Exec("INSERT INTO nums VALUES (1), (2), (3)")
	require.NoError(t, err)

	// Cross join with UNNEST
	rows, err := db.Query(
		"SELECT n.id, u.unnest FROM nums n, UNNEST(['a', 'b']) AS u ORDER BY n.id, u.unnest",
	)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		ID  int64
		Val string
	}
	for rows.Next() {
		var r struct {
			ID  int64
			Val string
		}
		err := rows.Scan(&r.ID, &r.Val)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	// Should have 6 rows (3 nums x 2 array elements)
	assert.Len(t, results, 6)
}
