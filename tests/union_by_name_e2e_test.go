package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnionAllByNamePartialOverlap verifies UNION ALL BY NAME with partially
// overlapping columns pads missing columns with NULL.
func TestUnionAllByNamePartialOverlap(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS c")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, cols)

	type row struct {
		a, b, c any
	}
	var results []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.a, &r.b, &r.c)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 2)

	// Row 1: a=1, b=2, c=NULL
	assert.EqualValues(t, 1, results[0].a)
	assert.EqualValues(t, 2, results[0].b)
	assert.Nil(t, results[0].c)

	// Row 2: a=NULL, b=3, c=4
	assert.Nil(t, results[1].a)
	assert.EqualValues(t, 3, results[1].b)
	assert.EqualValues(t, 4, results[1].c)
}

// TestUnionByNameDedup verifies UNION BY NAME removes duplicate rows.
func TestUnionByNameDedup(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT 1 AS a, 2 AS b UNION BY NAME SELECT 1 AS a, 2 AS b")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, cols)

	count := 0
	for rows.Next() {
		var a, b any
		err := rows.Scan(&a, &b)
		require.NoError(t, err)
		assert.EqualValues(t, 1, a)
		assert.EqualValues(t, 2, b)
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 1, count, "UNION BY NAME should deduplicate identical rows")
}

// TestUnionByNameNoOverlap verifies UNION ALL BY NAME with no overlapping
// columns produces NULL padding for all columns from the other side.
func TestUnionByNameNoOverlap(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT 1 AS x UNION ALL BY NAME SELECT 2 AS y")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, []string{"x", "y"}, cols)

	type row struct {
		x, y any
	}
	var results []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.x, &r.y)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 2)

	// Row 1: x=1, y=NULL
	assert.EqualValues(t, 1, results[0].x)
	assert.Nil(t, results[0].y)

	// Row 2: x=NULL, y=2
	assert.Nil(t, results[1].x)
	assert.EqualValues(t, 2, results[1].y)
}

// TestUnionByNameSameColumnsDifferentOrder verifies UNION ALL BY NAME aligns
// columns by name even when they appear in different order.
func TestUnionByNameSameColumnsDifferentOrder(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 4 AS b, 3 AS a")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, cols)

	type row struct {
		a, b any
	}
	var results []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.a, &r.b)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 2)

	// Row 1: a=1, b=2
	assert.EqualValues(t, 1, results[0].a)
	assert.EqualValues(t, 2, results[0].b)

	// Row 2: a=3, b=4
	assert.EqualValues(t, 3, results[1].a)
	assert.EqualValues(t, 4, results[1].b)
}

// TestUnionByNameTypePromotion verifies UNION ALL BY NAME works with
// compatible types across the two sides.
func TestUnionByNameTypePromotion(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT 1 AS val UNION ALL BY NAME SELECT 'hello' AS other, 2 AS val")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, []string{"val", "other"}, cols)

	type row struct {
		val, other any
	}
	var results []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.val, &r.other)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 2)

	// Row 1: val=1, other=NULL
	assert.EqualValues(t, 1, results[0].val)
	assert.Nil(t, results[0].other)

	// Row 2: val=2, other='hello'
	assert.EqualValues(t, 2, results[1].val)
	assert.EqualValues(t, "hello", results[1].other)
}

// TestUnionByNameCaseInsensitive verifies that column name matching in
// UNION ALL BY NAME is case-insensitive.
func TestUnionByNameCaseInsensitive(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT 1 AS Name UNION ALL BY NAME SELECT 2 AS name")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 1, "case-insensitive match should produce a single column")

	var results []int64
	for rows.Next() {
		var v int64
		err := rows.Scan(&v)
		require.NoError(t, err)
		results = append(results, v)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 2)
	assert.EqualValues(t, 1, results[0])
	assert.EqualValues(t, 2, results[1])
}

// TestUnionByNameChained verifies chaining three UNION ALL BY NAME queries,
// each contributing a unique column.
func TestUnionByNameChained(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS b UNION ALL BY NAME SELECT 3 AS c")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, cols)

	type row struct {
		a, b, c any
	}
	var results []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.a, &r.b, &r.c)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 3)

	// Row 1: a=1, b=NULL, c=NULL
	assert.EqualValues(t, 1, results[0].a)
	assert.Nil(t, results[0].b)
	assert.Nil(t, results[0].c)

	// Row 2: a=NULL, b=2, c=NULL
	assert.Nil(t, results[1].a)
	assert.EqualValues(t, 2, results[1].b)
	assert.Nil(t, results[1].c)

	// Row 3: a=NULL, b=NULL, c=3
	assert.Nil(t, results[2].a)
	assert.Nil(t, results[2].b)
	assert.EqualValues(t, 3, results[2].c)
}
