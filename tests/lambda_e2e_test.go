package tests

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scanAnyList scans a row result into []any via the any type.
func scanAnyList(t *testing.T, row *sql.Row) []any {
	t.Helper()
	var result any
	err := row.Scan(&result)
	require.NoError(t, err)
	list, ok := result.([]any)
	require.True(t, ok, "expected []any, got %T: %v", result, result)
	return list
}

// toFloat64 converts any numeric value to float64 for comparison.
func toFloat64(v any) float64 {
	switch val := v.(type) {
	case int64:
		return float64(val)
	case float64:
		return val
	case int:
		return float64(val)
	case int32:
		return float64(val)
	default:
		panic(fmt.Sprintf("cannot convert %T to float64", v))
	}
}

// TestListTransformBasic tests list_transform with x -> x * 2.
func TestListTransformBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	list := scanAnyList(t, db.QueryRow("SELECT list_transform([1, 2, 3], x -> x * 2)"))

	expected := []float64{2, 4, 6}
	require.Equal(t, len(expected), len(list))
	for i, v := range list {
		assert.Equal(t, expected[i], toFloat64(v), "element %d", i)
	}
}

// TestListTransformAddition tests list_transform with x -> x + 10.
func TestListTransformAddition(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	list := scanAnyList(t, db.QueryRow("SELECT list_transform([1, 2, 3], x -> x + 10)"))

	expected := []float64{11, 12, 13}
	require.Equal(t, len(expected), len(list))
	for i, v := range list {
		assert.Equal(t, expected[i], toFloat64(v), "element %d", i)
	}
}

// TestListFilterBasic tests list_filter with x -> x > 3.
func TestListFilterBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	list := scanAnyList(t, db.QueryRow("SELECT list_filter([1, 2, 3, 4, 5], x -> x > 3)"))

	expected := []float64{4, 5}
	require.Equal(t, len(expected), len(list))
	for i, v := range list {
		assert.Equal(t, expected[i], toFloat64(v), "element %d", i)
	}
}

// TestListFilterGe tests list_filter with x -> x >= 25.
func TestListFilterGe(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	list := scanAnyList(t, db.QueryRow("SELECT list_filter([10, 20, 30, 40], x -> x >= 25)"))

	expected := []float64{30, 40}
	require.Equal(t, len(expected), len(list))
	for i, v := range list {
		assert.Equal(t, expected[i], toFloat64(v), "element %d", i)
	}
}

// TestListSortBasic tests list_sort with natural ordering.
func TestListSortBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	list := scanAnyList(t, db.QueryRow("SELECT list_sort([3, 1, 4, 1, 5])"))

	expected := []float64{1, 1, 3, 4, 5}
	require.Equal(t, len(expected), len(list))
	for i, v := range list {
		assert.Equal(t, expected[i], toFloat64(v), "element %d", i)
	}
}

// TestLambdaWithTableData tests lambda with column references from a table.
func TestLambdaWithTableData(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ltest(id INTEGER, vals INTEGER[])")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO ltest VALUES (1, [1, 2, 3])")
	require.NoError(t, err)

	list := scanAnyList(t, db.QueryRow("SELECT list_transform(vals, x -> x + 10) FROM ltest WHERE id = 1"))

	expected := []float64{11, 12, 13}
	require.Equal(t, len(expected), len(list))
	for i, v := range list {
		assert.Equal(t, expected[i], toFloat64(v), "element %d", i)
	}
}

// TestListFilterEmptyResult tests list_filter when no elements match.
func TestListFilterEmptyResult(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	list := scanAnyList(t, db.QueryRow("SELECT list_filter([1, 2, 3], x -> x > 100)"))
	assert.Equal(t, 0, len(list))
}

// TestListSortEmptyList tests list_sort with an empty list.
func TestListSortEmptyList(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	list := scanAnyList(t, db.QueryRow("SELECT list_sort([])"))
	assert.Equal(t, 0, len(list))
}

// TestListTransformEmptyList tests list_transform with an empty list.
func TestListTransformEmptyList(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	list := scanAnyList(t, db.QueryRow("SELECT list_transform([], x -> x * 2)"))
	assert.Equal(t, 0, len(list))
}
