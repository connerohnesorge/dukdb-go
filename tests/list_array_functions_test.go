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

// scanNullableAny scans a result that may be NULL. Returns nil for NULL.
func scanNullableAny(t *testing.T, row *sql.Row) any {
	t.Helper()
	var result any
	err := row.Scan(&result)
	require.NoError(t, err)
	return result
}

// scanNullableList scans a list result that may be NULL.
func scanNullableList(t *testing.T, row *sql.Row) []any {
	t.Helper()
	result := scanNullableAny(t, row)
	if result == nil {
		return nil
	}
	list, ok := result.([]any)
	require.True(t, ok, "expected []any, got %T: %v", result, result)
	return list
}

func TestListArrayFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// ---------------------------------------------------------------
	// LIST_ELEMENT / ARRAY_EXTRACT
	// ---------------------------------------------------------------
	t.Run("LIST_ELEMENT", func(t *testing.T) {
		t.Run("basic 1-based indexing", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_ELEMENT([10, 20, 30], 2)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(20), result)
		})

		t.Run("first element", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_ELEMENT([10, 20, 30], 1)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(10), result)
		})

		t.Run("last element", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_ELEMENT([10, 20, 30], 3)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(30), result)
		})

		t.Run("negative index from end", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_ELEMENT([10, 20, 30], -1)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(30), result)
		})

		t.Run("negative index second from end", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_ELEMENT([10, 20, 30], -2)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(20), result)
		})

		t.Run("out of bounds returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT LIST_ELEMENT([10, 20, 30], 5)"))
			assert.Nil(t, result)
		})

		t.Run("index zero returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT LIST_ELEMENT([10, 20, 30], 0)"))
			assert.Nil(t, result)
		})

		t.Run("negative out of bounds returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT LIST_ELEMENT([10, 20, 30], -5)"))
			assert.Nil(t, result)
		})

		t.Run("NULL list returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT LIST_ELEMENT(NULL, 1)"))
			assert.Nil(t, result)
		})

		t.Run("empty list returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT LIST_ELEMENT([], 1)"))
			assert.Nil(t, result)
		})

		t.Run("string list", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT LIST_ELEMENT(['a', 'b', 'c'], 2)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "b", result)
		})

		t.Run("single element list", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_ELEMENT([42], 1)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(42), result)
		})
	})

	t.Run("ARRAY_EXTRACT alias", func(t *testing.T) {
		t.Run("basic indexing", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT ARRAY_EXTRACT([10, 20, 30], 2)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(20), result)
		})

		t.Run("negative index", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT ARRAY_EXTRACT([10, 20, 30], -1)").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(30), result)
		})

		t.Run("out of bounds returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT ARRAY_EXTRACT([10, 20, 30], 10)"))
			assert.Nil(t, result)
		})

		t.Run("NULL list returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT ARRAY_EXTRACT(NULL, 1)"))
			assert.Nil(t, result)
		})
	})

	// ---------------------------------------------------------------
	// LIST_AGGREGATE / ARRAY_AGGREGATE
	// ---------------------------------------------------------------
	t.Run("LIST_AGGREGATE", func(t *testing.T) {
		t.Run("sum", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_AGGREGATE([1, 2, 3], 'sum')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(6), result)
		})

		t.Run("avg", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_AGGREGATE([1, 2, 3], 'avg')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(2), result)
		})

		t.Run("min", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_AGGREGATE([5, 1, 3], 'min')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(1), result)
		})

		t.Run("max", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_AGGREGATE([5, 1, 3], 'max')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(5), result)
		})

		t.Run("count", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_AGGREGATE([1, 2, 3], 'count')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(3), result)
		})

		t.Run("first", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_AGGREGATE([10, 20, 30], 'first')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(10), result)
		})

		t.Run("last", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_AGGREGATE([10, 20, 30], 'last')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(30), result)
		})

		t.Run("string_agg", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT LIST_AGGREGATE(['a', 'b', 'c'], 'string_agg', ',')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "a,b,c", result)
		})

		t.Run("bool_and true", func(t *testing.T) {
			var result bool
			err := db.QueryRow("SELECT LIST_AGGREGATE([true, true, true], 'bool_and')").Scan(&result)
			require.NoError(t, err)
			assert.True(t, result)
		})

		t.Run("bool_and false", func(t *testing.T) {
			var result bool
			err := db.QueryRow("SELECT LIST_AGGREGATE([true, false, true], 'bool_and')").Scan(&result)
			require.NoError(t, err)
			assert.False(t, result)
		})

		t.Run("bool_or true", func(t *testing.T) {
			var result bool
			err := db.QueryRow("SELECT LIST_AGGREGATE([false, false, true], 'bool_or')").Scan(&result)
			require.NoError(t, err)
			assert.True(t, result)
		})

		t.Run("bool_or false", func(t *testing.T) {
			var result bool
			err := db.QueryRow("SELECT LIST_AGGREGATE([false, false, false], 'bool_or')").Scan(&result)
			require.NoError(t, err)
			assert.False(t, result)
		})

		t.Run("NULL list returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT LIST_AGGREGATE(NULL, 'sum')"))
			assert.Nil(t, result)
		})

		t.Run("empty list count", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_AGGREGATE([], 'count')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(0), result)
		})

		t.Run("single element sum", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT LIST_AGGREGATE([42], 'sum')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(42), result)
		})

		t.Run("string_agg with custom separator", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT LIST_AGGREGATE(['x', 'y', 'z'], 'string_agg', ' - ')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "x - y - z", result)
		})
	})

	t.Run("ARRAY_AGGREGATE alias", func(t *testing.T) {
		t.Run("sum", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT ARRAY_AGGREGATE([1, 2, 3], 'sum')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(6), result)
		})

		t.Run("min", func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT ARRAY_AGGREGATE([5, 1, 3], 'min')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, int64(1), result)
		})

		t.Run("bool_or", func(t *testing.T) {
			var result bool
			err := db.QueryRow("SELECT ARRAY_AGGREGATE([false, true], 'bool_or')").Scan(&result)
			require.NoError(t, err)
			assert.True(t, result)
		})

		t.Run("NULL list returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT ARRAY_AGGREGATE(NULL, 'sum')"))
			assert.Nil(t, result)
		})
	})

	// ---------------------------------------------------------------
	// LIST_REVERSE_SORT / ARRAY_REVERSE_SORT
	// ---------------------------------------------------------------
	t.Run("LIST_REVERSE_SORT", func(t *testing.T) {
		t.Run("basic descending sort", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_REVERSE_SORT([3, 1, 2])"))
			require.Len(t, list, 3)
			assert.Equal(t, float64(3), toFloat64(list[0]))
			assert.Equal(t, float64(2), toFloat64(list[1]))
			assert.Equal(t, float64(1), toFloat64(list[2]))
		})

		t.Run("already sorted descending", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_REVERSE_SORT([5, 4, 3, 2, 1])"))
			require.Len(t, list, 5)
			for i, expected := range []float64{5, 4, 3, 2, 1} {
				assert.Equal(t, expected, toFloat64(list[i]), "element %d", i)
			}
		})

		t.Run("NULLs sort to end", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_REVERSE_SORT([3, NULL, 1])"))
			require.Len(t, list, 3)
			assert.Equal(t, float64(3), toFloat64(list[0]))
			assert.Equal(t, float64(1), toFloat64(list[1]))
			assert.Nil(t, list[2])
		})

		t.Run("NULL list returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT LIST_REVERSE_SORT(NULL)"))
			assert.Nil(t, result)
		})

		t.Run("empty list", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_REVERSE_SORT([])"))
			assert.Len(t, list, 0)
		})

		t.Run("single element", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_REVERSE_SORT([42])"))
			require.Len(t, list, 1)
			assert.Equal(t, float64(42), toFloat64(list[0]))
		})

		t.Run("string list", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_REVERSE_SORT(['apple', 'cherry', 'banana'])"))
			require.Len(t, list, 3)
			assert.Equal(t, "cherry", list[0])
			assert.Equal(t, "banana", list[1])
			assert.Equal(t, "apple", list[2])
		})
	})

	t.Run("ARRAY_REVERSE_SORT alias", func(t *testing.T) {
		t.Run("basic descending sort", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT ARRAY_REVERSE_SORT([3, 1, 2])"))
			require.Len(t, list, 3)
			assert.Equal(t, float64(3), toFloat64(list[0]))
			assert.Equal(t, float64(2), toFloat64(list[1]))
			assert.Equal(t, float64(1), toFloat64(list[2]))
		})

		t.Run("NULL list returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT ARRAY_REVERSE_SORT(NULL)"))
			assert.Nil(t, result)
		})
	})

	// ---------------------------------------------------------------
	// ARRAY_TO_STRING / LIST_TO_STRING
	// ---------------------------------------------------------------
	t.Run("ARRAY_TO_STRING", func(t *testing.T) {
		t.Run("basic join", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT ARRAY_TO_STRING([1, 2, 3], ',')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "1,2,3", result)
		})

		t.Run("string list join", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], '-')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "a-b-c", result)
		})

		t.Run("NULLs skipped without null_string", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT ARRAY_TO_STRING([1, NULL, 3], ',')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "1,3", result)
		})

		t.Run("NULLs replaced with null_string", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT ARRAY_TO_STRING([1, NULL, 3], ',', 'N')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "1,N,3", result)
		})

		t.Run("NULL list returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT ARRAY_TO_STRING(NULL, ',')"))
			assert.Nil(t, result)
		})

		t.Run("empty list returns empty string", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT ARRAY_TO_STRING([], ',')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "", result)
		})

		t.Run("single element", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT ARRAY_TO_STRING([42], ',')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "42", result)
		})

		t.Run("multi-char separator", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT ARRAY_TO_STRING([1, 2, 3], ' :: ')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "1 :: 2 :: 3", result)
		})
	})

	t.Run("LIST_TO_STRING alias", func(t *testing.T) {
		t.Run("basic join", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT LIST_TO_STRING([1, 2, 3], ',')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "1,2,3", result)
		})

		t.Run("with null_string", func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT LIST_TO_STRING([1, NULL, 3], ',', 'X')").Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, "1,X,3", result)
		})

		t.Run("NULL list returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT LIST_TO_STRING(NULL, ',')"))
			assert.Nil(t, result)
		})
	})

	// ---------------------------------------------------------------
	// LIST_ZIP
	// ---------------------------------------------------------------
	t.Run("LIST_ZIP", func(t *testing.T) {
		t.Run("equal length lists", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_ZIP([1, 2], ['a', 'b'])"))
			require.Len(t, list, 2)
			row0, ok := list[0].(map[string]any)
			require.True(t, ok, "expected map, got %T", list[0])
			assert.Equal(t, int64(1), row0["f1"])
			assert.Equal(t, "a", row0["f2"])
			row1, ok := list[1].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, int64(2), row1["f1"])
			assert.Equal(t, "b", row1["f2"])
		})

		t.Run("unequal length lists pads with NULL", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_ZIP([1, 2, 3], ['a'])"))
			require.Len(t, list, 3)
			row0, ok := list[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, int64(1), row0["f1"])
			assert.Equal(t, "a", row0["f2"])
			row1, ok := list[1].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, int64(2), row1["f1"])
			assert.Nil(t, row1["f2"])
			row2, ok := list[2].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, int64(3), row2["f1"])
			assert.Nil(t, row2["f2"])
		})

		t.Run("second list longer", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_ZIP([1], ['a', 'b', 'c'])"))
			require.Len(t, list, 3)
			row0, ok := list[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, int64(1), row0["f1"])
			assert.Equal(t, "a", row0["f2"])
			row1, ok := list[1].(map[string]any)
			require.True(t, ok)
			assert.Nil(t, row1["f1"])
			assert.Equal(t, "b", row1["f2"])
		})

		t.Run("three lists", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_ZIP([1], ['a'], [true])"))
			require.Len(t, list, 1)
			row0, ok := list[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, int64(1), row0["f1"])
			assert.Equal(t, "a", row0["f2"])
			assert.Equal(t, true, row0["f3"])
		})
	})

	// ---------------------------------------------------------------
	// LIST_RESIZE / ARRAY_RESIZE
	// ---------------------------------------------------------------
	t.Run("LIST_RESIZE", func(t *testing.T) {
		t.Run("pad with NULL", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_RESIZE([1, 2, 3], 5)"))
			require.Len(t, list, 5)
			assert.Equal(t, float64(1), toFloat64(list[0]))
			assert.Equal(t, float64(2), toFloat64(list[1]))
			assert.Equal(t, float64(3), toFloat64(list[2]))
			assert.Nil(t, list[3])
			assert.Nil(t, list[4])
		})

		t.Run("truncate", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_RESIZE([1, 2, 3], 2)"))
			require.Len(t, list, 2)
			assert.Equal(t, float64(1), toFloat64(list[0]))
			assert.Equal(t, float64(2), toFloat64(list[1]))
		})

		t.Run("pad with explicit value", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_RESIZE([1], 3, 0)"))
			require.Len(t, list, 3)
			assert.Equal(t, float64(1), toFloat64(list[0]))
			assert.Equal(t, float64(0), toFloat64(list[1]))
			assert.Equal(t, float64(0), toFloat64(list[2]))
		})

		t.Run("same size no change", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_RESIZE([1, 2, 3], 3)"))
			require.Len(t, list, 3)
			assert.Equal(t, float64(1), toFloat64(list[0]))
			assert.Equal(t, float64(2), toFloat64(list[1]))
			assert.Equal(t, float64(3), toFloat64(list[2]))
		})

		t.Run("size zero", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_RESIZE([1, 2, 3], 0)"))
			assert.Len(t, list, 0)
		})

		t.Run("NULL list returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT LIST_RESIZE(NULL, 3)"))
			assert.Nil(t, result)
		})

		t.Run("empty list padded", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_RESIZE([], 3)"))
			require.Len(t, list, 3)
			assert.Nil(t, list[0])
			assert.Nil(t, list[1])
			assert.Nil(t, list[2])
		})

		t.Run("empty list pad with value", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_RESIZE([], 2, 99)"))
			require.Len(t, list, 2)
			assert.Equal(t, float64(99), toFloat64(list[0]))
			assert.Equal(t, float64(99), toFloat64(list[1]))
		})

		t.Run("negative size treated as zero", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_RESIZE([1, 2, 3], -1)"))
			assert.Len(t, list, 0)
		})

		t.Run("single element truncate", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_RESIZE([42], 1)"))
			require.Len(t, list, 1)
			assert.Equal(t, float64(42), toFloat64(list[0]))
		})

		t.Run("string list pad", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT LIST_RESIZE(['a'], 3, 'z')"))
			require.Len(t, list, 3)
			assert.Equal(t, "a", list[0])
			assert.Equal(t, "z", list[1])
			assert.Equal(t, "z", list[2])
		})
	})

	t.Run("ARRAY_RESIZE alias", func(t *testing.T) {
		t.Run("pad with NULL", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT ARRAY_RESIZE([1, 2], 4)"))
			require.Len(t, list, 4)
			assert.Equal(t, float64(1), toFloat64(list[0]))
			assert.Equal(t, float64(2), toFloat64(list[1]))
			assert.Nil(t, list[2])
			assert.Nil(t, list[3])
		})

		t.Run("truncate", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT ARRAY_RESIZE([1, 2, 3, 4], 2)"))
			require.Len(t, list, 2)
			assert.Equal(t, float64(1), toFloat64(list[0]))
			assert.Equal(t, float64(2), toFloat64(list[1]))
		})

		t.Run("pad with value", func(t *testing.T) {
			list := scanNullableList(t, db.QueryRow("SELECT ARRAY_RESIZE([10], 3, 5)"))
			require.Len(t, list, 3)
			assert.Equal(t, float64(10), toFloat64(list[0]))
			assert.Equal(t, float64(5), toFloat64(list[1]))
			assert.Equal(t, float64(5), toFloat64(list[2]))
		})

		t.Run("NULL list returns NULL", func(t *testing.T) {
			result := scanNullableAny(t, db.QueryRow("SELECT ARRAY_RESIZE(NULL, 3)"))
			assert.Nil(t, result)
		})
	})
}

// TestListArrayFunctionsCrossType tests list/array functions with mixed type scenarios
// and verifies functions compose correctly.
func TestListArrayFunctionsCrossType(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("LIST_ELEMENT on reverse sorted list via subquery", func(t *testing.T) {
		// Compose: extract element from a reverse-sorted list using a table
		_, err := db.Exec("CREATE TABLE test_list_compose (vals INTEGER[])")
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO test_list_compose VALUES ([3, 1, 4, 1, 5])")
		require.NoError(t, err)

		var result string
		err = db.QueryRow("SELECT ARRAY_TO_STRING(LIST_REVERSE_SORT(vals), ',') FROM test_list_compose").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "5,4,3,1,1", result)
	})

	t.Run("ARRAY_TO_STRING with integer list", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT ARRAY_TO_STRING([100, 200, 300], ' + ')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "100 + 200 + 300", result)
	})

	t.Run("LIST_AGGREGATE sum with single element", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT LIST_AGGREGATE([100], 'sum')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(100), result)
	})

	t.Run("LIST_AGGREGATE string_agg single element", func(t *testing.T) {
		var result string
		err := db.QueryRow("SELECT LIST_AGGREGATE(['hello'], 'string_agg', ',')").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "hello", result)
	})
}

// TestListArrayFunctionsAllAliasesExist verifies every alias is recognized by the engine.
func TestListArrayFunctionsAllAliasesExist(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Each entry: SQL query, description, expected non-error
	aliasQueries := []struct {
		name  string
		query string
	}{
		{"LIST_ELEMENT", "SELECT LIST_ELEMENT([1,2,3], 1)"},
		{"ARRAY_EXTRACT", "SELECT ARRAY_EXTRACT([1,2,3], 1)"},
		{"LIST_AGGREGATE", "SELECT LIST_AGGREGATE([1,2,3], 'sum')"},
		{"ARRAY_AGGREGATE", "SELECT ARRAY_AGGREGATE([1,2,3], 'sum')"},
		{"LIST_REVERSE_SORT", "SELECT LIST_REVERSE_SORT([3,1,2])"},
		{"ARRAY_REVERSE_SORT", "SELECT ARRAY_REVERSE_SORT([3,1,2])"},
		{"ARRAY_TO_STRING", "SELECT ARRAY_TO_STRING([1,2,3], ',')"},
		{"LIST_TO_STRING", "SELECT LIST_TO_STRING([1,2,3], ',')"},
		{"LIST_ZIP", "SELECT LIST_ZIP([1,2], ['a','b'])"},
		{"LIST_RESIZE", "SELECT LIST_RESIZE([1,2,3], 2)"},
		{"ARRAY_RESIZE", "SELECT ARRAY_RESIZE([1,2,3], 2)"},
	}

	for _, tc := range aliasQueries {
		t.Run(fmt.Sprintf("%s is recognized", tc.name), func(t *testing.T) {
			var result any
			err := db.QueryRow(tc.query).Scan(&result)
			require.NoError(t, err, "function %s should be recognized", tc.name)
			require.NotNil(t, result, "function %s should return a non-nil result", tc.name)
		})
	}
}
