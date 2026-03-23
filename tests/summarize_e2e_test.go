package tests

import (
	"database/sql"
	"fmt"
	"math"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/require"
)

func TestSummarizeBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO products VALUES (1, 'apple', 1.50), (2, 'banana', 0.75), (3, 'cherry', 3.00)")
	require.NoError(t, err)

	rows, err := db.Query("SUMMARIZE products")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, []string{"column_name", "column_type", "min", "max", "approx_unique", "avg", "std", "q25", "q50", "q75", "count", "null_percentage"}, cols)

	var results []map[string]any
	for rows.Next() {
		var colName, colType string
		var minVal, maxVal, q25, q50, q75 sql.NullString
		var approxUnique, count int64
		var avg, std, nullPct sql.NullFloat64
		err = rows.Scan(&colName, &colType, &minVal, &maxVal, &approxUnique, &avg, &std, &q25, &q50, &q75, &count, &nullPct)
		require.NoError(t, err)
		results = append(results, map[string]any{
			"column_name": colName, "column_type": colType,
			"min": minVal, "max": maxVal, "approx_unique": approxUnique,
			"avg": avg, "std": std, "q25": q25, "q50": q50, "q75": q75,
			"count": count, "null_percentage": nullPct,
		})
	}
	require.Len(t, results, 3) // 3 columns
}

func TestSummarizeEmptyTable(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE empty (a INTEGER, b VARCHAR)")
	require.NoError(t, err)

	rows, err := db.Query("SUMMARIZE empty")
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		var colName, colType string
		var minVal, maxVal, q25, q50, q75 sql.NullString
		var approxUnique, cnt int64
		var avg, std, nullPct sql.NullFloat64
		err = rows.Scan(&colName, &colType, &minVal, &maxVal, &approxUnique, &avg, &std, &q25, &q50, &q75, &cnt, &nullPct)
		require.NoError(t, err)
		require.Equal(t, int64(0), cnt)
		count++
	}
	require.Equal(t, 2, count)
}

func TestSummarizeNullPercentage(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE nulls (a INTEGER, b INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO nulls VALUES (1, NULL), (2, NULL), (3, 30), (4, 40)")
	require.NoError(t, err)

	rows, err := db.Query("SUMMARIZE nulls")
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var colName, colType string
		var minVal, maxVal, q25, q50, q75 sql.NullString
		var approxUnique, cnt int64
		var avg, std, nullPct sql.NullFloat64
		err = rows.Scan(&colName, &colType, &minVal, &maxVal, &approxUnique, &avg, &std, &q25, &q50, &q75, &cnt, &nullPct)
		require.NoError(t, err)

		if colName == "a" {
			require.Equal(t, int64(4), cnt) // all non-null
			require.InDelta(t, 0.0, nullPct.Float64, 0.01)
		}
		if colName == "b" {
			require.Equal(t, int64(2), cnt) // 2 non-null
			require.InDelta(t, 50.0, nullPct.Float64, 0.01)
		}
	}
}

func TestSummarizeNumericMinMax(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE nums (v INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO nums VALUES (2), (10), (100)")
	require.NoError(t, err)

	rows, err := db.Query("SUMMARIZE nums")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var colName, colType string
	var minVal, maxVal, q25, q50, q75 sql.NullString
	var approxUnique, cnt int64
	var avg, std, nullPct sql.NullFloat64
	err = rows.Scan(&colName, &colType, &minVal, &maxVal, &approxUnique, &avg, &std, &q25, &q50, &q75, &cnt, &nullPct)
	require.NoError(t, err)
	require.Equal(t, "2", minVal.String)
	require.Equal(t, "100", maxVal.String)
}

func TestSummarizeSampleStdDev(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE stdtest (v INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO stdtest VALUES (2), (4)")
	require.NoError(t, err)

	rows, err := db.Query("SUMMARIZE stdtest")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var colName, colType string
	var minVal, maxVal, q25, q50, q75 sql.NullString
	var approxUnique, cnt int64
	var avg, std, nullPct sql.NullFloat64
	err = rows.Scan(&colName, &colType, &minVal, &maxVal, &approxUnique, &avg, &std, &q25, &q50, &q75, &cnt, &nullPct)
	require.NoError(t, err)
	// Sample std dev of [2, 4] = sqrt(2) ~ 1.414
	require.True(t, std.Valid)
	require.InDelta(t, math.Sqrt(2), std.Float64, 0.01)
}

func TestSummarizeSelect(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE items (category VARCHAR, price DOUBLE)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO items VALUES ('A', 10.0), ('A', 20.0), ('B', 30.0)")
	require.NoError(t, err)

	rows, err := db.Query("SUMMARIZE SELECT price FROM items WHERE category = 'A'")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var colName, colType string
	var minVal, maxVal, q25, q50, q75 sql.NullString
	var approxUnique, cnt int64
	var avg, std, nullPct sql.NullFloat64
	err = rows.Scan(&colName, &colType, &minVal, &maxVal, &approxUnique, &avg, &std, &q25, &q50, &q75, &cnt, &nullPct)
	require.NoError(t, err)
	require.Equal(t, "price", colName)
	require.Equal(t, int64(2), cnt) // Only category A rows
	require.InDelta(t, 15.0, avg.Float64, 0.01)
}

func TestSummarizeLargeTable(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE large (v INTEGER)")
	require.NoError(t, err)
	for i := 1; i <= 100; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO large VALUES (%d)", i))
		require.NoError(t, err)
	}

	rows, err := db.Query("SUMMARIZE large")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var colName, colType string
	var minVal, maxVal, q25Val, q50Val, q75Val sql.NullString
	var approxUnique, cnt int64
	var avg, std, nullPct sql.NullFloat64
	err = rows.Scan(&colName, &colType, &minVal, &maxVal, &approxUnique, &avg, &std, &q25Val, &q50Val, &q75Val, &cnt, &nullPct)
	require.NoError(t, err)
	require.Equal(t, "1", minVal.String)
	require.Equal(t, "100", maxVal.String)
	require.Equal(t, int64(100), cnt)
	require.InDelta(t, 50.5, avg.Float64, 0.01)
}
