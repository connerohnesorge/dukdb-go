package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/require"
)

func TestBoolAndAggregate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ba (g TEXT, v BOOLEAN)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO ba VALUES ('a', true), ('a', true), ('b', true), ('b', false)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT g, BOOL_AND(v) FROM ba GROUP BY g ORDER BY g")
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		g string
		v bool
	}
	for rows.Next() {
		var g string
		var v bool
		require.NoError(t, rows.Scan(&g, &v))
		results = append(results, struct {
			g string
			v bool
		}{g, v})
	}
	require.Len(t, results, 2)
	require.True(t, results[0].v)  // a: all true
	require.False(t, results[1].v) // b: has false
}

func TestBoolOrAggregate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE bo (v BOOLEAN)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO bo VALUES (false), (false), (true)")
	require.NoError(t, err)

	var result bool
	err = db.QueryRow("SELECT BOOL_OR(v) FROM bo").Scan(&result)
	require.NoError(t, err)
	require.True(t, result)
}

func TestEveryAggregate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE ev (x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO ev VALUES (1), (2), (3)")
	require.NoError(t, err)

	var result bool
	err = db.QueryRow("SELECT EVERY(x > 0) FROM ev").Scan(&result)
	require.NoError(t, err)
	require.True(t, result)
}

func TestBitAndAggregate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE bta (x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO bta VALUES (7), (3)")
	require.NoError(t, err)

	var result int64
	err = db.QueryRow("SELECT BIT_AND(x) FROM bta").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, int64(3), result) // 111 & 011 = 011
}

func TestBitOrAggregate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE bto (x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO bto VALUES (1), (2)")
	require.NoError(t, err)

	var result int64
	err = db.QueryRow("SELECT BIT_OR(x) FROM bto").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, int64(3), result) // 01 | 10 = 11
}

func TestBitXorAggregate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE btx (x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO btx VALUES (5), (3)")
	require.NoError(t, err)

	var result int64
	err = db.QueryRow("SELECT BIT_XOR(x) FROM btx").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, int64(6), result) // 101 ^ 011 = 110
}

func TestArbitraryAlias(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE arb (x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO arb VALUES (42)")
	require.NoError(t, err)

	var result int
	err = db.QueryRow("SELECT ARBITRARY(x) FROM arb").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 42, result)
}

func TestMeanAlias(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE mn (x INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO mn VALUES (10), (20)")
	require.NoError(t, err)

	var result float64
	err = db.QueryRow("SELECT MEAN(x) FROM mn").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 15.0, result)
}

func TestGeometricMean(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE gm (x DOUBLE)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO gm VALUES (2.0), (8.0)")
	require.NoError(t, err)

	var result float64
	err = db.QueryRow("SELECT GEOMETRIC_MEAN(x) FROM gm").Scan(&result)
	require.NoError(t, err)
	require.InDelta(t, 4.0, result, 0.0001)
}

func TestGeomeanAlias(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE gm2 (x DOUBLE)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO gm2 VALUES (4.0), (9.0)")
	require.NoError(t, err)

	var result float64
	err = db.QueryRow("SELECT GEOMEAN(x) FROM gm2").Scan(&result)
	require.NoError(t, err)
	require.InDelta(t, 6.0, result, 0.0001)
}

func TestGeometricMeanNonPositive(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE gm3 (x DOUBLE)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO gm3 VALUES (2.0), (-1.0)")
	require.NoError(t, err)

	var result sql.NullFloat64
	err = db.QueryRow("SELECT GEOMETRIC_MEAN(x) FROM gm3").Scan(&result)
	require.NoError(t, err)
	require.False(t, result.Valid) // NULL for non-positive values
}

func TestWeightedAvg(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE wa (score INTEGER, weight INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO wa VALUES (90, 3), (80, 1)")
	require.NoError(t, err)

	var result float64
	err = db.QueryRow("SELECT WEIGHTED_AVG(score, weight) FROM wa").Scan(&result)
	require.NoError(t, err)
	require.InDelta(t, 87.5, result, 0.0001)
}

func TestRegrCountInGroupBy(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE rc (g TEXT, x DOUBLE, y DOUBLE)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO rc VALUES ('a', 1, 2), ('a', 3, 4), ('b', 5, 6)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT g, REGR_COUNT(y, x) FROM rc GROUP BY g ORDER BY g")
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		g string
		c float64
	}
	for rows.Next() {
		var g string
		var c float64
		require.NoError(t, rows.Scan(&g, &c))
		results = append(results, struct {
			g string
			c float64
		}{g, c})
	}
	require.Len(t, results, 2)
	require.Equal(t, 2.0, results[0].c) // a: 2 pairs
	require.Equal(t, 1.0, results[1].c) // b: 1 pair
}
