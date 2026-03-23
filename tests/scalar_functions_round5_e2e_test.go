package tests

import (
	"database/sql"
	"math"
	"regexp"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/require"
)

func TestEConstant(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result float64
	err = db.QueryRow("SELECT E()").Scan(&result)
	require.NoError(t, err)
	require.InDelta(t, math.E, result, 0.0001)
}

func TestInfConstant(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result float64
	err = db.QueryRow("SELECT INF()").Scan(&result)
	require.NoError(t, err)
	require.True(t, math.IsInf(result, 1))
}

func TestNanConstant(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result float64
	err = db.QueryRow("SELECT NAN()").Scan(&result)
	require.NoError(t, err)
	require.True(t, math.IsNaN(result))
}

func TestUUID(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT UUID()").Scan(&result)
	require.NoError(t, err)
	// UUID format: 8-4-4-4-12 hex chars
	matched, _ := regexp.MatchString(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`, result)
	require.True(t, matched, "UUID format invalid: %s", result)
}

func TestUUIDUniqueness(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var u1, u2 string
	err = db.QueryRow("SELECT UUID()").Scan(&u1)
	require.NoError(t, err)
	err = db.QueryRow("SELECT UUID()").Scan(&u2)
	require.NoError(t, err)
	require.NotEqual(t, u1, u2)
}

func TestGenRandomUUID(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT GEN_RANDOM_UUID()").Scan(&result)
	require.NoError(t, err)
	require.Len(t, result, 36)
}

func TestSplitPart(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT SPLIT_PART('a-b-c', '-', 2)").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, "b", result)
}

func TestSplitPartNegativeIndex(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT SPLIT_PART('a-b-c', '-', -1)").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, "c", result)
}

func TestSplitPartOutOfRange(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT SPLIT_PART('a-b', '-', 5)").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, "", result)
}

func TestLogTwoArgs(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result float64
	err = db.QueryRow("SELECT LOG(8, 2)").Scan(&result)
	require.NoError(t, err)
	require.InDelta(t, 3.0, result, 0.0001)
}

func TestLogOneArgStillWorks(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result float64
	err = db.QueryRow("SELECT LOG(100)").Scan(&result)
	require.NoError(t, err)
	require.InDelta(t, 2.0, result, 0.0001)
}

func TestSHA512(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT SHA512('hello')").Scan(&result)
	require.NoError(t, err)
	require.Len(t, result, 128) // 512 bits = 64 bytes = 128 hex chars
}

func TestMillisecond(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result int
	err = db.QueryRow("SELECT MILLISECOND(MAKE_TIMESTAMP(2024, 1, 1, 12, 34, 56.789))").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 789, result)
}

func TestMicrosecond(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result int
	err = db.QueryRow("SELECT MICROSECOND(MAKE_TIMESTAMP(2024, 1, 1, 12, 34, 56.5))").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 500000, result)
}

func TestSplitPartNull(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var result sql.NullString
	err = db.QueryRow("SELECT SPLIT_PART(NULL, '-', 1)").Scan(&result)
	require.NoError(t, err)
	require.False(t, result.Valid)
}
