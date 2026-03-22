package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNumericFunctionsRound2(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("SIGNBIT negative", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT SIGNBIT(-1.0)").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("SIGNBIT positive", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT SIGNBIT(1.0)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("SIGNBIT zero", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT SIGNBIT(0.0)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("SIGNBIT NULL", func(t *testing.T) {
		var result sql.NullBool
		err := db.QueryRow("SELECT SIGNBIT(NULL)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	t.Run("WIDTH_BUCKET middle", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT WIDTH_BUCKET(5.0, 0.0, 10.0, 5)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(3), result)
	})

	t.Run("WIDTH_BUCKET below range", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT WIDTH_BUCKET(-1.0, 0.0, 10.0, 5)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(0), result)
	})

	t.Run("WIDTH_BUCKET above range", func(t *testing.T) {
		var result int64
		err := db.QueryRow("SELECT WIDTH_BUCKET(10.0, 0.0, 10.0, 5)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(6), result)
	})

	t.Run("BETA 1 1", func(t *testing.T) {
		var result float64
		err := db.QueryRow("SELECT BETA(1, 1)").Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, result, 1e-10)
	})

	t.Run("BETA 2 3", func(t *testing.T) {
		var result float64
		err := db.QueryRow("SELECT BETA(2, 3)").Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 1.0/12.0, result, 1e-10)
	})

	t.Run("BETA NULL", func(t *testing.T) {
		var result sql.NullFloat64
		err := db.QueryRow("SELECT BETA(NULL, 1)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})
}

func TestConditionalAggregates(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Setup test data
	_, err = db.Exec("CREATE TABLE sales(amount DOUBLE, category VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO sales VALUES (10.0, 'A'), (20.0, 'B'), (30.0, 'A'), (40.0, 'B'), (NULL, 'A')")
	require.NoError(t, err)

	t.Run("SUM_IF", func(t *testing.T) {
		var result float64
		err := db.QueryRow("SELECT SUM_IF(amount, category = 'A') FROM sales").Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 40.0, result, 1e-10) // 10 + 30
	})

	t.Run("AVG_IF", func(t *testing.T) {
		var result float64
		err := db.QueryRow("SELECT AVG_IF(amount, category = 'B') FROM sales").Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 30.0, result, 1e-10) // (20 + 40) / 2
	})

	t.Run("MIN_IF", func(t *testing.T) {
		var result float64
		err := db.QueryRow("SELECT MIN_IF(amount, category = 'A') FROM sales").Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 10.0, result, 1e-10)
	})

	t.Run("MAX_IF", func(t *testing.T) {
		var result float64
		err := db.QueryRow("SELECT MAX_IF(amount, category = 'B') FROM sales").Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 40.0, result, 1e-10)
	})

	t.Run("SUM_IF no match returns NULL", func(t *testing.T) {
		var result sql.NullFloat64
		err := db.QueryRow("SELECT SUM_IF(amount, category = 'C') FROM sales").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	t.Run("AVG_IF no match returns NULL", func(t *testing.T) {
		var result sql.NullFloat64
		err := db.QueryRow("SELECT AVG_IF(amount, category = 'C') FROM sales").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})
}
