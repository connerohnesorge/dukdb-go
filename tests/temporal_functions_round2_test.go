package tests

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTemporalFunctionsRound2(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("ISODOW Monday", func(t *testing.T) {
		var result float64
		// 2024-01-01 is a Monday
		err := db.QueryRow("SELECT DATE_PART('isodow', '2024-01-01 00:00:00'::TIMESTAMP)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 1.0, result)
	})

	t.Run("ISODOW Sunday", func(t *testing.T) {
		var result float64
		// 2024-01-07 is a Sunday
		err := db.QueryRow("SELECT DATE_PART('isodow', '2024-01-07 00:00:00'::TIMESTAMP)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 7.0, result)
	})

	t.Run("ISOYEAR", func(t *testing.T) {
		var result float64
		// 2024-12-30 is in ISO year 2025
		err := db.QueryRow("SELECT DATE_PART('isoyear', '2024-12-30 00:00:00'::TIMESTAMP)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 2025.0, result)
	})

	t.Run("DATEPART alias", func(t *testing.T) {
		var result float64
		err := db.QueryRow("SELECT DATEPART('year', '2024-06-15 00:00:00'::TIMESTAMP)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 2024.0, result)
	})

	t.Run("TIME_BUCKET hourly", func(t *testing.T) {
		var result time.Time
		err := db.QueryRow("SELECT TIME_BUCKET(INTERVAL '1 hour', '2024-01-01 14:37:00'::TIMESTAMP)").Scan(&result)
		require.NoError(t, err)
		utc := result.UTC()
		assert.Equal(t, 14, utc.Hour())
		assert.Equal(t, 0, utc.Minute())
	})

	t.Run("TIME_BUCKET 5 minutes", func(t *testing.T) {
		var result time.Time
		err := db.QueryRow("SELECT TIME_BUCKET(INTERVAL '5 minutes', '2024-01-01 14:37:22'::TIMESTAMP)").Scan(&result)
		require.NoError(t, err)
		utc := result.UTC()
		assert.Equal(t, 14, utc.Hour())
		assert.Equal(t, 35, utc.Minute())
	})

	t.Run("TIME_BUCKET NULL", func(t *testing.T) {
		var result sql.NullTime
		err := db.QueryRow("SELECT TIME_BUCKET(INTERVAL '1 hour', NULL)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	t.Run("MAKE_TIMESTAMPTZ basic", func(t *testing.T) {
		var result time.Time
		err := db.QueryRow("SELECT MAKE_TIMESTAMPTZ(2024, 1, 1, 12, 0, 0)").Scan(&result)
		require.NoError(t, err)
		utc := result.UTC()
		assert.Equal(t, 2024, utc.Year())
		assert.Equal(t, time.January, utc.Month())
		assert.Equal(t, 1, utc.Day())
		assert.Equal(t, 12, utc.Hour())
	})

	t.Run("TIMEZONE function", func(t *testing.T) {
		var result time.Time
		err := db.QueryRow("SELECT TIMEZONE('UTC', '2024-01-01 12:00:00'::TIMESTAMP)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 12, result.Hour())
	})

	t.Run("TIMEZONE NULL", func(t *testing.T) {
		var result sql.NullTime
		err := db.QueryRow("SELECT TIMEZONE('UTC', NULL)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	t.Run("EPOCH_NS", func(t *testing.T) {
		var result time.Time
		// 1704067200000000000 ns = 2024-01-01 00:00:00 UTC
		err := db.QueryRow("SELECT EPOCH_NS(1704067200000000000)").Scan(&result)
		require.NoError(t, err)
		utc := result.UTC()
		assert.Equal(t, 2024, utc.Year())
		assert.Equal(t, time.January, utc.Month())
		assert.Equal(t, 1, utc.Day())
	})

	t.Run("EPOCH_NS NULL", func(t *testing.T) {
		var result sql.NullTime
		err := db.QueryRow("SELECT EPOCH_NS(NULL)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})

	t.Run("nanosecond date part", func(t *testing.T) {
		var result float64
		err := db.QueryRow("SELECT DATE_PART('nanosecond', '2024-01-01 12:30:45'::TIMESTAMP)").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 0.0, result) // no fractional seconds
	})
}
