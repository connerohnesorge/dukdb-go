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

// TestGenerateSeriesIntegerAscending verifies that generate_series(1, 5) returns
// [1, 2, 3, 4, 5] (inclusive of both endpoints with default step 1).
func TestGenerateSeriesIntegerAscending(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM generate_series(1, 5)")
	require.NoError(t, err)
	defer rows.Close()

	var values []int64
	for rows.Next() {
		var v int64
		err := rows.Scan(&v)
		require.NoError(t, err)
		values = append(values, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, values)
}

// TestGenerateSeriesIntegerWithStep verifies that generate_series(0, 10, 3)
// returns [0, 3, 6, 9] (inclusive, step=3, stops because 12 > 10).
func TestGenerateSeriesIntegerWithStep(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM generate_series(0, 10, 3)")
	require.NoError(t, err)
	defer rows.Close()

	var values []int64
	for rows.Next() {
		var v int64
		err := rows.Scan(&v)
		require.NoError(t, err)
		values = append(values, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{0, 3, 6, 9}, values)
}

// TestGenerateSeriesDescending verifies that generate_series(5, 1, -1)
// returns [5, 4, 3, 2, 1] (descending with negative step).
func TestGenerateSeriesDescending(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM generate_series(5, 1, -1)")
	require.NoError(t, err)
	defer rows.Close()

	var values []int64
	for rows.Next() {
		var v int64
		err := rows.Scan(&v)
		require.NoError(t, err)
		values = append(values, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{5, 4, 3, 2, 1}, values)
}

// TestGenerateSeriesDateWithInterval verifies that generate_series with DATE
// arguments and an INTERVAL step produces the expected date range.
func TestGenerateSeriesDateWithInterval(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM generate_series('2024-01-01'::DATE, '2024-01-05'::DATE, INTERVAL '1 day')")
	require.NoError(t, err)
	defer rows.Close()

	var values []any
	for rows.Next() {
		var v any
		err := rows.Scan(&v)
		require.NoError(t, err)
		values = append(values, v)
	}
	require.NoError(t, rows.Err())
	require.Len(t, values, 5, "expected 5 dates from Jan 1 to Jan 5 inclusive")

	// Dates are stored as int32 (days since Unix epoch 1970-01-01).
	// Verify by converting the first and last values.
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	firstDays, ok := values[0].(int32)
	require.True(t, ok, "expected int32 for date, got %T", values[0])
	firstDate := epoch.AddDate(0, 0, int(firstDays))
	assert.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), firstDate)

	lastDays := values[4].(int32)
	lastDate := epoch.AddDate(0, 0, int(lastDays))
	assert.Equal(t, time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC), lastDate)
}

// TestGenerateSeriesTimestampWithInterval verifies that generate_series with
// TIMESTAMP arguments and an INTERVAL step produces the expected timestamp range.
func TestGenerateSeriesTimestampWithInterval(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM generate_series('2024-01-01 00:00:00'::TIMESTAMP, '2024-01-01 03:00:00'::TIMESTAMP, INTERVAL '1 hour')")
	require.NoError(t, err)
	defer rows.Close()

	var values []any
	for rows.Next() {
		var v any
		err := rows.Scan(&v)
		require.NoError(t, err)
		values = append(values, v)
	}
	require.NoError(t, rows.Err())
	require.Len(t, values, 4, "expected 4 timestamps: 00:00, 01:00, 02:00, 03:00")

	// Timestamps are stored as int64 (microseconds since Unix epoch).
	firstMicros, ok := values[0].(int64)
	require.True(t, ok, "expected int64 for timestamp, got %T", values[0])
	firstTime := time.Unix(firstMicros/1_000_000, (firstMicros%1_000_000)*1000).UTC()
	assert.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), firstTime)

	lastMicros := values[3].(int64)
	lastTime := time.Unix(lastMicros/1_000_000, (lastMicros%1_000_000)*1000).UTC()
	assert.Equal(t, time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC), lastTime)
}

// TestRangeIntegerExclusive verifies that range(1, 5) returns [1, 2, 3, 4]
// (exclusive of the stop value).
func TestRangeIntegerExclusive(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM range(1, 5)")
	require.NoError(t, err)
	defer rows.Close()

	var values []int64
	for rows.Next() {
		var v int64
		err := rows.Scan(&v)
		require.NoError(t, err)
		values = append(values, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{1, 2, 3, 4}, values)
}

// TestRangeDateExclusive verifies that range with DATE arguments excludes the
// stop date.
func TestRangeDateExclusive(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM range('2024-01-01'::DATE, '2024-01-04'::DATE, INTERVAL '1 day')")
	require.NoError(t, err)
	defer rows.Close()

	var values []any
	for rows.Next() {
		var v any
		err := rows.Scan(&v)
		require.NoError(t, err)
		values = append(values, v)
	}
	require.NoError(t, rows.Err())
	require.Len(t, values, 3, "expected 3 dates (Jan 1, 2, 3) excluding Jan 4")

	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	lastDays, ok := values[2].(int32)
	require.True(t, ok, "expected int32 for date, got %T", values[2])
	lastDate := epoch.AddDate(0, 0, int(lastDays))
	assert.Equal(t, time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC), lastDate)
}

// TestGenerateSeriesZeroStepError verifies that a zero step size produces an error.
func TestGenerateSeriesZeroStepError(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM generate_series(1, 5, 0)")
	if err != nil {
		// Error returned at query time - this is acceptable.
		assert.Contains(t, err.Error(), "zero")
		return
	}
	defer rows.Close()

	// If no error at query time, iterating should produce an error.
	for rows.Next() {
		var v int64
		_ = rows.Scan(&v)
	}
	err = rows.Err()
	if err != nil {
		assert.Contains(t, err.Error(), "zero")
	} else {
		t.Fatal("expected an error for zero step size, but got none")
	}
}

// TestGenerateSeriesDirectionMismatch verifies that a positive step with
// start > stop produces an empty result set.
func TestGenerateSeriesDirectionMismatch(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM generate_series(5, 1, 1)")
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 0, count, "direction mismatch should produce empty result")
}

// TestGenerateSeriesStartEqualsStopInclusive verifies that generate_series(3, 3)
// returns a single row with value 3 (start equals stop, inclusive).
func TestGenerateSeriesStartEqualsStopInclusive(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM generate_series(3, 3)")
	require.NoError(t, err)
	defer rows.Close()

	var values []int64
	for rows.Next() {
		var v int64
		err := rows.Scan(&v)
		require.NoError(t, err)
		values = append(values, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{3}, values)
}

// TestRangeStartEqualsStopEmpty verifies that range(3, 3) returns an empty
// result set (start equals stop, exclusive).
func TestRangeStartEqualsStopEmpty(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM range(3, 3)")
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 0, count, "range(3, 3) should produce empty result")
}

// TestGenerateSeriesColumnAlias verifies that the result column can be aliased
// using a SELECT-level alias.
func TestGenerateSeriesColumnAlias(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT generate_series AS n FROM generate_series(1, 3)")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, []string{"n"}, cols, "column should be aliased as 'n'")

	var values []int64
	for rows.Next() {
		var v int64
		err := rows.Scan(&v)
		require.NoError(t, err)
		values = append(values, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{1, 2, 3}, values)
}

// TestGenerateSeriesLargeBatched verifies that generate_series correctly handles
// a large series that crosses the internal DataChunk boundary of 2048 rows.
func TestGenerateSeriesLargeBatched(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	row := db.QueryRow("SELECT COUNT(*) FROM generate_series(1, 5000)")
	var count int64
	err = row.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(5000), count)
}
