package executor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Phase 8: Integration and Compatibility Testing
// This file contains comprehensive integration tests for all temporal functions.
// =============================================================================

// =============================================================================
// Task 8.1: Test all extraction functions with DATE input
// =============================================================================

func TestIntegration_ExtractionFunctions_DateInput(t *testing.T) {
	// Test date: 2024-06-15 (a Saturday)
	// Days since epoch: 19889
	dateValue := int32(19889)

	testCases := []struct {
		name     string
		fn       func([]any) (any, error)
		expected any
	}{
		{"YEAR", evalYear, int32(2024)},
		{"MONTH", evalMonth, int32(6)},
		{"DAY", evalDay, int32(15)},
		{"HOUR", evalHour, int32(0)},          // DATE has no time component
		{"MINUTE", evalMinute, int32(0)},      // DATE has no time component
		{"DAYOFWEEK", evalDayOfWeek, int32(6)}, // Saturday = 6
		{"DAYOFYEAR", evalDayOfYear, int32(167)},
		{"QUARTER", evalQuarter, int32(2)}, // June is Q2
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.fn([]any{dateValue})
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}

	// SECOND returns float64
	t.Run("SECOND", func(t *testing.T) {
		result, err := evalSecond([]any{dateValue})
		require.NoError(t, err)
		assert.InDelta(t, float64(0), result.(float64), 0.0001)
	})

	// WEEK returns ISO week number
	t.Run("WEEK", func(t *testing.T) {
		result, err := evalWeek([]any{dateValue})
		require.NoError(t, err)
		// 2024-06-15 is in ISO week 24
		assert.Equal(t, int32(24), result)
	})
}

// =============================================================================
// Task 8.2: Test all extraction functions with TIMESTAMP input
// =============================================================================

func TestIntegration_ExtractionFunctions_TimestampInput(t *testing.T) {
	// Test timestamp: 2024-06-15 14:30:45.123456 UTC
	// Use time.Date to get correct microseconds since epoch
	testTime := time.Date(2024, time.June, 15, 14, 30, 45, 123456000, time.UTC)
	timestampValue := testTime.UnixMicro()

	testCases := []struct {
		name     string
		fn       func([]any) (any, error)
		expected any
	}{
		{"YEAR", evalYear, int32(2024)},
		{"MONTH", evalMonth, int32(6)},
		{"DAY", evalDay, int32(15)},
		{"HOUR", evalHour, int32(14)},
		{"MINUTE", evalMinute, int32(30)},
		{"DAYOFWEEK", evalDayOfWeek, int32(6)}, // Saturday = 6
		{"DAYOFYEAR", evalDayOfYear, int32(167)},
		{"QUARTER", evalQuarter, int32(2)}, // June is Q2
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.fn([]any{timestampValue})
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}

	// SECOND returns float64 with fractional seconds
	t.Run("SECOND", func(t *testing.T) {
		result, err := evalSecond([]any{timestampValue})
		require.NoError(t, err)
		assert.InDelta(t, 45.123456, result.(float64), 0.0001)
	})

	// WEEK returns ISO week number
	t.Run("WEEK", func(t *testing.T) {
		result, err := evalWeek([]any{timestampValue})
		require.NoError(t, err)
		assert.Equal(t, int32(24), result)
	})
}

// =============================================================================
// Task 8.3: Test NULL propagation for all functions
// =============================================================================

func TestIntegration_NullPropagation(t *testing.T) {
	// Single-argument extraction functions
	singleArgFuncs := []struct {
		name string
		fn   func([]any) (any, error)
	}{
		{"YEAR", evalYear},
		{"MONTH", evalMonth},
		{"DAY", evalDay},
		{"HOUR", evalHour},
		{"MINUTE", evalMinute},
		{"SECOND", evalSecond},
		{"DAYOFWEEK", evalDayOfWeek},
		{"DAYOFYEAR", evalDayOfYear},
		{"WEEK", evalWeek},
		{"QUARTER", evalQuarter},
		{"LAST_DAY", evalLastDay},
		{"EPOCH", evalEpoch},
		{"EPOCH_MS", evalEpochMs},
		{"TO_TIMESTAMP", evalToTimestamp},
		{"TO_YEARS", evalToYears},
		{"TO_MONTHS", evalToMonths},
		{"TO_DAYS", evalToDays},
		{"TO_HOURS", evalToHours},
		{"TO_MINUTES", evalToMinutes},
		{"TO_SECONDS", evalToSeconds},
		{"TOTAL_YEARS", evalTotalYears},
		{"TOTAL_MONTHS", evalTotalMonths},
		{"TOTAL_DAYS", evalTotalDays},
		{"TOTAL_HOURS", evalTotalHours},
		{"TOTAL_MINUTES", evalTotalMinutes},
		{"TOTAL_SECONDS", evalTotalSeconds},
	}

	for _, tc := range singleArgFuncs {
		t.Run(tc.name+" NULL propagation", func(t *testing.T) {
			result, err := tc.fn([]any{nil})
			require.NoError(t, err)
			assert.Nil(t, result)
		})
	}

	// Two-argument functions
	t.Run("DATE_ADD NULL propagation - first arg", func(t *testing.T) {
		result, err := evalDateAdd([]any{nil, Interval{Days: 1}})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("DATE_ADD NULL propagation - second arg", func(t *testing.T) {
		result, err := evalDateAdd([]any{int32(19889), nil})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("DATE_SUB NULL propagation - first arg", func(t *testing.T) {
		result, err := evalDateSub([]any{nil, Interval{Days: 1}})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("DATE_SUB NULL propagation - second arg", func(t *testing.T) {
		result, err := evalDateSub([]any{int32(19889), nil})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("DATE_TRUNC NULL propagation - first arg", func(t *testing.T) {
		result, err := evalDateTrunc([]any{nil, int64(1718458245123456)})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("DATE_TRUNC NULL propagation - second arg", func(t *testing.T) {
		result, err := evalDateTrunc([]any{"day", nil})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("DATE_PART NULL propagation - first arg", func(t *testing.T) {
		result, err := evalDatePart([]any{nil, int64(1718458245123456)})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("DATE_PART NULL propagation - second arg", func(t *testing.T) {
		result, err := evalDatePart([]any{"year", nil})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("STRFTIME NULL propagation - first arg", func(t *testing.T) {
		result, err := evalStrftime([]any{nil, int64(1718458245123456)})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("STRFTIME NULL propagation - second arg", func(t *testing.T) {
		result, err := evalStrftime([]any{"%Y-%m-%d", nil})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("STRPTIME NULL propagation - first arg", func(t *testing.T) {
		result, err := evalStrptime([]any{nil, "%Y-%m-%d"})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("STRPTIME NULL propagation - second arg", func(t *testing.T) {
		result, err := evalStrptime([]any{"2024-06-15", nil})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	// Three-argument functions
	t.Run("DATE_DIFF NULL propagation - first arg", func(t *testing.T) {
		result, err := evalDateDiff([]any{nil, int32(19889), int32(19890)})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("DATE_DIFF NULL propagation - second arg", func(t *testing.T) {
		result, err := evalDateDiff([]any{"day", nil, int32(19890)})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("DATE_DIFF NULL propagation - third arg", func(t *testing.T) {
		result, err := evalDateDiff([]any{"day", int32(19889), nil})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	// MAKE_DATE NULL propagation
	t.Run("MAKE_DATE NULL propagation - year", func(t *testing.T) {
		result, err := evalMakeDate([]any{nil, int32(6), int32(15)})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("MAKE_DATE NULL propagation - month", func(t *testing.T) {
		result, err := evalMakeDate([]any{int32(2024), nil, int32(15)})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("MAKE_DATE NULL propagation - day", func(t *testing.T) {
		result, err := evalMakeDate([]any{int32(2024), int32(6), nil})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	// MAKE_TIME NULL propagation
	t.Run("MAKE_TIME NULL propagation - hour", func(t *testing.T) {
		result, err := evalMakeTime([]any{nil, int32(30), float64(45)})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("MAKE_TIME NULL propagation - minute", func(t *testing.T) {
		result, err := evalMakeTime([]any{int32(14), nil, float64(45)})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("MAKE_TIME NULL propagation - second", func(t *testing.T) {
		result, err := evalMakeTime([]any{int32(14), int32(30), nil})
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

// =============================================================================
// Task 8.5: Test DATE_ADD/SUB with various intervals
// =============================================================================

func TestIntegration_DateAddSub_VariousIntervals(t *testing.T) {
	// Base date: 2024-06-15 (days since epoch: 19889)
	baseDate := int32(19889)

	// Base timestamp: 2024-06-15 14:30:45 UTC
	baseTime := time.Date(2024, time.June, 15, 14, 30, 45, 0, time.UTC)
	baseTimestamp := baseTime.UnixMicro()

	t.Run("DATE_ADD days to DATE", func(t *testing.T) {
		result, err := evalDateAdd([]any{baseDate, Interval{Days: 10}})
		require.NoError(t, err)
		// 2024-06-15 + 10 days = 2024-06-25 (19899 days since epoch)
		assert.Equal(t, int32(19899), result)
	})

	t.Run("DATE_ADD months to DATE", func(t *testing.T) {
		result, err := evalDateAdd([]any{baseDate, Interval{Months: 2}})
		require.NoError(t, err)
		// 2024-06-15 + 2 months = 2024-08-15
		resultDate := dateToTime(result.(int32))
		assert.Equal(t, 2024, resultDate.Year())
		assert.Equal(t, time.August, resultDate.Month())
		assert.Equal(t, 15, resultDate.Day())
	})

	t.Run("DATE_ADD years to DATE", func(t *testing.T) {
		result, err := evalDateAdd([]any{baseDate, Interval{Months: 12}})
		require.NoError(t, err)
		// 2024-06-15 + 1 year = 2025-06-15
		resultDate := dateToTime(result.(int32))
		assert.Equal(t, 2025, resultDate.Year())
		assert.Equal(t, time.June, resultDate.Month())
		assert.Equal(t, 15, resultDate.Day())
	})

	t.Run("DATE_ADD hours to TIMESTAMP", func(t *testing.T) {
		result, err := evalDateAdd([]any{baseTimestamp, Interval{Micros: 3 * 3600 * 1_000_000}})
		require.NoError(t, err)
		// 14:30:45 + 3 hours = 17:30:45
		resultTime := timestampToTime(result.(int64))
		assert.Equal(t, 17, resultTime.Hour())
		assert.Equal(t, 30, resultTime.Minute())
	})

	t.Run("DATE_SUB days from DATE", func(t *testing.T) {
		result, err := evalDateSub([]any{baseDate, Interval{Days: 15}})
		require.NoError(t, err)
		// 2024-06-15 - 15 days = 2024-05-31
		resultDate := dateToTime(result.(int32))
		assert.Equal(t, 2024, resultDate.Year())
		assert.Equal(t, time.May, resultDate.Month())
		assert.Equal(t, 31, resultDate.Day())
	})

	t.Run("DATE_SUB months from DATE", func(t *testing.T) {
		result, err := evalDateSub([]any{baseDate, Interval{Months: 6}})
		require.NoError(t, err)
		// 2024-06-15 - 6 months = 2023-12-15
		resultDate := dateToTime(result.(int32))
		assert.Equal(t, 2023, resultDate.Year())
		assert.Equal(t, time.December, resultDate.Month())
		assert.Equal(t, 15, resultDate.Day())
	})

	t.Run("DATE_ADD complex interval", func(t *testing.T) {
		// Add 1 month, 5 days, 2 hours
		interval := Interval{
			Months: 1,
			Days:   5,
			Micros: 2 * 3600 * 1_000_000,
		}
		result, err := evalDateAdd([]any{baseTimestamp, interval})
		require.NoError(t, err)
		resultTime := timestampToTime(result.(int64))
		assert.Equal(t, 2024, resultTime.Year())
		assert.Equal(t, time.July, resultTime.Month())
		assert.Equal(t, 20, resultTime.Day())
		assert.Equal(t, 16, resultTime.Hour())
	})
}

// =============================================================================
// Task 8.6: Test DATE_DIFF with various parts
// =============================================================================

func TestIntegration_DateDiff_VariousParts(t *testing.T) {
	// Date1: 2024-01-01
	date1 := int32(19723) // Days since epoch for 2024-01-01
	// Date2: 2024-06-15
	date2 := int32(19889) // Days since epoch for 2024-06-15

	// Timestamp1: 2024-01-01 00:00:00
	time1 := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	ts1 := time1.UnixMicro()
	// Timestamp2: 2024-06-15 14:30:45
	time2 := time.Date(2024, time.June, 15, 14, 30, 45, 0, time.UTC)
	ts2 := time2.UnixMicro()

	t.Run("DATE_DIFF day", func(t *testing.T) {
		result, err := evalDateDiff([]any{"day", date1, date2})
		require.NoError(t, err)
		assert.Equal(t, int64(166), result)
	})

	t.Run("DATE_DIFF month", func(t *testing.T) {
		result, err := evalDateDiff([]any{"month", date1, date2})
		require.NoError(t, err)
		assert.Equal(t, int64(5), result) // Jan to Jun = 5 months
	})

	t.Run("DATE_DIFF year", func(t *testing.T) {
		result, err := evalDateDiff([]any{"year", date1, date2})
		require.NoError(t, err)
		assert.Equal(t, int64(0), result) // Same year
	})

	t.Run("DATE_DIFF year across years", func(t *testing.T) {
		// 2023-06-15 to 2024-06-15
		date2023 := int32(19523) // Approximate days since epoch for 2023-06-15
		result, err := evalDateDiff([]any{"year", date2023, date2})
		require.NoError(t, err)
		assert.Equal(t, int64(1), result)
	})

	t.Run("DATE_DIFF hour", func(t *testing.T) {
		result, err := evalDateDiff([]any{"hour", ts1, ts2})
		require.NoError(t, err)
		// Difference should be approximately 4006 hours (166 days * 24 + 14.5 hours)
		assert.Greater(t, result.(int64), int64(3990))
	})

	t.Run("DATE_DIFF minute", func(t *testing.T) {
		result, err := evalDateDiff([]any{"minute", ts1, ts2})
		require.NoError(t, err)
		assert.Greater(t, result.(int64), int64(239000))
	})

	t.Run("DATE_DIFF second", func(t *testing.T) {
		result, err := evalDateDiff([]any{"second", ts1, ts2})
		require.NoError(t, err)
		assert.Greater(t, result.(int64), int64(14000000))
	})

	t.Run("DATE_DIFF quarter", func(t *testing.T) {
		result, err := evalDateDiff([]any{"quarter", date1, date2})
		require.NoError(t, err)
		assert.Equal(t, int64(1), result) // Q1 to Q2
	})

	t.Run("DATE_DIFF week", func(t *testing.T) {
		result, err := evalDateDiff([]any{"week", date1, date2})
		require.NoError(t, err)
		assert.Equal(t, int64(23), result) // Approximately 23-24 weeks
	})

	t.Run("DATE_DIFF negative", func(t *testing.T) {
		result, err := evalDateDiff([]any{"day", date2, date1})
		require.NoError(t, err)
		assert.Equal(t, int64(-166), result)
	})
}

// =============================================================================
// Task 8.7: Test DATE_TRUNC at various granularities
// =============================================================================

func TestIntegration_DateTrunc_Granularities(t *testing.T) {
	// Timestamp: 2024-06-15 14:30:45.123456 UTC
	testTime := time.Date(2024, time.June, 15, 14, 30, 45, 123456000, time.UTC)
	timestamp := testTime.UnixMicro()

	t.Run("DATE_TRUNC year", func(t *testing.T) {
		result, err := evalDateTrunc([]any{"year", timestamp})
		require.NoError(t, err)
		resultTime := timestampToTime(result.(int64))
		assert.Equal(t, 2024, resultTime.Year())
		assert.Equal(t, time.January, resultTime.Month())
		assert.Equal(t, 1, resultTime.Day())
		assert.Equal(t, 0, resultTime.Hour())
	})

	t.Run("DATE_TRUNC quarter", func(t *testing.T) {
		result, err := evalDateTrunc([]any{"quarter", timestamp})
		require.NoError(t, err)
		resultTime := timestampToTime(result.(int64))
		assert.Equal(t, 2024, resultTime.Year())
		assert.Equal(t, time.April, resultTime.Month()) // Q2 starts in April
		assert.Equal(t, 1, resultTime.Day())
	})

	t.Run("DATE_TRUNC month", func(t *testing.T) {
		result, err := evalDateTrunc([]any{"month", timestamp})
		require.NoError(t, err)
		resultTime := timestampToTime(result.(int64))
		assert.Equal(t, 2024, resultTime.Year())
		assert.Equal(t, time.June, resultTime.Month())
		assert.Equal(t, 1, resultTime.Day())
		assert.Equal(t, 0, resultTime.Hour())
	})

	t.Run("DATE_TRUNC week", func(t *testing.T) {
		result, err := evalDateTrunc([]any{"week", timestamp})
		require.NoError(t, err)
		resultTime := timestampToTime(result.(int64))
		// Week starts on Sunday
		assert.Equal(t, time.Sunday, resultTime.Weekday())
		assert.Equal(t, 0, resultTime.Hour())
	})

	t.Run("DATE_TRUNC day", func(t *testing.T) {
		result, err := evalDateTrunc([]any{"day", timestamp})
		require.NoError(t, err)
		resultTime := timestampToTime(result.(int64))
		assert.Equal(t, 2024, resultTime.Year())
		assert.Equal(t, time.June, resultTime.Month())
		assert.Equal(t, 15, resultTime.Day())
		assert.Equal(t, 0, resultTime.Hour())
		assert.Equal(t, 0, resultTime.Minute())
		assert.Equal(t, 0, resultTime.Second())
	})

	t.Run("DATE_TRUNC hour", func(t *testing.T) {
		result, err := evalDateTrunc([]any{"hour", timestamp})
		require.NoError(t, err)
		resultTime := timestampToTime(result.(int64))
		assert.Equal(t, 14, resultTime.Hour())
		assert.Equal(t, 0, resultTime.Minute())
		assert.Equal(t, 0, resultTime.Second())
	})

	t.Run("DATE_TRUNC minute", func(t *testing.T) {
		result, err := evalDateTrunc([]any{"minute", timestamp})
		require.NoError(t, err)
		resultTime := timestampToTime(result.(int64))
		assert.Equal(t, 14, resultTime.Hour())
		assert.Equal(t, 30, resultTime.Minute())
		assert.Equal(t, 0, resultTime.Second())
	})

	t.Run("DATE_TRUNC second", func(t *testing.T) {
		result, err := evalDateTrunc([]any{"second", timestamp})
		require.NoError(t, err)
		resultTime := timestampToTime(result.(int64))
		assert.Equal(t, 14, resultTime.Hour())
		assert.Equal(t, 30, resultTime.Minute())
		assert.Equal(t, 45, resultTime.Second())
		assert.Equal(t, 0, resultTime.Nanosecond())
	})

	t.Run("DATE_TRUNC millisecond", func(t *testing.T) {
		result, err := evalDateTrunc([]any{"millisecond", timestamp})
		require.NoError(t, err)
		resultTime := timestampToTime(result.(int64))
		// Should have microseconds truncated to milliseconds
		assert.Equal(t, 123000000, resultTime.Nanosecond())
	})
}

// =============================================================================
// Task 8.8: Test edge cases (leap years, month boundaries)
// =============================================================================

func TestIntegration_EdgeCases(t *testing.T) {
	t.Run("Leap year Feb 29", func(t *testing.T) {
		// 2024 is a leap year
		result, err := evalMakeDate([]any{int32(2024), int32(2), int32(29)})
		require.NoError(t, err)
		resultDate := dateToTime(result.(int32))
		assert.Equal(t, 2024, resultDate.Year())
		assert.Equal(t, time.February, resultDate.Month())
		assert.Equal(t, 29, resultDate.Day())
	})

	t.Run("Non-leap year Feb 29 should fail", func(t *testing.T) {
		// 2023 is not a leap year
		_, err := evalMakeDate([]any{int32(2023), int32(2), int32(29)})
		require.Error(t, err)
	})

	t.Run("Month boundary - Jan 31 + 1 month", func(t *testing.T) {
		// Jan 31 + 1 month should clamp to Feb 28/29
		jan31 := time.Date(2024, time.January, 31, 0, 0, 0, 0, time.UTC)
		result := addMonths(jan31, 1)
		assert.Equal(t, time.February, result.Month())
		assert.Equal(t, 29, result.Day()) // 2024 is a leap year
	})

	t.Run("Month boundary - Jan 31 + 1 month non-leap", func(t *testing.T) {
		// Jan 31 + 1 month should clamp to Feb 28 in non-leap year
		jan31 := time.Date(2023, time.January, 31, 0, 0, 0, 0, time.UTC)
		result := addMonths(jan31, 1)
		assert.Equal(t, time.February, result.Month())
		assert.Equal(t, 28, result.Day())
	})

	t.Run("Month boundary - Mar 31 + 1 month", func(t *testing.T) {
		// Mar 31 + 1 month should clamp to Apr 30
		mar31 := time.Date(2024, time.March, 31, 0, 0, 0, 0, time.UTC)
		result := addMonths(mar31, 1)
		assert.Equal(t, time.April, result.Month())
		assert.Equal(t, 30, result.Day())
	})

	t.Run("Year boundary", func(t *testing.T) {
		// Dec 31 + 1 day = Jan 1 next year
		dec31 := int32(19722) // 2023-12-31
		result, err := evalDateAdd([]any{dec31, Interval{Days: 1}})
		require.NoError(t, err)
		resultDate := dateToTime(result.(int32))
		assert.Equal(t, 2024, resultDate.Year())
		assert.Equal(t, time.January, resultDate.Month())
		assert.Equal(t, 1, resultDate.Day())
	})

	t.Run("DAYOFYEAR leap year", func(t *testing.T) {
		// Dec 31 of leap year should be day 366
		dec31_2024 := time.Date(2024, time.December, 31, 0, 0, 0, 0, time.UTC)
		result, err := evalDayOfYear([]any{dec31_2024})
		require.NoError(t, err)
		assert.Equal(t, int32(366), result)
	})

	t.Run("DAYOFYEAR non-leap year", func(t *testing.T) {
		// Dec 31 of non-leap year should be day 365
		dec31_2023 := time.Date(2023, time.December, 31, 0, 0, 0, 0, time.UTC)
		result, err := evalDayOfYear([]any{dec31_2023})
		require.NoError(t, err)
		assert.Equal(t, int32(365), result)
	})

	t.Run("LAST_DAY February leap year", func(t *testing.T) {
		// Feb 2024 has 29 days
		feb2024 := time.Date(2024, time.February, 15, 0, 0, 0, 0, time.UTC)
		result, err := evalLastDay([]any{feb2024})
		require.NoError(t, err)
		resultDate := dateToTime(result.(int32))
		assert.Equal(t, 29, resultDate.Day())
	})

	t.Run("LAST_DAY February non-leap year", func(t *testing.T) {
		// Feb 2023 has 28 days
		feb2023 := time.Date(2023, time.February, 15, 0, 0, 0, 0, time.UTC)
		result, err := evalLastDay([]any{feb2023})
		require.NoError(t, err)
		resultDate := dateToTime(result.(int32))
		assert.Equal(t, 28, resultDate.Day())
	})

	t.Run("Century leap year (2000)", func(t *testing.T) {
		// 2000 is a leap year (divisible by 400)
		result, err := evalMakeDate([]any{int32(2000), int32(2), int32(29)})
		require.NoError(t, err)
		resultDate := dateToTime(result.(int32))
		assert.Equal(t, 29, resultDate.Day())
	})

	t.Run("Century non-leap year (1900)", func(t *testing.T) {
		// 1900 is not a leap year (divisible by 100 but not 400)
		_, err := evalMakeDate([]any{int32(1900), int32(2), int32(29)})
		require.Error(t, err)
	})
}

// =============================================================================
// Task 8.9: Test STRFTIME with all supported specifiers
// =============================================================================

func TestIntegration_Strftime_AllSpecifiers(t *testing.T) {
	// Use a Saturday for weekday tests: 2024-06-15 14:30:45.123456
	testTime := time.Date(2024, time.June, 15, 14, 30, 45, 123456000, time.UTC)
	timestamp := testTime.UnixMicro()

	testCases := []struct {
		format   string
		expected string
	}{
		{"%Y", "2024"},
		{"%y", "24"},
		{"%m", "06"},
		{"%d", "15"},
		{"%H", "14"},
		{"%I", "02"},
		{"%M", "30"},
		{"%S", "45"},
		{"%f", "123456"},
		{"%p", "PM"},
		{"%j", "167"},
		{"%w", "6"}, // Saturday
		{"%a", "Sat"},
		{"%A", "Saturday"},
		{"%b", "Jun"},
		{"%B", "June"},
		{"%%", "%"},
		{"%Y-%m-%d", "2024-06-15"},
		{"%Y-%m-%d %H:%M:%S", "2024-06-15 14:30:45"},
		{"%I:%M %p", "02:30 PM"},
		{"%A, %B %d, %Y", "Saturday, June 15, 2024"},
	}

	for _, tc := range testCases {
		t.Run("STRFTIME "+tc.format, func(t *testing.T) {
			result, err := evalStrftime([]any{tc.format, timestamp})
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// =============================================================================
// Task 8.10: Test STRPTIME with various formats
// =============================================================================

func TestIntegration_Strptime_VariousFormats(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		format   string
		year     int
		month    time.Month
		day      int
		hour     int
		minute   int
		second   int
	}{
		{"ISO date", "2024-06-15", "%Y-%m-%d", 2024, time.June, 15, 0, 0, 0},
		{"ISO datetime", "2024-06-15 14:30:45", "%Y-%m-%d %H:%M:%S", 2024, time.June, 15, 14, 30, 45},
		{"US date format", "06/15/2024", "%m/%d/%Y", 2024, time.June, 15, 0, 0, 0},
		{"European date format", "15.06.2024", "%d.%m.%Y", 2024, time.June, 15, 0, 0, 0},
		{"12-hour time", "02:30 PM", "%I:%M %p", 1970, time.January, 1, 14, 30, 0},
		{"12-hour time AM", "11:30 AM", "%I:%M %p", 1970, time.January, 1, 11, 30, 0},
		{"12-hour noon", "12:00 PM", "%I:%M %p", 1970, time.January, 1, 12, 0, 0},
		{"12-hour midnight", "12:00 AM", "%I:%M %p", 1970, time.January, 1, 0, 0, 0},
		{"With weekday", "Saturday, June 15, 2024", "%A, %B %d, %Y", 2024, time.June, 15, 0, 0, 0},
		{"Short month", "15 Jun 2024", "%d %b %Y", 2024, time.June, 15, 0, 0, 0},
		{"2-digit year (70s)", "15/06/85", "%d/%m/%y", 1985, time.June, 15, 0, 0, 0},
		{"2-digit year (2000s)", "15/06/24", "%d/%m/%y", 2024, time.June, 15, 0, 0, 0},
	}

	for _, tc := range testCases {
		t.Run("STRPTIME "+tc.name, func(t *testing.T) {
			result, err := evalStrptime([]any{tc.input, tc.format})
			require.NoError(t, err)
			require.NotNil(t, result)

			resultTime := timestampToTime(result.(int64))
			assert.Equal(t, tc.year, resultTime.Year())
			assert.Equal(t, tc.month, resultTime.Month())
			assert.Equal(t, tc.day, resultTime.Day())
			assert.Equal(t, tc.hour, resultTime.Hour())
			assert.Equal(t, tc.minute, resultTime.Minute())
			assert.Equal(t, tc.second, resultTime.Second())
		})
	}
}

// =============================================================================
// Task 8.11: Test round-trip (format then parse)
// =============================================================================

func TestIntegration_RoundTrip_FormatParse(t *testing.T) {
	// Original timestamp: 2024-06-15 14:30:45 UTC
	baseTime := time.Date(2024, time.June, 15, 14, 30, 45, 0, time.UTC)
	originalTimestamp := baseTime.UnixMicro()

	formats := []string{
		"%Y-%m-%d %H:%M:%S",
		"%Y-%m-%d",
		"%d/%m/%Y %H:%M",
		"%Y%m%d%H%M%S",
	}

	for _, format := range formats {
		t.Run("Round-trip "+format, func(t *testing.T) {
			// Format
			formatted, err := evalStrftime([]any{format, originalTimestamp})
			require.NoError(t, err)
			require.NotNil(t, formatted)

			// Parse back
			parsed, err := evalStrptime([]any{formatted, format})
			require.NoError(t, err)
			require.NotNil(t, parsed)

			// Compare (accounting for precision loss based on format)
			originalTimeVal := timestampToTime(originalTimestamp)
			parsedTime := timestampToTime(parsed.(int64))

			assert.Equal(t, originalTimeVal.Year(), parsedTime.Year())
			assert.Equal(t, originalTimeVal.Month(), parsedTime.Month())
			assert.Equal(t, originalTimeVal.Day(), parsedTime.Day())

			// Only check time if format includes it
			if len(format) > 10 {
				assert.Equal(t, originalTimeVal.Hour(), parsedTime.Hour())
				assert.Equal(t, originalTimeVal.Minute(), parsedTime.Minute())
			}
		})
	}
}

// =============================================================================
// Task 8.12: Test error handling for invalid formats
// =============================================================================

func TestIntegration_ErrorHandling(t *testing.T) {
	t.Run("STRPTIME invalid date", func(t *testing.T) {
		result, err := evalStrptime([]any{"not-a-date", "%Y-%m-%d"})
		require.NoError(t, err) // STRPTIME returns NULL for unparseable, not error
		assert.Nil(t, result)
	})

	t.Run("STRPTIME invalid month in string", func(t *testing.T) {
		result, err := evalStrptime([]any{"2024-13-01", "%Y-%m-%d"})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("STRPTIME invalid day in string", func(t *testing.T) {
		result, err := evalStrptime([]any{"2024-06-32", "%Y-%m-%d"})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("MAKE_DATE invalid month", func(t *testing.T) {
		_, err := evalMakeDate([]any{int32(2024), int32(13), int32(1)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	})

	t.Run("MAKE_DATE invalid day", func(t *testing.T) {
		_, err := evalMakeDate([]any{int32(2024), int32(6), int32(31)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	})

	t.Run("MAKE_TIME invalid hour", func(t *testing.T) {
		_, err := evalMakeTime([]any{int32(24), int32(0), float64(0)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	})

	t.Run("MAKE_TIME invalid minute", func(t *testing.T) {
		_, err := evalMakeTime([]any{int32(12), int32(60), float64(0)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	})

	t.Run("MAKE_TIME invalid second", func(t *testing.T) {
		_, err := evalMakeTime([]any{int32(12), int32(30), float64(60)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	})

	t.Run("MAKE_TIMESTAMP invalid components", func(t *testing.T) {
		_, err := evalMakeTimestamp([]any{int32(2024), int32(6), int32(15), int32(25), int32(0), float64(0)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	})

	t.Run("DATE_PART invalid part", func(t *testing.T) {
		_, err := evalDatePart([]any{"invalid_part", int64(1718458245000000)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid date part")
	})

	t.Run("DATE_TRUNC invalid part", func(t *testing.T) {
		_, err := evalDateTrunc([]any{"invalid_part", int64(1718458245000000)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid date part")
	})

	t.Run("DATE_DIFF invalid part", func(t *testing.T) {
		_, err := evalDateDiff([]any{"invalid_part", int32(19889), int32(19890)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid date part")
	})
}

// =============================================================================
// Task 8.13: Test extraction functions per spec scenarios
// =============================================================================

func TestIntegration_SpecScenarios_Extraction(t *testing.T) {
	// Test DATE_PART with all valid parts
	testTime := time.Date(2024, time.June, 15, 14, 30, 45, 123456000, time.UTC)
	timestamp := testTime.UnixMicro()

	partTests := []struct {
		part     string
		expected float64
	}{
		{"year", 2024},
		{"month", 6},
		{"day", 15},
		{"hour", 14},
		{"minute", 30},
		{"quarter", 2},
		{"dow", 6},     // Saturday
		{"doy", 167},
		{"week", 24},
	}

	for _, tc := range partTests {
		t.Run("DATE_PART "+tc.part, func(t *testing.T) {
			result, err := evalDatePart([]any{tc.part, timestamp})
			require.NoError(t, err)
			assert.InDelta(t, tc.expected, result.(float64), 0.01)
		})
	}

	// Test part name aliases
	t.Run("DATE_PART aliases", func(t *testing.T) {
		// "years" should work same as "year"
		result1, err := evalDatePart([]any{"years", timestamp})
		require.NoError(t, err)
		result2, err := evalDatePart([]any{"year", timestamp})
		require.NoError(t, err)
		assert.Equal(t, result1, result2)

		// "y" should work same as "year"
		result3, err := evalDatePart([]any{"y", timestamp})
		require.NoError(t, err)
		assert.Equal(t, result1, result3)
	})
}

// =============================================================================
// Task 8.14: Test date construction functions per spec scenarios
// =============================================================================

func TestIntegration_SpecScenarios_Construction(t *testing.T) {
	t.Run("MAKE_DATE constructs valid date", func(t *testing.T) {
		result, err := evalMakeDate([]any{int32(2024), int32(6), int32(15)})
		require.NoError(t, err)
		resultDate := dateToTime(result.(int32))
		assert.Equal(t, 2024, resultDate.Year())
		assert.Equal(t, time.June, resultDate.Month())
		assert.Equal(t, 15, resultDate.Day())
	})

	t.Run("MAKE_TIMESTAMP constructs valid timestamp", func(t *testing.T) {
		result, err := evalMakeTimestamp([]any{
			int32(2024), int32(6), int32(15),
			int32(14), int32(30), float64(45.5),
		})
		require.NoError(t, err)
		resultTime := timestampToTime(result.(int64))
		assert.Equal(t, 2024, resultTime.Year())
		assert.Equal(t, time.June, resultTime.Month())
		assert.Equal(t, 15, resultTime.Day())
		assert.Equal(t, 14, resultTime.Hour())
		assert.Equal(t, 30, resultTime.Minute())
		assert.Equal(t, 45, resultTime.Second())
		// Check microseconds (0.5 seconds = 500000 microseconds)
		assert.InDelta(t, 500000, resultTime.Nanosecond()/1000, 1)
	})

	t.Run("MAKE_TIME constructs valid time", func(t *testing.T) {
		result, err := evalMakeTime([]any{int32(14), int32(30), float64(45.123456)})
		require.NoError(t, err)
		// Result is microseconds since midnight
		expectedMicros := int64(14*3600+30*60+45)*1_000_000 + 123456
		assert.Equal(t, expectedMicros, result)
	})
}

// =============================================================================
// Task 8.15: Test date arithmetic per spec scenarios
// =============================================================================

func TestIntegration_SpecScenarios_Arithmetic(t *testing.T) {
	t.Run("AGE between timestamps", func(t *testing.T) {
		// AGE from 2023-01-15 to 2024-06-20
		ts1 := time.Date(2023, time.January, 15, 0, 0, 0, 0, time.UTC)
		ts2 := time.Date(2024, time.June, 20, 0, 0, 0, 0, time.UTC)

		result, err := evalAge([]any{ts2, ts1})
		require.NoError(t, err)

		interval := result.(Interval)
		// Should be approximately 17 months and 5 days
		assert.Equal(t, int32(17), interval.Months)
		assert.Equal(t, int32(5), interval.Days)
	})

	t.Run("DATE_ADD preserves type", func(t *testing.T) {
		// Adding to DATE should return DATE
		dateVal := int32(19889) // 2024-06-15
		result, err := evalDateAdd([]any{dateVal, Interval{Days: 1}})
		require.NoError(t, err)
		_, ok := result.(int32)
		assert.True(t, ok, "DATE_ADD on DATE should return int32 (DATE)")

		// Adding to TIMESTAMP should return TIMESTAMP
		tsVal := int64(1718458245000000)
		result, err = evalDateAdd([]any{tsVal, Interval{Days: 1}})
		require.NoError(t, err)
		_, ok = result.(int64)
		assert.True(t, ok, "DATE_ADD on TIMESTAMP should return int64 (TIMESTAMP)")
	})
}

// =============================================================================
// Task 8.16: Test formatting/parsing per spec scenarios
// =============================================================================

func TestIntegration_SpecScenarios_Formatting(t *testing.T) {
	t.Run("EPOCH conversion", func(t *testing.T) {
		// 2024-06-15 14:30:45 UTC
		testTime := time.Date(2024, time.June, 15, 14, 30, 45, 0, time.UTC)
		timestamp := testTime.UnixMicro()
		result, err := evalEpoch([]any{timestamp})
		require.NoError(t, err)
		// Epoch seconds should match Unix timestamp
		expectedEpoch := float64(testTime.Unix())
		assert.InDelta(t, expectedEpoch, result.(float64), 0.01)
	})

	t.Run("EPOCH_MS conversion", func(t *testing.T) {
		// 2024-06-15 14:30:45 UTC
		testTime := time.Date(2024, time.June, 15, 14, 30, 45, 0, time.UTC)
		timestamp := testTime.UnixMicro()
		result, err := evalEpochMs([]any{timestamp})
		require.NoError(t, err)
		expectedMs := testTime.UnixMilli()
		assert.Equal(t, expectedMs, result)
	})

	t.Run("TO_TIMESTAMP conversion", func(t *testing.T) {
		// Convert epoch seconds back to timestamp
		testTime := time.Date(2024, time.June, 15, 14, 30, 45, 123456000, time.UTC)
		epochSeconds := float64(testTime.UnixMicro()) / 1_000_000
		result, err := evalToTimestamp([]any{epochSeconds})
		require.NoError(t, err)
		// Should be close to the original microseconds value
		assert.InDelta(t, testTime.UnixMicro(), result.(int64), 1)
	})
}

// =============================================================================
// Task 8.4: Test TIME extraction (already covered in 8.2)
// Additional TIME-specific tests
// =============================================================================

func TestIntegration_TimeExtraction(t *testing.T) {
	// TIME value: 14:30:45.123456 (microseconds since midnight)
	timeValue := int64(14*3600*1_000_000 + 30*60*1_000_000 + 45*1_000_000 + 123456)

	t.Run("HOUR from TIME", func(t *testing.T) {
		result, err := evalHour([]any{timeValue})
		require.NoError(t, err)
		assert.Equal(t, int32(14), result)
	})

	t.Run("MINUTE from TIME", func(t *testing.T) {
		result, err := evalMinute([]any{timeValue})
		require.NoError(t, err)
		assert.Equal(t, int32(30), result)
	})

	t.Run("SECOND from TIME", func(t *testing.T) {
		result, err := evalSecond([]any{timeValue})
		require.NoError(t, err)
		assert.InDelta(t, 45.123456, result.(float64), 0.0001)
	})
}

// =============================================================================
// Interval Tests
// =============================================================================

func TestIntegration_IntervalExtraction(t *testing.T) {
	// Test interval: 1 year, 3 months, 15 days, 2 hours, 30 minutes, 45.5 seconds
	interval := Interval{
		Months: 15, // 1 year + 3 months
		Days:   15,
		Micros: 2*3600*1_000_000 + 30*60*1_000_000 + 45*1_000_000 + 500_000, // 2:30:45.5
	}

	t.Run("TO_YEARS", func(t *testing.T) {
		result, err := evalToYears([]any{interval})
		require.NoError(t, err)
		assert.Equal(t, int64(1), result)
	})

	t.Run("TO_MONTHS", func(t *testing.T) {
		result, err := evalToMonths([]any{interval})
		require.NoError(t, err)
		assert.Equal(t, int64(3), result) // Remaining months after years
	})

	t.Run("TO_DAYS", func(t *testing.T) {
		result, err := evalToDays([]any{interval})
		require.NoError(t, err)
		assert.Equal(t, int64(15), result)
	})

	t.Run("TO_HOURS", func(t *testing.T) {
		result, err := evalToHours([]any{interval})
		require.NoError(t, err)
		assert.Equal(t, int64(2), result)
	})

	t.Run("TO_MINUTES", func(t *testing.T) {
		result, err := evalToMinutes([]any{interval})
		require.NoError(t, err)
		assert.Equal(t, int64(30), result)
	})

	t.Run("TO_SECONDS", func(t *testing.T) {
		result, err := evalToSeconds([]any{interval})
		require.NoError(t, err)
		assert.InDelta(t, 45.5, result.(float64), 0.01)
	})

	// Total extraction tests
	t.Run("TOTAL_DAYS", func(t *testing.T) {
		result, err := evalTotalDays([]any{interval})
		require.NoError(t, err)
		// 15 months * 30.4375 + 15 days + fraction of day
		expected := float64(15)*30.4375 + float64(15) + float64(interval.Micros)/(24*60*60*1_000_000)
		assert.InDelta(t, expected, result.(float64), 0.01)
	})

	t.Run("TOTAL_HOURS", func(t *testing.T) {
		result, err := evalTotalHours([]any{interval})
		require.NoError(t, err)
		// Should be significant number of hours
		assert.Greater(t, result.(float64), float64(10000))
	})
}

// =============================================================================
// Comprehensive Integration Test with Multiple Functions
// =============================================================================

func TestIntegration_ComprehensiveWorkflow(t *testing.T) {
	// Simulate a typical workflow using multiple temporal functions

	t.Run("Create date, add time, extract, format", func(t *testing.T) {
		// 1. Create a date
		date, err := evalMakeDate([]any{int32(2024), int32(1), int32(15)})
		require.NoError(t, err)

		// 2. Add 6 months
		futureDate, err := evalDateAdd([]any{date, Interval{Months: 6}})
		require.NoError(t, err)

		// 3. Extract components
		year, err := evalYear([]any{futureDate})
		require.NoError(t, err)
		assert.Equal(t, int32(2024), year)

		month, err := evalMonth([]any{futureDate})
		require.NoError(t, err)
		assert.Equal(t, int32(7), month) // January + 6 = July

		day, err := evalDay([]any{futureDate})
		require.NoError(t, err)
		assert.Equal(t, int32(15), day)

		// 4. Calculate quarter
		quarter, err := evalQuarter([]any{futureDate})
		require.NoError(t, err)
		assert.Equal(t, int32(3), quarter) // July is Q3
	})

	t.Run("Parse, manipulate, format round-trip", func(t *testing.T) {
		// 1. Parse a date string
		parsed, err := evalStrptime([]any{"2024-06-15 10:30:00", "%Y-%m-%d %H:%M:%S"})
		require.NoError(t, err)
		require.NotNil(t, parsed)

		// 2. Add 3 hours
		result, err := evalDateAdd([]any{parsed, Interval{Micros: 3 * 3600 * 1_000_000}})
		require.NoError(t, err)

		// 3. Extract hour
		hour, err := evalHour([]any{result})
		require.NoError(t, err)
		assert.Equal(t, int32(13), hour) // 10 + 3 = 13

		// 4. Format back to string
		formatted, err := evalStrftime([]any{"%Y-%m-%d %H:%M:%S", result})
		require.NoError(t, err)
		assert.Equal(t, "2024-06-15 13:30:00", formatted)
	})

	t.Run("Date difference and interval extraction", func(t *testing.T) {
		// Two timestamps
		time1 := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
		time2 := time.Date(2024, time.June, 15, 14, 30, 45, 0, time.UTC)
		ts1 := time1.UnixMicro()
		ts2 := time2.UnixMicro()

		// Calculate difference in days
		daysDiff, err := evalDateDiff([]any{"day", ts1, ts2})
		require.NoError(t, err)
		assert.Greater(t, daysDiff.(int64), int64(160))

		// Calculate age
		age, err := evalAge([]any{ts2, ts1})
		require.NoError(t, err)
		interval := age.(Interval)
		assert.Equal(t, int32(5), interval.Months) // Approximately 5 months
	})
}
