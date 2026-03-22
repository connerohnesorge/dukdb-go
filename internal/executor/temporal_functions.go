package executor

import (
	"fmt"
	"strings"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
)

// =============================================================================
// DatePart Type and Constants
// =============================================================================

// DatePart represents a part specifier for functions like DATE_DIFF, DATE_TRUNC, DATE_PART.
type DatePart string

const (
	DatePartYear        DatePart = "year"
	DatePartQuarter     DatePart = "quarter"
	DatePartMonth       DatePart = "month"
	DatePartWeek        DatePart = "week"
	DatePartDay         DatePart = "day"
	DatePartDayOfWeek   DatePart = "dayofweek"
	DatePartDayOfYear   DatePart = "dayofyear"
	DatePartHour        DatePart = "hour"
	DatePartMinute      DatePart = "minute"
	DatePartSecond      DatePart = "second"
	DatePartMillisecond DatePart = "millisecond"
	DatePartMicrosecond DatePart = "microsecond"
	DatePartEpoch       DatePart = "epoch"
	DatePartISODow     DatePart = "isodow"
	DatePartISOYear    DatePart = "isoyear"
	DatePartNanosecond DatePart = "nanosecond"
)

// parseDatePart parses a string into a DatePart, case-insensitively.
func parseDatePart(s string) (DatePart, error) {
	switch strings.ToLower(s) {
	case "year", "years", "y":
		return DatePartYear, nil
	case "quarter", "quarters":
		return DatePartQuarter, nil
	case "month", "months", "mon":
		return DatePartMonth, nil
	case "week", "weeks", "w":
		return DatePartWeek, nil
	case "day", "days", "d":
		return DatePartDay, nil
	case "dayofweek", "dow":
		return DatePartDayOfWeek, nil
	case "dayofyear", "doy":
		return DatePartDayOfYear, nil
	case "hour", "hours", "h":
		return DatePartHour, nil
	case "minute", "minutes", "min":
		return DatePartMinute, nil
	case "second", "seconds", "s":
		return DatePartSecond, nil
	case "millisecond", "milliseconds", "ms":
		return DatePartMillisecond, nil
	case "microsecond", "microseconds", "us":
		return DatePartMicrosecond, nil
	case "epoch":
		return DatePartEpoch, nil
	case "isodow":
		return DatePartISODow, nil
	case "isoyear":
		return DatePartISOYear, nil
	case "weekday":
		return DatePartDayOfWeek, nil
	case "weekofyear":
		return DatePartWeek, nil
	case "nanosecond", "nanoseconds", "ns":
		return DatePartNanosecond, nil
	default:
		return "", &dukdb.Error{
			Type: dukdb.ErrorTypeBinder,
			Msg:  fmt.Sprintf("invalid date part specifier: %s", s),
		}
	}
}

// =============================================================================
// Interval Type
// =============================================================================

// Interval represents a time interval with months, days, and microseconds components.
// This follows DuckDB's internal representation.
type Interval struct {
	Months int32 // Number of months
	Days   int32 // Number of days
	Micros int64 // Number of microseconds
}

// =============================================================================
// Helper Functions for Temporal Type Conversions
// =============================================================================

// dateToTime converts a DATE value (days since Unix epoch) to time.Time.
// DuckDB DATE is stored as int32 representing days since 1970-01-01.
func dateToTime(days int32) time.Time {
	return time.Unix(int64(days)*86400, 0).UTC()
}

// timestampToTime converts a TIMESTAMP value (microseconds since Unix epoch) to time.Time.
// DuckDB TIMESTAMP is stored as int64 representing microseconds since 1970-01-01 00:00:00 UTC.
func timestampToTime(micros int64) time.Time {
	return time.UnixMicro(micros).UTC()
}

// timeToComponents extracts hour, minute, second, and fractional seconds from a TIME value.
// DuckDB TIME is stored as int64 representing microseconds since midnight.
func timeToComponents(micros int64) (hour, minute, second int, frac float64) {
	hour = int(micros / (3600 * 1_000_000))
	micros %= 3600 * 1_000_000
	minute = int(micros / (60 * 1_000_000))
	micros %= 60 * 1_000_000
	second = int(micros / 1_000_000)
	frac = float64(micros%1_000_000) / 1_000_000
	return
}

// =============================================================================
// Date Extraction Functions
// =============================================================================

// evalYear extracts the year from a DATE or TIMESTAMP value.
// Returns int32 for the year, or nil for NULL input.
func evalYear(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "YEAR requires exactly 1 argument",
		}
	}

	if args[0] == nil {
		return nil, nil // NULL propagation
	}

	switch v := args[0].(type) {
	case int32: // DATE (days since epoch)
		t := dateToTime(v)
		return int32(t.Year()), nil
	case int64: // TIMESTAMP (microseconds since epoch)
		t := timestampToTime(v)
		return int32(t.Year()), nil
	case time.Time:
		return int32(v.Year()), nil
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("YEAR: unsupported type %T", args[0]),
		}
	}
}

// evalMonth extracts the month (1-12) from a DATE or TIMESTAMP value.
// Returns int32 for the month, or nil for NULL input.
func evalMonth(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "MONTH requires exactly 1 argument",
		}
	}

	if args[0] == nil {
		return nil, nil // NULL propagation
	}

	switch v := args[0].(type) {
	case int32: // DATE (days since epoch)
		t := dateToTime(v)
		return int32(t.Month()), nil
	case int64: // TIMESTAMP (microseconds since epoch)
		t := timestampToTime(v)
		return int32(t.Month()), nil
	case time.Time:
		return int32(v.Month()), nil
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("MONTH: unsupported type %T", args[0]),
		}
	}
}

// evalDay extracts the day of month (1-31) from a DATE or TIMESTAMP value.
// Returns int32 for the day, or nil for NULL input.
func evalDay(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DAY requires exactly 1 argument",
		}
	}

	if args[0] == nil {
		return nil, nil // NULL propagation
	}

	switch v := args[0].(type) {
	case int32: // DATE (days since epoch)
		t := dateToTime(v)
		return int32(t.Day()), nil
	case int64: // TIMESTAMP (microseconds since epoch)
		t := timestampToTime(v)
		return int32(t.Day()), nil
	case time.Time:
		return int32(v.Day()), nil
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("DAY: unsupported type %T", args[0]),
		}
	}
}

// evalHour extracts the hour (0-23) from a TIMESTAMP or TIME value.
// Returns int32 for the hour, or nil for NULL input.
func evalHour(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "HOUR requires exactly 1 argument",
		}
	}

	if args[0] == nil {
		return nil, nil // NULL propagation
	}

	switch v := args[0].(type) {
	case int32: // DATE (days since epoch) - hour is always 0
		return int32(0), nil
	case int64:
		// Could be TIMESTAMP (microseconds since epoch) or TIME (microseconds since midnight)
		// For TIMESTAMP values (large numbers), convert to time.Time
		// For TIME values (smaller numbers < 24*60*60*1_000_000), extract hour directly
		if v < 24*60*60*1_000_000 {
			// Treat as TIME (microseconds since midnight)
			hour, _, _, _ := timeToComponents(v)
			return int32(hour), nil
		}
		// Treat as TIMESTAMP
		t := timestampToTime(v)
		return int32(t.Hour()), nil
	case time.Time:
		return int32(v.Hour()), nil
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("HOUR: unsupported type %T", args[0]),
		}
	}
}

// evalMinute extracts the minute (0-59) from a TIMESTAMP or TIME value.
// Returns int32 for the minute, or nil for NULL input.
func evalMinute(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "MINUTE requires exactly 1 argument",
		}
	}

	if args[0] == nil {
		return nil, nil // NULL propagation
	}

	switch v := args[0].(type) {
	case int32: // DATE (days since epoch) - minute is always 0
		return int32(0), nil
	case int64:
		// Could be TIMESTAMP or TIME
		if v < 24*60*60*1_000_000 {
			// Treat as TIME (microseconds since midnight)
			_, minute, _, _ := timeToComponents(v)
			return int32(minute), nil
		}
		// Treat as TIMESTAMP
		t := timestampToTime(v)
		return int32(t.Minute()), nil
	case time.Time:
		return int32(v.Minute()), nil
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("MINUTE: unsupported type %T", args[0]),
		}
	}
}

// evalSecond extracts the second (0-59.999...) from a TIMESTAMP or TIME value.
// Returns float64 for the second including fractional microseconds, or nil for NULL input.
func evalSecond(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "SECOND requires exactly 1 argument",
		}
	}

	if args[0] == nil {
		return nil, nil // NULL propagation
	}

	switch v := args[0].(type) {
	case int32: // DATE (days since epoch) - second is always 0
		return float64(0), nil
	case int64:
		// Could be TIMESTAMP or TIME
		if v < 24*60*60*1_000_000 {
			// Treat as TIME (microseconds since midnight)
			_, _, second, frac := timeToComponents(v)
			return float64(second) + frac, nil
		}
		// Treat as TIMESTAMP
		t := timestampToTime(v)
		// Include nanoseconds for fractional seconds
		return float64(t.Second()) + float64(t.Nanosecond())/1e9, nil
	case time.Time:
		return float64(v.Second()) + float64(v.Nanosecond())/1e9, nil
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("SECOND: unsupported type %T", args[0]),
		}
	}
}

// evalDayOfWeek extracts the day of week from a DATE or TIMESTAMP value.
// Returns int32 where 0=Sunday, 6=Saturday (matching DuckDB convention).
func evalDayOfWeek(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DAYOFWEEK requires exactly 1 argument",
		}
	}

	if args[0] == nil {
		return nil, nil // NULL propagation
	}

	var t time.Time
	switch v := args[0].(type) {
	case int32: // DATE (days since epoch)
		t = dateToTime(v)
	case int64: // TIMESTAMP (microseconds since epoch)
		t = timestampToTime(v)
	case time.Time:
		t = v
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("DAYOFWEEK: unsupported type %T", args[0]),
		}
	}

	// Go's time.Weekday() returns Sunday=0, Saturday=6, which matches DuckDB
	return int32(t.Weekday()), nil
}

// evalDayOfYear extracts the day of year (1-366) from a DATE or TIMESTAMP value.
// Returns int32 for the day of year, or nil for NULL input.
func evalDayOfYear(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DAYOFYEAR requires exactly 1 argument",
		}
	}

	if args[0] == nil {
		return nil, nil // NULL propagation
	}

	var t time.Time
	switch v := args[0].(type) {
	case int32: // DATE (days since epoch)
		t = dateToTime(v)
	case int64: // TIMESTAMP (microseconds since epoch)
		t = timestampToTime(v)
	case time.Time:
		t = v
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("DAYOFYEAR: unsupported type %T", args[0]),
		}
	}

	return int32(t.YearDay()), nil
}

// evalWeek extracts the ISO week number (1-53) from a DATE or TIMESTAMP value.
// Returns int32 for the week number, or nil for NULL input.
func evalWeek(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "WEEK requires exactly 1 argument",
		}
	}

	if args[0] == nil {
		return nil, nil // NULL propagation
	}

	var t time.Time
	switch v := args[0].(type) {
	case int32: // DATE (days since epoch)
		t = dateToTime(v)
	case int64: // TIMESTAMP (microseconds since epoch)
		t = timestampToTime(v)
	case time.Time:
		t = v
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("WEEK: unsupported type %T", args[0]),
		}
	}

	// Go's ISOWeek returns (year, week)
	_, week := t.ISOWeek()
	return int32(week), nil
}

// evalQuarter extracts the quarter (1-4) from a DATE or TIMESTAMP value.
// Returns int32 for the quarter, or nil for NULL input.
func evalQuarter(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "QUARTER requires exactly 1 argument",
		}
	}

	if args[0] == nil {
		return nil, nil // NULL propagation
	}

	var t time.Time
	switch v := args[0].(type) {
	case int32: // DATE (days since epoch)
		t = dateToTime(v)
	case int64: // TIMESTAMP (microseconds since epoch)
		t = timestampToTime(v)
	case time.Time:
		t = v
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("QUARTER: unsupported type %T", args[0]),
		}
	}

	// Calculate quarter from month: (month-1)/3 + 1
	month := int(t.Month())
	quarter := (month-1)/3 + 1
	return int32(quarter), nil
}

// =============================================================================
// Date Arithmetic Functions
// =============================================================================

// timeToDate converts a time.Time to a DATE value (days since epoch).
func timeToDate(t time.Time) int32 {
	return int32(t.Unix() / 86400)
}

// timeToTimestamp converts a time.Time to a TIMESTAMP value (microseconds since epoch).
func timeToTimestamp(t time.Time) int64 {
	return t.UnixMicro()
}

// addMonths adds months to a time, handling month-end clamping.
// For example, Jan 31 + 1 month = Feb 28/29 (not March 3).
func addMonths(t time.Time, months int) time.Time {
	year := t.Year()
	month := int(t.Month())
	day := t.Day()

	// Add months
	totalMonths := year*12 + (month - 1) + months
	newYear := totalMonths / 12
	newMonth := time.Month(totalMonths%12 + 1)

	// Handle negative months
	if totalMonths%12 < 0 {
		newMonth = time.Month(totalMonths%12 + 13)
		newYear--
	}

	// Clamp day to valid range for new month
	daysInMonth := daysInMonthFor(newYear, newMonth)
	if day > daysInMonth {
		day = daysInMonth
	}

	return time.Date(
		newYear,
		newMonth,
		day,
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
}

// daysInMonthFor returns the number of days in the given month/year.
func daysInMonthFor(year int, month time.Month) int {
	// Use the trick of going to day 0 of next month
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// evalDateAdd adds an interval to a date or timestamp.
// Returns the same type as input (DATE or TIMESTAMP).
func evalDateAdd(args []any) (any, error) {
	if len(args) != 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DATE_ADD requires exactly 2 arguments",
		}
	}

	// NULL propagation
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	// Get the interval
	interval, err := toInterval(args[1])
	if err != nil {
		return nil, err
	}

	// Handle different input types
	switch v := args[0].(type) {
	case int32: // DATE (days since epoch)
		t := dateToTime(v)
		result := addInterval(t, interval)
		return timeToDate(result), nil

	case int64: // TIMESTAMP (microseconds since epoch)
		t := timestampToTime(v)
		result := addInterval(t, interval)
		return timeToTimestamp(result), nil

	case time.Time:
		result := addInterval(v, interval)
		return result, nil

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("DATE_ADD: unsupported type %T for first argument", args[0]),
		}
	}
}

// evalDateSub subtracts an interval from a date or timestamp.
// Returns the same type as input (DATE or TIMESTAMP).
func evalDateSub(args []any) (any, error) {
	if len(args) != 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DATE_SUB requires exactly 2 arguments",
		}
	}

	// NULL propagation
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	// Get the interval and negate it
	interval, err := toInterval(args[1])
	if err != nil {
		return nil, err
	}

	// Negate the interval
	negInterval := Interval{
		Months: -interval.Months,
		Days:   -interval.Days,
		Micros: -interval.Micros,
	}

	// Handle different input types
	switch v := args[0].(type) {
	case int32: // DATE (days since epoch)
		t := dateToTime(v)
		result := addInterval(t, negInterval)
		return timeToDate(result), nil

	case int64: // TIMESTAMP (microseconds since epoch)
		t := timestampToTime(v)
		result := addInterval(t, negInterval)
		return timeToTimestamp(result), nil

	case time.Time:
		result := addInterval(v, negInterval)
		return result, nil

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("DATE_SUB: unsupported type %T for first argument", args[0]),
		}
	}
}

// addInterval adds an Interval to a time.Time value.
func addInterval(t time.Time, interval Interval) time.Time {
	// Add months first (with clamping)
	if interval.Months != 0 {
		t = addMonths(t, int(interval.Months))
	}

	// Add days
	if interval.Days != 0 {
		t = t.AddDate(0, 0, int(interval.Days))
	}

	// Add microseconds
	if interval.Micros != 0 {
		t = t.Add(time.Duration(interval.Micros) * time.Microsecond)
	}

	return t
}

// toInterval converts a value to an Interval.
func toInterval(v any) (Interval, error) {
	switch val := v.(type) {
	case Interval:
		return val, nil
	case *Interval:
		if val == nil {
			return Interval{}, nil
		}
		return *val, nil
	case int32:
		// Treat as days
		return Interval{Days: val}, nil
	case int64:
		// Treat as microseconds
		return Interval{Micros: val}, nil
	case int:
		// Treat as days
		return Interval{Days: int32(val)}, nil
	default:
		return Interval{}, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("cannot convert %T to INTERVAL", v),
		}
	}
}

// evalDateDiff calculates the difference between two dates in the specified units.
// Returns BIGINT (int64).
func evalDateDiff(args []any) (any, error) {
	if len(args) != 3 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DATE_DIFF requires exactly 3 arguments (part, start, end)",
		}
	}

	// NULL propagation
	if args[0] == nil || args[1] == nil || args[2] == nil {
		return nil, nil
	}

	// Parse the part specifier
	partStr, ok := args[0].(string)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DATE_DIFF: first argument must be a string (part specifier)",
		}
	}

	part, err := parseDatePart(partStr)
	if err != nil {
		return nil, err
	}

	// Get the start and end times
	startTime, err := toTime(args[1])
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("DATE_DIFF: cannot convert start to time: %v", err),
		}
	}

	endTime, err := toTime(args[2])
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("DATE_DIFF: cannot convert end to time: %v", err),
		}
	}

	// Calculate the difference
	diff := dateDiff(part, startTime, endTime)
	return diff, nil
}

// toTime converts a value to time.Time.
func toTime(v any) (time.Time, error) {
	switch val := v.(type) {
	case int32: // DATE (days since epoch)
		return dateToTime(val), nil
	case int64: // TIMESTAMP (microseconds since epoch)
		return timestampToTime(val), nil
	case time.Time:
		return val, nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", v)
	}
}

// dateDiff calculates the difference between two times in the specified units.
func dateDiff(part DatePart, start, end time.Time) int64 {
	switch part {
	case DatePartYear:
		return int64(end.Year() - start.Year())

	case DatePartQuarter:
		startQ := (start.Year()*12 + int(start.Month()) - 1) / 3
		endQ := (end.Year()*12 + int(end.Month()) - 1) / 3
		return int64(endQ - startQ)

	case DatePartMonth:
		startMonths := start.Year()*12 + int(start.Month()) - 1
		endMonths := end.Year()*12 + int(end.Month()) - 1
		return int64(endMonths - startMonths)

	case DatePartWeek:
		// Truncate to start of week and calculate difference
		startWeek := start.Truncate(24*time.Hour).AddDate(0, 0, -int(start.Weekday()))
		endWeek := end.Truncate(24*time.Hour).AddDate(0, 0, -int(end.Weekday()))
		days := endWeek.Sub(startWeek).Hours() / 24
		return int64(days / 7)

	case DatePartDay, DatePartDayOfWeek, DatePartDayOfYear:
		// Calculate using Unix timestamps for consistency
		startDay := start.Unix() / 86400
		endDay := end.Unix() / 86400
		return endDay - startDay

	case DatePartHour:
		return int64(end.Sub(start).Hours())

	case DatePartMinute:
		return int64(end.Sub(start).Minutes())

	case DatePartSecond:
		return int64(end.Sub(start).Seconds())

	case DatePartMillisecond:
		return end.Sub(start).Milliseconds()

	case DatePartMicrosecond:
		return end.Sub(start).Microseconds()

	case DatePartEpoch:
		// Return difference in seconds as epoch
		return int64(end.Sub(start).Seconds())
	}

	// Fallback to days (should not reach here due to exhaustive switch)
	return int64(end.Sub(start).Hours() / 24)
}

// evalDateTrunc truncates a timestamp to the specified precision.
// Returns TIMESTAMP (int64 microseconds since epoch).
func evalDateTrunc(args []any) (any, error) {
	if len(args) != 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DATE_TRUNC requires exactly 2 arguments (part, timestamp)",
		}
	}

	// NULL propagation
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	// Parse the part specifier
	partStr, ok := args[0].(string)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DATE_TRUNC: first argument must be a string (part specifier)",
		}
	}

	part, err := parseDatePart(partStr)
	if err != nil {
		return nil, err
	}

	// Get the timestamp
	t, err := toTime(args[1])
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("DATE_TRUNC: cannot convert timestamp: %v", err),
		}
	}

	// Truncate based on part
	result := truncateTime(t, part)
	return timeToTimestamp(result), nil
}

// truncateTime truncates a time to the specified precision.
func truncateTime(t time.Time, part DatePart) time.Time {
	switch part {
	case DatePartYear:
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())

	case DatePartQuarter:
		month := ((int(t.Month())-1)/3)*3 + 1
		return time.Date(t.Year(), time.Month(month), 1, 0, 0, 0, 0, t.Location())

	case DatePartMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())

	case DatePartWeek:
		// Truncate to start of week (Sunday)
		weekday := int(t.Weekday())
		return time.Date(t.Year(), t.Month(), t.Day()-weekday, 0, 0, 0, 0, t.Location())

	case DatePartDay, DatePartDayOfWeek, DatePartDayOfYear:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())

	case DatePartHour:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())

	case DatePartMinute:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())

	case DatePartSecond:
		return time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			t.Second(),
			0,
			t.Location(),
		)

	case DatePartMillisecond:
		// Truncate nanoseconds to milliseconds
		ns := (t.Nanosecond() / 1_000_000) * 1_000_000
		return time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			t.Second(),
			ns,
			t.Location(),
		)

	case DatePartMicrosecond:
		// Truncate nanoseconds to microseconds
		ns := (t.Nanosecond() / 1_000) * 1_000
		return time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			t.Second(),
			ns,
			t.Location(),
		)

	case DatePartEpoch:
		// Epoch truncation doesn't make sense, return as-is at second precision
		return time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			t.Second(),
			0,
			t.Location(),
		)
	}

	// Default to day truncation (should not reach here due to exhaustive switch)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

// evalDatePart extracts a part from a timestamp as DOUBLE.
// This is similar to EXTRACT.
func evalDatePart(args []any) (any, error) {
	if len(args) != 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DATE_PART requires exactly 2 arguments (part, timestamp)",
		}
	}

	// NULL propagation
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	// Parse the part specifier
	partStr, ok := args[0].(string)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DATE_PART: first argument must be a string (part specifier)",
		}
	}

	part, err := parseDatePart(partStr)
	if err != nil {
		return nil, err
	}

	// Get the timestamp
	t, err := toTime(args[1])
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("DATE_PART: cannot convert timestamp: %v", err),
		}
	}

	// Extract the part
	result := extractPart(t, part)
	return result, nil
}

// extractPart extracts a part from a time as a float64.
func extractPart(t time.Time, part DatePart) float64 {
	switch part {
	case DatePartYear:
		return float64(t.Year())

	case DatePartQuarter:
		return float64((int(t.Month())-1)/3 + 1)

	case DatePartMonth:
		return float64(t.Month())

	case DatePartWeek:
		_, week := t.ISOWeek()
		return float64(week)

	case DatePartDay:
		return float64(t.Day())

	case DatePartDayOfWeek:
		return float64(t.Weekday())

	case DatePartDayOfYear:
		return float64(t.YearDay())

	case DatePartHour:
		return float64(t.Hour())

	case DatePartMinute:
		return float64(t.Minute())

	case DatePartSecond:
		// Include fractional seconds
		return float64(t.Second()) + float64(t.Nanosecond())/1e9

	case DatePartMillisecond:
		return float64(t.Nanosecond() / 1_000_000)

	case DatePartMicrosecond:
		return float64(t.Nanosecond() / 1_000)

	case DatePartEpoch:
		return float64(t.UnixMicro()) / 1e6

	case DatePartISODow:
		dow := int(t.Weekday())
		if dow == 0 {
			dow = 7
		}
		return float64(dow)

	case DatePartISOYear:
		year, _ := t.ISOWeek()
		return float64(year)

	case DatePartNanosecond:
		return float64(t.Nanosecond())

	default:
		return 0
	}
}

// evalAge calculates the age (interval) between two timestamps.
// Returns an Interval.
func evalAge(args []any) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "AGE requires 1 or 2 arguments",
		}
	}

	// NULL propagation
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
	}

	var startTime, endTime time.Time
	var err error

	if len(args) == 1 {
		// AGE(timestamp) - calculate from current time
		endTime = time.Now().UTC()
		startTime, err = toTime(args[0])
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("AGE: cannot convert timestamp: %v", err),
			}
		}
	} else {
		// AGE(timestamp1, timestamp2) - calculate difference
		endTime, err = toTime(args[0])
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("AGE: cannot convert first timestamp: %v", err),
			}
		}
		startTime, err = toTime(args[1])
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("AGE: cannot convert second timestamp: %v", err),
			}
		}
	}

	// Calculate the interval between the two times
	interval := calculateAge(startTime, endTime)
	return interval, nil
}

// calculateAge calculates the interval between two times.
// This is a simplified implementation that returns months, days, and microseconds.
func calculateAge(start, end time.Time) Interval {
	// Handle negative intervals
	negative := false
	if end.Before(start) {
		start, end = end, start
		negative = true
	}

	// Calculate years and months
	years := end.Year() - start.Year()
	months := int(end.Month()) - int(start.Month())
	days := end.Day() - start.Day()

	// Adjust for negative days
	if days < 0 {
		months--
		// Get days in previous month
		prevMonth := end.AddDate(0, -1, 0)
		days += daysInMonthFor(prevMonth.Year(), prevMonth.Month())
	}

	// Adjust for negative months
	if months < 0 {
		years--
		months += 12
	}

	// Total months
	totalMonths := years*12 + months

	// Calculate remaining time difference
	startOfDay := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, start.Location())
	endOfDay := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, end.Location())

	// Time within the day
	startMicros := start.Sub(startOfDay).Microseconds()
	endMicros := end.Sub(endOfDay).Microseconds()
	micros := endMicros - startMicros

	// Adjust if micros is negative
	if micros < 0 {
		days--
		micros += 24 * 60 * 60 * 1_000_000 // Add one day in microseconds
	}

	result := Interval{
		Months: int32(totalMonths),
		Days:   int32(days),
		Micros: micros,
	}

	if negative {
		result.Months = -result.Months
		result.Days = -result.Days
		result.Micros = -result.Micros
	}

	return result
}

// evalLastDay returns the last day of the month for a given date.
// Returns DATE (int32 days since epoch).
func evalLastDay(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "LAST_DAY requires exactly 1 argument",
		}
	}

	// NULL propagation
	if args[0] == nil {
		return nil, nil
	}

	// Get the date/timestamp
	t, err := toTime(args[0])
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("LAST_DAY: cannot convert date: %v", err),
		}
	}

	// Get the last day of the month
	// Go to the first day of next month, then subtract one day
	lastDay := time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, t.Location())

	return timeToDate(lastDay), nil
}

// =============================================================================
// Date Construction Functions
// =============================================================================

// evalMakeDate constructs a DATE from year, month, and day components.
// Returns DATE (int32 days since epoch).
// Returns error for invalid dates (e.g., Feb 30, month 13).
// Returns NULL if any argument is NULL.
func evalMakeDate(args []any) (any, error) {
	if len(args) != 3 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "MAKE_DATE requires exactly 3 arguments (year, month, day)",
		}
	}

	// NULL propagation
	if args[0] == nil || args[1] == nil || args[2] == nil {
		return nil, nil
	}

	// Extract year, month, day from arguments
	year, err := toIntArg(args[0], "MAKE_DATE", "year")
	if err != nil {
		return nil, err
	}

	month, err := toIntArg(args[1], "MAKE_DATE", "month")
	if err != nil {
		return nil, err
	}

	day, err := toIntArg(args[2], "MAKE_DATE", "day")
	if err != nil {
		return nil, err
	}

	// Validate month range
	if month < 1 || month > 12 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("MAKE_DATE: month value %d is out of range (1-12)", month),
		}
	}

	// Validate day range for the given month/year
	daysInMonth := daysInMonthFor(year, time.Month(month))
	if day < 1 || day > daysInMonth {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"MAKE_DATE: day value %d is out of range for month %d (1-%d)",
				day,
				month,
				daysInMonth,
			),
		}
	}

	// Construct the date
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return timeToDate(t), nil
}

// evalMakeTimestamp constructs a TIMESTAMP from year, month, day, hour, minute, second components.
// Returns TIMESTAMP (int64 microseconds since epoch).
// The second argument is a DOUBLE to support fractional seconds.
// Returns error for invalid date/time components.
// Returns NULL if any argument is NULL.
func evalMakeTimestamp(args []any) (any, error) {
	if len(args) != 6 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "MAKE_TIMESTAMP requires exactly 6 arguments (year, month, day, hour, minute, second)",
		}
	}

	// NULL propagation
	for i, arg := range args {
		if arg == nil {
			return nil, nil
		}
		_ = i // avoid unused variable warning
	}

	// Extract year, month, day, hour, minute from arguments
	year, err := toIntArg(args[0], "MAKE_TIMESTAMP", "year")
	if err != nil {
		return nil, err
	}

	month, err := toIntArg(args[1], "MAKE_TIMESTAMP", "month")
	if err != nil {
		return nil, err
	}

	day, err := toIntArg(args[2], "MAKE_TIMESTAMP", "day")
	if err != nil {
		return nil, err
	}

	hour, err := toIntArg(args[3], "MAKE_TIMESTAMP", "hour")
	if err != nil {
		return nil, err
	}

	minute, err := toIntArg(args[4], "MAKE_TIMESTAMP", "minute")
	if err != nil {
		return nil, err
	}

	// Second is a DOUBLE (can have fractional seconds)
	second, err := toFloatArg(args[5], "MAKE_TIMESTAMP", "second")
	if err != nil {
		return nil, err
	}

	// Validate month range
	if month < 1 || month > 12 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("MAKE_TIMESTAMP: month value %d is out of range (1-12)", month),
		}
	}

	// Validate day range for the given month/year
	daysInMonth := daysInMonthFor(year, time.Month(month))
	if day < 1 || day > daysInMonth {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"MAKE_TIMESTAMP: day value %d is out of range for month %d (1-%d)",
				day,
				month,
				daysInMonth,
			),
		}
	}

	// Validate hour range
	if hour < 0 || hour > 23 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("MAKE_TIMESTAMP: hour value %d is out of range (0-23)", hour),
		}
	}

	// Validate minute range
	if minute < 0 || minute > 59 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("MAKE_TIMESTAMP: minute value %d is out of range (0-59)", minute),
		}
	}

	// Validate second range (0-59.999...)
	if second < 0 || second >= 60 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"MAKE_TIMESTAMP: second value %f is out of range (0-59.999...)",
				second,
			),
		}
	}

	// Extract whole seconds and fractional microseconds
	wholeSeconds := int(second)
	fractionalMicros := int((second - float64(wholeSeconds)) * 1_000_000)

	// Construct the timestamp
	t := time.Date(
		year,
		time.Month(month),
		day,
		hour,
		minute,
		wholeSeconds,
		fractionalMicros*1000,
		time.UTC,
	)
	return timeToTimestamp(t), nil
}

// evalMakeTime constructs a TIME from hour, minute, second components.
// Returns TIME (int64 microseconds since midnight).
// The second argument is a DOUBLE to support fractional seconds.
// Returns error for invalid time components.
// Returns NULL if any argument is NULL.
func evalMakeTime(args []any) (any, error) {
	if len(args) != 3 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "MAKE_TIME requires exactly 3 arguments (hour, minute, second)",
		}
	}

	// NULL propagation
	if args[0] == nil || args[1] == nil || args[2] == nil {
		return nil, nil
	}

	// Extract hour, minute from arguments
	hour, err := toIntArg(args[0], "MAKE_TIME", "hour")
	if err != nil {
		return nil, err
	}

	minute, err := toIntArg(args[1], "MAKE_TIME", "minute")
	if err != nil {
		return nil, err
	}

	// Second is a DOUBLE (can have fractional seconds)
	second, err := toFloatArg(args[2], "MAKE_TIME", "second")
	if err != nil {
		return nil, err
	}

	// Validate hour range
	if hour < 0 || hour > 23 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("MAKE_TIME: hour value %d is out of range (0-23)", hour),
		}
	}

	// Validate minute range
	if minute < 0 || minute > 59 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("MAKE_TIME: minute value %d is out of range (0-59)", minute),
		}
	}

	// Validate second range (0-59.999...)
	if second < 0 || second >= 60 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("MAKE_TIME: second value %f is out of range (0-59.999...)", second),
		}
	}

	// Calculate microseconds since midnight
	// hour * 3600 * 1_000_000 + minute * 60 * 1_000_000 + second * 1_000_000
	micros := int64(hour)*3600*1_000_000 +
		int64(minute)*60*1_000_000 +
		int64(second*1_000_000)

	return micros, nil
}

// =============================================================================
// Helper Functions for Argument Conversion
// =============================================================================

// toIntArg converts an argument to an int, returning a descriptive error if it fails.
func toIntArg(arg any, funcName, argName string) (int, error) {
	switch v := arg.(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case float64:
		return int(v), nil
	case float32:
		return int(v), nil
	default:
		return 0, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("%s: cannot convert %s argument of type %T to integer", funcName, argName, arg),
		}
	}
}

// toFloatArg converts an argument to a float64, returning a descriptive error if it fails.
func toFloatArg(arg any, funcName, argName string) (float64, error) {
	switch v := arg.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	default:
		return 0, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("%s: cannot convert %s argument of type %T to float", funcName, argName, arg),
		}
	}
}

// =============================================================================
// Formatting/Parsing Functions
// =============================================================================

// FormatTokenType represents the type of a format token.
type FormatTokenType int

const (
	// TokenLiteral is a literal string token.
	TokenLiteral FormatTokenType = iota
	// TokenSpecifier is a format specifier token (e.g., %Y, %m).
	TokenSpecifier
)

// FormatToken represents a token in a parsed format string.
type FormatToken struct {
	Type      FormatTokenType
	Value     string // For literals, the literal string; for specifiers, the specifier character
	Specifier byte   // The specifier character (e.g., 'Y', 'm', 'd')
}

// Weekday names for formatting
var weekdayAbbr = []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}

var weekdayFull = []string{
	"Sunday",
	"Monday",
	"Tuesday",
	"Wednesday",
	"Thursday",
	"Friday",
	"Saturday",
}

// Month names for formatting
var monthAbbr = []string{
	"",
	"Jan",
	"Feb",
	"Mar",
	"Apr",
	"May",
	"Jun",
	"Jul",
	"Aug",
	"Sep",
	"Oct",
	"Nov",
	"Dec",
}

var monthFull = []string{
	"",
	"January",
	"February",
	"March",
	"April",
	"May",
	"June",
	"July",
	"August",
	"September",
	"October",
	"November",
	"December",
}

// parseStrftimeFormat parses a strftime format string into tokens.
// Supported specifiers:
//
//	%Y - 4-digit year (2024)
//	%y - 2-digit year (24)
//	%m - Month (01-12)
//	%d - Day of month (01-31)
//	%H - Hour 24h (00-23)
//	%I - Hour 12h (01-12)
//	%M - Minute (00-59)
//	%S - Second (00-59)
//	%f - Microseconds (000000-999999)
//	%p - AM/PM
//	%j - Day of year (001-366)
//	%W - Week of year (00-53)
//	%w - Weekday (0-6, Sunday=0)
//	%a - Abbreviated weekday (Mon, Tue, etc.)
//	%A - Full weekday (Monday, Tuesday, etc.)
//	%b - Abbreviated month (Jan, Feb, etc.)
//	%B - Full month (January, February, etc.)
//	%% - Literal %
func parseStrftimeFormat(format string) []FormatToken {
	var tokens []FormatToken
	var literal strings.Builder

	i := 0
	for i < len(format) {
		if format[i] == '%' && i+1 < len(format) {
			// Flush any accumulated literal
			if literal.Len() > 0 {
				tokens = append(tokens, FormatToken{
					Type:  TokenLiteral,
					Value: literal.String(),
				})
				literal.Reset()
			}

			spec := format[i+1]
			switch spec {
			case 'Y',
				'y',
				'm',
				'd',
				'H',
				'I',
				'M',
				'S',
				'f',
				'p',
				'j',
				'W',
				'w',
				'a',
				'A',
				'b',
				'B':
				tokens = append(tokens, FormatToken{
					Type:      TokenSpecifier,
					Specifier: spec,
				})
			case '%':
				// Literal %
				literal.WriteByte('%')
			default:
				// Unknown specifier, treat as literal
				literal.WriteByte('%')
				literal.WriteByte(spec)
			}
			i += 2
		} else {
			literal.WriteByte(format[i])
			i++
		}
	}

	// Flush any remaining literal
	if literal.Len() > 0 {
		tokens = append(tokens, FormatToken{
			Type:  TokenLiteral,
			Value: literal.String(),
		})
	}

	return tokens
}

// formatTime formats a time.Time according to parsed format tokens.
func formatTime(t time.Time, tokens []FormatToken) string {
	var result strings.Builder

	for _, token := range tokens {
		switch token.Type {
		case TokenLiteral:
			result.WriteString(token.Value)
		case TokenSpecifier:
			result.WriteString(formatSpecifier(t, token.Specifier))
		}
	}

	return result.String()
}

// formatSpecifier formats a single specifier.
func formatSpecifier(t time.Time, spec byte) string {
	switch spec {
	case 'Y':
		// 4-digit year
		return fmt.Sprintf("%04d", t.Year())
	case 'y':
		// 2-digit year
		return fmt.Sprintf("%02d", t.Year()%100)
	case 'm':
		// Month (01-12)
		return fmt.Sprintf("%02d", t.Month())
	case 'd':
		// Day of month (01-31)
		return fmt.Sprintf("%02d", t.Day())
	case 'H':
		// Hour 24h (00-23)
		return fmt.Sprintf("%02d", t.Hour())
	case 'I':
		// Hour 12h (01-12)
		hour := t.Hour() % 12
		if hour == 0 {
			hour = 12
		}
		return fmt.Sprintf("%02d", hour)
	case 'M':
		// Minute (00-59)
		return fmt.Sprintf("%02d", t.Minute())
	case 'S':
		// Second (00-59)
		return fmt.Sprintf("%02d", t.Second())
	case 'f':
		// Microseconds (000000-999999)
		return fmt.Sprintf("%06d", t.Nanosecond()/1000)
	case 'p':
		// AM/PM
		if t.Hour() < 12 {
			return "AM"
		}
		return "PM"
	case 'j':
		// Day of year (001-366)
		return fmt.Sprintf("%03d", t.YearDay())
	case 'W':
		// Week of year (00-53)
		_, week := t.ISOWeek()
		return fmt.Sprintf("%02d", week)
	case 'w':
		// Weekday (0-6, Sunday=0)
		return fmt.Sprintf("%d", int(t.Weekday()))
	case 'a':
		// Abbreviated weekday
		return weekdayAbbr[int(t.Weekday())]
	case 'A':
		// Full weekday
		return weekdayFull[int(t.Weekday())]
	case 'b':
		// Abbreviated month
		return monthAbbr[int(t.Month())]
	case 'B':
		// Full month
		return monthFull[int(t.Month())]
	default:
		return ""
	}
}

// evalStrftime formats a timestamp according to a format string.
// Returns VARCHAR (string).
// STRFTIME(format, timestamp) -> VARCHAR
func evalStrftime(args []any) (any, error) {
	if len(args) != 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "STRFTIME requires exactly 2 arguments (format, timestamp)",
		}
	}

	// NULL propagation
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	// Get the format string
	formatStr, ok := args[0].(string)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("STRFTIME: format must be a string, got %T", args[0]),
		}
	}

	// Get the timestamp
	t, err := toTime(args[1])
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("STRFTIME: cannot convert timestamp: %v", err),
		}
	}

	// Parse and format
	tokens := parseStrftimeFormat(formatStr)
	result := formatTime(t, tokens)

	return result, nil
}

// evalStrptime parses a string according to a format string.
// Returns TIMESTAMP or NULL if unparseable.
// STRPTIME(string, format) -> TIMESTAMP
func evalStrptime(args []any) (any, error) {
	if len(args) != 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "STRPTIME requires exactly 2 arguments (string, format)",
		}
	}

	// NULL propagation
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	// Get the input string
	inputStr, ok := args[0].(string)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("STRPTIME: first argument must be a string, got %T", args[0]),
		}
	}

	// Get the format string
	formatStr, ok := args[1].(string)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("STRPTIME: format must be a string, got %T", args[1]),
		}
	}

	// Parse the format and try to extract values
	t, ok := parseStrptime(inputStr, formatStr)
	if !ok {
		// Return NULL for unparseable strings (not an error)
		return nil, nil
	}

	// Return TIMESTAMP (int64 microseconds since epoch)
	return timeToTimestamp(t), nil
}

// parseStrptime parses an input string according to a strftime-style format.
// Returns the parsed time and whether parsing was successful.
func parseStrptime(input, format string) (time.Time, bool) {
	// Initialize default values
	year := 1970
	month := 1
	day := 1
	hour := 0
	minute := 0
	second := 0
	microsecond := 0
	isPM := false
	has12Hour := false

	inputIdx := 0
	formatIdx := 0

	for formatIdx < len(format) {
		if format[formatIdx] == '%' && formatIdx+1 < len(format) {
			spec := format[formatIdx+1]
			formatIdx += 2

			switch spec {
			case 'Y':
				// 4-digit year
				if inputIdx+4 > len(input) {
					return time.Time{}, false
				}
				val, err := parseDigits(input[inputIdx:inputIdx+4], 4)
				if err != nil {
					return time.Time{}, false
				}
				year = val
				inputIdx += 4

			case 'y':
				// 2-digit year
				if inputIdx+2 > len(input) {
					return time.Time{}, false
				}
				val, err := parseDigits(input[inputIdx:inputIdx+2], 2)
				if err != nil {
					return time.Time{}, false
				}
				// Assume 2000s for 2-digit years
				if val >= 70 {
					year = 1900 + val
				} else {
					year = 2000 + val
				}
				inputIdx += 2

			case 'm':
				// Month (01-12)
				if inputIdx+2 > len(input) {
					return time.Time{}, false
				}
				val, err := parseDigits(input[inputIdx:inputIdx+2], 2)
				if err != nil {
					return time.Time{}, false
				}
				if val < 1 || val > 12 {
					return time.Time{}, false
				}
				month = val
				inputIdx += 2

			case 'd':
				// Day (01-31)
				if inputIdx+2 > len(input) {
					return time.Time{}, false
				}
				val, err := parseDigits(input[inputIdx:inputIdx+2], 2)
				if err != nil {
					return time.Time{}, false
				}
				if val < 1 || val > 31 {
					return time.Time{}, false
				}
				day = val
				inputIdx += 2

			case 'H':
				// Hour 24h (00-23)
				if inputIdx+2 > len(input) {
					return time.Time{}, false
				}
				val, err := parseDigits(input[inputIdx:inputIdx+2], 2)
				if err != nil {
					return time.Time{}, false
				}
				if val < 0 || val > 23 {
					return time.Time{}, false
				}
				hour = val
				inputIdx += 2

			case 'I':
				// Hour 12h (01-12)
				if inputIdx+2 > len(input) {
					return time.Time{}, false
				}
				val, err := parseDigits(input[inputIdx:inputIdx+2], 2)
				if err != nil {
					return time.Time{}, false
				}
				if val < 1 || val > 12 {
					return time.Time{}, false
				}
				hour = val
				has12Hour = true
				inputIdx += 2

			case 'M':
				// Minute (00-59)
				if inputIdx+2 > len(input) {
					return time.Time{}, false
				}
				val, err := parseDigits(input[inputIdx:inputIdx+2], 2)
				if err != nil {
					return time.Time{}, false
				}
				if val < 0 || val > 59 {
					return time.Time{}, false
				}
				minute = val
				inputIdx += 2

			case 'S':
				// Second (00-59)
				if inputIdx+2 > len(input) {
					return time.Time{}, false
				}
				val, err := parseDigits(input[inputIdx:inputIdx+2], 2)
				if err != nil {
					return time.Time{}, false
				}
				if val < 0 || val > 59 {
					return time.Time{}, false
				}
				second = val
				inputIdx += 2

			case 'f':
				// Microseconds (up to 6 digits)
				if inputIdx >= len(input) {
					return time.Time{}, false
				}
				// Parse up to 6 digits
				end := inputIdx
				for end < len(input) && end-inputIdx < 6 && input[end] >= '0' && input[end] <= '9' {
					end++
				}
				if end == inputIdx {
					return time.Time{}, false
				}
				val, err := parseDigits(input[inputIdx:end], end-inputIdx)
				if err != nil {
					return time.Time{}, false
				}
				// Pad to 6 digits
				for i := end - inputIdx; i < 6; i++ {
					val *= 10
				}
				microsecond = val
				inputIdx = end

			case 'p':
				// AM/PM
				if inputIdx+2 > len(input) {
					return time.Time{}, false
				}
				ampm := strings.ToUpper(input[inputIdx : inputIdx+2])
				if ampm == "PM" {
					isPM = true
				} else if ampm != "AM" {
					return time.Time{}, false
				}
				inputIdx += 2

			case 'j':
				// Day of year (001-366) - we skip this for now in parsing
				if inputIdx+3 > len(input) {
					return time.Time{}, false
				}
				_, err := parseDigits(input[inputIdx:inputIdx+3], 3)
				if err != nil {
					return time.Time{}, false
				}
				inputIdx += 3

			case 'W':
				// Week of year (00-53) - we skip this for now in parsing
				if inputIdx+2 > len(input) {
					return time.Time{}, false
				}
				_, err := parseDigits(input[inputIdx:inputIdx+2], 2)
				if err != nil {
					return time.Time{}, false
				}
				inputIdx += 2

			case 'w':
				// Weekday (0-6) - we skip this for now in parsing
				if inputIdx+1 > len(input) {
					return time.Time{}, false
				}
				_, err := parseDigits(input[inputIdx:inputIdx+1], 1)
				if err != nil {
					return time.Time{}, false
				}
				inputIdx++

			case 'a':
				// Abbreviated weekday - skip 3 chars
				if inputIdx+3 > len(input) {
					return time.Time{}, false
				}
				found := false
				for _, name := range weekdayAbbr {
					if strings.EqualFold(input[inputIdx:inputIdx+3], name) {
						found = true

						break
					}
				}
				if !found {
					return time.Time{}, false
				}
				inputIdx += 3

			case 'A':
				// Full weekday - variable length
				found := false
				for _, name := range weekdayFull {
					if len(input) >= inputIdx+len(name) &&
						strings.EqualFold(input[inputIdx:inputIdx+len(name)], name) {
						inputIdx += len(name)
						found = true

						break
					}
				}
				if !found {
					return time.Time{}, false
				}

			case 'b':
				// Abbreviated month - 3 chars
				if inputIdx+3 > len(input) {
					return time.Time{}, false
				}
				found := false
				for m, name := range monthAbbr {
					if m > 0 && strings.EqualFold(input[inputIdx:inputIdx+3], name) {
						month = m
						found = true

						break
					}
				}
				if !found {
					return time.Time{}, false
				}
				inputIdx += 3

			case 'B':
				// Full month - variable length
				found := false
				for m, name := range monthFull {
					if m > 0 && len(input) >= inputIdx+len(name) &&
						strings.EqualFold(input[inputIdx:inputIdx+len(name)], name) {
						month = m
						inputIdx += len(name)
						found = true

						break
					}
				}
				if !found {
					return time.Time{}, false
				}

			case '%':
				// Literal %
				if inputIdx >= len(input) || input[inputIdx] != '%' {
					return time.Time{}, false
				}
				inputIdx++

			default:
				// Unknown specifier - treat as literal
				if inputIdx >= len(input) || input[inputIdx] != spec {
					return time.Time{}, false
				}
				inputIdx++
			}
		} else {
			// Literal character
			if inputIdx >= len(input) || input[inputIdx] != format[formatIdx] {
				return time.Time{}, false
			}
			inputIdx++
			formatIdx++
		}
	}

	// Handle 12-hour time with AM/PM
	if has12Hour {
		if isPM && hour != 12 {
			hour += 12
		} else if !isPM && hour == 12 {
			hour = 0
		}
	}

	// Construct the time
	t := time.Date(year, time.Month(month), day, hour, minute, second, microsecond*1000, time.UTC)
	return t, true
}

// parseDigits parses a string of digits into an integer.
func parseDigits(s string, expectedLen int) (int, error) {
	if len(s) != expectedLen {
		return 0, fmt.Errorf("expected %d digits", expectedLen)
	}
	val := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid digit: %c", c)
		}
		val = val*10 + int(c-'0')
	}
	return val, nil
}

// evalToTimestamp converts epoch seconds to a timestamp.
// TO_TIMESTAMP(seconds) -> TIMESTAMP
func evalToTimestamp(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TO_TIMESTAMP requires exactly 1 argument (seconds)",
		}
	}

	// NULL propagation
	if args[0] == nil {
		return nil, nil
	}

	// Get seconds as float64
	seconds, err := toFloatArg(args[0], "TO_TIMESTAMP", "seconds")
	if err != nil {
		return nil, err
	}

	// Convert to microseconds
	micros := int64(seconds * 1_000_000)
	return micros, nil
}

// evalEpoch converts a timestamp to epoch seconds.
// EPOCH(timestamp) -> DOUBLE
func evalEpoch(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "EPOCH requires exactly 1 argument (timestamp)",
		}
	}

	// NULL propagation
	if args[0] == nil {
		return nil, nil
	}

	// Get the timestamp
	t, err := toTime(args[0])
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("EPOCH: cannot convert timestamp: %v", err),
		}
	}

	// Return epoch seconds as DOUBLE
	return float64(t.UnixMicro()) / 1_000_000, nil
}

// evalEpochMs converts a timestamp to epoch milliseconds.
// EPOCH_MS(timestamp) -> BIGINT
func evalEpochMs(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "EPOCH_MS requires exactly 1 argument (timestamp)",
		}
	}

	// NULL propagation
	if args[0] == nil {
		return nil, nil
	}

	// Get the timestamp
	t, err := toTime(args[0])
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("EPOCH_MS: cannot convert timestamp: %v", err),
		}
	}

	// Return epoch milliseconds as BIGINT
	return t.UnixMilli(), nil
}

// =============================================================================
// Interval Extraction Functions
// =============================================================================

// evalToYears extracts the years component from an interval.
// This extracts the component value (months / 12), not the total.
// TO_YEARS(interval) -> BIGINT
func evalToYears(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TO_YEARS requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Extract years component (months / 12)
	return int64(interval.Months / 12), nil
}

// evalToMonths extracts the months component from an interval.
// This extracts the component value (months % 12), not the total.
// TO_MONTHS(interval) -> BIGINT
func evalToMonths(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TO_MONTHS requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Extract months component (months % 12)
	return int64(interval.Months % 12), nil
}

// evalToDays extracts the days component from an interval.
// This extracts the component value, not the total.
// TO_DAYS(interval) -> BIGINT
func evalToDays(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TO_DAYS requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Extract days component
	return int64(interval.Days), nil
}

// evalToHours extracts the hours component from an interval.
// This extracts the component value (microseconds / (3600 * 1_000_000)), not the total.
// TO_HOURS(interval) -> BIGINT
func evalToHours(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TO_HOURS requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Extract hours component from microseconds
	hoursInMicros := int64(3600 * 1_000_000)
	return interval.Micros / hoursInMicros, nil
}

// evalToMinutes extracts the minutes component from an interval.
// This extracts the component value (remaining after hours), not the total.
// TO_MINUTES(interval) -> BIGINT
func evalToMinutes(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TO_MINUTES requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Extract minutes component (after removing hours)
	hoursInMicros := int64(3600 * 1_000_000)
	minutesInMicros := int64(60 * 1_000_000)
	remainingMicros := interval.Micros % hoursInMicros
	return remainingMicros / minutesInMicros, nil
}

// evalToSeconds extracts the seconds component from an interval.
// This extracts the component value (remaining after minutes), not the total.
// Returns DOUBLE to include fractional seconds.
// TO_SECONDS(interval) -> DOUBLE
func evalToSeconds(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TO_SECONDS requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Extract seconds component (after removing hours and minutes)
	minutesInMicros := int64(60 * 1_000_000)
	remainingMicros := interval.Micros % minutesInMicros
	return float64(remainingMicros) / 1_000_000, nil
}

// =============================================================================
// Total Extraction Functions
// =============================================================================

// evalTotalYears returns the total number of years in an interval.
// This converts the entire interval to years.
// TOTAL_YEARS(interval) -> DOUBLE
func evalTotalYears(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TOTAL_YEARS requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Convert everything to years (approximate: 365.25 days/year, 30.4375 days/month)
	totalDays := float64(interval.Months)*30.4375 + float64(interval.Days) +
		float64(interval.Micros)/(24*60*60*1_000_000)
	return totalDays / 365.25, nil
}

// evalTotalMonths returns the total number of months in an interval.
// This converts the entire interval to months.
// TOTAL_MONTHS(interval) -> DOUBLE
func evalTotalMonths(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TOTAL_MONTHS requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Convert everything to months (approximate: 30.4375 days/month)
	totalDays := float64(interval.Days) + float64(interval.Micros)/(24*60*60*1_000_000)
	return float64(interval.Months) + totalDays/30.4375, nil
}

// evalTotalDays returns the total number of days in an interval.
// This converts the entire interval to days.
// TOTAL_DAYS(interval) -> DOUBLE
func evalTotalDays(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TOTAL_DAYS requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Convert everything to days (30.4375 days/month average)
	return float64(interval.Months)*30.4375 + float64(interval.Days) +
		float64(interval.Micros)/(24*60*60*1_000_000), nil
}

// evalTotalHours returns the total number of hours in an interval.
// This converts the entire interval to hours.
// TOTAL_HOURS(interval) -> DOUBLE
func evalTotalHours(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TOTAL_HOURS requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Convert everything to hours
	totalDays := float64(interval.Months)*30.4375 + float64(interval.Days)
	return totalDays*24 + float64(interval.Micros)/(60*60*1_000_000), nil
}

// evalTotalMinutes returns the total number of minutes in an interval.
// This converts the entire interval to minutes.
// TOTAL_MINUTES(interval) -> DOUBLE
func evalTotalMinutes(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TOTAL_MINUTES requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Convert everything to minutes
	totalDays := float64(interval.Months)*30.4375 + float64(interval.Days)
	return totalDays*24*60 + float64(interval.Micros)/(60*1_000_000), nil
}

// evalTotalSeconds returns the total number of seconds in an interval.
// This converts the entire interval to seconds.
// TOTAL_SECONDS(interval) -> DOUBLE
func evalTotalSeconds(args []any) (any, error) {
	if len(args) != 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "TOTAL_SECONDS requires exactly 1 argument (interval)",
		}
	}

	if args[0] == nil {
		return nil, nil
	}

	interval, err := toInterval(args[0])
	if err != nil {
		return nil, err
	}

	// Convert everything to seconds
	totalDays := float64(interval.Months)*30.4375 + float64(interval.Days)
	return totalDays*24*60*60 + float64(interval.Micros)/1_000_000, nil
}
