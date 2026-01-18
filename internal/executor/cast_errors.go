package executor

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/google/uuid"
)

// PostgreSQL SQLSTATE error codes for type cast errors.
// These follow the PostgreSQL error codes specification.
// See: https://www.postgresql.org/docs/current/errcodes-appendix.html
const (
	// SQLStateInvalidTextRepresentation is used for parse errors when converting
	// text to other types (e.g., 'abc' to integer).
	SQLStateInvalidTextRepresentation = "22P02"

	// SQLStateNumericValueOutOfRange is used when a numeric value is out of range
	// for the target type (e.g., 999999999999 to SMALLINT).
	SQLStateNumericValueOutOfRange = "22003"

	// SQLStateInvalidDatetimeFormat is used for invalid date/time format errors.
	SQLStateInvalidDatetimeFormat = "22007"

	// SQLStateStringDataRightTruncation is used when string data would be truncated.
	SQLStateStringDataRightTruncation = "22001"

	// SQLStateDivisionByZero is used for division by zero errors.
	SQLStateDivisionByZero = "22012"

	// SQLStateDataException is a general data exception code.
	SQLStateDataException = "22000"
)

// CastError represents a type cast error with a PostgreSQL SQLSTATE code.
type CastError struct {
	// SQLState is the PostgreSQL SQLSTATE error code
	SQLState string
	// Message is the user-friendly error message
	Message string
	// Detail provides additional detail about the error
	Detail string
	// Hint provides a suggestion for how to fix the problem
	Hint string
}

// Error implements the error interface.
func (e *CastError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("ERROR: %s (SQLSTATE %s)\nDETAIL: %s", e.Message, e.SQLState, e.Detail)
	}
	return fmt.Sprintf("ERROR: %s (SQLSTATE %s)", e.Message, e.SQLState)
}

// GetSQLState returns the SQLSTATE code.
// This method allows CastError to be detected by postgres/server.ToPgError.
func (e *CastError) GetSQLState() string {
	return e.SQLState
}

// GetMessage returns the error message.
func (e *CastError) GetMessage() string {
	return e.Message
}

// GetDetail returns the error detail.
func (e *CastError) GetDetail() string {
	return e.Detail
}

// GetHint returns the error hint.
func (e *CastError) GetHint() string {
	return e.Hint
}

// NewInvalidTextRepresentationError creates an error for invalid text representations.
func NewInvalidTextRepresentationError(typeName, value string) *CastError {
	return &CastError{
		SQLState: SQLStateInvalidTextRepresentation,
		Message:  fmt.Sprintf("invalid input syntax for type %s: \"%s\"", typeName, value),
	}
}

// NewNumericOutOfRangeError creates an error for numeric values out of range.
func NewNumericOutOfRangeError(typeName string, value any) *CastError {
	return &CastError{
		SQLState: SQLStateNumericValueOutOfRange,
		Message:  fmt.Sprintf("%s out of range", typeName),
		Detail:   fmt.Sprintf("Value %v is outside the valid range for %s", value, typeName),
	}
}

// NewInvalidDatetimeFormatError creates an error for invalid date/time formats.
func NewInvalidDatetimeFormatError(typeName, value string) *CastError {
	return &CastError{
		SQLState: SQLStateInvalidDatetimeFormat,
		Message:  fmt.Sprintf("invalid input syntax for type %s: \"%s\"", typeName, value),
		Hint:     "Check the date/time format. Expected formats vary by type.",
	}
}

// castValueWithValidation performs type casting with proper error handling and validation.
// It returns PostgreSQL-compatible errors with SQLSTATE codes for invalid casts.
func castValueWithValidation(v any, targetType dukdb.Type) (any, error) {
	if v == nil {
		return nil, nil
	}

	switch targetType {
	case dukdb.TYPE_BOOLEAN:
		return castToBoolean(v)
	case dukdb.TYPE_TINYINT:
		return castToTinyInt(v)
	case dukdb.TYPE_SMALLINT:
		return castToSmallInt(v)
	case dukdb.TYPE_INTEGER:
		return castToInteger(v)
	case dukdb.TYPE_BIGINT:
		return castToBigInt(v)
	case dukdb.TYPE_UTINYINT:
		return castToUTinyInt(v)
	case dukdb.TYPE_USMALLINT:
		return castToUSmallInt(v)
	case dukdb.TYPE_UINTEGER:
		return castToUInteger(v)
	case dukdb.TYPE_UBIGINT:
		return castToUBigInt(v)
	case dukdb.TYPE_FLOAT:
		return castToFloat(v)
	case dukdb.TYPE_DOUBLE:
		return castToDouble(v)
	case dukdb.TYPE_VARCHAR:
		return toString(v), nil
	case dukdb.TYPE_DATE:
		return castToDate(v)
	case dukdb.TYPE_TIME, dukdb.TYPE_TIME_TZ:
		return castToTime(v)
	case dukdb.TYPE_TIMESTAMP,
		dukdb.TYPE_TIMESTAMP_S,
		dukdb.TYPE_TIMESTAMP_MS,
		dukdb.TYPE_TIMESTAMP_NS,
		dukdb.TYPE_TIMESTAMP_TZ:
		return castToTimestamp(v)
	case dukdb.TYPE_UUID:
		return castToUUID(v)
	case dukdb.TYPE_INTERVAL:
		return castToInterval(v)
	default:
		// For unsupported types, just return the value as-is
		return v, nil
	}
}

// castToBoolean converts a value to boolean with proper error handling.
func castToBoolean(v any) (bool, error) {
	switch val := v.(type) {
	case bool:
		return val, nil
	case int, int8, int16, int32, int64:
		return toInt64Value(v) != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		return toInt64Value(v) != 0, nil
	case float32, float64:
		return toFloat64Value(v) != 0, nil
	case string:
		lower := strings.ToLower(strings.TrimSpace(val))
		switch lower {
		case "true", "t", "yes", "y", "1", "on":
			return true, nil
		case "false", "f", "no", "n", "0", "off":
			return false, nil
		default:
			return false, NewInvalidTextRepresentationError("boolean", val)
		}
	default:
		return false, NewInvalidTextRepresentationError("boolean", fmt.Sprintf("%v", v))
	}
}

// castToTinyInt converts a value to int8 with range checking.
func castToTinyInt(v any) (int8, error) {
	intVal, err := parseToInt64(v, "tinyint")
	if err != nil {
		return 0, err
	}
	if intVal < math.MinInt8 || intVal > math.MaxInt8 {
		return 0, NewNumericOutOfRangeError("tinyint", intVal)
	}
	return int8(intVal), nil
}

// castToSmallInt converts a value to int16 with range checking.
func castToSmallInt(v any) (int16, error) {
	intVal, err := parseToInt64(v, "smallint")
	if err != nil {
		return 0, err
	}
	if intVal < math.MinInt16 || intVal > math.MaxInt16 {
		return 0, NewNumericOutOfRangeError("smallint", intVal)
	}
	return int16(intVal), nil
}

// castToInteger converts a value to int32 with range checking.
func castToInteger(v any) (int32, error) {
	intVal, err := parseToInt64(v, "integer")
	if err != nil {
		return 0, err
	}
	if intVal < math.MinInt32 || intVal > math.MaxInt32 {
		return 0, NewNumericOutOfRangeError("integer", intVal)
	}
	return int32(intVal), nil
}

// castToBigInt converts a value to int64 with proper error handling.
func castToBigInt(v any) (int64, error) {
	return parseToInt64(v, "bigint")
}

// castToUTinyInt converts a value to uint8 with range checking.
func castToUTinyInt(v any) (uint8, error) {
	intVal, err := parseToInt64(v, "utinyint")
	if err != nil {
		return 0, err
	}
	if intVal < 0 || intVal > math.MaxUint8 {
		return 0, NewNumericOutOfRangeError("utinyint", intVal)
	}
	return uint8(intVal), nil
}

// castToUSmallInt converts a value to uint16 with range checking.
func castToUSmallInt(v any) (uint16, error) {
	intVal, err := parseToInt64(v, "usmallint")
	if err != nil {
		return 0, err
	}
	if intVal < 0 || intVal > math.MaxUint16 {
		return 0, NewNumericOutOfRangeError("usmallint", intVal)
	}
	return uint16(intVal), nil
}

// castToUInteger converts a value to uint32 with range checking.
func castToUInteger(v any) (uint32, error) {
	intVal, err := parseToInt64(v, "uinteger")
	if err != nil {
		return 0, err
	}
	if intVal < 0 || intVal > math.MaxUint32 {
		return 0, NewNumericOutOfRangeError("uinteger", intVal)
	}
	return uint32(intVal), nil
}

// castToUBigInt converts a value to uint64 with range checking.
func castToUBigInt(v any) (uint64, error) {
	switch val := v.(type) {
	case uint64:
		return val, nil
	case uint:
		return uint64(val), nil
	case uint32:
		return uint64(val), nil
	case uint16:
		return uint64(val), nil
	case uint8:
		return uint64(val), nil
	case int64:
		if val < 0 {
			return 0, NewNumericOutOfRangeError("ubigint", val)
		}
		return uint64(val), nil
	case int, int32, int16, int8:
		intVal := toInt64Value(v)
		if intVal < 0 {
			return 0, NewNumericOutOfRangeError("ubigint", intVal)
		}
		return uint64(intVal), nil
	case float64:
		if val < 0 || val > float64(math.MaxUint64) {
			return 0, NewNumericOutOfRangeError("ubigint", val)
		}
		return uint64(val), nil
	case float32:
		if val < 0 || val > float32(math.MaxUint64) {
			return 0, NewNumericOutOfRangeError("ubigint", val)
		}
		return uint64(val), nil
	case string:
		// Try parsing as unsigned first
		uval, err := strconv.ParseUint(strings.TrimSpace(val), 10, 64)
		if err == nil {
			return uval, nil
		}
		// Try parsing as signed (for negative error)
		sval, serr := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
		if serr == nil && sval < 0 {
			return 0, NewNumericOutOfRangeError("ubigint", sval)
		}
		return 0, NewInvalidTextRepresentationError("ubigint", val)
	default:
		return 0, NewInvalidTextRepresentationError("ubigint", fmt.Sprintf("%v", v))
	}
}

// castToFloat converts a value to float32 with range checking.
func castToFloat(v any) (float32, error) {
	floatVal, err := parseToFloat64(v, "real")
	if err != nil {
		return 0, err
	}
	// Check for overflow (but allow infinity)
	if !math.IsInf(floatVal, 0) && (floatVal < -math.MaxFloat32 || floatVal > math.MaxFloat32) {
		return 0, NewNumericOutOfRangeError("real", floatVal)
	}
	return float32(floatVal), nil
}

// castToDouble converts a value to float64 with proper error handling.
func castToDouble(v any) (float64, error) {
	return parseToFloat64(v, "double precision")
}

// castToDate converts a value to a date (int32 days since epoch).
func castToDate(v any) (int32, error) {
	switch val := v.(type) {
	case int32:
		return val, nil
	case int64:
		return int32(val), nil
	case int:
		return int32(val), nil
	case time.Time:
		// Convert to days since Unix epoch
		return int32(val.Unix() / 86400), nil
	case string:
		// Try parsing common date formats
		formats := []string{
			"2006-01-02",
			"01/02/2006",
			"02-Jan-2006",
			"January 2, 2006",
			"Jan 2, 2006",
			"2006/01/02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, val); err == nil {
				return int32(t.Unix() / 86400), nil
			}
		}
		return 0, NewInvalidDatetimeFormatError("date", val)
	default:
		return 0, NewInvalidTextRepresentationError("date", fmt.Sprintf("%v", v))
	}
}

// castToTime converts a value to a time (int64 microseconds since midnight).
func castToTime(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int32:
		return int64(val), nil
	case int:
		return int64(val), nil
	case time.Time:
		// Convert to microseconds since midnight
		return int64(val.Hour())*3600000000 + int64(val.Minute())*60000000 +
			int64(val.Second())*1000000 + int64(val.Nanosecond()/1000), nil
	case string:
		// Try parsing common time formats
		formats := []string{
			"15:04:05.999999",
			"15:04:05",
			"15:04",
			"3:04:05 PM",
			"3:04 PM",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, val); err == nil {
				return int64(t.Hour())*3600000000 + int64(t.Minute())*60000000 +
					int64(t.Second())*1000000 + int64(t.Nanosecond()/1000), nil
			}
		}
		return 0, NewInvalidDatetimeFormatError("time", val)
	default:
		return 0, NewInvalidTextRepresentationError("time", fmt.Sprintf("%v", v))
	}
}

// castToTimestamp converts a value to a timestamp (int64 microseconds since epoch).
func castToTimestamp(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int32:
		return int64(val), nil
	case int:
		return int64(val), nil
	case time.Time:
		return val.UnixMicro(), nil
	case string:
		// Try parsing common timestamp formats
		formats := []string{
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05.999999Z",
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05.999999",
			"2006-01-02T15:04:05",
			time.RFC3339,
			time.RFC3339Nano,
			"01/02/2006 15:04:05",
			"02-Jan-2006 15:04:05",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, val); err == nil {
				return t.UnixMicro(), nil
			}
		}
		return 0, NewInvalidDatetimeFormatError("timestamp", val)
	default:
		return 0, NewInvalidTextRepresentationError("timestamp", fmt.Sprintf("%v", v))
	}
}

// castToUUID converts a value to a UUID.
func castToUUID(v any) (any, error) {
	switch val := v.(type) {
	case [16]byte:
		return val, nil
	case []byte:
		if len(val) == 16 {
			var arr [16]byte
			copy(arr[:], val)
			return arr, nil
		}
		// Try parsing as string
		return castToUUID(string(val))
	case string:
		// Validate UUID format
		parsed, err := uuid.Parse(val)
		if err != nil {
			return nil, &CastError{
				SQLState: SQLStateInvalidTextRepresentation,
				Message:  fmt.Sprintf("invalid input syntax for type uuid: \"%s\"", val),
				Hint:     "UUID must be in format xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
			}
		}
		return [16]byte(parsed), nil
	default:
		return nil, NewInvalidTextRepresentationError("uuid", fmt.Sprintf("%v", v))
	}
}

// castToInterval converts a value to an Interval.
func castToInterval(v any) (Interval, error) {
	switch val := v.(type) {
	case Interval:
		return val, nil
	case time.Duration:
		// Convert duration to interval
		micros := val.Microseconds()
		return Interval{Micros: micros}, nil
	case string:
		// Try to parse interval string
		return parseIntervalString(val)
	default:
		return Interval{}, NewInvalidTextRepresentationError("interval", fmt.Sprintf("%v", v))
	}
}

// parseToInt64 parses a value to int64 with proper error handling.
func parseToInt64(v any, typeName string) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case uint64:
		if val > math.MaxInt64 {
			return 0, NewNumericOutOfRangeError(typeName, val)
		}
		return int64(val), nil
	case uint:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint8:
		return int64(val), nil
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return 0, NewNumericOutOfRangeError(typeName, val)
		}
		return int64(val), nil
	case float32:
		if math.IsNaN(float64(val)) || math.IsInf(float64(val), 0) {
			return 0, NewNumericOutOfRangeError(typeName, val)
		}
		return int64(val), nil
	case string:
		trimmed := strings.TrimSpace(val)
		if trimmed == "" {
			return 0, NewInvalidTextRepresentationError(typeName, val)
		}
		// Try parsing as integer
		intVal, err := strconv.ParseInt(trimmed, 10, 64)
		if err == nil {
			return intVal, nil
		}
		// Try parsing as float (common for casts from numeric strings with decimals)
		floatVal, ferr := strconv.ParseFloat(trimmed, 64)
		if ferr == nil {
			return int64(floatVal), nil
		}
		return 0, NewInvalidTextRepresentationError(typeName, val)
	case bool:
		if val {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, NewInvalidTextRepresentationError(typeName, fmt.Sprintf("%v", v))
	}
}

// parseToFloat64 parses a value to float64 with proper error handling.
func parseToFloat64(v any, typeName string) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int8:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case uint:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case uint16:
		return float64(val), nil
	case uint8:
		return float64(val), nil
	case string:
		trimmed := strings.TrimSpace(val)
		if trimmed == "" {
			return 0, NewInvalidTextRepresentationError(typeName, val)
		}
		// Handle special values
		switch strings.ToLower(trimmed) {
		case "nan":
			return math.NaN(), nil
		case "infinity", "inf", "+infinity", "+inf":
			return math.Inf(1), nil
		case "-infinity", "-inf":
			return math.Inf(-1), nil
		}
		floatVal, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, NewInvalidTextRepresentationError(typeName, val)
		}
		return floatVal, nil
	case bool:
		if val {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return 0, NewInvalidTextRepresentationError(typeName, fmt.Sprintf("%v", v))
	}
}

// parseIntervalString parses an interval string like '1 year 2 months 3 days'.
func parseIntervalString(s string) (Interval, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return Interval{}, NewInvalidTextRepresentationError("interval", s)
	}

	var interval Interval

	// Try PostgreSQL verbose format: '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'
	// or ISO 8601 format: 'P1Y2M3DT4H5M6S'

	if strings.HasPrefix(s, "p") {
		// ISO 8601 format
		return parseISO8601Interval(s)
	}

	// Parse PostgreSQL verbose format
	re := regexp.MustCompile(
		`(-?\d+)\s*(years?|mons?|months?|days?|hours?|hrs?|mins?|minutes?|secs?|seconds?|milliseconds?|microseconds?)`,
	)
	matches := re.FindAllStringSubmatch(s, -1)

	if len(matches) == 0 {
		// Try simple HH:MM:SS format
		if strings.Contains(s, ":") {
			return parseTimeInterval(s)
		}
		return Interval{}, NewInvalidTextRepresentationError("interval", s)
	}

	for _, match := range matches {
		val, _ := strconv.ParseInt(match[1], 10, 64)
		unit := match[2]

		switch {
		case strings.HasPrefix(unit, "year"):
			interval.Months += int32(val * 12)
		case strings.HasPrefix(unit, "mon"):
			interval.Months += int32(val)
		case strings.HasPrefix(unit, "day"):
			interval.Days += int32(val)
		case strings.HasPrefix(unit, "hour"), strings.HasPrefix(unit, "hr"):
			interval.Micros += val * 3600000000
		case strings.HasPrefix(unit, "min"):
			interval.Micros += val * 60000000
		case strings.HasPrefix(unit, "sec"):
			interval.Micros += val * 1000000
		case strings.HasPrefix(unit, "millisec"):
			interval.Micros += val * 1000
		case strings.HasPrefix(unit, "microsec"):
			interval.Micros += val
		}
	}

	return interval, nil
}

// parseTimeInterval parses a time-like interval string (HH:MM:SS).
func parseTimeInterval(s string) (Interval, error) {
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return Interval{}, NewInvalidTextRepresentationError("interval", s)
	}

	hours, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return Interval{}, NewInvalidTextRepresentationError("interval", s)
	}

	minutes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return Interval{}, NewInvalidTextRepresentationError("interval", s)
	}

	var seconds float64
	if len(parts) == 3 {
		seconds, err = strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return Interval{}, NewInvalidTextRepresentationError("interval", s)
		}
	}

	micros := hours*3600000000 + minutes*60000000 + int64(seconds*1000000)
	return Interval{Micros: micros}, nil
}

// parseISO8601Interval parses an ISO 8601 duration string (P1Y2M3DT4H5M6S).
func parseISO8601Interval(s string) (Interval, error) {
	var interval Interval

	// Remove 'P' prefix
	s = strings.TrimPrefix(strings.ToUpper(s), "P")

	// Split by 'T' for date and time parts
	parts := strings.SplitN(s, "T", 2)
	datePart := parts[0]
	var timePart string
	if len(parts) > 1 {
		timePart = parts[1]
	}

	// Parse date part
	re := regexp.MustCompile(`(\d+)([YMD])`)
	matches := re.FindAllStringSubmatch(datePart, -1)
	for _, match := range matches {
		val, _ := strconv.ParseInt(match[1], 10, 64)
		switch match[2] {
		case "Y":
			interval.Months += int32(val * 12)
		case "M":
			interval.Months += int32(val)
		case "D":
			interval.Days += int32(val)
		}
	}

	// Parse time part
	if timePart != "" {
		re = regexp.MustCompile(`(\d+(?:\.\d+)?)([HMS])`)
		matches = re.FindAllStringSubmatch(timePart, -1)
		for _, match := range matches {
			val, _ := strconv.ParseFloat(match[1], 64)
			switch match[2] {
			case "H":
				interval.Micros += int64(val * 3600000000)
			case "M":
				interval.Micros += int64(val * 60000000)
			case "S":
				interval.Micros += int64(val * 1000000)
			}
		}
	}

	return interval, nil
}
