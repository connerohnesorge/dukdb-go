// Package parquet provides Apache Parquet file reading and writing capabilities for dukdb-go.
// This file contains value conversion functions for the Parquet writer.
package parquet

import (
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	dukdb "github.com/dukdb/dukdb-go"
)

// Time-related constants for value conversions.
const (
	// secondsPerHour is the number of seconds in an hour (60 minutes * 60 seconds).
	secondsPerHour = 3600
	// secondsPerMinute is the number of seconds in a minute.
	secondsPerMinute = 60
)

// convertValueToParquet converts a DuckDB value to a Parquet-compatible value.
// This function handles the type-specific conversions needed when writing values
// to Parquet format, including integer normalization, time value conversion,
// and handling of special types like UUID and Decimal.
//
//nolint:exhaustive,cyclop,revive // Type dispatch function - complexity is inherent in type handling.
func convertValueToParquet(value any, typ dukdb.Type) any {
	if value == nil {
		return nil
	}

	switch typ {
	case dukdb.TYPE_BOOLEAN:
		if b, ok := value.(bool); ok {
			return b
		}

	case dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER,
		dukdb.TYPE_UTINYINT, dukdb.TYPE_USMALLINT:
		return convertToInt32(value)

	case dukdb.TYPE_BIGINT, dukdb.TYPE_UINTEGER, dukdb.TYPE_UBIGINT:
		return convertToInt64(value)

	case dukdb.TYPE_FLOAT:
		return convertToFloat32(value)

	case dukdb.TYPE_DOUBLE:
		return convertToFloat64(value)

	case dukdb.TYPE_VARCHAR:
		if s, ok := value.(string); ok {
			return s
		}

		return fmt.Sprint(value)

	case dukdb.TYPE_BLOB:
		if b, ok := value.([]byte); ok {
			return b
		}

	case dukdb.TYPE_DATE:
		return convertDateValue(value)

	case dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_TZ:
		return convertTimestampMicros(value)

	case dukdb.TYPE_TIMESTAMP_MS:
		return convertTimestampMillis(value)

	case dukdb.TYPE_TIMESTAMP_NS:
		return convertTimestampNanos(value)

	case dukdb.TYPE_TIME, dukdb.TYPE_TIME_TZ:
		return convertTimeValue(value)

	case dukdb.TYPE_UUID:
		return convertUUIDValue(value)

	case dukdb.TYPE_DECIMAL:
		return convertDecimalValue(value)

	case dukdb.TYPE_INTERVAL:
		return convertIntervalValue(value)
	}

	return value
}

// convertToFloat32 converts a value to float32 for Parquet FLOAT type.
// Accepts both float32 and float64 inputs, converting float64 to float32.
func convertToFloat32(value any) any {
	if f, ok := value.(float32); ok {
		return f
	}

	if f, ok := value.(float64); ok {
		return float32(f)
	}

	return value
}

// convertToFloat64 converts a value to float64 for Parquet DOUBLE type.
// Accepts both float32 and float64 inputs, promoting float32 to float64.
func convertToFloat64(value any) any {
	if f, ok := value.(float64); ok {
		return f
	}

	if f, ok := value.(float32); ok {
		return float64(f)
	}

	return value
}

// convertDateValue converts a time.Time to Parquet DATE format.
// Parquet stores DATE as int32 representing days since Unix epoch (1970-01-01).
func convertDateValue(value any) any {
	if t, ok := value.(time.Time); ok {
		return int32(t.Unix() / secondsPerDay)
	}

	return value
}

// convertTimestampMicros converts a time.Time to Parquet TIMESTAMP(MICROS) format.
// Returns microseconds since Unix epoch as int64.
func convertTimestampMicros(value any) any {
	if t, ok := value.(time.Time); ok {
		return t.UnixMicro()
	}

	return value
}

// convertTimestampMillis converts a time.Time to Parquet TIMESTAMP(MILLIS) format.
// Returns milliseconds since Unix epoch as int64.
func convertTimestampMillis(value any) any {
	if t, ok := value.(time.Time); ok {
		return t.UnixMilli()
	}

	return value
}

// convertTimestampNanos converts a time.Time to Parquet TIMESTAMP(NANOS) format.
// Returns nanoseconds since Unix epoch as int64.
func convertTimestampNanos(value any) any {
	if t, ok := value.(time.Time); ok {
		return t.UnixNano()
	}

	return value
}

// convertTimeValue converts a time.Time to Parquet TIME format.
// Parquet stores TIME as int64 representing microseconds since midnight.
// Only the time-of-day component is used; the date is ignored.
func convertTimeValue(value any) any {
	if t, ok := value.(time.Time); ok {
		seconds := int64(t.Hour())*secondsPerHour + int64(t.Minute())*secondsPerMinute + int64(t.Second())

		return seconds*microsecondsPerSecond + int64(t.Nanosecond())/nanosecondsPerMicrosecond
	}

	return value
}

// convertUUIDValue converts a dukdb.UUID to a byte slice for Parquet.
// Parquet stores UUID as FIXED_LEN_BYTE_ARRAY(16).
func convertUUIDValue(value any) any {
	if uuid, ok := value.(dukdb.UUID); ok {
		return uuid[:]
	}

	return value
}

// convertDecimalValue converts a dukdb.Decimal to float64 for Parquet.
// This provides a simple representation; precise decimal handling may require
// different approaches depending on the use case.
func convertDecimalValue(value any) any {
	if decimal, ok := value.(dukdb.Decimal); ok {
		return decimal.Float64()
	}

	return value
}

// convertIntervalValue converts a dukdb.Interval to a JSON string for Parquet.
// Since Parquet doesn't have a native interval type, we serialize as JSON
// containing months, days, and microseconds components.
func convertIntervalValue(value any) any {
	if interval, ok := value.(dukdb.Interval); ok {
		return fmt.Sprintf(`{"months":%d,"days":%d,"micros":%d}`,
			interval.Months, interval.Days, interval.Micros)
	}

	return value
}

// convertToInt32 converts various integer types to int32 for Parquet INT32 type.
// Handles int8, int16, int32, int64, int, uint8, uint16, and uint32.
// Returns 0 for unrecognized types.
func convertToInt32(value any) int32 {
	switch v := value.(type) {
	case int8:
		return int32(v)
	case int16:
		return int32(v)
	case int32:
		return v
	case int64:
		return int32(v)
	case int:
		return int32(v)
	case uint8:
		return int32(v)
	case uint16:
		return int32(v)
	case uint32:
		return int32(v)
	default:
		return 0
	}
}

// convertToInt64 converts various integer types to int64 for Parquet INT64 type.
// Handles int8, int16, int32, int64, int, uint8, uint16, uint32, and uint64.
// Returns 0 for unrecognized types.
func convertToInt64(value any) int64 {
	switch v := value.(type) {
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	default:
		return 0
	}
}

// toExportedName converts a column name to a valid exported Go identifier.
// This is needed because parquet-go uses reflection and requires exported fields.
// The function capitalizes the first letter and replaces invalid characters
// with underscores. Returns "Column" for empty input.
func toExportedName(name string) string {
	if name == "" {
		return "Column"
	}

	runes := []rune(name)
	firstUpper, _ := utf8.DecodeRuneInString(strings.ToUpper(string(runes[0])))
	runes[0] = firstUpper

	result := make([]rune, 0, len(runes))
	for i, r := range runes {
		// Allow letters, digits (except first position), and underscores.
		// Replace other characters with underscores.
		if r == '_' || unicode.IsLetter(r) || (i > 0 && unicode.IsDigit(r)) {
			result = append(result, r)
		} else {
			result = append(result, '_')
		}
	}

	return string(result)
}

// duckDBTypeToGoType returns the Go reflect.Type corresponding to a DuckDB type.
// This mapping is used to build dynamic struct types for Parquet schema generation.
// The mapping follows Parquet type conventions:
//   - Boolean types map to bool
//   - Small integers (TINYINT, SMALLINT, INTEGER) map to int32
//   - Large integers (BIGINT) map to int64
//   - Float types map to float32/float64
//   - String types (VARCHAR) map to string
//   - Binary types (BLOB) map to []byte
//   - Date/Time types map to their Parquet numeric representations
//   - UUID maps to [16]byte
//   - Unknown types default to string
//
//nolint:exhaustive // We handle common types; others default to string.
func duckDBTypeToGoType(typ dukdb.Type) reflect.Type {
	switch typ {
	case dukdb.TYPE_BOOLEAN:
		return reflect.TypeOf(false)
	case dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER,
		dukdb.TYPE_UTINYINT, dukdb.TYPE_USMALLINT:
		return reflect.TypeOf(int32(0))
	case dukdb.TYPE_BIGINT, dukdb.TYPE_UINTEGER, dukdb.TYPE_UBIGINT:
		return reflect.TypeOf(int64(0))
	case dukdb.TYPE_FLOAT:
		return reflect.TypeOf(float32(0))
	case dukdb.TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case dukdb.TYPE_VARCHAR:
		return reflect.TypeOf("")
	case dukdb.TYPE_BLOB:
		return reflect.TypeOf([]byte(nil))
	case dukdb.TYPE_DATE:
		return reflect.TypeOf(int32(0))
	case dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_TZ,
		dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS:
		return reflect.TypeOf(int64(0))
	case dukdb.TYPE_UUID:
		return reflect.TypeOf([16]byte{})
	default:
		return reflect.TypeOf("")
	}
}

// generateColumnNames creates default column names for a DataChunk.
// Names are generated in the format "column0", "column1", etc.
// This is used when column names are not explicitly provided.
func generateColumnNames(count int) []string {
	columns := make([]string, count)
	for i := range count {
		columns[i] = fmt.Sprintf("column%d", i)
	}

	return columns
}
