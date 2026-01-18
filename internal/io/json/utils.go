// Package json provides JSON and NDJSON file reading and writing capabilities for dukdb-go.
// This file contains utility types and functions for the JSON package.
// It includes parsing functions for various data types and value conversion utilities.
package json

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
)

// Constants for parsing and formatting.
const (
	// parseIntBase is the base for integer parsing (decimal).
	parseIntBase = 10
	// parseIntBitSize is the bit size for 64-bit integers.
	parseIntBitSize = 64
	// parseFloatBitSize is the bit size for 64-bit floats.
	parseFloatBitSize = 64
	// boolTrue is the string representation of true.
	boolTrue = "true"
	// boolFalse is the string representation of false.
	boolFalse = "false"
)

// combinedCloser closes both a decompressor and underlying file.
// This ensures proper cleanup when reading compressed files.
type combinedCloser struct {
	decompressor io.Closer
	file         io.Closer
}

// Close closes both the decompressor and the underlying file.
// The first error encountered is returned.
func (c *combinedCloser) Close() error {
	var firstErr error

	if c.decompressor != nil {
		if err := c.decompressor.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if c.file != nil {
		if err := c.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// tryParseInteger attempts to parse a string as an int64.
// Returns the parsed value and a boolean indicating success.
func tryParseInteger(s string) (int64, bool) {
	trimmed := strings.TrimSpace(s)
	v, err := strconv.ParseInt(trimmed, parseIntBase, parseIntBitSize)
	if err != nil {
		return 0, false
	}

	return v, true
}

// tryParseDouble attempts to parse a string as a float64.
// Returns the parsed value and a boolean indicating success.
func tryParseDouble(s string) (float64, bool) {
	trimmed := strings.TrimSpace(s)
	v, err := strconv.ParseFloat(trimmed, parseFloatBitSize)
	if err != nil {
		return 0, false
	}

	return v, true
}

// dateFormats lists supported date formats in order of preference.
var dateFormats = []string{
	"2006-01-02",   // ISO 8601 / YYYY-MM-DD
	"01/02/2006",   // US format MM/DD/YYYY
	"02-Jan-2006",  // DD-Mon-YYYY
	"2006/01/02",   // YYYY/MM/DD
	"02/01/2006",   // European format DD/MM/YYYY
	"Jan 02, 2006", // Mon DD, YYYY
	"January 2, 2006",
}

// tryParseDate attempts to parse a string as a date.
// It tries multiple common date formats and returns the first successful parse.
func tryParseDate(s string) (time.Time, bool) {
	trimmed := strings.TrimSpace(s)
	for _, format := range dateFormats {
		t, err := time.Parse(format, trimmed)
		if err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

// timestampFormats lists supported timestamp formats in order of preference.
var timestampFormats = []string{
	time.RFC3339,           // "2006-01-02T15:04:05Z07:00"
	time.RFC3339Nano,       // "2006-01-02T15:04:05.999999999Z07:00"
	"2006-01-02T15:04:05",  // ISO 8601 without timezone
	"2006-01-02 15:04:05",  // Common SQL format
	"2006-01-02T15:04:05Z", // ISO 8601 with Z suffix
	"2006-01-02 15:04:05.000000",
	"2006-01-02 15:04:05.000",
	"01/02/2006 15:04:05", // US format with time
	"02/01/2006 15:04:05", // European format with time
}

// tryParseTimestamp attempts to parse a string as a timestamp.
// It tries multiple common timestamp formats and returns the first successful parse.
func tryParseTimestamp(s string) (time.Time, bool) {
	trimmed := strings.TrimSpace(s)
	for _, format := range timestampFormats {
		t, err := time.Parse(format, trimmed)
		if err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

// tryParseBoolean attempts to parse a string as a boolean.
// Accepts: true/false, t/f, yes/no, y/n, 1/0 (case insensitive).
// Returns the parsed value and a boolean indicating success.
func tryParseBoolean(s string) (parsed, ok bool) {
	lower := strings.ToLower(strings.TrimSpace(s))
	switch lower {
	case boolTrue, "t", "yes", "y", "1":
		return true, true
	case boolFalse, "f", "no", "n", "0":
		return false, true
	default:
		return false, false
	}
}

// convertValue converts a JSON value to the appropriate Go type based on target DuckDB type.
// Returns the converted value and whether conversion succeeded.
//
//nolint:exhaustive // We only handle types that JSON can infer to.
func convertValue(
	value any,
	targetType dukdb.Type,
	dateFormat, timestampFormat string,
) (any, bool) {
	if value == nil {
		return nil, true
	}

	switch targetType {
	case dukdb.TYPE_BOOLEAN:
		return convertToBoolean(value)

	case dukdb.TYPE_INTEGER:
		return convertToInteger(value)

	case dukdb.TYPE_BIGINT:
		return convertToBigint(value)

	case dukdb.TYPE_DOUBLE:
		return convertToDouble(value)

	case dukdb.TYPE_DATE:
		return convertToDate(value, dateFormat)

	case dukdb.TYPE_TIMESTAMP:
		return convertToTimestamp(value, timestampFormat)

	case dukdb.TYPE_VARCHAR:
		return convertToVarchar(value)

	default:
		return convertToVarchar(value)
	}
}

// convertToBoolean converts a value to a boolean.
// Handles bool and string types.
func convertToBoolean(value any) (any, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		return tryParseBoolean(v)
	default:
		return nil, false
	}
}

// convertToInteger converts a value to a 32-bit integer.
// Handles float64, string, and json.Number types.
func convertToInteger(value any) (any, bool) {
	switch v := value.(type) {
	case float64:
		if v != math.Trunc(v) {
			return nil, false
		}

		if v < math.MinInt32 || v > math.MaxInt32 {
			return nil, false
		}

		return int32(v), true
	case string:
		intVal, ok := tryParseInteger(v)
		if !ok {
			return nil, false
		}

		if intVal < math.MinInt32 || intVal > math.MaxInt32 {
			return nil, false
		}

		return int32(intVal), true
	case json.Number:
		intVal, err := v.Int64()
		if err != nil {
			return nil, false
		}

		if intVal < math.MinInt32 || intVal > math.MaxInt32 {
			return nil, false
		}

		return int32(intVal), true
	default:
		return nil, false
	}
}

// convertToBigint converts a value to a 64-bit integer.
// Handles float64, string, and json.Number types.
func convertToBigint(value any) (any, bool) {
	switch v := value.(type) {
	case float64:
		if v != math.Trunc(v) {
			return nil, false
		}

		return int64(v), true
	case string:
		intVal, ok := tryParseInteger(v)
		if !ok {
			return nil, false
		}

		return intVal, true
	case json.Number:
		intVal, err := v.Int64()
		if err != nil {
			return nil, false
		}

		return intVal, true
	default:
		return nil, false
	}
}

// convertToDouble converts a value to a float64.
// Handles float64, string, and json.Number types.
func convertToDouble(value any) (any, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case string:
		return tryParseDouble(v)
	case json.Number:
		floatVal, err := v.Float64()
		if err != nil {
			return nil, false
		}

		return floatVal, true
	default:
		return nil, false
	}
}

// convertToDate converts a value to a date (time.Time).
// Only handles string values.
func convertToDate(value any, dateFormat string) (any, bool) {
	strVal, ok := value.(string)
	if !ok {
		return nil, false
	}

	if dateFormat != "" {
		t, err := time.Parse(dateFormat, strings.TrimSpace(strVal))
		if err != nil {
			return nil, false
		}

		return t, true
	}

	return tryParseDate(strVal)
}

// convertToTimestamp converts a value to a timestamp (time.Time).
// Only handles string values.
func convertToTimestamp(value any, timestampFormat string) (any, bool) {
	strVal, ok := value.(string)
	if !ok {
		return nil, false
	}

	if timestampFormat != "" {
		t, err := time.Parse(timestampFormat, strings.TrimSpace(strVal))
		if err != nil {
			return nil, false
		}

		return t, true
	}

	return tryParseTimestamp(strVal)
}

// convertToVarchar converts a value to a string.
// Handles string, bool, float64, json.Number, arrays, and maps.
// Arrays and maps are serialized as JSON strings.
func convertToVarchar(value any) (any, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case bool:
		if v {
			return boolTrue, true
		}

		return boolFalse, true
	case float64:
		return strconv.FormatFloat(v, 'f', -1, parseFloatBitSize), true
	case json.Number:
		return v.String(), true
	case []any, map[string]any:
		// Serialize arrays and objects as JSON strings.
		bytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v), true
		}

		return string(bytes), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}
