// Package parquet provides Apache Parquet file reading and writing capabilities for dukdb-go.
// This file contains value conversion utilities for Parquet data.
package parquet

import (
	"encoding/json"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/parquet-go/parquet-go"
)

// Constants for time conversion.
const (
	// microsecondsPerSecond is the number of microseconds in a second.
	microsecondsPerSecond = 1_000_000
	// nanosecondsPerMicrosecond is the number of nanoseconds in a microsecond.
	nanosecondsPerMicrosecond = 1000
	// millisecondsPerSecond is the number of milliseconds in a second.
	millisecondsPerSecond = 1000
	// nanosecondsPerMillisecond is the number of nanoseconds in a millisecond.
	nanosecondsPerMillisecond = 1_000_000
	// secondsPerDay is the number of seconds in a day.
	secondsPerDay = 86400
	// uuidByteLength is the byte length of a UUID value.
	uuidByteLength = 16
)

// convertParquetValue converts a Parquet value to a Go value based on field type.
// For nested types (LIST, MAP, STRUCT), this returns a JSON string representation.
func convertParquetValue(v parquet.Value, field parquet.Field) any {
	if v.IsNull() {
		return nil
	}

	// Check if this is a nested type that needs special handling.
	if isNestedType(field) {
		return convertNestedValue(v, field)
	}

	duckType := parquetTypeToDuckDB(field)

	return convertValueToDuckDB(v, duckType)
}

// convertNestedValue converts a nested Parquet value (LIST, MAP, STRUCT) to a JSON string.
// This handles the conversion of complex nested types to a serialized VARCHAR representation.
func convertNestedValue(v parquet.Value, field parquet.Field) any {
	// Extract the nested structure and serialize to JSON.
	goValue := extractNestedGoValue(v, field)
	if goValue == nil {
		return nil
	}

	jsonBytes, err := json.Marshal(goValue)
	if err != nil {
		// If JSON marshaling fails, return a string representation.
		return "{}"
	}

	return string(jsonBytes)
}

// extractNestedGoValue extracts a nested Parquet value to a Go value.
// This recursively processes nested structures to create a Go representation
// that can be serialized to JSON.
func extractNestedGoValue(v parquet.Value, field parquet.Field) any {
	if v.IsNull() {
		return nil
	}

	// Detect the type of nested structure.
	if field.Type() != nil {
		lt := field.Type().LogicalType()
		if lt != nil {
			if lt.List != nil {
				return extractListValue(v)
			}

			if lt.Map != nil {
				return extractMapValue(v)
			}
		}
	}

	// Check for repeated fields (array/list).
	if field.Repeated() {
		return extractListValue(v)
	}

	// Check for group (struct).
	if field.Type() == nil && len(field.Fields()) > 0 {
		return extractStructValue(v, field)
	}

	// Fallback to basic value extraction.
	return extractBasicValue(v)
}

// extractListValue extracts a Parquet list value to a Go slice.
func extractListValue(v parquet.Value) any {
	// For list values, we try to extract elements.
	// Parquet lists have a specific structure:
	// list (repeated group) -> element
	result := make([]any, 0)

	// If the value itself is a byte array, try to parse it.
	if ba := v.ByteArray(); ba != nil {
		// Try to unmarshal as JSON array if it looks like one.
		var arr []any
		if err := json.Unmarshal(ba, &arr); err == nil {
			return arr
		}

		// Otherwise return the raw string.
		return string(ba)
	}

	// For primitive types, return as single-element array.
	basicVal := extractBasicValue(v)
	if basicVal != nil {
		result = append(result, basicVal)
	}

	return result
}

// extractMapValue extracts a Parquet map value to a Go map.
func extractMapValue(v parquet.Value) any {
	// For map values, try to extract key-value pairs.
	result := make(map[string]any)

	// If the value is a byte array, try to parse it as JSON.
	if ba := v.ByteArray(); ba != nil {
		var m map[string]any
		if err := json.Unmarshal(ba, &m); err == nil {
			return m
		}

		// If it's not a valid JSON object, return empty map.
		return result
	}

	return result
}

// extractStructValue extracts a Parquet struct value to a Go map.
func extractStructValue(v parquet.Value, field parquet.Field) any {
	result := make(map[string]any)

	// If the value is a byte array, try to parse it as JSON.
	if ba := v.ByteArray(); ba != nil {
		var m map[string]any
		if err := json.Unmarshal(ba, &m); err == nil {
			return m
		}
	}

	// Try to extract field values.
	fields := field.Fields()
	for _, f := range fields {
		fieldName := f.Name()
		// For struct fields, we'd need the child values which aren't directly accessible
		// from a single parquet.Value. This is a simplified implementation.
		result[fieldName] = nil
	}

	return result
}

// extractBasicValue extracts a basic (non-nested) Parquet value to a Go value.
func extractBasicValue(v parquet.Value) any {
	if v.IsNull() {
		return nil
	}

	switch v.Kind() {
	case parquet.Boolean:
		return v.Boolean()
	case parquet.Int32:
		return v.Int32()
	case parquet.Int64:
		return v.Int64()
	case parquet.Float:
		return v.Float()
	case parquet.Double:
		return v.Double()
	case parquet.ByteArray:
		return string(v.ByteArray())
	case parquet.FixedLenByteArray:
		return v.ByteArray()
	default:
		return nil
	}
}

// convertValueToDuckDB converts a Parquet value to the appropriate Go type for DuckDB.
// This is a type dispatch function that handles all supported Parquet types.
//
//nolint:exhaustive,cyclop,revive // Type dispatch function - complexity is inherent.
func convertValueToDuckDB(v parquet.Value, targetType dukdb.Type) any {
	if v.IsNull() {
		return nil
	}

	switch targetType {
	case dukdb.TYPE_BOOLEAN:
		return v.Boolean()

	case dukdb.TYPE_TINYINT:
		return int8(v.Int32())

	case dukdb.TYPE_SMALLINT:
		return int16(v.Int32())

	case dukdb.TYPE_INTEGER:
		return v.Int32()

	case dukdb.TYPE_BIGINT:
		return v.Int64()

	case dukdb.TYPE_UTINYINT:
		return uint8(v.Int32())

	case dukdb.TYPE_USMALLINT:
		return uint16(v.Int32())

	case dukdb.TYPE_UINTEGER:
		return uint32(v.Int32())

	case dukdb.TYPE_UBIGINT:
		return uint64(v.Int64())

	case dukdb.TYPE_FLOAT:
		return v.Float()

	case dukdb.TYPE_DOUBLE:
		return v.Double()

	case dukdb.TYPE_VARCHAR:
		return string(v.ByteArray())

	case dukdb.TYPE_BLOB:
		return v.ByteArray()

	case dukdb.TYPE_DATE:
		return convertDate(v)

	case dukdb.TYPE_TIME:
		return convertTime(v)

	case dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_TZ:
		return convertTimestamp(v)

	case dukdb.TYPE_TIMESTAMP_MS:
		return convertTimestampMS(v)

	case dukdb.TYPE_TIMESTAMP_NS:
		return convertTimestampNS(v)

	case dukdb.TYPE_UUID:
		return convertUUID(v)

	case dukdb.TYPE_DECIMAL:
		// For now, return as float64 - proper decimal handling in later task
		return v.Double()

	default:
		// Fallback to string representation
		return string(v.ByteArray())
	}
}

// convertDate converts a Parquet date value (days since epoch) to time.Time.
func convertDate(v parquet.Value) time.Time {
	days := v.Int32()

	return time.Unix(int64(days)*secondsPerDay, 0).UTC()
}

// convertTime converts a Parquet time value (microseconds since midnight) to time.Duration.
func convertTime(v parquet.Value) time.Duration {
	micros := v.Int64()

	return time.Duration(micros) * time.Microsecond
}

// convertTimestamp converts a Parquet timestamp (microseconds since epoch) to time.Time.
func convertTimestamp(v parquet.Value) time.Time {
	micros := v.Int64()
	secs := micros / microsecondsPerSecond
	nanos := (micros % microsecondsPerSecond) * nanosecondsPerMicrosecond

	return time.Unix(secs, nanos).UTC()
}

// convertTimestampMS converts a Parquet timestamp (milliseconds since epoch) to time.Time.
func convertTimestampMS(v parquet.Value) time.Time {
	millis := v.Int64()
	secs := millis / millisecondsPerSecond
	nanos := (millis % millisecondsPerSecond) * nanosecondsPerMillisecond

	return time.Unix(secs, nanos).UTC()
}

// convertTimestampNS converts a Parquet timestamp (nanoseconds since epoch) to time.Time.
func convertTimestampNS(v parquet.Value) time.Time {
	nanos := v.Int64()

	return time.Unix(0, nanos).UTC()
}

// convertUUID converts a Parquet UUID value to dukdb.UUID or []byte.
func convertUUID(v parquet.Value) any {
	data := v.ByteArray()
	if len(data) == uuidByteLength {
		var uuid dukdb.UUID
		copy(uuid[:], data)

		return uuid
	}

	return data
}
