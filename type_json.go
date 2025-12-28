package dukdb

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
)

// ParseValue parses JSON data according to the specified DuckDB type.
// This function handles DuckDB CLI JSON output format, including special
// representations for floats (Infinity, -Infinity, NaN), BLOBs (\x hex format),
// and nested types (LIST, STRUCT, MAP, UNION).
func ParseValue(
	data []byte,
	typ Type,
) (any, error) {
	// Handle null JSON value
	if string(data) == "null" {
		return nil, nil
	}

	switch typ {
	case TYPE_BOOLEAN:
		var v bool
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing BOOLEAN: %w",
				err,
			)
		}
		return v, nil

	case TYPE_TINYINT:
		var v int8
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing TINYINT: %w",
				err,
			)
		}
		return v, nil

	case TYPE_SMALLINT:
		var v int16
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing SMALLINT: %w",
				err,
			)
		}
		return v, nil

	case TYPE_INTEGER:
		var v int32
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing INTEGER: %w",
				err,
			)
		}
		return v, nil

	case TYPE_BIGINT:
		var v int64
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing BIGINT: %w",
				err,
			)
		}
		return v, nil

	case TYPE_UTINYINT:
		var v uint8
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing UTINYINT: %w",
				err,
			)
		}
		return v, nil

	case TYPE_USMALLINT:
		var v uint16
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing USMALLINT: %w",
				err,
			)
		}
		return v, nil

	case TYPE_UINTEGER:
		var v uint32
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing UINTEGER: %w",
				err,
			)
		}
		return v, nil

	case TYPE_UBIGINT:
		var v uint64
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing UBIGINT: %w",
				err,
			)
		}
		return v, nil

	case TYPE_FLOAT:
		return parseFloat(data)

	case TYPE_DOUBLE:
		return parseFloat(data)

	case TYPE_HUGEINT:
		// HUGEINT is serialized as a string in JSON due to precision limitations
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			// Try parsing as number
			var v int64
			if err := json.Unmarshal(data, &v); err != nil {
				return nil, fmt.Errorf(
					"parsing HUGEINT: %w",
					err,
				)
			}
			return big.NewInt(v), nil
		}
		i := new(big.Int)
		if _, ok := i.SetString(s, 10); !ok {
			return nil, fmt.Errorf(
				"parsing HUGEINT: invalid value %q",
				s,
			)
		}
		return i, nil

	case TYPE_UHUGEINT:
		// UHUGEINT is serialized as a string in JSON due to precision limitations
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			// Try parsing as number
			var v uint64
			if err := json.Unmarshal(data, &v); err != nil {
				return nil, fmt.Errorf(
					"parsing UHUGEINT: %w",
					err,
				)
			}
			i := new(big.Int)
			i.SetUint64(v)
			return i, nil
		}
		i := new(big.Int)
		if _, ok := i.SetString(s, 10); !ok {
			return nil, fmt.Errorf(
				"parsing UHUGEINT: invalid value %q",
				s,
			)
		}
		return i, nil

	case TYPE_VARCHAR:
		var v string
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing VARCHAR: %w",
				err,
			)
		}
		return v, nil

	case TYPE_BLOB:
		return parseBLOB(data)

	case TYPE_DECIMAL, TYPE_BIGNUM:
		// Decimal values are serialized as strings or numbers
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			// Try parsing as number
			var f float64
			if err := json.Unmarshal(data, &f); err != nil {
				return nil, fmt.Errorf(
					"parsing DECIMAL: %w",
					err,
				)
			}
			return f, nil
		}
		// Return as string, caller can convert to Decimal if needed
		return s, nil

	case TYPE_TIMESTAMP,
		TYPE_TIMESTAMP_S,
		TYPE_TIMESTAMP_MS,
		TYPE_TIMESTAMP_NS,
		TYPE_TIMESTAMP_TZ:
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, fmt.Errorf(
				"parsing TIMESTAMP: %w",
				err,
			)
		}
		// Try parsing various timestamp formats
		t, err := parseTimestamp(s)
		if err != nil {
			return nil, fmt.Errorf(
				"parsing TIMESTAMP: %w",
				err,
			)
		}
		return t, nil

	case TYPE_DATE:
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, fmt.Errorf(
				"parsing DATE: %w",
				err,
			)
		}
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return nil, fmt.Errorf(
				"parsing DATE: %w",
				err,
			)
		}
		return t, nil

	case TYPE_TIME, TYPE_TIME_TZ:
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, fmt.Errorf(
				"parsing TIME: %w",
				err,
			)
		}
		t, err := parseTime(s)
		if err != nil {
			return nil, fmt.Errorf(
				"parsing TIME: %w",
				err,
			)
		}
		return t, nil

	case TYPE_INTERVAL:
		var interval Interval
		if err := json.Unmarshal(data, &interval); err != nil {
			return nil, fmt.Errorf(
				"parsing INTERVAL: %w",
				err,
			)
		}
		return interval, nil

	case TYPE_UUID:
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, fmt.Errorf(
				"parsing UUID: %w",
				err,
			)
		}
		var u UUID
		if err := u.Scan(s); err != nil {
			return nil, fmt.Errorf(
				"parsing UUID: %w",
				err,
			)
		}
		return u, nil

	case TYPE_LIST, TYPE_ARRAY:
		return parseList(data)

	case TYPE_STRUCT:
		return parseStruct(data)

	case TYPE_MAP:
		return parseMap(data)

	case TYPE_UNION:
		return parseUnion(data)

	case TYPE_ENUM:
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, fmt.Errorf(
				"parsing ENUM: %w",
				err,
			)
		}
		return s, nil

	case TYPE_BIT:
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, fmt.Errorf(
				"parsing BIT: %w",
				err,
			)
		}
		return s, nil

	case TYPE_SQLNULL, TYPE_ANY, TYPE_INVALID:
		return nil, nil

	default:
		// For unknown types, try to parse as generic JSON
		var v any
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing unknown type %s: %w",
				typ.String(),
				err,
			)
		}
		return v, nil
	}
}

// parseFloat handles special float values that DuckDB outputs as strings:
// "Infinity", "-Infinity", "NaN"
func parseFloat(data []byte) (float64, error) {
	// First try parsing as string to handle special values
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		switch s {
		case "Infinity":
			return math.Inf(1), nil
		case "-Infinity":
			return math.Inf(-1), nil
		case "NaN":
			return math.NaN(), nil
		}
		// If it's a string but not a special value, this is an error
		return 0, fmt.Errorf(
			"unexpected string value for float: %q",
			s,
		)
	}

	// Try parsing as regular number
	var v float64
	if err := json.Unmarshal(data, &v); err != nil {
		return 0, fmt.Errorf(
			"parsing float: %w",
			err,
		)
	}
	return v, nil
}

// parseBLOB parses a DuckDB BLOB value.
// Input format: "\x48454C4C4F" (hex string with \x prefix)
// Output: decoded bytes
func parseBLOB(data []byte) ([]byte, error) {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf(
			"parsing BLOB: %w",
			err,
		)
	}

	// Check for \x prefix and decode hex
	if strings.HasPrefix(s, "\\x") {
		hexStr := s[2:] // Remove \x prefix
		decoded, err := hex.DecodeString(hexStr)
		if err != nil {
			return nil, fmt.Errorf(
				"parsing BLOB hex: %w",
				err,
			)
		}
		return decoded, nil
	}

	// If no \x prefix, return as raw bytes
	return []byte(s), nil
}

// parseList parses a DuckDB LIST or ARRAY as []any
func parseList(data []byte) ([]any, error) {
	var list []json.RawMessage
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf(
			"parsing LIST: %w",
			err,
		)
	}

	result := make([]any, len(list))
	for i, item := range list {
		var v any
		if err := json.Unmarshal(item, &v); err != nil {
			return nil, fmt.Errorf(
				"parsing LIST element %d: %w",
				i,
				err,
			)
		}
		result[i] = v
	}
	return result, nil
}

// parseStruct parses a DuckDB STRUCT as map[string]any
func parseStruct(
	data []byte,
) (map[string]any, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf(
			"parsing STRUCT: %w",
			err,
		)
	}

	result := make(map[string]any, len(m))
	for k, v := range m {
		var val any
		if err := json.Unmarshal(v, &val); err != nil {
			return nil, fmt.Errorf(
				"parsing STRUCT field %q: %w",
				k,
				err,
			)
		}
		result[k] = val
	}
	return result, nil
}

// parseMap parses a DuckDB MAP.
// DuckDB MAPs can have non-string keys, but JSON only supports string keys,
// so DuckDB serializes MAPs as arrays of {key, value} objects.
func parseMap(data []byte) (Map, error) {
	// Try parsing as array of {key, value} pairs (DuckDB format)
	var pairs []struct {
		Key   json.RawMessage `json:"key"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(data, &pairs); err != nil {
		// Fall back to regular object parsing
		var m map[string]json.RawMessage
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf(
				"parsing MAP: %w",
				err,
			)
		}
		result := make(Map, len(m))
		for k, v := range m {
			var val any
			if err := json.Unmarshal(v, &val); err != nil {
				return nil, fmt.Errorf(
					"parsing MAP value for key %q: %w",
					k,
					err,
				)
			}
			result[k] = val
		}
		return result, nil
	}

	result := make(Map, len(pairs))
	for i, pair := range pairs {
		var key, value any
		if err := json.Unmarshal(pair.Key, &key); err != nil {
			return nil, fmt.Errorf(
				"parsing MAP key %d: %w",
				i,
				err,
			)
		}
		if err := json.Unmarshal(pair.Value, &value); err != nil {
			return nil, fmt.Errorf(
				"parsing MAP value %d: %w",
				i,
				err,
			)
		}
		result[key] = value
	}
	return result, nil
}

// parseUnion parses a DuckDB UNION type.
// Format: {"tag": "member_name", "value": ...}
func parseUnion(data []byte) (Union, error) {
	var raw struct {
		Tag   string          `json:"tag"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return Union{}, fmt.Errorf(
			"parsing UNION: %w",
			err,
		)
	}

	var value any
	if err := json.Unmarshal(raw.Value, &value); err != nil {
		return Union{}, fmt.Errorf(
			"parsing UNION value: %w",
			err,
		)
	}

	return Union{
		Tag:   raw.Tag,
		Value: value,
	}, nil
}

// parseTimestamp attempts to parse a timestamp string in various formats
func parseTimestamp(s string) (time.Time, error) {
	// List of formats to try, in order of preference
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05.999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05.999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999-07:00",
		"2006-01-02 15:04:05-07:00",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf(
		"unrecognized timestamp format: %q",
		s,
	)
}

// parseTime attempts to parse a time string in various formats
func parseTime(s string) (time.Time, error) {
	// List of formats to try
	formats := []string{
		"15:04:05.999999999",
		"15:04:05.999999",
		"15:04:05.999",
		"15:04:05",
		"15:04:05.999999999-07:00",
		"15:04:05.999999-07:00",
		"15:04:05-07:00",
		"15:04:05+07:00",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf(
		"unrecognized time format: %q",
		s,
	)
}
