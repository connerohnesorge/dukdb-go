// Package json provides JSON path extraction utilities for the dukdb-go JSON type.
package json

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ExtractJSONPath extracts a value from JSON using DuckDB-style path syntax.
// Supports:
//   - $.field or field - object field access
//   - $[0] or [0] - array index access
//   - Chained: $.field.nested, $.array[0].field
//
// Returns nil if the path doesn't exist or JSON is invalid.
func ExtractJSONPath(jsonStr string, path string) (any, error) {
	if jsonStr == "" {
		return nil, nil
	}

	// Parse the JSON
	var data any
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	// Parse and apply the path
	return extractPath(data, path)
}

// ExtractJSONPathString extracts a value from JSON and converts to string.
// Returns empty string if path doesn't exist or value is null.
func ExtractJSONPathString(jsonStr string, path string) (string, error) {
	result, err := ExtractJSONPath(jsonStr, path)
	if err != nil {
		return "", err
	}

	return valueToString(result), nil
}

// extractPath applies a path to parsed JSON data.
func extractPath(data any, path string) (any, error) {
	if path == "" || path == "$" {
		return data, nil
	}

	// Normalize path - remove leading $ if present
	path = strings.TrimPrefix(path, "$")

	// Parse path segments
	segments, err := parsePathSegments(path)
	if err != nil {
		return nil, err
	}

	// Apply each segment
	current := data
	for _, seg := range segments {
		current, err = applySegment(current, seg)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, nil
		}
	}

	return current, nil
}

// pathSegment represents a single path component.
type pathSegment struct {
	isIndex bool
	index   int
	field   string
}

// parsePathSegments parses a JSON path into segments.
// Examples: ".field", "[0]", ".nested.field", "[0].name"
func parsePathSegments(path string) ([]pathSegment, error) {
	var segments []pathSegment

	i := 0
	for i < len(path) {
		switch path[i] {
		case '.':
			// Field access
			i++ // skip dot
			if i >= len(path) {
				return nil, fmt.Errorf("unexpected end of path after '.'")
			}
			// Read field name
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			if start == i {
				return nil, fmt.Errorf("empty field name in path")
			}
			segments = append(segments, pathSegment{
				isIndex: false,
				field:   path[start:i],
			})

		case '[':
			// Array index access
			i++ // skip [
			start := i
			for i < len(path) && path[i] != ']' {
				i++
			}
			if i >= len(path) {
				return nil, fmt.Errorf("unclosed bracket in path")
			}
			indexStr := path[start:i]
			i++ // skip ]

			// Check if it's a quoted string (field access) or number (array index)
			if len(indexStr) >= 2 && (indexStr[0] == '"' || indexStr[0] == '\'') {
				// Quoted field name
				quote := indexStr[0]
				if indexStr[len(indexStr)-1] != quote {
					return nil, fmt.Errorf("mismatched quotes in path")
				}
				segments = append(segments, pathSegment{
					isIndex: false,
					field:   indexStr[1 : len(indexStr)-1],
				})
			} else {
				// Array index
				index, err := strconv.Atoi(indexStr)
				if err != nil {
					return nil, fmt.Errorf("invalid array index: %s", indexStr)
				}
				segments = append(segments, pathSegment{
					isIndex: true,
					index:   index,
				})
			}

		default:
			// Direct field access without leading dot (e.g., "field" or "field.nested")
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			if start < i {
				segments = append(segments, pathSegment{
					isIndex: false,
					field:   path[start:i],
				})
			}
		}
	}

	return segments, nil
}

// applySegment applies a single path segment to data.
func applySegment(data any, seg pathSegment) (any, error) {
	if data == nil {
		return nil, nil
	}

	if seg.isIndex {
		// Array index access
		arr, ok := data.([]any)
		if !ok {
			return nil, nil // Not an array, return nil
		}
		if seg.index < 0 || seg.index >= len(arr) {
			return nil, nil // Out of bounds
		}
		return arr[seg.index], nil
	}

	// Field access
	obj, ok := data.(map[string]any)
	if !ok {
		return nil, nil // Not an object, return nil
	}
	return obj[seg.field], nil
}

// valueToString converts a JSON value to string representation.
func valueToString(val any) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case string:
		return v
	case float64:
		// Format numbers nicely
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case []any, map[string]any:
		// For complex types, return JSON representation
		bytes, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(bytes)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ValueToJSON converts a Go value back to JSON string.
// Used for the -> operator which returns JSON.
func ValueToJSON(val any) (string, error) {
	if val == nil {
		return "null", nil
	}

	bytes, err := json.Marshal(val)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// IsValidJSON checks if a string is valid JSON.
func IsValidJSON(s string) bool {
	var js any
	return json.Unmarshal([]byte(s), &js) == nil
}

// ExtractByKey extracts a value from JSON by a single key.
// This is used for the simple -> 'key' or ->> 'key' operators.
// Supports JSONPath syntax: $.field, $.a.b.c, $[0], etc.
func ExtractByKey(jsonStr string, key string) (any, error) {
	if jsonStr == "" {
		return nil, nil
	}

	var data any
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	// Handle JSONPath syntax (starts with $ or $.)
	if strings.HasPrefix(key, "$") {
		return extractPath(data, key)
	}

	switch d := data.(type) {
	case map[string]any:
		return d[key], nil
	case []any:
		// Try to parse key as index
		idx, err := strconv.Atoi(key)
		if err != nil {
			return nil, nil
		}
		if idx < 0 || idx >= len(d) {
			return nil, nil
		}
		return d[idx], nil
	default:
		return nil, nil
	}
}

// ExtractByIndex extracts a value from JSON array by index.
// This is used for the -> 0 or ->> 0 operators.
func ExtractByIndex(jsonStr string, index int) (any, error) {
	if jsonStr == "" {
		return nil, nil
	}

	var data any
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	arr, ok := data.([]any)
	if !ok {
		return nil, nil
	}

	if index < 0 || index >= len(arr) {
		return nil, nil
	}

	return arr[index], nil
}
