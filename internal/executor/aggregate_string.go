// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"strings"
)

// String and list aggregate computation functions.
// These functions implement string/list aggregate operations for dukdb-go.
// All functions handle NULL values by skipping them.

// computeStringAgg concatenates non-NULL string values with a delimiter.
// This is the SQL standard STRING_AGG function.
// Returns nil for empty input (all values are NULL or no values).
// delimiter: the string to place between concatenated values.
func computeStringAgg(values []any, delimiter string) (any, error) {
	var parts []string
	for _, v := range values {
		if v != nil {
			parts = append(parts, toString(v))
		}
	}

	if len(parts) == 0 {
		return nil, nil
	}

	return strings.Join(parts, delimiter), nil
}

// computeGroupConcat is a MySQL-compatible alias for STRING_AGG.
// It concatenates non-NULL string values with a delimiter.
// Returns nil for empty input (all values are NULL or no values).
// delimiter: the string to place between concatenated values (default is comma).
func computeGroupConcat(values []any, delimiter string) (any, error) {
	// GROUP_CONCAT is functionally identical to STRING_AGG
	return computeStringAgg(values, delimiter)
}

// computeList collects all non-NULL values into a list/array.
// This is the SQL ARRAY_AGG / LIST function.
// Returns an empty slice if all values are NULL or no values.
func computeList(values []any) (any, error) {
	var result []any
	for _, v := range values {
		if v != nil {
			result = append(result, v)
		}
	}

	// Return empty list for all-NULL or no input (per SQL standard behavior)
	if result == nil {
		result = []any{}
	}

	return result, nil
}

// computeListDistinct collects only distinct non-NULL values into a list/array.
// This is like LIST but removes duplicates.
// Returns an empty slice if all values are NULL or no values.
func computeListDistinct(values []any) (any, error) {
	seen := make(map[string]bool)
	var result []any

	for _, v := range values {
		if v != nil {
			// Use formatValue to create a comparable key for any type
			key := formatValue(v)
			if !seen[key] {
				seen[key] = true
				result = append(result, v)
			}
		}
	}

	// Return empty list for all-NULL or no input
	if result == nil {
		result = []any{}
	}

	return result, nil
}
