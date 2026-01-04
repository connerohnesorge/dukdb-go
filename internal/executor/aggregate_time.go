// Package executor provides query execution for the native Go DuckDB implementation.
package executor

// Time series aggregate computation functions.
// These functions implement time series aggregate operations for dukdb-go.
// All functions handle NULL values by skipping them and return nil for edge cases.

// computeCountIf counts the number of rows where the condition is true.
// NULL conditions are treated as FALSE.
// Returns 0 for empty input.
func computeCountIf(values []any) (any, error) {
	count := int64(0)
	for _, v := range values {
		if v != nil {
			// Check if value is a boolean true
			if b, ok := v.(bool); ok && b {
				count++
			}
		}
	}
	return count, nil
}

// computeFirst returns the first non-NULL value in the group.
// Returns nil if all values are NULL or if the input is empty.
func computeFirst(values []any) (any, error) {
	for _, v := range values {
		if v != nil {
			return v, nil
		}
	}
	return nil, nil
}

// computeLast returns the last non-NULL value in the group.
// Returns nil if all values are NULL or if the input is empty.
func computeLast(values []any) (any, error) {
	var last any
	for _, v := range values {
		if v != nil {
			last = v
		}
	}
	return last, nil
}

// computeArgmin returns the arg value where val has the minimum value.
// Takes two slices: argValues (values to return) and valValues (values to compare).
// Skips rows where either arg or val is NULL.
// Returns nil if all rows have NULL values or if input is empty.
func computeArgmin(argValues []any, valValues []any) (any, error) {
	if len(argValues) != len(valValues) {
		return nil, nil
	}

	var minArg any
	var minVal any
	hasValue := false

	for i := 0; i < len(argValues); i++ {
		arg := argValues[i]
		val := valValues[i]

		// Skip rows where either arg or val is NULL
		if arg == nil || val == nil {
			continue
		}

		if !hasValue {
			minArg = arg
			minVal = val
			hasValue = true
		} else if compareValues(val, minVal) < 0 {
			minArg = arg
			minVal = val
		}
	}

	if !hasValue {
		return nil, nil
	}
	return minArg, nil
}

// computeArgmax returns the arg value where val has the maximum value.
// Takes two slices: argValues (values to return) and valValues (values to compare).
// Skips rows where either arg or val is NULL.
// Returns nil if all rows have NULL values or if input is empty.
func computeArgmax(argValues []any, valValues []any) (any, error) {
	if len(argValues) != len(valValues) {
		return nil, nil
	}

	var maxArg any
	var maxVal any
	hasValue := false

	for i := 0; i < len(argValues); i++ {
		arg := argValues[i]
		val := valValues[i]

		// Skip rows where either arg or val is NULL
		if arg == nil || val == nil {
			continue
		}

		if !hasValue {
			maxArg = arg
			maxVal = val
			hasValue = true
		} else if compareValues(val, maxVal) > 0 {
			maxArg = arg
			maxVal = val
		}
	}

	if !hasValue {
		return nil, nil
	}
	return maxArg, nil
}
