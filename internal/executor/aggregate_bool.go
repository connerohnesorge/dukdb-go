// Package executor provides query execution for the native Go DuckDB implementation.
package executor

// Boolean and bitwise aggregate computation functions.
// These functions implement boolean (BOOL_AND, BOOL_OR, EVERY) and
// bitwise (BIT_AND, BIT_OR, BIT_XOR) aggregate operations for dukdb-go.
// All functions handle NULL values by skipping them and return nil for edge cases.

// computeBoolAnd computes the logical AND of all non-NULL boolean values.
// Returns TRUE if all non-NULL values are TRUE.
// Returns FALSE if any non-NULL value is FALSE.
// Returns nil if all values are NULL or input is empty.
func computeBoolAnd(values []any) (any, error) {
	hasNonNull := false

	for _, v := range values {
		if v == nil {
			continue
		}

		b, ok := toBoolForAggregate(v)
		if !ok {
			// Skip non-boolean values
			continue
		}

		hasNonNull = true
		if !b {
			// Short-circuit: if any value is FALSE, result is FALSE
			return false, nil
		}
	}

	// If no non-NULL values were found, return NULL
	if !hasNonNull {
		return nil, nil
	}

	// All non-NULL values were TRUE
	return true, nil
}

// computeBoolOr computes the logical OR of all non-NULL boolean values.
// Returns TRUE if any non-NULL value is TRUE.
// Returns FALSE if all non-NULL values are FALSE.
// Returns nil if all values are NULL or input is empty.
func computeBoolOr(values []any) (any, error) {
	hasNonNull := false

	for _, v := range values {
		if v == nil {
			continue
		}

		b, ok := toBoolForAggregate(v)
		if !ok {
			// Skip non-boolean values
			continue
		}

		hasNonNull = true
		if b {
			// Short-circuit: if any value is TRUE, result is TRUE
			return true, nil
		}
	}

	// If no non-NULL values were found, return NULL
	if !hasNonNull {
		return nil, nil
	}

	// All non-NULL values were FALSE
	return false, nil
}

// computeBitAnd computes the bitwise AND of all non-NULL integer values.
// Starts with all 1s (-1 in two's complement) and ANDs each value.
// Returns nil if all values are NULL or input is empty.
// Non-integer values are skipped.
func computeBitAnd(values []any) (any, error) {
	hasNonNull := false
	// Start with all 1s (-1 in two's complement is all bits set)
	result := int64(-1)

	for _, v := range values {
		if v == nil {
			continue
		}

		intVal, ok := toInt64ForBitwise(v)
		if !ok {
			// Skip non-integer values
			continue
		}

		hasNonNull = true
		result = result & intVal
	}

	// If no non-NULL integer values were found, return NULL
	if !hasNonNull {
		return nil, nil
	}

	return result, nil
}

// computeBitOr computes the bitwise OR of all non-NULL integer values.
// Starts with 0 and ORs each value.
// Returns nil if all values are NULL or input is empty.
// Non-integer values are skipped.
func computeBitOr(values []any) (any, error) {
	hasNonNull := false
	result := int64(0)

	for _, v := range values {
		if v == nil {
			continue
		}

		intVal, ok := toInt64ForBitwise(v)
		if !ok {
			// Skip non-integer values
			continue
		}

		hasNonNull = true
		result = result | intVal
	}

	// If no non-NULL integer values were found, return NULL
	if !hasNonNull {
		return nil, nil
	}

	return result, nil
}

// computeBitXor computes the bitwise XOR of all non-NULL integer values.
// Starts with 0 and XORs each value.
// Returns nil if all values are NULL or input is empty.
// Non-integer values are skipped.
func computeBitXor(values []any) (any, error) {
	hasNonNull := false
	result := int64(0)

	for _, v := range values {
		if v == nil {
			continue
		}

		intVal, ok := toInt64ForBitwise(v)
		if !ok {
			// Skip non-integer values
			continue
		}

		hasNonNull = true
		result = result ^ intVal
	}

	// If no non-NULL integer values were found, return NULL
	if !hasNonNull {
		return nil, nil
	}

	return result, nil
}

// toBoolForAggregate converts a value to bool for aggregate operations.
// Returns the bool value and a boolean indicating success.
// nil values return (false, false) to allow proper NULL handling.
// This function is named differently from toBool in expr.go to avoid conflicts.
func toBoolForAggregate(v any) (bool, bool) {
	if v == nil {
		return false, false
	}
	switch val := v.(type) {
	case bool:
		return val, true
	case int:
		return val != 0, true
	case int8:
		return val != 0, true
	case int16:
		return val != 0, true
	case int32:
		return val != 0, true
	case int64:
		return val != 0, true
	case uint:
		return val != 0, true
	case uint8:
		return val != 0, true
	case uint16:
		return val != 0, true
	case uint32:
		return val != 0, true
	case uint64:
		return val != 0, true
	case float32:
		return val != 0, true
	case float64:
		return val != 0, true
	default:
		return false, false
	}
}

// toInt64ForBitwise converts a value to int64 for bitwise operations.
// Returns the int64 value and a boolean indicating success.
// nil values return (0, false) to allow proper NULL handling.
// Only integer types are supported; floats and other types return false.
func toInt64ForBitwise(v any) (int64, bool) {
	if v == nil {
		return 0, false
	}
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case uint:
		return int64(val), true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		// Note: This may overflow for values > MaxInt64, but for bitwise ops
		// we want to preserve the bit pattern
		return int64(val), true
	default:
		return 0, false
	}
}
