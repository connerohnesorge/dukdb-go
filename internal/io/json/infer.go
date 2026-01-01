// Package json provides JSON and NDJSON file reading and writing capabilities for dukdb-go.
// This file implements type inference for JSON columns.
package json

import (
	"encoding/json"
	"math"
	"strings"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
)

// TypeInferrer performs type inference on JSON column data.
// It samples values from each column and determines the most specific type
// that can represent all sampled values.
type TypeInferrer struct {
	// sampleSize is the maximum number of rows to sample for type inference.
	sampleSize int
	// dateFormat is the expected date format (empty means try all common formats).
	dateFormat string
	// timestampFormat is the expected timestamp format (empty means try all common formats).
	timestampFormat string
}

// NewTypeInferrer creates a new TypeInferrer with the given sample size.
// The dateFormat and timestampFormat are optional format strings for parsing dates.
func NewTypeInferrer(size int, dateFormat, timestampFormat string) *TypeInferrer {
	sampleSize := size
	if sampleSize <= 0 {
		sampleSize = DefaultSampleSize
	}

	return &TypeInferrer{
		sampleSize:      sampleSize,
		dateFormat:      dateFormat,
		timestampFormat: timestampFormat,
	}
}

// InferTypes analyzes sample objects and returns the inferred types for each column.
// Each column is analyzed independently to find the most specific type that
// can represent all non-null values.
func (ti *TypeInferrer) InferTypes(columns []string, samples []map[string]any) []dukdb.Type {
	if len(columns) == 0 {
		return nil
	}

	// Track possible types for each column.
	columnCandidates := make([]typeCandidate, len(columns))
	for i := range columnCandidates {
		columnCandidates[i] = typeCandidate{
			canBoolean:   true,
			canInteger:   true,
			canBigint:    true,
			canDouble:    true,
			canDate:      true,
			canTimestamp: true,
			hasValues:    false,
		}
	}

	// Create column name to index map.
	colIndex := make(map[string]int)
	for i, name := range columns {
		colIndex[name] = i
	}

	// Limit samples to sampleSize.
	rowsToProcess := len(samples)
	if rowsToProcess > ti.sampleSize {
		rowsToProcess = ti.sampleSize
	}

	// Process each sample object.
	for rowIdx := range rowsToProcess {
		obj := samples[rowIdx]
		for colName, value := range obj {
			idx, ok := colIndex[colName]
			if !ok {
				continue
			}

			if value == nil {
				continue
			}

			columnCandidates[idx].hasValues = true
			ti.updateCandidate(&columnCandidates[idx], value)
		}
	}

	// Determine final types based on candidates.
	types := make([]dukdb.Type, len(columns))
	for i := range columns {
		types[i] = columnCandidates[i].resolveType()
	}

	return types
}

// typeCandidate tracks which types are still viable for a column.
type typeCandidate struct {
	canBoolean   bool
	canInteger   bool
	canBigint    bool
	canDouble    bool
	canDate      bool
	canTimestamp bool
	hasValues    bool // whether any non-null values were seen
}

// updateCandidate updates the candidate flags based on a JSON value.
// It handles boolean, float64, string, json.Number, arrays, and maps.
func (ti *TypeInferrer) updateCandidate(tc *typeCandidate, value any) {
	switch v := value.(type) {
	case bool:
		ti.handleBoolValue(tc)
	case float64:
		ti.handleFloat64Value(tc, v)
	case string:
		ti.handleStringValue(tc, v)
	case json.Number:
		ti.handleJSONNumber(tc, v)
	case []any, map[string]any:
		ti.markAsVarcharOnly(tc)
	default:
		ti.markAsVarcharOnly(tc)
	}
}

// handleBoolValue handles a boolean JSON value.
func (*TypeInferrer) handleBoolValue(tc *typeCandidate) {
	tc.canInteger = false
	tc.canBigint = false
	tc.canDouble = false
	tc.canDate = false
	tc.canTimestamp = false
}

// handleFloat64Value handles a float64 JSON value.
func (*TypeInferrer) handleFloat64Value(tc *typeCandidate, v float64) {
	tc.canBoolean = false
	tc.canDate = false
	tc.canTimestamp = false

	// Check if it can be an integer.
	if v != math.Trunc(v) {
		// Has fractional part - not an integer.
		tc.canInteger = false
		tc.canBigint = false
	} else {
		// Integer value - check range.
		if v < math.MinInt32 || v > math.MaxInt32 {
			tc.canInteger = false
		}

		if v < float64(math.MinInt64) || v > float64(math.MaxInt64) {
			tc.canBigint = false
		}
	}
}

// handleStringValue handles a string JSON value.
func (ti *TypeInferrer) handleStringValue(tc *typeCandidate, v string) {
	tc.canBoolean = tc.canBoolean && canParseBooleanStr(v)
	tc.canInteger = tc.canInteger && canParseIntegerStr(v)
	tc.canBigint = tc.canBigint && canParseBigintStr(v)
	tc.canDouble = tc.canDouble && canParseDoubleStr(v)
	tc.canDate = tc.canDate && ti.canParseDateStr(v)
	tc.canTimestamp = tc.canTimestamp && ti.canParseTimestampStr(v)
}

// handleJSONNumber handles a json.Number value.
func (*TypeInferrer) handleJSONNumber(tc *typeCandidate, v json.Number) {
	tc.canBoolean = false
	tc.canDate = false
	tc.canTimestamp = false

	// Try to parse as integer first.
	if intVal, err := v.Int64(); err != nil {
		tc.canInteger = false
		tc.canBigint = false
	} else if intVal < math.MinInt32 || intVal > math.MaxInt32 {
		tc.canInteger = false
	}
}

// markAsVarcharOnly marks a candidate as only valid for VARCHAR.
// Used for arrays, nested objects, and unknown types.
func (*TypeInferrer) markAsVarcharOnly(tc *typeCandidate) {
	tc.canBoolean = false
	tc.canInteger = false
	tc.canBigint = false
	tc.canDouble = false
	tc.canDate = false
	tc.canTimestamp = false
}

// canParseBooleanStr checks if a string can be parsed as a boolean.
func canParseBooleanStr(s string) bool {
	lower := strings.ToLower(strings.TrimSpace(s))
	switch lower {
	case "true", "false", "t", "f", "yes", "no", "y", "n", "1", "0":
		return true
	default:
		return false
	}
}

// canParseIntegerStr checks if a string can be parsed as a 32-bit integer.
func canParseIntegerStr(s string) bool {
	v, ok := tryParseInteger(s)
	if !ok {
		return false
	}

	return v >= math.MinInt32 && v <= math.MaxInt32
}

// canParseBigintStr checks if a string can be parsed as a 64-bit integer.
func canParseBigintStr(s string) bool {
	_, ok := tryParseInteger(s)

	return ok
}

// canParseDoubleStr checks if a string can be parsed as a float64.
func canParseDoubleStr(s string) bool {
	_, ok := tryParseDouble(s)

	return ok
}

// canParseDateStr checks if a string can be parsed as a date.
func (ti *TypeInferrer) canParseDateStr(s string) bool {
	if ti.dateFormat != "" {
		_, err := time.Parse(ti.dateFormat, strings.TrimSpace(s))

		return err == nil
	}

	_, ok := tryParseDate(s)

	return ok
}

// canParseTimestampStr checks if a string can be parsed as a timestamp.
func (ti *TypeInferrer) canParseTimestampStr(s string) bool {
	if ti.timestampFormat != "" {
		_, err := time.Parse(ti.timestampFormat, strings.TrimSpace(s))

		return err == nil
	}

	_, ok := tryParseTimestamp(s)

	return ok
}

// resolveType determines the final type based on candidate flags.
// Priority order: BOOLEAN > INTEGER > BIGINT > DOUBLE > DATE > TIMESTAMP > VARCHAR
func (tc *typeCandidate) resolveType() dukdb.Type {
	// If no non-null values were seen, default to VARCHAR.
	if !tc.hasValues {
		return dukdb.TYPE_VARCHAR
	}

	// Check types in priority order.
	if tc.canBoolean {
		return dukdb.TYPE_BOOLEAN
	}

	if tc.canInteger {
		return dukdb.TYPE_INTEGER
	}

	if tc.canBigint {
		return dukdb.TYPE_BIGINT
	}

	if tc.canDouble {
		return dukdb.TYPE_DOUBLE
	}

	if tc.canDate {
		return dukdb.TYPE_DATE
	}

	if tc.canTimestamp {
		return dukdb.TYPE_TIMESTAMP
	}

	return dukdb.TYPE_VARCHAR
}
