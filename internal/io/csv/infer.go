// Package csv provides CSV file reading and writing capabilities for dukdb-go.
// This file implements type inference for CSV columns.
package csv

import (
	"math"
	"strconv"
	"strings"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
)

// Constants for integer parsing.
const (
	// Base 10 for integer parsing.
	parseIntBase = 10
	// Bit size for 64-bit integers.
	parseIntBitSize = 64
	// Bit size for 64-bit floats.
	parseFloatBitSize = 64
)

// TypeInferrer performs type inference on CSV column data.
// It samples values from each column and determines the most specific type
// that can represent all sampled values.
type TypeInferrer struct {
	// sampleSize is the maximum number of rows to sample for type inference.
	sampleSize int
	// columnTypes holds the inferred type per column.
	columnTypes []dukdb.Type
}

// NewTypeInferrer creates a new TypeInferrer with the given sample size.
// If sampleSize is <= 0, DefaultSampleSize (1000) is used.
func NewTypeInferrer(size int) *TypeInferrer {
	sampleSize := size
	if sampleSize <= 0 {
		sampleSize = DefaultSampleSize
	}

	return &TypeInferrer{
		sampleSize:  sampleSize,
		columnTypes: nil,
	}
}

// InferTypes analyzes sample rows and returns the inferred types for each column.
// The inference strategy is to find the most specific type that works for ALL
// non-empty values in each column. Type priority (most to least specific):
//  1. BOOLEAN (true/false, yes/no, 1/0)
//  2. INTEGER (int64 within int32 range)
//  3. BIGINT (int64 for larger integers)
//  4. DOUBLE (float64)
//  5. DATE (YYYY-MM-DD or common formats)
//  6. TIMESTAMP (ISO 8601 or common formats)
//  7. VARCHAR (fallback)
//
// Empty strings and the configured null string are treated as NULL and do not
// affect type inference (a column with only NULLs becomes VARCHAR).
func (ti *TypeInferrer) InferTypes(samples [][]string) []dukdb.Type {
	if len(samples) == 0 {
		return nil
	}

	// Determine number of columns from the first row.
	numCols := len(samples[0])
	if numCols == 0 {
		return nil
	}

	// Limit samples to sampleSize.
	rowsToProcess := len(samples)
	if rowsToProcess > ti.sampleSize {
		rowsToProcess = ti.sampleSize
	}

	// Track possible types for each column.
	// Start with the most specific types and fall back as needed.
	columnCandidates := make([]typeCandidate, numCols)
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

	// Process each row.
	for rowIdx := range rowsToProcess {
		row := samples[rowIdx]
		for colIdx := 0; colIdx < numCols && colIdx < len(row); colIdx++ {
			value := strings.TrimSpace(row[colIdx])

			// Skip empty values (treated as NULL).
			if value == "" {
				continue
			}

			columnCandidates[colIdx].hasValues = true
			updateCandidate(&columnCandidates[colIdx], value)
		}
	}

	// Determine final types based on candidates.
	ti.columnTypes = make([]dukdb.Type, numCols)
	for i := range numCols {
		ti.columnTypes[i] = columnCandidates[i].resolveType()
	}

	return ti.columnTypes
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

// updateCandidate updates the candidate flags based on a value.
func updateCandidate(tc *typeCandidate, value string) {
	tc.canBoolean = tc.canBoolean && canParseBoolean(value)
	tc.canInteger = tc.canInteger && canParseInteger(value)
	tc.canBigint = tc.canBigint && canParseBigint(value)
	tc.canDouble = tc.canDouble && canParseDouble(value)
	tc.canDate = tc.canDate && canParseDate(value)
	tc.canTimestamp = tc.canTimestamp && canParseTimestamp(value)
}

func canParseBoolean(value string) bool {
	_, ok := TryParseBoolean(value)

	return ok
}

func canParseInteger(value string) bool {
	v, ok := TryParseInteger(value)
	if !ok {
		return false
	}
	// Check if value fits in int32 range.
	return v >= math.MinInt32 && v <= math.MaxInt32
}

func canParseBigint(value string) bool {
	_, ok := TryParseInteger(value)

	return ok
}

func canParseDouble(value string) bool {
	_, ok := TryParseDouble(value)

	return ok
}

func canParseDate(value string) bool {
	_, ok := TryParseDate(value)

	return ok
}

func canParseTimestamp(value string) bool {
	_, ok := TryParseTimestamp(value)

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

// TryParseBoolean attempts to parse a string as a boolean.
// Accepts: true/false, yes/no, 1/0, t/f, y/n (case insensitive).
// Returns the parsed value and whether parsing succeeded.
func TryParseBoolean(s string) (value, ok bool) {
	lower := strings.ToLower(strings.TrimSpace(s))
	switch lower {
	case "true", "t", "yes", "y", "1":
		return true, true
	case "false", "f", "no", "n", "0":
		return false, true
	default:
		return false, false
	}
}

// TryParseInteger attempts to parse a string as an int64.
// Returns the parsed value and whether parsing succeeded.
func TryParseInteger(s string) (int64, bool) {
	trimmed := strings.TrimSpace(s)
	v, err := strconv.ParseInt(trimmed, parseIntBase, parseIntBitSize)
	if err != nil {
		return 0, false
	}

	return v, true
}

// TryParseDouble attempts to parse a string as a float64.
// Returns the parsed value and whether parsing succeeded.
func TryParseDouble(s string) (float64, bool) {
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

// TryParseDate attempts to parse a string as a date.
// Supports multiple common date formats.
// Returns the parsed time and whether parsing succeeded.
func TryParseDate(s string) (time.Time, bool) {
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

// TryParseTimestamp attempts to parse a string as a timestamp.
// Supports multiple common timestamp formats including RFC3339.
// Returns the parsed time and whether parsing succeeded.
func TryParseTimestamp(s string) (time.Time, bool) {
	trimmed := strings.TrimSpace(s)
	for _, format := range timestampFormats {
		t, err := time.Parse(format, trimmed)
		if err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

// ConvertValue converts a string value to the appropriate Go type
// based on the target DuckDB type. This is used when filling typed vectors.
// Returns the converted value and whether conversion succeeded.
// For NULL values (empty string), returns nil and true.
//
//nolint:exhaustive // We only handle types that CSV can infer to.
func ConvertValue(s string, targetType dukdb.Type) (any, bool) {
	trimmed := strings.TrimSpace(s)

	// Empty strings are NULL.
	if trimmed == "" {
		return nil, true
	}

	switch targetType {
	case dukdb.TYPE_BOOLEAN:
		return TryParseBoolean(trimmed)

	case dukdb.TYPE_INTEGER:
		v, ok := TryParseInteger(trimmed)
		if !ok {
			return nil, false
		}

		return int32(v), true

	case dukdb.TYPE_BIGINT:
		return TryParseInteger(trimmed)

	case dukdb.TYPE_DOUBLE:
		return TryParseDouble(trimmed)

	case dukdb.TYPE_DATE:
		t, ok := TryParseDate(trimmed)
		if !ok {
			return nil, false
		}
		// Store as time.Time for the vector.
		return t, true

	case dukdb.TYPE_TIMESTAMP:
		t, ok := TryParseTimestamp(trimmed)
		if !ok {
			return nil, false
		}

		return t, true

	case dukdb.TYPE_VARCHAR:
		return trimmed, true

	default:
		// Unknown type, return as string.
		return trimmed, true
	}
}
