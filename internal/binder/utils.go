package binder

import (
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
)

// windowFunctionInfo holds metadata about a window function.
type windowFunctionInfo struct {
	FuncType    WindowFunctionType
	MinArgs     int
	MaxArgs     int
	ReturnType  dukdb.Type // Fixed return type (TYPE_ANY means inferred from first arg)
	SupportsIgnoreNulls bool
}

// windowFunctions maps function names to their window function metadata.
// Key is uppercase function name.
var windowFunctions = map[string]windowFunctionInfo{
	// Ranking functions - all return BIGINT
	"ROW_NUMBER": {WindowFunctionRanking, 0, 0, dukdb.TYPE_BIGINT, false},
	"RANK":       {WindowFunctionRanking, 0, 0, dukdb.TYPE_BIGINT, false},
	"DENSE_RANK": {WindowFunctionRanking, 0, 0, dukdb.TYPE_BIGINT, false},
	"NTILE":      {WindowFunctionRanking, 1, 1, dukdb.TYPE_BIGINT, false},

	// Value functions - return type of first argument
	"LAG":         {WindowFunctionValue, 1, 3, dukdb.TYPE_ANY, true},
	"LEAD":        {WindowFunctionValue, 1, 3, dukdb.TYPE_ANY, true},
	"FIRST_VALUE": {WindowFunctionValue, 1, 1, dukdb.TYPE_ANY, true},
	"LAST_VALUE":  {WindowFunctionValue, 1, 1, dukdb.TYPE_ANY, true},
	"NTH_VALUE":   {WindowFunctionValue, 2, 2, dukdb.TYPE_ANY, true},

	// Distribution functions - return DOUBLE
	"PERCENT_RANK": {WindowFunctionDistribution, 0, 0, dukdb.TYPE_DOUBLE, false},
	"CUME_DIST":    {WindowFunctionDistribution, 0, 0, dukdb.TYPE_DOUBLE, false},
}

// aggregateFunctions that can be used as window functions.
// These are marked as WindowFunctionAggregate when used with OVER.
var aggregateWindowFunctions = map[string]bool{
	// Basic aggregates
	"COUNT": true,
	"SUM":   true,
	"AVG":   true,
	"MIN":   true,
	"MAX":   true,

	// Statistical aggregates
	"MEDIAN":          true,
	"QUANTILE":        true,
	"PERCENTILE_CONT": true,
	"PERCENTILE_DISC": true,
	"MODE":            true,
	"ENTROPY":         true,
	"SKEWNESS":        true,
	"KURTOSIS":        true,
	"VAR_POP":         true,
	"VAR_SAMP":        true,
	"VARIANCE":        true,
	"STDDEV_POP":      true,
	"STDDEV_SAMP":     true,
	"STDDEV":          true,

	// Approximate aggregates
	"APPROX_COUNT_DISTINCT": true,
	"APPROX_QUANTILE":       true,
	"APPROX_MEDIAN":         true,

	// String/List aggregates
	"STRING_AGG":    true,
	"GROUP_CONCAT":  true,
	"LIST":          true,
	"ARRAY_AGG":     true,
	"LIST_DISTINCT": true,

	// Time series aggregates
	"COUNT_IF": true,
	"FIRST":    true,
	"LAST":     true,
	"ARGMIN":   true,
	"ARGMAX":   true,
	"MIN_BY":   true,
	"MAX_BY":   true,

	// Regression aggregates
	"COVAR_POP":      true,
	"COVAR_SAMP":     true,
	"CORR":           true,
	"REGR_SLOPE":     true,
	"REGR_INTERCEPT": true,
	"REGR_R2":        true,
	"REGR_COUNT":     true,
	"REGR_AVGX":      true,
	"REGR_AVGY":      true,
	"REGR_SXX":       true,
	"REGR_SYY":       true,
	"REGR_SXY":       true,

	// Boolean aggregates
	"BOOL_AND": true,
	"BOOL_OR":  true,
	"EVERY":    true,

	// Bitwise aggregates
	"BIT_AND": true,
	"BIT_OR":  true,
	"BIT_XOR": true,
}

// IsWindowFunction returns true if the function name is a window function.
func IsWindowFunction(name string) bool {
	upper := strings.ToUpper(name)
	_, ok := windowFunctions[upper]
	return ok
}

// IsAggregateWindowCapable returns true if the aggregate function can be used with OVER.
func IsAggregateWindowCapable(name string) bool {
	return aggregateWindowFunctions[strings.ToUpper(name)]
}

// GetWindowFunctionInfo returns metadata for a window function.
// Returns nil if not a window function.
func GetWindowFunctionInfo(name string) *windowFunctionInfo {
	upper := strings.ToUpper(name)
	if info, ok := windowFunctions[upper]; ok {
		return &info
	}
	return nil
}

// InferWindowFunctionResultType infers the result type of a window function.
func InferWindowFunctionResultType(name string, args []BoundExpr) dukdb.Type {
	upper := strings.ToUpper(name)

	// Check dedicated window functions
	if info, ok := windowFunctions[upper]; ok {
		if info.ReturnType != dukdb.TYPE_ANY {
			return info.ReturnType
		}
		// For TYPE_ANY, return type of first argument
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return dukdb.TYPE_ANY
	}

	// Check aggregate functions used as windows
	if aggregateWindowFunctions[upper] {
		return inferFunctionResultType(upper, args)
	}

	return dukdb.TYPE_ANY
}

func promoteType(t1, t2 dukdb.Type) dukdb.Type {
	if t1 == t2 {
		return t1
	}

	// NULL promotion
	if t1 == dukdb.TYPE_SQLNULL {
		return t2
	}
	if t2 == dukdb.TYPE_SQLNULL {
		return t1
	}

	// Integer to float promotion
	if isIntegerType(t1) && isFloatType(t2) {
		return t2
	}
	if isFloatType(t1) && isIntegerType(t2) {
		return t1
	}

	// Wider integer types take precedence
	if isIntegerType(t1) && isIntegerType(t2) {
		if typeSize(t1) > typeSize(t2) {
			return t1
		}

		return t2
	}

	// Wider float types take precedence
	if isFloatType(t1) && isFloatType(t2) {
		if t1 == dukdb.TYPE_DOUBLE {
			return t1
		}

		return t2
	}

	// Default to the first type
	return t1
}

func isIntegerType(t dukdb.Type) bool {
	switch t {
	case dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_UTINYINT,
		dukdb.TYPE_USMALLINT,
		dukdb.TYPE_UINTEGER,
		dukdb.TYPE_UBIGINT,
		dukdb.TYPE_HUGEINT,
		dukdb.TYPE_UHUGEINT:
		return true
	case dukdb.TYPE_INVALID, dukdb.TYPE_BOOLEAN, dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE,
		dukdb.TYPE_TIMESTAMP, dukdb.TYPE_DATE, dukdb.TYPE_TIME, dukdb.TYPE_INTERVAL,
		dukdb.TYPE_VARCHAR, dukdb.TYPE_BLOB, dukdb.TYPE_DECIMAL, dukdb.TYPE_TIMESTAMP_S,
		dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS, dukdb.TYPE_ENUM, dukdb.TYPE_LIST,
		dukdb.TYPE_STRUCT, dukdb.TYPE_MAP, dukdb.TYPE_ARRAY, dukdb.TYPE_UUID, dukdb.TYPE_UNION,
		dukdb.TYPE_BIT, dukdb.TYPE_TIME_TZ, dukdb.TYPE_TIMESTAMP_TZ, dukdb.TYPE_ANY,
		dukdb.TYPE_BIGNUM, dukdb.TYPE_SQLNULL, dukdb.TYPE_JSON, dukdb.TYPE_GEOMETRY,
		dukdb.TYPE_LAMBDA, dukdb.TYPE_VARIANT:
		return false
	}

	return false
}

func isFloatType(t dukdb.Type) bool {
	return t == dukdb.TYPE_FLOAT ||
		t == dukdb.TYPE_DOUBLE
}

func typeSize(t dukdb.Type) int {
	switch t {
	case dukdb.TYPE_TINYINT, dukdb.TYPE_UTINYINT:
		return 1
	case dukdb.TYPE_SMALLINT,
		dukdb.TYPE_USMALLINT:
		return 2
	case dukdb.TYPE_INTEGER, dukdb.TYPE_UINTEGER:
		return 4
	case dukdb.TYPE_BIGINT, dukdb.TYPE_UBIGINT:
		return 8
	case dukdb.TYPE_HUGEINT, dukdb.TYPE_UHUGEINT:
		return 16
	case dukdb.TYPE_INVALID, dukdb.TYPE_BOOLEAN, dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE,
		dukdb.TYPE_TIMESTAMP, dukdb.TYPE_DATE, dukdb.TYPE_TIME, dukdb.TYPE_INTERVAL,
		dukdb.TYPE_VARCHAR, dukdb.TYPE_BLOB, dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS,
		dukdb.TYPE_TIMESTAMP_NS, dukdb.TYPE_ENUM, dukdb.TYPE_LIST, dukdb.TYPE_STRUCT,
		dukdb.TYPE_MAP, dukdb.TYPE_ARRAY, dukdb.TYPE_UUID, dukdb.TYPE_UNION, dukdb.TYPE_BIT,
		dukdb.TYPE_TIME_TZ, dukdb.TYPE_TIMESTAMP_TZ, dukdb.TYPE_ANY, dukdb.TYPE_BIGNUM,
		dukdb.TYPE_SQLNULL, dukdb.TYPE_JSON, dukdb.TYPE_GEOMETRY, dukdb.TYPE_LAMBDA,
		dukdb.TYPE_VARIANT, dukdb.TYPE_DECIMAL:
		return 0
	}
	return 0
}

func inferFunctionResultType(
	name string,
	args []BoundExpr,
) dukdb.Type {
	name = strings.ToUpper(name)
	switch name {
	case "COUNT":
		return dukdb.TYPE_BIGINT
	case "SUM":
		if len(args) > 0 {
			switch args[0].ResultType() {
			case dukdb.TYPE_TINYINT,
				dukdb.TYPE_SMALLINT,
				dukdb.TYPE_INTEGER,
				dukdb.TYPE_BIGINT:
				return dukdb.TYPE_BIGINT
			case dukdb.TYPE_FLOAT,
				dukdb.TYPE_DOUBLE:
				return dukdb.TYPE_DOUBLE
			case dukdb.TYPE_DECIMAL:
				return dukdb.TYPE_DECIMAL
			case dukdb.TYPE_INVALID, dukdb.TYPE_BOOLEAN, dukdb.TYPE_UTINYINT,
				dukdb.TYPE_USMALLINT, dukdb.TYPE_UINTEGER, dukdb.TYPE_UBIGINT,
				dukdb.TYPE_TIMESTAMP, dukdb.TYPE_DATE, dukdb.TYPE_TIME, dukdb.TYPE_INTERVAL,
				dukdb.TYPE_HUGEINT, dukdb.TYPE_UHUGEINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_BLOB,
				dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS,
				dukdb.TYPE_ENUM, dukdb.TYPE_LIST, dukdb.TYPE_STRUCT, dukdb.TYPE_MAP,
				dukdb.TYPE_ARRAY, dukdb.TYPE_UUID, dukdb.TYPE_UNION, dukdb.TYPE_BIT,
				dukdb.TYPE_TIME_TZ, dukdb.TYPE_TIMESTAMP_TZ, dukdb.TYPE_ANY, dukdb.TYPE_BIGNUM,
				dukdb.TYPE_SQLNULL, dukdb.TYPE_JSON, dukdb.TYPE_GEOMETRY, dukdb.TYPE_LAMBDA,
				dukdb.TYPE_VARIANT:
				return dukdb.TYPE_BIGINT
			}
		}

		return dukdb.TYPE_BIGINT
	case "AVG":
		return dukdb.TYPE_DOUBLE
	case "MIN", "MAX":
		if len(args) > 0 {
			return args[0].ResultType()
		}

		return dukdb.TYPE_ANY
	case "COALESCE":
		if len(args) > 0 {
			return args[0].ResultType()
		}

		return dukdb.TYPE_ANY
	case "ABS":
		if len(args) > 0 {
			return args[0].ResultType()
		}

		return dukdb.TYPE_DOUBLE
	case "UPPER",
		"LOWER",
		"TRIM",
		"LTRIM",
		"RTRIM",
		"SUBSTR",
		"SUBSTRING",
		"CONCAT",
		"REPLACE":
		return dukdb.TYPE_VARCHAR
	case "LENGTH",
		"CHAR_LENGTH",
		"CHARACTER_LENGTH":
		return dukdb.TYPE_INTEGER
	case "NOW", "CURRENT_TIMESTAMP":
		return dukdb.TYPE_TIMESTAMP
	case "CURRENT_DATE":
		return dukdb.TYPE_DATE
	case "CURRENT_TIME":
		return dukdb.TYPE_TIME
	// Date extraction functions (tasks 1.1, 1.2, 1.3)
	case "YEAR", "MONTH", "DAY", "HOUR", "MINUTE":
		return dukdb.TYPE_INTEGER
	case "SECOND":
		return dukdb.TYPE_DOUBLE
	case "DAYOFWEEK", "DAYOFYEAR", "WEEK", "QUARTER":
		return dukdb.TYPE_INTEGER
	// Date arithmetic functions (tasks 1.5, 1.6, 1.7, 1.8, 1.9, 1.10)
	case "DATE_ADD", "DATE_SUB":
		// Return type matches first argument
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return dukdb.TYPE_TIMESTAMP
	case "DATE_DIFF":
		return dukdb.TYPE_BIGINT
	case "DATE_TRUNC":
		return dukdb.TYPE_TIMESTAMP
	case "DATE_PART":
		return dukdb.TYPE_DOUBLE
	case "AGE":
		return dukdb.TYPE_INTERVAL
	case "LAST_DAY":
		return dukdb.TYPE_DATE
	// Date construction functions (tasks 1.11, 1.12, 1.13)
	case "MAKE_DATE":
		return dukdb.TYPE_DATE
	case "MAKE_TIMESTAMP":
		return dukdb.TYPE_TIMESTAMP
	case "MAKE_TIME":
		return dukdb.TYPE_TIME
	// Formatting/parsing functions (tasks 1.15, 1.16, 1.17, 1.18, 1.19)
	case "STRFTIME":
		return dukdb.TYPE_VARCHAR
	case "STRPTIME", "TO_TIMESTAMP":
		return dukdb.TYPE_TIMESTAMP
	case "EPOCH":
		return dukdb.TYPE_DOUBLE
	case "EPOCH_MS":
		return dukdb.TYPE_BIGINT
	// Interval extraction functions (tasks 7.9, 7.10, 7.11)
	case "TO_YEARS", "TO_MONTHS", "TO_DAYS", "TO_HOURS", "TO_MINUTES":
		return dukdb.TYPE_BIGINT
	case "TO_SECONDS":
		return dukdb.TYPE_DOUBLE
	// Total extraction functions
	case "TOTAL_YEARS", "TOTAL_MONTHS", "TOTAL_DAYS",
		"TOTAL_HOURS", "TOTAL_MINUTES", "TOTAL_SECONDS":
		return dukdb.TYPE_DOUBLE

	// Statistical Aggregates (return DOUBLE)
	case "MEDIAN", "QUANTILE", "PERCENTILE_CONT", "PERCENTILE_DISC":
		return dukdb.TYPE_DOUBLE
	case "ENTROPY", "SKEWNESS", "KURTOSIS":
		return dukdb.TYPE_DOUBLE
	case "VAR_POP", "VAR_SAMP", "VARIANCE":
		return dukdb.TYPE_DOUBLE
	case "STDDEV_POP", "STDDEV_SAMP", "STDDEV":
		return dukdb.TYPE_DOUBLE
	case "MODE":
		// MODE returns same type as input, or DOUBLE if unknown
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return dukdb.TYPE_DOUBLE

	// Approximate Aggregates
	case "APPROX_COUNT_DISTINCT":
		return dukdb.TYPE_BIGINT
	case "APPROX_QUANTILE", "APPROX_MEDIAN":
		return dukdb.TYPE_DOUBLE

	// String/List Aggregates
	case "STRING_AGG", "GROUP_CONCAT":
		return dukdb.TYPE_VARCHAR
	case "LIST", "ARRAY_AGG", "LIST_DISTINCT":
		// LIST types return TYPE_ANY for now (no LIST type constant available)
		return dukdb.TYPE_ANY

	// Time Series Aggregates
	case "COUNT_IF":
		return dukdb.TYPE_BIGINT
	case "FIRST", "LAST":
		// FIRST/LAST return same type as input
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return dukdb.TYPE_ANY
	case "ARGMIN", "ARGMAX", "MIN_BY", "MAX_BY":
		// These return the type of the first argument (the value being returned)
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return dukdb.TYPE_ANY

	// Regression Aggregates (return DOUBLE)
	case "COVAR_POP", "COVAR_SAMP":
		return dukdb.TYPE_DOUBLE
	case "CORR":
		return dukdb.TYPE_DOUBLE
	case "REGR_SLOPE", "REGR_INTERCEPT", "REGR_R2":
		return dukdb.TYPE_DOUBLE
	case "REGR_AVGX", "REGR_AVGY", "REGR_SXX", "REGR_SYY", "REGR_SXY":
		return dukdb.TYPE_DOUBLE
	case "REGR_COUNT":
		return dukdb.TYPE_BIGINT

	// Boolean Aggregates
	case "BOOL_AND", "BOOL_OR", "EVERY":
		return dukdb.TYPE_BOOLEAN

	// Bitwise Aggregates
	case "BIT_AND", "BIT_OR", "BIT_XOR":
		return dukdb.TYPE_BIGINT

	default:
		return dukdb.TYPE_ANY
	}
}

// getFunctionArgTypes returns expected argument types for known functions.
// This is used for parameter type inference in function calls.
func getFunctionArgTypes(
	name string,
	argCount int,
) []dukdb.Type {
	name = strings.ToUpper(name)
	switch name {
	case "ABS", "SUM", "AVG", "MIN", "MAX":
		// Numeric functions - first arg should be numeric
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}
	case "UPPER",
		"LOWER",
		"TRIM",
		"LTRIM",
		"RTRIM",
		"LENGTH",
		"CHAR_LENGTH",
		"CHARACTER_LENGTH":
		// String functions
		if argCount >= 1 {
			return []dukdb.Type{
				dukdb.TYPE_VARCHAR,
			}
		}
	case "SUBSTR", "SUBSTRING":
		// SUBSTR(string, start, [length])
		types := make([]dukdb.Type, argCount)
		if argCount >= 1 {
			types[0] = dukdb.TYPE_VARCHAR
		}
		if argCount >= 2 {
			types[1] = dukdb.TYPE_INTEGER
		}
		if argCount >= 3 {
			types[2] = dukdb.TYPE_INTEGER
		}

		return types
	case "REPLACE":
		// REPLACE(string, from, to)
		if argCount >= 3 {
			return []dukdb.Type{
				dukdb.TYPE_VARCHAR,
				dukdb.TYPE_VARCHAR,
				dukdb.TYPE_VARCHAR,
			}
		}
	case "CONCAT":
		// CONCAT accepts varargs of VARCHAR
		types := make([]dukdb.Type, argCount)
		for i := range types {
			types[i] = dukdb.TYPE_VARCHAR
		}

		return types
	case "COALESCE":
		// COALESCE accepts homogeneous types - first arg determines type
		// Return empty to let the first arg drive inference
		return nil
	case "COUNT":
		// COUNT can take any type
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_ANY}
		}
	// Date extraction functions (task 1.4)
	case "YEAR", "MONTH", "DAY", "DAYOFWEEK", "DAYOFYEAR", "WEEK", "QUARTER":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DATE}
		}
	case "HOUR", "MINUTE", "SECOND":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_TIMESTAMP}
		}
	// Date arithmetic functions
	case "DATE_ADD", "DATE_SUB":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_TIMESTAMP, dukdb.TYPE_INTERVAL}
		}
	case "DATE_DIFF":
		if argCount >= 3 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP}
		}
	case "DATE_TRUNC", "DATE_PART":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_TIMESTAMP}
		}
	case "AGE":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP}
		}
	case "LAST_DAY":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DATE}
		}
	// Date construction functions (task 1.14)
	case "MAKE_DATE":
		if argCount >= 3 {
			return []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}
		}
	case "MAKE_TIMESTAMP":
		if argCount >= 6 {
			return []dukdb.Type{
				dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER,
				dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE,
			}
		}
	case "MAKE_TIME":
		if argCount >= 3 {
			return []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE}
		}
	// Formatting/parsing functions
	case "STRFTIME":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_TIMESTAMP}
		}
	case "STRPTIME":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
		}
	case "TO_TIMESTAMP":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}
	case "EPOCH", "EPOCH_MS":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_TIMESTAMP}
		}
	// Interval extraction functions (tasks 7.9, 7.10, 7.11)
	case "TO_YEARS", "TO_MONTHS", "TO_DAYS",
		"TO_HOURS", "TO_MINUTES", "TO_SECONDS",
		"TOTAL_YEARS", "TOTAL_MONTHS", "TOTAL_DAYS",
		"TOTAL_HOURS", "TOTAL_MINUTES", "TOTAL_SECONDS":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_INTERVAL}
		}

	// Statistical aggregate functions - take numeric values
	case "MEDIAN", "MODE", "ENTROPY", "SKEWNESS", "KURTOSIS",
		"VAR_POP", "VAR_SAMP", "VARIANCE",
		"STDDEV_POP", "STDDEV_SAMP", "STDDEV":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}
	case "QUANTILE", "PERCENTILE_CONT", "PERCENTILE_DISC",
		"APPROX_QUANTILE":
		// Takes (value, quantile)
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE, dukdb.TYPE_DOUBLE}
		}
	case "APPROX_MEDIAN":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}
	case "APPROX_COUNT_DISTINCT":
		// Takes any type
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_ANY}
		}

	// String/List aggregates
	case "STRING_AGG", "GROUP_CONCAT":
		// Takes (value, [delimiter])
		if argCount >= 1 {
			types := []dukdb.Type{dukdb.TYPE_VARCHAR}
			if argCount >= 2 {
				types = append(types, dukdb.TYPE_VARCHAR)
			}
			return types
		}
	case "LIST", "ARRAY_AGG", "LIST_DISTINCT":
		// Takes any type
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_ANY}
		}

	// Time series aggregates
	case "COUNT_IF":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_BOOLEAN}
		}
	case "FIRST", "LAST":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_ANY}
		}
	case "ARGMIN", "ARGMAX", "MIN_BY", "MAX_BY":
		// Takes (return_value, sort_value)
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_ANY, dukdb.TYPE_ANY}
		}

	// Regression aggregates - take two numeric values
	case "COVAR_POP", "COVAR_SAMP", "CORR",
		"REGR_SLOPE", "REGR_INTERCEPT", "REGR_R2",
		"REGR_COUNT", "REGR_AVGX", "REGR_AVGY",
		"REGR_SXX", "REGR_SYY", "REGR_SXY":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE, dukdb.TYPE_DOUBLE}
		}

	// Boolean aggregates
	case "BOOL_AND", "BOOL_OR", "EVERY":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_BOOLEAN}
		}

	// Bitwise aggregates
	case "BIT_AND", "BIT_OR", "BIT_XOR":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_BIGINT}
		}
	}

	return nil
}
