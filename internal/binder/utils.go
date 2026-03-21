package binder

import (
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
)

// windowFunctionInfo holds metadata about a window function.
type windowFunctionInfo struct {
	FuncType            WindowFunctionType
	MinArgs             int
	MaxArgs             int
	ReturnType          dukdb.Type // Fixed return type (TYPE_ANY means inferred from first arg)
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
	"LISTAGG":       true,

	// Time series aggregates
	"COUNT_IF":  true,
	"FIRST":     true,
	"LAST":      true,
	"ANY_VALUE": true,
	"ARGMIN":    true,
	"ARG_MIN":   true,
	"ARGMAX":    true,
	"ARG_MAX":   true,
	"MIN_BY":    true,
	"MAX_BY":    true,
	"HISTOGRAM": true,

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

	// JSON aggregates
	"JSON_GROUP_ARRAY":  true,
	"JSON_GROUP_OBJECT": true,

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

// inferCoalesceResultType infers the result type of COALESCE, GREATEST, or LEAST functions.
// Returns the common supertype of all arguments, properly handling NULL types and type promotion.
func inferCoalesceResultType(args []BoundExpr) dukdb.Type {
	if len(args) == 0 {
		return dukdb.TYPE_ANY
	}

	// Start with the first argument's type
	resultType := args[0].ResultType()

	// Find common supertype across all arguments
	for i := 1; i < len(args); i++ {
		argType := args[i].ResultType()
		resultType = promoteType(resultType, argType)
	}

	// If result is still ANY or SQLNULL, try to find a more specific type
	if resultType == dukdb.TYPE_ANY || resultType == dukdb.TYPE_SQLNULL {
		for _, arg := range args {
			argType := arg.ResultType()
			if argType != dukdb.TYPE_ANY && argType != dukdb.TYPE_SQLNULL {
				return argType
			}
		}
	}

	return resultType
}

func promoteType(t1, t2 dukdb.Type) dukdb.Type {
	if t1 == t2 {
		return t1
	}

	// TYPE_ANY yields to any concrete type
	if t1 == dukdb.TYPE_ANY {
		return t2
	}
	if t2 == dukdb.TYPE_ANY {
		return t1
	}

	// NULL promotion
	if t1 == dukdb.TYPE_SQLNULL {
		return t2
	}
	if t2 == dukdb.TYPE_SQLNULL {
		return t1
	}

	// VARCHAR is universal fallback for mixed types
	if t1 == dukdb.TYPE_VARCHAR || t2 == dukdb.TYPE_VARCHAR {
		// If mixing VARCHAR with non-string types, convert to VARCHAR
		return dukdb.TYPE_VARCHAR
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

	// Timestamp types - prefer the more precise one
	if isTimestampType(t1) && isTimestampType(t2) {
		return promoteTimestampTypes(t1, t2)
	}

	// Default to the first type
	return t1
}

// isTimestampType returns true if the type is a timestamp variant.
func isTimestampType(t dukdb.Type) bool {
	switch t {
	case dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS,
		dukdb.TYPE_TIMESTAMP_NS, dukdb.TYPE_TIMESTAMP_TZ:
		return true
	}
	return false
}

// promoteTimestampTypes returns the more precise timestamp type.
func promoteTimestampTypes(t1, t2 dukdb.Type) dukdb.Type {
	// TYPE_TIMESTAMP_TZ takes precedence (has timezone info)
	if t1 == dukdb.TYPE_TIMESTAMP_TZ || t2 == dukdb.TYPE_TIMESTAMP_TZ {
		return dukdb.TYPE_TIMESTAMP_TZ
	}
	// TYPE_TIMESTAMP_NS is most precise
	if t1 == dukdb.TYPE_TIMESTAMP_NS || t2 == dukdb.TYPE_TIMESTAMP_NS {
		return dukdb.TYPE_TIMESTAMP_NS
	}
	// TYPE_TIMESTAMP (microseconds) is next
	if t1 == dukdb.TYPE_TIMESTAMP || t2 == dukdb.TYPE_TIMESTAMP {
		return dukdb.TYPE_TIMESTAMP
	}
	// TYPE_TIMESTAMP_MS
	if t1 == dukdb.TYPE_TIMESTAMP_MS || t2 == dukdb.TYPE_TIMESTAMP_MS {
		return dukdb.TYPE_TIMESTAMP_MS
	}
	// TYPE_TIMESTAMP_S
	return dukdb.TYPE_TIMESTAMP_S
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
		// COALESCE returns the common type of all arguments
		return inferCoalesceResultType(args)
	case "IFNULL", "NVL":
		return inferCoalesceResultType(args)
	case "NULLIF":
		// NULLIF returns the type of the first argument
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return dukdb.TYPE_ANY
	case "GREATEST", "LEAST":
		// GREATEST/LEAST return the common type of all arguments
		return inferCoalesceResultType(args)
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
		"REPLACE",
		// New string functions (add-essential-string-functions)
		"REGEXP_REPLACE",
		"REGEXP_EXTRACT",
		"CONCAT_WS",
		"LPAD",
		"RPAD",
		"REVERSE",
		"REPEAT",
		"LEFT",
		"RIGHT",
		"STRIP",
		"LSTRIP",
		"RSTRIP",
		"CHR",
		"MD5",
		"SHA256",
		"SHA1",
		"TRANSLATE",
		"STRIP_ACCENTS":
		return dukdb.TYPE_VARCHAR
	// String functions returning BOOLEAN
	case "REGEXP_MATCHES",
		"CONTAINS",
		"PREFIX",
		"SUFFIX",
		"STARTS_WITH",
		"ENDS_WITH":
		return dukdb.TYPE_BOOLEAN
	// String functions returning INTEGER/BIGINT
	case "POSITION",
		"STRPOS",
		"INSTR",
		"ASCII",
		"ORD",
		"UNICODE",
		"LEVENSHTEIN",
		"DAMERAU_LEVENSHTEIN",
		"HAMMING",
		"HASH":
		return dukdb.TYPE_BIGINT
	// String functions returning DOUBLE (similarity)
	case "JACCARD",
		"JARO_SIMILARITY",
		"JARO_WINKLER_SIMILARITY":
		return dukdb.TYPE_DOUBLE
	// String functions returning arrays
	case "REGEXP_EXTRACT_ALL",
		"REGEXP_SPLIT_TO_ARRAY",
		"STRING_SPLIT",
		"STRING_SPLIT_REGEX":
		return dukdb.TYPE_ANY // Arrays represented as TYPE_ANY
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
	case "DATE_ADD", "DATEADD", "DATE_SUB":
		// Return type matches first argument
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return dukdb.TYPE_TIMESTAMP
	case "DATE_DIFF":
		return dukdb.TYPE_BIGINT
	case "DATE_TRUNC", "DATETRUNC":
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
	case "STRFTIME", "TO_CHAR":
		return dukdb.TYPE_VARCHAR
	case "TO_DATE":
		return dukdb.TYPE_DATE
	case "STRPTIME", "TO_TIMESTAMP":
		return dukdb.TYPE_TIMESTAMP
	case "EPOCH":
		return dukdb.TYPE_DOUBLE
	case "EPOCH_MS":
		return dukdb.TYPE_BIGINT
	case "EPOCH_US":
		return dukdb.TYPE_BIGINT
	// Array utility functions
	case "GENERATE_SUBSCRIPTS":
		return dukdb.TYPE_INTEGER
	// System functions
	case "CURRENT_DATABASE", "CURRENT_SCHEMA", "VERSION":
		return dukdb.TYPE_VARCHAR
	// Date/time name functions
	case "DAYNAME", "MONTHNAME":
		return dukdb.TYPE_VARCHAR
	case "YEARWEEK":
		return dukdb.TYPE_INTEGER
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
	case "STRING_AGG", "GROUP_CONCAT", "LISTAGG":
		return dukdb.TYPE_VARCHAR
	case "LIST", "ARRAY_AGG", "LIST_DISTINCT":
		// LIST types return TYPE_ANY for now (no LIST type constant available)
		return dukdb.TYPE_ANY

	// Struct and Map functions
	case "STRUCT_PACK":
		return dukdb.TYPE_STRUCT
	case "STRUCT_EXTRACT":
		return dukdb.TYPE_ANY // depends on field type
	case "STRUCT_KEYS":
		return dukdb.TYPE_LIST
	case "STRUCT_INSERT":
		return dukdb.TYPE_STRUCT
	case "LIST_VALUE", "LIST_PACK":
		return dukdb.TYPE_ANY
	case "MAP":
		return dukdb.TYPE_MAP
	case "MAP_KEYS", "MAP_VALUES", "MAP_ENTRIES":
		return dukdb.TYPE_LIST
	case "MAP_CONTAINS_KEY":
		return dukdb.TYPE_BOOLEAN
	case "MAP_FROM_ENTRIES":
		return dukdb.TYPE_MAP
	case "ELEMENT_AT", "MAP_EXTRACT":
		return dukdb.TYPE_ANY // depends on map value type

	// Lambda list functions
	case "LIST_TRANSFORM", "ARRAY_APPLY", "APPLY",
		"LIST_FILTER", "ARRAY_FILTER", "FILTER":
		return dukdb.TYPE_LIST
	case "LIST_SORT", "ARRAY_SORT":
		return dukdb.TYPE_LIST
	case "LIST_REVERSE_SORT", "ARRAY_REVERSE_SORT":
		return dukdb.TYPE_LIST
	case "LIST_ELEMENT", "ARRAY_EXTRACT":
		return dukdb.TYPE_ANY
	case "LIST_AGGREGATE", "ARRAY_AGGREGATE":
		return dukdb.TYPE_ANY

	// Time Series Aggregates
	case "COUNT_IF":
		return dukdb.TYPE_BIGINT
	case "FIRST", "LAST", "ANY_VALUE":
		// FIRST/LAST/ANY_VALUE return same type as input
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return dukdb.TYPE_ANY
	case "HISTOGRAM":
		return dukdb.TYPE_ANY
	case "ARGMIN", "ARG_MIN", "ARGMAX", "ARG_MAX", "MIN_BY", "MAX_BY":
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

	// Rounding functions - preserve input type for integers, return DOUBLE for floats
	case "ROUND", "ROUND_EVEN":
		// ROUND preserves integer types for integer inputs, returns DOUBLE for floats
		if len(args) > 0 {
			argType := args[0].ResultType()
			if isIntegerType(argType) {
				return argType
			}
		}
		return dukdb.TYPE_DOUBLE
	case "CEIL", "CEILING", "FLOOR", "TRUNC", "TRUNCATE":
		// Rounding to whole number preserves integer types
		if len(args) > 0 {
			argType := args[0].ResultType()
			if isIntegerType(argType) {
				return argType
			}
		}
		return dukdb.TYPE_DOUBLE
	case "EVEN":
		// EVEN always returns the same integer type as input
		if len(args) > 0 {
			argType := args[0].ResultType()
			if isIntegerType(argType) {
				return argType
			}
		}
		return dukdb.TYPE_DOUBLE

	// Scientific functions - always return DOUBLE
	case "SQRT", "CBRT", "POW", "POWER", "EXP", "LN", "LOG", "LOG10", "LOG2":
		return dukdb.TYPE_DOUBLE
	case "GAMMA", "LGAMMA":
		return dukdb.TYPE_DOUBLE

	// FACTORIAL returns BIGINT (integer result)
	case "FACTORIAL":
		return dukdb.TYPE_BIGINT

	// Trigonometric functions - always return DOUBLE
	case "SIN", "COS", "TAN", "COT":
		return dukdb.TYPE_DOUBLE
	case "ASIN", "ACOS", "ATAN":
		return dukdb.TYPE_DOUBLE
	case "ATAN2":
		return dukdb.TYPE_DOUBLE
	case "DEGREES", "RADIANS":
		return dukdb.TYPE_DOUBLE

	// Hyperbolic functions - always return DOUBLE
	case "SINH", "COSH", "TANH":
		return dukdb.TYPE_DOUBLE
	case "ASINH", "ACOSH", "ATANH":
		return dukdb.TYPE_DOUBLE

	// Utility functions
	case "PI", "RANDOM", "RAND":
		return dukdb.TYPE_DOUBLE
	case "SETSEED":
		return dukdb.TYPE_ANY
	case "SIGN":
		// SIGN returns INTEGER (-1, 0, or 1)
		return dukdb.TYPE_INTEGER
	case "GCD", "LCM":
		// GCD/LCM work on integers and return BIGINT
		return dukdb.TYPE_BIGINT
	case "ISNAN", "ISINF", "ISFINITE":
		// Boolean predicates
		return dukdb.TYPE_BOOLEAN

	// Bitwise functions
	case "BIT_COUNT":
		// BIT_COUNT returns INTEGER (count of set bits)
		return dukdb.TYPE_INTEGER
	case "BIT_LENGTH":
		return dukdb.TYPE_BIGINT
	case "GET_BIT":
		return dukdb.TYPE_INTEGER
	case "SET_BIT":
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return dukdb.TYPE_BLOB

	// Encoding functions
	case "ENCODE":
		return dukdb.TYPE_BLOB
	case "DECODE":
		return dukdb.TYPE_VARCHAR

	// JSON functions
	case "JSON_EXTRACT":
		return dukdb.TYPE_JSON
	case "JSON_EXTRACT_STRING", "JSON_EXTRACT_TEXT":
		return dukdb.TYPE_VARCHAR
	case "JSON_VALID":
		return dukdb.TYPE_BOOLEAN

	// Geometry functions
	case "ST_GEOMFROMTEXT",
		"ST_GEOMETRYFROMTEXT",
		"ST_POINT",
		"ST_MAKELINE",
		"ST_SETSRID",
		"ST_ENVELOPE":
		return dukdb.TYPE_GEOMETRY
	case "ST_ASTEXT", "ST_ASWKT", "ST_GEOMETRYTYPE":
		return dukdb.TYPE_VARCHAR
	case "ST_ASBINARY", "ST_ASWKB":
		return dukdb.TYPE_BLOB
	case "ST_X", "ST_Y", "ST_Z", "ST_DISTANCE", "ST_DISTANCE_SPHERE":
		return dukdb.TYPE_DOUBLE
	case "ST_SRID":
		return dukdb.TYPE_INTEGER
	case "ST_CONTAINS", "ST_WITHIN", "ST_INTERSECTS", "ST_DISJOINT",
		"ST_TOUCHES", "ST_CROSSES", "ST_OVERLAPS", "ST_EQUALS":
		return dukdb.TYPE_BOOLEAN

	// Geometric analysis functions (Phase 4)
	case "ST_AREA", "ST_LENGTH", "ST_PERIMETER":
		return dukdb.TYPE_DOUBLE
	case "ST_CENTROID":
		return dukdb.TYPE_GEOMETRY

	// Set operations (Phase 5)
	case "ST_UNION", "ST_INTERSECTION", "ST_DIFFERENCE", "ST_BUFFER", "ST_MAKEPOLYGON":
		return dukdb.TYPE_GEOMETRY

	// Encoding/decoding functions
	case "BASE64_ENCODE", "BASE64", "TO_BASE64",
		"URL_ENCODE", "URL_DECODE":
		return dukdb.TYPE_VARCHAR
	case "BASE64_DECODE", "FROM_BASE64":
		return dukdb.TYPE_BLOB

	case "IF", "IFF":
		// Return type is type of the true/false branch
		if len(args) > 1 {
			return args[1].ResultType()
		}
		return dukdb.TYPE_ANY

	case "TYPEOF", "PG_TYPEOF":
		return dukdb.TYPE_VARCHAR

	// Enum utility functions
	case "ENUM_RANGE":
		return dukdb.TYPE_ANY
	case "ENUM_FIRST", "ENUM_LAST":
		return dukdb.TYPE_VARCHAR

	// String formatting functions
	case "FORMAT", "PRINTF":
		return dukdb.TYPE_VARCHAR

	// JSON functions
	case "JSON_CONTAINS":
		return dukdb.TYPE_BOOLEAN
	case "JSON_QUOTE":
		return dukdb.TYPE_VARCHAR

	// JSON aggregate functions
	case "JSON_GROUP_ARRAY", "JSON_GROUP_OBJECT":
		return dukdb.TYPE_VARCHAR

	// List/Array functions
	case "ARRAY_TO_STRING", "LIST_TO_STRING":
		return dukdb.TYPE_VARCHAR
	case "LIST_ZIP":
		return dukdb.TYPE_LIST
	case "LIST_RESIZE", "ARRAY_RESIZE":
		return dukdb.TYPE_LIST

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
	// New string functions (add-essential-string-functions)
	case "REGEXP_MATCHES":
		// REGEXP_MATCHES(string, pattern)
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
		}
	case "REGEXP_REPLACE":
		// REGEXP_REPLACE(string, pattern, replacement [, flags])
		types := make([]dukdb.Type, argCount)
		for i := range types {
			types[i] = dukdb.TYPE_VARCHAR
		}
		return types
	case "REGEXP_EXTRACT":
		// REGEXP_EXTRACT(string, pattern [, group])
		if argCount >= 2 {
			types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
			if argCount >= 3 {
				types = append(types, dukdb.TYPE_INTEGER)
			}
			return types
		}
	case "REGEXP_EXTRACT_ALL":
		// REGEXP_EXTRACT_ALL(string, pattern [, group])
		if argCount >= 2 {
			types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
			if argCount >= 3 {
				types = append(types, dukdb.TYPE_INTEGER)
			}
			return types
		}
	case "REGEXP_SPLIT_TO_ARRAY":
		// REGEXP_SPLIT_TO_ARRAY(string, pattern)
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
		}
	case "CONCAT_WS":
		// CONCAT_WS(separator, str1, str2, ...)
		types := make([]dukdb.Type, argCount)
		for i := range types {
			types[i] = dukdb.TYPE_VARCHAR
		}
		return types
	case "STRING_SPLIT", "STRING_SPLIT_REGEX":
		// STRING_SPLIT(string, separator)
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
		}
	case "LPAD", "RPAD":
		// LPAD(string, length [, fill])
		if argCount >= 2 {
			types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER}
			if argCount >= 3 {
				types = append(types, dukdb.TYPE_VARCHAR)
			}
			return types
		}
	case "REVERSE", "STRIP", "LSTRIP", "RSTRIP", "ASCII", "ORD", "UNICODE", "MD5", "SHA256", "SHA1":
		// Single string argument functions
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR}
		}
	case "REPEAT":
		// REPEAT(string, count)
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER}
		}
	case "LEFT", "RIGHT":
		// LEFT(string, count), RIGHT(string, count)
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER}
		}
	case "POSITION":
		// POSITION(substring IN string) - reordered as (substring, string)
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
		}
	case "STRPOS", "INSTR", "CONTAINS":
		// STRPOS(string, substring)
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
		}
	case "PREFIX", "SUFFIX", "STARTS_WITH", "ENDS_WITH":
		// PREFIX(string, prefix)
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
		}
	case "CHR":
		// CHR(code)
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_INTEGER}
		}
	case "HASH":
		// HASH(string)
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR}
		}
	case "LEVENSHTEIN",
		"DAMERAU_LEVENSHTEIN",
		"HAMMING",
		"JACCARD",
		"JARO_SIMILARITY",
		"JARO_WINKLER_SIMILARITY":
		// Two string argument functions
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
		}
	case "COALESCE":
		// COALESCE accepts homogeneous types - first arg determines type
		// Return empty to let the first arg drive inference
		return nil
	case "IFNULL", "NVL":
		return nil
	case "NULLIF":
		// NULLIF(value, value) - returns first arg type
		// Both args should be same type for comparison
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_ANY, dukdb.TYPE_ANY}
		}
	case "GREATEST", "LEAST":
		// GREATEST/LEAST accept homogeneous types
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
	case "DATE_ADD", "DATEADD", "DATE_SUB":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_TIMESTAMP, dukdb.TYPE_INTERVAL}
		}
	case "DATE_DIFF":
		if argCount >= 3 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP}
		}
	case "DATE_TRUNC", "DATETRUNC", "DATE_PART":
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
	case "STRFTIME", "TO_CHAR":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_TIMESTAMP}
		}
	case "TO_DATE":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
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
	case "STRING_AGG", "GROUP_CONCAT", "LISTAGG":
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
	case "LIST_VALUE", "LIST_PACK":
		types := make([]dukdb.Type, argCount)
		for i := range types {
			types[i] = dukdb.TYPE_ANY
		}
		return types

	// Time series aggregates
	case "COUNT_IF":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_BOOLEAN}
		}
	case "FIRST", "LAST", "ANY_VALUE":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_ANY}
		}
	case "HISTOGRAM":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_ANY}
		}
	case "ARGMIN", "ARG_MIN", "ARGMAX", "ARG_MAX", "MIN_BY", "MAX_BY":
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

	// Rounding functions - accept numeric types
	case "ROUND", "ROUND_EVEN":
		// ROUND(value) or ROUND(value, decimals)
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE, dukdb.TYPE_INTEGER}
		}
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}
	case "CEIL", "CEILING", "FLOOR", "TRUNC", "TRUNCATE", "EVEN":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}

	// Scientific functions - require DOUBLE inputs
	case "SQRT", "CBRT", "EXP", "LN", "LOG", "LOG10", "LOG2", "GAMMA", "LGAMMA":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}
	case "POW", "POWER":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE, dukdb.TYPE_DOUBLE}
		}
	case "FACTORIAL":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_INTEGER}
		}

	// Trigonometric functions - require DOUBLE inputs
	case "SIN", "COS", "TAN", "COT", "ASIN", "ACOS", "ATAN", "DEGREES", "RADIANS":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}
	case "ATAN2":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE, dukdb.TYPE_DOUBLE}
		}

	// Hyperbolic functions - require DOUBLE inputs
	case "SINH", "COSH", "TANH", "ASINH", "ACOSH", "ATANH":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}

	// Utility functions
	case "PI", "RANDOM", "RAND":
		// No arguments needed
		return nil
	case "SETSEED":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}
	case "SIGN":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}
	case "GCD", "LCM":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_BIGINT}
		}
	case "ISNAN", "ISINF", "ISFINITE":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}

	// Bitwise functions
	case "BIT_COUNT":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_BIGINT}
		}
	case "BIT_LENGTH":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR}
		}
	case "GET_BIT":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_BLOB, dukdb.TYPE_INTEGER}
		}
	case "SET_BIT":
		if argCount >= 3 {
			return []dukdb.Type{dukdb.TYPE_BLOB, dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}
		}

	// Encoding functions
	case "ENCODE":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
		}
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR}
		}
	case "DECODE":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_BLOB, dukdb.TYPE_VARCHAR}
		}
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_BLOB}
		}

	// JSON functions
	case "JSON_EXTRACT", "JSON_EXTRACT_STRING", "JSON_EXTRACT_TEXT":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
		}
	case "JSON_VALID":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR}
		}

	// Geometry functions
	case "ST_GEOMFROMTEXT", "ST_GEOMETRYFROMTEXT":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR}
		}
	case "ST_ASTEXT",
		"ST_ASWKT",
		"ST_ASBINARY",
		"ST_ASWKB",
		"ST_GEOMETRYTYPE",
		"ST_X",
		"ST_Y",
		"ST_Z",
		"ST_SRID":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_GEOMETRY}
		}
	case "ST_POINT":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE, dukdb.TYPE_DOUBLE}
		}
	case "ST_MAKELINE":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_GEOMETRY, dukdb.TYPE_GEOMETRY}
		}
	case "ST_SETSRID":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_GEOMETRY, dukdb.TYPE_INTEGER}
		}
	case "ST_DISTANCE", "ST_DISTANCE_SPHERE", "ST_CONTAINS", "ST_WITHIN",
		"ST_INTERSECTS", "ST_DISJOINT", "ST_TOUCHES", "ST_CROSSES",
		"ST_OVERLAPS", "ST_EQUALS":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_GEOMETRY, dukdb.TYPE_GEOMETRY}
		}
	case "ST_ENVELOPE":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_GEOMETRY}
		}

	// Geometric analysis functions (Phase 4)
	case "ST_AREA", "ST_LENGTH", "ST_PERIMETER", "ST_CENTROID":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_GEOMETRY}
		}

	// Set operations (Phase 5)
	case "ST_UNION", "ST_INTERSECTION", "ST_DIFFERENCE":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_GEOMETRY, dukdb.TYPE_GEOMETRY}
		}
	case "ST_BUFFER":
		if argCount >= 2 {
			return []dukdb.Type{dukdb.TYPE_GEOMETRY, dukdb.TYPE_DOUBLE}
		}
	case "ST_MAKEPOLYGON":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_GEOMETRY}
		}

	// Enum utility functions
	case "ENUM_RANGE", "ENUM_FIRST", "ENUM_LAST":
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_VARCHAR}
		}
	}

	return nil
}
