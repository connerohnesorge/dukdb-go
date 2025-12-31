package binder

import (
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
)

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
	default:
		return 0
	}
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
	}

	return nil
}
