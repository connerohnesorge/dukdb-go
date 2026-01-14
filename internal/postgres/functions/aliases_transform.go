package functions

import (
	"fmt"
	"strconv"
)

// Constants for argument counts and strconv bases.
const (
	argsMinGenerateSeries = 2
	argsMaxGenerateSeries = 3
	strconvBase           = 10
	strconvBitSize        = 64
)

// registerTransformedAliases registers functions requiring argument transformation.
func (r *FunctionAliasRegistry) registerTransformedAliases() {
	aliases := []*FunctionAlias{
		// generate_series(start, stop) -> range(start, stop+1)
		// PostgreSQL's generate_series is inclusive, DuckDB's range is exclusive
		{
			PostgreSQLName: "generate_series",
			DuckDBName:     "range",
			Category:       TransformedAlias,
			MinArgs:        argsMinGenerateSeries,
			MaxArgs:        argsMaxGenerateSeries,
			Description:    "Generates a series of values",
			Transformer:    transformGenerateSeries,
		},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}

// transformGenerateSeries transforms generate_series(start, stop[, step])
// to range(start, stop+1[, step]) since PostgreSQL is inclusive and DuckDB is exclusive.
// The funcName parameter is part of the ArgumentTransformer interface.
func transformGenerateSeries(_ string, args []string) (string, []string, error) {
	if len(args) < argsMinGenerateSeries || len(args) > argsMaxGenerateSeries {
		return "", nil, fmt.Errorf(
			"generate_series requires %d or %d arguments, got %d",
			argsMinGenerateSeries, argsMaxGenerateSeries, len(args),
		)
	}

	// For integer ranges, we need to add 1 to the stop value
	// For timestamp ranges, the semantics may differ
	// This is a simplified transformation - full implementation would parse and evaluate

	newArgs := make([]string, len(args))
	copy(newArgs, args)

	// Try to increment the stop value
	// This is a simplified approach - real implementation would handle expressions
	if stopVal, err := strconv.ParseInt(args[1], strconvBase, strconvBitSize); err == nil {
		newArgs[1] = strconv.FormatInt(stopVal+1, strconvBase)
	} else {
		// For non-integer values, wrap in expression: (stop + 1)
		newArgs[1] = fmt.Sprintf("(%s + 1)", args[1])
	}

	return "range", newArgs, nil
}
