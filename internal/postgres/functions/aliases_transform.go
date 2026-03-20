package functions

import (
	"fmt"
)

// Constants for argument counts.
const (
	argsMinGenerateSeries = 2
	argsMaxGenerateSeries = 3
)

// registerTransformedAliases registers functions requiring argument transformation.
func (r *FunctionAliasRegistry) registerTransformedAliases() {
	aliases := []*FunctionAlias{
		// generate_series - native support with inclusive semantics
		{
			PostgreSQLName: "generate_series",
			DuckDBName:     "generate_series",
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

// transformGenerateSeries validates arguments for generate_series and passes through
// to native generate_series support. Previously this transformed to range(start, stop+1),
// but native generate_series is now supported with correct inclusive semantics.
// The funcName parameter is part of the ArgumentTransformer interface.
func transformGenerateSeries(_ string, args []string) (string, []string, error) {
	if len(args) < argsMinGenerateSeries || len(args) > argsMaxGenerateSeries {
		return "", nil, fmt.Errorf(
			"generate_series requires %d or %d arguments, got %d",
			argsMinGenerateSeries, argsMaxGenerateSeries, len(args),
		)
	}

	// Native generate_series is now supported - pass through unchanged
	return "generate_series", args, nil
}
