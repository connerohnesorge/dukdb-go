// Package functions provides PostgreSQL to DuckDB function alias mapping.
package functions

import (
	"strings"
)

// AliasCategory categorizes function aliases by their behavior.
type AliasCategory int

const (
	// DirectAlias - function name differs but behavior is identical.
	DirectAlias AliasCategory = iota
	// TransformedAlias - function requires argument transformation.
	TransformedAlias
	// SystemFunction - PostgreSQL system function requiring custom implementation.
	SystemFunction
)

// FunctionAlias represents a mapping from PostgreSQL to DuckDB function.
type FunctionAlias struct {
	// PostgreSQLName is the PostgreSQL function name (lowercase).
	PostgreSQLName string

	// DuckDBName is the equivalent DuckDB function name.
	DuckDBName string

	// Category indicates how the alias should be processed.
	Category AliasCategory

	// MinArgs is the minimum number of arguments.
	MinArgs int

	// MaxArgs is the maximum number of arguments (-1 for unlimited).
	MaxArgs int

	// Description provides documentation for the function.
	Description string

	// Transformer is an optional function to transform arguments.
	// Only used when Category == TransformedAlias.
	Transformer ArgumentTransformer
}

// ArgumentTransformer transforms PostgreSQL function arguments to DuckDB format.
// It receives the original function name and arguments, and returns the
// transformed function name and arguments.
type ArgumentTransformer func(funcName string, args []string) (string, []string, error)

// FunctionAliasRegistry manages PostgreSQL to DuckDB function mappings.
type FunctionAliasRegistry struct {
	aliases map[string]*FunctionAlias
}

// NewRegistry creates a new FunctionAliasRegistry with default aliases.
func NewRegistry() *FunctionAliasRegistry {
	r := &FunctionAliasRegistry{
		aliases: make(map[string]*FunctionAlias),
	}
	r.registerDefaultAliases()

	return r
}

// Register adds a function alias to the registry.
func (r *FunctionAliasRegistry) Register(alias *FunctionAlias) {
	key := strings.ToLower(alias.PostgreSQLName)
	r.aliases[key] = alias
}

// Resolve looks up a function alias by PostgreSQL name.
// Returns nil if no alias exists.
func (r *FunctionAliasRegistry) Resolve(name string) *FunctionAlias {
	key := strings.ToLower(strings.TrimSpace(name))

	// Try direct lookup
	if alias, ok := r.aliases[key]; ok {
		return alias
	}

	// Try without pg_catalog. prefix
	if strings.HasPrefix(key, "pg_catalog.") {
		unprefixed := key[len("pg_catalog."):]
		if alias, ok := r.aliases[unprefixed]; ok {
			return alias
		}
	}

	return nil
}

// Has checks if a function alias exists.
func (r *FunctionAliasRegistry) Has(name string) bool {
	return r.Resolve(name) != nil
}

// GetDuckDBName returns the DuckDB function name for a PostgreSQL function.
// Returns the original name if no alias exists.
func (r *FunctionAliasRegistry) GetDuckDBName(pgName string) string {
	if alias := r.Resolve(pgName); alias != nil {
		return alias.DuckDBName
	}

	return pgName
}

// AllAliases returns all registered aliases.
func (r *FunctionAliasRegistry) AllAliases() []*FunctionAlias {
	result := make([]*FunctionAlias, 0, len(r.aliases))
	for _, alias := range r.aliases {
		result = append(result, alias)
	}

	return result
}

// DirectAliases returns all direct alias mappings.
func (r *FunctionAliasRegistry) DirectAliases() []*FunctionAlias {
	var result []*FunctionAlias
	for _, alias := range r.aliases {
		if alias.Category == DirectAlias {
			result = append(result, alias)
		}
	}

	return result
}

// TransformedAliases returns aliases requiring argument transformation.
func (r *FunctionAliasRegistry) TransformedAliases() []*FunctionAlias {
	var result []*FunctionAlias
	for _, alias := range r.aliases {
		if alias.Category == TransformedAlias {
			result = append(result, alias)
		}
	}

	return result
}

// SystemFunctions returns system functions requiring custom implementation.
func (r *FunctionAliasRegistry) SystemFunctions() []*FunctionAlias {
	var result []*FunctionAlias
	for _, alias := range r.aliases {
		if alias.Category == SystemFunction {
			result = append(result, alias)
		}
	}

	return result
}

// registerDefaultAliases adds all default PostgreSQL function aliases.
func (r *FunctionAliasRegistry) registerDefaultAliases() {
	// Direct aliases - same behavior, different name
	r.registerDirectAliases()

	// Transformed aliases - requires argument transformation
	r.registerTransformedAliases()

	// System functions - PostgreSQL system functions
	r.registerSystemFunctions()
}

// Global default registry
var defaultRegistry = NewRegistry()

// GetDefaultRegistry returns the global default FunctionAliasRegistry.
func GetDefaultRegistry() *FunctionAliasRegistry {
	return defaultRegistry
}

// ResolveFunction resolves a PostgreSQL function name using the default registry.
func ResolveFunction(name string) *FunctionAlias {
	return defaultRegistry.Resolve(name)
}

// GetDuckDBFunctionName returns the DuckDB function name using the default registry.
func GetDuckDBFunctionName(pgName string) string {
	return defaultRegistry.GetDuckDBName(pgName)
}
