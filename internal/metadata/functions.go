package metadata

import (
	"fmt"
	"strings"

	pgfunctions "github.com/dukdb/dukdb-go/internal/postgres/functions"
)

// GetFunctions returns function metadata using the PostgreSQL alias registry.
func GetFunctions(registry *pgfunctions.FunctionAliasRegistry, databaseName string) []FunctionMetadata {
	reg := registry
	if reg == nil {
		reg = pgfunctions.GetDefaultRegistry()
	}
	dbName := databaseName
	if dbName == "" {
		dbName = DefaultDatabaseName
	}

	result := make([]FunctionMetadata, 0)
	seen := make(map[string]bool)

	for _, alias := range reg.AllAliases() {
		fnName := alias.DuckDBName
		if fnName == "" {
			fnName = alias.PostgreSQLName
		}
		if fnName == "" {
			continue
		}
		key := strings.ToLower(fnName)
		if seen[key] {
			continue
		}

		functionType := "scalar"
		if alias.Category == pgfunctions.SystemFunction {
			functionType = "system"
		}

		result = append(result, FunctionMetadata{
			DatabaseName: dbName,
			SchemaName:   DefaultSchemaName,
			FunctionName: fnName,
			FunctionType: functionType,
			Parameters:   formatParamCount(alias.MinArgs, alias.MaxArgs),
			ReturnType:   "",
			Description:  alias.Description,
		})
		seen[key] = true
	}

	systemFunctionNames := []string{
		"duckdb_settings",
		"duckdb_tables",
		"duckdb_columns",
		"duckdb_views",
		"duckdb_functions",
		"duckdb_constraints",
		"duckdb_indexes",
		"duckdb_databases",
		"duckdb_sequences",
		"duckdb_dependencies",
		"duckdb_optimizers",
		"duckdb_keywords",
		"duckdb_extensions",
		"duckdb_memory_usage",
		"duckdb_schemas",
		"duckdb_types",
		"duckdb_temp_directory",
	}

	for _, name := range systemFunctionNames {
		key := strings.ToLower(name)
		if seen[key] {
			continue
		}
		result = append(result, FunctionMetadata{
			DatabaseName: dbName,
			SchemaName:   DefaultSchemaName,
			FunctionName: name,
			FunctionType: "system",
			Parameters:   "",
			ReturnType:   "",
			Description:  "",
		})
		seen[key] = true
	}

	return result
}

func formatParamCount(minArgs, maxArgs int) string {
	if minArgs <= 0 && maxArgs == 0 {
		return ""
	}
	if maxArgs < 0 {
		return fmt.Sprintf("%d+", minArgs)
	}
	if minArgs == maxArgs {
		return fmt.Sprintf("%d", minArgs)
	}
	return fmt.Sprintf("%d-%d", minArgs, maxArgs)
}
