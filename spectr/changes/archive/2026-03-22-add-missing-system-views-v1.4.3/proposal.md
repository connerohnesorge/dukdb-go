# Proposal: Missing System Views

## Summary

Add duckdb_schemas() and duckdb_types() system table functions. These are the only two duckdb_* system views from DuckDB v1.4.3 that are not implemented. All other duckdb_* views (tables, columns, views, functions, constraints, indexes, databases, sequences, dependencies, optimizers, keywords, extensions, memory_usage, temp_directory) are already implemented.

## Motivation

duckdb_schemas() and duckdb_types() are commonly used for introspection. duckdb_schemas() lists all schemas in the database. duckdb_types() lists all registered types including enums and composite types.

## Scope

- **Metadata**: Add GetSchemas() and GetTypes() functions to internal/metadata/
- **Executor**: Add executeDuckDBSchemas() and executeDuckDBTypes() methods, register in table function dispatch
- **Metadata**: Add column definitions (DuckDBSchemasColumns, DuckDBTypesColumns)

## Files Affected

- `internal/metadata/` — new metadata gathering functions
- `internal/executor/system_functions.go` — new executor methods
- `internal/executor/table_function_csv.go` — register in dispatch (around line 113)
- `internal/metadata/functions.go` — add to systemFunctionNames list (line 54)
