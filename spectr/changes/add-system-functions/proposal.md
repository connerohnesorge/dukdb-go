# Change: DuckDB v1.4.3 Compatible System Functions

## Why

System functions provide SQL-accessible metadata about the database state. These functions are essential for:
- Database administration and monitoring (checking loaded extensions, configuration)
- Query optimization and debugging (understanding available functions, table schemas)
- Schema introspection and discovery (programmatic schema exploration)
- Tool integration (BI tools, ORMs, and database clients that query system metadata)
- DuckDB compatibility (users familiar with DuckDB expect these functions to work)

Currently, dukdb-go lacks these critical introspection capabilities, limiting adoption for administrative tooling and programmatic schema discovery.

## What Changes

This proposal implements 15 system functions and 8 PRAGMA statements providing complete access to database metadata through SQL queries:

### System Functions (as virtual table functions)
- `duckdb_settings()` - Query database settings and configuration
- `duckdb_functions()` - List available functions with signatures and categories
- `duckdb_tables()` - List all tables across all schemas
- `duckdb_columns()` - List all columns with their types and properties
- `duckdb_constraints()` - List table constraints (PK, unique, checks)
- `duckdb_databases()` - List databases and their properties
- `duckdb_views()` - List all views with their definitions
- `duckdb_indexes()` - List all indexes and their columns
- `duckdb_sequences()` - List sequences with increment/start values
- `duckdb_dependencies()` - Show object dependencies (views depending on tables, etc.)
- `duckdb_optimizers()` - List optimizer settings
- `duckdb_keywords()` - List reserved SQL keywords
- `duckdb_extensions()` - List loaded extensions (initially no-op, extensible)
- `duckdb_memory_usage()` - Memory usage statistics
- `duckdb_temp_directory()` - Temporary directory configuration

### PRAGMA Statements
- `PRAGMA database_size` - Total database size
- `PRAGMA table_info('table_name')` - Column metadata for table
- `PRAGMA database_list` - List available databases
- `PRAGMA version` - DuckDB version information
- `PRAGMA platform` - Platform/OS information
- `PRAGMA functions` - List functions
- `PRAGMA collations` - List available collations
- `PRAGMA table_storage_info('table_name')` - Storage statistics
- `PRAGMA storage_info('table_name')` - Storage breakdown by column

## Architectural Approach

### System Functions Implementation (Virtual Table Functions)
- Register as table functions in executor's `executeTableFunctionScan()`
- Query live catalog state to generate result rows dynamically
- No persistent storage - computed on-demand
- Each function returns TypedDataChunk with appropriate columns

### PRAGMA Statement Implementation
- Add PRAGMA parsing to `internal/parser/` (if not already present)
- Implement PRAGMA handlers in executor
- Return single-row or metadata results

### Metadata Infrastructure
- Create centralized `internal/metadata/` package for query helpers
- Helper functions to extract catalog information:
  - `GetAllTables(catalog)` → []TableMetadata
  - `GetTableColumns(catalog, table)` → []ColumnMetadata
  - `GetTableConstraints(catalog, table)` → []ConstraintMetadata
  - `GetAllViews(catalog)` → []ViewMetadata
  - etc.
- Consistent column schemas across all functions (matching DuckDB v1.4.3)

## Impact

- **Affected specs**: `system-functions` (new), `metadata-infrastructure` (new)
- **Affected code**:
  - `internal/executor/table_function_*.go` - add system function dispatchers
  - `internal/executor/operator.go` - extend tableFunction resolution
  - `internal/parser/` - add PRAGMA parsing if needed
  - `internal/metadata/` - new package for metadata query helpers
  - `internal/binder/` - PRAGMA binding if needed

- **Dependencies**:
  - **DEPENDS ON**: Requires `add-information-schema` to follow (for SQL-accessible metadata views)
  - **DEPENDS ON**: Requires `add-postgresql-catalog` to follow (for PostgreSQL tool compatibility)

- **Not Breaking**: Pure additions; existing API unchanged

## Success Criteria

1. All 15 system functions execute and return correct columns
2. All system functions respect schema visibility (return only relevant objects)
3. All PRAGMA statements work and return expected results
4. System functions produce output matching DuckDB v1.4.3 schema
5. Functions handle edge cases (empty schemas, no views, no indexes, etc.)
6. Performance acceptable for typical metadata queries (<100ms for most functions)
7. Functions properly escape special characters in identifiers
8. All test scenarios pass against reference DuckDB behavior
