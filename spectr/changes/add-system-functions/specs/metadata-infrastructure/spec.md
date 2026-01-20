# Metadata Infrastructure Specification

## ADDED Requirements

### Requirement: Centralized Metadata Query Package

The system SHALL provide an internal package `internal/metadata/` that centralizes metadata extraction from the catalog.

#### Scenario: Package organization

- WHEN duckdb-go builds
- THEN internal/metadata/ package exists with exported functions
- AND provides query helpers for all system function implementations
- AND decouples metadata queries from function implementations

#### Scenario: Table metadata extraction

- WHEN GetAllTables(catalog) is called
- THEN returns []TableMetadata with fields: database, schema, name, type
- AND includes both base tables and temporary tables
- AND type is 'BASE TABLE' or 'TEMPORARY'
- AND respects schema visibility

#### Scenario: Column metadata extraction

- WHEN GetTableColumns(catalog, tableName) is called
- THEN returns []ColumnMetadata with fields: database, schema, tableName, columnName, columnIndex, dataType, isNullable, defaultValue, comment
- AND columnIndex is 1-based position
- AND dataType matches SQL type names (VARCHAR, INTEGER, FLOAT, etc.)
- AND handles nested types (STRUCT, LIST, MAP) with string representation

#### Scenario: Constraint metadata extraction

- WHEN GetTableConstraints(catalog, tableName) is called
- THEN returns []ConstraintMetadata with fields: database, schema, tableName, constraintName, constraintType, constraintText
- AND constraintType includes: 'PRIMARY KEY', 'UNIQUE', 'CHECK'
- AND constraintText includes the constraint definition or column list

#### Scenario: View metadata extraction

- WHEN GetAllViews(catalog) is called
- THEN returns []ViewMetadata with fields: database, schema, viewName, viewDefinition, comment
- AND viewDefinition is the original SELECT query text
- AND respects schema visibility

#### Scenario: Index metadata extraction

- WHEN GetAllIndexes(catalog) is called
- THEN returns []IndexMetadata with fields: database, schema, tableName, indexName, indexColumns, isUnique, comment
- AND indexColumns is comma-separated list of column names
- AND isUnique reflects unique constraint

#### Scenario: Sequence metadata extraction

- WHEN GetAllSequences(catalog) is called
- THEN returns []SequenceMetadata with fields: database, schema, sequenceName, startValue, increment, minValue, maxValue, cycle, comment
- AND startValue, increment, minValue, maxValue are BIGINT values

#### Scenario: Function metadata extraction

- WHEN GetAllFunctions(engine) is called
- THEN returns []FunctionMetadata with fields: functionName, parameters, returnType, functionType, description
- AND functionType includes: 'scalar', 'aggregate', 'table', 'window'
- AND includes built-in and user-defined functions

#### Scenario: Dependency tracking

- WHEN GetObjectDependencies(catalog) is called
- THEN returns []Dependency with fields: dependentName, dependentType, dependencyName, dependencyType
- AND tracks view → table dependencies
- AND tracks index → table dependencies
- AND tracks view → view dependencies (for chained views)

### Requirement: Type System Helpers

The system SHALL provide helpers for consistent type representation in metadata.

#### Scenario: SQL type string representation

- WHEN TypeToString(logicalType) is called
- THEN returns SQL standard type name
- AND simple types: 'VARCHAR', 'INTEGER', 'FLOAT', 'BOOLEAN', 'DATE', 'TIME', 'TIMESTAMP'
- AND complex types: 'STRUCT(...)', 'LIST(...)', 'MAP(...)', 'UNION(...)'
- AND matches DuckDB v1.4.3 naming conventions

#### Scenario: Nullable representation

- WHEN column is parsed for metadata
- THEN isNullable is TRUE unless column has NOT NULL constraint
- AND NULL is default unless column is primary key or has NOT NULL

### Requirement: Settings and Configuration Metadata

The system SHALL provide access to all configuration settings.

#### Scenario: Settings extraction

- WHEN GetAllSettings(engine) is called
- THEN returns []Setting with fields: name, value, description
- AND includes configuration like 'threads', 'memory_limit', 'default_null_order'
- AND values are strings (e.g., "12" for threads, "4GB" for memory_limit)

#### Scenario: Platform information

- WHEN GetPlatformInfo() is called
- THEN returns PlatformInfo with fields: platform, os, arch
- AND platform is 'Linux', 'macOS', 'Windows'
- AND os matches GOOS (linux, darwin, windows)
- AND arch matches GOARCH (amd64, arm64, etc.)

#### Scenario: Version information

- WHEN GetVersionInfo() is called
- THEN returns VersionInfo with fields: version, gitCommit, buildDate
- AND version matches "1.4.3" for DuckDB v1.4.3 compatible version
- AND gitCommit and buildDate are informational

### Requirement: Metadata Caching and Performance

The system SHALL optimize metadata queries where appropriate.

#### Scenario: Schema-level caching (optional)

- WHEN multiple metadata queries reference same schema
- THEN results may be cached during statement execution
- AND cache is invalidated on schema modification
- AND cache is optional (implementations may skip)

#### Scenario: Query performance targets

- WHEN metadata query executes
- THEN returns results in <100ms for most operations
- AND duckdb_tables() completes in <50ms
- AND duckdb_columns() completes in <100ms
- AND duckdb_functions() completes in <100ms

### Requirement: Consistency and Correctness

The system SHALL ensure metadata queries are consistent and correct.

#### Scenario: Transactional consistency

- WHEN metadata query executes within transaction
- THEN sees snapshot of catalog at transaction start
- AND reflects changes from current transaction
- AND does not see concurrent modifications

#### Scenario: NULL handling

- WHEN metadata field is not applicable
- THEN returns NULL (not empty string or placeholder)
- AND e.g., comment is NULL for objects without comment
- AND e.g., default_value is NULL for columns without defaults
