# System Functions Specification

## Overview

This specification defines the implementation of DuckDB v1.4.3 compatible system functions (duckdb_*() functions) in dukdb-go. These functions provide introspection capabilities to query database metadata, settings, and internal state.

## Function Categories

### 1. Database Information Functions

#### duckdb_databases()

Returns information about all databases in the system.

**Signature:**
```sql
duckdb_databases() -> TABLE(
    database_name VARCHAR,
    database_oid BIGINT,
    path VARCHAR,
    type VARCHAR
)
```

**Description:** Lists all databases with their names, OIDs, file paths, and types.

**Example:**
```sql
SELECT * FROM duckdb_databases();
-- Result:
-- database_name | database_oid | path         | type
-- --------------|--------------|--------------|--------
-- memory        | 1            | :memory:     | memory
-- mydb          | 2            | /data/my.db  | file
```

**Implementation Notes:**
- Default database is always 'memory' with type 'memory'
- Attached databases show their file paths
- database_oid is unique across all databases

#### duckdb_settings()

Returns all database settings with their current values.

**Signature:**
```sql
duckdb_settings() -> TABLE(
    name VARCHAR,
    value VARCHAR,
    description VARCHAR,
    input_type VARCHAR,
    default_value VARCHAR,
    min_value VARCHAR,
    max_value VARCHAR,
    options VARCHAR[],
    requires_restart BOOLEAN
)
```

**Description:** Lists all configurable settings with descriptions and constraints.

**Example:**
```sql
SELECT name, value, description
FROM duckdb_settings()
WHERE name LIKE '%memory%';
-- Result:
-- name              | value | description
-- ------------------|-------|----------------------------------------
-- memory_limit      | 0     | The maximum memory limit in bytes
-- default_order     | asc   | The default order direction
-- max_memory        | 0     | The maximum memory available to DuckDB
```

**Implementation Notes:**
- Settings are stored in __system_settings table
- value column shows current runtime value
- requires_restart indicates if change needs restart

### 2. Schema Object Functions

#### duckdb_tables()

Returns information about all tables and views in the database.

**Signature:**
```sql
duckdb_tables() -> TABLE(
    schema_name VARCHAR,
    table_name VARCHAR,
    table_type VARCHAR,
    temporary BOOLEAN,
    internal BOOLEAN,
    cardinality BIGINT,
    column_count BIGINT,
    comment VARCHAR
)
```

**Description:** Lists all tables with their schema, type, and properties.

**Example:**
```sql
SELECT schema_name, table_name, table_type, cardinality
FROM duckdb_tables()
WHERE schema_name = 'main';
-- Result:
-- schema_name | table_name | table_type | cardinality
-- -------------|------------|------------|------------
-- main        | users      | BASE TABLE | 1000
-- main        | orders     | BASE TABLE | 5000
-- main        | user_view  | VIEW       | 1000
```

**Implementation Notes:**
- table_type: 'BASE TABLE', 'VIEW', 'LOCAL TEMPORARY', 'SYSTEM TABLE'
- cardinality is estimated row count (0 if unknown)
- internal=true for system tables

#### duckdb_columns()

Returns information about all columns in all tables.

**Signature:**
```sql
duckdb_columns() -> TABLE(
    schema_name VARCHAR,
    table_name VARCHAR,
    column_name VARCHAR,
    column_index BIGINT,
    data_type VARCHAR,
    data_type_id BIGINT,
    internal BOOLEAN,
    comment VARCHAR
)
```

**Description:** Lists all columns with their types and properties.

**Example:**
```sql
SELECT table_name, column_name, data_type
FROM duckdb_columns()
WHERE table_name = 'users';
-- Result:
-- table_name | column_name | data_type
-- ------------|-------------|----------
-- users      | id          | BIGINT
-- users      | name        | VARCHAR
-- users      | email       | VARCHAR
-- users      | created_at  | TIMESTAMP
```

**Implementation Notes:**
- column_index is 0-based position in table
- data_type_id maps to internal type system
- comment contains column comments if any

#### duckdb_views()

Returns information about all views in the database.

**Signature:**
```sql
duckdb_views() -> TABLE(
    schema_name VARCHAR,
    view_name VARCHAR,
    sql VARCHAR,
    temporary BOOLEAN,
    internal BOOLEAN,
    comment VARCHAR
)
```

**Description:** Lists all views with their defining SQL.

**Example:**
```sql
SELECT view_name, sql
FROM duckdb_views()
WHERE schema_name = 'main';
-- Result:
-- view_name | sql
-- -----------|----------------------------------------
-- active_users | SELECT * FROM users WHERE active = true
```

**Implementation Notes:**
- sql column contains the original CREATE VIEW statement
- temporary views exist only in session
- internal views are system views

#### duckdb_constraints()

Returns information about all table constraints.

**Signature:**
```sql
duckdb_constraints() -> TABLE(
    schema_name VARCHAR,
    table_name VARCHAR,
    constraint_name VARCHAR,
    constraint_type VARCHAR,
    expression VARCHAR,
    constraint_text VARCHAR
)
```

**Description:** Lists all constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK).

**Example:**
```sql
SELECT table_name, constraint_name, constraint_type
FROM duckdb_constraints();
-- Result:
-- table_name | constraint_name | constraint_type
-- ------------|-----------------|----------------
-- users      | users_pkey      | PRIMARY KEY
-- orders     | orders_user_fk  | FOREIGN KEY
-- products   | products_code_u | UNIQUE
```

**Implementation Notes:**
- constraint_type: 'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK'
- expression contains CHECK constraint expression
- constraint_text contains full constraint definition

#### duckdb_indexes()

Returns information about all indexes in the database.

**Signature:**
```sql
duckdb_indexes() -> TABLE(
    schema_name VARCHAR,
    table_name VARCHAR,
    index_name VARCHAR,
    index_type VARCHAR,
    sql VARCHAR,
    constraint_name VARCHAR,
    expression VARCHAR,
    comment VARCHAR
)
```

**Description:** Lists all indexes with their definitions.

**Example:**
```sql
SELECT table_name, index_name, index_type, expression
FROM duckdb_indexes();
-- Result:
-- table_name | index_name | index_type | expression
-- ------------|------------|------------|----------
-- users      | idx_users_name | ART | name
-- orders     | idx_orders_date | ART | order_date
```

**Implementation Notes:**
- index_type: 'ART', 'HASH', 'SKIP_LIST'
- expression contains indexed columns/expressions
- constraint_name for index-backed constraints

#### duckdb_sequences()

Returns information about all sequences.

**Signature:**
```sql
duckdb_sequences() -> TABLE(
    schema_name VARCHAR,
    sequence_name VARCHAR,
    start_value BIGINT,
    min_value BIGINT,
    max_value BIGINT,
    increment_by BIGINT,
    cycle BOOLEAN,
    current_value BIGINT,
    comment VARCHAR
)
```

**Description:** Lists all sequences with their properties.

**Example:**
```sql
SELECT sequence_name, start_value, increment_by, current_value
FROM duckdb_sequences();
-- Result:
-- sequence_name | start_value | increment_by | current_value
-- ---------------|-------------|--------------|--------------
-- user_id_seq   | 1           | 1            | 1000
-- order_no_seq  | 1000        | 1            | 5000
```

**Implementation Notes:**
- current_value is last value returned by NEXTVAL
- cycle indicates if sequence wraps around
- min_value/max_value define bounds

### 3. Function Information Functions

#### duckdb_functions()

Returns information about all functions in the system.

**Signature:**
```sql
duckdb_functions() -> TABLE(
    schema_name VARCHAR,
    function_name VARCHAR,
    function_type VARCHAR,
    description VARCHAR,
    return_type VARCHAR,
    parameters VARCHAR[],
    parameter_types VARCHAR[],
    varargs BOOLEAN,
    macro_definition VARCHAR,
    has_side_effects BOOLEAN
)
```

**Description:** Lists all functions with signatures and descriptions.

**Example:**
```sql
SELECT function_name, return_type, parameters
FROM duckdb_functions()
WHERE function_type = 'scalar'
LIMIT 5;
-- Result:
-- function_name | return_type | parameters
-- ---------------|-------------|----------
-- abs           | BIGINT      | [x]
-- concat        | VARCHAR     | [str1, str2]
-- length        | BIGINT      | [str]
-- lower         | VARCHAR     | [str]
-- upper         | VARCHAR     | [str]
```

**Implementation Notes:**
- function_type: 'scalar', 'aggregate', 'table', 'window'
- parameters array contains parameter names
- parameter_types array contains parameter types
- varargs=true for functions with variable arguments

#### duckdb_keywords()

Returns all reserved keywords in DuckDB.

**Signature:**
```sql
duckdb_keywords() -> TABLE(
    keyword_name VARCHAR,
    keyword_category VARCHAR
)
```

**Description:** Lists all SQL keywords and their categories.

**Example:**
```sql
SELECT * FROM duckdb_keywords() WHERE keyword_category = 'reserved';
-- Result:
-- keyword_name | keyword_category
-- --------------|-----------------
-- SELECT       | reserved
-- FROM         | reserved
-- WHERE        | reserved
-- ORDER        | reserved
```

**Implementation Notes:**
- keyword_category: 'reserved', 'type_func', 'column_name'
- Reserved keywords cannot be used as identifiers

### 4. System State Functions

#### duckdb_extensions()

Returns information about loaded extensions.

**Signature:**
```sql
duckdb_extensions() -> TABLE(
    extension_name VARCHAR,
    loaded BOOLEAN,
    installed BOOLEAN,
    install_path VARCHAR,
    description VARCHAR,
    aliases VARCHAR[],
    extension_version VARCHAR,
    install_mode VARCHAR
)
```

**Description:** Lists all available extensions and their status.

**Example:**
```sql
SELECT extension_name, loaded, description
FROM duckdb_extensions()
WHERE loaded = true;
-- Result:
-- extension_name | loaded | description
-- ----------------|--------|----------------------------------------
-- parquet         | true   | Parquet file format support
-- json            | true   | JSON extension
-- fts             | true   | Full text search
```

**Implementation Notes:**
- loaded=true if currently active
- installed=true if extension is installed
- install_mode: 'repo', 'local', 'custom'

#### duckdb_optimizers()

Returns information about query optimizer rules.

**Signature:**
```sql
duckdb_optimizers() -> TABLE(
    optimizer_name VARCHAR,
    optimizer_enabled BOOLEAN,
    optimizer_description VARCHAR
)
```

**Description:** Lists all optimizer rules and their status.

**Example:**
```sql
SELECT optimizer_name, optimizer_enabled
FROM duckdb_optimizers();
-- Result:
-- optimizer_name | optimizer_enabled
-- ----------------|------------------
-- constant_folding | true
-- predicate_pushdown | true
-- join_order | true
-- index_selection | true
```

**Implementation Notes:**
- optimizer_enabled can be toggled
- Description explains what rule does

#### duckdb_memory_usage()

Returns memory usage statistics.

**Signature:**
```sql
duckdb_memory_usage() -> TABLE(
    memory_type VARCHAR,
    memory_usage BIGINT,
    description VARCHAR
)
```

**Description:** Shows breakdown of memory usage by component.

**Example:**
```sql
SELECT memory_type, memory_usage/1024/1024 AS mb
FROM duckdb_memory_usage();
-- Result:
-- memory_type | mb
-- --------------|-----
-- database      | 128
-- optimizer     | 64
-- execution     | 256
-- total         | 448
```

**Implementation Notes:**
- memory_usage in bytes
- Includes buffer pool, execution memory, etc.

### 5. Dependency Functions

#### duckdb_dependencies()

Returns object dependencies.

**Signature:**
```sql
duckdb_dependencies() -> TABLE(
    classid BIGINT,
    objid BIGINT,
    objsubid BIGINT,
    refclassid BIGINT,
    refobjid BIGINT,
    refobjsubid BIGINT,
    deptype VARCHAR
)
```

**Description:** Shows dependencies between database objects.

**Example:**
```sql
SELECT * FROM duckdb_dependencies() LIMIT 5;
-- Result shows internal object dependency graph
```

**Implementation Notes:**
- Used internally for DROP CASCADE
- deptype: 'normal', 'automatic', 'internal'

## Implementation Details

### Storage Structure

System function metadata is stored in system catalog tables:

```sql
-- Function registry
CREATE TABLE __system_functions (
    function_id BIGINT PRIMARY KEY,
    schema_name VARCHAR NOT NULL,
    function_name VARCHAR NOT NULL,
    function_type VARCHAR NOT NULL,
    return_type VARCHAR,
    parameters JSON,
    description TEXT,
    category VARCHAR,
    tags JSON[]
);

-- Settings storage
CREATE TABLE __system_settings (
    setting_name VARCHAR PRIMARY KEY,
    setting_value VARCHAR,
    setting_type VARCHAR NOT NULL,
    description TEXT,
    default_value VARCHAR,
    min_value VARCHAR,
    max_value VARCHAR,
    options VARCHAR[],
    requires_restart BOOLEAN
);
```

### Function Resolution

System functions are resolved through special table function mechanism:

```go
type SystemFunction interface {
    Name() string
    Arguments() []types.Type
    ReturnType() types.Type
    Execute(ctx Context, args []Expression) (*DataChunk, error)
}
```

### Caching Strategy

1. **Metadata Cache**: Function metadata cached in memory
2. **Settings Cache**: Current settings values cached
3. **Schema Cache**: Table/column metadata cached
4. **LRU Eviction**: Automatic cache eviction

### Performance Optimizations

1. **Lazy Loading**: Metadata loaded on first access
2. **Incremental Updates**: Only changed metadata refreshed
3. **Parallel Scanning**: Multiple system tables scanned in parallel
4. **Predicate Pushdown**: Filters pushed to metadata queries

## Testing Requirements

### Unit Tests

1. **Function Metadata Tests**: Verify all functions registered
2. **Return Schema Tests**: Verify correct return types
3. **Data Accuracy Tests**: Verify returned data matches catalog
4. **Filter Tests**: Test WHERE clause support
5. **Join Tests**: Test joining with other tables

### Integration Tests

1. **DuckDB Compatibility**: Compare output with DuckDB v1.4.3
2. **Performance Tests**: Benchmark large catalog queries
3. **Concurrent Access**: Test thread safety
4. **Transaction Tests**: Test visibility in transactions

### Test Data

```sql
-- Setup test schema
CREATE SCHEMA test_schema;
CREATE TABLE test_schema.users (
    id BIGINT PRIMARY KEY,
    name VARCHAR NOT NULL,
    email VARCHAR UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE VIEW test_schema.user_view AS SELECT * FROM test_schema.users;
CREATE INDEX idx_users_name ON test_schema.users(name);
CREATE SEQUENCE test_schema.user_id_seq START 1000;
```

## Error Handling

### Error Codes

- `SYSTEM_FUNCTION_ERROR`: General system function error
- `CATALOG_ERROR`: Catalog access error
- `PERMISSION_DENIED`: Insufficient privileges
- `INVALID_ARGUMENT`: Invalid function argument

### Error Messages

```sql
-- Invalid argument
SELECT * FROM duckdb_table_info(); -- ERROR: function requires 1 argument

-- Non-existent object
SELECT * FROM duckdb_table_info('nonexistent'); -- Returns empty result
```

## Security Considerations

### Access Control

1. **Schema Visibility**: Users see only accessible schemas
2. **Function Filtering**: Internal functions hidden from users
3. **Setting Sensitivity**: Sensitive settings masked
4. **Audit Logging**: All system function calls logged

### Privileges

```sql
-- Grant access to system functions
GRANT SELECT ON duckdb_tables() TO user;
GRANT EXECUTE ON FUNCTION duckdb_settings() TO user;
```

## Future Enhancements

1. **Custom System Functions**: User-defined system functions
2. **Dynamic Registration**: Runtime function registration
3. **Performance Metrics**: Query performance statistics
4. **Extended Metadata**: Additional metadata attributes
5. **Cross-Database**: Multi-database system functions