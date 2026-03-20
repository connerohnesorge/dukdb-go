# Pragma Specification

## Requirements

### Requirement: Overview

The system MUST implement the following functionality.


This specification defines the implementation of DuckDB v1.4.3 compatible PRAGMA statements in dukdb-go. PRAGMA statements provide a way to query or modify the internal state of the database engine.


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: PRAGMA Categories

The system MUST implement the following functionality.


#### 1. Information PRAGMAs

These PRAGMAs return information about the database or its objects.

##### PRAGMA database_list

Lists all databases attached to the current session.

**Syntax:**
```sql
PRAGMA database_list;
```

**Return Columns:**
- `seq` (INTEGER): Sequence number
- `name` (VARCHAR): Database name
- `file` (VARCHAR): Database file path

**Example:**
```sql
PRAGMA database_list;
-- Result:
-- seq | name  | file
-- ----|-------|--------
-- 0   | main  | /data/main.db
-- 1   | aux   | /data/aux.db
```

**Implementation Notes:**
- Returns all attached databases
- ':memory:' for in-memory databases
- Empty string for temporary databases

##### PRAGMA database_size

Returns information about the database file size and block count.

**Syntax:**
```sql
PRAGMA database_size;
```

**Return Columns:**
- `database_name` (VARCHAR): Name of the database
- `database_size` (VARCHAR): Human-readable size
- `block_size` (INTEGER): Size of each block
- `total_blocks` (INTEGER): Total number of blocks
- `free_blocks` (INTEGER): Number of free blocks
- `used_blocks` (INTEGER): Number of used blocks
- `percentage_used` (REAL): Percentage of blocks used

**Example:**
```sql
PRAGMA database_size;
-- Result:
-- database_name | database_size | block_size | total_blocks | free_blocks | used_blocks | percentage_used
-- --------------|---------------|------------|--------------|-------------|-------------|----------------
-- main          | 1.5GB         | 262144     | 6000         | 1000        | 5000        | 83.3
```

##### PRAGMA table_info(table_name)

Returns information about the columns in a table.

**Syntax:**
```sql
PRAGMA table_info('table_name');
```

**Parameters:**
- `table_name` (VARCHAR): Name of the table (case-sensitive)

**Return Columns:**
- `cid` (INTEGER): Column ID (0-based)
- `name` (VARCHAR): Column name
- `type` (VARCHAR): Column type
- `notnull` (BOOLEAN): Whether column has NOT NULL constraint
- `dflt_value` (VARCHAR): Default value (NULL if none)
- `pk` (BOOLEAN): Whether column is part of primary key

**Example:**
```sql
PRAGMA table_info('users');
-- Result:
-- cid | name     | type      | notnull | dflt_value | pk
-- ----|----------|-----------|---------|------------|----
-- 0   | id       | BIGINT    | true    | NULL       | true
-- 1   | name     | VARCHAR   | true    | NULL       | false
-- 2   | email    | VARCHAR   | false   | NULL       | false
-- 3   | created  | TIMESTAMP | false   | CURRENT_TIMESTAMP | false
```

**Implementation Notes:**
- Table name is case-sensitive
- Returns empty result if table doesn't exist
- Primary key columns marked with pk=true

##### PRAGMA table_storage_info(table_name)

Returns detailed storage information about a table.

**Syntax:**
```sql
PRAGMA table_storage_info('table_name');
```

**Parameters:**
- `table_name` (VARCHAR): Name of the table

**Return Columns:**
- `column_name` (VARCHAR): Name of the column
- `column_id` (INTEGER): ID of the column
- `column_path` (VARCHAR): Full path to column data
- `segment_id` (INTEGER): ID of the segment
- `segment_type` (VARCHAR): Type of segment
- `start` (INTEGER): Start row in segment
- `count` (INTEGER): Number of rows in segment
- `compression` (VARCHAR): Compression type
- `stats` (VARCHAR): Statistics information
- `persistent` (BOOLEAN): Whether data is persistent
- `block_id` (INTEGER): Block ID
- `block_offset` (INTEGER): Offset in block

**Example:**
```sql
PRAGMA table_storage_info('users');
-- Returns detailed storage layout information
```

##### PRAGMA storage_info(table_name)

Alias for table_storage_info.

**Syntax:**
```sql
PRAGMA storage_info('table_name');
```

##### PRAGMA stats(table_name)

Returns statistics for a table.

**Syntax:**
```sql
PRAGMA stats('table_name');
```

**Parameters:**
- `table_name` (VARCHAR): Name of the table

**Return Columns:**
- `name` (VARCHAR): Column name
- `statistics` (VARCHAR): Statistics as JSON

**Example:**
```sql
PRAGMA stats('users');
-- Result:
-- name | statistics
-- -------|---------------------------------------------------------------
-- id   | {"min": 1, "max": 1000, "null_count": 0, "distinct_count": 1000}
-- name | {"null_count": 0, "distinct_count": 950}
```

#### 2. Configuration PRAGMAs

These PRAGMAs modify the behavior of the database.

##### PRAGMA version

Returns the DuckDB version.

**Syntax:**
```sql
PRAGMA version;
```

**Return:**
Single row with version information.

**Example:**
```sql
PRAGMA version;
-- Result:
-- library_version | source_id
-- ----------------|----------
-- v1.4.3          | 5c5b5c9
```

**Implementation Notes:**
- Returns dukdb-go version matching DuckDB v1.4.3
- source_id should match DuckDB git hash

##### PRAGMA platform

Returns platform information.

**Syntax:**
```sql
PRAGMA platform;
```

**Return:**
Single row with platform details.

**Example:**
```sql
PRAGMA platform;
-- Result:
-- platform
-- -----------
-- linux_amd64
```

##### PRAGMA functions

Lists all functions or searches for specific functions.

**Syntax:**
```sql
PRAGMA functions;                    -- List all functions
PRAGMA functions('pattern');         -- Search functions
```

**Parameters:**
- `pattern` (VARCHAR, optional): Search pattern (LIKE syntax)

**Return Columns:**
- `name` (VARCHAR): Function name
- `type` (VARCHAR): Function type
- `parameters` (VARCHAR): Parameter list
- `return_type` (VARCHAR): Return type
- `description` (VARCHAR): Function description

**Example:**
```sql
PRAGMA functions('string%');
-- Result:
-- name        | type   | parameters         | return_type | description
-- ------------|--------|--------------------|-------------|-------------
-- string_agg  | scalar | (text, delimiter)  | VARCHAR     | Concatenate strings
-- string_split| scalar | (text, delimiter)  | VARCHAR[]   | Split string
```

##### PRAGMA collations

Lists all available collations.

**Syntax:**
```sql
PRAGMA collations;
```

**Return Columns:**
- `name` (VARCHAR): Collation name
- `description` (VARCHAR): Collation description

**Example:**
```sql
PRAGMA collations;
-- Result:
-- name      | description
-- ----------|-------------------------------
-- binary    | Binary collation
-- nocase    | Case-insensitive collation
-- rtrim     | Right-trim collation
```

#### 3. Debug PRAGMAs

These PRAGMAs are used for debugging and development.

##### PRAGMA disable_optimizer

Disables the query optimizer.

**Syntax:**
```sql
PRAGMA disable_optimizer;
```

**Implementation Notes:**
- Affects current session only
- Can significantly impact performance
- Useful for debugging query plans

##### PRAGMA enable_optimizer

Enables the query optimizer (default).

**Syntax:**
```sql
PRAGMA enable_optimizer;
```

##### PRAGMA enable_profiling

Enables query profiling.

**Syntax:**
```sql
PRAGMA enable_profiling;
PRAGMA enable_profiling = 'json';     -- Output format
PRAGMA enable_profiling = 'query_tree';
PRAGMA enable_profiling = 'detailed';
```

**Parameters:**
- `output_format` (VARCHAR, optional): 'json', 'query_tree', 'detailed'

**Implementation Notes:**
- Profiling information printed after each query
- Affects current session only
- May impact query performance

##### PRAGMA disable_profiling

Disables query profiling (default).

**Syntax:**
```sql
PRAGMA disable_profiling;
```

##### PRAGMA profiling_output

Sets the output file for profiling information.

**Syntax:**
```sql
PRAGMA profiling_output = 'profile.json';
```

**Parameters:**
- `filename` (VARCHAR): Output file path

##### PRAGMA explain_output

Controls the output format of EXPLAIN.

**Syntax:**
```sql
PRAGMA explain_output = 'all';        -- Default
PRAGMA explain_output = 'optimized';  -- Only optimized plan
PRAGMA explain_output = 'physical';   -- Only physical plan
```

#### 4. Memory PRAGMAs

##### PRAGMA memory_limit

Sets or shows the memory limit.

**Syntax:**
```sql
PRAGMA memory_limit;                  -- Show current limit
PRAGMA memory_limit = '1GB';          -- Set limit
PRAGMA memory_limit = 0;              -- No limit
```

**Parameters:**
- `limit` (VARCHAR or BIGINT): Memory limit (with units) or 0 for no limit

**Implementation Notes:**
- Units: B, KB, MB, GB, TB
- 0 means no limit
- Affects current session only

##### PRAGMA threads

Sets or shows the number of threads.

**Syntax:**
```sql
PRAGMA threads;                       -- Show current setting
PRAGMA threads = 4;                   -- Set thread count
```

**Parameters:**
- `count` (INTEGER): Number of threads (0 = auto)

**Implementation Notes:**
- 0 means use all available cores
- Affects current session only

#### 5. Checkpoint PRAGMAs

##### PRAGMA checkpoint

Performs a checkpoint operation.

**Syntax:**
```sql
PRAGMA checkpoint;
```

**Implementation Notes:**
- Writes all dirty pages to disk
- Truncates WAL file
- Blocks until complete

##### PRAGMA wal_checkpoint

Performs a WAL checkpoint.

**Syntax:**
```sql
PRAGMA wal_checkpoint;
```

**Return Columns:**
- `busy` (INTEGER): 1 if checkpoint was blocked
- `log` (INTEGER): Number of WAL frames
- `checkpointed` (INTEGER): Number of frames checkpointed

#### 6. Import/Export PRAGMAs

##### PRAGMA import_database

Imports a database from a directory.

**Syntax:**
```sql
PRAGMA import_database('path/to/directory');
```

**Parameters:**
- `directory` (VARCHAR): Path to directory containing exported database

##### PRAGMA export_database

Exports the entire database to a directory.

**Syntax:**
```sql
PRAGMA export_database('path/to/directory');
```

**Parameters:**
- `directory` (VARCHAR): Path to directory for export

**Implementation Notes:**
- Creates directory if it doesn't exist
- Overwrites existing files
- Exports all schemas and data

#### 7. Transaction PRAGMAs

##### PRAGMA transaction_isolation

Sets or shows the transaction isolation level.

**Syntax:**
```sql
PRAGMA transaction_isolation;         -- Show current level
PRAGMA transaction_isolation = 'read committed';
PRAGMA transaction_isolation = 'repeatable read';
PRAGMA transaction_isolation = 'serializable';
```

**Parameters:**
- `level` (VARCHAR): Isolation level

**Implementation Notes:**
- Default is SERIALIZABLE
- Affects current transaction only


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Implementation Details

The system MUST implement the following functionality.


#### PRAGMA Handler Framework

```go
type PragmaHandler interface {
    Name() string
    Description() string
    Execute(ctx Context, args []Expression) (Result, error)
}

type PragmaRegistry struct {
    handlers map[string]PragmaHandler
}

func (r *PragmaRegistry) Register(handler PragmaHandler) {
    r.handlers[handler.Name()] = handler
}
```

#### PRAGMA Parser Extension

```go
type PragmaStatement struct {
    Name      string
    Arguments []Expression
}

// Parser recognizes PRAGMA keyword
pragma_stmt:
    PRAGMA identifier
    | PRAGMA identifier '(' expression_list ')'
    | PRAGMA identifier '=' expression
```

#### PRAGMA Execution

```go
func (e *Engine) ExecutePragma(stmt PragmaStatement) (Result, error) {
    handler, ok := e.pragmaRegistry.Get(stmt.Name)
    if !ok {
        return nil, ErrUnknownPragma
    }

    return handler.Execute(e.context, stmt.Arguments)
}
```

#### PRAGMA Categories

```go
const (
    CategoryInformation = "Information"
    CategoryConfiguration = "Configuration"
    CategoryDebug = "Debug"
    CategoryMemory = "Memory"
    CategoryCheckpoint = "Checkpoint"
    CategoryImportExport = "Import/Export"
    CategoryTransaction = "Transaction"
)
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Error Handling

The system MUST implement the following functionality.


#### PRAGMA Errors

- `UNKNOWN_PRAGMA`: PRAGMA statement not recognized
- `INVALID_ARGUMENT_COUNT`: Wrong number of arguments
- `INVALID_ARGUMENT_TYPE`: Wrong argument type
- `INVALID_ARGUMENT_VALUE`: Invalid argument value
- `OBJECT_NOT_FOUND`: Referenced object doesn't exist

#### Error Examples

```sql
-- Unknown PRAGMA
PRAGMA unknown_pragma;
-- ERROR: unknown PRAGMA: unknown_pragma

-- Wrong argument count
PRAGMA table_info();
-- ERROR: PRAGMA table_info requires 1 argument

-- Non-existent table
PRAGMA table_info('nonexistent');
-- Returns empty result set
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Testing Requirements

The system MUST implement the following functionality.


#### Unit Tests

1. **PRAGMA Parser Tests**: Verify correct parsing
2. **PRAGMA Handler Tests**: Test each PRAGMA implementation
3. **Argument Validation Tests**: Test error handling
4. **Result Format Tests**: Verify correct output format

#### Integration Tests

1. **DuckDB Compatibility**: Compare output with DuckDB v1.4.3
2. **Transaction Tests**: Test PRAGMAs in transactions
3. **Multi-session Tests**: Test session-specific PRAGMAs
4. **Error Condition Tests**: Test all error paths

#### Test Examples

```sql
-- Test table_info
PRAGMA table_info('users');

-- Test database_size
PRAGMA database_size;

-- Test version
PRAGMA version;

-- Test functions
PRAGMA functions('string%');

-- Test checkpoint
PRAGMA checkpoint;

-- Test memory limit
PRAGMA memory_limit = '500MB';
PRAGMA memory_limit;
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Security Considerations

The system MUST implement the following functionality.


#### Access Control

1. **Information Disclosure**: Some PRAGMAs reveal system information
2. **Configuration Changes**: PRAGMAs can modify system behavior
3. **Resource Usage**: PRAGMAs can affect resource limits

#### Privileges

```sql
-- Grant PRAGMA execution
GRANT PRAGMA ON database_size TO user;
GRANT PRAGMA ON table_info TO user;

-- Revoke dangerous PRAGMAs
REVOKE PRAGMA ON checkpoint FROM user;
```

#### Audit Logging

All PRAGMA executions are logged:
```
[timestamp] user='user1' pragma='table_info' args=['users'] result='SUCCESS'
[timestamp] user='user2' pragma='memory_limit' args=['1GB'] result='SUCCESS'
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Performance Considerations

The system MUST implement the following functionality.


#### Implementation Optimizations

1. **Cached Results**: Some PRAGMAs cache results
2. **Lazy Loading**: Metadata loaded on demand
3. **Index Usage**: Use indexes for metadata queries
4. **Parallel Scanning**: Scan multiple objects in parallel

#### Resource Usage

1. **Memory Usage**: PRAGMAs should use minimal memory
2. **CPU Usage**: Avoid expensive computations
3. **I/O Usage**: Minimize disk access
4. **Locking**: Avoid long-held locks


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Future Enhancements

The system MUST implement the following functionality.


1. **Custom PRAGMAs**: User-defined PRAGMA support
2. **Persistent Settings**: Some PRAGMAs persist across sessions
3. **Group PRAGMAs**: PRAGMAs that affect multiple settings
4. **Conditional PRAGMAs**: PRAGMAs with conditions
5. **Plugin PRAGMAs**: PRAGMAs from extensions

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

