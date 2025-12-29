# Query Appender Specification

## Requirements

### Requirement: Query Appender Creation

The package SHALL create query appenders with custom SQL.

#### Scenario: Create query appender with INSERT query
- GIVEN valid connection and INSERT query
- WHEN calling NewQueryAppender(conn, "INSERT INTO t SELECT * FROM appended_data", "", colTypes, nil)
- THEN Appender is returned without error

#### Scenario: Create query appender with MERGE query
- GIVEN valid connection and MERGE INTO query
- WHEN calling NewQueryAppender(conn, "MERGE INTO t USING appended_data ...", "", colTypes, nil)
- THEN Appender is returned without error

#### Scenario: Empty query error
- GIVEN valid connection
- WHEN calling NewQueryAppender(conn, "", "", colTypes, nil)
- THEN error indicating empty query is returned

#### Scenario: Empty column types error
- GIVEN valid connection and query
- WHEN calling NewQueryAppender(conn, query, "", nil, nil)
- THEN error indicating missing column types is returned

#### Scenario: Column name count mismatch error
- GIVEN 3 column types and 2 column names
- WHEN calling NewQueryAppender(conn, query, "", colTypes, colNames)
- THEN error indicating column count mismatch is returned

#### Scenario: Default table name
- GIVEN table parameter is empty string
- WHEN query appender is created
- THEN temporary table name defaults to "appended_data"

### Requirement: Query Appender Row Batching

The query appender SHALL batch rows like standard appender.

#### Scenario: Append single row
- GIVEN query appender with 2 columns
- WHEN calling AppendRow(1, "test")
- THEN row is buffered without error

#### Scenario: Append row with wrong column count
- GIVEN query appender with 2 columns
- WHEN calling AppendRow(1, "test", "extra")
- THEN error indicating column count mismatch is returned

#### Scenario: Auto-flush at threshold
- GIVEN query appender with threshold 10
- WHEN appending 10 rows
- THEN query is automatically executed

### Requirement: Query Appender Flush Execution

The query appender SHALL execute the custom query on flush.

#### Scenario: Flush executes INSERT query
- GIVEN query appender with INSERT query and 3 batched rows
- WHEN calling Flush()
- THEN rows are inserted into target table

#### Scenario: Flush executes MERGE query
- GIVEN query appender with MERGE INTO query and batched rows
- WHEN calling Flush()
- THEN MERGE operation is performed

#### Scenario: Flush executes UPDATE query
- GIVEN query appender with UPDATE query and batched criteria
- WHEN calling Flush()
- THEN matching rows are updated

#### Scenario: Flush executes DELETE query
- GIVEN query appender with DELETE query and batched criteria
- WHEN calling Flush()
- THEN matching rows are deleted

#### Scenario: Flush clears buffer on success
- GIVEN query appender with 5 batched rows
- WHEN Flush() succeeds
- THEN buffer is empty

#### Scenario: Query error preserves buffer
- GIVEN query appender with invalid query
- WHEN Flush() fails
- THEN buffer still contains batched rows

### Requirement: Temporary Table Management

The query appender SHALL manage temporary tables correctly.

#### Scenario: Temp table created on first flush
- GIVEN new query appender
- WHEN first Flush() is called
- THEN temporary table is created with correct schema

#### Scenario: Temp table reused across flushes
- GIVEN query appender after successful flush
- WHEN second Flush() is called
- THEN same temporary table is reused

#### Scenario: Temp table cleaned up on close
- GIVEN query appender with temp table
- WHEN Close() is called
- THEN temporary table is dropped

#### Scenario: Nested types in temp table
- GIVEN query appender with LIST(INTEGER) column type
- WHEN creating temp table
- THEN column has correct nested type

### Requirement: Query Appender Close

The query appender SHALL clean up resources on close.

#### Scenario: Close flushes remaining data
- GIVEN query appender with unbuffered rows
- WHEN Close() is called
- THEN remaining rows are flushed before close

#### Scenario: Close drops temp table
- GIVEN query appender with created temp table
- WHEN Close() is called
- THEN temp table no longer exists

#### Scenario: Close after close returns error
- GIVEN closed query appender
- WHEN Close() is called again
- THEN error is returned

### Requirement: TypeInfo SQL Generation

TypeInfo SHALL generate correct SQL type declarations.

#### Scenario: Primitive type SQL
- GIVEN TypeInfo with TYPE_INTEGER
- WHEN calling SQLType()
- THEN "INTEGER" is returned

#### Scenario: LIST type SQL
- GIVEN TypeInfo with TYPE_LIST containing TYPE_VARCHAR
- WHEN calling SQLType()
- THEN "LIST(VARCHAR)" is returned

#### Scenario: STRUCT type SQL
- GIVEN TypeInfo with TYPE_STRUCT with name/age fields
- WHEN calling SQLType()
- THEN "STRUCT(name VARCHAR, age INTEGER)" is returned

#### Scenario: MAP type SQL
- GIVEN TypeInfo with TYPE_MAP from VARCHAR to INTEGER
- WHEN calling SQLType()
- THEN "MAP(VARCHAR, INTEGER)" is returned

