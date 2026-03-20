# Sql Syntax Specification

## Requirements

### Requirement: TRUNCATE TABLE SHALL support IF EXISTS clause

The TRUNCATE TABLE statement SHALL accept an optional IF EXISTS clause that suppresses the error when the target table does not exist.

#### Scenario: TRUNCATE TABLE IF EXISTS on existing table
```
Given a table "test_table" with 50 rows
When the user executes "TRUNCATE TABLE IF EXISTS test_table"
Then all rows are removed
And the result indicates 50 rows affected
```

#### Scenario: TRUNCATE TABLE IF EXISTS on non-existent table
```
Given no table named "ghost_table" exists
When the user executes "TRUNCATE TABLE IF EXISTS ghost_table"
Then no error is raised
And the result indicates 0 rows affected
```

#### Scenario: TRUNCATE TABLE without IF EXISTS on non-existent table
```
Given no table named "ghost_table" exists
When the user executes "TRUNCATE TABLE ghost_table"
Then an error is returned indicating the table does not exist
```

### Requirement: TRUNCATE TABLE SHALL support schema-qualified names in executor

The TRUNCATE TABLE executor SHALL correctly resolve schema-qualified table names when looking up the storage table.

#### Scenario: TRUNCATE with schema-qualified table name
```
Given a schema "myschema" exists
And a table "myschema.test_table" with 10 rows exists
When the user executes "TRUNCATE TABLE myschema.test_table"
Then all rows in myschema.test_table are removed
And the result indicates 10 rows affected
```

### Requirement: TRUNCATE TABLE SHALL be transactional

The TRUNCATE TABLE statement SHALL support transaction rollback via WAL integration and undo recording.

#### Scenario: TRUNCATE is rolled back in transaction
```
Given a table "test_table" with 10 rows
When the user executes "BEGIN"
And the user executes "TRUNCATE TABLE test_table"
And SELECT count(*) FROM test_table returns 0
And the user executes "ROLLBACK"
Then SELECT count(*) FROM test_table returns 10
```

### Requirement: TRUNCATE TABLE SHALL clear associated index entries

When a table is truncated, all associated indexes SHALL be cleared of stale entries.

#### Scenario: TRUNCATE clears indexes
```
Given a table "test_table" with an index on column "id"
And the table has rows with id values 1, 2, 3
When the user executes "TRUNCATE TABLE test_table"
Then the index contains no entries
And a subsequent INSERT with id=1 succeeds without unique violation
```

### Requirement: VALUES type inference SHALL use supertype promotion

The VALUES clause binder SHALL use proper supertype promotion across all rows to determine column types, matching UNION type coercion behavior.

#### Scenario: VALUES with mixed integer and float types
```
When the user executes "VALUES (1, 'text'), (2.5, NULL)"
Then column1 type is DOUBLE (promoted from INTEGER and DOUBLE)
And column2 type is VARCHAR (NULL inherits type from non-NULL rows)
```

#### Scenario: VALUES with all NULLs in a column
```
When the user executes "VALUES (1, NULL), (2, NULL)"
Then column2 type defaults to VARCHAR
Because all-NULL columns default to VARCHAR matching DuckDB behavior
```

#### Scenario: VALUES with implicit cast insertion
```
When the user executes "VALUES (1), (2.5), (3)"
Then all values in column1 are cast to DOUBLE
And the integer values 1 and 3 are implicitly cast to 1.0 and 3.0
```

### Requirement: FETCH FIRST SHALL extend SelectStmt AST

The parser SHALL extend the SelectStmt AST node with a `FetchWithTies` boolean field to support the WITH TIES variant of FETCH FIRST.

#### Scenario: FetchWithTies field set on WITH TIES
```
Given a SELECT statement with FETCH FIRST 3 ROWS WITH TIES
When the parser produces a SelectStmt
Then FetchWithTies is true
And Limit contains the count expression (3)
```

### Requirement: VALUES SHALL extend TableRef AST

The parser SHALL extend the TableRef struct with a `ValuesRef` field pointing to a ValuesClause for VALUES table constructors.

#### Scenario: ValuesRef field set for VALUES in FROM
```
Given a FROM clause containing VALUES (1, 2), (3, 4)
When the parser produces a TableRef
Then ValuesRef is non-nil
And ValuesRef.Rows contains 2 rows of 2 expressions each
```

