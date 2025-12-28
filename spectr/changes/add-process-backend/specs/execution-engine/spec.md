## ADDED Requirements

### Requirement: SQL Parser

The engine SHALL parse SQL statements into an Abstract Syntax Tree (AST).

#### Scenario: Simple SELECT parsing
- GIVEN the SQL "SELECT 1"
- WHEN parsing the statement
- THEN a SelectStmt AST node is returned with one literal expression

#### Scenario: SELECT with FROM clause
- GIVEN the SQL "SELECT a, b FROM t"
- WHEN parsing the statement
- THEN column references and table reference are correctly parsed

#### Scenario: SELECT with WHERE clause
- GIVEN the SQL "SELECT * FROM t WHERE x > 5"
- WHEN parsing the statement
- THEN the WHERE predicate is parsed as a BinaryExpr

#### Scenario: INSERT statement parsing
- GIVEN the SQL "INSERT INTO t (a, b) VALUES (1, 2)"
- WHEN parsing the statement
- THEN an InsertStmt is returned with correct columns and values

#### Scenario: CREATE TABLE parsing
- GIVEN the SQL "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR)"
- WHEN parsing the statement
- THEN column definitions and constraints are correctly parsed

#### Scenario: Syntax error handling
- GIVEN invalid SQL "SELEC * FROM"
- WHEN parsing the statement
- THEN ErrorTypeParser is returned with descriptive message

### Requirement: Catalog Management

The engine SHALL maintain a catalog of schemas, tables, and columns.

#### Scenario: Default schema creation
- GIVEN a new engine instance
- WHEN initialized
- THEN a "main" schema exists by default

#### Scenario: CREATE TABLE registers in catalog
- GIVEN an empty catalog
- WHEN executing "CREATE TABLE t (id INT)"
- THEN table "t" is registered in the catalog
- AND column "id" with type INT is recorded

#### Scenario: DROP TABLE removes from catalog
- GIVEN a table "t" in the catalog
- WHEN executing "DROP TABLE t"
- THEN table "t" is removed from catalog

#### Scenario: Table not found error
- GIVEN no table "nonexistent" in catalog
- WHEN executing "SELECT * FROM nonexistent"
- THEN ErrorTypeCatalog is returned

### Requirement: Query Binding

The engine SHALL resolve names and types during query binding.

#### Scenario: Column resolution
- GIVEN table "t" with column "x" of type INT
- WHEN binding "SELECT x FROM t"
- THEN column reference resolves to t.x with type INT

#### Scenario: Type checking for operators
- GIVEN table "t" with INT column "x" and VARCHAR column "y"
- WHEN binding "SELECT x + y FROM t"
- THEN ErrorTypeMismatchType is returned

#### Scenario: Aggregate function binding
- GIVEN table "t" with INT column "x"
- WHEN binding "SELECT SUM(x) FROM t"
- THEN aggregate function is bound with correct return type

### Requirement: Query Planning

The engine SHALL create logical and physical query plans.

#### Scenario: Simple scan plan
- GIVEN table "t" with data
- WHEN planning "SELECT * FROM t"
- THEN logical plan contains LogicalScan for "t"
- AND physical plan contains SeqScan operator

#### Scenario: Filter plan
- GIVEN table "t" with data
- WHEN planning "SELECT * FROM t WHERE x > 5"
- THEN physical plan contains Filter operator above Scan

#### Scenario: Projection plan
- GIVEN table "t" with columns a, b, c
- WHEN planning "SELECT a, b FROM t"
- THEN physical plan contains Project operator

#### Scenario: Join plan
- GIVEN tables "a" and "b"
- WHEN planning "SELECT * FROM a JOIN b ON a.id = b.id"
- THEN physical plan contains HashJoin operator

### Requirement: Query Execution

The engine SHALL execute queries and return results.

#### Scenario: Literal SELECT
- GIVEN an engine instance
- WHEN executing "SELECT 1"
- THEN result contains one row with value 1

#### Scenario: Arithmetic expression
- GIVEN an engine instance
- WHEN executing "SELECT 1 + 2 * 3"
- THEN result contains one row with value 7

#### Scenario: Table scan
- GIVEN table "t" with rows [(1, 'a'), (2, 'b')]
- WHEN executing "SELECT * FROM t"
- THEN result contains both rows

#### Scenario: Filtered query
- GIVEN table "t" with rows [(1, 'a'), (2, 'b'), (3, 'c')]
- WHEN executing "SELECT * FROM t WHERE id > 1"
- THEN result contains rows with id 2 and 3

#### Scenario: Aggregate query
- GIVEN table "t" with values [1, 2, 3, 4, 5]
- WHEN executing "SELECT SUM(x), AVG(x) FROM t"
- THEN result contains SUM=15 and AVG=3

#### Scenario: GROUP BY query
- GIVEN table with groups
- WHEN executing "SELECT category, COUNT(*) FROM t GROUP BY category"
- THEN result contains one row per unique category

#### Scenario: ORDER BY query
- GIVEN table with unordered data
- WHEN executing "SELECT * FROM t ORDER BY x DESC"
- THEN results are sorted in descending order

#### Scenario: LIMIT query
- GIVEN table with 100 rows
- WHEN executing "SELECT * FROM t LIMIT 10"
- THEN result contains exactly 10 rows

### Requirement: DML Operations

The engine SHALL support INSERT, UPDATE, and DELETE.

#### Scenario: INSERT adds rows
- GIVEN empty table "t"
- WHEN executing "INSERT INTO t VALUES (1, 'a')"
- THEN table contains one row
- AND subsequent SELECT returns that row

#### Scenario: UPDATE modifies rows
- GIVEN table "t" with row (1, 'a')
- WHEN executing "UPDATE t SET name = 'b' WHERE id = 1"
- THEN row is modified to (1, 'b')
- AND affected row count is 1

#### Scenario: DELETE removes rows
- GIVEN table "t" with rows [(1, 'a'), (2, 'b')]
- WHEN executing "DELETE FROM t WHERE id = 1"
- THEN only row (2, 'b') remains
- AND affected row count is 1

### Requirement: Columnar Storage

The engine SHALL store data in columnar format.

#### Scenario: Column data storage
- GIVEN table with INT column
- WHEN inserting 1000 rows
- THEN data is stored as contiguous []int64

#### Scenario: NULL handling
- GIVEN table with nullable column
- WHEN inserting NULL values
- THEN null bitmap correctly tracks NULL positions

#### Scenario: Chunk-based organization
- GIVEN table with many rows
- WHEN storing data
- THEN rows are organized into chunks of ~2048 rows

### Requirement: Type Support

The engine SHALL support DuckDB-compatible types.

#### Scenario: Integer types
- GIVEN columns of TINYINT, SMALLINT, INTEGER, BIGINT
- WHEN inserting and selecting values
- THEN values are correctly stored and retrieved

#### Scenario: Floating point types
- GIVEN columns of FLOAT, DOUBLE
- WHEN inserting and selecting values
- THEN values maintain precision

#### Scenario: String types
- GIVEN VARCHAR column
- WHEN inserting Unicode strings
- THEN strings are correctly stored and retrieved

#### Scenario: Boolean type
- GIVEN BOOLEAN column
- WHEN inserting true/false values
- THEN values are correctly stored and retrieved

#### Scenario: NULL handling
- GIVEN nullable column
- WHEN inserting NULL
- THEN NULL is correctly represented and queryable

### Requirement: Concurrent Access

The engine SHALL support concurrent query execution.

#### Scenario: Concurrent reads
- GIVEN table with data
- WHEN 10 goroutines execute SELECT queries simultaneously
- THEN all queries complete successfully
- AND results are correct

#### Scenario: Thread-safe catalog
- GIVEN engine instance
- WHEN multiple goroutines access catalog
- THEN no race conditions occur

### Requirement: Backend Interface Compliance

The Engine SHALL implement the Backend interface from project-foundation.

#### Scenario: Backend interface satisfaction
- GIVEN the Engine type
- WHEN checking `var _ Backend = (*Engine)(nil)`
- THEN compilation succeeds

#### Scenario: BackendConn interface satisfaction
- GIVEN the EngineConn type
- WHEN checking `var _ BackendConn = (*EngineConn)(nil)`
- THEN compilation succeeds

#### Scenario: Open returns usable connection
- GIVEN an Engine instance
- WHEN calling Open(":memory:", nil)
- THEN a BackendConn is returned
- AND conn.Ping(ctx) returns nil

#### Scenario: Close releases resources
- GIVEN an Engine with open connections
- WHEN calling Close()
- THEN all resources are released
- AND no goroutines are leaked

### Requirement: Error Handling

The engine SHALL return structured errors with appropriate types.

#### Scenario: Parser error
- GIVEN invalid SQL syntax
- WHEN executing query
- THEN ErrorTypeParser is returned

#### Scenario: Catalog error
- GIVEN reference to non-existent table
- WHEN executing query
- THEN ErrorTypeCatalog is returned

#### Scenario: Binder error
- GIVEN reference to non-existent column
- WHEN executing query
- THEN ErrorTypeBinder is returned

#### Scenario: Type mismatch error
- GIVEN incompatible types in expression
- WHEN executing query
- THEN ErrorTypeMismatchType is returned

#### Scenario: Division by zero
- GIVEN expression with division by zero
- WHEN executing query
- THEN ErrorTypeDivideByZero is returned
