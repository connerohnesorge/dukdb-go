# Execution Engine Specification

## Requirements

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

The engine SHALL support INSERT, UPDATE, and DELETE with full WHERE clause evaluation, bulk operation optimization, and WAL persistence.

**Changes from base spec**:
- ADDED: WHERE clause evaluation for UPDATE and DELETE
- ADDED: Bulk INSERT optimization via DataChunk batching
- ADDED: INSERT...SELECT support
- ADDED: WAL logging for all DML operations
- ADDED: RowsAffected count for all DML operations

#### Scenario: DELETE with simple WHERE clause
- GIVEN table "t" with rows [(1, 'a'), (2, 'b'), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE id = 2"
- THEN only row with id=2 is deleted
- AND rows with id=1 and id=3 remain
- AND RowsAffected returns 1
- AND WAL contains DELETE entry for row 2

#### Scenario: DELETE with complex WHERE clause
- GIVEN table "t" with rows [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')]
- WHEN executing "DELETE FROM t WHERE id > 1 AND id < 4"
- THEN rows with id=2 and id=3 are deleted
- AND rows with id=1 and id=4 remain
- AND RowsAffected returns 2
- AND WAL contains DELETE entry for rows 2 and 3

#### Scenario: DELETE with no matching rows
- GIVEN table "t" with rows [(1, 'a'), (2, 'b')]
- WHEN executing "DELETE FROM t WHERE id = 999"
- THEN no rows are deleted
- AND RowsAffected returns 0
- AND WAL contains no DELETE entry (optimization: WAL write skipped when len(deletedRowIDs) = 0)

#### Scenario: DELETE with NULL handling (IS NULL)
- GIVEN table "t" with rows [(1, 'a'), (2, NULL), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE name IS NULL"
- THEN only row with id=2 is deleted
- AND RowsAffected returns 1

#### Scenario: DELETE with NULL in comparison (three-valued logic)
- GIVEN table "t" with rows [(1, 'a'), (2, NULL), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE name = 'a'"
- THEN only row with id=1 is deleted
- AND row with id=2 (NULL) is NOT deleted (NULL = 'a' evaluates to NULL, not true)
- AND RowsAffected returns 1

#### Scenario: DELETE with complex WHERE and NULLs
- GIVEN table "t" with rows [(1, 'a'), (2, NULL), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE name IS NULL OR name = 'c'"
- THEN rows with id=2 and id=3 are deleted
- AND row with id=1 remains
- AND RowsAffected returns 2

#### Scenario: DELETE with subquery
- GIVEN table "t" with rows [(1, 'a'), (2, 'b'), (3, 'c')]
- AND table "ids_to_delete" with values [1, 3]
- WHEN executing "DELETE FROM t WHERE id IN (SELECT id FROM ids_to_delete)"
- THEN rows with id=1 and id=3 are deleted
- AND RowsAffected returns 2

#### Scenario: UPDATE with WHERE clause
- GIVEN table "t" with rows [(1, 'a'), (2, 'b'), (3, 'c')]
- WHEN executing "UPDATE t SET name = 'updated' WHERE id = 2"
- THEN row 2 is updated to (2, 'updated')
- AND rows 1 and 3 are unchanged
- AND RowsAffected returns 1
- AND WAL contains UPDATE entry with before=(2, 'b'), after=(2, 'updated')

#### Scenario: UPDATE with expression in SET clause
- GIVEN table "t" with column "count" INT
- AND rows [(1, 10), (2, 20), (3, 30)]
- WHEN executing "UPDATE t SET count = count + 5 WHERE id > 1"
- THEN row 2 becomes (2, 25)
- AND row 3 becomes (3, 35)
- AND row 1 is unchanged at (1, 10)
- AND RowsAffected returns 2

#### Scenario: UPDATE with multi-column SET
- GIVEN table "t" with columns (id, name, count)
- AND row (1, 'old', 10)
- WHEN executing "UPDATE t SET name = 'new', count = 20 WHERE id = 1"
- THEN row becomes (1, 'new', 20)
- AND RowsAffected returns 1
- AND WAL entry includes both column updates

#### Scenario: UPDATE with no matching rows
- GIVEN table "t" with rows [(1, 'a'), (2, 'b')]
- WHEN executing "UPDATE t SET name = 'x' WHERE id = 999"
- THEN no rows are updated
- AND RowsAffected returns 0
- AND WAL contains no UPDATE entry (optimization: WAL write skipped when len(updatedRowIDs) = 0)

#### Scenario: INSERT with 0 rows (empty VALUES)
- GIVEN empty table "t" with columns (id INT, name VARCHAR)
- WHEN executing "INSERT INTO t VALUES" (empty VALUES clause)
- THEN RowsAffected returns 0
- AND WAL contains no INSERT entry (optimization: WAL write skipped when chunk.Size() = 0)
- AND no storage operations are performed

#### Scenario: Bulk INSERT with VALUES
- GIVEN empty table "t" with columns (id INT, name VARCHAR)
- WHEN executing "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"
- THEN all 3 rows are inserted as single batch
- AND RowsAffected returns 3
- AND DataChunk batching is used (not row-by-row)
- AND WAL contains single INSERT entry with 3 rows

#### Scenario: Bulk INSERT with 10,000 rows
- GIVEN empty table "t"
- WHEN executing INSERT with 10,000 rows in VALUES clause
- THEN all rows are inserted
- AND operation completes in <100ms
- AND memory usage stays <20MB during insertion
- AND DataChunks are flushed incrementally (not all buffered)
- AND WAL contains multiple entries (one per chunk of 2048 rows)

#### Scenario: INSERT...SELECT basic
- GIVEN table "source" with rows [(1, 'a'), (2, 'b'), (3, 'c')]
- AND empty table "target" with same schema
- WHEN executing "INSERT INTO target SELECT * FROM source"
- THEN all 3 rows are copied to target
- AND RowsAffected returns 3
- AND source table is unchanged

#### Scenario: INSERT...SELECT with large result set
- GIVEN table "source" with 1,000,000 rows
- AND empty table "target" with same schema
- WHEN executing "INSERT INTO target SELECT * FROM source"
- THEN all rows are copied
- AND memory usage stays <100MB (streaming via DataChunk batching)
- AND operation completes in <30 seconds

#### Scenario: INSERT...SELECT with WHERE clause
- GIVEN table "source" with rows [(1, 'a'), (2, 'b'), (3, 'c')]
- AND empty table "target"
- WHEN executing "INSERT INTO target SELECT * FROM source WHERE id > 1"
- THEN only rows 2 and 3 are copied
- AND RowsAffected returns 2

#### Scenario: INSERT with expression evaluation
- GIVEN empty table "t" with columns (id INT, upper_name VARCHAR)
- WHEN executing "INSERT INTO t VALUES (1, UPPER('hello')), (2, UPPER('world'))"
- THEN rows are inserted with values [(1, 'HELLO'), (2, 'WORLD')]
- AND expressions are evaluated during insertion

#### Scenario: DML operation timeout (deterministic)
- GIVEN table "t" with 1,000,000 rows
- AND context with deadline 5 seconds from now
- AND deterministic clock (quartz.Mock)
- WHEN executing "DELETE FROM t WHERE expensive_function(x) = true"
- AND clock advances 6 seconds
- THEN operation is cancelled
- AND ErrorTypeInterrupt or context.DeadlineExceeded is returned
- AND partial deletions are rolled back (ACID atomicity)

#### Scenario: Transaction rollback of DML
- GIVEN table "t" with rows [(1, 'a'), (2, 'b')]
- WHEN BEGIN TRANSACTION
- AND executing "INSERT INTO t VALUES (3, 'c')"
- AND executing "DELETE FROM t WHERE id = 1"
- AND executing ROLLBACK
- THEN table contains original rows [(1, 'a'), (2, 'b')]
- AND row 3 was not inserted
- AND row 1 was not deleted
- AND WAL contains rollback entry

#### Scenario: Transaction atomicity - partial operations rolled back
- GIVEN table "t" with rows [(1, 'a'), (2, 'b')]
- AND transaction with 5 INSERT operations
- WHEN 3 INSERTs succeed
- AND 4th INSERT fails (e.g., constraint violation)
- AND transaction is rolled back
- THEN table contains only original rows [(1, 'a'), (2, 'b')]
- AND first 3 INSERTs are undone (atomicity)
- AND WAL recovery does not replay uncommitted operations

#### Scenario: Executor initialization with clock injection
- GIVEN storage layer and WAL writer
- WHEN creating new Executor via `NewExecutor(storage, wal, clock)`
- THEN Executor is initialized with:
  - storage reference
  - wal reference
  - clock (quartz.Clock) for deterministic timestamps
  - currentTx set to nil (no active transaction)
- AND clock is injected for testability (quartz.Mock in tests, quartz.NewReal() in production)
- AND currentTx is set by BeginTransaction() when transaction starts

#### Scenario: RowID tracking across operations
- GIVEN empty table "t"
- WHEN executing "INSERT INTO t VALUES (1, 'a'), (2, 'b')"
- THEN rows are assigned RowIDs 0 and 1 (monotonic)
- WHEN executing "DELETE FROM t WHERE id = 1"
- THEN row with RowID 0 is tombstoned
- WHEN executing "INSERT INTO t VALUES (3, 'c')"
- THEN new row is assigned RowID 2 (RowID counter continues)
- AND tombstoned RowID 0 is NOT reused

#### Scenario: WAL recovery after crash during INSERT
- GIVEN database with table "t" containing [(1, 'a')]
- WHEN executing "INSERT INTO t VALUES (2, 'b'), (3, 'c')"
- AND INSERT is logged to WAL
- AND database crashes before checkpoint
- AND database restarts
- THEN WAL recovery replays INSERT
- AND table contains [(1, 'a'), (2, 'b'), (3, 'c')]

#### Scenario: Performance - 100K row INSERT
- GIVEN empty table "t"
- WHEN executing INSERT with 100,000 rows
- THEN operation completes in <1 second
- AND throughput >100,000 rows/second
- AND memory usage <50MB peak

#### Scenario: Performance - UPDATE with selective WHERE
- GIVEN table "t" with 100,000 rows
- WHEN executing "UPDATE t SET x = 1 WHERE id < 1000"
- THEN only 1,000 rows are updated (not full table scan with updates)
- AND operation completes in <50ms
- AND RowsAffected returns 1000

#### Scenario: Performance - DELETE with selective WHERE
- GIVEN table "t" with 100,000 rows
- WHEN executing "DELETE FROM t WHERE id < 1000"
- THEN only 1,000 rows are deleted
- AND operation completes in <50ms
- AND RowsAffected returns 1000

### Requirement: Columnar Storage

**Base Spec** (lines 174-192): Engine SHALL store data in columnar format.

**Enhancement**: Integrate P0-2 DataChunk/Vector for columnar storage.

#### Scenario: Column data stored in DataChunk vectors

```go
// Original spec says: "data is stored as contiguous []int64"
// Enhanced: Data stored in Vector from P0-2
table, _ := storage.GetTable("t")
chunk := table.GetChunk(0)
vec := chunk.GetVector(0) // INT column
assert.IsType(t, &IntVector{}, vec)
```

#### Scenario: NULL handling via ValidityMask

```go
// Original spec says: "null bitmap correctly tracks NULL positions"
// Enhanced: Use ValidityMask from P0-2
vec := chunk.GetVector(0)
assert.False(t, vec.IsValid(5)) // Row 5 is NULL
assert.True(t, vec.IsValid(0))  // Row 0 is valid
```

---

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

### Requirement: DataChunk Operator Interface

All physical operators MUST produce and consume DataChunks from P0-2.

**Context**: Integrates columnar storage into execution pipeline.

#### Scenario: Scan produces DataChunk

```go
scan := PhysicalScan{table: t}
chunk, err := scan.Next()
assert.NoError(t, err)
assert.IsType(t, &DataChunk{}, chunk)
assert.Equal(t, 2048, chunk.Capacity())
```

#### Scenario: Filter consumes/produces DataChunk

```go
filter := PhysicalFilter{child: scan, predicate: expr}
chunk, err := filter.Next()
assert.NoError(t, err)
assert.IsType(t, &DataChunk{}, chunk)
```

#### Scenario: Operators propagate TypeInfo

```go
chunk, _ := scan.Next()
types := chunk.GetTypes()
assert.Len(t, types, 2)
assert.Equal(t, TYPE_INTEGER, types[0].InternalType())
```

---

### Requirement: Result Set Implementation

QueryContext MUST return ResultSet implementing driver.Rows with DataChunk backing.

**Context**: Provides row-by-row access to query results.

#### Scenario: ResultSet wraps DataChunks

```go
chunks := []*DataChunk{chunk1, chunk2}
rs := NewResultSet(chunks)
assert.Len(t, rs.chunks, 2)
```

#### Scenario: ResultSet iterates rows

```go
rs := NewResultSet(chunks)
count := 0
dest := make([]driver.Value, 2)
for rs.Next(dest) == nil {
    count++
}
assert.Equal(t, 2048, count) // Full chunk
```

#### Scenario: ColumnTypeDatabaseTypeName for P0-3

```go
rs := NewResultSet(chunks)
typeName := rs.ColumnTypeDatabaseTypeName(0)
assert.Equal(t, "INTEGER", typeName)
```

---

### Requirement: EngineConn Pipeline Integration

EngineConn MUST route ExecContext and QueryContext through the execution pipeline.

**Context**: Wires database/sql driver to execution engine.

#### Scenario: ExecContext routes through pipeline

```go
conn, _ := engine.Open(":memory:", nil)
result, err := conn.ExecContext(ctx, "INSERT INTO t VALUES (1)", nil)
assert.NoError(t, err)
affected, _ := result.RowsAffected()
assert.Equal(t, int64(1), affected)
```

#### Scenario: QueryContext routes through pipeline

```go
conn, _ := engine.Open(":memory:", nil)
rows, err := conn.QueryContext(ctx, "SELECT * FROM t", nil)
assert.NoError(t, err)
assert.NotNil(t, rows)
```

#### Scenario: Error types propagated correctly

```go
conn, _ := engine.Open(":memory:", nil)
_, err := conn.QueryContext(ctx, "SELECT * FROM nonexistent", nil)
assert.Error(t, err)
assert.Equal(t, ErrorTypeCatalog, err.(*Error).Type)
```

---

### Requirement: Column Metadata for P0-3

PreparedStmt MUST expose column metadata via TypeInfo after binding.

**Context**: Enables P0-3 statement introspection completion.

#### Scenario: Prepared statement has column metadata

```go
stmt, _ := conn.Prepare("SELECT id, name FROM users")
// After binding in EngineConn.Prepare:
plan := stmt.(*PreparedPlan)
assert.Len(t, plan.ColumnTypes, 2)
assert.Equal(t, TYPE_INTEGER, plan.ColumnTypes[0].InternalType())
assert.Equal(t, TYPE_VARCHAR, plan.ColumnTypes[1].InternalType())
```

#### Scenario: ColumnTypeInfo returns full TypeInfo

```go
typeInfo := plan.ColumnTypeInfo(0)
assert.Equal(t, TYPE_INTEGER, typeInfo.InternalType())
assert.Equal(t, "INTEGER", typeInfo.SQLType())
```

---

### Requirement: DataChunk Batching for DML

The engine SHALL batch DML operations using DataChunk columnar format for optimal performance.

#### Scenario: INSERT VALUES batched into DataChunks
- GIVEN INSERT statement with 5000 rows in VALUES clause
- WHEN executing the INSERT
- THEN rows are batched into DataChunks of 2048 rows each
- AND 3 chunks are created: [2048 rows, 2048 rows, 904 rows]
- AND each chunk is inserted to storage as atomic unit
- AND WAL logs one entry per chunk (3 total)

#### Scenario: DataChunk memory usage bounded
- GIVEN INSERT statement with 1,000,000 rows
- WHEN executing the INSERT
- THEN memory usage never exceeds 100MB
- AND chunks are flushed incrementally (not all buffered)
- AND garbage collector can reclaim flushed chunks

#### Scenario: UPDATE batching with WHERE filter
- GIVEN table with 10,000 rows
- WHEN executing "UPDATE t SET x = 1 WHERE id % 2 = 0"
- THEN matching rows (5,000) are collected into DataChunk
- AND update is applied as batch to storage
- AND WAL logs single UPDATE entry with 5,000 rows (or multiple chunk-sized entries)

### Requirement: Expression Evaluation in DML Context

The engine SHALL evaluate expressions in INSERT VALUES, UPDATE SET, and WHERE clauses.

#### Scenario: Expression in INSERT VALUES
- GIVEN empty table "t" with columns (id INT, computed INT)
- WHEN executing "INSERT INTO t VALUES (1, 10 + 20), (2, 5 * 6)"
- THEN expressions are evaluated to (1, 30), (2, 30)

#### Scenario: Expression in UPDATE SET clause
- GIVEN table "t" with row (1, 10)
- WHEN executing "UPDATE t SET count = count * 2 + 5"
- THEN count is updated to 25 (10 * 2 + 5)

#### Scenario: Expression in WHERE clause
- GIVEN table "t" with rows [(1, 'HELLO'), (2, 'WORLD')]
- WHEN executing "DELETE FROM t WHERE LOWER(name) = 'hello'"
- THEN row 1 is deleted (expression evaluated per row)

### Requirement: NULL Handling in DML Operations

The engine SHALL correctly handle NULL values in all DML operations per SQL three-valued logic.

#### Scenario: INSERT with NULL values
- GIVEN table "t" with nullable column "name"
- WHEN executing "INSERT INTO t VALUES (1, NULL)"
- THEN row is inserted with NULL in name column
- AND validity mask correctly marks NULL

#### Scenario: UPDATE to NULL
- GIVEN table "t" with row (1, 'value')
- WHEN executing "UPDATE t SET name = NULL WHERE id = 1"
- THEN name is updated to NULL
- AND validity mask is updated

#### Scenario: WHERE clause with NULL
- GIVEN table "t" with rows [(1, 'a'), (2, NULL), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE name = 'a'"
- THEN only row 1 is deleted (row 2 with NULL does not match)

#### Scenario: WHERE IS NULL
- GIVEN table "t" with rows [(1, 'a'), (2, NULL), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE name IS NULL"
- THEN only row 2 is deleted

### Requirement: Error Handling in DML Operations

The engine SHALL return appropriate errors for invalid DML operations.

#### Scenario: INSERT into non-existent table
- GIVEN no table "nonexistent"
- WHEN executing "INSERT INTO nonexistent VALUES (1)"
- THEN ErrorTypeCatalog is returned
- AND no WAL entry is created

#### Scenario: UPDATE with non-existent column
- GIVEN table "t" with column "x"
- WHEN executing "UPDATE t SET nonexistent = 1"
- THEN ErrorTypeBinder is returned

#### Scenario: DELETE with invalid WHERE expression
- GIVEN table "t" with INT column "x"
- WHEN executing "DELETE FROM t WHERE x = 'string'"
- THEN ErrorTypeMismatchType is returned

#### Scenario: INSERT with column count mismatch
- GIVEN table "t" with columns (id, name)
- WHEN executing "INSERT INTO t VALUES (1)"
- THEN error is returned (column count mismatch)
- AND no rows are inserted

#### Scenario: UPDATE with type mismatch
- GIVEN table "t" with INT column "x"
- WHEN executing "UPDATE t SET x = 'string'"
- THEN ErrorTypeMismatchType is returned
- AND no rows are updated

#### Scenario: Error type compatibility - catalog errors
- GIVEN no table "nonexistent"
- WHEN executing "INSERT INTO nonexistent VALUES (1)"
- THEN ErrorTypeCatalog is returned (matching duckdb-go behavior)
- WHEN executing "UPDATE nonexistent SET x = 1"
- THEN ErrorTypeCatalog is returned (matching duckdb-go behavior)
- WHEN executing "DELETE FROM nonexistent"
- THEN ErrorTypeCatalog is returned (matching duckdb-go behavior)

#### Scenario: Error type compatibility - binder errors
- GIVEN table "t" with columns (id, name)
- WHEN executing "UPDATE t SET nonexistent_column = 1"
- THEN ErrorTypeBinder is returned (matching duckdb-go behavior)
- WHEN executing "DELETE FROM t WHERE nonexistent_column = 1"
- THEN ErrorTypeBinder is returned (matching duckdb-go behavior)

#### Scenario: Error type compatibility - interrupt errors
- GIVEN long-running DML operation
- WHEN context is cancelled
- THEN ErrorTypeInterrupt is returned (matching duckdb-go behavior)
- AND operation is aborted
- AND partial changes are rolled back

### Requirement: Date Extraction Functions

The engine SHALL evaluate date extraction functions on DATE and TIMESTAMP types.

#### Scenario: YEAR extracts year from date
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT YEAR(date_col) FROM t"
- THEN the result is 2024

#### Scenario: YEAR extracts year from timestamp
- GIVEN a table with TIMESTAMP column containing '2024-03-15 14:30:00'
- WHEN executing "SELECT YEAR(ts_col) FROM t"
- THEN the result is 2024

#### Scenario: MONTH extracts month from date
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT MONTH(date_col) FROM t"
- THEN the result is 3

#### Scenario: DAY extracts day from date
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT DAY(date_col) FROM t"
- THEN the result is 15

#### Scenario: HOUR extracts hour from timestamp
- GIVEN a table with TIMESTAMP column containing '2024-03-15 14:30:45'
- WHEN executing "SELECT HOUR(ts_col) FROM t"
- THEN the result is 14

#### Scenario: MINUTE extracts minute from timestamp
- GIVEN a table with TIMESTAMP column containing '2024-03-15 14:30:45'
- WHEN executing "SELECT MINUTE(ts_col) FROM t"
- THEN the result is 30

#### Scenario: SECOND extracts second with fraction
- GIVEN a table with TIMESTAMP column containing '2024-03-15 14:30:45.123456'
- WHEN executing "SELECT SECOND(ts_col) FROM t"
- THEN the result is 45.123456 (DOUBLE)

#### Scenario: DAYOFWEEK returns correct day
- GIVEN a table with DATE column containing '2024-03-15' (Friday)
- WHEN executing "SELECT DAYOFWEEK(date_col) FROM t"
- THEN the result is 5 (Friday, 0=Sunday)

#### Scenario: DAYOFYEAR returns correct day
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT DAYOFYEAR(date_col) FROM t"
- THEN the result is 75 (leap year)

#### Scenario: WEEK returns ISO week number
- GIVEN a table with DATE column containing '2024-01-01'
- WHEN executing "SELECT WEEK(date_col) FROM t"
- THEN the result is 1 (ISO week)

#### Scenario: QUARTER returns quarter
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT QUARTER(date_col) FROM t"
- THEN the result is 1 (Q1: Jan-Mar)

#### Scenario: Extraction from NULL returns NULL
- GIVEN a table with NULL DATE column
- WHEN executing "SELECT YEAR(date_col) FROM t"
- THEN the result is NULL

### Requirement: Date Arithmetic Functions

The engine SHALL evaluate date arithmetic functions.

#### Scenario: DATE_ADD adds interval to date
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT DATE_ADD(date_col, INTERVAL '5' DAY) FROM t"
- THEN the result is DATE '2024-03-20'

#### Scenario: DATE_ADD adds months correctly
- GIVEN a table with DATE column containing '2024-01-31'
- WHEN executing "SELECT DATE_ADD(date_col, INTERVAL '1' MONTH) FROM t"
- THEN the result is DATE '2024-02-29' (leap year, clamps to month end)

#### Scenario: DATE_SUB subtracts interval from date
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT DATE_SUB(date_col, INTERVAL '10' DAY) FROM t"
- THEN the result is DATE '2024-03-05'

#### Scenario: DATE_DIFF calculates difference in days
- GIVEN two dates '2024-03-20' and '2024-03-15'
- WHEN executing "SELECT DATE_DIFF('day', DATE '2024-03-15', DATE '2024-03-20')"
- THEN the result is 5

#### Scenario: DATE_DIFF calculates difference in months
- GIVEN two dates '2024-06-15' and '2024-03-15'
- WHEN executing "SELECT DATE_DIFF('month', DATE '2024-03-15', DATE '2024-06-15')"
- THEN the result is 3

#### Scenario: DATE_DIFF with negative difference
- GIVEN two dates where end is before start
- WHEN executing "SELECT DATE_DIFF('day', DATE '2024-03-20', DATE '2024-03-15')"
- THEN the result is -5

#### Scenario: DATE_TRUNC truncates to day
- GIVEN a TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT DATE_TRUNC('day', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is TIMESTAMP '2024-03-15 00:00:00'

#### Scenario: DATE_TRUNC truncates to month
- GIVEN a TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT DATE_TRUNC('month', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is TIMESTAMP '2024-03-01 00:00:00'

#### Scenario: DATE_TRUNC truncates to hour
- GIVEN a TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT DATE_TRUNC('hour', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is TIMESTAMP '2024-03-15 14:00:00'

#### Scenario: DATE_PART extracts part as double
- GIVEN a TIMESTAMP '2024-03-15 14:30:45.5'
- WHEN executing "SELECT DATE_PART('second', TIMESTAMP '2024-03-15 14:30:45.5')"
- THEN the result is 45.5 (DOUBLE)

#### Scenario: AGE calculates interval between timestamps
- GIVEN two timestamps
- WHEN executing "SELECT AGE(TIMESTAMP '2024-03-15', TIMESTAMP '2024-01-15')"
- THEN the result is INTERVAL '2 months'

#### Scenario: LAST_DAY returns last day of month
- GIVEN a DATE '2024-02-15'
- WHEN executing "SELECT LAST_DAY(DATE '2024-02-15')"
- THEN the result is DATE '2024-02-29' (leap year)

#### Scenario: Date arithmetic with NULL returns NULL
- GIVEN a NULL date
- WHEN executing "SELECT DATE_ADD(NULL, INTERVAL '1' DAY)"
- THEN the result is NULL

### Requirement: Date Construction Functions

The engine SHALL construct date/time values from components.

#### Scenario: MAKE_DATE constructs date
- GIVEN year=2024, month=3, day=15
- WHEN executing "SELECT MAKE_DATE(2024, 3, 15)"
- THEN the result is DATE '2024-03-15'

#### Scenario: MAKE_DATE with invalid components
- GIVEN month=13
- WHEN executing "SELECT MAKE_DATE(2024, 13, 15)"
- THEN ErrorTypeExecutor is returned with message about invalid month

#### Scenario: MAKE_DATE with invalid day
- GIVEN February 30
- WHEN executing "SELECT MAKE_DATE(2024, 2, 30)"
- THEN ErrorTypeExecutor is returned with message about invalid day

#### Scenario: MAKE_TIMESTAMP constructs timestamp
- GIVEN full components
- WHEN executing "SELECT MAKE_TIMESTAMP(2024, 3, 15, 14, 30, 45.5)"
- THEN the result is TIMESTAMP '2024-03-15 14:30:45.5'

#### Scenario: MAKE_TIME constructs time
- GIVEN hour=14, minute=30, second=45
- WHEN executing "SELECT MAKE_TIME(14, 30, 45)"
- THEN the result is TIME '14:30:45'

#### Scenario: Construction with NULL returns NULL
- GIVEN NULL for any component
- WHEN executing "SELECT MAKE_DATE(2024, NULL, 15)"
- THEN the result is NULL

### Requirement: Date Formatting Functions

The engine SHALL format dates to strings and parse strings to dates.

#### Scenario: STRFTIME formats with year
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT STRFTIME('%Y-%m-%d', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is VARCHAR '2024-03-15'

#### Scenario: STRFTIME formats with time components
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT STRFTIME('%H:%M:%S', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is VARCHAR '14:30:45'

#### Scenario: STRFTIME with full format
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT STRFTIME('%Y-%m-%d %H:%M:%S', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is VARCHAR '2024-03-15 14:30:45'

#### Scenario: STRFTIME with day name
- GIVEN TIMESTAMP '2024-03-15 14:30:45' (Friday)
- WHEN executing "SELECT STRFTIME('%A', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is VARCHAR 'Friday'

#### Scenario: STRFTIME with month name
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT STRFTIME('%B', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is VARCHAR 'March'

#### Scenario: STRPTIME parses date string
- GIVEN string '2024-03-15'
- WHEN executing "SELECT STRPTIME('2024-03-15', '%Y-%m-%d')"
- THEN the result is TIMESTAMP '2024-03-15 00:00:00'

#### Scenario: STRPTIME parses datetime string
- GIVEN string '2024-03-15 14:30:45'
- WHEN executing "SELECT STRPTIME('2024-03-15 14:30:45', '%Y-%m-%d %H:%M:%S')"
- THEN the result is TIMESTAMP '2024-03-15 14:30:45'

#### Scenario: STRPTIME with unparseable string
- GIVEN invalid date string
- WHEN executing "SELECT STRPTIME('not-a-date', '%Y-%m-%d')"
- THEN the result is NULL

#### Scenario: Formatting NULL returns NULL
- GIVEN NULL timestamp
- WHEN executing "SELECT STRFTIME('%Y', NULL)"
- THEN the result is NULL

### Requirement: Epoch Conversion Functions

The engine SHALL convert between timestamps and Unix epoch values.

#### Scenario: TO_TIMESTAMP converts epoch seconds
- GIVEN epoch 1710510645 (2024-03-15 14:30:45 UTC)
- WHEN executing "SELECT TO_TIMESTAMP(1710510645)"
- THEN the result is TIMESTAMP '2024-03-15 14:30:45'

#### Scenario: EPOCH extracts seconds from timestamp
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT EPOCH(TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is approximately 1710510645.0 (DOUBLE)

#### Scenario: EPOCH_MS extracts milliseconds
- GIVEN TIMESTAMP '2024-03-15 14:30:45.123'
- WHEN executing "SELECT EPOCH_MS(TIMESTAMP '2024-03-15 14:30:45.123')"
- THEN the result is 1710510645123 (BIGINT)

#### Scenario: Epoch of NULL returns NULL
- GIVEN NULL timestamp
- WHEN executing "SELECT EPOCH(NULL)"
- THEN the result is NULL

### Requirement: EXTRACT Syntax

The engine SHALL support SQL standard EXTRACT syntax.

#### Scenario: EXTRACT year from timestamp
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is 2024.0 (DOUBLE per SQL standard)

#### Scenario: EXTRACT month from date
- GIVEN DATE '2024-03-15'
- WHEN executing "SELECT EXTRACT(MONTH FROM DATE '2024-03-15')"
- THEN the result is 3.0 (DOUBLE)

#### Scenario: EXTRACT epoch from timestamp
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT EXTRACT(EPOCH FROM TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is approximately 1710510645.0 (DOUBLE)

### Requirement: Interval Parsing

The engine SHALL parse INTERVAL literals.

#### Scenario: Parse single-unit interval
- GIVEN the SQL "SELECT INTERVAL '5' DAY"
- WHEN executing the statement
- THEN the result is an INTERVAL value of 5 days

#### Scenario: Parse hour interval
- GIVEN the SQL "SELECT INTERVAL '3' HOUR"
- WHEN executing the statement
- THEN the result is an INTERVAL value of 3 hours

#### Scenario: Parse string interval
- GIVEN the SQL "SELECT INTERVAL '1 day'"
- WHEN executing the statement
- THEN the result is an INTERVAL value of 1 day

#### Scenario: Parse compound interval
- GIVEN the SQL "SELECT INTERVAL '2 hours 30 minutes'"
- WHEN executing the statement
- THEN the result is an INTERVAL value of 2 hours 30 minutes

### Requirement: Interval Arithmetic

The engine SHALL perform arithmetic with INTERVAL values.

#### Scenario: Add interval to date
- GIVEN DATE '2024-03-15' and INTERVAL '5 days'
- WHEN executing "SELECT DATE '2024-03-15' + INTERVAL '5' DAY"
- THEN the result is DATE '2024-03-20'

#### Scenario: Subtract interval from timestamp
- GIVEN TIMESTAMP '2024-03-15 14:30:00' and INTERVAL '2 hours'
- WHEN executing "SELECT TIMESTAMP '2024-03-15 14:30:00' - INTERVAL '2' HOUR"
- THEN the result is TIMESTAMP '2024-03-15 12:30:00'

#### Scenario: Multiply interval
- GIVEN INTERVAL '1' DAY and multiplier 5
- WHEN executing "SELECT INTERVAL '1' DAY * 5"
- THEN the result is INTERVAL '5 days'

### Requirement: Interval Extraction Functions

The engine SHALL extract components from INTERVAL values.

#### Scenario: TO_YEARS extracts years
- GIVEN INTERVAL '2 years 3 months'
- WHEN executing "SELECT TO_YEARS(INTERVAL '2 years 3 months')"
- THEN the result is 2

#### Scenario: TO_MONTHS extracts total months
- GIVEN INTERVAL '2 years 3 months'
- WHEN executing "SELECT TO_MONTHS(INTERVAL '2 years 3 months')"
- THEN the result is 27 (2*12 + 3)

#### Scenario: TO_DAYS extracts days
- GIVEN INTERVAL '5 days 12 hours'
- WHEN executing "SELECT TO_DAYS(INTERVAL '5 days 12 hours')"
- THEN the result is 5

#### Scenario: TO_HOURS extracts total hours
- GIVEN INTERVAL '2 days 5 hours'
- WHEN executing "SELECT TO_HOURS(INTERVAL '2 days 5 hours')"
- THEN the result is 53 (2*24 + 5)

### Requirement: Date Function Error Handling

The engine SHALL return appropriate errors for invalid inputs.

#### Scenario: Invalid date part specifier
- GIVEN an unknown part specifier
- WHEN executing "SELECT DATE_PART('invalid', TIMESTAMP '2024-03-15')"
- THEN ErrorTypeBinder is returned with message about invalid date part

#### Scenario: YEAR with wrong type
- GIVEN a VARCHAR input
- WHEN executing "SELECT YEAR('not-a-date')"
- THEN ErrorTypeBinder is returned with message about type mismatch

#### Scenario: DATE_ADD with wrong interval type
- GIVEN an integer instead of interval
- WHEN executing "SELECT DATE_ADD(DATE '2024-03-15', 5)"
- THEN ErrorTypeBinder is returned with message about type mismatch

### Requirement: PhysicalWindow Operator

The executor SHALL implement a PhysicalWindow operator for window function evaluation.

#### Scenario: PhysicalWindow interface compliance
- GIVEN the PhysicalWindow type
- WHEN checking interface compliance
- THEN PhysicalWindow implements PhysicalOperator interface
- AND Next() returns *DataChunk
- AND GetTypes() returns child types plus window result types

#### Scenario: Window operator in execution switch
- GIVEN the Executor.Execute method
- WHEN plan is *PhysicalWindow
- THEN executeWindow is called
- AND result contains window function output columns

#### Scenario: Window operator preserves child columns
- GIVEN table "t" with columns (id, name, salary)
- WHEN executing "SELECT *, ROW_NUMBER() OVER (ORDER BY id) FROM t"
- THEN result contains columns (id, name, salary, row_number)
- AND all child values are preserved

### Requirement: ROW_NUMBER Function

The executor SHALL implement ROW_NUMBER() window function.

#### Scenario: ROW_NUMBER without partition
- GIVEN table "t" with rows [(1), (2), (3)]
- WHEN executing "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 1), (2, 2), (3, 3)]

#### Scenario: ROW_NUMBER with partition
- GIVEN table "t" with rows [('A', 1), ('A', 2), ('B', 1), ('B', 2)]
- WHEN executing "SELECT cat, val, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val) FROM t"
- THEN result equals [('A', 1, 1), ('A', 2, 2), ('B', 1, 1), ('B', 2, 2)]

#### Scenario: ROW_NUMBER type is BIGINT
- GIVEN any window query with ROW_NUMBER()
- WHEN executing the query
- THEN ROW_NUMBER column type is BIGINT

### Requirement: RANK Function

The executor SHALL implement RANK() window function with gap handling.

#### Scenario: RANK with ties
- GIVEN table "t" with rows [(10), (10), (20), (30)]
- WHEN executing "SELECT val, RANK() OVER (ORDER BY val) FROM t"
- THEN result equals [(10, 1), (10, 1), (20, 3), (30, 4)]
- AND rank 2 is skipped due to tie

#### Scenario: RANK with partition
- GIVEN table "t" with rows [('A', 10), ('A', 10), ('A', 20), ('B', 5), ('B', 5)]
- WHEN executing "SELECT cat, val, RANK() OVER (PARTITION BY cat ORDER BY val) FROM t"
- THEN result equals [('A', 10, 1), ('A', 10, 1), ('A', 20, 3), ('B', 5, 1), ('B', 5, 1)]

#### Scenario: RANK requires ORDER BY
- GIVEN query without ORDER BY in window
- WHEN executing "SELECT RANK() OVER (PARTITION BY x) FROM t"
- THEN all rows have rank 1 (no ordering means all are peers)

### Requirement: DENSE_RANK Function

The executor SHALL implement DENSE_RANK() window function without gaps.

#### Scenario: DENSE_RANK with ties
- GIVEN table "t" with rows [(10), (10), (20), (30)]
- WHEN executing "SELECT val, DENSE_RANK() OVER (ORDER BY val) FROM t"
- THEN result equals [(10, 1), (10, 1), (20, 2), (30, 3)]
- AND no rank values are skipped

#### Scenario: DENSE_RANK vs RANK comparison
- GIVEN table with values [1, 1, 2, 3, 3, 4]
- WHEN executing both RANK() and DENSE_RANK()
- THEN RANK returns [1, 1, 3, 4, 4, 6]
- AND DENSE_RANK returns [1, 1, 2, 3, 3, 4]

### Requirement: NTILE Function

The executor SHALL implement NTILE(n) window function for bucket distribution.

#### Scenario: NTILE with even distribution
- GIVEN table "t" with 8 rows ordered by id
- WHEN executing "SELECT id, NTILE(4) OVER (ORDER BY id) FROM t"
- THEN result has 2 rows per bucket: [(1,1), (2,1), (3,2), (4,2), (5,3), (6,3), (7,4), (8,4)]

#### Scenario: NTILE with uneven distribution
- GIVEN table "t" with 10 rows ordered by id
- WHEN executing "SELECT id, NTILE(4) OVER (ORDER BY id) FROM t"
- THEN buckets have sizes [3, 3, 2, 2] (extra rows go to earlier buckets)

#### Scenario: NTILE with more buckets than rows
- GIVEN table "t" with 3 rows
- WHEN executing "SELECT id, NTILE(10) OVER (ORDER BY id) FROM t"
- THEN each row gets a unique bucket [1, 2, 3]

#### Scenario: NTILE argument validation
- GIVEN NTILE with zero or negative argument
- WHEN executing "SELECT NTILE(0) OVER () FROM t"
- THEN ErrorTypeExecutor is returned
- AND error message indicates invalid bucket count

### Requirement: LAG Function

The executor SHALL implement LAG(expr, offset, default) window function.

#### Scenario: LAG with default offset
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, LAG(id) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, NULL), (2, 1), (3, 2)]

#### Scenario: LAG with custom offset
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, LAG(id, 2) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, NULL), (2, NULL), (3, 1), (4, 2)]

#### Scenario: LAG with default value
- GIVEN table "t" with rows [(1), (2), (3)]
- WHEN executing "SELECT id, LAG(id, 1, 0) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 0), (2, 1), (3, 2)]
- AND default value 0 replaces NULL for first row

#### Scenario: LAG respects partition boundaries
- GIVEN table with rows [('A', 1), ('A', 2), ('B', 1), ('B', 2)]
- WHEN executing "SELECT cat, val, LAG(val) OVER (PARTITION BY cat ORDER BY val) FROM t"
- THEN result equals [('A', 1, NULL), ('A', 2, 1), ('B', 1, NULL), ('B', 2, 1)]
- AND LAG does not cross partition boundary

### Requirement: LEAD Function

The executor SHALL implement LEAD(expr, offset, default) window function.

#### Scenario: LEAD with default offset
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, LEAD(id) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 2), (2, 3), (3, NULL)]

#### Scenario: LEAD with custom offset
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, LEAD(id, 2) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 3), (2, 4), (3, NULL), (4, NULL)]

#### Scenario: LEAD with default value
- GIVEN table "t" with rows [(1), (2), (3)]
- WHEN executing "SELECT id, LEAD(id, 1, 99) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 2), (2, 3), (3, 99)]

### Requirement: FIRST_VALUE Function

The executor SHALL implement FIRST_VALUE(expr) window function.

#### Scenario: FIRST_VALUE with default frame
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, FIRST_VALUE(id) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 1), (2, 1), (3, 1)]
- AND first value of partition is returned for all rows

#### Scenario: FIRST_VALUE with sliding frame
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, FIRST_VALUE(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- THEN result equals [(1, 1), (2, 1), (3, 2), (4, 3)]

#### Scenario: FIRST_VALUE ignores NULLs
- GIVEN table with rows [(NULL), (1), (2)]
- WHEN executing "SELECT FIRST_VALUE(val) OVER (ORDER BY id) FROM t"
- THEN first value is NULL (NULLs are included in FIRST_VALUE)

### Requirement: LAST_VALUE Function

The executor SHALL implement LAST_VALUE(expr) window function.

#### Scenario: LAST_VALUE with default frame
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, LAST_VALUE(id) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 1), (2, 2), (3, 3)]
- AND default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

#### Scenario: LAST_VALUE with full frame
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, LAST_VALUE(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t"
- THEN result equals [(1, 3), (2, 3), (3, 3)]

### Requirement: NTH_VALUE Function

The executor SHALL implement NTH_VALUE(expr, n) window function.

#### Scenario: NTH_VALUE basic
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, NTH_VALUE(id, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t"
- THEN result equals [(1, 2), (2, 2), (3, 2), (4, 2)]

#### Scenario: NTH_VALUE out of bounds
- GIVEN table "t" with 3 rows
- WHEN executing "SELECT id, NTH_VALUE(id, 10) OVER () FROM t"
- THEN result has NULL for NTH_VALUE column (10 > row count)

#### Scenario: NTH_VALUE with frame
- GIVEN table with rows [(1), (2), (3), (4), (5)] ordered by id
- WHEN executing "SELECT id, NTH_VALUE(id, 2) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- THEN result considers only frame for each row

### Requirement: PERCENT_RANK Function

The executor SHALL implement PERCENT_RANK() window function.

#### Scenario: PERCENT_RANK basic
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, PERCENT_RANK() OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 0.0), (2, 0.333...), (3, 0.666...), (4, 1.0)]
- AND formula is (rank - 1) / (partition_size - 1)

#### Scenario: PERCENT_RANK single row
- GIVEN table "t" with 1 row
- WHEN executing "SELECT PERCENT_RANK() OVER () FROM t"
- THEN result equals 0.0 (edge case: partition_size = 1)

#### Scenario: PERCENT_RANK with ties
- GIVEN table with values [10, 10, 20]
- WHEN executing "SELECT val, PERCENT_RANK() OVER (ORDER BY val) FROM t"
- THEN tied values have same percent_rank

### Requirement: CUME_DIST Function

The executor SHALL implement CUME_DIST() window function.

#### Scenario: CUME_DIST basic
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, CUME_DIST() OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 0.25), (2, 0.5), (3, 0.75), (4, 1.0)]
- AND formula is (rows_at_or_before) / partition_size

#### Scenario: CUME_DIST with ties
- GIVEN table with values [10, 10, 20]
- WHEN executing "SELECT val, CUME_DIST() OVER (ORDER BY val) FROM t"
- THEN tied values have same cume_dist [(10, 0.666...), (10, 0.666...), (20, 1.0)]

### Requirement: Aggregate Window Functions

The executor SHALL support aggregate functions with OVER clause.

#### Scenario: SUM with OVER
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, SUM(id) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 1), (2, 3), (3, 6)] (running sum with default frame)

#### Scenario: COUNT with OVER
- GIVEN table "t" with rows [(1), (2), (3)]
- WHEN executing "SELECT id, COUNT(*) OVER () FROM t"
- THEN result equals [(1, 3), (2, 3), (3, 3)]

#### Scenario: AVG with sliding frame
- GIVEN table "t" with rows [(1), (2), (3), (4), (5)] ordered by id
- WHEN executing "SELECT id, AVG(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- THEN result equals [(1, 1.5), (2, 2.0), (3, 3.0), (4, 4.0), (5, 4.5)]

#### Scenario: MIN/MAX with OVER
- GIVEN table "t" with rows [(3), (1), (4), (1), (5)]
- WHEN executing "SELECT val, MIN(val) OVER (), MAX(val) OVER () FROM t"
- THEN all rows have MIN=1 and MAX=5

### Requirement: Frame Boundary Evaluation

The executor SHALL correctly evaluate frame boundaries.

#### Scenario: ROWS BETWEEN n PRECEDING AND n FOLLOWING
- GIVEN table "t" with rows [(1), (2), (3), (4), (5)] ordered by id
- AND window "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"
- WHEN executing SUM(id) OVER (...)
- THEN result equals [(1, 3), (2, 6), (3, 9), (4, 12), (5, 9)]

#### Scenario: RANGE BETWEEN with numeric ORDER BY
- GIVEN table "t" with rows [(1), (3), (4), (6), (10)] ordered by val
- AND window "RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING"
- WHEN executing SUM(val) OVER (...)
- THEN each row sums values within val-2 to val+2

#### Scenario: ROWS CURRENT ROW
- GIVEN table "t" with rows
- WHEN executing "SELECT id, SUM(id) OVER (ROWS CURRENT ROW) FROM t"
- THEN each row's sum equals its own value

#### Scenario: UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING
- GIVEN any table "t"
- WHEN executing "SELECT SUM(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t"
- THEN all rows have same sum (total of partition)

### Requirement: NULL Handling in Windows

The executor SHALL handle NULL values according to DuckDB semantics.

#### Scenario: NULL in PARTITION BY
- GIVEN table with rows [('A', 1), (NULL, 2), (NULL, 3), ('B', 4)]
- WHEN executing "SELECT cat, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val) FROM t"
- THEN NULL category forms its own partition with row numbers 1, 2
- AND 'A' partition has row number 1
- AND 'B' partition has row number 1

#### Scenario: NULL in ORDER BY
- GIVEN table with values [(1), (NULL), (3), (2)]
- WHEN executing "SELECT val, ROW_NUMBER() OVER (ORDER BY val) FROM t"
- THEN NULLs are sorted last by default
- AND order is [1, 2, 3, NULL] with row numbers [1, 2, 3, 4]

#### Scenario: NULL in aggregate window
- GIVEN table with values [(1), (NULL), (3)]
- WHEN executing "SELECT val, SUM(val) OVER () FROM t"
- THEN NULL is excluded from sum, result is 4 for all rows

#### Scenario: NULL in LAG/LEAD
- GIVEN table with values [(1), (NULL), (3)]
- WHEN executing "SELECT val, LAG(val) OVER (ORDER BY rowid) FROM t"
- THEN result equals [(1, NULL), (NULL, 1), (3, NULL)]
- AND NULL values are passed through correctly

### Requirement: Window Ordering Preservation

The executor SHALL return rows in a deterministic order.

#### Scenario: Original row order preserved
- GIVEN table "t" with rows in specific insert order
- WHEN executing window query without ORDER BY in outer query
- THEN rows are returned in original order (by RowID)
- AND window results are correctly attached to each row

#### Scenario: Multiple windows with different ordering
- GIVEN query "SELECT ROW_NUMBER() OVER (ORDER BY a), ROW_NUMBER() OVER (ORDER BY b) FROM t"
- WHEN executing the query
- THEN each window is evaluated with its own ordering
- AND results are correctly combined in output

### Requirement: Performance Bounds

Window execution SHALL meet performance requirements.

#### Scenario: Large partition performance
- GIVEN table with 100,000 rows in single partition
- WHEN executing "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t"
- THEN execution completes in < 1 second
- AND memory usage < 100MB

#### Scenario: Many partitions performance
- GIVEN table with 100,000 rows in 10,000 partitions
- WHEN executing "SELECT ROW_NUMBER() OVER (PARTITION BY cat ORDER BY id) FROM t"
- THEN execution completes in < 2 seconds

#### Scenario: Sliding window performance
- GIVEN table with 10,000 rows
- WHEN executing "SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) FROM t"
- THEN execution completes in < 1 second
- AND time complexity is O(n) to O(n log n), not O(n²)

### Requirement: GROUPS Frame Type

The executor SHALL implement GROUPS frame type for peer group-based frames.

#### Scenario: GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
- GIVEN table with values [(10), (10), (20), (30), (30), (40)] ordered by val
- AND peer groups are {10, 10}, {20}, {30, 30}, {40}
- WHEN executing "SELECT val, SUM(val) OVER (ORDER BY val GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- THEN row with val=10 sums groups {10,10} and {20} = 40
- AND row with val=20 sums groups {10,10}, {20}, {30,30} = 90
- AND row with val=30 sums groups {20}, {30,30}, {40} = 110
- AND row with val=40 sums groups {30,30} and {40} = 100

#### Scenario: GROUPS UNBOUNDED PRECEDING
- GIVEN table with values [(10), (10), (20), (30)]
- WHEN executing "SELECT val, COUNT(*) OVER (ORDER BY val GROUPS UNBOUNDED PRECEDING) FROM t"
- THEN rows with val=10 have count 2 (just group {10,10})
- AND row with val=20 has count 3 (groups {10,10}, {20})
- AND row with val=30 has count 4 (all groups)

#### Scenario: GROUPS with no ORDER BY
- GIVEN window with GROUPS frame but no ORDER BY
- WHEN executing the query
- THEN all rows are in single peer group
- AND GROUPS frame behaves like full partition

### Requirement: EXCLUDE Clause

The executor SHALL implement EXCLUDE clause for frame specifications.

#### Scenario: EXCLUDE CURRENT ROW
- GIVEN table with values [(1), (2), (3)] ordered by val
- WHEN executing "SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t"
- THEN row 1 sums 2+3 = 5
- AND row 2 sums 1+3 = 4
- AND row 3 sums 1+2 = 3

#### Scenario: EXCLUDE GROUP
- GIVEN table with values [(10), (10), (20)] ordered by val
- WHEN executing "SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM t"
- THEN first row (val=10) sums 20 (excludes both 10s from peer group)
- AND second row (val=10) sums 20 (excludes both 10s)
- AND third row (val=20) sums 20 (excludes just the single 20)

#### Scenario: EXCLUDE TIES
- GIVEN table with values [(10), (10), (20)] ordered by val
- WHEN executing "SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM t"
- THEN first row sums 10+20 = 30 (includes self, excludes other 10)
- AND second row sums 10+20 = 30 (includes self, excludes other 10)
- AND third row sums 40 (no ties to exclude)

#### Scenario: EXCLUDE with single-row groups
- GIVEN table with values [(1), (2), (3)] all unique
- WHEN executing with EXCLUDE TIES
- THEN result equals EXCLUDE NO OTHERS (no ties exist)

### Requirement: NULLS FIRST/LAST in ORDER BY

The executor SHALL respect NULLS FIRST and NULLS LAST ordering.

#### Scenario: NULLS LAST (default)
- GIVEN table with values [(1), (NULL), (3), (2)]
- WHEN executing "SELECT val, ROW_NUMBER() OVER (ORDER BY val) FROM t"
- THEN order is [1, 2, 3, NULL] with row numbers [1, 2, 3, 4]

#### Scenario: NULLS FIRST
- GIVEN table with values [(1), (NULL), (3), (2)]
- WHEN executing "SELECT val, ROW_NUMBER() OVER (ORDER BY val NULLS FIRST) FROM t"
- THEN order is [NULL, 1, 2, 3] with row numbers [1, 2, 3, 4]

#### Scenario: DESC NULLS FIRST
- GIVEN table with values [(1), (NULL), (3), (2)]
- WHEN executing "SELECT val, ROW_NUMBER() OVER (ORDER BY val DESC NULLS FIRST) FROM t"
- THEN order is [NULL, 3, 2, 1] with row numbers [1, 2, 3, 4]

#### Scenario: DESC NULLS LAST
- GIVEN table with values [(1), (NULL), (3), (2)]
- WHEN executing "SELECT val, ROW_NUMBER() OVER (ORDER BY val DESC NULLS LAST) FROM t"
- THEN order is [3, 2, 1, NULL] with row numbers [1, 2, 3, 4]

### Requirement: IGNORE NULLS Modifier

The executor SHALL implement IGNORE NULLS for value functions.

#### Scenario: LAG IGNORE NULLS
- GIVEN table with values [(1), (NULL), (3)] ordered by id
- WHEN executing "SELECT val, LAG(val) IGNORE NULLS OVER (ORDER BY id) FROM t"
- THEN row 1 has LAG = NULL (no previous non-null)
- AND row 2 has LAG = 1 (previous non-null is 1)
- AND row 3 has LAG = 1 (skips NULL at row 2, finds 1)

#### Scenario: LEAD IGNORE NULLS
- GIVEN table with values [(1), (NULL), (3)] ordered by id
- WHEN executing "SELECT val, LEAD(val) IGNORE NULLS OVER (ORDER BY id) FROM t"
- THEN row 1 has LEAD = 3 (skips NULL, finds 3)
- AND row 2 has LEAD = 3 (next non-null is 3)
- AND row 3 has LEAD = NULL (no next non-null)

#### Scenario: FIRST_VALUE IGNORE NULLS
- GIVEN table with values [(NULL), (2), (3)] ordered by id
- WHEN executing "SELECT val, FIRST_VALUE(val) IGNORE NULLS OVER (ORDER BY id) FROM t"
- THEN all rows have FIRST_VALUE = 2 (first non-null in partition)

#### Scenario: LAST_VALUE IGNORE NULLS
- GIVEN table with values [(1), (2), (NULL)] ordered by id
- AND frame ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
- WHEN executing "SELECT val, LAST_VALUE(val) IGNORE NULLS OVER (...) FROM t"
- THEN all rows have LAST_VALUE = 2 (last non-null in partition)

#### Scenario: NTH_VALUE IGNORE NULLS
- GIVEN table with values [(NULL), (2), (NULL), (4)] ordered by id
- WHEN executing "SELECT val, NTH_VALUE(val, 2) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t"
- THEN all rows have NTH_VALUE = 4 (second non-null value)

### Requirement: FILTER Clause

The executor SHALL implement FILTER clause for aggregate window functions.

#### Scenario: COUNT with FILTER
- GIVEN table with values [(1, 'a'), (2, 'b'), (3, 'a'), (4, 'b')]
- WHEN executing "SELECT id, COUNT(*) FILTER (WHERE cat = 'a') OVER () FROM t"
- THEN all rows have count = 2 (only rows where cat='a' are counted)

#### Scenario: SUM with FILTER
- GIVEN table with values [(10, true), (20, false), (30, true)]
- WHEN executing "SELECT val, SUM(val) FILTER (WHERE active) OVER () FROM t"
- THEN all rows have sum = 40 (10 + 30 where active=true)

#### Scenario: FILTER with frame
- GIVEN table with values [(1, true), (2, false), (3, true), (4, true)]
- WHEN executing "SELECT val, SUM(val) FILTER (WHERE flag) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- THEN filter is applied within each row's frame

#### Scenario: FILTER excludes all rows
- GIVEN table with values [(1, 'a'), (2, 'a'), (3, 'a')]
- WHEN executing "SELECT id, COUNT(*) FILTER (WHERE cat = 'b') OVER () FROM t"
- THEN all rows have count = 0 (no rows match filter)

### Requirement: DISTINCT Aggregate Windows

The executor SHALL implement DISTINCT modifier for aggregate window functions.

#### Scenario: COUNT DISTINCT
- GIVEN table with values [('a'), ('b'), ('a'), ('c'), ('b')]
- WHEN executing "SELECT cat, COUNT(DISTINCT cat) OVER () FROM t"
- THEN all rows have count = 3 (distinct values: a, b, c)

#### Scenario: SUM DISTINCT
- GIVEN table with values [(10), (20), (10), (30), (20)]
- WHEN executing "SELECT val, SUM(DISTINCT val) OVER () FROM t"
- THEN all rows have sum = 60 (10 + 20 + 30)

#### Scenario: DISTINCT with frame
- GIVEN table with values [(1), (2), (1), (3), (2)] ordered by id
- WHEN executing "SELECT val, COUNT(DISTINCT val) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t"
- THEN row 1 has count 1 (just {1})
- AND row 2 has count 2 ({1, 2})
- AND row 3 has count 2 ({2, 1})
- AND row 4 has count 3 ({1, 3})
- AND row 5 has count 2 ({3, 2})

### Requirement: Window Function Return Types

The executor SHALL return correct types for each window function.

#### Scenario: Ranking functions return BIGINT
- GIVEN any query with ROW_NUMBER(), RANK(), DENSE_RANK(), or NTILE()
- WHEN executing the query
- THEN GetTypes() returns BIGINT for these columns

#### Scenario: Distribution functions return DOUBLE
- GIVEN any query with PERCENT_RANK() or CUME_DIST()
- WHEN executing the query
- THEN GetTypes() returns DOUBLE for these columns
- AND values are in range [0.0, 1.0] for PERCENT_RANK
- AND values are in range (0.0, 1.0] for CUME_DIST

#### Scenario: Value functions inherit argument type
- GIVEN query "SELECT LAG(name) OVER () FROM t" where name is VARCHAR
- WHEN executing the query
- THEN GetTypes() returns VARCHAR for LAG column

#### Scenario: Aggregate window return types
- GIVEN query with COUNT(*) OVER ()
- WHEN executing the query
- THEN GetTypes() returns BIGINT for COUNT column

### Requirement: Window Error Handling

The executor SHALL return appropriate errors for invalid window operations.

#### Scenario: NTILE with zero buckets
- GIVEN query "SELECT NTILE(0) OVER (ORDER BY id) FROM t"
- WHEN executing the query
- THEN ErrorTypeExecutor is returned
- AND error message indicates bucket count must be positive

#### Scenario: NTILE with negative buckets
- GIVEN query "SELECT NTILE(-5) OVER (ORDER BY id) FROM t"
- WHEN executing the query
- THEN ErrorTypeExecutor is returned

#### Scenario: NTH_VALUE with zero index
- GIVEN query "SELECT NTH_VALUE(x, 0) OVER () FROM t"
- WHEN executing the query
- THEN ErrorTypeExecutor is returned
- AND error message indicates index must be positive (1-based)

#### Scenario: LAG with negative offset
- GIVEN query "SELECT LAG(x, -1) OVER (ORDER BY id) FROM t"
- WHEN executing the query
- THEN ErrorTypeExecutor is returned
- AND error message indicates offset must be non-negative

#### Scenario: Frame offset is negative
- GIVEN query "SELECT SUM(x) OVER (ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM t"
- WHEN executing the query
- THEN ErrorTypeExecutor is returned
