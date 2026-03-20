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

### Requirement: Built-in read_csv Table Function

The engine SHALL provide a read_csv table function for CSV file reading.

#### Scenario: Basic read_csv usage
- GIVEN a CSV file "data.csv" with header and data
- WHEN executing `SELECT * FROM read_csv('data.csv')`
- THEN all rows are returned with inferred column names and types

#### Scenario: read_csv with explicit columns
- GIVEN a CSV file without header
- WHEN executing `SELECT * FROM read_csv('data.csv', columns={'id': 'INTEGER', 'name': 'VARCHAR'})`
- THEN columns are typed as specified

#### Scenario: read_csv with delimiter option
- GIVEN a tab-separated file
- WHEN executing `SELECT * FROM read_csv('data.tsv', delim='\t')`
- THEN file is parsed with tab delimiter

#### Scenario: read_csv with header option
- GIVEN a CSV file without header row
- WHEN executing `SELECT * FROM read_csv('data.csv', header=false)`
- THEN first row is treated as data, columns named column0, column1, etc.

### Requirement: Built-in read_csv_auto Table Function

The engine SHALL provide read_csv_auto for automatic CSV detection.

#### Scenario: Automatic format detection
- GIVEN a CSV file with standard format
- WHEN executing `SELECT * FROM read_csv_auto('data.csv')`
- THEN delimiter, quote, header, and types are auto-detected

#### Scenario: Auto-detect works with various formats
- GIVEN CSV files with comma, tab, semicolon, or pipe delimiters
- WHEN executing read_csv_auto on each
- THEN each is correctly parsed

### Requirement: Built-in read_json Table Function

The engine SHALL provide a read_json table function.

#### Scenario: Read JSON array file
- GIVEN a file containing JSON array of objects
- WHEN executing `SELECT * FROM read_json('data.json')`
- THEN objects are returned as rows

#### Scenario: Read NDJSON file
- GIVEN a newline-delimited JSON file
- WHEN executing `SELECT * FROM read_json('data.ndjson', format='newline_delimited')`
- THEN each line is parsed as a row

#### Scenario: read_json with columns
- GIVEN a JSON file
- WHEN executing `SELECT * FROM read_json('data.json', columns={'id': 'INTEGER', 'name': 'VARCHAR'})`
- THEN only specified columns are returned with specified types

### Requirement: Built-in read_json_auto Table Function

The engine SHALL provide read_json_auto for automatic JSON detection.

#### Scenario: Auto-detect JSON format
- GIVEN a JSON file (array or NDJSON)
- WHEN executing `SELECT * FROM read_json_auto('data.json')`
- THEN format and schema are auto-detected

### Requirement: Built-in read_ndjson Table Function

The engine SHALL provide read_ndjson as alias for NDJSON reading.

#### Scenario: Read NDJSON via alias
- GIVEN a newline-delimited JSON file
- WHEN executing `SELECT * FROM read_ndjson('data.ndjson')`
- THEN file is read as NDJSON format
- AND behavior is equivalent to `read_json('data.ndjson', format='newline_delimited')`

### Requirement: Built-in read_parquet Table Function

The engine SHALL provide a read_parquet table function.

#### Scenario: Read Parquet file
- GIVEN a Parquet file with data
- WHEN executing `SELECT * FROM read_parquet('data.parquet')`
- THEN all rows and columns are returned

#### Scenario: Column projection
- GIVEN a Parquet file with columns a, b, c, d
- WHEN executing `SELECT a, c FROM read_parquet('data.parquet')`
- THEN only columns a and c are read from file (I/O optimization)

#### Scenario: Read compressed Parquet
- GIVEN a Parquet file with ZSTD compression
- WHEN executing `SELECT * FROM read_parquet('data.parquet')`
- THEN data is decompressed and returned correctly

### Requirement: Table Function in FROM Clause

The engine SHALL allow file-reading table functions in FROM clause.

#### Scenario: Join with table function
- GIVEN a table "users" and CSV file "orders.csv"
- WHEN executing `SELECT * FROM users JOIN read_csv('orders.csv') o ON users.id = o.user_id`
- THEN join is performed correctly

#### Scenario: Subquery with table function
- GIVEN a Parquet file
- WHEN executing `SELECT * FROM (SELECT * FROM read_parquet('data.parquet') WHERE x > 10) sub`
- THEN subquery is evaluated correctly

#### Scenario: CTE with table function
- GIVEN a CSV file
- WHEN executing `WITH data AS (SELECT * FROM read_csv_auto('data.csv')) SELECT * FROM data WHERE id > 5`
- THEN CTE with table function works correctly

### Requirement: PIVOT Operator Execution

The executor SHALL implement PIVOT operation by transforming into conditional aggregation.

#### Scenario: PIVOT with single aggregate and grouping column
- WHEN executing `PIVOT sales ON quarter USING SUM(amount) GROUP BY product`
- THEN the executor SHALL compute `SUM(CASE WHEN quarter = 'Q1' THEN amount END)` for each quarter
- AND produce output with columns: product, Q1, Q2, Q3, Q4
- AND NULL used for missing combinations

#### Scenario: PIVOT with multiple aggregates
- WHEN executing `PIVOT sales ON quarter USING SUM(amount), COUNT(*) GROUP BY product`
- THEN the executor SHALL compute both SUM and COUNT for each pivot value
- AND produce output with columns: product, Q1_sum, Q1_count, Q2_sum, Q2_count, etc.

### Requirement: UNPIVOT Operator Execution

The executor SHALL implement UNPIVOT operation by transforming rows into columns.

#### Scenario: UNPIVOT with multiple columns
- WHEN executing `UNPIVOT sales INTO val FOR month IN (jan, feb, mar)`
- THEN for each input row, produce three output rows
- AND val column contains the original column value
- AND month column contains the pivot column name (jan, feb, or mar)

#### Scenario: UNPIVOT with data type preservation
- WHEN executing UNPIVOT on columns of various types
- THEN the executor SHALL preserve the original data types
- AND NULL values in source columns become NULL in unpivoted output

### Requirement: GROUPING SETS Execution

The executor SHALL support GROUP BY GROUPING SETS, ROLLUP, and CUBE by expanding into multiple grouping sets.

#### Scenario: GROUPING SETS with two sets
- WHEN executing `SELECT a, b, SUM(c) FROM t GROUP BY GROUPING SETS ((a), (b))`
- THEN the executor SHALL compute aggregates for grouping set (a,) and (b,)
- AND produce output where columns not in grouping set are NULL
- AND maintain correct row ordering

#### Scenario: ROLLUP with three columns
- WHEN executing `SELECT a, b, c, SUM(x) FROM t GROUP BY ROLLUP (a, b, c)`
- THEN the executor SHALL compute aggregates for:
  - (a, b, c)
  - (a, b)
  - (a)
  - ()
- AND set appropriate NULL values for rolled-up columns

#### Scenario: CUBE with two columns
- WHEN executing `SELECT a, b, SUM(x) FROM t GROUP BY CUBE (a, b)`
- THEN the executor SHALL compute aggregates for:
  - (a, b)
  - (a)
  - (b)
  - ()
- AND set appropriate NULL values

### Requirement: GROUPING() Function Execution

The executor SHALL implement GROUPING() function to identify grouping set membership.

#### Scenario: GROUPING() with simple grouping set
- WHEN executing `SELECT a, b, GROUPING(a), GROUPING(b), SUM(c) FROM t GROUP BY GROUPING SETS ((a), (b))`
- THEN GROUPING(a) returns 1 when a is NULL (rolled up), 0 otherwise
- AND GROUPING(b) returns 1 when b is NULL (rolled up), 0 otherwise

#### Scenario: GROUPING() with composite key
- WHEN executing `SELECT a, b, GROUPING(a, b), SUM(c) FROM t GROUP BY CUBE (a, b)`
- THEN GROUPING(a, b) returns bitmask indicating which columns are rolled up

### Requirement: RECURSIVE CTE Execution

The executor SHALL implement recursive CTEs using iterative fixpoint algorithm.

#### Scenario: Simple recursive CTE (sequence)
- WHEN executing `WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 5) SELECT * FROM cte`
- THEN the executor SHALL iterate:
  - Iteration 0: Insert (1) into work table
  - Iteration 1: Compute (2) from (1)
  - Iteration 2: Compute (3) from (2)
  - Iteration 3: Compute (4) from (3)
  - Iteration 4: Compute (5) from (4)
  - Iteration 5: Stop (n >= 5)
- AND return 5 rows: 1, 2, 3, 4, 5

#### Scenario: Recursive CTE with MAX RECURSION hint
- WHEN executing `WITH RECURSIVE cte AS (...) SELECT ... OPTION (MAX_RECURSION 10)`
- THEN the executor SHALL stop after 10 iterations
- AND raise error if recursion exceeds limit without termination

#### Scenario: Recursive CTE with multiple UNION ALL parts
- WHEN executing `WITH RECURSIVE cte AS (SELECT ... UNION ALL SELECT ... FROM cte JOIN ...)`
- THEN the executor SHALL correctly join recursive reference with base table

### Requirement: LATERAL Join Execution

The executor SHALL implement LATERAL joins by re-evaluating subquery for each outer row.

#### Scenario: LATERAL correlated subquery
- WHEN executing `SELECT t.id, sub.x FROM t, LATERAL (SELECT t.id + 1 AS x) AS sub`
- THEN for each row in t, execute subquery with t's bindings available
- AND produce one output row per input row with correlated value

#### Scenario: LATERAL with aggregation
- WHEN executing `SELECT t.id, sub.cnt FROM t, LATERAL (SELECT COUNT(*) FROM orders WHERE orders.customer_id = t.id) AS sub`
- THEN for each t row, execute aggregation with correlation
- AND correctly handle cases where no orders exist (NULL result)

#### Scenario: LATERAL table function
- WHEN executing `SELECT g.val FROM t, LATERAL generate_series(1, t.n) AS g(val)`
- THEN for each t row, generate series up to t.n value
- AND produce multiple rows per input row

### Requirement: MERGE INTO Execution

The executor SHALL implement MERGE INTO using HashJoin for matching.

#### Scenario: MERGE with WHEN MATCHED UPDATE
- WHEN executing `MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = s.x`
- THEN the executor SHALL:
  1. Build hash table from source
  2. Probe target against source hash table
  3. For matching rows, update target columns from source
  4. Return count of updated rows

#### Scenario: MERGE with WHEN NOT MATCHED INSERT
- WHEN executing `MERGE INTO target t USING source s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, x) VALUES (s.id, s.x)`
- THEN the executor SHALL:
  1. Build hash table from source
  2. Probe target against source hash table
  3. For non-matching source rows, insert into target
  4. Return count of inserted rows

#### Scenario: MERGE with multiple WHEN MATCHED conditions
- WHEN executing `MERGE INTO t USING s ON t.id = s.id WHEN MATCHED AND t.version < s.version THEN UPDATE SET x = s.x WHEN MATCHED THEN DELETE`
- THEN the executor SHALL evaluate conditions in order
- AND apply first matching action per source row

#### Scenario: MERGE with RETURNING
- WHEN executing `MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = s.x RETURNING old.*, new.*`
- THEN the executor SHALL return rows with old and new column values
- AND include only modified rows in result set

### Requirement: RETURNING Clause Execution

The executor SHALL return modified rows for INSERT, UPDATE, DELETE with RETURNING clause.

#### Scenario: INSERT RETURNING generated values
- WHEN executing `INSERT INTO t (a, b) VALUES (1, 2) RETURNING *`
- THEN the executor SHALL return row with generated rowid and column values

#### Scenario: UPDATE RETURNING modified columns
- WHEN executing `UPDATE t SET x = x + 1 WHERE id = 5 RETURNING id, x, old.x`
- THEN the executor SHALL return row with updated values and old.x before update

#### Scenario: DELETE RETURNING deleted values
- WHEN executing `DELETE FROM t WHERE id = 5 RETURNING *`
- THEN the executor SHALL return row with values before deletion
- AND row is removed from table

### Requirement: DISTINCT ON Execution

The executor SHALL implement DISTINCT ON using sort and first aggregate per key.

#### Scenario: DISTINCT ON with ORDER BY
- WHEN executing `SELECT DISTINCT ON (a) a, b, c FROM t ORDER BY a, b`
- THEN the executor SHALL sort by (a, b)
- AND for each unique a, return first row
- AND maintain ordering by ORDER BY columns

#### Scenario: DISTINCT ON without ORDER BY
- WHEN executing `SELECT DISTINCT ON (a) a, b FROM t`
- THEN the executor SHALL return arbitrary first row per a
- AND behavior matches DuckDB for undefined ordering

### Requirement: QUALIFY Clause Execution

The executor SHALL filter results after window function evaluation.

#### Scenario: QUALIFY with ROW_NUMBER
- WHEN executing `SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS rn FROM t QUALIFY rn <= 3`
- THEN window functions SHALL be evaluated first
- AND filter applied to keep only rows where rn <= 3

#### Scenario: QUALIFY with aggregate window
- WHEN executing `SELECT a, SUM(b) OVER (PARTITION BY a) AS sum_b FROM t QUALIFY sum_b > 100`
- THEN window frame computed before QUALIFY filter
- AND only partitions with sum > 100 included in output

### Requirement: SAMPLE Clause Execution

The executor SHALL implement SAMPLE clause using reservoir sampling algorithm.

#### Scenario: SAMPLE with percentage (BERNOULLI)
- WHEN executing `SELECT * FROM t SAMPLE 10 PERCENT`
- THEN each row SHALL have 10% probability of inclusion
- AND result size approximately 10% of input

#### Scenario: SAMPLE with row count (RESERVOIR)
- WHEN executing `SELECT * FROM t SAMPLE 100 ROWS`
- THEN the executor SHALL select exactly 100 rows
- AND all rows have equal probability of selection

#### Scenario: SAMPLE with seed
- WHEN executing `SELECT * FROM t SAMPLE 50 ROWS (RESERVOIR, 50, 42)`
- THEN with same seed, same input SHALL produce same sample
- AND seed parameter enables reproducibility

#### Scenario: SAMPLE applied before LIMIT
- WHEN executing `SELECT * FROM t SAMPLE 10 PERCENT LIMIT 10`
- THEN SAMPLE SHALL be applied first, then LIMIT on sampled data
- AND final result has at most 10 rows

### Requirement: Statistical Aggregate Functions

The executor SHALL implement statistical aggregate functions including median, quantile, mode, entropy, skewness, and kurtosis.

#### Scenario: MEDIAN calculation with odd number of values
- WHEN executing `SELECT MEDIAN(val) FROM (VALUES (1), (3), (5), (2), (4)) AS t(val)`
- THEN the executor SHALL return 3.0
- AND the value at position (n+1)/2 after sorting is returned

#### Scenario: MEDIAN calculation with even number of values
- WHEN executing `SELECT MEDIAN(val) FROM (VALUES (1), (2), (3), (4)) AS t(val)`
- THEN the executor SHALL return 2.5
- AND the average of the two middle values is returned

#### Scenario: QUANTILE at specific percentile
- WHEN executing `SELECT QUANTILE(val, 0.75) FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) AS t(val)`
- THEN the executor SHALL return approximately 7.75
- AND linear interpolation is used between adjacent values

#### Scenario: QUANTILE with array of percentiles
- WHEN executing `SELECT QUANTILE(val, [0.25, 0.5, 0.75]) FROM t`
- THEN the executor SHALL return an array with three values
- AND each value corresponds to the respective percentile

#### Scenario: MODE with single mode
- WHEN executing `SELECT MODE(val) FROM (VALUES ('a'), ('b'), ('a'), ('c'), ('a')) AS t(val)`
- THEN the executor SHALL return 'a'
- AND 'a' has the highest frequency count

#### Scenario: MODE with multiple values having same max frequency
- WHEN executing `SELECT MODE(val) FROM (VALUES ('a'), ('b')) AS t(val)`
- THEN the executor SHALL return one of the values
- AND the behavior matches DuckDB for tie-breaking

#### Scenario: ENTROPY calculation
- WHEN executing `SELECT ENTROPY(val) FROM (VALUES ('a'), ('a'), ('b'), ('b'), ('c')) AS t(val)`
- THEN the executor SHALL return the Shannon entropy
- AND H = -sum(p * log(p)) where p is frequency/total

#### Scenario: SKEWNESS calculation
- WHEN executing `SELECT SKEWNESS(val) FROM t`
- THEN the executor SHALL return the Fisher-Pearson coefficient of skewness
- AND the formula matches DuckDB's implementation

#### Scenario: KURTOSIS calculation
- WHEN executing `SELECT KURTOSIS(val) FROM t`
- THEN the executor SHALL return the excess kurtosis (Fisher's definition)
- AND the formula matches DuckDB's implementation

#### Scenario: VAR_POP calculation
- WHEN executing `SELECT VAR_POP(val) FROM (VALUES (1), (2), (3), (4), (5)) AS t(val)`
- THEN the executor SHALL return 2.0
- AND population variance formula is used

#### Scenario: VAR_SAMP calculation
- WHEN executing `SELECT VAR_SAMP(val) FROM (VALUES (1), (2), (3), (4), (5)) AS t(val)`
- THEN the executor SHALL return 2.5
- AND sample variance formula is used

#### Scenario: STDDEV_POP calculation
- WHEN executing `SELECT STDDEV_POP(val) FROM t`
- THEN the executor SHALL return the square root of population variance
- AND matches SQL standard definition

#### Scenario: STDDEV_SAMP calculation
- WHEN executing `SELECT STDDEV_SAMP(val) FROM t`
- THEN the executor SHALL return the square root of sample variance
- AND matches SQL standard definition

### Requirement: Approximate Aggregate Functions

The executor SHALL implement approximate aggregate functions using HyperLogLog and t-digest algorithms.

#### Scenario: APPROX_COUNT_DISTINCT accuracy
- WHEN executing `SELECT APPROX_COUNT_DISTINCT(val) FROM generate_series(1, 1000000) AS t(val)`
- THEN the result SHALL be within 5% of the exact COUNT(DISTINCT)
- AND the error rate improves with larger datasets

#### Scenario: APPROX_COUNT_DISTINCT with many duplicates
- WHEN executing `SELECT APPROX_COUNT_DISTINCT(val) FROM (SELECT 1 AS val UNION ALL SELECT 1 AS val UNION ALL SELECT 2 AS val)`
- THEN the executor SHALL return approximately 2
- AND small number correction is applied

#### Scenario: APPROX_QUANTILE calculation
- WHEN executing `SELECT APPROX_QUANTILE(val, 0.5) FROM generate_series(1, 10000) AS t(val)`
- THEN the result SHALL be within 1% of exact QUANTILE(val, 0.5)
- AND t-digest compression parameter controls accuracy

#### Scenario: APPROX_MEDIAN calculation
- WHEN executing `SELECT APPROX_MEDIAN(val) FROM t`
- THEN the executor SHALL return an approximation of the median
- AND uses t-digest with default compression

#### Scenario: APPROX functions with NULL values
- WHEN executing `SELECT APPROX_COUNT_DISTINCT(val) FROM (VALUES (1), (NULL), (2), (NULL)) AS t(val)`
- THEN NULL values SHALL be ignored
- AND only non-NULL values are counted

### Requirement: Boolean and Bitwise Aggregate Functions

The executor SHALL implement boolean and bitwise aggregate functions.

#### Scenario: BOOL_AND returns TRUE only if all values are TRUE
- WHEN executing `SELECT BOOL_AND(val) FROM (VALUES (TRUE), (TRUE), (FALSE)) AS t(val)`
- THEN the executor SHALL return FALSE
- AND if any value is FALSE, result is FALSE

#### Scenario: BOOL_AND with all NULL values
- WHEN executing `SELECT BOOL_AND(val) FROM (VALUES (NULL), (NULL)) AS t(val)`
- THEN the executor SHALL return NULL
- AND NULL values are not considered

#### Scenario: BOOL_OR returns TRUE if any value is TRUE
- WHEN executing `SELECT BOOL_OR(val) FROM (VALUES (FALSE), (FALSE), (TRUE)) AS t(val)`
- THEN the executor SHALL return TRUE
- AND if any value is TRUE, result is TRUE

#### Scenario: BOOL_OR with all FALSE values
- WHEN executing `SELECT BOOL_OR(val) FROM (VALUES (FALSE), (FALSE)) AS t(val)`
- THEN the executor SHALL return FALSE
- AND NULL values are not considered

#### Scenario: BIT_AND across integer values
- WHEN executing `SELECT BIT_AND(val) FROM (VALUES (CAST(5 AS TINYINT)), (CAST(3 AS TINYINT)), (CAST(7 AS TINYINT))) AS t(val)`
- THEN the executor SHALL return 1 (binary 001)
- AND bitwise AND is performed on each bit position

#### Scenario: BIT_OR across integer values
- WHEN executing `SELECT BIT_OR(val) FROM (VALUES (CAST(5 AS TINYINT)), (CAST(3 AS TINYINT)), (CAST(1 AS TINYINT))) AS t(val)`
- THEN the executor SHALL return 7 (binary 111)
- AND bitwise OR is performed on each bit position

#### Scenario: BIT_XOR across integer values
- WHEN executing `SELECT BIT_XOR(val) FROM (VALUES (CAST(5 AS TINYINT)), (CAST(3 AS TINYINT))) AS t(val)`
- THEN the executor SHALL return 6 (binary 110)
- AND bitwise XOR is performed on each bit position

### Requirement: String and List Aggregate Functions

The executor SHALL implement string and list aggregation functions.

#### Scenario: STRING_AGG with delimiter
- WHEN executing `SELECT STRING_AGG(val, ',') FROM (VALUES ('a'), ('b'), ('c')) AS t(val)`
- THEN the executor SHALL return 'a,b,c'
- AND values are concatenated with the delimiter

#### Scenario: STRING_AGG with ORDER BY
- WHEN executing `SELECT STRING_AGG(val, ',' ORDER BY val DESC) FROM (VALUES ('a'), ('b'), ('c')) AS t(val)`
- THEN the executor SHALL return 'c,b,a'
- AND values are ordered before concatenation

#### Scenario: GROUP_CONCAT MySQL compatibility
- WHEN executing `SELECT GROUP_CONCAT(val) FROM (VALUES ('a'), ('b')) AS t(val)`
- THEN the executor SHALL return 'a,b'
- AND default comma delimiter is used

#### Scenario: LIST aggregation
- WHEN executing `SELECT LIST(val) FROM (VALUES (1), (2), (3)) AS t(val)`
- THEN the executor SHALL return [1, 2, 3]
- AND values are aggregated into a list

#### Scenario: LIST with ORDER BY
- WHEN executing `SELECT LIST(val ORDER BY val DESC) FROM (VALUES (1), (2), (3)) AS t(val)`
- THEN the executor SHALL return [3, 2, 1]
- AND values are ordered before aggregation

#### Scenario: LIST_DISTINCT aggregation
- WHEN executing `SELECT LIST_DISTINCT(val) FROM (VALUES (1), (1), (2), (2), (3)) AS t(val)`
- THEN the executor SHALL return [1, 2, 3]
- AND duplicate values are removed

### Requirement: Time Series Aggregate Functions

The executor SHALL implement time series aggregate functions for conditional and positional aggregation.

#### Scenario: COUNT_IF with condition
- WHEN executing `SELECT COUNT_IF(val > 5) FROM (VALUES (1), (6), (3), (7), (4)) AS t(val)`
- THEN the executor SHALL return 2
- AND only rows where val > 5 are counted

#### Scenario: COUNT_IF with no matching rows
- WHEN executing `SELECT COUNT_IF(val > 100) FROM (VALUES (1), (2), (3)) AS t(val)`
- THEN the executor SHALL return 0
- AND NULL is not returned

#### Scenario: FIRST aggregation
- WHEN executing `SELECT FIRST(val) FROM t`
- THEN the executor SHALL return the first non-NULL value in the group
- AND order is determined by input order

#### Scenario: LAST aggregation
- WHEN executing `SELECT LAST(val) FROM t`
- THEN the executor SHALL return the last non-NULL value in the group
- AND order is determined by input order

#### Scenario: ARGMIN returns argument with minimum value
- WHEN executing `SELECT ARGMIN(name, score) FROM (VALUES ('a', 10), ('b', 5), ('c', 8)) AS t(name, score)`
- THEN the executor SHALL return 'b'
- AND 'b' has the minimum score of 5

#### Scenario: ARGMAX returns argument with maximum value
- WHEN executing `SELECT ARGMAX(name, score) FROM t`
- THEN the executor SHALL return the name with the maximum score
- AND if multiple rows have same max, returns first encountered

#### Scenario: MIN_BY aggregation
- WHEN executing `SELECT MIN_BY(name, score) FROM t`
- THEN the executor SHALL return the name associated with the minimum score
- AND score determines which name is chosen

#### Scenario: MAX_BY aggregation
- WHEN executing `SELECT MAX_BY(name, score) FROM t`
- THEN the executor SHALL return the name associated with the maximum score
- AND score determines which name is chosen

### Requirement: Regression and Correlation Functions

The executor SHALL implement regression and correlation functions for statistical analysis.

#### Scenario: COVAR_POP population covariance
- WHEN executing `SELECT COVAR_POP(x, y) FROM (VALUES (1, 2), (2, 4), (3, 5)) AS t(x, y)`
- THEN the executor SHALL return the population covariance
- AND formula is E[(X - E[X])(Y - E[Y])]

#### Scenario: COVAR_SAMP sample covariance
- WHEN executing `SELECT COVAR_SAMP(x, y) FROM t`
- THEN the executor SHALL return sample covariance
- AND denominator is N-1

#### Scenario: CORR correlation coefficient
- WHEN executing `SELECT CORR(x, y) FROM t`
- THEN the executor SHALL return Pearson correlation coefficient
- AND value is between -1 and 1

#### Scenario: Perfect positive correlation
- WHEN executing `SELECT CORR(x, y) FROM (VALUES (1, 2), (2, 4), (3, 6)) AS t(x, y)`
- THEN the executor SHALL return 1.0
- AND y = 2*x produces perfect positive correlation

#### Scenario: Perfect negative correlation
- WHEN executing `SELECT CORR(x, y) FROM (VALUES (1, 6), (2, 4), (3, 2)) AS t(x, y)`
- THEN the executor SHALL return -1.0
- AND y = -  82*x produces perfect negative correlation

#### Scenario: REGR_INTERCEPT returns y-intercept
- WHEN executing `SELECT REGR_INTERCEPT(y, x) FROM t`
- THEN the executor SHALL return the intercept of linear regression
- AND y = intercept + slope * x

#### Scenario: REGR_SLOPE returns slope
- WHEN executing `SELECT REGR_SLOPE(y, x) FROM t`
- THEN the executor SHALL return the slope of linear regression
- AND uses least squares estimation

#### Scenario: REGR_R2 returns coefficient of determination
- WHEN executing `SELECT REGR_R2(y, x) FROM t`
- THEN the executor SHALL return R-squared value
- AND value is between 0 and 1

### Requirement: Aggregate Function Edge Cases

The executor SHALL handle edge cases correctly for all aggregate functions.

#### Scenario: All NULL input for aggregates
- WHEN executing `SELECT COUNT(*), AVG(x), MEDIAN(x), MODE(x) FROM (VALUES (NULL), (NULL)) AS t(x)`
- THEN COUNT SHALL return 2
- AND AVG, MEDIAN, MODE SHALL return NULL

#### Scenario: Empty group with aggregates
- WHEN executing `SELECT COUNT(x), AVG(x), MEDIAN(x) FROM t GROUP BY y HAVING COUNT(*) = 0`
- THEN aggregates SHALL return NULL for groups with no rows
- AND COUNT returns 0

#### Scenario: Single-row group
- WHEN executing `SELECT MEDIAN(x), AVG(x), STDDEV(x) FROM (VALUES (5)) AS t(x)`
- THEN MEDIAN and AVG return 5
- AND STDDEV returns NULL (undefined for single value)

#### Scenario: DISTINCT modifier with aggregates
- WHEN executing `SELECT COUNT(DISTINCT x), MEDIAN(DISTINCT x) FROM t`
- THEN duplicate values SHALL be removed before aggregation
- AND the behavior matches DuckDB

#### Scenario: FILTER clause with aggregates
- WHEN executing `SELECT COUNT(*) FILTER (WHERE x > 0) FROM t`
- THEN the filter SHALL be applied before aggregation
- AND only matching rows are counted

#### Scenario: Aggregate with ORDER BY
- WHEN executing `SELECT ARRAY_AGG(x ORDER BY x DESC) FROM t`
- THEN the ORDER BY SHALL determine the order of values in the result
- AND the aggregate output respects the ordering

### Requirement: EXPORT DATABASE Execution

The engine SHALL export the entire database schema and data to a directory, producing schema.sql (DDL), data files (one per table), and load.sql (COPY FROM statements) in dependency order.

#### Scenario: Export database with single table as CSV

- GIVEN a database with table "users" (id INTEGER, name VARCHAR) containing rows (1, 'alice'), (2, 'bob')
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN directory '/tmp/export' is created
- AND file 'schema.sql' contains `CREATE TABLE users (id INTEGER, name VARCHAR);`
- AND file 'users.csv' contains the table data in CSV format
- AND file 'load.sql' contains `COPY users FROM '/tmp/export/users.csv';`

#### Scenario: Export database with FORMAT PARQUET

- GIVEN a database with table "data" containing rows
- WHEN executing `EXPORT DATABASE '/tmp/export' (FORMAT PARQUET)`
- THEN data files use .parquet extension
- AND load.sql COPY FROM statements reference .parquet files with FORMAT PARQUET

#### Scenario: Export database with multiple schemas

- GIVEN a database with schemas "main" and "analytics" each containing tables
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql contains `CREATE SCHEMA analytics;` before analytics tables
- AND tables in "analytics" schema are exported as `analytics_{table}.csv`
- AND tables in "main" schema are exported as `{table}.csv`

#### Scenario: Export database dependency ordering

- GIVEN a database with sequence "id_seq", table "t" using nextval('id_seq'), view "v" referencing "t", and index "idx" on "t"
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql contains statements in order: CREATE SEQUENCE, CREATE TABLE, CREATE VIEW, CREATE INDEX

#### Scenario: Export database with primary key

- GIVEN a table with PRIMARY KEY (id)
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql CREATE TABLE includes PRIMARY KEY constraint

#### Scenario: Export database with views

- GIVEN a view "v" defined as `SELECT id, name FROM users WHERE active = true`
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql contains `CREATE VIEW v AS SELECT id, name FROM users WHERE active = true;`
- AND no data file is created for the view

### Requirement: IMPORT DATABASE Execution

The engine SHALL import a previously exported database by executing schema.sql followed by load.sql from the specified directory.

#### Scenario: Import database round-trip

- GIVEN an exported database at '/tmp/export' with schema.sql and load.sql
- WHEN executing `IMPORT DATABASE '/tmp/export'`
- THEN all tables, views, sequences, and indexes are recreated
- AND all table data is loaded
- AND the resulting database is equivalent to the original

#### Scenario: Import database into non-empty database fails

- GIVEN a database with existing table "users"
- AND an export directory containing a table also named "users"
- WHEN executing `IMPORT DATABASE '/tmp/export'`
- THEN an error is returned indicating the database is not empty or table already exists

#### Scenario: Import database with missing schema.sql

- WHEN executing `IMPORT DATABASE '/tmp/nonexistent'`
- THEN an error is returned indicating the directory or schema.sql does not exist

#### Scenario: Import database with FORMAT PARQUET

- GIVEN an export directory with .parquet data files and load.sql referencing them
- WHEN executing `IMPORT DATABASE '/tmp/export'`
- THEN load.sql COPY FROM statements correctly load parquet data

### Requirement: DDL Generation

The catalog SHALL provide ToCreateSQL() methods on TableDef, ViewDef, SequenceDef, and IndexDef that generate valid CREATE statements parseable by the dukdb-go parser.

#### Scenario: TableDef DDL generation with all features

- GIVEN a TableDef with columns (id INTEGER NOT NULL, name VARCHAR DEFAULT 'unknown'), PRIMARY KEY (id), and schema "main"
- WHEN calling ToCreateSQL()
- THEN the output is `CREATE TABLE main.id_table (id INTEGER NOT NULL, name VARCHAR DEFAULT 'unknown', PRIMARY KEY (id));` or equivalent valid SQL

#### Scenario: ViewDef DDL generation

- GIVEN a ViewDef with name "active_users" and SQL "SELECT * FROM users WHERE active = true"
- WHEN calling ToCreateSQL()
- THEN the output is `CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;`

#### Scenario: SequenceDef DDL generation

- GIVEN a SequenceDef with START WITH 100, INCREMENT BY 5, CYCLE
- WHEN calling ToCreateSQL()
- THEN the output includes START WITH, INCREMENT BY, and CYCLE clauses

#### Scenario: DDL round-trip correctness

- GIVEN any catalog object
- WHEN calling ToCreateSQL() and parsing the result with the dukdb-go parser
- THEN the parsed AST correctly represents the original catalog object

### Requirement: generate_series Table Function

The engine SHALL provide a generate_series(start, stop[, step]) table function that produces sequential values inclusive of the stop value, supporting INTEGER, BIGINT, DATE, and TIMESTAMP types.

#### Scenario: Integer series with default step

- WHEN executing `SELECT * FROM generate_series(1, 5)`
- THEN the result contains rows: 1, 2, 3, 4, 5

#### Scenario: Integer series with explicit step

- WHEN executing `SELECT * FROM generate_series(0, 10, 3)`
- THEN the result contains rows: 0, 3, 6, 9

#### Scenario: Descending integer series

- WHEN executing `SELECT * FROM generate_series(5, 1, -1)`
- THEN the result contains rows: 5, 4, 3, 2, 1

#### Scenario: Date series with interval step

- WHEN executing `SELECT * FROM generate_series(DATE '2024-01-01', DATE '2024-01-03', INTERVAL '1 day')`
- THEN the result contains rows: 2024-01-01, 2024-01-02, 2024-01-03

#### Scenario: Timestamp series

- WHEN executing `SELECT * FROM generate_series(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 02:00:00', INTERVAL '1 hour')`
- THEN the result contains rows: 2024-01-01 00:00:00, 2024-01-01 01:00:00, 2024-01-01 02:00:00

#### Scenario: Single value when start equals stop

- WHEN executing `SELECT * FROM generate_series(5, 5)`
- THEN the result contains a single row: 5

#### Scenario: Empty result when direction mismatches step

- WHEN executing `SELECT * FROM generate_series(5, 1, 1)`
- THEN the result is empty (start > stop with positive step)

#### Scenario: Error on zero step

- WHEN executing `SELECT * FROM generate_series(1, 10, 0)`
- THEN an error is returned indicating step size cannot be zero

#### Scenario: Column named after function

- WHEN executing `SELECT generate_series FROM generate_series(1, 3)`
- THEN the output column is named "generate_series" and contains 1, 2, 3

### Requirement: range Table Function

The engine SHALL provide a range(start, stop[, step]) table function that produces sequential values exclusive of the stop value, supporting INTEGER, BIGINT, DATE, and TIMESTAMP types.

#### Scenario: Integer range with default step

- WHEN executing `SELECT * FROM range(1, 5)`
- THEN the result contains rows: 1, 2, 3, 4 (excludes 5)

#### Scenario: Integer range with explicit step

- WHEN executing `SELECT * FROM range(0, 10, 3)`
- THEN the result contains rows: 0, 3, 6, 9

#### Scenario: Empty range when start equals stop

- WHEN executing `SELECT * FROM range(5, 5)`
- THEN the result is empty (exclusive of stop)

#### Scenario: Descending range

- WHEN executing `SELECT * FROM range(5, 1, -1)`
- THEN the result contains rows: 5, 4, 3, 2 (excludes 1)

#### Scenario: Date range

- WHEN executing `SELECT * FROM range(DATE '2024-01-01', DATE '2024-01-04', INTERVAL '1 day')`
- THEN the result contains rows: 2024-01-01, 2024-01-02, 2024-01-03 (excludes 2024-01-04)

### Requirement: SQL-Level Prepared Statement Execution

The engine SHALL support PREPARE/EXECUTE/DEALLOCATE for named SQL-level prepared statements with plan caching and parameter substitution.

#### Scenario: PREPARE and EXECUTE a SELECT

- WHEN executing `PREPARE q AS SELECT $1 + $2`
- AND then executing `EXECUTE q(10, 20)`
- THEN the result contains a single row with value 30

#### Scenario: EXECUTE with different parameters reuses plan

- GIVEN `PREPARE q AS SELECT * FROM users WHERE id = $1`
- WHEN executing `EXECUTE q(1)` then `EXECUTE q(2)` then `EXECUTE q(3)`
- THEN each execution returns the correct filtered rows
- AND the plan is parsed, bound, and planned only once (during PREPARE)

#### Scenario: PREPARE INSERT and EXECUTE multiple times

- GIVEN `PREPARE ins AS INSERT INTO t (id, name) VALUES ($1, $2)`
- WHEN executing `EXECUTE ins(1, 'alice')` then `EXECUTE ins(2, 'bob')`
- THEN both rows are inserted into the table

#### Scenario: DEALLOCATE removes prepared statement

- GIVEN `PREPARE q AS SELECT 1`
- WHEN executing `DEALLOCATE q`
- AND then executing `EXECUTE q`
- THEN an error is returned: prepared statement "q" does not exist

#### Scenario: DEALLOCATE ALL removes all prepared statements

- GIVEN `PREPARE q1 AS SELECT 1` and `PREPARE q2 AS SELECT 2`
- WHEN executing `DEALLOCATE ALL`
- AND then executing `EXECUTE q1`
- THEN an error is returned: prepared statement "q1" does not exist

#### Scenario: Error on duplicate PREPARE name

- GIVEN `PREPARE q AS SELECT 1`
- WHEN executing `PREPARE q AS SELECT 2`
- THEN an error is returned: prepared statement "q" already exists

#### Scenario: Error on EXECUTE unknown name

- WHEN executing `EXECUTE nonexistent`
- THEN an error is returned: prepared statement "nonexistent" does not exist

#### Scenario: Error on EXECUTE wrong parameter count

- GIVEN `PREPARE q AS SELECT $1 + $2`
- WHEN executing `EXECUTE q(42)`
- THEN an error is returned: expected 2 parameters, got 1

#### Scenario: Error on DEALLOCATE unknown name

- WHEN executing `DEALLOCATE nonexistent`
- THEN an error is returned: prepared statement "nonexistent" does not exist

#### Scenario: Prepared statements are connection-scoped

- GIVEN connection A with `PREPARE q AS SELECT 1`
- WHEN connection B executes `EXECUTE q`
- THEN an error is returned (prepared statement not visible across connections)

#### Scenario: Connection close cleans up prepared statements

- GIVEN a connection with multiple prepared statements
- WHEN the connection is closed
- THEN all prepared statement resources are released

### Requirement: UNIQUE Constraint Enforcement

The engine SHALL enforce UNIQUE constraints on INSERT and UPDATE, rejecting rows that violate uniqueness.

#### Scenario: UNIQUE violation on INSERT

- GIVEN table "t" with UNIQUE (email) and existing row (1, 'alice@test.com')
- WHEN executing `INSERT INTO t VALUES (2, 'alice@test.com')`
- THEN a constraint violation error is returned

#### Scenario: UNIQUE allows NULL duplicates

- GIVEN table "t" with UNIQUE (email) and existing row (1, NULL)
- WHEN executing `INSERT INTO t VALUES (2, NULL)`
- THEN the insert succeeds (NULL != NULL per SQL standard)

#### Scenario: Composite UNIQUE violation

- GIVEN table "t" with UNIQUE (a, b) and existing row (1, 2, 'old')
- WHEN executing `INSERT INTO t VALUES (1, 2, 'new')`
- THEN a constraint violation error is returned

#### Scenario: UNIQUE enforced on UPDATE

- GIVEN table "t" with UNIQUE (email) and rows (1, 'alice'), (2, 'bob')
- WHEN executing `UPDATE t SET email = 'alice' WHERE id = 2`
- THEN a constraint violation error is returned

### Requirement: CHECK Constraint Enforcement

The engine SHALL enforce CHECK constraints on INSERT and UPDATE, rejecting rows where the CHECK expression evaluates to FALSE.

#### Scenario: CHECK violation on INSERT

- GIVEN table "t" with CHECK (age >= 0)
- WHEN executing `INSERT INTO t (name, age) VALUES ('alice', -1)`
- THEN a constraint violation error is returned

#### Scenario: CHECK passes with NULL

- GIVEN table "t" with CHECK (age >= 0)
- WHEN executing `INSERT INTO t (name, age) VALUES ('alice', NULL)`
- THEN the insert succeeds (NULL does not violate CHECK per SQL standard)

#### Scenario: CHECK with multiple columns

- GIVEN table "t" with CHECK (end_date > start_date)
- WHEN executing `INSERT INTO t VALUES ('2024-01-05', '2024-01-01')`
- THEN a constraint violation error is returned (end < start)

#### Scenario: CHECK enforced on UPDATE

- GIVEN table "t" with CHECK (age >= 0) and existing row ('alice', 25)
- WHEN executing `UPDATE t SET age = -5 WHERE name = 'alice'`
- THEN a constraint violation error is returned

### Requirement: FOREIGN KEY Enforcement

The engine SHALL enforce FOREIGN KEY constraints on INSERT/UPDATE of the child table and reject DELETE/UPDATE of referenced parent rows (NO ACTION/RESTRICT only, matching DuckDB v1.4.3).

#### Scenario: FK violation on INSERT into child

- GIVEN parent table "users" with PK (id) and rows (1), (2)
- AND child table "orders" with FK (user_id) REFERENCES users(id)
- WHEN executing `INSERT INTO orders (id, user_id) VALUES (1, 999)`
- THEN a FK violation error is returned (user 999 does not exist)

#### Scenario: FK allows NULL reference

- GIVEN parent table "users" with PK (id)
- AND child table "orders" with FK (user_id) REFERENCES users(id)
- WHEN executing `INSERT INTO orders (id, user_id) VALUES (1, NULL)`
- THEN the insert succeeds (NULL FK is allowed)

#### Scenario: FK ON DELETE RESTRICT prevents deletion

- GIVEN parent "users" with row (1) and child "orders" with FK REFERENCES users(id) and rows referencing user 1
- WHEN executing `DELETE FROM users WHERE id = 1`
- THEN a FK violation error is returned (cannot delete referenced row)

#### Scenario: FK ON DELETE NO ACTION (default) prevents deletion

- GIVEN parent "users" with row (1) and child "orders" with FK (default NO ACTION) and rows referencing user 1
- WHEN executing `DELETE FROM users WHERE id = 1`
- THEN a FK violation error is returned (same as RESTRICT for immediate constraints)

#### Scenario: FK rejects CASCADE action at parse time

- WHEN executing `CREATE TABLE t (ref_id INTEGER REFERENCES other(id) ON DELETE CASCADE)`
- THEN a parse error is returned: "FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT"

#### Scenario: FK validation during CREATE TABLE

- WHEN executing `CREATE TABLE t (ref_id INTEGER REFERENCES nonexistent(id))`
- THEN an error is returned indicating referenced table does not exist

#### Scenario: Self-referencing FK

- GIVEN table "employees" with PK (id) and FK (manager_id) REFERENCES employees(id)
- WHEN executing `INSERT INTO employees (id, name, manager_id) VALUES (1, 'CEO', NULL)`
- AND then `INSERT INTO employees (id, name, manager_id) VALUES (2, 'VP', 1)`
- THEN both inserts succeed (manager_id=NULL is allowed, manager_id=1 exists)

### Requirement: UNION BY NAME Execution

The engine SHALL execute UNION BY NAME by matching columns by name across both sides, padding missing columns with NULL, and producing a unified result set.

#### Scenario: Overlapping columns with different order

- GIVEN `SELECT a, b FROM t1` returns (1, 2) and `SELECT b, a FROM t2` returns (3, 4)
- WHEN executing `SELECT a, b FROM t1 UNION ALL BY NAME SELECT b, a FROM t2`
- THEN the result contains columns [a, b] with rows (1, 2) and (4, 3)

#### Scenario: Partially overlapping columns with NULL padding

- GIVEN `SELECT a, b FROM t1` returns (1, 2) and `SELECT b, c FROM t2` returns (3, 4)
- WHEN executing `SELECT a, b FROM t1 UNION ALL BY NAME SELECT b, c FROM t2`
- THEN the result has columns [a, b, c]
- AND row from t1 is (1, 2, NULL) — c padded with NULL
- AND row from t2 is (NULL, 3, 4) — a padded with NULL

#### Scenario: No overlapping columns

- GIVEN `SELECT a FROM t1` returns (1) and `SELECT b FROM t2` returns (2)
- WHEN executing `SELECT a FROM t1 UNION ALL BY NAME SELECT b FROM t2`
- THEN the result has columns [a, b]
- AND rows are (1, NULL) and (NULL, 2)

#### Scenario: UNION BY NAME with deduplication

- GIVEN `SELECT a, b FROM t1` returns (1, 2) and `SELECT a, b FROM t2` returns (1, 2)
- WHEN executing `SELECT a, b FROM t1 UNION BY NAME SELECT a, b FROM t2`
- THEN the result contains a single row (1, 2) — duplicates removed

#### Scenario: Type promotion for matching columns

- GIVEN `SELECT 1::INTEGER AS x` and `SELECT 1000000000::BIGINT AS x`
- WHEN executing the UNION BY NAME
- THEN column x has type BIGINT (common supertype)

#### Scenario: Case-insensitive column matching

- GIVEN `SELECT A FROM t1` and `SELECT a FROM t2`
- WHEN executing UNION BY NAME
- THEN columns A and a are matched as the same column

### Requirement: UPSERT Execution (INSERT ... ON CONFLICT)

The engine SHALL support INSERT ... ON CONFLICT DO NOTHING and INSERT ... ON CONFLICT DO UPDATE SET with conflict detection against PRIMARY KEY and UNIQUE indexes, EXCLUDED pseudo-table evaluation, and batch-optimized conflict resolution.

#### Scenario: DO NOTHING skips conflicting rows

- GIVEN table "t" with PRIMARY KEY (id) and existing row (1, 'old')
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'new'), (2, 'added') ON CONFLICT (id) DO NOTHING`
- THEN row (1, 'old') remains unchanged
- AND row (2, 'added') is inserted
- AND RowsAffected returns 1

#### Scenario: DO UPDATE updates conflicting rows with EXCLUDED

- GIVEN table "t" with PRIMARY KEY (id) and existing row (1, 'old')
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'new') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`
- THEN row (1, 'old') is updated to (1, 'new')
- AND RowsAffected returns 1

#### Scenario: DO UPDATE with WHERE filter on update action

- GIVEN table "t" with PRIMARY KEY (id) and existing rows (1, 10), (2, 20)
- WHEN executing `INSERT INTO t (id, val) VALUES (1, 5), (2, 30) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE EXCLUDED.val > t.val`
- THEN row (1, 10) remains unchanged (EXCLUDED.val=5 is NOT > t.val=10)
- AND row (2, 20) is updated to (2, 30) (EXCLUDED.val=30 > t.val=20)
- AND RowsAffected returns 1

#### Scenario: DO NOTHING with no conflicts inserts all rows

- GIVEN table "t" with PRIMARY KEY (id) and no existing rows
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b') ON CONFLICT (id) DO NOTHING`
- THEN both rows are inserted
- AND RowsAffected returns 2

#### Scenario: Conflict detection on UNIQUE index (non-PK)

- GIVEN table "t" with columns (id INTEGER, email VARCHAR) and UNIQUE INDEX on (email)
- AND existing row (1, 'alice@test.com')
- WHEN executing `INSERT INTO t VALUES (2, 'alice@test.com') ON CONFLICT (email) DO NOTHING`
- THEN the insert is skipped
- AND RowsAffected returns 0

#### Scenario: Conflict detection on composite key

- GIVEN table "t" with PRIMARY KEY (a, b) and existing row (1, 2, 'old')
- WHEN executing `INSERT INTO t (a, b, c) VALUES (1, 2, 'new') ON CONFLICT (a, b) DO UPDATE SET c = EXCLUDED.c`
- THEN row (1, 2, 'old') is updated to (1, 2, 'new')
- AND RowsAffected returns 1

#### Scenario: UPSERT with RETURNING clause

- GIVEN table "t" with PRIMARY KEY (id) and existing row (1, 'old')
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'new'), (2, 'fresh') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id, name`
- THEN the result set contains rows (1, 'new') and (2, 'fresh')
- AND the updated row appears in RETURNING output

#### Scenario: INSERT ... SELECT ... ON CONFLICT

- GIVEN table "target" with PRIMARY KEY (id) and existing row (1, 'old')
- AND table "source" with rows (1, 'updated'), (3, 'new')
- WHEN executing `INSERT INTO target SELECT * FROM source ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`
- THEN target contains (1, 'updated') and (3, 'new')
- AND the original row (1, 'old') is replaced

#### Scenario: DO NOTHING without explicit conflict columns infers PK

- GIVEN table "t" with PRIMARY KEY (id) and existing row (1, 'old')
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'new') ON CONFLICT DO NOTHING`
- THEN the insert is skipped (conflict detected on inferred PK column "id")
- AND RowsAffected returns 0

#### Scenario: Error when no unique constraint matches conflict columns

- GIVEN table "t" with PRIMARY KEY (id) and no unique index on (name)
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (name) DO NOTHING`
- THEN an error is returned indicating no unique constraint covers column "name"

#### Scenario: WAL logging for upsert operations

- GIVEN table "t" with PRIMARY KEY (id)
- WHEN executing an upsert that inserts some rows and updates others
- THEN inserted rows are logged as INSERT WAL entries
- AND updated rows are logged as UPDATE WAL entries
- AND skipped rows (DO NOTHING) produce no WAL entries

#### Scenario: Bulk upsert performance

- GIVEN table "t" with PRIMARY KEY (id) and 1000 existing rows
- WHEN executing an INSERT with 10000 rows and ON CONFLICT DO UPDATE
- THEN conflict detection uses batch-optimized key lookup
- AND the operation completes without per-row table scans for non-conflicting rows

### Requirement: EXCLUDED Pseudo-Table

The engine SHALL provide an EXCLUDED pseudo-table scope within ON CONFLICT DO UPDATE expressions that references the column values from the row that caused the conflict.

#### Scenario: EXCLUDED references insert values in SET clause

- GIVEN an INSERT that conflicts on row (1, 'old', 100)
- AND the INSERT attempted values (1, 'new', 200)
- WHEN the DO UPDATE SET clause references `EXCLUDED.name`
- THEN `EXCLUDED.name` evaluates to 'new' (the attempted insert value)

#### Scenario: EXCLUDED in WHERE clause of DO UPDATE

- WHEN the DO UPDATE WHERE clause references `EXCLUDED.val > t.val`
- THEN `EXCLUDED.val` evaluates to the attempted insert value for column "val"
- AND `t.val` evaluates to the existing row's value for column "val"

#### Scenario: EXCLUDED with expression combining existing and new values

- GIVEN an INSERT that conflicts with existing row (1, 100)
- AND attempted insert values (1, 50)
- WHEN executing `ON CONFLICT (id) DO UPDATE SET val = t.val + EXCLUDED.val`
- THEN the updated row has val = 150 (existing 100 + attempted 50)

#### Scenario: EXCLUDED is not accessible outside ON CONFLICT

- WHEN a SELECT statement references `EXCLUDED.col`
- THEN a binding error is returned indicating EXCLUDED is only valid in ON CONFLICT DO UPDATE

#### Scenario: NULL values in UNIQUE conflict columns do not trigger conflicts

- GIVEN table "t" with UNIQUE INDEX on (email) and existing row (1, NULL)
- WHEN executing `INSERT INTO t (id, email) VALUES (2, NULL) ON CONFLICT (email) DO NOTHING`
- THEN the insert succeeds (NULL != NULL per SQL standard)
- AND RowsAffected returns 1

#### Scenario: DO UPDATE preserves non-updated column values

- GIVEN table "t" with PRIMARY KEY (id) and columns (id, name, score) and existing row (1, 'alice', 100)
- WHEN executing `INSERT INTO t (id, name, score) VALUES (1, 'bob', 200) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`
- THEN row becomes (1, 'bob', 100) — score is NOT updated and retains its existing value
- AND RowsAffected returns 1

#### Scenario: DO NOTHING with RETURNING returns empty for skipped rows

- GIVEN table "t" with PRIMARY KEY (id) and existing row (1, 'old')
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'new') ON CONFLICT (id) DO NOTHING RETURNING id, name`
- THEN the result set is empty (skipped rows produce no RETURNING output)

#### Scenario: Error on partial composite key conflict target

- GIVEN table "t" with PRIMARY KEY (a, b)
- WHEN executing `INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO NOTHING`
- THEN an error is returned indicating conflict target must include all columns of the constraint

#### Scenario: Error when DO UPDATE SET modifies conflict target column

- GIVEN table "t" with PRIMARY KEY (id)
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id`
- THEN a binding error is returned indicating conflict target columns cannot be modified in DO UPDATE SET
