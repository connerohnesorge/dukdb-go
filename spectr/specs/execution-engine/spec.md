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
