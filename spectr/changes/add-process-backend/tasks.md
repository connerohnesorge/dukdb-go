## 1. Core Engine Structure

- [ ] 1.1 Create `internal/engine/engine.go` with Engine struct:
  - Fields: `catalog *Catalog`, `storage *Storage`, `txnMgr *TransactionManager`
  - Implement Backend interface from project-foundation
  - **Acceptance:** `var _ Backend = (*Engine)(nil)` compiles

- [ ] 1.2 Create `internal/engine/conn.go` with EngineConn struct:
  - Implement BackendConn interface
  - Methods: Execute, Query, Prepare, Close, Ping
  - **Acceptance:** `var _ BackendConn = (*EngineConn)(nil)` compiles

## 2. SQL Parser

- [ ] 2.1 Create `internal/parser/parser.go`:
  - Use github.com/auxten/postgresql-parser for PostgreSQL dialect
  - Wrap parser to return our AST types
  - **Acceptance:** `SELECT 1` parses successfully

- [ ] 2.2 Create `internal/parser/ast.go` with AST node types:
  - Statement: SelectStmt, InsertStmt, UpdateStmt, DeleteStmt, CreateTableStmt, DropTableStmt
  - Expr: ColumnRef, Literal, BinaryExpr, UnaryExpr, FunctionCall
  - **Acceptance:** All node types defined with String() method

- [ ] 2.3 Implement parser for SELECT statements:
  - Support: SELECT, FROM, WHERE, ORDER BY, LIMIT, OFFSET
  - Support: column aliases, table aliases
  - **Acceptance:** Parse `SELECT a, b FROM t WHERE x > 1 ORDER BY a LIMIT 10`

- [ ] 2.4 Implement parser for DML statements:
  - INSERT INTO ... VALUES (...)
  - UPDATE ... SET ... WHERE
  - DELETE FROM ... WHERE
  - **Acceptance:** All three statement types parse correctly

- [ ] 2.5 Implement parser for DDL statements:
  - CREATE TABLE with column definitions and constraints
  - DROP TABLE
  - **Acceptance:** `CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR)` parses

## 3. Catalog

- [ ] 3.1 Create `internal/catalog/catalog.go`:
  - Thread-safe schema/table registry
  - Methods: CreateSchema, DropSchema, GetSchema
  - Default "main" schema
  - **Acceptance:** Create and retrieve schemas

- [ ] 3.2 Create `internal/catalog/table.go`:
  - TableDef with columns, constraints, indexes
  - Methods: CreateTable, DropTable, GetTable
  - **Acceptance:** Create and retrieve table definitions

- [ ] 3.3 Create `internal/catalog/column.go`:
  - ColumnDef with name, type, nullable, default
  - Support all DuckDB types from add-type-system
  - **Acceptance:** Column definitions with all type variants

## 4. Storage

- [ ] 4.1 Create `internal/storage/column.go`:
  - Column struct with typed data slices
  - Null bitmap for NULL handling
  - Methods: Append, Get, Length
  - **Acceptance:** Store and retrieve typed column data

- [ ] 4.2 Create `internal/storage/chunk.go`:
  - Chunk as vector of columns (default 2048 rows)
  - Methods: AddRow, GetRow, Size
  - **Acceptance:** Store multiple rows across columns

- [ ] 4.3 Create `internal/storage/table.go`:
  - Table as collection of chunks
  - Methods: Insert, Scan, Delete
  - **Acceptance:** Insert rows, scan all rows

- [ ] 4.4 Create `internal/storage/storage.go`:
  - Storage manager for all tables
  - In-memory storage implementation
  - **Acceptance:** Create/drop tables, access by name

## 5. Binder

- [ ] 5.1 Create `internal/binder/binder.go`:
  - Resolve table/column names against catalog
  - Type checking for expressions
  - **Acceptance:** Bind `SELECT a FROM t` resolves column types

- [ ] 5.2 Implement expression binding:
  - Resolve ColumnRef to table.column with type
  - Check type compatibility for operators
  - Infer result types
  - **Acceptance:** `a + b` binds with correct result type

- [ ] 5.3 Implement aggregate binding:
  - Validate aggregate functions (COUNT, SUM, etc.)
  - Check GROUP BY columns match
  - **Acceptance:** `SELECT COUNT(*) FROM t GROUP BY a` binds correctly

## 6. Planner

- [ ] 6.1 Create `internal/planner/logical.go`:
  - Logical plan node interfaces and types
  - LogicalScan, LogicalFilter, LogicalProject, LogicalJoin, LogicalAggregate
  - **Acceptance:** Build logical plan for SELECT query

- [ ] 6.2 Create `internal/planner/physical.go`:
  - Physical plan node types
  - Convert logical to physical plans
  - **Acceptance:** Logical plan converts to physical plan

- [ ] 6.3 Implement SELECT planning:
  - FROM → Scan
  - WHERE → Filter
  - SELECT list → Project
  - ORDER BY → Sort
  - LIMIT → Limit
  - **Acceptance:** Full SELECT statement plans correctly

- [ ] 6.4 Implement JOIN planning:
  - Hash join for equality conditions
  - Nested loop join for complex conditions
  - **Acceptance:** `SELECT * FROM a JOIN b ON a.id = b.id` plans

## 7. Executor

- [ ] 7.1 Create `internal/executor/operator.go`:
  - Operator interface: Init, Next, Close
  - Base operator implementation
  - **Acceptance:** Interface compiles

- [ ] 7.2 Implement ScanOperator:
  - Iterate table chunks
  - Return chunks via Next()
  - **Acceptance:** Scan returns all rows

- [ ] 7.3 Implement FilterOperator:
  - Evaluate predicate on each row
  - Filter out non-matching rows
  - **Acceptance:** WHERE clause filters correctly

- [ ] 7.4 Implement ProjectOperator:
  - Evaluate SELECT expressions
  - Build output columns
  - **Acceptance:** SELECT a+1 computes correctly

- [ ] 7.5 Implement HashAggregateOperator:
  - Build hash table by GROUP BY keys
  - Compute aggregate values
  - **Acceptance:** COUNT, SUM, AVG work correctly

- [ ] 7.6 Implement SortOperator:
  - Sort chunks by ORDER BY columns
  - Support ASC/DESC, NULLS FIRST/LAST
  - **Acceptance:** Results sorted correctly

- [ ] 7.7 Implement LimitOperator:
  - Track row count
  - Stop after LIMIT reached
  - Skip OFFSET rows
  - **Acceptance:** LIMIT 10 OFFSET 5 works

- [ ] 7.8 Implement HashJoinOperator:
  - Build hash table on smaller relation
  - Probe with larger relation
  - **Acceptance:** INNER JOIN produces correct results

## 8. Expression Evaluation

- [ ] 8.1 Create `internal/executor/expr.go`:
  - Vectorized expression evaluation
  - Evaluate on Chunk, return Column
  - **Acceptance:** Literal expression evaluates

- [ ] 8.2 Implement arithmetic expressions:
  - +, -, *, /, % for numeric types
  - Handle NULL propagation
  - **Acceptance:** `1 + 2` returns 3

- [ ] 8.3 Implement comparison expressions:
  - =, <>, <, >, <=, >= for all comparable types
  - Return boolean column
  - **Acceptance:** `a > 5` filters correctly

- [ ] 8.4 Implement logical expressions:
  - AND, OR, NOT
  - Short-circuit evaluation
  - **Acceptance:** `a > 5 AND b < 10` works

- [ ] 8.5 Implement aggregate functions:
  - COUNT, SUM, AVG, MIN, MAX
  - COUNT(*) vs COUNT(column)
  - **Acceptance:** All aggregates compute correctly

## 9. DML Execution

- [ ] 9.1 Implement INSERT execution:
  - Parse VALUES list
  - Type check against table schema
  - Append to storage
  - **Acceptance:** INSERT adds rows

- [ ] 9.2 Implement UPDATE execution:
  - Scan with WHERE filter
  - Modify matching rows in place
  - **Acceptance:** UPDATE modifies rows

- [ ] 9.3 Implement DELETE execution:
  - Scan with WHERE filter
  - Remove matching rows
  - **Acceptance:** DELETE removes rows

## 10. DDL Execution

- [ ] 10.1 Implement CREATE TABLE:
  - Parse column definitions
  - Add to catalog
  - Create storage table
  - **Acceptance:** Table created and queryable

- [ ] 10.2 Implement DROP TABLE:
  - Remove from catalog
  - Release storage
  - **Acceptance:** Table no longer exists

## 11. Integration

- [ ] 11.1 Wire parser → binder → planner → executor:
  - Engine.Execute() runs full pipeline
  - Engine.Query() returns results as []map[string]any
  - **Acceptance:** `SELECT 1` returns correct result

- [ ] 11.2 Implement connection management:
  - Open/Close connections
  - Transaction boundaries
  - **Acceptance:** Multiple connections work

## 12. Testing

- [ ] 12.1 Unit tests for parser:
  - Test each statement type
  - Test expression parsing
  - **Acceptance:** Parser tests pass

- [ ] 12.2 Unit tests for executor:
  - Test each operator type
  - Test expression evaluation
  - **Acceptance:** Executor tests pass

- [ ] 12.3 Integration tests:
  - End-to-end query tests
  - TPC-H subset queries
  - **Acceptance:** All integration tests pass

- [ ] 12.4 Concurrent query tests:
  - Multiple goroutines querying
  - No race conditions
  - **Acceptance:** `go test -race` passes
