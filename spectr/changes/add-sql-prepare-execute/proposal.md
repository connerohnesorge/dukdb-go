# Change: Add SQL-Level PREPARE/EXECUTE/DEALLOCATE Statements

## Why

DuckDB v1.4.3 supports SQL-level prepared statements via
`PREPARE name AS ...`, `EXECUTE name(params)`, and `DEALLOCATE name`. While
dukdb-go already supports parameterized queries through the `database/sql`
driver interface (`?` and `$1` placeholders), the SQL-level PREPARE/EXECUTE
is missing. This blocks compatibility with SQL scripts, tools, and ORMs that
use explicit PREPARE/EXECUTE syntax, and prevents plan caching across multiple
query invocations within a single connection.

## What Changes

- **Parser**: New `PrepareStmt`, `ExecuteStmt`, and `DeallocateStmt` AST nodes
- **Engine/Connection**: `preparedStatements` map on `EngineConn` to store
  named prepared statements (parsed AST + bound statement + planned plan)
- **Executor**: PREPARE stores the plan without executing; EXECUTE substitutes
  parameters and executes the cached plan; DEALLOCATE removes the entry
- Reuses existing parameter infrastructure (`$1`, `$2` placeholders,
  parameter type inference, `CollectParameters`)

## Impact

- Affected specs: `parser`, `execution-engine`
- Affected code:
  - `internal/parser/ast.go` — new AST nodes
  - `internal/parser/parser.go` — PREPARE/EXECUTE/DEALLOCATE grammar
  - `internal/engine/conn.go` — prepared statement storage on connection
  - `internal/executor/operator.go` — execute prepared statement handler
  - `internal/planner/physical.go` — new physical plan nodes
