# Change: Add UPSERT (INSERT ... ON CONFLICT) Support

## Why

DuckDB v1.4.3 supports `INSERT ... ON CONFLICT DO NOTHING/UPDATE` (upsert), a
critical DML feature for idempotent data loading and merge-style operations.
dukdb-go currently raises a constraint error on primary key or unique index
violations during INSERT with no way to gracefully handle conflicts. This
blocks users who rely on upsert patterns for ETL pipelines, incremental loads,
and application-level conflict resolution.

## What Changes

- **Parser**: New `OnConflictClause` AST node; extend `InsertStmt` to carry
  optional conflict handling after VALUES/SELECT
- **Binder**: Resolve conflict target columns against unique/primary key
  constraints; introduce `EXCLUDED` pseudo-table scope for referencing
  incoming row values in UPDATE expressions
- **Planner**: New `PhysicalUpsert` plan node wrapping a `PhysicalInsert` with
  conflict-handling metadata
- **Executor**: Two-phase insert: attempt insert, on unique violation either
  skip (DO NOTHING) or apply UPDATE with EXCLUDED bindings; batch-aware
  conflict detection using primary key / unique index lookup
- **WAL**: Log upsert operations as atomic INSERT-or-UPDATE entries

## Impact

- Affected specs: `execution-engine`, `parser`
- Affected code:
  - `internal/parser/ast.go` — new AST nodes
  - `internal/parser/parser.go` — ON CONFLICT grammar
  - `internal/binder/statements.go` — BoundOnConflictClause, EXCLUDED scope
  - `internal/binder/bind_stmt.go` — conflict column resolution
  - `internal/planner/physical.go` — PhysicalUpsert node
  - `internal/executor/operator.go` — executeUpsert function
  - `internal/storage/` — unique constraint lookup helpers
