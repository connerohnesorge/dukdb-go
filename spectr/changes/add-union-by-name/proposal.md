# Change: Add UNION BY NAME / UNION ALL BY NAME Support

## Why

DuckDB v1.4.3 supports `UNION [ALL] BY NAME` which combines result sets by
matching column names rather than positions. This is essential for combining
queries with different column orders or partial column overlap. Standard UNION
requires identical column counts and types by position, while UNION BY NAME
aligns columns by name, filling missing columns with NULL.

## What Changes

- **Parser**: Add `SetOpUnionByName` and `SetOpUnionAllByName` to `SetOpType`
- **Binder**: Implement column matching by name with NULL padding for
  missing columns
- **Executor**: Reorder/pad columns in set operation execution

## Impact

- Affected specs: `parser`, `execution-engine`
- Affected code:
  - `internal/parser/ast.go` — new SetOpType values
  - `internal/parser/parser.go` — parse `BY NAME` after UNION [ALL]
  - `internal/binder/bind_stmt.go` — column name matching logic
  - `internal/executor/operator.go` — column reordering in set operations
  - `internal/planner/` — set operation planning
