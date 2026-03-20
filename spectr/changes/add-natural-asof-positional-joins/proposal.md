# Change: Add NATURAL, ASOF, and POSITIONAL JOIN Types

## Why

DuckDB v1.4.3 supports several join types beyond the standard INNER/LEFT/RIGHT/FULL/CROSS joins. NATURAL JOIN automatically matches columns by name (standard SQL), ASOF JOIN matches the nearest value for time-series data (DuckDB extension), and POSITIONAL JOIN matches rows by position rather than predicate (DuckDB extension). These are commonly used in analytics workflows and are missing from dukdb-go, blocking full DuckDB v1.4.3 compatibility.

## What Changes

- **Parser**: Add `JoinTypeNatural`, `JoinTypeAsOf`, `JoinTypePositional` to `JoinType` enum; add `USING` clause support to `JoinClause`; parse `NATURAL [LEFT|RIGHT|FULL] JOIN`, `ASOF [LEFT] JOIN ... ON ... [AND ...]`, and `POSITIONAL JOIN`
- **Binder**: Implement NATURAL join column matching (find common columns), ASOF join inequality condition validation, POSITIONAL join binding
- **Planner**: Generate physical plan nodes for each new join type
- **Executor**: Implement NATURAL join (rewrite to explicit equi-join on common columns), ASOF join (sorted merge with inequality), POSITIONAL join (zip-merge by row position)
- BREAKING: None — additive only

## Impact

- Affected specs: `parser`, `execution-engine`
- Affected code:
  - `internal/parser/ast.go` — JoinType enum, JoinClause struct (add Using field)
  - `internal/parser/parser.go` — parse new join syntax
  - `internal/binder/bind_stmt.go` — bind new join types
  - `internal/planner/logical.go` — logical join plan nodes
  - `internal/planner/physical.go` — physical join plan nodes
  - `internal/executor/join.go` or new files — join execution operators
