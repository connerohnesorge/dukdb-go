# Change: Implement Query Execution Layer

## Why

The query execution layer bridges the database/sql interface with the process backend. It handles parameter binding, query formatting, context propagation, and transaction management matching duckdb-go's behavior.

**Clarification:** This change creates NEW code in the root package for the pure Go `dukdb-go` driver. The `duckdb-go/` folder is reference material only.

## What Changes

- Create `stmt.go` implementing driver.Stmt and driver.StmtExecContext interfaces
- Create `tx.go` implementing driver.Tx interface
- Create `params.go` with parameter binding and SQL literal formatting
- Implement context-aware execution (timeout enforcement via process backend)
- Add transaction support (BEGIN, COMMIT, ROLLBACK via CLI commands)

## Context Cancellation Limitation

The subprocess backend cannot cancel a running DuckDB query mid-execution. Context handling is implemented as:
1. **Before execution:** Check if context is already cancelled before sending query
2. **Timeout enforcement:** Process backend has QueryTimeout; context deadline overrides if shorter
3. **After timeout:** Process is killed and restarted (handled by backend)

This matches practical behavior - queries complete or timeout, no mid-query interruption.

## Impact

- Affected specs: `query-execution` (new capability)
- Affected code: NEW files `stmt.go`, `tx.go`, `params.go` in root package
- Dependencies: Requires `add-project-foundation` for error types, `add-process-backend` for execution, `add-type-system` for value formatting
- Enables: Prepared statements, result handling, appender API
