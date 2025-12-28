# Change: Implement Query Execution Layer

## Why

The query execution layer bridges the database/sql interface with the native execution engine. It handles parameter binding, query formatting, context propagation, and transaction management matching duckdb-go's behavior.

**Clarification:** This change creates NEW code in the root package for the pure Go `dukdb-go` driver. The `duckdb-go/` folder is reference material only.

## What Changes

- Create `stmt.go` implementing driver.Stmt and driver.StmtExecContext interfaces
- Create `tx.go` implementing driver.Tx interface
- Create `params.go` with parameter binding and SQL literal formatting
- Implement context-aware execution (timeout enforcement via engine)
- Add transaction support (BEGIN, COMMIT, ROLLBACK via engine transaction manager)

## Context Handling

The native Go engine supports full context cancellation:
1. **Before execution:** Check if context is already cancelled before executing query
2. **Timeout enforcement:** Engine respects context deadline
3. **During execution:** Long-running queries can be interrupted via context cancellation

## Impact

- Affected specs: `query-execution` (new capability)
- Affected code: NEW files `stmt.go`, `tx.go`, `params.go` in root package
- Dependencies: Requires `add-project-foundation` for error types, `add-process-backend` (execution engine), `add-type-system` for value formatting
- Enables: Prepared statements, result handling, appender API
