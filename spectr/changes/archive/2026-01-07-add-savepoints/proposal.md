# Change: Add Savepoint Support (GAP-007)

## Why

The current transaction system in dukdb-go only supports full transaction COMMIT or ROLLBACK. Users cannot perform partial rollbacks within a transaction, which limits the ability to:

1. **Implement retry logic**: When a portion of a transaction fails, the entire transaction must be rolled back instead of just the failed portion
2. **Create transactional checkpoints**: Applications cannot mark stable points within complex multi-statement transactions
3. **Match DuckDB behavior**: DuckDB supports SAVEPOINT, ROLLBACK TO SAVEPOINT, and RELEASE SAVEPOINT commands

**Current Transaction System**:
- `TransactionManager` tracks active transactions in a map
- `Transaction` struct has `id`, `active`, and undo log (`operations`)
- Rollback replays undo log in reverse order
- No mechanism for partial rollback or savepoint tracking

**DuckDB Savepoint Behavior**:
- SAVEPOINT creates a named marker within a transaction
- ROLLBACK TO SAVEPOINT reverts to that marker without ending the transaction
- RELEASE SAVEPOINT removes the savepoint (making its changes permanent within the transaction)
- Savepoints can be nested; inner savepoints are released when outer ones are released

## What Changes

### New SQL Commands

1. **SAVEPOINT name** - Create a named savepoint at the current transaction state
2. **ROLLBACK TO SAVEPOINT name** - Rollback to a previously created savepoint
3. **RELEASE SAVEPOINT name** - Release (remove) a savepoint

### Internal Changes

- Add `Savepoint` struct to track savepoint state
- Extend `Transaction` with savepoint stack
- Add WAL entry types for savepoint operations
- Extend parser for savepoint SQL syntax
- Add binder/planner/executor support

## Impact

### Affected Specs

- `specs/persistence/spec.md` - Add savepoint WAL entries
- NEW: `specs/savepoints/spec.md` - Savepoint requirements

### Affected Code

| File | Change Type | Description |
|------|-------------|-------------|
| `internal/engine/engine.go` | MODIFIED | Add savepoint support to Transaction and TransactionManager |
| `internal/engine/savepoint.go` | ADDED | Savepoint struct and stack management |
| `internal/wal/entry.go` | MODIFIED | Add WAL_SAVEPOINT and WAL_RELEASE_SAVEPOINT entry types |
| `internal/parser/ast.go` | MODIFIED | Add SavepointStmt, RollbackToSavepointStmt, ReleaseSavepointStmt |
| `internal/parser/parser_txn.go` | ADDED | Parse savepoint statements |
| `internal/binder/bind_txn.go` | MODIFIED | Add savepoint binding |
| `internal/planner/logical.go` | MODIFIED | Add LogicalSavepoint nodes |
| `internal/planner/physical.go` | MODIFIED | Add PhysicalSavepoint operators |
| `internal/executor/savepoint.go` | ADDED | Savepoint execution operators |

### Dependencies

- Depends on: WAL system (implemented), Transaction system (implemented)
- Blocks: None

### Compatibility

- Full syntax compatibility with DuckDB savepoint commands
- Behavior matches DuckDB for nested savepoints
