# Change: Transaction Isolation Levels Support

## Why

Currently, dukdb-go only supports a default isolation level with no configurable transaction isolation:

1. **No Isolation Level Configuration**: Users cannot specify isolation levels when starting transactions. The database uses an implicit default behavior without formal guarantees.

2. **Missing Read Phenomena Control**: Without configurable isolation levels, users cannot prevent or allow specific read phenomena (dirty reads, non-repeatable reads, phantom reads) based on their application requirements.

3. **Incompatibility with DuckDB**: DuckDB supports full ACID transactions with configurable isolation levels. This gap prevents drop-in replacement for applications that rely on specific isolation behaviors.

4. **Limited Concurrent Transaction Support**: While basic transaction infrastructure exists, there is no mechanism to control how concurrent transactions interact through visibility rules.

## What Changes

### Breaking Changes

- None (purely additive functionality)

### New Features

- **Isolation Level Configuration**: Support for specifying isolation level at transaction start:
  - `BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED`
  - `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED`
  - `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ`
  - `BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE`

- **READ UNCOMMITTED**: Allows dirty reads (reading uncommitted data from other transactions)

- **READ COMMITTED**: Prevents dirty reads; each statement sees only committed data as of statement start

- **REPEATABLE READ**: Prevents dirty reads and non-repeatable reads; snapshot taken at transaction start

- **SERIALIZABLE**: Full isolation; prevents all anomalies including phantoms; uses conflict detection

- **Default Isolation Level Setting**: `SET default_transaction_isolation` to configure connection default

- **Current Isolation Query**: `SHOW transaction_isolation` to query current level

### Internal Changes

- **Parser**: Add isolation level clause parsing in BEGIN statement
- **Transaction Manager**: Add isolation level tracking per transaction
- **MVCC Enhancements**: Visibility rules based on isolation level
- **Lock Manager**: Write conflict detection for SERIALIZABLE
- **Snapshot Management**: Transaction-level vs statement-level snapshots

## Impact

### Affected Specs

- `specs/execution-engine/spec.md` - Add isolation level execution requirements
- `specs/persistence/spec.md` - Add MVCC and visibility requirements
- NEW `specs/isolation-levels/spec.md` - Define isolation level behaviors

### Affected Code

| File | Change Type | Description |
|------|-------------|-------------|
| `internal/parser/ast.go` | MODIFIED | Add IsolationLevel to BeginStmt |
| `internal/parser/parser_transaction.go` | MODIFIED | Parse ISOLATION LEVEL clause |
| `internal/engine/transaction.go` | MODIFIED | Add isolation level field to Transaction |
| `internal/engine/txn_manager.go` | MODIFIED | Create transactions with isolation level |
| `internal/storage/mvcc.go` | ADDED | MVCC visibility rules per isolation level |
| `internal/storage/snapshot.go` | ADDED | Snapshot management for isolation |
| `internal/storage/lock_manager.go` | ADDED | Lock management for SERIALIZABLE |
| `internal/executor/executor.go` | MODIFIED | Apply isolation rules during execution |

### Dependencies

- This proposal depends on: (none)
- This proposal blocks: GAP-009 (Snapshot Isolation/MVCC enhancements)

### Compatibility

- SQL syntax compatible with DuckDB/PostgreSQL isolation level specification
- Default behavior matches DuckDB (SERIALIZABLE by default)
- Applications using explicit isolation levels will work as expected
