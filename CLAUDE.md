# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

dukdb-go is a pure Go implementation of a DuckDB database driver. It maintains API compatibility with the original duckdb-go driver while requiring zero cgo dependencies. This enables cross-platform compilation without C toolchains and allows use in pure Go environments (TinyGo, WebAssembly, etc.).

## Development Commands

```bash
# Enter development shell (required for all commands)
nix develop

# Run tests
nix develop -c tests
# Or directly: gotestsum --format short-verbose ./...

# Run linting
nix develop -c lint
# Or directly: golangci-lint run

# Format code
nix fmt
# Includes: alejandra (Nix), gofmt, golines, goimports

# Run single test
go test -run TestName ./...

# Run tests for specific package
go test -v ./internal/io/csv/...

# Run tests with coverage
make check-coverage
```

## Architecture

### Pluggable Backend Architecture

The driver uses a pluggable backend design:

```
database/sql API
      │
      ▼
   Driver (driver.go) ──► registers with sql.Register("dukdb")
      │
      ▼
   Connector (connector.go) ──► manages connection lifecycle
      │
      ▼
   Conn (conn.go) ──► implements driver.Conn interfaces
      │
      ▼
   Backend interface (backend.go) ──► abstraction layer
      │
      ▼
   Engine (internal/engine/) ──► actual implementation
```

### Key Components

- **Root package (`dukdb`)**: Public API, driver registration, connection types
- **`internal/engine/`**: Core execution engine implementing Backend interface
- **`internal/catalog/`**: Schema and metadata management (tables, views, indexes, sequences, schemas)
- **`internal/storage/`**: Data persistence layer (DataChunk, Vector, ValidityMask, indexes)
- **`internal/parser/`**: SQL parsing and AST definitions
- **`internal/planner/`**: Query planning (logical → physical)
- **`internal/executor/`**: Query execution and table functions
- **`internal/binder/`**: SQL binding/resolution
- **`internal/io/`**: File format I/O (CSV, JSON, Parquet)

### File Format Support

**CSV**: `internal/io/csv/`
- `read_csv(path, delimiter, header, nullstr, ...)` - Read CSV with options
- `read_csv_auto(path)` - Auto-detect CSV format
- CSV writer with configurable options

**JSON**: `internal/io/json/`
- `read_json(path, format, ...)` - Read JSON array or NDJSON
- `read_json_auto(path)` - Auto-detect JSON format
- `read_ndjson(path)` - Read newline-delimited JSON
- JSON/NDJSON writer

**Parquet**: `internal/io/parquet/`
- `read_parquet(path)` - Read Parquet files with column projection
- Parquet writer with compression support (SNAPPY, GZIP, ZSTD, LZ4, BROTLI)
- Uses `github.com/parquet-go/parquet-go` (pure Go, no CGO)

**COPY Statement**:
- `COPY table FROM 'path' (OPTIONS)` - Import data
- `COPY table TO 'path' (OPTIONS)` - Export data
- `COPY (SELECT ...) TO 'path' (OPTIONS)` - Export query results

### DDL Support

**Views**: `internal/catalog/view.go`
- `CREATE VIEW name AS SELECT ...` - Create named query definition
- `CREATE OR REPLACE VIEW name AS SELECT ...` - Create or replace view
- `DROP VIEW name` - Drop view
- Views expand at query time (not materialized)

**Indexes**: `internal/catalog/index.go`, `internal/storage/index.go`, `internal/optimizer/index_matcher.go`
- `CREATE INDEX name ON table(columns)` - Create hash index
- `CREATE UNIQUE INDEX name ON table(columns)` - Create unique index
- `DROP INDEX name` - Drop index
- Currently supports hash indexes (ART indexes as future work)

**Index Usage in Query Plans**: `internal/optimizer/`, `internal/executor/index_scan.go`

The optimizer automatically uses indexes for equality predicates when cost-effective:

| Query Pattern | Index Usage |
|--------------|-------------|
| `WHERE col = value` | Uses index if exists on col |
| `WHERE a = 1 AND b = 2` | Uses composite index on (a, b) |
| `WHERE a = 1 AND c = 3` | Uses index on (a) with residual filter for c |
| `WHERE b = 2` (index on a, b) | Cannot use index (not prefix) |
| `WHERE col IN (1, 2, 3)` | Uses index with multiple lookups |

Cost-based selection:
- Index scan chosen when selectivity < ~10% (configurable)
- Sequential scan preferred for high-selectivity queries
- Cost model considers: IndexLookupCost, IndexTupleCost, RandomPageCost

Index-only scan (covering index):
- When index contains all required columns, can skip heap access
- Detected via `IsCoveringIndex()` function
- Note: Current HashIndex stores RowIDs only, true index-only optimization is future work

Key files:
- `internal/optimizer/index_matcher.go` - Index matching and selection
- `internal/optimizer/cost_model.go` - Cost estimation (EstimateIndexScanCost)
- `internal/executor/index_scan.go` - PhysicalIndexScanOperator
- `internal/planner/physical.go` - PhysicalIndexScan plan node

**Sequences**: `internal/catalog/sequence.go`
- `CREATE SEQUENCE name [START WITH n] [INCREMENT BY n] [CYCLE]` - Create sequence
- `DROP SEQUENCE name` - Drop sequence
- `NEXTVAL('sequence_name')` - Get next value from sequence
- `CURRVAL('sequence_name')` - Get current value without incrementing
- Supports START WITH, INCREMENT BY, MIN VALUE, MAX VALUE, and CYCLE options

**Schemas**: `internal/catalog/schema.go`
- `CREATE SCHEMA name` - Create namespace for organizing objects
- `DROP SCHEMA name` - Drop schema
- Cross-schema table/view resolution with qualified names (schema.table)

**ALTER TABLE**: `internal/executor/ddl.go`
- `ALTER TABLE table RENAME TO new_name` - Rename table
- `ALTER TABLE table RENAME COLUMN old TO new` - Rename column
- `ALTER TABLE table DROP COLUMN name` - Drop column
- `ALTER TABLE table ADD COLUMN name type` - Add column

### Transaction Support

**Isolation Levels**: `internal/engine/engine.go`, `internal/storage/mvcc.go`

Syntax:
- `BEGIN TRANSACTION ISOLATION LEVEL <level>` - Start transaction with specific isolation
- `SET default_transaction_isolation = '<level>'` - Set default for new transactions
- `SET transaction_isolation = '<level>'` - Synonym for default setting
- `SHOW transaction_isolation` - Show current transaction's isolation level
- `SHOW default_transaction_isolation` - Show connection's default level

Supported levels (from least to most restrictive):

| Level | Dirty Reads | Non-repeatable Reads | Phantom Reads | Conflict Detection |
|-------|-------------|---------------------|---------------|-------------------|
| READ UNCOMMITTED | Allowed | Allowed | Allowed | No |
| READ COMMITTED | Prevented | Allowed | Allowed | No |
| REPEATABLE READ | Prevented | Prevented | Allowed | No |
| SERIALIZABLE | Prevented | Prevented | Prevented | Yes |

Behavior details:
- **READ UNCOMMITTED**: Sees uncommitted changes from other transactions (dirty reads)
- **READ COMMITTED**: Each statement sees only data committed before that statement began
- **REPEATABLE READ**: Snapshot taken at transaction start; sees consistent view throughout
- **SERIALIZABLE**: Same as REPEATABLE READ plus conflict detection at commit time

SERIALIZABLE conflict detection (`internal/storage/conflict_detector.go`, `internal/storage/lock_manager.go`):
- Write-write conflicts: Two transactions cannot both modify the same row
- Read-write conflicts: If transaction A reads a row that concurrent transaction B modifies, A fails at commit
- On conflict: Returns `ErrSerializationFailure` - application should retry the transaction

Example - handling serialization failures:
```go
// Retry loop for SERIALIZABLE transactions
for retries := 0; retries < 3; retries++ {
    _, err := db.Exec("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
    if err != nil { return err }

    // ... perform transaction operations ...

    _, err = db.Exec("COMMIT")
    if err == nil {
        break // Success
    }
    if strings.Contains(err.Error(), "could not serialize") {
        db.Exec("ROLLBACK")
        continue // Retry
    }
    return err // Other error
}
```

Default isolation level is SERIALIZABLE (matching DuckDB/PostgreSQL behavior).

**Savepoints**: `internal/engine/savepoint.go`, `internal/engine/conn.go`
- `SAVEPOINT name` - Create named checkpoint within transaction
- `ROLLBACK TO SAVEPOINT name` - Rollback to checkpoint (also: `ROLLBACK TO name`)
- `RELEASE SAVEPOINT name` - Remove checkpoint (also: `RELEASE name`)
- Nested savepoints follow PostgreSQL semantics
- WAL integration for crash recovery (entry types 92, 93, 94)

### Query Execution Flow

```
SQL String
    ↓
Parser (internal/parser/) → AST
    ↓
Binder (internal/binder/) → Bound AST with resolved references
    ↓
Planner (internal/planner/) → Logical Plan → Physical Plan
    ↓
Executor (internal/executor/) → Results as DataChunks
```

### Data Representation

- **DataChunk** (`internal/storage/chunk.go`): Columnar batch of rows (default 2048 rows)
- **Vector** (`internal/storage/column.go`): Single column with typed data and ValidityMask for NULLs
- **SelectionVector**: Sparse row selection without copying

### Reference Implementation

The `duckdb/` and `duckdb-go/` directories contain the original DuckDB C++ source and cgo driver for reference. These are used to understand behavior and validate API compatibility.

## Key Constraints

1. **NO CGO**: Zero C dependencies or cgo usage allowed
2. **API Compatibility**: Must match duckdb-go's public API for drop-in replacement
3. **Pure Go**: Must work in TinyGo, WASM, and other pure Go environments

## Spectr Change Proposals

This project uses spectr for managing significant changes. When making proposals or architectural changes:

1. Read `spectr/AGENTS.md` for the change proposal process
2. Active proposals are in `spectr/changes/`
3. Completed proposals are archived in `spectr/changes/archive/`
4. Specifications live in `spectr/specs/`

<!-- spectr:start -->
# Spectr Instructions

These instructions are for AI assistants working in this project.

Always open `@/spectr/AGENTS.md` when the request:

- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big
  performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/spectr/AGENTS.md` to learn:

- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

When delegating tasks from a change proposal to subagents:

- Provide the proposal path: `spectr/changes/<id>/proposal.md`
- Include task context: `spectr/changes/<id>/tasks.jsonc`
- Reference delta specs: `spectr/changes/<id>/specs/<capability>/spec.md`

<!-- spectr:end -->

## Linting Configuration

The project uses golangci-lint (`.golangci.yml`):
- Enabled: govet, errcheck, staticcheck, unused
- Pre-existing exhaustive switch warnings can be ignored

## Testing Notes

- Uses testify for assertions (`github.com/stretchr/testify`)
- Integration tests run against the actual engine implementation
- Reference implementation in `duckdb-go/` can be used to validate expected behavior
