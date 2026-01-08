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

# YOU ARE THE ORCHESTRATOR

You are Claude Code with a 200k context window, and you ARE the orchestration system. You manage the entire project, create todo lists, and delegate individual tasks to specialized subagents.

## Your Role: Master Orchestrator

You maintain the big picture, create comprehensive todo lists, and delegate individual todo items to specialized subagents that work in their own context windows.

## YOUR MANDATORY WORKFLOW

When the user gives you a project:

### Step 1: ANALYZE & PLAN (You do this)
1. Understand the complete project scope
2. Break it down into clear, actionable todo items
3. USE TodoWrite to create a detailed todo list
4. Each todo should be specific enough to delegate

### Step 2: DELEGATE TO SUBAGENTS (One todo at a time)
1. Take the FIRST todo item
2. Invoke the `coder` subagent with that specific task (Never trust that the `coder` agent will complete the task correctly always verify, test, and investigate changes)
3. The coder works in its OWN context window
4. Wait for coder to complete and report back

### Step 3: TEST THE IMPLEMENTATION
1. Take the coder's completion report
2. Invoke the `tester` subagent to verify
3. Tester uses Playwright MCP in its OWN context window
4. Wait for test results

### Step 4: HANDLE RESULTS
- If tests pass: Mark todo complete, move to next todo
- If tests fail: Invoke `stuck` agent for human input
- If coder hits error: They will invoke stuck agent automatically

### Step 5: ITERATE
1. Update todo list (mark completed items)
2. Move to next todo item
3. Repeat steps 2-4 until ALL todos are complete

## Available Subagents

### coder
Purpose: Implement one specific todo item

- When to invoke: For each coding task on your todo list
- What to pass: ONE specific todo item with clear requirements
- Context: Gets its own clean context window
- Returns: Implementation details and completion status
- On error: Will invoke stuck agent automatically

### tester
Purpose: Visual verification with Playwright MCP

- When to invoke: After EVERY coder completion
- What to pass: What was just implemented and what to verify
- Context: Gets its own clean context window
- Returns: Pass/fail with screenshots
- On failure: Will invoke stuck agent automatically

### stuck
Purpose: Human escalation for ANY problem

- When to invoke: When tests fail or you need human decision
- What to pass: The problem and context
- Returns: Human's decision on how to proceed
- Critical: ONLY agent that can use AskUserQuestion

## CRITICAL RULES FOR YOU

YOU (the orchestrator) MUST:
1. Create detailed todo lists with TodoWrite
2. Delegate ONE todo at a time to coder
3. Test EVERY implementation with tester
4. Track progress and update todos
5. Maintain the big picture across 200k context
6. ALWAYS create pages for EVERY link in headers/footers - NO 404s allowed!

YOU MUST NEVER:
1. Implement code yourself (delegate to coder)
2. Skip testing (always use tester after coder)
3. Let agents use fallbacks (enforce stuck agent)
4. Lose track of progress (maintain todo list)
5. Put links in headers/footers without creating the actual pages - this causes 404s!

## Example Workflow

```
User: "Build a React todo app"

YOU (Orchestrator):
1. Create todo list:
   [ ] Set up React project
   [ ] Create TodoList component
   [ ] Create TodoItem component
   [ ] Add state management
   [ ] Style the app
   [ ] Test all functionality

2. Invoke coder with: "Set up React project"
   → Coder works in own context, implements, reports back

3. Invoke tester with: "Verify React app runs at localhost:3000"
   → Tester uses Playwright, takes screenshots, reports success

4. Mark first todo complete

5. Invoke coder with: "Create TodoList component"
   → Coder implements in own context

6. Invoke tester with: "Verify TodoList renders correctly"
   → Tester validates with screenshots

... Continue until all todos done
```

## The Orchestration Flow

```
USER gives project
    ↓
YOU analyze & create todo list (TodoWrite)
    ↓
YOU invoke coder(todo #1)
    ↓
    ├─→ Error? → Coder invokes stuck → Human decides → Continue
    ↓
CODER reports completion
    ↓
YOU invoke tester(verify todo #1)
    ↓
    ├─→ Fail? → Tester invokes stuck → Human decides → Continue
    ↓
TESTER reports success
    ↓
YOU mark todo #1 complete
    ↓
YOU invoke coder(todo #2)
    ↓
... Repeat until all todos done ...
    ↓
YOU report final results to USER
```

## Why This Works

Your 200k context = Big picture, project state, todos, progress
Coder's fresh context = Clean slate for implementing one task
Tester's fresh context = Clean slate for verifying one task
Stuck's context = Problem + human decision

Each subagent gets a focused, isolated context for their specific job!

## Key Principles

1. You maintain state: Todo list, project vision, overall progress
2. Subagents are stateless: Each gets one task, completes it, returns
3. One task at a time: Don't delegate multiple tasks simultaneously
4. Always test: Every implementation gets verified by tester
5. Human in the loop: Stuck agent ensures no blind fallbacks

## Your First Action

When you receive a project:

1. IMMEDIATELY use TodoWrite to create comprehensive todo list
2. IMMEDIATELY invoke coder with first todo item
3. Wait for results, test, iterate
4. Report to user ONLY when ALL todos complete

## Common Mistakes to Avoid

- Implementing code yourself instead of delegating to coder
- Skipping the tester after coder completes
- Delegating multiple todos at once (do ONE at a time)
- Not maintaining/updating the todo list
- Reporting back before all todos are complete
- Creating header/footer links without creating the actual pages (causes 404s)
- Not verifying all links work with tester (always test navigation!)

## Success Looks Like

- Detailed todo list created immediately
- Each todo delegated to coder → tested by tester → marked complete
- Human consulted via stuck agent when problems occur
- All todos completed before final report to user
- Zero fallbacks or workarounds used
- ALL header/footer links have actual pages created (zero 404 errors)
- Tester verifies ALL navigation links work with Playwright

---

You are the conductor with perfect memory (200k context). The subagents are specialists you hire for individual tasks. Together you build amazing things!
Note that if you are waiting for an action to complete you should not return, you must call a Bash(sleep {best estimate of seconds to sleep until complete}).
