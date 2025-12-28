## Context

The dukdb-go project needs a mechanism to execute DuckDB operations without CGO. Since DuckDB lacks a wire protocol, we use subprocess communication with the DuckDB CLI binary.

The DuckDB CLI supports:
- Interactive mode with prompts
- Non-interactive mode reading from stdin
- JSON output mode (`-json` flag)
- CSV output mode
- Multiple output formats

## Goals / Non-Goals

**Goals:**
- Reliable process lifecycle management
- Efficient stdin/stdout communication
- Thread-safe concurrent query support
- Proper error handling and recovery
- Configurable CLI binary location

**Non-Goals:**
- Matching embedded performance (subprocess overhead is expected)
- Supporting all DuckDB CLI features (focus on SQL execution)
- Hot-reloading the CLI binary

## Decisions

### Decision 1: JSON Output Mode

**What:** Use DuckDB CLI's JSON output mode (`-json` flag) for result parsing.

**Why:** JSON provides structured, unambiguous output that's easy to parse in Go. Avoids complex CSV escaping issues and supports nested types naturally.

**Alternatives considered:**
- CSV output: Simpler but problematic with nested types and escaping
- Custom format: Would require DuckDB modifications

### Decision 2: Process Per Database

**What:** Spawn one DuckDB process per database file (or in-memory instance).

**Why:** DuckDB's in-process nature means each process owns its database file. Multiple connections share the same process.

**Alternatives considered:**
- Process per connection: Wasteful and prevents transaction coordination
- Single global process: Cannot handle multiple databases

### Decision 3: Communication Protocol

**What:** Use newline-delimited commands with unique markers for result boundaries.

**Why:** The CLI reads SQL statements and outputs results. We use unique UUIDs as markers to detect result boundaries.

**Protocol:**
```
--> SELECT 'START:uuid' as marker;
--> <actual query>
--> SELECT 'END:uuid' as marker;
<-- [result with START marker]
<-- [actual results]
<-- [result with END marker]
```

### Decision 4: Internal Package

**What:** Place implementation in `internal/process/` package.

**Why:** Implementation details shouldn't be part of the public API. Users interact only through the `Backend` interface.

## Risks / Trade-offs

- **Risk:** CLI binary not available
  - Mitigation: Clear error messages; document installation; consider bundling option

- **Risk:** Process crashes
  - Mitigation: Automatic restart with state recovery; connection invalidation

- **Trade-off:** Higher latency than embedded
  - Mitigation: Connection pooling; batch queries where possible

- **Trade-off:** Requires external binary
  - Mitigation: Document clearly; future WASM backend could eliminate this

## Open Questions

1. Should we support bundling the DuckDB binary within the Go binary?
2. What's the minimum DuckDB CLI version we should support?
3. How to handle long-running queries and cancellation?
