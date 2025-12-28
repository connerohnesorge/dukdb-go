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
- Reliable process lifecycle management with defined crash detection
- Efficient stdin/stdout communication with JSON parsing
- Thread-safe concurrent query support with request isolation
- Configurable timeouts and retry behavior
- Clear error messages with actionable guidance

**Non-Goals:**
- Matching embedded performance (subprocess overhead is expected and acceptable)
- Supporting all DuckDB CLI features (focus on SQL execution only)
- Hot-reloading the CLI binary

## Decisions

### Decision 1: JSON Output Mode

**What:** Use DuckDB CLI's JSON output mode with flags: `-json -noheader -nullvalue NULL`.

**Why:** JSON provides structured, unambiguous output that's easy to parse in Go. The specific flags ensure:
- `-json`: Output as JSON array of objects
- `-noheader`: Suppress column headers (embedded in JSON keys)
- `-nullvalue NULL`: Consistent null representation

**Alternatives considered:**
- CSV output: Simpler but problematic with nested types and escaping
- Custom format: Would require DuckDB modifications

### Decision 2: Process Per Database

**What:** Spawn one DuckDB CLI process per database file (or in-memory instance). Each process handles multiple connections sharing the same database.

**Why:** DuckDB's in-process nature means each process owns its database file exclusively. Multiple Go connections share the same CLI process for the same database path.

**Connection isolation:** Each logical connection is assigned a unique connection ID. The backend tracks which queries belong to which connection using this ID for proper result routing.

### Decision 3: Communication Protocol

**What:** Use newline-delimited SQL commands with UUID markers for result boundaries.

**Protocol:** Three separate SQL statements are sent to stdin, each terminated by newline:
```sql
SELECT '__DUKDB_START_a1b2c3d4-e5f6-7890-abcd-ef1234567890__' AS __marker__;
<actual query>;
SELECT '__DUKDB_END_a1b2c3d4-e5f6-7890-abcd-ef1234567890__' AS __marker__;
```

**Important:** These are three separate statements, not one wrapped statement. The markers are executed independently before and after the user query.

**Serialization:** Only one query cycle (start marker + query + end marker) can be in-flight at a time. A mutex protects the entire write-read cycle:
1. Lock mutex
2. Write start marker query
3. Write user query
4. Write end marker query
5. Read and discard output until start marker JSON row appears
6. Collect all JSON rows until end marker JSON row appears
7. Return collected rows (excluding marker rows)
8. Unlock mutex

**JSON output format:** Each row is a complete JSON line:
```json
{"__marker__":"__DUKDB_START_a1b2c3d4-e5f6-7890-abcd-ef1234567890__"}
{"id":1,"name":"Alice"}
{"id":2,"name":"Bob"}
{"__marker__":"__DUKDB_END_a1b2c3d4-e5f6-7890-abcd-ef1234567890__"}
```

**Marker format:**
- Start: `__DUKDB_START_<uuid-v4>__`
- End: `__DUKDB_END_<uuid-v4>__`
- UUID v4 provides 122 bits of randomness

**Collision prevention:** Markers use UUID v4 with 2^122 possible values. The `__marker__` column name is reserved - user queries cannot use this column name for their own data. If a user query happens to produce a `__marker__` column, it will be overwritten by the marker SELECT, but this is an edge case that doesn't affect correctness since marker rows are filtered out.

### Decision 4: Internal Package

**What:** Place implementation in `internal/process/` package.

**Why:** Implementation details shouldn't be part of the public API. Users interact only through the `Backend` interface defined in project-foundation.

### Decision 5: Process Lifecycle Management

**What:** Define explicit crash detection and recovery:

**Crash detection methods (any one triggers crash state):**
1. **Pipe EOF:** stdout.Read() returns io.EOF unexpectedly → crash detected
2. **Exit code monitoring:** Background goroutine calling `cmd.Wait()` returns → crash detected
3. **Ping timeout:** Health check query `SELECT 1` times out after 5 seconds → crash suspected

**Detection timing:** Crash is detected within the following bounds:
- Pipe EOF: Detected on next read attempt (immediate if goroutine is reading)
- Exit monitoring: Detected when cmd.Wait() returns (typically < 100ms after process exit)
- No hard timing guarantee - detection depends on I/O scheduling

**Implementation:**
```go
type Process struct {
    crashed   atomic.Bool      // Set to true when crash detected
    crashOnce sync.Once        // Ensures crash handling runs once
    crashErr  error            // The error that caused the crash
    waiter    chan struct{}    // Closed when process exits
}

// Background goroutine started in NewProcess
func (p *Process) monitorExit() {
    p.cmd.Wait()  // Blocks until process exits
    close(p.waiter)
    p.crashOnce.Do(func() {
        p.crashed.Store(true)
        p.crashErr = &Error{Type: ErrorTypeConnection, Msg: "process exited unexpectedly"}
    })
}

func (p *Process) IsAlive() bool {
    return !p.crashed.Load()
}
```

**Recovery strategy:**
1. Mark all pending queries as failed with `ErrorTypeConnection`
2. Close all file descriptors for old process
3. Spawn new process with same configuration (done lazily on next query)
4. Resume accepting new queries (existing connections must re-prepare statements)

**State NOT recovered:**
- In-flight query results (failed and must be retried by caller)
- Prepared statements (must be re-prepared)
- Transaction state (transactions are lost on crash)

### Decision 5b: Error Type Mapping from CLI

**What:** Map DuckDB CLI stderr messages to error types.

**Mapping rules (applied in order):**
| CLI Stderr Pattern | Error Type |
|-------------------|------------|
| "Parser Error" | ErrorTypeParser |
| "Binder Error" | ErrorTypeBinder |
| "Catalog Error" | ErrorTypeCatalog |
| "division by zero" | ErrorTypeDivideByZero |
| "constraint" (case-insensitive) | ErrorTypeConstraint |
| "out of memory" | ErrorTypeOutOfMemory |
| "transaction" | ErrorTypeTransaction |
| Process exit/crash | ErrorTypeConnection |
| (default) | ErrorTypeUnknown |

**Implementation:**
```go
func classifyError(stderr string) ErrorType {
    if strings.Contains(stderr, "Parser Error") {
        return ErrorTypeParser
    }
    if strings.Contains(stderr, "Binder Error") {
        return ErrorTypeBinder
    }
    if strings.Contains(stderr, "Catalog Error") {
        return ErrorTypeCatalog
    }
    if strings.Contains(strings.ToLower(stderr), "division by zero") {
        return ErrorTypeDivideByZero
    }
    if strings.Contains(strings.ToLower(stderr), "constraint") {
        return ErrorTypeConstraint
    }
    if strings.Contains(strings.ToLower(stderr), "out of memory") {
        return ErrorTypeOutOfMemory
    }
    if strings.Contains(strings.ToLower(stderr), "transaction") {
        return ErrorTypeTransaction
    }
    return ErrorTypeUnknown
}
```

**Note:** Error types (ErrorTypeParser, etc.) are defined in add-project-foundation.

### Decision 6: Configuration Structure

**What:** Define ProcessBackendConfig struct:

```go
type ProcessBackendConfig struct {
    // BinaryPath is the path to DuckDB CLI binary.
    // If empty, searches PATH for "duckdb".
    BinaryPath string

    // QueryTimeout is maximum duration for a single query.
    // Zero means no timeout. Default: 30 seconds.
    QueryTimeout time.Duration

    // StartupTimeout is maximum duration to wait for process startup.
    // Default: 10 seconds.
    StartupTimeout time.Duration

    // MaxRetries is number of times to retry after process crash.
    // Zero means no retries. Default: 3.
    MaxRetries int

    // RetryBackoff is duration to wait between retries.
    // Default: 100 milliseconds.
    RetryBackoff time.Duration
}
```

**Binary path resolution order:**
1. Explicit `BinaryPath` if non-empty
2. `DUCKDB_PATH` environment variable if set
3. Search `PATH` for executable named "duckdb"

### Decision 7: Termination Protocol

**What:** Graceful shutdown uses two-phase termination:

1. Send `.quit` command to CLI stdin
2. Wait up to 5 seconds for clean exit
3. Send SIGTERM if still running
4. Wait up to 2 seconds for SIGTERM handling
5. Send SIGKILL if still running

### Decision 8: Minimum DuckDB Version

**What:** Require DuckDB CLI version 0.9.0 or later.

**Why:** Version 0.9.0 introduced stable JSON output format and fixed several CLI bugs. Version detection uses `duckdb --version` output parsing.

**Version check:** On backend initialization, run `duckdb --version` and parse output. If version < 0.9.0, return error with upgrade instructions.

## Risks / Trade-offs

- **Risk:** CLI binary not available
  - Mitigation: Clear error message: "DuckDB CLI binary not found. Install from https://duckdb.org/docs/installation or set DUCKDB_PATH environment variable."

- **Risk:** Process crashes mid-query
  - Mitigation: Automatic detection via pipe EOF; pending queries receive ErrorTypeConnection; automatic restart for new queries

- **Trade-off:** Higher latency than embedded (~5-50ms per query due to IPC)
  - Mitigation: Acceptable for most use cases; document performance characteristics; connection pooling at application level

- **Trade-off:** Requires external binary
  - Mitigation: Document installation clearly; future WASM backend eliminates this requirement
