# Design: database/sql Driver Integration

## Architecture Overview

The driver layer provides the database/sql integration, connecting Go's standard database interfaces to the ProcessBackend (from add-process-backend). The driver is stateless; all state lives in Connector and Conn.

```
sql.DB ──→ Driver ──→ Connector ──→ Backend ──→ Conn
           │              │            │          │
           │              │            │          └─ Per-connection state
           │              │            └─ Shared ProcessBackend
           │              └─ Parsed config, holds backend reference
           └─ Stateless, just routing
```

## Design Decisions

### Decision 1: ProcessBackend Dependency

The driver uses `ProcessBackend` from `add-process-backend`. This is a **forward reference** - ProcessBackend must be implemented first.

**ProcessBackend interface used by driver:**
```go
type Backend interface {
    Open(path string, config *Config) (BackendConn, error)
    Close() error
}

type BackendConn interface {
    Execute(ctx context.Context, query string) (Result, error)
    Query(ctx context.Context, query string) (Rows, error)
    Close() error
    IsAlive() bool  // Used by IsValid()
}
```

**Rationale:** The driver delegates all DuckDB communication to ProcessBackend. Driver only handles DSN parsing and interface adaptation.

### Decision 2: Backend Crash Detection

IsValid() uses the `BackendConn.IsAlive()` method provided by ProcessBackend.

**Detection mechanism (defined in add-process-backend):**
- ProcessBackend monitors the DuckDB CLI subprocess
- `IsAlive()` returns false if:
  - Process has exited (cmd.Process.Signal(0) fails)
  - stdout/stderr pipes are closed
  - Last heartbeat query timed out
- `IsAlive()` returns true otherwise

**Race condition handling:**
- IsValid() is advisory - caller must handle errors from subsequent operations
- If crash occurs after IsValid() returns true, next operation returns ErrorTypeConnection
- Connection pool uses IsValid() as a fast filter, not a guarantee

### Decision 3: Transaction State Tracking

ResetSession needs to know if a transaction is active. State is tracked in Conn:

```go
type Conn struct {
    backend  BackendConn
    inTx     bool       // true between BEGIN and COMMIT/ROLLBACK
    mu       sync.Mutex // protects inTx
}
```

**State transitions:**
- `BeginTx()` sets `inTx = true`
- `Commit()` sets `inTx = false`
- `Rollback()` sets `inTx = false`
- `ResetSession()` checks `inTx` and calls `Rollback()` if true

**ROLLBACK failure handling:**
- If ROLLBACK fails, ResetSession returns the error
- The connection is still valid (DuckDB auto-rollbacks on disconnect)
- Connection pool may retry reset or discard connection based on error type

### Decision 4: DSN Option Case Sensitivity

**Case sensitivity rules:**
| Option | Value Case | Example |
|--------|------------|---------|
| access_mode | lowercase only | "read_only" not "READ_ONLY" |
| threads | N/A (numeric) | "4" |
| max_memory | case-insensitive | "4GB", "4gb", "4Gb" all valid |

**Rationale:**
- `access_mode` values are DuckDB keywords - use lowercase for consistency
- `max_memory` follows DuckDB's case-insensitive size parsing

**Validation:**
```go
func validateAccessMode(s string) error {
    switch s {
    case "automatic", "read_only", "read_write":
        return nil
    default:
        return &Error{Type: ErrorTypeSettings, Msg: "invalid access_mode: " + s}
    }
}

func parseMaxMemory(s string) (string, error) {
    // Accept case-insensitive, normalize to uppercase for DuckDB
    upper := strings.ToUpper(s)
    if !regexp.MustCompile(`^\d+(KB|MB|GB|TB|%)$`).MatchString(upper) {
        return "", &Error{Type: ErrorTypeSettings, Msg: "invalid max_memory: " + s}
    }
    return upper, nil  // Return uppercase for DuckDB
}
```

### Decision 5: Backend Creation Synchronization

Connector creates the Backend lazily on first Connect() call. Synchronization uses sync.Once:

```go
type Connector struct {
    config  *Config
    backend Backend
    once    sync.Once
    initErr error
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
    c.once.Do(func() {
        c.backend, c.initErr = NewProcessBackend(c.config)
    })
    if c.initErr != nil {
        return nil, c.initErr
    }
    return c.backend.Open(ctx)
}
```

**Failure handling:**
- If backend creation fails, `initErr` is set
- All subsequent Connect() calls return the same error
- To retry, create a new Connector via OpenConnector()

**Rationale:** sync.Once is simple, safe, and prevents race conditions. Failed backends are not retried automatically - this is by design to surface configuration errors.

### Decision 6: max_memory Interpretation

`max_memory` specifies the maximum memory DuckDB may use for query processing.

**Interpretation:**
- Absolute values (e.g., "4GB", "1024MB") are passed directly to DuckDB
- Percentage values (e.g., "80%") are relative to **total system RAM**
- Default "80%" means 80% of system RAM

**Calculation for percentage:**
```go
func resolveMaxMemory(s string) (string, error) {
    if strings.HasSuffix(s, "%") {
        pctStr := strings.TrimSuffix(s, "%")
        pct, err := strconv.ParseFloat(pctStr, 64)
        if err != nil || pct <= 0 || pct > 100 {
            return "", &Error{Type: ErrorTypeSettings, Msg: "invalid percentage: " + s}
        }
        totalMem := getSystemTotalMemory()  // Uses runtime or syscall
        maxBytes := uint64(float64(totalMem) * pct / 100)
        return fmt.Sprintf("%dB", maxBytes), nil
    }
    return s, nil  // Absolute value, pass through
}
```

### Decision 7: Thread Bounds and Defaults

**Bounds:**
- Minimum: 1 thread
- Maximum: 128 threads (DuckDB practical limit)
- Default: runtime.NumCPU() clamped to [1, 128]

**Validation:**
```go
func parseThreads(s string) (int, error) {
    n, err := strconv.Atoi(s)
    if err != nil {
        return 0, &Error{Type: ErrorTypeSettings, Msg: "invalid threads: " + s}
    }
    if n < 1 || n > 128 {
        return 0, &Error{Type: ErrorTypeSettings,
            Msg: fmt.Sprintf("threads must be 1-128, got %d", n)}
    }
    return n, nil
}

func defaultThreads() int {
    n := runtime.NumCPU()
    if n < 1 {
        return 1
    }
    if n > 128 {
        return 128
    }
    return n
}
```

### Decision 8: Ping Implementation

Ping uses a simple health check query:

```go
func (c *Conn) Ping(ctx context.Context) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    _, err := c.backend.Execute(ctx, "SELECT 1")
    return err
}
```

**Semantics:**
- Returns nil if query succeeds (connection is healthy)
- Returns error if query fails (connection may be unhealthy)
- Uses ctx deadline - no separate timeout
- Does not check specific result value (just execution success)

### Decision 9: Context Cancellation

Context cancellation behavior:

| Method | Cancelled Context Behavior |
|--------|---------------------------|
| Connect() | Returns context.Canceled before attempting connection |
| Ping() | Returns context.Canceled or context.DeadlineExceeded |
| Query/Exec | Returns context error (wrapped in backend error) |
| ResetSession() | Proceeds anyway - session reset is critical |

**Rationale:** ResetSession must attempt cleanup even with cancelled context to prevent resource leaks. Other operations respect context immediately.

### Decision 10: Config Immutability

Config is immutable after creation:

```go
type Config struct {
    Path       string  // immutable
    AccessMode string  // immutable
    Threads    int     // immutable
    MaxMemory  string  // immutable
}
```

**Creation:**
- `ParseDSN()` creates Config with explicit values from DSN
- Defaults applied for omitted options at parse time
- No setters - all fields set during construction

**Rationale:** Immutable config prevents race conditions and ensures consistent behavior across all connections from the same Connector.

## Error Types

| Scenario | Error Type |
|----------|------------|
| Unknown DSN option | ErrorTypeSettings |
| Invalid access_mode value | ErrorTypeSettings |
| Invalid threads value | ErrorTypeSettings |
| Invalid max_memory value | ErrorTypeSettings |
| Backend creation failed | ErrorTypeConnection |
| Context cancelled | context.Canceled (unwrapped) |
| Context deadline | context.DeadlineExceeded (unwrapped) |
| Backend crash | ErrorTypeConnection |
| ROLLBACK failed | ErrorTypeConnection |

## Thread Safety Model

| Component | Thread Safety | Mechanism |
|-----------|---------------|-----------|
| Driver | Safe | Stateless |
| Connector | Safe | sync.Once for init, Config immutable |
| Conn | Safe* | sync.Mutex protects state |
| Config | Safe | Immutable |

*Conn is safe for concurrent use but serializes operations. sql.DB typically uses one Conn per goroutine.
