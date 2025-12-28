## 1. Process Management

- [ ] 1.1 Create `internal/process/process.go` with Process struct:
  - Fields: `cmd *exec.Cmd`, `stdin io.WriteCloser`, `stdout io.ReadCloser`, `stderr io.ReadCloser`
  - Fields: `crashed atomic.Bool`, `crashOnce sync.Once`, `crashErr error`, `waiter chan struct{}`
  - Fields: `mu sync.Mutex` (protects stdin/stdout access)
  - **Acceptance:** Struct compiles with all required fields

- [ ] 1.2 Implement `NewProcess(binaryPath, dbPath string, config *ProcessBackendConfig) (*Process, error)`:
  - Spawn process with flags: `-json`, `-noheader`, `-nullvalue`, `NULL`, database path
  - Capture stdin, stdout, stderr pipes via `cmd.StdinPipe()`, `cmd.StdoutPipe()`, `cmd.StderrPipe()`
  - Start background goroutine calling `cmd.Wait()` that closes waiter channel and sets crashed flag
  - Execute health check `SELECT 1` to verify process is responsive
  - **Acceptance:** Process spawns and health check returns within StartupTimeout (default 10 seconds)

- [ ] 1.3 Implement `Process.Shutdown() error` with two-phase termination:
  - Send `.quit\n` to stdin
  - Wait up to 5 seconds for clean exit via select on waiter channel
  - If still running, send SIGTERM via `cmd.Process.Signal(syscall.SIGTERM)` and wait 2 seconds
  - If still running, send SIGKILL via `cmd.Process.Kill()`
  - Close all pipes: `stdin.Close()`, `stdout.Close()`, `stderr.Close()`
  - **Acceptance:** Process terminates within 7 seconds guaranteed

- [ ] 1.4 Implement crash detection via exit monitoring:
  - Background goroutine waits on `cmd.Wait()`
  - When cmd.Wait() returns, close waiter channel
  - Set `crashed.Store(true)` via crashOnce.Do()
  - Set crashErr to `&Error{Type: ErrorTypeConnection, Msg: "process exited unexpectedly"}`
  - **Acceptance:** Crash detected when `cmd.Wait()` returns (no specific timing guarantee)

- [ ] 1.5 Implement `Process.IsAlive() bool`:
  - Return `!crashed.Load()`
  - **Acceptance:** Returns false after process exits, true otherwise

## 2. Communication Layer

- [ ] 2.1 Implement `Process.WriteCommand(sql string) error`:
  - Acquire mutex lock
  - Write SQL followed by newline to stdin: `stdin.Write([]byte(sql + "\n"))`
  - Return `&Error{Type: ErrorTypeConnection, Msg: "stdin pipe closed"}` if write fails
  - Keep mutex locked (released by ReadResult)
  - **Acceptance:** Command written to process stdin

- [ ] 2.2 Implement `Process.ReadResult(markerID string) ([]map[string]any, error)`:
  - Mutex already held from WriteCommand
  - Read JSON lines from stdout using `bufio.Scanner`
  - Each line is a complete JSON object, parse with `json.Unmarshal`
  - Skip lines until find row with `__marker__` == `__DUKDB_START_<markerID>__`
  - Collect all subsequent rows until find row with `__marker__` == `__DUKDB_END_<markerID>__`
  - Return collected rows (excluding marker rows)
  - Release mutex lock
  - **Acceptance:** Query results correctly parsed and isolated

- [ ] 2.3 Implement stderr capture:
  - Background goroutine reads stderr into circular buffer (last 4KB)
  - Use `ring.Ring` or `[]byte` with wrap-around index
  - Provide `GetStderr() string` method to retrieve buffered stderr
  - **Acceptance:** Last 4KB of stderr accessible via GetStderr()

- [ ] 2.4 Implement marker generation:
  - Generate UUID v4 using `github.com/google/uuid` package: `uuid.New().String()`
  - Format markers: `__DUKDB_START_<uuid>__` and `__DUKDB_END_<uuid>__`
  - **Acceptance:** Each call to generateMarker() returns unique UUID-based marker

- [ ] 2.5 Implement error classification from stderr:
  - Parse stderr content using classifyError() function (see design.md Decision 5b)
  - Map "Parser Error" → ErrorTypeParser
  - Map "Binder Error" → ErrorTypeBinder
  - Map "Catalog Error" → ErrorTypeCatalog
  - Map "division by zero" → ErrorTypeDivideByZero
  - Map "constraint" → ErrorTypeConstraint
  - Map process crash → ErrorTypeConnection
  - Default → ErrorTypeUnknown
  - **Acceptance:** Each error pattern maps to correct ErrorType

## 3. Backend Interface Implementation

- [ ] 3.1 Create `internal/process/backend.go` with ProcessBackend struct:
  - Implements `Backend` interface from project-foundation
  - Fields: `config ProcessBackendConfig`, `processes sync.Map` (keyed by db path)
  - Fields: `closed atomic.Bool`
  - **Acceptance:** `var _ Backend = (*ProcessBackend)(nil)` compiles

- [ ] 3.2 Implement `ProcessBackend.Open(path string, config *Config) (BackendConn, error)`:
  - If closed.Load() == true, return `&Error{Type: ErrorTypeConnection, Msg: "backend is closed"}`
  - Normalize path: "" and ":memory:" both become ":memory:"
  - Get existing Process from processes map, or create new one
  - Create ProcessConn wrapping the Process
  - Execute `SELECT 1` health check via ProcessConn
  - **Acceptance:** Returns usable BackendConn or error with clear message

- [ ] 3.3 Implement `ProcessBackend.Close() error`:
  - Set closed.Store(true)
  - Iterate processes map and call Shutdown() on each
  - Clear processes map
  - **Acceptance:** All processes terminated, no goroutine leaks

- [ ] 3.4 Implement `ProcessConn.Execute(ctx, query string, args []any) (int64, error)`:
  - Check ctx.Err() first - return context.Canceled or context.DeadlineExceeded if set
  - Format query with parameter substitution (delegated to add-query-execution's FormatValue)
  - Generate marker ID
  - Write marker + query + marker via WriteCommand
  - Read result via ReadResult with timeout from context
  - Parse result to extract affected row count from DuckDB's "Count" or similar field
  - **Acceptance:** INSERT/UPDATE/DELETE returns correct row count

- [ ] 3.5 Implement `ProcessConn.Query(ctx, query string, args []any) ([]map[string]any, []string, error)`:
  - Check ctx.Err() first
  - Format query with parameter substitution
  - Generate marker ID
  - Write marker + query + marker
  - Read result with timeout
  - Extract column names from first row's keys (sorted alphabetically for consistency)
  - Return parsed JSON rows and column names
  - **Acceptance:** SELECT returns correct data and column names

## 4. Concurrency Support

- [ ] 4.1 Add mutex to Process for stdin/stdout serialization:
  - `sync.Mutex` in Process struct
  - Lock in WriteCommand, unlock in ReadResult
  - Ensures only one query cycle runs at a time
  - **Acceptance:** 100 concurrent queries complete without data mixing

- [ ] 4.2 Implement query timeout via context:
  - Use `select` with `ctx.Done()` and result channel
  - On timeout, return context.DeadlineExceeded
  - Note: underlying CLI query continues - no way to cancel
  - Log warning if query completes after timeout
  - **Acceptance:** Query times out at context deadline

- [ ] 4.3 Implement context cancellation check:
  - Check `ctx.Err()` before starting query
  - Return context.Canceled immediately if context cancelled
  - **Acceptance:** Cancelled context returns immediately without executing query

## 5. Configuration

- [ ] 5.1 Implement ProcessBackendConfig with defaults:
  - QueryTimeout: 30 seconds (via `applyDefaults()` function)
  - StartupTimeout: 10 seconds
  - MaxRetries: 3
  - RetryBackoff: 100 milliseconds
  - **Acceptance:** Zero-value config uses documented defaults

- [ ] 5.2 Implement binary path resolution:
  1. Check config.BinaryPath - if non-empty, use it
  2. Check `os.Getenv("DUCKDB_PATH")` - if non-empty, use it
  3. Search PATH for "duckdb" using `exec.LookPath("duckdb")`
  - Return `&Error{Type: ErrorTypeConnection, Msg: "DuckDB CLI binary not found..."}` if not found
  - **Acceptance:** Binary found via each method, or clear error message

- [ ] 5.3 Implement DuckDB version check:
  - Run `duckdb --version`, capture stdout
  - Parse version number from output (format: "v0.9.2 abc123")
  - Extract major.minor.patch numbers
  - Require version >= 0.9.0
  - Return `&Error{Type: ErrorTypeConnection, Msg: "requires DuckDB 0.9.0 or later, found X.Y.Z"}` if too old
  - **Acceptance:** Old versions rejected with upgrade instructions

## 6. Testing

- [ ] 6.1 Unit tests for process lifecycle:
  - TestProcessSpawn: verify process starts and health check passes
  - TestProcessShutdown: verify graceful shutdown within 7 seconds
  - TestProcessCrashDetection: kill process, verify crash detected
  - **Acceptance:** All pass, no goroutine leaks (check runtime.NumGoroutine before/after)

- [ ] 6.2 Integration tests with real DuckDB CLI:
  - Skip if DuckDB not installed: `if _, err := exec.LookPath("duckdb"); err != nil { t.Skip(...) }`
  - TestSimpleQuery: SELECT 1 returns expected result
  - TestQueryWithNulls: verify NULL handling
  - TestQueryWithNestedTypes: verify STRUCT/LIST parsing
  - **Acceptance:** All pass when DuckDB installed

- [ ] 6.3 Concurrent query tests:
  - Spawn 10 goroutines each running 10 queries with unique identifiers
  - Each query: `SELECT <goroutine_id> * 1000 + <query_id> AS result`
  - Verify each goroutine receives its correct results
  - **Acceptance:** 100 total queries complete with correct results

- [ ] 6.4 Error handling tests:
  - TestBinaryNotFound: verify ErrorTypeConnection with correct message
  - TestInvalidSQL: verify ErrorTypeParser for syntax errors
  - TestTimeout: verify context.DeadlineExceeded for slow queries
  - TestDivideByZero: verify ErrorTypeDivideByZero
  - **Acceptance:** Each error case returns expected error type

- [ ] 6.5 Edge case tests:
  - TestEmptyResultSet: SELECT with WHERE false returns empty slice
  - TestLargeResultSet: 100,000 rows parse successfully
  - TestNullsInAllColumns: all-NULL row handled correctly
  - TestUnicodeInQuery: Unicode strings round-trip correctly
  - **Acceptance:** All edge cases handled correctly
