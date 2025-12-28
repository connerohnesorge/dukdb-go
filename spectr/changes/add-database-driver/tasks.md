## 1. DSN Parsing (config.go)

- [ ] 1.1 Implement ParseDSN(dsn string) (*Config, error)
  - Parse path from DSN (substring before first `?`, or entire string if no `?`)
  - Parse query string options using `net/url.ParseQuery()`
  - Apply defaults for omitted options (see 6.1)
  - Return *Config with parsed values
  - **Acceptance:** `ParseDSN(":memory:?threads=4")` returns Config{Path: ":memory:", Threads: 4, ...}

- [ ] 1.2 Handle special DSN values
  - `:memory:` → Config.Path = ":memory:" (in-memory database)
  - Empty string `""` → Config.Path = ":memory:" (same as :memory:)
  - Absolute path `/path/to/db.duckdb` → Config.Path = "/path/to/db.duckdb"
  - Relative path `./db.duckdb` → Config.Path = "./db.duckdb" (relative to process cwd)
  - **Acceptance:** All path formats produce correct Config.Path

- [ ] 1.3 Implement option parsing with validation
  - `access_mode`: validate lowercase values "automatic"|"read_only"|"read_write" only
  - `threads`: parse as int, validate 1-128 (returns ErrorTypeSettings if out of range)
  - `max_memory`: parse size string, case-insensitive (e.g., "4GB", "4gb", "4Gb" all valid)
  - Return ErrorTypeSettings with message "invalid access_mode: <value>" for invalid access_mode
  - Return ErrorTypeSettings with message "invalid threads: <value>" for non-numeric threads
  - Return ErrorTypeSettings with message "threads must be 1-128, got <value>" for out-of-range
  - Return ErrorTypeSettings with message "invalid max_memory: <value>" for invalid format
  - **Acceptance:** All validation errors return correct ErrorTypeSettings with specified messages

- [ ] 1.4 Implement unknown option detection
  - Known options: access_mode, threads, max_memory
  - If DSN contains unknown option, return ErrorTypeSettings with message "unknown option: <name>"
  - If DSN contains mix of valid and invalid options, return error for first unknown option
  - **Acceptance:** `ParseDSN(":memory:?foo=bar")` returns error "unknown option: foo"

## 2. Driver Implementation (driver.go)

- [ ] 2.1 Implement Driver struct
  - Empty struct (stateless driver)
  - **Acceptance:** `type Driver struct {}`

- [ ] 2.2 Implement Driver.Open(dsn string) (driver.Conn, error)
  - Parse DSN via ParseDSN()
  - Create ProcessBackend via NewProcessBackend(config) (from add-process-backend)
  - Open connection via backend.Open(config.Path, config)
  - Return *Conn wrapping BackendConn
  - **Acceptance:** `sql.Open("dukdb", ":memory:")` returns working *sql.DB

- [ ] 2.3 Implement Driver.OpenConnector(dsn string) (driver.Connector, error)
  - Parse DSN once via ParseDSN()
  - Return *Connector with parsed config (backend created lazily on first Connect)
  - **Acceptance:** `sql.OpenDB(connector)` works with connection pooling

- [ ] 2.4 Implement driver registration
  - `func init() { sql.Register("dukdb", &Driver{}) }`
  - **Acceptance:** `import _ "dukdb"` registers driver

## 3. Connector Implementation (connector.go)

- [ ] 3.1 Implement Connector struct
  - Fields: config *Config, backend Backend, once sync.Once, initErr error
  - Backend created lazily on first Connect() using sync.Once
  - **Acceptance:** Connector holds config, backend is nil until first Connect

- [ ] 3.2 Implement Connector.Connect(ctx context.Context) (driver.Conn, error)
  - Check context with `ctx.Err()` before attempting connection - return context.Canceled if cancelled
  - Create backend via sync.Once (see design.md Decision 5)
  - If backend creation failed, return stored initErr for all subsequent calls
  - Open connection via backend.Open(ctx)
  - Return *Conn wrapping BackendConn
  - **Acceptance:** `Connector.Connect(cancelledCtx)` returns context.Canceled

- [ ] 3.3 Implement Connector.Driver() driver.Driver
  - Return `&Driver{}` (new instance - Driver is stateless)
  - **Acceptance:** Returns driver.Driver interface

## 4. Connection Health (conn.go modifications)

- [ ] 4.1 Implement Conn.Ping(ctx context.Context) error
  - Lock mutex to serialize with other operations
  - Execute "SELECT 1" via backend.Execute(ctx, "SELECT 1")
  - Return nil if query succeeds
  - Return error if query fails or context cancelled/deadline exceeded
  - **Acceptance:** Ping returns nil on healthy connection, error on crashed backend

- [ ] 4.2 Implement Conn.ResetSession(ctx context.Context) error
  - Lock mutex
  - Check inTx field - if true, execute "ROLLBACK" via backend
  - Set inTx = false after ROLLBACK (regardless of success)
  - Return ROLLBACK error if it failed, nil otherwise
  - Note: Proceeds even with cancelled context (cleanup is critical)
  - **Acceptance:** ResetSession returns nil on clean connection, executes ROLLBACK if in transaction

- [ ] 4.3 Implement Conn.IsValid() bool
  - Call backend.IsAlive() (from add-process-backend)
  - Return false if backend process has crashed (IsAlive() returns false)
  - Return true if connection is healthy
  - Note: This is advisory - subsequent operations may still fail
  - **Acceptance:** IsValid returns false for crashed backend, true otherwise

## 5. Thread Safety

- [ ] 5.1 Verify Driver thread safety
  - Driver is stateless - no locking needed
  - Driver.Open and Driver.OpenConnector can be called concurrently
  - **Acceptance:** 10 goroutines calling Driver.Open concurrently succeeds

- [ ] 5.2 Verify Connector thread safety
  - Config is immutable after creation
  - Backend creation synchronized via sync.Once
  - Connect() can be called concurrently after backend initialized
  - **Acceptance:** 10 goroutines calling Connector.Connect concurrently succeeds

- [ ] 5.3 Verify Conn thread safety
  - Add sync.Mutex field to Conn
  - Lock mutex in Ping, ResetSession, IsValid, and all query methods
  - Serialize all operations on same connection
  - **Acceptance:** go test -race passes with concurrent Conn access

## 6. Configuration Defaults

- [ ] 6.1 Implement Config struct with defaults
  - Path string (no default - parsed from DSN)
  - AccessMode string (default "automatic")
  - Threads int (default runtime.NumCPU() clamped to 1-128)
  - MaxMemory string (default "80%" meaning 80% of total system RAM)
  - Config is immutable after construction
  - **Acceptance:** ParseDSN(":memory:") returns Config with all defaults applied

- [ ] 6.2 Implement max_memory percentage resolution
  - If MaxMemory ends with "%", calculate absolute value
  - Use runtime/debug.ReadMemStats or syscall to get total system RAM
  - Convert percentage to absolute bytes (e.g., "80%" on 16GB system → "12884901888B")
  - Pass resolved absolute value to ProcessBackend
  - **Acceptance:** 80% on 16GB system resolves to approximately 12.8GB

## 7. Testing

- [ ] 7.1 DSN parsing tests
  - Test: `:memory:` parses to Path=":memory:"
  - Test: empty string parses to Path=":memory:"
  - Test: `/path/to/db.duckdb` parses to Path="/path/to/db.duckdb"
  - Test: `?access_mode=read_only` parses AccessMode="read_only"
  - Test: `?threads=4` parses Threads=4
  - Test: `?max_memory=4GB` parses MaxMemory="4GB"
  - Test: `?threads=0` returns ErrorTypeSettings
  - Test: `?threads=200` returns ErrorTypeSettings
  - Test: `?access_mode=READONLY` returns ErrorTypeSettings (case sensitive)
  - Test: `?unknown=value` returns ErrorTypeSettings
  - **Acceptance:** All DSN parsing scenarios tested

- [ ] 7.2 Connection pooling tests
  - Test: sql.DB with SetMaxOpenConns(5)
  - Test: 20 goroutines performing queries, max 5 concurrent connections
  - Test: Connection reuse - same connection returned after release
  - Test: Pool exhaustion - 6th connection blocks until release
  - **Acceptance:** Pooling behavior matches sql.DB documentation

- [ ] 7.3 Thread safety tests
  - go test -race passes on all driver tests
  - Test: 10 goroutines calling Driver.Open concurrently
  - Test: 10 goroutines calling Connector.Connect concurrently
  - Test: 10 goroutines using same *sql.DB
  - **Acceptance:** No race conditions detected

- [ ] 7.4 Health check tests
  - Test: Ping on healthy connection returns nil
  - Test: Ping with cancelled context returns context.Canceled
  - Test: ResetSession on clean connection returns nil
  - Test: ResetSession on connection with uncommitted tx executes ROLLBACK
  - Test: IsValid on healthy connection returns true
  - **Acceptance:** All health check scenarios pass
