## 1. Module Initialization

- [ ] 1.1 Create `go.mod` with module path `github.com/dukdb/dukdb-go` and `go 1.21` directive
  - **Acceptance:** `go mod tidy` succeeds; `go.mod` contains exactly these two lines plus testify dependency
- [ ] 1.2 Add `github.com/stretchr/testify v1.9.0` as test-only dependency
  - **Acceptance:** Dependency appears in `go.mod` with `// indirect` or in `go.sum`; no production code imports testify
- [ ] 1.3 Create `.golangci.yml` with enabled linters: `gofmt`, `govet`, `errcheck`, `staticcheck`, `unused`
  - **Acceptance:** `golangci-lint run` returns exit code 0 on empty package

## 2. Core Package Structure

- [ ] 2.1 Create `doc.go` with package documentation:
  ```go
  // Package dukdb provides a pure Go database/sql driver for DuckDB.
  //
  // This package requires CGO_ENABLED=0 and does not use cgo.
  // It communicates with DuckDB via subprocess or WASM backends.
  //
  // Basic usage:
  //   db, err := sql.Open("dukdb", ":memory:")
  //   // use db as normal sql.DB
  //
  // For file-backed databases:
  //   db, err := sql.Open("dukdb", "/path/to/database.db")
  package dukdb
  ```
  - **Acceptance:** `go doc github.com/dukdb/dukdb-go` outputs text containing "pure Go" and "CGO_ENABLED=0"

- [ ] 2.2 Create `driver.go` with Driver type implementing `database/sql/driver.Driver`:
  ```go
  type Driver struct{}
  func (d Driver) Open(name string) (driver.Conn, error)
  func init() { sql.Register("dukdb", Driver{}) }
  ```
  - **Acceptance:** `sql.Drivers()` includes "dukdb" after importing package

- [ ] 2.3 Create `errors.go` with error types matching duckdb-go:
  - Define `Error` struct with `Type ErrorType` and `Msg string` fields
  - Define `ErrorType` enum with 41 constants (0-40, ErrorTypeInvalid through ErrorTypeSequence)
  - Implement `Error() string` returning `fmt.Sprintf("%s: %s", e.Type.String(), e.Msg)`
  - Implement `ErrorType.String()` returning human-readable names (e.g., "Parser Error", "Closed Error")
  - Implement `Is(target error) bool` for type-only matching (ignores Msg)
  - Key constants: ErrorTypeParser=14, ErrorTypeConnection=21, ErrorTypeClosed=38, ErrorTypeBadState=39
  - **Acceptance:** `errors.Is(err, &Error{Type: ErrorTypeParser})` works correctly

## 3. Interface Definitions

- [ ] 3.1 Create `backend.go` with Backend interface as specified in design.md:
  - `Backend` interface with `Open(path, config)` and `Close()` methods
  - `BackendConn` interface with `Execute`, `Query`, `Prepare`, `Close`, `Ping` methods
  - `BackendStmt` interface with `Execute`, `Query`, `Close`, `NumInput` methods
  - `Config` struct with fields: `AccessMode`, `Threads`, `MaxMemory`, `DefaultOrder`
  - **Acceptance:** `var _ Backend = (*ProcessBackend)(nil)` compiles (compile-time interface check)

- [ ] 3.2 Create `connector.go` with Connector type implementing `driver.Connector`:
  - `type Connector struct { dsn string; backend Backend }`
  - `func (c *Connector) Connect(ctx context.Context) (driver.Conn, error)`
  - `func (c *Connector) Driver() driver.Driver`
  - **Acceptance:** Type satisfies `driver.Connector` interface

- [ ] 3.3 Create `conn.go` with Conn type implementing driver connection interfaces:
  - Implements: `driver.Conn`, `driver.ConnBeginTx`, `driver.ExecerContext`, `driver.QueryerContext`, `driver.ConnPrepareContext`
  - Methods: `Begin`, `BeginTx`, `Prepare`, `PrepareContext`, `Close`, `ExecContext`, `QueryContext`
  - **Acceptance:** Type satisfies all five interfaces

## 4. Build Configuration

- [ ] 4.1 Add `//go:build !cgo` as first line of every `.go` file in package
  - **Acceptance:** `CGO_ENABLED=1 go build` fails with build constraint error
- [ ] 4.2 Create `Makefile` with targets:
  - `test`: runs `CGO_ENABLED=0 go test -v ./...`
  - `lint`: runs `golangci-lint run`
  - `fmt`: runs `gofmt -w .`
  - `check`: runs both `lint` and `test`
  - **Acceptance:** `make check` passes

## 5. Verification

- [ ] 5.1 Run `CGO_ENABLED=0 go build ./...` to verify package compiles
  - **Acceptance:** Exit code 0, no errors
- [ ] 5.2 Run `CGO_ENABLED=1 go build ./...` to verify cgo is rejected
  - **Acceptance:** Build fails with constraint-related error
- [ ] 5.3 Create `driver_test.go` with tests:
  - `TestDriverRegistration`: Verify "dukdb" in `sql.Drivers()`
  - `TestDriverOpen_NoBackend`: Verify `sql.Open` returns error when no backend registered
  - `TestErrorTypes`: Verify all 40 error type constants are defined and distinct
  - `TestErrorIs`: Verify `errors.Is` works with Error type
  - **Acceptance:** `go test` passes all 4 tests
