# Project Foundation Specification

## Requirements

### Requirement: Go Module Structure

The package SHALL be a valid Go module with module path `github.com/dukdb/dukdb-go` requiring Go 1.21 or later.

#### Scenario: Module initialization
- GIVEN a fresh clone of the repository
- WHEN running `go mod download`
- THEN all dependencies are resolved without errors
- AND the only runtime dependency is the Go standard library

#### Scenario: Go version enforcement
- GIVEN the go.mod file
- WHEN inspecting the go directive
- THEN it specifies `go 1.21` or later

#### Scenario: Test dependencies isolated
- GIVEN the package imports
- WHEN analyzing production code (non `_test.go` files)
- THEN no imports reference `github.com/stretchr/testify`

### Requirement: Package Documentation

The package SHALL include Godoc-compliant documentation in `doc.go` that explains its purpose as a pure Go DuckDB driver requiring CGO_ENABLED=0.

#### Scenario: Package doc content
- GIVEN the dukdb package
- WHEN running `go doc github.com/dukdb/dukdb-go`
- THEN output contains the phrase "pure Go"
- AND output contains "CGO_ENABLED=0"
- AND output contains usage example with `sql.Open("dukdb", ...)`

### Requirement: Driver Registration

The package SHALL register itself with database/sql under the driver name "dukdb" upon import, distinct from "duckdb".

#### Scenario: Driver availability after import
- GIVEN a Go program that imports `_ "github.com/dukdb/dukdb-go"`
- WHEN querying `sql.Drivers()`
- THEN "dukdb" appears in the returned slice

#### Scenario: Driver open without backend
- GIVEN the dukdb driver is registered but no backend is configured
- WHEN calling `sql.Open("dukdb", ":memory:")`
- THEN an error of type `*Error` is returned
- AND `Error.Type` equals `ErrorTypeConnection`
- AND `Error.Msg` contains "no backend registered"

#### Scenario: DSN format support
- GIVEN a registered dukdb driver with backend
- WHEN calling `sql.Open("dukdb", ":memory:")` for in-memory database
- THEN a valid connection is returned
- WHEN calling `sql.Open("dukdb", "/path/to/db.duckdb")` for file database
- THEN a valid connection is returned or file-not-found error

### Requirement: Zero CGO Enforcement

The package SHALL fail to compile when CGO_ENABLED=1 due to build constraints on all source files.

#### Scenario: Build with cgo disabled
- GIVEN environment variable CGO_ENABLED=0
- WHEN running `go build ./...`
- THEN the package compiles successfully with exit code 0

#### Scenario: Build with cgo enabled fails
- GIVEN environment variable CGO_ENABLED=1
- WHEN running `go build ./...`
- THEN the build fails
- AND error output indicates build constraint failure (no Go files match)

#### Scenario: All source files have constraint
- GIVEN all `.go` files in the package (excluding `_test.go`)
- WHEN checking the first line of each file
- THEN each file starts with `//go:build !cgo`

### Requirement: Error Types

The package SHALL define 40 error type constants matching duckdb-go's ErrorType enumeration exactly, plus an Error struct implementing the error interface.

#### Scenario: Error type constants defined
- GIVEN the ErrorType type
- THEN ErrorTypeInvalid equals 0
- AND ErrorTypeOutOfRange equals 1
- AND ErrorTypeConversion equals 2
- AND ErrorTypeParser equals 14
- AND ErrorTypeConnection equals 21
- AND ErrorTypeClosed equals 38
- AND ErrorTypeBadState equals 39
- AND ErrorTypeSequence equals 40
- AND all 41 constants (0-40) are defined with unique values

#### Scenario: ErrorType.String() returns human-readable names
- GIVEN ErrorTypeParser
- WHEN calling String()
- THEN returns "Parser Error"
- GIVEN ErrorTypeClosed
- WHEN calling String()
- THEN returns "Closed Error"

#### Scenario: Error interface implementation
- GIVEN an Error{Type: ErrorTypeParser, Msg: "syntax error"}
- WHEN calling err.Error()
- THEN result is "Parser Error: syntax error"

#### Scenario: Error type matching with errors.Is
- GIVEN err := &Error{Type: ErrorTypeParser, Msg: "test"}
- WHEN calling errors.Is(err, &Error{Type: ErrorTypeParser})
- THEN result is true
- WHEN calling errors.Is(err, &Error{Type: ErrorTypeConnection})
- THEN result is false

#### Scenario: Error type matching with errors.As
- GIVEN any error wrapping an *Error
- WHEN calling errors.As(err, &target) where target is *Error
- THEN target is populated with the underlying Error values

### Requirement: Backend Interface

The package SHALL define Backend, BackendConn, and BackendStmt interfaces for abstracting database communication.

#### Scenario: Backend interface methods
- GIVEN the Backend interface
- THEN it has method `Open(path string, config *Config) (BackendConn, error)`
- AND it has method `Close() error`

#### Scenario: BackendConn interface methods
- GIVEN the BackendConn interface
- THEN it has method `Execute(ctx context.Context, query string, args []driver.NamedValue) (int64, error)`
- AND it has method `Query(ctx context.Context, query string, args []driver.NamedValue) ([]map[string]any, []string, error)`
- AND it has method `Prepare(ctx context.Context, query string) (BackendStmt, error)`
- AND it has method `Close() error`
- AND it has method `Ping(ctx context.Context) error`

#### Scenario: BackendStmt interface methods
- GIVEN the BackendStmt interface
- THEN it has method `Execute(ctx context.Context, args []driver.NamedValue) (int64, error)`
- AND it has method `Query(ctx context.Context, args []driver.NamedValue) ([]map[string]any, []string, error)`
- AND it has method `Close() error`
- AND it has method `NumInput() int`

#### Scenario: Config struct fields
- GIVEN the Config struct
- THEN it has field `AccessMode string` for read/write mode
- AND it has field `Threads int` for worker thread count
- AND it has field `MaxMemory string` for memory limit
- AND it has field `DefaultOrder string` for default sort order

#### Scenario: Backend implementation satisfaction
- GIVEN any type implementing all Backend interface methods
- WHEN assigning to a Backend variable
- THEN compilation succeeds

### Requirement: Connector Type

The package SHALL provide a Connector type implementing driver.Connector for connection management.

#### Scenario: Connector interface compliance
- GIVEN the Connector type
- WHEN checking interface satisfaction
- THEN Connector implements driver.Connector

#### Scenario: Connector connect
- GIVEN a Connector with valid backend
- WHEN calling Connect(ctx)
- THEN a driver.Conn is returned
- AND the connection is usable for queries

### Requirement: Connection Type

The package SHALL provide a Conn type implementing driver.Conn and related interfaces for query execution.

#### Scenario: Conn interface compliance
- GIVEN the Conn type
- WHEN checking interface satisfaction
- THEN Conn implements driver.Conn
- AND Conn implements driver.ConnBeginTx
- AND Conn implements driver.ExecerContext
- AND Conn implements driver.QueryerContext
- AND Conn implements driver.ConnPrepareContext

#### Scenario: Connection thread safety
- GIVEN a single Conn instance
- WHEN accessed from multiple goroutines simultaneously
- THEN operations are serialized and no data races occur

