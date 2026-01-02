# PROJECT KNOWLEDGE BASE

**Generated:** 2026-01-02
**Context:** Pure Go DuckDB Driver

## OVERVIEW
`dukdb-go` is a pure Go implementation of the DuckDB driver, aiming for 100% API compatibility with the original CGO-based driver (`go-duckdb`) while removing all C dependencies. This enables cross-platform support (WASM, TinyGo) and simplifies builds.

## STRUCTURE
```
dukdb-go/
├── internal/         # Core database engine (parser, planner, engine, etc.)
├── spectr/           # Specifications, change proposals, and project docs
├── references/       # Upstream DuckDB C++ and go-duckdb source for reference
├── compatibility/    # API compatibility tests against the standard driver
├── tests/            # Integration and benchmark tests
├── driver.go         # `database/sql` driver registration
└── conn.go           # Connection implementation
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| **Driver/Conn** | `driver.go`, `conn.go`, `connector.go` | Implements `database/sql` interfaces |
| **Execution Engine** | `internal/engine/` | Orchestrates query execution |
| **Data/Vectors** | `internal/vector/` | Columnar data handling (Vectors) |
| **Storage** | `internal/storage/` | Data chunks, persistence |
| **Parsing** | `internal/parser/` | SQL to AST |
| **Planning** | `internal/planner/` | AST to Logical/Physical plans |
| **I/O** | `internal/io/` | CSV, JSON, Parquet support |
| **Specs/Changes** | `spectr/` | **READ FIRST** for architectural changes |

## CONVENTIONS
- **NO CGO**: Zero C dependencies. All logic must be pure Go.
- **API Compatibility**: Must match `github.com/marcboeker/go-duckdb` public API exactly.
- **Linting**: `golangci-lint` must pass (govet, errcheck, staticcheck).
- **Testing**: Use `testify` for assertions. Integration tests validation against `references/`.
- **Spectr**: Use `spectr/changes/` for all significant architectural changes or features.

## ANTI-PATTERNS (THIS PROJECT)
- **Adding C Dependencies**: Strictly forbidden.
- **Breaking API**: Do not change public API signatures unless matching upstream changes.
- **Ignoring Spectr**: Do not bypass the `spectr` process for complex features.

## COMMANDS
```bash
# Development Environment
nix develop

# Testing
go test ./...
nix develop -c tests     # Run all tests via Nix wrapper

# Linting
golangci-lint run
nix develop -c lint

# Formatting
nix fmt                  # Runs alejandra, gofmt, golines, goimports
```

## NOTES
- The `references/` directory is for **reference only**. Do not link against it.
- **Appender** and **Arrow** support are key features that must be maintained pure-Go.
- **WASM** compatibility is a core goal; avoid `unsafe` or OS-specific calls where possible.
