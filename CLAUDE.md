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
- **`internal/catalog/`**: Schema and metadata management
- **`internal/storage/`**: Data persistence layer
- **`internal/parser/`**: SQL parsing
- **`internal/planner/`**: Query planning
- **`internal/executor/`**: Query execution
- **`internal/binder/`**: SQL binding/resolution
- **`internal/vector/`**: Columnar data representation

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

## Linting Configuration

The project uses a minimal golangci-lint configuration (`.golangci.yml`):
- Enabled: govet, errcheck, staticcheck, unused
- Formatter: gofmt with simplify

## Testing Notes

- Uses testify for assertions (`github.com/stretchr/testify`)
- Integration tests run against the actual engine implementation
- Reference implementation in `duckdb-go/` can be used to validate expected behavior


<!-- spectr:START -->
# Spectr Instructions

These instructions are for AI assistants working in this project.

Always open `@/spectr/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/spectr/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

When delegating tasks from a change proposal to subagents:
- Provide the proposal path: `spectr/changes/<id>/proposal.md`
- Include task context: `spectr/changes/<id>/tasks.jsonc`
- Reference delta specs: `spectr/changes/<id>/specs/<capability>/spec.md`

<!-- spectr:END -->
