## Context

dukdb-go aims to be a drop-in replacement for the existing duckdb-go driver but without any cgo dependencies. This enables use in environments where cgo is unavailable or undesirable (TinyGo, WebAssembly, cross-compilation scenarios).

Since DuckDB is an embedded database without a native wire protocol, the pure Go implementation must use an alternative approach for database operations. This foundation establishes the interfaces that will be implemented by concrete backends in subsequent changes.

## Goals / Non-Goals

**Goals:**
- Establish clean module structure compatible with Go 1.21+
- Define interfaces matching duckdb-go's public API
- Create extensible architecture for multiple backend implementations
- Ensure zero cgo contamination through build constraints

**Non-Goals:**
- Implement actual database functionality (covered in subsequent changes)
- Create the specific backend (subprocess, WASM, etc.)
- Performance optimization (premature at this stage)

## Decisions

### Decision 1: Package Layout

**What:** Use a flat package structure with all public API in the root `dukdb` package.

**Why:** Matches duckdb-go's API surface and simplifies import paths for users. Internal implementation details can use internal packages if needed later.

**Alternatives considered:**
- Nested packages (e.g., `dukdb/driver`, `dukdb/types`) - rejected as it complicates the drop-in replacement goal

### Decision 2: Backend Interface Pattern

**What:** Define a `Backend` interface that abstracts the database communication mechanism.

**Why:** Allows swapping implementations (subprocess, WASM, future native) without changing the public API.

### Decision 3: Build Constraints

**What:** Use `//go:build !cgo` to ensure the package cannot be built with cgo enabled accidentally.

**Why:** Enforces the zero-cgo constraint at build time rather than relying on code review.

## Risks / Trade-offs

- **Risk:** API drift from upstream duckdb-go
  - Mitigation: Regular comparison against duckdb-go releases; compatibility test suite

- **Trade-off:** Build constraints may confuse users who have CGO_ENABLED=1
  - Mitigation: Clear documentation and helpful error messages

## Open Questions

1. Should we support building alongside the original duckdb-go (different import paths)?
2. What minimum Go version to require? (Proposing 1.21 for generics and updated stdlib)
