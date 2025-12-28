# Change: Implement DuckDB Type System

## Why

The type system is fundamental to all database operations. Query results and parameters must be correctly typed for API compatibility with duckdb-go. This proposal defines pure Go implementations of all DuckDB types in the NEW `dukdb-go` driver (root package), using `duckdb-go/` only as API reference.

**Clarification:** The `duckdb-go/` folder is REFERENCE material only (the existing CGO driver). This change creates NEW type implementations in the root package for the pure Go `dukdb-go` driver.

## What Changes

- Create `types.go` in root package with exported types: `UUID`, `Map`, `Interval`, `Decimal`, `Union`, `Composite[T]`
- Create `type_enum.go` with Type enumeration (54 constants matching DuckDB's type IDs)
- Create `type_json.go` with JSON unmarshaling for process backend communication
- Implement `sql.Scanner` and `driver.Valuer` interfaces for all exported types
- Define type inference functions for parameter binding

## Impact

- Affected specs: `type-system` (new capability)
- Affected code: NEW files `types.go`, `type_enum.go`, `type_json.go` in root package
- Dependencies: Requires `add-project-foundation` for module structure and error types
- Enables: Query execution, result handling, prepared statements
- Runtime dependencies: `math/big` for HUGEINT/DECIMAL, `encoding/json` for CLI output parsing
