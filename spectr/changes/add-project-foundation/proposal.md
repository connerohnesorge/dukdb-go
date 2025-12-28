# Change: Establish Project Foundation

## Why

The dukdb-go project requires a solid foundational structure before implementing specific features. This includes Go module initialization, core interfaces, build configuration, and the package structure that enables API compatibility with the original duckdb-go driver while maintaining zero cgo dependencies.

## What Changes

- Initialize Go module with proper module path and Go version requirement
- Create core package structure with Godoc-compliant documentation
- Define stub interfaces for database/sql/driver compatibility
- Establish error types and error handling patterns
- Set up build constraints ensuring no cgo usage
- Create development tooling configuration (golangci-lint, Makefile)

## Impact

- Affected specs: `project-foundation` (new capability)
- Affected code: Root package files (`go.mod`, `doc.go`, `driver.go`, `errors.go`)
- Dependencies: None - this is the foundational change
- Enables: All subsequent feature implementations
