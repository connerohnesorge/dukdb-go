# Change: Implement Subprocess-Based Backend

## Why

DuckDB is an embedded database without a native client/server wire protocol. To achieve a pure Go implementation without CGO, we need an alternative communication mechanism. The subprocess approach spawns the official DuckDB CLI binary and communicates via stdin/stdout, providing full SQL compatibility without any C bindings.

## What Changes

- Implement `ProcessBackend` type satisfying the `Backend` interface from `add-project-foundation`
- Create process lifecycle management (spawn, monitor, terminate)
- Implement stdin/stdout communication protocol with the DuckDB CLI
- Parse CLI output into structured Go types
- Handle concurrent query execution safely
- Implement connection pooling at the process level

## Impact

- Affected specs: `process-backend` (new capability)
- Affected code: `internal/process/` package (new)
- Dependencies: Requires `add-project-foundation` to be implemented first
- Enables: Connection management, query execution, and all higher-level features
- External dependency: Requires DuckDB CLI binary available in PATH or configured location
