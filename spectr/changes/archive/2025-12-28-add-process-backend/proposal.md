# Change: Implement Native Go Execution Engine

## Why

To achieve a truly pure Go DuckDB-compatible driver, we implement the core database engine natively in Go. This provides full DuckDB SQL compatibility without any external dependencies - no CGO, no WASM, no subprocess, no external binaries.

## What Changes

- Implement `Engine` type as the core execution engine
- Create SQL parser (leveraging existing Go SQL parsing libraries)
- Implement catalog for schema/table/column metadata
- Create query binder for name resolution and type checking
- Implement query planner for logical/physical plans
- Create vectorized executor for columnar query execution
- Implement in-memory columnar storage

## Architecture

```
SQL String
    ↓
┌─────────────────┐
│   SQL Parser    │  Parse SQL to AST
└────────┬────────┘
         ↓
┌─────────────────┐
│    Binder       │  Resolve names, check types
└────────┬────────┘
         ↓
┌─────────────────┐
│    Planner      │  Create logical plan → physical plan
└────────┬────────┘
         ↓
┌─────────────────┐
│   Optimizer     │  Optimize physical plan
└────────┬────────┘
         ↓
┌─────────────────┐
│   Executor      │  Execute plan, produce results
└────────┬────────┘
         ↓
     Results
```

## Package Structure

```
internal/
├── parser/      # SQL parsing
├── catalog/     # Schema metadata
├── binder/      # Name/type resolution
├── planner/     # Query planning
├── optimizer/   # Plan optimization
├── executor/    # Query execution
├── storage/     # Columnar storage
└── vector/      # Vectorized operations
```

## Impact

- Affected specs: `execution-engine` (new capability)
- Affected code: `internal/` packages (new)
- Dependencies: Requires `add-project-foundation` and `add-type-system`
- Enables: Full DuckDB SQL compatibility in pure Go
- External dependencies: None
