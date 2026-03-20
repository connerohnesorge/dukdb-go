## 1. Core Extension Framework

- [ ] 1.1 Create `internal/extension/` package with `Extension` interface, `Registrar` interface, and function/type definition structs
- [ ] 1.2 Implement `ExtensionRegistry` with Register, Load (per-extension mutex), Install, List, Get, and autoload methods
- [ ] 1.3 Add `loadMu sync.Mutex` per-extension field to `Info` for concurrent autoload safety
- [ ] 1.4 Add unit tests for `ExtensionRegistry` (register, load, install no-op, list, autoload, concurrent load safety)

## 2. Parser and AST Support

- [ ] 2.1 Add `InstallStmt` and `LoadStmt` AST nodes to `internal/parser/ast.go`
- [ ] 2.2 Add parser support for `INSTALL <name>` and `LOAD <name>` in `internal/parser/parser.go`
- [ ] 2.3 Add parser tests for INSTALL and LOAD statements

## 3. Binder and Executor

- [ ] 3.1 Add binder support to pass `InstallStmt` and `LoadStmt` through as utility statements
- [ ] 3.2 Add executor handling for `InstallStmt` (call `registry.Install()`)
- [ ] 3.3 Add executor handling for `LoadStmt` (call `registry.Load()`)
- [ ] 3.4 Add integration tests for `INSTALL` and `LOAD` SQL execution

## 4. duckdb_extensions() Integration

- [ ] 4.1 Update `internal/metadata/extensions.go` to query the `ExtensionRegistry`
- [ ] 4.2 Update the `duckdb_extensions()` executor path to return registry data with correct columns
- [ ] 4.3 Add tests verifying `duckdb_extensions()` returns registered extensions with correct state

## 5. Autoload Support

- [ ] 5.1 Add `autoload_extensions` setting support in engine configuration
- [ ] 5.2 Add `extRegistry` and `registrar` fields to `Binder` struct for autoload access
- [ ] 5.3 Wire autoload hook into `bindFunctionCall` in `internal/binder/bind_expr.go`: function not found -> `registry.TryAutoload(funcName, registrar)` -> retry once with `triedAutoload` flag to prevent infinite recursion
- [ ] 5.4 Wire autoload hook into `bindTableFunction` in `internal/binder/bind_stmt.go`: unrecognized table function -> TryAutoload -> retry once
- [ ] 5.5 Add tests for autoload behavior (scalar function triggers extension load, table function triggers extension load, no infinite retry)

## 6. Built-in Extension Adapters

- [ ] 6.1 Create `json` extension adapter wrapping `internal/io/json`
- [ ] 6.2 Create `parquet` extension adapter wrapping `internal/io/parquet`
- [ ] 6.3 Create stub extensions for `icu`, `fts`, `httpfs` (Load returns not-implemented error)
- [ ] 6.4 Register all built-in extensions in engine initialization
- [ ] 6.5 Populate function-name-to-extension-name index for autoload

## 7. Engine Integration -- Registrar-to-Function-Catalog Bridge

- [ ] 7.1 Add `ExtensionRegistry` field to `Engine` struct and initialize in `NewEngine()`
- [ ] 7.2 Implement `EngineRegistrar` in `internal/engine/registrar.go` with:
  - `tableFunctions map[string]*TableFunctionEntry` for table functions (checked before the hard-coded switch in `executeTableFunctionScan`)
  - `RegisterScalarFunction` that adds entries to `pgfunctions.FunctionAliasRegistry`
  - `RegisterAggregateFunction` that adds entries to both alias registry and dynamic aggregate set
  - `RegisterTableFunction` that adds entries to the `tableFunctions` map
- [ ] 7.3 Update `executeTableFunctionScan` in `internal/executor/table_function_csv.go` to check `EngineRegistrar.tableFunctions` map before the built-in switch
- [ ] 7.4 Wire INSTALL/LOAD execution through engine to registry
- [ ] 7.5 Add end-to-end integration tests (INSTALL, LOAD, duckdb_extensions, autoload)

## 8. Housekeeping

- [ ] 8.1 Archive `spectr/changes/extension-system-v1.4.3/` to `spectr/changes/archive/extension-system-v1.4.3/` with note that it is superseded by `add-extension-loading`
