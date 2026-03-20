## ADDED Requirements

### Requirement: Extension Registry

The system SHALL provide an `ExtensionRegistry` that manages compiled-in Go
extensions. Extensions register at initialization time and are activated on
demand via `LOAD` or autoload.

The registry SHALL be thread-safe and support concurrent reads. Concurrent
`Load()` calls for the same extension SHALL be serialized via a per-extension
mutex so that only one goroutine performs the actual load while others block
and then observe `StateLoaded`. Concurrent loads of different extensions SHALL
NOT block each other.

#### Scenario: Register and list extensions

- WHEN an extension is registered with the registry
- THEN `List()` SHALL include that extension with state `Available`
- AND `Get(name)` SHALL return the extension info

#### Scenario: Load an extension

- WHEN `Load(name, registrar)` is called for a registered extension
- THEN the extension's `Load()` method SHALL be called with the registrar
- AND the extension state SHALL change to `Loaded`
- AND subsequent calls to `Load()` for the same extension SHALL be no-ops

#### Scenario: Load unknown extension

- WHEN `Load(name, registrar)` is called for an unregistered name
- THEN the registry SHALL return an error containing the extension name

### Requirement: Extension Interface

The system SHALL define an `Extension` interface that all compiled-in extensions
implement. The interface SHALL include methods for `Name()`, `Description()`,
`Version()`, `Load(Registrar)`, and `Unload()`.

#### Scenario: Extension provides metadata

- WHEN an extension implements the `Extension` interface
- THEN `Name()` SHALL return the canonical extension name (e.g. "json")
- AND `Description()` SHALL return a human-readable description
- AND `Version()` SHALL return a version string

#### Scenario: Extension registers capabilities on load

- WHEN `Load(registrar)` is called on an extension
- THEN the extension SHALL register its functions, types, and handlers via the registrar

### Requirement: Registrar Interface

The system SHALL provide a `Registrar` interface that extensions use to register
scalar functions, aggregate functions, table functions, custom types, pragmas,
file handlers, and settings with the engine.

#### Scenario: Register a scalar function

- WHEN an extension calls `RegisterScalarFunction(def)` on the registrar
- THEN the function SHALL be added to the `FunctionAliasRegistry` (pgfunctions)
- AND the function SHALL become available for use in SQL queries via the binder

#### Scenario: Register a table function

- WHEN an extension calls `RegisterTableFunction(def)` on the registrar
- THEN the function SHALL be added to the `EngineRegistrar.tableFunctions` map
- AND the executor SHALL check this map before the built-in switch in `executeTableFunctionScan`
- AND the table function SHALL become available in `SELECT * FROM func(args)` queries

### Requirement: INSTALL Statement

The system SHALL parse and execute `INSTALL <extension_name>` SQL statements.
Since all extensions are compiled-in, `INSTALL` SHALL be a no-op that succeeds
if the extension is registered and returns an error if the extension is unknown.

#### Scenario: INSTALL known extension

- WHEN the user executes `INSTALL json`
- THEN the statement SHALL succeed without error
- AND the extension state SHALL remain unchanged (no side effects)

#### Scenario: INSTALL unknown extension

- WHEN the user executes `INSTALL nonexistent`
- THEN the statement SHALL return an error indicating the extension is not found

### Requirement: LOAD Statement

The system SHALL parse and execute `LOAD <extension_name>` SQL statements.
`LOAD` SHALL activate the extension, calling its `Load()` method and registering
all its functions and types with the engine.

#### Scenario: LOAD known extension

- WHEN the user executes `LOAD json`
- THEN the json extension's functions SHALL be registered with the engine
- AND `duckdb_extensions()` SHALL show the extension as loaded

#### Scenario: LOAD already-loaded extension

- WHEN the user executes `LOAD json` twice
- THEN the second LOAD SHALL succeed as a no-op

#### Scenario: LOAD stub extension

- WHEN the user executes `LOAD icu` and icu is a stub (not yet implemented)
- THEN the statement SHALL return an error indicating the extension is not yet implemented

### Requirement: duckdb_extensions() System Function

The system SHALL return extension information from the `ExtensionRegistry` when
the `duckdb_extensions()` table function is queried. The result SHALL include
columns: `extension_name`, `loaded`, `installed`, `install_path`, `description`,
`aliases`, and `extension_version`.

#### Scenario: Query extensions after registration

- WHEN the user executes `SELECT * FROM duckdb_extensions()`
- THEN the result SHALL include one row per registered extension
- AND `installed` SHALL be `true` for all extensions (they are compiled in)
- AND `loaded` SHALL reflect whether `LOAD` has been called
- AND `install_path` SHALL be `(BUILT_IN)` for all extensions

#### Scenario: Query extensions after LOAD

- WHEN the user executes `LOAD json` followed by `SELECT * FROM duckdb_extensions() WHERE extension_name = 'json'`
- THEN the result SHALL show `loaded = true`

### Requirement: Autoload Extensions

The system SHALL support automatic extension loading when `autoload_extensions`
is set to `true`. When an unrecognized function is encountered during binding
and autoload is enabled, the system SHALL check whether any registered extension
provides that function and load it automatically.

#### Scenario: Autoload on scalar function call

- WHEN `autoload_extensions` is `true`
- AND the user calls a scalar function not found in the built-in catalog or UDF resolver
- AND the function name exists in `registry.funcIndex`
- THEN `binder.bindFunctionCall` SHALL call `registry.TryAutoload(funcName, registrar)`
- AND the extension's `Load(registrar)` SHALL register functions into both catalogs
- AND the binder SHALL retry function resolution exactly once
- AND the query SHALL succeed

#### Scenario: Autoload on table function call

- WHEN `autoload_extensions` is `true`
- AND the user calls a table function not found in the executor's built-in switch or `tableFunctions` map
- AND the function name exists in `registry.funcIndex`
- THEN `binder.bindTableFunction` SHALL call `registry.TryAutoload(funcName, registrar)`
- AND the binder SHALL retry table function resolution exactly once
- AND the query SHALL succeed

#### Scenario: Autoload on function call (general)

- WHEN `autoload_extensions` is `true`
- AND the user calls `read_json('file.json')` without first running `LOAD json`
- THEN the system SHALL automatically load the `json` extension
- AND the query SHALL succeed

#### Scenario: Autoload disabled

- WHEN `autoload_extensions` is `false`
- AND the user calls `read_json('file.json')` without first running `LOAD json`
- THEN the query SHALL fail with a "function not found" error

#### Scenario: Autoload with unknown function

- WHEN `autoload_extensions` is `true`
- AND the user calls a function not provided by any extension
- THEN the query SHALL fail with a "function not found" error

### Requirement: Built-in Extension Adapters

The system SHALL provide built-in extensions that wrap existing dukdb-go
packages. At minimum: `json` (wrapping `internal/io/json`), `parquet`
(wrapping `internal/io/parquet`). Stub extensions SHALL be provided for `icu`,
`fts`, and `httpfs` that report as available but return an error on load.

#### Scenario: JSON extension provides read_json

- WHEN the `json` extension is loaded
- THEN `read_json`, `read_json_auto`, and `read_ndjson` SHALL be available as table functions

#### Scenario: Parquet extension provides read_parquet

- WHEN the `parquet` extension is loaded
- THEN `read_parquet` SHALL be available as a table function

#### Scenario: Stub extension reports availability

- WHEN `duckdb_extensions()` is queried
- THEN stub extensions (`icu`, `fts`, `httpfs`) SHALL appear with `installed = true` and `loaded = false`

### Requirement: Concurrent Autoload Safety

The system SHALL handle concurrent autoload requests safely. When multiple
goroutines trigger autoload for the same extension simultaneously, only one
SHALL execute the extension's `Load()` method. Others SHALL block on the
per-extension mutex and then observe the already-loaded state.

#### Scenario: Concurrent autoload same extension

- WHEN two goroutines simultaneously call `TryAutoload("read_json", registrar)`
- THEN exactly one goroutine SHALL call `extension.Load(registrar)`
- AND both goroutines SHALL observe `StateLoaded` when they return
- AND no duplicate function registrations SHALL occur

#### Scenario: Concurrent autoload different extensions

- WHEN goroutine A triggers autoload for extension "json"
- AND goroutine B simultaneously triggers autoload for extension "parquet"
- THEN both loads SHALL proceed concurrently without blocking each other

### Note: Supersedes extension-system-v1.4.3

This specification supersedes the `extension-system-v1.4.3` proposal, which
designed a C-plugin-style system with dynamic binary loading, code signing,
sandboxing, and remote registries. That design is incompatible with dukdb-go's
pure Go / no-cgo constraint. The `extension-system-v1.4.3` proposal SHALL be
archived.
