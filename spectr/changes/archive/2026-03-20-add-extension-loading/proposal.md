# Change: Add Extension Loading Framework

## Why

DuckDB v1.4.3 provides an extension system via `INSTALL` and `LOAD` SQL statements
and the `duckdb_extensions()` system function. The current dukdb-go implementation
returns an empty list from `internal/metadata/extensions.go` and does not support
`INSTALL` or `LOAD` statements. Users expect to be able to query available
extensions and activate functionality like `json`, `parquet`, and `icu` through
standard DuckDB SQL syntax.

An earlier proposal (`extension-system-v1.4.3`) designed a complex system with
dynamic binary loading, code signing, sandboxing, and remote registries. That
design is fundamentally incompatible with dukdb-go's pure Go constraint -- we
cannot load C shared libraries at runtime. This proposal supersedes
`extension-system-v1.4.3` with a practical, Go-native approach: extensions are
compiled-in Go packages that register with a central `ExtensionRegistry`.

## What Changes

- Add `internal/extension/` package with `Extension` interface and `ExtensionRegistry`
- Extensions register scalar functions, aggregate functions, table functions,
  types, pragmas, and file system handlers through the registry
- `INSTALL <name>` is parsed and executed as a no-op (extensions are always
  available since they are compiled in)
- `LOAD <name>` activates an extension, registering its functions and types
  with the engine
- `duckdb_extensions()` reports all registered extensions with their loaded
  and installed status
- `SET autoload_extensions = true` enables automatic loading when an
  unrecognized function name matches a known extension export
- Built-in extension adapters wrap existing packages (`internal/io/json`,
  `internal/io/parquet`, `internal/io/csv`) as extensions
- BREAKING: None. This is purely additive.

## Impact

- Affected specs: `extension-system` (new capability)
- Affected code:
  - `internal/extension/` (new package)
  - `internal/metadata/extensions.go` (populate from registry)
  - `internal/parser/parser.go` (parse INSTALL/LOAD)
  - `internal/parser/ast.go` (AST nodes for INSTALL/LOAD)
  - `internal/binder/bind_stmt.go` (bind INSTALL/LOAD)
  - `internal/executor/` (execute INSTALL/LOAD, autoload hook)
  - `internal/engine/engine.go` (hold registry, wire autoload)
- Supersedes: `extension-system-v1.4.3` (that proposal's design is not
  implementable under the pure Go constraint)
