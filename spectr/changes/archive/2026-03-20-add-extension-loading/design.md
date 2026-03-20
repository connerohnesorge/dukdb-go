# Extension Loading Framework -- Design

## Implementation Details

### Extension Interface

Every extension implements this interface. Extensions are Go packages compiled
into the binary. They do not load dynamically.

```go
// internal/extension/extension.go

package extension

// Extension is the interface all dukdb-go extensions implement.
// Extensions are compiled-in Go packages, not dynamically loaded binaries.
type Extension interface {
    // Name returns the canonical extension name (e.g. "json", "parquet", "icu").
    Name() string

    // Description returns a short human-readable description.
    Description() string

    // Version returns the extension version string.
    Version() string

    // Load activates the extension, registering all functions, types, and
    // handlers with the provided Registrar. Called when the user runs
    // LOAD <name> or when autoload triggers.
    Load(r Registrar) error

    // Unload deactivates the extension. Optional cleanup.
    Unload() error
}
```

### Registrar Interface

The `Registrar` is the interface extensions use to register their capabilities
with the engine. It is passed to `Extension.Load()`.

```go
// internal/extension/registrar.go

package extension

// Registrar allows extensions to register functions, types, and handlers
// with the engine.
type Registrar interface {
    // RegisterScalarFunction registers a scalar SQL function.
    RegisterScalarFunction(def ScalarFunctionDef) error

    // RegisterAggregateFunction registers an aggregate SQL function.
    RegisterAggregateFunction(def AggregateFunctionDef) error

    // RegisterTableFunction registers a table-valued function.
    RegisterTableFunction(def TableFunctionDef) error

    // RegisterType registers a custom SQL type.
    RegisterType(def TypeDef) error

    // RegisterPragma registers a PRAGMA handler.
    RegisterPragma(def PragmaDef) error

    // RegisterFileHandler registers a file format handler for COPY and
    // read_* functions.
    RegisterFileHandler(def FileHandlerDef) error

    // RegisterSetting registers a configuration setting.
    RegisterSetting(def SettingDef) error
}
```

### Function Definition Types

```go
// internal/extension/defs.go

package extension

// ScalarFunctionDef defines a scalar SQL function provided by an extension.
type ScalarFunctionDef struct {
    Name       string
    Args       []TypeTag   // argument types; nil means variadic
    ReturnType TypeTag
    Impl       ScalarFunctionImpl
}

// ScalarFunctionImpl is the implementation of a scalar function.
// args contains the evaluated argument values for one row.
type ScalarFunctionImpl func(args []any) (any, error)

// AggregateFunctionDef defines an aggregate SQL function.
type AggregateFunctionDef struct {
    Name       string
    Args       []TypeTag
    ReturnType TypeTag
    Create     func() AggregateState
}

// AggregateState is a stateful accumulator for aggregate functions.
type AggregateState interface {
    Update(args []any) error
    Combine(other AggregateState) error
    Finalize() (any, error)
}

// TableFunctionDef defines a table-valued function.
type TableFunctionDef struct {
    Name    string
    Bind    TableFunctionBind
    Execute TableFunctionExecute
}

// TableFunctionBind is called at plan time to determine output columns.
type TableFunctionBind func(args []any) (columns []ColumnDef, err error)

// TableFunctionExecute produces rows.
type TableFunctionExecute func(args []any, output func(row []any) error) error

// ColumnDef describes a result column from a table function.
type ColumnDef struct {
    Name string
    Type TypeTag
}

// TypeTag identifies a SQL type for function signatures.
type TypeTag int

// Standard type tags matching dukdb.Type constants.
const (
    TypeAny     TypeTag = -1
    TypeBoolean TypeTag = iota
    TypeTinyInt
    TypeSmallInt
    TypeInteger
    TypeBigInt
    TypeFloat
    TypeDouble
    TypeVarchar
    TypeBlob
    TypeDate
    TypeTime
    TypeTimestamp
    TypeInterval
    TypeDecimal
    TypeJSON
)

// TypeDef defines a custom SQL type.
type TypeDef struct {
    Name       string
    BaseType   TypeTag
    Serialize  func(v any) ([]byte, error)
    Deserialize func(b []byte) (any, error)
}

// PragmaDef defines a PRAGMA command handler.
type PragmaDef struct {
    Name    string
    Handler func(args []string) ([]map[string]any, []string, error)
}

// FileHandlerDef defines a file format handler.
type FileHandlerDef struct {
    Format     string   // e.g. "json", "parquet", "csv"
    Extensions []string // file extensions: [".json", ".ndjson"]
    Reader     FileReaderFactory
    Writer     FileWriterFactory
}

// FileReaderFactory creates a reader for the given path and options.
type FileReaderFactory func(path string, options map[string]string) (FileReader, error)

// FileWriterFactory creates a writer for the given path and options.
type FileWriterFactory func(path string, options map[string]string) (FileWriter, error)

// FileReader reads rows from a file.
type FileReader interface {
    Columns() []ColumnDef
    Next() ([]any, error) // returns io.EOF when done
    Close() error
}

// FileWriter writes rows to a file.
type FileWriter interface {
    WriteRow(row []any) error
    Close() error
}

// SettingDef defines a configuration setting.
type SettingDef struct {
    Name        string
    Default     string
    Description string
    Validate    func(value string) error
}
```

### ExtensionRegistry

The registry is the central data structure that tracks all known extensions
and their state.

```go
// internal/extension/registry.go

package extension

import "sync"

// State represents the lifecycle state of an extension.
type State int

const (
    StateAvailable State = iota // compiled in, not yet loaded
    StateLoaded                 // Load() has been called, functions registered
)

// Info holds runtime information about a registered extension.
type Info struct {
    Extension Extension
    State     State
    // Functions exported by this extension (populated after Load).
    ExportedFunctions []string
    loadMu            sync.Mutex // per-extension mutex for concurrent autoload safety
}

// Registry manages all compiled-in extensions.
type Registry struct {
    mu         sync.RWMutex
    extensions map[string]*Info        // name -> info
    funcIndex  map[string]string       // function_name -> extension_name (for autoload)
    autoload   bool
}

// NewRegistry creates an empty extension registry.
func NewRegistry() *Registry {
    return &Registry{
        extensions: make(map[string]*Info),
        funcIndex:  make(map[string]string),
        autoload:   false,
    }
}

// Register adds an extension to the registry. It does NOT call Load().
// This is called during init() by each extension package.
func (r *Registry) Register(ext Extension) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.extensions[ext.Name()] = &Info{
        Extension: ext,
        State:     StateAvailable,
    }
}

// Load activates an extension by name. Idempotent -- loading an already-loaded
// extension is a no-op. Uses per-extension mutex so concurrent callers
// targeting different extensions do not block each other.
func (r *Registry) Load(name string, registrar Registrar) error {
    r.mu.RLock()
    info, ok := r.extensions[name]
    r.mu.RUnlock()
    if !ok {
        return fmt.Errorf("extension %q not found", name)
    }

    // Per-extension lock prevents duplicate loads; non-contending
    // extensions proceed in parallel.
    info.loadMu.Lock()
    defer info.loadMu.Unlock()

    if info.State == StateLoaded {
        return nil // another goroutine completed the load
    }

    if err := info.Extension.Load(registrar); err != nil {
        return fmt.Errorf("loading extension %q: %w", name, err)
    }

    r.mu.Lock()
    info.State = StateLoaded
    r.mu.Unlock()
    return nil
}

// Install is a no-op for compiled-in extensions. It exists for SQL
// compatibility with INSTALL <name>.
func (r *Registry) Install(name string) error {
    r.mu.RLock()
    defer r.mu.RUnlock()
    if _, ok := r.extensions[name]; !ok {
        return fmt.Errorf("extension %q not found", name)
    }
    return nil
}

// SetAutoload enables or disables autoload behavior.
func (r *Registry) SetAutoload(enabled bool) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.autoload = enabled
}

// AutoloadEnabled returns whether autoload is enabled.
func (r *Registry) AutoloadEnabled() bool {
    r.mu.RLock()
    defer r.mu.RUnlock()
    return r.autoload
}

// TryAutoload attempts to load the extension that provides the given
// function name. Returns true if an extension was loaded.
func (r *Registry) TryAutoload(funcName string, registrar Registrar) bool {
    r.mu.RLock()
    if !r.autoload {
        r.mu.RUnlock()
        return false
    }
    extName, ok := r.funcIndex[funcName]
    r.mu.RUnlock()
    if !ok {
        return false
    }
    return r.Load(extName, registrar) == nil
}

// List returns information about all registered extensions.
func (r *Registry) List() []Info {
    r.mu.RLock()
    defer r.mu.RUnlock()
    result := make([]Info, 0, len(r.extensions))
    for _, info := range r.extensions {
        result = append(result, *info)
    }
    return result
}

// Get returns information about a specific extension.
func (r *Registry) Get(name string) (*Info, bool) {
    r.mu.RLock()
    defer r.mu.RUnlock()
    info, ok := r.extensions[name]
    if !ok {
        return nil, false
    }
    copy := *info
    return &copy, true
}

// RegisterFunctionMapping records that a function name is provided by a
// specific extension. Used for autoload resolution.
func (r *Registry) RegisterFunctionMapping(funcName, extName string) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.funcIndex[funcName] = extName
}
```

### INSTALL/LOAD Statement Handling

#### Parser Changes

Add AST nodes and parser support for `INSTALL` and `LOAD` statements.

```go
// In internal/parser/ast.go

// InstallStmt represents INSTALL <extension_name>.
type InstallStmt struct {
    ExtensionName string
}

// LoadStmt represents LOAD <extension_name>.
type LoadStmt struct {
    ExtensionName string
}
```

The parser recognizes:
- `INSTALL <identifier>;` -- parsed into `InstallStmt`
- `LOAD <identifier>;` -- parsed into `LoadStmt`

Both `INSTALL` and `LOAD` accept an unquoted identifier or a single-quoted
string as the extension name.

#### Binder Changes

The binder passes `InstallStmt` and `LoadStmt` through to the executor as
utility statements (similar to `SET` or `PRAGMA`).

#### Executor Changes

```
InstallStmt  --> registry.Install(name) --> no-op, returns success
LoadStmt     --> registry.Load(name, registrar) --> activates extension
```

### duckdb_extensions() Integration

The existing `duckdb_extensions()` table function (already recognized in
`internal/binder/bind_stmt.go` and `internal/executor/`) is updated to query
the `ExtensionRegistry` instead of returning an empty list.

Columns returned (matching DuckDB):

| Column | Type | Description |
|--------|------|-------------|
| extension_name | VARCHAR | Name of the extension |
| loaded | BOOLEAN | Whether the extension is loaded |
| installed | BOOLEAN | Always true (compiled in) |
| install_path | VARCHAR | Always "(BUILT_IN)" |
| description | VARCHAR | Extension description |
| aliases | VARCHAR[] | Alternative names |
| extension_version | VARCHAR | Version string |

### Autoload Mechanism

When `autoload_extensions` is `true` (set via `SET autoload_extensions = true`):

1. During function resolution in the binder, if a function name is not found
   in the function catalog, the binder calls `registry.TryAutoload(funcName, registrar)`.
2. If the registry finds an extension that exports that function, it loads
   the extension (calling `extension.Load(registrar)` which registers all
   functions into the table function map and alias registry).
3. The binder retries function resolution exactly once.
4. If still not found, the normal "function not found" error is returned.

This mirrors DuckDB's behavior where `SELECT read_json('file.json')` auto-loads
the `json` extension if `autoload_extensions` is enabled.

See **Autoload Binder Integration** and **Thread-Safety of Autoload** under
Engine Integration for the precise call chain and concurrency guarantees.

### Built-in Extension Mappings

These adapters wrap existing dukdb-go packages as extensions:

| Extension Name | Go Package | Functions Provided |
|---------------|------------|-------------------|
| `json` | `internal/io/json` | `read_json`, `read_json_auto`, `read_ndjson`, `write_json` |
| `parquet` | `internal/io/parquet` | `read_parquet`, `write_parquet` |
| `csv` | (not a DuckDB extension, but registered) | `read_csv`, `read_csv_auto`, `write_csv` |
| `icu` | (stub -- future) | `icu_sort_key`, collation functions |
| `fts` | (stub -- future) | `fts_create`, `fts_drop`, `fts_match` |
| `httpfs` | (stub -- future) | S3/HTTP file system handler |

Stub extensions are registered with `StateAvailable` but their `Load()` returns
a "not yet implemented" error. This allows `duckdb_extensions()` to report them
while making their unavailability explicit.

### Engine Integration

The `Engine` (in `internal/engine/engine.go`) owns the `ExtensionRegistry`.
It is created during engine initialization:

```go
type Engine struct {
    // ... existing fields ...
    extensions *extension.Registry
}

func NewEngine() *Engine {
    e := &Engine{
        extensions: extension.NewRegistry(),
    }
    // Register built-in extensions
    e.extensions.Register(jsonext.New())
    e.extensions.Register(parquetext.New())
    // ... etc
    return e
}
```

The engine's `Registrar` implementation bridges extension function definitions
to the engine's internal function catalog. See the next section for details.

### Registrar-to-Function-Catalog Bridge

dukdb-go has two separate function dispatch systems that the `Registrar`
implementation must feed into:

1. **Table function switch** (`internal/executor/table_function_csv.go`,
   `executeTableFunctionScan`): A hard-coded `switch` on
   `plan.FunctionName` that dispatches `read_csv`, `read_json`,
   `read_parquet`, `duckdb_settings`, etc.
2. **PostgreSQL function alias registry**
   (`internal/postgres/functions/registry.go`,
   `FunctionAliasRegistry`): Maps function names (including aliases) to
   canonical DuckDB names for scalar functions resolved in the binder.

The engine implements `Registrar` as `EngineRegistrar`:

```go
// internal/engine/registrar.go

package engine

import (
    "fmt"
    "sync"

    "github.com/dukdb/dukdb-go/internal/extension"
    pgfunctions "github.com/dukdb/dukdb-go/internal/postgres/functions"
)

// TableFunctionEntry is a registered table function implementation.
type TableFunctionEntry struct {
    Def     extension.TableFunctionDef
    ExtName string // owning extension
}

// EngineRegistrar implements extension.Registrar by bridging extension
// function definitions into the engine's two function catalogs.
type EngineRegistrar struct {
    mu             sync.RWMutex
    extName        string // the extension currently being loaded
    tableFunctions map[string]*TableFunctionEntry   // name -> entry
    aliasRegistry  *pgfunctions.FunctionAliasRegistry
}

// RegisterTableFunction adds a table function to the TableFunctionRegistry
// map. The executor's executeTableFunctionScan checks this map BEFORE the
// hard-coded switch, so extension-registered functions take precedence for
// new names and can override built-in stubs.
func (r *EngineRegistrar) RegisterTableFunction(def extension.TableFunctionDef) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.tableFunctions[def.Name] = &TableFunctionEntry{Def: def, ExtName: r.extName}
    return nil
}

// RegisterScalarFunction adds a scalar function entry into the
// pgfunctions.FunctionAliasRegistry so the binder can resolve it.
func (r *EngineRegistrar) RegisterScalarFunction(def extension.ScalarFunctionDef) error {
    r.aliasRegistry.Register(&pgfunctions.FunctionAlias{
        PostgreSQLName: def.Name,
        DuckDBName:     def.Name,
        Category:       pgfunctions.DirectAlias,
        MinArgs:        len(def.Args),
        MaxArgs:        len(def.Args),
        Description:    fmt.Sprintf("provided by extension %s", r.extName),
    })
    return nil
}
```

The executor's `executeTableFunctionScan` is updated to check the
`tableFunctions` map before falling through to the built-in switch:

```go
func (e *Executor) executeTableFunctionScan(
    ctx *ExecutionContext,
    plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
    // Check extension-registered table functions first.
    if entry, ok := e.tableFunctions[strings.ToLower(plan.FunctionName)]; ok {
        return e.executeExtensionTableFunction(ctx, plan, entry)
    }
    // Existing hard-coded switch ...
    switch strings.ToLower(plan.FunctionName) {
    case "read_csv":
        // ... existing cases unchanged ...
    }
}
```

This design means:
- **Table functions** (read_json, read_parquet, etc.) register into
  `EngineRegistrar.tableFunctions` via `RegisterTableFunction`, and the
  executor checks this map before its built-in switch.
- **Scalar functions** register into the `FunctionAliasRegistry` via
  `RegisterScalarFunction`, making them visible to the binder's
  `bindFunctionCall` resolution path.
- **Aggregate functions** follow the same `FunctionAliasRegistry` path
  with an additional entry in the `isAggregateFunc` switch (or a
  parallel dynamic set).

### Autoload Binder Integration

Autoload hooks into the binder's `bindFunctionCall` method in
`internal/binder/bind_expr.go`. The precise call chain is:

```
binder.bindFunctionCall(f *parser.FunctionCall)
  |
  +-> Try UDF resolver (existing path)
  |     Found? -> return BoundScalarUDF
  |
  +-> Try inferFunctionResultType (existing built-in path)
  |     Known function? -> return BoundFunctionCall
  |
  +-> Function not recognized:
        |
        +-> registry.TryAutoload(funcName, registrar)
              |
              +-> Check autoload enabled? No -> return false
              +-> Look up funcIndex[funcName] -> extName
              +-> registry.Load(extName, registrar)
                    |
                    +-> extension.Load(registrar)
                          |
                          +-> registrar.RegisterTableFunction(...)
                          +-> registrar.RegisterScalarFunction(...)
                          +-> ... (all extension functions now registered)
              |
              +-> return true (extension loaded)
        |
        +-> Retry: re-enter bindFunctionCall(f)
              |
              +-> Now resolves via UDF resolver or built-in path
              +-> If still not found -> return "function not found" error
```

The binder needs access to the extension registry. This is provided via
a new field on the `Binder` struct:

```go
// In internal/binder/binder.go
type Binder struct {
    // ... existing fields ...
    extRegistry *extension.Registry  // for autoload
    registrar   extension.Registrar  // passed to TryAutoload
}
```

The retry is bounded: `bindFunctionCall` sets a `triedAutoload` flag to
prevent infinite recursion. If the function is still not found after one
autoload attempt, the normal error is returned.

For table functions specifically, the autoload hook is in
`bindTableFunction` (in `internal/binder/bind_stmt.go`), which follows
the same pattern: unrecognized table function name -> TryAutoload ->
retry once.

### Thread-Safety of Autoload

Multiple goroutines may trigger autoload for the same extension
concurrently (e.g., two connections both call `read_json` for the first
time). The registry uses per-extension mutexes to ensure that:

1. Only one goroutine loads a given extension at a time.
2. Other goroutines waiting for the same extension block until loading
   completes, then see `StateLoaded` and return immediately.
3. Goroutines loading different extensions do not block each other.

```go
// In Registry struct
type Registry struct {
    mu         sync.RWMutex
    extensions map[string]*Info
    funcIndex  map[string]string
    autoload   bool
}

type Info struct {
    Extension         Extension
    State             State
    ExportedFunctions []string
    loadMu            sync.Mutex // per-extension mutex for concurrent autoload
}

// Load activates an extension by name. Idempotent and safe for
// concurrent callers -- only the first caller performs the actual load.
func (r *Registry) Load(name string, registrar Registrar) error {
    r.mu.RLock()
    info, ok := r.extensions[name]
    r.mu.RUnlock()
    if !ok {
        return fmt.Errorf("extension %q not found", name)
    }

    // Per-extension lock: only one goroutine loads at a time.
    // Others block here and will see StateLoaded when they acquire.
    info.loadMu.Lock()
    defer info.loadMu.Unlock()

    if info.State == StateLoaded {
        return nil // another goroutine completed the load
    }

    if err := info.Extension.Load(registrar); err != nil {
        return fmt.Errorf("loading extension %q: %w", name, err)
    }

    r.mu.Lock()
    info.State = StateLoaded
    r.mu.Unlock()

    return nil
}
```

This pattern avoids holding the global `r.mu` write lock during the
potentially expensive `extension.Load()` call, which would block all
other registry operations (including `List()`, `Get()`, and concurrent
loads of unrelated extensions).

### Supersedes extension-system-v1.4.3

This proposal fully supersedes `spectr/changes/extension-system-v1.4.3/`.
That proposal designed a C-plugin-style system with dynamic binary loading,
code signing, sandboxing, and remote registries -- none of which is possible
under dukdb-go's pure Go / no-cgo constraint.

**Action required**: `extension-system-v1.4.3` should be archived. Move it
to `spectr/changes/archive/extension-system-v1.4.3/` and add an
`archived_reason` noting it is superseded by `add-extension-loading`.

## Context

- dukdb-go is a pure Go DuckDB implementation with zero cgo dependencies
- The existing `extension-system-v1.4.3` proposal designed a C-plugin-style
  system with sandboxing, code signing, and dynamic binary loading -- none of
  which is possible or desirable in pure Go. **This proposal supersedes
  `extension-system-v1.4.3`; that proposal should be archived.**
- The `internal/metadata/extensions.go` currently returns an empty list
- DuckDB users expect `INSTALL`/`LOAD` syntax to work and `duckdb_extensions()`
  to return meaningful results

## Goals / Non-Goals

**Goals:**
- DuckDB SQL syntax compatibility for `INSTALL`, `LOAD`, and `duckdb_extensions()`
- Clean `Extension` interface for Go-native extensions
- Autoload support matching DuckDB behavior
- Wrap existing I/O packages (json, parquet, csv) as extensions
- Thread-safe registry

**Non-Goals:**
- Dynamic loading of shared libraries or Go plugins (violates pure Go constraint)
- Code signing or signature verification (no untrusted code is loaded)
- Sandboxing or resource quotas (extensions run in-process, trusted)
- Remote extension repositories or marketplace
- Extension hot reload
- Extension dependency resolution (extensions are self-contained)

## Decisions

- **Compiled-in only**: Extensions are Go packages linked at compile time.
  `INSTALL` is a no-op because all extensions are always installed. This is
  the only approach compatible with the pure Go / no-cgo constraint.
  Alternative considered: Go `plugin` package -- rejected because it requires
  cgo on most platforms, is not supported in WASM/TinyGo, and has known
  stability issues.

- **INSTALL as no-op**: Rather than returning an error, `INSTALL` succeeds
  silently. This allows existing DuckDB SQL scripts to run unmodified.

- **Stub extensions for unimplemented features**: Extensions like `icu`, `fts`,
  and `httpfs` are registered but return an error on `Load()`. This gives
  accurate `duckdb_extensions()` output while clearly communicating what is
  available.

- **Autoload via function name index**: Each extension declares its exported
  function names at registration time (before `Load()`). The registry builds
  a function-name-to-extension-name index. This avoids loading all extensions
  just to check what functions they provide.

## Risks / Trade-offs

- **Extensions cannot be added at runtime**: Users must recompile to add
  extensions. This is inherent to the pure Go constraint and is documented.
  Mitigation: The `Extension` interface is public, so users can write and
  register their own extensions before opening a database.

- **Stub extensions may confuse users**: `duckdb_extensions()` shows `icu` as
  available but `LOAD icu` fails. Mitigation: The error message clearly states
  "extension icu is not yet implemented in dukdb-go".

## Open Questions

- Should `LOAD` auto-run on first query if the database was previously opened
  with extensions loaded? (Probably not needed for in-memory compiled-in model.)
- Should we support `FORCE INSTALL` syntax? (Likely no-op like `INSTALL`.)
