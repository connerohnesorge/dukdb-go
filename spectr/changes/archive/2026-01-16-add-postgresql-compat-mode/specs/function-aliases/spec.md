# Function Alias System for PostgreSQL Compatibility

## Overview

This specification defines the function alias system for PostgreSQL compatibility mode in dukdb-go. The system enables PostgreSQL function names to resolve to their DuckDB equivalents during query binding, ensuring that PostgreSQL applications, ORMs, and tools (like psql) can execute queries without modification.

The function alias system handles three categories of functions:

1. **Direct Aliases**: PostgreSQL function names that map directly to DuckDB functions with identical semantics
2. **Transformed Functions**: Functions that require argument rewriting or semantic transformation
3. **PostgreSQL System Functions**: pg_* functions that need custom implementations for compatibility

---

## Design Goals

- Transparent function resolution during query binding
- Support for qualified names (e.g., `pg_catalog.function_name`)
- Minimal performance overhead (O(1) lookup)
- Extensible architecture for adding new aliases
- Compatible with psql, ORMs, and common PostgreSQL clients
- Preserve DuckDB's native function behavior when aliases are not needed

---

## Function Categories

### Category 1: Direct Aliases

These functions have identical semantics between PostgreSQL and DuckDB, differing only in name.

| PostgreSQL Function | DuckDB Function | Notes |
|---------------------|-----------------|-------|
| `now()` | `current_timestamp` | Current timestamp with timezone |
| `current_timestamp` | `current_timestamp` | Already supported |
| `current_date` | `current_date` | Already supported |
| `current_time` | `current_time` | Already supported |
| `current_user` | `current_user` | Already supported |
| `session_user` | `session_user` | Already supported |
| `version()` | `version()` | Already supported |
| `concat()` | `concat()` | Already supported |
| `concat_ws()` | `concat_ws()` | Already supported |
| `coalesce()` | `coalesce()` | Already supported |
| `nullif()` | `nullif()` | Already supported |
| `greatest()` | `greatest()` | Already supported |
| `least()` | `least()` | Already supported |
| `substr()` | `substr()` | Already supported |
| `substring()` | `substring()` | Already supported |
| `position()` | `position()` | Already supported |
| `overlay()` | `overlay()` | Already supported |
| `trim()` | `trim()` | Already supported |
| `ltrim()` | `ltrim()` | Already supported |
| `rtrim()` | `rtrim()` | Already supported |
| `upper()` | `upper()` | Already supported |
| `lower()` | `lower()` | Already supported |
| `length()` | `length()` | Already supported |
| `char_length()` | `char_length()` | Already supported |
| `character_length()` | `character_length()` | Already supported |
| `abs()` | `abs()` | Already supported |
| `floor()` | `floor()` | To implement |
| `ceil()` / `ceiling()` | `ceil()` | To implement |
| `round()` | `round()` | To implement |
| `trunc()` | `trunc()` | To implement |
| `power()` / `pow()` | `pow()` | To implement |
| `sqrt()` | `sqrt()` | To implement |
| `mod()` | `mod()` | To implement |
| `random()` | `random()` | To implement |
| `date_part()` | `date_part()` | Already supported |
| `date_trunc()` | `date_trunc()` | Already supported |
| `extract()` | `extract()` | Already supported |
| `age()` | `age()` | Already supported |
| `to_char()` | `strftime()` | Requires format conversion |
| `to_date()` | `strptime()` | Requires format conversion |
| `to_timestamp()` | `to_timestamp()` | Already supported |
| `string_agg()` | `string_agg()` | Already supported |
| `array_agg()` | `list()` | DuckDB uses `list()` |

### Category 2: Transformed Functions

These functions require argument transformation or have different semantics.

| PostgreSQL Function | DuckDB Equivalent | Transformation Required |
|---------------------|-------------------|------------------------|
| `generate_series(start, stop)` | `range(start, stop + 1)` | PostgreSQL is inclusive, DuckDB exclusive |
| `generate_series(start, stop, step)` | `range(start, stop + sign(step), step)` | Adjust endpoint for inclusivity |
| `pg_catalog.pg_current_time()` | `current_time` | Strip pg_catalog prefix |
| `pg_catalog.now()` | `current_timestamp` | Strip prefix, alias to timestamp |
| `regexp_match()` | `regexp_matches()` | Different return type handling |
| `regexp_matches()` | `regexp_matches()` | Global flag handling differs |
| `split_part()` | `split_part()` | 1-based vs 0-based indexing check |
| `string_to_array()` | `string_split()` | Name mapping |
| `array_to_string()` | `list_aggregate()` with `string_agg` | Transform to aggregate |

### Category 3: PostgreSQL System Functions

These require custom implementations to provide PostgreSQL-compatible behavior.

#### Introspection Functions

| Function | Description | Implementation Approach |
|----------|-------------|------------------------|
| `pg_typeof(expression)` | Returns type name as text | Binder-level type inference |
| `pg_column_size(value)` | Estimated storage size | Approximate based on DuckDB types |
| `pg_database_size(name)` | Database size in bytes | Query storage layer |
| `pg_table_size(regclass)` | Table size excluding indexes | Query catalog/storage |
| `pg_relation_size(regclass)` | Relation size in bytes | Query storage layer |
| `pg_total_relation_size(regclass)` | Total size including indexes | Sum table + index sizes |
| `pg_indexes_size(regclass)` | Total size of indexes | Sum index sizes |

#### Catalog Functions

| Function | Description | Implementation Approach |
|----------|-------------|------------------------|
| `pg_get_constraintdef(oid, pretty)` | Constraint definition | Query internal catalog |
| `pg_get_indexdef(oid, column, pretty)` | Index definition | Query index catalog |
| `pg_get_viewdef(oid, pretty)` | View definition | Query view catalog |
| `pg_get_function_arguments(oid)` | Function arguments | Query UDF registry |
| `pg_get_function_result(oid)` | Function return type | Query UDF registry |
| `pg_get_expr(expr_text, relid)` | Deparse expression | Return stored expression |

#### Comment Functions

| Function | Description | Implementation Approach |
|----------|-------------|------------------------|
| `obj_description(oid, catalog)` | Object comment | Return empty (comments not supported) |
| `col_description(table_oid, column_num)` | Column comment | Return empty (comments not supported) |
| `shobj_description(oid, catalog)` | Shared object comment | Return empty |

#### Privilege Functions

| Function | Description | Implementation Approach |
|----------|-------------|------------------------|
| `has_table_privilege(user, table, priv)` | Check table privilege | Return true (no privilege system) |
| `has_column_privilege(user, table, col, priv)` | Check column privilege | Return true |
| `has_schema_privilege(user, schema, priv)` | Check schema privilege | Return true |
| `has_database_privilege(user, db, priv)` | Check database privilege | Return true |
| `has_function_privilege(user, func, priv)` | Check function privilege | Return true |
| `has_sequence_privilege(user, seq, priv)` | Check sequence privilege | Return true |
| `pg_has_role(user, role, priv)` | Check role membership | Return true |

#### Session/Backend Functions

| Function | Description | Implementation Approach |
|----------|-------------|------------------------|
| `current_schema()` | Current schema name | Return 'main' (default schema) |
| `current_schemas(include_implicit)` | Search path schemas | Return ['main', 'pg_catalog'] |
| `pg_backend_pid()` | Backend process ID | Return stable connection ID |
| `pg_postmaster_start_time()` | Server start time | Track server startup time |
| `inet_server_addr()` | Server IP address | Return connection info |
| `inet_server_port()` | Server port | Return listening port |
| `inet_client_addr()` | Client IP address | Return connection info |
| `inet_client_port()` | Client port | Return connection info |

#### Settings Functions

| Function | Description | Implementation Approach |
|----------|-------------|------------------------|
| `current_setting(name)` | Get setting value | Query settings registry |
| `set_config(name, value, is_local)` | Set configuration | Update settings registry |
| `pg_is_in_recovery()` | Recovery mode check | Return false |
| `pg_is_wal_replay_paused()` | WAL replay paused | Return false |

---

## Alias Registry Design

### Data Structure

```go
// FunctionAlias represents a function alias mapping.
type FunctionAlias struct {
    // PostgreSQLName is the PostgreSQL function name (lowercase).
    PostgreSQLName string

    // DuckDBName is the target DuckDB function name (uppercase).
    DuckDBName string

    // Category indicates the type of alias.
    Category AliasCategory

    // Transformer is an optional function to transform arguments.
    // nil for direct aliases.
    Transformer ArgumentTransformer

    // MinArgs is the minimum number of arguments.
    MinArgs int

    // MaxArgs is the maximum number of arguments (-1 for variadic).
    MaxArgs int

    // QualifiedNames lists alternate qualified forms (e.g., "pg_catalog.now").
    QualifiedNames []string
}

// AliasCategory identifies the type of function alias.
type AliasCategory int

const (
    // AliasDirect maps directly to a DuckDB function.
    AliasDirect AliasCategory = iota

    // AliasTransformed requires argument transformation.
    AliasTransformed

    // AliasSystemFunction is a PostgreSQL system function.
    AliasSystemFunction

    // AliasNotSupported returns an error when called.
    AliasNotSupported
)

// ArgumentTransformer transforms function arguments.
// Returns the transformed function name and arguments.
type ArgumentTransformer func(
    funcName string,
    args []BoundExpr,
) (string, []BoundExpr, error)
```

### Registry Implementation

```go
// FunctionAliasRegistry provides function alias resolution.
type FunctionAliasRegistry struct {
    // aliases maps lowercase PostgreSQL names to their aliases.
    aliases map[string]*FunctionAlias

    // qualifiedAliases maps qualified names (e.g., "pg_catalog.now").
    qualifiedAliases map[string]*FunctionAlias
}

// NewFunctionAliasRegistry creates a new registry with default aliases.
func NewFunctionAliasRegistry() *FunctionAliasRegistry {
    r := &FunctionAliasRegistry{
        aliases:          make(map[string]*FunctionAlias),
        qualifiedAliases: make(map[string]*FunctionAlias),
    }
    r.registerDefaults()
    return r
}

// Resolve looks up a function alias by name.
// Returns nil if no alias exists (use DuckDB function directly).
func (r *FunctionAliasRegistry) Resolve(name string) *FunctionAlias {
    // Check qualified names first
    if alias, ok := r.qualifiedAliases[strings.ToLower(name)]; ok {
        return alias
    }
    // Check unqualified names
    return r.aliases[strings.ToLower(name)]
}

// Register adds a function alias to the registry.
func (r *FunctionAliasRegistry) Register(alias *FunctionAlias) {
    r.aliases[strings.ToLower(alias.PostgreSQLName)] = alias
    for _, qname := range alias.QualifiedNames {
        r.qualifiedAliases[strings.ToLower(qname)] = alias
    }
}
```

### Default Alias Registration

```go
func (r *FunctionAliasRegistry) registerDefaults() {
    // Direct aliases
    r.Register(&FunctionAlias{
        PostgreSQLName: "now",
        DuckDBName:     "CURRENT_TIMESTAMP",
        Category:       AliasDirect,
        MinArgs:        0,
        MaxArgs:        0,
        QualifiedNames: []string{"pg_catalog.now"},
    })

    r.Register(&FunctionAlias{
        PostgreSQLName: "array_agg",
        DuckDBName:     "LIST",
        Category:       AliasDirect,
        MinArgs:        1,
        MaxArgs:        1,
    })

    // Transformed functions
    r.Register(&FunctionAlias{
        PostgreSQLName: "generate_series",
        DuckDBName:     "RANGE",
        Category:       AliasTransformed,
        Transformer:    transformGenerateSeries,
        MinArgs:        2,
        MaxArgs:        3,
    })

    // System functions
    r.Register(&FunctionAlias{
        PostgreSQLName: "pg_typeof",
        DuckDBName:     "PG_TYPEOF",
        Category:       AliasSystemFunction,
        MinArgs:        1,
        MaxArgs:        1,
        QualifiedNames: []string{"pg_catalog.pg_typeof"},
    })

    r.Register(&FunctionAlias{
        PostgreSQLName: "current_schema",
        DuckDBName:     "CURRENT_SCHEMA",
        Category:       AliasSystemFunction,
        MinArgs:        0,
        MaxArgs:        0,
    })

    r.Register(&FunctionAlias{
        PostgreSQLName: "current_schemas",
        DuckDBName:     "CURRENT_SCHEMAS",
        Category:       AliasSystemFunction,
        MinArgs:        1,
        MaxArgs:        1,
    })

    // Privilege functions (always return true)
    r.Register(&FunctionAlias{
        PostgreSQLName: "has_table_privilege",
        DuckDBName:     "HAS_TABLE_PRIVILEGE",
        Category:       AliasSystemFunction,
        MinArgs:        2,
        MaxArgs:        3,
    })

    // More registrations...
}
```

---

## Argument Transformation

### generate_series Transformation

PostgreSQL's `generate_series(start, stop)` is inclusive (includes both start and stop), while DuckDB's `range(start, stop)` is exclusive (excludes stop). The transformer adjusts for this difference.

```go
// transformGenerateSeries converts generate_series to range.
// generate_series(1, 5) -> range(1, 6)    -- adds 1 to stop
// generate_series(1, 5, 2) -> range(1, 6, 2)  -- adds step direction to stop
func transformGenerateSeries(
    funcName string,
    args []BoundExpr,
) (string, []BoundExpr, error) {
    if len(args) < 2 {
        return "", nil, fmt.Errorf("generate_series requires at least 2 arguments")
    }

    newArgs := make([]BoundExpr, len(args))
    copy(newArgs, args)

    // Transform: stop -> stop + 1 (or stop + sign(step))
    // For constant values, we can compute at bind time
    // For expressions, we wrap in an addition expression

    if len(args) == 2 {
        // generate_series(start, stop) -> range(start, stop + 1)
        newArgs[1] = &BoundBinaryExpr{
            Left:  args[1],
            Op:    parser.OpAdd,
            Right: &BoundLiteral{Value: int64(1), ValType: dukdb.TYPE_BIGINT},
        }
    } else if len(args) == 3 {
        // generate_series(start, stop, step) -> range(start, stop + sign(step), step)
        // For simplicity, add 1 (assuming positive step)
        // Full implementation would evaluate sign(step) at runtime
        newArgs[1] = &BoundBinaryExpr{
            Left:  args[1],
            Op:    parser.OpAdd,
            Right: &BoundLiteral{Value: int64(1), ValType: dukdb.TYPE_BIGINT},
        }
    }

    return "RANGE", newArgs, nil
}
```

### to_char Format Transformation

PostgreSQL's `to_char(timestamp, format)` uses different format codes than DuckDB's `strftime(format, timestamp)`. The transformer maps format codes and swaps argument order.

```go
// PostgreSQL to strftime format code mapping
var pgToStrftimeFormat = map[string]string{
    "YYYY": "%Y",     // 4-digit year
    "YY":   "%y",     // 2-digit year
    "MM":   "%m",     // Month (01-12)
    "DD":   "%d",     // Day (01-31)
    "HH24": "%H",     // Hour (00-23)
    "HH12": "%I",     // Hour (01-12)
    "HH":   "%I",     // Hour (01-12)
    "MI":   "%M",     // Minute (00-59)
    "SS":   "%S",     // Second (00-59)
    "MS":   "%f",     // Milliseconds (approximated by microseconds)
    "US":   "%f",     // Microseconds
    "AM":   "%p",     // AM/PM
    "PM":   "%p",     // AM/PM
    "TZ":   "%Z",     // Time zone name
    "Day":  "%A",     // Full weekday name
    "Mon":  "%b",     // Abbreviated month name
    "Month": "%B",    // Full month name
}

// transformToChar converts to_char to strftime.
// to_char(timestamp, 'YYYY-MM-DD') -> strftime('%Y-%m-%d', timestamp)
func transformToChar(
    funcName string,
    args []BoundExpr,
) (string, []BoundExpr, error) {
    if len(args) != 2 {
        return "", nil, fmt.Errorf("to_char requires exactly 2 arguments")
    }

    // Swap argument order: to_char(value, format) -> strftime(format, value)
    // Also transform format string if it's a literal
    formatArg := args[1]
    if lit, ok := formatArg.(*BoundLiteral); ok {
        if formatStr, ok := lit.Value.(string); ok {
            transformed := transformPGFormatToStrftime(formatStr)
            formatArg = &BoundLiteral{Value: transformed, ValType: dukdb.TYPE_VARCHAR}
        }
    }

    return "STRFTIME", []BoundExpr{formatArg, args[0]}, nil
}

// transformPGFormatToStrftime converts PostgreSQL format codes to strftime.
func transformPGFormatToStrftime(pgFormat string) string {
    result := pgFormat
    for pg, sf := range pgToStrftimeFormat {
        result = strings.ReplaceAll(result, pg, sf)
    }
    return result
}
```

---

## System Function Implementations

### pg_typeof Implementation

```go
// evalPgTypeof returns the PostgreSQL type name for a value.
// This is implemented at the binder level since it needs type information.
func evalPgTypeof(args []any) (any, error) {
    if len(args) != 1 {
        return nil, fmt.Errorf("pg_typeof requires exactly 1 argument")
    }

    // Return the PostgreSQL type name for the value
    switch args[0].(type) {
    case nil:
        return "unknown", nil
    case bool:
        return "boolean", nil
    case int8, int16:
        return "smallint", nil
    case int, int32:
        return "integer", nil
    case int64:
        return "bigint", nil
    case float32:
        return "real", nil
    case float64:
        return "double precision", nil
    case string:
        return "text", nil
    case []byte:
        return "bytea", nil
    case time.Time:
        return "timestamp without time zone", nil
    default:
        return "unknown", nil
    }
}
```

### current_schema Implementation

```go
// evalCurrentSchema returns the current schema name.
func evalCurrentSchema(ctx *ExecutionContext, args []any) (any, error) {
    // Return the current schema (default to 'main')
    if ctx.CurrentSchema != "" {
        return ctx.CurrentSchema, nil
    }
    return "main", nil
}
```

### current_schemas Implementation

```go
// evalCurrentSchemas returns the current search path schemas.
func evalCurrentSchemas(ctx *ExecutionContext, args []any) (any, error) {
    if len(args) != 1 {
        return nil, fmt.Errorf("current_schemas requires exactly 1 argument")
    }

    includeImplicit := false
    if b, ok := args[0].(bool); ok {
        includeImplicit = b
    }

    // Build search path
    schemas := []string{"main"}
    if includeImplicit {
        schemas = append(schemas, "pg_catalog")
    }

    return schemas, nil
}
```

### Privilege Functions Implementation

Since dukdb-go does not implement a privilege system, privilege functions always return true.

```go
// evalHasTablePrivilege checks table privilege (always returns true).
func evalHasTablePrivilege(args []any) (any, error) {
    // No privilege system - always return true
    return true, nil
}

// evalHasSchemaPrivilege checks schema privilege (always returns true).
func evalHasSchemaPrivilege(args []any) (any, error) {
    return true, nil
}

// evalHasDatabasePrivilege checks database privilege (always returns true).
func evalHasDatabasePrivilege(args []any) (any, error) {
    return true, nil
}
```

### pg_backend_pid Implementation

```go
// evalPgBackendPid returns a stable backend process ID for the connection.
func evalPgBackendPid(ctx *ExecutionContext, args []any) (any, error) {
    // Return the connection ID as the backend PID
    return int64(ctx.ConnectionID), nil
}
```

---

## Binder Integration

### Function Resolution Flow

```
SQL Query
    |
    v
Parser (parse function call AST)
    |
    v
Binder.bindFunctionCall()
    |
    +---> Check FunctionAliasRegistry
    |           |
    |           +---> Alias found?
    |                   |
    |                   Yes --> Transform to DuckDB function
    |                   |       Apply ArgumentTransformer if needed
    |                   |
    |                   No --> Use function name as-is
    |
    v
BoundFunctionCall with resolved name
    |
    v
Executor.evaluateFunctionCall()
```

### Binder Modifications

```go
// bindFunctionCall binds a function call expression.
func (b *Binder) bindFunctionCall(f *parser.FunctionCall) (BoundExpr, error) {
    // Build qualified name if table prefix exists
    funcName := f.Name
    if f.Schema != "" {
        funcName = f.Schema + "." + f.Name
    }

    // Check PostgreSQL compatibility aliases
    if b.pgCompatMode {
        if alias := b.aliasRegistry.Resolve(funcName); alias != nil {
            return b.bindAliasedFunction(f, alias)
        }
    }

    // Continue with standard function binding...
    // (existing logic)
}

// bindAliasedFunction binds a function using its alias mapping.
func (b *Binder) bindAliasedFunction(
    f *parser.FunctionCall,
    alias *FunctionAlias,
) (BoundExpr, error) {
    // Validate argument count
    argCount := len(f.Args)
    if argCount < alias.MinArgs || (alias.MaxArgs >= 0 && argCount > alias.MaxArgs) {
        return nil, b.errorf(
            "function %s requires %d-%d arguments, got %d",
            f.Name, alias.MinArgs, alias.MaxArgs, argCount,
        )
    }

    // Bind arguments first
    args := make([]BoundExpr, len(f.Args))
    for i, arg := range f.Args {
        bound, err := b.bindExpr(arg, dukdb.TYPE_ANY)
        if err != nil {
            return nil, err
        }
        args[i] = bound
    }

    // Apply transformation if needed
    targetName := alias.DuckDBName
    if alias.Transformer != nil {
        var err error
        targetName, args, err = alias.Transformer(f.Name, args)
        if err != nil {
            return nil, err
        }
    }

    // Build bound function call with target name
    resType := inferFunctionResultType(targetName, args)

    return &BoundFunctionCall{
        Name:     targetName,
        Args:     args,
        Distinct: f.Distinct,
        Star:     f.Star,
        ResType:  resType,
    }, nil
}
```

---

## Qualified Name Handling

PostgreSQL clients often use fully qualified names like `pg_catalog.function_name`. The alias system handles these by:

1. Stripping the `pg_catalog.` prefix for standard functions
2. Maintaining qualified name mappings for pg_* functions
3. Supporting schema-qualified function lookups

### Supported Qualified Prefixes

| Prefix | Description | Handling |
|--------|-------------|----------|
| `pg_catalog.` | PostgreSQL system catalog | Strip prefix, resolve alias |
| `public.` | Default schema | Strip prefix, resolve normally |
| `information_schema.` | Information schema | Map to internal views |

### Schema Resolution Logic

```go
// resolveQualifiedFunction resolves a schema-qualified function name.
func (r *FunctionAliasRegistry) resolveQualifiedFunction(
    schema string,
    funcName string,
) (*FunctionAlias, bool) {
    // Build full qualified name
    qualifiedName := strings.ToLower(schema + "." + funcName)

    // Check qualified alias map
    if alias, ok := r.qualifiedAliases[qualifiedName]; ok {
        return alias, true
    }

    // For pg_catalog, try stripping prefix
    if strings.EqualFold(schema, "pg_catalog") {
        if alias := r.aliases[strings.ToLower(funcName)]; alias != nil {
            return alias, true
        }
    }

    return nil, false
}
```

---

## Implementation Plan

### File Structure

```
internal/postgres/
    compat/
        alias_registry.go     # FunctionAliasRegistry implementation
        aliases_direct.go     # Direct alias registrations
        aliases_transform.go  # Transformation functions
        aliases_system.go     # System function registrations

    functions/
        pg_typeof.go          # pg_typeof implementation
        pg_current.go         # current_schema, current_schemas
        pg_privilege.go       # has_*_privilege functions
        pg_catalog.go         # pg_get_* catalog functions
        pg_size.go            # pg_*_size functions
        pg_backend.go         # Backend/session functions
```

### Implementation Phases

#### Phase 1: Registry Infrastructure

1. Implement `FunctionAliasRegistry` structure
2. Define `FunctionAlias` and `AliasCategory` types
3. Implement `Resolve()` and `Register()` methods
4. Add qualified name resolution

**Deliverables:**
- `internal/postgres/compat/alias_registry.go`
- Unit tests for registry operations

#### Phase 2: Direct Aliases

1. Register all direct alias mappings
2. Integrate registry with binder
3. Add pgCompatMode flag to binder
4. Test with psql client

**Deliverables:**
- `internal/postgres/compat/aliases_direct.go`
- Binder integration in `internal/binder/bind_expr.go`
- Integration tests with PostgreSQL function names

#### Phase 3: Argument Transformations

1. Implement `generate_series` transformer
2. Implement `to_char` / `to_date` format conversion
3. Implement `regexp_*` function mapping
4. Add tests for edge cases

**Deliverables:**
- `internal/postgres/compat/aliases_transform.go`
- Comprehensive transformation tests

#### Phase 4: System Functions

1. Implement `pg_typeof` at binder level
2. Implement `current_schema`, `current_schemas`
3. Implement privilege functions (return true)
4. Implement `pg_backend_pid`

**Deliverables:**
- `internal/postgres/functions/` implementations
- Executor integration for new functions

#### Phase 5: Catalog Functions

1. Implement `pg_get_constraintdef`
2. Implement `pg_get_indexdef`
3. Implement `pg_get_viewdef`
4. Return empty strings for comment functions

**Deliverables:**
- `internal/postgres/functions/pg_catalog.go`
- Catalog integration tests

#### Phase 6: Size Functions

1. Implement `pg_database_size`
2. Implement `pg_table_size`
3. Implement `pg_relation_size`
4. Implement `pg_indexes_size`

**Deliverables:**
- `internal/postgres/functions/pg_size.go`
- Storage layer integration

---

## Testing Strategy

### Unit Tests

1. **Registry Tests**
   - Resolve direct aliases correctly
   - Resolve qualified names correctly
   - Handle unknown functions gracefully
   - Validate argument count checking

2. **Transformation Tests**
   - `generate_series` produces correct `range` calls
   - `to_char` format conversion is accurate
   - Edge cases (NULL args, empty strings)

3. **System Function Tests**
   - `pg_typeof` returns correct type names
   - `current_schema` returns expected value
   - Privilege functions return true

### Integration Tests

1. **psql Client Tests**
   - `SELECT now()` returns current timestamp
   - `SELECT pg_typeof(123)` returns 'integer'
   - `SELECT current_schema()` returns 'main'
   - `SELECT has_table_privilege(...)` returns true

2. **ORM Compatibility Tests**
   - GORM model creation with sequences
   - SQLAlchemy schema introspection
   - TypeORM entity scanning

3. **Edge Cases**
   - Nested function calls with aliases
   - Aggregates with aliases (`array_agg`)
   - Window functions with PostgreSQL names

---

## Compatibility Notes

### psql Startup Queries

psql sends several queries on connection. The function alias system must handle:

```sql
-- psql startup queries that use pg_* functions
SELECT pg_catalog.pg_encoding_to_char(encoding) FROM pg_catalog.pg_database WHERE datname = current_database();
SELECT pg_catalog.set_config('search_path', '', false);
SELECT current_schema();
```

### ORM Schema Introspection

ORMs query PostgreSQL system catalogs and functions:

```sql
-- GORM/SQLAlchemy introspection
SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid = '...'::regclass;
SELECT obj_description('tablename'::regclass, 'pg_class');
SELECT has_schema_privilege(current_user, 'public', 'CREATE');
```

### Known Limitations

1. **pg_catalog Tables**: Function aliases do not provide pg_catalog table emulation. Use system views spec for that.
2. **Custom Types**: `pg_typeof` returns DuckDB type names, not custom domain types.
3. **Exact Format Compatibility**: Some `to_char` formats may have minor differences.
4. **Binary Format**: System functions return text format only.

---

## References

- [PostgreSQL Functions and Operators](https://www.postgresql.org/docs/current/functions.html)
- [PostgreSQL System Information Functions](https://www.postgresql.org/docs/current/functions-info.html)
- [PostgreSQL System Administration Functions](https://www.postgresql.org/docs/current/functions-admin.html)
- [DuckDB Functions](https://duckdb.org/docs/sql/functions/overview)
- [psql Startup Sequence](https://www.postgresql.org/docs/current/libpq-connect.html)
