# System Functions and Metadata Tables Design

## Architecture Overview

The system functions and metadata tables implementation follows a layered architecture that integrates with the existing catalog system while providing efficient access to database metadata.

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL Layer                               │
│  SELECT * FROM duckdb_tables()                             │
│  PRAGMA table_info('users')                                │
│  SELECT * FROM information_schema.tables                   │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│              System Function Registry                      │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐          │
│  │ duckdb_*()  │ │ PRAGMA      │ │ Metadata    │          │
│  │ Functions   │ │ Support     │ │ Views       │          │
│  └─────────────┘ └─────────────┘ └─────────────┘          │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│              System Catalog Manager                        │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐          │
│  │ Catalog     │ │ Function    │ │ Metadata    │          │
│  │ Storage     │ │ Registry    │ │ Cache       │          │
│  └─────────────┘ └─────────────┘ └─────────────┘          │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│              Storage Layer                                 │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐          │
│  │ Catalog     │ │ System      │ │ Metadata    │          │
│  │ Tables      │ │ Tables      │ │ Indexes     │          │
│  └─────────────┘ └─────────────┘ └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

## System Catalog Storage Design

### Catalog Metadata Tables

The system catalog is extended with dedicated tables for storing system metadata:

```sql
-- Core system metadata tables
CREATE TABLE __system_catalog (
    catalog_id BIGINT PRIMARY KEY,
    catalog_name VARCHAR NOT NULL,
    catalog_type VARCHAR NOT NULL, -- 'database', 'schema', 'table', 'view', 'index', 'sequence'
    parent_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSON
);

-- Function registry table
CREATE TABLE __system_functions (
    function_id BIGINT PRIMARY KEY,
    function_name VARCHAR NOT NULL,
    schema_name VARCHAR NOT NULL,
    function_type VARCHAR NOT NULL, -- 'scalar', 'aggregate', 'table', 'window'
    return_type VARCHAR,
    parameters JSON,
    description TEXT,
    example TEXT,
    category VARCHAR,
    tags JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Settings registry table
CREATE TABLE __system_settings (
    setting_name VARCHAR PRIMARY KEY,
    setting_value VARCHAR,
    setting_type VARCHAR NOT NULL,
    description TEXT,
    default_value VARCHAR,
    min_value VARCHAR,
    max_value VARCHAR,
    options JSON,
    requires_restart BOOLEAN DEFAULT FALSE,
    is_runtime BOOLEAN DEFAULT TRUE
);

-- Dependency tracking
CREATE TABLE __system_dependencies (
    dependency_id BIGINT PRIMARY KEY,
    object_id BIGINT NOT NULL,
    object_type VARCHAR NOT NULL,
    dependency_object_id BIGINT NOT NULL,
    dependency_type VARCHAR NOT NULL,
    dependency_kind VARCHAR NOT NULL -- 'normal', 'automatic', 'internal'
);
```

### Storage Layout

```
internal/catalog/system/
├── catalog_manager.go      # System catalog manager
├── metadata_storage.go     # Metadata persistence layer
├── cache.go               # Metadata caching
├── function_registry.go   # Function registration and lookup
├── dependency_tracker.go  # Object dependency tracking
└── system_tables.go       # System table definitions
```

## Function Metadata Storage

### Function Registry Structure

Functions are stored with comprehensive metadata for introspection:

```go
type FunctionMetadata struct {
    ID           int64
    Name         string
    Schema       string
    Type         FunctionType // SCALAR, AGGREGATE, TABLE, WINDOW
    ReturnType   types.Type
    Parameters   []ParameterMetadata
    Description  string
    Example      string
    Category     string
    Tags         []string
    Variadic     bool
    Volatility   Volatility // IMMUTABLE, STABLE, VOLATILE
    SQL          string     // For SQL functions
    Language     string     // SQL, C, INTERNAL
}

type ParameterMetadata struct {
    Name         string
    Type         types.Type
    Mode         ParameterMode // IN, OUT, INOUT, VARIADIC
    DefaultValue interface{}
    Description  string
}
```

### Function Categories

Functions are organized into categories for better organization:

```go
const (
    CategoryAggregate    = "Aggregate"
    CategoryArray        = "Array"
    CategoryBit          = "Bit"
    CategoryCollation    = "Collation"
    CategoryConversion   = "Conversion"
    CategoryDateTime     = "Date/Time"
    CategoryEnum         = "Enum"
    CategoryFullText     = "Full Text Search"
    CategoryJSON         = "JSON"
    CategoryLogical      = "Logical"
    CategoryMath         = "Mathematical"
    CategoryString       = "String"
    CategoryStruct       = "Struct"
    CategorySystem       = "System"
    CategoryUtility      = "Utility"
    CategoryWindow       = "Window"
)
```

### Function Registration

Functions are registered at startup and stored in the system catalog:

```go
type FunctionRegistry interface {
    // Register a function
    RegisterFunction(metadata FunctionMetadata) error

    // Lookup functions
    GetFunction(name string, schema string) (*FunctionMetadata, error)
    ListFunctions(schema string) ([]FunctionMetadata, error)
    SearchFunctions(pattern string) ([]FunctionMetadata, error)

    // Function resolution
    ResolveFunction(name string, args []types.Type) (*FunctionMetadata, error)

    // Categories
    GetFunctionsByCategory(category string) ([]FunctionMetadata, error)
    GetCategories() []string
}
```

## Runtime Function Resolution

### Resolution Process

Function resolution follows a multi-step process:

1. **Name Resolution**: Resolve function name to candidate functions
2. **Type Matching**: Match argument types to function signatures
3. **Overload Selection**: Select best match among overloads
4. **Cast Insertion**: Insert implicit casts if necessary

```go
type FunctionResolver struct {
    registry FunctionRegistry
    catalog  Catalog
}

func (r *FunctionResolver) ResolveFunction(
    name string,
    args []Expression,
    schema string,
) (*ResolvedFunction, error) {
    // 1. Get candidate functions
    candidates, err := r.getCandidates(name, schema)
    if err != nil {
        return nil, err
    }

    // 2. Score candidates
    scores := make([]int, len(candidates))
    for i, candidate := range candidates {
        scores[i] = r.scoreCandidate(candidate, args)
    }

    // 3. Select best match
    bestIdx := r.selectBestMatch(scores)
    if bestIdx == -1 {
        return nil, ErrNoMatchingFunction
    }

    // 4. Apply casts if needed
    casts := r.inferCasts(candidates[bestIdx], args)

    return &ResolvedFunction{
        Metadata: candidates[bestIdx],
        Casts:    casts,
    }, nil
}
```

### Type Scoring Algorithm

The scoring algorithm determines the best function match:

```go
func (r *FunctionResolver) scoreCandidate(
    metadata FunctionMetadata,
    args []Expression,
) int {
    score := 0

    // Exact type match: +10
    // Implicit cast: +5
    // Explicit cast: +1
    // No match: -1000

    for i, param := range metadata.Parameters {
        if i >= len(args) {
            break
        }

        argType := args[i].Type()
        if argType.Equals(param.Type) {
            score += 10
        } else if r.canImplicitlyCast(argType, param.Type) {
            score += 5
        } else if r.canExplicitlyCast(argType, param.Type) {
            score += 1
        } else {
            return -1000
        }
    }

    return score
}
```

## Dynamic Metadata Queries

### Metadata Query Engine

The metadata query engine provides efficient access to system metadata:

```go
type MetadataQueryEngine struct {
    catalog  Catalog
    cache    *MetadataCache
    executor Executor
}

type MetadataQuery struct {
    Target   MetadataTarget // TABLES, COLUMNS, FUNCTIONS, etc.
    Filters  []MetadataFilter
    OrderBy  []OrderByClause
    Limit    int
    Offset   int
}

type MetadataFilter struct {
    Column   string
    Operator FilterOperator // EQ, NE, LT, GT, LIKE, IN
    Value    interface{}
}
```

### Query Optimization

Metadata queries are optimized using:

1. **Indexes**: B-tree indexes on frequently queried columns
2. **Caching**: LRU cache for hot metadata
3. **Materialized Views**: Pre-computed joins for complex queries
4. **Lazy Loading**: Load metadata on demand

```go
type MetadataCache struct {
    tables      *lru.Cache[string, []TableMetadata]
    columns     *lru.Cache[string, []ColumnMetadata]
    functions   *lru.Cache[string, []FunctionMetadata]
    settings    *lru.Cache[string, SettingMetadata]

    hitRate     atomic.Uint64
    missRate    atomic.Uint64
}
```

## Integration with Existing Catalog

### Catalog Extension

The existing catalog is extended to support system metadata:

```go
type ExtendedCatalog interface {
    Catalog // Existing catalog interface

    // System metadata access
    GetSystemTables() []TableMetadata
    GetSystemFunctions() []FunctionMetadata
    GetSystemSettings() []SettingMetadata

    // Dynamic metadata
    QueryMetadata(query MetadataQuery) (*DataChunk, error)

    // Dependency tracking
    GetDependencies(objectID int64) ([]Dependency, error)
    GetDependents(objectID int64) ([]Dependency, error)
}
```

### System Table Integration

System tables are integrated as special catalog objects:

```go
type SystemTable struct {
    name     string
    schema   string
    columns  []Column
    resolver MetadataResolver
}

func (t *SystemTable) Scan(
    ctx context.Context,
    txn Transaction,
    columns []string,
    filters []Filter,
) (*DataChunk, error) {
    // Delegate to metadata resolver
    return t.resolver.Resolve(ctx, txn, columns, filters)
}
```

## System Function Implementations

### duckdb_settings()

Returns database settings with their current values:

```go
func duckdbSettings(ctx Context, args []Expression) (*DataChunk, error) {
    settings := ctx.GetSettings()

    chunk := NewDataChunk(len(settings))
    chunk.AddVector("name", types.VARCHAR)
    chunk.AddVector("value", types.VARCHAR)
    chunk.AddVector("description", types.VARCHAR)
    chunk.AddVector("input_type", types.VARCHAR)
    chunk.AddVector("default_value", types.VARCHAR)
    chunk.AddVector("min_value", types.VARCHAR)
    chunk.AddVector("max_value", types.VARCHAR)
    chunk.AddVector("options", types.JSON)
    chunk.AddVector("requires_restart", types.BOOLEAN)

    for _, setting := range settings {
        chunk.Append([]interface{}{
            setting.Name,
            setting.Value,
            setting.Description,
            setting.Type,
            setting.Default,
            setting.Min,
            setting.Max,
            setting.Options,
            setting.RequiresRestart,
        })
    }

    return chunk, nil
}
```

### duckdb_functions()

Returns all available functions:

```go
func duckdbFunctions(ctx Context, args []Expression) (*DataChunk, error) {
    functions := ctx.GetFunctionRegistry().ListFunctions()

    chunk := NewDataChunk(len(functions))
    chunk.AddVector("schema_name", types.VARCHAR)
    chunk.AddVector("function_name", types.VARCHAR)
    chunk.AddVector("function_type", types.VARCHAR)
    chunk.AddVector("description", types.VARCHAR)
    chunk.AddVector("return_type", types.VARCHAR)
    chunk.AddVector("parameters", types.JSON)
    chunk.AddVector("parameter_types", types.JSON)
    chunk.AddVector("varargs", types.BOOLEAN)
    chunk.AddVector("macro_definition", types.VARCHAR)
    chunk.AddVector("has_side_effects", types.BOOLEAN)

    for _, fn := range functions {
        chunk.Append([]interface{}{
            fn.Schema,
            fn.Name,
            string(fn.Type),
            fn.Description,
            fn.ReturnType.String(),
            fn.ParametersJSON(),
            fn.ParameterTypesJSON(),
            fn.Variadic,
            fn.MacroDefinition,
            fn.HasSideEffects,
        })
    }

    return chunk, nil
}
```

### duckdb_tables()

Returns all tables in the database:

```go
func duckdbTables(ctx Context, args []Expression) (*DataChunk, error) {
    tables := ctx.GetCatalog().GetTables()

    chunk := NewDataChunk(len(tables))
    chunk.AddVector("schema_name", types.VARCHAR)
    chunk.AddVector("table_name", types.VARCHAR)
    chunk.AddVector("table_type", types.VARCHAR)
    chunk.AddVector("temporary", types.BOOLEAN)
    chunk.AddVector("internal", types.BOOLEAN)
    chunk.AddVector("cardinality", types.BIGINT)
    chunk.AddVector("column_count", types.BIGINT)
    chunk.AddVector("comment", types.VARCHAR)

    for _, table := range tables {
        chunk.Append([]interface{}{
            table.Schema,
            table.Name,
            table.Type,
            table.Temporary,
            table.Internal,
            table.Cardinality,
            table.ColumnCount,
            table.Comment,
        })
    }

    return chunk, nil
}
```

## PRAGMA Implementation

### PRAGMA Framework

PRAGMA statements are implemented as a special statement type:

```go
type PragmaStatement struct {
    Name      string
    Arguments []Expression
}

type PragmaHandler interface {
    Execute(ctx Context, stmt PragmaStatement) (Result, error)
    GetName() string
    GetDescription() string
}
```

### PRAGMA table_info

Returns column information for a table:

```go
type TableInfoPragma struct{}

func (p *TableInfoPragma) Execute(
    ctx Context,
    stmt PragmaStatement,
) (Result, error) {
    if len(stmt.Arguments) != 1 {
        return nil, ErrInvalidArgumentCount
    }

    tableName, err := stmt.Arguments[0].Evaluate(ctx)
    if err != nil {
        return nil, err
    }

    table := ctx.GetCatalog().GetTable(tableName.(string))
    if table == nil {
        return nil, ErrTableNotFound
    }

    chunk := NewDataChunk(len(table.Columns))
    chunk.AddVector("cid", types.BIGINT)
    chunk.AddVector("name", types.VARCHAR)
    chunk.AddVector("type", types.VARCHAR)
    chunk.AddVector("notnull", types.BOOLEAN)
    chunk.AddVector("dflt_value", types.VARCHAR)
    chunk.AddVector("pk", types.BOOLEAN)

    for i, col := range table.Columns {
        chunk.Append([]interface{}{
            int64(i),
            col.Name,
            col.Type.String(),
            col.NotNull,
            col.DefaultValue,
            col.IsPrimaryKey,
        })
    }

    return NewDataChunkResult(chunk), nil
}
```

## Metadata Table Views

### information_schema Implementation

information_schema views are implemented as system views:

```go
func createInformationSchemaViews(catalog Catalog) error {
    views := []SystemView{
        {
            Name:   "tables",
            Schema: "information_schema",
            Query: `
                SELECT
                    database_name AS table_catalog,
                    schema_name AS table_schema,
                    table_name,
                    CASE table_type
                        WHEN 'BASE TABLE' THEN 'TABLE'
                        WHEN 'VIEW' THEN 'VIEW'
                        ELSE table_type
                    END AS table_type,
                    NULL AS self_referencing_column_name,
                    NULL AS reference_generation,
                    NULL AS user_defined_type_catalog,
                    NULL AS user_defined_type_schema,
                    NULL AS user_defined_type_name,
                    'NO' AS is_insertable_into,
                    'NO' AS is_typed,
                    CASE temporary
                        WHEN TRUE THEN 'YES'
                        ELSE 'NO'
                    END AS commit_action
                FROM duckdb_tables()
            `,
        },
        {
            Name:   "columns",
            Schema: "information_schema",
            Query: `
                SELECT
                    t.database_name AS table_catalog,
                    t.schema_name AS table_schema,
                    t.table_name,
                    c.column_name,
                    c.ordinal_position,
                    c.column_default AS column_default,
                    CASE c.is_nullable
                        WHEN TRUE THEN 'YES'
                        ELSE 'NO'
                    END AS is_nullable,
                    c.data_type AS data_type,
                    c.character_maximum_length,
                    c.character_octet_length,
                    c.numeric_precision,
                    c.numeric_precision_radix,
                    c.numeric_scale,
                    c.datetime_precision,
                    c.interval_type,
                    c.interval_precision,
                    NULL AS character_set_catalog,
                    NULL AS character_set_schema,
                    NULL AS character_set_name,
                    NULL AS collation_catalog,
                    NULL AS collation_schema,
                    NULL AS collation_name,
                    NULL AS domain_catalog,
                    NULL AS domain_schema,
                    NULL AS domain_name,
                    NULL AS udt_catalog,
                    NULL AS udt_schema,
                    NULL AS udt_name,
                    NULL AS scope_catalog,
                    NULL AS scope_schema,
                    NULL AS scope_name,
                    NULL AS maximum_cardinality,
                    NULL AS dtd_identifier,
                    NULL AS is_self_referencing,
                    NULL AS is_identity,
                    NULL AS identity_generation,
                    NULL AS identity_start,
                    NULL AS identity_increment,
                    NULL AS identity_maximum,
                    NULL AS identity_minimum,
                    NULL AS identity_cycle,
                    NULL AS is_generated,
                    NULL AS generation_expression,
                    NULL AS is_updatable
                FROM duckdb_tables() t
                JOIN duckdb_columns() c ON t.table_name = c.table_name
                ORDER BY t.table_name, c.ordinal_position
            `,
        },
    }

    for _, view := range views {
        if err := catalog.CreateSystemView(view); err != nil {
            return err
        }
    }

    return nil
}
```

### pg_catalog Implementation

pg_catalog tables provide PostgreSQL compatibility:

```go
func createPgCatalogTables(catalog Catalog) error {
    tables := []SystemTable{
        {
            Name:   "pg_class",
            Schema: "pg_catalog",
            Columns: []Column{
                {Name: "oid", Type: types.OID},
                {Name: "relname", Type: types.VARCHAR},
                {Name: "relnamespace", Type: types.OID},
                {Name: "reltype", Type: types.OID},
                {Name: "reloftype", Type: types.OID},
                {Name: "relowner", Type: types.OID},
                {Name: "relam", Type: types.OID},
                {Name: "relfilenode", Type: types.OID},
                {Name: "reltablespace", Type: types.OID},
                {Name: "relpages", Type: types.INTEGER},
                {Name: "reltuples", Type: types.REAL},
                {Name: "relallvisible", Type: types.INTEGER},
                {Name: "reltoastrelid", Type: types.OID},
                {Name: "relhasindex", Type: types.BOOLEAN},
                {Name: "relisshared", Type: types.BOOLEAN},
                {Name: "relpersistence", Type: types.CHAR},
                {Name: "relkind", Type: types.CHAR},
                {Name: "relnatts", Type: types.SMALLINT},
                {Name: "relchecks", Type: types.SMALLINT},
                {Name: "relhasoids", Type: types.BOOLEAN},
                {Name: "relhaspkey", Type: types.BOOLEAN},
                {Name: "relhasrules", Type: types.BOOLEAN},
                {Name: "relhastriggers", Type: types.BOOLEAN},
                {Name: "relhassubclass", Type: types.BOOLEAN},
                {Name: "relrowsecurity", Type: types.BOOLEAN},
                {Name: "relforcerowsecurity", Type: types.BOOLEAN},
                {Name: "relispopulated", Type: types.BOOLEAN},
                {Name: "relreplident", Type: types.CHAR},
                {Name: "relispartition", Type: types.BOOLEAN},
                {Name: "relfrozenxid", Type: types.XID},
                {Name: "relminmxid", Type: types.XID},
                {Name: "relacl", Type: types.ACLITEM_ARRAY},
                {Name: "reloptions", Type: types.TEXT_ARRAY},
                {Name: "relpartbound", Type: types.TEXT},
            },
            Data: generatePgClassData,
        },
    }

    for _, table := range tables {
        if err := catalog.CreateSystemTable(table); err != nil {
            return err
        }
    }

    return nil
}
```

## Performance Considerations

### Caching Strategy

1. **Metadata Cache**: LRU cache for frequently accessed metadata
2. **Function Cache**: Compiled function lookups
3. **Schema Cache**: Cached schema information
4. **Dependency Cache**: Object dependency relationships

### Indexing

1. **Function Name Index**: B-tree on function name
2. **Table Name Index**: B-tree on table name
3. **Schema Index**: B-tree on schema name
4. **Dependency Index**: Graph index for dependencies

### Lazy Loading

Metadata is loaded on demand to minimize memory usage:

```go
type LazyMetadataLoader struct {
    loader    func() (interface{}, error)
    cache     atomic.Value
    loaded    atomic.Bool
    loadOnce  sync.Once
}

func (l *LazyMetadataLoader) Get() (interface{}, error) {
    if l.loaded.Load() {
        return l.cache.Load(), nil
    }

    var err error
    l.loadOnce.Do(func() {
        data, e := l.loader()
        if e != nil {
            err = e
            return
        }
        l.cache.Store(data)
        l.loaded.Store(true)
    })

    if err != nil {
        return nil, err
    }

    return l.cache.Load(), nil
}
```

## Security Considerations

### Access Control

System metadata access is controlled through:

1. **Schema Visibility**: Users see only schemas they have access to
2. **Table Permissions**: Filter based on table privileges
3. **Function Security**: Hide internal functions from regular users
4. **Setting Sensitivity**: Mask sensitive configuration values

### Audit Trail

All system metadata access is logged:

```go
type MetadataAccessLog struct {
    Timestamp time.Time
    User      string
    Action    string // SELECT, PRAGMA, SYSTEM_FUNCTION
    Object    string
    Result    string // SUCCESS, DENIED, ERROR
    Duration  time.Duration
}
```

## Testing Strategy

### Unit Tests

1. **Function Registry Tests**: Test function registration and lookup
2. **Metadata Query Tests**: Test metadata query engine
3. **Cache Tests**: Test caching behavior and invalidation
4. **Dependency Tests**: Test dependency tracking

### Integration Tests

1. **System Function Tests**: Test all duckdb_*() functions
2. **PRAGMA Tests**: Test all PRAGMA statements
3. **Metadata Table Tests**: Test information_schema and pg_catalog
4. **Performance Tests**: Benchmark metadata queries

### Compatibility Tests

1. **DuckDB Compatibility**: Compare results with DuckDB v1.4.3
2. **PostgreSQL Compatibility**: Test pg_catalog compatibility
3. **SQL Standard Compliance**: Validate information_schema compliance

## Conclusion

This design provides a comprehensive system for implementing DuckDB v1.4.3 compatible system functions and metadata tables. The architecture is extensible, performant, and maintains compatibility with existing systems while providing the rich metadata access required for database administration and tool integration.