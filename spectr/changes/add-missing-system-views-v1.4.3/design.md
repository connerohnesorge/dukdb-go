# Design: Missing System Views

## Architecture

Follow the exact same pattern as executeDuckDBTables() (system_functions.go:30-53): metadata package provides data, executor formats as rows, column definitions come from metadata package.

## 1. duckdb_schemas()

### 1.1 Metadata (internal/metadata/)

Add schema metadata gathering. Schemas come from the catalog:

```go
// SchemaMetadata represents a row in duckdb_schemas().
type SchemaMetadata struct {
    DatabaseName string
    SchemaName   string
    SchemaOID    int64
    Internal     bool  // true for system schemas (main, information_schema, pg_catalog)
    SQL          string
}

// GetSchemas follows the GetTables(cat *catalog.Catalog, stor *storage.Storage, databaseName string)
// pattern at metadata/tables.go:11. Note: catalog.ListSchemas() returns []*catalog.Schema objects,
// not strings — must call .Name() on each schema object.
func GetSchemas(cat *catalog.Catalog, stor *storage.Storage, dbName string) []SchemaMetadata {
    schemas := cat.ListSchemas() // Returns []*catalog.Schema
    result := make([]SchemaMetadata, 0, len(schemas)+3)

    // Add built-in schemas
    builtins := []struct{ name string; oid int64 }{
        {"main", 1},
        {"information_schema", 2},
        {"pg_catalog", 3},
    }
    for _, b := range builtins {
        result = append(result, SchemaMetadata{
            DatabaseName: dbName,
            SchemaName:   b.name,
            SchemaOID:    b.oid,
            Internal:     true,
            SQL:          "",
        })
    }

    // Add user-created schemas
    oid := int64(100)
    for _, s := range schemas {
        name := s.Name() // ListSchemas() returns []*Schema, must call .Name()
        // Skip built-ins already added
        if name == "main" || name == "information_schema" || name == "pg_catalog" {
            continue
        }
        result = append(result, SchemaMetadata{
            DatabaseName: dbName,
            SchemaName:   name,
            SchemaOID:    oid,
            Internal:     false,
            SQL:          fmt.Sprintf("CREATE SCHEMA %s;", name),
        })
        oid++
    }
    return result
}

func DuckDBSchemasColumns() []*catalog.ColumnDef {
    return []*catalog.ColumnDef{
        {Name: "database_name", Type: "VARCHAR"},
        {Name: "schema_name", Type: "VARCHAR"},
        {Name: "schema_oid", Type: "BIGINT"},
        {Name: "internal", Type: "BOOLEAN"},
        {Name: "sql", Type: "VARCHAR"},
    }
}
```

### 1.2 Executor (system_functions.go)

```go
func (e *Executor) executeDuckDBSchemas(
    _ *ExecutionContext,
    _ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
    schemas := metadata.GetSchemas(e.catalog, e.storage, metadata.DefaultDatabaseName)
    rows := make([]map[string]any, 0, len(schemas))
    for _, s := range schemas {
        rows = append(rows, map[string]any{
            "database_name": s.DatabaseName,
            "schema_name":   s.SchemaName,
            "schema_oid":    s.SchemaOID,
            "internal":      s.Internal,
            "sql":           s.SQL,
        })
    }
    return &ExecutionResult{
        Rows:    rows,
        Columns: metadata.ColumnNames(metadata.DuckDBSchemasColumns()),
    }, nil
}
```

Note: catalog.ListSchemas() exists at catalog.go:51 and returns []*Schema. The executor accesses the catalog via `e.catalog` (type *catalog.Catalog).

## 2. duckdb_types()

### 2.1 Metadata (internal/metadata/)

```go
// TypeMetadata represents a row in duckdb_types().
type TypeMetadata struct {
    DatabaseName string
    SchemaName   string
    TypeName     string
    TypeSize     int64   // -1 for variable size
    TypeCategory string  // "NUMERIC", "STRING", "DATETIME", "BOOLEAN", "COMPOSITE", "NESTED"
    Internal     bool    // true for built-in types
    SQL          string
}

// PREREQUISITE: catalog.Catalog must have a ListTypes() method added.
// Currently ListSchemas() exists (catalog.go:51) and ListTables() exists (catalog.go:244),
// but ListTypes() does NOT exist. Must be added to internal/catalog/catalog.go first.
func GetTypes(cat *catalog.Catalog, stor *storage.Storage, dbName string) []TypeMetadata {
    result := make([]TypeMetadata, 0)

    // Built-in types
    builtinTypes := []struct {
        name     string
        size     int64
        category string
    }{
        {"BOOLEAN", 1, "BOOLEAN"},
        {"TINYINT", 1, "NUMERIC"},
        {"SMALLINT", 2, "NUMERIC"},
        {"INTEGER", 4, "NUMERIC"},
        {"BIGINT", 8, "NUMERIC"},
        {"HUGEINT", 16, "NUMERIC"},
        {"UTINYINT", 1, "NUMERIC"},
        {"USMALLINT", 2, "NUMERIC"},
        {"UINTEGER", 4, "NUMERIC"},
        {"UBIGINT", 8, "NUMERIC"},
        {"FLOAT", 4, "NUMERIC"},
        {"DOUBLE", 8, "NUMERIC"},
        {"DECIMAL", -1, "NUMERIC"},
        {"VARCHAR", -1, "STRING"},
        {"BLOB", -1, "STRING"},
        {"DATE", 4, "DATETIME"},
        {"TIME", 8, "DATETIME"},
        {"TIMESTAMP", 8, "DATETIME"},
        {"TIMESTAMP WITH TIME ZONE", 8, "DATETIME"},
        {"INTERVAL", 16, "DATETIME"},
        {"UUID", 16, "STRING"},
        {"JSON", -1, "STRING"},
        {"LIST", -1, "NESTED"},
        {"MAP", -1, "NESTED"},
        {"STRUCT", -1, "NESTED"},
        {"UNION", -1, "NESTED"},
        {"ARRAY", -1, "NESTED"},
        {"BIT", -1, "STRING"},
    }

    for _, t := range builtinTypes {
        result = append(result, TypeMetadata{
            DatabaseName: dbName,
            SchemaName:   "main",
            TypeName:     t.name,
            TypeSize:     t.size,
            TypeCategory: t.category,
            Internal:     true,
        })
    }

    // User-created types (ENUMs from CREATE TYPE)
    // Requires ListTypes() method to be added to catalog.Catalog
    for _, userType := range cat.ListTypes() {
        result = append(result, TypeMetadata{
            DatabaseName: dbName,
            SchemaName:   "main",
            TypeName:     userType.Name,
            TypeSize:     -1,
            TypeCategory: "COMPOSITE",
            Internal:     false,
            SQL:          userType.SQL,
        })
    }

    return result
}

func DuckDBTypesColumns() []*catalog.ColumnDef {
    return []*catalog.ColumnDef{
        {Name: "database_name", Type: "VARCHAR"},
        {Name: "schema_name", Type: "VARCHAR"},
        {Name: "type_name", Type: "VARCHAR"},
        {Name: "type_size", Type: "BIGINT"},
        {Name: "type_category", Type: "VARCHAR"},
        {Name: "internal", Type: "BOOLEAN"},
        {Name: "sql", Type: "VARCHAR"},
    }
}
```

Note: catalog.Catalog does NOT have a ListTypes() method. This must be added as a prerequisite. ListSchemas() at catalog.go:51 and ListTables() at catalog.go:244 serve as patterns. If ListTypes() cannot be added in time, the user-created types section can be deferred and only built-in types returned initially.

### 2.2 Executor (system_functions.go)

```go
func (e *Executor) executeDuckDBTypes(
    _ *ExecutionContext,
    _ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
    types := metadata.GetTypes(e.catalog, e.storage, metadata.DefaultDatabaseName)
    rows := make([]map[string]any, 0, len(types))
    for _, t := range types {
        rows = append(rows, map[string]any{
            "database_name": t.DatabaseName,
            "schema_name":   t.SchemaName,
            "type_name":     t.TypeName,
            "type_size":     t.TypeSize,
            "type_category": t.TypeCategory,
            "internal":      t.Internal,
            "sql":           t.SQL,
        })
    }
    return &ExecutionResult{
        Rows:    rows,
        Columns: metadata.ColumnNames(metadata.DuckDBTypesColumns()),
    }, nil
}
```

## 3. Registration

### 3.1 Table Function Dispatch (table_function_csv.go:113)

Add to the system function switch:

```go
case "duckdb_schemas":
    return e.executeDuckDBSchemas(ctx, plan)
case "duckdb_types":
    return e.executeDuckDBTypes(ctx, plan)
```

### 3.2 Function List (metadata/functions.go:54)

Add to systemFunctionNames:

```go
"duckdb_schemas",
"duckdb_types",
```

## Helper Signatures Reference (Verified)

- executeDuckDBTables() — system_functions.go:30-53 — pattern for system table functions
- Table function dispatch — table_function_csv.go:112-142 — case switch for duckdb_* functions
- systemFunctionNames — metadata/functions.go:54-70 — registered system function list
- ColumnDef — `*catalog.ColumnDef` — column definition type (returns `[]*catalog.ColumnDef`)
- ColumnNames() — metadata/ — extracts column names from ColumnDef slice
- GetTables(cat *catalog.Catalog, stor *storage.Storage, databaseName string) — metadata/tables.go:11 — pattern for metadata gathering (3 params)
- metadata.DefaultDatabaseName — "memory" — default database name constant
- catalog.ListSchemas() — catalog.go:51 — returns []*Schema (call .Name() for name)
- catalog.ListTables() — catalog.go:244 — pattern for listing
- catalog.ListTypes() — DOES NOT EXIST, must be added as prerequisite
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeCatalog, Msg: fmt.Sprintf(...)}` (use ErrorTypeCatalog for catalog operations)

## Testing Strategy

1. `SELECT * FROM duckdb_schemas()` → returns at least 'main' schema
2. `SELECT schema_name FROM duckdb_schemas() WHERE internal = true` → includes main, information_schema, pg_catalog
3. `CREATE SCHEMA test_schema; SELECT * FROM duckdb_schemas() WHERE schema_name = 'test_schema'` → returns the new schema
4. `SELECT * FROM duckdb_types()` → returns all built-in types
5. `SELECT type_name FROM duckdb_types() WHERE type_category = 'NUMERIC'` → returns all numeric types
6. `SELECT * FROM duckdb_types() WHERE type_name = 'INTEGER'` → returns INTEGER with size 4
7. `CREATE TYPE mood AS ENUM('happy', 'sad'); SELECT * FROM duckdb_types() WHERE type_name = 'mood'` → returns user type
