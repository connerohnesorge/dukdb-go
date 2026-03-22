# Tasks: Missing System Views

- [ ] 1. Add schema metadata — Create GetSchemas(cat *catalog.Catalog, stor *storage.Storage, dbName string) in internal/metadata/. Return built-in schemas (main, information_schema, pg_catalog) plus user-created schemas from catalog.ListSchemas() (returns []*Schema, call .Name()). Add DuckDBSchemasColumns() returning []*catalog.ColumnDef. Validate: Compiles without error.

- [ ] 2. Add ListTypes() to catalog — Add ListTypes() method to internal/catalog/catalog.go following ListSchemas() (line 51) and ListTables() (line 244) patterns. Returns user-created types (ENUMs). Validate: Compiles without error.

- [ ] 3. Add type metadata — Create GetTypes() in internal/metadata/. Signature: GetTypes(cat *catalog.Catalog, stor *storage.Storage, dbName string). Return all built-in DuckDB types with size and category. Include user-created types (ENUMs) from catalog.ListTypes(). Add DuckDBTypesColumns() returning []*catalog.ColumnDef. Validate: Compiles without error.

- [ ] 4. Implement executeDuckDBSchemas — Add executor method in system_functions.go following executeDuckDBTables() pattern at line 30. Register in table function dispatch at table_function_csv.go (add case around line 142). Add "duckdb_schemas" to systemFunctionNames at metadata/functions.go:54. Validate: `SELECT * FROM duckdb_schemas()` returns rows.

- [ ] 5. Implement executeDuckDBTypes — Add executor method in system_functions.go. Register in table function dispatch. Add "duckdb_types" to systemFunctionNames. Validate: `SELECT * FROM duckdb_types()` returns rows.

- [ ] 6. Integration tests — Test duckdb_schemas() with default and user-created schemas. Test duckdb_types() with built-in and user-created types. Test filtering and ordering.
