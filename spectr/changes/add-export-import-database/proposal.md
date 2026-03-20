# Change: Add EXPORT DATABASE and IMPORT DATABASE Support

## Why

DuckDB v1.4.3 provides `EXPORT DATABASE` and `IMPORT DATABASE` for full
database portability — exporting schema DDL and table data to a directory,
and importing it back. dukdb-go currently has no way to serialize an entire
database to a portable format, blocking backup/restore, migration, and
cross-environment transfer workflows.

## What Changes

- **Parser**: New `ExportDatabaseStmt` and `ImportDatabaseStmt` AST nodes
  with path and FORMAT option
- **Executor**: `executeExportDatabase` iterates all catalog objects, generates
  DDL into `schema.sql`, writes table data via existing COPY TO infrastructure,
  and generates `load.sql` with COPY FROM statements
- **Executor**: `executeImportDatabase` reads and executes `schema.sql`, then
  reads and executes `load.sql`
- **Catalog**: New DDL generation helpers (`TableDef.ToCreateSQL()`,
  `ViewDef.ToCreateSQL()`, etc.) for serializing catalog objects to SQL text

## Impact

- Affected specs: `parser`, `execution-engine`
- Affected code:
  - `internal/parser/ast.go` — new statement types
  - `internal/parser/parser.go` — EXPORT/IMPORT DATABASE parsing
  - `internal/executor/export_import.go` — new file for export/import logic
  - `internal/catalog/ddl_gen.go` — new file for DDL generation helpers
  - `internal/planner/physical.go` — new physical plan nodes
