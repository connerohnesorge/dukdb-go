# Change: Wire IMPORT/EXPORT DATABASE Execution for DuckDB v1.4.3 Compatibility

**Change ID:** `add-import-export-database-execution-v1.4.3`
**Created:** 2026-03-22
**Status:** PROPOSED
**Scope:** Medium — Planner, executor
**Estimated Complexity:** Medium — Parser exists; need planner nodes and executor logic for directory-based export/import
**User-Visible:** Yes — Enables full database backup/restore via SQL

## Why

DuckDB v1.4.3 supports `EXPORT DATABASE 'path' (OPTIONS)` and `IMPORT DATABASE 'path'` for full database backup and restore. EXPORT writes all tables, views, schemas, sequences, and macros to a directory as SQL DDL scripts plus data files (CSV, Parquet, or JSON). IMPORT reads them back. The dukdb-go parser already has `ExportDatabaseStmt` (ast.go:1322) and `ImportDatabaseStmt` (ast.go:1338) but no planner or executor handles them. This is important for database migration, backup, and interoperability between DuckDB and dukdb-go instances.

## What Changes

- **Planner**: Add `PhysicalExportDatabase` and `PhysicalImportDatabase` plan nodes
- **Executor — EXPORT**: Iterate all schemas, tables, views, sequences, macros. Write `schema.sql` (DDL), `load.sql` (COPY FROM statements), and data files (one per table in the specified format)
- **Executor — IMPORT**: Read `schema.sql` to create DDL objects, then execute `load.sql` to load data files
- **Format support**: CSV (default), Parquet, JSON — matching DuckDB v1.4.3 export formats

## Impact

- Affected specs: `database-backup` (new capability)
- Affected code:
  - `internal/planner/physical.go` — new physical plan nodes
  - `internal/executor/ddl.go` — execution handlers
  - `internal/executor/operator.go` — operator type registration
  - `internal/catalog/catalog.go` — enumerate schemas, tables, views, sequences, macros
