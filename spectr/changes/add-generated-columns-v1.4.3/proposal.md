# Change: Add GENERATED Columns (STORED/VIRTUAL) for DuckDB v1.4.3 Compatibility

**Change ID:** `add-generated-columns-v1.4.3`
**Created:** 2026-03-22
**Status:** PROPOSED
**Scope:** Medium — Parser, catalog, executor, storage
**Estimated Complexity:** Medium — Touches column definition parsing, table creation, INSERT/UPDATE execution, and storage serialization
**User-Visible:** Yes — New DDL syntax for computed columns

## Why

DuckDB v1.4.3 supports GENERATED ALWAYS AS columns (also called computed columns) in CREATE TABLE statements. These are columns whose values are automatically computed from an expression involving other columns in the same row. This is a standard SQL feature (SQL:2003) and a commonly used DDL capability. Currently, dukdb-go has no parser or executor support for generated columns, but the DuckDB binary format serializer already recognizes `ColumnCategoryGenerated` (catalog_types.go:60), meaning files with generated columns can be partially read but not created or maintained.

## What Changes

- **Parser**: Add `GeneratedExpr` and `GeneratedKind` fields to `ColumnDefClause` in ast.go. Parse `GENERATED ALWAYS AS (expr) [STORED|VIRTUAL]` syntax in column definitions.
- **Catalog**: Store generated column metadata (expression + kind) in `TableEntry` column definitions.
- **Executor**: During INSERT/UPDATE, evaluate generated column expressions using values from the same row. Prevent direct writes to generated columns. Support STORED (persisted) and VIRTUAL (computed on read) semantics.
- **Binder**: Validate generated column expressions reference only non-generated columns in the same table. Detect circular dependencies.
- **Storage**: Serialize/deserialize generated column metadata to/from the DuckDB binary format using existing `PropColumnDefExpression` and `ColumnCategoryGenerated` constants.

## Impact

- Affected specs: `generated-columns` (new capability)
- Affected code:
  - `internal/parser/ast.go` — ColumnDefClause struct
  - `internal/parser/parser_ddl.go` — parseColumnDef
  - `internal/catalog/table.go` — TableEntry column metadata
  - `internal/executor/ddl.go` — executeCreateTable validation
  - `internal/executor/physical_insert.go` — generated column evaluation on INSERT
  - `internal/executor/physical_update.go` — generated column re-evaluation on UPDATE
  - `internal/binder/bind_expr.go` — generated expression validation
  - `internal/storage/duckdb/catalog_serialize.go` — binary format serialization
