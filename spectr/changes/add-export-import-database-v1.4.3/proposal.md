# Change: Complete EXPORT DATABASE and IMPORT DATABASE Implementation

## Why

The EXPORT DATABASE and IMPORT DATABASE statements have partial implementations that lack critical functionality. The current export omits sequences and indexes from schema.sql, only supports CSV output (ignoring the FORMAT option), and does not handle multi-schema data file naming. The import side works for the basic schema.sql + load.sql flow but inherits all gaps from the export (missing DDL means lost sequences/indexes on round-trip). Completing this brings dukdb-go to full DuckDB v1.4.3 parity for database backup and restore.

## Current State

Implemented (in `internal/engine/export_import.go`):

- Directory creation and basic export flow
- `schema.sql` generation for CREATE SCHEMA, CREATE TABLE (with columns, NOT NULL, PRIMARY KEY), and CREATE VIEW
- Table data export to CSV via `exportTableToCSV` using Go's `encoding/csv`
- `load.sql` generation with COPY FROM statements referencing CSV files
- Import reads `schema.sql`, splits on semicolons (handling string literals), parses and executes each statement
- Import reads `load.sql` and executes COPY FROM statements
- `splitSQLStatements` handles basic semicolon splitting with quoted-string awareness

Not implemented / missing:

1. **Sequence DDL in schema.sql** -- `ListSequences()` exists on Schema but `handleExportDatabase` never calls it; sequences are silently lost on export
2. **Index DDL in schema.sql** -- `ListIndexes()` exists on Schema but `handleExportDatabase` never calls it; non-primary-key indexes are lost on export
3. **FORMAT option support** -- `ExportDatabaseStmt.Options` map is parsed but completely ignored; export always writes CSV regardless of FORMAT PARQUET or FORMAT JSON
4. **Multi-schema data file naming** -- Tables in non-main schemas should use `{schema}_{table}.{ext}` filenames to avoid collisions; current code uses `{table}.csv` for all
5. **Dependency ordering** -- schema.sql should emit DDL in order: schemas, sequences, tables, views, indexes; current code does schemas then tables then views but omits sequences/indexes entirely
6. **Error handling for non-existent export path parent** -- No validation that the parent directory is writable
7. **Default column values** -- `generateCreateTableSQL` does not emit DEFAULT clauses

## What Changes

- Add sequence DDL generation (`CREATE SEQUENCE ... START WITH ... INCREMENT BY ...`)
- Add index DDL generation (`CREATE [UNIQUE] INDEX ... ON ...`)
- Add FORMAT option handling for Parquet and JSON export (via existing `internal/io/parquet` and `internal/io/json` writers)
- Fix multi-schema data file naming to `{schema}_{table}.{ext}`
- Enforce dependency ordering in schema.sql: schemas, sequences, tables, views, indexes
- Add DEFAULT clause support in `generateCreateTableSQL`
- Add error reporting for invalid export directories

## Impact

- Affected specs: execution-engine, copy-statement
- Affected code: `internal/engine/export_import.go`, `internal/catalog/` (read-only usage of ListSequences, ListIndexes)
- No breaking changes -- existing CSV-only export behavior is preserved as the default
