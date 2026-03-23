## 1. Planner

- [ ] 1.1 Add `PhysicalPlanExportDatabase` and `PhysicalPlanImportDatabase` type constants to `PhysicalPlanType` enum in `internal/planner/physical.go`.
- [ ] 1.2 Add `PhysicalExportDatabase` and `PhysicalImportDatabase` structs in `internal/planner/physical.go`.
- [ ] 1.3 Add `planStatement()` cases for `*parser.ExportDatabaseStmt` and `*parser.ImportDatabaseStmt`.

## 2. Catalog SQL Generation

- [ ] 2.1 Add `ToCreateSQL() string` method to `catalog.TableDef` that generates a complete CREATE TABLE statement including all column types, constraints (PK, UNIQUE, CHECK, NOT NULL, DEFAULT, FOREIGN KEY), and collation. Note: the actual type is `catalog.TableDef`, not `catalog.TableEntry`.
- [ ] 2.2 Add `ToCreateSQL() string` method to `catalog.ViewEntry` that generates CREATE VIEW AS query.
- [ ] 2.3 Add `ToSQL() string` method to `catalog.SequenceEntry` that generates CREATE SEQUENCE with START WITH, INCREMENT BY, MIN/MAX VALUE, CYCLE options.
- [ ] 2.4 Add `ToSQL() string` method for ENUM type entries.
- [ ] 2.5 Add `ToSQL() string` method for macro entries (`MacroDef`) that generates `CREATE MACRO name(params) AS expr` DDL.
- [ ] 2.6 Add `ListAllTables()`, `ListAllViews()`, `ListTypes()` methods to catalog if not already present.
- [ ] 2.7 Add `ListMacros() []*MacroDef` method to both `catalog.Schema` and `catalog.Catalog` to enumerate all macros for export (currently only `GetMacro`, `CreateMacro`, `DropMacro` exist).
- [ ] 2.8 Ensure `ToCreateSQL()` on `TableDef` iterates over `Constraints []any` and emits SQL for each constraint type: `*UniqueConstraintDef` (UNIQUE), `*CheckConstraintDef` (CHECK), `*ForeignKeyConstraintDef` (FOREIGN KEY REFERENCES).
- [ ] 2.9 Add unit tests for all ToCreateSQL/ToSQL methods with roundtrip verification (generate SQL → parse → compare).

## 3. Binder

- [ ] 3.1 Add `bindExportDatabase(*parser.ExportDatabaseStmt)` to `internal/binder/bind_stmt.go` with basic validation (non-empty path).
- [ ] 3.2 Add `bindImportDatabase(*parser.ImportDatabaseStmt)` to `internal/binder/bind_stmt.go` with basic validation (non-empty path).
- [ ] 3.3 Wire both bind methods into the binder's main statement dispatch.

## 4. Parser/Statement Fixes

- [ ] 4.1 **CRITICAL**: Fix `ImportDatabaseStmt.Type()` in `internal/parser/ast.go:1346` to return `STATEMENT_TYPE_COPY_DATABASE` instead of `STATEMENT_TYPE_COPY`.
- [ ] 4.2 Add `"IMPORT"` case to `keywordToStmtType()` in `stmt_detector.go`, mapping to `STATEMENT_TYPE_COPY_DATABASE`.

## 5. Executor — EXPORT DATABASE

- [ ] 5.1 Create `internal/executor/export_database.go` with `executeExportDatabase()` function.
- [ ] 5.2 Implement directory creation and validation.
- [ ] 5.3 Generate `schema.sql` with CREATE SCHEMA, CREATE TYPE, CREATE SEQUENCE, CREATE TABLE (topologically sorted), CREATE VIEW, CREATE MACRO statements.
- [ ] 5.4 Implement topological sort for tables based on foreign key dependencies (Kahn's algorithm; fall back to alphabetical on cycles).
- [ ] 5.5 Export data files using existing COPY TO infrastructure for each table. Support FORMAT option (CSV default, PARQUET, JSON).
- [ ] 5.6 Generate `load.sql` with COPY FROM statements matching the exported data files.
- [ ] 5.7 Handle OPTIONS passthrough (DELIMITER, HEADER, NULL, etc.) to COPY TO.
- [ ] 5.8 Add integration test: create tables with data, EXPORT DATABASE, verify schema.sql and load.sql content, verify data files exist.

## 6. Executor — IMPORT DATABASE

- [ ] 6.1 Create `internal/executor/import_database.go` with `executeImportDatabase()` function.
- [ ] 6.2 Read and parse `schema.sql` — execute each DDL statement in order.
- [ ] 6.3 Read and parse `load.sql` — execute each COPY FROM statement to load data.
- [ ] 6.4 Wrap import in a transaction — rollback on any failure.
- [ ] 6.5 Validate directory exists and contains required files (schema.sql, load.sql).
- [ ] 6.6 Add integration test: EXPORT DATABASE then IMPORT DATABASE into a fresh instance, verify all tables, views, sequences restored with data.

## 7. Operator Registration

- [ ] 7.1 Register `PhysicalPlanExportDatabase` and `PhysicalPlanImportDatabase` operator types in `internal/executor/operator.go`.
- [ ] 7.2 Add execution dispatch in the main executor switch statement.

## 8. Parser: Multiple Statement Parsing

- [ ] 8.1 Ensure parser can handle multiple semicolon-separated statements from a string (for executing schema.sql and load.sql). Add `ParseMultiple()` if not already available.
- [ ] 8.2 Add unit test for parsing multi-statement SQL strings.

## 9. End-to-End Integration Tests

- [ ] 9.1 Test EXPORT DATABASE with CSV format (default).
- [ ] 9.2 Test EXPORT DATABASE with Parquet format: `EXPORT DATABASE 'dir' (FORMAT PARQUET)`.
- [ ] 9.3 Test EXPORT DATABASE with JSON format.
- [ ] 9.4 Test roundtrip: EXPORT → fresh database → IMPORT → verify all data matches.
- [ ] 9.5 Test EXPORT preserves views, sequences, schemas.
- [ ] 9.6 Test IMPORT into non-empty database (should fail or merge — match DuckDB behavior).
- [ ] 9.7 Test IMPORT from non-existent directory returns clear error.
