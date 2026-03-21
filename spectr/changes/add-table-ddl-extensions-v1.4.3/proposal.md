# Add Table DDL Extensions (CREATE OR REPLACE TABLE, CREATE TEMP TABLE, ALTER TABLE ADD/DROP CONSTRAINT)

**Change ID:** `add-table-ddl-extensions-v1.4.3`
**Created:** 2026-03-21
**Status:** PROPOSED
**Scope:** Medium — Three DDL enhancements across parser, binder, planner, executor
**Estimated Complexity:** Medium — Threading new fields through existing pipeline
**User-Visible:** Yes — New SQL DDL syntax

## Summary

Add DuckDB v1.4.3 DDL extensions:

1. **CREATE OR REPLACE TABLE** — Drop and recreate a table in one statement
2. **CREATE TEMP/TEMPORARY TABLE** — Create session-scoped temporary tables
3. **ALTER TABLE ADD CONSTRAINT** — Add UNIQUE, CHECK, or FOREIGN KEY constraints to existing tables
4. **ALTER TABLE DROP CONSTRAINT** — Remove named constraints from tables

## Verification

- `parseCreate()` at parser.go:2009-2016 parses `orReplace` but does NOT pass it to parseCreateTable() (line 2037)
- `parseCreate()` at parser.go:2025-2027 parses `temporary` but does NOT pass it to parseCreateTable()
- `CreateTableStmt` at ast.go:391-410 has NO `OrReplace` or `Temporary` fields
- `parseAlterTable()` at parser_ddl.go:477-604 supports RENAME, DROP COLUMN, ADD COLUMN, ALTER COLUMN TYPE only
- Error at parser_ddl.go:600: "expected RENAME, DROP, ADD, ALTER, or SET after ALTER TABLE"
- Constraint infrastructure exists: catalog/constraint.go has UniqueConstraintDef, CheckConstraintDef, ForeignKeyConstraintDef

## Current Infrastructure

- `parseCreate()` — parser.go:2004-2061 — already parses OR REPLACE and TEMPORARY flags
- `parseCreateTable()` — parser.go:2063-2210 — creates CreateTableStmt but doesn't accept flags
- `CreateTableStmt` — ast.go:391-410 — Schema, Table, IfNotExists, Columns, PrimaryKey, Constraints, AsSelect
- `parseAlterTable()` — parser_ddl.go:470-604 — dispatches on operation keyword
- `AlterTableStmt` — ast.go:654-681 — has AlterTableOp enum and operation-specific fields
- `AlterTableOp` — ast.go:655-664 — RenameTo, RenameColumn, DropColumn, AddColumn, SetOption, AlterColumnType
- `TableConstraint` — ast.go:379-389 — Name, Type, Columns, Expression, RefTable, RefColumns, OnDelete, OnUpdate
- `parseTableConstraint()` — parser.go:2212-2299 — parses UNIQUE, CHECK, FOREIGN KEY constraints
- `TableDef.Constraints` — catalog/table.go:32-34 — `[]any` holding *UniqueConstraintDef, *CheckConstraintDef
- `bindCreateTable()` — binder/bind_stmt.go:3193-3302 — converts parser constraints to catalog constraints
- `BoundCreateTableStmt` — binder/statements.go:134-142 — has IfNotExists but no OrReplace/Temporary
- `executeCreateTable()` — executor/operator.go:2603-2727 — validates, creates table, stores constraints
- `executeAlterTable()` — executor/ddl.go:457-576 — dispatches on operation type

## Goals

1. Pass `orReplace` and `temporary` from parseCreate() to parseCreateTable()
2. Add OrReplace and Temporary fields to CreateTableStmt and thread through binder/planner/executor
3. Implement OR REPLACE logic: drop existing table before creating new one
4. Implement TEMP logic: create table in temp schema, auto-cleanup on connection close
5. Add ADD CONSTRAINT and DROP CONSTRAINT operations to AlterTableStmt
6. Parse constraint definitions reusing existing parseTableConstraint()
7. Execute ADD CONSTRAINT by appending to TableDef.Constraints
8. Execute DROP CONSTRAINT by removing named constraint from TableDef.Constraints
