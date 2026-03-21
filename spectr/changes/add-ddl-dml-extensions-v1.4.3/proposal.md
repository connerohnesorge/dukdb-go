# Add DDL/DML Extensions for DuckDB v1.4.3 Compatibility

**Change ID:** `add-ddl-dml-extensions-v1.4.3`
**Created:** 2026-03-20
**Status:** PROPOSED
**Scope:** Medium — Three new DDL/DML features across parser, binder, planner, executor, and catalog
**Estimated Complexity:** Medium — COMMENT ON and ALTER COLUMN TYPE touch 5 layers; DELETE USING touches 4
**User-Visible:** Yes — New SQL statements

## Summary

Add three missing DDL/DML features that DuckDB v1.4.3 supports:

1. **COMMENT ON** — Attach comments to tables, columns, views, indexes, and schemas. Comments are metadata stored in the catalog and queryable via `duckdb_tables()` and `information_schema`.
2. **ALTER TABLE ALTER COLUMN TYPE** — Change a column's data type. Requires data conversion in the storage layer.
3. **DELETE ... USING** — Multi-table delete with a join source. Standard PostgreSQL-compatible DELETE syntax.

## Verification

- `grep -r 'CommentStmt' internal/parser/ast.go` → no matches
- `grep -r 'AlterTableAlterColumnType' internal/parser/ast.go` → no matches (only RENAME, ADD, DROP, SET)
- `DeleteStmt` in ast.go has no `Using` field — only `Schema`, `Table`, `Where`, `Returning`

## Current Infrastructure

- **ALTER TABLE**: `AlterTableOp` enum (ast.go:653) with 5 ops. Parsed in `parser_ddl.go:470`. Bound in `bind_ddl.go:308`. Executed in `ddl.go:457`.
- **DELETE**: `DeleteStmt` (ast.go:359). Parsed in `parser.go:1879`. Bound in `bind_stmt.go:3086`. Executed in `physical_delete.go:18`.
- **Catalog**: `TableDef` (catalog/table.go:15) and `ColumnDef` (catalog/column.go:7) have no `Comment` field.
- **DDL dispatch**: Parser (parser.go:47-100) → Binder (binder.go:~200) → Planner (physical.go:~1400) → Executor (operator.go:364-495).

## Goals

1. Add `COMMENT ON {TABLE|COLUMN|VIEW|INDEX|SCHEMA} object IS 'text'` statement
2. Add `ALTER TABLE t ALTER COLUMN c TYPE new_type` statement
3. Add `DELETE FROM t USING source WHERE condition` multi-table delete syntax
4. Store comments in catalog metadata (TableDef, ColumnDef)
5. All features queryable/usable through standard SQL
