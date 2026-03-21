# Add Metadata Commands (DESCRIBE, SHOW TABLES/COLUMNS, SUMMARIZE, CALL)

**Change ID:** `add-metadata-commands-v1.4.3`
**Created:** 2026-03-21
**Status:** IMPLEMENTED
**Scope:** Medium — Four metadata/utility statement types across parser, binder, executor
**Estimated Complexity:** Medium — Each statement queries catalog metadata
**User-Visible:** Yes — New SQL statements for introspection

## Summary

Add DuckDB v1.4.3 metadata and utility commands:

1. **DESCRIBE table** / **DESCRIBE SELECT ...** — Show column names, types, nullability
2. **SHOW TABLES** / **SHOW ALL TABLES** — List tables in current or all schemas
3. **SHOW COLUMNS FROM table** — Same as DESCRIBE but different syntax
4. **SUMMARIZE table** / **SUMMARIZE SELECT ...** — Show column statistics (min, max, count, nulls, distinct)
5. **CALL function(args)** — Execute a table function as a statement (returns result set)

## Verification

- `grep -r 'DescribeStmt' internal/parser/ast.go` → no matches
- `grep -r 'SummarizeStmt' internal/parser/ast.go` → no matches
- `grep -r 'CallStmt' internal/parser/ast.go` → no matches
- `grep -r '"SHOW TABLES"' internal/` → no matches
- SHOW exists but only for variables (parser_pragma.go:275)

## Current Infrastructure

- ShowStmt: ast.go:1662 — only has `Variable string` field
- parseShow(): parser_pragma.go:275-289 — only handles `SHOW variable`
- handleShow(): engine/conn.go:626 — handles SHOW at connection level
- SHOW dispatched at conn.go:1025 (before normal binder/executor flow)
- EXPLAIN pattern: ast.go:1360-1372 wraps another statement with Query field
- Information schema: binder/information_schema.go, executor/information_schema.go — virtual table functions
- TableDef.Columns: catalog/table.go:24 — `[]*ColumnDef` with Name, Type, Nullable, HasDefault, DefaultValue, Comment
- Main parser dispatch: parser.go:47-129 — keyword switch
- ExecutionResult: executor/operator.go:74-78 — has Rows, Columns, RowsAffected fields
- Catalog.ListTables(): catalog.go:244 — lists all tables
- Schema.ListTables(): catalog.go:675 — lists tables in a schema
- STATEMENT_TYPE_CALL: already defined in stmt_type.go:44

## Goals

1. Parse DESCRIBE table/query, SHOW TABLES, SHOW COLUMNS FROM table, SUMMARIZE, CALL
2. Implement as catalog metadata queries returning result sets
3. DESCRIBE/SHOW COLUMNS return column_name, column_type, null, key, default, extra
4. SHOW TABLES returns database, schema, name, column_count, estimated_size
5. SUMMARIZE returns per-column statistics
6. CALL delegates to table function execution
