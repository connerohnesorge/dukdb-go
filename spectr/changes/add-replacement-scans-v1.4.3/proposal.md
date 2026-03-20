# Add Replacement Scans for DuckDB v1.4.3 Compatibility

**Change ID:** `add-replacement-scans-v1.4.3`
**Created:** 2026-03-20
**Scope:** Small — Parser and binder changes to detect file paths as table names
**Estimated Complexity:** Low — ~150-200 lines across parser and binder
**User-Visible:** Yes — Enables `FROM 'file.csv'` syntax

## Summary

DuckDB allows using file paths directly as table references in FROM clauses:

```sql
SELECT * FROM 'data.csv';           -- auto-detects read_csv_auto
SELECT * FROM 'results.parquet';    -- auto-detects read_parquet
SELECT * FROM 'records.json';       -- auto-detects read_json_auto
```

Currently, dukdb-go requires explicit table function calls:

```sql
SELECT * FROM read_csv('data.csv');
SELECT * FROM read_parquet('results.parquet');
```

This proposal adds "replacement scans" — when the parser encounters a string literal in a FROM position, it recognizes it as a file path and the binder rewrites it to the appropriate table function call based on file extension.

## Verification

- `parseTableRef()` at `internal/parser/parser.go:761` handles `tokenIdent` (line 804) but NOT `tokenString` — string literals in FROM position currently produce a parse error ("expected table name or subquery" at line 827)
- Table functions `read_csv`, `read_parquet`, `read_json`, `read_ndjson` are all fully implemented in the executor
- File extension detection utilities exist in COPY statement handling (`internal/executor/physical_copy.go`)

## Current Infrastructure

- Table reference parsing: `parseTableRef()` at `parser.go:761`
- Table function parsing: `parseTableFunction()` called at `parser.go:810`
- Table function AST: `TableFunction` struct in `ast.go`
- Binder table resolution: `bindFrom()` in `bind_stmt.go`
- File format detection: Used in COPY statement for auto-format selection

## Goals

1. Parse string literals in FROM clause position as file path table references
2. Auto-detect format from file extension (.csv, .parquet, .json, .ndjson, .xlsx, .arrow)
3. Rewrite to appropriate table function call (read_csv_auto, read_parquet, read_json_auto, etc.)
4. Support cloud URLs (s3://, gs://, az://, https://) through existing filesystem infrastructure

## Non-Goals

- Custom replacement scan registration (DuckDB allows user-defined replacement scans — out of scope)
- Glob pattern expansion in FROM position (e.g., `FROM '*.csv'`) — future enhancement
- Format options in FROM syntax (use explicit table functions for options)
