# Add Struct Dot Notation Field Access

**Change ID:** `add-struct-field-access-v1.4.3`
**Created:** 2026-03-21
**Status:** PROPOSED
**Scope:** Medium — Parser, binder, and executor changes for struct field access syntax
**Estimated Complexity:** Medium — Requires AST extension and binder logic for type resolution
**User-Visible:** Yes — New SQL syntax for accessing struct fields

## Summary

Add DuckDB v1.4.3 struct dot notation syntax: `struct_column.field_name`. Currently, struct field access requires the verbose `STRUCT_EXTRACT(struct_column, 'field_name')` function call. This proposal adds the standard dot notation syntax used by DuckDB and PostgreSQL.

## Verification

- `grep -r 'FieldAccessExpr\|MemberExpr\|DotExpr' internal/parser/ast.go` → no matches
- parseIdentExpr() at parser.go:5089-5110 treats ALL dot references as table.column
- bindColumnRef() at binder/bind_expr.go:113-181 fails if Table doesn't exist as table name
- STRUCT_EXTRACT exists at expr.go:2233-2255 (uses map[string]any for struct values)
- ColumnRef at ast.go:719-724 has only Table and Column fields

## Current Infrastructure

- `parseIdentExpr()` — parser.go:5033-5113 — handles identifier expressions
- Dot handling — parser.go:5089-5110 — creates ColumnRef{Table: name, Column: col}
- `ColumnRef` — ast.go:719-724 — Table and Column fields only
- `bindColumnRef()` — binder/bind_expr.go:113-181 — resolves to BoundColumnRef
- `BoundColumnRef` — binder/expressions.go:8-18 — Table, Column, ColumnIdx, ColType
- `STRUCT_EXTRACT` — expr.go:2233-2255 — extracts field from map[string]any
- Struct runtime type — `map[string]any` consistently throughout codebase
- TYPE_STRUCT — type_enum.go:36 — Type = 26

## Goals

1. Add FieldAccessExpr AST node to represent struct.field expressions
2. Handle dot notation in parser for struct field access
3. Resolve ambiguity between table.column and struct.field in binder
4. Evaluate FieldAccessExpr by extracting from map[string]any at runtime
