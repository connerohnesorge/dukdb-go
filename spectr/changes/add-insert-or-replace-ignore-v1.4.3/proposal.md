# Proposal: Add INSERT OR REPLACE/IGNORE Syntax

## Summary

Implement SQLite-compatible `INSERT OR REPLACE` and `INSERT OR IGNORE` syntax as syntactic sugar for the existing ON CONFLICT mechanism. DuckDB supports both the modern `ON CONFLICT DO UPDATE/DO NOTHING` and the legacy `INSERT OR REPLACE/IGNORE` forms.

## Motivation

Many applications migrating from SQLite or using ORMs that generate SQLite-style SQL expect INSERT OR REPLACE/IGNORE to work. DuckDB v1.4.3 supports this syntax for compatibility.

## Scope

- **Parser**: Detect `OR REPLACE` / `OR IGNORE` after INSERT keyword at parser.go:1509
- **Desugar**: Convert to existing OnConflictClause mechanism (no new executor logic needed)

## Files Affected

- `internal/parser/parser.go` — parseInsert() (line 1509): detect OR REPLACE/IGNORE
- No AST changes needed — reuses existing OnConflictClause at ast.go:278
