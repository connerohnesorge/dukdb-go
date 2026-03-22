# Proposal: Missing String Functions Round 4

## Summary

Add missing string functions for DuckDB v1.4.3 compatibility: OCTET_LENGTH, INITCAP, SOUNDEX, LCASE/UCASE aliases, and LIKE_ESCAPE. All are simple scalar functions added to evaluateFunctionCall() in internal/executor/expr.go.

## Motivation

These are standard SQL or DuckDB-supported string functions that are missing from the executor. OCTET_LENGTH is SQL standard. INITCAP is common in PostgreSQL/DuckDB. SOUNDEX is a widely-used phonetic algorithm. LCASE/UCASE are MySQL-compatible aliases for LOWER/UPPER.

## Scope

- **Executor**: Add 6 function cases to evaluateFunctionCall() (expr.go:661)
- **Binder**: Add type inference entries in inferFunctionResultType() (binder/utils.go:347)

## Files Affected

- `internal/executor/expr.go` — evaluateFunctionCall() function dispatch
- `internal/binder/utils.go` — inferFunctionResultType() type inference
