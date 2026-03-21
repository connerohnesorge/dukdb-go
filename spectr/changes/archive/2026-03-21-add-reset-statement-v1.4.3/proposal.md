# Add RESET Statement

**Change ID:** `add-reset-statement-v1.4.3`
**Created:** 2026-03-21
**Status:** PROPOSED
**Scope:** Small — One statement type across parser and executor
**Estimated Complexity:** Small — Follows SET pattern
**User-Visible:** Yes — New SQL statement

## Summary

Add DuckDB v1.4.3 `RESET variable` statement to reset configuration variables to their default values. Also support `RESET ALL` to reset all variables.

## Verification

- `grep -r 'RESET' internal/parser/` → no matches
- `grep -r 'RESET' internal/engine/conn.go` → no matches
- `grep -r 'parseReset' internal/` → no matches
- SET exists: parseSet() at parser_pragma.go:187, handleSet at conn.go
- SHOW exists: parseShow() at parser_pragma.go:278

## Current Infrastructure

- `parseSet()` — parser_pragma.go:187 — SET variable = value parsing
- `SetStmt` — ast.go — set configuration variable
- `handleShow()` — engine/conn.go:626 — SHOW handler (returns current value)
- Main parser dispatch — parser.go:47-129 — keyword switch
- Configuration variables stored in connection settings

## Goals

1. Add RESET keyword to parser dispatch (parser.go:47-129)
2. Add ResetStmt to ast.go with Variable field
3. Parse RESET variable and RESET ALL
4. Execute by resetting variable to default value
