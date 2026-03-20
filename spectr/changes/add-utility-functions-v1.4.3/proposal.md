# Add Missing Utility Functions for DuckDB v1.4.3 Compatibility

**Change ID:** `add-utility-functions-v1.4.3`
**Created:** 2026-03-20
**Scope:** Small — Adds 9 small utility functions across 3 categories
**Estimated Complexity:** Low — Each function is 5-20 lines, self-contained
**User-Visible:** Yes — New SQL functions available

## Summary

This proposal bundles 9 small missing utility functions into a single change:

1. **System:** `CURRENT_DATABASE()`, `CURRENT_SCHEMA()`, `VERSION()`
2. **Date/Time:** `DAYNAME(date)`, `MONTHNAME(date)`, `YEARWEEK(date)`, `EPOCH_US(timestamp)`
3. **String:** `TRANSLATE(str, from, to)`, `STRIP_ACCENTS(str)`

## Verification

All functions confirmed missing:
- None of these appear in the function dispatch switch in `internal/executor/expr.go`
- `EPOCH` (line 1624) and `EPOCH_MS` (line 1627) already exist — `EPOCH_US` is the missing microsecond variant
- `golang.org/x/text` v0.32.0 already in `go.mod` — needed for `STRIP_ACCENTS`

## Current Infrastructure

- Function dispatch: `evaluateFunctionCall()` at `internal/executor/expr.go:587`
- Has access to `ctx *ExecutionContext` with `ctx.conn ConnectionInterface`
- `ConnectionInterface` (operator.go:22) provides `GetSetting(key string) string`
- Engine stores `path` field (engine.go:41) and computes `dbName` (engine.go:263-271)
- Type inference: `inferFunctionResultType()` in `internal/binder/utils.go`
- Date helpers: `toTime()` likely exists for existing EPOCH/EPOCH_MS functions

## Goals

1. Register all 9 functions following the existing dispatch pattern
2. System functions access connection context for database/schema info
3. No new external dependencies (golang.org/x/text already in go.mod)

## Non-Goals

- CURRENT_SETTING(name) for arbitrary settings (more complex, separate proposal)
- SET statement for configuration (already handled elsewhere)
