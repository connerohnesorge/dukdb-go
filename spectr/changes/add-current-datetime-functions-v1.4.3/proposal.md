# Proposal: Add Current Date/Time Functions

## Summary

Implement NOW(), CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, and TODAY() as executable functions. These are listed in binder type inference (internal/binder/utils.go:475-479) and the PRAGMA functions list (internal/executor/physical_maintenance.go:252) but have no executor implementation — calling them returns "unknown function" at internal/executor/expr.go:3332.

## Motivation

These are among the most commonly used SQL functions. Any DuckDB-compatible application expects `SELECT NOW()`, `SELECT CURRENT_DATE`, etc. to work. Without these, basic temporal queries fail.

## Scope

- **Parser**: Add CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP as bare keywords (no parentheses required) in parseIdentExpr() at parser.go:5035-5083
- **Executor**: Add function dispatch cases in evaluateFunctionCall() for NOW, CURRENT_TIMESTAMP, CURRENT_DATE, TODAY, CURRENT_TIME
- **Binder**: Add TODAY to type inference (utils.go:475-479) alongside CURRENT_DATE
- **Query Cache**: Add TODAY to volatileFuncs map (query_cache.go:211-217)

## Files Affected

- `internal/parser/parser.go` — parseIdentExpr() (line 5035)
- `internal/executor/expr.go` — evaluateFunctionCall() (line 661)
