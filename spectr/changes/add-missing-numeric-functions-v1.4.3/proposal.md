# Proposal: Missing Numeric Functions

## Summary

Add missing numeric scalar and conditional aggregate functions for DuckDB v1.4.3 compatibility: SIGNBIT, WIDTH_BUCKET, BETA scalar functions, and SUM_IF/AVG_IF/MIN_IF/MAX_IF conditional aggregate functions.

## Motivation

SIGNBIT is needed for IEEE 754 compliance. WIDTH_BUCKET is SQL standard for histogram computation. BETA is a statistical function. SUM_IF/AVG_IF/MIN_IF/MAX_IF are widely used conditional aggregates (COUNT_IF already exists at operator.go:113).

## Scope

- **Executor**: Add scalar functions to evaluateFunctionCall() (expr.go:661)
- **Executor**: Add conditional aggregates to computeAggregate() (physical_aggregate.go:295)
- **Executor**: Register conditional aggregates in isAggregateFunc() (operator.go:99-122)
- **Binder**: Add type inference entries

## Files Affected

- `internal/executor/expr.go` — scalar function dispatch
- `internal/executor/physical_aggregate.go` — aggregate dispatch
- `internal/executor/operator.go` — aggregate registration
- `internal/binder/utils.go` — type inference
