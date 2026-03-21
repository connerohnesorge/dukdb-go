# Add Missing Function Aliases and Small Scalar Functions

**Change ID:** `add-function-aliases-v1.4.3`
**Created:** 2026-03-20
**Status:** PROPOSED
**Scope:** Small — Function aliases and simple new scalar functions
**Estimated Complexity:** Low — Each is 5-30 lines of code
**User-Visible:** Yes — New SQL functions available

## Summary

Add missing DuckDB v1.4.3 function aliases and small scalar functions that are not yet implemented. These fall into three categories:

1. **Aliases for existing functions:** DATETRUNC→DATE_TRUNC, DATEADD→DATE_ADD, ORD→ASCII
2. **Missing NULL-handling functions:** IFNULL (2-arg COALESCE), NVL (alias for IFNULL)
3. **Missing bit/encoding functions:** BIT_LENGTH, GET_BIT, SET_BIT, ENCODE, DECODE

## Verification

- `grep -r '"DATETRUNC"' internal/` → no matches (only DATE_TRUNC exists)
- `grep -r '"DATEADD"' internal/` → no matches (only DATE_ADD exists)
- `grep -r 'case "IFNULL"' internal/executor/expr.go` → no matches
- `grep -r '"NVL"' internal/` → no matches
- `grep -r '"ORD"' internal/` → no matches
- `grep -r '"GET_BIT"' internal/` → no matches
- `grep -r '"SET_BIT"' internal/` → no matches
- `grep -r '"BIT_LENGTH"' internal/` → no matches
- `grep -r '"ENCODE"' internal/` → no matches (BASE64_ENCODE exists, but not general ENCODE)
- `grep -r '"DECODE"' internal/` → no matches (BASE64_DECODE exists, but not general DECODE)

## Current Infrastructure

- Function dispatch: `evaluateFunctionCall()` in `internal/executor/expr.go` (large switch statement)
- Type inference: `inferFunctionResultType()` in `internal/binder/utils.go` at line 342
- DATE_TRUNC: dispatched at expr.go:1694 → `evalDateTrunc()` in temporal_functions.go:776
- DATE_ADD: dispatched at expr.go:1685 → `evalDateAdd()` in temporal_functions.go:512
- ASCII: dispatched at expr.go:1515 → `asciiValue()` in string.go:283
- COALESCE: dispatched at expr.go:1162 (IFNULL is 2-arg version)
- BIT_COUNT: dispatched at expr.go:1016 → `bitCountValue()` (similar pattern for BIT_LENGTH)

## Goals

1. Add DATETRUNC and DATEADD as case labels alongside DATE_TRUNC and DATE_ADD
2. Implement IFNULL as 2-argument COALESCE with NVL alias
3. Add ORD as case label alongside ASCII
4. Implement BIT_LENGTH, GET_BIT, SET_BIT scalar functions
5. Implement ENCODE/DECODE for character set conversion (using golang.org/x/text v0.34.0)
6. Add type inference entries in binder for all new functions
