# Add Missing Aggregates Round 3 (PRODUCT, MAD, FAVG, FSUM, BITSTRING_AGG)

**Change ID:** `add-missing-aggregates-round3-v1.4.3`
**Created:** 2026-03-21
**Status:** PROPOSED
**Scope:** Small — Five aggregate functions following established patterns
**Estimated Complexity:** Small — Each follows existing aggregate dispatch patterns
**User-Visible:** Yes — New SQL aggregate functions

## Summary

Add missing DuckDB v1.4.3 aggregate functions:

1. **PRODUCT(expr)** — Multiplicative aggregate (multiply all values)
2. **MAD(expr)** — Median Absolute Deviation
3. **FAVG(expr)** — Fast (Kahan summation) average for floating-point accuracy
4. **FSUM(expr)** — Fast (Kahan summation) sum for floating-point accuracy
5. **BITSTRING_AGG(expr)** — Aggregate values into a bitstring

## Verification

- `grep -r '"PRODUCT"' internal/executor/` → no matches
- `grep -r '"MAD"' internal/executor/` → no matches
- `grep -r '"FAVG"' internal/executor/` → no matches
- `grep -r '"FSUM"' internal/executor/` → no matches
- `grep -r '"BITSTRING_AGG"' internal/executor/` → no matches
- SUM exists at physical_aggregate.go:342 (pattern for PRODUCT/FSUM)
- AVG exists at physical_aggregate.go:368 (pattern for FAVG)
- MEDIAN exists at physical_aggregate.go:449 (pattern for MAD)

## Current Infrastructure

- `computeAggregate()` — physical_aggregate.go:295 — aggregate dispatch
- `isAggregateFunc()` — operator.go:99-122 — aggregate function registration
- `inferFunctionResultType()` — binder/utils.go:347 — type inference
- SUM — physical_aggregate.go:342 — additive accumulator pattern
- AVG — physical_aggregate.go:368 — sum/count pattern
- MEDIAN — physical_aggregate.go:449 — collect values then compute
- `toFloat64Value()` — used by SUM/AVG for numeric conversion
- `collectValues()` — physical_aggregate.go helper for collecting all values
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Goals

1. Add PRODUCT: multiply all non-NULL values (identity = 1.0)
2. Add MAD: collect values, compute median, then median of absolute deviations
3. Add FAVG: Kahan summation average for better floating-point accuracy
4. Add FSUM: Kahan summation for better floating-point accuracy
5. Add BITSTRING_AGG: bitwise OR aggregate of integer values
6. Register all in isAggregateFunc() and inferFunctionResultType()
