# Add Missing Functions Round 2

**Change ID:** `add-missing-functions-round2-v1.4.3`
**Created:** 2026-03-21
**Status:** PROPOSED
**Scope:** Small — Six scalar functions + three aggregate functions/aliases
**Estimated Complexity:** Low — Each follows established patterns in the codebase
**User-Visible:** Yes — New SQL functions

## Summary

Add remaining DuckDB v1.4.3 functions not covered by other proposals:

**Scalar Functions:**
1. `SHA1(string)` → SHA-1 hash as hex string (SHA256 pattern exists at hash.go:24)
2. `SETSEED(value)` → Set random number seed (RANDOM exists at math.go:763)
3. `LIST_VALUE(args...)` / `LIST_PACK(args...)` → Create list from arguments

**Aggregate Functions:**
4. `ANY_VALUE(expr)` → Returns arbitrary non-NULL value (identical to FIRST at physical_aggregate.go:707)
5. `HISTOGRAM(expr)` → Returns MAP of value→count
6. `ARG_MIN` / `ARG_MAX` underscore aliases (ARGMIN/ARGMAX exist at physical_aggregate.go:727,742)

## Verification

- `grep -r '"SHA1"' internal/executor/` → no matches (SHA256 exists at expr.go:1749)
- `grep -r '"SETSEED"' internal/` → no matches (RANDOM exists at expr.go:953)
- `grep -r '"LIST_VALUE"' internal/` → no matches
- `grep -r '"ANY_VALUE"' internal/` → no matches (FIRST exists at physical_aggregate.go:707)
- `grep -r '"HISTOGRAM"' internal/` → no matches
- `grep -r '"ARG_MIN"' internal/` → no matches (ARGMIN exists at physical_aggregate.go:727)

## Current Infrastructure

- SHA256: hash.go:24 `sha256Value()`, dispatched at expr.go:1749
- RANDOM: math.go:763 `randomValue()`, dispatched at expr.go:953
- FIRST: physical_aggregate.go:707, uses `computeFirst()` at aggregate_time.go:24
- ARGMIN: physical_aggregate.go:727 with `computeArgmin()` at aggregate_time.go:47
- MAP creation pattern: expr.go:2222 — returns `map[string]any`
- Aggregate function name list: operator.go:100-115

## Goals

1. Add SHA1 following SHA256 pattern
2. Add SETSEED with per-connection random seed via SetSetting
3. Add LIST_VALUE/LIST_PACK as variadic list constructors
4. Add ANY_VALUE as alias for FIRST logic
5. Add HISTOGRAM aggregate returning value count map
6. Add ARG_MIN/ARG_MAX as aliases for ARGMIN/ARGMAX
