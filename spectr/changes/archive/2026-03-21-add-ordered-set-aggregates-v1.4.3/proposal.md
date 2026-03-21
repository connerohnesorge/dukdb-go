# Add Ordered-Set Aggregates (WITHIN GROUP + LISTAGG)

**Change ID:** `add-ordered-set-aggregates-v1.4.3`
**Created:** 2026-03-20
**Status:** PROPOSED
**Scope:** Small — Parser syntax extension + one new aggregate function
**Estimated Complexity:** Low-Medium — WITHIN GROUP is a parser change; LISTAGG reuses STRING_AGG logic
**User-Visible:** Yes — New SQL aggregate syntax and function

## Summary

Add the SQL standard `WITHIN GROUP (ORDER BY ...)` syntax for ordered-set aggregate functions, and the `LISTAGG` aggregate function which uses it.

**WITHIN GROUP** is the SQL standard way to specify ordering for ordered-set aggregates:
```sql
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary)
LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name)
MODE() WITHIN GROUP (ORDER BY value)
```

**LISTAGG** is the SQL standard equivalent of `STRING_AGG`/`GROUP_CONCAT`:
```sql
LISTAGG(column, delimiter) WITHIN GROUP (ORDER BY column)
```

## Verification

- `grep -r 'WITHIN' internal/parser/` → no matches for WITHIN GROUP parsing
- `grep -r '"LISTAGG"' internal/executor/` → no matches
- PERCENTILE_CONT already works with internal ORDER BY syntax (`PERCENTILE_CONT(0.5 ORDER BY salary)`) but not with WITHIN GROUP syntax
- STRING_AGG infrastructure (`collectValuesWithOrderBy`, `computeStringAgg`) can be reused for LISTAGG

## Current Infrastructure

- Aggregate ORDER BY parsing: parser.go:5139-5147 (inside function call parentheses)
- `collectValuesWithOrderBy()`: physical_aggregate.go:1045-1099
- `computeStringAgg()`: aggregate_string.go:19-36
- STRING_AGG dispatch: physical_aggregate.go:630-649
- FunctionCall AST: ast.go:815-823 (has OrderBy field)
- BoundFunctionCall: binder/expressions.go:60-85 (has OrderBy field)

## Goals

1. Parse `func(args) WITHIN GROUP (ORDER BY expr)` syntax
2. Convert WITHIN GROUP ordering into the existing FunctionCall.OrderBy field
3. Implement LISTAGG aggregate (reusing STRING_AGG logic)
4. Ensure PERCENTILE_CONT/PERCENTILE_DISC/MODE work with WITHIN GROUP syntax
