# Add IS DISTINCT FROM / IS NOT DISTINCT FROM Operators

**Change ID:** `add-is-distinct-from-v1.4.3`
**Created:** 2026-03-20
**Scope:** Small — Two new binary operators across parser, binder, and executor
**Estimated Complexity:** Low — Each is a simple NULL-safe comparison
**User-Visible:** Yes — New SQL comparison operators

## Summary

Add the SQL standard `IS DISTINCT FROM` and `IS NOT DISTINCT FROM` comparison operators. These are NULL-safe equality comparisons:

- `a IS DISTINCT FROM b` → true when `a != b` OR exactly one is NULL. Treats NULL=NULL as equal.
- `a IS NOT DISTINCT FROM b` → true when `a = b` OR both are NULL. NULL-safe equality.

These are commonly used in WHERE clauses and JOIN conditions where NULL comparison semantics matter.

## Verification

- `BinaryOp` enum at `ast.go:742-789` has no `OpIsDistinctFrom` or `OpIsNotDistinctFrom`
- Parser does not handle `IS DISTINCT FROM` syntax (only `IS NULL`, `IS NOT NULL`, `IS`)
- No test files reference "DISTINCT FROM" in comparison context

## Current Infrastructure

- `BinaryOp` enum at `ast.go:742-789` with OpIs/OpIsNot for `IS`/`IS NOT`
- Binary expression evaluation at `expr.go:337-355` handles NULL cases for OpIs/OpIsNot
- `compareValues()` at `expr.go:3457` handles comparison of two non-NULL values
- Parser handles `IS NULL`, `IS NOT NULL`, `IS TRUE`, `IS FALSE` in expression parsing

## Goals

1. Add `OpIsDistinctFrom` and `OpIsNotDistinctFrom` to BinaryOp enum
2. Parse `expr IS [NOT] DISTINCT FROM expr` syntax
3. Evaluate with NULL-safe semantics in executor
4. Add type inference in binder (returns BOOLEAN)
