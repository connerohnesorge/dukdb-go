# Add Enum Utility Functions

**Change ID:** `add-enum-utility-functions-v1.4.3`
**Created:** 2026-03-21
**Status:** PROPOSED
**Scope:** Small — Three enum introspection scalar functions
**Estimated Complexity:** Low — Each reads from existing TypeEntry.EnumValues
**User-Visible:** Yes — New SQL functions for enum type introspection

## Summary

Add DuckDB v1.4.3 enum introspection functions:

- `ENUM_RANGE(enum_type)` → Returns all values of an enum type as a list
- `ENUM_FIRST(enum_type)` → Returns the first value of an enum type
- `ENUM_LAST(enum_type)` → Returns the last value of an enum type

These require looking up enum types in the catalog via `Catalog.GetType()` at catalog.go:1112.

## Verification

- `grep -r '"ENUM_RANGE"' internal/` → no matches
- `grep -r '"ENUM_FIRST"' internal/` → no matches
- `grep -r '"ENUM_LAST"' internal/` → no matches

## Current Infrastructure

- `TypeEntry` at catalog.go:589 has `EnumValues []string` field
- `Catalog.GetType(name, schemaName)` at catalog.go:1112 retrieves enum definitions
- `Schema.GetType(name)` at catalog.go:919 — schema-level lookup
- Executor has access to catalog via `ExecutionContext.conn` → `ConnectionInterface`
- Function dispatch in `evaluateFunctionCall()` at expr.go, switch starting at line 688
- The argument to these functions is an enum type name (string), NOT an enum value

## Goals

1. Implement ENUM_RANGE returning `[]any` (list of enum values as strings)
2. Implement ENUM_FIRST returning the first enum value string
3. Implement ENUM_LAST returning the last enum value string
4. Add type inference in binder
