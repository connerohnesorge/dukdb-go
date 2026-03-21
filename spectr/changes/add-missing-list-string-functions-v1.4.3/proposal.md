# Add Missing List/String Functions

**Change ID:** `add-missing-list-string-functions-v1.4.3`
**Created:** 2026-03-21
**Status:** PROPOSED
**Scope:** Small — Five scalar functions following established patterns
**Estimated Complexity:** Small — Each follows existing list/string function patterns
**User-Visible:** Yes — New SQL functions

## Summary

Add missing list manipulation and string/regex functions for DuckDB v1.4.3 compatibility:

1. **LIST_APPEND(list, element)** / **ARRAY_APPEND** / **ARRAY_PUSH_BACK** — Append element to list
2. **LIST_PREPEND(element, list)** / **ARRAY_PREPEND** / **ARRAY_PUSH_FRONT** — Prepend element to list
3. **LIST_HAS(list, element)** — Alias for LIST_CONTAINS (already at expr.go:2538)
4. **STRING_TO_ARRAY(string, delimiter)** — Alias for STRING_SPLIT (already at expr.go:1522)
5. **REGEXP_FULL_MATCH(string, pattern)** — Full regex match (returns bool, not partial match)

## Verification

- `grep -r '"LIST_APPEND"' internal/executor/expr.go` → no matches
- `grep -r '"LIST_PREPEND"' internal/executor/expr.go` → no matches
- `grep -r '"LIST_HAS"' internal/executor/expr.go` → no matches
- `grep -r '"STRING_TO_ARRAY"' internal/executor/expr.go` → no matches
- `grep -r '"REGEXP_FULL_MATCH"' internal/executor/expr.go` → no matches
- LIST_CONCAT exists at expr.go:2590 (pattern for append/prepend)
- LIST_CONTAINS exists at expr.go:2538 (LIST_HAS is just an alias)
- STRING_SPLIT exists at expr.go:1522 (STRING_TO_ARRAY is just an alias)
- REGEXP_MATCHES exists at expr.go:1455 (pattern for REGEXP_FULL_MATCH)

## Current Infrastructure

- `evaluateFunctionCall()` — expr.go:630 — main function dispatch
- `inferFunctionResultType()` — binder/utils.go:347 — return type inference
- `LIST_CONCAT` — expr.go:2590 — concatenates two lists (pattern for append)
- `LIST_CONTAINS` — expr.go:2538 — checks list membership
- `STRING_SPLIT` — expr.go:1522 — splits string by delimiter
- `REGEXP_MATCHES` — expr.go:1455 — regex match returning array
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Goals

1. Add LIST_APPEND/ARRAY_APPEND: append element to end of list
2. Add LIST_PREPEND/ARRAY_PREPEND: prepend element to start of list
3. Add LIST_HAS as alias for LIST_CONTAINS case label
4. Add STRING_TO_ARRAY as alias for STRING_SPLIT case label
5. Add REGEXP_FULL_MATCH: return true if entire string matches pattern
