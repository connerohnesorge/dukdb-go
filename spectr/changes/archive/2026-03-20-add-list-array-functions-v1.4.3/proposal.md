# Add Missing List/Array Functions for DuckDB v1.4.3 Compatibility

**Change ID:** `add-list-array-functions-v1.4.3`
**Created:** 2026-03-20
**Scope:** Medium — Adds 6 new list/array functions with aliases
**Estimated Complexity:** Medium — Each function is self-contained, touches executor and binder
**User-Visible:** Yes — New SQL functions available

## Summary

This proposal adds 6 missing list/array functions that DuckDB v1.4.3 supports but dukdb-go currently lacks:

1. **Element access:** `LIST_ELEMENT(list, index)` / `ARRAY_EXTRACT(list, index)`
2. **List aggregation:** `LIST_AGGREGATE(list, name)` / `ARRAY_AGGREGATE(list, name)`
3. **Reverse sort:** `LIST_REVERSE_SORT(list)` / `ARRAY_REVERSE_SORT(list)`
4. **Join to string:** `ARRAY_TO_STRING(list, sep [, null_str])` / `LIST_TO_STRING(list, sep [, null_str])`
5. **Zip lists:** `LIST_ZIP(list1, list2, ...)`
6. **Resize list:** `LIST_RESIZE(list, size [, value])` / `ARRAY_RESIZE(list, size [, value])`

Note: UNNEST was initially considered but is already implemented as a table function in `internal/executor/table_function_unnest.go`.

## Verification

All functions confirmed missing via code search:

- `LIST_ELEMENT`/`ARRAY_EXTRACT`: Not in `internal/executor/expr.go` function switch. No matches in `internal/executor/list_functions.go`.
- `LIST_AGGREGATE`/`ARRAY_AGGREGATE`: No matches in `internal/executor/`.
- `LIST_REVERSE_SORT`: No matches. `LIST_SORT` exists (line 599 in expr.go, line 150 in list_functions.go) but only supports ascending order — no direction parameter implemented.
- `ARRAY_TO_STRING`/`LIST_TO_STRING`: No matches in `internal/executor/`.
- `LIST_ZIP`: No matches in `internal/executor/`.
- `LIST_RESIZE`/`ARRAY_RESIZE`: No matches in `internal/executor/`.

## Current List Function Infrastructure

Functions are dispatched via a `switch` in `internal/executor/expr.go`:
- Lambda functions at lines 595-610: LIST_TRANSFORM, LIST_FILTER, LIST_SORT
- Regular list functions at lines 2197-2389: LIST_CONTAINS, LIST_POSITION, LIST_CONCAT, LIST_DISTINCT, LIST_REVERSE, LIST_SLICE, FLATTEN
- List helper functions in `internal/executor/list_functions.go`: `evaluateListSort()` (line 150), `toSlice()` (line 401)

Runtime representation: Lists are `[]any` slices, normalized via `toSlice(v any) ([]any, bool)` at list_functions.go:401.

Type inference: `inferFunctionResultType()` in `internal/binder/utils.go` lines 545-574 handles list return types.

## Goals

1. Register all 6 functions (with 5 aliases = 11 total case entries) following the existing dispatch pattern
2. Add type inference for each function in the binder
3. NULL propagation: all functions return NULL when list argument is NULL
4. Full alias support matching DuckDB naming conventions

## Non-Goals

- Subscript operator `list[index]` syntax (requires parser changes — separate proposal)
- LIST_SORT direction parameter support (existing function enhancement — separate concern)
- New list lambda functions (list_reduce, etc. — separate proposal)

## Capabilities

### Capability 1: LIST_ELEMENT Element Access

`LIST_ELEMENT(list, index)` — Returns the element at 1-based index. Negative indices count from end. Out-of-bounds returns NULL. Aliases: `ARRAY_EXTRACT`.

### Capability 2: LIST_AGGREGATE List Aggregation

`LIST_AGGREGATE(list, aggregate_name)` — Applies a named aggregate function to list elements. Supported aggregates: 'sum', 'avg', 'min', 'max', 'count', 'string_agg', 'first', 'last', 'bool_and', 'bool_or'. Aliases: `ARRAY_AGGREGATE`.

### Capability 3: LIST_REVERSE_SORT Descending Sort

`LIST_REVERSE_SORT(list)` — Sorts list elements in descending order. Complement to existing LIST_SORT (ascending). Aliases: `ARRAY_REVERSE_SORT`.

### Capability 4: ARRAY_TO_STRING Join to String

`ARRAY_TO_STRING(list, separator [, null_string])` — Joins list elements into a string with separator. NULL elements skipped unless null_string provided. Aliases: `LIST_TO_STRING`.

### Capability 5: LIST_ZIP Zip Multiple Lists

`LIST_ZIP(list1, list2, ...)` — Zips 2+ lists into a list of structs. Shorter lists padded with NULL.

### Capability 6: LIST_RESIZE Resize List

`LIST_RESIZE(list, size [, value])` — Resizes list to target size. Truncates if smaller, pads with value (default NULL) if larger. Aliases: `ARRAY_RESIZE`.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| LIST_AGGREGATE dispatch complexity | Medium | Low | Only support common aggregates; error on unknown |
| LIST_ZIP struct type creation | Low | Medium | Use map[string]any for struct representation |
| toSlice() edge cases with typed slices | Low | Low | Existing helper handles major types; extend if needed |
