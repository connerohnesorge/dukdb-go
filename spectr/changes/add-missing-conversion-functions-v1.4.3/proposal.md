# Add Missing Conversion Functions (TO_DATE, TO_CHAR, GENERATE_SUBSCRIPTS)

**Change ID:** `add-missing-conversion-functions-v1.4.3`
**Created:** 2026-03-21
**Status:** PROPOSED
**Scope:** Small — Three scalar/table functions following established patterns
**Estimated Complexity:** Small — All follow existing patterns in expr.go and temporal_functions.go
**User-Visible:** Yes — New SQL functions

## Summary

Add DuckDB v1.4.3 conversion/utility functions:

1. **TO_DATE(string, format)** — Parse a string to DATE using a format string (like STRPTIME but returns DATE)
2. **TO_CHAR(value, format)** — Format a date/timestamp/number to string (like STRFTIME, PostgreSQL-compatible alias)
3. **GENERATE_SUBSCRIPTS(array, dim)** — Generate array indices as a set-returning function

## Verification

- `grep -r '"TO_DATE"' internal/executor/expr.go` → no matches
- `grep -r '"TO_CHAR"' internal/executor/expr.go` → no matches
- `grep -r '"GENERATE_SUBSCRIPTS"' internal/` → no matches
- STRPTIME exists at temporal_functions.go:1718-1758 (returns TIMESTAMP, not DATE)
- STRFTIME exists at temporal_functions.go:1677-1713 (TO_CHAR is its alias)
- GENERATE_SERIES exists at table_function_series.go:13-127 (pattern to follow)

## Current Infrastructure

- `evaluateFunctionCall()` — executor/expr.go:630 — main function dispatch
- `inferFunctionResultType()` — binder/utils.go:347 — return type inference
- `evalStrptime()` — temporal_functions.go:1718-1758 — string→timestamp parsing
- `evalStrftime()` — temporal_functions.go:1677-1713 — timestamp→string formatting
- `timeToDate()` — temporal_functions.go:461-463 — converts time.Time to DATE (int32 days)
- `dateToTime()` — temporal_functions.go:89-91 — converts DATE to time.Time
- `parseStrftimeFormat()` — temporal_functions.go:1505-1589 — format string parser
- `executeGenerateSeries()` — table_function_series.go:13-127 — table function pattern
- `toString()` — expr.go:4202 — `func toString(v any) string`
- TYPE_DATE — type_enum.go — Type = 13 (days since epoch)
- TYPE_VARCHAR — standard string type
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Goals

1. Add TO_DATE as STRPTIME variant returning DATE instead of TIMESTAMP
2. Add TO_CHAR as alias for STRFTIME
3. Add GENERATE_SUBSCRIPTS as table function generating array indices
