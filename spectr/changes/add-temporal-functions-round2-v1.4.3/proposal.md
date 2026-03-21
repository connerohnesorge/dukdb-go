# Proposal: Temporal Functions Round 2

## Summary

Add missing temporal functions and date part specifiers for DuckDB v1.4.3 compatibility: TIME_BUCKET, MAKE_TIMESTAMPTZ, additional date part aliases (ISODOW, ISOYEAR, WEEKDAY, WEEKOFYEAR, DATEPART), EPOCH_NS, and the TIMEZONE/AT TIME ZONE conversion function.

## Motivation

Several temporal functions and date part specifiers that DuckDB v1.4.3 supports are missing. TIME_BUCKET is critical for time-series analytics. ISODOW/ISOYEAR are PostgreSQL standard. MAKE_TIMESTAMPTZ completes the MAKE_* family.

## Scope

- **Executor**: Add TIME_BUCKET, MAKE_TIMESTAMPTZ, TIMEZONE functions to evaluateFunctionCall() (expr.go:661)
- **Executor**: Add DATEPART alias for DATE_PART
- **Executor**: Add ISODOW, ISOYEAR, WEEKDAY, WEEKOFYEAR, EPOCH_NS to parseDatePart() (temporal_functions.go:35)
- **Binder**: Add type inference entries

## Files Affected

- `internal/executor/expr.go` — function dispatch
- `internal/executor/temporal_functions.go` — parseDatePart() and extractPart(), new functions
- `internal/binder/utils.go` — type inference
