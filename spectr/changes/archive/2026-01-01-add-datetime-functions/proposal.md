# Proposal: Add Date/Time Functions

## Summary

Add comprehensive date/time function support to dukdb-go, implementing 30+ temporal functions from DuckDB v1.4.3. This includes date extraction functions (YEAR, MONTH, DAY, etc.), date arithmetic (DATE_ADD, DATE_SUB, DATE_DIFF), formatting/parsing (STRFTIME, STRPTIME), and interval operations.

## Motivation

Date/time functions are essential for analytical SQL workloads. Currently, dukdb-go has minimal date/time function support:

**Existing Functions** (already implemented):
- NOW(), CURRENT_TIMESTAMP -> TIMESTAMP
- CURRENT_DATE -> DATE
- CURRENT_TIME -> TIME

**Missing Functions** (this proposal):
- Date extraction: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DAYOFWEEK, DAYOFYEAR, WEEK, QUARTER
- Date arithmetic: DATE_ADD, DATE_SUB, DATE_DIFF, DATE_TRUNC, DATE_PART, AGE
- Date construction: MAKE_DATE, MAKE_TIMESTAMP, MAKE_TIME
- Formatting/parsing: STRFTIME, STRPTIME, TO_TIMESTAMP, EPOCH
- Interval operations: INTERVAL literal parsing, interval arithmetic

This gap prevents users from performing common analytical queries such as:
- Extracting date components for grouping (GROUP BY YEAR(date))
- Calculating date differences (days between orders)
- Formatting dates for display
- Parsing dates from string inputs

## Scope

### In Scope

1. **Date Extraction Functions** (10 functions):
   - YEAR(date/timestamp) -> INTEGER
   - MONTH(date/timestamp) -> INTEGER (1-12)
   - DAY(date/timestamp) -> INTEGER (1-31)
   - HOUR(timestamp) -> INTEGER (0-23)
   - MINUTE(timestamp) -> INTEGER (0-59)
   - SECOND(timestamp) -> DOUBLE (0-59.999...)
   - DAYOFWEEK(date) -> INTEGER (0=Sunday, 6=Saturday)
   - DAYOFYEAR(date) -> INTEGER (1-366)
   - WEEK(date) -> INTEGER (ISO week 1-53)
   - QUARTER(date) -> INTEGER (1-4)

2. **Date Arithmetic Functions** (9 functions):
   - DATE_ADD(date, interval) -> date/timestamp
   - DATE_SUB(date, interval) -> date/timestamp
   - DATE_DIFF(part, start, end) -> BIGINT
   - DATE_TRUNC(part, timestamp) -> timestamp
   - DATE_PART(part, timestamp) -> DOUBLE
   - AGE(timestamp1, timestamp2) -> INTERVAL
   - LAST_DAY(date) -> DATE
   - EXTRACT(part FROM timestamp) -> DOUBLE (SQL standard syntax)

3. **Date Construction Functions** (4 functions):
   - MAKE_DATE(year, month, day) -> DATE
   - MAKE_TIMESTAMP(year, month, day, hour, min, sec) -> TIMESTAMP
   - MAKE_TIME(hour, minute, second) -> TIME

4. **Formatting/Parsing Functions** (5 functions):
   - STRFTIME(format, timestamp) -> VARCHAR
   - STRPTIME(string, format) -> TIMESTAMP
   - TO_TIMESTAMP(seconds) -> TIMESTAMP
   - EPOCH(timestamp) -> DOUBLE
   - EPOCH_MS(timestamp) -> BIGINT

5. **Interval Operations**:
   - INTERVAL literal parsing ('1 day', '2 hours')
   - Interval + date/timestamp arithmetic
   - Interval - date/timestamp arithmetic
   - TO_YEARS, TO_MONTHS, TO_DAYS, TO_HOURS, TO_MINUTES, TO_SECONDS

### Out of Scope

- Timezone-aware operations (AT TIME ZONE) - complex, Phase 2
- Calendar functions (holidays, business days) - domain-specific
- Temporal tables (system time, application time) - SQL:2011 feature
- Date formatting localization - complex i18n support

## Design

See [design.md](./design.md) for detailed architectural decisions and implementation strategy.

## Capabilities Affected

| Capability | Change Type |
|------------|-------------|
| execution-engine | MODIFIED - Add temporal function evaluation |

## Dependencies

- None - builds on existing type system (TYPE_DATE, TYPE_TIMESTAMP, TYPE_TIME, TYPE_INTERVAL)

## Risks

1. **Format String Complexity**: STRFTIME/STRPTIME format specifiers are complex.
   - Mitigation: Implement common specifiers first, document limitations.

2. **Timezone Edge Cases**: Date arithmetic near DST boundaries can be tricky.
   - Mitigation: Initial implementation uses UTC-only; add timezone in Phase 2.

3. **INTERVAL Parsing**: DuckDB INTERVAL syntax is complex ('1 year 2 months 3 days').
   - Mitigation: Start with simple single-unit intervals, expand to compound intervals.

## Success Criteria

1. All 30+ date/time functions pass compatibility tests against DuckDB CLI
2. Date extraction functions work with DATE, TIMESTAMP, and TIME types
3. STRFTIME supports common format specifiers (%Y, %m, %d, %H, %M, %S, etc.)
4. INTERVAL arithmetic produces correct results
5. NULL handling matches DuckDB semantics
