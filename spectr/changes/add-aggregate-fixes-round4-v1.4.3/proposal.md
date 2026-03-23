# Proposal: Aggregate Registration Fixes and Missing Aggregates Round 4

## Summary

Fix 12 aggregate functions that are implemented in computeAggregate() dispatch but NOT registered in isAggregateFunc(), causing query plan failures. Also add 4 missing aggregate functions: GEOMETRIC_MEAN/GEOMEAN, WEIGHTED_AVG, ARBITRARY (alias for FIRST), MEAN (alias for AVG).

## Motivation

The 12 unregistered aggregates (BOOL_AND, BOOL_OR, EVERY, BIT_AND, BIT_OR, BIT_XOR, REGR_COUNT, REGR_AVGX, REGR_AVGY, REGR_SXX, REGR_SYY, REGR_SXY) have working implementations in physical_aggregate.go but fail in query plans because isAggregateFunc() at operator.go:99-124 doesn't list them. The planner treats them as scalar functions and generates incorrect plans.

The 4 new aggregates are standard DuckDB v1.4.3 functions that complete the aggregate function set.

## Scope

- **Registration**: Add 12 function names to isAggregateFunc() at operator.go:99-124
- **New aggregates**: Add GEOMETRIC_MEAN, WEIGHTED_AVG, ARBITRARY, MEAN to computeAggregate() at physical_aggregate.go:295
- **Binder**: Add type inference for new aggregates

## Files Affected

- `internal/executor/operator.go` — isAggregateFunc() (line 99)
- `internal/executor/physical_aggregate.go` — computeAggregate() (line 295)
- `internal/binder/utils.go` — inferFunctionResultType() (line 347)
