# Tasks: Missing Numeric Functions

- [ ] 1. Add SIGNBIT function — Add case in evaluateFunctionCall() (expr.go:661). Use math.Signbit(). Add type inference returning TYPE_BOOLEAN. Validate: `SELECT SIGNBIT(-0.0)` returns true.

- [ ] 2. Add WIDTH_BUCKET function — Add case in evaluateFunctionCall(). Implement SQL standard equi-width histogram bucketing. Return 0 for below-range, numBuckets+1 for above-range. Add type inference returning TYPE_INTEGER. Validate: `SELECT WIDTH_BUCKET(5.0, 0.0, 10.0, 5)` returns 3.

- [ ] 3. Add BETA function — Add case in evaluateFunctionCall(). Use exp(lgamma(a) + lgamma(b) - lgamma(a+b)) for numerical stability. Add type inference returning TYPE_DOUBLE. Validate: `SELECT BETA(1, 1)` returns 1.0.

- [ ] 4. Register conditional aggregates — Add SUM_IF, AVG_IF, MIN_IF, MAX_IF to isAggregateFunc() at operator.go:99-122. Add type inference entries in binder/utils.go.

- [ ] 5. Implement conditional aggregates — Add SUM_IF, AVG_IF, MIN_IF, MAX_IF cases in computeAggregate() at physical_aggregate.go:295. Follow COUNT_IF pattern at line 764. Evaluate condition (2nd arg), only aggregate matching rows. Validate: `SELECT SUM_IF(x, x > 0) FROM t` sums only positive values.

- [ ] 6. Integration tests — Test all scalar and aggregate functions with NULL propagation, edge cases, type verification.
