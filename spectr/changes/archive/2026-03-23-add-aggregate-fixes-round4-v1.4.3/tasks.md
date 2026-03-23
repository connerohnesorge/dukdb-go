# Tasks: Aggregate Registration Fixes and Missing Aggregates Round 4

- [ ] 1. Fix isAggregateFunc registration — Add BOOL_AND, BOOL_OR, EVERY, BIT_AND, BIT_OR, BIT_XOR, REGR_COUNT, REGR_AVGX, REGR_AVGY, REGR_SXX, REGR_SYY, REGR_SXY to the switch in isAggregateFunc() at operator.go:99-124. Also add the 4 new aggregate names. Validate: `SELECT BOOL_AND(true)` executes without error.

- [ ] 2. Add ARBITRARY alias — Add "ARBITRARY" to existing FIRST case label in computeAggregate(). Register in isAggregateFunc(). Validate: `SELECT ARBITRARY(x) FROM (VALUES (1)) t(x)` returns 1.

- [ ] 3. Add MEAN alias — Add "MEAN" to existing AVG case label in computeAggregate(). Register in isAggregateFunc(). Validate: `SELECT MEAN(x) FROM (VALUES (10), (20)) t(x)` returns 15.0.

- [ ] 4. Implement GEOMETRIC_MEAN/GEOMEAN — Add case in computeAggregate() using exp(avg(ln(x))) formula. Handle non-positive values by returning NULL. Add type inference (TYPE_DOUBLE). Validate: `SELECT GEOMETRIC_MEAN(x) FROM (VALUES (2), (8)) t(x)` returns 4.0.

- [ ] 5. Implement WEIGHTED_AVG — Add case in computeAggregate() with 2-arg dispatch (value, weight). Compute sum(v*w)/sum(w). Add type inference (TYPE_DOUBLE). Validate: `SELECT WEIGHTED_AVG(s, w) FROM (VALUES (90, 3), (80, 1)) t(s, w)` returns 87.5.

- [ ] 6. Integration tests — Test all 12 newly-registered aggregates work in GROUP BY queries. Test 4 new aggregates with NULL handling, empty groups, and edge cases.
