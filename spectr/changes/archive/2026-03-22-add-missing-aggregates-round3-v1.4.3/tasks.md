# Tasks: Missing Aggregates Round 3

- [ ] 1. Add PRODUCT aggregate — Add "PRODUCT" case to computeAggregate() (physical_aggregate.go:295). Multiply all non-NULL values with identity 1.0. Follow SUM pattern (line 342). Register in isAggregateFunc() (operator.go:99-122). Add TYPE_DOUBLE to inferFunctionResultType() (binder/utils.go:347). Validate: `SELECT PRODUCT(x) FROM (VALUES (2), (3), (4)) t(x)` returns 24.

- [ ] 2. Add MAD aggregate — Add "MAD" case to computeAggregate(). Collect values (reuse collectValues helper), compute median, then compute median of absolute deviations. Follow MEDIAN pattern (physical_aggregate.go:449). Register and add type inference. Validate: `SELECT MAD(x) FROM (VALUES (1),(2),(3),(4),(5)) t(x)` returns 1.0.

- [ ] 3. Add FAVG and FSUM aggregates — Add "FAVG" and "FSUM" cases using Kahan summation algorithm for floating-point accuracy. FAVG divides by count, FSUM returns raw sum. Follow AVG (line 368) and SUM (line 342) patterns. Register and add type inference. Validate: Both return TYPE_DOUBLE.

- [ ] 4. Add BITSTRING_AGG — Add "BITSTRING_AGG" case. Aggregate boolean values into a VARCHAR bitstring ('101011'). Use toBool() (expr.go:4184) for conversion. Register in isAggregateFunc(). Return TYPE_VARCHAR from inferFunctionResultType(). Validate: Aggregating true/false/true returns '101'.

- [ ] 5. Integration tests — Test all aggregates with NULL values, empty sets, GROUP BY. Verify PRODUCT identity (empty → NULL). Verify MAD computation. Compare FAVG/FSUM accuracy to AVG/SUM.
