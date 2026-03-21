# Tasks: Ordered-Set Aggregates (WITHIN GROUP + LISTAGG)

- [ ] 1. Parse WITHIN GROUP syntax — In `internal/parser/parser.go`, after consuming the closing `)` of a function call, check for WITHIN keyword. Parse `WITHIN GROUP (ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST])`. Map the parsed ORDER BY into the existing `FunctionCall.OrderBy` field. Error if both internal ORDER BY and WITHIN GROUP are used. Validate: `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary)` parses correctly with OrderBy populated.

- [ ] 2. Add LISTAGG aggregate dispatch — In `internal/executor/physical_aggregate.go`, add case for "LISTAGG" near STRING_AGG (line 630). Use `collectValuesWithOrderBy()` for value collection. Default delimiter is empty string (unlike STRING_AGG's comma). Reuse `computeStringAgg()` for concatenation. Register "LISTAGG" in aggregate function name list at operator.go:109. Validate: `SELECT LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) FROM t` returns concatenated sorted names.

- [ ] 3. Add LISTAGG type inference — In `internal/binder/utils.go`, add "LISTAGG" to the STRING_AGG/GROUP_CONCAT case at line 554 that returns `dukdb.TYPE_VARCHAR`. Validate: LISTAGG queries compile without type errors.

- [ ] 4. Integration tests — Test WITHIN GROUP with PERCENTILE_CONT, PERCENTILE_DISC, MODE. Test LISTAGG with: delimiter, no delimiter (empty default), GROUP BY, NULL values, WITHIN GROUP ORDER BY ASC/DESC. Test error: conflicting internal ORDER BY and WITHIN GROUP. Verify no regressions in existing STRING_AGG and PERCENTILE_CONT tests.
