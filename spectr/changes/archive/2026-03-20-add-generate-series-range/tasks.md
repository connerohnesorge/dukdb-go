## 1. Binder: Dedicated Table Function Binding

- [ ] 1.1 Add `bindGenerateSeriesTableFunction()` to binder (following UNNEST pattern)
- [ ] 1.2 Add dispatch for "generate_series" and "range" in `bindTableFunction()`
- [ ] 1.3 Implement argument count validation (2 or 3 args required)
- [ ] 1.4 Implement type resolution: determine output type from bound start/stop expression types
- [ ] 1.5 Store bound expressions in Options map (keys: "start", "stop", "step")
- [ ] 1.6 Update PostgreSQL alias transformer to pass through to native generate_series

## 2. Executor: Series Generation Implementation

- [ ] 2.1 Create `internal/executor/table_function_series.go`
- [ ] 2.2 Add `generate_series` and `range` cases to `executeTableFunctionScan` dispatch
- [ ] 2.3 Extract and evaluate bound expressions from plan.Options via evaluateExpr()
- [ ] 2.4 Implement integer series generation with batching (StandardVectorSize chunks)
- [ ] 2.5 Implement DATE series generation with INTERVAL step (use addInterval helper)
- [ ] 2.6 Implement TIMESTAMP series generation with INTERVAL step
- [ ] 2.7 Handle edge cases: zero step error, empty ranges, single-value series, overflow
- [ ] 2.8 Handle NULL arguments (return empty result for any NULL arg)
- [ ] 2.9 Set output column name to function name

## 3. Integration Tests

- [ ] 3.1 Test: generate_series integer ascending with default step
- [ ] 3.2 Test: generate_series integer with explicit step
- [ ] 3.3 Test: generate_series descending with negative step
- [ ] 3.4 Test: generate_series DATE with INTERVAL step
- [ ] 3.5 Test: generate_series TIMESTAMP with INTERVAL step
- [ ] 3.6 Test: range integer (exclusive stop verification)
- [ ] 3.7 Test: range DATE (exclusive stop verification)
- [ ] 3.8 Test: Error on zero step
- [ ] 3.9 Test: Empty result on direction mismatch
- [ ] 3.10 Test: Single value when start == stop (generate_series)
- [ ] 3.11 Test: Empty when start == stop (range)
- [ ] 3.12 Test: Column aliasing with AS t(n)
- [ ] 3.13 Test: Large series (>2048 rows) verifies batching works
