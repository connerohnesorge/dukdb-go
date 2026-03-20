# GROUPING SETS/ROLLUP/CUBE Execution Tasks

## 1. Executor Fixes

- [ ] 1.1 Fix GROUPING() column naming bug in `executeHashAggregateWithGroupingSets` (operator.go:1356-1359): replace the hardcoded `"GROUPING"` column name with `plan.Aliases[numGroupBy+numAgg+i]` so that multiple GROUPING() calls get distinct column names matching their SELECT aliases
- [ ] 1.2 Verify that inactive GROUP BY columns are set to NULL (not missing) in each grouping set's output rows
- [ ] 1.3 Verify that aggregates (SUM, COUNT, AVG, MIN, MAX) operate on the correct subset of rows per grouping set

## 2. Planner Fixes

- [ ] 2.1 Fix `extractGroupingSets` to prepend regular GROUP BY columns to each expanded grouping set when mixed GROUP BY clauses are used (e.g., `GROUP BY department, ROLLUP(region, product)` should produce sets `(department, region, product), (department, region), (department)`)
- [ ] 2.2 Verify `extractGroupingCalls` correctly extracts GROUPING() calls regardless of their position in the SELECT list
- [ ] 2.3 Verify that PhysicalHashAggregate correctly propagates GroupingSets and GroupingCalls from LogicalAggregate

## 3. Binder Verification

- [ ] 3.1 Verify ROLLUP expansion produces correct sets (e.g., `ROLLUP(a, b)` produces `(a, b), (a), ()`)
- [ ] 3.2 Verify CUBE expansion produces correct sets (e.g., `CUBE(a, b)` produces `(a, b), (a), (b), ()`)
- [ ] 3.3 Verify GROUPING() function call is correctly bound via the dedicated binder path (not function registry) with column references as arguments
- [ ] 3.4 Verify that GROUPING SETS with explicit sets passes through binding without expansion

## 4. Integration Tests

- [ ] 4.1 Test: `GROUP BY GROUPING SETS ((region, product), (region), ())` with SUM aggregate
- [ ] 4.2 Test: `GROUP BY ROLLUP(region, product)` produces same results as equivalent GROUPING SETS
- [ ] 4.3 Test: `GROUP BY CUBE(region, product)` produces all 2^n grouping combinations
- [ ] 4.4 Test: `GROUPING(region)` returns correct bitmask for each grouping set
- [ ] 4.5 Test: Multiple GROUPING() calls in same SELECT (`GROUPING(region) AS g1, GROUPING(product) AS g2`)
- [ ] 4.6 Test: Grand total row (empty grouping set) has all GROUP BY columns as NULL
- [ ] 4.7 Test: ROLLUP/CUBE with NULL values in grouping columns
- [ ] 4.8 Test: GROUPING SETS with single-column groups
- [ ] 4.9 Test: ROLLUP with three columns (three-level hierarchy)

## 5. Edge Cases

- [ ] 5.1 Test: Empty table with GROUPING SETS (should produce grand total row for empty set)
- [ ] 5.2 Test: ROLLUP with single column (`ROLLUP(a)` = `GROUPING SETS ((a), ())`)
- [ ] 5.3 Test: CUBE with single column (`CUBE(a)` = `GROUPING SETS ((a), ())`)
- [ ] 5.4 Test: COUNT(*) aggregate with grouping sets (should count rows, not non-NULL values)
- [ ] 5.5 Test: Multiple aggregate functions (SUM, COUNT, AVG) in same query with ROLLUP

## 6. Validation

- [ ] 6.1 Run full test suite: `nix develop -c tests`
- [ ] 6.2 Run linter: `nix develop -c lint`
- [ ] 6.3 Verify no regressions in existing aggregate tests
