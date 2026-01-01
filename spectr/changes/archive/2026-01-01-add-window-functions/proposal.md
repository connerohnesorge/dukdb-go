# Proposal: Add Window Functions

## Summary

Add comprehensive SQL window function support to dukdb-go, implementing 11 dedicated window functions plus 5 aggregate-over-window functions from DuckDB v1.4.3. This includes ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE), value access functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE), distribution functions (PERCENT_RANK, CUME_DIST), and aggregate windows (COUNT, SUM, AVG, MIN, MAX with OVER clause).

## Motivation

Window functions are essential for analytical SQL workloads. Currently, dukdb-go has **zero** window function support:

- No ROW_NUMBER, RANK, DENSE_RANK, NTILE ranking functions
- No LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE value functions
- No PERCENT_RANK, CUME_DIST distribution functions
- No OVER clause parsing or execution
- No PARTITION BY or ORDER BY within windows
- No frame specification (ROWS/RANGE BETWEEN)

This gap prevents users from performing common analytical queries such as:
- Ranking results within categories
- Computing running totals and moving averages
- Comparing values with previous/next rows
- Calculating percentiles and distributions

## Scope

### In Scope

1. **Parser Extensions**: OVER clause parsing with PARTITION BY, ORDER BY, and frame specifications
2. **AST Additions**: WindowExpr and WindowFrame node types
3. **Binder Extensions**: Window expression binding and type resolution
4. **Physical Operator**: PhysicalWindow operator for partitioned evaluation
5. **Window Functions**:
   - Ranking: ROW_NUMBER, RANK, DENSE_RANK, NTILE (return BIGINT)
   - Value: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE (return argument type)
   - Distribution: PERCENT_RANK, CUME_DIST (return DOUBLE)
6. **Aggregate-over-Window**: COUNT, SUM, AVG, MIN, MAX with OVER clause
7. **Frame Specifications**: ROWS BETWEEN, RANGE BETWEEN, GROUPS BETWEEN, UNBOUNDED PRECEDING/FOLLOWING
8. **Frame EXCLUDE Clause**: EXCLUDE NO OTHERS, EXCLUDE CURRENT ROW, EXCLUDE GROUP, EXCLUDE TIES
9. **IGNORE NULLS Clause**: For LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
10. **NULLS FIRST/LAST**: In ORDER BY within window specification
11. **FILTER Clause**: For aggregate window functions (e.g., COUNT(*) FILTER (WHERE x > 5) OVER (...))
12. **DISTINCT Aggregates**: COUNT(DISTINCT x) OVER (...), SUM(DISTINCT x) OVER (...)

### Out of Scope

- Named window definitions (WINDOW w AS (...) ... OVER w)
- Window function optimization (e.g., segment trees for O(log n) frame evaluation)
- FILL() window function (complex interpolation, can add in Phase 2)
- Argument-specific ORDER BY (FIRST_VALUE(x ORDER BY y) OVER (...)) - can add in Phase 2

## Design

See [design.md](./design.md) for detailed architectural decisions and implementation strategy.

## Capabilities Affected

| Capability | Change Type |
|------------|-------------|
| execution-engine | MODIFIED - Add PhysicalWindow operator |
| parser | MODIFIED - Add OVER clause and WindowExpr AST |

## Dependencies

- None - builds on existing DataChunk and operator infrastructure

## Risks

1. **Memory Usage**: Window functions materialize partitions in memory. Large partitions may cause OOM.
   - Mitigation: Document memory requirements, consider streaming for simple cases in future.

2. **Performance**: Naive frame evaluation is O(n²) for sliding windows.
   - Mitigation: Initial implementation uses O(n²); optimize with segment trees in follow-up proposal.

3. **Complexity**: Window semantics are intricate (NULL handling, frame boundaries, peer groups).
   - Mitigation: Comprehensive test suite against DuckDB CLI reference.

## Success Criteria

1. All 11 window functions pass compatibility tests against DuckDB CLI
2. OVER clause with PARTITION BY, ORDER BY, and frame specifications works correctly
3. Aggregate functions (COUNT, SUM, AVG, MIN, MAX) work with OVER clause
4. NULL handling matches DuckDB semantics
5. Performance: 100K rows with 100 partitions processes in <1 second
