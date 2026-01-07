# Proposal: Cost-Based Query Optimization

## Summary

Implement a cost-based query optimizer that uses table statistics (cardinality, selectivity, histograms) to select optimal join orders, access methods, and execution strategies, replacing the current heuristic-based optimizer with data-driven decision making.

## Motivation

Currently, dukdb-go uses a simple heuristic-based planner that:
- Applies fixed transformation rules without considering data characteristics
- Uses join order as written in the query (left-to-right)
- Always chooses hash join regardless of table sizes
- Cannot estimate query costs for EXPLAIN output

This results in:
- Suboptimal query plans for complex multi-join queries
- Poor performance when small tables should be used as build side
- No ability to compare alternative execution strategies
- Inaccurate cost estimates in EXPLAIN output

## Problem Statement

### Current State
- Physical planner converts logical plans 1:1 without optimization
- No statistics infrastructure to estimate cardinalities
- Join order follows query structure, not optimal order
- No cost model for comparing alternative plans
- EXPLAIN shows structure but not costs

### Target State
- Physical planner explores alternative plans and selects lowest cost
- Statistics available for all tables (via ANALYZE)
- Join order optimized using dynamic programming
- Cost model estimates CPU, I/O, and memory costs
- EXPLAIN shows estimated costs and row counts

## Scope

### In Scope
1. **Statistics Infrastructure**
   - Table statistics: row count, page count, data size
   - Column statistics: distinct count, null fraction, min/max
   - Histogram support (equi-depth) for selectivity estimation
   - Statistics storage in catalog

2. **Cardinality Estimation**
   - Base table cardinality from statistics
   - Filter selectivity using histograms and min/max
   - Join cardinality estimation
   - Aggregation cardinality estimation

3. **Cost Model**
   - CPU cost (tuple processing, expression evaluation)
   - I/O cost (sequential vs random page access)
   - Memory cost (hash tables, sort buffers)
   - Configurable cost constants

4. **Join Order Optimization**
   - Dynamic programming for N <= 12 tables
   - Greedy heuristic for N > 12 tables
   - Support all join types (inner, left, right, full, semi, anti)
   - Build side selection for hash joins

5. **Physical Plan Selection**
   - Hash join vs nested loop selection
   - Index scan consideration (when indexes exist)
   - Sort-merge join for pre-sorted data

6. **Integration**
   - Integrate between logical and physical planning
   - Preserve all existing functionality
   - Add cost annotations to EXPLAIN output

### Out of Scope
- Adaptive query execution (runtime re-optimization)
- Query plan caching
- Materialized view matching
- Automatic index recommendation
- Parallel execution planning (future work)

## Approach

### Phase 1: Statistics Infrastructure
1. Define TableStatistics and ColumnStatistics structures
2. Implement statistics storage in catalog
3. Implement basic ANALYZE command (full table scan)
4. Add sample-based ANALYZE for large tables

### Phase 2: Cardinality Estimation
1. Implement base table cardinality from row count
2. Implement filter selectivity estimation
3. Implement join cardinality estimation
4. Handle complex predicates (AND, OR, NOT)

### Phase 3: Cost Model
1. Define cost constants and formulas
2. Implement cost estimation for each physical operator
3. Add cumulative cost calculation for plan trees
4. Make cost constants configurable

### Phase 4: Plan Enumeration
1. Implement join order enumeration (DPccp algorithm)
2. Implement greedy fallback for large queries
3. Add build side selection for hash joins
4. Consider index scans where applicable

### Phase 5: Integration
1. Create CostBasedOptimizer component
2. Wire into query execution pipeline
3. Add cost annotations to EXPLAIN
4. Comprehensive testing

## Success Criteria

1. **TPC-H Performance**: 2x improvement on multi-join TPC-H queries
2. **Optimizer Overhead**: < 5% overhead for simple queries
3. **Join Handling**: Correctly optimize 10+ table joins
4. **Cardinality Accuracy**: Estimates within 10x of actual for common patterns
5. **Backward Compatibility**: All existing tests pass

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Cardinality estimation errors | High | Medium | Conservative defaults, testing |
| Optimizer overhead | Medium | Medium | Fast-path for simple queries |
| Regression on existing queries | Low | High | Extensive regression testing |
| Complex predicate handling | Medium | Medium | Fall back to defaults |

## Dependencies

- **GAP-003 (Statistics/ANALYZE)**: Provides statistics collection
- Current logical planner: Provides input logical plans
- Current executor: Provides execution capabilities
- Catalog system: Stores statistics

## Affected Specs

- **NEW**: `cost-based-optimizer` - Core optimizer functionality
- **MODIFIED**: `execution-engine` - Add optimizer integration
- **MODIFIED**: `catalog` - Add statistics storage
