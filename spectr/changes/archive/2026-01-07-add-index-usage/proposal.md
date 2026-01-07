# Proposal: Index Usage in Query Plans

## Summary

Implement index-aware query optimization that enables the query planner to use existing indexes for equality and range predicates, including index scan operators, cost-based index vs table scan selection, index-only scans (covering indexes), and composite index handling.

## Motivation

Currently, dukdb-go supports CREATE INDEX statements and maintains hash-based indexes (HashIndex in internal/storage/index.go), but these indexes are never used during query execution:

- Indexes are created and stored in the catalog (catalog.IndexDef)
- Index data structures are maintained with Insert/Delete/Lookup operations
- Query plans always use full table scans regardless of available indexes
- Predicates like `WHERE id = 5` scan all rows even when an index exists on `id`

This results in:
- Wasted resources maintaining indexes that provide no benefit
- Poor query performance for selective lookups
- Incompatibility with DuckDB behavior where indexes accelerate queries

## Problem Statement

### Current State
- Hash indexes exist but are only used for unique constraint enforcement
- Physical planner always generates SeqScan for table access
- No mechanism to detect when indexes can satisfy predicates
- No cost comparison between index scan and table scan
- No index-only scan for queries selecting only indexed columns

### Target State
- Query planner checks for applicable indexes on filter predicates
- IndexScan operator retrieves rows via index lookup
- Cost model compares index scan vs table scan costs
- Index-only scan avoids table access when index covers query columns
- Composite indexes match prefix predicates correctly

## Scope

### In Scope

1. **Index Scan Operator**
   - New PhysicalIndexScan operator type
   - Integration with existing HashIndex.Lookup()
   - Support for single and composite key lookups
   - Row ID to tuple resolution

2. **Index Selection in Optimizer**
   - Detect equality predicates on indexed columns
   - Match predicates to available indexes
   - Handle composite index prefix matching
   - Integrate with cost-based optimizer decision making

3. **Cost Model for Index Scan**
   - Estimate index lookup cost
   - Estimate random I/O for row fetches
   - Compare with sequential scan cost
   - Consider selectivity and table size

4. **Index-Only Scan**
   - Detect when index covers all required columns
   - Avoid heap table access when possible
   - Project columns directly from index

5. **Integration with Cost-Based Optimizer**
   - Add index scan as alternative access method
   - Select cheapest access method per table
   - Pass index hints to physical planner

### Out of Scope
- Range indexes (B-tree, ART) - current HashIndex only supports equality
- Index intersection/union
- Partial indexes
- Expression indexes
- Automatic index creation/recommendation
- Index maintenance optimizations

## Approach

### Phase 1: Index Scan Infrastructure
1. Create PhysicalIndexScan operator
2. Wire IndexScan execution to HashIndex.Lookup()
3. Resolve RowIDs to actual tuples

### Phase 2: Index Selection
1. Analyze filter predicates for index applicability
2. Match predicates to catalog indexes
3. Handle composite index prefix matching

### Phase 3: Cost Model Integration
1. Add cost formulas for index scan
2. Implement index vs table scan comparison
3. Integrate with existing CostModel

### Phase 4: Index-Only Scan
1. Detect covering index scenarios
2. Implement index-only scan path
3. Project columns from index structure

### Phase 5: Optimizer Integration
1. Enumerate index scan alternatives in optimizer
2. Select cheapest access method
3. Generate optimal physical plan

## Dependencies

- **GAP-002 (Cost-Based Optimizer)**: Provides cost model and plan enumeration framework
- **Existing HashIndex**: Provides index lookup infrastructure
- **Existing Catalog**: Provides index metadata

## Success Criteria

1. **Selective Query Improvement**: 10x+ speedup for highly selective indexed lookups
2. **Correct Plan Selection**: Index scan chosen only when cheaper than table scan
3. **Index-Only Scan**: Heap access avoided when index covers query
4. **Backward Compatibility**: All existing tests pass

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| HashIndex only supports equality | Known | Medium | Document limitation, future B-tree work |
| Cost model inaccuracy | Medium | Medium | Conservative defaults, testing |
| Composite index matching complexity | Medium | Low | Start with simple prefix matching |
| Integration with optimizer | Low | Medium | Build on GAP-002 infrastructure |

## Affected Specs

- **NEW**: `index-usage` - Index scan operators and optimization
- **MODIFIED**: `cost-based-optimizer` - Add index scan cost formulas
- **MODIFIED**: `execution-engine` - Add PhysicalIndexScan operator
