# Change: Enhance Cost-Based Optimizer with Advanced Statistics and Subquery Optimization

## Why

The dukdb-go project has a solid foundation for cost-based optimization with ~5,900 lines of optimizer code, but significant gaps remain compared to DuckDB v1.4.3. These gaps impact query performance and correctness:

1. **No Statistics Persistence**: Query plans degrade over time as data changes without updating statistics
2. **No Subquery Decorrelation**: Correlated subqueries execute row-by-row instead of being converted to joins
3. **Limited Predicate Pushdown**: Filters not pushed to scan level, causing unnecessary data processing
4. **No Cross-Predicate Selectivity**: Poor estimates for queries with multiple correlated predicates
5. **No Auto-Update Statistics**: Stale statistics after bulk inserts/deletes

## What Changes

### Phase 1: Statistics Foundation
- **ADDED**: Statistics persistence to on-disk metadata
- **ADDED**: Auto-update statistics trigger after DML threshold
- **ADDED**: Incremental statistics updates for large tables
- **ADDED**: Statistics loading on database open

### Phase 2: Subquery Optimization
- **ADDED**: Subquery decorrelation (FlattenDependentJoin algorithm)
- **ADDED**: Correlated EXISTS, SCALAR, and ANY subquery support
- **ADDED**: LATERAL join support
- **ADDED**: Correlated column tracking

### Phase 3: Advanced Optimizations
- **ADDED**: Predicate pushdown into table scans
- **ADDED**: Filter pushdown past joins
- **ADDED**: Multi-column statistics
- **ADDED**: Cross-predicate selectivity estimation

### Phase 4: Runtime Adaptation
- **ADDED**: Cardinality learning (track actual vs estimate)
- **ADDED**: Adaptive cost constants
- **ADDED**: Runtime feedback for future estimates

## Impact

- Affected specs: `specs/cost-based-optimizer/spec.md`
- Affected code:
  - `internal/optimizer/statistics.go` - Extended structures
  - `internal/optimizer/stats_manager.go` - Persistence + auto-update
  - `internal/optimizer/cardinality.go` - Cross-predicate estimates
  - `internal/optimizer/analyze.go` - Enhanced sampling
  - `internal/optimizer/optimizer.go` - Decorrelation integration
  - `internal/planner/` - Subquery transformation integration
  - `internal/binder/` - Correlated column handling
- Breaking changes: None
- Dependencies: Storage layer for statistics persistence

## Priority

**HIGH** - The cost-based optimizer is critical for query performance. Without these enhancements, complex queries will have suboptimal plans.
