# Change: Enhance Cost-Based Optimizer with Advanced Statistics and Subquery Optimization

## Why

The dukdb-go project has a solid foundation for cost-based optimization with ~5,900 lines of optimizer code, but significant gaps remain compared to DuckDB v1.4.3. These gaps impact query performance and correctness:

1. **No Statistics Persistence**: Query plans degrade over time as data changes without updating statistics
2. **No Subquery Decorrelation**: Correlated subqueries execute row-by-row instead of being converted to joins
3. **Limited Predicate Pushdown**: Filters not pushed to scan level, causing unnecessary data processing
4. **No Cross-Predicate Selectivity**: Poor estimates for queries with multiple correlated predicates
5. **No Auto-Update Statistics**: Stale statistics after bulk inserts/deletes

## What Changes

**Implementation Strategy**: All features implemented together as complete optimizer overhaul. No incremental releases.

**DuckDB Parity Goal**: Match DuckDB v1.4.3 optimizer behavior exactly across all areas.

### Statistics Persistence and Auto-Update
- **ADDED**: Statistics persistence in DuckDB-compatible binary format
- **ADDED**: Auto-update statistics trigger matching DuckDB behavior
- **ADDED**: Incremental statistics updates for large tables
- **ADDED**: Statistics loading on database open
- **ADDED**: Migration support for older statistics formats

### Subquery Decorrelation (Full DuckDB v1.4.3 Parity)
- **ADDED**: Subquery decorrelation (FlattenDependentJoin algorithm)
- **ADDED**: EXISTS, NOT EXISTS, SCALAR subqueries
- **ADDED**: IN, NOT IN, ANY/ALL subqueries
- **ADDED**: Multi-level correlation support
- **ADDED**: LATERAL join support
- **ADDED**: Correlated CTEs
- **ADDED**: Correlated column tracking

### Predicate Pushdown and Multi-Column Statistics
- **ADDED**: Predicate pushdown into table scans
- **ADDED**: Filter pushdown past joins (matching DuckDB filter_pushdown.cpp)
- **ADDED**: Complex AND/OR filter tree handling
- **ADDED**: Multi-column statistics with joint NDV
- **ADDED**: Cross-predicate selectivity estimation matching DuckDB heuristics

### Cardinality Learning and Adaptive Optimization
- **ADDED**: Cardinality learning (track actual vs estimate)
- **ADDED**: Conservative N-observation threshold before applying corrections
- **ADDED**: Adaptive cost constants based on runtime feedback
- **ADDED**: Bounded memory usage for historical corrections

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

**CRITICAL** - Full DuckDB v1.4.3 optimizer parity required. All phases are critical and must be implemented together.

**Primary Concern**: Avoiding partial/lazy implementations. This change requires:
- Complete feature implementation (not incremental)
- 60/40+ testing-to-implementation effort ratio
- Triple validation: correctness, EXPLAIN comparison, cardinality estimates
- DuckDB source code reference in all complex functions

**Quality Bar**:
- Match DuckDB query performance on TPC-H benchmark
- Pass all three validation methods (see design.md)
- Zero semantic differences from DuckDB behavior
