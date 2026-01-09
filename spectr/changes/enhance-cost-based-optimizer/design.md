## Context

The dukdb-go project has a solid foundation for cost-based optimization with these existing components:
- `statistics.go`: TableStatistics, ColumnStatistics, Histogram, Bucket structures
- `stats_manager.go`: StatisticsManager with default fallback
- `analyze.go`: StatisticsCollector with reservoir sampling, equi-depth histograms
- `cardinality.go`: CardinalityEstimator with full selectivity estimation
- `cost_model.go`: Comprehensive CostModel for all major operators
- `join_order.go`: JoinOrderOptimizer with DPccp algorithm
- `plan_enumerator.go`: PlanEnumerator for physical plan selection
- `index_matcher.go`: IndexMatcher for index selection

However, several advanced features are missing compared to DuckDB v1.4.3.

## Goals / Non-Goals

### Goals
- Implement statistics persistence and auto-update
- Implement subquery decorrelation for correlated subqueries
- Implement predicate pushdown optimization
- Implement multi-column statistics and cross-predicate selectivity
- Implement cardinality learning for runtime adaptation

### Non-Goals
- Full distributed query execution
- GPU acceleration
- SIMD vectorization (already exists in executor)
- Query result caching

## Technical Approach

### Phase 1: Statistics Persistence and Auto-Update

**Statistics Persistence**:
```go
// internal/optimizer/stats_manager.go

type PersistentStats struct {
    TableName     string
    TableStats    TableStatistics
    ColumnStats   map[string]ColumnStatistics
    LastUpdated   time.Time
    SampleRate    float64
}

// Store statistics in catalog metadata
func (sm *StatisticsManager) SavePersistentStats(db *Database, stats *PersistentStats) error
func (sm *StatisticsManager) LoadPersistentStats(db *Database, tableName string) (*PersistentStats, error)
```

**Auto-Update Trigger**:
```go
// Track modifications per table
type ModificationTracker struct {
    tables map[string]*TableModification
}

const AUTO_ANALYZE_THRESHOLD = 0.10 // 10% change triggers ANALYZE

func (t *ModificationTracker) RecordModification(tableName string, rowCount int64) {
    // Update modification count
    // Trigger ANALYZE when threshold exceeded
}
```

### Phase 2: Subquery Decorrelation

**FlattenDependentJoin Algorithm**:
```go
// Transform correlated subquery to JOIN
func (d *Decorator) FlattenDependentJoin(subquery *LogicalPlan) (*LogicalPlan, error) {
    // 1. Identify correlated columns
    correlatedCols := d.findCorrelatedColumns(subquery)

    // 2. Create decorrelation plan
    decorrelated, err := d.createDecorrelationPlan(subquery, correlatedCols)
    if err != nil {
        return nil, err
    }

    // 3. Generate dependent join
    return &PhysicalDepJoin{
        Left:     d.outerPlan,
        Right:    decorrelated,
        Columns:  correlatedCols,
    }, nil
}
```

**Subquery Types Supported**:
- EXISTS correlated subqueries
- SCALAR correlated subqueries
- ANY/ALL subqueries with correlation
- IN correlated subqueries

### Phase 3: Predicate Pushdown and Multi-Column Statistics

**Filter Pushdown**:
```go
type FilterPushdown struct {
    pushedFilters map[string][]Expression
}

func (f *FilterPushdown) pushFilter(
    tableName string,
    filter Expression,
) {
    // Analyze filter
    // Determine if pushable to scan level
    // Update plan with pushed filter
}
```

**Multi-Column Statistics**:
```go
type MultiColumnStats struct {
    Columns      []string
    NDV          int64  // Number of distinct value combinations
    Histogram    *Histogram // Joint distribution
    Correlations map[string]float64
}
```

### Phase 4: Cardinality Learning

**Runtime Feedback**:
```go
type CardinalityLearner struct {
    estimates    map[string]*Estimate
    actuals      map[string]*int64
    corrections  map[string]float64 // Multiplier for future estimates
}

func (l *CardinalityLearner) Record(
    queryID string,
    operator string,
    estimated int64,
    actual int64,
) {
    ratio := float64(actual) / float64(estimated)
    l.corrections[operator] = l.adaptiveAverage(operator, ratio)
}

func (l *CardinalityLearner) GetCorrection(operator string) float64 {
    return l.corrections[operator]
}
```

## Decisions

### Decision 1: Statistics Storage Location

**Status**: Approved

Statistics will be stored in the catalog metadata table alongside table definitions. This ensures:
- Persistence across database restarts
- Consistent with how DuckDB stores statistics
- Easy to query and modify

### Decision 2: Subquery Decorrelation Scope

**Status**: Approved

Initial scope includes:
- EXISTS, SCALAR, ANY subqueries
- Single-level correlation
- Non-recursive correlated subqueries

Future work (Phase 3):
- Multi-level correlation
- Recursive CTEs with correlation
- Complex correlation patterns

### Decision 3: Filter Pushdown Strategy

**Status**: Approved

**Decision**: Match DuckDB v1.4.3 filter pushdown behavior exactly.

Implementation will follow DuckDB source code in `references/duckdb/src/optimizer/filter_pushdown.cpp` to ensure full parity. This includes:
- Push filters to scan level when safe
- Push filters past joins based on column dependencies
- Handle complex AND/OR filter trees correctly
- Preserve filter semantics for outer joins

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Partial/lazy implementations** | **CRITICAL** | Require DuckDB v1.4.3 feature completeness checklist (see below), 60/40+ testing-to-code ratio, peer review before merge |
| Subquery decorrelation correctness | High | Triple validation: EXPLAIN comparison, correctness testing, cardinality estimates vs DuckDB |
| Statistics format reverse engineering | High | Read DuckDB source code directly, validate with DuckDB-generated databases |
| Performance regression on simple queries | Medium | TPC-H benchmark parity requirement, profile optimization overhead |
| Code complexity and maintenance | Medium | Comprehensive inline documentation, architectural comments explaining DuckDB parity |
| Memory usage for learning | Low | Conservative N-observation threshold before applying corrections |

### Critical: Avoiding Partial Implementations

To prevent incomplete "looks done but has gaps" implementations:

1. **Feature Completeness Checklist**: Every DuckDB v1.4.3 optimizer feature must be explicitly checked off (see Implementation Plan below)
2. **Triple Validation**: All features must pass:
   - Correctness testing (same results as DuckDB)
   - EXPLAIN output comparison (same plans as DuckDB)
   - Cardinality estimate comparison (same estimates as DuckDB)
3. **No Incremental Shipping**: All phases implemented together, merged as complete optimizer overhaul
4. **60/40+ Testing Ratio**: More effort on testing than implementation to catch gaps early
5. **DuckDB Source Reference**: Every complex function must reference corresponding DuckDB source file in inline comments

## Implementation Plan

### Phase 1: Statistics (2-3 weeks)

```
internal/optimizer/
├── stats_persistence.go  # Save/load statistics to catalog
├── stats_auto_update.go  # Track modifications, trigger ANALYZE
└── stats_incremental.go  # Incremental updates for large tables
```

### Phase 2: Subquery (3-4 weeks)

```
internal/optimizer/
├── subquery_decorrelate.go  # FlattenDependentJoin algorithm
├── correlated_tracker.go    # Track correlated columns
└── lateral_join.go          # LATERAL subquery execution
```

### Phase 3: Optimizations (4-5 weeks)

```
internal/optimizer/
├── filter_pushdown.go      # Push filters to scans
├── multi_column_stats.go   # Cross-predicate statistics
└── predicate_selectivity.go # Advanced selectivity estimation
```

### Phase 4: Learning (2-3 weeks)

```
internal/optimizer/
├── cardinality_learner.go  # Runtime feedback
└── adaptive_cost.go        # Adaptive cost constants
```

## Effort Estimate

| Phase | Effort | Dependencies |
|-------|--------|--------------|
| Phase 1: Statistics | 2-3 weeks | Storage layer |
| Phase 2: Subquery | 3-4 weeks | Binder, Parser |
| Phase 3: Optimizations | 4-5 weeks | Planner |
| Phase 4: Learning | 2-3 weeks | None |
| **Total** | **11-15 weeks** | |

## Resolved Questions

### 1. Statistics Persistence Format
**Decision**: DuckDB-compatible binary format

Read DuckDB source code in `references/duckdb/src/storage/statistics/` to reverse engineer the exact binary format. This enables:
- Potential import of statistics from DuckDB databases
- Exact parity with DuckDB behavior
- Future compatibility as format evolves

Implementation approach:
- Study `references/duckdb/src/storage/statistics/base_statistics.cpp`
- Create serialization/deserialization matching DuckDB format
- Validate with test databases created by DuckDB CLI
- Add version checking for format compatibility

### 2. Auto-Update Statistics Threshold
**Decision**: Match DuckDB CLI behavior

Research exact threshold and batching behavior from `references/duckdb/src/optimizer/statistics/` to ensure identical auto-update behavior. No custom thresholds or configuration options - pure DuckDB parity.

### 3. Subquery Decorrelation Behavior
**Decision**: Match DuckDB CLI behavior, always enabled

Study `references/duckdb/src/optimizer/unnest_rewriter.cpp` and related files to implement:
- Full DuckDB v1.4.3 subquery decorrelation capabilities
- Exact same fallback behavior when decorrelation not possible
- All subquery types: EXISTS, SCALAR, IN, ANY/ALL, multi-level correlation
- No opt-in/opt-out - always attempt decorrelation like DuckDB

### 4. Cardinality Learning History Size
**Decision**: After N observations (conservative approach)

Require minimum sample size before applying corrections. Research DuckDB's adaptive optimizer settings if they have similar features. Conservative to avoid overfitting to outlier queries.

## References

- DuckDB Optimizer: references/duckdb/src/optimizer/
- Subquery Decorrelation: https://paper.grabmueller.de/
- PostgreSQL Statistics: https://www.postgresql.org/docs/current/planner-stats.html
