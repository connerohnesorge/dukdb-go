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

**Status**: Pending

Options:
1. Push all filters to scan level (simplest)
2. Push filters based on selectivity (more complex)
3. Push filters only if index available (most selective)

**Recommendation**: Start with Option 1, evolve to Option 2 based on performance data.

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Statistics persistence complexity | Medium | Start simple, add features incrementally |
| Subquery decorrelation correctness | High | Extensive testing with correlated subqueries |
| Performance regression | Medium | Benchmark before/after, gradual rollout |
| Memory usage for learning | Low | Configurable history size |

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

## Open Questions

1. Should we support statistics persistence in DuckDB format?
2. What's the right auto-update threshold (10%, 20%, configurable)?
3. Should subquery decorrelation be opt-in or always-on?
4. How much history should cardinality learning keep?

## References

- DuckDB Optimizer: references/duckdb/src/optimizer/
- Subquery Decorrelation: https://paper.grabmueller.de/
- PostgreSQL Statistics: https://www.postgresql.org/docs/current/planner-stats.html
