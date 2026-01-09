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

**Delivery Strategy**: All phases implemented together, merged as complete optimizer overhaul. No incremental releases to prevent partial implementations.

### Phase 1: Statistics Persistence and Auto-Update

```
internal/optimizer/
├── stats_persistence.go  # DuckDB binary format serialization/deserialization
├── stats_auto_update.go  # Modification tracking, threshold-based ANALYZE trigger
└── stats_incremental.go  # Large table incremental sampling
```

**DuckDB Reference Files**:
- `references/duckdb/src/storage/statistics/base_statistics.cpp`
- `references/duckdb/src/storage/statistics/numeric_statistics.cpp`
- `references/duckdb/src/storage/statistics/string_statistics.cpp`

### Phase 2: Subquery Decorrelation (Full DuckDB v1.4.3 Parity)

```
internal/optimizer/
├── subquery_decorrelate.go  # FlattenDependentJoin, unnesting algorithms
├── correlated_tracker.go    # Correlated column identification and tracking
└── lateral_join.go          # LATERAL subquery support
```

**DuckDB Reference Files**:
- `references/duckdb/src/optimizer/unnest_rewriter.cpp`
- `references/duckdb/src/optimizer/decorrelate.cpp`
- `references/duckdb/src/planner/subquery/flatten_dependent_join.cpp`

**Required Subquery Support** (DuckDB v1.4.3 Feature Checklist):
- [ ] EXISTS correlated subqueries
- [ ] NOT EXISTS correlated subqueries
- [ ] SCALAR correlated subqueries (single value)
- [ ] IN correlated subqueries
- [ ] NOT IN correlated subqueries
- [ ] ANY/ALL correlated subqueries
- [ ] Multi-level correlation (outer -> middle -> inner)
- [ ] LATERAL joins
- [ ] Correlated CTEs
- [ ] Recursive CTEs with correlation (if supported by DuckDB v1.4.3)
- [ ] Mixed correlation patterns (multiple outer references)

### Phase 3: Predicate Pushdown and Multi-Column Statistics

```
internal/optimizer/
├── filter_pushdown.go       # Filter pushdown to scans and past joins
├── multi_column_stats.go    # Joint NDV, column pair statistics
└── predicate_selectivity.go  # Cross-predicate correlation estimates
```

**DuckDB Reference Files**:
- `references/duckdb/src/optimizer/filter_pushdown.cpp`
- `references/duckdb/src/optimizer/column_binding_replacer.cpp`
- `references/duckdb/src/storage/statistics/distinct_statistics.cpp`

**Filter Pushdown Feature Checklist**:
- [ ] Push filters to table scans
- [ ] Push filters past inner joins
- [ ] Preserve filters for outer joins (left/right/full)
- [ ] Handle complex AND/OR filter trees
- [ ] Respect filter dependencies on join columns
- [ ] Push filters into subqueries when safe
- [ ] Maintain predicate equivalence

**Multi-Column Statistics Checklist**:
- [ ] Collect joint NDV for column pairs
- [ ] HyperLogLog for large cardinality estimation (if DuckDB uses it)
- [ ] Detect correlated columns during ANALYZE
- [ ] Match DuckDB heuristics for which column pairs to track
- [ ] Integrate with cardinality estimation

### Phase 4: Cardinality Learning and Adaptive Optimization

```
internal/optimizer/
├── cardinality_learner.go  # Track actual vs estimated, compute corrections
└── adaptive_cost.go         # Adjust cost constants based on runtime data
```

**DuckDB Reference**: Research if DuckDB has similar adaptive features. If not, implement conservative approach.

**Cardinality Learning Checklist**:
- [ ] Track actual row counts per operator
- [ ] Compare actual vs estimated cardinalities
- [ ] Compute correction multipliers after N observations
- [ ] Apply corrections to future estimates
- [ ] Bound memory usage (evict old corrections)
- [ ] Adaptive cost constants based on execution timing

## Testing and Validation Strategy

**Testing Effort**: 60% testing, 40% implementation (more testing than coding)

### Triple Validation Requirement

Every optimizer enhancement MUST pass all three validation methods:

#### 1. Correctness Testing with DuckDB

```bash
# For every test query:
# 1. Run query on DuckDB CLI (references/duckdb/duckdb)
# 2. Run same query on dukdb-go
# 3. Compare results (row-by-row, order-independent)

# Example test case:
duckdb_result=$(duckdb test.db "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)")
dukdb_result=$(./dukdb-go-cli test.db "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)")
diff <(echo "$duckdb_result" | sort) <(echo "$dukdb_result" | sort) || echo "FAIL"
```

**Test Coverage Requirements**:
- All subquery types from checklist (EXISTS, SCALAR, IN, ANY/ALL, etc.)
- Edge cases: empty subqueries, NULL handling, multi-level correlation
- Complex queries from TPC-H benchmark
- Queries with mixed optimization opportunities

#### 2. EXPLAIN Output Comparison

```bash
# Compare query plans between systems
duckdb test.db "EXPLAIN SELECT ..." > duckdb_plan.txt
./dukdb-go-cli test.db "EXPLAIN SELECT ..." > dukdb_plan.txt

# Plans don't need to be identical in formatting, but should have:
# - Same join order
# - Same join types (HASH JOIN, MERGE JOIN, etc.)
# - Same filter placement
# - Similar cardinality estimates (within reasonable margin)
```

**Plan Comparison Checklist**:
- [ ] Join order matches DuckDB
- [ ] Join algorithm selection matches (hash vs merge vs nested loop)
- [ ] Filters pushed to same level
- [ ] Subqueries decorrelated identically
- [ ] Index usage matches (when applicable)

#### 3. Cardinality Estimate Comparison

```bash
# Use EXPLAIN ANALYZE to compare estimated vs actual cardinalities
duckdb test.db "EXPLAIN ANALYZE SELECT ..."
./dukdb-go-cli test.db "EXPLAIN ANALYZE SELECT ..."

# Compare:
# - Estimated row counts at each operator
# - Should be within 2x of DuckDB estimates (or better)
# - Actual row counts should match exactly (correctness requirement)
```

**Cardinality Validation**:
- Track estimate accuracy over TPC-H queries
- Compare selectivity factors for common predicates
- Ensure statistics collection produces similar histograms

### Test Database Generation

```bash
# Create test databases using DuckDB CLI for validation
cd references/duckdb
./duckdb test.db <<EOF
CREATE TABLE t1 (id INT, value VARCHAR);
INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c');
ANALYZE t1;
.save test.db
EOF

# Now use test.db for correctness comparison
```

### TPC-H Benchmark Requirement

**Goal**: Match DuckDB performance on TPC-H queries (within 10-20% variance)

```bash
# Run TPC-H benchmark suite
./run-tpch-benchmark.sh duckdb > duckdb-results.txt
./run-tpch-benchmark.sh dukdb-go > dukdb-results.txt

# Compare execution times per query
# Acceptable: dukdb-go within 0.8x to 1.2x of DuckDB times
```

**Performance Acceptance Criteria**:
- No query should be >2x slower than DuckDB
- Average across all queries within 10-20% of DuckDB
- Some queries may be faster due to Go-specific optimizations

### Test Organization

```
internal/optimizer/
├── optimizer_test.go           # Unit tests for optimizer components
├── subquery_correctness_test.go # DuckDB comparison tests for subqueries
├── filter_pushdown_test.go     # Filter optimization tests
├── statistics_test.go          # Statistics persistence/loading tests
└── testdata/
    ├── duckdb_databases/       # Test DBs generated by DuckDB CLI
    ├── expected_plans/         # Golden EXPLAIN output from DuckDB
    └── tpch/                   # TPC-H queries and expected results
```

### Inline Documentation Requirements

Every complex function MUST include:

```go
// DecorrelateSCALARSubquery converts a SCALAR correlated subquery to a LEFT OUTER JOIN.
//
// DuckDB Reference: references/duckdb/src/optimizer/unnest_rewriter.cpp::UnnestSCALAR
//
// Algorithm:
// 1. Identify correlated columns (columns referencing outer query)
// 2. Create LEFT OUTER JOIN with subquery (preserves NULL for no match)
// 3. Add LIMIT 1 if subquery could return multiple rows (SCALAR semantics)
// 4. Replace subquery reference with join column reference
//
// Example transformation:
//   SELECT (SELECT t2.val FROM t2 WHERE t2.id = t1.id) FROM t1
//   =>
//   SELECT t2.val FROM t1 LEFT OUTER JOIN (SELECT id, val FROM t2 LIMIT 1) t2 ON t2.id = t1.id
func (d *Decorrelator) DecorrelateSCALARSubquery(subquery *ast.SelectStmt) (*PhysicalPlan, error) {
    // ... implementation ...
}
```

**Documentation Must Include**:
- DuckDB reference file and function name
- High-level algorithm explanation
- Example transformation (before/after SQL)
- Edge cases and NULL handling
- Cardinality impact

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
