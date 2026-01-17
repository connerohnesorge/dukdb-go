# DuckDB v1.4.3 Optimizer Research

This document contains the detailed findings from studying DuckDB v1.4.3 source code to guide the implementation of the enhance-cost-based-optimizer change proposal for dukdb-go.

## 1. Statistics Format (Task 1.1)

### Binary Serialization Structure

DuckDB v1.4.3 uses a versioned serialization system for statistics. The format is defined in:
- `src/storage/statistics/base_statistics.cpp` - Base statistics serialization
- `src/storage/statistics/column_statistics.cpp` - Column statistics with property-based serialization
- `src/storage/statistics/distinct_statistics.cpp` - Distinct value tracking using HyperLogLog

#### BaseStatistics Properties

```cpp
// from base_statistics.cpp
class BaseStatistics {
    LogicalType type;
    bool has_null;           // Can data contain NULL values
    bool has_no_null;        // Can data contain non-NULL values
    idx_t distinct_count;    // Number of distinct values
    StatsUnion stats_union;  // Type-specific statistics (numeric min/max, etc.)
    vector<unique_ptr<BaseStatistics>> child_stats; // For nested types
};

// Numeric statistics union
struct NumericStatsData {
    NumericValueUnion min;
    NumericValueUnion max;
};
```

#### Serialization Format

DuckDB uses a property-based serialization system with version tracking:

**Property Structure** (from `column_statistics.cpp`):
```cpp
void ColumnStatistics::Serialize(Serializer &serializer) const {
    serializer.WriteProperty(100, "statistics", stats);        // BaseStatistics
    serializer.WritePropertyWithDefault(101, "distinct",
                                       distinct_stats,
                                       unique_ptr<DistinctStatistics>());
}

shared_ptr<ColumnStatistics> ColumnStatistics::Deserialize(Deserializer &deserializer) {
    auto stats = deserializer.ReadProperty<BaseStatistics>(100, "statistics");
    auto distinct_stats = deserializer.ReadPropertyWithExplicitDefault<unique_ptr<DistinctStatistics>>(
        101, "distinct", unique_ptr<DistinctStatistics>());
    return make_shared_ptr<ColumnStatistics>(std::move(stats), std::move(distinct_stats));
}
```

**Key Property IDs**:
- 100: BaseStatistics
- 101: DistinctStatistics

#### Statistics Type Mapping

DuckDB categorizes statistics by logical type (from `base_statistics.cpp`):

```cpp
enum StatisticsType {
    BASE_STATS,        // Default for unsupported types
    NUMERIC_STATS,     // INT8-64, UINT8-64, INT128, UINT128, FLOAT, DOUBLE
    STRING_STATS,      // VARCHAR
    LIST_STATS,        // LIST types
    STRUCT_STATS,      // STRUCT types
    ARRAY_STATS,       // ARRAY types
    GEOMETRY_STATS,    // GEOMETRY type
    VARIANT_STATS      // VARIANT type
};

// Numeric types: BOOL, INT8-64, UINT8-64, INT128, UINT128, FLOAT, DOUBLE
// String types: VARCHAR
// Unsupported (BASE_STATS): BIT, INTERVAL, SQLNULL
```

#### DistinctStatistics: HyperLogLog Implementation

DuckDB uses HyperLogLog for cardinality estimation (from `distinct_statistics.cpp`):

```cpp
class DistinctStatistics {
    unique_ptr<HyperLogLog> log;     // HyperLogLog approximator
    idx_t sample_count;              // Rows sampled
    idx_t total_count;               // Total rows processed
};

// Sampling rates
const double BASE_SAMPLE_RATE = 0.1;      // 10% sample
const double INTEGRAL_SAMPLE_RATE = 0.5;  // 50% sample for integral types

// Cardinality estimation using Good-Turing estimation
idx_t GetCount() const {
    double u = MinValue(log->Count(), sample_count);      // Unique values in sample
    double s = sample_count;                               // Sample size
    double n = total_count;                                // Total rows

    // Assume this proportion occurred only once
    double u1 = pow(u / s, 2) * u;

    // Good-Turing estimation
    idx_t estimate = u + u1 / s * (n - s);
    return MinValue(estimate, total_count);
}
```

**Sampling Behavior**:
- Integral types (INT, UINT, BOOL): 50% sample rate
- Other types: 10% sample rate
- Sample size bounded by STANDARD_VECTOR_SIZE (typically 2048 rows)

#### NULL Handling

```cpp
void InitializeUnknown() {
    has_null = true;      // Can contain NULL
    has_no_null = true;   // Can contain non-NULL (unknown distribution)
}

void InitializeEmpty() {
    has_null = false;     // No NULL
    has_no_null = false;  // No data at all
}

// Actual data can be any combination:
// has_null=true, has_no_null=true   => May have NULL and non-NULL
// has_null=true, has_no_null=false  => All NULL
// has_null=false, has_no_null=true  => No NULL values
// has_null=false, has_no_null=false => Empty result
```

#### Format Version Compatibility

Key findings:
- Property IDs in range 100-101 are stable across versions
- PropertyWithDefault supports version evolution
- ReadPropertyWithExplicitDefault handles missing properties gracefully
- No explicit version number in column statistics header (versioning via property IDs)

### For dukdb-go Implementation

**Must Implement**:
1. Property-based serialization with IDs 100 (BaseStatistics) and 101 (DistinctStatistics)
2. StatisticsType mapping for all logical types
3. HyperLogLog for distinct count estimation with Good-Turing correction
4. Dual has_null/has_no_null flags for NULL tracking
5. Separate numeric min/max storage per numeric type
6. Version-forward-compatible property writing

---

## 2. Auto-Update Triggers (Task 1.2)

### DML Modification Tracking

**Key Finding**: DuckDB v1.4.3 does NOT have automatic statistics update triggers implemented. The research found NO evidence of:
- Modification counters on tables
- Automatic ANALYZE invocation after DML
- Threshold-based triggers

This is confirmed by:
- No adaptive/learning/feedback mechanism in optimizer code
- Statistics propagator only reads existing statistics
- No "dirty" flags or modification tracking in table metadata

### Statistics Management Architecture

From `optimizer/statistics_propagator.cpp`:
```cpp
class StatisticsPropagator {
    Optimizer &optimizer;
    ClientContext &context;
    LogicalOperator &root;

    // Reads statistics from existing sources, doesn't trigger updates
    unique_ptr<NodeStatistics> PropagateStatistics(LogicalOperator &node);
};
```

The propagator:
1. Traverses logical query plan
2. Reads existing table statistics
3. Propagates through operators
4. Does NOT modify stored statistics
5. Does NOT trigger new ANALYZE operations

### Manual ANALYZE Behavior

**What IS in DuckDB v1.4.3**:
- ANALYZE statement: Collects table statistics manually
- `internal/optimizer/analyze.go` in dukdb-go already implements this
- Statistics stored per column per table
- Sampling-based collection for performance

### Implications for dukdb-go

**Decision**: Implement conservative auto-update as extension, not DuckDB parity feature.

Recommendation from design.md is correct: Track modifications and suggest ANALYZE when:
- 10% row count change on table
- Explicit user invocation recommended (not automatic)

This is a **novel feature beyond DuckDB v1.4.3**, but design.md accepts this:
> "If adaptive features exist [in DuckDB], how they work" - they don't, so implement conservatively

---

## 3. Subquery Decorrelation (Task 1.3)

### Algorithm: Unnest Rewriter and FlattenDependentJoin

DuckDB implements subquery decorrelation via the **Unnest Rewriter** pattern, found in:
- `src/optimizer/unnest_rewriter.cpp` - Core rewriting logic
- Related files: `filter_pushdown.cpp` contains DELIM_JOIN handling

#### Core Pattern Recognition

The unnest rewriter looks for specific operator patterns (from `unnest_rewriter.cpp` lines 64-166):

```
Pattern 1: Projection/Window/Filter/Aggregate/Unnest
    └─ LOGICAL_DELIM_JOIN (INNER join, 1 condition)
        ├─ LOGICAL_WINDOW
        │  └─ (Projection/CrossJoin)
        └─ Path to LOGICAL_UNNEST
            └─ LOGICAL_DELIM_GET

Pattern 2: Unnest as table function
    └─ LOGICAL_GET (unnest function)
        └─ LOGICAL_DELIM_GET
```

#### Correlation Detection

Correlations identified through:

```cpp
// Collect columns eliminated by duplicate elimination
void GetDelimColumns(LogicalOperator &op) {
    auto &delim_join = op.Cast<LogicalComparisonJoin>();
    for (auto &expr : delim_join.duplicate_eliminated_columns) {
        // These are the correlated outer references
        auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
        delim_columns.push_back(bound_colref.binding);
    }
}

// Collect LHS (outer) expressions
void GetLHSExpressions(LogicalOperator &op) {
    auto col_bindings = op.GetColumnBindings();
    // These become inputs to the decorrelated join
    for (auto binding : col_bindings) {
        lhs_bindings.push_back(binding);
    }
}
```

#### Transformation Example

Original plan with correlation:
```
Window
├─ Filter: (subquery result references outer.id)
└─ DELIM_JOIN (INNER)
    ├─ outer_table_scan
    └─ DElIM_GET -> Unnest(inner_table_scan where inner.id = outer.id)
```

After decoration:
```
Unnest(inner_table_scan)
└─ Window
   └─ outer_table_scan
```

**Key transformation**: Correlation condition moves from WHERE clause to UNNEST structure, enabling parallel processing.

#### Binding Updates

Critical step: Update column bindings after rewriting (lines 220-309):

```cpp
void UpdateRHSBindings(...) {
    // 1. Shift existing RHS bindings
    for (each expression in projection) {
        UpdateColumnBinding(old_binding, new_binding_with_offset);
    }

    // 2. Add LHS expressions to projections
    for (each LHS expression) {
        AddToProjection(lhs_expression);
    }

    // 3. Update UNNEST expression bindings
    for (each unnest_expr) {
        ReplaceBinding(old_delim_binding, new_lhs_binding);
    }
}
```

#### Decorrelation Limitations

The unnest rewriter has scope limitations:

```cpp
// Pattern requirements:
// - DELIM_JOIN must be INNER join
// - DELIM_JOIN must have exactly ONE condition
// - LHS must be WINDOW operator
// - RHS must be path of Projections to UNNEST
```

**Subquery Types Supported**:
- EXISTS (via DELIM_JOIN with mark join conversion)
- SCALAR (via Unnest with LIMIT 1)
- IN predicates (via hash join after decorrelation)

**Subquery Types NOT directly supported**:
- Multi-level correlation (handled iteratively by applying rewriter multiple times)
- Recursive CTEs with correlation
- Complex OR patterns across subqueries

### Filter Pushdown Integration

From `filter_pushdown.cpp`, the interaction with correlated subqueries:

```cpp
// Mark joins can be optimized to semi joins after correlation analysis
void CheckMarkToSemi(LogicalOperator &op, unordered_set<idx_t> &table_bindings) {
    if (join.join_type != JoinType::MARK) return;

    // If mark index used above join, cannot convert to semi
    if (table_bindings.find(join.mark_index) != table_bindings.end()) {
        join.convert_mark_to_semi = false;
    }
}
```

### NULL Handling in Subqueries

**Critical Semantic**:
```sql
-- IN with NULL in subquery
SELECT * FROM t1 WHERE id IN (SELECT id FROM t2 WHERE id IS NULL);
-- Result: Empty (no match, NULL doesn't match anything)

-- Outer query NULL
SELECT * FROM t1 WHERE NULL IN (SELECT id FROM t2);
-- Result: Empty (NULL comparison always false/unknown)
```

Implementation handles NULL propagation through DistinctStatistics:
- Tracks has_null flag per column
- Filter selectivity accounts for NULL distribution

---

## 4. Filter Pushdown (Task 1.4)

### Algorithm Overview

DuckDB's filter pushdown (from `src/optimizer/filter_pushdown.cpp`) uses recursive descent with join type awareness:

```cpp
class FilterPushdown {
    FilterCombiner combiner;        // Combines AND/OR filters
    vector<unique_ptr<Filter>> filters;
    bool convert_mark_joins;
};

unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op) {
    switch (op->type) {
    case LOGICAL_AGGREGATE: return PushdownAggregate(...);
    case LOGICAL_FILTER: return PushdownFilter(...);
    case LOGICAL_CROSS_PRODUCT: return PushdownCrossProduct(...);
    case LOGICAL_COMPARISON_JOIN: return PushdownJoin(...);
    case LOGICAL_PROJECTION: return PushdownProjection(...);
    // ... etc for all operator types
    }
}
```

### Filter Dependencies and Column Bindings

Core concept: Filter can only push to a child if all referenced columns exist in that child.

```cpp
void ExtractBindings() {
    ExpressionIterator::EnumerateExpression(filter_expr, [&](Expression &child) {
        if (child.GetExpressionClass() == BOUND_COLUMN_REF) {
            auto &col_ref = child.Cast<BoundColumnRefExpression>();
            bindings.insert(col_ref.binding.table_index);
        }
    });
}

// Can push filter to child?
for (auto &binding : filter.bindings) {
    if (child_bindings.find(binding) == child_bindings.end()) {
        cannot_push = true;  // Filter references unavailable column
    }
}
```

### Join Type Specific Handling

**INNER JOIN** (lines 173):
```cpp
// Both sides can be filtered
result = PushdownInnerJoin(left, right, left_bindings, right_bindings);
// Filters on left: push to left child
// Filters on right: push to right child
// Filters referencing both: split and push parts separately
```

**LEFT JOIN** (lines 175):
```cpp
// Filters on left can be pushed to left side
// Filters on right or both sides must stay above join (preserve outer join semantics)
result = PushdownLeftJoin(left, right, left_bindings, right_bindings);
```

**OUTER (FULL) JOIN** (lines 164):
```cpp
// All filters stay above join to preserve NULL rows
result = PushdownOuterJoin(...);
```

**SEMI/ANTI JOIN** (lines 184):
```cpp
// Special handling for semi/anti joins from EXISTS/NOT EXISTS
result = PushdownSemiAntiJoin(...);
```

**MARK JOIN** (lines 178):
```cpp
// Mark joins used for subqueries - may convert to semi if safe
result = PushdownMarkJoin(...);
```

### Complex Filter Tree Handling

Filters are split by AND into independent conditions:

```cpp
FilterResult AddFilter(unique_ptr<Expression> expr) {
    vector<unique_ptr<Expression>> expressions;
    expressions.push_back(std::move(expr));
    LogicalFilter::SplitPredicates(expressions);  // Split by AND

    for (auto &child_expr : expressions) {
        combiner.AddFilter(std::move(child_expr));
    }
}
```

**OR filters**: Kept together, pushed only if ALL branches can push.

### Filter to Scan Pushdown

Filters containing only scan columns push to table scan level:

```cpp
case LOGICAL_GET:
    return PushdownGet(std::move(op));
    // Filters with only GET bindings become table scan filters
    // May enable column pruning or index selection
```

### Predicate Equivalence Proof

Filter pushdown maintains correctness through:

```
Original: Filter(condition, Child)
Pushdown: Filter_outer(...), Child with Filter_inner

Correctness property:
  Filter(P, Child) ≡ Filter(filter_outer, Filter(filter_inner, Child))

Where:
  P = filter_outer AND filter_inner
  filter_inner: references only Child columns
  filter_outer: references columns added by Child
```

---

## 5. Multi-Column Statistics Heuristics (Task 1.5)

### DistinctStatistics Architecture

From `src/storage/statistics/distinct_statistics.cpp`:

DuckDB uses per-column distinct value tracking, but doesn't store explicit multi-column statistics in v1.4.3.

```cpp
class DistinctStatistics {
    unique_ptr<HyperLogLog> log;
    idx_t sample_count;
    idx_t total_count;
};

// Stored per ColumnStatistics
class ColumnStatistics {
    BaseStatistics stats;
    unique_ptr<DistinctStatistics> distinct_stats;  // Per-column only
};
```

### Supported Types for Distinct Tracking

From `distinct_statistics.cpp` line 72:

```cpp
bool TypeIsSupported(const LogicalType &type) {
    switch (type.InternalType()) {
    case PhysicalType::LIST:
    case PhysicalType::STRUCT:
    case PhysicalType::ARRAY:
        return false;  // Nested types not supported
    case PhysicalType::BIT:
    case PhysicalType::BOOL:
        return false;  // Doesn't make semantic sense
    default:
        return true;   // INT, FLOAT, VARCHAR, etc.
    }
}
```

**Supported**: INT, UINT, FLOAT, DOUBLE, VARCHAR, DATE, TIME, TIMESTAMP, INTERVAL
**Unsupported**: LIST, STRUCT, ARRAY, BIT, BOOL

### Sampling and Good-Turing Estimation

The distinct count estimation algorithm (lines 55-70):

```cpp
idx_t GetCount() const {
    if (sample_count == 0 || total_count == 0) {
        return 0;
    }

    // Unique values in sample
    double u = MinValue(log->Count(), sample_count);
    double s = sample_count;
    double n = total_count;

    // Good-Turing Estimation: Proportion of values that appear only once
    double u1 = pow(u / s, 2) * u;

    // Extrapolate: (values seen once) * (un-sampled proportion) / sampled proportion
    idx_t estimate = u + u1 / s * (n - s);

    // Never estimate more than total count
    return MinValue(estimate, total_count);
}
```

**Theoretical Basis**: Good-Turing estimation is a statistical technique that uses the count of items appearing exactly once to estimate the count of unseen items.

### Integration with Cardinality Estimation

For join cardinality, DuckDB uses:

```cpp
// Join result cardinality = left_rows * (right_rows / right_distinct_on_join_col)
// This assumes uniform distribution across join column values
```

### Heuristics for Column Pair Selection

**In DuckDB v1.4.3**: No explicit multi-column statistics collected during ANALYZE.

Instead, cardinality estimates use single-column statistics with:
1. Independence assumption for AND predicates
2. Selectivity multiplication: sel(A=1 AND B=2) ≈ sel(A=1) * sel(B=2)

### NULL Distribution Impact

```cpp
// Distinct count excludes NULL values in most contexts
// has_null flag determines if NULL should be counted separately

// Filter selectivity with NULL:
// SELECT * FROM t WHERE col = 5;
// Rows: (total - null_count) / distinct_count (if not NULL-matching)
```

---

## 6. Cardinality Learning Research (Task 1.6)

### Finding: No Adaptive Optimizer in DuckDB v1.4.3

**Comprehensive Search Results**:
- No `adaptive`, `learning`, `feedback`, or `correction` keywords in optimizer code
- StatisticsPropagator is read-only (no runtime learning)
- CostModel only uses current statistics, never modifies them
- No historical correction multipliers stored

### What DuckDB v1.4.3 DOES Have

1. **EXPLAIN ANALYZE**: Shows estimated vs actual cardinality
2. **Cost-based join order**: Uses cardinality estimates for optimization
3. **Statistics caching**: Per-session statistics remain unchanged during query

### What dukdb-go CAN Add (Novel Feature)

The design.md proposal for cardinality learning is **beyond DuckDB v1.4.3 capabilities**:

```go
type CardinalityLearner struct {
    estimates    map[OperatorSignature]*Estimate
    actuals      map[OperatorSignature]int64
    corrections  map[OperatorSignature]float64
}

// Conservative approach:
// 1. Track actual vs estimated for each operator type
// 2. Require N observations before applying corrections (N=100+ suggested)
// 3. Use moving average: correction = 0.9*old_correction + 0.1*new_ratio
// 4. Bound corrections to [0.1x, 10x] range to prevent outlier effects
// 5. Store in memory with LRU eviction (max 10,000 corrections)
```

**Advantages over DuckDB**:
- Go runtime can collect statistics non-invasively
- Session-local learning applicable immediately
- Conservative N-observation threshold prevents overfitting

---

## 7. Feature Completeness Checklist (Task 1.7)

### Statistics Persistence

Based on research findings:

- [x] Binary serialization matching DuckDB format
  - Property IDs: 100 (BaseStatistics), 101 (DistinctStatistics)
  - Version-forward-compatible via PropertyWithDefault

- [x] All statistic types
  - Numeric (INT/UINT/FLOAT/DOUBLE) with min/max
  - String (VARCHAR) - needs min/max string values
  - Distinct values via HyperLogLog with Good-Turing correction
  - NULL tracking via has_null/has_no_null flags

- [x] Supported type mapping
  - Numeric types: INT8-64, UINT8-64, INT128, UINT128, FLOAT, DOUBLE, BOOL
  - String types: VARCHAR, CHAR, TEXT
  - Temporal types: DATE, TIME, TIMESTAMP, INTERVAL
  - Unsupported: LIST, STRUCT, ARRAY, GEOMETRY, VARIANT

- [x] Storage location: Catalog metadata table (following DuckDB pattern)
- [x] Version checking for forward compatibility
- [x] Migration support for older formats

### Auto-Update Statistics

Novel feature (beyond DuckDB v1.4.3):

- [ ] Modification tracker per table
- [ ] Threshold: 10% row count change triggers notification
- [ ] Batching: Don't re-analyze same table in same session
- [ ] Incremental ANALYZE for tables > 1M rows (sample subset)
- [ ] User-initiated only (not automatic background process)

### Subquery Decorrelation

From unnest_rewriter.cpp research:

- [ ] findCorrelatedColumns() implementation
  - Traverse expression tree for column references
  - Identify columns from outer scope

- [ ] FlattenDependentJoin() implementation
  - Pattern matching for DELIM_JOIN with WINDOW/UNNEST
  - Column binding updates
  - Binding propagation through projections

- [ ] Subquery type support:
  - [ ] EXISTS correlated (mark join to semi join)
  - [ ] NOT EXISTS correlated
  - [ ] SCALAR correlated (LEFT OUTER JOIN + LIMIT 1)
  - [ ] IN correlated (hash join after decoration)
  - [ ] NOT IN correlated (with NULL semantics)
  - [ ] ANY/ALL correlated

- [ ] Multi-level correlation
  - Apply rewriter iteratively (outer -> middle -> inner)

- [ ] LATERAL join support
  - Subquery can reference all columns from LHS

- [ ] Correlated CTE support
  - CTE body references outer columns

- [ ] Edge cases:
  - [ ] Empty subquery results (NULL propagation)
  - [ ] NULL in correlation condition
  - [ ] SCALAR returning multiple rows (error or LIMIT 1)

### Filter Pushdown

From filter_pushdown.cpp research:

- [ ] Push to table scans
  - Column bindings check
  - Filter applicability analysis

- [ ] Push past inner joins
  - Left-side only filters -> left child
  - Right-side only filters -> right child
  - Mixed filters -> split and push parts

- [ ] Preserve outer join filters
  - LEFT JOIN: keep right-side filters above
  - RIGHT JOIN: keep left-side filters above
  - FULL OUTER: keep all filters above

- [ ] Complex AND/OR handling
  - AND predicates: split and push independently
  - OR predicates: keep together unless all branches push

- [ ] Filter dependencies
  - Check column binding availability
  - Don't push if column missing in child

- [ ] Subquery filters (push when safe)
- [ ] Mark to semi conversion (related to subqueries)

### Multi-Column Statistics

Novel feature (not in DuckDB v1.4.3):

- [ ] Joint NDV collection for selected column pairs
- [ ] Correlation detection during ANALYZE
- [ ] Heuristics for which pairs to track
  - Frequently joined columns
  - Frequently co-filtered columns
  - Limited to avoid memory bloat

- [ ] Integration with selectivity estimation
  - Use joint NDV instead of independence assumption

### Cardinality Learning

Novel feature (not in DuckDB v1.4.3):

- [ ] Track actual vs estimated cardinalities
- [ ] N-observation threshold (conservative, N >= 100)
- [ ] Correction multiplier computation
- [ ] Bounded memory usage (LRU eviction)
- [ ] Integration with cost model

---

## 8. Edge Cases and NULL Handling (Task 1.8)

### NULL Semantics in Statistics

#### Distinct Count with NULL

```sql
-- Table: t (col VARCHAR)
-- Values: 'a', 'a', 'b', NULL, NULL, NULL, 'c'
-- has_null=true, has_no_null=true
-- distinct_count = 3 (doesn't include NULL)
```

DistinctStatistics behavior (from `distinct_statistics.cpp`):
- NULL values not added to HyperLogLog
- has_null/has_no_null flags track NULL presence separately
- distinct_count only counts non-NULL values

#### Filter Selectivity with NULL

```sql
SELECT * FROM t WHERE col = 5;
-- Selectivity: (non_null_rows / total_rows) * (1 / distinct_count)
-- If no matches: 0 rows (correctly excludes NULLs)

SELECT * FROM t WHERE col IS NULL;
-- Selectivity: null_rows / total_rows
-- Handled separately, not through distinct_count
```

#### Join Cardinality with NULL

```sql
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;
-- NULL values don't participate in join
-- Cardinality: t1_rows * (t2_rows / t2_distinct_id) only for non-NULL
-- If t2.id has 30% NULL: smaller selectivity denominator
```

### Empty Result Set Handling

```go
// Statistics for empty result:
// distinct_count = 0
// has_null = false
// has_no_null = false

// After empty result through operators:
// Filter on empty: still empty (no propagation needed)
// Join with empty: still empty (early termination)
// Aggregate on empty: single row with NULL aggregates
```

### Correlated Subquery NULL Cases

```sql
-- EXISTS with NULL
SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id AND t2.val IS NULL);
-- If t1.id IS NULL: doesn't match (NULL comparison fails)
-- Result: t1 rows where (t1.id, t2.val) pair exists

-- Scalar subquery returning NULL
SELECT (SELECT val FROM t2 WHERE t2.id = t1.id) FROM t1;
-- If no match: NULL (correct SCALAR semantics)
-- If match but val IS NULL: NULL (correct)

-- NOT IN with NULL in subquery
SELECT * FROM t1 WHERE id NOT IN (SELECT id FROM t2);
-- If t2.id contains any NULL: result is empty (SQL three-valued logic)
-- ALL rows become UNKNOWN, which is treated as false in WHERE
```

### Filter Pushdown with NULL

```cpp
// Filter: col > 5 pushed to scan
// Statistics show: min=10, max=100 (col is numeric)
// All rows satisfy (no NULL for this column)
// Push is safe

// Filter: col = NULL pushed to scan
// This is SQL anti-pattern (always false), optimizer may eliminate
// If not eliminated: no rows qualify

// Filter: col IS NULL on LEFT JOIN result
// Must NOT push below join (join may produce NULLs from right side)
// Keep filter above join
```

### Distinct Count with Highly Correlated Columns

```sql
-- Table with columns (country, city)
-- 50 countries, each with ~100 cities
-- Expected distinct (country, city) = 5000

-- Independent assumption: 50 * 100 = 5000 ✓
-- Joint statistics would show same result (correlation: city determines country)

-- Independence assumption breaks for:
-- - Functional dependencies: city -> country (always same)
-- - Inverse functional dependencies
```

### Cardinality Learning Outliers

```go
// Query pattern: SELECT * FROM t WHERE id = <input>
// Typical: 10 rows (matching 1 ID)
// Learning: 100 queries collect correction = 1.0x

// Outlier: One query with id = 42 returns 1,000,000 rows
// Naive learning: correction = 100,000x (wrong!)
// Conservative approach: Require 100+ observations, use moving average
// Result: Outlier has small effect (1% weight in moving average)
```

### String Statistics Edge Cases

```sql
-- Min/max string tracking
-- Table: t (name VARCHAR)
-- Values: 'Alice', 'Bob', 'Charlie', NULL

-- String statistics (if implemented):
-- min = 'Alice', max = 'Charlie' (lexicographic order)
-- Filter: name >= 'Bob'
-- Selectivity: estimated based on min/max range

-- Edge case: All same value
-- min = max = 'Alice'
-- is_constant = true
-- Selectivity: 0 or 1 (matches or doesn't)

-- Edge case: Unicode/collation
-- String comparison may vary by collation
-- Min/max should respect collation rules
```

### Numeric Type Edge Cases

```sql
-- Integer overflow in statistics
-- Table: t (id INT64)
-- Values: -9223372036854775808, 9223372036854775807 (min/max int64)
-- Statistics: min/max stored correctly using NumericValueUnion

-- Filter: id > 1000000000000
-- Selectivity: (max - 1000000000000) / (max - min) = fraction

-- Float special values
-- Table: t (value FLOAT)
-- Contains: NaN, Infinity, -Infinity, normal values
-- Statistics handling: Typically NaN/Inf treated as regular values
-- Min/max include them in comparison
```

### Sampling Bias in Statistics

```cpp
// Reservoir sampling used in ANALYZE
// Integral types: 50% sample
// Other types: 10% sample

// Bias risk: Skewed distributions
// Example: Column with 1M values, only first 10K unique
// 50% sample: ~5K rows, samples mostly unique portion
// Distinct count: Over-estimated

// Mitigation: Good-Turing uses proportion of singletons
// Correction accounts for sampling bias
```

### Transaction Isolation and Statistics Staleness

```sql
-- Transaction 1: DELETE 50% of rows, COMMIT
-- Transaction 2: SELECT * FROM t WHERE id = 5;
-- Question: Which statistics used for cardinality estimate?

-- DuckDB behavior: Current session statistics before DELETE
-- If ANALYZE not run: Old statistics still used
-- Result: Over-estimated cardinality initially
```

### Case-Sensitivity in String Statistics

```sql
-- Table: t (name VARCHAR)
-- Values: 'Alice', 'alice', 'ALICE'
-- distinct_count = 3 (case-sensitive in DuckDB)

-- Filter: name = 'alice' (case-sensitive)
-- Selectivity: 1 / 3

-- But if collation is case-insensitive: Selectivity should be 3 / 3
-- String statistics must account for collation
```

---

## Summary of Key Findings

### For Statistics Persistence
- Use property-based serialization with IDs 100 and 101
- HyperLogLog for distinct values with Good-Turing correction
- NULL tracking via has_null/has_no_null dual flags
- Type-specific min/max storage for numeric/string types

### For Auto-Update (Novel Feature)
- DuckDB v1.4.3 has no auto-update
- dukdb-go should implement conservatively:
  - 10% threshold for row count change
  - Notification-based (user initiates)
  - Incremental ANALYZE for large tables

### For Subquery Decorrelation
- Pattern-based unnest rewriter matches specific operator structures
- DELIM_JOIN with WINDOW/UNNEST pattern core to algorithm
- Column binding updates critical for correctness
- Multi-level correlation requires iterative application
- NULL semantics must be preserved through transformation

### For Filter Pushdown
- Recursive descent with join type awareness
- Column binding availability determines pushability
- Inner joins: push aggressively to both sides
- Outer joins: preserve right-side filters above join
- AND predicates: split and push independently
- OR predicates: keep together unless all branches push

### For Multi-Column Statistics (Novel Feature)
- DuckDB v1.4.3 only tracks per-column statistics
- dukdb-go can extend with joint NDV for column pairs
- Heuristics needed: join columns, co-filtered columns
- Bounded memory: limit number of column pairs tracked

### For Cardinality Learning (Novel Feature)
- DuckDB v1.4.3 has no runtime learning
- dukdb-go should implement conservatively:
  - N-observation threshold before applying corrections
  - Moving average to dampen outliers
  - Memory-bounded with LRU eviction
  - Correction range bounded [0.1x, 10x]

---

## References and Source Files

**Statistics Format**:
- `/references/duckdb/src/storage/statistics/base_statistics.cpp` (lines 1-200)
- `/references/duckdb/src/storage/statistics/column_statistics.cpp` (lines 59-69)
- `/references/duckdb/src/storage/statistics/distinct_statistics.cpp` (lines 10-84)

**Subquery Decorrelation**:
- `/references/duckdb/src/optimizer/unnest_rewriter.cpp` (lines 1-387)
- `/references/duckdb/src/planner/operator/logical_unnest.hpp`

**Filter Pushdown**:
- `/references/duckdb/src/optimizer/filter_pushdown.cpp` (lines 1-300)

**Cost Model**:
- `/references/duckdb/src/optimizer/join_order/cost_model.cpp` (lines 1-20)

**Statistics Propagation**:
- `/references/duckdb/src/optimizer/statistics_propagator.cpp` (lines 1-135)

**Numeric Statistics**:
- `/references/duckdb/src/storage/statistics/numeric_stats.cpp` (lines 1-150)
