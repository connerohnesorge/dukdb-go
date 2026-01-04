# Design: Cost-Based Query Optimization

This document captures architectural decisions for implementing cost-based query optimization in dukdb-go.

## Architecture Overview

```
SQL Query
    ↓
Parser → AST
    ↓
Binder → Resolved AST
    ↓
Logical Planner → LogicalPlan
    ↓
Cost-Based Optimizer (NEW)
    ├─→ Statistics Manager
    ├─→ Cardinality Estimator
    ├─→ Cost Model
    ├─→ Join Order Optimizer
    └─→ Physical Plan Generator
    ↓
PhysicalPlan (optimized)
    ↓
Executor → Results
```

---

## Decision 1: Statistics Data Structures

### Context
Statistics are the foundation of cost-based optimization. They must capture enough information for accurate cardinality estimation while remaining efficient to store and query.

### Decision
Implement a two-level statistics system:

```go
// TableStatistics captures table-level statistics
type TableStatistics struct {
    RowCount      int64              // Total rows in table
    PageCount     int64              // Storage pages used
    DataSizeBytes int64              // Total data size
    LastAnalyzed  time.Time          // When stats were collected
    SampleRate    float64            // Sample rate used (1.0 = full scan)
    Columns       []ColumnStatistics // Per-column stats
}

// ColumnStatistics captures column-level statistics
type ColumnStatistics struct {
    ColumnName    string
    ColumnType    dukdb.Type
    NullFraction  float64    // Fraction of NULL values (0.0-1.0)
    DistinctCount int64      // Estimated distinct values
    MinValue      any        // Minimum value (type-specific)
    MaxValue      any        // Maximum value (type-specific)
    AvgWidth      int32      // Average value width in bytes
    Histogram     *Histogram // Optional equi-depth histogram
}

// Histogram represents an equi-depth histogram for selectivity estimation
type Histogram struct {
    NumBuckets int        // Number of buckets (default 100)
    Buckets    []Bucket   // Bucket boundaries and counts
}

type Bucket struct {
    LowerBound    any     // Lower bound (inclusive)
    UpperBound    any     // Upper bound (exclusive for non-last)
    Frequency     float64 // Fraction of values in bucket
    DistinctCount int64   // Distinct values in bucket
}
```

### Rationale
- **Row count**: Essential for all cardinality estimates
- **Null fraction**: Affects selectivity of IS NULL predicates
- **Distinct count**: Used for join and group by estimates
- **Min/Max**: Used for range predicate selectivity
- **Histogram**: Provides accurate selectivity for value distributions

---

## Decision 2: Cardinality Estimation Model

### Context
Cardinality estimation predicts the number of rows produced by each plan operator. Accuracy directly impacts plan quality.

### Decision
Implement a hybrid estimation approach:

```go
// CardinalityEstimator estimates output cardinality for plan nodes
type CardinalityEstimator struct {
    stats *StatisticsManager
}

// EstimateCardinality returns estimated rows for a logical plan node
func (e *CardinalityEstimator) EstimateCardinality(plan LogicalPlan) float64 {
    switch p := plan.(type) {
    case *LogicalScan:
        return e.estimateScan(p)
    case *LogicalFilter:
        return e.estimateFilter(p)
    case *LogicalJoin:
        return e.estimateJoin(p)
    case *LogicalAggregate:
        return e.estimateAggregate(p)
    // ... other node types
    }
}
```

**Selectivity Estimation Rules:**

1. **Equality predicates**: `selectivity = 1 / distinct_count`
2. **Range predicates**: Use histogram if available, else use default selectivity `0.2`
3. **NULL predicates**: `selectivity = null_fraction` (or `1 - null_fraction` for IS NOT NULL)
4. **LIKE predicates**: Use default selectivity `0.2` (DuckDB does not implement pattern-specific heuristics)
5. **AND predicates**: `selectivity = s1 * s2` (independence assumption)
6. **OR predicates**: Handled conservatively; DuckDB does not fully implement OR selectivity combination
7. **NOT predicates**: `selectivity = 1 - s`
8. **Non-equality comparisons** (<, <=, >, >=, !=): Apply penalty `distinct_count^(2/3)` to base selectivity

**Default Selectivity:**
DuckDB uses `DEFAULT_SELECTIVITY = 0.2` when no statistics or specific rules apply.

**Distinct Count Estimation:**
Use HyperLogLog for approximate distinct count tracking during statistics collection.

**Histogram Selectivity Algorithm:**
For range predicates with histogram available:
- Use linear interpolation between bucket boundaries
- Selectivity = (value - lower_bound) / (upper_bound - lower_bound) * bucket_frequency

**Join Cardinality:**
```
join_cardinality = (left_rows * right_rows) / max(left_key_distinct, right_key_distinct)
```
Note: The distinct counts used are for the JOIN KEY columns specifically, not overall table distinct counts.

### Rationale
- Independence assumption is standard and works well in practice
- Histogram-based estimation with linear interpolation provides accuracy for skewed data
- Default selectivity of 0.2 aligns with DuckDB's proven defaults
- HyperLogLog provides memory-efficient distinct count approximation

---

## Decision 3: Cost Model

### Context
The cost model assigns numeric costs to physical plans, enabling comparison of alternatives.

**Note on DuckDB's Cost Model:**
DuckDB uses a relatively simple cardinality-based cost model where `cost = cardinality + left_cost + right_cost`. This simplicity works well for DuckDB's use case but may not always produce optimal plans.

**dukdb-go's Approach:**
dukdb-go intentionally implements a more sophisticated cost model than DuckDB. This targets better plan quality through detailed costing of I/O operations, CPU costs, and memory considerations. The additional complexity is justified by improved plan selection, especially for complex analytical queries.

### Decision
Implement a configurable cost model with three components:

```go
// CostModel estimates execution cost for physical plans
type CostModel struct {
    // Configurable cost constants
    SeqPageCost     float64 // Cost of sequential page read (default: 1.0)
    RandomPageCost  float64 // Cost of random page read (default: 4.0)
    CPUTupleCost    float64 // Cost per tuple processed (default: 0.01)
    CPUOperatorCost float64 // Cost per operator evaluation (default: 0.0025)
    HashBuildCost   float64 // Cost per tuple for hash table build (default: 0.02) - empirically tuned for in-memory operations
    HashProbeCost   float64 // Cost per tuple for hash table probe (default: 0.01) - empirically tuned for in-memory operations
    SortCost        float64 // Cost per comparison in sort (default: 0.05) - empirically tuned for in-memory operations
}

// PlanCost represents the estimated cost of a plan
type PlanCost struct {
    StartupCost float64 // Cost before first row produced
    TotalCost   float64 // Cost to produce all rows
    OutputRows  float64 // Estimated output cardinality
    OutputWidth int32   // Average row width in bytes
}

// EstimateCost calculates cost for a physical plan node
func (m *CostModel) EstimateCost(plan PhysicalPlan, childCosts []PlanCost) PlanCost
```

**Cost Formulas:**

1. **Sequential Scan**:
   ```
   startup_cost = 0
   total_cost = (pages * SeqPageCost) + (rows * CPUTupleCost)
   ```

2. **Index Scan**:
   ```
   startup_cost = index_height * RandomPageCost
   total_cost = startup_cost + (selected_rows * (RandomPageCost + CPUTupleCost))
   ```

3. **Hash Join**:
   ```
   startup_cost = build_rows * HashBuildCost
   total_cost = startup_cost + (probe_rows * HashProbeCost) + child_costs
   ```

4. **Nested Loop Join**:
   ```
   startup_cost = outer_startup
   total_cost = outer_cost + (outer_rows * inner_cost)
   ```

5. **Sort**:
   ```
   startup_cost = child_total + (rows * log2(rows) * SortCost)
   total_cost = startup_cost + (rows * CPUTupleCost)
   ```

6. **Hash Aggregate**:
   ```
   startup_cost = child_total + (rows * HashBuildCost)
   total_cost = startup_cost + (groups * CPUTupleCost)
   ```

### Rationale
- Cost model based on PostgreSQL's proven approach
- Separating startup and total cost enables better pipelining analysis
- Configurable constants allow tuning for different hardware

---

## Decision 4: Join Order Optimization Algorithm

### Context
Join ordering is the most impactful optimization for multi-table queries. We need an algorithm that finds optimal or near-optimal orders efficiently.

### Decision
Use dynamic programming for small queries, greedy for large:

```go
// JoinOrderOptimizer finds optimal join order
type JoinOrderOptimizer struct {
    estimator     *CardinalityEstimator
    costModel     *CostModel
    dpThreshold   int // Max tables for DP (default: 12)
    pairLimit     int // Max pairs to enumerate before switching to greedy (default: 10000)
}

// OptimizeJoinOrder returns optimally ordered join tree
func (o *JoinOrderOptimizer) OptimizeJoinOrder(
    tables []LogicalPlan,
    predicates []JoinPredicate,
) LogicalPlan {
    if len(tables) <= o.dpThreshold {
        return o.dpOptimize(tables, predicates)
    }
    return o.greedyOptimize(tables, predicates)
}
```

**DPhy Algorithm (N <= 12 AND pairs <= 10,000):**
Based on "Dynamic Programming Strikes Back" by Moerkotte & Neumann.
```
1. Initialize: dp[{t}] = scan(t) for each table t
2. For size = 2 to N:
   For each subset S of size 'size':
     For each way to partition S into (S1, S2):
       If exists predicate joining S1 and S2:
         cost = combine(dp[S1], dp[S2])
         if cost < dp[S]:
           dp[S] = (S1, S2, cost)
       Track pairs_emitted; if > 10,000, switch to greedy
3. Return dp[{all tables}]
```

**Greedy Algorithm (N > 12 OR pairs > 10,000):**
```
1. Start with lowest cardinality table
2. Repeatedly join two relations with minimum intermediate result cost
3. Complexity: O(n^3) where n is number of relations
```
Note: DuckDB's greedy algorithm does not perform local search or swapping after initial construction.

**Cartesian Product Handling:**
For disconnected relation groups (no join predicate connecting them), handle each connected component separately, then combine with Cartesian products at the end.

**Join Type Selection:**
- **Hash Join**: Default for equi-joins, build smaller side
- **Nested Loop**: When inner is very small or has index
- **Sort-Merge**: When inputs are already sorted on join key

### Rationale
- DPhy guarantees optimal within search space with efficient enumeration
- N=12 threshold combined with 10,000 pair limit balances optimality vs compilation time
- Greedy provides reasonable plans for very large queries without local search overhead

---

## Decision 5: Build Side Selection for Hash Joins

### Context
Hash join performance depends heavily on choosing the smaller table as the build side, considering both row count and row width.

### Decision
Select build side based on estimated memory cost (cardinality * row width):

```go
func selectBuildSide(left, right LogicalPlan, estimator *CardinalityEstimator) (build, probe LogicalPlan) {
    leftRows := estimator.EstimateCardinality(left)
    rightRows := estimator.EstimateCardinality(right)
    leftWidth := estimator.EstimateRowWidth(left)
    rightWidth := estimator.EstimateRowWidth(right)

    // Estimate memory for hash table
    leftMemory := leftRows * float64(leftWidth)
    rightMemory := rightRows * float64(rightWidth)

    // Build side with smaller memory footprint
    if leftMemory <= rightMemory {
        return left, right
    }
    return right, left
}

// EstimateRowWidth calculates average row width in bytes
func (e *CardinalityEstimator) EstimateRowWidth(plan LogicalPlan) int32 {
    // Sum of column widths with variable-width type penalties:
    // - VARCHAR: base width + 8 bytes overhead
    // - LIST: base width + 32 bytes overhead
    // - Fixed types: sizeof(type)
}
```

**Variable-Width Type Penalties:**
- VARCHAR columns: Add +8 bytes for length prefix and pointer overhead
- LIST columns: Add +32 bytes for list metadata and child pointer overhead
- BLOB columns: Add +8 bytes similar to VARCHAR

**Additional Considerations:**
- Memory constraint: If build side exceeds memory, consider partition-based hash join
- For LEFT/RIGHT joins, preserve semantics (build must be appropriate side)
- When memory estimates are close, prefer row count as tiebreaker

### Rationale
- Building smaller memory footprint minimizes hash table memory and improves cache efficiency
- Row width consideration is critical for tables with wide rows or variable-width columns
- Variable-width penalties account for actual memory overhead in hash table entries

---

## Decision 6: Statistics Storage

### Context
Statistics must persist across sessions and be efficiently accessible during optimization.

### Decision
Store statistics in catalog alongside table metadata:

```go
// In catalog package
type TableDef struct {
    // ... existing fields ...
    Statistics *TableStatistics // May be nil if not analyzed
}

// StatisticsManager provides access to statistics
type StatisticsManager struct {
    catalog *Catalog
}

func (m *StatisticsManager) GetTableStats(schema, table string) *TableStatistics {
    tableDef := m.catalog.GetTable(schema, table)
    if tableDef == nil || tableDef.Statistics == nil {
        return m.defaultStats(tableDef)
    }
    return tableDef.Statistics
}

func (m *StatisticsManager) defaultStats(table *TableDef) *TableStatistics {
    // Return conservative defaults when no stats available
    return &TableStatistics{
        RowCount: 1000, // Default assumption
        PageCount: 10,
        // ... conservative defaults ...
    }
}
```

### Rationale
- Statistics naturally belong with table metadata
- Persists automatically with catalog persistence
- Default stats ensure optimizer works without ANALYZE

---

## Decision 7: Integration Point

### Context
The optimizer must integrate seamlessly into the existing query execution pipeline without breaking backward compatibility.

### Decision
Insert optimizer between logical planner and physical planner:

```go
// Modified query execution flow
func (e *Engine) executeQuery(query string) (Result, error) {
    // 1. Parse
    ast, err := e.parser.Parse(query)

    // 2. Bind
    bound, err := e.binder.Bind(ast)

    // 3. Logical Plan
    logical, err := e.logicalPlanner.Plan(bound)

    // 4. Cost-Based Optimization (NEW)
    optimized, err := e.optimizer.Optimize(logical)

    // 5. Physical Plan (from optimized logical)
    physical, err := e.physicalPlanner.Plan(optimized)

    // 6. Execute
    return e.executor.Execute(physical)
}
```

**Optimizer Interface:**
```go
type Optimizer interface {
    // Optimize transforms a logical plan into an optimized logical plan
    // with hints for physical implementation
    Optimize(plan LogicalPlan) (OptimizedPlan, error)
}

// OptimizedPlan wraps logical plan with optimization annotations
type OptimizedPlan struct {
    Plan          LogicalPlan
    EstimatedCost PlanCost
    JoinOrder     []int // Optimized join order
    AccessMethods map[string]AccessMethod // Per-table access hints
}
```

### Rationale
- Clean separation between logical and physical planning
- Optimizer output includes hints for physical planner
- Backward compatible: can be disabled or bypassed

---

## Decision 8: EXPLAIN Cost Integration

### Context
EXPLAIN should show cost estimates to help users understand query performance.

### Decision
Add cost annotations to EXPLAIN output:

```
EXPLAIN SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.country = 'USA';

┌───────────────────────────────────────────────────────────────────────────────────┐
│ Physical Plan                                                                      │
├───────────────────────────────────────────────────────────────────────────────────┤
│ HashJoin (cost=125.50..450.00 rows=500 width=120)                                 │
│   Join Condition: (o.customer_id = c.id)                                          │
│   ├─ SeqScan on orders o (cost=0.00..100.00 rows=5000 width=80)                   │
│   └─ SeqScan on customers c (cost=0.00..25.50 rows=100 width=40)                  │
│        Filter: (country = 'USA')                                                   │
└───────────────────────────────────────────────────────────────────────────────────┘
```

Format: `(cost=startup..total rows=estimated_rows width=avg_width)`

### Rationale
- Follows PostgreSQL EXPLAIN format (familiar to users)
- Shows both startup and total cost
- Row estimates help identify cardinality issues

---

## Decision 9: Fast Path for Simple Queries

### Context
Optimizer overhead should not significantly impact simple queries.

### Decision
Implement fast path that bypasses full optimization:

```go
func (o *CostBasedOptimizer) Optimize(plan LogicalPlan) (OptimizedPlan, error) {
    // Fast path: single table queries without joins
    if o.isSimpleQuery(plan) {
        return o.fastPathOptimize(plan)
    }

    // Full optimization for complex queries
    return o.fullOptimize(plan)
}

func (o *CostBasedOptimizer) isSimpleQuery(plan LogicalPlan) bool {
    // No joins
    if countJoins(plan) > 0 {
        return false
    }
    // Single table or simple union
    return countTables(plan) <= 1
}
```

### Rationale
- Simple queries don't benefit from join ordering
- Avoids unnecessary overhead for common case
- Full optimization reserved for complex queries

---

## File Organization

```
internal/
├── optimizer/
│   ├── optimizer.go        // Main Optimizer interface and implementation
│   ├── statistics.go       // TableStatistics, ColumnStatistics, Histogram
│   ├── cardinality.go      // CardinalityEstimator
│   ├── cost_model.go       // CostModel, PlanCost
│   ├── join_order.go       // JoinOrderOptimizer (DP + Greedy)
│   ├── plan_enumerator.go  // Physical plan enumeration
│   └── optimizer_test.go   // Comprehensive tests
├── catalog/
│   └── statistics.go       // Statistics storage in catalog
└── planner/
    └── physical.go         // Modified to use optimizer hints
```

---

## Test Strategy

1. **Unit Tests**: Each component tested in isolation
2. **Integration Tests**: Full query optimization paths
3. **Cardinality Tests**: Compare estimates vs actual counts
4. **TPC-H Benchmark**: Measure improvement on standard queries
5. **Regression Tests**: Ensure no existing tests break
