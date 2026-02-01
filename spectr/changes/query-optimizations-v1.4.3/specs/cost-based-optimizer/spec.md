# Cost-Based Optimizer Specification

## Overview

The cost-based optimizer (CBO) is responsible for selecting the most efficient execution plan for a given query by estimating the cost of alternative physical plans and choosing the one with the lowest estimated cost.

## Goals

1. Generate execution plans within 10% of optimal cost
2. Support all physical operators (scan, join, aggregate, sort, etc.)
3. Handle complex queries with 20+ joins efficiently
4. Optimize in under 100ms for typical OLAP queries
5. Adapt to different hardware configurations

## Architecture

### Components

```
┌─────────────────┐
│   Statistics    │
│   Provider      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Cost Model    │
│   Calculator    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Plan Space    │
│   Enumerator    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Best Plan     │
│   Selector      │
└─────────────────┘
```

## Cost Model

### Cost Components

Every physical operator has an associated cost consisting of:

1. **Startup Cost**: Resources required to initialize the operator
2. **Per-Row Cost**: CPU and I/O cost per row processed
3. **Memory Cost**: Peak memory usage during execution

### Cost Formulas

#### Table Scan

```
Sequential Scan Cost:
  Startup: 0
  CPU: rows * cpu_tuple_cost
  IO: pages * seq_page_cost

Index Scan Cost:
  Startup: index_pages * random_page_cost
  CPU: (rows * selectivity) * cpu_tuple_cost
  IO: (index_pages + table_pages * selectivity) * random_page_cost
```

#### Join Operators

```
Hash Join Cost:
  Startup: 0
  Build CPU: inner_rows * cpu_tuple_cost
  Probe CPU: outer_rows * cpu_tuple_cost
  Memory: inner_rows * tuple_width

Merge Join Cost:
  Startup: sort_cost (if needed)
  CPU: (outer_rows + inner_rows) * cpu_tuple_cost
  IO: 0 (if already sorted)

Nested Loop Join Cost:
  Startup: 0
  CPU: outer_rows * inner_rows * cpu_tuple_cost
  IO: outer_rows * inner_index_cost (if indexed)
```

#### Aggregation

```
Hash Aggregate Cost:
  Startup: 0
  CPU: rows * cpu_tuple_cost
  Memory: groups * tuple_width

Sort Aggregate Cost:
  Startup: sort_cost
  CPU: rows * cpu_tuple_cost
  IO: sort_io_cost
```

### Hardware Parameters

```go
type HardwareConfig struct {
    // I/O Costs
    SeqPageCost    float64 // 1.0 (baseline)
    RandomPageCost float64 // 4.0 (default)

    // CPU Costs
    CPUPageCost     float64 // 0.01
    CPUTupleCost    float64 // 0.01
    CPUOperatorCost float64 // 0.0025

    // Memory
    WorkMem int64 // 256MB default

    // Parallelism
    MaxWorkers int
}
```

## Statistics Integration

### Cardinality Estimation

The cost model relies on accurate cardinality estimates from the statistics framework:

```go
func (cm *CostModel) EstimateCardinality(expr Expression, stats *TableStatistics) int64 {
    switch e := expr.(type) {
    case *Scan:
        return applySelectivity(stats.RowCount, e.Predicate, stats)

    case *Filter:
        inputCard := cm.EstimateCardinality(e.Child, stats)
        selectivity := estimateSelectivity(e.Predicate, stats)
        return int64(float64(inputCard) * selectivity)

    case *Join:
        leftCard := cm.EstimateCardinality(e.Left, stats)
        rightCard := cm.EstimateCardinality(e.Right, stats)
        selectivity := estimateJoinSelectivity(e.Condition, e.Left, e.Right)
        return int64(float64(leftCard) * float64(rightCard) * selectivity)
    }
}
```

### Selectivity Estimation

```go
func estimateSelectivity(expr Expression, stats *TableStatistics) float64 {
    switch e := expr.(type) {
    case *ComparisonExpr:
        colStats := stats.ColumnStats[e.Column]
        switch e.Op {
        case EQ:
            return 1.0 / float64(max(colStats.DistinctCount, 1))
        case LT, GT:
            return estimateRangeSelectivity(e, colStats)
        case BETWEEN:
            return estimateRangeSelectivity(e, colStats) * 0.5
        }

    case *LogicalExpr:
        switch e.Op {
        case AND:
            // Assume independence
            return estimateSelectivity(e.Left, stats) *
                   estimateSelectivity(e.Right, stats)
        case OR:
            // Inclusion-exclusion principle
            left := estimateSelectivity(e.Left, stats)
            right := estimateSelectivity(e.Right, stats)
            return left + right - left*right
        }
    }

    return 0.1 // Default selectivity
}
```

## Plan Enumeration

### Search Strategy

The optimizer uses a branch-and-bound search with:

1. **Dynamic Programming**: For join ordering (left-deep trees)
2. **Memoization**: To avoid recomputing plans
3. **Pruning**: Based on cost bounds

### Search Algorithm

```go
func (o *CostBasedOptimizer) FindBestPlan(logical LogicalPlan) PhysicalPlan {
    // Initialize memo structure
    memo := NewMemo(logical)

    // Explore search space
    for !o.searchComplete(memo) {
        // Apply transformation rules
        o.applyRules(memo)

        // Generate physical implementations
        o.implementRules(memo)

        // Prune dominated plans
        o.prunePlans(memo)
    }

    // Return best plan
    return memo.GetBestPlan()
}
```

### Search Space Management

```go
type SearchSpace struct {
    // Maximum search depth
    MaxDepth int

    // Timeout for optimization
    Timeout time.Duration

    // Pruning threshold
    PruningFactor float64

    // Interesting orders to preserve
    InterestingOrders []ColumnOrder
}
```

## Physical Operators

### Scan Operators

1. **Sequential Scan**: Full table scan
2. **Index Scan**: Index-based scan with predicate
3. **Index-Only Scan**: Covering index scan
4. **Bitmap Scan**: Bitmap index scan

### Join Operators

1. **Hash Join**: Build-probe hash table
2. **Merge Join**: Sort-merge join
3. **Nested Loop Join**: Index nested loop
4. **Semi Join**: For EXISTS/IN subqueries

### Aggregate Operators

1. **Hash Aggregate**: Hash-based grouping
2. **Sort Aggregate**: Sort-based grouping
3. **Streaming Aggregate**: For pre-sorted input

### Sort Operators

1. **In-Memory Sort**: QuickSort for small data
2. **External Sort**: Multi-phase merge sort
3. **Incremental Sort**: For partially sorted data

## Cost Calculation Examples

### Example 1: Simple Table Scan

```sql
SELECT * FROM users WHERE age > 25;
```

- Table: users (1M rows, 1000 pages)
- Selectivity: age > 25 → 0.4
- Plan: Sequential Scan
- Cost: 1000 * 1.0 + 1M * 0.4 * 0.01 = 1000 + 4000 = 5000

### Example 2: Index Scan

```sql
SELECT * FROM users WHERE id = 100;
```

- Index on id (unique)
- Selectivity: 1/1M
- Plan: Index Scan
- Cost: 1 * 4.0 + 1 * 0.01 = 4.01

### Example 3: Hash Join

```sql
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id;
```

- orders: 10M rows
- customers: 100K rows
- Join selectivity: 1 (foreign key)
- Plan: Hash Join (customers build, orders probe)
- Cost:
  - Build: 100K * 0.01 = 1000
  - Probe: 10M * 0.01 = 100000
  - Total: 101000

### Example 4: Multi-Join Query

```sql
SELECT * FROM a JOIN b ON a.id = b.a_id
               JOIN c ON b.id = c.b_id
               JOIN d ON c.id = d.c_id;
```

The optimizer explores different join orders:

```
Option 1: (((a × b) × c) × d)
Option 2: ((a × b) × (c × d))
Option 3: (a × ((b × c) × d))
...
```

Each option is costed based on:
- Input cardinalities
- Join selectivities
- Available indexes
- Memory requirements

## Calibration

### Automatic Calibration

The cost model can be calibrated automatically:

```go
func (cm *CostModel) Calibrate() {
    // Run micro-benchmarks
    seqPageTime := benchmarkSequentialScan()
    randomPageTime := benchmarkRandomScan()
    cpuTupleTime := benchmarkCPUPerTuple()

    // Update cost parameters
    cm.SeqPageCost = 1.0
    cm.RandomPageCost = randomPageTime / seqPageTime
    cm.CPUTupleCost = cpuTupleTime / seqPageTime
}
```

### Manual Tuning

Users can tune cost parameters:

```sql
-- Make index scans more attractive
SET random_page_cost = 2.0;

-- Reduce CPU cost (faster CPU)
SET cpu_tuple_cost = 0.005;

-- Increase work memory
SET work_mem = '512MB';
```

## Quality Assurance

### Plan Validation

```go
func (o *CostBasedOptimizer) ValidatePlan(plan PhysicalPlan) error {
    // Check plan correctness
    if err := o.checkPlanCorrectness(plan); err != nil {
        return err
    }

    // Verify cost reasonableness
    if err := o.checkCostReasonableness(plan); err != nil {
        return err
    }

    return nil
}
```

### Regression Testing

1. **Cost Model Tests**: Verify cost calculations
2. **Plan Selection Tests**: Ensure optimal plans selected
3. **TPC-H Benchmark**: Standard OLAP workload
4. **Edge Cases**: Empty tables, skewed data, etc.

## Performance Considerations

### Optimization Time

- Target: < 100ms for typical queries
- Timeout: 1s maximum
- Progressive refinement for complex queries

### Memory Usage

- Memo structure size limit: 100MB
- Pruning to control search space
- Streaming optimization for very large plans

### Concurrency

- Thread-safe statistics access
- Isolated optimization sessions
- Shared rule definitions

## Future Enhancements

1. **Machine Learning**: Learn cost parameters from runtime
2. **Adaptive Costing**: Update costs based on actuals
3. **Multi-Query Optimization**: Optimize query batches
4. **GPU Cost Model**: Include GPU operators
5. **Cloud Cost Model**: Include cloud-specific costs (serverless, etc.)