# Query Planning and Optimization Design

## Architecture Overview

The query optimizer follows a cascades-based architecture with cost-based optimization. The high-level flow is:

```
Parsed Query (AST)
    ↓
Binder (Resolved AST)
    ↓
Logical Plan Generator
    ↓
Optimizer (Cascades Framework)
    ├── Statistics Provider
    ├── Cost Model
    ├── Transformation Rules
    └── Plan Enumerator
    ↓
Physical Plan (Selected by Cost)
    ↓
Executor
```

## Core Components

### 1. Statistics Framework

The statistics system provides cardinality and data distribution estimates to the optimizer.

#### Statistics Storage

```go
// Statistics are stored in the catalog alongside tables
type TableStatistics struct {
    TableName     string
    RowCount      int64
    PageCount     int64
    ColumnStats   map[string]*ColumnStatistics
    MultiColStats []*MultiColumnStatistics
    LastUpdated   time.Time
    SampleRate    float64
}

type ColumnStatistics struct {
    ColumnName    string
    NullCount     int64
    DistinctCount int64
    MinValue      interface{}
    MaxValue      interface{}
    AvgWidth      int64
    Histogram     *Histogram
    Skewness      float64
}

type Histogram struct {
    Buckets []HistogramBucket
    Type    HistogramType // EQUIDEPTH, EQUIDEPTH, VARYING
}

type HistogramBucket struct {
    LowerBound interface{}
    UpperBound interface{}
    Frequency  int64
    Distinct   int64
}

type MultiColumnStatistics struct {
    Columns     []string
    Correlation float64
    Dependency  float64 // Functional dependency measure
}
```

#### Statistics Collection

Statistics are collected through:

1. **Automatic Collection**: Triggered when data modification exceeds threshold
2. **Manual Collection**: `ANALYZE table_name` command
3. **Sampling**: For large tables, collect statistics on samples

```go
type StatisticsCollector struct {
    catalog    *catalog.Catalog
    storage    *storage.Storage
    sampleRate float64
}

func (sc *StatisticsCollector) CollectTableStats(tableName string) error {
    // 1. Determine sample size based on table size and desired accuracy
    // 2. Scan table (or sample) to collect statistics
    // 3. Build histograms for each column
    // 4. Calculate multi-column correlations
    // 5. Store in catalog
}

func (sc *StatisticsCollector) CollectHistogram(column *Column, buckets int) *Histogram {
    // Use equi-depth histograms as default
    // Support different histogram types based on data distribution
}
```

#### Statistics Maintenance

Statistics are incrementally updated to avoid full recalculation:

```go
type StatisticsUpdater struct {
    catalog *catalog.Catalog
}

func (su *StatisticsUpdater) UpdateOnInsert(table string, insertedRows int) {
    // Update row count
    // Update column statistics based on new values
    // Mark statistics as potentially stale if change exceeds threshold
}

func (su *StatisticsUpdater) UpdateOnDelete(table string, deletedRows int, deletedValues [][]interface{}) {
    // Update row count
    // Adjust histograms for deleted values
    // Recalculate if significant portion deleted
}
```

### 2. Cost Model

The cost model estimates resource consumption for physical operators.

#### Cost Components

```go
type Cost struct {
    StartupCost float64 // Cost to initialize operator
    TotalCost   float64 // Total cost including all rows

    // Detailed breakdown for debugging
    IOCost      float64
    CPUCost     float64
    NetworkCost float64
    MemoryCost  float64
}

type CostModel struct {
    // Hardware parameters
    SeqPageCost      float64 // Cost of sequential page read
    RandomPageCost   float64 // Cost of random page read
    CPUPageCost      float64 // CPU cost per page processed
    CPUOperatorCost  float64 // CPU cost per operator
    NetworkTransfer  float64 // Network transfer cost per byte

    // Derived parameters
    HashJoinCost     float64
    MergeJoinCost    float64
    NestedLoopCost   float64
}
```

#### Cost Calculation

```go
func (cm *CostModel) EstimateScanCost(table *Table, filter *Expression) Cost {
    // Calculate selectivity of filter
    selectivity := cm.estimateSelectivity(filter, table.Stats)

    // Estimate pages to read
    pages := table.Stats.PageCount
    if selectivity < 0.1 {
        // Index scan
        pages *= selectivity * 0.5 // Rough estimate
    }

    // Calculate costs
    ioCost := pages * cm.SeqPageCost
    cpuCost := table.Stats.RowCount * selectivity * cm.CPUPageCost

    return Cost{
        StartupCost: 0,
        TotalCost:   ioCost + cpuCost,
        IOCost:      ioCost,
        CPUCost:     cpuCost,
    }
}

func (cm *CostModel) EstimateJoinCost(left, right Plan, joinType JoinType, condition Expression) Cost {
    // Get cardinality estimates
    leftCard := left.EstimatedCardinality()
    rightCard := right.EstimatedCardinality()

    // Calculate join selectivity
    selectivity := cm.estimateJoinSelectivity(condition, left, right)
    outputCard := leftCard * rightCard * selectivity

    // Calculate costs based on join algorithm
    switch joinType {
    case HashJoin:
        return cm.estimateHashJoinCost(left, right, outputCard)
    case MergeJoin:
        return cm.estimateMergeJoinCost(left, right, outputCard)
    case NestedLoopJoin:
        return cm.estimateNestedLoopCost(left, right, outputCard)
    }
}
```

#### Selectivity Estimation

```go
func (cm *CostModel) estimateSelectivity(expr Expression, stats *TableStatistics) float64 {
    switch e := expr.(type) {
    case *ComparisonExpr:
        // Use histogram for range selectivity
        colStats := stats.ColumnStats[e.Left.(*ColumnRef).Name]
        return cm.estimateRangeSelectivity(e.Op, e.Right.(*Literal).Value, colStats)

    case *LogicalExpr:
        switch e.Op {
        case AND:
            // Assume independence for simplicity
            left := cm.estimateSelectivity(e.Left, stats)
            right := cm.estimateSelectivity(e.Right, stats)
            return left * right
        case OR:
            left := cm.estimateSelectivity(e.Left, stats)
            right := cm.estimateSelectivity(e.Right, stats)
            return left + right - left*right
        }
    }

    return 0.1 // Default selectivity
}
```

### 3. Plan Enumeration

The optimizer uses a memo structure to efficiently explore plan alternatives.

#### Memo Structure

```go
type Memo struct {
    groups     []*MemoGroup
    exprMap    map[Expression]GroupID // Expression to group mapping

    // Optimization context
    requiredProps *PhysicalProperties
}

type MemoGroup struct {
    id          GroupID
    logical     LogicalExpr
    expressions []*MemoExpression

    // Best plans for different properties
    bestPlans   map[PhysicalProperties]*BestPlan
}

type MemoExpression struct {
    op       Operator
    children []GroupID
    cost     Cost

    // Physical properties delivered
    props    PhysicalProperties
}

type PhysicalProperties struct {
    Ordering   []ColumnOrder
    Distribution DistributionSpec
    Parallel   bool
    MaxMemory  int64
}
```

#### Search Algorithm

```go
type Optimizer struct {
    memo        *Memo
    rules       []TransformationRule
    costModel   *CostModel
    stats       *StatisticsProvider

    // Search parameters
    maxDepth    int
    timeout     time.Duration
}

func (o *Optimizer) Optimize(root LogicalExpr) PhysicalPlan {
    // 1. Build initial memo from logical expression
    o.memo = buildMemo(root)

    // 2. Apply transformation rules
    o.exploreSearchSpace()

    // 3. Find best plan
    return o.findBestPlan()
}

func (o *Optimizer) exploreSearchSpace() {
    // Iterative improvement
    improved := true
    for improved && !o.timeoutReached() {
        improved = false

        // Apply rules to each group
        for _, group := range o.memo.groups {
            for _, rule := range o.rules {
                if o.applyRule(rule, group) {
                    improved = true
                }
            }
        }
    }
}
```

### 4. Transformation Rules

Rules transform logical expressions into equivalent forms.

#### Rule Framework

```go
type TransformationRule interface {
    Name() string
    Pattern() Pattern
    Transform(expr Expression, ctx *TransformContext) []Expression

    // Rule properties
    IsExploring() bool     // Generates new logical expressions
    IsImplementing() bool  // Generates physical expressions
}

type TransformContext struct {
    stats      *StatisticsProvider
    bindings   map[string]Expression
    ruleID     int
}
```

#### Predicate Pushdown

```go
type PredicatePushdown struct{}

func (pp *PredicatePushdown) Pattern() Pattern {
    // Match: Filter -> Scan
    return And{
        Type: (*Filter)(nil),
        Child: Type((*Scan)(nil)),
    }
}

func (pp *PredicatePushdown) Transform(expr Expression, ctx *TransformContext) []Expression {
    filter := expr.(*Filter)
    scan := filter.Child.(*Scan)

    // Push filter into scan
    newScan := &Scan{
        Table:     scan.Table,
        Predicate: filter.Predicate,
    }

    return []Expression{newScan}
}
```

#### Join Reordering

```go
type JoinReorder struct{}

func (jr *JoinReorder) Transform(expr Expression, ctx *TransformContext) []Expression {
    // Apply bushy join tree exploration
    // Use dynamic programming for optimal ordering
    // Consider:
    // - Left-deep vs bushy trees
    // - Interesting orderings
    // - Join selectivity
}
```

#### Constant Folding

```go
type ConstantFolding struct{}

func (cf *ConstantFolding) Transform(expr Expression, ctx *TransformContext) []Expression {
    switch e := expr.(type) {
    case *BinaryExpr:
        if left, ok := e.Left.(*Literal); ok {
            if right, ok := e.Right.(*Literal); ok {
                // Fold constants
                value := cf.evaluate(e.Op, left.Value, right.Value)
                return []Expression{&Literal{Value: value}}
            }
        }
    }
    return nil
}
```

### 5. Join Optimization

#### Join Enumeration

```go
type JoinEnumerator struct {
    stats     *StatisticsProvider
    costModel *CostModel
}

func (je *JoinEnumerator) EnumerateJoins(relations []Relation) []JoinPlan {
    // Use dynamic programming
    // State: subset of relations → best plan
    dp := make(map[uint64]*JoinPlan)

    // Base case: single relations
    for i, rel := range relations {
        mask := uint64(1) << i
        dp[mask] = &JoinPlan{
            Relations: []Relation{rel},
            Cost:      je.costModel.EstimateScanCost(rel.Table, nil),
        }
    }

    // Build up larger joins
    for size := 2; size <= len(relations); size++ {
        for _, subset := range generateSubsets(len(relations), size) {
            bestPlan := je.findBestJoinPlan(subset, dp)
            dp[subset] = bestPlan
        }
    }

    return dp[(1<<len(relations))-1]
}
```

#### Join Algorithm Selection

```go
func (je *JoinEnumerator) selectJoinAlgorithm(left, right Plan, cond Expression) JoinAlgorithm {
    // Consider factors:
    // - Input sizes
    // - Available memory
    // - Join condition properties
    // - Existing indexes

    leftCard := left.EstimatedCardinality()
    rightCard := right.EstimatedCardinality()

    // Small × Small → Nested loop
    if leftCard < 1000 && rightCard < 1000 {
        return NestedLoopJoin
    }

    // Has equality condition → Hash join
    if hasEqualityCondition(cond) {
        return HashJoin
    }

    // Both inputs ordered → Merge join
    if left.Ordering().Satisfies(cond) && right.Ordering().Satisfies(cond) {
        return MergeJoin
    }

    // Default to hash join
    return HashJoin
}
```

### 6. Index Optimization

#### Index Selection

```go
type IndexSelector struct {
    catalog *catalog.Catalog
    stats   *StatisticsProvider
}

func (is *IndexSelector) SelectIndexes(scan *Scan) []IndexAccess {
    // Find all usable indexes
    indexes := is.findMatchingIndexes(scan.Table, scan.Predicate)

    // Estimate cost for each index access
    var accesses []IndexAccess
    for _, idx := range indexes {
        access := is.estimateIndexAccess(idx, scan.Predicate)
        accesses = append(accesses, access)
    }

    // Sort by cost
    sort.Slice(accesses, func(i, j int) bool {
        return accesses[i].Cost.TotalCost < accesses[j].Cost.TotalCost
    })

    return accesses
}

func (is *IndexSelector) findMatchingIndexes(table string, predicate Expression) []Index {
    var indexes []Index

    // Get all indexes on table
    tableIndexes := is.catalog.GetIndexes(table)

    // Check which indexes match predicate
    for _, idx := range tableIndexes {
        if matches := is.matchIndex(idx, predicate); len(matches) > 0 {
            indexes = append(indexes, idx)
        }
    }

    return indexes
}
```

#### Index-Only Scan Detection

```go
func (is *IndexSelector) canUseIndexOnlyScan(index Index, requiredCols []string) bool {
    // Check if index contains all required columns
    indexCols := make(map[string]bool)
    for _, col := range index.Columns {
        indexCols[col] = true
    }

    // Include columns in predicate
    predicateCols := extractColumns(predicate)
    for _, col := range predicateCols {
        indexCols[col] = true
    }

    // Check if all required columns are in index
    for _, col := range requiredCols {
        if !indexCols[col] {
            return false
        }
    }

    return true
}
```

### 7. Parallel Execution Planning

#### Parallelism Detection

```go
type ParallelPlanner struct {
    maxWorkers int
    minRows    int64 // Minimum rows for parallel execution
}

func (pp *ParallelPlanner) PlanParallel(root PhysicalPlan) PhysicalPlan {
    // Traverse plan and identify parallelizable operators
    return pp.parallelizePlan(root)
}

func (pp *ParallelPlanner) parallelizePlan(plan PhysicalPlan) PhysicalPlan {
    switch p := plan.(type) {
    case *TableScan:
        if p.EstimatedRows > pp.minRows {
            return pp.createParallelScan(p)
        }

    case *HashJoin:
        if p.Left.EstimatedRows > pp.minRows || p.Right.EstimatedRows > pp.minRows {
            return pp.createParallelHashJoin(p)
        }

    case *Aggregate:
        if p.EstimatedRows > pp.minRows {
            return pp.createParallelAggregate(p)
        }
    }

    return plan
}
```

#### Parallel Operator Design

```go
// Parallel table scan
type ParallelTableScan struct {
    Table       string
    Workers     int
    PartitionFn PartitionFunction
}

// Parallel hash join
type ParallelHashJoin struct {
    Left        PhysicalPlan
    Right       PhysicalPlan
    Condition   Expression
    Workers     int
    PartitionFn PartitionFunction
}

// Two-phase aggregation
type ParallelAggregate struct {
    Child      PhysicalPlan
    GroupBy    []Expression
    Aggregates []AggregateFunction

    // Phase 1: Partial aggregation
    PartialAgg *HashAggregate

    // Phase 2: Final aggregation
    FinalAgg   *HashAggregate
}
```

### 8. Adaptive Optimization

#### Runtime Feedback

```go
type RuntimeStatistics struct {
    ActualCardinality   int64
    ActualTime          time.Duration
    MemoryUsage         int64
    SpillCount          int
}

type AdaptiveOptimizer struct {
    stats      map[PlanID]*RuntimeStatistics
    threshold  float64 // Deviation threshold for replanning
}

func (ao *AdaptiveOptimizer) UpdateStatistics(planID PlanID, stats *RuntimeStatistics) {
    ao.stats[planID] = stats

    // Check if actual deviates significantly from estimate
    if ao.shouldReplan(planID) {
        ao.triggerReoptimization(planID)
    }
}
```

### 9. Memory Management

#### Memory-Aware Planning

```go
type MemoryManager struct {
    totalMemory   int64
    reserved      int64
    workMem       int64 // Per-operator memory limit
}

func (mm *MemoryManager) CheckMemoryRequirements(plan PhysicalPlan) error {
    // Estimate memory usage for each operator
    usage := mm.estimateMemoryUsage(plan)

    if usage > mm.totalMemory {
        return fmt.Errorf("insufficient memory for query execution")
    }

    // Apply memory-limiting transformations
    return mm.applyMemoryOptimizations(plan)
}

func (mm *MemoryManager) applyMemoryOptimizations(plan PhysicalPlan) PhysicalPlan {
    // Convert hash joins to merge joins if memory constrained
    // Add external sort operators
    // Enable spilling for aggregations
}
```

## Implementation Considerations

### 1. Performance

- Memoization of intermediate results
- Incremental plan refinement
- Parallel exploration of search space
- Early termination for timeout

### 2. Correctness

- Rule validation framework
- Plan equivalence checking
- Cost model calibration
- Extensive test coverage

### 3. Extensibility

- Plugin architecture for rules
- Configurable cost parameters
- Statistics customization
- Rule priority management

### 4. Debugging

- Plan visualization
- Cost breakdown reporting
- Rule application tracing
- Statistics inspection tools

## Testing Strategy

1. **Unit Tests**: Individual components (rules, cost model, statistics)
2. **Integration Tests**: End-to-end optimization
3. **Regression Tests**: TPC-H and TPC-DS queries
4. **Performance Tests**: Optimization time and plan quality
5. **Randomized Tests**: SQLSmith for coverage

## Future Enhancements

1. **Incremental Optimization**: Reuse previous optimization results
2. **Machine Learning**: Learn selectivity and cost estimates
3. **Multi-Query Optimization**: Optimize across query batches
4. **Materialized Views**: Automatic view selection
5. **GPU Acceleration**: Cost model for GPU operators