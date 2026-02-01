# Window Functions Design Document

## Architecture Overview

The window functions implementation follows a vectorized execution model with the following key components:

### 1. Core Execution Flow

```
SQL Query with Window Functions
    ↓
Parser → AST with WindowExpr nodes
    ↓
Binder → BoundWindowExpr with resolved types
    ↓
Planner → PhysicalWindow plan node
    ↓
Executor → PhysicalWindowExecutor
    ↓
WindowState → Partitioned data with peer groups
    ↓
FrameComputer → Dynamic frame boundaries
    ↓
WindowFunctionEvaluator → Vectorized computation
    ↓
DataChunk → Results with window columns
```

### 2. Key Data Structures

#### WindowState
The WindowState manages all window-related data during execution:

```go
type WindowState struct {
    // Partition management
    Partitions     map[string]*WindowPartition
    PartitionOrder []string  // Deterministic iteration order

    // Global state
    TotalRows      int64
    MemoryUsage    int64

    // Spilling support
    SpillManager   *WindowSpillManager
    InMemoryRows   int64
    MaxMemoryRows  int64
}

type WindowPartition struct {
    Rows           []WindowRow
    PeerBoundaries []int      // Peer group start indices
    SpillFile      string     // For out-of-core processing

    // Optimized access patterns
    ValueCache     map[int]any  // Cached expression values
    FrameCache     *LRUCache    // Cached frame boundaries
}
```

#### WindowRow
Each row maintains its original position and computed results:

```go
type WindowRow struct {
    OriginalIndex int           // Position in input
    Values        []any         // Input column values
    WindowResults []any         // Computed window function results
    PeerGroupID   int           // Cached peer group membership

    // For vectorized processing
    SelectionMask []bool        // Which rows to include
}
```

### 3. Frame Computation Engine

The frame computation engine handles three frame types with different semantics:

#### ROWS Frame
Physical row-based boundaries:

```go
type RowsFrameComputer struct {
    partition *WindowPartition

    // For sliding windows optimization
    currentStart int
    currentEnd   int

    // Incremental computation
    prevRowIdx   int
    frameCache   []FrameBounds
}

func (r *RowsFrameComputer) ComputeBounds(rowIdx int, frame *WindowFrame) FrameBounds {
    // Handle incremental updates for sliding windows
    if r.canIncrementalUpdate(rowIdx) {
        return r.incrementalUpdate(rowIdx, frame)
    }

    // Full computation
    start := r.computeStartBound(rowIdx, frame.Start)
    end := r.computeEndBound(rowIdx, frame.End)

    return FrameBounds{Start: start, End: end}
}
```

#### RANGE Frame
Value-based boundaries with peer groups:

```go
type RangeFrameComputer struct {
    partition     *WindowPartition
    orderByValues []any  // Cached ORDER BY values

    // Binary search optimization
    valueIndices  map[any]int  // Value → first occurrence

    // For temporal ranges (INTERVAL support)
    isTemporal    bool
    timeUnit      TimeUnit
}

func (r *RangeFrameComputer) ComputeBounds(
    rowIdx int,
    frame *WindowFrame,
    windowExpr *BoundWindowExpr,
) FrameBounds {
    // Get current row's ORDER BY value
    currentValue := r.getOrderByValue(rowIdx, windowExpr)

    // Compute target values with offsets
    startValue := r.computeStartValue(currentValue, frame.Start)
    endValue := r.computeEndValue(currentValue, frame.End)

    // Binary search for bounds
    start := r.findFirstGEQ(startValue)
    end := r.findLastLEQ(endValue)

    return FrameBounds{Start: start, End: end}
}
```

#### GROUPS Frame
Peer group-based boundaries:

```go
type GroupsFrameComputer struct {
    partition      *WindowPartition
    peerBoundaries []int

    // Current peer group tracking
    currentGroup   int
    groupStart     int
    groupEnd       int
}

func (g *GroupsFrameComputer) ComputeBounds(
    rowIdx int,
    frame *WindowFrame,
) FrameBounds {
    // Find current peer group
    currentGroup := g.findPeerGroup(rowIdx)

    // Compute target groups with offsets
    startGroup := g.computeStartGroup(currentGroup, frame.Start)
    endGroup := g.computeEndGroup(currentGroup, frame.End)

    // Convert groups to row boundaries
    start := g.peerBoundaries[startGroup]
    end := g.peerBoundaries[endGroup+1] - 1

    return FrameBounds{Start: start, End: end}
}
```

### 4. Vectorized Window Functions

#### Ranking Functions
Vectorized implementation for ranking functions:

```go
type RankingFunctionEvaluator struct {
    // Pre-computed peer groups
    peerGroupIDs    []int
    peerGroupStarts map[int]int

    // For NTILE
    ntileBuckets    []int

    // Vectorized output
    resultVector    []int64
}

func (r *RankingFunctionEvaluator) EvaluateRowNumber(
    partition *WindowPartition,
    startRow, endRow int,
) []int64 {
    // Vectorized row number assignment
    results := make([]int64, endRow-startRow)
    for i := range results {
        results[i] = int64(startRow + i + 1)
    }
    return results
}

func (r *RankingFunctionEvaluator) EvaluateRank(
    partition *WindowPartition,
    startRow, endRow int,
) []int64 {
    results := make([]int64, endRow-startRow)

    for i := startRow; i < endRow; i++ {
        peerGroup := r.peerGroupIDs[i]
        results[i-startRow] = int64(r.peerGroupStarts[peerGroup] + 1)
    }

    return results
}
```

#### Value Functions
Optimized value access with caching:

```go
type ValueFunctionEvaluator struct {
    // Expression evaluation cache
    valueCache *ValueCache

    // For LAG/LEAD with IGNORE NULLS
    nonNullIndices map[int][]int

    // Frame-aware optimization
    frameType      FrameType
    isSliding      bool
}

func (v *ValueFunctionEvaluator) EvaluateLag(
    partition *WindowPartition,
    expr *BoundExpr,
    offset int,
    defaultValue any,
    ignoreNulls bool,
    frame FrameBounds,
) []any {
    results := make([]any, frame.End-frame.Start+1)

    if ignoreNulls {
        // Pre-compute non-null indices
        nonNulls := v.computeNonNullIndices(partition, expr, frame)

        for i, rowIdx := range nonNulls {
            targetIdx := i - offset
            if targetIdx >= 0 && targetIdx < len(nonNulls) {
                results[rowIdx-frame.Start] = v.getCachedValue(partition, expr, nonNulls[targetIdx])
            } else {
                results[rowIdx-frame.Start] = defaultValue
            }
        }
    } else {
        // Standard LAG
        for i := frame.Start; i <= frame.End; i++ {
            targetIdx := i - offset
            if targetIdx >= frame.Start && targetIdx <= frame.End {
                results[i-frame.Start] = v.getCachedValue(partition, expr, targetIdx)
            } else {
                results[i-frame.Start] = defaultValue
            }
        }
    }

    return results
}
```

#### Aggregate Window Functions
Sliding window optimization for aggregates:

```go
type AggregateWindowEvaluator struct {
    // Incremental computation state
    currentSum   float64
    currentCount int64
    currentMin   any
    currentMax   any

    // For distinct aggregates
    distinctSet  map[string]bool

    // Sliding window optimization
    slidingState *SlidingWindowState
}

type SlidingWindowState struct {
    // Ring buffer for incremental updates
    values       []float64
    startIdx     int
    endIdx       int

    // Running statistics
    sum          float64
    count        int64

    // Min/max heap for efficient updates
    minHeap      *MinHeap
    maxHeap      *MaxHeap
}

func (a *AggregateWindowEvaluator) EvaluateSumSliding(
    partition *WindowPartition,
    expr *BoundExpr,
    frame FrameBounds,
    prevFrame FrameBounds,
) float64 {
    // Handle incremental updates
    if a.canSlide(frame, prevFrame) {
        // Remove values leaving the frame
        for i := prevFrame.Start; i < frame.Start; i++ {
            if !a.isRowExcluded(i) {
                val := a.getValue(partition, expr, i)
                a.slidingState.remove(val)
            }
        }

        // Add values entering the frame
        for i := prevFrame.End + 1; i <= frame.End; i++ {
            if !a.isRowExcluded(i) {
                val := a.getValue(partition, expr, i)
                a.slidingState.add(val)
            }
        }

        return a.slidingState.sum
    }

    // Full recomputation
    return a.evaluateSumFull(partition, expr, frame)
}
```

### 5. Memory Management

#### Memory Pools
Pre-allocated memory pools for common operations:

```go
type WindowMemoryPool struct {
    // Fixed-size pools
    int64Pool    *sync.Pool
    float64Pool  *sync.Pool
    stringPool   *sync.Pool

    // Variable-size pools
    slicePool    *sync.Pool
    mapPool      *sync.Pool

    // Memory tracking
    allocated    int64
    maxAllocated int64
}

func (p *WindowMemoryPool) GetInt64Slice(size int) []int64 {
    v := p.int64Pool.Get()
    if slice, ok := v.([]int64); ok && cap(slice) >= size {
        return slice[:size]
    }
    return make([]int64, size)
}

func (p *WindowMemoryPool) PutInt64Slice(slice []int64) {
    // Clear references
    for i := range slice {
        slice[i] = 0
    }
    p.int64Pool.Put(slice)
}
```

#### Spilling Strategy
Automatic spilling when memory limits are exceeded:

```go
type WindowSpillManager struct {
    tempDir      string
    spillFiles   []string

    // Thresholds
    memoryLimit  int64
    rowLimit     int64

    // Spilling state
    isSpilling   bool
    spilledRows  int64
}

func (s *WindowSpillManager) SpillPartition(
    partition *WindowPartition,
    partitionKey string,
) error {
    // Create spill file
    spillFile := filepath.Join(s.tempDir, fmt.Sprintf("window_%s.tmp", partitionKey))

    // Write partition data
    file, err := os.Create(spillFile)
    if err != nil {
        return err
    }
    defer file.Close()

    encoder := gob.NewEncoder(file)

    // Write metadata
    metadata := SpillMetadata{
        PartitionKey:   partitionKey,
        RowCount:       len(partition.Rows),
        PeerBoundaries: partition.PeerBoundaries,
    }

    if err := encoder.Encode(metadata); err != nil {
        return err
    }

    // Write rows in batches
    batchSize := 1000
    for i := 0; i < len(partition.Rows); i += batchSize {
        end := i + batchSize
        if end > len(partition.Rows) {
            end = len(partition.Rows)
        }

        batch := partition.Rows[i:end]
        if err := encoder.Encode(batch); err != nil {
            return err
        }
    }

    s.spillFiles = append(s.spillFiles, spillFile)
    s.spilledRows += int64(len(partition.Rows))

    // Clear from memory
    partition.Rows = nil
    partition.SpillFile = spillFile

    return nil
}
```

### 6. Vectorized Execution

#### Batch Processing
Process multiple rows simultaneously:

```go
type VectorizedWindowExecutor struct {
    batchSize    int

    // Vector caches
    frameStarts  []int
    frameEnds    []int
    peerGroups   []int

    // SIMD-friendly arrays
    int64Results []int64
    floatResults []float64

    // Selection vectors
    validMask    []bool
    nullMask     []bool
}

func (v *VectorizedWindowExecutor) ProcessBatch(
    partition *WindowPartition,
    startRow, endRow int,
    windowExpr *BoundWindowExpr,
) (*storage.DataChunk, error) {
    batchSize := endRow - startRow

    // Pre-compute frame boundaries
    v.computeFrameBoundariesVectorized(partition, startRow, endRow, windowExpr)

    // Vectorized function evaluation
    switch windowExpr.FunctionName {
    case "ROW_NUMBER":
        v.evaluateRowNumberVectorized(startRow, batchSize)
    case "RANK":
        v.evaluateRankVectorized(partition, startRow, batchSize)
    case "SUM":
        v.evaluateSumVectorized(partition, startRow, endRow, windowExpr)
    }

    // Build output chunk
    return v.buildOutputChunk(partition, startRow, endRow)
}
```

#### Parallel Partition Processing
Process independent partitions concurrently:

```go
type ParallelWindowProcessor struct {
    workerCount int
    partitionCh chan *WindowPartition
    resultCh    chan *PartitionResult

    // Synchronization
    wg          sync.WaitGroup
    ctx         context.Context
}

func (p *ParallelWindowProcessor) ProcessPartitions(
    state *WindowState,
    windowExpr *BoundWindowExpr,
) error {
    // Start workers
    for i := 0; i < p.workerCount; i++ {
        p.wg.Add(1)
        go p.worker(i, windowExpr)
    }

    // Send partitions to workers
    go func() {
        defer close(p.partitionCh)
        for _, key := range state.PartitionOrder {
            partition := state.Partitions[key]
            select {
            case p.partitionCh <- partition:
            case <-p.ctx.Done():
                return
            }
        }
    }()

    // Collect results
    go func() {
        p.wg.Wait()
        close(p.resultCh)
    }()

    // Merge results back into state
    for result := range p.resultCh {
        state.Partitions[result.PartitionKey] = result.Partition
    }

    return nil
}
```

### 7. Optimization Strategies

#### Index-Based Frame Computation
Use indexes for efficient range queries:

```go
type IndexedRangeComputer struct {
    // B-tree index on ORDER BY columns
    orderByIndex *btree.BTree

    // Value-to-position mapping
    valueMap     map[any][]int

    // Pre-sorted arrays for binary search
    sortedValues []any
    positions    []int
}

func (i *IndexedRangeComputer) BuildIndex(partition *WindowPartition, windowExpr *BoundWindowExpr) {
    // Extract ORDER BY values
    values := make([]indexedValue, 0, len(partition.Rows))

    for idx, row := range partition.Rows {
        value := i.getOrderByValue(row, windowExpr)
        values = append(values, indexedValue{
            Value: value,
            Position: idx,
        })
    }

    // Sort by value
    sort.Slice(values, func(i, j int) bool {
        return compareValues(values[i].Value, values[j].Value) < 0
    })

    // Build index structures
    i.sortedValues = make([]any, len(values))
    i.positions = make([]int, len(values))

    for i, v := range values {
        i.sortedValues[i] = v.Value
        i.positions[i] = v.Position
    }
}
```

#### Incremental Aggregation
Maintain running aggregates for sliding windows:

```go
type IncrementalAggregator struct {
    // Current window state
    sum          float64
    count        int64

    // Deque for sliding window
    window       *Deque

    // For distinct aggregates
    valueCounts  map[any]int64
}

type Deque struct {
    items    []any
    head     int
    tail     int
    size     int
}

func (d *Deque) PushBack(item any) {
    if d.size == len(d.items) {
        d.grow()
    }

    d.items[d.tail] = item
    d.tail = (d.tail + 1) % len(d.items)
    d.size++
}

func (d *Deque) PopFront() any {
    if d.size == 0 {
        return nil
    }

    item := d.items[d.head]
    d.items[d.head] = nil  // Clear reference
    d.head = (d.head + 1) % len(d.items)
    d.size--

    return item
}
```

### 8. Integration with Query Optimizer

#### Window Function Pushdown
Push window functions closer to data:

```go
type WindowPushdownRule struct {
    // Statistics for cost estimation
    rowCount        int64
    partitionCount  int64
    avgPartitionSize int64

    // Cost model
    cpuCost         float64
    memoryCost      float64
    ioCost          float64
}

func (w *WindowPushdownRule) Apply(plan *LogicalPlan) *LogicalPlan {
    // Find window functions that can be pushed down
    for _, node := range plan.Nodes {
        if windowNode, ok := node.(*LogicalWindow); ok {
            // Check if we can push through joins
            if w.canPushThroughJoin(windowNode) {
                return w.pushThroughJoin(windowNode)
            }

            // Check if we can push through aggregates
            if w.canPushThroughAggregate(windowNode) {
                return w.pushThroughAggregate(windowNode)
            }
        }
    }

    return plan
}
```

#### Partition Pruning
Eliminate unnecessary partitions:

```go
type PartitionPruningRule struct {
    // Partition statistics
    partitionStats map[string]*PartitionStat

    // Query predicates
    predicates     []Expression
}

type PartitionStat struct {
    MinValue     any
    MaxValue     any
    NullCount    int64
    RowCount     int64
}

func (p *PartitionPruningRule) Apply(windowNode *LogicalWindow) *LogicalWindow {
    // Extract partition predicates
    partitionPreds := p.extractPartitionPredicates(windowNode)

    // Check each partition
    prunedPartitions := make([]string, 0)
    for key, stat := range p.partitionStats {
        if !p.canPrunePartition(stat, partitionPreds) {
            prunedPartitions = append(prunedPartitions, key)
        }
    }

    // Update window node with pruned partitions
    windowNode.PrunedPartitions = prunedPartitions

    return windowNode
}
```

### 9. Error Handling and Edge Cases

#### NULL Handling
Proper NULL handling in all window functions:

```go
func handleNullsInFrame(partition *WindowPartition, frame FrameBounds) FrameBounds {
    // Adjust frame bounds to exclude NULL values if IGNORE NULLS is specified
    if frame.IgnoreNulls {
        // Find first non-NULL in frame
        for frame.Start <= frame.End && partition.Rows[frame.Start].Value == nil {
            frame.Start++
        }

        // Find last non-NULL in frame
        for frame.End >= frame.Start && partition.Rows[frame.End].Value == nil {
            frame.End--
        }
    }

    return frame
}
```

#### Overflow Handling
Handle arithmetic overflow in aggregates:

```go
func safeAddInt64(a, b int64) (int64, bool) {
    if b > 0 && a > math.MaxInt64 - b {
        return 0, true  // Overflow
    }
    if b < 0 && a < math.MinInt64 - b {
        return 0, true  // Underflow
    }
    return a + b, false
}

func safeMultiplyFloat64(a, b float64) (float64, bool) {
    result := a * b
    if math.IsInf(result, 0) {
        return 0, true
    }
    return result, false
}
```

### 10. Testing Strategy

#### Unit Tests
Comprehensive unit tests for each component:

```go
func TestRowsFrameComputer(t *testing.T) {
    tests := []struct {
        name     string
        frame    *WindowFrame
        rowIdx   int
        expected FrameBounds
    }{
        {
            name: "unbounded_preceding_to_current_row",
            frame: &WindowFrame{
                Type: FrameTypeRows,
                Start: WindowBound{Type: BoundUnboundedPreceding},
                End: WindowBound{Type: BoundCurrentRow},
            },
            rowIdx: 5,
            expected: FrameBounds{Start: 0, End: 5},
        },
        {
            name: "rows_between_1_preceding_and_1_following",
            frame: &WindowFrame{
                Type: FrameTypeRows,
                Start: WindowBound{Type: BoundPreceding, Offset: 1},
                End: WindowBound{Type: BoundFollowing, Offset: 1},
            },
            rowIdx: 5,
            expected: FrameBounds{Start: 4, End: 6},
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            computer := NewRowsFrameComputer(partition)
            bounds := computer.ComputeBounds(tt.rowIdx, tt.frame)

            if bounds != tt.expected {
                t.Errorf("expected %v, got %v", tt.expected, bounds)
            }
        })
    }
}
```

#### Integration Tests
End-to-end tests with real data:

```go
func TestWindowFunctionsIntegration(t *testing.T) {
    // Create test database
    db := createTestDB(t)

    // Create test data
    db.Exec(`
        CREATE TABLE sales (
            date DATE,
            product STRING,
            amount DECIMAL(10,2)
        )
    `)

    // Insert test data
    db.Exec(`
        INSERT INTO sales VALUES
        ('2024-01-01', 'A', 100),
        ('2024-01-02', 'A', 200),
        ('2024-01-03', 'A', 150),
        ('2024-01-01', 'B', 300),
        ('2024-01-02', 'B', 250)
    `)

    // Test ranking functions
    rows := db.Query(`
        SELECT
            product,
            date,
            amount,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY date) as row_num,
            RANK() OVER (PARTITION BY product ORDER BY amount DESC) as rank,
            SUM(amount) OVER (PARTITION BY product ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum
        FROM sales
        ORDER BY product, date
    `)

    // Verify results
    expected := []struct {
        product   string
        date      string
        amount    float64
        rowNum    int
        rank      int
        runningSum float64
    }{
        {"A", "2024-01-01", 100, 1, 3, 100},
        {"A", "2024-01-02", 200, 2, 1, 300},
        {"A", "2024-01-03", 150, 3, 2, 450},
        {"B", "2024-01-01", 300, 1, 1, 300},
        {"B", "2024-01-02", 250, 2, 2, 550},
    }

    verifyResults(t, rows, expected)
}
```

#### Performance Tests
Benchmark critical paths:

```go
func BenchmarkWindowFunctions(b *testing.B) {
    benchmarks := []struct {
        name      string
        rows      int
        partitions int
        setup     string
        query     string
    }{
        {
            name:      "row_number_over_partition",
            rows:      1000000,
            partitions: 100,
            setup: `
                CREATE TABLE test (id INT, category STRING, value FLOAT);
                INSERT INTO test
                SELECT i, 'cat' || (i % 100), random()
                FROM generate_series(1, 1000000) t(i);
            `,
            query: `
                SELECT id, category, value,
                       ROW_NUMBER() OVER (PARTITION BY category ORDER BY value)
                FROM test;
            `,
        },
        {
            name:      "sliding_window_sum",
            rows:      1000000,
            partitions: 1,
            setup: `
                CREATE TABLE test (ts TIMESTAMP, value FLOAT);
                INSERT INTO test
                SELECT timestamp '2024-01-01' + interval '1 second' * i, random()
                FROM generate_series(1, 1000000) t(i);
            `,
            query: `
                SELECT ts, value,
                       SUM(value) OVER (ORDER BY ts ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)
                FROM test;
            `,
        },
    }

    for _, bm := range benchmarks {
        b.Run(bm.name, func(b *testing.B) {
            // Setup
            db := createTestDB(b)
            db.Exec(bm.setup)

            b.ResetTimer()

            for i := 0; i < b.N; i++ {
                db.Exec(bm.query)
            }
        })
    }
}
```

This design provides a comprehensive foundation for implementing window functions with:
1. Vectorized execution for performance
2. Memory-efficient frame handling
3. Parallel processing capabilities
4. Optimization strategies
5. Comprehensive error handling
6. Thorough testing approach

The implementation will be modular, allowing for incremental development and testing of each component while maintaining compatibility with the existing codebase."}