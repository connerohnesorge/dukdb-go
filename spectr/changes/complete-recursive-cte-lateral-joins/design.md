# Design: Recursive CTE and Lateral Join Completion

## Implementation Details

### Cycle Detection Architecture

#### CycleDetector Interface

```go
// CycleDetector detects cycles in recursive CTEs using row identity.
type CycleDetector interface {
    // Add inserts a row identifier into the cycle detector.
    // Returns true if row was already present (cycle detected).
    Add(keyValues []any) bool

    // Contains checks if a row has been seen without adding it.
    Contains(keyValues []any) bool

    // Reset clears all tracked rows for the next iteration.
    Reset()

    // Size returns the number of tracked rows.
    Size() int
}
```

#### HashCycleDetector Implementation

```go
// HashCycleDetector uses Go maps for O(1) cycle lookup.
type HashCycleDetector struct {
    seenRows map[string]bool  // row hash → seen
    keyNames []string         // column names being tracked
}

// Key generation: concatenate column values with separators to create composite key
// Handle NULLs: use special marker "\x00NULL\x00" for NULL values
// Type coercion: convert all values to string for consistent comparison
```

**Rationale**: Go maps provide efficient O(1) lookup and insertion. Alternative trie-based approach considered but map simpler and sufficient.

### Work Table Memory Pool

#### WorkTablePool Design

```go
// WorkTablePool manages reusable DataChunk buffers for recursive CTE work tables.
type WorkTablePool struct {
    available chan *storage.DataChunk  // available chunks
    maxSize   int                       // max chunks in pool
    factory   ChunkFactory              // creates new chunks
}

// Strategy: Pool maintains fixed number of pre-allocated chunks
// - Acquire(): returns available chunk or creates new (if under limit)
// - Release(): returns chunk to pool for reuse
// - Eviction: LRU or time-based if pool exceeds maxSize
```

**Rationale**: Fixed pool size prevents unbounded memory growth. Pre-allocation reduces allocation overhead. Alternative: Dynamic pool with backpressure. Chosen fixed for deterministic memory behavior.

#### Memory Scaling Analysis

For a query with 1000 recursion levels:
- With pooling: Memory = base_chunk_size × concurrent_chunks (typically 2-3 chunks)
- Without pooling: Memory = base_chunk_size × 1000 (linear growth)
- Result: ~100-500x memory improvement

### Recursive CTE Iteration Strategy

#### Fixpoint Algorithm with Streaming

```
1. Execute anchor query → produces initial WorkTable
2. Iterator 1:
   - Execute recursive part with WorkTable as input
   - Filter rows via CycleDetector
   - Check MAX_RECURSION limit
   - Combine results into combined table
   - Return DataChunk to application (streaming)
   - If no new rows, STOP
   - Otherwise, combined table becomes new WorkTable
3. Repeat iterator steps until termination
```

**Streaming Decision**: Each iteration's results become available immediately via `Next()`. Application can process partial results without waiting for full recursion. Enables:
- Early termination (cancel after N chunks)
- Progressive analysis
- Better resource utilization on large recursion trees

### LATERAL Join Row-by-Row Evaluation

#### Operator Design

```go
type PhysicalLateralJoinOperator struct {
    left         PhysicalOperator      // outer table
    rightPlan    planner.PhysicalPlan  // subquery plan (re-executed per row)
    joinType     planner.JoinType      // INNER/LEFT/RIGHT/FULL/CROSS

    // Execution state
    leftChunk    *storage.DataChunk    // current outer chunk
    leftRowIdx   int                    // current row in chunk
    rightResult  *ExecutionResult      // cached results from last subquery execution
    rightRowIdx  int                    // current row in rightResult
}

// Next() algorithm:
// 1. If leftRowIdx >= leftChunk.Count():
//    - Fetch next leftChunk
//    - Reset rightRowIdx
// 2. If rightRowIdx < rightResult.RowCount():
//    - Combine current left row with current right row
//    - Increment rightRowIdx
//    - Return combined row
// 3. Else (no more right rows for this left row):
//    - If LEFT/FULL join: return left row with NULLs for right columns
//    - Move to next left row
//    - Re-execute right plan with new left row context
//    - Reset rightRowIdx to 0
```

**Design Rationale**:
- Row-by-row evaluation allows full correlation with outer context
- Caching right result for current left row avoids re-execution until done with all right rows
- Join type handling via `hasEmittedMatch` flag

### Type Coercion Strategy

#### Type Promotion Rules (per DuckDB)

```
INT + FLOAT → FLOAT
INT + DECIMAL → DECIMAL
FLOAT + DECIMAL → DECIMAL
VARCHAR + STRING → VARCHAR (larger string type wins)
ANY + NULL → (type of ANY, NULL doesn't affect result type)
```

**Implementation**: Traverse output columns, collect types from non-NULL values, determine promoted type using promotion rules, cast all values to promoted type.

### Cost-Based LATERAL Join Optimization

#### Cost Model

```
LateralJoinCost = (outer_row_count × subquery_cost) + join_overhead
RegularJoinCost = outer_scan + inner_scan + join_hash

Decision: Use LATERAL if:
1. Correlation required (forced LATERAL syntax), OR
2. LateralJoinCost < RegularJoinCost AND outer_row_count small (< 10K)
```

**Rationale**: LATERAL is necessary for correlated subqueries but expensive for large outer tables due to per-row evaluation. Cost model enables intelligent decisions.

### Error Detection Mechanisms

#### Forward Reference Detection
- During binding phase, check if LATERAL subquery references tables not in FROM scope
- Error: "table 'X' not available in LATERAL scope"

#### Circular LATERAL Reference Detection
- Track LATERAL subquery dependency chain
- Detect if subquery references table that itself has LATERAL reference back
- Error: "circular LATERAL reference detected"

#### Ambiguous Column Resolution
- When resolving outer column, track which outer tables have it
- If multiple matches: error "column 'X' is ambiguous" with list of tables
- Require qualified name (table.column)

#### Type Mismatch Detection
- After type coercion, verify no data loss
- Warning (not error) if narrowing conversion (FLOAT → INT)

## Context

### Why This Approach?

1. **Cycle Detection via Hashing**:
   - Simpler than DuckDB's approach (uses Bloom filters)
   - Go map provides sufficient performance
   - Composite key handling straightforward

2. **Fixed Memory Pool**:
   - Deterministic memory behavior
   - No need for complex eviction strategies
   - Pre-allocation reduces GC pressure

3. **Streaming Output**:
   - Matches DuckDB's iterator model
   - Enables progressive analysis
   - Allows cancellation without waiting

4. **Row-by-Row LATERAL Evaluation**:
   - Only way to maintain correlation context
   - Matches SQL standard semantics
   - Result caching minimizes re-execution

## Goals / Non-Goals

### Goals
- ✅ Full DuckDB v1.4.3 compatibility for CTE/LATERAL syntax
- ✅ Sub-second performance for typical queries (< 1K nodes)
- ✅ Bounded memory usage (linear scaling with recursion depth)
- ✅ Support advanced patterns (cycles, JOINs, aggregation)
- ✅ Clear error messages for invalid patterns
- ✅ Streaming results for progressive analysis

### Non-Goals
- ❌ DuckDB source code compatibility (pure Go implementation)
- ❌ Bloom filter optimization (Go map sufficient)
- ❌ GPU acceleration (out of scope for this phase)
- ❌ Distributed recursion (single-machine only)
- ❌ Bidirectional joins (forward evaluation only)

## Decisions

### Decision: Hash-Based vs Trie-Based Cycle Detection
- **Choice**: Hash-based (Go map)
- **Alternatives**:
  - Bloom filters (more memory-efficient but can have false positives)
  - Trie (better for string keys, worse for mixed types)
- **Rationale**: Go map provides O(1) performance, simplicity, and false-negative-free correctness

### Decision: Fixed vs Dynamic Memory Pool
- **Choice**: Fixed-size pool with LRU eviction
- **Alternatives**:
  - Unbounded pool (risk of OOM)
  - Dynamic with exponential backoff (complex)
- **Rationale**: Fixed size prevents surprises, LRU ensures hot chunks are retained

### Decision: Streaming vs Batch Output
- **Choice**: Streaming (return results as iterations complete)
- **Alternatives**:
  - Batch (accumulate all results, return at end)
  - Materialized (write to temp file, stream from file)
- **Rationale**: Streaming matches standard iterator pattern, enables cancellation, better for large results

### Decision: Row-by-Row vs Vectorized LATERAL
- **Choice**: Row-by-row (one left row, N right rows per iteration)
- **Alternatives**:
  - Vectorized (batch of left rows evaluated together)
  - Correlated subquery optimization (convert to JOIN when possible)
- **Rationale**: Correlation requires row-by-row context; vectorization possible future optimization

## Risks / Trade-offs

| Risk | Mitigation |
|------|-----------|
| **Memory exhaustion on 1M+ recursion levels** | Memory pool with configurable max size; hard limit enforced |
| **Performance degradation on very large outer tables (10M+)** | Cost model may suggest materialization; optional hint support |
| **Type coercion subtle bugs** | Comprehensive test suite covering all type combinations |
| **Circular reference detection incomplete** | Depth-first traversal during binding phase to catch cycles |
| **NULL handling edge cases** | Cross-validate against DuckDB on all NULL scenarios |

## Migration Plan

### Backward Compatibility
- Existing recursive CTE queries without USING KEY continue working
- Existing LATERAL joins continue working
- No breaking changes to public API

### Rollout Strategy
1. Phase 1-2 (Cycle detection + MAX_RECURSION) - low risk, independent
2. Phase 3-4 (Memory pooling + streaming) - tested with benchmarks
3. Phase 5-6 (LATERAL completion) - parallel, lower priority than CTE

### Rollback Plan
- Feature flags for USING KEY and streaming (disable via environment variable)
- Keep old non-pooled code path as fallback
- Gradual rollout: opt-in → default → mandatory (after 2 releases)

## Open Questions

1. **MAX_RECURSION default value**: Should we have a default (e.g., 10000) or require explicit specification?
   - **Decision needed**: Default sensible or too risky?

2. **LATERAL performance acceptable for 10M outer rows?**: Need to determine threshold where LATERAL should be discouraged.
   - **Recommendation**: Suggest materialization in EXPLAIN for outer > 100K with expensive subquery

3. **Streaming chunking strategy**: How large should each returned chunk be (1024 rows, 10K rows, etc.)?
   - **Current plan**: Use default 2048-row chunks from storage layer

4. **Type coercion warnings**: Should narrowing conversions (FLOAT→INT) produce warnings?
   - **Recommendation**: Warning only in debug mode, not default behavior

5. **Cycle detection key performance**: What's acceptable overhead for composite keys?
   - **Benchmark needed**: Compare 1-column vs 4-column key performance
