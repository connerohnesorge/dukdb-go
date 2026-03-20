## Implementation Tasks

### Phase 1: Cycle Detection and USING KEY Optimization

- [ ] 1.1 Create CycleDetector interface and hash-based implementation
  - Implement `CycleDetector` interface with `Add()`, `Contains()`, `Reset()` methods
  - Implement `HashCycleDetector` using Go map with composite key hashing
  - Support single and multi-column keys
  - Add tests for cycle detection correctness

- [ ] 1.2 Update PhysicalRecursiveCTE to use cycle detection
  - Modify `execute()` method to instantiate CycleDetector from USING KEY clause
  - Filter rows in each iteration that would create cycles
  - Test with graph traversal queries (parent-child trees, etc.)

- [ ] 1.3 Add USING KEY clause parsing support
  - Update `ParserCTE` struct to include `UsingKey []string` field
  - Parse USING KEY syntax in SQL parser (after UNION ALL)
  - Validate that USING KEY columns exist in CTE output schema
  - Test parser with valid and invalid USING KEY clauses

- [ ] 1.4 Add integration tests for USING KEY
  - Create test graphs (cycles, DAGs, trees)
  - Test cycle detection on 100K node graphs
  - Verify sub-second performance with cycle detection
  - Test multi-column cycle keys

### Phase 2: MAX_RECURSION and Recursion Control

- [ ] 2.1 Add MAX_RECURSION option parsing
  - Create `RecursionOption` struct with `MaxRecursion` field
  - Parse OPTION (MAX_RECURSION N) syntax after recursive CTE query
  - Store in `PlannedQuery` or `PhysicalRecursiveCTE`
  - Test parser for valid and invalid values

- [ ] 2.2 Implement MAX_RECURSION enforcement in executor
  - Add iteration counter to `PhysicalRecursiveCTEOperator`
  - Check counter against MAX_RECURSION limit each iteration
  - Return error when exceeded: "recursion limit exceeded: max N iterations"
  - Return all accumulated rows when limit hit
  - Test with simple CTEs hitting and not hitting limit

- [ ] 2.3 Add RowsAffected for recursive CTEs
  - Modify executor to return accurate RowsAffected count
  - Count total rows generated across all iterations
  - Test with various recursion depths

- [ ] 2.4 Integration tests for MAX_RECURSION
  - Test CTEs that terminate before limit
  - Test CTEs that hit limit and error
  - Test with different limit values (1, 10, 100, 1000)
  - Test edge case: MAX_RECURSION 0 (should error immediately)

### Phase 3: Memory Pooling and Complex Patterns

- [ ] 3.1 Create work table memory pool
  - Implement `WorkTablePool` with reusable `DataChunk` buffers
  - Add `Acquire(size, columnTypes)` method to get or allocate chunk
  - Add `Release(chunk)` method to return for reuse
  - Implement LRU or size-based eviction policy
  - Test memory reuse and deallocation

- [ ] 3.2 Integrate memory pooling into PhysicalRecursiveCTE
  - Replace direct `DataChunk` allocation with pool
  - Update `execute()` to use pool for work table
  - Measure memory usage: should scale linearly with depth, not exponentially
  - Test with 1000-level deep recursion and 1M-row base tables

- [ ] 3.3 Support complex recursive patterns with JOINs
  - Test recursive CTE with `JOIN table` in recursive part
  - Test with aggregation (`GROUP BY`) in recursive part
  - Test with multiple recursive parts (multiple `UNION ALL`)
  - Verify correctness against DuckDB reference implementation

- [ ] 3.4 Implementation tests for memory efficiency
  - Benchmark recursion depth 10, 100, 1000, 10000
  - Verify linear memory scaling (not exponential)
  - Test with various column types and sizes
  - Generate memory usage report

### Phase 4: Streaming Results

- [ ] 4.1 Implement streaming in PhysicalRecursiveCTE
  - Modify `Next()` method to return results as iterations complete
  - Don't wait for full recursion before returning first chunk
  - Add cancellation support via context
  - Test that application can process partial results

- [ ] 4.2 Add cancellation support
  - Implement graceful shutdown when context cancelled
  - Free pending work tables and resources
  - Test with early termination (Stop() after N chunks)

- [ ] 4.3 Tests for streaming behavior
  - Verify results available incrementally
  - Test cancellation at various iteration points
  - Verify resource cleanup on cancellation

### Phase 5: Lateral Join Completion

- [ ] 5.1 Complete LATERAL join type support
  - Verify INNER LATERAL works correctly
  - Implement LEFT LATERAL with proper NULL handling
  - Implement RIGHT LATERAL (uncommon, but spec-required)
  - Implement FULL LATERAL with NULL rows
  - Test all join types with 0, 1, and multiple result rows

- [ ] 5.2 Enhance correlated column resolution
  - Verify all outer columns are properly bound
  - Test with multiple outer tables
  - Test with aggregates in outer context
  - Test with outer columns in ORDER BY within LATERAL subquery
  - Add tests from spec scenarios

- [ ] 5.3 Add type coercion in LATERAL joins
  - Implement automatic type promotion for result columns
  - Handle INTEGER + FLOAT → FLOAT
  - Handle STRING + VARCHAR → VARCHAR
  - Handle NULL type propagation
  - Test all type combinations

- [ ] 5.4 Implement LATERAL join cost estimation
  - Create `LateralJoinCostEstimator`
  - Estimate per-row evaluation cost
  - Detect uncorrelated subqueries and suggest optimization
  - Add cost estimates to EXPLAIN output
  - Test plan selection for various scenarios

- [ ] 5.5 Implement error handling for LATERAL
  - Detect forward references (inner → outer)
  - Detect circular LATERAL references
  - Detect ambiguous column names
  - Detect non-existent outer columns
  - Provide clear error messages for each case
  - Test all error scenarios

- [ ] 5.6 Integration tests for LATERAL joins
  - Test all join types with various cardinalities
  - Test with selective outer table (1%, 50%, 100% match)
  - Test type coercion scenarios
  - Test error conditions
  - Performance test: 100K rows with expensive subquery

### Phase 6: DuckDB Compatibility and Integration

- [ ] 6.1 Cross-validate against DuckDB reference
  - For each test scenario in specs, run against both dukdb-go and DuckDB v1.4.3
  - Compare result row counts, column values, types
  - Document any differences and rationale
  - Test edge cases: empty results, NULL values, large datasets

- [ ] 6.2 Add comprehensive integration tests
  - Combine recursive CTEs with LATERAL joins
  - Test CTEs using LATERAL subqueries
  - Test LATERAL subqueries containing CTEs
  - Test performance on realistic workloads

- [ ] 6.3 Update documentation and examples
  - Document USING KEY syntax and performance benefits
  - Document MAX_RECURSION usage
  - Document LATERAL join patterns
  - Add performance tuning guide

- [ ] 6.4 Final validation
  - Run full test suite: `go test ./...`
  - Run linter: `golangci-lint run`
  - Run benchmarks on common patterns
  - Verify no memory leaks or goroutine leaks
  - Test on multiple platforms (linux, darwin, windows)

## Dependency Graph

```
Phase 1 (Cycle Detection)
  ↓
Phase 2 (MAX_RECURSION) - depends on Phase 1
  ↓
Phase 3 (Memory Pooling) - depends on Phase 2
  ↓
Phase 4 (Streaming) - depends on Phase 3
  ↓
Phase 5 (LATERAL Joins) - independent, can run in parallel with Phases 1-4
  ↓
Phase 6 (Integration & Validation) - depends on Phases 1-5
```

## Estimated Scope

- **New code**: ~2000-2500 lines (CycleDetector, WorkTablePool, cost estimator, error handling)
- **Modified code**: ~1000-1500 lines (PhysicalRecursiveCTE, PhysicalLateralJoin, parser)
- **Tests**: ~2000-2500 lines (comprehensive test coverage per spec scenarios)
- **Total**: ~5500-6500 lines of new/modified code and tests

## Validation Checklist

- [ ] All spec scenarios pass integration tests
- [ ] All error scenarios handled with correct error messages
- [ ] Memory usage scales linearly (not exponential)
- [ ] Performance meets targets (sub-second for typical queries)
- [ ] DuckDB compatibility validated for all test cases
- [ ] No memory leaks or goroutine leaks
- [ ] All linting passes
- [ ] Code coverage > 80% for new code
