# Change: Complete Recursive CTEs and Lateral Joins with DuckDB v1.4.3 Compatibility

## Why

dukdb-go has partial implementations of Recursive CTEs and Lateral Joins in the executor and planner, but they lack critical features required for full DuckDB v1.4.3 compatibility:

1. **Incomplete Recursive CTE Features**: Missing cycle detection (USING KEY), advanced recursion control (MAX_RECURSION), and proper handling of complex recursive patterns with JOINs
2. **Memory Efficiency Gap**: No bounded memory strategy for deep recursion (1000+ levels), risk of unbounded memory growth on large datasets
3. **Lateral Join Limitations**: Partial implementation in executor; may lack full correlated subquery evaluation and optimization
4. **Specification Gaps**: Parser and execution specs exist but advanced features not implemented

This proposal completes these implementations and adds critical optimizations for production use.

## What Changes

### 1. Recursive CTE Enhancements
- **Cycle Detection & USING KEY Optimization**: Implement cycle tracking using row identity hashing for graph algorithms
- **MAX_RECURSION Support**: Add hard recursion limits to prevent infinite loops
- **Complex Recursive Patterns**: Handle recursive CTEs with multiple UNION ALL parts and JOINs
- **Memory Pooling**: Implement work table memory reuse for bounded memory usage
- **Streaming Results**: Support streaming output during recursion

### 2. Lateral Join Completion
- **Full Correlated Subquery Support**: Ensure all outer columns are properly bound in subquery context
- **All Join Types**: Complete support for INNER/LEFT/RIGHT/FULL/CROSS LATERAL joins
- **Performance Optimization**: Add cost-based planning hints for when to use LATERAL vs correlated subqueries
- **Error Handling**: Proper error messages for invalid lateral specifications

### 3. Integration & Testing
- **Parser Updates**: Ensure USING KEY clause parsing is complete
- **Planner Integration**: Verify LATERAL join planning handles all join types correctly
- **Executor Completeness**: Test all spec scenarios defined in execution-engine/spec.md
- **DuckDB Compatibility Tests**: Cross-validate behavior with reference DuckDB v1.4.3

## Impact

- **Affected specs**:
  - `parser/spec.md` (already defined recursive CTE and LATERAL parsing)
  - `execution-engine/spec.md` (recursive CTE and LATERAL execution requirements)
  - NEW: `recursive-cte/spec.md` (advanced CTE features)
  - NEW: `lateral-joins/spec.md` (complete LATERAL join spec)

- **Affected code**:
  - `internal/executor/physical_recursive_cte.go` (enhance recursive execution)
  - `internal/executor/physical_lateral.go` (complete lateral join execution)
  - `internal/planner/physical.go` (ensure proper plan construction)
  - `internal/parser/ast.go` and parser files (cycle detection syntax)

- **New components**:
  - Memory pooling for recursive work tables
  - Hash-based cycle detector for USING KEY
  - Lateral join cost estimator

## Timeline

- Phase 1: Cycle detection and USING KEY optimization
- Phase 2: Recursive CTE memory pooling and advanced patterns
- Phase 3: Lateral join completion and optimization
- Phase 4: Integration testing and DuckDB compatibility validation
