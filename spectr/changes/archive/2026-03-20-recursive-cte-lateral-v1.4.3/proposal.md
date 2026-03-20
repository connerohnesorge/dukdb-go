# Change Proposal: Recursive CTEs and Lateral Joins for DuckDB v1.4.3 Compatibility

**Change ID:** `recursive-cte-lateral-v1.4.3`
**Status:** Proposed
**Target Version:** v1.4.3
**Priority:** High
**Estimated Effort:** 3-4 sprints

## Executive Summary

This proposal outlines the implementation of two critical SQL features for dukdb-go to achieve DuckDB v1.4.3 compatibility: Recursive Common Table Expressions (CTEs) and Lateral Joins. These features enable sophisticated analytical queries including hierarchical data traversal, graph algorithms, and correlated subqueries that reference outer query columns during execution.

## Motivation

### Business Value

1. **DuckDB Compatibility**: These features are essential for full DuckDB v1.4.3 compatibility, enabling users to migrate existing DuckDB workloads to dukdb-go without modification.

2. **Advanced Analytics**: Recursive CTEs unlock powerful analytical capabilities:
   - Hierarchical data analysis (organizational charts, bill of materials)
   - Graph traversal algorithms (shortest path, network analysis)
   - Iterative computations (convergence algorithms, recursive calculations)

3. **Performance Optimization**: The USING KEY optimization for recursive CTEs provides substantial performance improvements (up to 100x) for graph traversal queries by avoiding redundant path exploration.

4. **SQL Standard Compliance**: Both features are part of SQL:1999 standard and widely adopted by modern databases.

### Technical Requirements

- **Recursive CTEs**: Support for WITH RECURSIVE syntax, UNION ALL operations, cycle detection, and the USING KEY optimization
- **Lateral Joins**: Implementation of LATERAL keyword for row-by-row subquery evaluation
- **Performance**: Sub-second response times for recursive queries on datasets up to 1M nodes
- **Memory Efficiency**: Bounded memory usage even for deep recursion (1000+ levels)
- **Integration**: Seamless integration with existing query planner and executor

## Scope

### In Scope

1. **Recursive CTE Implementation**
   - WITH RECURSIVE syntax parsing and validation
   - Recursive execution engine with work table management
   - UNION ALL support for combining recursive members
   - Cycle detection and termination
   - USING KEY optimization for graph algorithms
   - Memory-efficient recursion handling

2. **Lateral Join Implementation**
   - LATERAL keyword support in FROM clause
   - Row-by-row subquery evaluation
   - Integration with existing join operators
   - Correlated subquery optimization

3. **Performance Optimizations**
   - Hash-based USING KEY dictionary for graph traversal
   - Streaming execution for large result sets
   - Memory pooling for recursive work tables
   - Early termination for cycle detection

### Out of Scope

1. **Advanced Features**
   - Recursive CTEs with DISTINCT (non-standard)
   - Mutually recursive CTEs (multiple recursive references)
   - Recursive views
   - Lateral joins with window functions

2. **Performance Enhancements**
   - Parallel recursive execution
   - GPU acceleration for graph algorithms
   - Persistent caching of recursive results

## Success Criteria

1. **Functionality**: All documented examples execute correctly and produce expected results
2. **Performance**:
   - Basic recursive queries: <100ms for 10-level recursion
   - Graph traversal with USING KEY: <1s for 1M node graphs
   - Lateral joins: <10ms per correlated subquery
3. **Memory Usage**: <1GB for 1000-level recursion
4. **Compatibility**: 100% DuckDB v1.4.3 syntax and behavior compatibility
5. **Test Coverage**: >95% code coverage for new functionality

## Risk Assessment

### High Risk
1. **Memory Management**: Deep recursion could exhaust memory without proper bounds
2. **Performance**: Naive implementation could be 10-100x slower than DuckDB
3. **Complexity**: Recursive query optimization is notoriously difficult

### Medium Risk
1. **Integration**: Changes to planner may affect existing query plans
2. **Cycle Detection**: Incorrect detection could cause infinite loops

### Low Risk
1. **Syntax**: Well-defined SQL standard syntax
2. **Testing**: Comprehensive test suite available from DuckDB

## Dependencies

### Internal Dependencies
- Query planner modifications for recursive CTE handling
- Executor framework extension for iterative execution
- Memory management system for work tables
- Catalog support for CTE registration

### External Dependencies
- None (pure Go implementation required)

## Implementation Approach

### Phase 1: Foundation (Sprint 1)
- Parser extensions for RECURSIVE and LATERAL keywords
- AST node definitions for recursive CTEs and lateral joins
- Basic planner integration

### Phase 2: Recursive CTE Core (Sprint 2)
- Recursive execution engine implementation
- Work table management
- UNION ALL support
- Basic cycle detection

### Phase 3: USING KEY Optimization (Sprint 3)
- Hash dictionary implementation
- Graph algorithm optimization
- Performance tuning
- Memory management improvements

### Phase 4: Lateral Joins (Sprint 4)
- Lateral join executor
- Correlated subquery handling
- Performance optimization
- Final integration testing

## Resources

### Development Team
- 2 Senior Engineers (query engine expertise)
- 1 Performance Engineer
- 1 QA Engineer

### Timeline
- Total: 12 weeks across 4 sprints
- Design review: Week 1
- Implementation: Weeks 2-10
- Performance tuning: Weeks 9-11
- Final testing: Week 12

## Conclusion

Implementing recursive CTEs and lateral joins is essential for dukdb-go to achieve full DuckDB v1.4.3 compatibility. These features enable sophisticated analytical workloads while the USING KEY optimization provides competitive performance for graph algorithms. The implementation requires careful attention to memory management and performance optimization but delivers significant value to users requiring advanced SQL capabilities.

## Approval

| Role | Name | Approval Date |
|------|------|---------------|
| Tech Lead | ________________ | ___________ |
| Product Manager | ________________ | ___________ |
| Engineering Director | ________________ | ___________ |