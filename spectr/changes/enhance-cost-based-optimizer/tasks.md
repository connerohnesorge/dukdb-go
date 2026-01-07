## 1. Research & Design

- [ ] 1.1 Research DuckDB v1.4.3 statistics and optimizer implementation
- [ ] 1.2 Design statistics persistence format (catalog metadata integration)
- [ ] 1.3 Design auto-update trigger logic and thresholds
- [ ] 1.4 Design subquery decorrelation algorithm (Flat

tentDependentJoin)
- [ ] 1.5 Design predicate pushdown strategy
- [ ] 1.6 Design multi-column statistics structure
- [ ] 1.7 Design cardinality learning algorithm
- [ ] 1.8 Create detailed implementation spec

## 2. Statistics Persistence

- [ ] 2.1 Design persistent statistics data structure
- [ ] 2.2 Implement SavePersistentStats() function
- [ ] 2.3 Implement LoadPersistentStats() function
- [ ] 2.4 Integrate with catalog for statistics storage
- [ ] 2.5 Handle statistics migration from older versions
- [ ] 2.6 Write unit tests for persistence
- [ ] 2.7 Test with existing databases (backward compatibility)

## 3. Auto-Update Statistics

- [ ] 3.1 Implement ModificationTracker to track DML operations
- [ ] 3.2 Implement auto-update trigger at threshold (10% change)
- [ ] 3.3 Implement incremental ANALYZE for large tables
- [ ] 3.4 Add configuration options (threshold, enable/disable)
- [ ] 3.5 Write integration tests for auto-update
- [ ] 3.6 Test performance impact of auto-update

## 4. Subquery Decorrelation

- [ ] 4.1 Implement findCorrelatedColumns() for subquery analysis
- [ ] 4.2 Implement FlattenDependentJoin() algorithm
- [ ] 4.3 Handle EXISTS correlated subqueries
- [ ] 4.4 Handle SCALAR correlated subqueries
- [ ] 4.5 Handle ANY/ALL correlated subqueries
- [ ] 4.6 Handle IN correlated subqueries
- [ ] 4.7 Implement LATERAL join support
- [ ] 4.8 Write unit tests for decorrelation
- [ ] 4.9 Test with correlated subquery queries from references/

## 5. Predicate Pushdown

- [ ] 5.1 Analyze filter pushdown opportunities
- [ ] 5.2 Implement filter pushdown into table scans
- [ ] 5.3 Implement filter pushdown past joins
- [ ] 5.4 Handle complex predicate structures (AND/OR trees)
- [ ] 5.5 Integrate with planner for pushdown decisions
- [ ] 5.6 Write unit tests for pushdown
- [ ] 5.7 Benchmark pushdown effectiveness

## 6. Multi-Column Statistics

- [ ] 6.1 Design multi-column statistics structure
- [ ] 6.2 Implement collection of joint NDV
- [ ] 6.3 Implement cross-predicate selectivity estimation
- [ ] 6.4 Handle predicate correlation in estimates
- [ ] 6.5 Integrate with cardinality estimation
- [ ] 6.6 Write unit tests
- [ ] 6.7 Test with correlated predicates

## 7. Cardinality Learning

- [ ] 7.1 Implement CardinalityLearner with estimate tracking
- [ ] 7.2 Track actual vs estimated cardinalities
- [ ] 7.3 Implement adaptive correction calculation
- [ ] 7.4 Integrate with cost model for adaptive costing
- [ ] 7.5 Add configuration (history size, enable/disable)
- [ ] 7.6 Write unit tests
- [ ] 7.7 Benchmark learning effectiveness

## 8. Integration

- [ ] 8.1 Integrate statistics persistence with storage layer
- [ ] 8.2 Integrate decorrelation with binder/planner
- [ ] 8.3 Integrate pushdown with optimizer
- [ ] 8.4 Integrate learning with cost model
- [ ] 8.5 Update optimizer configuration options
- [ ] 8.6 Write integration tests

## 9. Testing

- [ ] 9.1 Write unit tests for all new components
- [ ] 9.2 Create integration tests for end-to-end optimization
- [ ] 9.3 Test with TPC-H queries for performance
- [ ] 9.4 Test EXPLAIN ANALYZE for cardinality accuracy
- [ ] 9.5 Test subquery performance improvements
- [ ] 9.6 Test auto-update behavior under load
- [ ] 9.7 Test cardinality learning convergence

## 10. Documentation

- [ ] 10.1 Document ANALYZE command and statistics
- [ ] 10.2 Document auto-update behavior and configuration
- [ ] 10.3 Document subquery decorrelation
- [ ] 10.4 Document predicate pushdown
- [ ] 10.5 Document cardinality learning
- [ ] 10.6 Add examples for configuration options

## 11. Verification

- [ ] 11.1 Run `spectr validate enhance-cost-based-optimizer`
- [ ] 11.2 Run DuckDB compatibility tests
- [ ] 11.3 TPC-H benchmark comparison (before/after)
- [ ] 11.4 Ensure all existing tests pass
- [ ] 11.5 Performance profiling for optimization overhead
