# Tasks: Cost-Based Query Optimization

## Phase 1: Statistics Infrastructure

- [ ] 1.1 Create internal/optimizer/ package with statistics.go
- [ ] 1.2 Implement TableStatistics struct with row count, page count, data size
- [ ] 1.3 Implement ColumnStatistics struct with null fraction, distinct count, min/max
- [ ] 1.4 Implement Histogram struct with equi-depth bucket representation
- [ ] 1.5 Add Statistics field to catalog.TableDef
- [ ] 1.6 Implement StatisticsManager for statistics access with defaults

## Phase 2: ANALYZE Command

- [ ] 2.1 Parse ANALYZE statement in parser (already exists as LogicalAnalyze)
- [ ] 2.2 Implement full-scan statistics collection for small tables
- [ ] 2.3 Implement sample-based statistics collection for large tables (>100K rows)
- [ ] 2.4 Implement histogram construction with configurable bucket count
- [ ] 2.5 Integrate ANALYZE execution with catalog persistence
- [ ] 2.6 Add tests for ANALYZE with various data distributions

## Phase 3: Cardinality Estimation

- [ ] 3.1 Create cardinality.go with CardinalityEstimator
- [ ] 3.2 Implement base table cardinality from TableStatistics
- [ ] 3.3 Implement equality predicate selectivity (1/distinct)
- [ ] 3.4 Implement range predicate selectivity using histograms
- [ ] 3.5 Implement NULL predicate selectivity from null fraction
- [ ] 3.6 Implement LIKE predicate selectivity heuristics
- [ ] 3.7 Implement compound predicate selectivity (AND, OR, NOT)
- [ ] 3.8 Implement join cardinality estimation
- [ ] 3.9 Implement aggregate cardinality estimation (GROUP BY)
- [ ] 3.10 Add tests comparing estimates to actual counts

## Phase 4: Cost Model

- [ ] 4.1 Create cost_model.go with CostModel and PlanCost
- [ ] 4.2 Define configurable cost constants (SeqPageCost, RandomPageCost, etc.)
- [ ] 4.3 Implement cost estimation for PhysicalScan (sequential)
- [ ] 4.4 Implement cost estimation for PhysicalFilter
- [ ] 4.5 Implement cost estimation for PhysicalProject
- [ ] 4.6 Implement cost estimation for PhysicalHashJoin
- [ ] 4.7 Implement cost estimation for PhysicalNestedLoopJoin
- [ ] 4.8 Implement cost estimation for PhysicalSort
- [ ] 4.9 Implement cost estimation for PhysicalHashAggregate
- [ ] 4.10 Implement cumulative cost calculation for plan trees
- [ ] 4.11 Add tests for cost model accuracy

## Phase 5: Join Order Optimization

- [ ] 5.1 Create join_order.go with JoinOrderOptimizer
- [ ] 5.2 Extract join predicates from logical plan
- [ ] 5.3 Build join graph representing table relationships
- [ ] 5.4 Implement DPccp algorithm for N <= 12 tables
- [ ] 5.5 Implement greedy algorithm for N > 12 tables
- [ ] 5.6 Implement build side selection for hash joins
- [ ] 5.7 Handle outer join reordering constraints
- [ ] 5.8 Add comprehensive join ordering tests

## Phase 6: Physical Plan Selection

- [ ] 6.1 Create plan_enumerator.go for physical plan alternatives
- [ ] 6.2 Implement hash join vs nested loop selection
- [ ] 6.3 Implement index scan consideration (when indexes exist)
- [ ] 6.4 Implement sort-merge join for sorted inputs
- [ ] 6.5 Select cheapest physical plan from alternatives

## Phase 7: Optimizer Integration

- [ ] 7.1 Create optimizer.go with main Optimizer interface
- [ ] 7.2 Implement fast path for simple queries (no joins)
- [ ] 7.3 Integrate optimizer into Engine.executeQuery
- [ ] 7.4 Pass optimization hints to physical planner
- [ ] 7.5 Add optimizer bypass configuration option

## Phase 8: EXPLAIN Integration

- [ ] 8.1 Add cost annotations to EXPLAIN output format
- [ ] 8.2 Show estimated rows and width in EXPLAIN
- [ ] 8.3 Implement EXPLAIN ANALYZE actual vs estimated comparison
- [ ] 8.4 Add tests for EXPLAIN with costs

## Phase 9: Testing and Validation

- [ ] 9.1 Create TPC-H subset benchmark queries
- [ ] 9.2 Measure query plan improvement on TPC-H
- [ ] 9.3 Verify optimizer overhead < 5% on simple queries
- [ ] 9.4 Run full regression test suite
- [ ] 9.5 Add documentation for optimizer configuration
