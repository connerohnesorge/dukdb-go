# Tasks: Cost-Based Query Optimization

## Phase 1: Statistics Infrastructure

- [x] 1.1 Create internal/optimizer/ package with statistics.go
- [x] 1.2 Implement TableStatistics struct with row count, page count, data size
- [x] 1.3 Implement ColumnStatistics struct with null fraction, distinct count, min/max
- [x] 1.4 Implement Histogram struct with equi-depth bucket representation
- [x] 1.5 Add Statistics field to catalog.TableDef
- [x] 1.6 Implement StatisticsManager for statistics access with defaults

## Phase 2: ANALYZE Command

- [x] 2.1 Parse ANALYZE statement in parser (already exists as LogicalAnalyze)
- [x] 2.2 Implement full-scan statistics collection for small tables
- [x] 2.3 Implement sample-based statistics collection for large tables (>100K rows)
- [x] 2.4 Implement histogram construction with configurable bucket count
- [x] 2.5 Integrate ANALYZE execution with catalog persistence
- [x] 2.6 Add tests for ANALYZE with various data distributions

## Phase 3: Cardinality Estimation

- [x] 3.1 Create cardinality.go with CardinalityEstimator
- [x] 3.2 Implement base table cardinality from TableStatistics
- [x] 3.3 Implement equality predicate selectivity (1/distinct)
- [x] 3.4 Implement range predicate selectivity using histograms
- [x] 3.5 Implement NULL predicate selectivity from null fraction
- [x] 3.6 Implement LIKE predicate selectivity heuristics
- [x] 3.7 Implement compound predicate selectivity (AND, OR, NOT)
- [x] 3.8 Implement join cardinality estimation
- [x] 3.9 Implement aggregate cardinality estimation (GROUP BY)
- [x] 3.10 Add tests comparing estimates to actual counts

## Phase 4: Cost Model

- [x] 4.1 Create cost_model.go with CostModel and PlanCost
- [x] 4.2 Define configurable cost constants (SeqPageCost, RandomPageCost, etc.)
- [x] 4.3 Implement cost estimation for PhysicalScan (sequential)
- [x] 4.4 Implement cost estimation for PhysicalFilter
- [x] 4.5 Implement cost estimation for PhysicalProject
- [x] 4.6 Implement cost estimation for PhysicalHashJoin
- [x] 4.7 Implement cost estimation for PhysicalNestedLoopJoin
- [x] 4.8 Implement cost estimation for PhysicalSort
- [x] 4.9 Implement cost estimation for PhysicalHashAggregate
- [x] 4.10 Implement cumulative cost calculation for plan trees
- [x] 4.11 Add tests for cost model accuracy

## Phase 5: Join Order Optimization

- [x] 5.1 Create join_order.go with JoinOrderOptimizer
- [x] 5.2 Extract join predicates from logical plan
- [x] 5.3 Build join graph representing table relationships
- [x] 5.4 Implement DPccp algorithm for N <= 12 tables
- [x] 5.5 Implement greedy algorithm for N > 12 tables
- [x] 5.6 Implement build side selection for hash joins
- [x] 5.7 Handle outer join reordering constraints
- [x] 5.8 Add comprehensive join ordering tests

## Phase 6: Physical Plan Selection

- [x] 6.1 Create plan_enumerator.go for physical plan alternatives
- [x] 6.2 Implement hash join vs nested loop selection
- [x] 6.3 Implement index scan consideration (when indexes exist)
- [x] 6.4 Implement sort-merge join for sorted inputs
- [x] 6.5 Select cheapest physical plan from alternatives

## Phase 7: Optimizer Integration

- [x] 7.1 Create optimizer.go with main Optimizer interface
- [x] 7.2 Implement fast path for simple queries (no joins)
- [x] 7.3 Integrate optimizer into Engine.executeQuery
- [x] 7.4 Pass optimization hints to physical planner
- [x] 7.5 Add optimizer bypass configuration option

## Phase 8: EXPLAIN Integration

- [x] 8.1 Add cost annotations to EXPLAIN output format
- [x] 8.2 Show estimated rows and width in EXPLAIN
- [x] 8.3 Implement EXPLAIN ANALYZE actual vs estimated comparison
- [x] 8.4 Add tests for EXPLAIN with costs

## Phase 9: Testing and Validation

- [x] 9.1 Create TPC-H subset benchmark queries
- [x] 9.2 Measure query plan improvement on TPC-H
- [x] 9.3 Verify optimizer overhead < 5% on simple queries
- [x] 9.4 Run full regression test suite
- [x] 9.5 Add documentation for optimizer configuration
