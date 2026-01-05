# Tasks: Index Usage in Query Plans

## Phase 1: Index Scan Infrastructure

- [ ] 1.1 Create internal/executor/index_scan.go with PhysicalIndexScan struct
- [ ] 1.2 Implement Execute() method for PhysicalIndexScan
- [ ] 1.3 Wire PhysicalIndexScan to HashIndex.Lookup() for key lookups
- [ ] 1.4 Implement RowID to tuple resolution from storage layer
- [ ] 1.5 Add PhysicalIndexScan to physical plan node types
- [ ] 1.6 Add unit tests for index scan execution

## Phase 2: Index Scan Operator

- [ ] 2.1 Create internal/optimizer/index_matcher.go
- [ ] 2.2 Implement IndexMatcher.FindApplicableIndexes() for single-column indexes
- [ ] 2.3 Implement findEqualityPredicate() to extract column = value predicates
- [ ] 2.4 Implement IndexMatch struct with index, predicates, lookup keys, selectivity
- [ ] 2.5 Add unit tests for index matching on equality predicates
- [ ] 2.6 Test index matching with IN clauses (multiple equality values)

## Phase 3: Cost Model Integration

- [ ] 3.1 Add IndexLookupCost and IndexTupleCost constants to CostModel
- [ ] 3.2 Implement EstimateIndexScanCost() method
- [ ] 3.3 Implement index scan vs seq scan cost comparison logic
- [ ] 3.4 Add selectivity estimation for indexed equality predicates
- [ ] 3.5 Add unit tests for index scan cost estimation
- [ ] 3.6 Add tests verifying correct access method selection by cost

## Phase 4: Index-Only Scan

- [ ] 4.1 Implement IsCoveringIndex() function
- [ ] 4.2 Implement GetRequiredColumns() to extract columns needed by query
- [ ] 4.3 Add IsIndexOnly flag to PhysicalIndexScan
- [ ] 4.4 Implement index-only scan execution path (RowID filtering)
- [ ] 4.5 Add unit tests for covering index detection
- [ ] 4.6 Add integration tests for index-only scan execution

## Phase 5: Optimizer Integration

- [ ] 5.1 Add IndexMatcher to CostBasedOptimizer struct
- [ ] 5.2 Implement enumerateAccessMethods() to generate IndexScan alternatives
- [ ] 5.3 Implement selectBestAccessMethod() to pick cheapest access method
- [ ] 5.4 Modify physical planner to accept optimizer access method hints
- [ ] 5.5 Wire index scan generation into query execution pipeline
- [ ] 5.6 Add remaining filter application for partial index matches

## Phase 6: Composite Index Handling

- [ ] 6.1 Extend IndexMatcher for composite index prefix matching
- [ ] 6.2 Implement matchCompositeIndex() with prefix-only semantics
- [ ] 6.3 Handle partial matches (some but not all index columns)
- [ ] 6.4 Estimate selectivity for composite key matches
- [ ] 6.5 Add unit tests for composite index matching
- [ ] 6.6 Add tests for partial composite matches with residual filter

## Phase 7: Testing

- [ ] 7.1 Add integration tests for simple indexed lookups
- [ ] 7.2 Add tests verifying index not used when no matching predicate
- [ ] 7.3 Add tests for index-only scan scenarios
- [ ] 7.4 Add tests for composite index prefix scenarios
- [ ] 7.5 Add performance benchmarks comparing index scan vs seq scan
- [ ] 7.6 Add EXPLAIN output tests showing index usage
- [ ] 7.7 Run full regression test suite to verify no regressions
- [ ] 7.8 Add documentation for index usage behavior
