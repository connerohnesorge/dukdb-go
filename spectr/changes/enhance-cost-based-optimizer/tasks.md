## 1. Research & Design

- [ ] 1.1 Study DuckDB v1.4.3 statistics format (references/duckdb/src/storage/statistics/)
- [ ] 1.2 Study DuckDB auto-update trigger logic (references/duckdb/src/optimizer/)
- [ ] 1.3 Study DuckDB subquery decorrelation (references/duckdb/src/optimizer/unnest_rewriter.cpp)
- [ ] 1.4 Study DuckDB filter pushdown (references/duckdb/src/optimizer/filter_pushdown.cpp)
- [ ] 1.5 Study DuckDB multi-column statistics heuristics
- [ ] 1.6 Research DuckDB cardinality learning (if exists in v1.4.3)
- [ ] 1.7 Create DuckDB v1.4.3 feature completeness checklist
- [ ] 1.8 Document all edge cases and NULL handling behaviors

## 2. Statistics Persistence (DuckDB Binary Format)

**DuckDB Reference**: references/duckdb/src/storage/statistics/

- [ ] 2.1 Reverse engineer DuckDB binary statistics format from source
- [ ] 2.2 Implement serialization matching DuckDB format (with inline docs)
- [ ] 2.3 Implement deserialization matching DuckDB format (with inline docs)
- [ ] 2.4 Add version checking for format compatibility
- [ ] 2.5 Integrate with catalog for statistics storage
- [ ] 2.6 Handle statistics migration from older versions
- [ ] 2.7 Create test databases with DuckDB CLI for validation
- [ ] 2.8 Unit test: save/load roundtrip matches DuckDB
- [ ] 2.9 Validation: Load statistics created by DuckDB CLI
- [ ] 2.10 Validation: DuckDB loads statistics created by dukdb-go

## 3. Auto-Update Statistics (Match DuckDB Behavior)

**DuckDB Reference**: Research exact behavior from DuckDB source

- [ ] 3.1 Implement ModificationTracker to track DML operations
- [ ] 3.2 Match DuckDB auto-update threshold behavior (no custom config)
- [ ] 3.3 Implement batching to prevent excessive ANALYZE calls
- [ ] 3.4 Implement incremental ANALYZE for large tables
- [ ] 3.5 Add inline documentation referencing DuckDB behavior
- [ ] 3.6 Unit test: modification tracking accuracy
- [ ] 3.7 Integration test: auto-update triggers at correct threshold
- [ ] 3.8 Validation: Compare with DuckDB auto-update behavior
- [ ] 3.9 Performance test: auto-update overhead measurement

## 4. Subquery Decorrelation (Full DuckDB v1.4.3 Parity)

**DuckDB Reference**: references/duckdb/src/optimizer/unnest_rewriter.cpp

### Implementation
- [ ] 4.1 Implement findCorrelatedColumns() with inline docs
- [ ] 4.2 Implement FlattenDependentJoin() algorithm with inline docs
- [ ] 4.3 EXISTS correlated subqueries (with example transformation in docs)
- [ ] 4.4 NOT EXISTS correlated subqueries
- [ ] 4.5 SCALAR correlated subqueries (with NULL handling docs)
- [ ] 4.6 IN correlated subqueries
- [ ] 4.7 NOT IN correlated subqueries (with NULL semantics)
- [ ] 4.8 ANY/ALL correlated subqueries
- [ ] 4.9 Multi-level correlation (outer -> middle -> inner)
- [ ] 4.10 LATERAL join support
- [ ] 4.11 Correlated CTEs
- [ ] 4.12 Recursive CTEs with correlation (if DuckDB v1.4.3 supports)
- [ ] 4.13 Mixed correlation patterns (multiple outer references)

### Testing (Triple Validation)
- [ ] 4.14 Correctness: EXISTS subquery results match DuckDB
- [ ] 4.15 Correctness: NOT EXISTS subquery results match DuckDB
- [ ] 4.16 Correctness: SCALAR subquery results match DuckDB
- [ ] 4.17 Correctness: IN subquery results match DuckDB
- [ ] 4.18 Correctness: NOT IN subquery results match DuckDB
- [ ] 4.19 Correctness: ANY/ALL subquery results match DuckDB
- [ ] 4.20 Correctness: Multi-level correlation results match DuckDB
- [ ] 4.21 Correctness: LATERAL join results match DuckDB
- [ ] 4.22 EXPLAIN comparison: EXISTS decorrelation matches DuckDB plan
- [ ] 4.23 EXPLAIN comparison: SCALAR decorrelation matches DuckDB plan
- [ ] 4.24 EXPLAIN comparison: IN decorrelation matches DuckDB plan
- [ ] 4.25 Cardinality estimates: Subquery estimates within 2x of DuckDB
- [ ] 4.26 Edge case: Empty subquery results
- [ ] 4.27 Edge case: NULL handling in correlated conditions
- [ ] 4.28 Edge case: Subquery returning multiple rows for SCALAR
- [ ] 4.29 Performance: TPC-H queries with subqueries match DuckDB

## 5. Predicate Pushdown (Match DuckDB Filter Pushdown)

**DuckDB Reference**: references/duckdb/src/optimizer/filter_pushdown.cpp

### Implementation
- [ ] 5.1 Study DuckDB filter pushdown algorithm
- [ ] 5.2 Implement filter pushdown to table scans (with inline docs)
- [ ] 5.3 Implement filter pushdown past inner joins
- [ ] 5.4 Preserve filters for outer joins (left/right/full)
- [ ] 5.5 Handle complex AND/OR filter trees
- [ ] 5.6 Respect filter dependencies on join columns
- [ ] 5.7 Push filters into subqueries when safe
- [ ] 5.8 Maintain predicate equivalence (with correctness proof in docs)

### Testing (Triple Validation)
- [ ] 5.9 Correctness: Pushdown to scan produces same results as DuckDB
- [ ] 5.10 Correctness: Pushdown past join produces same results
- [ ] 5.11 Correctness: Outer join filter placement matches DuckDB
- [ ] 5.12 EXPLAIN comparison: Filter placement matches DuckDB
- [ ] 5.13 EXPLAIN comparison: Complex AND/OR handled like DuckDB
- [ ] 5.14 Cardinality estimates: Post-pushdown estimates match DuckDB
- [ ] 5.15 Performance: Pushdown reduces execution time like DuckDB
- [ ] 5.16 Edge case: Filters with function calls
- [ ] 5.17 Edge case: Filters with subqueries

## 6. Multi-Column Statistics (Match DuckDB Heuristics)

**DuckDB Reference**: references/duckdb/src/storage/statistics/distinct_statistics.cpp

### Implementation
- [ ] 6.1 Study DuckDB multi-column statistics heuristics
- [ ] 6.2 Implement joint NDV collection for column pairs
- [ ] 6.3 Implement HyperLogLog (if DuckDB uses it)
- [ ] 6.4 Detect correlated columns during ANALYZE
- [ ] 6.5 Match DuckDB heuristics for which column pairs to track
- [ ] 6.6 Integrate with cardinality estimation
- [ ] 6.7 Add inline documentation with algorithm explanation

### Testing (Triple Validation)
- [ ] 6.8 Correctness: ANALYZE collects same statistics as DuckDB
- [ ] 6.9 EXPLAIN comparison: Correlated predicate estimates match DuckDB
- [ ] 6.10 Cardinality estimates: Multi-column NDV within 2x of DuckDB
- [ ] 6.11 Performance: Multi-column stats improve plan quality
- [ ] 6.12 Edge case: Highly correlated columns (r > 0.9)
- [ ] 6.13 Edge case: Independent columns (r ≈ 0)

## 7. Cardinality Learning (Conservative Approach)

**DuckDB Reference**: Research if DuckDB has similar features

### Implementation
- [ ] 7.1 Research DuckDB adaptive optimizer (if exists)
- [ ] 7.2 Implement CardinalityLearner with estimate tracking
- [ ] 7.3 Track actual vs estimated cardinalities per operator
- [ ] 7.4 Implement N-observation threshold (conservative)
- [ ] 7.5 Compute adaptive correction multipliers
- [ ] 7.6 Integrate with cost model for adaptive costing
- [ ] 7.7 Implement bounded memory (evict old corrections)
- [ ] 7.8 Add inline documentation with algorithm explanation

### Testing
- [ ] 7.9 Unit test: Correction calculation accuracy
- [ ] 7.10 Integration test: Corrections improve estimates over time
- [ ] 7.11 Performance: Learning doesn't add significant overhead
- [ ] 7.12 Edge case: Outlier queries don't corrupt learning
- [ ] 7.13 Edge case: Memory bounds respected under load

## 8. Integration

- [ ] 8.1 Integrate statistics persistence with storage layer
- [ ] 8.2 Integrate decorrelation with binder/planner
- [ ] 8.3 Integrate pushdown with optimizer pipeline
- [ ] 8.4 Integrate learning with cost model
- [ ] 8.5 Ensure all phases work together correctly
- [ ] 8.6 Full integration test suite

## 9. Testing (60% of Total Effort)

### Triple Validation for All Features
- [ ] 9.1 Create comprehensive DuckDB test database suite
- [ ] 9.2 Correctness: All queries produce same results as DuckDB
- [ ] 9.3 EXPLAIN: All query plans match DuckDB structure
- [ ] 9.4 Cardinality: All estimates within 2x of DuckDB
- [ ] 9.5 TPC-H: All queries within 10-20% of DuckDB performance
- [ ] 9.6 TPC-H: No query >2x slower than DuckDB
- [ ] 9.7 Edge case testing for all subquery types
- [ ] 9.8 Edge case testing for all filter pushdown scenarios
- [ ] 9.9 Edge case testing for statistics persistence
- [ ] 9.10 Stress test: Large databases (GB scale)
- [ ] 9.11 Stress test: Wide tables (100+ columns)
- [ ] 9.12 Stress test: Deep correlation (3+ levels)

### Test Infrastructure
- [ ] 9.13 Create test database generator using DuckDB CLI
- [ ] 9.14 Create EXPLAIN comparison tool
- [ ] 9.15 Create cardinality estimate comparison tool
- [ ] 9.16 Create TPC-H benchmark runner
- [ ] 9.17 Set up automated testing pipeline

## 10. Documentation (Inline Only)

- [ ] 10.1 Add DuckDB reference comments to all complex functions
- [ ] 10.2 Add algorithm explanation comments
- [ ] 10.3 Add example transformation comments for decorrelation
- [ ] 10.4 Add edge case and NULL handling comments
- [ ] 10.5 Add cardinality impact comments
- [ ] 10.6 Review: Every complex function has complete inline docs

## 11. Final Validation (Before Merge)

### DuckDB v1.4.3 Feature Completeness Checklist
- [ ] 11.1 Statistics persistence: DuckDB binary format compatibility
- [ ] 11.2 Auto-update: Matches DuckDB threshold and batching
- [ ] 11.3 Subquery: All types from checklist implemented
- [ ] 11.4 Subquery: Multi-level correlation works
- [ ] 11.5 Subquery: LATERAL joins work
- [ ] 11.6 Filter pushdown: All features from checklist
- [ ] 11.7 Multi-column stats: Matches DuckDB heuristics
- [ ] 11.8 Cardinality learning: N-observation threshold works

### Triple Validation Final Check
- [ ] 11.9 Correctness: 100% of test queries match DuckDB results
- [ ] 11.10 EXPLAIN: 100% of plans structurally match DuckDB
- [ ] 11.11 Cardinality: 95%+ of estimates within 2x of DuckDB
- [ ] 11.12 TPC-H: Average performance within 10-20% of DuckDB
- [ ] 11.13 TPC-H: No query >2x slower than DuckDB

### Code Quality
- [ ] 11.14 Every complex function has DuckDB reference comment
- [ ] 11.15 Every decorrelation has example transformation comment
- [ ] 11.16 Every edge case is documented
- [ ] 11.17 Code review by peer familiar with DuckDB
- [ ] 11.18 No partial/lazy implementations (all features complete)

### Performance Validation
- [ ] 11.19 Run full TPC-H benchmark suite
- [ ] 11.20 Profile optimization overhead (should be <5% for simple queries)
- [ ] 11.21 Memory usage profiling (statistics + learning)
- [ ] 11.22 Ensure no performance regressions on existing tests

### Final Steps
- [ ] 11.23 Run `spectr validate enhance-cost-based-optimizer`
- [ ] 11.24 Run all DuckDB compatibility tests
- [ ] 11.25 Ensure all existing tests pass
- [ ] 11.26 Final peer review
- [ ] 11.27 Merge as complete optimizer overhaul
