## 1. Research & Design

- [x] 1.1 Study DuckDB v1.4.3 statistics format (references/duckdb/src/storage/statistics/)
- [x] 1.2 Study DuckDB auto-update trigger logic (references/duckdb/src/optimizer/)
- [x] 1.3 Study DuckDB subquery decorrelation (references/duckdb/src/optimizer/unnest_rewriter.cpp)
- [x] 1.4 Study DuckDB filter pushdown (references/duckdb/src/optimizer/filter_pushdown.cpp)
- [x] 1.5 Study DuckDB multi-column statistics heuristics
- [x] 1.6 Research DuckDB cardinality learning (if exists in v1.4.3)
- [x] 1.7 Create DuckDB v1.4.3 feature completeness checklist
- [x] 1.8 Document all edge cases and NULL handling behaviors

## 2. Statistics Persistence (DuckDB Binary Format)

**DuckDB Reference**: references/duckdb/src/storage/statistics/

- [x] 2.1 Reverse engineer DuckDB binary statistics format from source
- [x] 2.2 Implement serialization matching DuckDB format (with inline docs)
- [x] 2.3 Implement deserialization matching DuckDB format (with inline docs)
- [x] 2.4 Add version checking for format compatibility
- [x] 2.5 Integrate with catalog for statistics storage
- [x] 2.6 Handle statistics migration from older versions
- [x] 2.7 Create test databases with DuckDB CLI for validation
- [x] 2.8 Unit test: save/load roundtrip matches DuckDB
- [x] 2.9 Validation: Load statistics created by DuckDB CLI
- [x] 2.10 Validation: DuckDB loads statistics created by dukdb-go

## 3. Auto-Update Statistics (Match DuckDB Behavior)

**DuckDB Reference**: Research exact behavior from DuckDB source

- [x] 3.1 Implement ModificationTracker to track DML operations
- [x] 3.2 Match DuckDB auto-update threshold behavior (no custom config)
- [x] 3.3 Implement batching to prevent excessive ANALYZE calls
- [x] 3.4 Implement incremental ANALYZE for large tables
- [x] 3.5 Add inline documentation referencing DuckDB behavior
- [x] 3.6 Unit test: modification tracking accuracy
- [x] 3.7 Integration test: auto-update triggers at correct threshold
- [x] 3.8 Validation: Compare with DuckDB auto-update behavior
- [x] 3.9 Performance test: auto-update overhead measurement

## 4. Subquery Decorrelation (Full DuckDB v1.4.3 Parity)

**DuckDB Reference**: references/duckdb/src/optimizer/unnest_rewriter.cpp

### Implementation
- [x] 4.1 Implement findCorrelatedColumns() with inline docs
- [x] 4.2 Implement FlattenDependentJoin() algorithm with inline docs
- [x] 4.3 EXISTS correlated subqueries (with example transformation in docs)
- [x] 4.4 NOT EXISTS correlated subqueries
- [x] 4.5 SCALAR correlated subqueries (with NULL handling docs)
- [x] 4.6 IN correlated subqueries
- [x] 4.7 NOT IN correlated subqueries (with NULL semantics)
- [x] 4.8 ANY/ALL correlated subqueries
- [x] 4.9 Multi-level correlation (outer -> middle -> inner)
- [x] 4.10 LATERAL join support
- [x] 4.11 Correlated CTEs
- [x] 4.12 Recursive CTEs with correlation (if DuckDB v1.4.3 supports)
- [x] 4.13 Mixed correlation patterns (multiple outer references)

### Testing (Triple Validation)
- [x] 4.14 Correctness: EXISTS subquery results match DuckDB
- [x] 4.15 Correctness: NOT EXISTS subquery results match DuckDB
- [x] 4.16 Correctness: SCALAR subquery results match DuckDB
- [x] 4.17 Correctness: IN subquery results match DuckDB
- [x] 4.18 Correctness: NOT IN subquery results match DuckDB
- [x] 4.19 Correctness: ANY/ALL subquery results match DuckDB
- [x] 4.20 Correctness: Multi-level correlation results match DuckDB
- [x] 4.21 Correctness: LATERAL join results match DuckDB
- [x] 4.22 EXPLAIN comparison: EXISTS decorrelation matches DuckDB plan
- [x] 4.23 EXPLAIN comparison: SCALAR decorrelation matches DuckDB plan
- [x] 4.24 EXPLAIN comparison: IN decorrelation matches DuckDB plan
- [x] 4.25 Cardinality estimates: Subquery estimates within 2x of DuckDB
- [x] 4.26 Edge case: Empty subquery results
- [x] 4.27 Edge case: NULL handling in correlated conditions
- [x] 4.28 Edge case: Subquery returning multiple rows for SCALAR
- [x] 4.29 Performance: TPC-H queries with subqueries match DuckDB

## 5. Predicate Pushdown (Match DuckDB Filter Pushdown)

**DuckDB Reference**: references/duckdb/src/optimizer/filter_pushdown.cpp

### Implementation
- [x] 5.1 Study DuckDB filter pushdown algorithm
- [x] 5.2 Implement filter pushdown to table scans (with inline docs)
- [x] 5.3 Implement filter pushdown past inner joins
- [x] 5.4 Preserve filters for outer joins (left/right/full)
- [x] 5.5 Handle complex AND/OR filter trees
- [x] 5.6 Respect filter dependencies on join columns
- [x] 5.7 Push filters into subqueries when safe
- [x] 5.8 Maintain predicate equivalence (with correctness proof in docs)

### Testing (Triple Validation)
- [x] 5.9 Correctness: Pushdown to scan produces same results as DuckDB
- [x] 5.10 Correctness: Pushdown past join produces same results
- [x] 5.11 Correctness: Outer join filter placement matches DuckDB
- [x] 5.12 EXPLAIN comparison: Filter placement matches DuckDB
- [x] 5.13 EXPLAIN comparison: Complex AND/OR handled like DuckDB
- [x] 5.14 Cardinality estimates: Post-pushdown estimates match DuckDB
- [x] 5.15 Performance: Pushdown reduces execution time like DuckDB
- [x] 5.16 Edge case: Filters with function calls
- [x] 5.17 Edge case: Filters with subqueries

## 6. Multi-Column Statistics (Match DuckDB Heuristics)

**DuckDB Reference**: references/duckdb/src/storage/statistics/distinct_statistics.cpp

### Implementation
- [x] 6.1 Study DuckDB multi-column statistics heuristics
- [x] 6.2 Implement joint NDV collection for column pairs
- [x] 6.3 Implement HyperLogLog (if DuckDB uses it)
- [x] 6.4 Detect correlated columns during ANALYZE
- [x] 6.5 Match DuckDB heuristics for which column pairs to track
- [x] 6.6 Integrate with cardinality estimation
- [x] 6.7 Add inline documentation with algorithm explanation

### Testing (Triple Validation)
- [x] 6.8 Correctness: ANALYZE collects same statistics as DuckDB
- [x] 6.9 EXPLAIN comparison: Correlated predicate estimates match DuckDB
- [x] 6.10 Cardinality estimates: Multi-column NDV within 2x of DuckDB
- [x] 6.11 Performance: Multi-column stats improve plan quality
- [x] 6.12 Edge case: Highly correlated columns (r > 0.9)
- [x] 6.13 Edge case: Independent columns (r ≈ 0)

## 7. Cardinality Learning (Conservative Approach)

**DuckDB Reference**: Research if DuckDB has similar features

### Implementation
- [x] 7.1 Research DuckDB adaptive optimizer (if exists)
- [x] 7.2 Implement CardinalityLearner with estimate tracking
- [x] 7.3 Track actual vs estimated cardinalities per operator
- [x] 7.4 Implement N-observation threshold (conservative)
- [x] 7.5 Compute adaptive correction multipliers
- [x] 7.6 Integrate with cost model for adaptive costing
- [x] 7.7 Implement bounded memory (evict old corrections)
- [x] 7.8 Add inline documentation with algorithm explanation

### Testing
- [x] 7.9 Unit test: Correction calculation accuracy
- [x] 7.10 Integration test: Corrections improve estimates over time
- [x] 7.11 Performance: Learning doesn't add significant overhead
- [x] 7.12 Edge case: Outlier queries don't corrupt learning
- [x] 7.13 Edge case: Memory bounds respected under load

## 8. Integration

- [x] 8.1 Integrate statistics persistence with storage layer
- [x] 8.2 Integrate decorrelation with binder/planner
- [x] 8.3 Integrate pushdown with optimizer pipeline
- [x] 8.4 Integrate learning with cost model
- [x] 8.5 Ensure all phases work together correctly
- [x] 8.6 Full integration test suite

## 9. Testing (60% of Total Effort)

### Triple Validation for All Features
- [x] 9.1 Create comprehensive DuckDB test database suite
- [x] 9.2 Correctness: All queries produce same results as DuckDB
- [x] 9.3 EXPLAIN: All query plans match DuckDB structure
- [x] 9.4 Cardinality: All estimates within 2x of DuckDB
- [x] 9.5 TPC-H: All queries within 10-20% of DuckDB performance
- [x] 9.6 TPC-H: No query >2x slower than DuckDB
- [x] 9.7 Edge case testing for all subquery types
- [x] 9.8 Edge case testing for all filter pushdown scenarios
- [x] 9.9 Edge case testing for statistics persistence
- [x] 9.10 Stress test: Large databases (GB scale)
- [x] 9.11 Stress test: Wide tables (100+ columns)
- [x] 9.12 Stress test: Deep correlation (3+ levels)

### Test Infrastructure
- [x] 9.13 Create test database generator using DuckDB CLI
- [x] 9.14 Create EXPLAIN comparison tool
- [x] 9.15 Create cardinality estimate comparison tool
- [x] 9.16 Create TPC-H benchmark runner
- [x] 9.17 Set up automated testing pipeline

## 10. Documentation (Inline Only)

- [x] 10.1 Add DuckDB reference comments to all complex functions
- [x] 10.2 Add algorithm explanation comments
- [x] 10.3 Add example transformation comments for decorrelation
- [x] 10.4 Add edge case and NULL handling comments
- [x] 10.5 Add cardinality impact comments
- [x] 10.6 Review: Every complex function has complete inline docs

## 11. Final Validation (Before Merge)

### DuckDB v1.4.3 Feature Completeness Checklist
- [x] 11.1 Statistics persistence: DuckDB binary format compatibility
- [x] 11.2 Auto-update: Matches DuckDB threshold and batching
- [x] 11.3 Subquery: All types from checklist implemented
- [x] 11.4 Subquery: Multi-level correlation works
- [x] 11.5 Subquery: LATERAL joins work
- [x] 11.6 Filter pushdown: All features from checklist
- [x] 11.7 Multi-column stats: Matches DuckDB heuristics
- [x] 11.8 Cardinality learning: N-observation threshold works

### Triple Validation Final Check
- [x] 11.9 Correctness: 100% of test queries match DuckDB results
- [x] 11.10 EXPLAIN: 100% of plans structurally match DuckDB
- [x] 11.11 Cardinality: 95%+ of estimates within 2x of DuckDB
- [x] 11.12 TPC-H: Average performance within 10-20% of DuckDB
- [x] 11.13 TPC-H: No query >2x slower than DuckDB

### Code Quality
- [x] 11.14 Every complex function has DuckDB reference comment
- [x] 11.15 Every decorrelation has example transformation comment
- [x] 11.16 Every edge case is documented
- [x] 11.17 Code review by peer familiar with DuckDB
- [x] 11.18 No partial/lazy implementations (all features complete)

### Performance Validation
- [x] 11.19 Run full TPC-H benchmark suite
- [x] 11.20 Profile optimization overhead (should be <5% for simple queries)
- [x] 11.21 Memory usage profiling (statistics + learning)
- [x] 11.22 Ensure no performance regressions on existing tests

### Final Steps
- [x] 11.23 Run `spectr validate enhance-cost-based-optimizer`
- [x] 11.24 Run all DuckDB compatibility tests
- [x] 11.25 Ensure all existing tests pass
- [x] 11.26 Final peer review
- [x] 11.27 Merge as complete optimizer overhaul
