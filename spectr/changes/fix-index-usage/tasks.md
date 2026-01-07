## 1. Research & Design

- [ ] 1.1 Document current optimizer→planner disconnect
- [ ] 1.2 Design hints passing mechanism
- [ ] 1.3 Design PhysicalIndexScan creation
- [ ] 1.4 Design range scan interface for ART
- [ ] 1.5 Design EXPLAIN integration
- [ ] 1.6 Create detailed implementation spec

## 2. Connect Optimizer to Planner

- [ ] 2.1 Modify `Planner.Plan()` to accept hints parameter
- [ ] 2.2 Modify `engine/conn.go:Query()` to pass hints to planner
- [ ] 2.3 Modify `createPhysicalPlan()` for LogicalScan to check hints
- [ ] 2.4 Implement `createPhysicalIndexScan()` method
- [ ] 2.5 Implement `Planner.SetHints()` to store hints
- [ ] 2.6 Write unit tests for hint passing
- [ ] 2.7 Write integration test: CREATE INDEX → query uses index
- [ ] 2.8 Add error handling: index not found scenario
- [ ] 2.9 Add error handling: corrupted index scenario

## 3. ART Range Scan

- [ ] 3.1 Implement `ARTIterator` struct with stack-based traversal
- [ ] 3.2 Implement `ART.RangeScan()` method (lower/upper bounds)
- [ ] 3.3 Implement iterator `Next()` method
- [ ] 3.4 Handle inclusive/exclusive bounds
- [ ] 3.5 Handle composite keys for range scans
- [ ] 3.6 Write unit tests for range scan
- [ ] 3.7 Test with various range predicates (<, >, BETWEEN)

## 4. Index Matcher Range Support

- [ ] 4.1 Implement `findRangePredicates()` function
- [ ] 4.2 Handle BETWEEN predicates
- [ ] 4.3 Handle <, >, <=, >= predicates
- [ ] 4.4 Handle composite key ranges
- [ ] 4.5 Integrate with access hint generation
- [ ] 4.6 Write unit tests for range matching

## 5. EXPLAIN Integration

- [ ] 5.1 Add IndexScan to EXPLAIN output format
- [ ] 5.2 Show index name in EXPLAIN
- [ ] 5.3 Show lookup keys in EXPLAIN
- [ ] 5.4 Show residual filters in EXPLAIN
- [ ] 5.5 Write tests for EXPLAIN index output

## 6. Integration

- [ ] 6.1 Connect range scan to PhysicalIndexScan
- [ ] 6.2 Update cost model for range scan cost
- [ ] 6.3 Handle residual filters for range scans
- [ ] 6.4 Test end-to-end: range query uses index
- [ ] 6.5 Test with composite indexes

## 7. Testing

- [ ] 7.1 Write unit tests for hint passing
- [ ] 7.2 Write integration test: CREATE INDEX → query uses index
- [ ] 7.3 Write unit tests for ART range scan
- [ ] 7.4 Write unit tests for range predicate matching
- [ ] 7.5 Write integration tests for range queries
- [ ] 7.6 Write EXPLAIN tests for index output
- [ ] 7.7 Test with TPC-H queries for performance

## 8. Verification

- [ ] 8.1 Run `spectr validate fix-index-usage`
- [ ] 8.2 Verify index is actually used in queries (EXPLAIN)
- [ ] 8.3 Verify range queries use index
- [ ] 8.4 TPC-H benchmark comparison (with/without indexes)
- [ ] 8.5 Ensure all existing tests pass
