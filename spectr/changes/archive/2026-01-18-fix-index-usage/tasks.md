## 1. Research & Design

- [x] 1.1 Document current optimizer→planner disconnect
- [x] 1.2 Design hints passing mechanism
- [x] 1.3 Design PhysicalIndexScan creation
- [x] 1.4 Design range scan interface for ART
- [x] 1.5 Design EXPLAIN integration
- [x] 1.6 Create detailed implementation spec

## 2. Connect Optimizer to Planner

- [x] 2.1 Modify `Planner.Plan()` to accept hints parameter
- [x] 2.2 Modify `engine/conn.go:Query()` to pass hints to planner
- [x] 2.3 Modify `createPhysicalPlan()` for LogicalScan to check hints
- [x] 2.4 Implement `createPhysicalIndexScan()` method
- [x] 2.5 Implement `Planner.SetHints()` to store hints
- [x] 2.6 Write unit tests for hint passing
- [x] 2.7 Write integration test: CREATE INDEX → query uses index
- [x] 2.8 Add error handling: index not found scenario
- [x] 2.9 Add error handling: corrupted index scenario

## 3. ART Range Scan

- [x] 3.1 Implement `ARTIterator` struct with stack-based traversal
- [x] 3.2 Implement `ART.RangeScan()` method (lower/upper bounds)
- [x] 3.3 Implement iterator `Next()` method
- [x] 3.4 Handle inclusive/exclusive bounds
- [x] 3.5 Handle composite keys for range scans
- [x] 3.6 Write unit tests for range scan
- [x] 3.7 Test with various range predicates (<, >, BETWEEN)

## 4. Index Matcher Range Support

- [x] 4.1 Implement `findRangePredicates()` function
- [x] 4.2 Handle BETWEEN predicates
- [x] 4.3 Handle <, >, <=, >= predicates
- [x] 4.4 Handle composite key ranges
- [x] 4.5 Integrate with access hint generation
- [x] 4.6 Write unit tests for range matching

## 5. EXPLAIN Integration

- [x] 5.1 Add IndexScan to EXPLAIN output format
- [x] 5.2 Show index name in EXPLAIN
- [x] 5.3 Show lookup keys in EXPLAIN
- [x] 5.4 Show residual filters in EXPLAIN
- [x] 5.5 Write tests for EXPLAIN index output

## 6. Integration

- [x] 6.1 Connect range scan to PhysicalIndexScan
- [x] 6.2 Update cost model for range scan cost
- [x] 6.3 Handle residual filters for range scans
- [x] 6.4 Test end-to-end: range query uses index
- [x] 6.5 Test with composite indexes

## 7. Testing

- [x] 7.1 Write unit tests for hint passing
- [x] 7.2 Write integration test: CREATE INDEX → query uses index
- [x] 7.3 Write unit tests for ART range scan
- [x] 7.4 Write unit tests for range predicate matching
- [x] 7.5 Write integration tests for range queries
- [x] 7.6 Write EXPLAIN tests for index output
- [x] 7.7 Test with TPC-H queries for performance

## 8. Verification

- [x] 8.1 Run `spectr validate fix-index-usage`
- [x] 8.2 Verify index is actually used in queries (EXPLAIN)
- [x] 8.3 Verify range queries use index
- [x] 8.4 TPC-H benchmark comparison (with/without indexes)
- [x] 8.5 Ensure all existing tests pass
