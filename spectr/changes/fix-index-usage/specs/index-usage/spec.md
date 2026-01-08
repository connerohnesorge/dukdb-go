## MODIFIED Requirements

### Requirement: Index Scan Operator

The system MUST provide an index scan operator that retrieves rows via index lookup.

#### Scenario: Index scan for equality predicate

- WHEN a query has WHERE column = value
- AND an index exists on that column
- AND the optimizer selects index scan
- THEN the executor SHALL use HashIndex.Lookup() to find matching RowIDs
- THEN the executor SHALL retrieve rows only for those RowIDs

#### Scenario: Index scan returns correct results

- WHEN PhysicalIndexScan executes with lookup key [42]
- THEN it SHALL return exactly the rows where the indexed column equals 42
- AND results SHALL match what a full table scan with filter would return

#### Scenario: Index scan with no matches

- WHEN PhysicalIndexScan executes with a key that has no matches
- THEN it SHALL return an empty result set
- AND no table rows SHALL be scanned

#### Scenario: Index scan with multiple matches

- WHEN PhysicalIndexScan executes on a non-unique index
- AND the lookup key matches multiple rows
- THEN all matching rows SHALL be returned

#### Scenario: Planner creates PhysicalIndexScan when hinted

- WHEN the optimizer generates an AccessHint with Method = PlanTypeIndexScan
- AND the planner receives this hint
- THEN the planner SHALL create a PhysicalIndexScan operator
- AND the PhysicalIndexScan SHALL use the specified index

---

### Requirement: EXPLAIN Shows Index Usage

The system MUST show index usage in EXPLAIN output.

#### Scenario: EXPLAIN shows lookup keys

- WHEN EXPLAIN is run on a query using index scan
- THEN output SHALL show lookup keys if applicable

#### Scenario: EXPLAIN shows residual filters

- WHEN EXPLAIN is run on a query with residual filters
- AND the query uses index scan
- THEN output SHALL show residual filter expressions
- AND output SHALL indicate filters applied after index lookup

---

## ADDED Requirements

### Requirement: Planner Uses Optimizer Hints

The system MUST pass optimizer access hints to the planner for physical plan generation.

#### Scenario: Pass hints from engine to planner

- WHEN a query is prepared for execution
- AND the optimizer generates access hints
- THEN the engine SHALL pass hints to the planner
- AND the planner SHALL store hints for physical plan generation

#### Scenario: Planner creates index scan when hinted

- WHEN creating physical plan for LogicalScan
- AND hints indicate index scan should be used
- THEN the planner SHALL create PhysicalIndexScan
- AND the planner SHALL NOT create PhysicalScan

#### Scenario: Planner falls back to sequential scan when no hint

- WHEN creating physical plan for LogicalScan
- AND no index scan hint exists
- THEN the planner SHALL create PhysicalScan
- AND sequential scan SHALL be used

---

### Requirement: Range Scan Support

The system MUST support index range scans for <, >, BETWEEN predicates.

#### Scenario: Range scan for BETWEEN predicate

- WHEN a query has WHERE column BETWEEN 10 AND 100
- AND an ART index exists on that column
- AND the optimizer selects range scan
- THEN the executor SHALL use ART.RangeScan() to find matching RowIDs
- AND only rows with values in range SHALL be returned

#### Scenario: Range scan for < predicate

- WHEN a query has WHERE column < 50
- AND an ART index exists on that column
- AND the optimizer selects range scan
- THEN the executor SHALL use ART.RangeScan() with lower bound = -inf
- AND upper bound = 50 (exclusive)

#### Scenario: Range scan for > predicate

- WHEN a query has WHERE column > 75
- AND an ART index exists on that column
- AND the optimizer selects range scan
- THEN the executor SHALL use ART.RangeScan() with lower bound = 75 (exclusive)
- AND upper bound = +inf

#### Scenario: Range scan with composite index

- WHEN a query has WHERE (a, b) BETWEEN (1, 10) AND (5, 50)
- AND a composite ART index exists on (a, b)
- AND the optimizer selects range scan
- THEN the executor SHALL use ART.RangeScan() with composite bounds
- AND only rows matching the range SHALL be returned

---

### Requirement: Index Usage Verification

The system MUST verify that indexes are actually used in query execution.

#### Scenario: CREATE INDEX improves query performance

- WHEN a CREATE INDEX is executed on a column
- AND a query filtering on that column is executed
- THEN EXPLAIN SHALL show IndexScan
- AND query execution time SHALL be less than without index

#### Scenario: Multiple predicates use composite index

- WHEN a composite index exists on (a, b)
- AND a query has WHERE a = 1 AND b = 2
- THEN the optimizer SHALL use the composite index
- AND EXPLAIN SHALL show IndexScan with both keys

#### Scenario: Prefix match on composite index

- WHEN a composite index exists on (a, b, c)
- AND a query has WHERE a = 1 AND b = 2
- THEN the optimizer SHALL use the composite index prefix
- AND the predicate on c SHALL be applied as residual filter
