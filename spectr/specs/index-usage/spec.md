# Index Usage Specification

## Requirements

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

---

### Requirement: Cost-Based Index Selection

The system MUST select between index scan and table scan based on estimated cost.

#### Scenario: Index scan selected for selective query

- WHEN a query filters on an indexed column
- AND the filter selectivity is low (e.g., 1%)
- AND the table is large (e.g., 100,000 rows)
- THEN the optimizer SHALL select index scan over sequential scan
- AND EXPLAIN output SHALL show IndexScan operator

#### Scenario: Sequential scan selected for non-selective query

- WHEN a query filters on an indexed column
- AND the filter selectivity is high (e.g., 50%)
- THEN the optimizer SHALL select sequential scan over index scan
- AND EXPLAIN output SHALL show SeqScan operator

#### Scenario: Sequential scan when no matching index

- WHEN a query filters on a column without an index
- THEN the optimizer SHALL select sequential scan
- AND the query SHALL execute correctly

#### Scenario: Cost comparison uses table statistics

- WHEN an index exists and statistics are available
- THEN the optimizer SHALL use row count from statistics
- AND selectivity estimation SHALL use column statistics
- AND access method with lower estimated cost SHALL be selected

---

### Requirement: Index-Only Scan

The system MUST support index-only scan when index covers all required columns.

#### Scenario: Index-only scan when index covers projection

- WHEN a query selects only columns included in an index
- AND the index is selected for the query
- THEN PhysicalIndexScan.IsIndexOnly SHALL be true
- AND the execution MAY avoid accessing heap table data

#### Scenario: Regular index scan when columns not covered

- WHEN a query selects columns not included in the index
- THEN PhysicalIndexScan.IsIndexOnly SHALL be false
- THEN the executor SHALL fetch row data from the heap table

#### Scenario: Covering index detection

- WHEN checking if index covers query columns
- THEN IsCoveringIndex() SHALL return true only if all projected columns are in index
- AND IsCoveringIndex() SHALL return true only if all filter columns are in index

---

### Requirement: Composite Index Support

The system MUST support composite (multi-column) indexes with prefix matching.

#### Scenario: Full composite index match

- WHEN an index exists on columns (a, b, c)
- AND the query has WHERE a = 1 AND b = 2 AND c = 3
- THEN the optimizer SHALL match all three columns
- AND lookup SHALL use key [1, 2, 3]

#### Scenario: Prefix match on composite index

- WHEN an index exists on columns (a, b, c)
- AND the query has WHERE a = 1 AND b = 2
- THEN the optimizer SHALL match the first two columns
- AND lookup SHALL use key [1, 2]
- AND remaining filter (if any) SHALL be applied after index scan

#### Scenario: First column only match

- WHEN an index exists on columns (a, b, c)
- AND the query has WHERE a = 1
- THEN the optimizer SHALL match only the first column
- AND lookup SHALL use key [1]

#### Scenario: Non-prefix column cannot use index

- WHEN an index exists on columns (a, b, c)
- AND the query has WHERE b = 2 (no predicate on a)
- THEN the index SHALL NOT be matched
- AND sequential scan SHALL be used

#### Scenario: Gap in prefix cannot use index

- WHEN an index exists on columns (a, b, c)
- AND the query has WHERE a = 1 AND c = 3 (no predicate on b)
- THEN the optimizer SHALL match only column a
- AND the predicate on c SHALL be applied as a residual filter

---

### Requirement: Index Scan Cost Model

The system MUST estimate costs for index scan operations.

#### Scenario: Index scan cost includes lookup overhead

- WHEN estimating index scan cost
- THEN cost SHALL include IndexLookupCost for the index lookup itself
- AND cost SHALL include IndexTupleCost per expected row

#### Scenario: Index scan cost includes heap access

- WHEN index scan is not index-only
- THEN cost SHALL include RandomPageCost for heap tuple fetches
- AND cost SHALL account for estimated number of rows to fetch

#### Scenario: Index-only scan has lower cost

- WHEN index scan is index-only (covering index)
- THEN heap access cost SHALL NOT be included
- AND index-only scan cost SHALL be lower than regular index scan

#### Scenario: Cost model selects cheaper access method

- WHEN both index scan and sequential scan are possible
- THEN the optimizer SHALL estimate cost for both
- AND the access method with lower TotalCost SHALL be selected

---

### Requirement: EXPLAIN Shows Index Usage

The system MUST show index usage in EXPLAIN output.

#### Scenario: EXPLAIN shows IndexScan operator

- WHEN EXPLAIN is run on a query using index scan
- THEN output SHALL show IndexScan as the access method
- AND output SHALL show the index name being used

#### Scenario: EXPLAIN shows SeqScan when no index used

- WHEN EXPLAIN is run on a query using sequential scan
- THEN output SHALL show SeqScan as the access method

#### Scenario: EXPLAIN shows cost estimates for index scan

- WHEN EXPLAIN is run on a query with index scan
- THEN output SHALL show estimated cost for the IndexScan operator
- AND output SHALL show estimated row count

---

