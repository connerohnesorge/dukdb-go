# Execution Engine Specification Delta: Window Functions

## ADDED Requirements

### Requirement: PhysicalWindow Operator

The executor SHALL implement a PhysicalWindow operator for window function evaluation.

#### Scenario: PhysicalWindow interface compliance
- GIVEN the PhysicalWindow type
- WHEN checking interface compliance
- THEN PhysicalWindow implements PhysicalOperator interface
- AND Next() returns *DataChunk
- AND GetTypes() returns child types plus window result types

#### Scenario: Window operator in execution switch
- GIVEN the Executor.Execute method
- WHEN plan is *PhysicalWindow
- THEN executeWindow is called
- AND result contains window function output columns

#### Scenario: Window operator preserves child columns
- GIVEN table "t" with columns (id, name, salary)
- WHEN executing "SELECT *, ROW_NUMBER() OVER (ORDER BY id) FROM t"
- THEN result contains columns (id, name, salary, row_number)
- AND all child values are preserved

### Requirement: ROW_NUMBER Function

The executor SHALL implement ROW_NUMBER() window function.

#### Scenario: ROW_NUMBER without partition
- GIVEN table "t" with rows [(1), (2), (3)]
- WHEN executing "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 1), (2, 2), (3, 3)]

#### Scenario: ROW_NUMBER with partition
- GIVEN table "t" with rows [('A', 1), ('A', 2), ('B', 1), ('B', 2)]
- WHEN executing "SELECT cat, val, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val) FROM t"
- THEN result equals [('A', 1, 1), ('A', 2, 2), ('B', 1, 1), ('B', 2, 2)]

#### Scenario: ROW_NUMBER type is BIGINT
- GIVEN any window query with ROW_NUMBER()
- WHEN executing the query
- THEN ROW_NUMBER column type is BIGINT

### Requirement: RANK Function

The executor SHALL implement RANK() window function with gap handling.

#### Scenario: RANK with ties
- GIVEN table "t" with rows [(10), (10), (20), (30)]
- WHEN executing "SELECT val, RANK() OVER (ORDER BY val) FROM t"
- THEN result equals [(10, 1), (10, 1), (20, 3), (30, 4)]
- AND rank 2 is skipped due to tie

#### Scenario: RANK with partition
- GIVEN table "t" with rows [('A', 10), ('A', 10), ('A', 20), ('B', 5), ('B', 5)]
- WHEN executing "SELECT cat, val, RANK() OVER (PARTITION BY cat ORDER BY val) FROM t"
- THEN result equals [('A', 10, 1), ('A', 10, 1), ('A', 20, 3), ('B', 5, 1), ('B', 5, 1)]

#### Scenario: RANK requires ORDER BY
- GIVEN query without ORDER BY in window
- WHEN executing "SELECT RANK() OVER (PARTITION BY x) FROM t"
- THEN all rows have rank 1 (no ordering means all are peers)

### Requirement: DENSE_RANK Function

The executor SHALL implement DENSE_RANK() window function without gaps.

#### Scenario: DENSE_RANK with ties
- GIVEN table "t" with rows [(10), (10), (20), (30)]
- WHEN executing "SELECT val, DENSE_RANK() OVER (ORDER BY val) FROM t"
- THEN result equals [(10, 1), (10, 1), (20, 2), (30, 3)]
- AND no rank values are skipped

#### Scenario: DENSE_RANK vs RANK comparison
- GIVEN table with values [1, 1, 2, 3, 3, 4]
- WHEN executing both RANK() and DENSE_RANK()
- THEN RANK returns [1, 1, 3, 4, 4, 6]
- AND DENSE_RANK returns [1, 1, 2, 3, 3, 4]

### Requirement: NTILE Function

The executor SHALL implement NTILE(n) window function for bucket distribution.

#### Scenario: NTILE with even distribution
- GIVEN table "t" with 8 rows ordered by id
- WHEN executing "SELECT id, NTILE(4) OVER (ORDER BY id) FROM t"
- THEN result has 2 rows per bucket: [(1,1), (2,1), (3,2), (4,2), (5,3), (6,3), (7,4), (8,4)]

#### Scenario: NTILE with uneven distribution
- GIVEN table "t" with 10 rows ordered by id
- WHEN executing "SELECT id, NTILE(4) OVER (ORDER BY id) FROM t"
- THEN buckets have sizes [3, 3, 2, 2] (extra rows go to earlier buckets)

#### Scenario: NTILE with more buckets than rows
- GIVEN table "t" with 3 rows
- WHEN executing "SELECT id, NTILE(10) OVER (ORDER BY id) FROM t"
- THEN each row gets a unique bucket [1, 2, 3]

#### Scenario: NTILE argument validation
- GIVEN NTILE with zero or negative argument
- WHEN executing "SELECT NTILE(0) OVER () FROM t"
- THEN ErrorTypeExecutor is returned
- AND error message indicates invalid bucket count

### Requirement: LAG Function

The executor SHALL implement LAG(expr, offset, default) window function.

#### Scenario: LAG with default offset
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, LAG(id) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, NULL), (2, 1), (3, 2)]

#### Scenario: LAG with custom offset
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, LAG(id, 2) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, NULL), (2, NULL), (3, 1), (4, 2)]

#### Scenario: LAG with default value
- GIVEN table "t" with rows [(1), (2), (3)]
- WHEN executing "SELECT id, LAG(id, 1, 0) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 0), (2, 1), (3, 2)]
- AND default value 0 replaces NULL for first row

#### Scenario: LAG respects partition boundaries
- GIVEN table with rows [('A', 1), ('A', 2), ('B', 1), ('B', 2)]
- WHEN executing "SELECT cat, val, LAG(val) OVER (PARTITION BY cat ORDER BY val) FROM t"
- THEN result equals [('A', 1, NULL), ('A', 2, 1), ('B', 1, NULL), ('B', 2, 1)]
- AND LAG does not cross partition boundary

### Requirement: LEAD Function

The executor SHALL implement LEAD(expr, offset, default) window function.

#### Scenario: LEAD with default offset
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, LEAD(id) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 2), (2, 3), (3, NULL)]

#### Scenario: LEAD with custom offset
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, LEAD(id, 2) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 3), (2, 4), (3, NULL), (4, NULL)]

#### Scenario: LEAD with default value
- GIVEN table "t" with rows [(1), (2), (3)]
- WHEN executing "SELECT id, LEAD(id, 1, 99) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 2), (2, 3), (3, 99)]

### Requirement: FIRST_VALUE Function

The executor SHALL implement FIRST_VALUE(expr) window function.

#### Scenario: FIRST_VALUE with default frame
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, FIRST_VALUE(id) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 1), (2, 1), (3, 1)]
- AND first value of partition is returned for all rows

#### Scenario: FIRST_VALUE with sliding frame
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, FIRST_VALUE(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- THEN result equals [(1, 1), (2, 1), (3, 2), (4, 3)]

#### Scenario: FIRST_VALUE ignores NULLs
- GIVEN table with rows [(NULL), (1), (2)]
- WHEN executing "SELECT FIRST_VALUE(val) OVER (ORDER BY id) FROM t"
- THEN first value is NULL (NULLs are included in FIRST_VALUE)

### Requirement: LAST_VALUE Function

The executor SHALL implement LAST_VALUE(expr) window function.

#### Scenario: LAST_VALUE with default frame
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, LAST_VALUE(id) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 1), (2, 2), (3, 3)]
- AND default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

#### Scenario: LAST_VALUE with full frame
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, LAST_VALUE(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t"
- THEN result equals [(1, 3), (2, 3), (3, 3)]

### Requirement: NTH_VALUE Function

The executor SHALL implement NTH_VALUE(expr, n) window function.

#### Scenario: NTH_VALUE basic
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, NTH_VALUE(id, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t"
- THEN result equals [(1, 2), (2, 2), (3, 2), (4, 2)]

#### Scenario: NTH_VALUE out of bounds
- GIVEN table "t" with 3 rows
- WHEN executing "SELECT id, NTH_VALUE(id, 10) OVER () FROM t"
- THEN result has NULL for NTH_VALUE column (10 > row count)

#### Scenario: NTH_VALUE with frame
- GIVEN table with rows [(1), (2), (3), (4), (5)] ordered by id
- WHEN executing "SELECT id, NTH_VALUE(id, 2) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- THEN result considers only frame for each row

### Requirement: PERCENT_RANK Function

The executor SHALL implement PERCENT_RANK() window function.

#### Scenario: PERCENT_RANK basic
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, PERCENT_RANK() OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 0.0), (2, 0.333...), (3, 0.666...), (4, 1.0)]
- AND formula is (rank - 1) / (partition_size - 1)

#### Scenario: PERCENT_RANK single row
- GIVEN table "t" with 1 row
- WHEN executing "SELECT PERCENT_RANK() OVER () FROM t"
- THEN result equals 0.0 (edge case: partition_size = 1)

#### Scenario: PERCENT_RANK with ties
- GIVEN table with values [10, 10, 20]
- WHEN executing "SELECT val, PERCENT_RANK() OVER (ORDER BY val) FROM t"
- THEN tied values have same percent_rank

### Requirement: CUME_DIST Function

The executor SHALL implement CUME_DIST() window function.

#### Scenario: CUME_DIST basic
- GIVEN table "t" with rows [(1), (2), (3), (4)] ordered by id
- WHEN executing "SELECT id, CUME_DIST() OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 0.25), (2, 0.5), (3, 0.75), (4, 1.0)]
- AND formula is (rows_at_or_before) / partition_size

#### Scenario: CUME_DIST with ties
- GIVEN table with values [10, 10, 20]
- WHEN executing "SELECT val, CUME_DIST() OVER (ORDER BY val) FROM t"
- THEN tied values have same cume_dist [(10, 0.666...), (10, 0.666...), (20, 1.0)]

### Requirement: Aggregate Window Functions

The executor SHALL support aggregate functions with OVER clause.

#### Scenario: SUM with OVER
- GIVEN table "t" with rows [(1), (2), (3)] ordered by id
- WHEN executing "SELECT id, SUM(id) OVER (ORDER BY id) FROM t"
- THEN result equals [(1, 1), (2, 3), (3, 6)] (running sum with default frame)

#### Scenario: COUNT with OVER
- GIVEN table "t" with rows [(1), (2), (3)]
- WHEN executing "SELECT id, COUNT(*) OVER () FROM t"
- THEN result equals [(1, 3), (2, 3), (3, 3)]

#### Scenario: AVG with sliding frame
- GIVEN table "t" with rows [(1), (2), (3), (4), (5)] ordered by id
- WHEN executing "SELECT id, AVG(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- THEN result equals [(1, 1.5), (2, 2.0), (3, 3.0), (4, 4.0), (5, 4.5)]

#### Scenario: MIN/MAX with OVER
- GIVEN table "t" with rows [(3), (1), (4), (1), (5)]
- WHEN executing "SELECT val, MIN(val) OVER (), MAX(val) OVER () FROM t"
- THEN all rows have MIN=1 and MAX=5

### Requirement: Frame Boundary Evaluation

The executor SHALL correctly evaluate frame boundaries.

#### Scenario: ROWS BETWEEN n PRECEDING AND n FOLLOWING
- GIVEN table "t" with rows [(1), (2), (3), (4), (5)] ordered by id
- AND window "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"
- WHEN executing SUM(id) OVER (...)
- THEN result equals [(1, 3), (2, 6), (3, 9), (4, 12), (5, 9)]

#### Scenario: RANGE BETWEEN with numeric ORDER BY
- GIVEN table "t" with rows [(1), (3), (4), (6), (10)] ordered by val
- AND window "RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING"
- WHEN executing SUM(val) OVER (...)
- THEN each row sums values within val-2 to val+2

#### Scenario: ROWS CURRENT ROW
- GIVEN table "t" with rows
- WHEN executing "SELECT id, SUM(id) OVER (ROWS CURRENT ROW) FROM t"
- THEN each row's sum equals its own value

#### Scenario: UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING
- GIVEN any table "t"
- WHEN executing "SELECT SUM(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t"
- THEN all rows have same sum (total of partition)

### Requirement: NULL Handling in Windows

The executor SHALL handle NULL values according to DuckDB semantics.

#### Scenario: NULL in PARTITION BY
- GIVEN table with rows [('A', 1), (NULL, 2), (NULL, 3), ('B', 4)]
- WHEN executing "SELECT cat, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val) FROM t"
- THEN NULL category forms its own partition with row numbers 1, 2
- AND 'A' partition has row number 1
- AND 'B' partition has row number 1

#### Scenario: NULL in ORDER BY
- GIVEN table with values [(1), (NULL), (3), (2)]
- WHEN executing "SELECT val, ROW_NUMBER() OVER (ORDER BY val) FROM t"
- THEN NULLs are sorted last by default
- AND order is [1, 2, 3, NULL] with row numbers [1, 2, 3, 4]

#### Scenario: NULL in aggregate window
- GIVEN table with values [(1), (NULL), (3)]
- WHEN executing "SELECT val, SUM(val) OVER () FROM t"
- THEN NULL is excluded from sum, result is 4 for all rows

#### Scenario: NULL in LAG/LEAD
- GIVEN table with values [(1), (NULL), (3)]
- WHEN executing "SELECT val, LAG(val) OVER (ORDER BY rowid) FROM t"
- THEN result equals [(1, NULL), (NULL, 1), (3, NULL)]
- AND NULL values are passed through correctly

### Requirement: Window Ordering Preservation

The executor SHALL return rows in a deterministic order.

#### Scenario: Original row order preserved
- GIVEN table "t" with rows in specific insert order
- WHEN executing window query without ORDER BY in outer query
- THEN rows are returned in original order (by RowID)
- AND window results are correctly attached to each row

#### Scenario: Multiple windows with different ordering
- GIVEN query "SELECT ROW_NUMBER() OVER (ORDER BY a), ROW_NUMBER() OVER (ORDER BY b) FROM t"
- WHEN executing the query
- THEN each window is evaluated with its own ordering
- AND results are correctly combined in output

### Requirement: Performance Bounds

Window execution SHALL meet performance requirements.

#### Scenario: Large partition performance
- GIVEN table with 100,000 rows in single partition
- WHEN executing "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t"
- THEN execution completes in < 1 second
- AND memory usage < 100MB

#### Scenario: Many partitions performance
- GIVEN table with 100,000 rows in 10,000 partitions
- WHEN executing "SELECT ROW_NUMBER() OVER (PARTITION BY cat ORDER BY id) FROM t"
- THEN execution completes in < 2 seconds

#### Scenario: Sliding window performance
- GIVEN table with 10,000 rows
- WHEN executing "SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) FROM t"
- THEN execution completes in < 1 second
- AND time complexity is O(n) to O(n log n), not O(n²)

### Requirement: GROUPS Frame Type

The executor SHALL implement GROUPS frame type for peer group-based frames.

#### Scenario: GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
- GIVEN table with values [(10), (10), (20), (30), (30), (40)] ordered by val
- AND peer groups are {10, 10}, {20}, {30, 30}, {40}
- WHEN executing "SELECT val, SUM(val) OVER (ORDER BY val GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- THEN row with val=10 sums groups {10,10} and {20} = 40
- AND row with val=20 sums groups {10,10}, {20}, {30,30} = 90
- AND row with val=30 sums groups {20}, {30,30}, {40} = 110
- AND row with val=40 sums groups {30,30} and {40} = 100

#### Scenario: GROUPS UNBOUNDED PRECEDING
- GIVEN table with values [(10), (10), (20), (30)]
- WHEN executing "SELECT val, COUNT(*) OVER (ORDER BY val GROUPS UNBOUNDED PRECEDING) FROM t"
- THEN rows with val=10 have count 2 (just group {10,10})
- AND row with val=20 has count 3 (groups {10,10}, {20})
- AND row with val=30 has count 4 (all groups)

#### Scenario: GROUPS with no ORDER BY
- GIVEN window with GROUPS frame but no ORDER BY
- WHEN executing the query
- THEN all rows are in single peer group
- AND GROUPS frame behaves like full partition

### Requirement: EXCLUDE Clause

The executor SHALL implement EXCLUDE clause for frame specifications.

#### Scenario: EXCLUDE CURRENT ROW
- GIVEN table with values [(1), (2), (3)] ordered by val
- WHEN executing "SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t"
- THEN row 1 sums 2+3 = 5
- AND row 2 sums 1+3 = 4
- AND row 3 sums 1+2 = 3

#### Scenario: EXCLUDE GROUP
- GIVEN table with values [(10), (10), (20)] ordered by val
- WHEN executing "SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM t"
- THEN first row (val=10) sums 20 (excludes both 10s from peer group)
- AND second row (val=10) sums 20 (excludes both 10s)
- AND third row (val=20) sums 20 (excludes just the single 20)

#### Scenario: EXCLUDE TIES
- GIVEN table with values [(10), (10), (20)] ordered by val
- WHEN executing "SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM t"
- THEN first row sums 10+20 = 30 (includes self, excludes other 10)
- AND second row sums 10+20 = 30 (includes self, excludes other 10)
- AND third row sums 40 (no ties to exclude)

#### Scenario: EXCLUDE with single-row groups
- GIVEN table with values [(1), (2), (3)] all unique
- WHEN executing with EXCLUDE TIES
- THEN result equals EXCLUDE NO OTHERS (no ties exist)

### Requirement: NULLS FIRST/LAST in ORDER BY

The executor SHALL respect NULLS FIRST and NULLS LAST ordering.

#### Scenario: NULLS LAST (default)
- GIVEN table with values [(1), (NULL), (3), (2)]
- WHEN executing "SELECT val, ROW_NUMBER() OVER (ORDER BY val) FROM t"
- THEN order is [1, 2, 3, NULL] with row numbers [1, 2, 3, 4]

#### Scenario: NULLS FIRST
- GIVEN table with values [(1), (NULL), (3), (2)]
- WHEN executing "SELECT val, ROW_NUMBER() OVER (ORDER BY val NULLS FIRST) FROM t"
- THEN order is [NULL, 1, 2, 3] with row numbers [1, 2, 3, 4]

#### Scenario: DESC NULLS FIRST
- GIVEN table with values [(1), (NULL), (3), (2)]
- WHEN executing "SELECT val, ROW_NUMBER() OVER (ORDER BY val DESC NULLS FIRST) FROM t"
- THEN order is [NULL, 3, 2, 1] with row numbers [1, 2, 3, 4]

#### Scenario: DESC NULLS LAST
- GIVEN table with values [(1), (NULL), (3), (2)]
- WHEN executing "SELECT val, ROW_NUMBER() OVER (ORDER BY val DESC NULLS LAST) FROM t"
- THEN order is [3, 2, 1, NULL] with row numbers [1, 2, 3, 4]

### Requirement: IGNORE NULLS Modifier

The executor SHALL implement IGNORE NULLS for value functions.

#### Scenario: LAG IGNORE NULLS
- GIVEN table with values [(1), (NULL), (3)] ordered by id
- WHEN executing "SELECT val, LAG(val) IGNORE NULLS OVER (ORDER BY id) FROM t"
- THEN row 1 has LAG = NULL (no previous non-null)
- AND row 2 has LAG = 1 (previous non-null is 1)
- AND row 3 has LAG = 1 (skips NULL at row 2, finds 1)

#### Scenario: LEAD IGNORE NULLS
- GIVEN table with values [(1), (NULL), (3)] ordered by id
- WHEN executing "SELECT val, LEAD(val) IGNORE NULLS OVER (ORDER BY id) FROM t"
- THEN row 1 has LEAD = 3 (skips NULL, finds 3)
- AND row 2 has LEAD = 3 (next non-null is 3)
- AND row 3 has LEAD = NULL (no next non-null)

#### Scenario: FIRST_VALUE IGNORE NULLS
- GIVEN table with values [(NULL), (2), (3)] ordered by id
- WHEN executing "SELECT val, FIRST_VALUE(val) IGNORE NULLS OVER (ORDER BY id) FROM t"
- THEN all rows have FIRST_VALUE = 2 (first non-null in partition)

#### Scenario: LAST_VALUE IGNORE NULLS
- GIVEN table with values [(1), (2), (NULL)] ordered by id
- AND frame ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
- WHEN executing "SELECT val, LAST_VALUE(val) IGNORE NULLS OVER (...) FROM t"
- THEN all rows have LAST_VALUE = 2 (last non-null in partition)

#### Scenario: NTH_VALUE IGNORE NULLS
- GIVEN table with values [(NULL), (2), (NULL), (4)] ordered by id
- WHEN executing "SELECT val, NTH_VALUE(val, 2) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t"
- THEN all rows have NTH_VALUE = 4 (second non-null value)

### Requirement: FILTER Clause

The executor SHALL implement FILTER clause for aggregate window functions.

#### Scenario: COUNT with FILTER
- GIVEN table with values [(1, 'a'), (2, 'b'), (3, 'a'), (4, 'b')]
- WHEN executing "SELECT id, COUNT(*) FILTER (WHERE cat = 'a') OVER () FROM t"
- THEN all rows have count = 2 (only rows where cat='a' are counted)

#### Scenario: SUM with FILTER
- GIVEN table with values [(10, true), (20, false), (30, true)]
- WHEN executing "SELECT val, SUM(val) FILTER (WHERE active) OVER () FROM t"
- THEN all rows have sum = 40 (10 + 30 where active=true)

#### Scenario: FILTER with frame
- GIVEN table with values [(1, true), (2, false), (3, true), (4, true)]
- WHEN executing "SELECT val, SUM(val) FILTER (WHERE flag) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- THEN filter is applied within each row's frame

#### Scenario: FILTER excludes all rows
- GIVEN table with values [(1, 'a'), (2, 'a'), (3, 'a')]
- WHEN executing "SELECT id, COUNT(*) FILTER (WHERE cat = 'b') OVER () FROM t"
- THEN all rows have count = 0 (no rows match filter)

### Requirement: DISTINCT Aggregate Windows

The executor SHALL implement DISTINCT modifier for aggregate window functions.

#### Scenario: COUNT DISTINCT
- GIVEN table with values [('a'), ('b'), ('a'), ('c'), ('b')]
- WHEN executing "SELECT cat, COUNT(DISTINCT cat) OVER () FROM t"
- THEN all rows have count = 3 (distinct values: a, b, c)

#### Scenario: SUM DISTINCT
- GIVEN table with values [(10), (20), (10), (30), (20)]
- WHEN executing "SELECT val, SUM(DISTINCT val) OVER () FROM t"
- THEN all rows have sum = 60 (10 + 20 + 30)

#### Scenario: DISTINCT with frame
- GIVEN table with values [(1), (2), (1), (3), (2)] ordered by id
- WHEN executing "SELECT val, COUNT(DISTINCT val) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t"
- THEN row 1 has count 1 (just {1})
- AND row 2 has count 2 ({1, 2})
- AND row 3 has count 2 ({2, 1})
- AND row 4 has count 3 ({1, 3})
- AND row 5 has count 2 ({3, 2})

### Requirement: Window Function Return Types

The executor SHALL return correct types for each window function.

#### Scenario: Ranking functions return BIGINT
- GIVEN any query with ROW_NUMBER(), RANK(), DENSE_RANK(), or NTILE()
- WHEN executing the query
- THEN GetTypes() returns BIGINT for these columns

#### Scenario: Distribution functions return DOUBLE
- GIVEN any query with PERCENT_RANK() or CUME_DIST()
- WHEN executing the query
- THEN GetTypes() returns DOUBLE for these columns
- AND values are in range [0.0, 1.0] for PERCENT_RANK
- AND values are in range (0.0, 1.0] for CUME_DIST

#### Scenario: Value functions inherit argument type
- GIVEN query "SELECT LAG(name) OVER () FROM t" where name is VARCHAR
- WHEN executing the query
- THEN GetTypes() returns VARCHAR for LAG column

#### Scenario: Aggregate window return types
- GIVEN query with COUNT(*) OVER ()
- WHEN executing the query
- THEN GetTypes() returns BIGINT for COUNT column

### Requirement: Window Error Handling

The executor SHALL return appropriate errors for invalid window operations.

#### Scenario: NTILE with zero buckets
- GIVEN query "SELECT NTILE(0) OVER (ORDER BY id) FROM t"
- WHEN executing the query
- THEN ErrorTypeExecutor is returned
- AND error message indicates bucket count must be positive

#### Scenario: NTILE with negative buckets
- GIVEN query "SELECT NTILE(-5) OVER (ORDER BY id) FROM t"
- WHEN executing the query
- THEN ErrorTypeExecutor is returned

#### Scenario: NTH_VALUE with zero index
- GIVEN query "SELECT NTH_VALUE(x, 0) OVER () FROM t"
- WHEN executing the query
- THEN ErrorTypeExecutor is returned
- AND error message indicates index must be positive (1-based)

#### Scenario: LAG with negative offset
- GIVEN query "SELECT LAG(x, -1) OVER (ORDER BY id) FROM t"
- WHEN executing the query
- THEN ErrorTypeExecutor is returned
- AND error message indicates offset must be non-negative

#### Scenario: Frame offset is negative
- GIVEN query "SELECT SUM(x) OVER (ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM t"
- WHEN executing the query
- THEN ErrorTypeExecutor is returned
