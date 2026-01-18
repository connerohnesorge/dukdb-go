# Cost Based Optimizer Specification

## Requirements

### Requirement: Table Statistics Collection

The system MUST collect and maintain statistics for tables via ANALYZE.

#### Scenario: Collect basic table statistics
- **Given** a table with data exists
- **When** ANALYZE is run on the table
- **Then** row count is recorded accurately
- **And** page count is estimated
- **And** data size is calculated

#### Scenario: Collect column statistics
- **Given** a table with columns exists
- **When** ANALYZE is run on the table
- **Then** distinct count is estimated for each column
- **And** null fraction is calculated for each column
- **And** min/max values are recorded for each column

#### Scenario: Build histogram for column
- **Given** a column with varied value distribution
- **When** ANALYZE is run with histogram collection
- **Then** equi-depth histogram is built with configured bucket count
- **And** bucket boundaries represent equal row counts

#### Scenario: Sample-based analysis for large tables
- **Given** a table with more than 100,000 rows
- **When** ANALYZE is run
- **Then** statistics are collected using random sampling
- **And** sample size is configurable

---

### Requirement: Cardinality Estimation

The system MUST estimate output cardinality for all plan operators.

#### Scenario: Estimate base table cardinality
- **Given** a table with statistics
- **When** cardinality is estimated for a scan
- **Then** row count from statistics is returned

#### Scenario: Estimate filter selectivity with equality
- **Given** a filter with equality predicate (col = value)
- **When** selectivity is estimated
- **Then** selectivity equals 1 / distinct_count

#### Scenario: Estimate filter selectivity with range
- **Given** a filter with range predicate (col > value)
- **When** selectivity is estimated with histogram
- **Then** selectivity is calculated from histogram bucket fractions

#### Scenario: Estimate join cardinality
- **Given** a join between two tables
- **When** cardinality is estimated
- **Then** result considers both input cardinalities
- **And** result considers join predicate selectivity

#### Scenario: Estimate aggregate cardinality
- **Given** an aggregation with GROUP BY
- **When** cardinality is estimated
- **Then** result approximates distinct combinations of group keys

#### Scenario: Handle missing statistics
- **Given** a table without statistics
- **When** cardinality is estimated
- **Then** conservative default estimates are used
- **And** query execution proceeds normally

---

### Requirement: Cost Model

The system MUST estimate execution cost for physical plans.

#### Scenario: Cost sequential scan
- **Given** a sequential table scan
- **When** cost is estimated
- **Then** cost reflects pages read sequentially
- **And** cost reflects tuples processed

#### Scenario: Cost hash join
- **Given** a hash join between two inputs
- **When** cost is estimated
- **Then** cost includes hash table build cost
- **And** cost includes probe cost for outer tuples

#### Scenario: Cost nested loop join
- **Given** a nested loop join
- **When** cost is estimated
- **Then** cost reflects outer rows times inner cost

#### Scenario: Cost sort operation
- **Given** a sort operation
- **When** cost is estimated
- **Then** cost reflects O(n log n) comparisons

#### Scenario: Cumulative plan cost
- **Given** a complete physical plan tree
- **When** total cost is calculated
- **Then** cost aggregates all operator costs correctly

---

### Requirement: Join Order Optimization

The system MUST optimize join order for multi-table queries.

#### Scenario: Optimize two-table join
- **Given** a query joining two tables
- **When** join order is optimized
- **Then** smaller table is selected as hash build side

#### Scenario: Optimize multi-table join with DP
- **Given** a query joining N tables where N <= 12
- **When** join order is optimized
- **Then** dynamic programming finds optimal order
- **And** all valid join orders are considered

#### Scenario: Optimize large join with greedy
- **Given** a query joining N tables where N > 12
- **When** join order is optimized
- **Then** greedy algorithm produces reasonable order
- **And** optimization completes in acceptable time

#### Scenario: Preserve outer join semantics
- **Given** a query with LEFT/RIGHT/FULL joins
- **When** join order is optimized
- **Then** outer join semantics are preserved
- **And** invalid reorderings are rejected

#### Scenario: Select build side for hash join
- **Given** a hash join between two inputs
- **When** physical plan is generated
- **Then** input with smaller estimated cardinality is build side

---

### Requirement: Physical Plan Selection

The system MUST select optimal physical operators.

#### Scenario: Choose hash join for equi-join
- **Given** an equi-join between tables
- **When** physical operator is selected
- **Then** hash join is chosen when appropriate

#### Scenario: Choose nested loop for small inner
- **Given** a join where inner table is very small
- **When** physical operator is selected
- **Then** nested loop join may be chosen

#### Scenario: Consider index scan
- **Given** a filter on indexed column
- **When** physical operator is selected
- **Then** index scan is considered as alternative
- **And** cheaper option is selected

---

### Requirement: EXPLAIN Cost Output

The system MUST show cost estimates in EXPLAIN output.

#### Scenario: Show costs in EXPLAIN
- **Given** a query with EXPLAIN prefix
- **When** EXPLAIN is executed
- **Then** output shows startup and total cost per operator
- **And** output shows estimated row count per operator

#### Scenario: EXPLAIN ANALYZE shows actuals
- **Given** a query with EXPLAIN ANALYZE prefix
- **When** query is executed with analysis
- **Then** output shows estimated vs actual row counts
- **And** output shows actual execution time per operator

---

### Requirement: Optimizer Performance

The system MUST optimize queries efficiently.

#### Scenario: Fast path for simple queries
- **Given** a single-table query without joins
- **When** query is optimized
- **Then** optimization completes with minimal overhead
- **And** overhead is less than 5% of execution time

#### Scenario: Reasonable optimization time for complex queries
- **Given** a query with 10 joined tables
- **When** query is optimized
- **Then** optimization completes in under 1 second

---

### Requirement: Statistics Persistence

The system MUST persist statistics to storage and restore them on database open.

#### Scenario: Save statistics to catalog
- **Given** statistics have been collected
- **When** CHECKPOINT is executed
- **Then** statistics are serialized and stored in catalog metadata
- **And** storage format is versioned for future compatibility

#### Scenario: Load statistics on database open
- **Given** a database with persisted statistics
- **When** the database is opened
- **Then** statistics are loaded from catalog metadata
- **And** missing statistics use default estimates

#### Scenario: Statistics migration
- **Given** a database with older statistics format
- **When** the database is opened
- **Then** statistics are migrated to new format if needed
- **And** migration failures use fresh ANALYZE as fallback

---

### Requirement: Subquery Decorrelation

The system MUST convert correlated subqueries to JOINs for efficient execution.

#### Scenario: Decorrelate EXISTS subquery
- **Given** a query with EXISTS (SELECT ... WHERE correlated.col = outer.col)
- **When** the query is optimized
- **Then** subquery is converted to SEMI JOIN
- **And** results are semantically equivalent

#### Scenario: Decorrelate SCALAR subquery
- **Given** a query with SCALAR subquery referencing outer table
- **When** the query is optimized
- **Then** subquery is converted to correlated JOIN
- **And** NULL handling matches original semantics

#### Scenario: Decorrelate ANY subquery
- **Given** a query with col = ANY (SELECT ... WHERE correlated.col = outer.col)
- **When** the query is optimized
- **Then** subquery is converted to IN JOIN
- **And** empty subquery results handled correctly

#### Scenario: Preserve semantics for non-correlated subqueries
- **Given** a query with non-correlated subquery
- **When** the query is optimized
- **Then** subquery is NOT decorrelated
- **And** execution uses standard subquery evaluation

---

### Requirement: Predicate Pushdown

The system MUST push filters to the lowest possible level in the query plan.

#### Scenario: Push filter to table scan
- **Given** a query with WHERE filter on a table
- **When** the query is optimized
- **Then** filter is pushed to table scan operator
- **And** unnecessary rows are filtered before other operations

#### Scenario: Push filter past join
- **Given** a query with filter that can be evaluated before join
- **When** the query is optimized
- **Then** filter is pushed to appropriate input of join
- **And** join input cardinality is reduced

#### Scenario: Preserve filter order for complex predicates
- **Given** a query with AND/OR filter tree
- **When** the query is optimized
- **Then** filter pushdown respects filter dependencies
- **And** correctness is maintained

#### Scenario: No pushdown for dependent filters
- **Given** a filter that references columns from both join inputs
- **When** the query is optimized
- **Then** filter is NOT pushed past the join
- **And** filter remains at join level

---

### Requirement: Multi-Column Statistics

The system MUST collect and use statistics on column combinations.

#### Scenario: Collect joint NDV
- **Given** a table with multiple columns
- **When** ANALYZE is run with multi-column stats
- **Then** number of distinct value combinations is estimated
- **And** statistics are stored for future queries

#### Scenario: Use multi-column stats for correlated predicates
- **Given** a query with predicates on correlated columns (e.g., a = 1 AND b = 2)
- **When** cardinality is estimated
- **Then** multi-column statistics are used for selectivity
- **And** estimates account for column correlation

#### Scenario: Handle missing multi-column stats
- **Given** a query with correlated predicates
- **When** multi-column statistics are not available
- **Then** system falls back to independence assumption
- **And** warning is logged for debugging

---

### Requirement: Cardinality Learning

The system MUST learn from actual execution cardinalities to improve future estimates.

#### Scenario: Track actual vs estimated cardinalities
- **Given** a query is executed
- **When** execution completes
- **Then** actual row counts are compared to estimates
- **And** corrections are stored for future use

#### Scenario: Apply learned corrections
- **Given** a learned correction exists for an operator
- **When** a similar query is planned
- **Then** correction multiplier is applied to estimate
- **And** estimates improve over time

#### Scenario: Adaptive cost constants
- **Given** execution timing data is collected
- **When** planning future queries
- **Then** cost constants are adjusted based on actual performance
- **And** plans adapt to hardware characteristics

#### Scenario: Learning configuration
- **Given** cardinality learning is enabled
- **When** maximum history size is exceeded
- **Then** oldest corrections are evicted
- **And** memory usage is bounded

---

### Requirement: Auto-Update Statistics

The system MUST automatically update statistics when significant data changes occur.

#### Scenario: Trigger after row modifications
- **Given** statistics exist for a table
- **When** more than 10% of rows are inserted, updated, or deleted
- **Then** statistics are automatically refreshed
- **And** refresh uses incremental sampling when table is large

#### Scenario: Batch multiple modifications
- **Given** multiple modifications occur in quick succession
- **When** modification count exceeds threshold
- **Then** single ANALYZE is triggered
- **And** excessive ANALYZE calls are prevented

#### Scenario: Disable auto-update
- **Given** auto-update statistics is disabled
- **When** data modifications occur
- **Then** statistics are NOT automatically updated
- **And** manual ANALYZE is required
