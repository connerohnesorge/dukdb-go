## ADDED Requirements

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
