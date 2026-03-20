# Recursive Cte Specification

## Requirements

### Requirement: Cycle Detection with USING KEY

The executor SHALL support cycle detection in recursive CTEs using the USING KEY syntax to prevent infinite recursion in graph traversal algorithms.

#### Scenario: Basic cycle detection with USING KEY
- GIVEN a recursive CTE with `WITH RECURSIVE cte AS (SELECT ... UNION ALL SELECT ... FROM cte) ... USING KEY (id)`
- WHEN executing a query that would create cycles (e.g., graph traversal returning to same node)
- THEN the executor SHALL use the specified KEY column to detect cycles
- AND rows that would duplicate an already-visited KEY value are excluded from output
- AND the query completes without infinite recursion

#### Scenario: USING KEY with multiple columns
- GIVEN a recursive CTE with `... USING KEY (node_id, direction)`
- WHEN executing a query where cycle detection should consider composite keys
- THEN the executor SHALL use all specified columns as the composite cycle detection key
- AND rows matching any previously-seen key combination are excluded

#### Scenario: USING KEY performance benefit
- GIVEN a recursive query on a 100,000-node graph with USING KEY specified
- WHEN the query is executed
- THEN the executor SHALL complete in sub-second time (< 1 second)
- AND memory usage remains bounded during recursion

### Requirement: MAX_RECURSION Control

The executor SHALL support MAX_RECURSION option to enforce hard limits on recursion depth, preventing accidental infinite loops.

#### Scenario: MAX_RECURSION limit enforcement
- GIVEN a recursive CTE with `SELECT ... OPTION (MAX_RECURSION 10)`
- WHEN the query would recurse beyond 10 iterations
- THEN the executor SHALL stop after 10 iterations
- AND raise an error with message "recursion limit exceeded: max 10 iterations"
- AND all rows computed up to iteration 10 are returned

#### Scenario: MAX_RECURSION with valid termination
- GIVEN a recursive CTE with `... OPTION (MAX_RECURSION 100)` and a query that terminates at iteration 5
- WHEN executing the query
- THEN the query completes successfully after iteration 5
- AND RowsAffected returns the total number of rows generated

### Requirement: Complex Recursive Patterns with JOINs

The executor SHALL handle recursive CTEs that perform JOINs with other tables in the recursive part.

#### Scenario: Recursive CTE with table JOIN
- GIVEN a recursive CTE: `WITH RECURSIVE cte AS (SELECT id FROM nodes WHERE parent IS NULL UNION ALL SELECT n.id FROM nodes n JOIN cte c ON n.parent_id = c.id) SELECT * FROM cte`
- WHEN executing this query against a nodes table with parent-child relationships
- THEN the executor SHALL correctly:
  - Execute the anchor (base) part: find root nodes
  - Execute the recursive part: find children of previously-found nodes by joining with original table
  - Continue iterations until no new rows are found
  - Return all nodes in the tree (hierarchical traversal)

#### Scenario: Recursive CTE with aggregation in recursive part
- GIVEN a recursive CTE with aggregation in the recursive part (e.g., `GROUP BY` in recursive SELECT)
- WHEN executing the query
- THEN the executor SHALL correctly compute aggregates
- AND return one row per distinct aggregation group per iteration

#### Scenario: Multiple recursive references in single CTE
- GIVEN a recursive CTE that references itself multiple times: `WITH RECURSIVE cte AS (...UNION ALL SELECT * FROM cte WHERE cond1 UNION ALL SELECT * FROM cte WHERE cond2)`
- WHEN executing the query
- THEN the executor SHALL treat both references as the same CTE instance (current work table)
- AND combine results from both recursive parts

### Requirement: Memory Pooling for Bounded Recursion

The executor SHALL implement memory pooling for work tables to ensure bounded memory usage even for deep recursion on large datasets.

#### Scenario: Memory-efficient deep recursion
- GIVEN a recursive query with 1000 recursion levels on a 1M-row base table
- WHEN executing the query
- THEN memory usage SHALL remain bounded (< 500MB for typical column types)
- AND work table memory is reused across iterations
- AND old work tables are garbage collected after each iteration

#### Scenario: Work table reuse across iterations
- GIVEN a recursive CTE executing iterations 1 through 100
- WHEN iteration N+1 begins
- THEN the work table from iteration N is either:
  - Reused for iteration N+1 output (if sizes allow), OR
  - Deallocated and a new table allocated
- AND deallocated memory is freed back to the Go runtime

#### Scenario: Memory usage scales linearly with recursion depth
- GIVEN recursive queries with depths 10, 100, 1000, 10000
- WHEN measuring peak memory during execution
- THEN memory usage SHALL scale linearly (not exponentially) with depth
- AND memory per iteration remains constant (amortized)

### Requirement: Streaming Results During Recursion

The executor SHALL support streaming output results while recursion continues, enabling analysis of partial results before recursion completes.

#### Scenario: Results streamed as iterations complete
- GIVEN a recursive CTE with application using the DataChunk streaming API
- WHEN calling Next() on the recursive CTE operator
- THEN DataChunks become available as each iteration completes
- AND application code can process results without waiting for full recursion

#### Scenario: Partial result cancellation
- GIVEN a recursive query executing with streaming
- WHEN the application calls Stop() or closes the connection after receiving N chunks
- THEN remaining recursion iterations are cancelled
- AND resources are freed immediately

### Requirement: Basic Recursive CTE Support

The system MUST support basic recursive CTEs including anchor members, recursive members, and UNION ALL combination.

#### Scenario: Verify Basic Recursion
- **Given** a recursive CTE definition
- **When** executed
- **Then** it MUST execute the anchor member
- **And** it MUST iteratively execute the recursive member
- **And** it MUST terminate when no new rows are produced

#### Scenario: Verify Termination
- **Given** a recursive CTE with a termination condition
- **When** executed
- **Then** it MUST stop recursion when the condition is met

### Requirement: Column Specification

The system MUST support explicit column specification in recursive CTEs.

#### Scenario: Verify Column Alias
- **Given** a recursive CTE with column aliases
- **When** executed
- **Then** it MUST use the specified column names in the result set
- **And** it MUST handle type casting if specified

### Requirement: Hierarchical Queries

The system MUST efficiently handle hierarchical data structures.

#### Scenario: Traverse Hierarchy
- **Given** a hierarchical dataset (e.g., employees)
- **When** a recursive CTE is executed
- **Then** it MUST correctly traverse the hierarchy levels
- **And** it MUST maintain parent-child relationships

### Requirement: Graph Traversal

The system MUST support graph traversal queries.

#### Scenario: Traverse Graph
- **Given** a graph dataset (nodes and edges)
- **When** a recursive CTE is executed
- **Then** it MUST find paths between nodes
- **And** it MUST handle cycles if detection is implemented

### Requirement: USING KEY Optimization

The system MUST implement the USING KEY optimization for performance.

#### Scenario: Verify Optimization
- **Given** a recursive CTE with USING KEY
- **When** executed on a large graph
- **Then** it MUST use the key to filter inferior rows
- **And** it MUST show performance improvement over standard recursion

### Requirement: Cycle Detection

The system MUST detect and handle cycles in recursive queries.

#### Scenario: Detect Cycle
- **Given** a recursive query on a cyclic graph
- **When** executed
- **Then** it SHOULD detect the cycle
- **And** it SHOULD prevent infinite loops

### Requirement: Performance Requirements

The recursive CTE implementation MUST meet performance targets.

#### Scenario: Verify Performance
- **Given** a standard recursion workload
- **When** executed
- **Then** it MUST complete within defined time limits (e.g., 100ms for 100 levels)
- **And** it MUST scale linearly with depth

### Requirement: Error Handling

The system MUST provide clear error messages for common issues.

#### Scenario: Handle Max Depth
- **Given** a recursion exceeding the limit
- **When** executed
- **Then** it MUST return a "Maximum Recursion Depth" error

#### Scenario: Handle Type Mismatch
- **Given** mismatched types between anchor and recursive members
- **When** executed
- **Then** it MUST return a "Type Mismatch" error

### Requirement: Integration Requirements

Recursive CTEs MUST integrate seamlessly with existing features.

#### Scenario: Verify Integration
- **Given** a recursive CTE
- **When** used in transactions, views, or subqueries
- **Then** it MUST function correctly and respect isolation levels
