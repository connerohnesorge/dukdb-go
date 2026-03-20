# Query Rewrite Transformations - Delta Spec

## ADDED Requirements

### Requirement: Rule-Based Query Rewriting Engine

The system MUST provide a framework for applying semantic-preserving query transformations.

#### Scenario: Apply constant folding rule
- GIVEN a query with expression `1 + 2 * 3`
- WHEN rewrite engine processes it
- THEN expression is rewritten to `7`
- AND result is equivalent to original

#### Scenario: Skip rule if it increases cost
- GIVEN a query where a rewrite would increase execution cost
- WHEN rewrite engine evaluates the rule
- THEN rule application is skipped
- AND original plan is kept

#### Scenario: Apply multiple rewrite rules
- GIVEN a query with multiple optimization opportunities
- WHEN rewrite engine processes it
- THEN all applicable rules are applied iteratively
- AND fixed-point iteration terminates
- AND result is fully optimized

#### Scenario: Prevent infinite rewrite loops
- GIVEN a pathological rewrite configuration
- WHEN rewrite engine runs
- THEN iteration count is limited to threshold (default 100)
- AND rewrite process terminates
- AND warning is logged

---

### Requirement: Expression Simplification Rewrites

The system MUST simplify expressions to reduce computation.

#### Scenario: Boolean simplification for AND
- GIVEN expressions `x AND TRUE`, `x AND FALSE`
- WHEN rewrite engine processes them
- THEN `x AND TRUE` → `x`
- AND `x AND FALSE` → `FALSE`

#### Scenario: Boolean simplification for OR
- GIVEN expressions `x OR TRUE`, `x OR FALSE`
- WHEN rewrite engine processes them
- THEN `x OR TRUE` → `TRUE`
- AND `x OR FALSE` → `x`

#### Scenario: NULL propagation
- GIVEN expressions with NULL operands
- WHEN rewrite engine processes them
- THEN `x AND NULL` → `NULL` (when x is not FALSE)
- AND `x OR NULL` → evaluates correctly
- AND `x = NULL` → `NULL` (three-valued logic)

#### Scenario: Arithmetic identity elimination
- GIVEN expressions `x + 0`, `x * 1`, `x / 1`
- WHEN rewrite engine processes them
- THEN `x + 0` → `x`
- AND `x * 1` → `x`
- AND `x / 1` → `x`

#### Scenario: De Morgan's laws
- GIVEN expression `NOT(x AND y)`
- WHEN rewrite engine processes it
- THEN expression is rewritten to `(NOT x) OR (NOT y)`
- AND semantics are preserved

#### Scenario: Comparison tautologies
- GIVEN expressions `x = x`, `x < x`, `x >= x`
- WHEN rewrite engine processes them
- THEN `x = x` → `TRUE`
- AND `x < x` → `FALSE`
- AND `x >= x` → `TRUE`

---

### Requirement: Filter and Predicate Rewrites

The system MUST optimize WHERE clauses through predicate rewriting.

#### Scenario: Combine adjacent filters
- GIVEN adjacent filters `WHERE x > 5` and `WHERE x < 10`
- WHEN rewrite engine processes them
- THEN filters are combined to single `WHERE x > 5 AND x < 10`
- AND plan has fewer filter operators

#### Scenario: IN list to equality conversion
- GIVEN predicate `x IN (5)`
- WHEN rewrite engine processes it
- THEN predicate is rewritten to `x = 5`
- AND plan is simpler

#### Scenario: Range predicate merging
- GIVEN predicates `x > 5 AND x > 10`
- WHEN rewrite engine processes them
- THEN merged to `x > 10` (stronger condition)
- AND redundant check is eliminated

#### Scenario: BETWEEN expansion
- GIVEN predicate `x BETWEEN 5 AND 10`
- WHEN rewrite engine processes it
- THEN can be rewritten to `x >= 5 AND x <= 10` if beneficial
- AND cost comparison determines which form to keep

#### Scenario: Filter hoisting
- GIVEN query with filter after expensive operation
- WHEN rewrite engine processes it
- THEN filter is moved earlier if safe
- AND execution is faster (fewer rows processed)

---

### Requirement: Join Rewrites

The system MUST optimize joins through semantic transformations.

#### Scenario: INNER JOIN to SEMI JOIN
- GIVEN `SELECT a.* FROM a INNER JOIN b ON a.id = b.id`
- WHEN rewrite engine processes it
- THEN converted to SEMI JOIN (return each 'a' at most once)
- AND execution is faster

#### Scenario: Outer JOIN to INNER JOIN
- GIVEN `SELECT * FROM a LEFT JOIN b WHERE b.id IS NOT NULL`
- WHEN rewrite engine processes it
- THEN converted to `INNER JOIN` (WHERE filters out NULLs)
- AND execution is faster

#### Scenario: Self-join elimination
- GIVEN query joining table to itself with matching predicates
- WHEN rewrite engine processes it
- THEN self-join is eliminated
- AND plan is simplified

#### Scenario: Redundant join elimination
- GIVEN query joining table that's filtered out completely
- WHEN rewrite engine processes it
- THEN unnecessary join is removed
- AND execution is faster

#### Scenario: Join predicate pushdown
- GIVEN join condition with predicates on one side only
- WHEN rewrite engine processes it
- THEN predicates are pushed to filter
- AND join input cardinality reduced

---

### Requirement: Aggregation Rewrites

The system MUST optimize GROUP BY and aggregate functions.

#### Scenario: GROUP BY elimination
- GIVEN `SELECT col FROM table GROUP BY col` where col is PRIMARY KEY
- WHEN rewrite engine processes it
- THEN GROUP BY is eliminated (no duplicates possible)
- AND execution skips aggregation

#### Scenario: Aggregate simplification
- GIVEN `SUM(constant)` aggregation
- WHEN rewrite engine processes it
- THEN rewritten to `COUNT(*) * constant`
- AND semantics preserved, execution simpler

#### Scenario: COUNT(DISTINCT) optimization
- GIVEN `SELECT COUNT(DISTINCT x)` aggregation
- WHEN rewrite engine processes it
- THEN rewritten form allows index usage if index exists
- AND execution is faster for large tables

#### Scenario: MIN/MAX pushdown to storage
- GIVEN `SELECT MIN(indexed_col) FROM table` with index
- WHEN rewrite engine processes it
- THEN index is marked for min/max evaluation
- AND executor can use index metadata

#### Scenario: Redundant DISTINCT removal
- GIVEN `SELECT DISTINCT x FROM (SELECT DISTINCT x FROM table)`
- WHEN rewrite engine processes it
- THEN inner DISTINCT is removed
- AND outer DISTINCT remains

---

### Requirement: Set Operation Rewrites

The system MUST optimize UNION, EXCEPT, INTERSECT operations.

#### Scenario: UNION ALL redundancy
- GIVEN `SELECT x FROM a UNION ALL SELECT x FROM a`
- WHEN rewrite engine processes it
- THEN detected as redundant
- AND warning logged (may be user error)

#### Scenario: UNION to UNION ALL conversion
- GIVEN `SELECT x FROM a WHERE x > 5 UNION SELECT x FROM b WHERE x < 5`
- WHEN rewrite engine processes it AND sets are provably disjoint
- THEN converted to `UNION ALL` (no duplicates possible)
- AND execution is faster (no duplicate detection needed)

#### Scenario: EXCEPT simplification
- GIVEN `SELECT x FROM a EXCEPT SELECT x FROM a`
- WHEN rewrite engine processes it
- THEN entire expression is rewritten to empty set
- AND execution is immediate

#### Scenario: INTERSECT simplification
- GIVEN `SELECT x FROM a INTERSECT SELECT x FROM a`
- WHEN rewrite engine processes it
- THEN rewritten to `SELECT x FROM a` (self-intersection is identity)
- AND unnecessary operation eliminated

---

### Requirement: Subquery Rewrites

The system MUST optimize subqueries through flattening and conversion.

#### Scenario: IN subquery to JOIN conversion
- GIVEN `SELECT * FROM a WHERE x IN (SELECT x FROM b)`
- WHEN rewrite engine processes it
- THEN converted to `SELECT a.* FROM a SEMI JOIN b ON a.x = b.x`
- AND execution is faster

#### Scenario: EXISTS to ANY conversion
- GIVEN `SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE a.id = b.id)`
- WHEN rewrite engine processes it
- THEN converted to semantic JOIN when beneficial
- AND execution is optimized

#### Scenario: Uncorrelated subquery hoisting
- GIVEN `SELECT * FROM a WHERE x > (SELECT COUNT(*) FROM b)`
- WHEN rewrite engine processes it AND subquery is uncorrelated
- THEN subquery is evaluated once before main query
- AND execution is faster

#### Scenario: Scalar subquery flattening
- GIVEN `SELECT (SELECT MAX(salary) FROM employees) as max_salary`
- WHEN rewrite engine processes it
- THEN subquery is flattened into main query when safe
- AND execution is simplified

#### Scenario: Correlated subquery detection
- GIVEN `SELECT * FROM a WHERE x IN (SELECT x FROM b WHERE a.id = b.id)`
- WHEN rewrite engine processes it
- THEN correlation is detected
- AND rewrite applies JOIN conversion with proper semantics

---

### Requirement: Configuration and Control

The system MUST provide options to control rewrite behavior.

#### Scenario: Enable/disable rewrites
- GIVEN `PRAGMA enable_rewrites = false`
- WHEN query is planned
- THEN no rewrite rules are applied
- AND plan is generated from unmodified logical plan

#### Scenario: Enable specific rewrite categories
- GIVEN `PRAGMA enable_expression_rewrites = false` but other rewrites enabled
- WHEN query is planned
- THEN expression rewrites are skipped
- AND other rewrites are applied

#### Scenario: Set rewrite iteration limit
- GIVEN `PRAGMA rewrite_iteration_limit = 50`
- WHEN rewrite engine runs
- THEN terminates after 50 iterations
- AND safety is maintained

#### Scenario: Cost threshold for rewrites
- GIVEN `PRAGMA rewrite_cost_threshold = 1.05`
- WHEN rewrite engine evaluates rules
- THEN only rewrites improving cost by > 5% are applied
- AND performance is balanced with stability

---

### Requirement: EXPLAIN Output for Rewrites

The system MUST show applied rewrites for debugging and optimization.

#### Scenario: Show rewrite chain in EXPLAIN
- GIVEN query with multiple rewrites applied
- WHEN EXPLAIN is executed
- THEN output shows sequence of transformations
- AND before/after plans shown

#### Scenario: Show cost improvement from rewrites
- GIVEN rewrite that improves cost
- WHEN EXPLAIN is executed
- THEN "Cost improvement: 40%" shown
- AND aggregate improvement across all rewrites displayed

#### Scenario: Show skipped rewrites
- GIVEN rules that were considered but not applied
- WHEN EXPLAIN REWRITE is executed (special mode)
- THEN skipped rules and reasons shown
- AND useful for optimization analysis

#### Scenario: Rewrite diagnostics mode
- GIVEN EXPLAIN REWRITE_ANALYSIS
- WHEN executed
- THEN shows all attempted rewrites
- AND detailed reasoning for each decision
- AND useful for troubleshooting optimizer behavior

---

### Requirement: Correctness Preservation

The system MUST guarantee rewrites preserve query semantics.

#### Scenario: Rewrites produce identical results
- GIVEN any query with rewrites applied
- WHEN compared to original unoptimized query
- THEN results are identical
- AND NULL handling is preserved
- AND aggregates match exactly

#### Scenario: Exception handling for unsafe rewrites
- GIVEN potential unsupported rewrite scenario
- WHEN rewrite engine detects it
- THEN rewrite is skipped
- AND conservative approach is taken
- AND warning may be logged

#### Scenario: Type coercion preservation
- GIVEN query with type conversions and rewrites
- WHEN rewrite engine processes it
- THEN type semantics are preserved
- AND rewrites don't change implicit conversions

---

### Requirement: Performance Improvement

The system MUST deliver measurable performance benefits.

#### Scenario: Faster execution for simplified queries
- GIVEN query with redundant filters
- WHEN rewritten and executed
- THEN execution time is 30-50% faster
- AND simplifications eliminate unnecessary computation

#### Scenario: Better join execution
- GIVEN query with INNER JOIN convertible to SEMI JOIN
- WHEN rewritten and executed
- THEN execution time is 40-60% faster
- AND simpler join produces fewer output rows

#### Scenario: Faster aggregation
- GIVEN query with GROUP BY on unique key
- WHEN eliminated via rewrite and executed
- THEN execution time is 50-70% faster
- AND aggregation stage is skipped entirely

#### Scenario: Faster subquery evaluation
- GIVEN query with IN subquery converted to JOIN
- WHEN rewritten and executed
- THEN execution time is 20-40% faster
- AND better plan allows parallelization
