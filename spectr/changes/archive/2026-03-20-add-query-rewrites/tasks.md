## 1. Rewrite Engine Foundation

- [ ] 1.1 Design rule interface and registration system
- [ ] 1.2 Implement rule matcher (pattern matching for plan nodes)
- [ ] 1.3 Implement rule application mechanism with fixed-point iteration
- [ ] 1.4 Add safety checks to prevent infinite rewrite loops
- [ ] 1.5 Implement cost-aware rule pruning (skip rewrites that increase cost)
- [ ] 1.6 Write tests for rule engine on simple expressions and plans

## 2. Expression Rewrite Rules

- [ ] 2.1 Implement constant folding (e.g., 1 + 2 → 3)
- [ ] 2.2 Implement boolean simplification (e.g., x AND TRUE → x, x OR FALSE → x)
- [ ] 2.3 Implement NULL propagation (e.g., x AND NULL → NULL for unknown, x OR TRUE → TRUE)
- [ ] 2.4 Implement comparison simplification (e.g., x = x → TRUE, x < x → FALSE)
- [ ] 2.5 Implement logical reordering (e.g., (x OR y) AND x → x)
- [ ] 2.6 Implement arithmetic identities (e.g., x * 1 → x, x + 0 → x)
- [ ] 2.7 Implement De Morgan's laws (e.g., NOT(x AND y) → (NOT x OR NOT y))
- [ ] 2.8 Write comprehensive tests for expression rewriting

## 3. Filter and Predicate Rewrites

- [ ] 3.1 Implement predicate pushdown (already exists, may refactor into engine)
- [ ] 3.2 Implement filter combination (merge adjacent filters on same table)
- [ ] 3.3 Implement filter hoisting (move filters before joins/unions)
- [ ] 3.4 Implement IN list simplification (convert IN to = for single element)
- [ ] 3.5 Implement BETWEEN conversion (x BETWEEN a AND b equivalent forms)
- [ ] 3.6 Implement range predicate merging (x > 5 AND x > 10 → x > 10)
- [ ] 3.7 Write tests for filter rewriting with complex WHERE clauses

## 4. Join Rewrites

- [ ] 4.1 Implement elimination of redundant joins (join + filter that eliminates one side)
- [ ] 4.2 Implement self-join elimination (joining table to itself with filter)
- [ ] 4.3 Implement INNER JOIN to SEMI JOIN conversion (when only left columns needed)
- [ ] 4.4 Implement INNER JOIN to ANTI JOIN conversion (NOT EXISTS pattern)
- [ ] 4.5 Implement outer join conversion (LEFT to INNER when right side has WHERE)
- [ ] 4.6 Implement join predicate movement (move join conditions to filter when valid)
- [ ] 4.7 Write tests for join rewriting with various join types

## 5. Aggregation Rewrites

- [ ] 5.1 Implement removal of redundant aggregates
- [ ] 5.2 Implement GROUP BY elimination (GROUP BY on unique key → no group)
- [ ] 5.3 Implement aggregate simplification (SUM of 1 → COUNT, etc.)
- [ ] 5.4 Implement MIN/MAX with index pushdown
- [ ] 5.5 Implement removal of DISTINCT when not needed
- [ ] 5.6 Implement correlated aggregate detection
- [ ] 5.7 Write tests for aggregation rewriting

## 6. Set Operation Rewrites

- [ ] 6.1 Implement elimination of redundant UNION ALL (union with same operand)
- [ ] 6.2 Implement UNION/UNION ALL conversion based on data
- [ ] 6.3 Implement EXCEPT simplification
- [ ] 6.4 Implement INTERSECT simplification
- [ ] 6.5 Write tests for set operation rewriting

## 7. Subquery Rewrites

- [ ] 7.1 Implement removal of unnecessary outer subqueries
- [ ] 7.2 Implement scalar subquery flattening when appropriate
- [ ] 7.3 Implement IN subquery to JOIN conversion
- [ ] 7.4 Implement EXISTS to ANY conversion
- [ ] 7.5 Implement uncorrelated subquery hoisting
- [ ] 7.6 Write tests for subquery rewriting with various patterns

## 8. View and Expression Rewrites

- [ ] 8.1 Implement column alias consolidation
- [ ] 8.2 Implement removal of unused columns in projection
- [ ] 8.3 Implement tautology elimination (always true conditions)
- [ ] 8.4 Implement contradiction elimination (always false conditions)
- [ ] 8.5 Implement common subexpression extraction
- [ ] 8.6 Write tests for view and expression rewrites

## 9. Planner Integration

- [ ] 9.1 Integrate rewrite engine into logical planning phase
- [ ] 9.2 Add rewrite configuration options (enable/disable rules)
- [ ] 9.3 Add PRAGMA-based rule control (e.g., PRAGMA enable_algebraic_rewrites = true)
- [ ] 9.4 Add safety mechanism to prevent runaway rewriting
- [ ] 9.5 Add rule application stats for debugging

## 10. Diagnostics and EXPLAIN

- [ ] 10.1 Add EXPLAIN output showing applied rewrite rules
- [ ] 10.2 Show rule application order and transformations
- [ ] 10.3 Track cost impact of each rewrite
- [ ] 10.4 Add special EXPLAIN mode for rewrite diagnostics
- [ ] 10.5 Log skipped rules and reasons

## 11. Testing and Validation

- [ ] 11.1 Test expression rewrites on all data types
- [ ] 11.2 Test filter rewrites on single and multi-table queries
- [ ] 11.3 Test join rewrites preserve semantics
- [ ] 11.4 Test aggregation rewrites with various functions
- [ ] 11.5 Test subquery rewrites with correlated and uncorrelated
- [ ] 11.6 Test rewrite combinations (multiple rewrites on same query)
- [ ] 11.7 Test correctness: rewritten results match original
- [ ] 11.8 Test performance: rewritten plans execute faster
- [ ] 11.9 Equivalence checking against reference DuckDB implementation

## 12. Documentation and Examples

- [ ] 12.1 Document all rewrite rules with before/after examples
- [ ] 12.2 Create performance comparison benchmarks
- [ ] 12.3 Write troubleshooting guide for unexpected plans
- [ ] 12.4 Add examples to EXPLAIN documentation
