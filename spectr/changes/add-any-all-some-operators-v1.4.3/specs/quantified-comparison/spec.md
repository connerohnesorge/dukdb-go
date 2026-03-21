# Quantified Comparison Operators

## ADDED Requirements

### Requirement: ANY operator SHALL match at least one row

The expression `expr op ANY(subquery)` SHALL return true if the comparison is true for at least one row returned by the subquery. SOME SHALL be an alias for ANY.

#### Scenario: ANY with equality

Given a running database
When the user executes `SELECT 1 = ANY(SELECT 1 UNION ALL SELECT 2)`
Then the result MUST be true

#### Scenario: ANY with no match

Given a running database
When the user executes `SELECT 3 = ANY(SELECT 1 UNION ALL SELECT 2)`
Then the result MUST be false

#### Scenario: SOME is alias for ANY

Given a running database
When the user executes `SELECT 1 = SOME(SELECT 1 UNION ALL SELECT 2)`
Then the result MUST be true

### Requirement: ALL operator SHALL match every row

The expression `expr op ALL(subquery)` SHALL return true if the comparison is true for every row returned by the subquery. ALL of an empty result MUST return true (vacuous truth).

#### Scenario: ALL with all matching

Given a running database
When the user executes `SELECT 3 > ALL(SELECT 1 UNION ALL SELECT 2)`
Then the result MUST be true

#### Scenario: ALL with empty set

Given a table empty_t with no rows
When the user executes `SELECT 1 = ALL(SELECT * FROM empty_t)`
Then the result MUST be true

### Requirement: Quantified comparisons SHALL handle NULL correctly

When the left operand is NULL, the result SHALL be NULL. When comparing against NULL subquery rows, standard SQL three-valued logic MUST apply.

#### Scenario: NULL left operand

Given a running database
When the user executes `SELECT NULL = ANY(SELECT 1)`
Then the result MUST be NULL
