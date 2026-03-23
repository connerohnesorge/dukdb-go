# GROUP BY ALL

## ADDED Requirements

### Requirement: GROUP BY ALL SHALL auto-group by non-aggregate columns

GROUP BY ALL SHALL automatically include all non-aggregate columns from the SELECT list as grouping columns. Aggregate functions (SUM, COUNT, AVG, etc.) and window functions MUST be excluded from the grouping set.

#### Scenario: Simple group by all

Given a table t(a INTEGER, b INTEGER, c INTEGER)
When the user executes `SELECT a, SUM(b) FROM t GROUP BY ALL`
Then the query MUST group by column a only

#### Scenario: Multiple non-aggregate columns

Given a table t(a INTEGER, b INTEGER, c INTEGER)
When the user executes `SELECT a, b, COUNT(*) FROM t GROUP BY ALL`
Then the query MUST group by columns a and b

#### Scenario: All aggregates

Given a table t(a INTEGER, b INTEGER)
When the user executes `SELECT SUM(a), AVG(b) FROM t GROUP BY ALL`
Then the query MUST execute with no grouping columns (whole-table aggregate)

#### Scenario: Expression grouping

Given a table t(a INTEGER, b INTEGER)
When the user executes `SELECT a + 1, SUM(b) FROM t GROUP BY ALL`
Then the query MUST group by the expression a + 1
