# Aggregate Fixes Specification

## Requirements

### Requirement: Implemented aggregates SHALL be registered in isAggregateFunc

All aggregate functions that have implementations in computeAggregate() MUST be registered in isAggregateFunc() so the planner correctly identifies them as aggregate functions.

#### Scenario: Boolean aggregates recognized

Given a running database with table t(x BOOLEAN)
When the user executes `SELECT BOOL_AND(x) FROM t`
Then the query MUST execute without error and return a boolean result

#### Scenario: Bitwise aggregates recognized

Given a running database with table t(x INTEGER)
When the user executes `SELECT BIT_AND(x), BIT_OR(x), BIT_XOR(x) FROM t`
Then the query MUST execute without error

#### Scenario: Regression aggregates recognized

Given a running database with table t(x DOUBLE, y DOUBLE)
When the user executes `SELECT REGR_COUNT(y, x), REGR_AVGX(y, x), REGR_AVGY(y, x) FROM t`
Then the query MUST execute without error

### Requirement: GEOMETRIC_MEAN SHALL compute nth root of product

GEOMETRIC_MEAN(x) and its alias GEOMEAN(x) SHALL return the geometric mean of all non-null values. The geometric mean MUST be computed as exp(avg(ln(x))).

#### Scenario: Geometric mean of two values

Given a running database
When the user executes `SELECT GEOMETRIC_MEAN(x) FROM (VALUES (2), (8)) t(x)`
Then the result MUST be 4.0

### Requirement: WEIGHTED_AVG SHALL compute weighted average

WEIGHTED_AVG(value, weight) SHALL return sum(value * weight) / sum(weight) for all non-null pairs.

#### Scenario: Weighted average

Given a running database
When the user executes `SELECT WEIGHTED_AVG(score, weight) FROM (VALUES (90, 3), (80, 1)) t(score, weight)`
Then the result MUST be 87.5

### Requirement: ARBITRARY and MEAN SHALL be aggregate aliases

ARBITRARY SHALL be an alias for FIRST (returns an arbitrary non-null value). MEAN SHALL be an alias for AVG.

#### Scenario: ARBITRARY returns a value

Given a running database
When the user executes `SELECT ARBITRARY(x) FROM (VALUES (1), (2), (3)) t(x)`
Then the result MUST be a non-null integer value from the input

#### Scenario: MEAN equals AVG

Given a running database
When the user executes `SELECT MEAN(x) FROM (VALUES (10), (20), (30)) t(x)`
Then the result MUST be 20.0

