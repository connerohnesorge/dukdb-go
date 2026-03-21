# Ordered-Set Aggregates

## ADDED Requirements

### Requirement: WITHIN GROUP syntax for ordered-set aggregates

The parser SHALL support `function(args) WITHIN GROUP (ORDER BY expr)` syntax. The WITHIN GROUP ordering MUST be mapped to the function's ORDER BY field, making it functionally equivalent to internal ORDER BY.

#### Scenario: PERCENTILE_CONT with WITHIN GROUP

When `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) FROM employees` is executed
Then the result MUST equal `SELECT PERCENTILE_CONT(0.5 ORDER BY salary) FROM employees`

#### Scenario: Error when both internal and WITHIN GROUP ORDER BY

When `SELECT STRING_AGG(name, ',' ORDER BY name) WITHIN GROUP (ORDER BY id) FROM t` is executed
Then an error MUST be returned indicating that both internal ORDER BY and WITHIN GROUP cannot be used

### Requirement: LISTAGG aggregate function

LISTAGG(expr [, delimiter]) SHALL concatenate values into a string, with ordering specified via WITHIN GROUP. The default delimiter MUST be an empty string.

#### Scenario: LISTAGG with delimiter and ordering

Given a table with names 'Charlie', 'Alice', 'Bob'
When `SELECT LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) FROM t` is executed
Then the result MUST be 'Alice, Bob, Charlie'

#### Scenario: LISTAGG with default empty delimiter

Given a table with values 'a', 'b', 'c'
When `SELECT LISTAGG(value) WITHIN GROUP (ORDER BY value) FROM t` is executed
Then the result MUST be 'abc'

#### Scenario: LISTAGG NULL handling

Given a table with values 'a', NULL, 'c'
When `SELECT LISTAGG(value, ',') WITHIN GROUP (ORDER BY value) FROM t` is executed
Then the result MUST be 'a,c' (NULL values SHALL be skipped)

#### Scenario: LISTAGG with GROUP BY

When `SELECT dept, LISTAGG(name, ',') WITHIN GROUP (ORDER BY name) FROM emp GROUP BY dept` is executed
Then each department MUST have its employees concatenated in alphabetical order

#### Scenario: LISTAGG on empty input

When `SELECT LISTAGG(name, ',') WITHIN GROUP (ORDER BY name) FROM t WHERE 1=0` is executed
Then the result MUST be NULL
