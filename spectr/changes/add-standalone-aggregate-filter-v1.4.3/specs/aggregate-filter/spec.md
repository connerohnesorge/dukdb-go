# Aggregate Filter Clause

## ADDED Requirements

### Requirement: FILTER clause on standalone aggregates

The FILTER (WHERE ...) clause SHALL be supported on non-window aggregate function calls. It MUST filter which rows are included in the aggregate computation.

#### Scenario: COUNT with FILTER

Given a table `users(id INTEGER, active BOOLEAN)` with rows (1, true), (2, false), (3, true)
When `SELECT COUNT(*) FILTER (WHERE active) FROM users` is executed
Then the result MUST be 2

#### Scenario: SUM with FILTER

Given a table `transactions(amount DOUBLE, type VARCHAR)` with rows (100, 'credit'), (50, 'debit'), (200, 'credit')
When `SELECT SUM(amount) FILTER (WHERE type = 'credit') FROM transactions` is executed
Then the result MUST be 300

#### Scenario: FILTER with GROUP BY

Given a table `employees(dept VARCHAR, active BOOLEAN)` with mixed data
When `SELECT dept, COUNT(*) FILTER (WHERE active) FROM employees GROUP BY dept` is executed
Then each group MUST only count rows where active is true

### Requirement: FILTER backward compatibility with window aggregates

The FILTER clause SHALL continue to work with window aggregate functions using OVER clause.

#### Scenario: FILTER with OVER clause

When `SELECT COUNT(*) FILTER (WHERE x > 0) OVER () FROM t` is executed
Then the FILTER MUST apply within the window frame as before
