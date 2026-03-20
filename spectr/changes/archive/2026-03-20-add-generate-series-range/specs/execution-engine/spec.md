## ADDED Requirements

### Requirement: generate_series Table Function

The engine SHALL provide a generate_series(start, stop[, step]) table function that produces sequential values inclusive of the stop value, supporting INTEGER, BIGINT, DATE, and TIMESTAMP types.

#### Scenario: Integer series with default step

- WHEN executing `SELECT * FROM generate_series(1, 5)`
- THEN the result contains rows: 1, 2, 3, 4, 5

#### Scenario: Integer series with explicit step

- WHEN executing `SELECT * FROM generate_series(0, 10, 3)`
- THEN the result contains rows: 0, 3, 6, 9

#### Scenario: Descending integer series

- WHEN executing `SELECT * FROM generate_series(5, 1, -1)`
- THEN the result contains rows: 5, 4, 3, 2, 1

#### Scenario: Date series with interval step

- WHEN executing `SELECT * FROM generate_series(DATE '2024-01-01', DATE '2024-01-03', INTERVAL '1 day')`
- THEN the result contains rows: 2024-01-01, 2024-01-02, 2024-01-03

#### Scenario: Timestamp series

- WHEN executing `SELECT * FROM generate_series(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 02:00:00', INTERVAL '1 hour')`
- THEN the result contains rows: 2024-01-01 00:00:00, 2024-01-01 01:00:00, 2024-01-01 02:00:00

#### Scenario: Single value when start equals stop

- WHEN executing `SELECT * FROM generate_series(5, 5)`
- THEN the result contains a single row: 5

#### Scenario: Empty result when direction mismatches step

- WHEN executing `SELECT * FROM generate_series(5, 1, 1)`
- THEN the result is empty (start > stop with positive step)

#### Scenario: Error on zero step

- WHEN executing `SELECT * FROM generate_series(1, 10, 0)`
- THEN an error is returned indicating step size cannot be zero

#### Scenario: Column named after function

- WHEN executing `SELECT generate_series FROM generate_series(1, 3)`
- THEN the output column is named "generate_series" and contains 1, 2, 3

### Requirement: range Table Function

The engine SHALL provide a range(start, stop[, step]) table function that produces sequential values exclusive of the stop value, supporting INTEGER, BIGINT, DATE, and TIMESTAMP types.

#### Scenario: Integer range with default step

- WHEN executing `SELECT * FROM range(1, 5)`
- THEN the result contains rows: 1, 2, 3, 4 (excludes 5)

#### Scenario: Integer range with explicit step

- WHEN executing `SELECT * FROM range(0, 10, 3)`
- THEN the result contains rows: 0, 3, 6, 9

#### Scenario: Empty range when start equals stop

- WHEN executing `SELECT * FROM range(5, 5)`
- THEN the result is empty (exclusive of stop)

#### Scenario: Descending range

- WHEN executing `SELECT * FROM range(5, 1, -1)`
- THEN the result contains rows: 5, 4, 3, 2 (excludes 1)

#### Scenario: Date range

- WHEN executing `SELECT * FROM range(DATE '2024-01-01', DATE '2024-01-04', INTERVAL '1 day')`
- THEN the result contains rows: 2024-01-01, 2024-01-02, 2024-01-03 (excludes 2024-01-04)
