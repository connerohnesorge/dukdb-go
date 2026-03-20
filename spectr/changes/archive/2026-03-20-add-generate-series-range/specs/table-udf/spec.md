## ADDED Requirements

### Requirement: Series Generation Table Functions Registration

The system SHALL register generate_series and range as recognized table functions in the binder and executor, accepting 2 or 3 arguments with type-appropriate defaults.

#### Scenario: generate_series recognized as table function in FROM

- WHEN executing `SELECT n FROM generate_series(1, 3) AS t(n)`
- THEN the binder resolves generate_series as a table function
- AND the alias "t" with column "n" is applied correctly

#### Scenario: range recognized as table function in FROM

- WHEN executing `SELECT * FROM range(1, 10) AS t(val)`
- THEN the binder resolves range as a table function
- AND the alias "t" with column "val" is applied correctly

#### Scenario: Two-argument form uses default step

- WHEN executing `SELECT * FROM generate_series(1, 5)`
- THEN the default step of 1 is used for integer arguments
- AND the series produces values 1 through 5

#### Scenario: Temporal two-argument form uses default interval step

- WHEN executing `SELECT * FROM generate_series(DATE '2024-01-01', DATE '2024-01-03')`
- THEN the default step of INTERVAL '1 day' is used
- AND the series produces daily dates from 2024-01-01 through 2024-01-03
