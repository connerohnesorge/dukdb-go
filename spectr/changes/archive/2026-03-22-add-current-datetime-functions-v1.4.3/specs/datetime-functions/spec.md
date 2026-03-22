# Current Date/Time Functions

## ADDED Requirements

### Requirement: NOW and CURRENT_TIMESTAMP SHALL return current timestamp

The NOW() and CURRENT_TIMESTAMP functions SHALL return the current date and time as a TIMESTAMP value. CURRENT_TIMESTAMP MUST work both with and without parentheses per the SQL standard.

#### Scenario: NOW returns current timestamp

Given a running database
When the user executes `SELECT NOW()`
Then the result MUST be a non-null TIMESTAMP value within 1 second of the actual current time

#### Scenario: CURRENT_TIMESTAMP bare keyword

Given a running database
When the user executes `SELECT CURRENT_TIMESTAMP`
Then the result MUST be equivalent to `SELECT NOW()`

#### Scenario: CURRENT_TIMESTAMP with parentheses

Given a running database
When the user executes `SELECT CURRENT_TIMESTAMP()`
Then the result MUST be equivalent to `SELECT NOW()`

### Requirement: CURRENT_DATE and TODAY SHALL return current date

The CURRENT_DATE function SHALL return the current date as a DATE value with time components set to zero. TODAY() SHALL be an alias for CURRENT_DATE. CURRENT_DATE MUST work without parentheses per the SQL standard.

#### Scenario: CURRENT_DATE returns today

Given a running database
When the user executes `SELECT CURRENT_DATE`
Then the result MUST be a DATE value representing the current calendar date

#### Scenario: TODAY returns same as CURRENT_DATE

Given a running database
When the user executes `SELECT TODAY()`
Then the result MUST be equivalent to `SELECT CURRENT_DATE`

### Requirement: CURRENT_TIME SHALL return current time

The CURRENT_TIME function SHALL return the current time of day as a TIME value. CURRENT_TIME MUST work without parentheses per the SQL standard.

#### Scenario: CURRENT_TIME returns time of day

Given a running database
When the user executes `SELECT CURRENT_TIME`
Then the result MUST be a TIME value representing the current time of day
