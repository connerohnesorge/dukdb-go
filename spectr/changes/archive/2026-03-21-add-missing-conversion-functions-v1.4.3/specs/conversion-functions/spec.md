# Conversion Functions

## ADDED Requirements

### Requirement: TO_DATE function

TO_DATE SHALL parse a string to a DATE value using an optional format string. It MUST support both explicit format and auto-detection of ISO date format.

#### Scenario: TO_DATE with format string

When `SELECT TO_DATE('2024-01-15', '%Y-%m-%d')` is executed
Then the result MUST be a DATE value representing January 15, 2024

#### Scenario: TO_DATE auto-detection

When `SELECT TO_DATE('2024-01-15')` is executed
Then the result MUST be a DATE value representing January 15, 2024

### Requirement: TO_CHAR function

TO_CHAR SHALL format a date, timestamp, or numeric value to a string using a format specifier. It MUST be an alias for STRFTIME for temporal types.

#### Scenario: TO_CHAR with DATE

When `SELECT TO_CHAR(DATE '2024-01-15', '%Y/%m/%d')` is executed
Then the result MUST be '2024/01/15'

#### Scenario: TO_CHAR with TIMESTAMP

When `SELECT TO_CHAR(TIMESTAMP '2024-01-15 10:30:00', '%Y-%m-%d %H:%M')` is executed
Then the result MUST be '2024-01-15 10:30'

### Requirement: GENERATE_SUBSCRIPTS function

GENERATE_SUBSCRIPTS SHALL return 1-based integer indices for the elements of an array.

#### Scenario: GENERATE_SUBSCRIPTS basic

When `SELECT GENERATE_SUBSCRIPTS([10, 20, 30], 1)` is executed
Then the result MUST contain values 1, 2, 3
