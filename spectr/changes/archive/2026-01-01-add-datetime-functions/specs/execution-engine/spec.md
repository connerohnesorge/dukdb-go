# Execution Engine Specification Delta: Date/Time Functions

## ADDED Requirements

### Requirement: Date Extraction Functions

The engine SHALL evaluate date extraction functions on DATE and TIMESTAMP types.

#### Scenario: YEAR extracts year from date
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT YEAR(date_col) FROM t"
- THEN the result is 2024

#### Scenario: YEAR extracts year from timestamp
- GIVEN a table with TIMESTAMP column containing '2024-03-15 14:30:00'
- WHEN executing "SELECT YEAR(ts_col) FROM t"
- THEN the result is 2024

#### Scenario: MONTH extracts month from date
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT MONTH(date_col) FROM t"
- THEN the result is 3

#### Scenario: DAY extracts day from date
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT DAY(date_col) FROM t"
- THEN the result is 15

#### Scenario: HOUR extracts hour from timestamp
- GIVEN a table with TIMESTAMP column containing '2024-03-15 14:30:45'
- WHEN executing "SELECT HOUR(ts_col) FROM t"
- THEN the result is 14

#### Scenario: MINUTE extracts minute from timestamp
- GIVEN a table with TIMESTAMP column containing '2024-03-15 14:30:45'
- WHEN executing "SELECT MINUTE(ts_col) FROM t"
- THEN the result is 30

#### Scenario: SECOND extracts second with fraction
- GIVEN a table with TIMESTAMP column containing '2024-03-15 14:30:45.123456'
- WHEN executing "SELECT SECOND(ts_col) FROM t"
- THEN the result is 45.123456 (DOUBLE)

#### Scenario: DAYOFWEEK returns correct day
- GIVEN a table with DATE column containing '2024-03-15' (Friday)
- WHEN executing "SELECT DAYOFWEEK(date_col) FROM t"
- THEN the result is 5 (Friday, 0=Sunday)

#### Scenario: DAYOFYEAR returns correct day
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT DAYOFYEAR(date_col) FROM t"
- THEN the result is 75 (leap year)

#### Scenario: WEEK returns ISO week number
- GIVEN a table with DATE column containing '2024-01-01'
- WHEN executing "SELECT WEEK(date_col) FROM t"
- THEN the result is 1 (ISO week)

#### Scenario: QUARTER returns quarter
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT QUARTER(date_col) FROM t"
- THEN the result is 1 (Q1: Jan-Mar)

#### Scenario: Extraction from NULL returns NULL
- GIVEN a table with NULL DATE column
- WHEN executing "SELECT YEAR(date_col) FROM t"
- THEN the result is NULL

### Requirement: Date Arithmetic Functions

The engine SHALL evaluate date arithmetic functions.

#### Scenario: DATE_ADD adds interval to date
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT DATE_ADD(date_col, INTERVAL '5' DAY) FROM t"
- THEN the result is DATE '2024-03-20'

#### Scenario: DATE_ADD adds months correctly
- GIVEN a table with DATE column containing '2024-01-31'
- WHEN executing "SELECT DATE_ADD(date_col, INTERVAL '1' MONTH) FROM t"
- THEN the result is DATE '2024-02-29' (leap year, clamps to month end)

#### Scenario: DATE_SUB subtracts interval from date
- GIVEN a table with DATE column containing '2024-03-15'
- WHEN executing "SELECT DATE_SUB(date_col, INTERVAL '10' DAY) FROM t"
- THEN the result is DATE '2024-03-05'

#### Scenario: DATE_DIFF calculates difference in days
- GIVEN two dates '2024-03-20' and '2024-03-15'
- WHEN executing "SELECT DATE_DIFF('day', DATE '2024-03-15', DATE '2024-03-20')"
- THEN the result is 5

#### Scenario: DATE_DIFF calculates difference in months
- GIVEN two dates '2024-06-15' and '2024-03-15'
- WHEN executing "SELECT DATE_DIFF('month', DATE '2024-03-15', DATE '2024-06-15')"
- THEN the result is 3

#### Scenario: DATE_DIFF with negative difference
- GIVEN two dates where end is before start
- WHEN executing "SELECT DATE_DIFF('day', DATE '2024-03-20', DATE '2024-03-15')"
- THEN the result is -5

#### Scenario: DATE_TRUNC truncates to day
- GIVEN a TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT DATE_TRUNC('day', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is TIMESTAMP '2024-03-15 00:00:00'

#### Scenario: DATE_TRUNC truncates to month
- GIVEN a TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT DATE_TRUNC('month', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is TIMESTAMP '2024-03-01 00:00:00'

#### Scenario: DATE_TRUNC truncates to hour
- GIVEN a TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT DATE_TRUNC('hour', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is TIMESTAMP '2024-03-15 14:00:00'

#### Scenario: DATE_PART extracts part as double
- GIVEN a TIMESTAMP '2024-03-15 14:30:45.5'
- WHEN executing "SELECT DATE_PART('second', TIMESTAMP '2024-03-15 14:30:45.5')"
- THEN the result is 45.5 (DOUBLE)

#### Scenario: AGE calculates interval between timestamps
- GIVEN two timestamps
- WHEN executing "SELECT AGE(TIMESTAMP '2024-03-15', TIMESTAMP '2024-01-15')"
- THEN the result is INTERVAL '2 months'

#### Scenario: LAST_DAY returns last day of month
- GIVEN a DATE '2024-02-15'
- WHEN executing "SELECT LAST_DAY(DATE '2024-02-15')"
- THEN the result is DATE '2024-02-29' (leap year)

#### Scenario: Date arithmetic with NULL returns NULL
- GIVEN a NULL date
- WHEN executing "SELECT DATE_ADD(NULL, INTERVAL '1' DAY)"
- THEN the result is NULL

### Requirement: Date Construction Functions

The engine SHALL construct date/time values from components.

#### Scenario: MAKE_DATE constructs date
- GIVEN year=2024, month=3, day=15
- WHEN executing "SELECT MAKE_DATE(2024, 3, 15)"
- THEN the result is DATE '2024-03-15'

#### Scenario: MAKE_DATE with invalid components
- GIVEN month=13
- WHEN executing "SELECT MAKE_DATE(2024, 13, 15)"
- THEN ErrorTypeExecutor is returned with message about invalid month

#### Scenario: MAKE_DATE with invalid day
- GIVEN February 30
- WHEN executing "SELECT MAKE_DATE(2024, 2, 30)"
- THEN ErrorTypeExecutor is returned with message about invalid day

#### Scenario: MAKE_TIMESTAMP constructs timestamp
- GIVEN full components
- WHEN executing "SELECT MAKE_TIMESTAMP(2024, 3, 15, 14, 30, 45.5)"
- THEN the result is TIMESTAMP '2024-03-15 14:30:45.5'

#### Scenario: MAKE_TIME constructs time
- GIVEN hour=14, minute=30, second=45
- WHEN executing "SELECT MAKE_TIME(14, 30, 45)"
- THEN the result is TIME '14:30:45'

#### Scenario: Construction with NULL returns NULL
- GIVEN NULL for any component
- WHEN executing "SELECT MAKE_DATE(2024, NULL, 15)"
- THEN the result is NULL

### Requirement: Date Formatting Functions

The engine SHALL format dates to strings and parse strings to dates.

#### Scenario: STRFTIME formats with year
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT STRFTIME('%Y-%m-%d', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is VARCHAR '2024-03-15'

#### Scenario: STRFTIME formats with time components
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT STRFTIME('%H:%M:%S', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is VARCHAR '14:30:45'

#### Scenario: STRFTIME with full format
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT STRFTIME('%Y-%m-%d %H:%M:%S', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is VARCHAR '2024-03-15 14:30:45'

#### Scenario: STRFTIME with day name
- GIVEN TIMESTAMP '2024-03-15 14:30:45' (Friday)
- WHEN executing "SELECT STRFTIME('%A', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is VARCHAR 'Friday'

#### Scenario: STRFTIME with month name
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT STRFTIME('%B', TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is VARCHAR 'March'

#### Scenario: STRPTIME parses date string
- GIVEN string '2024-03-15'
- WHEN executing "SELECT STRPTIME('2024-03-15', '%Y-%m-%d')"
- THEN the result is TIMESTAMP '2024-03-15 00:00:00'

#### Scenario: STRPTIME parses datetime string
- GIVEN string '2024-03-15 14:30:45'
- WHEN executing "SELECT STRPTIME('2024-03-15 14:30:45', '%Y-%m-%d %H:%M:%S')"
- THEN the result is TIMESTAMP '2024-03-15 14:30:45'

#### Scenario: STRPTIME with unparseable string
- GIVEN invalid date string
- WHEN executing "SELECT STRPTIME('not-a-date', '%Y-%m-%d')"
- THEN the result is NULL

#### Scenario: Formatting NULL returns NULL
- GIVEN NULL timestamp
- WHEN executing "SELECT STRFTIME('%Y', NULL)"
- THEN the result is NULL

### Requirement: Epoch Conversion Functions

The engine SHALL convert between timestamps and Unix epoch values.

#### Scenario: TO_TIMESTAMP converts epoch seconds
- GIVEN epoch 1710510645 (2024-03-15 14:30:45 UTC)
- WHEN executing "SELECT TO_TIMESTAMP(1710510645)"
- THEN the result is TIMESTAMP '2024-03-15 14:30:45'

#### Scenario: EPOCH extracts seconds from timestamp
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT EPOCH(TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is approximately 1710510645.0 (DOUBLE)

#### Scenario: EPOCH_MS extracts milliseconds
- GIVEN TIMESTAMP '2024-03-15 14:30:45.123'
- WHEN executing "SELECT EPOCH_MS(TIMESTAMP '2024-03-15 14:30:45.123')"
- THEN the result is 1710510645123 (BIGINT)

#### Scenario: Epoch of NULL returns NULL
- GIVEN NULL timestamp
- WHEN executing "SELECT EPOCH(NULL)"
- THEN the result is NULL

### Requirement: EXTRACT Syntax

The engine SHALL support SQL standard EXTRACT syntax.

#### Scenario: EXTRACT year from timestamp
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is 2024.0 (DOUBLE per SQL standard)

#### Scenario: EXTRACT month from date
- GIVEN DATE '2024-03-15'
- WHEN executing "SELECT EXTRACT(MONTH FROM DATE '2024-03-15')"
- THEN the result is 3.0 (DOUBLE)

#### Scenario: EXTRACT epoch from timestamp
- GIVEN TIMESTAMP '2024-03-15 14:30:45'
- WHEN executing "SELECT EXTRACT(EPOCH FROM TIMESTAMP '2024-03-15 14:30:45')"
- THEN the result is approximately 1710510645.0 (DOUBLE)

### Requirement: Interval Parsing

The engine SHALL parse INTERVAL literals.

#### Scenario: Parse single-unit interval
- GIVEN the SQL "SELECT INTERVAL '5' DAY"
- WHEN executing the statement
- THEN the result is an INTERVAL value of 5 days

#### Scenario: Parse hour interval
- GIVEN the SQL "SELECT INTERVAL '3' HOUR"
- WHEN executing the statement
- THEN the result is an INTERVAL value of 3 hours

#### Scenario: Parse string interval
- GIVEN the SQL "SELECT INTERVAL '1 day'"
- WHEN executing the statement
- THEN the result is an INTERVAL value of 1 day

#### Scenario: Parse compound interval
- GIVEN the SQL "SELECT INTERVAL '2 hours 30 minutes'"
- WHEN executing the statement
- THEN the result is an INTERVAL value of 2 hours 30 minutes

### Requirement: Interval Arithmetic

The engine SHALL perform arithmetic with INTERVAL values.

#### Scenario: Add interval to date
- GIVEN DATE '2024-03-15' and INTERVAL '5 days'
- WHEN executing "SELECT DATE '2024-03-15' + INTERVAL '5' DAY"
- THEN the result is DATE '2024-03-20'

#### Scenario: Subtract interval from timestamp
- GIVEN TIMESTAMP '2024-03-15 14:30:00' and INTERVAL '2 hours'
- WHEN executing "SELECT TIMESTAMP '2024-03-15 14:30:00' - INTERVAL '2' HOUR"
- THEN the result is TIMESTAMP '2024-03-15 12:30:00'

#### Scenario: Multiply interval
- GIVEN INTERVAL '1' DAY and multiplier 5
- WHEN executing "SELECT INTERVAL '1' DAY * 5"
- THEN the result is INTERVAL '5 days'

### Requirement: Interval Extraction Functions

The engine SHALL extract components from INTERVAL values.

#### Scenario: TO_YEARS extracts years
- GIVEN INTERVAL '2 years 3 months'
- WHEN executing "SELECT TO_YEARS(INTERVAL '2 years 3 months')"
- THEN the result is 2

#### Scenario: TO_MONTHS extracts total months
- GIVEN INTERVAL '2 years 3 months'
- WHEN executing "SELECT TO_MONTHS(INTERVAL '2 years 3 months')"
- THEN the result is 27 (2*12 + 3)

#### Scenario: TO_DAYS extracts days
- GIVEN INTERVAL '5 days 12 hours'
- WHEN executing "SELECT TO_DAYS(INTERVAL '5 days 12 hours')"
- THEN the result is 5

#### Scenario: TO_HOURS extracts total hours
- GIVEN INTERVAL '2 days 5 hours'
- WHEN executing "SELECT TO_HOURS(INTERVAL '2 days 5 hours')"
- THEN the result is 53 (2*24 + 5)

### Requirement: Date Function Error Handling

The engine SHALL return appropriate errors for invalid inputs.

#### Scenario: Invalid date part specifier
- GIVEN an unknown part specifier
- WHEN executing "SELECT DATE_PART('invalid', TIMESTAMP '2024-03-15')"
- THEN ErrorTypeBinder is returned with message about invalid date part

#### Scenario: YEAR with wrong type
- GIVEN a VARCHAR input
- WHEN executing "SELECT YEAR('not-a-date')"
- THEN ErrorTypeBinder is returned with message about type mismatch

#### Scenario: DATE_ADD with wrong interval type
- GIVEN an integer instead of interval
- WHEN executing "SELECT DATE_ADD(DATE '2024-03-15', 5)"
- THEN ErrorTypeBinder is returned with message about type mismatch
