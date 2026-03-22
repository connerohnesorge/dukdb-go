# Temporal Functions Round 2

## ADDED Requirements

### Requirement: ISODOW and ISOYEAR date parts SHALL be supported

The DATE_PART function SHALL support 'isodow' (ISO day of week, Monday=1 through Sunday=7) and 'isoyear' (ISO year) as date part specifiers. DATEPART MUST be an alias for DATE_PART.

#### Scenario: ISODOW returns ISO day of week

Given a running database
When the user executes `SELECT DATE_PART('isodow', TIMESTAMP '2024-01-01')`
Then the result MUST be 1 since January 1, 2024 is a Monday

#### Scenario: ISODOW Sunday returns 7

Given a running database
When the user executes `SELECT DATE_PART('isodow', TIMESTAMP '2024-01-07')`
Then the result MUST be 7 since January 7, 2024 is a Sunday

#### Scenario: DATEPART alias

Given a running database
When the user executes `SELECT DATEPART('year', TIMESTAMP '2024-06-15')`
Then the result MUST be 2024

### Requirement: TIME_BUCKET SHALL truncate timestamps to bucket boundaries

TIME_BUCKET(interval, timestamp [, origin]) SHALL truncate a timestamp to the nearest bucket boundary. It MUST support configurable bucket widths and optional origin offsets.

#### Scenario: Hourly bucket

Given a running database
When the user executes `SELECT TIME_BUCKET(INTERVAL '1 hour', TIMESTAMP '2024-01-01 14:37:00')`
Then the result MUST be TIMESTAMP '2024-01-01 14:00:00'

#### Scenario: Five-minute bucket

Given a running database
When the user executes `SELECT TIME_BUCKET(INTERVAL '5 minutes', TIMESTAMP '2024-01-01 14:37:22')`
Then the result MUST be TIMESTAMP '2024-01-01 14:35:00'

### Requirement: MAKE_TIMESTAMPTZ SHALL construct timezone-aware timestamps

MAKE_TIMESTAMPTZ(year, month, day, hour, min, sec [, timezone]) SHALL construct a TIMESTAMP WITH TIME ZONE value. Without timezone argument, UTC MUST be assumed.

#### Scenario: With timezone argument

Given a running database
When the user executes `SELECT MAKE_TIMESTAMPTZ(2024, 1, 1, 12, 0, 0, 'UTC')`
Then the result MUST be a TIMESTAMP WITH TIME ZONE value at noon UTC

### Requirement: TIMEZONE function SHALL convert between timezones

TIMEZONE(timezone_string, timestamp) SHALL convert a timestamp to the specified timezone. It MUST return a TIMESTAMP WITH TIME ZONE value.

#### Scenario: UTC conversion

Given a running database
When the user executes `SELECT TIMEZONE('UTC', TIMESTAMP '2024-01-01 12:00:00')`
Then the result MUST be a TIMESTAMP WITH TIME ZONE value in UTC

### Requirement: EPOCH_NS SHALL convert nanoseconds to timestamp

EPOCH_NS(nanoseconds) SHALL convert nanoseconds since Unix epoch to a TIMESTAMP value. It MUST handle the full range of valid timestamps.

#### Scenario: Epoch nanoseconds conversion

Given a running database
When the user executes `SELECT EPOCH_NS(1704067200000000000)`
Then the result MUST be TIMESTAMP '2024-01-01 00:00:00'
