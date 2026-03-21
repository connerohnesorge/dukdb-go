# Utility Functions Specification

## Requirements

### Requirement: CURRENT_DATABASE() SHALL return the name of the active database

The CURRENT_DATABASE() function SHALL return the database name derived from the connection path.

#### Scenario: In-memory database
```
When the user executes "SELECT CURRENT_DATABASE()"
And the database was opened with ":memory:"
Then the result is "memory"
```

#### Scenario: File-based database
```
When the user executes "SELECT CURRENT_DATABASE()"
And the database was opened with "/path/to/test.db"
Then the result is "test.db"
```

### Requirement: CURRENT_SCHEMA() SHALL return the current schema name

The CURRENT_SCHEMA() function SHALL return the active schema, defaulting to "main".

#### Scenario: Default schema
```
When the user executes "SELECT CURRENT_SCHEMA()"
Then the result is "main"
```

### Requirement: VERSION() SHALL return the DuckDB compatibility version string

The VERSION() function SHALL return a version string indicating dukdb-go's DuckDB compatibility level.

#### Scenario: Version string format
```
When the user executes "SELECT VERSION()"
Then the result contains "v1.4.3"
```

### Requirement: DAYNAME(date) SHALL return the weekday name

The DAYNAME() function SHALL accept a date or timestamp and return the English weekday name.

#### Scenario: Known date weekday
```
When the user executes "SELECT DAYNAME(DATE '2024-01-15')"
Then the result is "Monday"
```

#### Scenario: NULL input returns NULL
```
When the user executes "SELECT DAYNAME(NULL)"
Then the result is NULL
```

### Requirement: MONTHNAME(date) SHALL return the month name

The MONTHNAME() function SHALL accept a date or timestamp and return the English month name.

#### Scenario: Known date month
```
When the user executes "SELECT MONTHNAME(DATE '2024-01-15')"
Then the result is "January"
```

#### Scenario: NULL input returns NULL
```
When the user executes "SELECT MONTHNAME(NULL)"
Then the result is NULL
```

### Requirement: YEARWEEK(date) SHALL return ISO year and week as BIGINT

The YEARWEEK() function SHALL return the ISO year and week number as a YYYYWW BIGINT value.

#### Scenario: Known date yearweek
```
When the user executes "SELECT YEARWEEK(DATE '2024-01-15')"
Then the result is 202403
```

#### Scenario: NULL input returns NULL
```
When the user executes "SELECT YEARWEEK(NULL)"
Then the result is NULL
```

### Requirement: EPOCH_US(timestamp) SHALL return epoch in microseconds

The EPOCH_US() function SHALL return the Unix epoch time in microseconds, complementing existing EPOCH (seconds) and EPOCH_MS (milliseconds).

#### Scenario: Known timestamp
```
When the user executes "SELECT EPOCH_US(TIMESTAMP '1970-01-01 00:00:01')"
Then the result is 1000000
```

#### Scenario: NULL input returns NULL
```
When the user executes "SELECT EPOCH_US(NULL)"
Then the result is NULL
```

### Requirement: TRANSLATE(string, from, to) SHALL perform character-level replacement

The TRANSLATE() function SHALL replace each character in the input string that appears in the `from` string with the corresponding character in the `to` string. Characters in `from` without a corresponding `to` character SHALL be deleted.

#### Scenario: Character substitution
```
When the user executes "SELECT TRANSLATE('hello', 'el', 'ip')"
Then the result is "hippo"
```

#### Scenario: Character deletion when to is shorter
```
When the user executes "SELECT TRANSLATE('hello', 'lo', 'r')"
Then the result is "herr"
```

#### Scenario: NULL input returns NULL
```
When the user executes "SELECT TRANSLATE(NULL, 'a', 'b')"
Then the result is NULL
```

### Requirement: STRIP_ACCENTS(string) SHALL remove diacritical marks

The STRIP_ACCENTS() function SHALL remove accent marks from characters using Unicode NFD normalization and filtering combining diacritical marks.

#### Scenario: Remove accents from string
```
When the user executes "SELECT STRIP_ACCENTS('café')"
Then the result is "cafe"
```

#### Scenario: String without accents unchanged
```
When the user executes "SELECT STRIP_ACCENTS('hello')"
Then the result is "hello"
```

#### Scenario: NULL input returns NULL
```
When the user executes "SELECT STRIP_ACCENTS(NULL)"
Then the result is NULL
```

### Requirement: NOW() and CURRENT_TIMESTAMP SHALL return the current timestamp

The NOW() and CURRENT_TIMESTAMP functions SHALL return the current date and time as a TIMESTAMP value.

#### Scenario: NOW returns non-null timestamp
```
When the user executes "SELECT NOW()"
Then the result is a non-null TIMESTAMP value
And the result is approximately the current time
```

#### Scenario: CURRENT_TIMESTAMP returns same as NOW
```
When the user executes "SELECT CURRENT_TIMESTAMP"
Then the result is a non-null TIMESTAMP value
And the result is approximately the current time
```

### Requirement: CURRENT_DATE SHALL return today's date

The CURRENT_DATE function SHALL return the current date with time components set to zero.

#### Scenario: Returns today's date
```
When the user executes "SELECT CURRENT_DATE"
Then the result is today's date as a DATE value
And the time components are zero
```

### Requirement: CURRENT_TIME SHALL return the current time

The CURRENT_TIME function SHALL return the current time of day as a TIME string.

#### Scenario: Returns valid time string
```
When the user executes "SELECT CURRENT_TIME"
Then the result is a valid time in "HH:MM:SS" format
```

### Requirement: IFNULL(expr, default) and NVL(expr, default) SHALL return first non-NULL argument

The IFNULL() and NVL() functions SHALL accept exactly 2 arguments and return the first argument if non-NULL, otherwise the second argument. They are equivalent to COALESCE(a, b) but strictly 2 arguments.

#### Scenario: First argument is NULL
```
When the user executes "SELECT IFNULL(NULL, 42)"
Then the result is 42
```

#### Scenario: First argument is non-NULL
```
When the user executes "SELECT IFNULL(1, 42)"
Then the result is 1
```

#### Scenario: NVL alias works identically
```
When the user executes "SELECT NVL(NULL, 'default')"
Then the result is "default"
```

#### Scenario: Both arguments NULL
```
When the user executes "SELECT IFNULL(NULL, NULL)"
Then the result is NULL
```

