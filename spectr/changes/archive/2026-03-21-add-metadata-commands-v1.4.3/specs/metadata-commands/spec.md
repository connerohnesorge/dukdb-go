# Metadata Commands

## ADDED Requirements

### Requirement: DESCRIBE statement

DESCRIBE SHALL return column metadata for a table or query. It MUST return column_name, column_type, null, key, default, and extra columns.

#### Scenario: DESCRIBE table

Given a table `employees(id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, salary DOUBLE)`
When `DESCRIBE employees` is executed
Then the result MUST contain 3 rows with correct column names, types, and nullability

#### Scenario: DESCRIBE SELECT query

When `DESCRIBE SELECT 1 AS x, 'hello' AS y` is executed
Then the result MUST show column x with INTEGER type and column y with VARCHAR type

### Requirement: SHOW TABLES command

SHOW TABLES SHALL list all tables in the current schema.

#### Scenario: SHOW TABLES with data

Given tables `employees` and `departments` exist
When `SHOW TABLES` is executed
Then the result MUST include both 'employees' and 'departments'

#### Scenario: SHOW ALL TABLES across schemas

When `SHOW ALL TABLES` is executed
Then the result MUST include tables from all schemas

### Requirement: SHOW COLUMNS FROM table

SHOW COLUMNS FROM table SHALL return the same information as DESCRIBE table.

#### Scenario: SHOW COLUMNS equivalence

When `SHOW COLUMNS FROM employees` is executed
Then the result MUST match `DESCRIBE employees`

### Requirement: SUMMARIZE statement

SUMMARIZE SHALL return per-column statistics including min, max, unique count, null count, and approximate statistics.

#### Scenario: SUMMARIZE table

Given a table with numeric and string columns
When `SUMMARIZE table_name` is executed
Then the result MUST contain one row per column with statistics

### Requirement: CALL statement

CALL SHALL execute a function and return its result set.

#### Scenario: CALL table function

When `CALL generate_series(1, 5)` is executed
Then the result MUST contain rows 1 through 5
