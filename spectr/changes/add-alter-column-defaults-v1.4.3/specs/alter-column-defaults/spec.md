# ALTER COLUMN Defaults and Nullability

## ADDED Requirements

### Requirement: SET DEFAULT SHALL set column default value

ALTER TABLE t ALTER COLUMN c SET DEFAULT expr SHALL set the default value for column c. Subsequent INSERT statements that omit c MUST use the default value.

#### Scenario: Set integer default

Given a table t with columns (id INTEGER, name VARCHAR)
When the user executes `ALTER TABLE t ALTER COLUMN name SET DEFAULT 'unknown'`
And then executes `INSERT INTO t(id) VALUES (1)`
Then the row MUST have name = 'unknown'

### Requirement: DROP DEFAULT SHALL remove column default

ALTER TABLE t ALTER COLUMN c DROP DEFAULT SHALL remove any existing default value for column c.

#### Scenario: Drop default

Given a table t with column c having DEFAULT 42
When the user executes `ALTER TABLE t ALTER COLUMN c DROP DEFAULT`
And then executes `INSERT INTO t() VALUES (DEFAULT)`
Then the column c MUST be NULL

### Requirement: SET NOT NULL SHALL enforce non-null constraint

ALTER TABLE t ALTER COLUMN c SET NOT NULL SHALL add a NOT NULL constraint to column c. It MUST fail if the column already contains NULL values.

#### Scenario: Set not null succeeds

Given a table t with no NULL values in column c
When the user executes `ALTER TABLE t ALTER COLUMN c SET NOT NULL`
Then the operation MUST succeed

#### Scenario: Set not null fails with existing nulls

Given a table t with NULL values in column c
When the user executes `ALTER TABLE t ALTER COLUMN c SET NOT NULL`
Then the operation MUST return an error

### Requirement: DROP NOT NULL SHALL remove non-null constraint

ALTER TABLE t ALTER COLUMN c DROP NOT NULL SHALL remove the NOT NULL constraint from column c, allowing NULL values.

#### Scenario: Drop not null

Given a table t with column c having NOT NULL constraint
When the user executes `ALTER TABLE t ALTER COLUMN c DROP NOT NULL`
And then executes `INSERT INTO t(c) VALUES (NULL)`
Then the insert MUST succeed
