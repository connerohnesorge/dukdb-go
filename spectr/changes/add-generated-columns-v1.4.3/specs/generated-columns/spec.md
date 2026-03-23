## ADDED Requirements

### Requirement: CREATE TABLE SHALL support GENERATED ALWAYS AS columns

The system SHALL support defining generated columns in CREATE TABLE statements using the syntax `column_name type GENERATED ALWAYS AS (expression) [STORED|VIRTUAL]` and the shorthand `column_name type AS (expression) [STORED|VIRTUAL]`. Generated column values SHALL be automatically computed from the given expression. When neither STORED nor VIRTUAL is specified, the default SHALL be VIRTUAL (matching DuckDB behavior). Initial implementation SHALL support STORED columns.

#### Scenario: STORED generated column with string concatenation

- WHEN the user executes `CREATE TABLE people (first_name VARCHAR, last_name VARCHAR, full_name VARCHAR GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)`
- AND the user executes `INSERT INTO people (first_name, last_name) VALUES ('John', 'Doe')`
- THEN `SELECT full_name FROM people` returns `'John Doe'`

#### Scenario: Shorthand AS syntax

- WHEN the user executes `CREATE TABLE items (price DOUBLE, qty INT, total DOUBLE AS (price * qty) STORED)`
- AND the user executes `INSERT INTO items (price, qty) VALUES (9.99, 3)`
- THEN `SELECT total FROM items` returns `29.97`

#### Scenario: Generated column with function call

- WHEN the user executes `CREATE TABLE names (name VARCHAR, upper_name VARCHAR GENERATED ALWAYS AS (UPPER(name)) STORED)`
- AND the user executes `INSERT INTO names (name) VALUES ('hello')`
- THEN `SELECT upper_name FROM names` returns `'HELLO'`

### Requirement: INSERT SHALL auto-compute generated column values

The system SHALL automatically evaluate generated column expressions during INSERT operations. The system SHALL reject explicit non-DEFAULT values for generated columns.

#### Scenario: INSERT omitting generated column succeeds

- WHEN the table has a generated column `full_name AS (first || ' ' || last) STORED`
- AND the user executes `INSERT INTO t (first, last) VALUES ('A', 'B')`
- THEN the row is inserted with `full_name = 'A B'`

#### Scenario: INSERT with DEFAULT for generated column succeeds

- WHEN the table has a generated column `total AS (price * qty) STORED`
- AND the user executes `INSERT INTO t (price, qty, total) VALUES (10, 2, DEFAULT)`
- THEN the row is inserted with `total = 20`

#### Scenario: INSERT with explicit value for generated column fails

- WHEN the table has a generated column `full_name`
- AND the user executes `INSERT INTO t (first, last, full_name) VALUES ('A', 'B', 'override')`
- THEN the system returns an error containing "cannot insert a non-DEFAULT value into column"

### Requirement: UPDATE SHALL recompute generated column values

The system SHALL re-evaluate generated column expressions when base columns are updated. The system SHALL reject direct SET on generated columns.

#### Scenario: UPDATE base column triggers recomputation

- WHEN the table has `full_name AS (first || ' ' || last) STORED`
- AND the row has `first='John', last='Doe', full_name='John Doe'`
- AND the user executes `UPDATE t SET first = 'Jane' WHERE last = 'Doe'`
- THEN `SELECT full_name FROM t` returns `'Jane Doe'`

#### Scenario: UPDATE directly setting generated column fails

- WHEN the table has a generated column `total`
- AND the user executes `UPDATE t SET total = 100`
- THEN the system returns an error containing "column .* is a generated column"

### Requirement: Generated column validation SHALL enforce correctness

The system SHALL validate generated column expressions during CREATE TABLE. The system SHALL reject invalid definitions.

#### Scenario: Generated column with DEFAULT is rejected

- WHEN the user executes `CREATE TABLE t (x INT, y INT GENERATED ALWAYS AS (x+1) STORED DEFAULT 0)`
- THEN the system returns an error containing "cannot have both DEFAULT and GENERATED"

#### Scenario: Generated column as PRIMARY KEY is rejected

- WHEN the user executes `CREATE TABLE t (x INT, y INT GENERATED ALWAYS AS (x+1) STORED PRIMARY KEY)`
- THEN the system returns an error containing "generated column cannot be a primary key"

#### Scenario: Generated column with volatile function is rejected

- WHEN the user executes `CREATE TABLE t (x INT, y VARCHAR GENERATED ALWAYS AS (RANDOM()) STORED)`
- THEN the system returns an error containing "generated column expression must be deterministic"

#### Scenario: Generated column with forward reference is rejected

- WHEN the user executes `CREATE TABLE t (y INT GENERATED ALWAYS AS (x+1) STORED, x INT)`
- THEN the system returns an error containing "references column .* which is defined after it"

### Requirement: ALTER TABLE SHALL handle generated column interactions

The system SHALL allow dropping generated columns and adding new generated columns. The system SHALL prevent dropping base columns that are referenced by generated columns.

#### Scenario: DROP generated column succeeds

- WHEN the table has a generated column `full_name`
- AND the user executes `ALTER TABLE t DROP COLUMN full_name`
- THEN the column is removed successfully

#### Scenario: DROP base column referenced by generated column fails

- WHEN the table has `full_name AS (first || ' ' || last) STORED`
- AND the user executes `ALTER TABLE t DROP COLUMN first`
- THEN the system returns an error containing "referenced by generated column"

#### Scenario: ADD generated column succeeds

- WHEN the user executes `ALTER TABLE t ADD COLUMN upper_name VARCHAR GENERATED ALWAYS AS (UPPER(name)) STORED`
- THEN the column is added and existing rows have computed values

### Requirement: Generated columns SHALL appear in information_schema

Generated columns SHALL be queryable via `information_schema.columns` with appropriate metadata.

#### Scenario: information_schema shows generated column metadata

- WHEN the table has a generated column `full_name AS (first || ' ' || last) STORED`
- AND the user queries `SELECT column_name, is_generated, generation_expression FROM information_schema.columns WHERE table_name = 't' AND column_name = 'full_name'`
- THEN `is_generated` is `'ALWAYS'` and `generation_expression` is `first || ' ' || last`
