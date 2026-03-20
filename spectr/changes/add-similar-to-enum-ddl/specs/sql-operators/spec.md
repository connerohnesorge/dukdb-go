## ADDED Requirements

### Requirement: SIMILAR TO Pattern Matching

The system SHALL support the SIMILAR TO operator for SQL standard regex pattern matching. SIMILAR TO matches the entire string against a pattern using SQL regex syntax where `%` matches zero or more characters, `_` matches any single character, `|` provides alternation, `()` provides grouping, and `[]` provides character classes. The system SHALL also support NOT SIMILAR TO as the negated form and an optional ESCAPE clause to specify a custom escape character.

#### Scenario: Basic SIMILAR TO match

- WHEN a query contains `SELECT 'abc' SIMILAR TO 'a%'`
- THEN the result SHALL be true

#### Scenario: SIMILAR TO with alternation

- WHEN a query contains `SELECT 'cat' SIMILAR TO '(cat|dog)'`
- THEN the result SHALL be true

#### Scenario: SIMILAR TO with character class

- WHEN a query contains `SELECT 'a1' SIMILAR TO '[a-z][0-9]'`
- THEN the result SHALL be true

#### Scenario: SIMILAR TO with negated character class

- WHEN a query contains `SELECT 'x1' SIMILAR TO '[!abc][0-9]'`
- THEN the result SHALL be true because `[!abc]` matches any character NOT in {a, b, c}, and 'x' qualifies
- NOTE: SQL uses `[!...]` for character class negation; the implementation SHALL convert this to Go regexp `[^...]` syntax

#### Scenario: SIMILAR TO underscore wildcard

- WHEN a query contains `SELECT 'ab' SIMILAR TO 'a_'`
- THEN the result SHALL be true

#### Scenario: NOT SIMILAR TO

- WHEN a query contains `SELECT 'abc' NOT SIMILAR TO 'x%'`
- THEN the result SHALL be true

#### Scenario: SIMILAR TO no match

- WHEN a query contains `SELECT 'abc' SIMILAR TO 'x%'`
- THEN the result SHALL be false

#### Scenario: SIMILAR TO with ESCAPE clause

- WHEN a query contains `SELECT 'a%b' SIMILAR TO 'a#%b' ESCAPE '#'`
- THEN the result SHALL be true because `#%` matches a literal `%`

#### Scenario: SIMILAR TO in WHERE clause

- WHEN a table `t` contains rows with column `name` having values 'alice', 'bob', 'anna'
- AND a query contains `SELECT name FROM t WHERE name SIMILAR TO 'a%'`
- THEN the result SHALL contain 'alice' and 'anna' but not 'bob'

#### Scenario: SIMILAR TO with NULL

- WHEN a query contains `SELECT NULL SIMILAR TO 'a%'`
- THEN the result SHALL be NULL

### Requirement: CREATE TYPE AS ENUM DDL

The system SHALL support CREATE TYPE name AS ENUM ('value1', 'value2', ...) to define named enum types in the catalog. Named enum types SHALL be usable as column types in CREATE TABLE statements. The system SHALL support DROP TYPE to remove user-defined types. The system SHALL prevent dropping a type that is currently in use by a table column.

#### Scenario: Create an enum type

- WHEN a query contains `CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')`
- THEN a type named 'mood' SHALL be registered in the catalog with the specified values

#### Scenario: Use enum type in table definition

- WHEN a type 'mood' has been created with `CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')`
- AND a query contains `CREATE TABLE t (m mood)`
- THEN the table SHALL be created with column 'm' of enum type 'mood'

#### Scenario: Insert valid enum value

- WHEN a table `t` has an enum column `m` of type 'mood' with values ('happy', 'sad', 'neutral')
- AND a query contains `INSERT INTO t VALUES ('happy')`
- THEN the insert SHALL succeed

#### Scenario: Insert invalid enum value

- WHEN a table `t` has an enum column `m` of type 'mood' with values ('happy', 'sad', 'neutral')
- AND a query contains `INSERT INTO t VALUES ('angry')`
- THEN the system SHALL return an error indicating 'angry' is not a valid enum value

#### Scenario: Drop an unused type

- WHEN a type 'mood' exists and no tables reference it
- AND a query contains `DROP TYPE mood`
- THEN the type SHALL be removed from the catalog

#### Scenario: Drop a type in use

- WHEN a type 'mood' exists and table `t` has a column of type 'mood'
- AND a query contains `DROP TYPE mood`
- THEN the system SHALL return an error indicating the type is in use

#### Scenario: CREATE TYPE IF NOT EXISTS

- WHEN a type 'mood' already exists
- AND a query contains `CREATE TYPE IF NOT EXISTS mood AS ENUM ('x', 'y')`
- THEN the system SHALL not return an error and the existing type SHALL remain unchanged

#### Scenario: DROP TYPE IF EXISTS on nonexistent type

- WHEN no type named 'phantom' exists
- AND a query contains `DROP TYPE IF EXISTS phantom`
- THEN the system SHALL not return an error

#### Scenario: Schema-qualified enum type

- WHEN a schema 'myschema' exists
- AND a query contains `CREATE TYPE myschema.status AS ENUM ('active', 'inactive')`
- THEN the type SHALL be created in the 'myschema' schema

#### Scenario: Select from enum column

- WHEN a table `t` has an enum column `m` with values ('happy', 'sad') and contains rows ('happy'), ('sad')
- AND a query contains `SELECT m FROM t WHERE m = 'happy'`
- THEN the result SHALL contain one row with value 'happy'
