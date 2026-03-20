## ADDED Requirements

### Requirement: UNIQUE Constraint Parsing

The parser SHALL parse UNIQUE constraints in CREATE TABLE at both column-level and table-level.

#### Scenario: Column-level UNIQUE

- WHEN parsing `CREATE TABLE t (id INTEGER, email VARCHAR UNIQUE)`
- THEN the parsed statement includes a UNIQUE constraint on column "email"

#### Scenario: Table-level UNIQUE on multiple columns

- WHEN parsing `CREATE TABLE t (a INTEGER, b INTEGER, UNIQUE (a, b))`
- THEN the parsed statement includes a UNIQUE constraint on columns ["a", "b"]

#### Scenario: Named UNIQUE constraint

- WHEN parsing `CREATE TABLE t (id INTEGER, CONSTRAINT uq_email UNIQUE (email))`
- THEN the constraint has Name="uq_email"

### Requirement: CHECK Constraint Parsing

The parser SHALL parse CHECK constraints with arbitrary boolean expressions.

#### Scenario: Column-level CHECK

- WHEN parsing `CREATE TABLE t (age INTEGER CHECK (age >= 0))`
- THEN the parsed statement includes a CHECK constraint with expression `age >= 0`

#### Scenario: Table-level CHECK with multiple columns

- WHEN parsing `CREATE TABLE t (start_date DATE, end_date DATE, CHECK (end_date > start_date))`
- THEN the CHECK constraint references both columns

### Requirement: FOREIGN KEY Constraint Parsing

The parser SHALL parse FOREIGN KEY constraints with REFERENCES clause and optional ON DELETE/UPDATE actions.

#### Scenario: Column-level REFERENCES

- WHEN parsing `CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id))`
- THEN the parsed statement includes a FK constraint referencing users(id)

#### Scenario: Table-level FOREIGN KEY with RESTRICT

- WHEN parsing `CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT)`
- THEN the FK constraint has OnDelete=RESTRICT

#### Scenario: FOREIGN KEY rejects CASCADE action

- WHEN parsing `CREATE TABLE t (ref_id INTEGER REFERENCES other(id) ON DELETE CASCADE)`
- THEN a parse error is returned: "FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT"

#### Scenario: FOREIGN KEY with NO ACTION (default)

- WHEN parsing `CREATE TABLE t (ref_id INTEGER REFERENCES other(id))`
- THEN OnDelete defaults to NO ACTION and OnUpdate defaults to NO ACTION
