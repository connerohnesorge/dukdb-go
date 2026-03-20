## ADDED Requirements

### Requirement: EXPORT DATABASE Execution

The engine SHALL export the entire database schema and data to a directory, producing schema.sql (DDL), data files (one per table), and load.sql (COPY FROM statements) in dependency order.

#### Scenario: Export database with single table as CSV

- GIVEN a database with table "users" (id INTEGER, name VARCHAR) containing rows (1, 'alice'), (2, 'bob')
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN directory '/tmp/export' is created
- AND file 'schema.sql' contains `CREATE TABLE users (id INTEGER, name VARCHAR);`
- AND file 'users.csv' contains the table data in CSV format
- AND file 'load.sql' contains `COPY users FROM '/tmp/export/users.csv';`

#### Scenario: Export database with FORMAT PARQUET

- GIVEN a database with table "data" containing rows
- WHEN executing `EXPORT DATABASE '/tmp/export' (FORMAT PARQUET)`
- THEN data files use .parquet extension
- AND load.sql COPY FROM statements reference .parquet files with FORMAT PARQUET

#### Scenario: Export database with multiple schemas

- GIVEN a database with schemas "main" and "analytics" each containing tables
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql contains `CREATE SCHEMA analytics;` before analytics tables
- AND tables in "analytics" schema are exported as `analytics_{table}.csv`
- AND tables in "main" schema are exported as `{table}.csv`

#### Scenario: Export database dependency ordering

- GIVEN a database with sequence "id_seq", table "t" using nextval('id_seq'), view "v" referencing "t", and index "idx" on "t"
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql contains statements in order: CREATE SEQUENCE, CREATE TABLE, CREATE VIEW, CREATE INDEX

#### Scenario: Export database with primary key

- GIVEN a table with PRIMARY KEY (id)
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql CREATE TABLE includes PRIMARY KEY constraint

#### Scenario: Export database with views

- GIVEN a view "v" defined as `SELECT id, name FROM users WHERE active = true`
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql contains `CREATE VIEW v AS SELECT id, name FROM users WHERE active = true;`
- AND no data file is created for the view

### Requirement: IMPORT DATABASE Execution

The engine SHALL import a previously exported database by executing schema.sql followed by load.sql from the specified directory.

#### Scenario: Import database round-trip

- GIVEN an exported database at '/tmp/export' with schema.sql and load.sql
- WHEN executing `IMPORT DATABASE '/tmp/export'`
- THEN all tables, views, sequences, and indexes are recreated
- AND all table data is loaded
- AND the resulting database is equivalent to the original

#### Scenario: Import database into non-empty database fails

- GIVEN a database with existing table "users"
- AND an export directory containing a table also named "users"
- WHEN executing `IMPORT DATABASE '/tmp/export'`
- THEN an error is returned indicating the database is not empty or table already exists

#### Scenario: Import database with missing schema.sql

- WHEN executing `IMPORT DATABASE '/tmp/nonexistent'`
- THEN an error is returned indicating the directory or schema.sql does not exist

#### Scenario: Import database with FORMAT PARQUET

- GIVEN an export directory with .parquet data files and load.sql referencing them
- WHEN executing `IMPORT DATABASE '/tmp/export'`
- THEN load.sql COPY FROM statements correctly load parquet data

### Requirement: DDL Generation

The catalog SHALL provide ToCreateSQL() methods on TableDef, ViewDef, SequenceDef, and IndexDef that generate valid CREATE statements parseable by the dukdb-go parser.

#### Scenario: TableDef DDL generation with all features

- GIVEN a TableDef with columns (id INTEGER NOT NULL, name VARCHAR DEFAULT 'unknown'), PRIMARY KEY (id), and schema "main"
- WHEN calling ToCreateSQL()
- THEN the output is `CREATE TABLE main.id_table (id INTEGER NOT NULL, name VARCHAR DEFAULT 'unknown', PRIMARY KEY (id));` or equivalent valid SQL

#### Scenario: ViewDef DDL generation

- GIVEN a ViewDef with name "active_users" and SQL "SELECT * FROM users WHERE active = true"
- WHEN calling ToCreateSQL()
- THEN the output is `CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;`

#### Scenario: SequenceDef DDL generation

- GIVEN a SequenceDef with START WITH 100, INCREMENT BY 5, CYCLE
- WHEN calling ToCreateSQL()
- THEN the output includes START WITH, INCREMENT BY, and CYCLE clauses

#### Scenario: DDL round-trip correctness

- GIVEN any catalog object
- WHEN calling ToCreateSQL() and parsing the result with the dukdb-go parser
- THEN the parsed AST correctly represents the original catalog object
