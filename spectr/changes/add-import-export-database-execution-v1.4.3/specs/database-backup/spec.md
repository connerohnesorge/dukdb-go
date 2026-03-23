## ADDED Requirements

### Requirement: EXPORT DATABASE SHALL write all database objects and data to a directory

The system SHALL execute `EXPORT DATABASE 'path' [(OPTIONS)]` by creating a directory containing `schema.sql` (DDL statements for all schemas, types, sequences, tables, views, and macros), `load.sql` (COPY FROM statements for each table), and one data file per table in the specified format (CSV by default, also supporting PARQUET and JSON).

#### Scenario: EXPORT DATABASE with default CSV format

- WHEN the database contains tables 'users' and 'orders' with data
- AND the user executes `EXPORT DATABASE '/tmp/backup'`
- THEN the directory `/tmp/backup` is created
- AND it contains `schema.sql` with CREATE TABLE statements
- AND it contains `load.sql` with COPY FROM statements
- AND it contains `users.csv` and `orders.csv` with table data

#### Scenario: EXPORT DATABASE with Parquet format

- WHEN the user executes `EXPORT DATABASE '/tmp/backup' (FORMAT PARQUET)`
- THEN data files use `.parquet` extension
- AND `load.sql` references `.parquet` files with `FORMAT PARQUET`

#### Scenario: EXPORT DATABASE preserves views and sequences

- WHEN the database contains a view 'active_users' and a sequence 'user_id_seq'
- AND the user executes `EXPORT DATABASE '/tmp/backup'`
- THEN `schema.sql` contains `CREATE VIEW active_users AS ...`
- AND `schema.sql` contains `CREATE SEQUENCE user_id_seq ...`

#### Scenario: EXPORT DATABASE with custom delimiter

- WHEN the user executes `EXPORT DATABASE '/tmp/backup' (FORMAT CSV, DELIMITER '|')`
- THEN data files use `|` as delimiter
- AND `load.sql` includes the delimiter option

### Requirement: IMPORT DATABASE SHALL restore from an exported directory

The system SHALL execute `IMPORT DATABASE 'path'` by reading `schema.sql` to create all database objects and then executing `load.sql` to load data from the data files. The import SHALL be wrapped in a transaction to ensure atomicity.

#### Scenario: IMPORT DATABASE from exported directory

- WHEN the directory `/tmp/backup` contains `schema.sql`, `load.sql`, and data files
- AND the user executes `IMPORT DATABASE '/tmp/backup'`
- THEN all tables, views, and sequences from `schema.sql` are created
- AND all data from `load.sql` is loaded

#### Scenario: IMPORT DATABASE roundtrip preserves data

- WHEN the user exports with `EXPORT DATABASE '/tmp/backup'`
- AND imports into a fresh database with `IMPORT DATABASE '/tmp/backup'`
- THEN all table data matches the original database
- AND all views and sequences are restored

#### Scenario: IMPORT DATABASE from non-existent directory fails

- WHEN the directory `/tmp/nonexistent` does not exist
- AND the user executes `IMPORT DATABASE '/tmp/nonexistent'`
- THEN the system returns an error containing "does not exist"

#### Scenario: IMPORT DATABASE rolls back on failure

- WHEN `schema.sql` contains a valid CREATE TABLE but `load.sql` references a missing data file
- AND the user executes `IMPORT DATABASE '/tmp/broken'`
- THEN the system returns an error
- AND the tables created by schema.sql are rolled back

### Requirement: EXPORT DATABASE SHALL output tables in dependency order

The system SHALL topologically sort tables based on foreign key relationships so that `schema.sql` can be executed sequentially without reference errors.

#### Scenario: Tables with foreign keys exported in dependency order

- WHEN table 'orders' has a foreign key referencing 'users'
- AND the user executes `EXPORT DATABASE '/tmp/backup'`
- THEN `schema.sql` contains `CREATE TABLE users` before `CREATE TABLE orders`
