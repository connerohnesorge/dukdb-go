# Export Import Database Specification

## Requirements

### Requirement: Sequence DDL in EXPORT DATABASE

The system SHALL include CREATE SEQUENCE statements in schema.sql when exporting a database that contains sequences. Each sequence DDL MUST include START WITH, INCREMENT BY, MINVALUE, MAXVALUE, and CYCLE/NO CYCLE clauses reflecting the sequence definition at export time. Sequences SHALL appear before CREATE TABLE statements in schema.sql.

#### Scenario: Export database with sequence

- GIVEN a database with sequence "id_seq" defined as START WITH 10 INCREMENT BY 5 MINVALUE 1 MAXVALUE 1000 NO CYCLE
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql contains `CREATE SEQUENCE id_seq START WITH 10 INCREMENT BY 5 MINVALUE 1 MAXVALUE 1000 NO CYCLE;`
- AND the CREATE SEQUENCE statement appears before any CREATE TABLE statements

#### Scenario: Export database with sequence in non-main schema

- GIVEN a database with schema "analytics" and sequence "analytics.counter_seq"
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql contains `CREATE SEQUENCE analytics.counter_seq START WITH ...;`
- AND the CREATE SCHEMA analytics statement appears before the CREATE SEQUENCE statement

#### Scenario: Round-trip preserves sequences

- GIVEN a database with sequence "my_seq" at current value 42
- WHEN executing `EXPORT DATABASE '/tmp/export'` followed by `IMPORT DATABASE '/tmp/export'` on a fresh database
- THEN the imported database contains sequence "my_seq" with the same configuration

### Requirement: Index DDL in EXPORT DATABASE

The system SHALL include CREATE INDEX statements in schema.sql when exporting a database that contains non-primary-key indexes. Unique indexes MUST use CREATE UNIQUE INDEX. Primary key indexes SHALL be omitted from CREATE INDEX since they are declared inline in CREATE TABLE. Index DDL SHALL appear after CREATE VIEW statements in schema.sql.

#### Scenario: Export database with index

- GIVEN a table "users" with a non-unique index "idx_users_name" on column "name"
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql contains `CREATE INDEX idx_users_name ON users (name);`

#### Scenario: Export database with unique index

- GIVEN a table "users" with a unique index "idx_users_email" on column "email"
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql contains `CREATE UNIQUE INDEX idx_users_email ON users (email);`

#### Scenario: Export database skips primary key indexes

- GIVEN a table "users" with PRIMARY KEY (id) which creates an internal primary key index
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql does not contain a separate CREATE INDEX statement for the primary key
- AND the PRIMARY KEY is declared inline in the CREATE TABLE statement

#### Scenario: Export database with multi-column index

- GIVEN a table "orders" with index "idx_orders_customer_date" on columns (customer_id, order_date)
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql contains `CREATE INDEX idx_orders_customer_date ON orders (customer_id, order_date);`

### Requirement: FORMAT Option Support for EXPORT DATABASE

The system SHALL respect the FORMAT option in EXPORT DATABASE to produce data files in the specified format. Supported formats SHALL be CSV (default), PARQUET, and JSON. When FORMAT is not specified, the system MUST default to CSV. An unsupported FORMAT value SHALL produce an error.

#### Scenario: Export with default CSV format

- GIVEN a database with table "data" containing rows
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN data files have .csv extension
- AND load.sql contains COPY FROM statements with `(FORMAT CSV, HEADER true)`

#### Scenario: Export with FORMAT PARQUET

- GIVEN a database with table "data" containing rows
- WHEN executing `EXPORT DATABASE '/tmp/export' (FORMAT PARQUET)`
- THEN data files have .parquet extension
- AND load.sql contains COPY FROM statements with `(FORMAT PARQUET)`

#### Scenario: Export with FORMAT JSON

- GIVEN a database with table "data" containing rows
- WHEN executing `EXPORT DATABASE '/tmp/export' (FORMAT JSON)`
- THEN data files have .json extension in NDJSON format
- AND load.sql contains COPY FROM statements with `(FORMAT JSON)`

#### Scenario: Export with unsupported format

- GIVEN a database with tables
- WHEN executing `EXPORT DATABASE '/tmp/export' (FORMAT XML)`
- THEN the system returns an error with Msg containing "unsupported export format"

### Requirement: Multi-Schema Data File Naming

The system SHALL use `{schema}_{table}.{ext}` naming for data files of tables in non-main schemas to avoid filename collisions. Tables in the main schema SHALL use `{table}.{ext}` naming.

#### Scenario: Export with multiple schemas

- GIVEN schema "main" with table "users" and schema "analytics" with table "users"
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN the export directory contains "users.csv" for main.users
- AND "analytics_users.csv" for analytics.users
- AND load.sql references the correct filenames for each table

#### Scenario: Export single main schema table

- GIVEN only the default "main" schema with table "orders"
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN the data file is named "orders.csv" without schema prefix

### Requirement: Dependency Ordering in schema.sql

The system SHALL emit DDL statements in schema.sql in strict dependency order: CREATE SCHEMA statements first, then CREATE SEQUENCE, then CREATE TABLE, then CREATE VIEW, then CREATE INDEX. This ordering MUST ensure that all referenced objects exist before they are used.

#### Scenario: Full dependency chain ordering

- GIVEN a database with schema "s", sequence "s.seq1", table "s.t1", view "s.v1" referencing "s.t1", and index "s.idx1" on "s.t1"
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql statements appear in order: CREATE SCHEMA s, CREATE SEQUENCE s.seq1, CREATE TABLE s.t1, CREATE VIEW s.v1, CREATE INDEX s.idx1

#### Scenario: Mixed main and custom schema ordering

- GIVEN sequences in main schema and tables in both main and custom schemas
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN all CREATE SCHEMA statements precede all CREATE SEQUENCE statements
- AND all CREATE SEQUENCE statements precede all CREATE TABLE statements

### Requirement: DEFAULT Clause in Table DDL Generation

The system SHALL include DEFAULT clauses in CREATE TABLE statements when columns have default values defined. The generated DEFAULT expression MUST match the original column definition.

#### Scenario: Export table with default value

- GIVEN a table "config" with column "active" BOOLEAN DEFAULT true
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql CREATE TABLE includes `active BOOLEAN DEFAULT true`

#### Scenario: Export table with string default

- GIVEN a table "users" with column "status" VARCHAR DEFAULT 'active'
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql CREATE TABLE includes `status VARCHAR DEFAULT 'active'`

#### Scenario: Export table with no default

- GIVEN a table "data" with column "value" INTEGER and no default
- WHEN executing `EXPORT DATABASE '/tmp/export'`
- THEN schema.sql CREATE TABLE includes `value INTEGER` without a DEFAULT clause

