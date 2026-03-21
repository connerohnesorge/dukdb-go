# Table DDL Extensions

## ADDED Requirements

### Requirement: CREATE OR REPLACE TABLE

CREATE OR REPLACE TABLE SHALL drop an existing table and create a new one in a single statement. It MUST be mutually exclusive with IF NOT EXISTS.

#### Scenario: Replace existing table

Given a table `employees(id INTEGER, name VARCHAR)` exists
When `CREATE OR REPLACE TABLE employees(id INTEGER, name VARCHAR, salary DOUBLE)` is executed
Then the old table MUST be dropped and a new table with 3 columns MUST be created

#### Scenario: Replace non-existing table

When `CREATE OR REPLACE TABLE new_table(id INTEGER)` is executed on a non-existing table
Then the table MUST be created normally

### Requirement: CREATE TEMP TABLE

CREATE TEMP TABLE or CREATE TEMPORARY TABLE SHALL create a session-scoped temporary table in the temp schema.

#### Scenario: Create temporary table

When `CREATE TEMP TABLE tmp(id INTEGER, value VARCHAR)` is executed
Then the table MUST be created in the temp schema
And the table MUST be visible to the current connection

#### Scenario: Temporary table shadows regular table

Given a regular table `data(id INTEGER)` exists
When `CREATE TEMP TABLE data(id INTEGER, extra VARCHAR)` is executed
Then queries against `data` MUST resolve to the temporary table

### Requirement: ALTER TABLE ADD CONSTRAINT

ALTER TABLE ADD CONSTRAINT SHALL add a named constraint to an existing table.

#### Scenario: Add UNIQUE constraint

Given a table `employees(id INTEGER, email VARCHAR)` exists
When `ALTER TABLE employees ADD CONSTRAINT uq_email UNIQUE(email)` is executed
Then duplicate values in the email column MUST be rejected

#### Scenario: Add FOREIGN KEY constraint

Given tables `departments(id INTEGER PRIMARY KEY)` and `employees(id INTEGER, dept_id INTEGER)` exist
When `ALTER TABLE employees ADD CONSTRAINT fk_dept FOREIGN KEY(dept_id) REFERENCES departments(id)` is executed
Then referential integrity MUST be enforced

### Requirement: ALTER TABLE DROP CONSTRAINT

ALTER TABLE DROP CONSTRAINT SHALL remove a named constraint from a table.

#### Scenario: Drop named constraint

Given a table with constraint `uq_email` exists
When `ALTER TABLE employees DROP CONSTRAINT uq_email` is executed
Then the constraint MUST be removed and previously constrained values MUST be allowed
