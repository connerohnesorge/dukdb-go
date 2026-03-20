# Multi Database Specification

## Requirements

### Requirement: ATTACH Database Statement

The system SHALL support attaching additional databases to a connection using the ATTACH statement. The ATTACH statement SHALL accept a file path or `:memory:` identifier, an optional alias, and optional access mode. Each attached database SHALL have its own isolated catalog, storage, and WAL. The attached database SHALL be accessible via its alias in qualified table names.

#### Scenario: Attach a file-based database

- WHEN user executes `ATTACH 'test.db' AS test_db`
- THEN the database file `test.db` is opened
- AND a new catalog is created for the attached database
- AND tables in the attached database are accessible via `test_db.main.tablename`

#### Scenario: Attach an in-memory database

- WHEN user executes `ATTACH ':memory:' AS mem_db`
- THEN a new in-memory database is created
- AND it is accessible via `mem_db.main.tablename`

#### Scenario: Attach with READ_ONLY option

- WHEN user executes `ATTACH 'data.db' AS ref (READ_ONLY true)`
- THEN the database is attached in read-only mode
- AND any INSERT, UPDATE, or DELETE against tables in `ref` SHALL return an error

#### Scenario: Attach without alias

- WHEN user executes `ATTACH 'sales.db'`
- THEN the database is attached using the filename stem as the alias (`sales`)

#### Scenario: Attach duplicate alias

- WHEN user executes `ATTACH 'a.db' AS mydb` and then `ATTACH 'b.db' AS mydb`
- THEN the second ATTACH SHALL return an error indicating the alias is already in use

#### Scenario: Attach same file with different aliases

- WHEN user executes `ATTACH 'data.db' AS db1` and then `ATTACH 'data.db' AS db2`
- THEN the second ATTACH SHALL return an error indicating the file is already attached
- AND the error message SHALL include the existing alias (`db1`)

#### Scenario: Attach with DATABASE keyword

- WHEN user executes `ATTACH DATABASE 'test.db' AS test_db`
- THEN the behavior is identical to `ATTACH 'test.db' AS test_db`

### Requirement: DETACH Database Statement

The system SHALL support detaching previously attached databases using the DETACH statement. Detaching SHALL close the database's resources (catalog, storage, WAL) and remove it from the connection's database registry. The default database SHALL NOT be detachable.

#### Scenario: Detach an attached database

- WHEN user has attached `ATTACH 'test.db' AS test_db`
- AND user executes `DETACH test_db`
- THEN the database `test_db` is closed and removed from the connection
- AND subsequent references to `test_db.main.tablename` SHALL return an error

#### Scenario: Detach with IF EXISTS

- WHEN user executes `DETACH IF EXISTS nonexistent_db`
- THEN no error is returned

#### Scenario: Detach non-existent database without IF EXISTS

- WHEN user executes `DETACH nonexistent_db`
- THEN an error is returned indicating the database does not exist

#### Scenario: Detach the default database

- WHEN user executes `DETACH main`
- THEN an error is returned indicating the default database cannot be detached

#### Scenario: Detach with pending transaction

- WHEN user begins a transaction with `BEGIN`
- AND user executes `DETACH test_db`
- THEN the DETACH SHALL return an error indicating that databases cannot be detached inside a transaction
- AND the attached database SHALL remain accessible

#### Scenario: Read-only enforcement for DDL statements

- WHEN user has attached `ATTACH 'data.db' AS ref (READ_ONLY true)`
- AND user executes `CREATE TABLE ref.main.new_table (id INTEGER)`
- THEN an error SHALL be returned indicating the database is read-only
- AND the same enforcement SHALL apply to `DROP TABLE`, `ALTER TABLE`, `CREATE INDEX`, and `DROP INDEX` on tables in the read-only database

#### Scenario: Detach with DATABASE keyword

- WHEN user executes `DETACH DATABASE test_db`
- THEN the behavior is identical to `DETACH test_db`

### Requirement: USE Database Statement

The system SHALL support switching the default database and optionally the default schema using the USE statement. After USE, unqualified table references SHALL resolve against the new default database.

#### Scenario: Switch default database

- WHEN user has attached `ATTACH ':memory:' AS db2`
- AND user executes `USE db2`
- THEN unqualified table references resolve against `db2.main`

#### Scenario: Switch default database and schema

- WHEN user executes `USE db2.my_schema`
- THEN unqualified table references resolve against `db2.my_schema`

#### Scenario: USE non-existent database

- WHEN user executes `USE nonexistent`
- THEN an error is returned indicating the database does not exist

### Requirement: Three-Part Qualified Name Resolution

The system SHALL support 3-part qualified names (`database.schema.table`) in all SQL statements that reference tables. When a catalog/database name is specified in a table reference, the system SHALL resolve the table from the corresponding attached database's catalog.

#### Scenario: Cross-database SELECT

- WHEN user has two databases attached (`main` and `db2`)
- AND user executes `SELECT * FROM db2.main.users`
- THEN the query reads from the `users` table in the `db2` database

#### Scenario: Cross-database JOIN

- WHEN user has two databases attached (`main` and `db2`)
- AND user executes `SELECT * FROM main.main.orders o JOIN db2.main.customers c ON o.customer_id = c.id`
- THEN the join reads from both databases correctly

#### Scenario: Two-part name with default database

- WHEN user executes `SELECT * FROM my_schema.my_table`
- THEN the system resolves against the current default database

#### Scenario: Unqualified name with default database

- WHEN user executes `SELECT * FROM my_table`
- THEN the system resolves against the current default database and default schema

#### Scenario: Three-part name in INSERT

- WHEN user executes `INSERT INTO db2.main.target SELECT * FROM main.main.source`
- THEN rows are inserted into the target table in `db2`

#### Scenario: Three-part name in UPDATE

- WHEN user executes `UPDATE db2.main.users SET name = 'test' WHERE id = 1`
- THEN the update operates on the `users` table in `db2`

#### Scenario: Three-part name in DELETE

- WHEN user executes `DELETE FROM db2.main.users WHERE id = 1`
- THEN the delete operates on the `users` table in `db2`

### Requirement: CREATE DATABASE Statement

The system SHALL support creating new databases using the CREATE DATABASE statement. CREATE DATABASE SHALL create and attach a new database in a single operation.

#### Scenario: Create in-memory database

- WHEN user executes `CREATE DATABASE test_db`
- THEN a new in-memory database named `test_db` is created and attached

#### Scenario: Create database with IF NOT EXISTS

- WHEN database `test_db` already exists
- AND user executes `CREATE DATABASE IF NOT EXISTS test_db`
- THEN no error is returned

#### Scenario: Create duplicate database

- WHEN database `test_db` already exists
- AND user executes `CREATE DATABASE test_db`
- THEN an error is returned indicating the database already exists

### Requirement: DROP DATABASE Statement

The system SHALL support dropping databases using the DROP DATABASE statement. DROP DATABASE SHALL detach the database and, for file-based databases, optionally remove the file.

#### Scenario: Drop an attached database

- WHEN database `test_db` is attached
- AND user executes `DROP DATABASE test_db`
- THEN the database is detached and removed

#### Scenario: Drop with IF EXISTS

- WHEN database `nonexistent` does not exist
- AND user executes `DROP DATABASE IF EXISTS nonexistent`
- THEN no error is returned

#### Scenario: Drop the default database

- WHEN user executes `DROP DATABASE main`
- THEN an error is returned indicating the default database cannot be dropped

### Requirement: System Function Integration

The system SHALL update `duckdb_databases()` and `PRAGMA database_list` to reflect all currently attached databases, including their names, paths, access modes, and whether they are the default database.

#### Scenario: duckdb_databases reflects attached databases

- WHEN user attaches `ATTACH ':memory:' AS db2`
- AND user executes `SELECT * FROM duckdb_databases()`
- THEN the result includes both `main` and `db2` with their properties

#### Scenario: PRAGMA database_list reflects attached databases

- WHEN user attaches `ATTACH 'test.db' AS test_db`
- AND user executes `PRAGMA database_list`
- THEN the result includes both `main` and `test_db` with their paths

### Requirement: Transaction Behavior with Attached Databases

The system SHALL support reading from attached databases within a transaction. The system SHALL return a clear error when attempting to write to a non-default database within a transaction that also writes to the default database.

#### Scenario: Cross-database read in transaction

- WHEN user begins a transaction
- AND user executes `SELECT * FROM db2.main.users`
- THEN the read succeeds and returns data from the attached database

#### Scenario: Write to default database in transaction

- WHEN user begins a transaction
- AND user executes `INSERT INTO main.main.orders VALUES (1, 'test')`
- THEN the write succeeds normally

#### Scenario: Attempt write to non-default database

- WHEN user has attached `ATTACH ':memory:' AS db2`
- AND user begins a transaction
- AND user executes `INSERT INTO db2.main.users VALUES (1, 'alice')`
- THEN the system SHALL return an error with message containing "cannot write to non-default database"
- AND the transaction SHALL remain active (not automatically rolled back)
- AND subsequent writes to the default database within the same transaction SHALL still succeed

#### Scenario: Attempt write to non-default database outside transaction

- WHEN user has attached `ATTACH ':memory:' AS db2`
- AND there is no active explicit transaction
- AND user executes `INSERT INTO db2.main.users VALUES (1, 'alice')`
- THEN the system SHALL return an error with message containing "cannot write to non-default database"

#### Scenario: Connection close detaches all databases

- WHEN user has multiple databases attached
- AND the connection is closed
- THEN all attached databases are properly closed and their resources released

