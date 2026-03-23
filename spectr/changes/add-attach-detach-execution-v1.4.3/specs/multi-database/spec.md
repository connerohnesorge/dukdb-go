## ADDED Requirements

### Requirement: ATTACH DATABASE SHALL open and register external database files

The system SHALL execute `ATTACH [DATABASE] 'path' [AS alias] [(options)]` by opening the specified database file, initializing a catalog and storage for it, and registering it with the DatabaseManager. When no alias is given, the filename without extension SHALL be used. The system SHALL support `READ_ONLY` option and `:memory:` for in-memory attached databases.

#### Scenario: ATTACH with explicit alias

- WHEN the user executes `ATTACH 'analytics.db' AS analytics`
- THEN the database file is opened and registered as 'analytics'
- AND `SELECT * FROM analytics.main.some_table` resolves against the attached database

#### Scenario: ATTACH with default alias

- WHEN the user executes `ATTACH '/path/to/sales.db'`
- THEN the database is registered with alias 'sales' (filename without extension)

#### Scenario: ATTACH with READ_ONLY option

- WHEN the user executes `ATTACH 'shared.db' AS shared (READ_ONLY)`
- THEN `INSERT INTO shared.t VALUES (1)` returns an error containing "read-only"

#### Scenario: ATTACH in-memory database

- WHEN the user executes `ATTACH ':memory:' AS temp`
- THEN an in-memory database is registered as 'temp'

#### Scenario: ATTACH with duplicate alias fails

- WHEN a database named 'analytics' is already attached
- AND the user executes `ATTACH 'other.db' AS analytics`
- THEN the system returns an error containing "already exists"

### Requirement: DETACH DATABASE SHALL remove an attached database

The system SHALL execute `DETACH [DATABASE] [IF EXISTS] name` by flushing pending writes, closing storage, and removing the database from the registry. The primary database SHALL NOT be detachable.

#### Scenario: DETACH existing database

- WHEN the database 'analytics' is attached
- AND the user executes `DETACH analytics`
- THEN `SELECT * FROM analytics.t` returns an error containing "not found"

#### Scenario: DETACH with IF EXISTS on non-existent database

- WHEN no database named 'foo' is attached
- AND the user executes `DETACH IF EXISTS foo`
- THEN no error is raised

#### Scenario: DETACH primary database fails

- WHEN the user executes `DETACH memory` (the primary database)
- THEN the system returns an error containing "cannot detach"

### Requirement: USE SHALL change the current database/schema context

The system SHALL execute `USE database[.schema]` to change which database and schema unqualified table names resolve against.

#### Scenario: USE changes default database

- WHEN the database 'analytics' is attached
- AND the user executes `USE analytics`
- AND the user executes `CREATE TABLE t (id INT)`
- THEN the table is created in the 'analytics' database

#### Scenario: USE with schema qualification

- WHEN the user executes `USE analytics.public`
- THEN unqualified names resolve against database 'analytics', schema 'public'

### Requirement: Cross-database name resolution SHALL support three-part names

The system SHALL resolve table references with three-part names (database.schema.table) by looking up the database in the DatabaseManager, the schema in that database's catalog, and the table in that schema.

#### Scenario: Three-part name resolution

- WHEN the database 'analytics' is attached with a table 'sales' in schema 'main'
- AND the user executes `SELECT * FROM analytics.main.sales`
- THEN the query returns data from the attached database's table

#### Scenario: Two-part name checks database first

- WHEN the database 'analytics' is attached with a table 'sales'
- AND the user executes `SELECT * FROM analytics.sales`
- THEN the system checks if 'analytics' is an attached database name
- AND resolves 'sales' in that database's default schema

#### Scenario: Cross-database JOIN

- WHEN databases 'db1' and 'db2' are attached
- AND the user executes `SELECT a.id FROM db1.t1 a JOIN db2.t2 b ON a.id = b.id`
- THEN the join operates across both attached databases

### Requirement: CREATE/DROP DATABASE SHALL manage database lifecycle

The system SHALL execute `CREATE DATABASE [IF NOT EXISTS] name` to create a new empty database and attach it. The system SHALL execute `DROP DATABASE [IF EXISTS] name` to detach and optionally remove a database.

#### Scenario: CREATE DATABASE creates and attaches

- WHEN the user executes `CREATE DATABASE test_db`
- THEN a new empty database is created and attached as 'test_db'

#### Scenario: DROP DATABASE detaches and removes

- WHEN the database 'test_db' is attached
- AND the user executes `DROP DATABASE test_db`
- THEN the database is detached and the file is removed

### Requirement: SHOW DATABASES SHALL list all attached databases

The system SHALL support `SHOW DATABASES` to list all currently attached databases with their name, path, and read-only status.

#### Scenario: SHOW DATABASES lists primary and attached

- WHEN the primary database is in-memory
- AND the user has attached 'analytics.db' as 'analytics'
- AND the user executes `SHOW DATABASES`
- THEN the result includes both 'memory' and 'analytics' entries
