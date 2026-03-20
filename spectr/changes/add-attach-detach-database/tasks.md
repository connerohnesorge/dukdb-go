## 1. Parser Support (including 3-part name parsing)

- [ ] 1.1 Add `AttachStmt`, `DetachStmt`, `UseStmt`, `CreateDatabaseStmt`, `DropDatabaseStmt` AST nodes to `internal/parser/ast.go`
- [ ] 1.2 Implement ATTACH parsing: `ATTACH [DATABASE] 'path' [AS alias] [(READ_ONLY [true|false])]` in `internal/parser/parser.go`
- [ ] 1.3 Implement DETACH parsing: `DETACH [DATABASE] [IF EXISTS] name` in `internal/parser/parser.go`
- [ ] 1.4 Implement USE parsing: `USE database[.schema]` in `internal/parser/parser.go`
- [ ] 1.5 Implement CREATE DATABASE parsing: `CREATE DATABASE [IF NOT EXISTS] name [PATH 'path']`
- [ ] 1.6 Implement DROP DATABASE parsing: `DROP DATABASE [IF EXISTS] name [CASCADE]`
- [ ] 1.7 Add Visitor pattern methods for all new AST nodes
- [ ] 1.8 Update table name parsing to accept `catalog.schema.table` 3-part syntax in SELECT FROM, INSERT INTO, UPDATE, DELETE FROM, and CREATE TABLE (moved from old task 9 — this is a prerequisite for binder work)
- [ ] 1.9 Write parser unit tests for all new statement types and 3-part name combinations

## 2. DatabaseManager

- [ ] 2.1 Create `internal/engine/database_manager.go` with `DatabaseManager` struct and `AttachedDatabase` struct
- [ ] 2.2 Implement `Register()`, `Unregister()`, `GetDatabase()`, `DefaultDatabase()`, `SetDefaultDatabase()`, `ListDatabases()` methods
- [ ] 2.3 Implement `AttachDatabase()` that opens a database file (or creates in-memory), creates Catalog/Storage/WAL, and registers it
- [ ] 2.4 Implement `DetachDatabase()` that closes resources and unregisters
- [ ] 2.5 Add thread-safety with RWMutex for concurrent access
- [ ] 2.6 Write unit tests for DatabaseManager lifecycle (register, unregister, lookup, default switching)

## 3. Engine Integration

- [ ] 3.1 Add `DatabaseManager` field to `Conn` struct in `internal/engine/conn.go`
- [ ] 3.2 Initialize `DatabaseManager` in `Engine.Open()` and register the primary database as "main"
- [ ] 3.3 Create `CatalogResolver` interface in `internal/binder/catalog_resolver.go` and implement it on `DatabaseManager`. Pass resolver to `NewBinder()` in `EngineConn.Execute()` and `EngineConn.Query()`
- [ ] 3.4 Update connection close to detach all attached databases

## 4. Binder: 3-Part Name Resolution

- [ ] 4.1 Update `bindTableRef()` to resolve `TableRef.Catalog` field using `DatabaseManager`
- [ ] 4.2 Update `bindInsertStmt()` to support catalog-qualified table names
- [ ] 4.3 Update `bindUpdateStmt()` to support catalog-qualified table names
- [ ] 4.4 Update `bindDeleteStmt()` to support catalog-qualified table names
- [ ] 4.5 Update `bindCreateTableStmt()` to support catalog-qualified table names
- [ ] 4.6 Update `bindDropTableStmt()` to support catalog-qualified table names
- [ ] 4.7 Write integration tests for 3-part name resolution across all DML/DDL statement types

## 5. Connection-Level Handling: ATTACH/DETACH/USE

ATTACH, DETACH, USE, CREATE DATABASE, and DROP DATABASE are connection-level operations (like BEGIN/COMMIT). They are handled in `EngineConn.Execute()` before the binder/planner/executor pipeline.

- [ ] 5.1 Add `handleAttach()` method to `EngineConn` in `internal/engine/conn.go` — validates no pending transaction, opens target database, creates AttachedDatabase, registers with DatabaseManager
- [ ] 5.2 Add `handleDetach()` method to `EngineConn` — validates no pending transaction, looks up database, closes resources, unregisters
- [ ] 5.3 Add `handleUse()` method to `EngineConn` — sets default database (and optionally schema) on connection's DatabaseManager
- [ ] 5.4 Add `handleCreateDatabase()` method to `EngineConn` — creates new database file/memory, registers
- [ ] 5.5 Add `handleDropDatabase()` method to `EngineConn` — detaches and deletes database file if file-based
- [ ] 5.6 Wire new statement types into the `switch` block in `EngineConn.Execute()` and `EngineConn.Query()` (alongside existing BEGIN/COMMIT cases)
- [ ] 5.7 Write integration tests for ATTACH/DETACH/USE/CREATE DATABASE/DROP DATABASE execution, including error cases (ATTACH inside transaction, DETACH inside transaction)

## 6. Cross-Database Queries

- [ ] 6.1 Update planner to propagate database reference through logical plan nodes
- [ ] 6.2 Update physical scan operator to read from the correct database's storage
- [ ] 6.3 Write integration tests for cross-database SELECT (JOIN across two databases)
- [ ] 6.4 Write integration tests for cross-database INSERT...SELECT

## 7. System Functions Update

- [ ] 7.1 Update `duckdb_databases()` to return all attached databases from DatabaseManager
- [ ] 7.2 Update `PRAGMA database_list` to reflect attached databases
- [ ] 7.3 Update `PRAGMA database_size` to work with database name parameter
- [ ] 7.4 Write tests for updated system functions

## 8. Transaction Coordination

- [ ] 8.1 Ensure transactions on the default database work unchanged
- [ ] 8.2 Add read-only transaction support for attached databases (cross-database reads within a transaction)
- [ ] 8.3 Add write-guard in executor: when the target table's catalog belongs to a non-default database, return error "cannot write to non-default database 'X' in a transaction" (applies to INSERT, UPDATE, DELETE, and DDL)
- [ ] 8.4 Add read-only enforcement: when the target database has `ReadOnly: true`, reject all DML (INSERT/UPDATE/DELETE) and DDL (CREATE TABLE/DROP TABLE/ALTER TABLE/CREATE INDEX/DROP INDEX) with error "database 'X' is read-only"
- [ ] 8.5 Write tests for: write to non-default db (both in-txn and auto-commit), write to read-only db, read from non-default db succeeds, ATTACH/DETACH inside transaction fails

## 9. (Removed — merged into Task 1)

3-part name parsing was moved to Task 1.8 as it is a prerequisite for binder name resolution work in Task 4.
