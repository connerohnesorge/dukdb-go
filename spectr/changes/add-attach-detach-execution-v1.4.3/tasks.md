## 1. Binder

- [ ] 1.1 Add `BoundAttachStmt`, `BoundDetachStmt`, `BoundUseStmt`, `BoundCreateDatabaseStmt`, `BoundDropDatabaseStmt` types to `internal/binder/statements.go`, each implementing the `BoundStatement` interface (`boundStatement()` marker method).
- [ ] 1.2 Add binding cases in `Bind()` switch in `internal/binder/binder.go` for `*parser.AttachStmt`, `*parser.DetachStmt`, `*parser.UseStmt`, `*parser.CreateDatabaseStmt`, `*parser.DropDatabaseStmt`.

## 2. Planner — Logical Plan Nodes

- [ ] 2.1 Add `LogicalAttach`, `LogicalDetach`, `LogicalUse`, `LogicalCreateDatabase`, `LogicalDropDatabase` structs in `internal/planner/logical.go`, each implementing the `LogicalPlan` interface (`logicalPlanNode()`, `Children()`, `OutputColumns()`).
- [ ] 2.2 Add `createLogicalPlan()` cases for the five new `Bound*Stmt` types, converting each to its `Logical*` equivalent.

## 3. Planner — Physical Plan Nodes

- [ ] 3.1 Add `PhysicalAttach`, `PhysicalDetach`, `PhysicalUse`, `PhysicalCreateDatabase`, `PhysicalDropDatabase` structs in `internal/planner/physical.go`, each implementing the `PhysicalPlan` interface via the `physicalPlanNode()` marker method (there is no `PhysicalPlanType` enum).
- [ ] 3.2 Add `createPhysicalPlan()` cases for the five new `Logical*` types, converting each to its `Physical*` equivalent.
- [ ] 3.3 Add planner unit tests for all five new statement types (binder -> logical -> physical).

## 4. Executor — ATTACH

- [ ] 4.1 Add `executeAttach()` in `internal/executor/ddl.go` that resolves the path, opens/creates the database file, determines alias (filename without extension if not specified), and calls `DatabaseManager.Attach()`.
- [ ] 4.2 Create new `OpenDatabase(path string, readOnly bool)` method on `Engine` (does not exist yet) that creates a new Catalog and Storage for the attached database file. Also create `loadCatalogFromStorage()` helper (does not exist yet).
- [ ] 4.3 Handle ATTACH options: `READ_ONLY`, `TYPE` (currently only 'duckdb' supported).
- [ ] 4.4 Handle `:memory:` as a special case for in-memory attached databases.
- [ ] 4.5 Add integration test: `ATTACH 'test.db' AS test_db` followed by `CREATE TABLE test_db.t (id INT)` and `SELECT * FROM test_db.t`.

## 5. Executor — DETACH

- [ ] 5.1 Add `executeDetach()` in `internal/executor/ddl.go` that calls `DatabaseManager.Detach()`.
- [ ] 5.2 Flush and close the detached database's storage (WAL checkpoint, close file handles).
- [ ] 5.3 Handle IF EXISTS variant (no error when database not found).
- [ ] 5.4 Prevent detaching the primary database (error: "cannot detach the primary database").
- [ ] 5.5 Add integration test: ATTACH then DETACH, verify tables no longer accessible.

## 6. Executor — USE

- [ ] 6.1 Add `executeUse()` in `internal/executor/ddl.go` that calls `DatabaseManager.Use(database)` (single argument — database name only, no schema parameter).
- [ ] 6.2 After USE, unqualified table names resolve against the new current database.
- [ ] 6.3 Support `USE database` form. For `USE database.schema`, call `Use(database)` and set the schema on the session context separately.
- [ ] 6.4 Add integration test: ATTACH db, USE db, CREATE TABLE (unqualified), verify table is in attached db.

## 7. Executor — CREATE/DROP DATABASE

- [ ] 7.1 Add `executeCreateDatabase()` — creates a new empty database file at the default location (or specified path) and attaches it.
- [ ] 7.2 Add `executeDropDatabase()` — detaches the database and optionally removes the file.
- [ ] 7.3 Handle IF NOT EXISTS / IF EXISTS variants.
- [ ] 7.4 Add integration tests for CREATE DATABASE and DROP DATABASE.

## 8. Cross-Database Name Resolution

- [ ] 8.1 In the binder, modify table reference resolution to support three-part names (db.schema.table).
- [ ] 8.2 For two-part names (x.y), check if `x` is an attached database name before treating it as schema.table.
- [ ] 8.3 Add integration test: ATTACH db, then `SELECT * FROM db.main.table_name`.
- [ ] 8.4 Add integration test: cross-database JOIN (`SELECT a.* FROM db1.t1 a JOIN db2.t2 b ON a.id = b.id`).

## 9. Parser Fix

- [ ] 9.1 Fix `UseStmt.Type()` at `internal/parser/ast.go:1615` — currently returns `STATEMENT_TYPE_SET` which conflates USE with SET statements. Consider adding a dedicated `STATEMENT_TYPE_USE` or using `STATEMENT_TYPE_ATTACH` to properly distinguish it.

## 10. Operator Registration

- [ ] 10.1 Register all five new operator types in `internal/executor/operator.go`.
- [ ] 10.2 Add execution dispatch in the main executor switch statement.

## 11. System Functions

- [ ] 11.1 Add `SHOW DATABASES` support that lists all attached databases with name, path, read_only status.
- [ ] 11.2 Add `duckdb_databases()` table function as an alternative interface.
- [ ] 11.3 Add integration tests for SHOW DATABASES.

## 12. End-to-End Integration Tests

- [ ] 12.1 Full workflow: ATTACH -> CREATE TABLE -> INSERT -> SELECT -> DETACH.
- [ ] 12.2 Multiple attached databases with cross-database queries.
- [ ] 12.3 Read-only ATTACH prevents writes.
- [ ] 12.4 ATTACH with duplicate alias returns error.
- [ ] 12.5 File persistence: ATTACH, INSERT, DETACH, re-ATTACH, verify data persists.
