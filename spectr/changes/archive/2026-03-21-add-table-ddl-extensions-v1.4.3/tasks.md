# Tasks: Table DDL Extensions

- [ ] 1. Add OrReplace and Temporary to CreateTableStmt — Add `OrReplace bool` and `Temporary bool` fields to CreateTableStmt (ast.go:391-410). Update parseCreateTable() signature (parser.go:2063) to accept these parameters. Pass `orReplace` and `temporary` from parseCreate() at line 2037. Thread through BoundCreateTableStmt (binder/statements.go:134-142) and PhysicalCreateTable (planner/physical.go:600-607). Validate: `CREATE OR REPLACE TABLE t(x INT)` parses with OrReplace=true.

- [ ] 2. Implement CREATE OR REPLACE TABLE execution — In executeCreateTable() (executor/operator.go:2603-2727), when OrReplace is true, drop existing table before creating. Reuse existing drop logic. OR REPLACE and IF NOT EXISTS should be mutually exclusive (error if both). Use `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: ...}` for errors. Validate: CREATE OR REPLACE on existing table succeeds.

- [ ] 3. Implement CREATE TEMP TABLE execution — In executeCreateTable(), when Temporary is true, create table in "temp" schema. Auto-create temp schema if needed. Register for cleanup on connection close. Validate: `CREATE TEMP TABLE t(x INT)` creates table in temp schema.

- [ ] 4. Add ALTER TABLE ADD CONSTRAINT — Add `AlterTableAddConstraint` to AlterTableOp enum (ast.go:655-664). Add `Constraint *TableConstraint` field to AlterTableStmt (ast.go:654-681). In parseAlterTable() (parser_ddl.go:470-604), handle ADD CONSTRAINT/UNIQUE/CHECK/FOREIGN KEY by reusing parseTableConstraint() (parser.go:2212-2299). Validate: `ALTER TABLE t ADD CONSTRAINT uq UNIQUE(name)` parses correctly.

- [ ] 5. Add ALTER TABLE DROP CONSTRAINT — Add `AlterTableDropConstraint` to AlterTableOp enum. Add `ConstraintName string` to AlterTableStmt. Parse DROP CONSTRAINT name [IF EXISTS] in parseAlterTable(). Validate: `ALTER TABLE t DROP CONSTRAINT uq` parses correctly.

- [ ] 6. Implement ADD/DROP CONSTRAINT execution — In executeAlterTable() (executor/ddl.go:457-576), add cases for AddConstraint (validate columns, append to TableDef.Constraints at catalog/table.go:32-34) and DropConstraint (find by name, remove from slice). For FK constraints, validate referenced table/columns exist. Use error pattern `&dukdb.Error{}`. Validate: Adding and dropping constraints works correctly.

- [ ] 7. Integration tests — Test CREATE OR REPLACE TABLE with existing/non-existing tables. Test CREATE TEMP TABLE visibility and schema. Test ALTER TABLE ADD CONSTRAINT for UNIQUE/CHECK/FK. Test ALTER TABLE DROP CONSTRAINT with named constraints. Test error cases: conflicting OR REPLACE + IF NOT EXISTS, drop non-existent constraint, add constraint on missing column.
