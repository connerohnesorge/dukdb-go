# Tasks: ALTER COLUMN SET/DROP DEFAULT and NOT NULL

- [ ] 1. AST additions — Add AlterTableSetColumnDefault, AlterTableDropColumnDefault, AlterTableSetColumnNotNull, AlterTableDropColumnNotNull to AlterTableOp at ast.go:659-668. Add DefaultExpr field to AlterTableStmt. Validate: Compiles without error.

- [ ] 2. Parser changes — Extend parseAlterTable() at parser_ddl.go:598-631 to handle SET DEFAULT expr, DROP DEFAULT, SET NOT NULL, DROP NOT NULL after ALTER COLUMN col_name. Use p.parseExpr() for default value. Validate: `ALTER TABLE t ALTER COLUMN c SET DEFAULT 42` parses.

- [ ] 3. Executor handlers — Add execution cases for 4 new operations in ddl.go ALTER TABLE handler. Use catalog API to find table/column and update metadata. SET NOT NULL must validate no existing NULLs. Validate: `ALTER TABLE t ALTER COLUMN c SET DEFAULT 42; INSERT INTO t() VALUES (DEFAULT)` works.

- [ ] 4. Integration tests — Test SET/DROP DEFAULT with various types. Test SET NOT NULL with and without existing NULLs. Test DROP NOT NULL. Test existing ALTER COLUMN TYPE still works.
