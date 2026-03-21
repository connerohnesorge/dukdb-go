# Tasks: DDL/DML Extensions

## COMMENT ON

- [ ] 1. Add CommentStmt AST type — In `internal/parser/ast.go`, add `CommentStmt` struct with ObjectType, Schema, ObjectName, ColumnName, Comment fields. Add `stmtNode()` method. Validate: AST compiles.

- [ ] 2. Parse COMMENT ON statement — In `internal/parser/parser_ddl.go`, add `parseComment()` function. Register "COMMENT" keyword in main parser switch at parser.go:47-100. Handle TABLE, COLUMN, VIEW, INDEX, SCHEMA object types. Handle `IS 'text'` and `IS NULL` (to drop). Validate: `COMMENT ON TABLE t IS 'hello'` parses without error.

- [ ] 3. Add Comment field to catalog types — In `internal/catalog/table.go`, add `Comment string` to `TableDef`. In `internal/catalog/column.go`, add `Comment string` to `ColumnDef`. Validate: catalog compiles.

- [ ] 4. Bind and plan COMMENT ON — Add `BoundCommentStmt` to `internal/binder/statements.go`. Add `bindComment()` to `internal/binder/bind_ddl.go` (resolve object exists). Add `PhysicalComment` plan node to `internal/planner/physical.go`. Validate: binding resolves table/column references.

- [ ] 5. Execute COMMENT ON — Add `executeComment()` to `internal/executor/ddl.go`. Register in executor dispatch at `operator.go:364-495`. Update catalog metadata with comment text. Validate: `COMMENT ON TABLE t IS 'hello'` succeeds and comment is stored.

## ALTER TABLE ALTER COLUMN TYPE

- [ ] 6. Add AlterTableAlterColumnType operation — In `internal/parser/ast.go`, add `AlterTableAlterColumnType` to `AlterTableOp` enum. Add `AlterColumn string` and `NewColumnType` fields to `AlterTableStmt`. Validate: AST compiles.

- [ ] 7. Parse ALTER TABLE ALTER COLUMN TYPE — In `internal/parser/parser_ddl.go`, in `parseAlterTable()` (line 470), add case for `ALTER [COLUMN] colname [SET DATA] TYPE typename`. Use existing `parseColumnType()` for type parsing. Validate: `ALTER TABLE t ALTER COLUMN c TYPE INTEGER` parses correctly.

- [ ] 8. Bind and execute ALTER COLUMN TYPE — In `internal/binder/bind_ddl.go`, add case in `bindAlterTable()` for type resolution. In `internal/executor/ddl.go`, add case in `executeAlterTable()` — update column type in catalog and convert storage data. Validate: `ALTER TABLE t ALTER COLUMN c TYPE INTEGER` changes column type and existing data is converted.

## DELETE ... USING

- [ ] 9. Add Using field to DeleteStmt — In `internal/parser/ast.go`, add `Using []TableRef` field to `DeleteStmt` (line 359). Validate: AST compiles.

- [ ] 10. Parse DELETE ... USING — In `internal/parser/parser.go`, in `parseDelete()` (line 1879), add USING clause parsing after FROM table and before WHERE. Use existing `parseTableRef()` for each USING table. Support comma-separated multiple USING tables. Validate: `DELETE FROM t1 USING t2 WHERE t1.id = t2.id` parses correctly.

- [ ] 11. Bind and plan DELETE USING — In `internal/binder/bind_stmt.go`, extend `bindDelete()` (line 3086) to bind USING table references. Push USING table scopes for WHERE column resolution. In planner, generate cross-join of target + USING tables filtered by WHERE. Validate: `DELETE FROM orders USING customers WHERE orders.cid = customers.id` binds and plans correctly.

- [ ] 12. Integration tests — Test all three features: COMMENT ON (set, drop, column comments, errors), ALTER COLUMN TYPE (conversion, errors), DELETE USING (single table, multi-table, with RETURNING). Verify no regressions in existing ALTER TABLE and DELETE tests.
