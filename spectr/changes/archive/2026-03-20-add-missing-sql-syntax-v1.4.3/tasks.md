# Tasks: Complete Missing SQL Syntax for DuckDB v1.4.3

- [ ] 1. Add IfExists field to TruncateStmt and update parser — Add `IfExists bool` field to `TruncateStmt` in `internal/parser/ast.go:426`. In `parseTruncate()` at `internal/parser/parser.go:2416`, insert IF EXISTS parsing after optional TABLE keyword. Validate with parser tests: `TRUNCATE TABLE IF EXISTS t`, `TRUNCATE IF EXISTS schema.t`.

- [ ] 2. Update binder and executor for TRUNCATE IF EXISTS — In `internal/binder/bind_stmt.go:3093`, when IfExists is true and table not found, return a no-op bound statement. In `internal/executor/ddl.go:838`, handle no-op case returning `{RowsAffected: 0}`. Validate with integration test: TRUNCATE TABLE IF EXISTS on non-existent table returns no error.

- [ ] 3. Fix TRUNCATE schema-qualified storage lookup — In `internal/executor/ddl.go:846`, change `e.storage.GetTable(plan.Table)` to `e.storage.GetTableInSchema(plan.Schema, plan.Table)` with fallback to unqualified lookup. Validate: `CREATE SCHEMA s; CREATE TABLE s.t(id INT); INSERT INTO s.t VALUES (1); TRUNCATE TABLE s.t;` succeeds.

- [ ] 4. Add WAL logging and undo recording for TRUNCATE — Add WAL entry type for TRUNCATE. In `executeTruncate()`, log WAL entry after truncate and record undo snapshot before truncate for transaction rollback. Validate: `BEGIN; TRUNCATE TABLE t; ROLLBACK;` restores original data.

- [ ] 5. Clear index entries on TRUNCATE — In `executeTruncate()`, after calling `table.Truncate()`, look up indexes for the table via catalog and call `Clear()` on each storage index. Validate: after TRUNCATE, re-inserting a previously-existing key succeeds without unique violation.

- [ ] 6. Improve VALUES type inference to use supertype promotion — In `internal/binder/bind_stmt.go:501`, replace first-non-NULL type selection with `types.GetCommonSupertype()` across all rows per column. Insert implicit CAST nodes where expression types differ from inferred column type. Default all-NULL columns to VARCHAR. Validate: `VALUES (1, 'text'), (2.5, NULL)` produces DOUBLE and VARCHAR columns.

- [ ] 7. Integration tests for all changes — Write comprehensive tests covering: TRUNCATE IF EXISTS (exists/not exists), schema-qualified TRUNCATE, TRUNCATE in transaction with ROLLBACK, TRUNCATE with indexes, VALUES type promotion with mixed types. Verify no regressions in existing test suite.
