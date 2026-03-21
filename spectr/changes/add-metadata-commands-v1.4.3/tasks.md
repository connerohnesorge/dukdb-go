# Tasks: Metadata Commands

- [ ] 1. Add DESCRIBE statement — Add `DescribeStmt` to ast.go. Add `parseDescribe()` to parser_pragma.go. Register "DESCRIBE" and "DESC" keywords in parser.go main switch. Support DESCRIBE table and DESCRIBE SELECT. Validate: `DESCRIBE employees` parses correctly.

- [ ] 2. Implement DESCRIBE execution — Handle DescribeStmt in engine/conn.go alongside handleShow() (line 626). For tables, return column_name, column_type, null, key, default, extra from TableDef.Columns (catalog/table.go:24). For queries, bind the inner query and return output column metadata. Use inline null-to-YES/NO logic (no boolToYesNo helper exists). Use `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: ...}` for errors. Validate: `DESCRIBE employees` returns correct column info.

- [ ] 3. Extend SHOW for TABLES and COLUMNS — Extend `parseShow()` at parser_pragma.go:275 to handle SHOW TABLES, SHOW ALL TABLES, SHOW COLUMNS FROM table. Add TableName field to ShowStmt (ast.go:1662). In handleShow() at conn.go:626, add cases for __tables, __all_tables, __columns. Use Schema.ListTables() (catalog.go:675). Validate: `SHOW TABLES` returns table list.

- [ ] 4. Add SUMMARIZE statement — Add `SummarizeStmt` to ast.go. Add `parseSummarize()` to parser_pragma.go. Register in main parser switch. Execute by querying all rows and computing per-column min, max, unique_count, null_count, avg, std. Validate: `SUMMARIZE employees` returns statistics.

- [ ] 5. Add CALL statement — Add `CallStmt` to ast.go with Type() returning STATEMENT_TYPE_CALL (already exists in stmt_type.go:44). Add `parseCall()` to parser_pragma.go. Register "CALL" keyword in parser switch. Execute by delegating to table function execution (executeTableFunctionScan) for table functions, or wrapping scalar function result. Validate: `CALL generate_series(1, 5)` returns rows.

- [ ] 6. Integration tests — Test all commands with existing tables, empty tables, non-existent tables. Verify DESCRIBE SELECT returns correct types. Verify SHOW TABLES lists all tables. Verify SUMMARIZE produces valid statistics.
