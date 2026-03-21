# Tasks: RESET Statement

- [ ] 1. Add ResetStmt AST and parser — Add `ResetStmt` to ast.go with Variable and All fields. Add parseReset() to parser_pragma.go following parseSet() pattern (line 187). Register "RESET" in parser.go main switch (line 47-129). Support RESET variable and RESET ALL. Validate: `RESET transaction_isolation` parses correctly.

- [ ] 2. Implement RESET execution — Handle ResetStmt in engine/conn.go. For specific variables, reset to default value. For RESET ALL, reset all settings. Use `&dukdb.Error{}` for unknown variables. Validate: SET then RESET then SHOW returns default.

- [ ] 3. Integration tests — Test RESET with SET/SHOW round-trip. Test RESET ALL. Test RESET of unknown variable.
