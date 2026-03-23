# Proposal: Add ALTER COLUMN SET/DROP DEFAULT and NOT NULL

## Summary

Implement four missing ALTER TABLE ALTER COLUMN operations: SET DEFAULT, DROP DEFAULT, SET NOT NULL, DROP NOT NULL. Currently only ALTER COLUMN ... TYPE is supported (parser_ddl.go:598-630). The parser errors on SET at line 597 with "ALTER TABLE SET not yet implemented".

## Motivation

These are standard SQL DDL operations critical for schema evolution. Applications need to add/remove column defaults and change nullability constraints without recreating tables.

## Scope

- **AST**: Add 4 new AlterTableOp constants at ast.go:659-668
- **Parser**: Extend parseAlterTable() at parser_ddl.go:598-631 to handle SET DEFAULT, DROP DEFAULT, SET NOT NULL, DROP NOT NULL after ALTER COLUMN
- **Executor**: Add execution handlers in ddl.go for the 4 new operations
- **Catalog**: Update column metadata for defaults and nullability

## Files Affected

- `internal/parser/ast.go` — AlterTableOp constants (line 659) + AlterTableStmt fields (line 671)
- `internal/parser/parser_ddl.go` — parseAlterTable() (line 598)
- `internal/executor/ddl.go` — ALTER TABLE execution
- `internal/catalog/` — column metadata updates
