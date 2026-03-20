# Change: Add Foreign Key Constraint Enforcement

## Why

Foreign key constraints are currently parsed but not enforced by the dukdb-go engine. The parser recognizes `REFERENCES` as a type-spec terminator keyword (`internal/parser/parser.go:2169`) but does not actually parse FK syntax (column-level REFERENCES or table-level FOREIGN KEY). The catalog defines `ConstraintForeignKey` (`internal/catalog/constraint.go:12`) but has no `ForeignKeyConstraintDef` struct. The executor performs no FK validation on INSERT, UPDATE, or DELETE. This means referential integrity is silently ignored, which is incorrect behavior for any SQL database claiming constraint support.

## What Changes

- **Parser**: Add parsing for column-level `REFERENCES parent(col)` and table-level `FOREIGN KEY (cols) REFERENCES parent(cols)` with optional `ON DELETE` and `ON UPDATE` action clauses (NO ACTION, RESTRICT only; CASCADE/SET NULL/SET DEFAULT rejected at parse time)
- **AST**: Add `ForeignKeyRef` field to `ColumnDefClause` and `FOREIGN_KEY` type to `TableConstraint`
- **Catalog**: Add `ForeignKeyConstraintDef` struct storing child columns, referenced table/columns, and referential actions; store in `TableDef.Constraints`
- **Executor (INSERT)**: Before inserting rows into a child table, verify that each FK column value exists in the referenced parent table's key columns (NULL FK values are allowed)
- **Executor (UPDATE)**: On updating a child table's FK columns, verify new values exist in the parent; on updating a parent table's key columns, verify no child rows reference the old values
- **Executor (DELETE)**: Before deleting rows from a parent table, verify no child rows reference the deleted key values
- **Executor (CREATE TABLE)**: Validate that the referenced parent table and columns exist at table creation time
- **Error messages**: Use `dukdb.Error{Type: dukdb.ErrorTypeConstraint, Msg: "..."}` for FK violations

## Impact

- Affected specs: `parser`, `catalog`, `execution-engine`
- Affected code:
  - `internal/parser/parser.go` - parseColumnDef, parseTableConstraint, parseCreateTable
  - `internal/parser/ast.go` - ColumnDefClause, TableConstraint
  - `internal/catalog/constraint.go` - ForeignKeyConstraintDef
  - `internal/catalog/table.go` - TableDef.Constraints
  - `internal/executor/operator.go` - executeInsert
  - `internal/executor/physical_update.go` - executeUpdate
  - `internal/executor/physical_delete.go` - executeDelete
  - `internal/executor/operator.go` - CREATE TABLE validation (executeCreateTable at ~line 2573)
