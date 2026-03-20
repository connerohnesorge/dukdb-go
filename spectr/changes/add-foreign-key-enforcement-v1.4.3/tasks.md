## 1. Parser: FK AST and Parsing

- [ ] 1.1 Add `ForeignKeyAction` constants and `ForeignKeyRef` struct to `internal/parser/ast.go`
- [ ] 1.2 Add `ForeignKey *ForeignKeyRef` field to `ColumnDefClause` in `internal/parser/ast.go`
- [ ] 1.3 Extend `TableConstraint` with `RefTable`, `RefColumns`, `OnDelete`, `OnUpdate` fields in `internal/parser/ast.go`
- [ ] 1.4 Implement `parseForeignKeyRef()` helper in `internal/parser/parser.go` (parses table name, column list, ON DELETE/UPDATE actions; rejects CASCADE/SET NULL/SET DEFAULT)
- [ ] 1.5 Add `REFERENCES` case to `parseColumnDef` constraint loop in `internal/parser/parser.go`
- [ ] 1.6 Add `FOREIGN` case to `parseTableConstraint` in `internal/parser/parser.go`
- [ ] 1.7 Add `p.isKeyword("FOREIGN")` dispatch in `parseCreateTable` at line ~2053 to route to `parseTableConstraint`
- [ ] 1.8 Convert column-level ForeignKeyRef to TableConstraint entries in `parseCreateTable` (alongside existing UNIQUE/CHECK conversion at lines 2078-2091)
- [ ] 1.9 Write parser tests for all FK parsing scenarios (column-level, table-level, composite, named, rejected actions, default actions)

## 2. Catalog: FK Constraint Storage

- [ ] 2.1 Add `ForeignKeyConstraintDef` struct with `Clone()` to `internal/catalog/constraint.go`
- [ ] 2.2 Add `ForeignKeyAction` type and constants (NoAction, Restrict) to `internal/catalog/constraint.go`
- [ ] 2.3 Update `TableDef.Clone()` in `internal/catalog/table.go` to handle `*ForeignKeyConstraintDef` in the constraint switch
- [ ] 2.4 Write catalog tests verifying FK constraint storage and cloning

## 3. Executor: CREATE TABLE FK Validation

- [ ] 3.1 In `executeCreateTable` (`internal/executor/operator.go` ~line 2573), add validation that referenced parent table exists
- [ ] 3.2 Add validation that referenced columns exist in the parent table
- [ ] 3.3 Add validation that referenced columns form a PK or UNIQUE constraint on the parent
- [ ] 3.4 Write tests for CREATE TABLE FK validation (missing table, missing column, non-key reference, valid FK)

## 4. Executor: INSERT FK Enforcement

- [ ] 4.1 Create `internal/executor/fk_check.go` with `checkParentKeyExists` helper that scans parent table for matching key values
- [ ] 4.2 Add `checkForeignKeys` function call in `executeInsert` (after PK check, before row append)
- [ ] 4.3 Handle NULL FK values (skip validation when all FK columns are NULL)
- [ ] 4.4 Write tests for INSERT FK enforcement (violation, NULL allowed, valid reference, composite FK)

## 5. Executor: DELETE FK Enforcement

- [ ] 5.1 Add `findChildForeignKeys` helper to `internal/executor/fk_check.go` that finds all FK constraints referencing a given table
- [ ] 5.2 Add `checkNoChildReferences` helper that scans child tables for rows referencing given key values
- [ ] 5.3 Add FK check in `executeDelete` before marking rows as deleted
- [ ] 5.4 Write tests for DELETE FK enforcement (blocked by reference, allowed when no references, NULL child FK does not block)

## 6. Executor: UPDATE FK Enforcement

- [ ] 6.1 Add child-side FK check in `executeUpdate` when FK columns are being modified
- [ ] 6.2 Add parent-side FK check in `executeUpdate` when PK/unique columns are being modified
- [ ] 6.3 Write tests for UPDATE FK enforcement (child update to non-existent parent, valid child update, parent update blocked, parent update allowed)

## 7. Self-Referencing FK

- [ ] 7.1 Ensure self-referencing FK works for INSERT (NULL manager, valid manager, invalid manager)
- [ ] 7.2 Write tests for self-referencing FK scenarios

## 8. Integration and Lint

- [ ] 8.1 Run `nix develop -c lint` and fix any linting issues
- [ ] 8.2 Run `nix develop -c tests` and ensure all existing tests still pass
- [ ] 8.3 Verify FK error messages use `dukdb.Error{Type: dukdb.ErrorTypeConstraint, Msg: "..."}` format
