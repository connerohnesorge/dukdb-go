## 1. Catalog: Constraint Definitions

- [ ] 1.1 Create `internal/catalog/constraint.go` with ConstraintType, UniqueConstraintDef, CheckConstraintDef, ForeignKeyConstraintDef, ForeignKeyAction types
- [ ] 1.2 Add `Constraints []any` field to TableDef
- [ ] 1.3 Add constraint serialization/deserialization for catalog persistence
- [ ] 1.4 Add unit tests for constraint definition creation and cloning

## 2. Parser: Constraint Syntax

- [ ] 2.1 Add `TableConstraint` struct to `internal/parser/ast.go`
- [ ] 2.2 Add `Constraints []TableConstraint` to CreateTableStmt
- [ ] 2.3 Parse column-level UNIQUE constraint
- [ ] 2.4 Parse column-level CHECK (expr) constraint
- [ ] 2.5 Parse column-level REFERENCES table(col) constraint
- [ ] 2.6 Parse table-level UNIQUE (col_list) constraint
- [ ] 2.7 Parse table-level CHECK (expr) constraint
- [ ] 2.8 Parse table-level FOREIGN KEY (col_list) REFERENCES table(col_list) with ON DELETE/UPDATE NO ACTION|RESTRICT
- [ ] 2.8.1 Reject CASCADE, SET NULL, SET DEFAULT with error matching DuckDB behavior
- [ ] 2.9 Parse optional CONSTRAINT name prefix
- [ ] 2.10 Extend ColumnDefClause with Unique, Check, References fields for column-level constraints
- [ ] 2.11 Normalize column-level constraints to table-level during parsing
- [ ] 2.11 Add parser unit tests for all constraint syntax variants

## 3. Binder: Constraint Validation

- [ ] 3.1 Validate UNIQUE constraint columns exist in table
- [ ] 3.2 Validate CHECK expression is a valid boolean expression with table columns only
- [ ] 3.3 Validate FK referenced table exists and referenced columns form PK or UNIQUE
- [ ] 3.4 Validate FK column types match referenced column types
- [ ] 3.5 Store resolved constraints in catalog TableDef during CREATE TABLE

## 4. Executor: UNIQUE Enforcement

- [ ] 4.1 Generalize existing `checkPrimaryKey` to `checkUniqueConstraint` supporting multiple unique constraints
- [ ] 4.2 Build hash sets for all UNIQUE constraints at start of INSERT/UPDATE
- [ ] 4.3 Check UNIQUE constraints on each inserted/updated row
- [ ] 4.4 Handle NULL values (not considered duplicates per SQL standard)

## 5. Executor: CHECK Enforcement

- [ ] 5.1 Create `internal/executor/constraint_check.go`
- [ ] 5.2 Parse and bind CHECK expression at enforcement time
- [ ] 5.3 Evaluate CHECK expression for each row on INSERT/UPDATE
- [ ] 5.4 Handle NULL result (passes CHECK per SQL standard)
- [ ] 5.5 Return clear constraint violation error with constraint name

## 6. Executor: FOREIGN KEY Enforcement

- [ ] 6.1 Implement FK validation on INSERT into child table (check parent row exists)
- [ ] 6.2 Implement FK validation on UPDATE of child table FK columns
- [ ] 6.3 Implement ON DELETE NO ACTION/RESTRICT (error if child rows exist)
- [ ] 6.4 Implement ON UPDATE NO ACTION/RESTRICT (error if child rows reference updated PK)
- [ ] 6.5 Handle NULL FK values (skip validation)
- [ ] 6.6 Handle self-referencing FK tables

## 7. Integration Tests

- [ ] 7.1 Test: UNIQUE violation on INSERT
- [ ] 7.2 Test: UNIQUE allows NULL duplicates
- [ ] 7.3 Test: Composite UNIQUE constraint
- [ ] 7.4 Test: UNIQUE enforced on UPDATE
- [ ] 7.5 Test: CHECK violation on INSERT
- [ ] 7.6 Test: CHECK passes with NULL
- [ ] 7.7 Test: CHECK with multi-column expression
- [ ] 7.8 Test: FK violation on INSERT (referenced row missing)
- [ ] 7.9 Test: FK allows NULL reference
- [ ] 7.10 Test: FK ON DELETE RESTRICT prevents deletion
- [ ] 7.11 Test: FK rejects CASCADE/SET NULL/SET DEFAULT at parse time
- [ ] 7.12 Test: FK validation on CREATE TABLE (invalid reference)
- [ ] 7.13 Test: Named constraints with CONSTRAINT keyword
- [ ] 7.14 Test: Self-referencing FK (e.g., employee.manager_id REFERENCES employee.id)
