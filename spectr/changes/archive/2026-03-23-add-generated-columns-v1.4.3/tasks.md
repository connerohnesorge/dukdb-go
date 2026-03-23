## 1. Parser

- [x] 1.1 Add `GeneratedExpr Expr`, `GeneratedKind GeneratedKind`, and `IsGenerated bool` fields to `ColumnDefClause` in `internal/parser/ast.go`. Add `GeneratedKind` type constants (`GeneratedKindStored`, `GeneratedKindVirtual`).
- [x] 1.2 In `parseColumnDef()` at `internal/parser/parser_ddl.go`, add parsing for `GENERATED ALWAYS AS (expr) [STORED|VIRTUAL]` syntax after existing constraint handling. Also handle shorthand `AS (expr) [STORED|VIRTUAL]` form.
- [x] 1.3 Add parser unit tests: valid GENERATED ALWAYS AS syntax, shorthand AS syntax, STORED keyword, VIRTUAL keyword, default to VIRTUAL when omitted.
- [x] 1.4 Add Visitor pattern support for GeneratedExpr in `internal/parser/visitor.go`.

## 2. Validation

- [x] 2.1 In binder/DDL validation, reject generated columns with DEFAULT values, PRIMARY KEY, or FOREIGN KEY constraints.
- [x] 2.2 Validate generated expressions only reference columns defined before the generated column (no forward references, no self-references).
- [x] 2.3 Validate generated expressions are deterministic — reject volatile functions (RANDOM, UUID, GEN_RANDOM_UUID, CURRENT_TIMESTAMP, NOW, etc.).
- [x] 2.4 Add validation unit tests for all rejection cases.

## 3. Catalog

- [x] 3.1 Add `IsGenerated bool`, `GeneratedExpr string`, `GeneratedKind GeneratedKind` fields to the `ColumnDef` struct in `internal/catalog/column.go`.
- [x] 3.2 Store generated column SQL text during CREATE TABLE execution in `internal/executor/ddl.go`.
- [x] 3.3 Ensure generated columns appear in `information_schema.columns` with `is_generated` = 'ALWAYS' and `generation_expression` populated.

## 4. Executor — INSERT

- [x] 4.1 In `operator.go` (`executeInsert()`), detect tables with generated columns before insert execution.
- [x] 4.2 Reject explicit non-DEFAULT values for generated columns in INSERT statements (error: "cannot insert a non-DEFAULT value into column X").
- [x] 4.3 After binding non-generated values, evaluate generated column expressions using the current row's column values as context.
- [x] 4.4 Cache parsed generated expressions per table to avoid re-parsing on every INSERT.
- [x] 4.5 Add INSERT integration tests: INSERT with generated column omitted, INSERT with DEFAULT for generated column, INSERT with explicit value rejected.

## 5. Executor — UPDATE

- [x] 5.1 In `physical_update.go`, reject direct SET on generated columns (error: "column X is a generated column").
- [x] 5.2 After updating base columns, re-evaluate all generated columns that depend on changed columns.
- [x] 5.3 Add UPDATE integration tests: UPDATE base column triggers generated column recomputation, UPDATE generated column rejected.

## 6. Executor — SELECT

- [x] 6.1 For STORED generated columns, no special handling needed (value already in storage).
- [x] 6.2 Verify SELECT * includes generated columns with correct values.
- [x] 6.3 Add SELECT integration tests: SELECT generated column, SELECT * with generated columns, WHERE on generated column.

## 7. Storage Serialization

- [ ] 7.1 In `internal/storage/duckdb/catalog_serialize.go`, serialize generated column metadata using `PropColumnDefExpression` and `ColumnCategoryGenerated` constants.
- [ ] 7.2 In `internal/storage/duckdb/catalog_deserialize.go`, deserialize generated column metadata and reconstruct `IsGenerated`, `GeneratedExpr`, `GeneratedKind`.
- [ ] 7.3 Add round-trip serialization tests for tables with generated columns.

## 8. ALTER TABLE Interactions

- [ ] 8.1 Allow ALTER TABLE DROP COLUMN on generated columns.
- [ ] 8.2 Reject ALTER TABLE DROP COLUMN on base columns referenced by generated columns (error: "column X is referenced by generated column Y").
- [ ] 8.3 Allow ALTER TABLE ADD COLUMN with GENERATED ALWAYS AS syntax.
- [ ] 8.4 Add ALTER TABLE integration tests for all generated column interactions.

## 9. End-to-End Integration Tests

- [x] 9.1 Test CREATE TABLE with computed full_name from first_name || ' ' || last_name.
- [x] 9.2 Test generated column with arithmetic expression (e.g., total = price * quantity).
- [x] 9.3 Test generated column with function calls (e.g., upper_name = UPPER(name)).
- [x] 9.4 Test CREATE TABLE AS SELECT preserves generated column definitions.
- [x] 9.5 Test interaction with indexes on generated columns.
