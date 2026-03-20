# Change: Add UNIQUE, CHECK, and FOREIGN KEY Table Constraints

## Why

DuckDB v1.4.3 supports UNIQUE, CHECK, and FOREIGN KEY constraints in CREATE
TABLE. dukdb-go currently only supports PRIMARY KEY constraints. Missing
constraint support means no uniqueness enforcement beyond PK, no declarative
data validation, and no referential integrity — critical for data quality
and compatibility with SQL tools/ORMs that rely on constraints.

## What Changes

- **Parser**: Extend CREATE TABLE to parse UNIQUE, CHECK, FOREIGN KEY
  constraints (both column-level and table-level syntax)
- **Catalog**: Add `ConstraintDef` types to `TableDef` for storing constraint
  metadata
- **Executor**: Enforce UNIQUE via hash index lookup on INSERT/UPDATE, CHECK
  via expression evaluation on INSERT/UPDATE, FOREIGN KEY via referenced
  table lookup on INSERT/UPDATE and cascading actions on DELETE/UPDATE

## Impact

- Affected specs: `parser`, `catalog`, `execution-engine`
- Affected code:
  - `internal/parser/ast.go` — constraint AST nodes in CreateTableStmt
  - `internal/parser/parser.go` — constraint parsing in CREATE TABLE
  - `internal/catalog/column.go` — column-level constraint metadata
  - `internal/catalog/table.go` — table-level constraint storage
  - `internal/catalog/constraint.go` — new file for constraint definitions
  - `internal/executor/operator.go` — enforcement in executeInsert/executeUpdate
  - `internal/executor/constraint_check.go` — new file for constraint enforcement
