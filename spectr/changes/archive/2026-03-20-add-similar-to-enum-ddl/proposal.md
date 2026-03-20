# Change: Add SIMILAR TO Operator and CREATE TYPE AS ENUM DDL

## Why

dukdb-go is missing two SQL standard features that DuckDB supports: the SIMILAR TO pattern matching operator and CREATE TYPE AS ENUM DDL. SIMILAR TO is part of the SQL standard and provides regex-based pattern matching using SQL syntax (%, _, |, brackets). While the ENUM type already exists in the type system (`internal/types/type_system.go`), there is no DDL path to create named enum types via CREATE TYPE name AS ENUM (...) and no way to reference user-defined types in table definitions. Both features are needed for DuckDB compatibility and SQL standard compliance.

## What Changes

- Add SIMILAR TO and NOT SIMILAR TO expression parsing to the SQL parser
- Add OpSimilarTo and OpNotSimilarTo binary operators to the AST
- Implement SQL regex to Go regexp conversion for SIMILAR TO evaluation in the executor
- Add CREATE TYPE name AS ENUM (...) DDL statement parsing
- Add DROP TYPE name DDL statement parsing
- Add CreateTypeStmt and DropTypeStmt AST nodes
- Add user-defined type catalog storage for named enum types
- Implement binder resolution for user-defined enum type references in column definitions
- Add ESCAPE clause support for SIMILAR TO patterns
- Write comprehensive tests for both features

## Impact

- **Affected specs**:
  - `sql-operators` (add SIMILAR TO operator and ENUM DDL)

- **Affected code**:
  - `internal/parser/ast.go` - Add OpSimilarTo, OpNotSimilarTo, CreateTypeStmt, DropTypeStmt
  - `internal/parser/parser.go` - Parse SIMILAR TO, NOT SIMILAR TO, CREATE TYPE, DROP TYPE
  - `internal/executor/expr.go` - Evaluate SIMILAR TO using Go regexp
  - `internal/catalog/catalog.go` - Store and retrieve user-defined types
  - `internal/binder/` - Resolve user-defined type references
  - `internal/executor/ddl.go` - Execute CREATE TYPE and DROP TYPE

- **Dependencies**:
  - Go standard library `regexp` package (already available, no new dependencies)
