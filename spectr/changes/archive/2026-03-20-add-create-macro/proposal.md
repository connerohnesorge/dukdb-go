# Change: Add CREATE MACRO / TABLE MACRO Support

## Why

DuckDB v1.4.3 supports user-defined scalar macros and table macros via `CREATE MACRO`, enabling SQL-level function abstraction. dukdb-go currently has no macro support in the parser, catalog, binder, or executor, which limits SQL compatibility and prevents users from defining reusable SQL expressions and parameterized table-returning functions.

## What Changes

- Add `MacroDef` catalog entry for storing scalar and table macro definitions
- Add `CreateMacroStmt` and `DropMacroStmt` AST nodes to the parser
- Parse `CREATE MACRO`, `CREATE OR REPLACE MACRO`, and `DROP MACRO` syntax
- Support scalar macros (expression-based) and table macros (`AS TABLE (SELECT ...)`)
- Support macro parameters with optional default values
- Expand macro calls inline during binding (similar to view expansion)
- Register scalar macros as callable functions in the executor
- Register table macros as table function sources in the executor

BREAKING: No breaking changes to public API. Changes add new DDL capabilities.

## Impact

- **Affected specs**:
  - `sql-macros` (new spec for macro DDL and invocation)

- **Affected code**:
  - `internal/catalog/` - Add `MacroDef` struct, macro storage and lookup methods
  - `internal/parser/` - Add AST nodes, parse CREATE/DROP MACRO statements
  - `internal/parser/parser_ddl.go` - Macro DDL parsing logic
  - `internal/binder/` - Inline macro expansion during binding (substitute parameters)
  - `internal/executor/` - Execute CREATE MACRO and DROP MACRO DDL
  - `internal/executor/ddl.go` - DDL execution for macro statements

- **New catalog objects**:
  - `MacroDef` (scalar macro definition with expression body)
  - `MacroDef` with `IsTableMacro` flag (table macro definition with query body)

- **Dependencies**:
  - None on external packages
  - Builds on existing catalog, parser, and binder infrastructure
