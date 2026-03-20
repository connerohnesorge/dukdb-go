## 1. Catalog: MacroDef and Storage

- [ ] 1.1 Define `MacroDef`, `MacroParam`, and `MacroType` structs in `internal/catalog/macro.go`
- [ ] 1.2 Add `macros map[string]*MacroDef` to the schema storage in catalog
- [ ] 1.3 Implement `Catalog.CreateMacro(schema string, def *MacroDef, orReplace bool) error`
- [ ] 1.4 Implement `Catalog.DropMacro(schema, name string, ifExists bool) error`
- [ ] 1.5 Implement `Catalog.GetMacro(schema, name string) (*MacroDef, error)`
- [ ] 1.6 Write unit tests for macro catalog CRUD operations

## 2. Parser: AST Nodes and Parsing

- [ ] 2.1 Add `CreateMacroStmt` and `DropMacroStmt` AST nodes to `internal/parser/ast.go`
- [ ] 2.2 Add Visitor methods for `CreateMacroStmt` and `DropMacroStmt`
- [ ] 2.3 Implement `CREATE MACRO name(params) AS expression` parsing in `parser_ddl.go`
- [ ] 2.4 Implement `CREATE MACRO name(params) AS TABLE select_stmt` parsing
- [ ] 2.5 Implement `CREATE OR REPLACE MACRO` parsing
- [ ] 2.6 Implement parameter parsing with optional defaults (`:=` and `DEFAULT` syntax)
- [ ] 2.7 Implement `DROP MACRO [IF EXISTS] name` parsing
- [ ] 2.8 Implement `DROP MACRO TABLE [IF EXISTS] name` parsing
- [ ] 2.9 Write parser unit tests for all macro DDL syntax forms

## 3. Executor: DDL Execution

- [ ] 3.1 Add `executeCreateMacro` handler in `internal/executor/ddl.go`
- [ ] 3.2 Add `executeDropMacro` handler in `internal/executor/ddl.go`
- [ ] 3.3 Wire up macro DDL dispatch in the executor's main DDL switch
- [ ] 3.4 Write integration tests for CREATE and DROP MACRO via `database/sql`

## 4. Binder: Scalar Macro Expansion

- [ ] 4.1 Add `ParseExpr` helper to parser for parsing standalone expressions
- [ ] 4.2 Implement parameter substitution walker (`substituteParams`) for expression trees
- [ ] 4.3 Implement `expandScalarMacro` in binder: lookup macro, parse body, substitute args
- [ ] 4.4 Hook scalar macro expansion into function call resolution in binder
- [ ] 4.5 Handle default parameter values when fewer arguments are provided
- [ ] 4.6 Add expansion depth limit (max 32) to prevent infinite recursion
- [ ] 4.7 Write unit tests for scalar macro expansion with various expressions

## 5. Binder: Table Macro Expansion

- [ ] 5.1 Implement `expandTableMacro` in binder: lookup macro, parse query, substitute args
- [ ] 5.2 Implement parameter substitution walker for `SelectStmt` trees
- [ ] 5.3 Hook table macro expansion into table function resolution in binder
- [ ] 5.4 Write unit tests for table macro expansion

## 6. Integration Testing

- [ ] 6.1 Test scalar macro creation and invocation end-to-end
- [ ] 6.2 Test table macro creation and invocation end-to-end
- [ ] 6.3 Test macro with default parameters
- [ ] 6.4 Test CREATE OR REPLACE MACRO overwrites existing macro
- [ ] 6.5 Test DROP MACRO and DROP MACRO IF EXISTS
- [ ] 6.6 Test error cases: missing arguments, too many arguments, undefined macro
- [ ] 6.7 Test nested macro calls (macro calling another macro)
- [ ] 6.8 Test macro shadowing built-in functions
- [ ] 6.9 Test macros with complex expressions (CASE, subqueries, aggregates)
