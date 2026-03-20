## 1. SIMILAR TO Parser Support

- [ ] 1.1 Add OpSimilarTo and OpNotSimilarTo to BinaryOp constants in ast.go
- [ ] 1.2 Add SimilarToExpr AST node for ESCAPE clause support in ast.go
- [ ] 1.3 Parse SIMILAR TO expression in parser.go (after ILIKE block)
- [ ] 1.4 Parse NOT SIMILAR TO expression in parser.go (requires multi-token lookahead: check peek()=SIMILAR after NOT, then advance twice and expect TO)
- [ ] 1.5 Parse ESCAPE clause for SIMILAR TO
- [ ] 1.6 Add String() representation for OpSimilarTo and OpNotSimilarTo
- [ ] 1.7 Write parser tests for SIMILAR TO, NOT SIMILAR TO, and ESCAPE

## 2. SIMILAR TO Executor Support

- [ ] 2.1 Implement sqlRegexToGoRegex conversion function in executor (must convert SQL [!abc] negation to Go [^abc])
- [ ] 2.2 Handle SIMILAR TO evaluation in expr.go (OpSimilarTo, OpNotSimilarTo)
- [ ] 2.3 Handle SimilarToExpr evaluation with ESCAPE in expr.go
- [ ] 2.4 Write unit tests for sqlRegexToGoRegex edge cases (%, _, |, [], (), escape)
- [ ] 2.5 Write integration tests: SIMILAR TO in SELECT WHERE clause
- [ ] 2.6 Write integration tests: NOT SIMILAR TO, alternation, character classes

## 3. CREATE TYPE AS ENUM DDL

- [ ] 3.1 Add CreateTypeStmt and DropTypeStmt AST nodes to ast.go
- [ ] 3.2 Parse CREATE TYPE name AS ENUM (...) in parser.go
- [ ] 3.3 Parse DROP TYPE [IF EXISTS] name in parser.go
- [ ] 3.4 Parse CREATE TYPE IF NOT EXISTS variant
- [ ] 3.5 Write parser tests for CREATE TYPE and DROP TYPE

## 4. Catalog Storage for User-Defined Types

- [ ] 4.1 Add TypeEntry struct to catalog
- [ ] 4.2 Add types map to Schema struct (not Catalog) and add CreateType/DropType/GetType methods to Schema, with Catalog convenience methods that delegate to the appropriate schema
- [ ] 4.3 Implement dependency checking for DROP TYPE (prevent dropping type in use)
- [ ] 4.4 Write catalog tests for type creation, lookup, and deletion

## 5. Binder and Executor Integration

- [ ] 5.0 Add case statements for CreateTypeStmt and DropTypeStmt in binder.go Bind() switch
- [ ] 5.1 Resolve user-defined type references in CREATE TABLE column definitions (parser stores raw type name for unknowns; binder resolves against catalog)
- [ ] 5.2 Execute CREATE TYPE statement in DDL executor
- [ ] 5.3 Execute DROP TYPE statement in DDL executor
- [ ] 5.4 Validate enum values on INSERT into enum-typed columns
- [ ] 5.5 Write integration tests: CREATE TYPE, CREATE TABLE with enum column, INSERT, SELECT
- [ ] 5.6 Write integration tests: DROP TYPE, error on drop type in use

## 6. Comprehensive Testing

- [ ] 6.1 End-to-end test: full enum workflow (CREATE TYPE, CREATE TABLE, INSERT, SELECT, DROP)
- [ ] 6.2 Test SIMILAR TO with various SQL regex patterns matching DuckDB behavior
- [ ] 6.3 Test error cases: invalid patterns, unknown types, duplicate type names
- [ ] 6.4 Verify linting passes (nix develop -c lint)
- [ ] 6.5 Verify all tests pass (nix develop -c tests)
