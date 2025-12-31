# Implementation Tasks: Add GetTableNames() Public API

## Phase 1: Foundation - AST Enhancement & Visitor Infrastructure

### Parser & AST Review (COMPLETED)
- [x] Read internal/parser/ast.go to understand current AST structure
- [x] Verify SelectStmt has From, Joins, Where fields accessible (YES - CTEs NOT available)
- [x] Verify InsertStmt has Schema, Table, and Select fields accessible (YES)
- [x] Verify UpdateStmt has Schema, Table fields accessible (YES - From NOT available)
- [x] Verify DeleteStmt has Schema, Table field accessible (YES)
- [x] Document missing AST fields:
  - SelectStmt: NO CTEs field (CTE support blocked)
  - TableRef: NO Catalog field (full qualified names blocked)
  - UpdateStmt: NO From field (UPDATE...FROM blocked)
  - No SetOperation types (UNION/INTERSECT/EXCEPT blocked)
  - CreateTableStmt: NO Select field (CREATE TABLE AS SELECT blocked)

### AST Enhancements (Required for Phase 1)
- [ ] Add Catalog field to TableRef struct in internal/parser/ast.go
- [ ] Update TableRef documentation to explain Catalog usage
- [ ] Test: Verify parser can populate Catalog field correctly

### Visitor Pattern Implementation
- [ ] Add Visitor interface to internal/parser/visitor.go (new file)
- [ ] Add Accept(Visitor) method to SelectStmt
- [ ] Add Accept(Visitor) method to InsertStmt
- [ ] Add Accept(Visitor) method to UpdateStmt
- [ ] Add Accept(Visitor) method to DeleteStmt
- [ ] Add Accept(Visitor) method to CreateTableAsSelectStmt (if exists)
- [ ] Test: Create dummy visitor that counts statements, verify it works

### TableRef Structure
- [ ] Create internal/parser/table_ref.go (new file)
- [ ] Define TableRef struct with Catalog, Schema, Table, Alias fields
- [ ] Implement QualifiedName() method returning catalog.schema.table format
- [ ] Implement String() method for debugging
- [ ] Test: Verify QualifiedName() handles partial qualifications correctly

## Phase 2: Core Implementation - TableExtractor

### TableExtractor Basic Structure
- [ ] Create internal/parser/table_extractor.go (new file)
- [ ] Define TableExtractor struct with tables map and qualified bool
- [ ] Implement NewTableExtractor(qualified bool) constructor
- [ ] Implement GetTables() method returning sorted []string
- [ ] Test: Verify empty extractor returns empty slice

### SELECT Statement Handling
- [ ] Implement VisitSelectStmt() to extract FROM clause tables
- [ ] Implement visitTableSource() helper for TableName vs Subquery vs Join
- [ ] Handle JOIN clauses (INNER, LEFT, RIGHT, FULL, CROSS)
- [ ] Handle subqueries in FROM clause recursively
- [ ] Handle WHERE clause subqueries (if parser exposes them)
- [ ] Test: SELECT * FROM users → ["users"]
- [ ] Test: SELECT * FROM users u JOIN orders o → ["orders", "users"] (sorted)
- [ ] Test: SELECT * FROM (SELECT * FROM products) AS p → ["products"]

### CTE (Common Table Expression) Handling - DEFERRED to Phase 2
- [ ] NOTE: CTEs NOT supported - requires AST enhancement (SelectStmt has no CTEs field)
- [ ] FUTURE: Add cteNames map[string]struct{} to TableExtractor once AST supports CTEs
- [ ] FUTURE: Implement CTE name tracking (mark as temporary, don't include in output)
- [ ] FUTURE: Visit CTE source queries recursively
- [ ] FUTURE: Test WITH tmp AS (SELECT * FROM users) SELECT * FROM tmp → ["users"]

### INSERT Statement Handling
- [ ] Implement VisitInsertStmt() to extract target table
- [ ] Handle INSERT...SELECT to extract source tables
- [ ] Test: INSERT INTO archive VALUES (...) → ["archive"]
- [ ] Test: INSERT INTO archive SELECT * FROM users → ["archive", "users"]

### UPDATE Statement Handling
- [ ] Implement VisitUpdateStmt() to extract target table (Schema, Table fields)
- [ ] Handle WHERE clause subqueries
- [ ] Handle SET clause subqueries (rare but possible)
- [ ] Test: UPDATE users SET x = 1 → ["users"]
- [ ] Test: UPDATE users SET x = (SELECT MAX(y) FROM stats) WHERE id = 1 → ["stats", "users"]
- [ ] NOTE: UPDATE...FROM NOT supported - requires AST enhancement (UpdateStmt has no From field)

### DELETE Statement Handling
- [ ] Implement VisitDeleteStmt() to extract target table
- [ ] Handle WHERE clause subqueries
- [ ] Test: DELETE FROM users → ["users"]
- [ ] Test: DELETE FROM users WHERE id IN (SELECT user_id FROM deleted) → ["deleted", "users"]

### Deduplication & Sorting (CRITICAL FIX for Issue #7)
- [ ] Verify map[TableRef]struct{} deduplicates TableRef objects correctly
- [ ] IMPLEMENT string deduplication AFTER converting to string slice (CRITICAL):
  ```go
  seen := make(map[string]bool)
  dedupedNames := []string{}
  for _, name := range tableNames {
      if !seen[name] {
          seen[name] = true
          dedupedNames = append(dedupedNames, name)
      }
  }
  ```
- [ ] Implement sort.Strings() on final output
- [ ] Test: SELECT * FROM users u1 JOIN users u2 → ["users"] (deduplicated)
- [ ] Test: SELECT * FROM schema1.users JOIN schema2.users (qualified=false) → ["users"] (deduplicated)
- [ ] Test: Output is always sorted alphabetically
- [ ] Test: Case-sensitive deduplication works correctly

## Phase 3: Public API - GetTableNames() Function

### Public Function Implementation
- [ ] Create gettablenames.go in root package (new file)
- [ ] Implement GetTableNames(conn *sql.Conn, query string, qualified bool) ([]string, error)
- [ ] Handle empty query → return []string{}, nil
- [ ] Handle parse errors → return nil, fmt.Errorf("parse error: %w", err)
- [ ] Use TableExtractor to traverse AST
- [ ] Return sorted, deduplicated table names
- [ ] Test: Basic integration test calling public API

### Qualified Name Handling
- [ ] Implement qualified vs unqualified mode switching
- [ ] If qualified=false, return only TableRef.TableName (unqualified)
- [ ] If qualified=true, return TableRef.QualifiedName() (schema.table or catalog.schema.table)
- [ ] Test: SELECT * FROM public.users with qualified=true → ["public.users"]
- [ ] Test: SELECT * FROM public.users with qualified=false → ["users"]
- [ ] Test: Partially qualified names (schema.table) work correctly
- [ ] PHASE 2: Test SELECT * FROM main.public.users with qualified=true → ["main.public.users"] (once Catalog field added)

### Error Handling
- [ ] Handle parser errors gracefully (return nil, *ParserError)
- [ ] Handle empty/whitespace-only queries (return []string{}, nil)
- [ ] Handle queries with no tables (SELECT 1+1, SELECT NULL) (return []string{}, nil)
- [ ] Test: Invalid syntax returns *ParserError with descriptive message
- [ ] Test: Error messages include "parse error" context
- [ ] Test: Error messages are user-friendly and descriptive

## Phase 4: Edge Cases & Advanced Features

### Edge Case: Table Functions
- [ ] Verify table functions (read_csv, read_parquet, unnest, json_each) are NOT extracted
- [ ] Implementation: Only extract TableRef nodes where Subquery is nil and TableName is non-empty
- [ ] Test: SELECT * FROM read_csv('file.csv') → [] (empty, not an error)
- [ ] Test: SELECT * FROM read_parquet('file.parquet') → []
- [ ] Test: SELECT * FROM users JOIN read_csv('file.csv') → ["users"] (mixed case)

### Edge Case: Views
- [ ] Verify views are extracted as table names (view name, not underlying tables)
- [ ] Test: SELECT * FROM user_view → ["user_view"] (view expansion out of scope)

### Edge Case: Nested Subqueries
- [ ] Test: 3-level nested subqueries extract innermost table correctly
- [ ] Test: Multiple subqueries at same level are all extracted

### Edge Case: Complex CTEs - DEFERRED to Phase 2
- [ ] NOTE: CTEs NOT supported - requires AST enhancement
- [ ] FUTURE: Test multiple CTEs in same query
- [ ] FUTURE: Test CTEs referencing other CTEs
- [ ] FUTURE: Test recursive CTEs

### Edge Case: UNION/INTERSECT/EXCEPT - DEFERRED to Phase 2
- [ ] NOTE: Set operations NOT supported - requires AST enhancement (no SetOperation types)
- [ ] FUTURE: Handle set operations extracting tables from all branches
- [ ] FUTURE: Test SELECT * FROM users UNION SELECT * FROM admins → ["admins", "users"]
- [ ] FUTURE: Test INTERSECT and EXCEPT operations

## Phase 5: Testing & Validation

### Unit Test Suite (30+ tests required)
- [ ] Create gettablenames_test.go in root package
- [ ] Add TestGetTableNames_SimpleSelect
- [ ] Add TestGetTableNames_InnerJoin
- [ ] Add TestGetTableNames_LeftJoin
- [ ] Add TestGetTableNames_RightJoin
- [ ] Add TestGetTableNames_FullJoin
- [ ] Add TestGetTableNames_CrossJoin
- [ ] Add TestGetTableNames_MultipleJoins
- [ ] Add TestGetTableNames_SubqueryInFrom
- [ ] Add TestGetTableNames_SubqueryInWhere
- [ ] Add TestGetTableNames_SubqueryInHaving
- [ ] Add TestGetTableNames_SubqueryInSelect
- [ ] Add TestGetTableNames_SubqueryInJoinOn
- [ ] Add TestGetTableNames_Insert
- [ ] Add TestGetTableNames_InsertSelect
- [ ] Add TestGetTableNames_Update
- [ ] Add TestGetTableNames_UpdateWithSubquery
- [ ] Add TestGetTableNames_Delete
- [ ] Add TestGetTableNames_DeleteWithSubquery
- [ ] Add TestGetTableNames_CreateTable (plain)
- [ ] Add TestGetTableNames_InvalidSyntax (error case)
- [ ] Add TestGetTableNames_NoTables (SELECT 1)
- [ ] Add TestGetTableNames_SelectNull
- [ ] Add TestGetTableNames_EmptyQuery
- [ ] Add TestGetTableNames_Unqualified
- [ ] Add TestGetTableNames_SchemaQualified
- [ ] Add TestGetTableNames_PartialQualification
- [ ] Add TestGetTableNames_SelfJoin (deduplication)
- [ ] Add TestGetTableNames_CaseSensitivity
- [ ] Add TestGetTableNames_Sorting (deterministic output)
- [ ] Add TestGetTableNames_Deterministic (100 runs)
- [ ] Add TestGetTableNames_TableFunctions (edge case)
- [ ] Add TestGetTableNames_Views (edge case)
- [ ] Add TestGetTableNames_Aliases (not in output)
- [ ] Add TestGetTableNames_Parameterized (parameter markers)
- [ ] Add TestGetTableNames_Comments
- [ ] Add TestGetTableNames_QuotedIdentifiers
- [ ] Target: 30+ unit tests passing

### Compatibility Testing (for supported features only)
- [ ] Create compatibility/gettablenames_test.go
- [ ] Implement comparison test against reference duckdb-go v1.4.3
- [ ] Add 30+ query patterns for SUPPORTED features (SELECT, INSERT, UPDATE, DELETE with subqueries/joins)
- [ ] Skip unsupported features (CTEs, UNION, CREATE TABLE AS, UPDATE...FROM) with documentation
- [ ] Verify match with reference implementation for supported queries
- [ ] Test: Complex JOIN patterns (all types)
- [ ] Test: Nested subqueries up to 5 levels deep
- [ ] Test: Subqueries in WHERE, HAVING, SELECT, JOIN ON clauses
- [ ] NOTE: 100% compatibility only expected for supported feature subset

### Performance Benchmarking
- [ ] FIRST: Establish parser baseline performance (measure parser alone)
- [ ] Create benchmark tests (Benchmark_GetTableNames_*)
- [ ] Benchmark simple SELECT (<100 nodes, <10 tables): Target <500µs
- [ ] Benchmark complex JOIN (10 tables, 100-500 nodes): Target <1ms
- [ ] Benchmark nested subqueries (5 levels): Target <1ms
- [ ] Benchmark deep nesting (10 levels): Target <1ms
- [ ] Benchmark large query (1000 nodes, 100 tables): Target <2ms
- [ ] Verify memory usage <2KB for small queries, <15KB for large queries
- [ ] NOTE: All targets assume modern CPU (~3GHz+), after parser optimization

### Deterministic Testing
- [ ] Verify GetTableNames is deterministic (same query → same output)
- [ ] Test: Run same query 100 times, verify identical output
- [ ] Test: Order of tables always alphabetical
- [ ] No need for quartz clock injection (parse-time only, no async operations)

## Phase 6: Documentation & Polish

### Code Documentation
- [ ] Add godoc comments to GetTableNames() function (include supported/unsupported features)
- [ ] Add godoc comments to TableExtractor
- [ ] Add godoc comments to TableRef
- [ ] Add usage examples in godoc
- [ ] Verify godoc examples are runnable (Example_GetTableNames)

### README/Guide Updates
- [ ] Add GetTableNames() to API reference (if exists)
- [ ] Add usage examples to documentation
- [ ] Add common pitfalls section (CTEs NOT supported, table functions excluded, views not expanded)
- [ ] Document qualified vs unqualified mode (schema.table in Phase 1, catalog.schema.table in Phase 2)
- [ ] Document unsupported features clearly (CTEs, UNION, CREATE TABLE AS, UPDATE...FROM)

### EXAMPLES.md Creation
- [ ] Create EXAMPLES.md with usage patterns:
  - Query routing based on table names
  - Access control validation
  - Multi-tenant query filtering
  - Data lineage tracking
- [ ] Include code examples for each pattern

### Error Message Polish
- [ ] Verify parse errors are user-friendly
- [ ] Add context to error messages (query snippet if helpful)
- [ ] Ensure no internal panic paths (all errors caught)

## Phase 7: Final Validation

### Regression Testing
- [ ] Run full test suite: `nix develop -c gotestsum --format short-verbose ./...`
- [ ] Verify no regressions in existing tests
- [ ] Verify all new tests pass
- [ ] Run linter: `nix develop -c golangci-lint run`
- [ ] Fix any linting issues

### Compatibility Verification
- [ ] Run compatibility tests against reference duckdb-go v1.4.3
- [ ] Verify 100% API signature match (func signature identical)
- [ ] Verify behavior match for SUPPORTED features (SELECT, INSERT, UPDATE, DELETE with subqueries/joins)
- [ ] Document unsupported features (CTEs, UNION, CREATE TABLE AS, UPDATE...FROM) with AST limitations explained
- [ ] Document architectural difference (pure Go AST vs C bindings)

### Performance Validation
- [ ] Run benchmarks on real hardware (modern CPU ~3GHz+)
- [ ] Verify performance targets met:
  - Small queries (<100 nodes): <500µs
  - Medium queries (100-500 nodes): <1ms
  - Large queries (500-1000 nodes): <2ms
  - Deep nesting (10 levels): <1ms
- [ ] Verify memory usage targets met:
  - Small queries: <2KB
  - Large queries (100 tables): <15KB
- [ ] Profile any slow paths (if benchmarks fail)
- [ ] Measure parser baseline for comparison

### Final Checklist
- [ ] All unit tests pass (30+)
- [ ] All compatibility tests pass for supported features (30+)
- [ ] All benchmarks meet targets (with modern CPU)
- [ ] No regressions in existing test suite
- [ ] Code coverage >80% on new code
- [ ] Documentation complete (including EXAMPLES.md)
- [ ] Unsupported features clearly documented
- [ ] golangci-lint passes

## Parallel Work Opportunities

**Can be done in parallel**:
- Phase 1 (AST review) and Phase 5 (test planning)
- Phase 3 (public API) can start once Phase 2 TableExtractor is functional
- Documentation (Phase 6) can start anytime

**Must be sequential**:
- Phase 1 → Phase 2 (need visitor infrastructure before implementing TableExtractor)
- Phase 2 → Phase 3 (need TableExtractor before public API)
- Phase 4 depends on Phase 3 (edge cases need basic functionality working)
- Phase 7 depends on all previous phases (final validation)

## Definition of Done

- [ ] GetTableNames() public API implemented and tested
- [ ] Handles supported SQL statement types (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE)
- [ ] Handles JOINs (all types), subqueries in all locations correctly
- [ ] Qualified vs unqualified modes work (schema.table in Phase 1)
- [ ] 30+ unit tests pass (covering all JOIN types, subquery locations, edge cases)
- [ ] 30+ compatibility tests pass for supported features against reference duckdb-go v1.4.3
- [ ] Performance targets met (varies by query size, see benchmarks)
- [ ] Zero regressions in existing test suite
- [ ] Documentation complete with examples (including EXAMPLES.md)
- [ ] Unsupported features clearly documented (CTEs, UNION, CREATE TABLE AS, UPDATE...FROM)
- [ ] Code review approved
- [ ] CI pipeline green (nix develop -c tests and nix develop -c lint pass)
