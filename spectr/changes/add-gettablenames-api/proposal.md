# Change: Add GetTableNames() Public API for Query Table Introspection

## Why

dukdb-go currently lacks the `GetTableNames()` public API that exists in duckdb-go v1.4.3 (connection.go:292). This function extracts table names from SQL queries without execution, enabling query analysis, validation, and dependency tracking workflows. Without this API, users cannot inspect query table dependencies before execution, blocking use cases like query routing, access control validation, and data lineage tracking.

## What

Implement `GetTableNames(conn *sql.Conn, query string, qualified bool) ([]string, error)` public API that:

1. **Parses SQL query** using existing parser infrastructure (no execution)
2. **Traverses AST** to extract table references from FROM/JOIN clauses and subqueries
3. **Returns table names** as deduplicated, sorted string slice
4. **Supports qualified names**: Returns `schema.table` when `qualified=true`, otherwise just `table` (catalog support requires AST enhancement)
5. **Handles core DML**: SELECT, INSERT, UPDATE, DELETE

**Architecture Note**: This is a pure Go AST traversal implementation, not a C binding wrapper like the reference implementation. Behavior matches the reference API but implementation approach differs fundamentally.

## Impact

### Users Affected
- **Query Routing Systems**: Can now inspect tables before execution for shard routing decisions
- **Access Control Systems**: Can validate table-level permissions before allowing queries
- **Data Lineage Tools**: Can build dependency graphs by extracting table references
- **Multi-Tenant Applications**: Can validate queries don't cross tenant boundaries

### Breaking Changes
None - this is additive functionality only.

### Performance Impact
- **Parse-time only**: No storage access, O(AST nodes) complexity
- **Target latency**: <1ms for typical queries (<1000 lines) after parser optimization (parser benchmarks TBD, on modern CPU ~3GHz+)
- **Memory**: O(table references), typically <1KB (~90-140 bytes per TableRef)

### Dependencies
- Existing parser (internal/parser/) - already handles core SQL syntax
- **AST Enhancements Required**:
  - Add `Catalog` field to `TableRef` struct for full qualified name support
  - Add visitor pattern infrastructure (Accept methods on AST nodes)
- Catalog (internal/catalog/) - optional, for qualified name resolution (deferred to Phase 2)

## Alternatives Considered

### Alternative 1: Expose full AST instead of just table names
- **Pro**: More flexible for power users
- **Con**: Breaks API compatibility with duckdb-go v1.4.3; requires users to traverse AST
- **Rejected**: Must match reference API for drop-in replacement

### Alternative 2: Execute EXPLAIN and parse output
- **Pro**: Simpler implementation using existing infrastructure
- **Con**: Slower (requires executor), less accurate (EXPLAIN may optimize away tables)
- **Rejected**: Performance and accuracy requirements not met

### Alternative 3: Require qualified=true always
- **Pro**: Simpler implementation (no name resolution ambiguity)
- **Con**: Breaks API compatibility; forces performance overhead on all users
- **Rejected**: Must match reference API signature

## Success Criteria

- [ ] GetTableNames() function added to public API matching signature
- [ ] Handles SELECT, INSERT, UPDATE, DELETE (core DML)
- [ ] Handles JOINs (INNER, LEFT, RIGHT, FULL, CROSS), subqueries, nested queries correctly
- [ ] Returns deduplicated, sorted table names (case-sensitive deduplication)
- [ ] Qualified vs unqualified modes work correctly (schema.table support, catalog.schema.table in Phase 2)
- [ ] All unit tests pass (30+ test cases covering all query patterns and JOIN types)
- [ ] Compatibility tests pass against reference duckdb-go v1.4.3 for supported features
- [ ] Performance <1ms for queries <1000 lines (parse-time only, on modern CPU)
- [ ] Error types specified: `*ParserError` for syntax errors with descriptive messages
- [ ] No regression in existing test suite

## Rollout Plan

### Phase 1: AST Enhancement & Core Implementation (Week 1)
- Add `Catalog` field to `TableRef` struct in AST
- Implement visitor pattern infrastructure (Accept methods on AST nodes)
- Implement TableExtractor AST visitor
- Add GetTableNames() public function
- Unit tests for basic SELECT/INSERT/UPDATE/DELETE with all JOIN types
- Target: 15+ unit tests passing

### Phase 2: Advanced Features & Full Qualified Names (Week 2)
- Handle subqueries in WHERE, HAVING, SELECT list, JOIN ON clauses
- Implement full qualified name resolution (catalog.schema.table)
- Add comprehensive test suite (30+ query patterns)
- Target: All supported query types working

### Phase 3: Compatibility & Polish (Week 3)
- Compatibility tests against reference implementation for supported features
- Performance benchmarks (<1ms target on modern CPU)
- Documentation with usage examples (create EXAMPLES.md)
- Target: API compatibility for core features

## Out of Scope

- Table existence validation (caller's responsibility to check catalog)
- View expansion (returns view name, not underlying tables)
- Table function introspection (e.g., read_csv(), read_parquet(), unnest(), json_each() - all excluded from extraction)
- Permissions checking (returns all table references regardless of permissions)
- Query optimization or execution planning hints
- **Common Table Expressions (CTEs)**: WITH clause support requires AST enhancement (no CTEs field in SelectStmt currently)
- **Set Operations**: UNION, INTERSECT, EXCEPT support requires AST enhancement (no SetOperation types currently)
- **CREATE TABLE AS SELECT**: Requires AST enhancement (current CreateTableStmt has no SELECT field)
- **UPDATE...FROM**: Requires AST enhancement (UpdateStmt has no From field currently)
- **Multiple statements**: Queries with semicolon-separated statements (only first statement processed)
- **LATERAL subqueries**: Advanced feature, deferred to future enhancement
- **Recursive queries**: Require CTE support (deferred)
