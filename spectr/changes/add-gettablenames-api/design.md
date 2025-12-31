# Design: GetTableNames() Public API

## Architecture Overview

**IMPORTANT**: This is a pure Go AST traversal implementation. Unlike the reference duckdb-go implementation which uses C bindings to DuckDB's internal query parser, this implementation directly traverses the Go AST. Behavior matches the reference API but the implementation approach differs fundamentally.

The GetTableNames() API consists of three layers:

```
Public API (root package)
    ↓
AST Traversal (TableExtractor visitor)
    ↓
Catalog Resolution (optional, for qualified names - Phase 2)
```

**Version Compatibility**: Targets duckdb-go v1.4.3 API compatibility for supported features.

###

 Layer 1: Public API

Location: Root package (`/gettablenames.go` - new file)

```go
// GetTableNames extracts table names from SQL query without execution.
// If qualified=true, returns qualified names (schema.table, or catalog.schema.table in Phase 2).
// If qualified=false, returns unqualified table names only.
//
// Does NOT execute the query - parse-time only.
// Does NOT validate table existence - caller's responsibility.
// DOES return deduplicated, sorted table names (case-sensitive deduplication).
//
// Example:
//   tables, err := dukdb.GetTableNames(conn, "SELECT * FROM users u JOIN orders o ON u.id = o.user_id", false)
//   // returns: ["orders", "users"]  (sorted alphabetically)
//
// Supported SQL:
//   - SELECT with FROM/JOIN clauses (INNER, LEFT, RIGHT, FULL, CROSS joins)
//   - INSERT INTO table / INSERT...SELECT
//   - UPDATE table (basic UPDATE, UPDATE...FROM requires AST enhancement)
//   - DELETE FROM table
//   - Subqueries in FROM, WHERE, HAVING, SELECT list, JOIN ON clauses (recursively)
//
// NOT Supported (require AST enhancements):
//   - Common Table Expressions (WITH clauses) - AST has no CTEs field
//   - Set operations (UNION, INTERSECT, EXCEPT) - AST has no SetOperation types
//   - CREATE TABLE AS SELECT - AST has no SELECT field in CreateTableStmt
//   - UPDATE...FROM - AST has no From field in UpdateStmt
//
// Edge cases:
//   - Empty query → []string{}, nil
//   - Query with no tables (SELECT 1) → []string{}, nil
//   - Invalid syntax → nil, *ParserError with descriptive message
//   - Table functions (read_csv(), unnest(), etc.) → NOT extracted
//   - Views → view name returned (not underlying tables)
//   - NULL in SELECT (SELECT NULL) → []string{}, nil (no tables)
//   - Parameterized queries → parameter markers ($1, ?) left as-is in expressions
//   - Comments in queries → handled by parser, no special action needed
//   - Quoted identifiers → returned as parser provides them
//   - Aliases → NOT included in output (only actual table names)
func GetTableNames(conn *sql.Conn, query string, qualified bool) ([]string, error) {
	// Step 1: Parse query into AST
	parser := parser.NewParser(query)
	stmts, err := parser.Parse()
	if err != nil {
		// Return *ParserError with descriptive message
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if len(stmts) == 0 {
		return []string{}, nil  // Empty query returns empty slice (NOT nil)
	}

	// Step 2: Extract table references using visitor pattern
	extractor := &TableExtractor{
		tables:    make(map[TableRef]struct{}),  // Deduplicate with map (pre-allocated for efficiency)
		qualified: qualified,
	}

	// Note: Only process first statement (multi-statement queries not supported)
	for _, stmt := range stmts {
		stmt.Accept(extractor)  // Traverse AST via visitor pattern
	}

	// Step 3: Convert to string slice
	var tableNames []string
	for ref := range extractor.tables {
		if qualified {
			// Return qualified name (schema.table or catalog.schema.table)
			tableNames = append(tableNames, ref.QualifiedName())
		} else {
			// Return just table name (unqualified)
			tableNames = append(tableNames, ref.Table)
		}
	}

	// Step 3.5: Deduplicate unqualified names (CRITICAL FIX for Issue #7)
	// Multiple qualified names can map to same unqualified string
	seen := make(map[string]bool)
	dedupedNames := []string{}
	for _, name := range tableNames {
		if !seen[name] {
			seen[name] = true
			dedupedNames = append(dedupedNames, name)
		}
	}
	tableNames = dedupedNames

	// Step 4: Sort for deterministic output (alphabetical order)
	sort.Strings(tableNames)

	return tableNames, nil
}
```

### Layer 2: AST Visitor (TableExtractor)

Location: `internal/parser/table_extractor.go` (new file)

```go
// TableRef represents a table reference in a query
// NOTE: This is an ENHANCED version of the AST's TableRef
// AST TableRef currently has: Schema, TableName, Alias, Subquery
// This version adds Catalog field for Phase 2 support
type TableRef struct {
	Catalog string  // Optional catalog name (e.g., "main") - REQUIRES AST ENHANCEMENT
	Schema  string  // Optional schema name (e.g., "public")
	Table   string  // Table name (required)
	Alias   string  // Optional alias (not used in output)
}

// QualifiedName returns catalog.schema.table format
func (t TableRef) QualifiedName() string {
	parts := []string{}
	if t.Catalog != "" {
		parts = append(parts, t.Catalog)
	}
	if t.Schema != "" {
		parts = append(parts, t.Schema)
	}
	parts = append(parts, t.Table)
	return strings.Join(parts, ".")
}

// TableExtractor implements visitor pattern to extract table references
type TableExtractor struct {
	tables    map[TableRef]struct{}  // Use map for automatic deduplication (O(1) lookup)
	qualified bool                   // Whether to resolve qualified names

	// NOTE: CTE support requires AST enhancement (SelectStmt has no CTEs field)
	// cteNames would track CTE names to exclude from output once AST supports CTEs
}

// Visitor methods for each AST node type
// NOTE: AST nodes need Accept(Visitor) methods added for visitor pattern

func (te *TableExtractor) VisitSelectStmt(stmt *SelectStmt) {
	// Extract FROM clause tables
	if stmt.From != nil {
		// Current AST: FromClause has Tables []TableRef and Joins []JoinClause
		for _, tableRef := range stmt.From.Tables {
			te.visitTableRef(&tableRef)
		}

		// Extract JOIN clause tables (all types: INNER, LEFT, RIGHT, FULL, CROSS)
		for _, join := range stmt.From.Joins {
			te.visitTableRef(&join.Table)
		}
	}

	// Recursively visit WHERE clause subqueries
	if stmt.Where != nil {
		te.visitExpr(stmt.Where)  // Extract subqueries in WHERE
	}

	// Recursively visit HAVING clause subqueries
	if stmt.Having != nil {
		te.visitExpr(stmt.Having)  // Extract subqueries in HAVING
	}

	// Recursively visit SELECT list subqueries
	for _, col := range stmt.Columns {
		if col.Expr != nil {
			te.visitExpr(col.Expr)  // Extract subqueries in SELECT list
		}
	}

	// NOTE: CTEs NOT supported - requires AST enhancement
	// Once SelectStmt has CTEs field, add:
	// for _, cte := range stmt.CTEs {
	//     te.cteNames[cte.Name] = struct{}{}
	//     te.VisitSelectStmt(cte.Query)
	// }
}

func (te *TableExtractor) VisitInsertStmt(stmt *InsertStmt) {
	// Extract INSERT INTO target table
	// Current AST: InsertStmt has Schema and Table fields (not full TableRef)
	ref := TableRef{
		Schema: stmt.Schema,
		Table:  stmt.Table,
	}
	te.tables[ref] = struct{}{}

	// If INSERT...SELECT, visit SELECT subquery
	if stmt.Select != nil {
		te.VisitSelectStmt(stmt.Select)
	}
}

func (te *TableExtractor) VisitUpdateStmt(stmt *UpdateStmt) {
	// Extract UPDATE target table
	// Current AST: UpdateStmt has Schema and Table fields (not full TableRef)
	ref := TableRef{
		Schema: stmt.Schema,
		Table:  stmt.Table,
	}
	te.tables[ref] = struct{}{}

	// NOTE: UPDATE...FROM NOT supported - requires AST enhancement
	// UpdateStmt currently has no From field
	// Once From field added, extract those tables too

	// Visit WHERE clause subqueries
	if stmt.Where != nil {
		te.visitExpr(stmt.Where)
	}

	// Visit SET clause subqueries (rare but possible)
	for _, setClause := range stmt.Set {
		if setClause.Value != nil {
			te.visitExpr(setClause.Value)
		}
	}
}

func (te *TableExtractor) VisitDeleteStmt(stmt *DeleteStmt) {
	// Extract DELETE FROM target table
	// Current AST: DeleteStmt has Schema and Table fields (not full TableRef)
	ref := TableRef{
		Schema: stmt.Schema,
		Table:  stmt.Table,
	}
	te.tables[ref] = struct{}{}

	// Visit WHERE clause subqueries
	if stmt.Where != nil {
		te.visitExpr(stmt.Where)
	}
}

// visitTableRef extracts table from AST TableRef
func (te *TableExtractor) visitTableRef(astRef *TableRef) {
	// Current AST TableRef has: Schema, TableName, Alias, Subquery
	if astRef.Subquery != nil {
		// This is a subquery in FROM clause, recurse into it
		te.VisitSelectStmt(astRef.Subquery)
		return
	}

	// This is a real table reference
	ref := TableRef{
		Schema: astRef.Schema,
		Table:  astRef.TableName,  // AST uses "TableName" field, not "Table"
		Alias:  astRef.Alias,
		// Catalog: astRef.Catalog,  // Not available in current AST - requires enhancement
	}

	te.tables[ref] = struct{}{}  // Deduplication via map
}

// visitExpr traverses expressions to extract subqueries
// Handles: InSubqueryExpr, ExistsExpr, and SelectStmt as expression
func (te *TableExtractor) visitExpr(expr Expr) {
	switch e := expr.(type) {
	case *InSubqueryExpr:
		// Subquery in WHERE...IN clause
		te.VisitSelectStmt(e.Subquery)

	case *ExistsExpr:
		// Subquery in WHERE EXISTS clause
		te.VisitSelectStmt(e.Subquery)

	case *SelectStmt:
		// SelectStmt can be used as scalar subquery expression
		te.VisitSelectStmt(e)

	case *BinaryExpr:
		// Recurse into left and right sides for nested subqueries
		te.visitExpr(e.Left)
		te.visitExpr(e.Right)

	case *UnaryExpr:
		// Recurse into operand
		te.visitExpr(e.Expr)

	case *BetweenExpr:
		// Check all three expressions
		te.visitExpr(e.Expr)
		te.visitExpr(e.Low)
		te.visitExpr(e.High)

	case *InListExpr:
		// Check expression and all list values
		te.visitExpr(e.Expr)
		for _, val := range e.Values {
			te.visitExpr(val)
		}

	case *CaseExpr:
		// Check operand, all WHEN conditions/results, and ELSE
		if e.Operand != nil {
			te.visitExpr(e.Operand)
		}
		for _, when := range e.Whens {
			te.visitExpr(when.Condition)
			te.visitExpr(when.Result)
		}
		if e.Else != nil {
			te.visitExpr(e.Else)
		}

	case *FunctionCall:
		// Check all function arguments for subqueries
		// NOTE: Also distinguishes table functions from regular functions
		for _, arg := range e.Args {
			te.visitExpr(arg)
		}

	case *CastExpr:
		te.visitExpr(e.Expr)

	// Leaf expression types (no recursion needed):
	case *ColumnRef, *Literal, *Parameter, *StarExpr:
		// No subqueries in these
		return

	default:
		// Unknown expression type - no action
		return
	}
}
```

### Layer 3: Qualified Name Resolution (Phase 2)

If `qualified=true` and table reference doesn't have catalog/schema, use catalog to resolve:

```go
// ResolveQualifiedName fills in missing catalog/schema using current connection's catalog
// NOTE: This is Phase 2 functionality - requires Catalog field in AST TableRef
func (te *TableExtractor) ResolveQualifiedName(catalog *Catalog, ref TableRef) TableRef {
	// Phase 1 MVP: Just return whatever the query specifies
	// No catalog access, no default schema resolution

	// Phase 2: Full resolution
	// If fully qualified already, return as-is
	if ref.Catalog != "" && ref.Schema != "" {
		return ref
	}

	// If only table name provided, prepend default catalog.schema
	if ref.Catalog == "" && ref.Schema == "" {
		ref.Catalog = catalog.DefaultCatalog()  // e.g., "main"
		ref.Schema = catalog.DefaultSchema()    // e.g., "public"
		return ref
	}

	// If schema.table provided, prepend default catalog
	if ref.Catalog == "" {
		ref.Catalog = catalog.DefaultCatalog()
		return ref
	}

	return ref
}
```

**MVP Approach (Phase 1)**: Return qualified names as specified in query. No default schema/catalog resolution. If query says "users", return "users". If query says "public.users", return "public.users". If query says "main.public.users", return "main.public.users" (once Catalog field added to AST).

**Full Resolution (Phase 2)**: Use catalog to fill in missing catalog/schema for fully qualified names. Requires catalog integration.

## Implementation Patterns

### AST Visitor Pattern

The core design uses the **Visitor pattern** for AST traversal:

```go
// Visitor interface (add to internal/parser/ast.go if not present)
type Visitor interface {
	VisitSelectStmt(*SelectStmt)
	VisitInsertStmt(*InsertStmt)
	VisitUpdateStmt(*UpdateStmt)
	VisitDeleteStmt(*DeleteStmt)
	// ... other statement types
}

// Each AST node implements Accept method
type SelectStmt struct {
	// ... fields
}

func (s *SelectStmt) Accept(v Visitor) {
	v.VisitSelectStmt(s)
}
```

**Why Visitor Pattern?**
- Clean separation: TableExtractor logic separate from AST structure
- Extensible: Easy to add new statement types
- Reusable: Other extractors can reuse visitor infrastructure (column names, function calls, etc.)

### Deduplication Strategy

Use **map[TableRef]struct{}** instead of slice for automatic deduplication:

```go
tables := make(map[TableRef]struct{})
tables[ref1] = struct{}{}
tables[ref1] = struct{}{}  // Duplicate automatically discarded
```

**Why?**
- O(1) deduplication vs O(n²) with slice Contains()
- Simpler code (no manual duplicate checking)
- Memory efficient (struct{} is zero bytes)

### Deterministic Output

Always sort table names before returning:

```go
sort.Strings(tableNames)
```

**Why?**
- Deterministic output for testing (same query → same order)
- Matches reference duckdb-go behavior
- User expectations (alphabetical is intuitive)

## Edge Cases & Handling

### Case 1: CTE (Common Table Expression) Names - NOT SUPPORTED

```sql
WITH active_users AS (SELECT * FROM users WHERE active = true)
SELECT * FROM active_users JOIN orders ON active_users.id = orders.user_id
```

**Expected (once supported)**: `["orders", "users"]` (NOT "active_users" - it's a CTE, not a table)

**Current Status**: CTEs NOT supported - requires AST enhancement (SelectStmt has no CTEs field)

**Future Implementation**: Track CTE names in `cteNames map[string]struct{}`, skip them in visitTableRef()

### Case 2: Self-Joins (Same Table Multiple Times)

```sql
SELECT * FROM users u1 JOIN users u2 ON u1.manager_id = u2.id
```

**Expected**: `["users"]` (deduplicated, appears once)

**Implementation**: Map automatically deduplicates

### Case 3: Table Functions (read_csv, read_parquet, unnest, json_each)

```sql
SELECT * FROM read_csv('data.csv')
```

**Expected**: `[]` (table functions are NOT table references)

**Implementation**: AST distinguishes TableRef (with Subquery or TableName) from FunctionCall in FROM clause. Only extract TableRef nodes where Subquery is nil and TableName is non-empty. FunctionCall nodes are naturally excluded.

**Table functions to exclude**: read_csv(), read_parquet(), unnest(), json_each(), and any other functions that can appear in FROM clause.

### Case 4: Views

```sql
CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;
SELECT * FROM active_users;
```

**Expected**: `["active_users"]` (view name, not underlying "users" table)

**Implementation**: Views are TableName references in AST, extracted normally. View expansion is out of scope.

### Case 5: Nested Subqueries

```sql
SELECT * FROM (SELECT * FROM (SELECT * FROM users) AS t1) AS t2
```

**Expected**: `["users"]` (innermost table extracted)

**Implementation**: Recursive visitTableSource() handles arbitrary nesting

### Case 6: Empty Query or No Tables

```sql
SELECT 1 + 1;
SELECT NULL;
```

**Expected**: `[]` (empty slice, NOT nil)

**Implementation**: Return `[]string{}` when `len(extractor.tables) == 0` after traversal. This handles:
- Queries with no FROM clause (SELECT 1+1)
- Queries selecting NULL
- Empty query string

## Performance Considerations

### Parse-Time Only
- **No execution**: Parser is fast, but baseline needs measurement
- **No storage access**: No I/O overhead
- **Target**: <1ms for queries <1000 lines (after parser optimization, on modern CPU ~3GHz+)
- **Baseline task**: Measure parser performance to establish realistic targets

### Memory Efficiency
- **TableRef struct**: ~90-140 bytes (3-4 strings @ 16 bytes header + data, plus map overhead)
- **Typical query**: 5-10 tables = <2KB
- **Large query**: 100 tables = ~14KB
- **Map vs slice**: Using map for deduplication adds ~20-30% memory overhead but provides O(1) deduplication. Worthwhile for large result sets (>50 tables).

### Scalability
- **O(n) complexity**: Linear with AST node count
- **Not affected by table size**: No data scanning
- **Bottleneck**: Parser, not table extraction
- **Nested queries**: 10-level nesting should complete in <1ms (recursion depth handled without stack overflow)

### Performance Targets by Category
- **Small queries** (<100 AST nodes, <10 tables): <500µs (not <100µs - unrealistic)
- **Medium queries** (100-500 nodes, 10-50 tables): <1ms
- **Large queries** (500-1000 nodes, 50-100 tables): <2ms
- **Deep nesting** (10 levels): <1ms (not <500µs)

## Testing Strategy

### Unit Test Categories (30+ tests)

1. **Basic SELECT**: Single table, no joins (TestGetTableNames_SimpleSelect)
2. **JOINs**: All join types explicitly tested:
   - TestGetTableNames_InnerJoin
   - TestGetTableNames_LeftJoin
   - TestGetTableNames_RightJoin
   - TestGetTableNames_FullJoin
   - TestGetTableNames_CrossJoin
   - TestGetTableNames_MultipleJoins
3. **Subqueries**: Nested SELECT in different locations:
   - TestGetTableNames_SubqueryInFrom
   - TestGetTableNames_SubqueryInWhere
   - TestGetTableNames_SubqueryInHaving
   - TestGetTableNames_SubqueryInSelect
   - TestGetTableNames_SubqueryInJoinOn
4. **DML**: INSERT, UPDATE, DELETE statements:
   - TestGetTableNames_Insert
   - TestGetTableNames_InsertSelect
   - TestGetTableNames_Update
   - TestGetTableNames_UpdateWithSubquery
   - TestGetTableNames_Delete
   - TestGetTableNames_DeleteWithSubquery
5. **Edge Cases**:
   - TestGetTableNames_EmptyQuery
   - TestGetTableNames_NoTables (SELECT 1+1)
   - TestGetTableNames_SelectNull
   - TestGetTableNames_TableFunctions (read_csv excluded)
   - TestGetTableNames_Views (view name, not underlying tables)
   - TestGetTableNames_Aliases (aliases NOT in output)
6. **Qualified Names**:
   - TestGetTableNames_Unqualified
   - TestGetTableNames_SchemaQualified
   - TestGetTableNames_PartialQualification (schema.table)
7. **Deduplication**:
   - TestGetTableNames_SelfJoin (deduplicated)
   - TestGetTableNames_CaseSensitivity
8. **Error Handling**:
   - TestGetTableNames_InvalidSyntax (returns *ParserError)
   - TestGetTableNames_ErrorMessage (descriptive messages)
9. **Deterministic Output**:
   - TestGetTableNames_Sorting (alphabetical)
   - TestGetTableNames_Deterministic (100 runs, same output)

### Compatibility Testing

Compare against reference duckdb-go v1.4.3 for supported features only:

```go
func TestGetTableNames_Compatibility(t *testing.T) {
	testCases := []struct {
		query     string
		qualified bool
		expected  []string
		skipReason string // For unsupported features
	}{
		{"SELECT * FROM users", false, []string{"users"}, ""},
		{"SELECT * FROM users u JOIN orders o ON u.id = o.user_id", false, []string{"orders", "users"}, ""},
		{"WITH tmp AS (SELECT * FROM users) SELECT * FROM tmp", false, []string{"users"}, "CTEs not supported - requires AST enhancement"},
		// ... 30+ supported test cases, plus documentation of unsupported features
	}

	for _, tc := range testCases {
		if tc.skipReason != "" {
			t.Skipf("Feature not supported: %s", tc.skipReason)
			continue
		}

		// Test dukdb-go
		dukdbResult, err := dukdb.GetTableNames(dukdbConn, tc.query, tc.qualified)
		require.NoError(t, err)

		// Test reference duckdb-go v1.4.3
		refResult, err := duckdb.GetTableNames(refConn, tc.query, tc.qualified)
		require.NoError(t, err)

		// Results must match exactly for supported features
		assert.Equal(t, refResult, dukdbResult)
	}
}
```

**Compatibility Note**: Not all reference implementation features are supported due to AST limitations. Focus on core DML (SELECT, INSERT, UPDATE, DELETE) with subqueries and joins.

## Dependencies

### Existing Infrastructure
- **Parser** (`internal/parser/`): Handles core SQL syntax - READY (but limited: no CTEs, no UNION, no CREATE TABLE AS)
- **AST structures** (`internal/parser/ast.go`): SelectStmt, InsertStmt, UpdateStmt, DeleteStmt exist - NEEDS ENHANCEMENTS:
  - TableRef missing Catalog field (required for Phase 2)
  - SelectStmt missing CTEs field (CTE support blocked)
  - No SetOperation types (UNION/INTERSECT/EXCEPT blocked)
  - CreateTableStmt missing Select field (CREATE TABLE AS blocked)
  - UpdateStmt missing From field (UPDATE...FROM blocked)
- **Catalog** (`internal/catalog/`): Optional for qualified name resolution - READY but deferred to Phase 2

### New Components Required
- **Visitor pattern infrastructure** (`internal/parser/visitor.go`): NEW, ~50 lines
  - Visitor interface with Visit methods for each statement type
  - Accept(Visitor) methods added to all AST node types
- **TableExtractor visitor** (`internal/parser/table_extractor.go`): NEW, ~300 lines
  - Implements visitor pattern
  - Extracts table references with deduplication
  - Handles subquery recursion
- **GetTableNames() public function** (root package `gettablenames.go`): NEW, ~80 lines
  - Public API matching duckdb-go v1.4.3 signature
  - Orchestrates parsing and extraction

### External Dependencies
- None - pure Go implementation, no cgo

### AST Enhancements Timeline
- **Phase 1 (Required)**: Add Catalog field to TableRef, implement visitor pattern
- **Phase 2 (Nice-to-have)**: CTEs, UNION, CREATE TABLE AS SELECT, UPDATE...FROM support

## Risks & Mitigation

### Risk 1: AST Visitor Pattern Not Implemented
**Impact**: Medium - need to add visitor infrastructure to parser
**Mitigation**: Visitor pattern is simple (~50 lines of interface + Accept methods)
**Estimated Effort**: 2-3 hours

### Risk 2: Parser AST Structure Incomplete
**Impact**: High - if parser doesn't expose table references correctly
**Mitigation**: Review parser.go and ast.go before implementation; existing tests suggest structure is complete
**Estimated Effort**: 1 hour investigation

### Risk 3: Qualified Name Resolution Complex
**Impact**: Low - only affects qualified=true mode
**Mitigation**: MVP can skip full resolution, just return what query specifies
**Estimated Effort**: Can defer to Phase 2

### Risk 4: CTE Support Blocked by AST
**Impact**: HIGH - CTEs cannot be supported without AST enhancement
**Mitigation**: Document as out-of-scope for MVP; add AST CTE support to Phase 2 roadmap
**Estimated Effort**: 8-12 hours for AST enhancement + testing (deferred)

## Alternative Designs Considered

### Alternative 1: Execute EXPLAIN and Parse Output
```go
func GetTableNames(conn *sql.Conn, query string) ([]string, error) {
	rows, err := conn.QueryContext(ctx, "EXPLAIN "+query)
	// Parse EXPLAIN output to extract table names
}
```

**Rejected**: Slower (requires executor), less accurate (EXPLAIN optimizes), breaks no-execution guarantee

### Alternative 2: Regex-Based Table Extraction
```go
func GetTableNames(query string) []string {
	re := regexp.MustCompile(`(?i)FROM\s+(\w+)`)
	matches := re.FindAllStringSubmatch(query, -1)
	// ...
}
```

**Rejected**: Fragile (doesn't handle subqueries, CTEs, quoted identifiers), inaccurate

### Alternative 3: Full Binder Integration
```go
func GetTableNames(conn *sql.Conn, query string) ([]string, error) {
	binder := NewBinder(conn.Catalog())
	boundQuery, err := binder.Bind(query)
	return boundQuery.TableReferences(), nil
}
```

**Rejected**: Overkill (binder does type checking, column resolution - unnecessary), slower, requires catalog access

## Future Enhancements (Out of Scope for MVP)

### Phase 2 AST Enhancements
1. **CTE Support**: Add CTEs field to SelectStmt for WITH clause support
2. **Set Operations**: Add SetOperation types for UNION, INTERSECT, EXCEPT
3. **CREATE TABLE AS SELECT**: Add Select field to CreateTableStmt
4. **UPDATE...FROM**: Add From field to UpdateStmt
5. **Full Catalog Support**: Add Catalog field to TableRef for catalog.schema.table names

### Additional APIs (Phase 3+)
6. **GetColumnNames() API**: Extract column references from query
7. **GetFunctionCalls() API**: Extract UDF/built-in function usage
8. **GetViewExpansion() API**: Recursively expand views to underlying tables
9. **GetQueryDependencyGraph() API**: Return full dependency graph (tables + columns)
10. **GetTableAccessPattern() API**: Categorize access as read/write/delete

### Documentation
11. **EXAMPLES.md**: Create usage examples document showing common patterns:
    - Query routing based on table names
    - Access control validation
    - Multi-tenant query filtering
    - Data lineage tracking
