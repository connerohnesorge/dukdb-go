# Parser Specification - Delta

## ADDED Requirements

### Requirement: AST Visitor Pattern for Table Extraction

The parser SHALL provide a visitor pattern infrastructure for traversing AST nodes to extract table references.

**Rationale**: Table extraction requires traversing SELECT, INSERT, UPDATE, DELETE statements recursively. Visitor pattern cleanly separates traversal logic from AST structure, enabling reusable extraction logic.

#### Scenario: Visitor interface defined
- GIVEN parser package
- WHEN defining visitor pattern
- THEN Visitor interface includes methods for all statement types:
  - VisitSelectStmt(*SelectStmt)
  - VisitInsertStmt(*InsertStmt)
  - VisitUpdateStmt(*UpdateStmt)
  - VisitDeleteStmt(*DeleteStmt)
- AND each AST node implements Accept(Visitor) method

#### Scenario: SelectStmt traversal with FROM clause
- GIVEN SelectStmt AST node with FROM clause referencing "users" table
- WHEN visitor traverses AST
- THEN visitor.VisitSelectStmt() is called
- AND FROM clause table reference is accessible
- AND table name "users" can be extracted

#### Scenario: SelectStmt traversal with JOIN clauses
- GIVEN SelectStmt with "users" in FROM and "orders" in JOIN
- WHEN visitor traverses AST
- THEN both table references are accessible
- AND visitor can extract ["orders", "users"]

#### Scenario: SelectStmt traversal with subquery in FROM
- GIVEN SelectStmt with subquery in FROM clause
- WHEN visitor traverses AST
- THEN subquery is recognized as nested SelectStmt
- AND visitor can recursively traverse subquery
- AND innermost table references are extractable

#### Scenario: SelectStmt traversal with CTE (WITH clause) - NOT SUPPORTED
- GIVEN SelectStmt (current AST has no CTEs field)
- WHEN visitor traverses AST
- THEN CTE support is NOT available (requires AST enhancement)
- NOTE: Once SelectStmt.CTEs field added, CTE name exclusion and recursive traversal can be implemented

#### Scenario: InsertStmt traversal with VALUES
- GIVEN InsertStmt with target table "archive"
- AND VALUES clause (not SELECT)
- WHEN visitor traverses AST
- THEN visitor.VisitInsertStmt() is called
- AND target table "archive" is accessible

#### Scenario: InsertStmt traversal with SELECT subquery
- GIVEN InsertStmt with target table "archive"
- AND SELECT subquery referencing "users" table
- WHEN visitor traverses AST
- THEN target table "archive" is accessible
- AND SELECT subquery is accessible for recursive traversal
- AND source table "users" is extractable

#### Scenario: UpdateStmt traversal with target table
- GIVEN UpdateStmt with target table "users"
- AND no FROM clause
- WHEN visitor traverses AST
- THEN visitor.VisitUpdateStmt() is called
- AND target table "users" is accessible

#### Scenario: UpdateStmt traversal with FROM clause - NOT SUPPORTED
- GIVEN UpdateStmt (current AST has no From field)
- WHEN visitor traverses AST
- THEN UPDATE...FROM is NOT supported (requires AST enhancement)
- NOTE: Once UpdateStmt.From field added, FROM clause tables can be extracted

#### Scenario: DeleteStmt traversal with target table
- GIVEN DeleteStmt with target table "users"
- WHEN visitor traverses AST
- THEN visitor.VisitDeleteStmt() is called
- AND target table "users" is accessible

#### Scenario: DeleteStmt traversal with WHERE subquery
- GIVEN DeleteStmt with target table "users"
- AND WHERE clause with subquery referencing "deleted_accounts"
- WHEN visitor traverses AST
- THEN target table "users" is accessible
- AND WHERE subquery is accessible for recursive traversal
- AND "deleted_accounts" table reference is extractable

### Requirement: TableRef Structure for Qualified Names

The parser SHALL provide a TableRef structure representing table references with optional schema qualifiers (catalog support in Phase 2).

**Rationale**: SQL tables can be referenced as `table` or `schema.table`. Full `catalog.schema.table` support requires AST Catalog field enhancement.

**Current AST**: TableRef has Schema, TableName, Alias, Subquery fields (NO Catalog field yet).

#### Scenario: TableRef with unqualified name
- GIVEN table reference "users" in query (current AST)
- WHEN creating TableRef
- THEN TableRef.TableName = "users"
- AND TableRef.Schema = ""
- NOTE: Catalog field not available in current AST

#### Scenario: TableRef with schema-qualified name
- GIVEN table reference "public.users" in query (current AST)
- WHEN creating TableRef
- THEN TableRef.Schema = "public"
- AND TableRef.TableName = "users"

#### Scenario: TableRef with fully-qualified name (Phase 2 - requires Catalog field)
- GIVEN table reference "main.public.users" in query
- WHEN creating TableRef (once Catalog field added)
- THEN TableRef.Catalog = "main" (after enhancement)
- AND TableRef.Schema = "public"
- AND TableRef.TableName = "users"

#### Scenario: TableRef QualifiedName() method (Phase 1: schema.table, Phase 2: catalog.schema.table)
- GIVEN TableRef with Schema="public", TableName="users" (current AST)
- WHEN calling QualifiedName()
- THEN returns "public.users"
- GIVEN TableRef with Schema="", TableName="users"
- WHEN calling QualifiedName()
- THEN returns "users"
- NOTE: Full catalog.schema.table format requires Catalog field enhancement

#### Scenario: TableRef with alias
- GIVEN table reference "users u" in query (with alias)
- WHEN creating TableRef
- THEN TableRef.Table = "users"
- AND TableRef.Alias = "u"
- AND alias is NOT used in qualified name output

### Requirement: TableExtractor Visitor Implementation

The parser SHALL provide a TableExtractor visitor that collects table references from AST traversal with deduplication.

**Rationale**: TableExtractor implements the visitor pattern to extract table names from queries, handling deduplication and qualified name modes automatically.

#### Scenario: TableExtractor initialization
- GIVEN qualified mode flag (true or false)
- WHEN creating TableExtractor
- THEN extractor is initialized with empty tables map
- AND qualified mode is stored for later use

#### Scenario: TableExtractor collects FROM clause tables
- GIVEN SelectStmt with FROM clause referencing "users"
- WHEN TableExtractor visits AST
- THEN "users" is added to tables map
- AND GetTables() returns ["users"]

#### Scenario: TableExtractor deduplicates self-joins
- GIVEN SelectStmt with FROM users u1 JOIN users u2
- WHEN TableExtractor visits AST
- THEN "users" is added to tables map once (map key deduplication)
- AND GetTables() returns ["users"] (single occurrence)

#### Scenario: TableExtractor excludes CTE names (NOT SUPPORTED - requires AST enhancement)
- GIVEN SelectStmt (current AST has no CTEs field)
- WHEN TableExtractor visits AST
- THEN CTE support NOT available
- NOTE: Once CTEs field added, "tmp" would be marked as CTE name and excluded from output

#### Scenario: TableExtractor handles nested subqueries
- GIVEN SelectStmt with 3-level nested subqueries, innermost references "products"
- WHEN TableExtractor visits AST
- THEN extractor recursively traverses all 3 levels
- AND "products" is extracted from innermost subquery
- AND GetTables() returns ["products"]

#### Scenario: TableExtractor returns sorted output
- GIVEN SelectStmt with tables ["zebra", "apple", "mango"]
- WHEN TableExtractor visits AST and GetTables() is called
- THEN returns ["apple", "mango", "zebra"] (alphabetically sorted)
- AND sort order is deterministic (same every time)

#### Scenario: TableExtractor qualified mode
- GIVEN SelectStmt with "main.public.users" table reference
- AND TableExtractor with qualified=true
- WHEN GetTables() is called
- THEN returns ["main.public.users"] (fully qualified)
- GIVEN same query but TableExtractor with qualified=false
- WHEN GetTables() is called
- THEN returns ["users"] (unqualified, just table name)

#### Scenario: TableExtractor handles INSERT...SELECT
- GIVEN InsertStmt with target "archive" and SELECT from "users"
- WHEN TableExtractor visits AST
- THEN both "archive" and "users" are added to tables map
- AND GetTables() returns ["archive", "users"]

#### Scenario: TableExtractor handles UPDATE with WHERE subquery
- GIVEN UpdateStmt with target "users" and WHERE subquery from "deleted_accounts"
- WHEN TableExtractor visits AST
- THEN both "users" and "deleted_accounts" are added to tables map
- AND GetTables() returns ["deleted_accounts", "users"] (sorted)

#### Scenario: TableExtractor handles multiple CTEs (NOT SUPPORTED - requires AST enhancement)
- GIVEN SelectStmt (current AST has no CTEs field)
- WHEN TableExtractor visits AST
- THEN CTE support NOT available
- NOTE: Once CTEs field added, both CTE names would be excluded and source tables extracted

#### Scenario: TableExtractor empty result for no tables
- GIVEN SelectStmt with no table references (SELECT 1+1)
- WHEN TableExtractor visits AST
- THEN tables map remains empty
- AND GetTables() returns [] (empty slice, not nil)

#### Scenario: TableExtractor handles UNION operations (NOT SUPPORTED - requires AST enhancement)
- GIVEN query with UNION (current AST has no SetOperation types)
- WHEN TableExtractor visits AST
- THEN UNION support NOT available
- NOTE: Once SetOperation types added, both branches would be traversed and tables extracted

### Requirement: Table Function Exclusion

The parser SHALL NOT extract table functions (e.g., read_csv, read_parquet) as table references.

**Rationale**: Table functions like read_csv() generate tables dynamically but are not table references in the catalog. They should be excluded from table name extraction.

#### Scenario: read_csv excluded from table extraction
- GIVEN SelectStmt with FROM read_csv('data.csv')
- WHEN TableExtractor visits AST
- THEN read_csv is recognized as function call (not table reference)
- AND no table name is extracted
- AND GetTables() returns [] (empty)

#### Scenario: read_parquet excluded from table extraction
- GIVEN SelectStmt with FROM read_parquet('data.parquet')
- WHEN TableExtractor visits AST
- THEN read_parquet is recognized as function call
- AND GetTables() returns [] (empty)

#### Scenario: Normal table and table function mixed
- GIVEN SelectStmt with "FROM users JOIN read_csv('data.csv')"
- WHEN TableExtractor visits AST
- THEN "users" is extracted (table reference)
- AND read_csv is excluded (function call)
- AND GetTables() returns ["users"]

### Requirement: View Handling

The parser SHALL extract view names as table references without expanding to underlying tables.

**Rationale**: View expansion requires catalog access and is out of scope for parse-time table extraction. Views are treated as opaque table references.

#### Scenario: View extracted as table reference
- GIVEN database with view "active_users" (defined as SELECT * FROM users)
- AND SelectStmt with "FROM active_users"
- WHEN TableExtractor visits AST
- THEN "active_users" is extracted as table reference
- AND underlying "users" table is NOT extracted (view not expanded)
- AND GetTables() returns ["active_users"]

### Requirement: Performance Optimization for Large Queries

The parser table extraction SHALL complete in <1ms for typical queries (after parser optimization) and scale linearly with AST size.

**Rationale**: Table extraction is parse-time only with no I/O, should be fast. Users expect instant results for query analysis tools.

**Note**: Parser baseline performance needs measurement. Targets assume modern CPU (~3GHz+).

#### Scenario: Small query performance
- GIVEN query with <100 AST nodes and <10 tables
- AND modern CPU (~3GHz+)
- WHEN TableExtractor traverses AST
- THEN completes in <500µs (not <100µs - unrealistic)
- AND memory usage <2KB (~90-140 bytes per TableRef)

#### Scenario: Large query performance
- GIVEN query with 1000 AST nodes and 100 tables
- WHEN TableExtractor traverses AST
- THEN completes in <2ms (not <1ms for this size)
- AND memory usage <15KB
- AND complexity is O(n) where n = AST nodes

#### Scenario: Deep nesting performance
- GIVEN query with 10-level nested subqueries
- WHEN TableExtractor traverses AST
- THEN recursion depth is handled without stack overflow
- AND completes in <1ms (not <500µs - unrealistic for 10 levels)
