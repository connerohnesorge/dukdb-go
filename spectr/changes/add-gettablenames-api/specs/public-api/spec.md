# Public API Specification - Delta

## ADDED Requirements

### Requirement: GetTableNames() Query Introspection API

The system SHALL provide a public API to extract table names from SQL queries without execution.

**Rationale**: Users need to analyze query dependencies before execution for query routing, access control validation, and data lineage tracking. This API exists in duckdb-go v1.4.3 and must be implemented for drop-in compatibility.

#### Scenario: Simple SELECT table extraction
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM users", false)
- THEN returns []string{"users"}
- AND query is NOT executed (parse-time only)

#### Scenario: JOIN table extraction with sorting
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM users u JOIN orders o ON u.id = o.user_id", false)
- THEN returns []string{"orders", "users"}
- AND result is sorted alphabetically
- AND deduplication occurs if same table appears multiple times

#### Scenario: Subquery table extraction (recursive)
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM (SELECT * FROM users WHERE age > 18) AS adults", false)
- THEN returns []string{"users"}
- AND subquery is traversed recursively
- AND alias "adults" is NOT included in output

#### Scenario: CTE table extraction (NOT SUPPORTED - requires AST enhancement)
- GIVEN database connection
- WHEN calling GetTableNames(conn, "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users JOIN orders ON active_users.id = orders.user_id", false)
- THEN feature is NOT supported (AST has no CTEs field in SelectStmt)
- NOTE: Expected behavior once supported: returns []string{"orders", "users"} with CTE name "active_users" excluded

#### Scenario: INSERT...SELECT table extraction
- GIVEN database connection
- WHEN calling GetTableNames(conn, "INSERT INTO archive SELECT * FROM users WHERE created_at < '2020-01-01'", false)
- THEN returns []string{"archive", "users"}
- AND both target and source tables are extracted

#### Scenario: UPDATE with subquery table extraction
- GIVEN database connection
- WHEN calling GetTableNames(conn, "UPDATE users SET status = 'inactive' WHERE id IN (SELECT user_id FROM deleted_accounts)", false)
- THEN returns []string{"deleted_accounts", "users"}
- AND subquery in WHERE clause is traversed

#### Scenario: DELETE with WHERE clause table extraction
- GIVEN database connection
- WHEN calling GetTableNames(conn, "DELETE FROM users WHERE id = 123", false)
- THEN returns []string{"users"}
- AND only the target table is extracted (no subqueries)

#### Scenario: Qualified vs unqualified names (schema.table supported, catalog requires Phase 2)
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM public.users", false)
- THEN returns []string{"users"} (unqualified mode)
- WHEN calling GetTableNames(conn, "SELECT * FROM public.users", true)
- THEN returns []string{"public.users"} (qualified mode with schema)
- NOTE: catalog.schema.table format requires AST Catalog field enhancement (Phase 2)

#### Scenario: Empty query handling
- GIVEN database connection
- WHEN calling GetTableNames(conn, "", false)
- THEN returns []string{} (empty slice, not nil)
- AND no error is returned

#### Scenario: Query with no tables
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT 1 + 1", false)
- THEN returns []string{} (empty slice)
- AND no error is returned (valid query, just no tables)

#### Scenario: Invalid syntax error handling
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM", false)
- THEN returns nil for table names
- AND returns *ParserError with descriptive message
- AND error message includes "parse error" context

#### Scenario: Table functions excluded
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM read_csv('data.csv')", false)
- THEN returns []string{} (empty, table functions are not table references)
- AND no error is returned

#### Scenario: View names included (not expanded)
- GIVEN database with view "active_users" defined as SELECT * FROM users WHERE active = true
- WHEN calling GetTableNames(conn, "SELECT * FROM active_users", false)
- THEN returns []string{"active_users"}
- AND view is NOT expanded to underlying "users" table (out of scope)

#### Scenario: Deduplication for self-joins
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM users u1 JOIN users u2 ON u1.manager_id = u2.id", false)
- THEN returns []string{"users"} (appears once, deduplicated)

#### Scenario: UNION table extraction (NOT SUPPORTED - requires AST enhancement)
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM users UNION SELECT * FROM admins", false)
- THEN feature is NOT supported (AST has no SetOperation types)
- NOTE: Expected behavior once supported: returns []string{"admins", "users"} with tables from all branches

#### Scenario: Deterministic output (always same order)
- GIVEN database connection
- WHEN calling GetTableNames(conn, query, false) 100 times with same query
- THEN all 100 results are identical
- AND order is always alphabetical

#### Scenario: Performance target for typical queries
- GIVEN database connection
- AND query with <1000 lines and <10 tables
- AND modern CPU (~3GHz+)
- WHEN calling GetTableNames(conn, query, false)
- THEN execution completes in <1ms (after parser optimization, parser benchmarks TBD)
- AND no database/storage access occurs (parse-time only)

#### Scenario: CREATE TABLE AS SELECT table extraction (NOT SUPPORTED - requires AST enhancement)
- GIVEN database connection
- WHEN calling GetTableNames(conn, "CREATE TABLE new_table AS SELECT * FROM old_table", false)
- THEN feature is NOT supported (CreateTableStmt has no Select field)
- NOTE: Expected behavior once supported: returns []string{"new_table", "old_table"} with both target and source tables

#### Scenario: Multiple CTEs table extraction (NOT SUPPORTED - requires AST enhancement)
- GIVEN database connection
- WHEN calling GetTableNames(conn, "WITH cte1 AS (SELECT * FROM users), cte2 AS (SELECT * FROM orders) SELECT * FROM cte1 JOIN cte2", false)
- THEN feature is NOT supported (AST has no CTEs field)
- NOTE: Expected behavior once supported: returns []string{"orders", "users"} with CTE names excluded

#### Scenario: Nested subqueries up to 5 levels
- GIVEN database connection
- AND query with 5 levels of nested subqueries, innermost references "products"
- WHEN calling GetTableNames(conn, query, false)
- THEN returns []string{"products"}
- AND recursion handles arbitrary depth

#### Scenario: All JOIN types supported individually
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id", false)
- THEN returns []string{"orders", "users"}
- WHEN calling GetTableNames(conn, "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id", false)
- THEN returns []string{"orders", "users"}
- WHEN calling GetTableNames(conn, "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id", false)
- THEN returns []string{"orders", "users"}
- WHEN calling GetTableNames(conn, "SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id", false)
- THEN returns []string{"orders", "users"}
- WHEN calling GetTableNames(conn, "SELECT * FROM users CROSS JOIN orders", false)
- THEN returns []string{"orders", "users"}

#### Scenario: Subquery in HAVING clause
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT category FROM products GROUP BY category HAVING COUNT(*) > (SELECT AVG(cnt) FROM category_stats)", false)
- THEN returns []string{"category_stats", "products"}

#### Scenario: Subquery in SELECT list
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) FROM users", false)
- THEN returns []string{"orders", "users"}

#### Scenario: Subquery in JOIN ON clause
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND o.amount > (SELECT AVG(amount) FROM transactions)", false)
- THEN returns []string{"orders", "transactions", "users"}

#### Scenario: INSERT INTO without SELECT (VALUES clause)
- GIVEN database connection
- WHEN calling GetTableNames(conn, "INSERT INTO users (name, age) VALUES ('Alice', 30)", false)
- THEN returns []string{"users"}

#### Scenario: CREATE TABLE (plain, not AS SELECT)
- GIVEN database connection
- WHEN calling GetTableNames(conn, "CREATE TABLE users (id INT, name VARCHAR)", false)
- THEN returns []string{"users"}
- AND only the table being created is extracted

#### Scenario: Multiple qualified names with same unqualified part
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM db1.schema1.users JOIN db2.schema2.users", false)
- THEN returns []string{"users"} (deduplicated case-sensitive)
- AND qualified version would handle correctly once catalog support added

#### Scenario: Aliases not extracted
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT u.name FROM users u", false)
- THEN returns []string{"users"}
- AND alias "u" is NOT in output

#### Scenario: Parameterized queries
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM users WHERE id = $1", false)
- THEN returns []string{"users"}
- AND parameter marker $1 is left as-is in expressions

#### Scenario: Comments in queries
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM users -- this is a comment\nWHERE active = true", false)
- THEN returns []string{"users"}
- AND comments are handled by parser (no special action needed)

#### Scenario: Quoted identifiers
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM \"MyTable\"", false)
- THEN returns []string{"MyTable"}
- AND quoted names returned as parser provides them

#### Scenario: SELECT NULL handling
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT NULL", false)
- THEN returns []string{} (empty slice, no tables)

#### Scenario: LATERAL subqueries (NOT SUPPORTED - deferred)
- GIVEN database connection
- WHEN calling GetTableNames(conn, "SELECT * FROM users, LATERAL (SELECT * FROM orders WHERE orders.user_id = users.id)", false)
- THEN feature is NOT supported (LATERAL is advanced feature, deferred)

### Requirement: API Signature Compatibility

The GetTableNames() function SHALL match the signature and behavior of duckdb-go v1.4.3 for supported features (drop-in replacement for core DML).

#### Scenario: Function signature matches reference
- GIVEN reference duckdb-go v1.4.3 API
- THEN dukdb-go GetTableNames() has signature: func(conn *sql.Conn, query string, qualified bool) ([]string, error)
- AND parameter types match exactly
- AND return types match exactly

#### Scenario: Behavior matches reference for supported query types
- GIVEN reference duckdb-go v1.4.3 implementation
- AND set of 30+ test queries covering supported SQL patterns (SELECT, INSERT, UPDATE, DELETE with subqueries and joins)
- WHEN executing GetTableNames() on both dukdb-go and reference duckdb-go
- THEN results are identical for all supported queries
- NOTE: Unsupported features (CTEs, UNION, CREATE TABLE AS, UPDATE...FROM) are documented out-of-scope

#### Scenario: Error types match reference
- GIVEN reference duckdb-go v1.4.3 implementation
- WHEN calling GetTableNames() with invalid syntax
- THEN error type is *ParserError
- AND error message includes "parse error" context
- AND error message is descriptive
