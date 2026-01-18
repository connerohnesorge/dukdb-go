package tests

import (
	"context"
	"database/sql"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import engine to register the backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func TestGetTableNames(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Get a connection
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	tests := []struct {
		name           string
		query          string
		qualified      bool
		expectedTables []string
		expectError    bool
	}{
		{
			name:           "simple SELECT",
			query:          "SELECT * FROM users",
			qualified:      false,
			expectedTables: []string{"users"},
		},
		{
			name:           "SELECT with alias",
			query:          "SELECT u.id FROM users u",
			qualified:      false,
			expectedTables: []string{"users"},
		},
		{
			name:           "SELECT with JOIN",
			query:          "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id",
			qualified:      false,
			expectedTables: []string{"customers", "orders"},
		},
		{
			name:           "SELECT with LEFT JOIN",
			query:          "SELECT * FROM products p LEFT JOIN categories c ON p.category_id = c.id",
			qualified:      false,
			expectedTables: []string{"categories", "products"},
		},
		{
			name:           "INSERT statement",
			query:          "INSERT INTO users (id, name) VALUES (1, 'Alice')",
			qualified:      false,
			expectedTables: []string{"users"},
		},
		{
			name:           "UPDATE statement",
			query:          "UPDATE users SET name = 'Bob' WHERE id = 1",
			qualified:      false,
			expectedTables: []string{"users"},
		},
		{
			name:           "DELETE statement",
			query:          "DELETE FROM users WHERE id = 1",
			qualified:      false,
			expectedTables: []string{"users"},
		},
		{
			name:           "CREATE TABLE statement",
			query:          "CREATE TABLE test_table (id INTEGER, name VARCHAR)",
			qualified:      false,
			expectedTables: []string{"test_table"},
		},
		{
			name:           "DROP TABLE statement",
			query:          "DROP TABLE test_table",
			qualified:      false,
			expectedTables: []string{"test_table"},
		},
		{
			name:           "empty query",
			query:          "",
			qualified:      false,
			expectedTables: []string{},
		},
		{
			name:           "whitespace only query",
			query:          "   \t\n   ",
			qualified:      false,
			expectedTables: []string{},
		},
		{
			name:           "SELECT with no tables",
			query:          "SELECT 1",
			qualified:      false,
			expectedTables: []string{},
		},
		{
			name:           "SELECT with subquery in WHERE",
			query:          "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers)",
			qualified:      false,
			expectedTables: []string{"customers", "orders"},
		},
		{
			name:           "qualified SELECT with schema",
			query:          "SELECT * FROM myschema.users",
			qualified:      true,
			expectedTables: []string{"myschema.users"},
		},
		{
			name:           "unqualified SELECT with schema (returns just table name)",
			query:          "SELECT * FROM myschema.users",
			qualified:      false,
			expectedTables: []string{"users"},
		},
		{
			name:        "parse error - unterminated string",
			query:       "SELECT * FROM users WHERE name = 'unclosed",
			qualified:   false,
			expectError: true,
		},
		{
			name:           "multiple tables comma-separated",
			query:          "SELECT * FROM users, orders, products",
			qualified:      false,
			expectedTables: []string{"orders", "products", "users"},
		},
		{
			name:           "INSERT with SELECT",
			query:          "INSERT INTO archive SELECT * FROM users WHERE active = false",
			qualified:      false,
			expectedTables: []string{"archive", "users"},
		},
		{
			name:           "BEGIN statement (no tables)",
			query:          "BEGIN",
			qualified:      false,
			expectedTables: []string{},
		},
		{
			name:           "COMMIT statement (no tables)",
			query:          "COMMIT",
			qualified:      false,
			expectedTables: []string{},
		},
		{
			name:           "ROLLBACK statement (no tables)",
			query:          "ROLLBACK",
			qualified:      false,
			expectedTables: []string{},
		},
		// CTE (Common Table Expression) tests
		{
			name:           "simple CTE",
			query:          "WITH tmp AS (SELECT * FROM users) SELECT * FROM tmp",
			qualified:      false,
			expectedTables: []string{"users"},
		},
		{
			name:           "multiple CTEs",
			query:          "WITH a AS (SELECT * FROM t1), b AS (SELECT * FROM t2) SELECT * FROM a, b",
			qualified:      false,
			expectedTables: []string{"t1", "t2"},
		},
		{
			name:           "CTE with join to real table",
			query:          "WITH tmp AS (SELECT * FROM users) SELECT * FROM tmp JOIN orders ON tmp.id = orders.user_id",
			qualified:      false,
			expectedTables: []string{"orders", "users"},
		},
		{
			name:           "CTE with column list",
			query:          "WITH tmp(id, name) AS (SELECT id, name FROM users) SELECT * FROM tmp",
			qualified:      false,
			expectedTables: []string{"users"},
		},
		{
			name:           "CTE referencing another CTE",
			query:          "WITH base AS (SELECT * FROM users), derived AS (SELECT * FROM base) SELECT * FROM derived",
			qualified:      false,
			expectedTables: []string{"users"},
		},
		// Set operations (UNION/INTERSECT/EXCEPT)
		{
			name:           "UNION",
			query:          "SELECT * FROM users UNION SELECT * FROM admins",
			qualified:      false,
			expectedTables: []string{"admins", "users"},
		},
		{
			name:           "UNION ALL",
			query:          "SELECT * FROM users UNION ALL SELECT * FROM admins",
			qualified:      false,
			expectedTables: []string{"admins", "users"},
		},
		{
			name:           "INTERSECT",
			query:          "SELECT * FROM t1 INTERSECT SELECT * FROM t2",
			qualified:      false,
			expectedTables: []string{"t1", "t2"},
		},
		{
			name:           "INTERSECT ALL",
			query:          "SELECT * FROM t1 INTERSECT ALL SELECT * FROM t2",
			qualified:      false,
			expectedTables: []string{"t1", "t2"},
		},
		{
			name:           "EXCEPT",
			query:          "SELECT * FROM t1 EXCEPT SELECT * FROM t2",
			qualified:      false,
			expectedTables: []string{"t1", "t2"},
		},
		{
			name:           "EXCEPT ALL",
			query:          "SELECT * FROM t1 EXCEPT ALL SELECT * FROM t2",
			qualified:      false,
			expectedTables: []string{"t1", "t2"},
		},
		{
			name:           "chained UNION",
			query:          "SELECT * FROM t1 UNION ALL SELECT * FROM t2 UNION SELECT * FROM t3",
			qualified:      false,
			expectedTables: []string{"t1", "t2", "t3"},
		},
		{
			name:           "UNION with qualified tables",
			query:          "SELECT * FROM schema1.users UNION SELECT * FROM schema2.admins",
			qualified:      true,
			expectedTables: []string{"schema1.users", "schema2.admins"},
		},
		{
			name:           "UNION with subquery",
			query:          "SELECT * FROM users WHERE id IN (SELECT user_id FROM active) UNION SELECT * FROM admins",
			qualified:      false,
			expectedTables: []string{"active", "admins", "users"},
		},
		// CREATE TABLE AS SELECT tests
		{
			name:           "CREATE TABLE AS SELECT simple",
			query:          "CREATE TABLE new_table AS SELECT * FROM old_table",
			qualified:      false,
			expectedTables: []string{"new_table", "old_table"},
		},
		{
			name:           "CREATE TABLE AS SELECT with columns",
			query:          "CREATE TABLE archive AS SELECT id, name FROM users WHERE deleted = true",
			qualified:      false,
			expectedTables: []string{"archive", "users"},
		},
		{
			name:           "CREATE TABLE IF NOT EXISTS AS SELECT",
			query:          "CREATE TABLE IF NOT EXISTS summary AS SELECT * FROM data",
			qualified:      false,
			expectedTables: []string{"data", "summary"},
		},
		{
			name:           "CREATE TABLE AS SELECT with UNION",
			query:          "CREATE TABLE combined AS SELECT * FROM t1 UNION SELECT * FROM t2",
			qualified:      false,
			expectedTables: []string{"combined", "t1", "t2"},
		},
		{
			name:           "CREATE TABLE AS SELECT with JOIN",
			query:          "CREATE TABLE joined AS SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
			qualified:      false,
			expectedTables: []string{"joined", "orders", "users"},
		},
		{
			name:           "CREATE TABLE AS SELECT qualified",
			query:          "CREATE TABLE myschema.new_table AS SELECT * FROM otherschema.source",
			qualified:      true,
			expectedTables: []string{"myschema.new_table", "otherschema.source"},
		},
		{
			name:           "CREATE TABLE AS SELECT with subquery",
			query:          "CREATE TABLE filtered AS SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)",
			qualified:      false,
			expectedTables: []string{"active_users", "filtered", "users"},
		},
		{
			name:           "CREATE TABLE AS SELECT with CTE",
			query:          "CREATE TABLE result AS WITH tmp AS (SELECT * FROM source) SELECT * FROM tmp",
			qualified:      false,
			expectedTables: []string{"result", "source"},
		},
		// UPDATE...FROM tests
		{
			name:           "UPDATE FROM simple",
			query:          "UPDATE users SET x = s.y FROM stats s WHERE users.id = s.id",
			qualified:      false,
			expectedTables: []string{"stats", "users"},
		},
		{
			name:           "UPDATE FROM without alias",
			query:          "UPDATE t1 SET col = t2.col FROM t2 WHERE t1.id = t2.id",
			qualified:      false,
			expectedTables: []string{"t1", "t2"},
		},
		{
			name:           "UPDATE FROM with JOIN",
			query:          "UPDATE users u SET email = n.email FROM new_emails n JOIN domains d ON n.domain_id = d.id WHERE u.id = n.user_id",
			qualified:      false,
			expectedTables: []string{"domains", "new_emails", "users"},
		},
		{
			name:           "UPDATE FROM qualified",
			query:          "UPDATE schema1.users SET x = 1 FROM schema2.data d WHERE users.id = d.id",
			qualified:      true,
			expectedTables: []string{"schema1.users", "schema2.data"},
		},
		{
			name:           "UPDATE FROM with multiple tables",
			query:          "UPDATE target SET val = a.x + b.y FROM source_a a, source_b b WHERE target.id = a.id AND a.id = b.id",
			qualified:      false,
			expectedTables: []string{"source_a", "source_b", "target"},
		},
		{
			name:           "UPDATE FROM with subquery in WHERE",
			query:          "UPDATE users SET active = true FROM premium p WHERE users.id = p.user_id AND p.tier IN (SELECT tier FROM gold_tiers)",
			qualified:      false,
			expectedTables: []string{"gold_tiers", "premium", "users"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tables, err := dukdb.GetTableNames(conn, tt.query, tt.qualified)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, tables)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, tables, "tables should not be nil")
				assert.ElementsMatch(t, tt.expectedTables, tables)
			}
		})
	}
}

func TestGetTableNames_DedupesAndSorts(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	// Query with duplicate table references
	query := "SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.parent_id"
	tables, err := dukdb.GetTableNames(conn, query, false)

	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables, "duplicate tables should be deduplicated")
}

func TestGetTableNames_ReturnsEmptySliceNotNil(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	tables, err := dukdb.GetTableNames(conn, "SELECT 1", false)

	require.NoError(t, err)
	assert.NotNil(t, tables, "should return empty slice, not nil")
	assert.Len(t, tables, 0)
}

func TestGetTableNames_ParseErrorHasPrefix(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	_, err = dukdb.GetTableNames(conn, "INVALID QUERY SYNTAX !!!", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse error")
}

func TestGetTableNames_Deterministic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	queries := []string{
		"SELECT * FROM users JOIN orders ON users.id = orders.user_id",
		"WITH tmp AS (SELECT * FROM users) SELECT * FROM tmp JOIN orders ON tmp.id = orders.user_id",
		"SELECT * FROM t1 UNION SELECT * FROM t2 UNION SELECT * FROM t3",
		"SELECT * FROM zebra, alpha, middle, beta",
		"SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id LEFT JOIN t3 ON t2.id = t3.t2_id",
		"UPDATE users SET x = s.y FROM stats s WHERE users.id = s.id",
		"CREATE TABLE new_table AS SELECT * FROM old_table JOIN other ON old_table.id = other.old_id",
	}

	for _, query := range queries {
		t.Run(query[:min(40, len(query))], func(t *testing.T) {
			// Get first result
			first, err := dukdb.GetTableNames(conn, query, false)
			require.NoError(t, err)

			// Run 100 times and verify identical
			for i := 0; i < 100; i++ {
				result, err := dukdb.GetTableNames(conn, query, false)
				require.NoError(t, err)
				assert.Equal(t, first, result, "Result should be identical on iteration %d", i)
			}
		})
	}
}

func TestGetTableNames_AlphabeticalOrder(t *testing.T) {
	// Verify tables are always sorted alphabetically
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	query := "SELECT * FROM zebra, alpha, middle, beta"
	tables, err := dukdb.GetTableNames(conn, query, false)
	require.NoError(t, err)

	// Should be in alphabetical order
	assert.Equal(t, []string{"alpha", "beta", "middle", "zebra"}, tables)
}

func TestGetTableNames_ComplexJoinPatterns(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	tests := []struct {
		name           string
		query          string
		expectedTables []string
	}{
		{
			name: "all join types in one query",
			query: `SELECT * FROM t1
                    INNER JOIN t2 ON t1.id = t2.t1_id
                    LEFT JOIN t3 ON t2.id = t3.t2_id
                    RIGHT JOIN t4 ON t3.id = t4.t3_id
                    FULL OUTER JOIN t5 ON t4.id = t5.t4_id
                    CROSS JOIN t6`,
			expectedTables: []string{"t1", "t2", "t3", "t4", "t5", "t6"},
		},
		{
			name: "LEFT OUTER JOIN explicit",
			query: `SELECT * FROM orders
                    LEFT OUTER JOIN customers ON orders.customer_id = customers.id
                    LEFT OUTER JOIN products ON orders.product_id = products.id`,
			expectedTables: []string{"customers", "orders", "products"},
		},
		{
			name:           "RIGHT OUTER JOIN explicit",
			query:          `SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.t1_id`,
			expectedTables: []string{"t1", "t2"},
		},
		// Note: NATURAL JOIN is not supported by the parser
		{
			name: "self join multiple times",
			query: `SELECT * FROM employees e1
                    JOIN employees e2 ON e1.manager_id = e2.id
                    JOIN employees e3 ON e2.manager_id = e3.id`,
			expectedTables: []string{"employees"},
		},
		{
			name:           "mixed implicit and explicit joins",
			query:          `SELECT * FROM t1, t2 JOIN t3 ON t2.id = t3.t2_id, t4 LEFT JOIN t5 ON t4.id = t5.t4_id`,
			expectedTables: []string{"t1", "t2", "t3", "t4", "t5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tables, err := dukdb.GetTableNames(conn, tt.query, false)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.expectedTables, tables)
		})
	}
}

func TestGetTableNames_SubqueriesInVariousLocations(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	tests := []struct {
		name           string
		query          string
		expectedTables []string
	}{
		{
			name:           "subquery in WHERE with IN",
			query:          "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE active = true)",
			expectedTables: []string{"customers", "orders"},
		},
		{
			name:           "subquery in WHERE with EXISTS",
			query:          "SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)",
			expectedTables: []string{"customers", "orders"},
		},
		{
			name:           "subquery in WHERE with comparison",
			query:          "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)",
			expectedTables: []string{"products"},
		},
		{
			name:           "scalar subquery in SELECT",
			query:          "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count FROM users u",
			expectedTables: []string{"orders", "users"},
		},
		{
			name: "multiple scalar subqueries in SELECT",
			query: `SELECT u.name,
                           (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count,
                           (SELECT SUM(amount) FROM payments p WHERE p.user_id = u.id) AS total_paid
                    FROM users u`,
			expectedTables: []string{"orders", "payments", "users"},
		},
		{
			name:           "subquery in HAVING",
			query:          "SELECT category_id, COUNT(*) FROM products GROUP BY category_id HAVING COUNT(*) > (SELECT AVG(cnt) FROM product_counts)",
			expectedTables: []string{"product_counts", "products"},
		},
		{
			name:           "subquery in JOIN ON clause",
			query:          "SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND o.total > (SELECT AVG(total) FROM orders)",
			expectedTables: []string{"orders", "users"},
		},
		{
			name:           "derived table (subquery in FROM)",
			query:          "SELECT * FROM (SELECT * FROM users WHERE active = true) AS active_users JOIN orders ON active_users.id = orders.user_id",
			expectedTables: []string{"orders", "users"},
		},
		{
			name: "nested derived tables",
			query: `SELECT * FROM
                    (SELECT * FROM
                        (SELECT * FROM users WHERE active = true) AS inner_users
                     WHERE inner_users.age > 18) AS outer_users
                    JOIN orders ON outer_users.id = orders.user_id`,
			expectedTables: []string{"orders", "users"},
		},
		{
			name:           "subquery with NOT IN",
			query:          "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)",
			expectedTables: []string{"banned_users", "users"},
		},
		{
			name:           "subquery with NOT EXISTS",
			query:          "SELECT * FROM products p WHERE NOT EXISTS (SELECT 1 FROM discontinued d WHERE d.product_id = p.id)",
			expectedTables: []string{"discontinued", "products"},
		},
		{
			name:           "correlated subquery",
			query:          "SELECT * FROM employees e WHERE salary > (SELECT AVG(salary) FROM employees WHERE department_id = e.department_id)",
			expectedTables: []string{"employees"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tables, err := dukdb.GetTableNames(conn, tt.query, false)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.expectedTables, tables)
		})
	}
}
