// Package tests provides integration tests and examples for dukdb-go.
package tests

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

// Example_getTableNames demonstrates basic usage of GetTableNames
// to extract table names from SQL queries.
func Example_getTableNames() {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer func() { _ = conn.Close() }()

	// Simple SELECT
	tables, _ := dukdb.GetTableNames(conn, "SELECT * FROM users", false)
	fmt.Println("Simple:", tables)

	// JOIN query
	tables, _ = dukdb.GetTableNames(conn, "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id", false)
	fmt.Println("Join:", tables)

	// CTE query
	tables, _ = dukdb.GetTableNames(conn, "WITH active AS (SELECT * FROM users WHERE active) SELECT * FROM active", false)
	fmt.Println("CTE:", tables)

	// Output:
	// Simple: [users]
	// Join: [customers orders]
	// CTE: [users]
}

// Example_getTableNames_joins demonstrates extracting tables from various JOIN types.
func Example_getTableNames_joins() {
	db, _ := sql.Open("dukdb", ":memory:")
	defer func() { _ = db.Close() }()

	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()

	// INNER JOIN
	tables, _ := dukdb.GetTableNames(conn, "SELECT * FROM a INNER JOIN b ON a.id = b.a_id", false)
	fmt.Println("INNER JOIN:", tables)

	// LEFT JOIN
	tables, _ = dukdb.GetTableNames(conn, "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id", false)
	fmt.Println("LEFT JOIN:", tables)

	// Multiple JOINs
	tables, _ = dukdb.GetTableNames(conn, "SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id", false)
	fmt.Println("Multiple JOINs:", tables)

	// Output:
	// INNER JOIN: [a b]
	// LEFT JOIN: [a b]
	// Multiple JOINs: [t1 t2 t3]
}

// Example_getTableNames_subqueries demonstrates extracting tables from queries with subqueries.
func Example_getTableNames_subqueries() {
	db, _ := sql.Open("dukdb", ":memory:")
	defer func() { _ = db.Close() }()

	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()

	// Subquery in WHERE with IN
	tables, _ := dukdb.GetTableNames(conn, "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers)", false)
	fmt.Println("WHERE IN:", tables)

	// Scalar subquery in SELECT
	tables, _ = dukdb.GetTableNames(conn, "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) FROM users u", false)
	fmt.Println("Scalar subquery:", tables)

	// Nested subqueries
	tables, _ = dukdb.GetTableNames(conn, "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2 WHERE id IN (SELECT id FROM t3))", false)
	fmt.Println("Nested subqueries:", tables)

	// Output:
	// WHERE IN: [customers orders]
	// Scalar subquery: [orders users]
	// Nested subqueries: [t1 t2 t3]
}

// Example_getTableNames_dml demonstrates extracting tables from DML statements.
func Example_getTableNames_dml() {
	db, _ := sql.Open("dukdb", ":memory:")
	defer func() { _ = db.Close() }()

	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()

	// INSERT with SELECT
	tables, _ := dukdb.GetTableNames(conn, "INSERT INTO archive SELECT * FROM users WHERE deleted = true", false)
	fmt.Println("INSERT SELECT:", tables)

	// UPDATE with FROM
	tables, _ = dukdb.GetTableNames(conn, "UPDATE users SET x = s.y FROM stats s WHERE users.id = s.id", false)
	fmt.Println("UPDATE FROM:", tables)

	// DELETE with subquery
	tables, _ = dukdb.GetTableNames(conn, "DELETE FROM users WHERE id IN (SELECT user_id FROM deleted)", false)
	fmt.Println("DELETE subquery:", tables)

	// Output:
	// INSERT SELECT: [archive users]
	// UPDATE FROM: [stats users]
	// DELETE subquery: [deleted users]
}

// Example_getTableNames_qualified demonstrates qualified vs unqualified table names.
func Example_getTableNames_qualified() {
	db, _ := sql.Open("dukdb", ":memory:")
	defer func() { _ = db.Close() }()

	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()

	// Unqualified (default)
	tables, _ := dukdb.GetTableNames(conn, "SELECT * FROM myschema.users", false)
	fmt.Println("Unqualified:", tables)

	// Qualified
	tables, _ = dukdb.GetTableNames(conn, "SELECT * FROM myschema.users", true)
	fmt.Println("Qualified:", tables)

	// Output:
	// Unqualified: [users]
	// Qualified: [myschema.users]
}

// Example_getTableNames_setOperations demonstrates extracting tables from UNION/INTERSECT/EXCEPT.
func Example_getTableNames_setOperations() {
	db, _ := sql.Open("dukdb", ":memory:")
	defer func() { _ = db.Close() }()

	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()

	// UNION
	tables, _ := dukdb.GetTableNames(conn, "SELECT * FROM users UNION SELECT * FROM admins", false)
	fmt.Println("UNION:", tables)

	// INTERSECT
	tables, _ = dukdb.GetTableNames(conn, "SELECT * FROM t1 INTERSECT SELECT * FROM t2", false)
	fmt.Println("INTERSECT:", tables)

	// Chained UNION
	tables, _ = dukdb.GetTableNames(conn, "SELECT * FROM a UNION SELECT * FROM b UNION SELECT * FROM c", false)
	fmt.Println("Chained UNION:", tables)

	// Output:
	// UNION: [admins users]
	// INTERSECT: [t1 t2]
	// Chained UNION: [a b c]
}
