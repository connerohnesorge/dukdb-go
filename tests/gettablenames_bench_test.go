package tests

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"

	// Import engine to register the backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func BenchmarkGetTableNames_SimpleSelect(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get conn: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	query := "SELECT * FROM users"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dukdb.GetTableNames(conn, query, false)
	}
}

func BenchmarkGetTableNames_ComplexJoin(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get conn: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	// 10 tables joined
	query := `SELECT * FROM t1
              JOIN t2 ON t1.id = t2.t1_id
              JOIN t3 ON t2.id = t3.t2_id
              JOIN t4 ON t3.id = t4.t3_id
              JOIN t5 ON t4.id = t5.t4_id
              JOIN t6 ON t5.id = t6.t5_id
              JOIN t7 ON t6.id = t7.t6_id
              JOIN t8 ON t7.id = t8.t7_id
              JOIN t9 ON t8.id = t9.t8_id
              JOIN t10 ON t9.id = t10.t9_id`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dukdb.GetTableNames(conn, query, false)
	}
}

func BenchmarkGetTableNames_NestedSubqueries(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get conn: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	// 5 levels deep
	query := `SELECT * FROM t1 WHERE id IN (
        SELECT id FROM t2 WHERE id IN (
            SELECT id FROM t3 WHERE id IN (
                SELECT id FROM t4 WHERE id IN (
                    SELECT id FROM t5
                )
            )
        )
    )`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dukdb.GetTableNames(conn, query, false)
	}
}

func BenchmarkGetTableNames_CTE(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get conn: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	query := `WITH cte1 AS (SELECT * FROM users),
                   cte2 AS (SELECT * FROM orders)
              SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.user_id`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dukdb.GetTableNames(conn, query, false)
	}
}

func BenchmarkGetTableNames_Union(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get conn: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	query := `SELECT * FROM t1 UNION SELECT * FROM t2 UNION SELECT * FROM t3`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dukdb.GetTableNames(conn, query, false)
	}
}

func BenchmarkGetTableNames_QualifiedNames(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get conn: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	query := `SELECT * FROM schema1.users u JOIN schema2.orders o ON u.id = o.user_id`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dukdb.GetTableNames(conn, query, true)
	}
}

func BenchmarkGetTableNames_MixedJoinTypes(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get conn: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	query := `SELECT * FROM t1
              INNER JOIN t2 ON t1.id = t2.t1_id
              LEFT JOIN t3 ON t2.id = t3.t2_id
              RIGHT JOIN t4 ON t3.id = t4.t3_id
              FULL OUTER JOIN t5 ON t4.id = t5.t4_id
              CROSS JOIN t6`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dukdb.GetTableNames(conn, query, false)
	}
}

func BenchmarkGetTableNames_UpdateFrom(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get conn: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	query := `UPDATE users u SET email = n.email FROM new_emails n JOIN domains d ON n.domain_id = d.id WHERE u.id = n.user_id`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dukdb.GetTableNames(conn, query, false)
	}
}

func BenchmarkGetTableNames_CreateTableAsSelect(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get conn: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	query := `CREATE TABLE result AS WITH tmp AS (SELECT * FROM source) SELECT * FROM tmp JOIN other ON tmp.id = other.src_id`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dukdb.GetTableNames(conn, query, false)
	}
}

func BenchmarkGetTableNames_LargeQuery(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get conn: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	// Build a query with 100 tables
	var query strings.Builder
	query.WriteString("SELECT * FROM t1")
	for i := 2; i <= 100; i++ {
		query.WriteString(fmt.Sprintf(" JOIN t%d ON t%d.id = t%d.fk", i, i-1, i))
	}
	largeQuery := query.String()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dukdb.GetTableNames(conn, largeQuery, false)
	}
}
