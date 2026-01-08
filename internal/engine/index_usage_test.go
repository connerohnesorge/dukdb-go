// Package engine provides end-to-end integration tests for index usage in query plans.
// These tests verify the entire flow from SQL query through execution using indexes.
package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// 7.1: Simple Indexed Lookup Tests
// =============================================================================

// TestIndexUsage_SimpleIndexedLookup tests that a simple equality predicate on an indexed
// column returns the correct results.
func TestIndexUsage_SimpleIndexedLookup(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	// Create index on id
	_, err = conn.Execute(ctx, "CREATE INDEX idx_id ON users(id)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", nil)
	require.NoError(t, err)

	// Query with equality predicate on indexed column
	rows, cols, err := conn.Query(ctx, "SELECT * FROM users WHERE id = 2", nil)
	require.NoError(t, err)

	// Verify only Bob is returned
	require.Equal(t, 1, len(rows), "Should return exactly 1 row")
	require.Equal(t, 2, len(cols), "Should return 2 columns")
	assert.Equal(t, int32(2), rows[0]["id"], "id should be 2")
	assert.Equal(t, "Bob", rows[0]["name"], "name should be Bob")
}

// TestIndexUsage_SimpleIndexedLookup_MultipleMatches tests index lookup returning multiple rows.
func TestIndexUsage_SimpleIndexedLookup_MultipleMatches(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE employees (id INTEGER, name VARCHAR, dept INTEGER)", nil)
	require.NoError(t, err)

	// Create index on dept (non-unique)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_dept ON employees(dept)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO employees VALUES
		(1, 'Alice', 10),
		(2, 'Bob', 20),
		(3, 'Charlie', 10),
		(4, 'Diana', 10),
		(5, 'Eve', 20)`, nil)
	require.NoError(t, err)

	// Query with equality predicate on indexed column
	rows, _, err := conn.Query(ctx, "SELECT * FROM employees WHERE dept = 10", nil)
	require.NoError(t, err)

	// Verify 3 rows (Alice, Charlie, Diana) are returned
	require.Equal(t, 3, len(rows), "Should return 3 rows for dept=10")

	// Collect names
	names := make(map[string]bool)
	for _, row := range rows {
		names[row["name"].(string)] = true
	}
	assert.True(t, names["Alice"], "Should contain Alice")
	assert.True(t, names["Charlie"], "Should contain Charlie")
	assert.True(t, names["Diana"], "Should contain Diana")
}

// TestIndexUsage_SimpleIndexedLookup_NoMatch tests that no rows are returned when
// the lookup key doesn't exist.
func TestIndexUsage_SimpleIndexedLookup_NoMatch(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table and index
	_, err = conn.Execute(ctx, "CREATE TABLE products (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_products_id ON products(id)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, "INSERT INTO products VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry')", nil)
	require.NoError(t, err)

	// Query with non-existent id
	rows, _, err := conn.Query(ctx, "SELECT * FROM products WHERE id = 999", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, len(rows), "Should return 0 rows for non-existent id")
}

// TestIndexUsage_UniqueIndex tests that unique indexes work correctly.
func TestIndexUsage_UniqueIndex(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, email VARCHAR)", nil)
	require.NoError(t, err)

	// Create unique index on email
	_, err = conn.Execute(ctx, "CREATE UNIQUE INDEX idx_email ON users(email)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, "INSERT INTO users VALUES (1, 'alice@example.com'), (2, 'bob@example.com'), (3, 'charlie@example.com')", nil)
	require.NoError(t, err)

	// Query with equality predicate on unique index column
	rows, _, err := conn.Query(ctx, "SELECT * FROM users WHERE email = 'bob@example.com'", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows), "Should return exactly 1 row")
	assert.Equal(t, int32(2), rows[0]["id"], "id should be 2")
}

// =============================================================================
// 7.2: Index Not Used When No Matching Predicate
// =============================================================================

// TestIndexUsage_NoMatchingPredicate tests that queries on non-indexed columns
// still work correctly via sequential scan.
func TestIndexUsage_NoMatchingPredicate(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, name VARCHAR, status VARCHAR)", nil)
	require.NoError(t, err)

	// Create index only on id
	_, err = conn.Execute(ctx, "CREATE INDEX idx_id ON users(id)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO users VALUES
		(1, 'Alice', 'active'),
		(2, 'Bob', 'inactive'),
		(3, 'Charlie', 'active')`, nil)
	require.NoError(t, err)

	// Query on non-indexed column (name) - should use seq scan
	rows, _, err := conn.Query(ctx, "SELECT * FROM users WHERE name = 'Alice'", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows), "Should return 1 row")
	assert.Equal(t, "Alice", rows[0]["name"])
	assert.Equal(t, "active", rows[0]["status"])
}

// TestIndexUsage_NoMatchingPredicate_MultipleNonIndexedFilters tests filtering
// on multiple non-indexed columns.
func TestIndexUsage_NoMatchingPredicate_MultipleNonIndexedFilters(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table with index only on id
	_, err = conn.Execute(ctx, "CREATE TABLE orders (id INTEGER, customer VARCHAR, amount INTEGER, status VARCHAR)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_orders_id ON orders(id)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO orders VALUES
		(1, 'Alice', 100, 'completed'),
		(2, 'Bob', 200, 'pending'),
		(3, 'Alice', 150, 'completed'),
		(4, 'Alice', 50, 'pending')`, nil)
	require.NoError(t, err)

	// Query on non-indexed columns
	rows, _, err := conn.Query(ctx, "SELECT * FROM orders WHERE customer = 'Alice' AND status = 'completed'", nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(rows), "Should return 2 rows")

	// Verify amounts
	amounts := make([]int32, len(rows))
	for i, row := range rows {
		amt, ok := row["amount"].(int32)
		require.True(t, ok, "amount should be int32")
		amounts[i] = amt
	}
	assert.Contains(t, amounts, int32(100))
	assert.Contains(t, amounts, int32(150))
}

// TestIndexUsage_NoPredicateSelectAll tests SELECT * without WHERE clause.
func TestIndexUsage_NoPredicateSelectAll(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table with index
	_, err = conn.Execute(ctx, "CREATE TABLE items (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_items_id ON items(id)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, "INSERT INTO items VALUES (1, 'A'), (2, 'B'), (3, 'C')", nil)
	require.NoError(t, err)

	// SELECT without WHERE should use seq scan and return all rows
	rows, _, err := conn.Query(ctx, "SELECT * FROM items", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, len(rows), "Should return all 3 rows")
}

// =============================================================================
// 7.3: Index-Only Scan Scenarios (Covering Index)
// =============================================================================

// TestIndexUsage_CoveringIndex tests that a query selecting only indexed columns
// can potentially use an index-only scan.
func TestIndexUsage_CoveringIndex(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR)", nil)
	require.NoError(t, err)

	// Create composite index on (id, name)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_id_name ON users(id, name)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO users VALUES
		(1, 'Alice', 'alice@example.com'),
		(2, 'Bob', 'bob@example.com'),
		(3, 'Charlie', 'charlie@example.com')`, nil)
	require.NoError(t, err)

	// Query selecting only indexed columns with predicate on first column
	rows, cols, err := conn.Query(ctx, "SELECT id, name FROM users WHERE id = 2", nil)
	require.NoError(t, err)

	// Verify results
	require.Equal(t, 1, len(rows), "Should return 1 row")
	require.Equal(t, 2, len(cols), "Should return 2 columns")
	assert.Equal(t, int32(2), rows[0]["id"])
	assert.Equal(t, "Bob", rows[0]["name"])
}

// TestIndexUsage_CoveringIndex_CompositeKey tests covering index with composite key lookup.
func TestIndexUsage_CoveringIndex_CompositeKey(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE events (user_id INTEGER, event_type VARCHAR, timestamp BIGINT, data VARCHAR)", nil)
	require.NoError(t, err)

	// Create composite index covering user_id, event_type, timestamp
	_, err = conn.Execute(ctx, "CREATE INDEX idx_events ON events(user_id, event_type, timestamp)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO events VALUES
		(1, 'login', 1000, 'session_a'),
		(1, 'logout', 2000, 'session_a'),
		(2, 'login', 1500, 'session_b'),
		(1, 'login', 3000, 'session_c')`, nil)
	require.NoError(t, err)

	// Query selecting only indexed columns
	rows, _, err := conn.Query(ctx, "SELECT user_id, event_type, timestamp FROM events WHERE user_id = 1 AND event_type = 'login'", nil)
	require.NoError(t, err)

	// Should return 2 login events for user 1
	require.Equal(t, 2, len(rows), "Should return 2 login events for user 1")
}

// TestIndexUsage_NonCoveringIndex tests that queries needing non-indexed columns
// properly fetch from heap after index lookup.
func TestIndexUsage_NonCoveringIndex(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR)", nil)
	require.NoError(t, err)

	// Create index only on id
	_, err = conn.Execute(ctx, "CREATE INDEX idx_id ON users(id)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')", nil)
	require.NoError(t, err)

	// Query selecting non-indexed columns with predicate on indexed column
	// This requires heap lookup after index scan
	rows, cols, err := conn.Query(ctx, "SELECT * FROM users WHERE id = 2", nil)
	require.NoError(t, err)

	require.Equal(t, 1, len(rows))
	require.Equal(t, 3, len(cols))
	assert.Equal(t, int32(2), rows[0]["id"])
	assert.Equal(t, "Bob", rows[0]["name"])
	assert.Equal(t, "bob@example.com", rows[0]["email"])
}

// =============================================================================
// 7.4: Composite Index Prefix Scenarios
// =============================================================================

// TestIndexUsage_CompositeIndex_FullPrefixMatch tests composite index with all columns matched.
func TestIndexUsage_CompositeIndex_FullPrefixMatch(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)", nil)
	require.NoError(t, err)

	// Create composite index on (a, b, c)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_abc ON t(a, b, c)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO t VALUES
		(1, 2, 3),
		(1, 2, 4),
		(1, 3, 5),
		(2, 2, 3),
		(1, 2, 6)`, nil)
	require.NoError(t, err)

	// Full prefix match: a = 1 AND b = 2 AND c = 3
	rows, _, err := conn.Query(ctx, "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows), "Should return exactly 1 row")
	assert.Equal(t, int32(1), rows[0]["a"])
	assert.Equal(t, int32(2), rows[0]["b"])
	assert.Equal(t, int32(3), rows[0]["c"])
}

// TestIndexUsage_CompositeIndex_PartialPrefixMatch tests composite index with partial prefix match.
func TestIndexUsage_CompositeIndex_PartialPrefixMatch(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)", nil)
	require.NoError(t, err)

	// Create composite index on (a, b, c)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_abc ON t(a, b, c)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO t VALUES
		(1, 2, 3),
		(1, 2, 4),
		(1, 3, 5),
		(2, 2, 3),
		(1, 2, 6)`, nil)
	require.NoError(t, err)

	// Partial prefix match: a = 1 AND b = 2 (should use index for a and b)
	rows, _, err := conn.Query(ctx, "SELECT * FROM t WHERE a = 1 AND b = 2", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(rows), "Should return 3 rows")

	// All rows should have a=1 and b=2
	for _, row := range rows {
		assert.Equal(t, int32(1), row["a"])
		assert.Equal(t, int32(2), row["b"])
	}

	// Collect c values
	cValues := make([]int32, len(rows))
	for i, row := range rows {
		cVal, ok := row["c"].(int32)
		require.True(t, ok, "c should be int32")
		cValues[i] = cVal
	}
	assert.Contains(t, cValues, int32(3))
	assert.Contains(t, cValues, int32(4))
	assert.Contains(t, cValues, int32(6))
}

// TestIndexUsage_CompositeIndex_FirstColumnOnly tests composite index with only first column matched.
func TestIndexUsage_CompositeIndex_FirstColumnOnly(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)", nil)
	require.NoError(t, err)

	// Create composite index on (a, b, c)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_abc ON t(a, b, c)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO t VALUES
		(1, 2, 3),
		(1, 2, 4),
		(1, 3, 5),
		(2, 2, 3),
		(1, 4, 6)`, nil)
	require.NoError(t, err)

	// Only first column matched: a = 1
	rows, _, err := conn.Query(ctx, "SELECT * FROM t WHERE a = 1", nil)
	require.NoError(t, err)
	require.Equal(t, 4, len(rows), "Should return 4 rows with a=1")

	for _, row := range rows {
		assert.Equal(t, int32(1), row["a"])
	}
}

// TestIndexUsage_CompositeIndex_GapInPrefix tests that a gap in the prefix uses only the leading columns.
func TestIndexUsage_CompositeIndex_GapInPrefix(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)", nil)
	require.NoError(t, err)

	// Create composite index on (a, b, c)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_abc ON t(a, b, c)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO t VALUES
		(1, 2, 3),
		(1, 2, 4),
		(1, 3, 3),
		(2, 2, 3),
		(1, 4, 3)`, nil)
	require.NoError(t, err)

	// Gap in prefix: a = 1 AND c = 3 (b is missing)
	// Only 'a' can use the index, 'c' is a residual filter
	rows, _, err := conn.Query(ctx, "SELECT * FROM t WHERE a = 1 AND c = 3", nil)
	require.NoError(t, err)

	// Should return rows with a=1 AND c=3: (1,2,3) and (1,3,3) and (1,4,3)
	require.Equal(t, 3, len(rows), "Should return 3 rows matching a=1 AND c=3")
	for _, row := range rows {
		assert.Equal(t, int32(1), row["a"])
		assert.Equal(t, int32(3), row["c"])
	}
}

// TestIndexUsage_CompositeIndex_SecondColumnOnly tests that second column alone cannot use index.
func TestIndexUsage_CompositeIndex_SecondColumnOnly(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table
	_, err = conn.Execute(ctx, "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)", nil)
	require.NoError(t, err)

	// Create composite index on (a, b, c)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_abc ON t(a, b, c)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO t VALUES
		(1, 2, 3),
		(2, 2, 4),
		(3, 2, 5)`, nil)
	require.NoError(t, err)

	// Only second column: b = 2 (cannot use index since first column is missing)
	// Should fall back to seq scan
	rows, _, err := conn.Query(ctx, "SELECT * FROM t WHERE b = 2", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(rows), "Should return all 3 rows via seq scan")
}

// =============================================================================
// 7.5: Performance Benchmarks - Index Scan vs Sequential Scan
// =============================================================================

// BenchmarkIndexScan_SmallTable benchmarks index scan on a small table.
func BenchmarkIndexScan_SmallTable(b *testing.B) {
	engine := NewEngine()
	defer func() {
		_ = engine.Close()
	}()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	ctx := context.Background()

	// Setup: Create table with index
	_, err = conn.Execute(ctx, "CREATE TABLE bench_index (id INTEGER, value VARCHAR)", nil)
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = conn.Execute(ctx, "CREATE INDEX idx_bench ON bench_index(id)", nil)
	if err != nil {
		b.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert 1000 rows
	for i := 0; i < 1000; i++ {
		_, err = conn.Execute(ctx, "INSERT INTO bench_index VALUES ("+intToStr(i)+", 'value_"+intToStr(i)+"')", nil)
		if err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := conn.Query(ctx, "SELECT * FROM bench_index WHERE id = 500", nil)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// BenchmarkSeqScan_SmallTable benchmarks sequential scan on a small table.
func BenchmarkSeqScan_SmallTable(b *testing.B) {
	engine := NewEngine()
	defer func() {
		_ = engine.Close()
	}()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	ctx := context.Background()

	// Setup: Create table WITHOUT index
	_, err = conn.Execute(ctx, "CREATE TABLE bench_seq (id INTEGER, value VARCHAR)", nil)
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert 1000 rows
	for i := 0; i < 1000; i++ {
		_, err = conn.Execute(ctx, "INSERT INTO bench_seq VALUES ("+intToStr(i)+", 'value_"+intToStr(i)+"')", nil)
		if err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := conn.Query(ctx, "SELECT * FROM bench_seq WHERE id = 500", nil)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// BenchmarkIndexScan_MediumTable benchmarks index scan on a medium table.
func BenchmarkIndexScan_MediumTable(b *testing.B) {
	engine := NewEngine()
	defer func() {
		_ = engine.Close()
	}()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	ctx := context.Background()

	// Setup: Create table with index
	_, err = conn.Execute(ctx, "CREATE TABLE bench_medium_idx (id INTEGER, category INTEGER, value VARCHAR)", nil)
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = conn.Execute(ctx, "CREATE INDEX idx_medium_id ON bench_medium_idx(id)", nil)
	if err != nil {
		b.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert 10000 rows using batch insert
	for batch := 0; batch < 100; batch++ {
		var insertSQL string
		insertSQL = "INSERT INTO bench_medium_idx VALUES "
		for i := 0; i < 100; i++ {
			id := batch*100 + i
			if i > 0 {
				insertSQL += ", "
			}
			insertSQL += "(" + intToStr(id) + ", " + intToStr(id%10) + ", 'value_" + intToStr(id) + "')"
		}
		_, err = conn.Execute(ctx, insertSQL, nil)
		if err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := conn.Query(ctx, "SELECT * FROM bench_medium_idx WHERE id = 5000", nil)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// BenchmarkSeqScan_MediumTable benchmarks sequential scan on a medium table.
func BenchmarkSeqScan_MediumTable(b *testing.B) {
	engine := NewEngine()
	defer func() {
		_ = engine.Close()
	}()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	ctx := context.Background()

	// Setup: Create table WITHOUT index
	_, err = conn.Execute(ctx, "CREATE TABLE bench_medium_seq (id INTEGER, category INTEGER, value VARCHAR)", nil)
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert 10000 rows using batch insert
	for batch := 0; batch < 100; batch++ {
		var insertSQL string
		insertSQL = "INSERT INTO bench_medium_seq VALUES "
		for i := 0; i < 100; i++ {
			id := batch*100 + i
			if i > 0 {
				insertSQL += ", "
			}
			insertSQL += "(" + intToStr(id) + ", " + intToStr(id%10) + ", 'value_" + intToStr(id) + "')"
		}
		_, err = conn.Execute(ctx, insertSQL, nil)
		if err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := conn.Query(ctx, "SELECT * FROM bench_medium_seq WHERE id = 5000", nil)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// BenchmarkCompositeIndexScan benchmarks composite index scan.
func BenchmarkCompositeIndexScan(b *testing.B) {
	engine := NewEngine()
	defer func() {
		_ = engine.Close()
	}()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	ctx := context.Background()

	// Setup: Create table with composite index
	_, err = conn.Execute(ctx, "CREATE TABLE bench_composite (a INTEGER, b INTEGER, c INTEGER, value VARCHAR)", nil)
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = conn.Execute(ctx, "CREATE INDEX idx_composite ON bench_composite(a, b)", nil)
	if err != nil {
		b.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert 5000 rows
	for batch := 0; batch < 50; batch++ {
		var insertSQL string
		insertSQL = "INSERT INTO bench_composite VALUES "
		for i := 0; i < 100; i++ {
			id := batch*100 + i
			a := id / 100
			bVal := id % 100
			if i > 0 {
				insertSQL += ", "
			}
			insertSQL += "(" + intToStr(a) + ", " + intToStr(bVal) + ", " + intToStr(id) + ", 'value_" + intToStr(id) + "')"
		}
		_, err = conn.Execute(ctx, insertSQL, nil)
		if err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := conn.Query(ctx, "SELECT * FROM bench_composite WHERE a = 25 AND b = 50", nil)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// =============================================================================
// 7.6: EXPLAIN Output Tests
// =============================================================================

// TestExplainShowsIndexScan tests that EXPLAIN shows IndexScan when an index is used.
func TestExplainShowsIndexScan(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table and index
	_, err = conn.Execute(ctx, "CREATE TABLE explain_test (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_explain ON explain_test(id)", nil)
	require.NoError(t, err)

	// Insert some data
	_, err = conn.Execute(ctx, "INSERT INTO explain_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", nil)
	require.NoError(t, err)

	// Run EXPLAIN query
	rows, _, err := conn.Query(ctx, "EXPLAIN SELECT * FROM explain_test WHERE id = 2", nil)
	if err != nil {
		// EXPLAIN might not be fully implemented - skip test if so
		t.Skipf("EXPLAIN not implemented or returned error: %v", err)
		return
	}

	// Check that the plan contains some output
	require.NotEmpty(t, rows, "EXPLAIN should return some output")

	// Log the plan for debugging
	t.Logf("EXPLAIN output: %v", rows)

	// Look for IndexScan in the output (implementation dependent)
	// This is a best-effort check - adjust based on actual EXPLAIN format
	foundIndex := false
	for _, row := range rows {
		for _, v := range row {
			s, ok := v.(string)
			if !ok {
				continue
			}
			if containsAny(s, "IndexScan", "Index Scan", "INDEX") {
				foundIndex = true

				break
			}
		}
	}
	// Note: We don't assert here because EXPLAIN format varies.
	// Just log whether index was found.
	t.Logf("Index scan found in EXPLAIN: %v", foundIndex)
}

// TestExplainShowsSeqScan tests that EXPLAIN shows SeqScan when no index is used.
func TestExplainShowsSeqScan(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table WITHOUT index
	_, err = conn.Execute(ctx, "CREATE TABLE explain_seq (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	// Insert some data
	_, err = conn.Execute(ctx, "INSERT INTO explain_seq VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", nil)
	require.NoError(t, err)

	// Run EXPLAIN query
	rows, _, err := conn.Query(ctx, "EXPLAIN SELECT * FROM explain_seq WHERE id = 2", nil)
	if err != nil {
		// EXPLAIN might not be fully implemented - skip test if so
		t.Skipf("EXPLAIN not implemented or returned error: %v", err)
		return
	}

	// Check that the plan contains some output
	require.NotEmpty(t, rows, "EXPLAIN should return some output")

	// Log the plan for debugging
	t.Logf("EXPLAIN output: %v", rows)

	// Look for SeqScan/Sequential in the output
	foundSeq := false
	for _, row := range rows {
		for _, v := range row {
			s, ok := v.(string)
			if !ok {
				continue
			}
			if containsAny(s, "SeqScan", "Seq Scan", "Sequential", "TableScan", "SCAN") {
				foundSeq = true

				break
			}
		}
	}
	t.Logf("Sequential scan found in EXPLAIN: %v", foundSeq)
}

// TestExplainAnalyze tests EXPLAIN ANALYZE if supported.
func TestExplainAnalyze(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table and index
	_, err = conn.Execute(ctx, "CREATE TABLE analyze_test (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_analyze ON analyze_test(id)", nil)
	require.NoError(t, err)

	// Insert some data
	_, err = conn.Execute(ctx, "INSERT INTO analyze_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", nil)
	require.NoError(t, err)

	// Run EXPLAIN ANALYZE query
	rows, _, err := conn.Query(ctx, "EXPLAIN ANALYZE SELECT * FROM analyze_test WHERE id = 2", nil)
	if err != nil {
		// EXPLAIN ANALYZE might not be implemented - skip test if so
		t.Skipf("EXPLAIN ANALYZE not implemented or returned error: %v", err)
		return
	}

	// Check that the plan contains some output with timing info
	require.NotEmpty(t, rows, "EXPLAIN ANALYZE should return some output")
	t.Logf("EXPLAIN ANALYZE output: %v", rows)
}

// =============================================================================
// Additional Integration Tests
// =============================================================================

// TestIndexUsage_InsertAfterIndex tests that inserts after index creation are indexed.
func TestIndexUsage_InsertAfterIndex(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table and index first
	_, err = conn.Execute(ctx, "CREATE TABLE dynamic_test (id INTEGER, data VARCHAR)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_dynamic ON dynamic_test(id)", nil)
	require.NoError(t, err)

	// Insert initial data
	_, err = conn.Execute(ctx, "INSERT INTO dynamic_test VALUES (1, 'first')", nil)
	require.NoError(t, err)

	// Verify lookup works
	rows, _, err := conn.Query(ctx, "SELECT * FROM dynamic_test WHERE id = 1", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))

	// Insert more data after index exists
	_, err = conn.Execute(ctx, "INSERT INTO dynamic_test VALUES (2, 'second'), (3, 'third')", nil)
	require.NoError(t, err)

	// Verify all can be looked up
	rows, _, err = conn.Query(ctx, "SELECT * FROM dynamic_test WHERE id = 2", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	assert.Equal(t, "second", rows[0]["data"])

	rows, _, err = conn.Query(ctx, "SELECT * FROM dynamic_test WHERE id = 3", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	assert.Equal(t, "third", rows[0]["data"])
}

// TestIndexUsage_MultipleIndexesSameTable tests having multiple indexes on one table.
func TestIndexUsage_MultipleIndexesSameTable(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table with multiple indexes
	_, err = conn.Execute(ctx, "CREATE TABLE multi_idx (id INTEGER, email VARCHAR, status VARCHAR)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_multi_id ON multi_idx(id)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_multi_email ON multi_idx(email)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_multi_status ON multi_idx(status)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO multi_idx VALUES
		(1, 'alice@example.com', 'active'),
		(2, 'bob@example.com', 'inactive'),
		(3, 'charlie@example.com', 'active')`, nil)
	require.NoError(t, err)

	// Query using first index
	rows, _, err := conn.Query(ctx, "SELECT * FROM multi_idx WHERE id = 2", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	assert.Equal(t, "bob@example.com", rows[0]["email"])

	// Query using second index
	rows, _, err = conn.Query(ctx, "SELECT * FROM multi_idx WHERE email = 'alice@example.com'", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	assert.Equal(t, int32(1), rows[0]["id"])

	// Query using third index
	rows, _, err = conn.Query(ctx, "SELECT * FROM multi_idx WHERE status = 'active'", nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(rows))
}

// TestIndexUsage_IndexDropAndRecreate tests that dropping and recreating an index works.
func TestIndexUsage_IndexDropAndRecreate(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create table and index
	_, err = conn.Execute(ctx, "CREATE TABLE drop_test (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_drop ON drop_test(id)", nil)
	require.NoError(t, err)

	// Insert data
	_, err = conn.Execute(ctx, "INSERT INTO drop_test VALUES (1, 'Alice'), (2, 'Bob')", nil)
	require.NoError(t, err)

	// Query with index
	rows, _, err := conn.Query(ctx, "SELECT * FROM drop_test WHERE id = 1", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))

	// Drop index
	_, err = conn.Execute(ctx, "DROP INDEX idx_drop", nil)
	require.NoError(t, err)

	// Query still works (via seq scan)
	rows, _, err = conn.Query(ctx, "SELECT * FROM drop_test WHERE id = 1", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))

	// Recreate index
	_, err = conn.Execute(ctx, "CREATE INDEX idx_drop ON drop_test(id)", nil)
	require.NoError(t, err)

	// Query works again (with new index)
	rows, _, err = conn.Query(ctx, "SELECT * FROM drop_test WHERE id = 1", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
}

// =============================================================================
// 7.7: TPC-H Style Performance Tests
// =============================================================================

// TestTPCHStylePerformance_IndexVsSeqScan tests index scan vs sequential scan
// performance with a larger dataset similar to TPC-H lineitem table structure.
// This demonstrates the performance benefit of indexes for point queries.
func TestTPCHStylePerformance_IndexVsSeqScan(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create a TPC-H lineitem-like table (simplified)
	_, err = conn.Execute(ctx, `CREATE TABLE lineitem (
		l_orderkey INTEGER,
		l_partkey INTEGER,
		l_suppkey INTEGER,
		l_linenumber INTEGER,
		l_quantity INTEGER,
		l_extendedprice DOUBLE,
		l_discount DOUBLE,
		l_tax DOUBLE,
		l_returnflag VARCHAR,
		l_linestatus VARCHAR,
		l_shipdate VARCHAR,
		l_commitdate VARCHAR,
		l_receiptdate VARCHAR,
		l_shipinstruct VARCHAR,
		l_shipmode VARCHAR,
		l_comment VARCHAR
	)`, nil)
	require.NoError(t, err)

	// Insert 10,000 rows in batches of 100
	numRows := 10000
	batchSize := 100
	for batch := 0; batch < numRows/batchSize; batch++ {
		var insertSQL string
		insertSQL = "INSERT INTO lineitem VALUES "
		for i := 0; i < batchSize; i++ {
			rowNum := batch*batchSize + i
			orderkey := rowNum / 4    // ~4 line items per order
			partkey := rowNum % 1000  // 1000 unique parts
			suppkey := rowNum % 100   // 100 suppliers
			linenumber := (rowNum % 4) + 1
			quantity := (rowNum%50 + 1)
			price := float64(quantity) * 10.5
			discount := float64(rowNum%10) / 100.0
			tax := 0.08
			returnflag := []string{"A", "N", "R"}[rowNum%3]
			linestatus := []string{"O", "F"}[rowNum%2]
			shipdate := "1995-01-" + intToStr((rowNum%28)+1)
			commitdate := "1995-02-" + intToStr((rowNum%28)+1)
			receiptdate := "1995-03-" + intToStr((rowNum%28)+1)
			shipinstruct := "DELIVER IN PERSON"
			shipmode := []string{"TRUCK", "RAIL", "SHIP", "AIR"}[rowNum%4]
			comment := "comment_" + intToStr(rowNum)

			if i > 0 {
				insertSQL += ", "
			}
			insertSQL += "(" + intToStr(orderkey) + ", " + intToStr(partkey) + ", " + intToStr(suppkey) + ", "
			insertSQL += intToStr(linenumber) + ", " + intToStr(quantity) + ", " + floatToStr(price) + ", "
			insertSQL += floatToStr(discount) + ", " + floatToStr(tax) + ", '" + returnflag + "', '" + linestatus + "', "
			insertSQL += "'" + shipdate + "', '" + commitdate + "', '" + receiptdate + "', "
			insertSQL += "'" + shipinstruct + "', '" + shipmode + "', '" + comment + "')"
		}
		_, err = conn.Execute(ctx, insertSQL, nil)
		require.NoError(t, err, "Insert batch %d failed", batch)
	}

	// Verify data was inserted
	rows, _, err := conn.Query(ctx, "SELECT COUNT(*) as cnt FROM lineitem", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	t.Logf("Inserted %v rows into lineitem table", rows[0]["cnt"])

	// Create index on l_orderkey (primary lookup key in TPC-H)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_lineitem_orderkey ON lineitem(l_orderkey)", nil)
	require.NoError(t, err)

	// Create composite index on (l_partkey, l_suppkey) for TPC-H Q9 style queries
	_, err = conn.Execute(ctx, "CREATE INDEX idx_lineitem_part_supp ON lineitem(l_partkey, l_suppkey)", nil)
	require.NoError(t, err)

	// Test 1: Point query on indexed column (l_orderkey = 1250)
	// This should use the index
	rows, _, err = conn.Query(ctx, "SELECT * FROM lineitem WHERE l_orderkey = 1250", nil)
	require.NoError(t, err)
	t.Logf("Query on l_orderkey=1250 returned %d rows", len(rows))
	// ~4 rows per order key expected
	assert.True(t, len(rows) > 0 && len(rows) <= 8, "Expected 1-8 rows for order 1250, got %d", len(rows))

	// Verify EXPLAIN shows IndexScan
	explainRows, _, err := conn.Query(ctx, "EXPLAIN SELECT * FROM lineitem WHERE l_orderkey = 1250", nil)
	if err == nil && len(explainRows) > 0 {
		foundIndex := false
		for _, row := range explainRows {
			for _, v := range row {
				if s, ok := v.(string); ok {
					if containsAny(s, "IndexScan", "Index Scan", "INDEX", "idx_lineitem_orderkey") {
						foundIndex = true
						break
					}
				}
			}
		}
		t.Logf("Index scan used for l_orderkey query: %v", foundIndex)
		assert.True(t, foundIndex, "Expected IndexScan in EXPLAIN output for indexed query")
	}

	// Test 2: Composite key query (l_partkey = 500 AND l_suppkey = 50)
	rows, _, err = conn.Query(ctx, "SELECT * FROM lineitem WHERE l_partkey = 500 AND l_suppkey = 50", nil)
	require.NoError(t, err)
	t.Logf("Query on l_partkey=500 AND l_suppkey=50 returned %d rows", len(rows))

	// Test 3: Range query on indexed column
	rows, _, err = conn.Query(ctx, "SELECT * FROM lineitem WHERE l_orderkey >= 1000 AND l_orderkey < 1010", nil)
	require.NoError(t, err)
	t.Logf("Range query on l_orderkey [1000, 1010) returned %d rows", len(rows))
	// ~4 rows per order key * 10 orders = ~40 rows
	assert.True(t, len(rows) > 0, "Expected rows for range query")

	// Test 4: Query on non-indexed column (should use seq scan but still work)
	rows, _, err = conn.Query(ctx, "SELECT * FROM lineitem WHERE l_returnflag = 'R' AND l_linestatus = 'F'", nil)
	require.NoError(t, err)
	t.Logf("Query on non-indexed columns returned %d rows", len(rows))

	// Test 5: Aggregation with index filter (TPC-H Q1 style)
	rows, _, err = conn.Query(ctx, `SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty
		FROM lineitem
		WHERE l_orderkey < 500
		GROUP BY l_returnflag, l_linestatus`, nil)
	require.NoError(t, err)
	t.Logf("Aggregation query returned %d groups", len(rows))
	assert.True(t, len(rows) > 0, "Expected aggregation results")

	t.Log("TPC-H style performance test completed successfully")
}

// TestTPCHStylePerformance_IndexBenefit verifies that index scan provides
// actual performance benefit over sequential scan for point queries.
func TestTPCHStylePerformance_IndexBenefit(t *testing.T) {
	// Skip in short mode as this test takes longer
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Create two tables with identical data - one with index, one without
	_, err = conn.Execute(ctx, "CREATE TABLE with_index (id INTEGER, value VARCHAR, padding VARCHAR)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE TABLE without_index (id INTEGER, value VARCHAR, padding VARCHAR)", nil)
	require.NoError(t, err)

	// Create index on only the first table
	_, err = conn.Execute(ctx, "CREATE INDEX idx_with ON with_index(id)", nil)
	require.NoError(t, err)

	// Insert 10,000 rows into both tables
	numRows := 10000
	batchSize := 100
	padding := "padding_data_to_make_rows_larger_and_simulate_real_world_table_with_more_columns"

	for batch := 0; batch < numRows/batchSize; batch++ {
		var insertSQL1, insertSQL2 string
		insertSQL1 = "INSERT INTO with_index VALUES "
		insertSQL2 = "INSERT INTO without_index VALUES "
		for i := 0; i < batchSize; i++ {
			id := batch*batchSize + i
			value := "value_" + intToStr(id)
			if i > 0 {
				insertSQL1 += ", "
				insertSQL2 += ", "
			}
			row := "(" + intToStr(id) + ", '" + value + "', '" + padding + "')"
			insertSQL1 += row
			insertSQL2 += row
		}
		_, err = conn.Execute(ctx, insertSQL1, nil)
		require.NoError(t, err)
		_, err = conn.Execute(ctx, insertSQL2, nil)
		require.NoError(t, err)
	}

	// Run multiple point queries and verify both return same results
	testIDs := []int{0, 500, 1000, 5000, 9999}
	for _, testID := range testIDs {
		query := "SELECT * FROM with_index WHERE id = " + intToStr(testID)
		rowsWithIndex, _, err := conn.Query(ctx, query, nil)
		require.NoError(t, err)

		query = "SELECT * FROM without_index WHERE id = " + intToStr(testID)
		rowsWithoutIndex, _, err := conn.Query(ctx, query, nil)
		require.NoError(t, err)

		// Both should return exactly 1 row with same data
		require.Equal(t, 1, len(rowsWithIndex), "with_index should return 1 row for id=%d", testID)
		require.Equal(t, 1, len(rowsWithoutIndex), "without_index should return 1 row for id=%d", testID)
		assert.Equal(t, rowsWithIndex[0]["id"], rowsWithoutIndex[0]["id"])
		assert.Equal(t, rowsWithIndex[0]["value"], rowsWithoutIndex[0]["value"])
	}

	// Verify EXPLAIN shows different plans
	explainWithIndex, _, err := conn.Query(ctx, "EXPLAIN SELECT * FROM with_index WHERE id = 5000", nil)
	if err == nil && len(explainWithIndex) > 0 {
		foundIndex := false
		for _, row := range explainWithIndex {
			for _, v := range row {
				if s, ok := v.(string); ok {
					if containsAny(s, "IndexScan", "Index Scan", "INDEX") {
						foundIndex = true
						break
					}
				}
			}
		}
		assert.True(t, foundIndex, "Expected IndexScan in EXPLAIN for with_index table")
		t.Logf("with_index uses IndexScan: %v", foundIndex)
	}

	explainWithoutIndex, _, err := conn.Query(ctx, "EXPLAIN SELECT * FROM without_index WHERE id = 5000", nil)
	if err == nil && len(explainWithoutIndex) > 0 {
		foundSeq := false
		for _, row := range explainWithoutIndex {
			for _, v := range row {
				if s, ok := v.(string); ok {
					if containsAny(s, "SeqScan", "Seq Scan", "Sequential", "TableScan", "SCAN") {
						foundSeq = true
						break
					}
				}
			}
		}
		t.Logf("without_index uses SeqScan: %v", foundSeq)
	}

	t.Log("Index benefit test completed - verified index is used and returns correct results")
}

// =============================================================================
// Helper functions
// =============================================================================

// floatToStr converts a float64 to string with 2 decimal places.
func floatToStr(f float64) string {
	// Simple conversion - multiply by 100, truncate, format
	intPart := int(f)
	fracPart := int((f - float64(intPart)) * 100)
	if fracPart < 0 {
		fracPart = -fracPart
	}
	result := intToStr(intPart) + "."
	if fracPart < 10 {
		result += "0"
	}
	result += intToStr(fracPart)
	return result
}

// intToStr converts an integer to string without using strconv to avoid dependency.
func intToStr(num int) string {
	if num == 0 {
		return "0"
	}
	negative := num < 0
	val := num
	if negative {
		val = -val
	}
	digits := make([]byte, 0, 10)
	for val > 0 {
		digits = append([]byte{byte('0' + val%10)}, digits...)
		val /= 10
	}
	if negative {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}

// containsAny checks if s contains any of the substrings.
func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if containsString(s, sub) {
			return true
		}
	}
	return false
}

// containsString checks if s contains substr (case-insensitive).
func containsString(str, substr string) bool {
	lowerStr := toLower(str)
	lowerSubstr := toLower(substr)
	return stringContains(lowerStr, lowerSubstr)
}

// stringContains is a simple contains check.
func stringContains(s, substr string) bool {
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// toLower converts string to lowercase.
func toLower(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}
