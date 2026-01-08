// Package engine provides integration tests for range predicates with indexes.
//
// Task 3.7: Test with various range predicates (<, >, BETWEEN)
//
// These tests verify that SQL queries with range predicates work correctly.
// The ART range scan implementation is fully functional at the storage layer
// (tested in internal/storage/index/art_test.go). These integration tests
// verify end-to-end functionality from SQL parsing through execution.
//
// NOTE: Currently, the index matcher (internal/optimizer/index_matcher.go) only
// supports equality predicates (=) and IN lists. Range predicate support in the
// index matcher will be added in tasks 4.1-4.5. Until then, range queries use
// sequential scan but still return correct results.
package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Task 3.7: Integration Tests for Range Predicates
// =============================================================================

// TestRangePredicate_LessThan tests WHERE col < value queries.
func TestRangePredicate_LessThan(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data with values: 10, 20, 30, 40, 50, 60, 70, 80, 90, 100
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10, 'ten'),
		(2, 20, 'twenty'),
		(3, 30, 'thirty'),
		(4, 40, 'forty'),
		(5, 50, 'fifty'),
		(6, 60, 'sixty'),
		(7, 70, 'seventy'),
		(8, 80, 'eighty'),
		(9, 90, 'ninety'),
		(10, 100, 'hundred')`, nil)
	require.NoError(t, err)

	// Test: WHERE value < 50
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value < 50", nil)
	require.NoError(t, err)

	// Should return rows with values 10, 20, 30, 40
	require.Equal(t, 4, len(rows), "Should return 4 rows where value < 50")

	// Verify all returned values are < 50
	for _, row := range rows {
		val, ok := row["value"].(int32)
		require.True(t, ok, "value should be int32")
		assert.Less(t, val, int32(50), "All values should be < 50")
	}
}

// TestRangePredicate_LessThanOrEqual tests WHERE col <= value queries.
func TestRangePredicate_LessThanOrEqual(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
		(6, 60), (7, 70), (8, 80), (9, 90), (10, 100)`, nil)
	require.NoError(t, err)

	// Test: WHERE value <= 50
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value <= 50", nil)
	require.NoError(t, err)

	// Should return rows with values 10, 20, 30, 40, 50
	require.Equal(t, 5, len(rows), "Should return 5 rows where value <= 50")

	// Verify all returned values are <= 50
	for _, row := range rows {
		val, ok := row["value"].(int32)
		require.True(t, ok, "value should be int32")
		assert.LessOrEqual(t, val, int32(50), "All values should be <= 50")
	}
}

// TestRangePredicate_GreaterThan tests WHERE col > value queries.
func TestRangePredicate_GreaterThan(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
		(6, 60), (7, 70), (8, 80), (9, 90), (10, 100)`, nil)
	require.NoError(t, err)

	// Test: WHERE value > 50
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value > 50", nil)
	require.NoError(t, err)

	// Should return rows with values 60, 70, 80, 90, 100
	require.Equal(t, 5, len(rows), "Should return 5 rows where value > 50")

	// Verify all returned values are > 50
	for _, row := range rows {
		val, ok := row["value"].(int32)
		require.True(t, ok, "value should be int32")
		assert.Greater(t, val, int32(50), "All values should be > 50")
	}
}

// TestRangePredicate_GreaterThanOrEqual tests WHERE col >= value queries.
func TestRangePredicate_GreaterThanOrEqual(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
		(6, 60), (7, 70), (8, 80), (9, 90), (10, 100)`, nil)
	require.NoError(t, err)

	// Test: WHERE value >= 50
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value >= 50", nil)
	require.NoError(t, err)

	// Should return rows with values 50, 60, 70, 80, 90, 100
	require.Equal(t, 6, len(rows), "Should return 6 rows where value >= 50")

	// Verify all returned values are >= 50
	for _, row := range rows {
		val, ok := row["value"].(int32)
		require.True(t, ok, "value should be int32")
		assert.GreaterOrEqual(t, val, int32(50), "All values should be >= 50")
	}
}

// TestRangePredicate_Between tests WHERE col BETWEEN low AND high queries.
func TestRangePredicate_Between(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
		(6, 60), (7, 70), (8, 80), (9, 90), (10, 100)`, nil)
	require.NoError(t, err)

	// Test: WHERE value BETWEEN 30 AND 70 (inclusive)
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value BETWEEN 30 AND 70", nil)
	require.NoError(t, err)

	// Should return rows with values 30, 40, 50, 60, 70
	require.Equal(t, 5, len(rows), "Should return 5 rows where value BETWEEN 30 AND 70")

	// Verify all returned values are in range [30, 70]
	for _, row := range rows {
		val, ok := row["value"].(int32)
		require.True(t, ok, "value should be int32")
		assert.GreaterOrEqual(t, val, int32(30), "All values should be >= 30")
		assert.LessOrEqual(t, val, int32(70), "All values should be <= 70")
	}
}

// TestRangePredicate_NotBetween tests WHERE col NOT BETWEEN low AND high queries.
func TestRangePredicate_NotBetween(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
		(6, 60), (7, 70), (8, 80), (9, 90), (10, 100)`, nil)
	require.NoError(t, err)

	// Test: WHERE value NOT BETWEEN 30 AND 70
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value NOT BETWEEN 30 AND 70", nil)
	require.NoError(t, err)

	// Should return rows with values 10, 20, 80, 90, 100
	require.Equal(t, 5, len(rows), "Should return 5 rows where value NOT BETWEEN 30 AND 70")

	// Verify all returned values are outside range [30, 70]
	for _, row := range rows {
		val, ok := row["value"].(int32)
		require.True(t, ok, "value should be int32")
		assert.True(t, val < 30 || val > 70, "All values should be < 30 or > 70")
	}
}

// TestRangePredicate_CombinedRanges tests combined range predicates.
func TestRangePredicate_CombinedRanges(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
		(6, 60), (7, 70), (8, 80), (9, 90), (10, 100)`, nil)
	require.NoError(t, err)

	// Test: WHERE value > 20 AND value < 80
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value > 20 AND value < 80", nil)
	require.NoError(t, err)

	// Should return rows with values 30, 40, 50, 60, 70
	require.Equal(t, 5, len(rows), "Should return 5 rows where value > 20 AND value < 80")

	// Verify all returned values are in range (20, 80)
	for _, row := range rows {
		val, ok := row["value"].(int32)
		require.True(t, ok, "value should be int32")
		assert.Greater(t, val, int32(20), "All values should be > 20")
		assert.Less(t, val, int32(80), "All values should be < 80")
	}
}

// TestRangePredicate_EmptyRange tests queries that return no results.
func TestRangePredicate_EmptyRange(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data with values: 10, 20, 30, 40, 50
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)`, nil)
	require.NoError(t, err)

	// Test: WHERE value > 1000 (no values in this range)
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value > 1000", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, len(rows), "Should return 0 rows for empty range")

	// Test: WHERE value < 0 (no values in this range)
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE value < 0", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, len(rows), "Should return 0 rows for empty range")

	// Test: WHERE value BETWEEN 100 AND 200 (no values in this range)
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE value BETWEEN 100 AND 200", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, len(rows), "Should return 0 rows for empty range")
}

// TestRangePredicate_FullTableRange tests queries that match all rows.
func TestRangePredicate_FullTableRange(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data with values: 10, 20, 30, 40, 50
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)`, nil)
	require.NoError(t, err)

	// Test: WHERE value >= 0 (all values match)
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value >= 0", nil)
	require.NoError(t, err)
	assert.Equal(t, 5, len(rows), "Should return all 5 rows")

	// Test: WHERE value < 1000 (all values match)
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE value < 1000", nil)
	require.NoError(t, err)
	assert.Equal(t, 5, len(rows), "Should return all 5 rows")

	// Test: WHERE value BETWEEN 0 AND 100 (all values match)
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE value BETWEEN 0 AND 100", nil)
	require.NoError(t, err)
	assert.Equal(t, 5, len(rows), "Should return all 5 rows")
}

// TestRangePredicate_SingleElementRange tests ranges that match exactly one row.
func TestRangePredicate_SingleElementRange(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data with values: 10, 20, 30, 40, 50
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)`, nil)
	require.NoError(t, err)

	// Test: WHERE value BETWEEN 30 AND 30 (single value range)
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value BETWEEN 30 AND 30", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows), "Should return exactly 1 row")
	assert.Equal(t, int32(30), rows[0]["value"])

	// Test: WHERE value >= 50 AND value <= 50 (single value range)
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE value >= 50 AND value <= 50", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows), "Should return exactly 1 row")
	assert.Equal(t, int32(50), rows[0]["value"])

	// Test: WHERE value > 9 AND value < 11 (only 10 matches)
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE value > 9 AND value < 11", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows), "Should return exactly 1 row")
	assert.Equal(t, int32(10), rows[0]["value"])
}

// =============================================================================
// Composite Index Tests with Range Predicates
// =============================================================================

// TestRangePredicate_CompositeIndex_PartialRange tests composite index with range
// on the second column after equality on the first column.
func TestRangePredicate_CompositeIndex_PartialRange(t *testing.T) {
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

	// Create table with composite index
	_, err = conn.Execute(ctx, "CREATE TABLE orders (customer_id INTEGER, order_date INTEGER, amount INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_orders_cust_date ON orders(customer_id, order_date)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO orders VALUES
		(1, 20240101, 100),
		(1, 20240115, 200),
		(1, 20240201, 150),
		(1, 20240215, 300),
		(2, 20240101, 250),
		(2, 20240115, 175),
		(2, 20240201, 225)`, nil)
	require.NoError(t, err)

	// Test: customer_id = 1 AND order_date BETWEEN 20240101 AND 20240131
	// This should use the index for customer_id lookup and range on order_date
	rows, _, err := conn.Query(ctx, "SELECT * FROM orders WHERE customer_id = 1 AND order_date BETWEEN 20240101 AND 20240131", nil)
	require.NoError(t, err)

	// Should return customer 1's orders in January (20240101 and 20240115)
	require.Equal(t, 2, len(rows), "Should return 2 orders for customer 1 in January")

	for _, row := range rows {
		custID, ok := row["customer_id"].(int32)
		require.True(t, ok)
		assert.Equal(t, int32(1), custID)

		orderDate, ok := row["order_date"].(int32)
		require.True(t, ok)
		assert.GreaterOrEqual(t, orderDate, int32(20240101))
		assert.LessOrEqual(t, orderDate, int32(20240131))
	}
}

// TestRangePredicate_CompositeIndex_RangeOnFirstColumn tests composite index with
// range predicate on the first column.
func TestRangePredicate_CompositeIndex_RangeOnFirstColumn(t *testing.T) {
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

	// Create table with composite index
	_, err = conn.Execute(ctx, "CREATE TABLE products (category INTEGER, price INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_products ON products(category, price)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO products VALUES
		(1, 100, 'A'),
		(1, 200, 'B'),
		(2, 150, 'C'),
		(2, 250, 'D'),
		(3, 175, 'E'),
		(3, 300, 'F')`, nil)
	require.NoError(t, err)

	// Test: category >= 2 (range on first column)
	rows, _, err := conn.Query(ctx, "SELECT * FROM products WHERE category >= 2", nil)
	require.NoError(t, err)

	// Should return products from categories 2 and 3
	require.Equal(t, 4, len(rows), "Should return 4 products from categories 2 and 3")

	for _, row := range rows {
		cat, ok := row["category"].(int32)
		require.True(t, ok)
		assert.GreaterOrEqual(t, cat, int32(2))
	}
}

// =============================================================================
// Edge Cases and Boundary Conditions
// =============================================================================

// TestRangePredicate_BoundaryValues tests queries with boundary values.
func TestRangePredicate_BoundaryValues(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data with distinct values
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30)`, nil)
	require.NoError(t, err)

	// Test exact boundary: value < 20 (should get only 10)
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value < 20", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	assert.Equal(t, int32(10), rows[0]["value"])

	// Test exact boundary: value <= 20 (should get 10 and 20)
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE value <= 20", nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(rows))

	// Test exact boundary: value > 20 (should get only 30)
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE value > 20", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	assert.Equal(t, int32(30), rows[0]["value"])

	// Test exact boundary: value >= 20 (should get 20 and 30)
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE value >= 20", nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(rows))
}

// TestRangePredicate_NegativeValues tests range queries with negative values.
func TestRangePredicate_NegativeValues(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data with negative values
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, -50), (2, -25), (3, 0), (4, 25), (5, 50)`, nil)
	require.NoError(t, err)

	// Test: value < 0 (negative values only)
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value < 0", nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(rows), "Should return 2 negative values")

	for _, row := range rows {
		val, ok := row["value"].(int32)
		require.True(t, ok)
		assert.Less(t, val, int32(0))
	}

	// Test: value BETWEEN -30 AND 30
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE value BETWEEN -30 AND 30", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(rows), "Should return 3 values in range [-30, 30]")
}

// TestRangePredicate_VarcharRange tests range queries on VARCHAR columns.
func TestRangePredicate_VarcharRange(t *testing.T) {
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

	// Create table and index on VARCHAR column
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_name ON range_test(name)", nil)
	require.NoError(t, err)

	// Insert test data with string values
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 'alice'),
		(2, 'bob'),
		(3, 'charlie'),
		(4, 'david'),
		(5, 'eve')`, nil)
	require.NoError(t, err)

	// Test: name < 'charlie' (should get alice, bob)
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE name < 'charlie'", nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(rows), "Should return 2 names before 'charlie'")

	// Test: name >= 'charlie' (should get charlie, david, eve)
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE name >= 'charlie'", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(rows), "Should return 3 names >= 'charlie'")

	// Test: name BETWEEN 'bob' AND 'david'
	rows, _, err = conn.Query(ctx, "SELECT * FROM range_test WHERE name BETWEEN 'bob' AND 'david'", nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(rows), "Should return bob, charlie, david")
}

// TestRangePredicate_RangeWithORCondition tests range predicates combined with OR.
func TestRangePredicate_RangeWithORCondition(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
		(6, 60), (7, 70), (8, 80), (9, 90), (10, 100)`, nil)
	require.NoError(t, err)

	// Test: value < 30 OR value > 70
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value < 30 OR value > 70", nil)
	require.NoError(t, err)

	// Should return: 10, 20 (< 30) and 80, 90, 100 (> 70) = 5 rows
	require.Equal(t, 5, len(rows), "Should return 5 rows")

	for _, row := range rows {
		val, ok := row["value"].(int32)
		require.True(t, ok)
		assert.True(t, val < 30 || val > 70, "Value should be < 30 or > 70")
	}
}

// TestRangePredicate_RangeWithANDCondition tests range predicates combined with AND.
func TestRangePredicate_RangeWithANDCondition(t *testing.T) {
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

	// Create table with two indexes
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, a INTEGER, b INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_a ON range_test(a)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_b ON range_test(b)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10, 100),
		(2, 20, 200),
		(3, 30, 300),
		(4, 40, 400),
		(5, 50, 500)`, nil)
	require.NoError(t, err)

	// Test: a > 20 AND b < 400
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE a > 20 AND b < 400", nil)
	require.NoError(t, err)

	// Should return: (30, 300)
	require.Equal(t, 1, len(rows), "Should return 1 row")
	assert.Equal(t, int32(30), rows[0]["a"])
	assert.Equal(t, int32(300), rows[0]["b"])
}

// TestRangePredicate_ImpossibleRange tests queries with impossible ranges.
func TestRangePredicate_ImpossibleRange(t *testing.T) {
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
	_, err = conn.Execute(ctx, "CREATE TABLE range_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_test(value)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO range_test VALUES
		(1, 10), (2, 20), (3, 30)`, nil)
	require.NoError(t, err)

	// Test: value > 50 AND value < 5 (impossible - no overlap)
	rows, _, err := conn.Query(ctx, "SELECT * FROM range_test WHERE value > 50 AND value < 5", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, len(rows), "Impossible range should return 0 rows")
}

// =============================================================================
// Benchmarks
// =============================================================================

// BenchmarkRangePredicate_WithIndex benchmarks range query with index.
func BenchmarkRangePredicate_WithIndex(b *testing.B) {
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

	// Setup: Create table with index and insert data
	_, err = conn.Execute(ctx, "CREATE TABLE bench_range (id INTEGER, value INTEGER)", nil)
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = conn.Execute(ctx, "CREATE INDEX idx_bench_value ON bench_range(value)", nil)
	if err != nil {
		b.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert 1000 rows with values 0-999
	for i := 0; i < 10; i++ {
		var sql string
		sql = "INSERT INTO bench_range VALUES "
		for j := 0; j < 100; j++ {
			val := i*100 + j
			if j > 0 {
				sql += ", "
			}
			sql += "(" + intToStr(val) + ", " + intToStr(val) + ")"
		}
		_, err = conn.Execute(ctx, sql, nil)
		if err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Query for ~10% of data (100 rows out of 1000)
		_, _, err := conn.Query(ctx, "SELECT * FROM bench_range WHERE value BETWEEN 200 AND 299", nil)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// BenchmarkRangePredicate_WithoutIndex benchmarks range query without index.
func BenchmarkRangePredicate_WithoutIndex(b *testing.B) {
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

	// Setup: Create table WITHOUT index and insert data
	_, err = conn.Execute(ctx, "CREATE TABLE bench_range_noidx (id INTEGER, value INTEGER)", nil)
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert 1000 rows with values 0-999
	for i := 0; i < 10; i++ {
		var sql string
		sql = "INSERT INTO bench_range_noidx VALUES "
		for j := 0; j < 100; j++ {
			val := i*100 + j
			if j > 0 {
				sql += ", "
			}
			sql += "(" + intToStr(val) + ", " + intToStr(val) + ")"
		}
		_, err = conn.Execute(ctx, sql, nil)
		if err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Query for ~10% of data (100 rows out of 1000)
		_, _, err := conn.Query(ctx, "SELECT * FROM bench_range_noidx WHERE value BETWEEN 200 AND 299", nil)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}
