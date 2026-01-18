package engine

import (
	"context"
	"fmt"
	"testing"
)

// TestDebug_CompositeIndex tests composite index with range predicate on first column.
func TestDebug_CompositeIndex(t *testing.T) {
	engine := NewEngine()
	defer func() {
		_ = engine.Close()
	}()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	ctx := context.Background()

	// Create table with composite index
	_, err = conn.Execute(
		ctx,
		"CREATE TABLE products (category INTEGER, price INTEGER, name VARCHAR)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = conn.Execute(ctx, "CREATE INDEX idx_products ON products(category, price)", nil)
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert test data
	_, err = conn.Execute(
		ctx,
		"INSERT INTO products VALUES (1, 100, 'A'), (1, 200, 'B'), (2, 150, 'C'), (2, 250, 'D'), (3, 175, 'E'), (3, 300, 'F')",
		nil,
	)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test without index
	fmt.Println("\n=== Test without index ===")

	// Drop index to test seq scan behavior
	_, err = conn.Execute(ctx, "DROP INDEX idx_products", nil)
	if err != nil {
		t.Fatalf("DROP INDEX failed: %v", err)
	}

	rows, _, err := conn.Query(ctx, "SELECT * FROM products WHERE category >= 2", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Without index - Rows: %d (expected 4)\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  %v\n", row)
	}

	// Recreate index
	_, err = conn.Execute(ctx, "CREATE INDEX idx_products ON products(category, price)", nil)
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Test with composite index
	fmt.Println("\n=== Test WITH composite index ===")
	rows, _, err = conn.Query(ctx, "SELECT * FROM products WHERE category >= 2", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("With index - Rows: %d (expected 4)\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  %v\n", row)
	}
}
