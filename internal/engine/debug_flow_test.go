package engine

import (
	"context"
	"testing"
	"fmt"
)

// TestDebug_RangeFlowNew traces the entire range query flow with updated ART.
func TestDebug_RangeFlowNew(t *testing.T) {
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

	// Create table without index first
	_, err = conn.Execute(ctx, "CREATE TABLE flow_test (id INTEGER, value INTEGER)", nil)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = conn.Execute(ctx, "INSERT INTO flow_test VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)", nil)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Query without index (should work with seq scan)
	fmt.Println("\n=== Test without index: value < 40 ===")
	rows, _, err := conn.Query(ctx, "SELECT * FROM flow_test WHERE value < 40", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Without index - Rows: %d\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  %v\n", row)
	}

	// Create index
	_, err = conn.Execute(ctx, "CREATE INDEX idx_value ON flow_test(value)", nil)
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Query WITH index (should use range scan)
	fmt.Println("\n=== Test WITH index: value < 40 ===")
	rows, _, err = conn.Query(ctx, "SELECT * FROM flow_test WHERE value < 40", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("With index - Rows: %d\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  %v\n", row)
	}
	
	// Verify BETWEEN still works
	fmt.Println("\n=== Test WITH index: BETWEEN 20 AND 40 ===")
	rows, _, err = conn.Query(ctx, "SELECT * FROM flow_test WHERE value BETWEEN 20 AND 40", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("BETWEEN - Rows: %d\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  %v\n", row)
	}
}
