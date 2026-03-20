package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/dukdb/dukdb-go"
	// Import engine to register backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
	// Create a new database connection
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	fmt.Println("=== JSON Schema Inference Example ===")

	// Example 1: Simple JSON array with consistent structure
	fmt.Println("\n1. Inferring schema from consistent JSON array:")

	simpleJSON := `[
  {"id": 1, "name": "Product A", "price": 29.99, "in_stock": true},
  {"id": 2, "name": "Product B", "price": 49.99, "in_stock": false},
  {"id": 3, "name": "Product C", "price": 89.99, "in_stock": true}
]`

	simplePath := "products.json"
	os.WriteFile(simplePath, []byte(simpleJSON), 0644)
	defer os.Remove(simplePath)

	// Read and check inferred schema
	fmt.Println("Schema inferred from 'products.json':")
	rows, _ := db.Query("SELECT * FROM read_json_auto('products.json')")
	defer rows.Close()

	cols, _ := rows.Columns()
	fmt.Printf("Columns: %v\n", cols)

	// Get column type information
	colTypes, _ := rows.ColumnTypes()
	for i, col := range colTypes {
		fmt.Printf("  Column %d: name='%s', type='%s'\n", i, col.Name(), col.DatabaseTypeName())
	}

	// Example 2: NDJSON with type inference
	fmt.Println("\n2. Inferring schema from NDJSON:")

	ndjsonJSON := `{"transaction_id": "TXN001", "amount": 150.50, "timestamp": "2024-01-01T10:30:00Z", "status": "completed"}
{"transaction_id": "TXN002", "amount": 200.75, "timestamp": "2024-01-01T10:35:00Z", "status": "pending"}
{"transaction_id": "TXN003", "amount": 99.99, "timestamp": "2024-01-01T10:40:00Z", "status": "completed"}`

	ndjsonPath := "transactions.json"
	os.WriteFile(ndjsonPath, []byte(ndjsonJSON), 0644)
	defer os.Remove(ndjsonPath)

	fmt.Println("Schema inferred from 'transactions.json':")
	rows, _ = db.Query("SELECT * FROM read_json_auto('transactions.json')")
	defer rows.Close()

	cols, _ = rows.Columns()
	fmt.Printf("Columns: %v\n", cols)

	// Example 3: Schema with different numeric types
	fmt.Println("\n3. Numeric type inference:")

	numericJSON := `[
  {"counter": 100, "rating": 4.5, "percentage": 95},
  {"counter": 250, "rating": 3.8, "percentage": 78},
  {"counter": 500, "rating": 4.9, "percentage": 99}
]`

	numericPath := "metrics.json"
	os.WriteFile(numericPath, []byte(numericJSON), 0644)
	defer os.Remove(numericPath)

	fmt.Println("Inferred numeric types:")
	rows, _ = db.Query("SELECT * FROM read_json_auto('metrics.json')")
	defer rows.Close()

	numColTypes, _ := rows.ColumnTypes()
	for _, colType := range numColTypes {
		fmt.Printf("  %s: %s\n", colType.Name(), colType.DatabaseTypeName())
	}

	// Example 4: Mixed JSON (careful with type inference)
	fmt.Println("\n4. Type consistency (all fields should have consistent types):")

	mixedJSON := `[
  {"id": 1, "value": 100, "label": "A"},
  {"id": 2, "value": 200, "label": "B"},
  {"id": 3, "value": 300, "label": "C"}
]`

	mixedPath := "consistent.json"
	os.WriteFile(mixedPath, []byte(mixedJSON), 0644)
	defer os.Remove(mixedPath)

	fmt.Println("Schema from consistent types:")
	rows, _ = db.Query("SELECT * FROM read_json_auto('consistent.json')")
	defer rows.Close()

	cols, _ = rows.Columns()
	colTypes, _ = rows.ColumnTypes()

	for _, col := range colTypes {
		scanType := col.ScanType()
		fmt.Printf("  %s: DuckDB type='%s', Go type='%s'\n", col.Name(), col.DatabaseTypeName(), scanType)
	}

	// Example 5: Querying with inferred schema
	fmt.Println("\n5. Using inferred schema for queries:")

	count := 0
	rows, _ = db.Query("SELECT COUNT(*) FROM read_json_auto('products.json')")
	defer rows.Close()

	for rows.Next() {
		rows.Scan(&count)
	}
	fmt.Printf("Product count: %d\n", count)

	// Example 6: Schema information
	fmt.Println("\n6. Schema Inference Rules:")
	fmt.Println("  - First record defines schema")
	fmt.Println("  - String values → VARCHAR")
	fmt.Println("  - Integer values → INTEGER")
	fmt.Println("  - Float values → DOUBLE")
	fmt.Println("  - Boolean values → BOOLEAN")
	fmt.Println("  - Arrays → ARRAY type")
	fmt.Println("  - Objects → STRUCT type")
	fmt.Println("  - Nulls → determined from other records")
	fmt.Println("  - Consistent types required for accuracy")

	fmt.Println("\n✓ JSON schema inference example completed successfully!")
}
