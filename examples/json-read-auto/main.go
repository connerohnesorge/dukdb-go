package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
	// Create sample JSON files with different formats

	// Sample 1: JSON Array format
	jsonArrayData := `[
		{"product_id": 1, "name": "Laptop", "price": 999.99, "category": "Electronics"},
		{"product_id": 2, "name": "Mouse", "price": 29.99, "category": "Electronics"},
		{"product_id": 3, "name": "Desk", "price": 299.99, "category": "Furniture"}
	]`

	// Sample 2: NDJSON format
	ndjsonData := `{"product_id": 1, "name": "Laptop", "price": 999.99, "category": "Electronics"}
{"product_id": 2, "name": "Mouse", "price": 29.99, "category": "Electronics"}
{"product_id": 3, "name": "Desk", "price": 299.99, "category": "Furniture"}`

	// Write sample data to files
	arrayFile := "products_array.json"
	ndjsonFile := "products_ndjson.json"

	err := os.WriteFile(arrayFile, []byte(jsonArrayData), 0644)
	if err != nil {
		log.Fatalf("Failed to write JSON array file: %v", err)
	}
	defer os.Remove(arrayFile)

	err = os.WriteFile(ndjsonFile, []byte(ndjsonData), 0644)
	if err != nil {
		log.Fatalf("Failed to write NDJSON file: %v", err)
	}
	defer os.Remove(ndjsonFile)

	// Connect to in-memory database
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Example 1: Using read_json_auto() on JSON Array
	fmt.Println("=== Example 1: read_json_auto() on JSON Array ===")
	query := fmt.Sprintf("SELECT * FROM read_json_auto('%s')", arrayFile)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read JSON array with auto-detection: %v", err)
	}
	defer rows.Close()

	// Print column information
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}
	fmt.Printf("Columns: %v\n\n", columns)

	// Print data
	fmt.Println("Data from JSON array:")
	for rows.Next() {
		var category string
		var name string
		var price float64
		var productID int

		err := rows.Scan(&category, &name, &price, &productID)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf(
			"ID: %d, Name: %s, Price: $%.2f, Category: %s\n",
			productID,
			name,
			price,
			category,
		)
	}
	if err = rows.Err(); err != nil {
		log.Fatalf("Error reading rows: %v", err)
	}

	// Example 2: Using read_json_auto() on NDJSON
	fmt.Println("\n=== Example 2: read_json_auto() on NDJSON ===")
	query = fmt.Sprintf("SELECT * FROM read_json_auto('%s')", ndjsonFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read NDJSON with auto-detection: %v", err)
	}
	defer rows.Close()

	fmt.Println("Data from NDJSON (auto-detected):")
	for rows.Next() {
		var category string
		var name string
		var price float64
		var productID int

		err := rows.Scan(&category, &name, &price, &productID)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf(
			"ID: %d, Name: %s, Price: $%.2f, Category: %s\n",
			productID,
			name,
			price,
			category,
		)
	}

	// Example 3: Comparing read_json_auto() with explicit format
	fmt.Println("\n=== Example 3: Comparison with Explicit Format ===")

	// Using explicit format
	query = fmt.Sprintf(
		"SELECT COUNT(*) as count FROM read_json('%s', format = 'array')",
		arrayFile,
	)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read with explicit format: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		err := rows.Scan(&count)
		if err != nil {
			log.Fatalf("Failed to scan count: %v", err)
		}
		fmt.Printf("Explicit 'array' format count: %d\n", count)
	}

	// Using auto-detection
	query = fmt.Sprintf("SELECT COUNT(*) as count FROM read_json_auto('%s')", arrayFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read with auto-detection: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		err := rows.Scan(&count)
		if err != nil {
			log.Fatalf("Failed to scan count: %v", err)
		}
		fmt.Printf("Auto-detection count: %d\n", count)
	}

	// Example 4: Auto-detection with different data types
	fmt.Println("\n=== Example 4: Auto-detection with Mixed Data Types ===")

	// Create a file with various data types
	mixedData := `[
		{"id": 1, "text": "Hello", "number": 42, "decimal": 3.14, "flag": true, "date": "2024-01-15"},
		{"id": 2, "text": "World", "number": 100, "decimal": 2.71, "flag": false, "date": "2024-01-16"},
		{"id": 3, "text": "Test", "number": 7, "decimal": 1.41, "flag": true, "date": "2024-01-17"}
	]`

	mixedFile := "mixed_types.json"
	err = os.WriteFile(mixedFile, []byte(mixedData), 0644)
	if err != nil {
		log.Fatalf("Failed to write mixed types file: %v", err)
	}
	defer os.Remove(mixedFile)

	query = fmt.Sprintf("SELECT * FROM read_json_auto('%s')", mixedFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read mixed types: %v", err)
	}
	defer rows.Close()

	columns, err = rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}
	fmt.Printf("Mixed types columns: %v\n", columns)

	fmt.Println("Data with mixed types:")
	for rows.Next() {
		var date string
		var decimal float64
		var flag bool
		var id int
		var number int
		var text string

		err := rows.Scan(&date, &decimal, &flag, &id, &number, &text)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("ID: %d, Text: '%s', Number: %d, Decimal: %.2f, Flag: %t, Date: %s\n",
			id, text, number, decimal, flag, date)
	}

	// Example 5: Auto-detection with nested structures
	fmt.Println("\n=== Example 5: Auto-detection with Nested JSON ===")

	// Create a file with nested structures
	nestedData := `[
		{
			"user": {
				"id": 1,
				"name": "Alice"
			},
			"location": {
				"city": "New York",
				"country": "USA"
			},
			"score": 95.5
		},
		{
			"user": {
				"id": 2,
				"name": "Bob"
			},
			"location": {
				"city": "London",
				"country": "UK"
			},
			"score": 87.3
		}
	]`

	nestedFile := "nested_data.json"
	err = os.WriteFile(nestedFile, []byte(nestedData), 0644)
	if err != nil {
		log.Fatalf("Failed to write nested data file: %v", err)
	}
	defer os.Remove(nestedFile)

	query = fmt.Sprintf("SELECT * FROM read_json_auto('%s')", nestedFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read nested data: %v", err)
	}
	defer rows.Close()

	columns, err = rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}
	fmt.Printf("Nested data columns: %v\n", columns)

	fmt.Println("Nested data (as JSON strings):")
	for rows.Next() {
		var location string
		var score float64
		var user string

		err := rows.Scan(&location, &score, &user)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("User: %s, Location: %s, Score: %.1f\n", user, location, score)
	}

	// Example 6: Error handling with auto-detection
	fmt.Println("\n=== Example 6: Error Handling ===")

	// Test with invalid JSON
	invalidFile := "invalid.json"
	invalidData := `{"invalid": json content}`
	err = os.WriteFile(invalidFile, []byte(invalidData), 0644)
	if err != nil {
		log.Fatalf("Failed to write invalid file: %v", err)
	}
	defer os.Remove(invalidFile)

	query = fmt.Sprintf("SELECT * FROM read_json_auto('%s')", invalidFile)
	rows, err = db.Query(query)
	if err != nil {
		fmt.Printf("Expected error for invalid JSON: %v\n", err)
	} else {
		rows.Close()
	}

	// Example 7: Performance comparison
	fmt.Println("\n=== Example 7: Performance Comparison ===")

	// Create a larger file
	largeData := "[\n"
	for i := 0; i < 1000; i++ {
		if i > 0 {
			largeData += ",\n"
		}
		largeData += fmt.Sprintf(`  {"id": %d, "value": "item_%d", "number": %d}`, i, i, i*10)
	}
	largeData += "\n]"

	largeFile := "large_array.json"
	err = os.WriteFile(largeFile, []byte(largeData), 0644)
	if err != nil {
		log.Fatalf("Failed to write large file: %v", err)
	}
	defer os.Remove(largeFile)

	// Time explicit format
	fmt.Println("Processing large file with explicit format...")
	query = fmt.Sprintf("SELECT COUNT(*) FROM read_json('%s', format = 'array')", largeFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read large file with explicit format: %v", err)
	}
	if rows.Next() {
		var count int
		rows.Scan(&count)
		fmt.Printf("Explicit format count: %d\n", count)
	}
	rows.Close()

	// Time auto-detection
	fmt.Println("Processing large file with auto-detection...")
	query = fmt.Sprintf("SELECT COUNT(*) FROM read_json_auto('%s')", largeFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read large file with auto-detection: %v", err)
	}
	if rows.Next() {
		var count int
		rows.Scan(&count)
		fmt.Printf("Auto-detection count: %d\n", count)
	}
	rows.Close()

	// Example 8: Working with file paths
	fmt.Println("\n=== Example 8: Working with File Paths ===")
	absPath, err := filepath.Abs(arrayFile)
	if err != nil {
		log.Fatalf("Failed to get absolute path: %v", err)
	}

	query = fmt.Sprintf(
		"SELECT COUNT(DISTINCT category) as unique_categories FROM read_json_auto('%s')",
		absPath,
	)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read with absolute path: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var uniqueCategories int
		err := rows.Scan(&uniqueCategories)
		if err != nil {
			log.Fatalf("Failed to scan count: %v", err)
		}
		fmt.Printf("Unique categories: %d\n", uniqueCategories)
	}

	fmt.Println("\nAll examples completed successfully!")
}
