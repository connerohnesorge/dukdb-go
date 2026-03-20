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

	fmt.Println("=== JSON Auto Format Detection Example ===")

	// Example 1: JSON Array format
	fmt.Println("\n1. JSON Array Format (Automatic Detection):")

	jsonArrayData := `[
  {"id": 1, "name": "Alice", "age": 25, "city": "New York"},
  {"id": 2, "name": "Bob", "age": 30, "city": "San Francisco"},
  {"id": 3, "name": "Charlie", "age": 28, "city": "Chicago"}
]`

	jsonArrayPath := "data_array.json"
	err = os.WriteFile(jsonArrayPath, []byte(jsonArrayData), 0644)
	if err != nil {
		log.Fatal("Failed to write JSON array file:", err)
	}
	defer os.Remove(jsonArrayPath)

	fmt.Println("Reading JSON Array with read_json_auto():")
	rows, err := db.Query("SELECT * FROM read_json_auto('data_array.json')")
	if err != nil {
		log.Fatal("Failed to read JSON array:", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Failed to get columns:", err)
	}
	fmt.Printf("Columns: %v\n", columns)

	fmt.Println("Data from JSON Array:")
	for rows.Next() {
		var id int
		var name string
		var age int
		var city string

		err := rows.Scan(&age, &city, &id, &name)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}
		fmt.Printf("  - %s (age %d) from %s\n", name, age, city)
	}

	// Example 2: NDJSON format
	fmt.Println("\n2. NDJSON Format (Automatic Detection):")

	ndjsonData := `{"id": 1, "product": "Laptop", "price": 999.99, "stock": 15}
{"id": 2, "product": "Mouse", "price": 29.99, "stock": 150}
{"id": 3, "product": "Keyboard", "price": 79.99, "stock": 75}`

	ndjsonPath := "data_ndjson.json"
	err = os.WriteFile(ndjsonPath, []byte(ndjsonData), 0644)
	if err != nil {
		log.Fatal("Failed to write NDJSON file:", err)
	}
	defer os.Remove(ndjsonPath)

	fmt.Println("Reading NDJSON with read_json_auto():")
	rows, err = db.Query("SELECT * FROM read_json_auto('data_ndjson.json')")
	if err != nil {
		log.Fatal("Failed to read NDJSON:", err)
	}
	defer rows.Close()

	columns, err = rows.Columns()
	if err != nil {
		log.Fatal("Failed to get columns:", err)
	}
	fmt.Printf("Columns: %v\n", columns)

	fmt.Println("Data from NDJSON:")
	for rows.Next() {
		var id int
		var product string
		var price float64
		var stock int

		err := rows.Scan(&id, &price, &product, &stock)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}
		fmt.Printf("  - %s: $%.2f (stock: %d)\n", product, price, stock)
	}

	// Example 3: Comparison of both formats with same data structure
	fmt.Println("\n3. Comparing Both Formats:")

	// Create both format versions
	arrayFormat := `[
  {"id": 1, "name": "Product A", "price": 100},
  {"id": 2, "name": "Product B", "price": 200},
  {"id": 3, "name": "Product C", "price": 150}
]`

	ndjsonFormat := `{"id": 1, "name": "Product A", "price": 100}
{"id": 2, "name": "Product B", "price": 200}
{"id": 3, "name": "Product C", "price": 150}`

	arrayPath := "products_array.json"
	ndjsonPath2 := "products_ndjson.json"

	os.WriteFile(arrayPath, []byte(arrayFormat), 0644)
	os.WriteFile(ndjsonPath2, []byte(ndjsonFormat), 0644)

	defer os.Remove(arrayPath)
	defer os.Remove(ndjsonPath2)

	// Read from both and compare
	fmt.Println("\nArray format row count:")
	var arrayCount int
	db.QueryRow("SELECT COUNT(*) FROM read_json_auto('products_array.json')").Scan(&arrayCount)
	fmt.Printf("  Count: %d\n", arrayCount)

	fmt.Println("NDJSON format row count:")
	var ndjsonCount int
	db.QueryRow("SELECT COUNT(*) FROM read_json_auto('products_ndjson.json')").Scan(&ndjsonCount)
	fmt.Printf("  Count: %d\n", ndjsonCount)

	// Get average price from both
	fmt.Println("\nAverage price from Array format:")
	var arrayAvgPrice float64
	db.QueryRow("SELECT AVG(price) FROM read_json_auto('products_array.json')").Scan(&arrayAvgPrice)
	fmt.Printf("  Average: $%.2f\n", arrayAvgPrice)

	fmt.Println("Average price from NDJSON format:")
	var ndjsonAvgPrice float64
	db.QueryRow("SELECT AVG(price) FROM read_json_auto('products_ndjson.json')").Scan(&ndjsonAvgPrice)
	fmt.Printf("  Average: $%.2f\n", ndjsonAvgPrice)

	fmt.Println("\n4. Auto-Detection Features:")
	fmt.Println("✓ Automatically detects JSON array vs NDJSON format")
	fmt.Println("✓ Infers column types from data")
	fmt.Println("✓ Supports both compact and pretty-printed JSON")
	fmt.Println("✓ Handles both formats transparently")
	fmt.Println("✓ Same SQL interface regardless of format")

	fmt.Println("\n✓ JSON auto-detection example completed successfully!")
}
