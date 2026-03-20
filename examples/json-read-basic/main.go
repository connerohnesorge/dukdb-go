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

	// Create sample JSON array file
	sampleJSON := `[
  {"id": 1, "name": "Alice", "age": 25, "city": "New York"},
  {"id": 2, "name": "Bob", "age": 30, "city": "San Francisco"},
  {"id": 3, "name": "Charlie", "age": 28, "city": "Chicago"},
  {"id": 4, "name": "Diana", "age": 35, "city": "Boston"},
  {"id": 5, "name": "Eve", "age": 22, "city": "Seattle"}
]`

	// Write sample JSON to file
	jsonPath := "sample_data.json"
	err = os.WriteFile(jsonPath, []byte(sampleJSON), 0644)
	if err != nil {
		log.Fatal("Failed to write JSON file:", err)
	}
	defer os.Remove(jsonPath) // Clean up after example

	fmt.Println("=== Basic JSON Reading Example ===")
	fmt.Println("\n1. Reading JSON Array using SQL:")

	// Read JSON using SQL query
	rows, err := db.Query("SELECT * FROM read_json('sample_data.json')")
	if err != nil {
		log.Fatal("Failed to read JSON:", err)
	}
	defer rows.Close()

	// Print column information
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Failed to get columns:", err)
	}
	fmt.Printf("Columns: %v\n", columns)

	// Read and display data
	fmt.Println("\nData:")
	for rows.Next() {
		var id int
		var name string
		var age int
		var city string

		// Note: DuckDB orders columns alphabetically
		err := rows.Scan(&age, &city, &id, &name)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("ID: %d, Name: %s, Age: %d, City: %s\n", id, name, age, city)
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error reading rows:", err)
	}

	fmt.Println("\n2. Reading JSON with Filtering:")

	// Read JSON using Go API with filtering
	rows, err = db.Query("SELECT * FROM read_json('sample_data.json') WHERE age > 25")
	if err != nil {
		log.Fatal("Failed to query JSON:", err)
	}
	defer rows.Close()

	fmt.Println("People older than 25:")
	for rows.Next() {
		var id int
		var name string
		var age int
		var city string

		// Note: DuckDB orders columns alphabetically
		err := rows.Scan(&age, &city, &id, &name)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("- %s (%d years old) from %s\n", name, age, city)
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error reading rows:", err)
	}

	fmt.Println("\n3. JSON Statistics:")

	// Get statistics about the JSON data
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM read_json('sample_data.json')").Scan(&count)
	if err != nil {
		log.Fatal("Failed to count rows:", err)
	}
	fmt.Printf("Total records: %d\n", count)

	// Calculate average age
	var avgAge float64
	err = db.QueryRow("SELECT AVG(age) FROM read_json('sample_data.json')").Scan(&avgAge)
	if err != nil {
		log.Fatal("Failed to calculate average age:", err)
	}
	fmt.Printf("Average age: %.2f\n", avgAge)

	// Count people per city
	fmt.Println("\nPeople per city:")
	rows, err = db.Query("SELECT city, COUNT(*) as city_count FROM read_json('sample_data.json') GROUP BY city ORDER BY COUNT(*) DESC")
	if err != nil {
		log.Fatal("Failed to query city counts:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var city string
		var count int

		// Note: DuckDB orders columns alphabetically
		err := rows.Scan(&city, &count)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("  %s: %d\n", city, count)
	}

	fmt.Println("\n✓ JSON reading example completed successfully!")
}
