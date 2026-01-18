package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
	// Create a sample JSON file
	jsonData := `[
		{"id": 1, "name": "Alice", "age": 30, "city": "New York"},
		{"id": 2, "name": "Bob", "age": 25, "city": "San Francisco"},
		{"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"},
		{"id": 4, "name": "Diana", "age": 28, "city": "Boston"},
		{"id": 5, "name": "Eve", "age": 32, "city": "Seattle"}
	]`

	// Write sample data to file
	sampleFile := "users.json"
	err := os.WriteFile(sampleFile, []byte(jsonData), 0644)
	if err != nil {
		log.Fatalf("Failed to write sample JSON file: %v", err)
	}
	defer os.Remove(sampleFile)

	// Connect to in-memory database
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Example 1: Basic JSON reading using SQL
	fmt.Println("=== Example 1: Basic JSON Reading (SQL) ===")
	query := fmt.Sprintf("SELECT * FROM read_json('%s')", sampleFile)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read JSON: %v", err)
	}
	defer rows.Close()

	// Print column information
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}
	fmt.Printf("Columns: %v\n\n", columns)

	// Print data
	fmt.Println("Data:")
	for rows.Next() {
		var age int
		var city string
		var id int
		var name string

		err := rows.Scan(&age, &city, &id, &name)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("ID: %d, Name: %s, Age: %d, City: %s\n", id, name, age, city)
	}
	if err = rows.Err(); err != nil {
		log.Fatalf("Error reading rows: %v", err)
	}

	// Example 2: Reading JSON with specific columns
	fmt.Println("\n=== Example 2: Reading Specific Columns ===")
	query = fmt.Sprintf("SELECT name, age FROM read_json('%s') WHERE age > 30", sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read JSON: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var age int

		err := rows.Scan(&name, &age)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("Name: %s, Age: %d\n", name, age)
	}

	// Example 3: Creating a view from JSON data
	fmt.Println("\n=== Example 3: Creating View from JSON ===")
	query = fmt.Sprintf("CREATE VIEW users_view AS SELECT * FROM read_json('%s')", sampleFile)
	_, err = db.Exec(query)
	if err != nil {
		log.Fatalf("Failed to create view: %v", err)
	}

	// Query the view
	rows, err = db.Query(
		"SELECT city, COUNT(*) FROM users_view GROUP BY city ORDER BY COUNT(*) DESC",
	)
	if err != nil {
		log.Fatalf("Failed to query view: %v", err)
	}
	defer rows.Close()

	fmt.Println("Users per city:")
	for rows.Next() {
		var city string
		var count int

		err := rows.Scan(&city, &count)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("%s: %d users\n", city, count)
	}

	// Example 4: Using Go API to read JSON
	fmt.Println("\n=== Example 4: Using Go API ===")
	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Execute query and get results
	err = conn.Raw(func(driverConn interface{}) error {
		// This would use the raw connection interface if needed
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to use raw connection: %v", err)
	}

	// Example 5: Error handling for missing file
	fmt.Println("\n=== Example 5: Error Handling ===")
	_, err = db.Query("SELECT * FROM read_json('nonexistent.json')")
	if err != nil {
		fmt.Printf("Expected error for missing file: %v\n", err)
	}

	// Example 6: Working with absolute paths
	fmt.Println("\n=== Example 6: Absolute Paths ===")
	absPath, err := filepath.Abs(sampleFile)
	if err != nil {
		log.Fatalf("Failed to get absolute path: %v", err)
	}

	query = fmt.Sprintf("SELECT COUNT(*) as total FROM read_json('%s')", absPath)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read with absolute path: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var total int
		err := rows.Scan(&total)
		if err != nil {
			log.Fatalf("Failed to scan count: %v", err)
		}
		fmt.Printf("Total records: %d\n", total)
	}

	fmt.Println("\nAll examples completed successfully!")
}
