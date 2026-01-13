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

	// Create sample CSV file
	sampleCSV := `id,name,age,city
1,Alice,25,New York
2,Bob,30,San Francisco
3,Charlie,28,Chicago
4,Diana,35,Boston
5,Eve,22,Seattle`

	// Write sample CSV to file
	csvPath := "sample_data.csv"
	err = os.WriteFile(csvPath, []byte(sampleCSV), 0644)
	if err != nil {
		log.Fatal("Failed to write CSV file:", err)
	}
	defer os.Remove(csvPath) // Clean up after example

	fmt.Println("=== Basic CSV Reading Example ===")
	fmt.Println("\n1. Reading CSV using SQL:")

	// Read CSV using SQL query
	rows, err := db.Query("SELECT * FROM read_csv('sample_data.csv')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
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

		err := rows.Scan(&id, &name, &age, &city)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("ID: %d, Name: %s, Age: %d, City: %s\n", id, name, age, city)
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error reading rows:", err)
	}

	fmt.Println("\n2. Reading CSV with Go API:")

	// Read CSV using Go API
	rows, err = db.Query("SELECT * FROM read_csv('sample_data.csv') WHERE age > 25")
	if err != nil {
		log.Fatal("Failed to query CSV:", err)
	}
	defer rows.Close()

	fmt.Println("People older than 25:")
	for rows.Next() {
		var id int
		var name string
		var age int
		var city string

		err := rows.Scan(&id, &name, &age, &city)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("- %s (%d years old) from %s\n", name, age, city)
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error reading rows:", err)
	}

	fmt.Println("\n3. CSV Statistics:")

	// Get statistics about the CSV
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM read_csv('sample_data.csv')").Scan(&count)
	if err != nil {
		log.Fatal("Failed to count rows:", err)
	}
	fmt.Printf("Total rows: %d\n", count)

	// Calculate average age
	var avgAge float64
	err = db.QueryRow("SELECT AVG(age) FROM read_csv('sample_data.csv')").Scan(&avgAge)
	if err != nil {
		log.Fatal("Failed to calculate average age:", err)
	}
	fmt.Printf("Average age: %.2f\n", avgAge)

	fmt.Println("\n✓ CSV reading example completed successfully!")
}
