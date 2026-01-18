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
	// Create a sample NDJSON file
	ndjsonData := `{"id": 1, "name": "Alice", "age": 30, "city": "New York", "timestamp": "2024-01-15T10:00:00Z"}
{"id": 2, "name": "Bob", "age": 25, "city": "San Francisco", "timestamp": "2024-01-15T11:00:00Z"}
{"id": 3, "name": "Charlie", "age": 35, "city": "Chicago", "timestamp": "2024-01-15T12:00:00Z"}
{"id": 4, "name": "Diana", "age": 28, "city": "Boston", "timestamp": "2024-01-15T13:00:00Z"}
{"id": 5, "name": "Eve", "age": 32, "city": "Seattle", "timestamp": "2024-01-15T14:00:00Z"}`

	// Write sample data to file
	sampleFile := "events.ndjson"
	err := os.WriteFile(sampleFile, []byte(ndjsonData), 0644)
	if err != nil {
		log.Fatalf("Failed to write sample NDJSON file: %v", err)
	}
	defer os.Remove(sampleFile)

	// Connect to in-memory database
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Example 1: Basic NDJSON reading using read_ndjson()
	fmt.Println("=== Example 1: Basic NDJSON Reading (SQL) ===")
	query := fmt.Sprintf("SELECT * FROM read_ndjson('%s')", sampleFile)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read NDJSON: %v", err)
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
		var timestamp string

		err := rows.Scan(&age, &city, &id, &name, &timestamp)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf(
			"ID: %d, Name: %s, Age: %d, City: %s, Time: %s\n",
			id,
			name,
			age,
			city,
			timestamp,
		)
	}
	if err = rows.Err(); err != nil {
		log.Fatalf("Error reading rows: %v", err)
	}

	// Example 2: Reading NDJSON with format option using read_json()
	fmt.Println("\n=== Example 2: Using read_json() with NDJSON format ===")
	query = fmt.Sprintf("SELECT * FROM read_json('%s', format = 'newline_delimited')", sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read NDJSON: %v", err)
	}
	defer rows.Close()

	fmt.Println("Data (using read_json with format option):")
	for rows.Next() {
		var age int
		var city string
		var id int
		var name string
		var timestamp string

		err := rows.Scan(&age, &city, &id, &name, &timestamp)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("ID: %d, Name: %s, Age: %d, City: %s\n", id, name, age, city)
	}

	// Example 3: Querying NDJSON data with time-based filters
	fmt.Println("\n=== Example 3: Time-based Queries ===")
	query = fmt.Sprintf(
		"SELECT name, city, timestamp FROM read_ndjson('%s') WHERE timestamp \u003e '2024-01-15T11:30:00Z' ORDER BY timestamp",
		sampleFile,
	)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to query NDJSON: %v", err)
	}
	defer rows.Close()

	fmt.Println("Users after 11:30 AM:")
	for rows.Next() {
		var name string
		var city string
		var timestamp string

		err := rows.Scan(&name, &city, &timestamp)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("  %s from %s at %s\n", name, city, timestamp)
	}

	// Example 4: Aggregating NDJSON data
	fmt.Println("\n=== Example 4: Aggregating NDJSON Data ===")
	query = fmt.Sprintf(
		"SELECT city, COUNT(*), AVG(age) FROM read_ndjson('%s') GROUP BY city ORDER BY COUNT(*) DESC",
		sampleFile,
	)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to aggregate NDJSON: %v", err)
	}
	defer rows.Close()

	fmt.Println("City statistics:")
	for rows.Next() {
		var city string
		var count int
		var avgAge float64

		err := rows.Scan(&city, &count, &avgAge)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("  %s: %d users, average age %.1f\n", city, count, avgAge)
	}

	// Example 5: Querying NDJSON with complex conditions
	fmt.Println("\n=== Example 5: Complex Queries on NDJSON ===")
	query = fmt.Sprintf(`SELECT name, city,
		CASE
			WHEN age < 30 THEN 'Young'
			WHEN age < 35 THEN 'Middle'
			ELSE 'Senior'
		END as age_group
	FROM read_ndjson('%s')
	ORDER BY age DESC`, sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to query NDJSON: %v", err)
	}
	defer rows.Close()

	fmt.Println("Users by age group:")
	for rows.Next() {
		var name string
		var city string
		var ageGroup string

		err := rows.Scan(&name, &city, &ageGroup)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("  %s from %s: %s\n", name, city, ageGroup)
	}

	// Example 6: Working with large NDJSON files (simulated)
	fmt.Println("\n=== Example 6: Processing Large NDJSON Files ===")

	// Create a larger NDJSON file
	largeFile := "large_events.ndjson"
	file, err := os.Create(largeFile)
	if err != nil {
		log.Fatalf("Failed to create large file: %v", err)
	}

	// Write 1000 records
	for i := 0; i < 1000; i++ {
		record := fmt.Sprintf(
			`{"id": %d, "event_type": "click", "user_id": %d, "timestamp": "2024-01-15T%02d:%02d:%02dZ"}`,
			i+1,
			(i%100)+1,
			(i/60)%24,
			i%60,
			i%60,
		)
		file.WriteString(record + "\n")
	}
	file.Close()
	defer os.Remove(largeFile)

	// Query with LIMIT
	query = fmt.Sprintf("SELECT COUNT(*) as total FROM read_ndjson('%s')", largeFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to count large NDJSON: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var total int
		err := rows.Scan(&total)
		if err != nil {
			log.Fatalf("Failed to scan count: %v", err)
		}
		fmt.Printf("Total events in large file: %d\n", total)
	}

	// Example 7: Error handling for malformed NDJSON
	fmt.Println("\n=== Example 7: Error Handling ===")

	// Create a malformed NDJSON file
	malformedFile := "malformed.ndjson"
	malformedData := `{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
This is not valid JSON
{"id": 3, "name": "Charlie"}`

	err = os.WriteFile(malformedFile, []byte(malformedData), 0644)
	if err != nil {
		log.Fatalf("Failed to write malformed file: %v", err)
	}
	defer os.Remove(malformedFile)

	// Try to read with ignore_errors option
	query = fmt.Sprintf("SELECT * FROM read_ndjson('%s', ignore_errors = true)", malformedFile)
	rows, err = db.Query(query)
	if err != nil {
		fmt.Printf("Error reading malformed NDJSON: %v\n", err)
	} else {
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
			var id int
			var name string
			err := rows.Scan(&id, &name)
			if err != nil {
				continue
			}
		}
		fmt.Printf("Successfully read %d valid records from malformed file\n", count)
	}

	// Example 8: Working with compressed NDJSON
	fmt.Println("\n=== Example 8: Working with File Paths ===")
	absPath, err := filepath.Abs(sampleFile)
	if err != nil {
		log.Fatalf("Failed to get absolute path: %v", err)
	}

	query = fmt.Sprintf(
		"SELECT COUNT(DISTINCT city) as unique_cities FROM read_ndjson('%s')",
		absPath,
	)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read with absolute path: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var uniqueCities int
		err := rows.Scan(&uniqueCities)
		if err != nil {
			log.Fatalf("Failed to scan count: %v", err)
		}
		fmt.Printf("Unique cities: %d\n", uniqueCities)
	}

	fmt.Println("\nAll examples completed successfully!")
}
