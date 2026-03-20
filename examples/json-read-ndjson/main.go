package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

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

	// Create sample NDJSON (newline-delimited JSON) data
	// Each line is a separate JSON object
	sampleNDJSON := `{"id": 1, "name": "Alice", "age": 25, "city": "New York", "salary": 75000}
{"id": 2, "name": "Bob", "age": 30, "city": "San Francisco", "salary": 95000}
{"id": 3, "name": "Charlie", "age": 28, "city": "Chicago", "salary": 85000}
{"id": 4, "name": "Diana", "age": 35, "city": "Boston", "salary": 105000}
{"id": 5, "name": "Eve", "age": 22, "city": "Seattle", "salary": 70000}`

	// Write sample NDJSON to file
	ndjsonPath := "sample_data.ndjson"
	err = os.WriteFile(ndjsonPath, []byte(sampleNDJSON), 0644)
	if err != nil {
		log.Fatal("Failed to write NDJSON file:", err)
	}
	defer os.Remove(ndjsonPath) // Clean up after example

	fmt.Println("=== NDJSON Reading Example ===")
	fmt.Println("\nNDJSON Format (Newline-Delimited JSON):")
	fmt.Println("Each line contains a complete JSON object")
	fmt.Println(strings.Repeat("-", 50))

	fmt.Println("\n1. Reading NDJSON using SQL:")

	// Read NDJSON using SQL query
	rows, err := db.Query("SELECT * FROM read_ndjson('sample_data.ndjson')")
	if err != nil {
		log.Fatal("Failed to read NDJSON:", err)
	}
	defer rows.Close()

	// Print column information
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Failed to get columns:", err)
	}
	fmt.Printf("Columns: %v\n", columns)

	// Read and display data
	fmt.Println("\nAll Records:")
	for rows.Next() {
		var id int
		var name string
		var age int
		var city string
		var salary int

		// Note: DuckDB orders columns alphabetically
		err := rows.Scan(&age, &city, &id, &name, &salary)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("ID: %d | Name: %-10s | Age: %d | City: %-15s | Salary: $%d\n",
			id, name, age, city, salary)
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error reading rows:", err)
	}

	fmt.Println("\n2. Reading NDJSON with Filtering:")

	// Read NDJSON with filtering
	rows, err = db.Query("SELECT * FROM read_ndjson('sample_data.ndjson') WHERE age > 28")
	if err != nil {
		log.Fatal("Failed to query NDJSON:", err)
	}
	defer rows.Close()

	fmt.Println("Employees older than 28:")
	for rows.Next() {
		var id int
		var name string
		var age int
		var city string
		var salary int

		// Note: DuckDB orders columns alphabetically
		err := rows.Scan(&age, &city, &id, &name, &salary)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("- %s, age %d, earning $%d\n", name, age, salary)
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error reading rows:", err)
	}

	fmt.Println("\n3. NDJSON Statistics:")

	// Get statistics about the NDJSON data
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM read_ndjson('sample_data.ndjson')").Scan(&count)
	if err != nil {
		log.Fatal("Failed to count rows:", err)
	}
	fmt.Printf("Total records: %d\n", count)

	// Calculate average salary
	var avgSalary float64
	err = db.QueryRow("SELECT AVG(salary) FROM read_ndjson('sample_data.ndjson')").Scan(&avgSalary)
	if err != nil {
		log.Fatal("Failed to calculate average salary:", err)
	}
	fmt.Printf("Average salary: $%.2f\n", avgSalary)

	// Find highest paid employee
	var topName string
	var topSalary int
	err = db.QueryRow("SELECT name, salary FROM read_ndjson('sample_data.ndjson') ORDER BY salary DESC LIMIT 1").Scan(&topName, &topSalary)
	if err != nil {
		log.Fatal("Failed to get top salary:", err)
	}
	fmt.Printf("Highest paid: %s ($%d)\n", topName, topSalary)

	// Group by city
	fmt.Println("\nSalary information by city:")
	rows, err = db.Query("SELECT city, COUNT(*) as emp_count, AVG(salary) as avg_sal FROM read_ndjson('sample_data.ndjson') GROUP BY city ORDER BY AVG(salary) DESC")
	if err != nil {
		log.Fatal("Failed to query city stats:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var city string
		var count int
		var avgSal float64

		// Column order: city, emp_count, avg_sal
		err := rows.Scan(&city, &count, &avgSal)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("  %s: %d employees, avg salary: $%.2f\n", city, count, avgSal)
	}

	fmt.Println("\n4. NDJSON Advanced Query:")

	// More complex query using a calculated average
	fmt.Println("Employees earning more than $85000:")
	rows, err = db.Query(`
		SELECT age, name, salary 
		FROM read_ndjson('sample_data.ndjson') 
		WHERE salary > 85000
		ORDER BY salary DESC
	`)
	if err != nil {
		log.Fatal("Failed to execute advanced query:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var age int
		var name string
		var salary int

		// Column order matches SELECT (age, name, salary)
		err := rows.Scan(&age, &name, &salary)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("- %s (%d years old): $%d\n", name, age, salary)
	}

	fmt.Println("\n✓ NDJSON reading example completed successfully!")
}
