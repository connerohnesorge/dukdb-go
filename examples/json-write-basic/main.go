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

	fmt.Println("=== JSON Write Example ===")

	// Create sample data in a table
	fmt.Println("\n1. Creating sample data:")

	// Create employees table
	_, err = db.Exec(`
		CREATE TABLE employees (id INT, name VARCHAR, department VARCHAR, salary INT)
	`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Insert sample data
	_, err = db.Exec(`
		INSERT INTO employees VALUES
		(1, 'Alice', 'Engineering', 75000),
		(2, 'Bob', 'Marketing', 65000),
		(3, 'Charlie', 'Engineering', 80000),
		(4, 'Diana', 'Sales', 70000),
		(5, 'Eve', 'Engineering', 85000)
	`)
	if err != nil {
		log.Fatal("Failed to insert data:", err)
	}

	// Verify data was created
	var count int
	db.QueryRow("SELECT COUNT(*) FROM employees").Scan(&count)
	fmt.Printf("Created table with %d employees\n", count)

	// Display the data
	fmt.Println("\nEmployee Data:")
	rows, err := db.Query("SELECT id, name, department, salary FROM employees ORDER BY id")
	if err != nil {
		log.Fatal("Failed to query employees:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		var department string
		var salary int

		err := rows.Scan(&id, &name, &department, &salary)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("  [%d] %s - %s - $%d\n", id, name, department, salary)
	}

	// Example 1: Export to JSON array format
	fmt.Println("\n2. Exporting to JSON array format:")

	arrayOutputPath := "employees_array.json"
	_, err = db.Exec(fmt.Sprintf(`
		COPY (SELECT * FROM employees ORDER BY id)
		TO '%s'
		(FORMAT JSON)
	`, arrayOutputPath))
	if err != nil {
		log.Fatal("Failed to export to JSON array:", err)
	}

	// Read and display the exported file
	data, err := os.ReadFile(arrayOutputPath)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}
	defer os.Remove(arrayOutputPath)

	fmt.Println("Generated JSON Array Format:")
	fmt.Println(string(data))

	// Example 2: Export to NDJSON format
	fmt.Println("\n3. Exporting to NDJSON format:")

	ndjsonOutputPath := "employees_ndjson.json"
	_, err = db.Exec(fmt.Sprintf(`
		COPY (SELECT * FROM employees ORDER BY id)
		TO '%s'
		(FORMAT NDJSON)
	`, ndjsonOutputPath))
	if err != nil {
		log.Fatal("Failed to export to NDJSON:", err)
	}

	// Read and display the exported file
	data, err = os.ReadFile(ndjsonOutputPath)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}
	defer os.Remove(ndjsonOutputPath)

	fmt.Println("Generated NDJSON Format:")
	fmt.Println(string(data))

	// Example 3: Export specific columns with filtering
	fmt.Println("\n4. Exporting filtered data (Engineering department only):")

	filteredOutputPath := "engineering_only.json"
	_, err = db.Exec(fmt.Sprintf(`
		COPY (
			SELECT id, name, salary 
			FROM employees 
			WHERE department = 'Engineering'
			ORDER BY salary DESC
		)
		TO '%s'
		(FORMAT NDJSON)
	`, filteredOutputPath))
	if err != nil {
		log.Fatal("Failed to export filtered data:", err)
	}

	data, err = os.ReadFile(filteredOutputPath)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}
	defer os.Remove(filteredOutputPath)

	fmt.Println("Engineering Employees (NDJSON):")
	fmt.Println(string(data))

	// Example 4: Export selected columns with different order
	fmt.Println("\n5. Exporting selected columns in custom order:")

	selectedOutputPath := "simple_export.json"
	_, err = db.Exec(fmt.Sprintf(`
		COPY (
			SELECT name, department
			FROM employees
			ORDER BY name
		)
		TO '%s'
		(FORMAT NDJSON)
	`, selectedOutputPath))
	if err != nil {
		log.Fatal("Failed to export selected columns:", err)
	}

	data, err = os.ReadFile(selectedOutputPath)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}
	defer os.Remove(selectedOutputPath)

	fmt.Println("Names and Departments (NDJSON):")
	fmt.Println(string(data))

	// Example 5: Export using SELECT with COPY
	fmt.Println("\n6. Verifying exported data can be read back:")

	// Verify we can read the exported JSON array
	var verifyCount int
	err = db.QueryRow("SELECT COUNT(*) FROM read_json_auto('employees_array.json')").Scan(&verifyCount)
	if err == nil {
		fmt.Printf("✓ Successfully exported and verified %d records in JSON array\n", verifyCount)
	}

	// Verify we can read the exported NDJSON
	err = db.QueryRow("SELECT COUNT(*) FROM read_json_auto('employees_ndjson.json')").Scan(&verifyCount)
	if err == nil {
		fmt.Printf("✓ Successfully exported and verified %d records in NDJSON\n", verifyCount)
	}

	// Clean up
	_, err = db.Exec("DROP TABLE employees")
	if err != nil {
		log.Fatal("Failed to drop table:", err)
	}

	fmt.Println("\n✓ JSON write example completed successfully!")
}
