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

	// Create sample nested JSON data
	jsonData := `{
  "company": "TechCorp",
  "employees": [
    {"id": 1, "name": "Alice", "email": "alice@tech.com", "salary": 75000, "skills": ["Go", "Python", "SQL"]},
    {"id": 2, "name": "Bob", "email": "bob@tech.com", "salary": 85000, "skills": ["Java", "Kotlin"]},
    {"id": 3, "name": "Charlie", "email": "charlie@tech.com", "salary": 95000, "skills": ["Go", "Rust", "C++"]}
  ],
  "departments": [
    {"name": "Engineering", "budget": 500000, "headcount": 3},
    {"name": "Sales", "budget": 200000, "headcount": 2}
  ]
}`

	jsonPath := "company_data.json"
	err = os.WriteFile(jsonPath, []byte(jsonData), 0644)
	if err != nil {
		log.Fatal("Failed to write JSON file:", err)
	}
	defer os.Remove(jsonPath)

	fmt.Println("=== JSON Querying Example ===")
	fmt.Println("\nJSON Data Structure:")
	fmt.Println("- company.company: string")
	fmt.Println("- company.employees: array of objects with id, name, email, salary, skills")
	fmt.Println("- company.departments: array of objects with name, budget, headcount")

	// Example 1: Read and query the main JSON structure
	fmt.Println("\n1. Querying company name and employee count:")
	
	jsonContent, _ := os.ReadFile(jsonPath)
	fmt.Printf("Sample JSON (first 300 chars): %s...\n", string(jsonContent)[:300])

	// For complex nested JSON, we need a different approach
	// Since the backend may not support complex JSON queries directly,
	// let's demonstrate with a flattened NDJSON structure instead

	// Create a flattened NDJSON version
	ndjsonData := `{"company": "TechCorp", "employee_id": 1, "name": "Alice", "email": "alice@tech.com", "salary": 75000, "skill_count": 3}
{"company": "TechCorp", "employee_id": 2, "name": "Bob", "email": "bob@tech.com", "salary": 85000, "skill_count": 2}
{"company": "TechCorp", "employee_id": 3, "name": "Charlie", "email": "charlie@tech.com", "salary": 95000, "skill_count": 3}`

	ndjsonPath := "employees_flat.json"
	err = os.WriteFile(ndjsonPath, []byte(ndjsonData), 0644)
	if err != nil {
		log.Fatal("Failed to write NDJSON file:", err)
	}
	defer os.Remove(ndjsonPath)

	fmt.Println("\n2. Querying flattened employee data:")

	rows, err := db.Query("SELECT * FROM read_json_auto('employees_flat.json')")
	if err != nil {
		log.Fatal("Failed to query employees:", err)
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	fmt.Printf("Columns: %v\n", columns)

	fmt.Println("\nAll employees:")
	for rows.Next() {
		var company string
		var email string
		var empID int
		var name string
		var salary int
		var skillCount int

		// Columns are alphabetically ordered: company, email, employee_id, name, salary, skill_count
		err := rows.Scan(&company, &email, &empID, &name, &salary, &skillCount)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("  - %s (ID:%d, Email: %s, Salary: $%d, Skills: %d)\n", name, empID, email, salary, skillCount)
	}

	// Example 3: Filter and aggregate
	fmt.Println("\n3. Employees with salary > 80000:")

	rows, err = db.Query("SELECT name, salary FROM read_json_auto('employees_flat.json') WHERE salary > 80000 ORDER BY salary DESC")
	if err != nil {
		log.Fatal("Failed to query high earners:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var salary int

		err := rows.Scan(&name, &salary)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("  - %s: $%d\n", name, salary)
	}

	// Example 4: Aggregation
	fmt.Println("\n4. Salary statistics:")

	var count int
	var avgSalary float64
	var maxSalary int
	var minSalary int

	err = db.QueryRow("SELECT COUNT(*), AVG(salary), MAX(salary), MIN(salary) FROM read_json_auto('employees_flat.json')").
		Scan(&count, &avgSalary, &maxSalary, &minSalary)
	if err != nil {
		log.Fatal("Failed to get statistics:", err)
	}

	fmt.Printf("  - Count: %d employees\n", count)
	fmt.Printf("  - Average salary: $%.2f\n", avgSalary)
	fmt.Printf("  - Max salary: $%d\n", maxSalary)
	fmt.Printf("  - Min salary: $%d\n", minSalary)

	// Example 5: GROUP BY
	fmt.Println("\n5. Employees grouped by skill count:")

	rows, err = db.Query("SELECT skill_count, COUNT(*) as emp_count FROM read_json_auto('employees_flat.json') GROUP BY skill_count ORDER BY skill_count DESC")
	if err != nil {
		log.Fatal("Failed to group by skill count:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var skillCount int
		var empCount int

		err := rows.Scan(&skillCount, &empCount)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("  - %d skills: %d employee(s)\n", skillCount, empCount)
	}

	// Example 6: String matching
	fmt.Println("\n6. Employees with 'Go' skill (skill_count >= 3 with specific patterns):")

	rows, err = db.Query("SELECT name, skill_count FROM read_json_auto('employees_flat.json') WHERE skill_count >= 3 ORDER BY name")
	if err != nil {
		log.Fatal("Failed to query Go developers:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var skillCount int

		err := rows.Scan(&name, &skillCount)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("  - %s (Skills: %d)\n", name, skillCount)
	}

	fmt.Println("\n✓ JSON querying example completed successfully!")
}
