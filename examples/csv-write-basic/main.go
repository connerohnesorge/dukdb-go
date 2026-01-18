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

	fmt.Println("=== Basic CSV Writing Example ===")

	// Create sample data
	fmt.Println("\n1. Creating sample data...")

	_, err = db.Exec(`
		CREATE TABLE employees AS
		SELECT
			1 as id, 'Alice Johnson' as name, 'Engineering' as department,
			75000 as salary, '2020-01-15'::date as hire_date
		UNION ALL
		SELECT 2, 'Bob Smith', 'Marketing', 65000, '2019-06-20'::date
		UNION ALL
		SELECT 3, 'Charlie Brown', 'Sales', 55000, '2021-03-10'::date
		UNION ALL
		SELECT 4, 'Diana Prince', 'HR', 60000, '2018-11-05'::date
		UNION ALL
		SELECT 5, 'Eve Wilson', 'Engineering', 80000, '2022-01-30'::date
	`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Example 1: Basic CSV export using COPY TO
	fmt.Println("\n2. Basic CSV export using COPY TO:")

	outputFile := "employees_basic.csv"
	_, err = db.Exec(fmt.Sprintf("COPY employees TO '%s'", outputFile))
	if err != nil {
		log.Fatal("Failed to export CSV:", err)
	}
	defer os.Remove(outputFile)

	// Read and display the exported file
	content, err := os.ReadFile(outputFile)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}

	fmt.Printf("Exported to %s:\n%s\n", outputFile, string(content))

	// Example 2: Export query results to CSV
	fmt.Println("\n3. Export query results to CSV:")

	outputFile = "high_earners.csv"
	query := `
		COPY (
			SELECT name, department, salary,
				EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM hire_date) as years_employed
			FROM employees
			WHERE salary > 60000
			ORDER BY salary DESC
		) TO '%s'
	`
	_, err = db.Exec(fmt.Sprintf(query, outputFile))
	if err != nil {
		log.Fatal("Failed to export query results:", err)
	}
	defer os.Remove(outputFile)

	content, err = os.ReadFile(outputFile)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}

	fmt.Printf("Exported high earners to %s:\n%s\n", outputFile, string(content))

	// Example 3: Export aggregated data
	fmt.Println("\n4. Export aggregated department statistics:")

	outputFile = "dept_stats.csv"
	aggQuery := `
		COPY (
			SELECT
				department,
				COUNT(*) as employee_count,
				ROUND(AVG(salary), 2) as avg_salary,
				MIN(salary) as min_salary,
				MAX(salary) as max_salary
			FROM employees
			GROUP BY department
			ORDER BY avg_salary DESC
		) TO '%s' WITH (HEADER true)
	`

	_, err = db.Exec(fmt.Sprintf(aggQuery, outputFile))
	if err != nil {
		log.Fatal("Failed to export aggregated data:", err)
	}
	defer os.Remove(outputFile)

	content, err = os.ReadFile(outputFile)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}

	fmt.Printf("Exported department stats to %s:\n%s\n", outputFile, string(content))

	// Example 4: Verify exported CSV by reading it back
	fmt.Println("\n5. Verifying exported CSV by reading it back:")

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM read_csv('%s')", outputFile))
	if err != nil {
		log.Fatal("Failed to read exported CSV:", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Failed to get columns:", err)
	}
	fmt.Printf("Columns: %v\n", columns)

	fmt.Println("\nData from exported CSV:")
	for rows.Next() {
		var dept string
		var count int
		var avgSal, minSal, maxSal float64

		err := rows.Scan(&dept, &count, &avgSal, &minSal, &maxSal)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("%s: %d employees, Avg: $%.2f, Min: $%.2f, Max: $%.2f\n",
			dept, count, avgSal, minSal, maxSal)
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error reading rows:", err)
	}

	fmt.Println("\n✓ CSV writing example completed successfully!")
}
