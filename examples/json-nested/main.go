package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
	// Create complex nested JSON data
	sampleData := `[
		{
			"company": "TechCorp",
			"employees": [
				{
					"id": 101,
					"name": "Alice Johnson",
					"department": "Engineering",
					"salary": 95000,
					"skills": ["Go", "Python", "Kubernetes"],
					"address": {
						"street": "123 Tech St",
						"city": "San Francisco",
						"state": "CA",
						"zip": "94105"
					},
					"projects": [
						{
							"name": "Cloud Migration",
							"status": "completed",
							"budget": 500000,
							"timeline": {
								"start": "2023-01-01",
								"end": "2023-06-30"
							}
						},
						{
							"name": "API Redesign",
							"status": "in_progress",
							"budget": 200000,
							"timeline": {
								"start": "2023-07-01",
								"end": "2023-12-31"
							}
						}
					]
				},
				{
					"id": 102,
					"name": "Bob Smith",
					"department": "Marketing",
					"salary": 75000,
					"skills": ["SEO", "Content Marketing", "Analytics"],
					"address": {
						"street": "456 Market St",
						"city": "San Francisco",
						"state": "CA",
						"zip": "94103"
					},
					"projects": [
						{
							"name": "Brand Refresh",
							"status": "completed",
							"budget": 150000,
							"timeline": {
								"start": "2023-03-01",
								"end": "2023-05-31"
							}
						}
					]
				}
			],
			"departments": {
				"Engineering": {
					"head": "Alice Johnson",
					"budget": 2000000,
					"locations": ["San Francisco", "Seattle"]
				},
				"Marketing": {
					"head": "Bob Smith",
					"budget": 1000000,
					"locations": ["San Francisco"]
				}
			},
			"financials": {
				"revenue": 10000000,
				"expenses": 7500000,
				"profit": 2500000,
				"quarters": [
					{"q": "Q1", "revenue": 2000000, "profit": 400000},
					{"q": "Q2", "revenue": 2500000, "profit": 600000},
					{"q": "Q3", "revenue": 3000000, "profit": 800000},
					{"q": "Q4", "revenue": 2500000, "profit": 700000}
				]
			}
		},
		{
			"company": "DataFlow Inc",
			"employees": [
				{
					"id": 201,
					"name": "Carol Davis",
					"department": "Data Science",
					"salary": 110000,
					"skills": ["Python", "R", "Machine Learning"],
					"address": {
						"street": "789 Data Ave",
						"city": "Boston",
						"state": "MA",
						"zip": "02101"
					},
					"projects": [
						{
							"name": "Predictive Analytics",
							"status": "completed",
							"budget": 300000,
							"timeline": {
								"start": "2023-02-01",
								"end": "2023-08-31"
							}
						}
					]
				}
			],
			"departments": {
				"Data Science": {
					"head": "Carol Davis",
					"budget": 1500000,
					"locations": ["Boston"]
				}
			},
			"financials": {
				"revenue": 5000000,
				"expenses": 3500000,
				"profit": 1500000,
				"quarters": [
					{"q": "Q1", "revenue": 1000000, "profit": 200000},
					{"q": "Q2", "revenue": 1200000, "profit": 300000},
					{"q": "Q3", "revenue": 1400000, "profit": 500000},
					{"q": "Q4", "revenue": 1400000, "profit": 500000}
				]
			}
		}
	]`

	// Write sample data to file
	sampleFile := "companies.json"
	err := os.WriteFile(sampleFile, []byte(sampleData), 0644)
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

	// Example 1: Reading nested JSON structure
	fmt.Println("=== Example 1: Reading Nested JSON Structure ===")
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
	fmt.Println("Company data (showing nested structures as JSON strings):")
	for rows.Next() {
		var company string
		var departments string
		var employees string
		var financials string

		err := rows.Scan(&company, &departments, &employees, &financials)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("\nCompany: %s\n", company)
		fmt.Printf("  Employees (JSON): %s\n", truncateJSON(employees, 100))
		fmt.Printf("  Departments (JSON): %s\n", truncateJSON(departments, 100))
		fmt.Printf("  Financials (JSON): %s\n", truncateJSON(financials, 100))
	}
	if err = rows.Err(); err != nil {
		log.Fatalf("Error reading rows: %v", err)
	}

	// Example 2: Parsing nested JSON in Go
	fmt.Println("\n=== Example 2: Parsing Nested JSON in Go ===")

	// Read a single company's data
	query = fmt.Sprintf("SELECT * FROM read_json('%s') WHERE company = 'TechCorp'", sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to query TechCorp: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var company string
		var departments string
		var employees string
		var financials string

		err := rows.Scan(&company, &departments, &employees, &financials)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		// Parse employees JSON
		var empData []map[string]interface{}
		err = json.Unmarshal([]byte(employees), &empData)
		if err != nil {
			log.Fatalf("Failed to parse employees JSON: %v", err)
		}

		fmt.Printf("\nTechCorp has %d employees:\n", len(empData))
		for _, emp := range empData {
			fmt.Printf("  - %s (ID: %.0f), Dept: %s, Salary: $%.0f\n",
				emp["name"], emp["id"], emp["department"], emp["salary"])
		}

		// Parse financials JSON
		var finData map[string]interface{}
		err = json.Unmarshal([]byte(financials), &finData)
		if err != nil {
			log.Fatalf("Failed to parse financials JSON: %v", err)
		}

		fmt.Printf("\nTechCorp Financials:\n")
		fmt.Printf("  Revenue: $%.0f\n", finData["revenue"])
		fmt.Printf("  Expenses: $%.0f\n", finData["expenses"])
		fmt.Printf("  Profit: $%.0f\n", finData["profit"])

		// Parse quarters data
		if quarters, ok := finData["quarters"].([]interface{}); ok {
			fmt.Printf("  Quarterly breakdown:\n")
			for _, q := range quarters {
				if quarter, ok := q.(map[string]interface{}); ok {
					fmt.Printf("    %s: Revenue=$%.0f, Profit=$%.0f\n",
						quarter["q"], quarter["revenue"], quarter["profit"])
				}
			}
		}
	}

	// Example 3: Creating a flattened view
	fmt.Println("\n=== Example 3: Creating Flattened Views ===")

	// Create a view with flattened employee data
	_, err = db.Exec(fmt.Sprintf(`
		CREATE VIEW company_employees AS
		SELECT
			company,
			employees,
			financials
		FROM read_json('%s')
	`, sampleFile))
	if err != nil {
		log.Fatalf("Failed to create view: %v", err)
	}
	defer db.Exec("DROP VIEW company_employees")

	// Query the view
	rows, err = db.Query("SELECT * FROM company_employees ORDER BY company")
	if err != nil {
		log.Fatalf("Failed to query view: %v", err)
	}
	defer rows.Close()

	fmt.Println("Companies in view:")
	for rows.Next() {
		var company string
		var employees string
		var financials string

		err := rows.Scan(&company, &employees, &financials)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("  - %s\n", company)
	}

	// Example 4: Aggregating nested data
	fmt.Println("\n=== Example 4: Aggregating Nested Data ===")

	// Process all companies to calculate aggregate statistics
	query = fmt.Sprintf("SELECT * FROM read_json('%s')", sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read all companies: %v", err)
	}
	defer rows.Close()

	totalRevenue := 0.0
	totalProfit := 0.0
	totalEmployees := 0

	for rows.Next() {
		var company string
		var departments string
		var employees string
		var financials string

		err := rows.Scan(&company, &departments, &employees, &financials)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		// Parse financials to get revenue and profit
		var finData map[string]interface{}
		err = json.Unmarshal([]byte(financials), &finData)
		if err != nil {
			continue
		}

		// Parse employees to count them
		var empData []map[string]interface{}
		err = json.Unmarshal([]byte(employees), &empData)
		if err != nil {
			continue
		}

		totalRevenue += finData["revenue"].(float64)
		totalProfit += finData["profit"].(float64)
		totalEmployees += len(empData)

		fmt.Printf("  %s: %d employees, Revenue: $%.0f, Profit: $%.0f\n",
			company, len(empData), finData["revenue"], finData["profit"])
	}

	fmt.Printf("\nAggregate Statistics:\n")
	fmt.Printf("  Total Companies: 2\n")
	fmt.Printf("  Total Employees: %d\n", totalEmployees)
	fmt.Printf("  Total Revenue: $%.0f\n", totalRevenue)
	fmt.Printf("  Total Profit: $%.0f\n", totalProfit)
	fmt.Printf("  Average Profit Margin: %.1f%%\n", (totalProfit/totalRevenue)*100)

	// Example 5: Extracting specific nested values
	fmt.Println("\n=== Example 5: Extracting Specific Values ===")

	// Create a more focused query for department heads
	query = fmt.Sprintf("SELECT company, departments FROM read_json('%s')", sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to query departments: %v", err)
	}
	defer rows.Close()

	fmt.Println("Department heads by company:")
	for rows.Next() {
		var company string
		var departments string

		err := rows.Scan(&company, &departments)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		// Parse departments
		var deptData map[string]interface{}
		err = json.Unmarshal([]byte(departments), &deptData)
		if err != nil {
			continue
		}

		fmt.Printf("\n%s:\n", company)
		for deptName, deptInfo := range deptData {
			if dept, ok := deptInfo.(map[string]interface{}); ok {
				if head, ok := dept["head"].(string); ok {
					fmt.Printf("  %s Department: Head = %s\n", deptName, head)
				}
			}
		}
	}

	// Example 6: Working with arrays in JSON
	fmt.Println("\n=== Example 6: Working with Arrays ===")

	// Query to get employee skills
	query = fmt.Sprintf("SELECT company, employees FROM read_json('%s')", sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to query employees: %v", err)
	}
	defer rows.Close()

	fmt.Println("Employee skills by company:")
	for rows.Next() {
		var company string
		var employees string

		err := rows.Scan(&company, &employees)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		var empData []map[string]interface{}
		err = json.Unmarshal([]byte(employees), &empData)
		if err != nil {
			continue
		}

		fmt.Printf("\n%s:\n", company)
		for _, emp := range empData {
			if skills, ok := emp["skills"].([]interface{}); ok {
				fmt.Printf("  %s: ", emp["name"])
				for i, skill := range skills {
					if i > 0 {
						fmt.Printf(", ")
					}
					fmt.Printf("%s", skill.(string))
				}
				fmt.Println()
			}
		}
	}

	// Example 7: Creating summary reports
	fmt.Println("\n=== Example 7: Creating Summary Reports ===")

	// Generate a comprehensive report
	report := make(map[string]interface{})
	companies := make([]map[string]interface{}, 0)

	query = fmt.Sprintf("SELECT * FROM read_json('%s')", sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read all data: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var company string
		var departments string
		var employees string
		var financials string

		err := rows.Scan(&company, &departments, &employees, &financials)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		// Parse all data
		var compData = make(map[string]interface{})
		compData["name"] = company

		// Parse employees
		var empData []map[string]interface{}
		json.Unmarshal([]byte(employees), &empData)
		compData["employee_count"] = len(empData)

		// Parse financials
		var finData map[string]interface{}
		json.Unmarshal([]byte(financials), &finData)
		compData["revenue"] = finData["revenue"]
		compData["profit"] = finData["profit"]

		// Calculate metrics
		if finData["revenue"].(float64) > 0 {
			compData["profit_margin"] = (finData["profit"].(float64) / finData["revenue"].(float64)) * 100
		}

		companies = append(companies, compData)
	}

	report["companies"] = companies
	report["summary"] = map[string]interface{}{
		"total_companies": len(companies),
		"total_employees": totalEmployees,
		"total_revenue":   totalRevenue,
		"total_profit":    totalProfit,
	}

	// Save report to file
	reportFile := "company_summary.json"
	reportJSON, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal report: %v", err)
	}

	err = os.WriteFile(reportFile, reportJSON, 0644)
	if err != nil {
		log.Fatalf("Failed to write report file: %v", err)
	}
	defer os.Remove(reportFile)

	fmt.Printf("Summary report saved to %s\n", reportFile)
	fmt.Printf("Report preview:\n%s\n", string(reportJSON[:min(300, len(reportJSON))]))
	fmt.Println("... (truncated)")

	fmt.Println("\nAll examples completed successfully!")
}

// Helper function to truncate JSON strings for display
func truncateJSON(jsonStr string, maxLen int) string {
	if len(jsonStr) <= maxLen {
		return jsonStr
	}
	return jsonStr[:maxLen] + "..."
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
