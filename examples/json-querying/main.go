package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
	// Create sample JSON data with nested structures
	sampleData := `[
		{
			"order_id": 1001,
			"customer": {
				"id": 1,
				"name": "Alice Johnson",
				"email": "alice@example.com"
			},
			"items": [
				{"product_id": 101, "name": "Laptop", "price": 999.99, "quantity": 1},
				{"product_id": 102, "name": "Mouse", "price": 29.99, "quantity": 2}
			],
			"shipping": {
				"address": {
					"street": "123 Main St",
					"city": "New York",
					"state": "NY",
					"zip": "10001"
				},
				"method": "Express",
				"cost": 15.99
			},
			"total": 1075.97,
			"status": "shipped",
			"order_date": "2024-01-15"
		},
		{
			"order_id": 1002,
			"customer": {
				"id": 2,
				"name": "Bob Smith",
				"email": "bob@example.com"
			},
			"items": [
				{"product_id": 103, "name": "Keyboard", "price": 79.99, "quantity": 1},
				{"product_id": 104, "name": "Monitor", "price": 399.99, "quantity": 1}
			],
			"shipping": {
				"address": {
					"street": "456 Oak Ave",
					"city": "Los Angeles",
					"state": "CA",
					"zip": "90001"
				},
				"method": "Standard",
				"cost": 9.99
			},
			"total": 489.97,
			"status": "processing",
			"order_date": "2024-01-16"
		},
		{
			"order_id": 1003,
			"customer": {
				"id": 3,
				"name": "Charlie Brown",
				"email": "charlie@example.com"
			},
			"items": [
				{"product_id": 105, "name": "Desk", "price": 299.99, "quantity": 1}
			],
			"shipping": {
				"address": {
					"street": "789 Pine Rd",
					"city": "Chicago",
					"state": "IL",
					"zip": "60601"
				},
				"method": "Express",
				"cost": 19.99
			},
			"total": 319.98,
			"status": "delivered",
			"order_date": "2024-01-14"
		}
	]`

	// Write sample data to file
	sampleFile := "orders.json"
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

	// Example 1: Basic JSON querying
	fmt.Println("=== Example 1: Basic JSON Querying ===")
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
	fmt.Println("Order data:")
	for rows.Next() {
		var customer string
		var items string
		var orderDate string
		var orderID int
		var shipping string
		var status string
		var total float64

		err := rows.Scan(&customer, &items, &orderDate, &orderID, &shipping, &status, &total)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("Order %d: Customer=%s, Total=$%.2f, Status=%s\n", orderID, customer, total, status)
	}
	if err = rows.Err(); err != nil {
		log.Fatalf("Error reading rows: %v", err)
	}

	// Example 2: Filtering JSON data
	fmt.Println("\n=== Example 2: Filtering JSON Data ===")
	query = fmt.Sprintf("SELECT * FROM read_json('%s') WHERE total \u003e 500", sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to query JSON: %v", err)
	}
	defer rows.Close()

	fmt.Println("High-value orders (total \u003e $500):")
	for rows.Next() {
		var customer string
		var items string
		var orderDate string
		var orderID int
		var shipping string
		var status string
		var total float64

		err := rows.Scan(&customer, &items, &orderDate, &orderID, &shipping, &status, &total)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("  Order %d: $%.2f - %s\n", orderID, total, status)
	}

	// Example 3: Aggregating JSON data
	fmt.Println("\n=== Example 3: Aggregating JSON Data ===")
	query = fmt.Sprintf(`SELECT
		status,
		COUNT(*) as order_count,
		AVG(total) as avg_total,
		SUM(total) as revenue
	FROM read_json('%s')
	GROUP BY status
	ORDER BY SUM(total) DESC`, sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to aggregate JSON: %v", err)
	}
	defer rows.Close()

	fmt.Println("Sales by status:")
	for rows.Next() {
		var status string
		var orderCount int
		var avgTotal float64
		var revenue float64

		err := rows.Scan(&status, &orderCount, &avgTotal, &revenue)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("  %s: %d orders, Avg: $%.2f, Total Revenue: $%.2f\n",
			status, orderCount, avgTotal, revenue)
	}

	// Example 4: Working with date fields
	fmt.Println("\n=== Example 4: Working with Date Fields ===")
	query = fmt.Sprintf(`SELECT
		order_id,
		order_date,
		total
	FROM read_json('%s')
	WHERE order_date \u003e= '2024-01-15'
	ORDER BY order_date`, sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to query by date: %v", err)
	}
	defer rows.Close()

	fmt.Println("Orders from Jan 15, 2024 onwards:")
	for rows.Next() {
		var orderID int
		var orderDate string
		var total float64

		err := rows.Scan(&orderID, &orderDate, &total)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("  Order %d on %s: $%.2f\n", orderID, orderDate, total)
	}

	// Example 5: Creating a view from JSON
	fmt.Println("\n=== Example 5: Creating View from JSON ===")
	_, err = db.Exec(fmt.Sprintf("CREATE VIEW orders_view AS SELECT * FROM read_json('%s')", sampleFile))
	if err != nil {
		log.Fatalf("Failed to create view: %v", err)
	}
	defer db.Exec("DROP VIEW orders_view")

	// Query the view
	rows, err = db.Query(`SELECT
		customer,
		COUNT(*) as order_count,
		SUM(total) as total_spent
	FROM orders_view
	GROUP BY customer
	ORDER BY SUM(total) DESC`)
	if err != nil {
		log.Fatalf("Failed to query view: %v", err)
	}
	defer rows.Close()

	fmt.Println("Customer spending summary:")
	for rows.Next() {
		var customer string
		var orderCount int
		var totalSpent float64

		err := rows.Scan(&customer, &orderCount, &totalSpent)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("  %s: %d orders, Total spent: $%.2f\n", customer, orderCount, totalSpent)
	}

	// Example 6: Complex queries with JSON data
	fmt.Println("\n=== Example 6: Complex Queries ===")
	query = fmt.Sprintf(`SELECT
		order_id,
		total,
		shipping,
		CASE
			WHEN total > 1000 THEN 'High Value'
			WHEN total > 500 THEN 'Medium Value'
			ELSE 'Low Value'
		END as value_category
	FROM read_json('%s')
	WHERE status IN ('shipped', 'delivered')
	ORDER BY total DESC`, sampleFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to execute complex query: %v", err)
	}
	defer rows.Close()

	fmt.Println("Completed orders by value category:")
	for rows.Next() {
		var orderID int
		var shipping string
		var total float64
		var valueCategory string

		err := rows.Scan(&orderID, &total, &shipping, &valueCategory)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("  Order %d: $%.2f (%s) - Shipping: %s\n", orderID, total, valueCategory, shipping)
	}

	// Example 7: Export query results
	fmt.Println("\n=== Example 7: Export Query Results ===")
	outputFile := "order_analysis.json"
	query = fmt.Sprintf(`COPY (
		SELECT
			order_id,
			customer,
			total,
			status,
			order_date,
			CASE
				WHEN total > 1000 THEN 'Premium'
				WHEN total > 500 THEN 'Standard'
				ELSE 'Basic'
			END as customer_tier
		FROM read_json('%s')
		ORDER BY total DESC
	) TO '%s'`, sampleFile, outputFile)
	_, err = db.Exec(query)
	if err != nil {
		// If COPY with complex query fails, try simpler approach
		query = fmt.Sprintf("COPY (SELECT order_id, customer, total, status FROM read_json('%s')) TO '%s'", sampleFile, outputFile)
		_, err = db.Exec(query)
		if err != nil {
			fmt.Printf("Note: Complex COPY query not supported in this version. Export skipped.\n")
		} else {
			fmt.Printf("Exported order analysis to %s\n", outputFile)
			defer os.Remove(outputFile)
		}
	} else {
		fmt.Printf("Exported order analysis to %s\n", outputFile)
		defer os.Remove(outputFile)
	}

	// Example 8: Working with nested JSON fields
	fmt.Println("\n=== Example 8: Working with Nested Fields ===")

	// Create a file with deeply nested data
	nestedData := `[
		{
			"id": 1,
			"data": {
				"level1": {
					"level2": {
						"value": "nested_value",
						"number": 42
					}
				}
			}
		},
		{
			"id": 2,
			"data": {
				"level1": {
					"level2": {
						"value": "another_value",
						"number": 100
					}
				}
			}
		}
	]`

	nestedFile := "nested_data.json"
	err = os.WriteFile(nestedFile, []byte(nestedData), 0644)
	if err != nil {
		log.Fatalf("Failed to write nested data file: %v", err)
	}
	defer os.Remove(nestedFile)

	query = fmt.Sprintf("SELECT * FROM read_json('%s')", nestedFile)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read nested data: %v", err)
	}
	defer rows.Close()

	columns, err = rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}
	fmt.Printf("\nNested data columns: %v\n", columns)

	fmt.Println("Nested data (as JSON strings):")
	for rows.Next() {
		var data string
		var id int

		err := rows.Scan(&data, &id)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("  ID %d: data=%s\n", id, data)
	}

	fmt.Println("\nAll examples completed successfully!")
}

// Helper function to display JSON structure
func analyzeJSONStructure(filename string) {
	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}
	fmt.Printf("\nFile size: %d bytes\n", len(data))
	fmt.Printf("First 200 characters:\n%s\n", string(data[:min(200, len(data))]))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}