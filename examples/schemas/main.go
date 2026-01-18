package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/dukdb/dukdb-go"
)

func main() {
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	fmt.Println("=== dukdb-go Schema Examples ===\n")

	// Demonstrate schema operations
	demonstrateCreateSchemas(db)
	demonstrateCrossSchemaQueries(db)
	demonstrateSchemaPermissions(db)

	fmt.Println("\n✓ All schema examples completed!")
}

func demonstrateCreateSchemas(db *sql.DB) {
	fmt.Println("1. Creating Schemas")

	// Create different schemas for organization
	_, err := db.Exec("CREATE SCHEMA IF NOT EXISTS sales")
	if err != nil {
		log.Printf("Failed to create sales schema: %v", err)
		return
	}
	fmt.Println("   ✓ Created sales schema")

	_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS marketing")
	if err != nil {
		log.Printf("Failed to create marketing schema: %v", err)
		return
	}
	fmt.Println("   ✓ Created marketing schema")

	_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS analytics")
	if err != nil {
		log.Printf("Failed to create analytics schema: %v", err)
		return
	}
	fmt.Println("   ✓ Created analytics schema")

	// Create tables in different schemas
	_, err = db.Exec(`
		CREATE TABLE sales.orders (
			order_id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			order_date DATE,
			total_amount DECIMAL(10,2)
		)
	`)
	if err != nil {
		log.Printf("Failed to create sales.orders: %v", err)
		return
	}
	fmt.Println("   ✓ Created sales.orders table")

	_, err = db.Exec(`
		CREATE TABLE marketing.campaigns (
			campaign_id INTEGER PRIMARY KEY,
			campaign_name VARCHAR(100),
			start_date DATE,
			budget DECIMAL(10,2)
		)
	`)
	if err != nil {
		log.Printf("Failed to create marketing.campaigns: %v", err)
		return
	}
	fmt.Println("   ✓ Created marketing.campaigns table")

	// Insert sample data
	db.Exec(`
		INSERT INTO sales.orders VALUES
			(1, 101, '2024-01-15', 299.99),
			(2, 102, '2024-01-16', 149.99),
			(3, 103, '2024-01-17', 499.99)
	`)

	db.Exec(`
		INSERT INTO marketing.campaigns VALUES
			(1, 'Winter Sale', '2024-01-01', 10000.00),
			(2, 'Spring Launch', '2024-03-01', 15000.00)
	`)
}

func demonstrateCrossSchemaQueries(db *sql.DB) {
	fmt.Println("\n2. Cross-Schema Queries")

	// Query single schema
	var orderCount int
	err := db.QueryRow("SELECT COUNT(*) FROM sales.orders").Scan(&orderCount)
	if err == nil {
		fmt.Printf("   ✓ Sales orders: %d\n", orderCount)
	}

	var campaignCount int
	err = db.QueryRow("SELECT COUNT(*) FROM marketing.campaigns").Scan(&campaignCount)
	if err == nil {
		fmt.Printf("   ✓ Marketing campaigns: %d\n", campaignCount)
	}

	// Create view in analytics schema
	_, err = db.Exec(`
		CREATE TABLE analytics.daily_metrics AS
		SELECT 
			CURRENT_DATE as metric_date,
			(SELECT COUNT(*) FROM sales.orders) as total_orders,
			(SELECT SUM(total_amount) FROM sales.orders) as total_revenue,
			(SELECT COUNT(*) FROM marketing.campaigns) as active_campaigns
	`)
	if err != nil {
		log.Printf("Failed to create analytics view: %v", err)
		return
	}
	fmt.Println("   ✓ Created analytics.daily_metrics view")

	// Query analytics
	var orders, campaigns int
	var revenue float64
	err = db.QueryRow(`
		SELECT total_orders, total_revenue, active_campaigns 
		FROM analytics.daily_metrics
	`).Scan(&orders, &revenue, &campaigns)
	if err == nil {
		fmt.Printf("   ✓ Daily metrics: %d orders, $%.2f revenue, %d campaigns\n",
			orders, revenue, campaigns)
	}
}

func demonstrateSchemaPermissions(db *sql.DB) {
	fmt.Println("\n3. Schema Organization Best Practices")

	fmt.Println("   ✓ Use schemas to:")
	fmt.Println("     - Organize related tables")
	fmt.Println("     - Separate concerns (sales, marketing, analytics)")
	fmt.Println("     - Manage permissions at schema level")
	fmt.Println("     - Avoid table name conflicts")

	// Show schema structure
	rows, err := db.Query(`
		SELECT table_schema, table_name 
		FROM information_schema.tables 
		WHERE table_schema IN ('sales', 'marketing', 'analytics')
		ORDER BY table_schema, table_name
	`)
	if err != nil {
		return
	}
	defer rows.Close()

	fmt.Println("\n   Schema structure:")
	currentSchema := ""
	for rows.Next() {
		var schema, table string
		rows.Scan(&schema, &table)
		if schema != currentSchema {
			fmt.Printf("\n   Schema: %s\n", schema)
			currentSchema = schema
		}
		fmt.Printf("     - %s\n", table)
	}
}
