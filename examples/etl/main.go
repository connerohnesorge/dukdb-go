package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/dukdb/dukdb-go"
)

func main() {
	// Connect to an in-memory database
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	fmt.Println("=== dukdb-go ETL Examples ===\n")

	// Demonstrate ETL patterns
	demonstrateExtract(db)
	demonstrateTransform(db)
	demonstrateLoad(db)
	demonstrateCompleteETL(db)

	fmt.Println("\n✓ All ETL examples completed!")
}

func demonstrateExtract(db *sql.DB) {
	fmt.Println("1. Extract: Reading from multiple sources")

	// Simulate extracting from CSV
	_, err := db.Exec(`
		CREATE TABLE staging_sales AS
		SELECT 
			CAST(id AS INTEGER) as sale_id,
			CAST(amount AS DECIMAL(10,2)) as amount,
			CAST(sale_date AS DATE) as sale_date,
			region
		FROM read_csv_auto('sales.csv')
	`)
	if err != nil {
		// Create sample data since file doesn't exist
		_, _ = db.Exec(`
			CREATE TABLE staging_sales (
				sale_id INTEGER,
				amount DECIMAL(10,2),
				sale_date DATE,
				region VARCHAR(50)
			)
		`)
		_, _ = db.Exec(`
			INSERT INTO staging_sales VALUES
				(1, 100.50, '2024-01-15', 'North'),
				(2, 250.00, '2024-01-15', 'South'),
				(3, 75.25, '2024-01-16', 'North'),
				(4, 500.00, '2024-01-16', 'East')
		`)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM staging_sales").Scan(&count)
	fmt.Printf("   ✓ Extracted %d records from sales data\n", count)
}

func demonstrateTransform(db *sql.DB) {
	fmt.Println("\n2. Transform: Cleaning and enriching data")

	_, err := db.Exec(`
		CREATE TABLE transformed_sales AS
		SELECT 
			sale_id,
			amount,
			sale_date,
			region,
			CASE 
				WHEN amount > 200 THEN 'High'
				WHEN amount > 100 THEN 'Medium'
				ELSE 'Low'
			END as value_category,
			EXTRACT(DOW FROM sale_date) as day_of_week
		FROM staging_sales
	`)
	if err != nil {
		log.Printf("Transform failed: %v", err)
		return
	}

	fmt.Println("   ✓ Applied transformations:")
	fmt.Println("     - Added value_category (High/Medium/Low)")
	fmt.Println("     - Extracted day_of_week from date")

	// Show transformed data
	rows, _ := db.Query("SELECT * FROM transformed_sales LIMIT 3")
	if rows != nil {
		defer rows.Close()
		fmt.Println("\n   Sample transformed data:")
		for rows.Next() {
			var id int
			var amount float64
			var date string
			var region, category string
			var dow int
			rows.Scan(&id, &amount, &date, &region, &category, &dow)
			fmt.Printf("   - Sale %d: $%.2f, %s, %s\n", id, amount, region, category)
		}
	}
}

func demonstrateLoad(db *sql.DB) {
	fmt.Println("\n3. Load: Loading into final tables")

	_, err := db.Exec(`
		CREATE TABLE fact_sales (
			sale_id INTEGER PRIMARY KEY,
			amount DECIMAL(10,2),
			value_category VARCHAR(20),
			sale_date DATE,
			region VARCHAR(50),
			day_of_week INTEGER,
			etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		log.Printf("Failed to create table: %v", err)
		return
	}

	_, err = db.Exec(`
		INSERT INTO fact_sales 
			(sale_id, amount, value_category, sale_date, region, day_of_week)
		SELECT sale_id, amount, value_category, sale_date, region, day_of_week
		FROM transformed_sales
	`)
	if err != nil {
		log.Printf("Failed to load data: %v", err)
		return
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM fact_sales").Scan(&count)
	fmt.Printf("   ✓ Loaded %d records into fact_sales\n", count)
}

func demonstrateCompleteETL(db *sql.DB) {
	fmt.Println("\n4. Complete ETL with Aggregation")

	// Create aggregate table
	_, err := db.Exec(`
		CREATE TABLE daily_sales_summary AS
		SELECT 
			sale_date,
			region,
			COUNT(*) as total_sales,
			SUM(amount) as total_revenue,
			AVG(amount) as avg_sale_amount
		FROM fact_sales
		GROUP BY sale_date, region
		ORDER BY sale_date, region
	`)
	if err != nil {
		log.Printf("Failed to create summary: %v", err)
		return
	}

	fmt.Println("   ✓ Created daily sales summary with aggregations")

	// Show summary
	rows, _ := db.Query("SELECT * FROM daily_sales_summary")
	if rows != nil {
		defer rows.Close()
		fmt.Println("\n   Sales Summary:")
		for rows.Next() {
			var date, region string
			var count int
			var revenue, avg float64
			rows.Scan(&date, &region, &count, &revenue, &avg)
			fmt.Printf("   - %s %s: %d sales, $%.2f total, $%.2f avg\n", 
				date, region, count, revenue, avg)
		}
	}
}
