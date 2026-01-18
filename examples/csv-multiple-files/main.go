package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
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

	fmt.Println("=== Multiple CSV Files Handling Example ===")

	// Create directory for CSV files
	dataDir := "sales_data"
	err = os.MkdirAll(dataDir, 0755)
	if err != nil {
		log.Fatal("Failed to create data directory:", err)
	}
	defer os.RemoveAll(dataDir)

	// Example 1: Create multiple CSV files (different months)
	fmt.Println("\n1. Creating multiple CSV files for different months...")

	months := []struct {
		name string
		data string
	}{
		{
			"january_2023.csv",
			`order_id,customer_id,product_id,quantity,price,order_date
1001,101,201,2,29.99,2023-01-05
1002,102,202,1,49.99,2023-01-07
1003,103,203,3,19.99,2023-01-10
1004,104,204,1,99.99,2023-01-12
1005,105,205,2,39.99,2023-01-15`,
		},
		{
			"february_2023.csv",
			`order_id,customer_id,product_id,quantity,price,order_date
2001,106,206,1,59.99,2023-02-02
2002,107,207,2,24.99,2023-02-05
2003,108,208,1,79.99,2023-02-08
2004,109,209,3,34.99,2023-02-11
2005,110,210,1,89.99,2023-02-14`,
		},
		{
			"march_2023.csv",
			`order_id,customer_id,product_id,quantity,price,order_date
3001,111,211,2,44.99,2023-03-01
3002,112,212,1,69.99,2023-03-04
3003,113,213,4,14.99,2023-03-07
3004,114,214,1,119.99,2023-03-10
3005,115,215,2,54.99,2023-03-13`,
		},
	}

	for _, month := range months {
		filePath := filepath.Join(dataDir, month.name)
		err = os.WriteFile(filePath, []byte(month.data), 0644)
		if err != nil {
			log.Fatal("Failed to create CSV file:", err)
		}
		fmt.Printf("Created %s\n", month.name)
	}

	// Example 2: Read multiple files using UNION ALL
	fmt.Println("\n2. Reading multiple files with UNION ALL:")

	query := fmt.Sprintf(`
		SELECT * FROM read_csv_auto('%s')
		UNION ALL
		SELECT * FROM read_csv_auto('%s')
		UNION ALL
		SELECT * FROM read_csv_auto('%s')
		ORDER BY order_date
	`,
		filepath.Join(dataDir, "january_2023.csv"),
		filepath.Join(dataDir, "february_2023.csv"),
		filepath.Join(dataDir, "march_2023.csv"),
	)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal("Failed to query multiple files:", err)
	}
	defer rows.Close()

	fmt.Println("Combined data from all months:")
	printRows(rows)

	// Example 3: Add filename column to track source
	fmt.Println("\n3. Adding source filename to each record:")

	query = fmt.Sprintf(`
		SELECT 'january_2023.csv' as source_file, j.* FROM read_csv_auto('%s') j
		UNION ALL
		SELECT 'february_2023.csv' as source_file, f.* FROM read_csv_auto('%s') f
		UNION ALL
		SELECT 'march_2023.csv' as source_file, m.* FROM read_csv_auto('%s') m
		ORDER BY source_file, order_id
	`,
		filepath.Join(dataDir, "january_2023.csv"),
		filepath.Join(dataDir, "february_2023.csv"),
		filepath.Join(dataDir, "march_2023.csv"),
	)

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal("Failed to query with source files:", err)
	}
	defer rows.Close()

	fmt.Println("Data with source tracking:")
	printRows(rows)

	// Example 4: Aggregate across multiple files
	fmt.Println("\n4. Aggregating data across multiple files:")

	query = fmt.Sprintf(`
		SELECT
			SUBSTRING(order_date, 1, 7) as month,
			COUNT(*) as total_orders,
			SUM(quantity) as total_quantity,
			SUM(quantity * price) as total_revenue,
			ROUND(AVG(quantity * price), 2) as avg_order_value
		FROM (
			SELECT * FROM read_csv_auto('%s')
			UNION ALL
			SELECT * FROM read_csv_auto('%s')
			UNION ALL
			SELECT * FROM read_csv_auto('%s')
		) all_data
		GROUP BY SUBSTRING(order_date, 1, 7)
		ORDER BY month
	`,
		filepath.Join(dataDir, "january_2023.csv"),
		filepath.Join(dataDir, "february_2023.csv"),
		filepath.Join(dataDir, "march_2023.csv"),
	)

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal("Failed to aggregate data:", err)
	}
	defer rows.Close()

	fmt.Println("Monthly summary:")
	printRows(rows)

	// Example 5: Create customer master file from multiple sources
	fmt.Println("\n5. Creating customer master from multiple files...")

	// Create customer files
	customerFiles := []struct {
		name string
		data string
	}{
		{
			"customers_north.csv",
			`customer_id,customer_name,email,region
101,North Customer 1,n1@email.com,North
102,North Customer 2,n2@email.com,North
103,North Customer 3,n3@email.com,North`,
		},
		{
			"customers_south.csv",
			`customer_id,customer_name,email,region
104,South Customer 1,s1@email.com,South
105,South Customer 2,s2@email.com,South
106,South Customer 3,s3@email.com,South`,
		},
		{
			"customers_east.csv",
			`customer_id,customer_name,email,region
107,East Customer 1,e1@email.com,East
108,East Customer 2,e2@email.com,East
109,East Customer 3,e3@email.com,East`,
		},
	}

	for _, file := range customerFiles {
		filePath := filepath.Join(dataDir, file.name)
		err = os.WriteFile(filePath, []byte(file.data), 0644)
		if err != nil {
			log.Fatal("Failed to create customer file:", err)
		}
	}

	// Combine all customers
	query = fmt.Sprintf(`
		SELECT * FROM read_csv_auto('%s')
		UNION
		SELECT * FROM read_csv_auto('%s')
		UNION
		SELECT * FROM read_csv_auto('%s')
		ORDER BY customer_id
	`,
		filepath.Join(dataDir, "customers_north.csv"),
		filepath.Join(dataDir, "customers_south.csv"),
		filepath.Join(dataDir, "customers_east.csv"),
	)

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal("Failed to query customers:", err)
	}
	defer rows.Close()

	fmt.Println("Combined customer master:")
	printRows(rows)

	// Example 6: Join data from multiple files
	fmt.Println("\n6. Joining sales data with customer information:")

	// Create a view for combined sales data
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TEMPORARY VIEW combined_sales AS
		SELECT * FROM read_csv_auto('%s')
		UNION ALL
		SELECT * FROM read_csv_auto('%s')
		UNION ALL
		SELECT * FROM read_csv_auto('%s')
	`,
		filepath.Join(dataDir, "january_2023.csv"),
		filepath.Join(dataDir, "february_2023.csv"),
		filepath.Join(dataDir, "march_2023.csv"),
	))
	if err != nil {
		log.Fatal("Failed to create combined sales view:", err)
	}

	// Create a view for combined customers
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TEMPORARY VIEW combined_customers AS
		SELECT * FROM read_csv_auto('%s')
		UNION
		SELECT * FROM read_csv_auto('%s')
		UNION
		SELECT * FROM read_csv_auto('%s')
	`,
		filepath.Join(dataDir, "customers_north.csv"),
		filepath.Join(dataDir, "customers_south.csv"),
		filepath.Join(dataDir, "customers_east.csv"),
	))
	if err != nil {
		log.Fatal("Failed to create combined customers view:", err)
	}

	// Join sales with customers
	query = `
		SELECT
			s.order_id,
			s.order_date,
			c.customer_name,
			c.region,
			s.product_id,
			s.quantity,
			s.price,
			s.quantity * s.price as total_amount
		FROM combined_sales s
		JOIN combined_customers c ON s.customer_id = c.customer_id
		ORDER BY s.order_date, s.order_id
	`

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal("Failed to join data:", err)
	}
	defer rows.Close()

	fmt.Println("Sales data with customer information:")
	printRows(rows, 10) // Show first 10 rows

	// Example 7: Handle files with different schemas
	fmt.Println("\n7. Handling files with different schemas...")

	// Create files with different columns
	schemaFiles := []struct {
		name string
		data string
	}{
		{
			"products_2022.csv",
			`product_id,product_name,category,price
201,Laptop,Electronics,999.99
202,Mouse,Accessories,29.99
203,Keyboard,Accessories,79.99`,
		},
		{
			"products_2023.csv",
			`product_id,product_name,category,price,stock
204,Monitor,Electronics,399.99,15
205,Headphones,Electronics,149.99,50
206,Webcam,Electronics,89.99,25`,
		},
	}

	for _, file := range schemaFiles {
		filePath := filepath.Join(dataDir, file.name)
		err = os.WriteFile(filePath, []byte(file.data), 0644)
		if err != nil {
			log.Fatal("Failed to create schema file:", err)
		}
	}

	// Read files with different schemas
	query = fmt.Sprintf(`
		SELECT
			product_id,
			product_name,
			category,
			price,
			NULL as stock  -- Add missing column
		FROM read_csv_auto('%s')
		UNION ALL
		SELECT * FROM read_csv_auto('%s')
	`,
		filepath.Join(dataDir, "products_2022.csv"),
		filepath.Join(dataDir, "products_2023.csv"),
	)

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal("Failed to query different schemas:", err)
	}
	defer rows.Close()

	fmt.Println("Products with unified schema:")
	printRows(rows)

	// Example 8: Dynamic file reading with glob patterns
	fmt.Println("\n8. Using glob patterns to read multiple files...")

	// Create more monthly files
	additionalMonths := []struct {
		name string
		data string
	}{
		{
			"april_2023.csv",
			`order_id,customer_id,product_id,quantity,price,order_date
4001,116,216,1,79.99,2023-04-02
4002,117,217,3,34.99,2023-04-05`,
		},
		{
			"may_2023.csv",
			`order_id,customer_id,product_id,quantity,price,order_date
5001,118,218,2,59.99,2023-05-01
5002,119,219,1,99.99,2023-05-04`,
		},
	}

	for _, month := range additionalMonths {
		filePath := filepath.Join(dataDir, month.name)
		err = os.WriteFile(filePath, []byte(month.data), 0644)
		if err != nil {
			log.Fatal("Failed to create additional month file:", err)
		}
	}

	// Note: DuckDB doesn't support glob patterns in read_csv_auto directly
	// So we build the query dynamically
	filePattern := filepath.Join(dataDir, "*_2023.csv")
	files, err := filepath.Glob(filePattern)
	if err != nil {
		log.Fatal("Failed to glob files:", err)
	}

	// Build UNION ALL query dynamically
	var parts []string
	for _, file := range files {
		parts = append(parts, fmt.Sprintf("SELECT * FROM read_csv_auto('%s')", file))
	}

	query = strings.Join(parts, "\nUNION ALL\n") + "\nORDER BY order_date"

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal("Failed to query glob files:", err)
	}
	defer rows.Close()

	fmt.Printf("All %d files matching pattern '*_2023.csv':\n", len(files))
	printRows(rows)

	// Example 9: Summary statistics across files
	fmt.Println("\n9. Summary statistics across all files:")

	query = fmt.Sprintf(`
		SELECT
			COUNT(DISTINCT source_file) as total_files,
			COUNT(*) as total_records,
			COUNT(DISTINCT customer_id) as unique_customers,
			COUNT(DISTINCT product_id) as unique_products,
			SUM(quantity) as total_quantity,
			SUM(quantity * price) as total_revenue,
			MIN(order_date) as earliest_date,
			MAX(order_date) as latest_date
		FROM (
			SELECT 'january_2023.csv' as source_file, j.* FROM read_csv_auto('%s') j
			UNION ALL
			SELECT 'february_2023.csv' as source_file, f.* FROM read_csv_auto('%s') f
			UNION ALL
			SELECT 'march_2023.csv' as source_file, m.* FROM read_csv_auto('%s') m
			UNION ALL
			SELECT 'april_2023.csv' as source_file, a.* FROM read_csv_auto('%s') a
			UNION ALL
			SELECT 'may_2023.csv' as source_file, m2.* FROM read_csv_auto('%s') m2
		) all_data
	`,
		filepath.Join(dataDir, "january_2023.csv"),
		filepath.Join(dataDir, "february_2023.csv"),
		filepath.Join(dataDir, "march_2023.csv"),
		filepath.Join(dataDir, "april_2023.csv"),
		filepath.Join(dataDir, "may_2023.csv"),
	)

	var stats struct {
		totalFiles      int
		totalRecords    int
		uniqueCustomers int
		uniqueProducts  int
		totalQuantity   int
		totalRevenue    float64
		earliestDate    string
		latestDate      string
	}

	err = db.QueryRow(query).Scan(
		&stats.totalFiles,
		&stats.totalRecords,
		&stats.uniqueCustomers,
		&stats.uniqueProducts,
		&stats.totalQuantity,
		&stats.totalRevenue,
		&stats.earliestDate,
		&stats.latestDate,
	)
	if err != nil {
		log.Fatal("Failed to get summary statistics:", err)
	}

	fmt.Println("Summary Statistics:")
	fmt.Printf("  Total files: %d\n", stats.totalFiles)
	fmt.Printf("  Total records: %d\n", stats.totalRecords)
	fmt.Printf("  Unique customers: %d\n", stats.uniqueCustomers)
	fmt.Printf("  Unique products: %d\n", stats.uniqueProducts)
	fmt.Printf("  Total quantity: %d\n", stats.totalQuantity)
	fmt.Printf("  Total revenue: $%.2f\n", stats.totalRevenue)
	fmt.Printf("  Date range: %s to %s\n", stats.earliestDate, stats.latestDate)

	// Example 10: Export combined data
	fmt.Println("\n10. Exporting combined data to single CSV...")

	outputFile := filepath.Join(dataDir, "combined_sales_2023.csv")
	_, err = db.Exec(fmt.Sprintf(`
		COPY (
			SELECT
				ROW_NUMBER() OVER (ORDER BY order_date, order_id) as combined_order_id,
				order_id as original_order_id,
				customer_id,
				product_id,
				quantity,
				price,
				quantity * price as total_amount,
				order_date,
				SUBSTRING(order_date, 1, 7) as order_month
			FROM (
				SELECT * FROM read_csv_auto('%s')
				UNION ALL
				SELECT * FROM read_csv_auto('%s')
				UNION ALL
				SELECT * FROM read_csv_auto('%s')
				UNION ALL
				SELECT * FROM read_csv_auto('%s')
				UNION ALL
				SELECT * FROM read_csv_auto('%s')
			) combined_data
		) TO '%s' WITH (HEADER true)
	`,
		filepath.Join(dataDir, "january_2023.csv"),
		filepath.Join(dataDir, "february_2023.csv"),
		filepath.Join(dataDir, "march_2023.csv"),
		filepath.Join(dataDir, "april_2023.csv"),
		filepath.Join(dataDir, "may_2023.csv"),
		outputFile,
	))
	if err != nil {
		log.Fatal("Failed to export combined data:", err)
	}

	fmt.Printf("Combined data exported to %s\n", outputFile)

	// Clean up temporary views
	_, err = db.Exec("DROP VIEW IF EXISTS combined_sales")
	if err != nil {
		log.Printf("Warning: Failed to drop combined_sales view: %v", err)
	}
	_, err = db.Exec("DROP VIEW IF EXISTS combined_customers")
	if err != nil {
		log.Printf("Warning: Failed to drop combined_customers view: %v", err)
	}

	fmt.Println("\n✓ Multiple CSV files handling example completed successfully!")
}

// Helper function to print query results
func printRows(rows *sql.Rows, limit ...int) {
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Failed to get columns:", err)
	}

	// Print header
	fmt.Printf("%-15s", strings.Join(columns, " | "))
	fmt.Println()
	fmt.Println(strings.Repeat("-", 15*len(columns)))

	// Print data
	rowCount := 0
	maxRows := 100 // Default to show all
	if len(limit) > 0 {
		maxRows = limit[0]
	}

	for rows.Next() && rowCount < maxRows {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		var row []string
		for _, val := range values {
			if val == nil {
				row = append(row, "NULL")
			} else {
				str := fmt.Sprintf("%v", val)
				if len(str) > 12 {
					str = str[:12] + "..."
				}
				row = append(row, str)
			}
		}
		fmt.Printf("%-15s\n", strings.Join(row, " | "))
		rowCount++
	}

	if rows.Next() {
		fmt.Printf("... (showing first %d rows)\n", maxRows)
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error reading rows:", err)
	}
}

// Helper function to demonstrate file pattern matching
func demonstrateFilePatterns() {
	fmt.Println("\nFile Pattern Examples:")
	fmt.Println("Pattern: sales_*.csv")
	fmt.Println("  Matches: sales_2023.csv, sales_jan.csv, sales_01.csv")
	fmt.Println("Pattern: */data/*.csv")
	fmt.Println("  Matches: january/data/sales.csv, february/data/sales.csv")
	fmt.Println("Pattern: data-{01..12}.csv")
	fmt.Println("  Matches: data-01.csv, data-02.csv, ..., data-12.csv")
}

// Helper function to show performance tips
func showPerformanceTips() {
	fmt.Println("\nPerformance Tips for Multiple Files:")
	fmt.Println("1. Use UNION ALL when duplicates are not a concern (faster)")
	fmt.Println("2. Create temporary views for repeated queries")
	fmt.Println("3. Process files in parallel if possible")
	fmt.Println("4. Use glob patterns to avoid manual file listing")
	fmt.Println("5. Consider file size and memory limits")
	fmt.Println("6. Index combined data for faster queries")
	fmt.Println("7. Compress large output files")
}

// Helper function to demonstrate error handling
func demonstrateErrorHandling() {
	fmt.Println("\nError Handling for Multiple Files:")
	fmt.Println("1. Check file existence before reading")
	fmt.Println("2. Handle schema mismatches gracefully")
	fmt.Println("3. Validate data types across files")
	fmt.Println("4. Handle encoding issues")
	fmt.Println("5. Manage memory for large file sets")
	fmt.Println("6. Provide fallback for missing files")
	fmt.Println("7. Log processing status for debugging")
}
