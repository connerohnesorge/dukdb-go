package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/dukdb/dukdb-go"
)

func main() {
	// Connect to an in-memory database
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	fmt.Println("=== Basic Example 09: Using LIMIT and ORDER BY ===\n")

	// Create a sales table
	_, err = db.Exec(`CREATE TABLE sales (
		id INTEGER PRIMARY KEY,
		product_name VARCHAR(100) NOT NULL,
		category VARCHAR(50),
		price DECIMAL(10,2),
		quantity INTEGER,
		sale_date DATE,
		region VARCHAR(50),
		salesperson VARCHAR(50)
	)`)
	if err != nil {
		log.Fatal("Failed to create sales table:", err)
	}

	// Insert sample sales data
	salesData := []struct {
		id          int
		product     string
		category    string
		price       float64
		quantity    int
		saleDate    string
		region      string
		salesperson string
	}{
		{1, "Laptop Pro", "Electronics", 1299.99, 2, "2024-01-15", "North", "Alice"},
		{2, "Wireless Mouse", "Accessories", 29.99, 5, "2024-01-16", "South", "Bob"},
		{3, "Office Chair", "Furniture", 249.99, 1, "2024-01-17", "East", "Charlie"},
		{4, "Smartphone", "Electronics", 699.99, 3, "2024-01-18", "West", "David"},
		{5, "Desk Lamp", "Furniture", 45.99, 4, "2024-01-19", "North", "Eve"},
		{6, "Keyboard", "Accessories", 79.99, 10, "2024-01-20", "South", "Frank"},
		{7, "Monitor", "Electronics", 399.99, 2, "2024-01-21", "East", "Grace"},
		{8, "Notebook", "Stationery", 4.99, 20, "2024-01-22", "West", "Henry"},
		{9, "Tablet", "Electronics", 449.99, 1, "2024-01-23", "North", "Ivy"},
		{10, "Headphones", "Accessories", 149.99, 6, "2024-01-24", "South", "Jack"},
		{11, "Standing Desk", "Furniture", 599.99, 1, "2024-01-25", "East", "Kate"},
		{12, "Pen Set", "Stationery", 12.99, 15, "2024-01-26", "West", "Liam"},
		{13, "Router", "Electronics", 199.99, 2, "2024-01-27", "North", "Mia"},
		{14, "Webcam", "Accessories", 89.99, 8, "2024-01-28", "South", "Noah"},
		{15, "Bookshelf", "Furniture", 179.99, 1, "2024-01-29", "East", "Olivia"},
		{16, "Printer", "Electronics", 299.99, 1, "2024-01-30", "West", "Paul"},
		{17, "USB Hub", "Accessories", 24.99, 12, "2024-02-01", "North", "Quinn"},
		{18, "Filing Cabinet", "Furniture", 299.99, 2, "2024-02-02", "South", "Ruby"},
		{19, "Paper", "Stationery", 8.99, 50, "2024-02-03", "East", "Sam"},
		{20, "Smart Watch", "Electronics", 299.99, 2, "2024-02-04", "West", "Tina"},
	}

	for _, sale := range salesData {
		_, err = db.Exec(
			`INSERT INTO sales (id, product_name, category, price, quantity, sale_date, region, salesperson)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			sale.id,
			sale.product,
			sale.category,
			sale.price,
			sale.quantity,
			sale.saleDate,
			sale.region,
			sale.salesperson,
		)
		if err != nil {
			log.Printf("Failed to insert sale %s: %v", sale.product, err)
		}
	}
	fmt.Println("✓ Sample sales data inserted (20 records)")

	// Example 1: Basic ORDER BY - Sort by price ascending
	fmt.Println("\n=== Example 1: Basic ORDER BY (ascending) ===")
	fmt.Println("Products sorted by price (lowest to highest):")
	rows, err := db.Query("SELECT product_name, price FROM sales ORDER BY price LIMIT 5")
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	displayProducts(rows)

	// Example 2: ORDER BY descending
	fmt.Println("\n=== Example 2: ORDER BY DESC (descending) ===")
	fmt.Println("Top 5 most expensive products:")
	rows, err = db.Query("SELECT product_name, price FROM sales ORDER BY price DESC LIMIT 5")
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	displayProducts(rows)

	// Example 3: ORDER BY multiple columns
	fmt.Println("\n=== Example 3: ORDER BY multiple columns ===")
	fmt.Println("Sales by region and then by price (top 8):")
	rows, err = db.Query(`SELECT region, product_name, price
		FROM sales
		ORDER BY region ASC, price DESC
		LIMIT 8`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	displaySalesByRegion(rows)

	// Example 4: LIMIT with offset (pagination)
	fmt.Println("\n=== Example 4: LIMIT with OFFSET (pagination) ===")
	fmt.Println("Page 1 (records 1-5):")
	rows, err = db.Query("SELECT product_name, price FROM sales ORDER BY id LIMIT 5 OFFSET 0")
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	displayProducts(rows)

	fmt.Println("\nPage 2 (records 6-10):")
	rows, err = db.Query("SELECT product_name, price FROM sales ORDER BY id LIMIT 5 OFFSET 5")
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	displayProducts(rows)

	fmt.Println("\nPage 3 (records 11-15):")
	rows, err = db.Query("SELECT product_name, price FROM sales ORDER BY id LIMIT 5 OFFSET 10")
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	displayProducts(rows)

	// Example 5: LIMIT without ORDER BY (not recommended)
	fmt.Println("\n=== Example 5: LIMIT without ORDER BY ===")
	fmt.Println("First 5 records (arbitrary order - not recommended):")
	rows, err = db.Query("SELECT product_name, price FROM sales LIMIT 5")
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	displayProducts(rows)

	// Example 6: ORDER BY with expressions
	fmt.Println("\n=== Example 6: ORDER BY with expressions ===")
	fmt.Println("Top 5 sales by total value (price * quantity):")
	rows, err = db.Query(`SELECT product_name, price, quantity,
			(price * quantity) as total_value
		FROM sales
		ORDER BY (price * quantity) DESC
		LIMIT 5`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	displaySalesByValue(rows)

	// Example 7: ORDER BY with NULL handling
	fmt.Println("\n=== Example 7: ORDER BY with NULL handling ===")
	fmt.Println("Products sorted by warranty months (NULLs last):")

	// Add warranty_months column for demonstration
	_, err = db.Exec("ALTER TABLE sales ADD COLUMN warranty_months INTEGER")
	if err != nil {
		log.Printf("Failed to add warranty column: %v", err)
	} else {
		// Set warranty months for some products
		warranties := map[int]int{
			1: 24, 3: 12, 4: 18, 6: 12, 7: 24, 9: 18, 10: 12,
			12: 6, 13: 24, 15: 12, 16: 18, 18: 12, 19: 3,
		}
		for id, months := range warranties {
			_, err = db.Exec("UPDATE sales SET warranty_months = ? WHERE id = ?", months, id)
			if err != nil {
				log.Printf("Failed to update warranty for ID %d: %v", id, err)
			}
		}

		// Set some NULL values
		_, err = db.Exec("UPDATE sales SET warranty_months = NULL WHERE id IN (2, 5, 8, 11, 14, 17, 20)")
		if err != nil {
			log.Printf("Failed to update warranty months: %v", err)
		}
	}

	// Query with NULLs sorted last
	rows, err = db.Query(`SELECT product_name, warranty_months
		FROM sales
		ORDER BY warranty_months IS NULL, warranty_months DESC
		LIMIT 10`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	displayWarrantyInfo(rows)

	// Example 8: Alternative sampling approach using modulo
	fmt.Println("\n=== Example 8: Alternative sampling with LIMIT ===")
	fmt.Println("5 products selected using modulo on ID (consistent results):")
	rows, err = db.Query(`SELECT product_name, price
		FROM sales
		WHERE id % 4 = 0
		ORDER BY id
		LIMIT 5`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	displayProducts(rows)

	// Example 9: Top N per category
	fmt.Println("\n=== Example 9: Top N per category (using LIMIT in subquery) ===")
	fmt.Println("Top 2 most expensive products per category:")

	categories := []string{"Electronics", "Accessories", "Furniture", "Stationery"}
	for _, category := range categories {
		fmt.Printf("\n%s:\n", category)
		rows, err = db.Query(`SELECT product_name, price
			FROM sales
			WHERE category = ?
			ORDER BY price DESC
			LIMIT 2`, category)
		if err != nil {
			log.Printf("Failed to query category %s: %v", category, err)
			continue
		}
		defer rows.Close()

		displayProducts(rows)
	}

	// Example 10: Dynamic LIMIT based on percentage
	fmt.Println("\n=== Example 10: Dynamic LIMIT based on percentage ===")
	// Get total count
	var totalCount int
	err = db.QueryRow("SELECT COUNT(*) FROM sales").Scan(&totalCount)
	if err != nil {
		log.Printf("Failed to get count: %v", err)
	} else {
		// Calculate 20% of total
		top20Percent := totalCount / 5
		if top20Percent < 1 {
			top20Percent = 1
		}

		fmt.Printf("Top %d products (top 20%% of %d total):\n", top20Percent, totalCount)
		rows, err = db.Query(fmt.Sprintf(`SELECT product_name, price
			FROM sales
			ORDER BY price DESC
			LIMIT %d`, top20Percent))
		if err != nil {
			log.Fatal("Failed to query:", err)
		}
		defer rows.Close()

		displayProducts(rows)
	}

	// Summary statistics
	fmt.Println("\n=== Summary Statistics ===")
	var stats struct {
		minPrice    float64
		maxPrice    float64
		avgPrice    float64
		totalSales  float64
		avgQuantity float64
	}

	err = db.QueryRow("SELECT MIN(price), MAX(price), AVG(price) FROM sales").Scan(
		&stats.minPrice, &stats.maxPrice, &stats.avgPrice)
	if err == nil {
		fmt.Printf("Price Statistics:\n")
		fmt.Printf("  Minimum: $%.2f\n", stats.minPrice)
		fmt.Printf("  Maximum: $%.2f\n", stats.maxPrice)
		fmt.Printf("  Average: $%.2f\n", stats.avgPrice)
	}

	err = db.QueryRow("SELECT SUM(price * quantity), AVG(quantity) FROM sales").Scan(
		&stats.totalSales, &stats.avgQuantity)
	if err == nil {
		fmt.Printf("Sales Statistics:\n")
		fmt.Printf("  Total Sales Value: $%.2f\n", stats.totalSales)
		fmt.Printf("  Average Quantity: %.1f\n", stats.avgQuantity)
	}

	// Clean up
	fmt.Println("\n=== Cleaning Up ===")
	_, err = db.Exec("DROP TABLE sales")
	if err != nil {
		log.Printf("Failed to drop table: %v", err)
	}
	fmt.Println("✓ Table dropped successfully")

	// Summary
	fmt.Println("\n=== Summary ===")
	fmt.Println("This example demonstrated:")
	fmt.Println("- Basic ORDER BY (ascending and descending)")
	fmt.Println("- ORDER BY multiple columns")
	fmt.Println("- LIMIT with OFFSET for pagination")
	fmt.Println("- Why LIMIT without ORDER BY is not recommended")
	fmt.Println("- ORDER BY with expressions/calculations")
	fmt.Println("- ORDER BY with NULL handling")
	fmt.Println("- Alternative sampling approach using WHERE and ORDER BY")
	fmt.Println("- Top N per category using LIMIT in subqueries")
	fmt.Println("- Dynamic LIMIT based on calculations")
	fmt.Println("\nAll operations completed successfully!")
}

// Helper functions for displaying results
func displayProducts(rows *sql.Rows) {
	for rows.Next() {
		var name string
		var price float64
		err := rows.Scan(&name, &price)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}
		fmt.Printf("  - %-20s $%8.2f\n", name, price)
	}
}

func displaySalesByRegion(rows *sql.Rows) {
	for rows.Next() {
		var region, product string
		var price float64
		err := rows.Scan(&region, &product, &price)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}
		fmt.Printf("  - %-5s | %-20s $%8.2f\n", region, product, price)
	}
}

func displaySalesByValue(rows *sql.Rows) {
	for rows.Next() {
		var product string
		var price, totalValue float64
		var quantity int
		err := rows.Scan(&product, &price, &quantity, &totalValue)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}
		fmt.Printf("  - %-20s $%8.2f x %2d = $%9.2f\n", product, price, quantity, totalValue)
	}
}

func displayWarrantyInfo(rows *sql.Rows) {
	for rows.Next() {
		var product string
		var warrantyMonths sql.NullInt64
		err := rows.Scan(&product, &warrantyMonths)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}
		warrantyStr := "NULL"
		if warrantyMonths.Valid {
			warrantyStr = fmt.Sprintf("%d months", warrantyMonths.Int64)
		}
		fmt.Printf("  - %-20s %s\n", product, warrantyStr)
	}
}
