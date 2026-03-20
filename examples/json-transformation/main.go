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

	fmt.Println("=== JSON Transformation Example ===")

	// Create source data
	sourceJSON := `{"product": "Laptop", "price": 999.99, "stock": 5, "category": "Electronics"}
{"product": "Mouse", "price": 29.99, "stock": 50, "category": "Electronics"}
{"product": "Desk", "price": 199.99, "stock": 12, "category": "Furniture"}
{"product": "Chair", "price": 149.99, "stock": 30, "category": "Furniture"}`

	sourcePath := "source.json"
	err = os.WriteFile(sourcePath, []byte(sourceJSON), 0644)
	if err != nil {
		log.Fatal("Failed to write source file:", err)
	}
	defer os.Remove(sourcePath)

	fmt.Println("\n1. Original Data:")
	rows, _ := db.Query("SELECT * FROM read_json_auto('source.json')")
	defer rows.Close()

	for rows.Next() {
		var category string
		var price float64
		var product string
		var stock int

		rows.Scan(&category, &price, &product, &stock)
		fmt.Printf("  %s: $%.2f (Stock: %d, Category: %s)\n", product, price, stock, category)
	}

	// Example 1: Filter and select
	fmt.Println("\n2. Filtered data (Electronics only):")
	rows, _ = db.Query("SELECT category, price, product FROM read_json_auto('source.json') WHERE category = 'Electronics'")
	defer rows.Close()

	for rows.Next() {
		var category string
		var price float64
		var product string

		rows.Scan(&category, &price, &product)
		fmt.Printf("  %s: $%.2f\n", product, price)
	}

	// Example 2: Simple projection
	fmt.Println("\n3. Products and stock levels:")
	rows, _ = db.Query(`
		SELECT product, stock FROM read_json_auto('source.json')
	`)
	defer rows.Close()

	for rows.Next() {
		var product string
		var stock int

		rows.Scan(&product, &stock)
		fmt.Printf("  %s: %d in stock\n", product, stock)
	}

	// Example 3: Aggregation and grouping
	fmt.Println("\n4. Aggregated data by category:")
	rows, _ = db.Query(`
		SELECT 
			category,
			COUNT(*) as item_count,
			AVG(price) as avg_price,
			SUM(stock) as total_stock
		FROM read_json_auto('source.json')
		GROUP BY category
		ORDER BY item_count DESC
	`)
	defer rows.Close()

	for rows.Next() {
		var avgPrice float64
		var category string
		var itemCount int
		var totalStock int

		rows.Scan(&avgPrice, &category, &itemCount, &totalStock)
		fmt.Printf("  %s: %d items, Avg Price: $%.2f, Total Stock: %d\n", category, itemCount, avgPrice, totalStock)
	}

	// Example 4: Export transformed data
	fmt.Println("\n5. Exporting transformed data:")

	outputPath := "transformed_output.json"
	_, err = db.Exec(fmt.Sprintf(`
		COPY (
			SELECT 
				product,
				price,
				stock,
				category
			FROM read_json_auto('source.json')
			WHERE stock > 10
			ORDER BY category, product
		)
		TO '%s'
		(FORMAT NDJSON)
	`, outputPath))

	if err == nil {
		data, _ := os.ReadFile(outputPath)
		fmt.Println("Exported (stock > 10):")
		fmt.Println(string(data))
		os.Remove(outputPath)
	}

	// Example 5: Data shape transformation
	fmt.Println("\n6. Reshaping data into categories list:")
	rows, _ = db.Query(`
		SELECT DISTINCT category FROM read_json_auto('source.json') ORDER BY category
	`)
	defer rows.Close()

	fmt.Println("Categories found:")
	for rows.Next() {
		var category string
		rows.Scan(&category)
		fmt.Printf("  - %s\n", category)
	}

	// Example 6: Summary statistics
	fmt.Println("\n7. Summary statistics:")

	var (
		totalProducts int
		avgPrice      float64
		maxPrice      float64
		minPrice      float64
		totalStock    int
	)

	row := db.QueryRow(`
		SELECT 
			COUNT(*),
			AVG(price),
			MAX(price),
			MIN(price),
			SUM(stock)
		FROM read_json_auto('source.json')
	`)

	row.Scan(&totalProducts, &avgPrice, &maxPrice, &minPrice, &totalStock)

	fmt.Printf("  Total Products: %d\n", totalProducts)
	fmt.Printf("  Average Price: $%.2f\n", avgPrice)
	fmt.Printf("  Price Range: $%.2f - $%.2f\n", minPrice, maxPrice)
	fmt.Printf("  Total Stock: %d\n", totalStock)

	fmt.Println("\n✓ JSON transformation example completed successfully!")
}
