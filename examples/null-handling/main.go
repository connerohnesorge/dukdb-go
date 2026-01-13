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

	fmt.Println("=== Basic Example 08: Working with NULL Values ===\n")

	// Create a products table with nullable columns
	_, err = db.Exec(`CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		description TEXT,
		price DECIMAL(10,2),
		category VARCHAR(50),
		stock_quantity INTEGER,
		manufacturer VARCHAR(100),
		release_date DATE,
		discontinued_date DATE,
		is_featured BOOLEAN,
		rating DECIMAL(3,2),
		warranty_months INTEGER
	)`)
	if err != nil {
		log.Fatal("Failed to create products table:", err)
	}
	fmt.Println("✓ Products table created with nullable columns")

	// Example 1: Insert data with NULL values
	fmt.Println("\n=== Example 1: Inserting NULL values ===")
	products := []struct {
		id              int
		name            string
		description     *string
		price           *float64
		category        *string
		stockQuantity   *int
		manufacturer    *string
		releaseDate     *string
		discontinuedDate *string
		isFeatured      *bool
		rating          *float64
		warrantyMonths  *int
	}{
		{
			id:           1,
			name:         "Laptop Pro",
			description:  stringPtr("High-performance laptop"),
			price:        float64Ptr(1299.99),
			category:     stringPtr("Electronics"),
			stockQuantity: intPtr(25),
			manufacturer: stringPtr("TechCorp"),
			releaseDate:  stringPtr("2024-01-15"),
			discontinuedDate: nil,
			isFeatured:   boolPtr(true),
			rating:       float64Ptr(4.5),
			warrantyMonths: intPtr(24),
		},
		{
			id:           2,
			name:         "Basic Mouse",
			description:  nil, // NULL description
			price:        float64Ptr(19.99),
			category:     stringPtr("Accessories"),
			stockQuantity: intPtr(100),
			manufacturer: nil, // NULL manufacturer
			releaseDate:  stringPtr("2023-06-01"),
			discontinuedDate: nil,
			isFeatured:   boolPtr(false),
			rating:       nil, // NULL rating
			warrantyMonths: nil, // NULL warranty
		},
		{
			id:           3,
			name:         "Vintage Keyboard",
			description:  stringPtr("Classic mechanical keyboard"),
			price:        nil, // NULL price (discontinued)
			category:     stringPtr("Accessories"),
			stockQuantity: intPtr(0),
			manufacturer: stringPtr("RetroTech"),
			releaseDate:  stringPtr("2020-03-10"),
			discontinuedDate: stringPtr("2022-12-31"),
			isFeatured:   boolPtr(false),
			rating:       float64Ptr(4.8),
			warrantyMonths: intPtr(12),
		},
		{
			id:           4,
			name:         "Wireless Headphones",
			description:  stringPtr("Premium noise-cancelling headphones"),
			price:        float64Ptr(299.99),
			category:     nil, // NULL category
			stockQuantity: nil, // NULL stock quantity
			manufacturer: stringPtr("AudioTech"),
			releaseDate:  nil, // NULL release date
			discontinuedDate: nil,
			isFeatured:   nil, // NULL featured flag
			rating:       float64Ptr(4.7),
			warrantyMonths: intPtr(18),
		},
	}

	for _, p := range products {
		_, err = db.Exec(`INSERT INTO products (
			id, name, description, price, category, stock_quantity,
			manufacturer, release_date, discontinued_date, is_featured,
			rating, warranty_months
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			p.id, p.name, p.description, p.price, p.category, p.stockQuantity,
			p.manufacturer, p.releaseDate, p.discontinuedDate, p.isFeatured,
			p.rating, p.warrantyMonths)
		if err != nil {
			log.Printf("Failed to insert product %s: %v", p.name, err)
		}
	}
	fmt.Println("✓ Products inserted with various NULL values")

	// Example 2: Query and handle NULL values with sql.Null types
	fmt.Println("\n=== Example 2: Querying NULL values with sql.Null types ===")
	rows, err := db.Query("SELECT id, name, price, category, stock_quantity FROM products ORDER BY id")
	if err != nil {
		log.Fatal("Failed to query products:", err)
	}
	defer rows.Close()

	type Product struct {
		ID          int
		Name        string
		Price       sql.NullFloat64
		Category    sql.NullString
		StockQuantity sql.NullInt64
	}

	fmt.Println("ID | Name              | Price     | Category    | Stock")
	fmt.Println("---|-------------------|-----------|-------------|-------")

	for rows.Next() {
		var p Product
		err := rows.Scan(&p.ID, &p.Name, &p.Price, &p.Category, &p.StockQuantity)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		priceStr := "NULL"
		if p.Price.Valid {
			priceStr = fmt.Sprintf("$%8.2f", p.Price.Float64)
		}

		categoryStr := "NULL"
		if p.Category.Valid {
			categoryStr = p.Category.String
		}

		stockStr := "NULL"
		if p.StockQuantity.Valid {
			stockStr = fmt.Sprintf("%5d", p.StockQuantity.Int64)
		}

		fmt.Printf("%2d | %-17s | %9s | %-11s | %5s\n",
			p.ID, p.Name, priceStr, categoryStr, stockStr)
	}

	// Example 3: Filter for NULL values using IS NULL
	fmt.Println("\n=== Example 3: Finding NULL values with IS NULL ===")
	rows, err = db.Query("SELECT id, name FROM products WHERE price IS NULL ORDER BY id")
	if err != nil {
		log.Fatal("Failed to query NULL prices:", err)
	}
	defer rows.Close()

	fmt.Println("Products with NULL prices (discontinued or not yet priced):")
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		fmt.Printf("  - ID %d: %s\n", id, name)
	}

	// Example 4: Filter for non-NULL values using IS NOT NULL
	fmt.Println("\n=== Example 4: Finding non-NULL values with IS NOT NULL ===")
	rows, err = db.Query("SELECT id, name, rating FROM products WHERE rating IS NOT NULL ORDER BY rating DESC")
	if err != nil {
		log.Fatal("Failed to query non-NULL ratings:", err)
	}
	defer rows.Close()

	fmt.Println("Products with ratings (highest rated first):")
	for rows.Next() {
		var id int
		var name string
		var rating float64
		rows.Scan(&id, &name, &rating)
		fmt.Printf("  - ID %d: %s (Rating: %.1f/5.0)\n", id, name, rating)
	}

	// Example 5: Use COALESCE to handle NULL values in queries
	fmt.Println("\n=== Example 5: Using COALESCE to handle NULL values ===")
	rows, err = db.Query(`SELECT
		id,
		name,
		COALESCE(description, 'No description available') as description,
		COALESCE(price, 0.00) as price,
		COALESCE(category, 'Uncategorized') as category
	FROM products ORDER BY id`)
	if err != nil {
		log.Fatal("Failed to query with COALESCE:", err)
	}
	defer rows.Close()

	fmt.Println("Products with NULL values replaced by defaults:")
	fmt.Println("ID | Name              | Price  | Category     | Description")
	fmt.Println("---|-------------------|--------|--------------|------------------------------")
	for rows.Next() {
		var id int
		var name, description, category string
		var price float64
		rows.Scan(&id, &name, &description, &price, &category)
		fmt.Printf("%2d | %-17s | $%6.2f | %-12s | %s\n",
			id, name, price, category, truncateString(description, 30))
	}

	// Example 6: Update NULL values
	fmt.Println("\n=== Example 6: Updating NULL values ===")
	// Set default description for products without one
	result, err := db.Exec(`UPDATE products
		SET description = 'Product description coming soon'
		WHERE description IS NULL`)
	if err != nil {
		log.Printf("Failed to update NULL descriptions: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("✓ Updated %d products with default description\n", rowsAffected)
	}

	// Set featured flag to false where it's NULL
	result, err = db.Exec("UPDATE products SET is_featured = false WHERE is_featured IS NULL")
	if err != nil {
		log.Printf("Failed to update NULL featured flags: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("✓ Updated %d products with default featured flag\n", rowsAffected)
	}

	// Example 7: Count NULL values
	fmt.Println("\n=== Example 7: Counting NULL values ===")
	nullCounts := []struct {
		column string
		query  string
	}{
		{"description", "SELECT COUNT(*) FROM products WHERE description IS NULL"},
		{"price", "SELECT COUNT(*) FROM products WHERE price IS NULL"},
		{"category", "SELECT COUNT(*) FROM products WHERE category IS NULL"},
		{"stock_quantity", "SELECT COUNT(*) FROM products WHERE stock_quantity IS NULL"},
		{"release_date", "SELECT COUNT(*) FROM products WHERE release_date IS NULL"},
	}

	fmt.Println("NULL value counts by column:")
	for _, nc := range nullCounts {
		var count int
		err = db.QueryRow(nc.query).Scan(&count)
		if err != nil {
			log.Printf("Failed to count NULL %s: %v", nc.column, err)
			continue
		}
		fmt.Printf("  - %s: %d NULL values\n", nc.column, count)
	}

	// Example 8: Complex query with multiple NULL checks
	fmt.Println("\n=== Example 8: Complex query with NULL handling ===")
	query := `SELECT
		name,
		CASE
			WHEN price IS NULL THEN 'Price TBD'
			WHEN price < 50 THEN 'Budget'
			WHEN price < 200 THEN 'Mid-range'
			ELSE 'Premium'
		END as price_category,
		CASE
			WHEN stock_quantity IS NULL THEN 'Unknown'
			WHEN stock_quantity = 0 THEN 'Out of Stock'
			WHEN stock_quantity < 10 THEN 'Low Stock'
			ELSE 'In Stock'
		END as stock_status,
		CASE
			WHEN discontinued_date IS NOT NULL THEN 'Discontinued'
			WHEN release_date IS NULL THEN 'Coming Soon'
			ELSE 'Available'
		END as availability
	FROM products
	ORDER BY id`

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal("Failed to query with complex NULL handling:", err)
	}
	defer rows.Close()

	fmt.Println("Product availability analysis:")
	fmt.Println("Name              | Price Cat | Stock Status | Availability")
	fmt.Println("------------------|-----------|--------------|-------------")
	for rows.Next() {
		var name, priceCategory, stockStatus, availability string
		rows.Scan(&name, &priceCategory, &stockStatus, &availability)
		fmt.Printf("%-17s | %-9s | %-12s | %s\n", name, priceCategory, stockStatus, availability)
	}

	// Example 9: NULL-safe comparisons
	fmt.Println("\n=== Example 9: NULL-safe comparisons ===")
	// Find products that might need attention
	fmt.Println("Products that might need attention:")

	// Products without prices
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM products WHERE price IS NULL AND discontinued_date IS NULL").Scan(&count)
	if err == nil {
		fmt.Printf("- %d products without prices (not discontinued)\n", count)
	}

	// Products without stock information
	err = db.QueryRow("SELECT COUNT(*) FROM products WHERE stock_quantity IS NULL").Scan(&count)
	if err == nil {
		fmt.Printf("- %d products without stock information\n", count)
	}

	// Products without categories
	err = db.QueryRow("SELECT COUNT(*) FROM products WHERE category IS NULL").Scan(&count)
	if err == nil {
		fmt.Printf("- %d products without categories\n", count)
	}

	// Example 10: Best practices for NULL handling
	fmt.Println("\n=== Example 10: NULL handling best practices ===")

	// Show proper NULL checking in WHERE clauses
	fmt.Println("\nProper NULL checking patterns:")
	fmt.Println("✓ Use IS NULL:      WHERE column IS NULL")
	fmt.Println("✓ Use IS NOT NULL:  WHERE column IS NOT NULL")
	fmt.Println("✗ Don't use = NULL: WHERE column = NULL  (won't work)")
	fmt.Println("✗ Don't use != NULL: WHERE column != NULL (won't work)")

	// Show COALESCE usage
	fmt.Println("\nUsing COALESCE for default values:")
	fmt.Println("SELECT COALESCE(price, 0.00) FROM products")
	fmt.Println("SELECT COALESCE(description, 'No description') FROM products")

	// Clean up
	fmt.Println("\n=== Cleaning Up ===")
	_, err = db.Exec("DROP TABLE products")
	if err != nil {
		log.Printf("Failed to drop table: %v", err)
	}
	fmt.Println("✓ Table dropped successfully")

	// Summary
	fmt.Println("\n=== Summary ===")
	fmt.Println("This example demonstrated:")
	fmt.Println("- Inserting NULL values using pointers")
	fmt.Println("- Querying NULL values with sql.Null types")
	fmt.Println("- Finding NULL values with IS NULL")
	fmt.Println("- Finding non-NULL values with IS NOT NULL")
	fmt.Println("- Using COALESCE to handle NULL values")
	fmt.Println("- Updating NULL values")
	fmt.Println("- Counting NULL values by column")
	fmt.Println("- Complex queries with NULL handling")
	fmt.Println("- NULL-safe comparisons and business logic")
	fmt.Println("- Best practices for NULL handling")
	fmt.Println("\nAll operations completed successfully!")
}

// Helper functions for creating pointers to values
func stringPtr(s string) *string {
	return &s
}

func float64Ptr(f float64) *float64 {
	return &f
}

func intPtr(i int) *int {
	return &i
}

func boolPtr(b bool) *bool {
	return &b
}

// Helper function to truncate strings for display
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}