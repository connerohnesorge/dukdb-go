package main

import (
	"database/sql"
	"fmt"
	"log"

	"time"
	_ "github.com/dukdb/dukdb-go"
)

func main() {
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	fmt.Println("=== dukdb-go Index Examples ===\n")

	// Create sample table
	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			category VARCHAR(50),
			price DECIMAL(10,2),
			created_at TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Insert sample data
	db.Exec(`
		INSERT INTO products VALUES
			(1, 'Laptop', 'Electronics', 999.99, '2024-01-01'),
			(2, 'Mouse', 'Electronics', 29.99, '2024-01-02'),
			(3, 'Desk', 'Furniture', 299.99, '2024-01-03'),
			(4, 'Chair', 'Furniture', 149.99, '2024-01-04'),
			(5, 'Monitor', 'Electronics', 399.99, '2024-01-05')
	`)

	// Create indexes
	fmt.Println("1. Creating indexes")
	
	_, err = db.Exec("CREATE INDEX idx_category ON products(category)")
	if err != nil {
		log.Printf("Failed to create index: %v", err)
		return
	}
	fmt.Println("   ✓ Created index on category column")

	_, err = db.Exec("CREATE INDEX idx_price ON products(price)")
	if err != nil {
		log.Printf("Failed to create index: %v", err)
		return
	}
	fmt.Println("   ✓ Created index on price column")

	// Demonstrate query optimization
	demonstrateQueryOptimization(db)

	// Show index usage
	showIndexUsage(db)

	fmt.Println("\n✓ Index examples completed!")
}

func demonstrateQueryOptimization(db *sql.DB) {
	fmt.Println("\n2. Query Performance with Indexes")

	// Query without index benefit
	fmt.Println("   Query by category (uses index):")
	start := time.Now()
	rows, _ := db.Query("SELECT * FROM products WHERE category = 'Electronics'")
	if rows != nil {
		defer rows.Close()
		var count int
		for rows.Next() {
			count++
		}
		elapsed := time.Since(start)
		fmt.Printf("   ✓ Found %d products in %v\n", count, elapsed)
	}

	// Range query benefits from index
	fmt.Println("\n   Range query on price (uses index):")
	start = time.Now()
	var avgPrice float64
	err := db.QueryRow("SELECT AVG(price) FROM products WHERE price BETWEEN 100 AND 500").Scan(&avgPrice)
	if err == nil {
		elapsed := time.Since(start)
		fmt.Printf("   ✓ Average price: $%.2f in %v\n", avgPrice, elapsed)
	}
}

func showIndexUsage(db *sql.DB) {
	fmt.Println("\n3. Index Information")
	
	// Show that queries are optimized
	fmt.Println("   Indexes created:")
	fmt.Println("   - idx_category: ON products(category)")
	fmt.Println("   - idx_price: ON products(price)")
	fmt.Println("\n   Benefits:")
	fmt.Println("   - Faster filtering with WHERE clause")
	fmt.Println("   - Improved JOIN performance")
	fmt.Println("   - Better sorting with ORDER BY")
}
