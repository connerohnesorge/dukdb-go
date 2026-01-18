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

	// Create a products table with sample data
	_, err = db.Exec(`CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name VARCHAR(100),
		category VARCHAR(50),
		price DECIMAL(10,2),
		stock INTEGER,
		available BOOLEAN,
		created_at DATE
	)`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Insert sample products
	products := []struct {
		id        int
		name      string
		category  string
		price     float64
		stock     int
		available bool
		createdAt string
	}{
		{1, "Laptop", "Electronics", 999.99, 10, true, "2024-01-15"},
		{2, "Smartphone", "Electronics", 699.99, 25, true, "2024-01-20"},
		{3, "Desk Chair", "Furniture", 249.99, 5, true, "2024-02-01"},
		{4, "Coffee Table", "Furniture", 399.99, 0, false, "2024-02-10"},
		{5, "Headphones", "Electronics", 149.99, 50, true, "2024-02-15"},
		{6, "Bookshelf", "Furniture", 179.99, 8, true, "2024-02-20"},
		{7, "Monitor", "Electronics", 299.99, 15, true, "2024-03-01"},
		{8, "Desk Lamp", "Furniture", 59.99, 20, true, "2024-03-05"},
		{9, "Tablet", "Electronics", 449.99, 0, false, "2024-03-10"},
		{10, "Office Desk", "Furniture", 599.99, 3, true, "2024-03-15"},
	}

	for _, p := range products {
		_, err = db.Exec(
			"INSERT INTO products (id, name, category, price, stock, available, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
			p.id,
			p.name,
			p.category,
			p.price,
			p.stock,
			p.available,
			p.createdAt,
		)
		if err != nil {
			log.Printf("Failed to insert product %s: %v", p.name, err)
		}
	}

	fmt.Println("Sample data inserted successfully")

	// Example 1: Simple WHERE with equality
	fmt.Println("\n=== Example 1: Simple WHERE with equality ===")
	fmt.Println("Find all Electronics products:")
	rows, err := db.Query(
		"SELECT name, price FROM products WHERE category = ? ORDER BY name",
		"Electronics",
	)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var price float64
		rows.Scan(&name, &price)
		fmt.Printf("  %s - $%.2f\n", name, price)
	}

	// Example 2: WHERE with comparison operators
	fmt.Println("\n=== Example 2: WHERE with comparison operators ===")
	fmt.Println("Products with price less than $200:")
	rows, err = db.Query("SELECT name, price FROM products WHERE price < 200 ORDER BY price")
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var price float64
		rows.Scan(&name, &price)
		fmt.Printf("  %s - $%.2f\n", name, price)
	}

	// Example 3: WHERE with range (BETWEEN)
	fmt.Println("\n=== Example 3: WHERE with range (BETWEEN) ===")
	fmt.Println("Products with price between $200 and $500:")
	rows, err = db.Query(
		"SELECT name, price FROM products WHERE price BETWEEN 200 AND 500 ORDER BY price",
	)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var price float64
		rows.Scan(&name, &price)
		fmt.Printf("  %s - $%.2f\n", name, price)
	}

	// Example 4: WHERE with multiple conditions (AND)
	fmt.Println("\n=== Example 4: WHERE with multiple conditions (AND) ===")
	fmt.Println("Available Electronics products under $300:")
	rows, err = db.Query(
		"SELECT name, price, stock FROM products WHERE category = ? AND available = ? AND price < ?",
		"Electronics",
		true,
		300,
	)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var price float64
		var stock int
		rows.Scan(&name, &price, &stock)
		fmt.Printf("  %s - $%.2f (Stock: %d)\n", name, price, stock)
	}

	// Example 5: WHERE with OR conditions
	fmt.Println("\n=== Example 5: WHERE with OR conditions ===")
	fmt.Println("Products that are either out of stock or unavailable:")
	rows, err = db.Query(
		"SELECT name, stock, available FROM products WHERE stock = 0 OR available = false ORDER BY name",
	)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var stock int
		var available bool
		rows.Scan(&name, &stock, &available)
		fmt.Printf("  %s (Stock: %d, Available: %t)\n", name, stock, available)
	}

	// Example 6: WHERE with IN clause
	fmt.Println("\n=== Example 6: WHERE with IN clause ===")
	fmt.Println("Products in specific categories:")
	categories := []string{"Electronics", "Furniture"}
	query := "SELECT name, category FROM products WHERE category IN (?, ?) ORDER BY category, name"
	rows, err = db.Query(query, categories[0], categories[1])
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, category string
		rows.Scan(&name, &category)
		fmt.Printf("  %s (%s)\n", name, category)
	}

	// Example 7: WHERE with LIKE (pattern matching)
	fmt.Println("\n=== Example 7: WHERE with LIKE (pattern matching) ===")
	fmt.Println("Products with names containing 'Desk':")
	rows, err = db.Query("SELECT name FROM products WHERE name LIKE ?", "%Desk%")
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		rows.Scan(&name)
		fmt.Printf("  %s\n", name)
	}

	// Example 8: WHERE with NOT conditions
	fmt.Println("\n=== Example 8: WHERE with NOT conditions ===")
	fmt.Println("Products that are NOT in Electronics category:")
	rows, err = db.Query(
		"SELECT name, category FROM products WHERE category != ? ORDER BY category, name",
		"Electronics",
	)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, category string
		rows.Scan(&name, &category)
		fmt.Printf("  %s (%s)\n", name, category)
	}

	// Example 9: WHERE with NULL handling
	fmt.Println("\n=== Example 9: WHERE with NULL handling ===")

	// First, add a product with NULL category
	_, err = db.Exec(
		"INSERT INTO products (id, name, category, price, stock, available, created_at) VALUES (?, ?, NULL, ?, ?, ?, ?)",
		11,
		"Mystery Item",
		99.99,
		5,
		true,
		"2024-03-20",
	)
	if err != nil {
		log.Printf("Failed to insert mystery item: %v", err)
	}

	fmt.Println("Products with NULL category:")
	rows, err = db.Query("SELECT name, category FROM products WHERE category IS NULL")
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var category sql.NullString
		rows.Scan(&name, &category)
		if category.Valid {
			fmt.Printf("  %s (Category: %s)\n", name, category.String)
		} else {
			fmt.Printf("  %s (Category: NULL)\n", name)
		}
	}

	// Example 10: WHERE with date comparisons
	fmt.Println("\n=== Example 10: WHERE with date comparisons ===")
	fmt.Println("Products created after February 1, 2024:")
	rows, err = db.Query(
		"SELECT name, created_at FROM products WHERE created_at > ? ORDER BY created_at",
		"2024-02-01",
	)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var createdAt string
		rows.Scan(&name, &createdAt)
		fmt.Printf("  %s (Created: %s)\n", name, createdAt)
	}

	// Summary statistics
	fmt.Println("\n=== Summary Statistics ===")
	var totalProducts, availableProducts, totalValue float64
	var avgPrice float64

	err = db.QueryRow("SELECT COUNT(*) FROM products").Scan(&totalProducts)
	if err != nil {
		log.Fatal("Failed to get count:", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM products WHERE available = true").
		Scan(&availableProducts)
	if err != nil {
		log.Fatal("Failed to get available count:", err)
	}

	err = db.QueryRow("SELECT AVG(price) FROM products").Scan(&avgPrice)
	if err != nil {
		log.Fatal("Failed to get average price:", err)
	}

	err = db.QueryRow("SELECT SUM(price * stock) FROM products").Scan(&totalValue)
	if err != nil {
		log.Fatal("Failed to get total value:", err)
	}

	fmt.Printf("Total products: %.0f\n", totalProducts)
	fmt.Printf("Available products: %.0f\n", availableProducts)
	fmt.Printf("Average price: $%.2f\n", avgPrice)
	fmt.Printf("Total inventory value: $%.2f\n", totalValue)

	// Clean up
	_, err = db.Exec("DROP TABLE products")
	if err != nil {
		log.Printf("Failed to drop table: %v", err)
	}
	fmt.Println("\nTable dropped successfully")
}
