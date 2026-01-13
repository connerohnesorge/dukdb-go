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

	// Create an inventory table
	_, err = db.Exec(`CREATE TABLE inventory (
		id INTEGER PRIMARY KEY,
		product_name VARCHAR(100),
		category VARCHAR(50),
		price DECIMAL(10,2),
		quantity INTEGER,
		last_updated TIMESTAMP,
		discount_percentage DECIMAL(5,2) DEFAULT 0.00
	)`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Insert sample inventory data
	items := []struct {
		id       int
		name     string
		category string
		price    float64
		quantity int
	}{
		{1, "Laptop", "Electronics", 999.99, 10},
		{2, "Mouse", "Electronics", 29.99, 50},
		{3, "Keyboard", "Electronics", 79.99, 30},
		{4, "Monitor", "Electronics", 299.99, 15},
		{5, "Desk", "Furniture", 199.99, 5},
		{6, "Chair", "Furniture", 149.99, 20},
		{7, "Bookshelf", "Furniture", 89.99, 8},
		{8, "Lamp", "Furniture", 39.99, 25},
		{9, "Notebook", "Stationery", 4.99, 100},
		{10, "Pen Set", "Stationery", 12.99, 75},
	}

	for _, item := range items {
		_, err = db.Exec("INSERT INTO inventory (id, product_name, category, price, quantity) VALUES (?, ?, ?, ?, ?)",
			item.id, item.name, item.category, item.price, item.quantity)
		if err != nil {
			log.Printf("Failed to insert item %s: %v", item.name, err)
		}
	}
	fmt.Println("Initial inventory data inserted")

	// Display initial data
	fmt.Println("\n=== Initial Inventory ===")
	displayInventory(db)

	// Example 1: Simple UPDATE with WHERE clause
	fmt.Println("\n=== Example 1: Simple UPDATE with WHERE clause ===")
	result, err := db.Exec("UPDATE inventory SET price = ? WHERE id = ?", 899.99, 1)
	if err != nil {
		log.Printf("Failed to update price: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Updated %d item(s): Laptop price reduced to $899.99\n", rowsAffected)
	}

	// Example 2: UPDATE multiple columns
	fmt.Println("\n=== Example 2: UPDATE multiple columns ===")
	result, err = db.Exec(`UPDATE inventory
		SET price = ?, quantity = ?
		WHERE product_name = ?`, 24.99, 75, "Mouse")
	if err != nil {
		log.Printf("Failed to update Mouse: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Updated %d item(s): Mouse price and quantity adjusted\n", rowsAffected)
	}

	// Example 3: UPDATE with calculation
	fmt.Println("\n=== Example 3: UPDATE with calculation ===")
	result, err = db.Exec("UPDATE inventory SET price = price * ? WHERE category = ?", 0.9, "Electronics")
	if err != nil {
		log.Printf("Failed to update Electronics: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Updated %d item(s): All Electronics discounted by 10%%\n", rowsAffected)
	}

	// Example 4: UPDATE with complex WHERE conditions
	fmt.Println("\n=== Example 4: UPDATE with complex WHERE conditions ===")
	result, err = db.Exec(`UPDATE inventory
		SET discount_percentage = 15.00
		WHERE quantity > ? AND price < ? AND category != ?`,
		20, 100.00, "Electronics")
	if err != nil {
		log.Printf("Failed to apply bulk discount: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Updated %d item(s): 15%% discount applied to qualifying items\n", rowsAffected)
	}

	// Example 5: UPDATE with subquery (if supported)
	fmt.Println("\n=== Example 5: UPDATE based on aggregate ===")
	// First, get the average price for each category
	var avgPrice float64
	err = db.QueryRow("SELECT AVG(price) FROM inventory WHERE category = ?", "Furniture").Scan(&avgPrice)
	if err != nil {
		log.Printf("Failed to get average price: %v", err)
	} else {
		// Update items above average price
		result, err = db.Exec(`UPDATE inventory
			SET discount_percentage = discount_percentage + 5.00
			WHERE category = ? AND price > ?`,
			"Furniture", avgPrice)
		if err != nil {
			log.Printf("Failed to update above-average items: %v", err)
		} else {
			rowsAffected, _ := result.RowsAffected()
			fmt.Printf("Updated %d item(s): Extra 5%% discount for Furniture above average ($%.2f)\n",
				rowsAffected, avgPrice)
		}
	}

	// Example 6: UPDATE with CASE statement
	fmt.Println("\n=== Example 6: UPDATE with CASE statement ===")
	_, err = db.Exec(`UPDATE inventory
		SET quantity = CASE
			WHEN quantity < 10 THEN quantity + 20
			WHEN quantity < 25 THEN quantity + 10
			ELSE quantity + 5
		END`)
	if err != nil {
		log.Printf("Failed to update quantities: %v", err)
	} else {
		fmt.Println("Updated all quantities based on current stock levels")
	}

	// Example 7: UPDATE with LIMIT (if supported)
	fmt.Println("\n=== Example 7: UPDATE with LIMIT ===")
	// Note: Some databases don't support LIMIT in UPDATE, we'll simulate with WHERE
	result, err = db.Exec(`UPDATE inventory
		SET discount_percentage = 25.00
		WHERE category = ? AND price < ?
		AND id IN (SELECT id FROM inventory WHERE category = ? AND price < ? LIMIT 3)`,
		"Stationery", 10.00, "Stationery", 10.00)
	if err != nil {
		// Try without subquery if not supported
		result, err = db.Exec(`UPDATE inventory
			SET discount_percentage = 25.00
			WHERE category = ? AND price < ?`,
			"Stationery", 10.00)
		if err != nil {
			log.Printf("Failed to update Stationery items: %v", err)
		} else {
			rowsAffected, _ := result.RowsAffected()
			fmt.Printf("Updated %d Stationery items under $10\n", rowsAffected)
		}
	}

	// Example 8: UPDATE with simple calculation
	fmt.Println("\n=== Example 8: UPDATE with simple arithmetic ===")
	// Apply restocking fee to low-quantity items
	result, err = db.Exec(`UPDATE inventory
		SET price = price * 1.05
		WHERE quantity < 10`)
	if err != nil {
		log.Printf("Failed to apply restocking fee: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Updated %d item(s): 5%% restocking fee applied to low-quantity items\n", rowsAffected)
	}

	// Example 9: UPDATE with COALESCE
	fmt.Println("\n=== Example 9: UPDATE with COALESCE ===")
	result, err = db.Exec(`UPDATE inventory
		SET discount_percentage = COALESCE(discount_percentage, 0.00) + 5.00,
			price = COALESCE(price, 0.00)
		WHERE discount_percentage IS NULL OR discount_percentage < 5.00`)
	if err != nil {
		log.Printf("Failed to update with COALESCE: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Updated %d item(s): Applied minimum discount using COALESCE\n", rowsAffected)
	}

	// Example 10: Conditional UPDATE with Go logic
	fmt.Println("\n=== Example 10: Conditional UPDATE with Go logic ===")
	// First, query items that need updating
	rows, err := db.Query("SELECT id, quantity, price FROM inventory WHERE quantity > ?", 50)
	if err != nil {
		log.Printf("Failed to query high-quantity items: %v", err)
	} else {
		defer rows.Close()
		count := 0
		for rows.Next() {
			var id, quantity int
			var price float64
			rows.Scan(&id, &quantity, &price)

			// Apply different discounts based on quantity
			discount := 0.0
			if quantity > 75 {
				discount = 30.0
			} else if quantity > 50 {
				discount = 20.0
			}

			_, err = db.Exec("UPDATE inventory SET discount_percentage = ? WHERE id = ?", discount, id)
			if err != nil {
				log.Printf("Failed to update item %d: %v", id, err)
			} else {
				count++
			}
		}
		fmt.Printf("Updated %d high-quantity items with conditional discounts\n", count)
	}

	// Display final inventory
	fmt.Println("\n=== Final Inventory After All Updates ===")
	displayInventory(db)

	// Show summary of changes
	fmt.Println("\n=== Summary of Changes ===")
	var totalItems, discountedItems int
	var avgDiscount float64

	err = db.QueryRow("SELECT COUNT(*) FROM inventory").Scan(&totalItems)
	if err == nil {
		err = db.QueryRow("SELECT COUNT(*) FROM inventory WHERE discount_percentage > 0").Scan(&discountedItems)
		if err == nil {
			err = db.QueryRow("SELECT AVG(discount_percentage) FROM inventory WHERE discount_percentage > 0").Scan(&avgDiscount)
			if err == nil {
				fmt.Printf("Total items: %d\n", totalItems)
				fmt.Printf("Items with discounts: %d (%.1f%%)\n", discountedItems, float64(discountedItems)/float64(totalItems)*100)
				fmt.Printf("Average discount: %.1f%%\n", avgDiscount)
			}
		}
	}

	// Clean up
	_, err = db.Exec("DROP TABLE inventory")
	if err != nil {
		log.Printf("Failed to drop table: %v", err)
	}
	fmt.Println("\nTable dropped successfully")
}

// Helper function to display inventory
func displayInventory(db *sql.DB) {
	rows, err := db.Query(`SELECT
		id, product_name, category, price, quantity,
		COALESCE(discount_percentage, 0) as discount
	FROM inventory
	ORDER BY category, product_name`)
	if err != nil {
		log.Printf("Failed to query inventory: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println("ID | Product Name         | Category   | Price  | Qty | Discount")
	fmt.Println("---|----------------------|------------|--------|-----|----------")

	for rows.Next() {
		var id, quantity int
		var productName, category string
		var price, discount float64

		err := rows.Scan(&id, &productName, &category, &price, &quantity, &discount)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		fmt.Printf("%2d | %-20s | %-10s | $%6.2f | %3d | %5.1f%%\n",
			id, productName, category, price, quantity, discount)
	}
}