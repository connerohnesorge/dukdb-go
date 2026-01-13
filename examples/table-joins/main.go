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

	fmt.Println("=== Basic Example 10: Simple JOIN Operations ===\n")

	// Create customers table
	_, err = db.Exec(`CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		first_name VARCHAR(50) NOT NULL,
		last_name VARCHAR(50) NOT NULL,
		email VARCHAR(100),
		phone VARCHAR(20),
		city VARCHAR(50),
		country VARCHAR(50)
	)`)
	if err != nil {
		log.Fatal("Failed to create customers table:", err)
	}

	// Create orders table
	_, err = db.Exec(`CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		order_date DATE,
		total_amount DECIMAL(10,2),
		status VARCHAR(20),
		shipping_city VARCHAR(50),
		shipping_country VARCHAR(50)
	)`)
	if err != nil {
		log.Fatal("Failed to create orders table:", err)
	}

	// Create products table
	_, err = db.Exec(`CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		category VARCHAR(50),
		price DECIMAL(10,2),
		stock_quantity INTEGER
	)`)
	if err != nil {
		log.Fatal("Failed to create products table:", err)
	}

	// Create order_items table
	_, err = db.Exec(`CREATE TABLE order_items (
		id INTEGER PRIMARY KEY,
		order_id INTEGER,
		product_id INTEGER,
		quantity INTEGER,
		unit_price DECIMAL(10,2),
		discount DECIMAL(5,2) DEFAULT 0
	)`)
	if err != nil {
		log.Fatal("Failed to create order_items table:", err)
	}

	// Insert sample customers
	customers := []struct {
		id        int
		firstName string
		lastName  string
		email     string
		phone     string
		city      string
		country   string
	}{
		{1, "John", "Doe", "john.doe@email.com", "555-0101", "New York", "USA"},
		{2, "Jane", "Smith", "jane.smith@email.com", "555-0102", "Los Angeles", "USA"},
		{3, "Bob", "Johnson", "bob.j@email.com", "555-0103", "Chicago", "USA"},
		{4, "Alice", "Williams", "alice.w@email.com", "555-0104", "Toronto", "Canada"},
		{5, "Charlie", "Brown", "charlie.b@email.com", "555-0105", "Vancouver", "Canada"},
		{6, "Diana", "Prince", "diana.p@email.com", "555-0106", "London", "UK"},
		{7, "Edward", "Norton", "edward.n@email.com", "555-0107", "Manchester", "UK"},
		{8, "Fiona", "Garcia", "fiona.g@email.com", "555-0108", "Madrid", "Spain"},
	}

	for _, c := range customers {
		_, err = db.Exec(`INSERT INTO customers (id, first_name, last_name, email, phone, city, country)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			c.id, c.firstName, c.lastName, c.email, c.phone, c.city, c.country)
		if err != nil {
			log.Printf("Failed to insert customer %s: %v", c.firstName, err)
		}
	}
	fmt.Println("✓ Sample customers inserted (8 records)")

	// Insert sample products
	products := []struct {
		id       int
		name     string
		category string
		price    float64
		stock    int
	}{
		{1, "Laptop Pro", "Electronics", 1299.99, 10},
		{2, "Wireless Mouse", "Accessories", 29.99, 50},
		{3, "Office Chair", "Furniture", 249.99, 15},
		{4, "Smartphone", "Electronics", 699.99, 25},
		{5, "Desk Lamp", "Furniture", 45.99, 30},
		{6, "Keyboard", "Accessories", 79.99, 40},
		{7, "Monitor", "Electronics", 399.99, 20},
		{8, "Notebook", "Stationery", 4.99, 100},
		{9, "Tablet", "Electronics", 449.99, 18},
		{10, "Headphones", "Accessories", 149.99, 35},
	}

	for _, p := range products {
		_, err = db.Exec(`INSERT INTO products (id, name, category, price, stock_quantity)
			VALUES (?, ?, ?, ?, ?)`,
			p.id, p.name, p.category, p.price, p.stock)
		if err != nil {
			log.Printf("Failed to insert product %s: %v", p.name, err)
		}
	}
	fmt.Println("✓ Sample products inserted (10 records)")

	// Insert sample orders
	orders := []struct {
		id          int
		customerID  int
		orderDate   string
		totalAmount float64
		status      string
		shipCity    string
		shipCountry string
	}{
		{1, 1, "2024-01-15", 1329.98, "Completed", "New York", "USA"},
		{2, 2, "2024-01-16", 729.98, "Completed", "Los Angeles", "USA"},
		{3, 3, "2024-01-17", 249.99, "Shipped", "Chicago", "USA"},
		{4, 4, "2024-01-18", 704.98, "Completed", "Toronto", "Canada"},
		{5, 5, "2024-01-19", 45.99, "Processing", "Vancouver", "Canada"},
		{6, 6, "2024-01-20", 404.98, "Completed", "London", "UK"},
		{7, 7, "2024-01-21", 79.99, "Shipped", "Manchester", "UK"},
		{8, 8, "2024-01-22", 454.98, "Completed", "Madrid", "Spain"},
		{9, 1, "2024-01-23", 1549.98, "Processing", "New York", "USA"},
		{10, 2, "2024-01-24", 154.98, "Shipped", "Los Angeles", "USA"},
		{11, 9, "2024-01-25", 0.00, "Pending", "Unknown", "Unknown"}, // Customer doesn't exist
	}

	for _, o := range orders {
		_, err = db.Exec(`INSERT INTO orders (id, customer_id, order_date, total_amount, status, shipping_city, shipping_country)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			o.id, o.customerID, o.orderDate, o.totalAmount, o.status, o.shipCity, o.shipCountry)
		if err != nil {
			log.Printf("Failed to insert order %d: %v", o.id, err)
		}
	}
	fmt.Println("✓ Sample orders inserted (11 records)")

	// Insert sample order items
	orderItems := []struct {
		id        int
		orderID   int
		productID int
		quantity  int
		unitPrice float64
		discount  float64
	}{
		{1, 1, 1, 1, 1299.99, 0},
		{2, 1, 2, 1, 29.99, 0},
		{3, 2, 4, 1, 699.99, 0},
		{4, 2, 6, 1, 79.99, 0},
		{5, 3, 3, 1, 249.99, 0},
		{6, 4, 4, 1, 699.99, 0},
		{7, 4, 8, 1, 4.99, 0},
		{8, 5, 5, 1, 45.99, 0},
		{9, 6, 7, 1, 399.99, 0},
		{10, 6, 8, 1, 4.99, 0},
		{11, 7, 6, 1, 79.99, 0},
		{12, 8, 9, 1, 449.99, 0},
		{13, 8, 2, 1, 24.99, 0},
		{14, 9, 1, 1, 1299.99, 0},
		{15, 9, 9, 1, 449.99, 0},
		{16, 10, 10, 1, 149.99, 0},
		{17, 10, 8, 1, 4.99, 0},
	}

	for _, item := range orderItems {
		_, err = db.Exec(`INSERT INTO order_items (id, order_id, product_id, quantity, unit_price, discount)
			VALUES (?, ?, ?, ?, ?, ?)`,
			item.id, item.orderID, item.productID, item.quantity, item.unitPrice, item.discount)
		if err != nil {
			log.Printf("Failed to insert order item %d: %v", item.id, err)
		}
	}
	fmt.Println("✓ Sample order items inserted (17 records)")

	// Example 1: Simple INNER JOIN - Get orders with customer information
	fmt.Println("\n=== Example 1: INNER JOIN - Orders with Customer Info ===")
	rows, err := db.Query(`SELECT
		o.id as order_id,
		c.first_name,
		c.last_name,
		o.order_date,
		o.total_amount,
		o.status
	FROM orders o
	INNER JOIN customers c ON o.customer_id = c.id
	ORDER BY o.order_date
	LIMIT 10`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	fmt.Println("Order ID | Customer Name     | Order Date | Total    | Status")
	fmt.Println("---------|-------------------|------------|----------|--------")
	for rows.Next() {
		var orderID int
		var firstName, lastName string
		var orderDate string
		var totalAmount float64
		var status string

		err := rows.Scan(&orderID, &firstName, &lastName, &orderDate, &totalAmount, &status)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		customerName := firstName + " " + lastName
		fmt.Printf("%8d | %-17s | %10s | $%7.2f | %s\n",
			orderID, customerName, orderDate, totalAmount, status)
	}

	// Example 2: LEFT JOIN - Get all customers and their orders
	fmt.Println("\n=== Example 2: LEFT JOIN - All Customers and Their Orders ===")
	rows, err = db.Query(`SELECT
		c.id,
		c.first_name,
		c.last_name,
		c.city,
		c.country,
		COUNT(o.id) as order_count,
		COALESCE(SUM(o.total_amount), 0) as total_spent
	FROM customers c
	LEFT JOIN orders o ON c.id = o.customer_id
	GROUP BY c.id, c.first_name, c.last_name, c.city, c.country
	ORDER BY COALESCE(SUM(o.total_amount), 0) DESC`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	fmt.Println("Customer Name     | City          | Country | Orders | Total Spent")
	fmt.Println("------------------|---------------|---------|--------|------------")
	for rows.Next() {
		var id int
		var firstName, lastName, city, country string
		var orderCount int
		var totalSpent float64

		err := rows.Scan(&id, &firstName, &lastName, &city, &country, &orderCount, &totalSpent)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		customerName := firstName + " " + lastName
		fmt.Printf("%-17s | %-13s | %-7s | %6d | $%9.2f\n",
			customerName, city, country, orderCount, totalSpent)
	}
	// Example 3: Multiple JOINs - Orders with items and products
	fmt.Println("\n=== Example 3: Multiple JOINs - Order Details ===")
	rows, err = db.Query(`SELECT
		o.id as order_id,
		c.first_name,
		c.last_name,
		p.name as product_name,
		oi.quantity,
		oi.unit_price,
		(oi.quantity * oi.unit_price * (1 - oi.discount/100)) as item_total
	FROM orders o
	JOIN customers c ON o.customer_id = c.id
	JOIN order_items oi ON o.id = oi.order_id
	JOIN products p ON oi.product_id = p.id
	WHERE o.status = 'Completed'
	ORDER BY o.id, p.name
	LIMIT 15`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	fmt.Println("Order | Customer          | Product              | Qty | Price   | Total")
	fmt.Println("------|-------------------|----------------------|-----|---------|--------")
	for rows.Next() {
		var orderID, quantity int
		var firstName, lastName, productName string
		var unitPrice, itemTotal float64

		err := rows.Scan(&orderID, &firstName, &lastName, &productName, &quantity, &unitPrice, &itemTotal)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		customerName := firstName + " " + lastName
		fmt.Printf("%5d | %-17s | %-20s | %3d | $%6.2f | $%6.2f\n",
			orderID, customerName, productName, quantity, unitPrice, itemTotal)
	}

	// Example 4: RIGHT JOIN - Get products that have never been ordered
	fmt.Println("\n=== Example 4: RIGHT JOIN - Products Never Ordered ===")
	rows, err = db.Query(`SELECT
		p.id,
		p.name,
		p.category,
		p.price,
		p.stock_quantity
	FROM order_items oi
	RIGHT JOIN products p ON oi.product_id = p.id
	WHERE oi.id IS NULL
	ORDER BY p.name`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	fmt.Println("ID | Product Name         | Category    | Price    | Stock")
	fmt.Println("---|----------------------|-------------|----------|-------")
	for rows.Next() {
		var id, stockQuantity int
		var name, category string
		var price float64

		err := rows.Scan(&id, &name, &category, &price, &stockQuantity)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		fmt.Printf("%2d | %-20s | %-11s | $%7.2f | %5d\n",
			id, name, category, price, stockQuantity)
	}

	// Example 5: FULL OUTER JOIN simulation (using UNION)
	fmt.Println("\n=== Example 5: FULL OUTER JOIN Simulation ===")
	fmt.Println("All customers and their orders (including customers without orders and orders without valid customers):")

	// Get customers with orders
	rows, err = db.Query(`SELECT
		c.id as customer_id,
		c.first_name,
		c.last_name,
		c.country,
		o.id as order_id,
		o.total_amount,
		'Customer with Order' as relationship
	FROM customers c
	JOIN orders o ON c.id = o.customer_id
	UNION ALL
	-- Customers without orders
	SELECT
		c.id,
		c.first_name,
		c.last_name,
		c.country,
		NULL,
		NULL,
		'Customer without Order'
	FROM customers c
	WHERE c.id NOT IN (SELECT DISTINCT customer_id FROM orders WHERE customer_id IS NOT NULL)
	UNION ALL
	-- Orders without valid customers
	SELECT
		NULL,
		'Unknown',
		'Customer',
		o.shipping_country,
		o.id,
		o.total_amount,
		'Order without Valid Customer'
	FROM orders o
	WHERE o.customer_id NOT IN (SELECT id FROM customers)
	ORDER BY name, order_id`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	fmt.Println("Customer ID | Customer Name     | Country     | Order ID | Amount   | Relationship")
	fmt.Println("------------|-------------------|-------------|----------|----------|----------------------")
	for rows.Next() {
		var customerID, orderID sql.NullInt64
		var firstName, lastName, country, relationship string
		var totalAmount sql.NullFloat64

		err := rows.Scan(&customerID, &firstName, &lastName, &country, &orderID, &totalAmount, &relationship)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		customerIDStr := "NULL"
		if customerID.Valid {
			customerIDStr = fmt.Sprintf("%9d", customerID.Int64)
		}

		orderIDStr := "NULL"
		if orderID.Valid {
			orderIDStr = fmt.Sprintf("%8d", orderID.Int64)
		}

		amountStr := "NULL"
		if totalAmount.Valid {
			amountStr = fmt.Sprintf("$%7.2f", totalAmount.Float64)
		}

		name := firstName + " " + lastName
		fmt.Printf("%11s | %-17s | %-11s | %8s | %8s | %s\n",
			customerIDStr, name, country, orderIDStr, amountStr, relationship)
	}

	// Example 6: Self JOIN - Get customers from the same city
	fmt.Println("\n=== Example 6: Self JOIN - Customers in Same City ===")
	rows, err = db.Query(`SELECT
		c1.first_name as customer1_first,
		c1.last_name as customer1_last,
		c2.first_name as customer2_first,
		c2.last_name as customer2_last,
		c1.city,
		c1.country
	FROM customers c1
	JOIN customers c2 ON c1.city = c2.city AND c1.id < c2.id
	ORDER BY c1.city, c1.last_name`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	fmt.Println("Customer 1          | Customer 2          | City          | Country")
	fmt.Println("--------------------|---------------------|---------------|--------")
	for rows.Next() {
		var customer1First, customer1Last, customer2First, customer2Last, city, country string
		err := rows.Scan(&customer1First, &customer1Last, &customer2First, &customer2Last, &city, &country)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		customer1 := customer1First + " " + customer1Last
		customer2 := customer2First + " " + customer2Last
		fmt.Printf("%-19s | %-19s | %-13s | %s\n", customer1, customer2, city, country)
	}

	// Example 7: Aggregate with JOIN - Sales by category and country
	fmt.Println("\n=== Example 7: Aggregate with JOIN - Sales by Category and Country ===")
	rows, err = db.Query(`SELECT
		c.country,
		p.category,
		COUNT(DISTINCT o.id) as order_count,
		SUM(oi.quantity) as total_quantity,
		SUM(oi.quantity * oi.unit_price * (1 - oi.discount/100)) as total_sales
	FROM customers c
	JOIN orders o ON c.id = o.customer_id
	JOIN order_items oi ON o.id = oi.order_id
	JOIN products p ON oi.product_id = p.id
	WHERE o.status = 'Completed'
	GROUP BY c.country, p.category
	ORDER BY c.country, SUM(oi.quantity * oi.unit_price * (1 - oi.discount/100)) DESC`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	fmt.Println("Country | Category    | Orders | Quantity | Total Sales")
	fmt.Println("--------|-------------|--------|----------|------------")
	for rows.Next() {
		var country, category string
		var orderCount, totalQuantity int
		var totalSales float64

		err := rows.Scan(&country, &category, &orderCount, &totalQuantity, &totalSales)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		fmt.Printf("%-7s | %-11s | %6d | %8d | $%10.2f\n",
			country, category, orderCount, totalQuantity, totalSales)
	}

	// Example 8: Subquery with JOIN - Top customers by category
	fmt.Println("\n=== Example 8: Subquery with JOIN - Top Customer per Category ===")
	rows, err = db.Query(`SELECT
		category_sales.category,
		COALESCE(c.first_name || ' ' || c.last_name, 'Unknown') as top_customer,
		COALESCE(category_sales.total_spent, 0) as total_spent
	FROM (
		SELECT
			p.category,
			o.customer_id,
			SUM(o.total_amount) as total_spent,
			RANK() OVER (PARTITION BY p.category ORDER BY SUM(o.total_amount) DESC) as rank
		FROM orders o
		JOIN order_items oi ON o.id = oi.order_id
		JOIN products p ON oi.product_id = p.id
		WHERE o.status = 'Completed'
		GROUP BY p.category, o.customer_id
	) category_sales
	LEFT JOIN customers c ON category_sales.customer_id = c.id
	WHERE category_sales.rank = 1
	ORDER BY category_sales.category`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	fmt.Println("Category    | Top Customer      | Amount Spent")
	fmt.Println("------------|-------------------|--------------")
	for rows.Next() {
		var category, topCustomer string
		var totalSpent float64

		err := rows.Scan(&category, &topCustomer, &totalSpent)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		fmt.Printf("%-11s | %-17s | $%12.2f\n", category, topCustomer, totalSpent)
	}

	// Example 9: Update with JOIN - Set order status based on customer location
	fmt.Println("\n=== Example 9: Update with JOIN - Set Order Status ===")
	// First, show current status
	fmt.Println("Before update:")
	var beforeCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM orders o
		JOIN customers c ON o.customer_id = c.id
		WHERE c.country = 'Canada' AND o.status = 'Processing'`).Scan(&beforeCount)
	if err == nil {
		fmt.Printf("  Processing orders for Canadian customers: %d\n", beforeCount)
	}

	// Update processing orders for Canadian customers to "Priority"
	_, err = db.Exec(`UPDATE orders
		SET status = 'Priority'
		WHERE id IN (
			SELECT o.id
			FROM orders o
			JOIN customers c ON o.customer_id = c.id
			WHERE c.country = 'Canada' AND o.status = 'Processing'
		)`)
	if err != nil {
		log.Printf("Failed to update orders: %v", err)
	} else {
		fmt.Println("✓ Updated Canadian orders to Priority status")
	}

	// Verify update
	fmt.Println("After update:")
	var afterCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM orders o
		JOIN customers c ON o.customer_id = c.id
		WHERE c.country = 'Canada' AND o.status = 'Priority'`).Scan(&afterCount)
	if err == nil {
		fmt.Printf("  Priority orders for Canadian customers: %d\n", afterCount)
	}

	// Example 10: Complex multi-table JOIN - Order summary with all details
	fmt.Println("\n=== Example 10: Complex Multi-table JOIN - Complete Order Summary ===")
	rows, err = db.Query(`SELECT
		COALESCE(o.id, 0) as order_id,
		COALESCE(o.order_date, 'Unknown') as order_date,
		c.first_name,
		c.last_name,
		COALESCE(c.email, 'Unknown') as email,
		COALESCE(c.city, 'Unknown') as customer_city,
		COALESCE(o.shipping_city, 'Unknown') as shipping_city,
		COUNT(DISTINCT COALESCE(oi.id, 0)) as item_count,
		COALESCE(SUM(oi.quantity), 0) as total_quantity,
		COALESCE(o.total_amount, 0) as total_amount,
		COALESCE(o.status, 'Unknown') as status,
		CASE
			WHEN COALESCE(o.shipping_city, '') = COALESCE(c.city, '') THEN 'Local Delivery'
			WHEN COALESCE(o.shipping_country, '') = COALESCE(c.country, '') THEN 'Domestic Shipping'
			ELSE 'International Shipping'
		END as shipping_type
	FROM orders o
	JOIN customers c ON o.customer_id = c.id
	LEFT JOIN order_items oi ON o.id = oi.order_id
	WHERE o.id <= 5  -- Limit to first 5 orders for display
	GROUP BY o.id, o.order_date, c.first_name, c.last_name, c.email, c.city, o.shipping_city, o.total_amount, o.status, o.shipping_country, c.country
	ORDER BY COALESCE(o.id, 0)`)
	if err != nil {
		log.Fatal("Failed to query:", err)
	}
	defer rows.Close()

	fmt.Println("Order | Date       | Customer          | Items | Qty | Amount   | Status   | Shipping Type")
	fmt.Println("------|------------|-------------------|-------|-----|----------|----------|------------------")
	for rows.Next() {
		var orderID, itemCount, totalQuantity int
		var orderDate, firstName, lastName, email, customerCity, shippingCity, status, shippingType string
		var totalAmount float64

		err := rows.Scan(
			&orderID, &orderDate, &firstName, &lastName, &email, &customerCity,
			&shippingCity, &itemCount, &totalQuantity, &totalAmount, &status, &shippingType)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		customerName := firstName + " " + lastName
		fmt.Printf("%5d | %10s | %-17s | %5d | %3d | $%7.2f | %-8s | %s\n",
			orderID, orderDate, customerName, itemCount, totalQuantity,
			totalAmount, status, shippingType)
	}

	// Summary statistics
	fmt.Println("\n=== JOIN Operations Summary ===")
	var stats struct {
		totalCustomers       int
		totalOrders          int
		avgOrderValue        sql.NullFloat64
		customersWithOrders  int
		productsNeverOrdered int
	}

	err = db.QueryRow("SELECT COUNT(*) FROM customers").Scan(&stats.totalCustomers)
	if err == nil {
		err = db.QueryRow("SELECT COUNT(*) FROM orders WHERE customer_id IN (SELECT id FROM customers)").Scan(
			&stats.totalOrders)
		if err == nil {
			err = db.QueryRow("SELECT AVG(COALESCE(total_amount, 0)) FROM orders WHERE status = 'Completed'").Scan(
				&stats.avgOrderValue)
			if err == nil {
				err = db.QueryRow(`SELECT COUNT(DISTINCT customer_id) FROM orders
					WHERE customer_id IN (SELECT id FROM customers)`).Scan(
					&stats.customersWithOrders)
				if err == nil {
					err = db.QueryRow(`SELECT COUNT(*) FROM products p
						WHERE p.id NOT IN (SELECT DISTINCT product_id FROM order_items)`).Scan(
						&stats.productsNeverOrdered)
				}
			}
		}
	}

	avgOrderValue := 0.0
	if stats.avgOrderValue.Valid {
		avgOrderValue = stats.avgOrderValue.Float64
	}

	fmt.Printf("Total Customers: %d\n", stats.totalCustomers)
	fmt.Printf("Total Orders: %d\n", stats.totalOrders)
	fmt.Printf("Customers with Orders: %d\n", stats.customersWithOrders)
	fmt.Printf("Average Order Value: $%.2f\n", avgOrderValue)
	fmt.Printf("Products Never Ordered: %d\n", stats.productsNeverOrdered)

	// Clean up
	fmt.Println("\n=== Cleaning Up ===")
	tables := []string{"order_items", "orders", "products", "customers"}
	for _, table := range tables {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", table))
		if err != nil {
			log.Printf("Failed to drop %s: %v", table, err)
		}
	}
	fmt.Println("✓ All tables dropped successfully")

	// Summary
	fmt.Println("\n=== Summary ===")
	fmt.Println("This example demonstrated:")
	fmt.Println("- INNER JOIN - Matching records in both tables")
	fmt.Println("- LEFT JOIN - All records from left table, matching from right")
	fmt.Println("- RIGHT JOIN - All records from right table, matching from left")
	fmt.Println("- Multiple JOINs - Joining more than two tables")
	fmt.Println("- Self JOIN - Joining a table to itself")
	fmt.Println("- FULL OUTER JOIN simulation - Using UNION")
	fmt.Println("- Aggregate functions with JOINs")
	fmt.Println("- Subqueries with JOINs")
	fmt.Println("- UPDATE statements with JOINs")
	fmt.Println("- Complex multi-table JOINs")
	fmt.Println("\nAll JOIN operations completed successfully!")
}

// Helper function to display join results
func displayJoinResults(rows *sql.Rows, headers []string) {
	// Calculate column widths
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}

	// Print headers
	for i, h := range headers {
		fmt.Printf("%-*s", widths[i]+2, h)
	}
	fmt.Println()

	// Print separator
	for _, w := range widths {
		for i := 0; i < w+2; i++ {
			fmt.Print("-")
		}
		fmt.Print(" ")
	}
	fmt.Println()

	// Print rows
	for rows.Next() {
		// This is a simplified version - in real code you'd scan into specific types
		var result []interface{}
		cols, _ := rows.Columns()
		for range cols {
			var value interface{}
			result = append(result, &value)
		}
		rows.Scan(result...)

		for i, v := range result {
			fmt.Printf("%-*s", widths[i]+2, fmt.Sprintf("%v", *v.(*interface{})))
		}
		fmt.Println()
	}
}

// Helper to safely get string from NullString
func nullStringToString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return "NULL"
}

// Helper to safely get float64 from NullFloat64
func nullFloatToString(nf sql.NullFloat64) string {
	if nf.Valid {
		return fmt.Sprintf("%.2f", nf.Float64)
	}
	return "NULL"
}

// Helper to safely get int64 from NullInt64
func nullIntToString(ni sql.NullInt64) string {
	if ni.Valid {
		return fmt.Sprintf("%d", ni.Int64)
	}
	return "NULL"
}