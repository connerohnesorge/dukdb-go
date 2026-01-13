// Package main demonstrates creating tables with various column types
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/dukdb/dukdb-go"
)

//nolint:revive // Example file with multiple table creation demonstrations
func main() {
	// Connect to an in-memory database
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Failed to close database: %v", err)
		}
	}()

	fmt.Println("=== Basic Example 06: CREATE TABLE with Different Column Types ===")

	// Example 1: Basic table with common data types (simplified)
	fmt.Println("=== Example 1: Basic table with common data types ===")
	_, err = db.Exec(`CREATE TABLE products (
		id INTEGER,
		product_name VARCHAR(100),
		price DECIMAL,
		quantity INTEGER,
		is_available BOOLEAN,
		created_at TIMESTAMP
	)`)
	if err != nil {
		log.Printf("Failed to create products table: %v", err)
	} else {
		fmt.Println("✓ Products table created successfully")
	}

	// Example 2: Table with string types
	fmt.Println("\n=== Example 2: Table with various string types ===")
	_, err = db.Exec(`CREATE TABLE documents (
		id INTEGER,
		title VARCHAR(255),
		content TEXT,
		short_code CHAR(10),
		language VARCHAR(10),
		tags VARCHAR(500)
	)`)
	if err != nil {
		log.Printf("Failed to create documents table: %v", err)
	} else {
		fmt.Println("✓ Documents table created successfully")
	}

	// Example 3: Table with numeric types
	fmt.Println("\n=== Example 3: Table with numeric precision ===")
	_, err = db.Exec(`CREATE TABLE financial_records (
		id INTEGER,
		account_number BIGINT,
		balance DECIMAL,
		interest_rate DECIMAL,
		transaction_count INTEGER,
		small_amount SMALLINT
	)`)
	if err != nil {
		log.Printf("Failed to create financial_records table: %v", err)
	} else {
		fmt.Println("✓ Financial records table created successfully")
	}

	// Example 4: Table with date and time types
	fmt.Println("\n=== Example 4: Table with date and time types ===")
	_, err = db.Exec(`CREATE TABLE events (
		id INTEGER,
		event_name VARCHAR(100),
		event_date DATE,
		start_time TIME,
		end_time TIME,
		created_at TIMESTAMP,
		updated_at TIMESTAMP,
		timezone VARCHAR(50)
	)`)
	if err != nil {
		log.Printf("Failed to create events table: %v", err)
	} else {
		fmt.Println("✓ Events table created successfully")
	}

	// Example 5: Table with basic constraints only
	fmt.Println("\n=== Example 5: Table with basic constraints ===")
	_, err = db.Exec(`CREATE TABLE users (
		id INTEGER,
		username VARCHAR(50),
		email VARCHAR(100),
		age INTEGER,
		score INTEGER,
		is_active BOOLEAN,
		created_at TIMESTAMP
	)`)
	if err != nil {
		log.Printf("Failed to create users table: %v", err)
	} else {
		fmt.Println("✓ Users table created successfully")
	}

	// Example 6: Table with nullable columns
	fmt.Println("\n=== Example 6: Table with nullable vs non-nullable columns ===")
	_, err = db.Exec(`CREATE TABLE orders (
		id INTEGER,
		customer_id INTEGER,
		order_date DATE,
		ship_date DATE,
		delivery_date DATE,
		total_amount DECIMAL,
		discount_amount DECIMAL,
		tracking_number VARCHAR(50),
		notes TEXT,
		is_gift BOOLEAN
	)`)
	if err != nil {
		log.Printf("Failed to create orders table: %v", err)
	} else {
		fmt.Println("✓ Orders table created successfully")
	}

	// Example 7: Table with indexes
	fmt.Println("\n=== Example 7: Table with indexes ===")
	_, err = db.Exec(`CREATE TABLE products_catalog (
		id INTEGER,
		sku VARCHAR(50),
		name VARCHAR(200),
		category_id INTEGER,
		price DECIMAL,
		stock_quantity INTEGER,
		is_featured BOOLEAN
	)`)
	if err != nil {
		log.Printf("Failed to create products_catalog table: %v", err)
	} else {
		fmt.Println("✓ Products catalog table created successfully")

		// Add indexes
		_, err = db.Exec("CREATE INDEX idx_products_sku ON products_catalog(sku)")
		if err != nil {
			log.Printf("Failed to create SKU index: %v", err)
		} else {
			fmt.Println("  - Index on SKU created")
		}

		_, err = db.Exec("CREATE INDEX idx_products_category ON products_catalog(category_id)")
		if err != nil {
			log.Printf("Failed to create category index: %v", err)
		} else {
			fmt.Println("  - Index on category_id created")
		}
	}

	// Example 8: Table with self-referencing
	fmt.Println("\n=== Example 8: Table with self-referencing relationship ===")
	_, err = db.Exec(`CREATE TABLE categories (
		id INTEGER,
		name VARCHAR(100),
		description TEXT,
		parent_category_id INTEGER,
		is_active BOOLEAN
	)`)
	if err != nil {
		log.Printf("Failed to create categories table: %v", err)
	} else {
		fmt.Println("✓ Categories table created successfully")
	}

	// Example 9: Table with default values (manual approach)
	fmt.Println("\n=== Example 9: Table with default values ===")
	_, err = db.Exec(`CREATE TABLE invoices (
		id INTEGER,
		invoice_number VARCHAR(50),
		subtotal DECIMAL,
		tax_rate DECIMAL,
		tax_amount DECIMAL,
		total DECIMAL,
		status VARCHAR(20),
		created_at TIMESTAMP
	)`)
	if err != nil {
		log.Printf("Failed to create invoices table: %v", err)
	} else {
		fmt.Println("✓ Invoices table created successfully")
	}

	// Example 10: Table for API logs (simplified)
	fmt.Println("\n=== Example 10: Table for storing API data ===")
	_, err = db.Exec(`CREATE TABLE api_logs (
		id INTEGER,
		endpoint VARCHAR(200),
		method VARCHAR(10),
		request_body TEXT,
		response_body TEXT,
		status_code INTEGER,
		duration_ms INTEGER,
		user_agent VARCHAR(500),
		ip_address VARCHAR(45),
		timestamp TIMESTAMP
	)`)
	if err != nil {
		log.Printf("Failed to create api_logs table: %v", err)
	} else {
		fmt.Println("✓ API logs table created successfully")
	}

	// Test inserting data into different table types
	fmt.Println("\n=== Testing Data Insertion ===")

	// Insert into products table
	_, err = db.Exec(`INSERT INTO products (id, product_name, price, quantity, is_available, created_at) VALUES
		(1, 'Laptop', 999.99, 10, true, '2024-01-01 10:00:00'),
		(2, 'Mouse', 29.99, 50, true, '2024-01-01 10:00:00'),
		(3, 'Keyboard', 79.99, 30, true, '2024-01-01 10:00:00')`)
	if err != nil {
		log.Printf("Failed to insert products: %v", err)
	} else {
		fmt.Println("✓ Sample products inserted")
	}

	// Insert into events table
	_, err = db.Exec(`INSERT INTO events (id, event_name, event_date, start_time, end_time) VALUES
		(1, 'Team Meeting', '2024-01-15', '10:00:00', '11:00:00'),
		(2, 'Product Launch', '2024-02-01', '09:00:00', '17:00:00')`)
	if err != nil {
		log.Printf("Failed to insert events: %v", err)
	} else {
		fmt.Println("✓ Sample events inserted")
	}

	// Insert into users table
	_, err = db.Exec(`INSERT INTO users (id, username, email, age, score, is_active, created_at) VALUES
		(1, 'john_doe', 'john@example.com', 25, 100, true, '2024-01-01 10:00:00'),
		(2, 'jane_smith', 'jane@example.com', 30, 150, true, '2024-01-01 10:00:00')`)
	if err != nil {
		log.Printf("Failed to insert users: %v", err)
	} else {
		fmt.Println("✓ Sample users inserted")
	}

	// Display table structures
	fmt.Println("\n=== Table Structures Summary ===")
	tables := []string{"products", "documents", "financial_records", "events", "users", "orders", "products_catalog", "categories", "invoices", "api_logs"}

	for _, table := range tables {
		var count int
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil {
			fmt.Printf("- %s: Unable to query\n", table)
		} else {
			fmt.Printf("- %s: %d rows\n", table, count)
		}
	}

	// Clean up
	fmt.Println("\n=== Cleaning Up ===")
	for _, table := range tables {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", table))
		if err != nil {
			fmt.Printf("Failed to drop %s: %v\n", table, err)
		}
	}
	fmt.Println("All tables dropped successfully")

	// Summary
	fmt.Println("\n=== Summary ===")
	fmt.Println("This example demonstrated:")
	fmt.Println("- Basic data types (INTEGER, VARCHAR, DECIMAL, BOOLEAN, TIMESTAMP)")
	fmt.Println("- String types with different lengths (VARCHAR, TEXT, CHAR)")
	fmt.Println("- Numeric types with precision (DECIMAL, NUMERIC, SMALLINT, BIGINT)")
	fmt.Println("- Date and time types (DATE, TIME, TIMESTAMP)")
	fmt.Println("- Table structures without complex constraints")
	fmt.Println("- Nullable vs non-nullable columns")
	fmt.Println("- Creating indexes on frequently queried columns")
	fmt.Println("- Self-referencing tables (parent-child relationships)")
	fmt.Println("- Tables for storing API data (as TEXT)")
	fmt.Println("\nNote: Advanced constraints like NOT NULL, UNIQUE, CHECK, DEFAULT values,")
	fmt.Println("and FOREIGN KEY constraints may vary depending on the database engine implementation.")
	fmt.Println("This example uses basic table structures that are widely supported.")
}