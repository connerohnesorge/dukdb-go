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

	fmt.Println("=== dukdb-go JSON Examples ===\n")

	// Create table with JSON data
	_, err = db.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			event_type VARCHAR(50),
			data JSON,
			timestamp TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Insert sample JSON data
	_, err = db.Exec(`
		INSERT INTO events VALUES
			(1, 'login', '{"user_id": 1001, "ip": "192.168.1.1"}', '2024-01-15 10:30:00'),
			(2, 'purchase', '{"user_id": 1002, "amount": 99.99, "items": ["book", "pen"]}', '2024-01-15 11:45:00'),
			(3, 'login', '{"user_id": 1001, "ip": "192.168.1.2"}', '2024-01-15 12:00:00')
	`)
	if err != nil {
		log.Fatal("Failed to insert data:", err)
	}

	// Run JSON examples
	demonstrateJSONFunctions(db)
	demonstrateJSONOperators(db)
	demonstrateJSONAggregation(db)

	fmt.Println("\n✓ All JSON examples completed!")
}

func demonstrateJSONFunctions(db *sql.DB) {
	fmt.Println("1. JSON Functions:")

	// Extract JSON values
	var userID int
	err := db.QueryRow(`
		SELECT data->>'user_id' FROM events WHERE id = 1
	`).Scan(&userID)
	if err != nil {
		log.Printf("Failed to extract JSON: %v", err)
		return
	}
	fmt.Printf("   ✓ Extracted user_id: %d\n", userID)

	// Check if key exists
	var hasItems bool
	err = db.QueryRow(`
		SELECT data ? 'items' FROM events WHERE id = 2
	`).Scan(&hasItems)
	if err != nil {
		log.Printf("Failed to check key: %v", err)
		return
	}
	fmt.Printf("   ✓ Event has items: %t\n", hasItems)
}

func demonstrateJSONOperators(db *sql.DB) {
	fmt.Println("\n2. JSON Operators:")

	// Get JSON keys
	rows, err := db.Query(`
		SELECT DISTINCT data->>'user_id' as user_id, COUNT(*) as event_count
		FROM events
		WHERE data ? 'user_id'
		GROUP BY user_id
		ORDER BY event_count DESC
	`)
	if err != nil {
		log.Printf("Query failed: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println("   Events per user:")
	for rows.Next() {
		var userID string
		var count int
		if err := rows.Scan(&userID, &count); err != nil {
			continue
		}
		fmt.Printf("   - User %s: %d events\n", userID, count)
	}

	// Filter by JSON content
	var purchaseAmount float64
	err = db.QueryRow(`
		SELECT (data->>'amount')::FLOAT FROM events
		WHERE data->>'amount' IS NOT NULL
	`).Scan(&purchaseAmount)
	if err != nil {
		return
	}
	fmt.Printf("   ✓ Purchase amount: $%.2f\n", purchaseAmount)
}

func demonstrateJSONAggregation(db *sql.DB) {
	fmt.Println("\n3. JSON Aggregation:")

	// Aggregate JSON data
	var summary string
	err := db.QueryRow(`
		SELECT JSON_OBJECT(
			'total_events', COUNT(*),
			'total_users', COUNT(DISTINCT data->>'user_id'),
			'total_purchases', COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)
		)
		FROM events
	`).Scan(&summary)
	if err != nil {
		log.Printf("Aggregation failed: %v", err)
		return
	}
	fmt.Printf("   ✓ Event summary: %s\n", summary)
}
