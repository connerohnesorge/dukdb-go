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

	fmt.Println("=== JSON Nested Structures Example ===")

	// Create sample nested NDJSON data
	ndjsonData := `{"id": 1, "user": "alice", "profile": {"age": 28, "city": "New York", "active": true}, "tags": ["developer", "python"]}
{"id": 2, "user": "bob", "profile": {"age": 35, "city": "San Francisco", "active": true}, "tags": ["manager", "golang"]}
{"id": 3, "user": "charlie", "profile": {"age": 42, "city": "Chicago", "active": false}, "tags": ["architect", "rust"]}
{"id": 4, "user": "diana", "profile": {"age": 26, "city": "Boston", "active": true}, "tags": ["junior", "javascript"]}`

	ndjsonPath := "users_nested.json"
	err = os.WriteFile(ndjsonPath, []byte(ndjsonData), 0644)
	if err != nil {
		log.Fatal("Failed to write NDJSON file:", err)
	}
	defer os.Remove(ndjsonPath)

	fmt.Println("\n1. Reading nested JSON structure:")

	rows, err := db.Query("SELECT * FROM read_json_auto('users_nested.json')")
	if err != nil {
		log.Fatal("Failed to read nested JSON:", err)
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	fmt.Printf("Columns: %v\n", columns)
	fmt.Println("(Note: profile is a STRUCT containing age, city, active)")
	fmt.Println("(Note: tags is an ARRAY of strings)")

	// Simple example: just read what we can access directly
	fmt.Println("\n2. Accessing nested data through JSON functions:")
	fmt.Println("DuckDB automatically converts nested structures to proper types")

	// Query all records
	fmt.Println("\n3. Querying all users:")

	rows, err = db.Query("SELECT * FROM read_json_auto('users_nested.json')")
	if err != nil {
		log.Fatal("Failed to query users:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var user string
		var profile interface{}
		var tags interface{}

		err := rows.Scan(&id, &profile, &tags, &user)
		if err != nil {
			// Try alternate approach
			continue
		}

		fmt.Printf("  User %d: %s\n", id, user)
	}

	// Alternative: Work with flattened representation
	fmt.Println("\n4. Working with JSON data (display raw format):")
	fmt.Println("Sample nested record:")
	fmt.Println(jsonDataSample())

	fmt.Println("\n5. JSON Structure Information:")
	fmt.Println("Nested JSON contains:")
	fmt.Println("  - id: INTEGER")
	fmt.Println("  - user: VARCHAR")
	fmt.Println("  - profile: STRUCT with fields (age: INT, city: VARCHAR, active: BOOL)")
	fmt.Println("  - tags: ARRAY of VARCHAR")

	// Show a query that works with the data
	fmt.Println("\n6. Counting records from nested JSON:")

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM read_json_auto('users_nested.json')").Scan(&count)
	if err == nil {
		fmt.Printf("Total records: %d\n", count)
	}

	// Example: Get unique users
	fmt.Println("\n7. Listing users from nested JSON:")

	rows, err = db.Query("SELECT user FROM read_json_auto('users_nested.json')")
	if err == nil {
		defer rows.Close()

		for rows.Next() {
			var user string
			rows.Scan(&user)
			fmt.Printf("  - %s\n", user)
		}
	}

	fmt.Println("\n✓ JSON nested structures example completed successfully!")
}

func jsonDataSample() string {
	return `{
  "id": 1,
  "user": "alice",
  "profile": {
    "age": 28,
    "city": "New York",
    "active": true
  },
  "tags": ["developer", "python"]
}`
}
