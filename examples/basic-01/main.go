package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/dukdb-go"
)

func main() {
	// Connect to an in-memory database
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name VARCHAR(100),
		email VARCHAR(100),
		age INTEGER
	)`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}
	fmt.Println("Table 'users' created successfully")

	// INSERT - Create
	fmt.Println("\n=== INSERT Operations ===")
	result, err := db.Exec("INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
		1, "Alice Johnson", "alice@example.com", 28)
	if err != nil {
		log.Fatal("Failed to insert user:", err)
	}
	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("Inserted %d row(s)\n", rowsAffected)

	// Insert multiple users
	users := []struct {
		id    int
		name  string
		email string
		age   int
	}{
		{2, "Bob Smith", "bob@example.com", 35},
		{3, "Carol Davis", "carol@example.com", 42},
		{4, "David Wilson", "david@example.com", 31},
	}

	for _, user := range users {
		_, err = db.Exec("INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
			user.id, user.name, user.email, user.age)
		if err != nil {
			log.Printf("Failed to insert user %s: %v", user.name, err)
			continue
		}
		fmt.Printf("Inserted user: %s\n", user.name)
	}

	// SELECT - Read
	fmt.Println("\n=== SELECT Operations ===")

	// Read all users
	rows, err := db.Query("SELECT id, name, email, age FROM users ORDER BY id")
	if err != nil {
		log.Fatal("Failed to query users:", err)
	}
	defer rows.Close()

	fmt.Println("All users:")
	for rows.Next() {
		var id, age int
		var name, email string
		err := rows.Scan(&id, &name, &email, &age)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}
		fmt.Printf("  ID: %d, Name: %s, Email: %s, Age: %d\n", id, name, email, age)
	}

	// Read specific user
	var userName, userEmail string
	var userAge int
	err = db.QueryRow("SELECT name, email, age FROM users WHERE id = ?", 2).Scan(&userName, &userEmail, &userAge)
	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Println("User not found")
		} else {
			log.Fatal("Failed to query user:", err)
		}
	} else {
		fmt.Printf("\nUser with ID 2: Name=%s, Email=%s, Age=%d\n", userName, userEmail, userAge)
	}

	// UPDATE - Update
	fmt.Println("\n=== UPDATE Operations ===")
	result, err = db.Exec("UPDATE users SET age = ? WHERE id = ?", 29, 1)
	if err != nil {
		log.Fatal("Failed to update user:", err)
	}
	rowsAffected, _ = result.RowsAffected()
	fmt.Printf("Updated %d row(s)\n", rowsAffected)

	// Update multiple fields
	result, err = db.Exec("UPDATE users SET email = ?, age = ? WHERE name = ?",
		"bob.smith@newdomain.com", 36, "Bob Smith")
	if err != nil {
		log.Fatal("Failed to update user:", err)
	}
	rowsAffected, _ = result.RowsAffected()
	fmt.Printf("Updated %d row(s) for Bob Smith\n", rowsAffected)

	// Verify updates
	fmt.Println("\nAfter updates:")
	rows, err = db.Query("SELECT id, name, email, age FROM users ORDER BY id")
	if err != nil {
		log.Fatal("Failed to query users:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, age int
		var name, email string
		rows.Scan(&id, &name, &email, &age)
		fmt.Printf("  ID: %d, Name: %s, Email: %s, Age: %d\n", id, name, email, age)
	}

	// DELETE - Delete
	fmt.Println("\n=== DELETE Operations ===")

	// Delete specific user
	result, err = db.Exec("DELETE FROM users WHERE id = ?", 4)
	if err != nil {
		log.Fatal("Failed to delete user:", err)
	}
	rowsAffected, _ = result.RowsAffected()
	fmt.Printf("Deleted %d row(s)\n", rowsAffected)

	// Delete with condition
	result, err = db.Exec("DELETE FROM users WHERE age > ?", 40)
	if err != nil {
		log.Fatal("Failed to delete users:", err)
	}
	rowsAffected, _ = result.RowsAffected()
	fmt.Printf("Deleted %d user(s) with age > 40\n", rowsAffected)

	// Verify deletions
	fmt.Println("\nAfter deletions:")
	rows, err = db.Query("SELECT id, name, email, age FROM users ORDER BY id")
	if err != nil {
		log.Fatal("Failed to query users:", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, age int
		var name, email string
		rows.Scan(&id, &name, &email, &age)
		fmt.Printf("  ID: %d, Name: %s, Email: %s, Age: %d\n", id, name, email, age)
		count++
	}
	fmt.Printf("Total users remaining: %d\n", count)

	// Count total records
	var total int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&total)
	if err != nil {
		log.Fatal("Failed to count users:", err)
	}
	fmt.Printf("\nFinal count: %d users\n", total)
}