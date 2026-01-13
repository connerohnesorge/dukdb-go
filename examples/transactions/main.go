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

	// Create a sample table
	_, err = db.Exec(`
		CREATE TABLE accounts (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			balance DECIMAL(10,2)
		)
	`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Insert sample data
	_, err = db.Exec(`
		INSERT INTO accounts VALUES
			(1, 'Alice', 1000.00),
			(2, 'Bob', 500.00)
	`)
	if err != nil {
		log.Fatal("Failed to insert data:", err)
	}

	fmt.Println("=== dukdb-go Transaction Examples ===\n")

	// Demonstrate different transaction patterns
	demonstrateBasicTransaction(db)
	demonstrateSavepoint(db)
	demonstrateIsolationLevels(db)

	fmt.Println("\n✓ All transaction examples completed!")
}

func demonstrateBasicTransaction(db *sql.DB) {
	fmt.Println("1. Basic Transaction (Transfer)")

	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}

	// Transfer $100 from Alice to Bob
	_, err = tx.Exec("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
	if err != nil {
		tx.Rollback()
		log.Printf("Failed to debit: %v", err)
		return
	}

	_, err = tx.Exec("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
	if err != nil {
		tx.Rollback()
		log.Printf("Failed to credit: %v", err)
		return
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit: %v", err)
		return
	}

	fmt.Println("   ✓ Transfer completed successfully")
	showBalances(db)
}

func demonstrateSavepoint(db *sql.DB) {
	fmt.Println("\n2. Transaction with Savepoint")

	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}

	// Add interest to Alice's account
	_, err = tx.Exec("UPDATE accounts SET balance = balance * 1.05 WHERE id = 1")
	if err != nil {
		tx.Rollback()
		return
	}

	// Create a savepoint
	_, err = tx.Exec("SAVEPOINT before_bob_update")
	if err != nil {
		tx.Rollback()
		return
	}

	// Try to update Bob's account (this might fail)
	_, err = tx.Exec("UPDATE accounts SET balance = balance * 1.05 WHERE id = 2")
	if err != nil {
		// Rollback to savepoint instead of full rollback
		_, _ = tx.Exec("ROLLBACK TO SAVEPOINT before_bob_update")
		fmt.Println("   ⚠ Bob's update failed, rolled back to savepoint")
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit: %v", err)
		return
	}

	fmt.Println("   ✓ Transaction with savepoint completed")
	showBalances(db)
}

func demonstrateIsolationLevels(db *sql.DB) {
	fmt.Println("\n3. Isolation Levels")

	// Set isolation level
	_, err := db.Exec("SET default_transaction_isolation = 'SERIALIZABLE'")
	if err != nil {
		log.Printf("Failed to set isolation level: %v", err)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}

	// Read current balance
	var balance float64
	err = tx.QueryRow("SELECT balance FROM accounts WHERE id = 1").Scan(&balance)
	if err != nil {
		tx.Rollback()
		return
	}

	fmt.Printf("   Started transaction with SERIALIZABLE isolation\n")
	fmt.Printf("   Alice's balance: $%.2f\n", balance)

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit: %v", err)
		return
	}

	fmt.Println("   ✓ Serializable transaction completed")
}

func showBalances(db *sql.DB) {
	rows, err := db.Query("SELECT name, balance FROM accounts ORDER BY id")
	if err != nil {
		return
	}
	defer rows.Close()

	fmt.Println("\n   Current balances:")
	for rows.Next() {
		var name string
		var balance float64
		if err := rows.Scan(&name, &balance); err != nil {
			continue
		}
		fmt.Printf("   - %s: $%.2f\n", name, balance)
	}
}
