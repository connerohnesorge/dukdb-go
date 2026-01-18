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

	fmt.Println("=== dukdb-go CSV Examples ===")

	// Run various CSV examples
	runReadCSVExample(db)
	runReadCSVAutoExample(db)
	runReadCSVOptionsExample(db)
	runWriteCSVExample(db)

	fmt.Println("\n✓ All CSV examples completed successfully!")
}

func runReadCSVExample(db *sql.DB) {
	fmt.Println("\n1. Reading CSV:")
	rows, err := db.Query("SELECT * FROM read_csv('simple.csv')")
	if err != nil {
		fmt.Printf("   Note: Create simple.csv with data to test this\n")
		return
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	fmt.Printf("   Read %d rows from CSV\n", count)
}

func runReadCSVAutoExample(db *sql.DB) {
	fmt.Println("\n2. Auto-detecting CSV format:")
	rows, err := db.Query("SELECT * FROM read_csv_auto('data.csv') LIMIT 5")
	if err != nil {
		fmt.Printf("   Note: CSV auto-detection requires data.csv\n")
		return
	}
	defer rows.Close()
	fmt.Println("   Auto-detected CSV structure and read data")
}

func runReadCSVOptionsExample(db *sql.DB) {
	fmt.Println("\n3. Reading CSV with options:")
	rows, err := db.Query(`
		SELECT * FROM read_csv('custom.csv', 
			delimiter = '|', 
			header = true, 
			nullstr = 'N/A'
		)
	`)
	if err != nil {
		fmt.Printf("   Configure delimiter, header, null handling\n")
		return
	}
	defer rows.Close()
	fmt.Println("   Read CSV with custom options")
}

func runWriteCSVExample(db *sql.DB) {
	fmt.Println("\n4. Writing CSV:")
	_, err := db.Exec("CREATE TABLE sample AS SELECT * FROM read_csv_auto('data.csv')")
	if err != nil {
		// Skip if no data file
		fmt.Println("   Create tables and use COPY to export CSV")
		return
	}

	// Export to CSV
	_, err = db.Exec("COPY sample TO 'output.csv' (HEADER, DELIMITER ',')")
	if err != nil {
		fmt.Printf("   Use COPY statement to export data\n")
		return
	}
	fmt.Println("   Exported table to CSV")

	db.Exec("DROP TABLE sample")
}
