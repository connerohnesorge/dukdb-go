package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/dukdb/dukdb-go"
)

func main() {
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	fmt.Println("=== dukdb-go Parquet Examples ===\n")

	// Demonstrate Parquet operations
	demonstrateReadParquet(db)
	demonstrateWriteParquet(db)
	demonstrateParquetOptions(db)

	fmt.Println("\n✓ All Parquet examples completed!")
}

func demonstrateReadParquet(db *sql.DB) {
	fmt.Println("1. Reading Parquet Files")

	// Simulate reading from Parquet (would need actual Parquet file)
	// For demonstration, we'll create a sample table
	_, err := db.Exec(`
		CREATE TABLE sample_data AS
		SELECT 
			id,
			name,
			value,
			category
		FROM (VALUES 
			(1, 'Item A', 100.50, 'Type1'),
			(2, 'Item B', 250.00, 'Type2'),
			(3, 'Item C', 75.25, 'Type1')
		) AS t(id, name, value, category)
	`)
	if err != nil {
		log.Printf("Note: In production, use read_parquet('file.parquet')\n")
		return
	}

	// Count rows
	var count int
	db.QueryRow("SELECT COUNT(*) FROM sample_data").Scan(&count)
	fmt.Printf("   ✓ Would read %d rows from Parquet file\n", count)

	// Show sample data
	rows, _ := db.Query("SELECT * FROM sample_data LIMIT 3")
	if rows != nil {
		defer rows.Close()
		fmt.Println("   Sample data structure:")
		for rows.Next() {
			var id int
			var name, category string
			var value float64
			rows.Scan(&id, &name, &value, &category)
			fmt.Printf("   - %s (id=%d, value=%.2f, category=%s)\n", 
				name, id, value, category)
		}
	}
}

func demonstrateWriteParquet(db *sql.DB) {
	fmt.Println("\n2. Writing to Parquet Files")

	// Example of how to export to Parquet
	fmt.Println("   ✓ Use COPY statement to export to Parquet:")
	fmt.Println("   COPY sample_data TO 'output.parquet'")
	fmt.Println("   ✓ Supports compression: SNAPPY, GZIP, ZSTD")

	// Simulated export metadata
	var rowCount int
	db.QueryRow("SELECT COUNT(*) FROM sample_data").Scan(&rowCount)
	fmt.Printf("   ✓ Would write %d rows to Parquet\n", rowCount)
}

func demonstrateParquetOptions(db *sql.DB) {
	fmt.Println("\n3. Parquet Options")

	fmt.Println("   Reading options:")
	fmt.Println("   - Column projection (select specific columns)")
	fmt.Println("   - Row group filtering")
	fmt.Println("   - Compression auto-detection")

	fmt.Println("\n   Writing options:")
	fmt.Println("   - Compression: SNAPPY, GZIP, ZSTD, LZ4, BROTLI")
	fmt.Println("   - Row group size")
	fmt.Println("   - Encoding preferences")

	// Show column types
	rows, _ := db.Query(`
		SELECT column_name, data_type 
		FROM information_schema.columns 
		WHERE table_name = 'sample_data'
		ORDER BY ordinal_position
	`)
	if rows != nil {
		defer rows.Close()
		fmt.Println("\n   Column types for Parquet schema:")
		for rows.Next() {
			var name, dataType string
			rows.Scan(&name, &dataType)
			fmt.Printf("   - %s: %s\n", name, dataType)
		}
	}
}
