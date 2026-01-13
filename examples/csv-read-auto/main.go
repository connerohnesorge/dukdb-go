package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

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

	fmt.Println("=== CSV Auto-Detection Example ===")

	// Example 1: Auto-detect delimiter
	fmt.Println("\n1. Auto-detecting delimiter:")
	createCSVFile("auto_delimiter.csv", `timestamp;user_id;action;value
2023-01-15 10:30:00;1001;login;1
2023-01-15 10:35:00;1002;purchase;99.99
2023-01-15 10:40:00;1003;view;0
2023-01-15 10:45:00;1001;logout;1`)
	defer os.Remove("auto_delimiter.csv")

	rows, err := db.Query("SELECT * FROM read_csv_auto('auto_delimiter.csv')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRowsWithTypes(rows, db, "auto_delimiter.csv")

	// Example 2: Auto-detect data types
	fmt.Println("\n2. Auto-detecting data types:")
	createCSVFile("mixed_types.csv", `id,name,age,salary,is_active,join_date
1,Alice,25,75000.50,true,2020-01-15
2,Bob,30,85000.00,false,2019-06-20
3,Charlie,28,80000.00,true,2021-03-10
4,Diana,35,95000.75,true,2018-11-05`)
	defer os.Remove("mixed_types.csv")

	rows, err = db.Query("SELECT * FROM read_csv_auto('mixed_types.csv')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRowsWithTypes(rows, db, "mixed_types.csv")

	// Example 3: Auto-detect with irregular formatting
	fmt.Println("\n3. Auto-detecting irregular CSV formatting:")
	createCSVFile("irregular.csv", `"Product ID","Product Name","Price","In Stock"
"001","Widget A","$19.99","Yes"
"002","Widget B","$29.99","No"
"003","Widget C, Special Edition","$49.99","Yes"
"004","Widget D","$39.99","Yes"`)
	defer os.Remove("irregular.csv")

	rows, err = db.Query("SELECT * FROM read_csv_auto('irregular.csv')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRowsWithTypes(rows, db, "irregular.csv")

	// Example 4: Auto-detect date formats
	fmt.Println("\n4. Auto-detecting date formats:")
	createCSVFile("dates.csv", `event,date,time,venue
Conference,2023-03-15,09:00,Convention Center
Workshop,15/04/2023,14:30,Training Room A
Meeting,2023-05-20,16:00,Board Room
Seminar,20-Jun-2023,10:00,Auditorium`)
	defer os.Remove("dates.csv")

	rows, err = db.Query("SELECT * FROM read_csv_auto('dates.csv')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRowsWithTypes(rows, db, "dates.csv")

	// Example 5: Auto-detect with potential null values
	fmt.Println("\n5. Auto-detecting null values:")
	createCSVFile("with_nulls.csv", `order_id,customer_id,product_id,quantity,discount,notes
1001,2001,3001,5,,Regular order
1002,2002,3002,3,0.1,Special discount
1003,,3003,,0.2,Cancelled order
1004,2004,3004,10,,Bulk purchase`)
	defer os.Remove("with_nulls.csv")

	rows, err = db.Query("SELECT * FROM read_csv_auto('with_nulls.csv')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRowsWithTypes(rows, db, "with_nulls.csv")

	// Example 6: Auto-detect numeric formats
	fmt.Println("\n6. Auto-detecting numeric formats (currency, percentages):")
	createCSVFile("numbers.csv", `item,cost,markup,tax_rate,profit_margin
Laptop,$999.99,25%,8.5%,15.2%
Mouse,$29.99,50%,8.5%,35.0%
Keyboard,$79.99,40%,8.5%,28.5%
Monitor,$299.99,30%,8.5%,20.0%`)
	defer os.Remove("numbers.csv")

	rows, err = db.Query("SELECT * FROM read_csv_auto('numbers.csv')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRowsWithTypes(rows, db, "numbers.csv")

	// Example 7: Compare read_csv vs read_csv_auto
	fmt.Println("\n7. Comparison: read_csv vs read_csv_auto:")
	createCSVFile("comparison.csv", `col1;col2;col3
1;hello;2023-01-01
2;world;2023-02-01
3;test;2023-03-01`)
	defer os.Remove("comparison.csv")

	// Using read_csv (would fail without proper delimiter)
	fmt.Println("\nUsing read_csv (default comma delimiter):")
	rows, err = db.Query("SELECT * FROM read_csv('comparison.csv') LIMIT 2")
	if err != nil {
		fmt.Printf("Error (expected): %v\n", err)
	} else {
		printRows(rows)
		rows.Close()
	}

	// Using read_csv_auto (detects semicolon delimiter)
	fmt.Println("\nUsing read_csv_auto (auto-detects semicolon delimiter):")
	rows, err = db.Query("SELECT * FROM read_csv_auto('comparison.csv')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRowsWithTypes(rows, db, "comparison.csv")

	fmt.Println("\n✓ CSV auto-detection example completed successfully!")
}

// Helper function to create CSV files
func createCSVFile(filename, content string) {
	err := os.WriteFile(filename, []byte(content), 0644)
	if err != nil {
		log.Fatalf("Failed to create %s: %v", filename, err)
	}
}

// Helper function to print query results with type information
func printRowsWithTypes(rows *sql.Rows, db *sql.DB, filename string) {
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Failed to get columns:", err)
	}

	// Get column types using DESCRIBE
// 	typeRows, err := db.Query(fmt.Sprintf("DESCRIBE SELECT * FROM read_csv_auto('%s')", filename))
// 	if err != nil {
// 		log.Fatal("Failed to get column types:", err)
// 	}
// 	defer typeRows.Close()
// 
// 	fmt.Println("Column Information:")
// 	fmt.Printf("%-20s %-20s %-20s\n", "Column Name", "Column Type", "Nullable")
// 	fmt.Println(strings.Repeat("-", 60))
// 
// 	for typeRows.Next() {
// 		var colName, colType, nullInfo string
// 		if err := typeRows.Scan(&colName, &colType, &nullInfo); err != nil {
// 			log.Fatal("Failed to scan type info:", err)
// 		}
// 		fmt.Printf("%-20s %-20s %-20s\n", colName, colType, nullInfo)
// 	}

	fmt.Println("\nData:")
	fmt.Printf("%-60s\n", strings.Join(columns, " | "))
	fmt.Println(strings.Repeat("-", 60))

	// Print data
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		var row []string
		for _, val := range values {
			if val == nil {
				row = append(row, "NULL")
			} else {
				row = append(row, fmt.Sprintf("%v", val))
			}
		}
		fmt.Printf("%-60s\n", strings.Join(row, " | "))
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error reading rows:", err)
	}
}

// Helper function to print query results (simplified version)
func printRows(rows *sql.Rows) {
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Failed to get columns:", err)
	}

	fmt.Printf("%-40s\n", strings.Join(columns, " | "))
	fmt.Println(strings.Repeat("-", 40))

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		var row []string
		for _, val := range values {
			if val == nil {
				row = append(row, "NULL")
			} else {
				row = append(row, fmt.Sprintf("%v", val))
			}
		}
		fmt.Printf("%-40s\n", strings.Join(row, " | "))
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error reading rows:", err)
	}
}