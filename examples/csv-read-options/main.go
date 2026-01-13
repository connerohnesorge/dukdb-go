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

	fmt.Println("=== CSV Reading with Options Example ===")

	// Example 1: Custom delimiter
	fmt.Println("\n1. Reading CSV with custom delimiter (|):")
	createCSVFile("pipe_delimited.csv", "id|name|department|salary\n1|John Doe|Engineering|75000\n2|Jane Smith|Marketing|65000\n3|Bob Johnson|Sales|55000")
	defer os.Remove("pipe_delimited.csv")

	rows, err := db.Query("SELECT * FROM read_csv('pipe_delimited.csv', delimiter = '|')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRows(rows)

	// Example 2: No header row
	fmt.Println("\n2. Reading CSV without header row:")
	createCSVFile("no_header.csv", "1001,Product A,19.99,150\n1002,Product B,29.99,200\n1003,Product C,39.99,75")
	defer os.Remove("no_header.csv")

	rows, err = db.Query("SELECT * FROM read_csv('no_header.csv', header = false, columns = ['product_id', 'product_name', 'price', 'stock'])")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRows(rows)

	// Example 3: Custom null string
	fmt.Println("\n3. Reading CSV with custom null representation (N/A):")
	createCSVFile("custom_null.csv", "order_id,customer_id,product_id,quantity,discount\n1,101,201,5,N/A\n2,102,202,3,0.1\n3,103,203,N/A,0.2\n4,104,204,10,N/A")
	defer os.Remove("custom_null.csv")

	rows, err = db.Query("SELECT * FROM read_csv('custom_null.csv', nullstr = 'N/A')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRows(rows)

	// Example 4: Quoted values with commas
	fmt.Println("\n4. Reading CSV with quoted values containing commas:")
	createCSVFile("quoted_values.csv", `id,description,price
1,"Large, comfortable sofa",599.99
2,"Dining table, oak wood",899.00
3,"Set of 4 chairs, leather",450.00`)
	defer os.Remove("quoted_values.csv")

	rows, err = db.Query("SELECT * FROM read_csv('quoted_values.csv')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRows(rows)

	// Example 5: Multiple options combined
	fmt.Println("\n5. Reading CSV with multiple options (tab delimiter, no header, custom null):")
	createCSVFile("complex.csv", "101\t2023-01-15\t100.50\t\n102\t2023-01-16\tNULL\t15.00\n103\t2023-01-17\t250.00\t20.00")
	defer os.Remove("complex.csv")

	rows, err = db.Query("SELECT * FROM read_csv('complex.csv', delimiter = '\t', header = false, nullstr = 'NULL', columns = ['transaction_id', 'date', 'amount', 'fee'])")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRows(rows)

	// Example 6: Skip rows and sample size
	fmt.Println("\n6. Reading CSV with skip and sample options:")
	createCSVFile("large_header.csv", `# This is a comment line
# Another comment line
# Configuration: version=1.0
id,name,score
1,Alice,95
2,Bob,87
3,Charlie,92
4,David,88
5,Eve,91`)
	defer os.Remove("large_header.csv")

	rows, err = db.Query("SELECT * FROM read_csv('large_header.csv', skip := 3, sample_size := 5)")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRows(rows)

	// Example 7: Date format specification
	fmt.Println("\n7. Reading CSV with custom date format:")
	createCSVFile("custom_date.csv", "event_id,event_date,description\n1,15/01/2023,New Year Sale\n2,28/02/2023,Spring Collection\n3,15/03/2023,Summer Preview")
	defer os.Remove("custom_date.csv")

	rows, err = db.Query("SELECT * FROM read_csv('custom_date.csv', dateformat := '%d/%m/%Y')")
	if err != nil {
		log.Fatal("Failed to read CSV:", err)
	}
	defer rows.Close()

	printRows(rows)

	fmt.Println("\n✓ CSV reading with options example completed successfully!")
}

// Helper function to create CSV files
func createCSVFile(filename, content string) {
	err := os.WriteFile(filename, []byte(content), 0644)
	if err != nil {
		log.Fatalf("Failed to create %s: %v", filename, err)
	}
}

// Helper function to print query results
func printRows(rows *sql.Rows) {
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Failed to get columns:", err)
	}

	// Print header
	fmt.Printf("%-30s", strings.Join(columns, " | "))
	fmt.Println()
	fmt.Println(strings.Repeat("-", 30))

	// Print data
	for rows.Next() {
		// Create a slice of interface{}'s to represent each column
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the result into the column pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		// Print each column value
		var row []string
		for _, val := range values {
			if val == nil {
				row = append(row, "NULL")
			} else {
				row = append(row, fmt.Sprintf("%v", val))
			}
		}
		fmt.Printf("%-30s\n", strings.Join(row, " | "))
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error reading rows:", err)
	}
}