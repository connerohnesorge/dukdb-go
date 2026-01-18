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

	fmt.Println("=== CSV Writing with Options Example ===")

	// Create sample data
	fmt.Println("\n1. Creating sample data...")

	_, err = db.Exec(`
		CREATE TABLE products AS
		SELECT
			1 as id, 'Laptop Computer' as name, 'Electronics' as category,
			1299.99 as price, 15 as stock, true as in_stock
		UNION ALL
		SELECT 2, 'Wireless Mouse', 'Accessories', 29.99, 50, true
		UNION ALL
		SELECT 3, 'USB-C Hub', 'Accessories', 49.99, 0, false
		UNION ALL
		SELECT 4, 'Mechanical Keyboard', 'Accessories', 89.99, 25, true
		UNION ALL
		SELECT 5, '27" Monitor', 'Electronics', 399.99, 10, true
	`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Example 1: Custom delimiter
	fmt.Println("\n2. Export with custom delimiter (pipe):")

	outputFile := "products_pipe.csv"
	_, err = db.Exec(fmt.Sprintf("COPY products TO '%s' WITH (DELIMITER '|')", outputFile))
	if err != nil {
		log.Fatal("Failed to export with custom delimiter:", err)
	}
	defer os.Remove(outputFile)

	content, err := os.ReadFile(outputFile)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}

	fmt.Printf("Exported with pipe delimiter to %s:\n%s\n", outputFile, string(content))

	// Example 2: No header row
	fmt.Println("\n3. Export without header row:")

	outputFile = "products_no_header.csv"
	_, err = db.Exec(fmt.Sprintf("COPY products TO '%s' WITH (HEADER false)", outputFile))
	if err != nil {
		log.Fatal("Failed to export without header:", err)
	}
	defer os.Remove(outputFile)

	content, err = os.ReadFile(outputFile)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}

	fmt.Printf("Exported without header to %s:\n%s\n", outputFile, string(content))

	// Example 3: Custom null string
	fmt.Println("\n4. Export with custom null representation:")

	// First create a table with null values
	_, err = db.Exec(`
		CREATE TABLE orders AS
		SELECT
			101 as order_id, 1001 as customer_id, '2023-01-15'::date as order_date,
			150.50 as total, NULL as discount, 'Completed' as status
		UNION ALL
		SELECT 102, 1002, '2023-01-16'::date, 299.99, 15.00, 'Completed'
		UNION ALL
		SELECT 103, 1003, '2023-01-17'::date, NULL, NULL, 'Pending'
		UNION ALL
		SELECT 104, 1004, '2023-01-18'::date, 89.99, 5.00, 'Completed'
	`)
	if err != nil {
		log.Fatal("Failed to create orders table:", err)
	}

	outputFile = "orders_custom_null.csv"
	_, err = db.Exec(fmt.Sprintf("COPY orders TO '%s' WITH (NULL 'N/A')", outputFile))
	if err != nil {
		log.Fatal("Failed to export with custom null:", err)
	}
	defer os.Remove(outputFile)

	content, err = os.ReadFile(outputFile)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}

	fmt.Printf("Exported with custom null to %s:\n%s\n", outputFile, string(content))

	// 	// Example 4: Force quotes on all fields
	// 	fmt.Println("\n5. Export with all fields quoted:")
	//
	// 	outputFile = "products_quoted.csv"
	// 	_, err = db.Exec(fmt.Sprintf("COPY products TO '%s' WITH (FORCE_QUOTE *)", outputFile))
	// 	if err != nil {
	// 		log.Fatal("Failed to export with quotes:", err)
	// 	}
	// 	defer os.Remove(outputFile)
	//
	// 	content, err = os.ReadFile(outputFile)
	// 	if err != nil {
	// 		log.Fatal("Failed to read exported file:", err)
	// 	}
	//
	// 	fmt.Printf("Exported with all fields quoted to %s:\n%s\n", outputFile, string(content))
	//
	// 	// Example 5: Selective quoting
	// 	fmt.Println("\n6. Export with selective field quoting:")
	//
	// 	outputFile = "products_selective_quote.csv"
	// 	_, err = db.Exec(fmt.Sprintf("COPY products TO '%s' WITH (FORCE_QUOTE (name, category))", outputFile))
	// 	if err != nil {
	// 		log.Fatal("Failed to export with selective quotes:", err)
	// 	}
	// 	defer os.Remove(outputFile)
	//
	// 	content, err = os.ReadFile(outputFile)
	// 	if err != nil {
	// 		log.Fatal("Failed to read exported file:", err)
	// 	}
	//
	// 	fmt.Printf("Exported with selective quoting to %s:\n%s\n", outputFile, string(content))

	// Example 6: Escape character
	fmt.Println("\n7. Export with custom escape character:")

	// Create data with special characters
	_, err = db.Exec(`
		CREATE TABLE descriptions AS
		SELECT
			1 as id, 'Product with "quotes"' as description
		UNION ALL
		SELECT 2, 'Product with, comma'
		UNION ALL
		SELECT 3, 'Product with\ttab'
		UNION ALL
		SELECT 4, 'Product with\nnewline'
	`)
	if err != nil {
		log.Fatal("Failed to create descriptions table:", err)
	}

	outputFile = "descriptions_escaped.csv"
	_, err = db.Exec(fmt.Sprintf("COPY descriptions TO '%s' WITH (ESCAPE '\\')", outputFile))
	if err != nil {
		log.Fatal("Failed to export with escape:", err)
	}
	defer os.Remove(outputFile)

	content, err = os.ReadFile(outputFile)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}

	fmt.Printf("Exported with escape character to %s:\n%s\n", outputFile, string(content))

	// Example 7: Encoding options
	fmt.Println("\n8. Export with UTF-8 encoding:")

	// Create data with UTF-8 characters
	_, err = db.Exec(`
		CREATE TABLE international AS
		SELECT
			1 as id, 'Café' as name, 'Français' as language
		UNION ALL
		SELECT 2, 'Naïve', 'English'
		UNION ALL
		SELECT 3, 'Résumé', 'English'
		UNION ALL
		SELECT 4, 'Über', 'German'
	`)
	if err != nil {
		log.Fatal("Failed to create international table:", err)
	}

	outputFile = "international_utf8.csv"
	_, err = db.Exec(fmt.Sprintf("COPY international TO '%s' WITH (ENCODING 'UTF-8')", outputFile))
	if err != nil {
		log.Fatal("Failed to export with UTF-8:", err)
	}
	defer os.Remove(outputFile)

	content, err = os.ReadFile(outputFile)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}

	fmt.Printf("Exported with UTF-8 encoding to %s:\n%s\n", outputFile, string(content))

	// 	// Example 8: Date format
	// 	fmt.Println("\n9. Export with custom date format:")
	//
	// 	outputFile = "products_date_format.csv"
	// 	_, err = db.Exec(fmt.Sprintf("COPY (SELECT id, name, CURRENT_DATE as created_date FROM products) TO '%s' WITH (DATEFORMAT '%%d/%%m/%%Y')", outputFile))
	// 	if err != nil {
	// 		log.Fatal("Failed to export with date format:", err)
	// 	}
	// 	defer os.Remove(outputFile)
	//
	// 	content, err = os.ReadFile(outputFile)
	// 	if err != nil {
	// 		log.Fatal("Failed to read exported file:", err)
	// 	}
	//
	// 	fmt.Printf("Exported with custom date format to %s:\n%s\n", outputFile, string(content))
	//
	// Example 9: Multiple options combined
	fmt.Println("\n10. Export with multiple options combined:")

	outputFile = "products_multi_options.csv"
	_, err = db.Exec(fmt.Sprintf(`COPY products TO '%s' WITH (
		DELIMITER '\t',
		HEADER false,
		NULL 'NULL',
		FORCE_QUOTE (name, category),
		ESCAPE '\\'
	)`, outputFile))
	if err != nil {
		log.Fatal("Failed to export with multiple options:", err)
	}
	defer os.Remove(outputFile)

	content, err = os.ReadFile(outputFile)
	if err != nil {
		log.Fatal("Failed to read exported file:", err)
	}

	fmt.Printf("Exported with multiple options to %s:\n%s\n", outputFile, string(content))

	// Example 10: Row-by-row comparison
	fmt.Println("\n11. Comparison of different export options:")

	// Create a simple test table
	_, err = db.Exec(`
		CREATE TABLE test_export AS
		SELECT 1 as id, 'Test, Product' as name, 99.99 as price
	`)
	if err != nil {
		log.Fatal("Failed to create test table:", err)
	}

	options := []struct {
		name    string
		options string
	}{
		{"Default", ""},
		{"Custom Delimiter", "DELIMITER '|'"},
		{"No Header", "HEADER false"},
		{"Force Quote", "FORCE_QUOTE *"},
		{"Custom Escape", "ESCAPE '\\'"},
	}

	for _, opt := range options {
		filename := fmt.Sprintf(
			"test_%s.csv",
			strings.ReplaceAll(strings.ToLower(opt.name), " ", "_"),
		)
		query := fmt.Sprintf("COPY test_export TO '%s'", filename)
		if opt.options != "" {
			query += fmt.Sprintf(" WITH (%s)", opt.options)
		}

		_, err = db.Exec(query)
		if err != nil {
			log.Printf("Failed to export with %s: %v", opt.name, err)
			continue
		}
		defer os.Remove(filename)

		content, err := os.ReadFile(filename)
		if err != nil {
			log.Printf("Failed to read %s: %v", filename, err)
			continue
		}

		fmt.Printf("\n%s:\n%s", opt.name, string(content))
	}

	// Clean up
	tables := []string{"products", "orders", "descriptions", "international", "test_export"}
	for _, table := range tables {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", table))
		if err != nil {
			log.Printf("Warning: Failed to drop %s table: %v", table, err)
		}
	}

	fmt.Println("\n✓ CSV writing with options example completed successfully!")
}

// Helper function to demonstrate file encoding detection
func detectEncoding(filename string) string {
	// This is a simplified demonstration
	// In practice, you'd use a proper encoding detection library
	content, err := os.ReadFile(filename)
	if err != nil {
		return "Unknown"
	}

	// Simple UTF-8 check
	if strings.Contains(string(content), "é") || strings.Contains(string(content), "ü") {
		return "UTF-8 detected"
	}
	return "ASCII/Unknown"
}

// Helper function to escape special characters for display
func escapeForDisplay(s string) string {
	s = strings.ReplaceAll(s, "\t", "\\t")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return s
}

// Helper function to show CSV parsing differences
func showCSVParsingDifferences(content string) {
	fmt.Println("\nParsing with different options:")

	lines := strings.Split(strings.TrimSpace(content), "\n")
	for i, line := range lines {
		fmt.Printf("Line %d: %q\n", i+1, line)

		// Show how it would be parsed with different delimiters
		if strings.Contains(line, "|") {
			parts := strings.Split(line, "|")
			fmt.Printf("  Pipe-delimited: %v\n", parts)
		}
		if strings.Contains(line, ",") {
			parts := strings.Split(line, ",")
			fmt.Printf("  Comma-delimited: %v\n", parts)
		}
	}
}

// Helper to create a comparison matrix
func createOptionComparison() {
	fmt.Println("\n=== CSV Export Options Summary ===")
	fmt.Println("\nOption              | Purpose                          | Example Usage")
	fmt.Println(
		"------------------- | -------------------------------- | -----------------------------",
	)
	fmt.Println("DELIMITER          | Change field separator           | DELIMITER '|'")
	fmt.Println("HEADER             | Include/exclude header row       | HEADER false")
	fmt.Println("NULL               | Custom null representation       | NULL 'N/A'")
	fmt.Println("FORCE_QUOTE        | Force quotes on fields           | FORCE_QUOTE *")
	fmt.Println("FORCE_QUOTE (cols) | Force quotes on specific columns | FORCE_QUOTE (name, desc)")
	fmt.Println("ESCAPE             | Custom escape character          | ESCAPE '\\'")
	fmt.Println("ENCODING           | Character encoding               | ENCODING 'UTF-8'")
	fmt.Println("DATEFORMAT         | Date format for date columns     | DATEFORMAT '%d/%m/%Y'")
}

func init() {
	// Run comparison at startup if needed
	// createOptionComparison()
}
