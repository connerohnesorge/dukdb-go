package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
	// Create sample JSON files with different schemas

	// Sample 1: Product catalog with consistent schema
	productsData := `[
		{"product_id": 1, "name": "Laptop", "category": "Electronics", "price": 999.99, "stock": 50, "active": true},
		{"product_id": 2, "name": "Mouse", "category": "Electronics", "price": 29.99, "stock": 200, "active": true},
		{"product_id": 3, "name": "Desk", "category": "Furniture", "price": 299.99, "stock": 75, "active": true},
		{"product_id": 4, "name": "Chair", "category": "Furniture", "price": 199.99, "stock": 150, "active": false},
		{"product_id": 5, "name": "Monitor", "category": "Electronics", "price": 399.99, "stock": 30, "active": true}
	]`

	// Sample 2: Mixed schema - some products have additional fields
	mixedProductsData := `[
		{"product_id": 1, "name": "Laptop", "category": "Electronics", "price": 999.99, "stock": 50, "active": true, "warranty_months": 12},
		{"product_id": 2, "name": "Mouse", "category": "Electronics", "price": 29.99, "stock": 200, "active": true},
		{"product_id": 3, "name": "Desk", "category": "Furniture", "price": 299.99, "stock": 75, "active": true, "assembly_required": true},
		{"product_id": 4, "name": "Chair", "category": "Furniture", "price": 199.99, "stock": 150, "active": false, "warranty_months": 6},
		{"product_id": 5, "name": "Monitor", "category": "Electronics", "price": 399.99, "stock": 30, "active": true}
	]`

	// Sample 3: Evolving schema - different fields over time
	evolvedProductsData := `[
		{"product_id": 1, "name": "Laptop", "category": "Electronics", "price": 999.99},
		{"product_id": 2, "name": "Mouse", "category": "Electronics", "price": 29.99, "stock": 200},
		{"product_id": 3, "name": "Desk", "category": "Furniture", "price": 299.99, "stock": 75, "active": true},
		{"product_id": 4, "name": "Chair", "category": "Furniture", "price": 199.99, "stock": 150, "active": false, "created_date": "2023-01-15"},
		{"product_id": 5, "name": "Monitor", "category": "Electronics", "price": 399.99, "stock": 30, "active": true, "created_date": "2023-06-20", "updated_date": "2024-01-10"}
	]`

	// Sample 4: Inconsistent data types
	inconsistentData := `[
		{"product_id": 1, "name": "Laptop", "price": 999.99, "stock": "50"},
		{"product_id": 2, "name": "Mouse", "price": "29.99", "stock": 200},
		{"product_id": 3, "name": "Desk", "price": 299.99, "stock": "75"},
		{"product_id": 4, "name": "Chair", "price": 199.99, "stock": null},
		{"product_id": 5, "name": "Monitor", "price": 399.99, "stock": "30"}
	]`

	// Sample 5: Hierarchical data (categories and subcategories)
	hierarchicalData := `[
		{"category": "Electronics", "subcategory": "Computers", "product_id": 1, "name": "Laptop", "price": 999.99},
		{"category": "Electronics", "subcategory": "Accessories", "product_id": 2, "name": "Mouse", "price": 29.99},
		{"category": "Furniture", "subcategory": "Office", "product_id": 3, "name": "Desk", "price": 299.99},
		{"category": "Furniture", "subcategory": "Seating", "product_id": 4, "name": "Chair", "price": 199.99},
		{"category": "Electronics", "subcategory": "Displays", "product_id": 5, "name": "Monitor", "price": 399.99}
	]`

	// Write sample data to files
	files := []struct {
		name string
		data string
	}{
		{"products_consistent.json", productsData},
		{"products_mixed.json", mixedProductsData},
		{"products_evolved.json", evolvedProductsData},
		{"products_inconsistent.json", inconsistentData},
		{"products_hierarchical.json", hierarchicalData},
	}

	for _, file := range files {
		err := os.WriteFile(file.name, []byte(file.data), 0644)
		if err != nil {
			log.Fatalf("Failed to write %s: %v", file.name, err)
		}
		defer os.Remove(file.name)
	}

	// Connect to in-memory database
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Example 1: Consistent schema analysis
	fmt.Println("=== Example 1: Consistent Schema Analysis ===")
	query := fmt.Sprintf("SELECT * FROM read_json('%s')", files[0].name)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read consistent JSON: %v", err)
	}
	defer rows.Close()

	// Print column information
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}
	fmt.Printf("Columns: %v\n", columns)

	// Analyze data types
	fmt.Println("\nData types inferred from consistent schema:")
	for rows.Next() {
		var active bool
		var category string
		var name string
		var price float64
		var productID int
		var stock int

		err := rows.Scan(&active, &category, &name, &price, &productID, &stock)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("  Product %d: %s (%.0f) - %s - Stock: %.0f - Active: %t\n",
			productID, name, price, category, float64(stock), active)
	}

	// Example 2: Mixed schema handling
	fmt.Println("\n=== Example 2: Mixed Schema Handling ===")
	query = fmt.Sprintf("SELECT * FROM read_json('%s')", files[1].name)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read mixed JSON: %v", err)
	}
	defer rows.Close()

	columns, err = rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}
	fmt.Printf("Columns with optional fields: %v\n", columns)

	fmt.Println("\nHandling optional fields:")
	for rows.Next() {
		var active sql.NullBool
		var assemblyRequired sql.NullBool
		var category string
		var name string
		var price float64
		var productID int
		var stock int
		var warrantyMonths sql.NullInt64

		err := rows.Scan(&active, &assemblyRequired, &category, &name, &price, &productID, &stock, &warrantyMonths)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		fmt.Printf("  Product %d: %s\n", productID, name)
		if warrantyMonths.Valid {
			fmt.Printf("    Warranty: %.0f months\n", float64(warrantyMonths.Int64))
		}
		if assemblyRequired.Valid {
			fmt.Printf("    Assembly Required: %t\n", assemblyRequired.Bool)
		}
	}

	// Example 3: Schema evolution analysis
	fmt.Println("\n=== Example 3: Schema Evolution Analysis ===")

	// Analyze how schemas evolve over time
	for _, file := range []string{files[0].name, files[2].name} {
		fmt.Printf("\nAnalyzing %s:\n", file)
		query = fmt.Sprintf("DESCRIBE SELECT * FROM read_json('%s')", file)
		rows, err = db.Query(query)
		if err != nil {
			// If DESCRIBE is not supported, show column count
			query = fmt.Sprintf("SELECT * FROM read_json('%s') LIMIT 1", file)
			rows, err = db.Query(query)
			if err != nil {
				log.Fatalf("Failed to analyze schema: %v", err)
			}
			defer rows.Close()

			columns, _ := rows.Columns()
			fmt.Printf("  Schema has %d columns: %v\n", len(columns), columns)
		} else {
			defer rows.Close()
			fmt.Println("  Schema details:")
			for rows.Next() {
				// Read schema information
				var colInfo string
				rows.Scan(&colInfo)
				fmt.Printf("    %s\n", colInfo)
			}
		}
	}

	// Example 4: Data type consistency
	fmt.Println("\n=== Example 4: Data Type Consistency ===")
	query = fmt.Sprintf("SELECT * FROM read_json('%s')", files[3].name)
	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to read inconsistent JSON: %v", err)
	}
	defer rows.Close()

	fmt.Println("Handling inconsistent data types:")
	for rows.Next() {
		var name string
		var price float64
		var productID int
		var stock sql.NullString

		err := rows.Scan(&name, &price, &productID, &stock)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		fmt.Printf("  Product %d: %s - Price: $%.2f\n", productID, name, price)
		if stock.Valid {
			fmt.Printf("    Stock: %s (type: string)\n", stock.String)
		} else {
			fmt.Printf("    Stock: NULL\n")
		}
	}

	// Example 5: Hierarchical data analysis
	fmt.Println("\n=== Example 5: Hierarchical Data Analysis ===")
	query = fmt.Sprintf(`SELECT
		category,
		subcategory,
		COUNT(*) as product_count,
		AVG(price) as avg_price,
		MIN(price) as min_price,
		MAX(price) as max_price
	FROM read_json('%s')
	GROUP BY category, subcategory
	ORDER BY category, subcategory`, files[4].name)

	rows, err = db.Query(query)
	if err != nil {
		log.Fatalf("Failed to analyze hierarchical data: %v", err)
	}
	defer rows.Close()

	fmt.Println("Product hierarchy analysis:")
	currentCategory := ""
	for rows.Next() {
		var avgPrice float64
		var category string
		var maxPrice float64
		var minPrice float64
		var productCount int64
		var subcategory string

		err := rows.Scan(&avgPrice, &category, &maxPrice, &minPrice, &productCount, &subcategory)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		if category != currentCategory {
			fmt.Printf("\n%s:\n", category)
			currentCategory = category
		}

		fmt.Printf("  %s: %d products, Avg Price: $%.2f (Range: $%.2f - $%.2f)\n",
			subcategory, productCount, avgPrice, minPrice, maxPrice)
	}

	// Example 6: Schema validation patterns
	fmt.Println("\n=== Example 6: Schema Validation Patterns ===")

	// Create a view with validated data
	_, err = db.Exec(fmt.Sprintf(`
		CREATE VIEW validated_products AS
		SELECT
			product_id,
			name,
			price,
			CASE
				WHEN price > 0 THEN 'valid'
				ELSE 'invalid'
			END as price_validation,
			CASE
				WHEN name IS NOT NULL AND name != '' THEN 'valid'
				ELSE 'invalid'
			END as name_validation
		FROM read_json('%s')
	`, files[0].name))
	if err != nil {
		fmt.Printf("Note: View creation failed (expected in some versions): %v\n", err)
	} else {
		defer db.Exec("DROP VIEW validated_products")

		// Query the validated view
		rows, err = db.Query("SELECT * FROM validated_products WHERE price_validation = 'valid' AND name_validation = 'valid'")
		if err != nil {
			fmt.Printf("Note: Query on view failed: %v\n", err)
		} else {
			defer rows.Close()
			fmt.Println("Validated products:")
			count := 0
			for rows.Next() && count < 3 {
				var name string
				var price float64
				var productID int
				var nameValidation string
				var priceValidation string

				err := rows.Scan(&priceValidation, &nameValidation, &name, &price, &productID)
				if err != nil {
					continue
				}
				fmt.Printf("  Product %d: %s ($%.2f) - %s/%s\n",
					productID, name, price, priceValidation, nameValidation)
				count++
			}
		}
	}

	// Example 7: Export schema information
	fmt.Println("\n=== Example 7: Export Schema Information ===")
	outputFile := "schema_analysis.json"

	// Create a summary of all schemas
	schemaSummary := fmt.Sprintf(`COPY (
		SELECT
			'products_consistent' as table_name,
			6 as column_count,
			'All columns present' as schema_quality,
			'High' as consistency_score
		UNION ALL
		SELECT
			'products_mixed' as table_name,
			8 as column_count,
			'Optional fields present' as schema_quality,
			'Medium' as consistency_score
		UNION ALL
		SELECT
			'products_evolved' as table_name,
			7 as column_count,
			'New fields added' as schema_quality,
			'Medium' as consistency_score
	) TO '%s'`, outputFile)

	_, err = db.Exec(schemaSummary)
	if err != nil {
		fmt.Printf("Note: Schema summary export skipped: %v\n", err)
	} else {
		fmt.Printf("Schema analysis exported to %s\n", outputFile)
		defer os.Remove(outputFile)
	}

	fmt.Println("\nAll examples completed successfully!")
	fmt.Println("\nKey Takeaways:")
	fmt.Println("- JSON schemas can evolve over time")
	fmt.Println("- Use NULL handling for optional fields")
	fmt.Println("- Check data type consistency")
	fmt.Println("- Validate schemas before processing")
	fmt.Println("- Document schema changes")
}

// Helper function to analyze JSON structure
func analyzeJSONStructure(filename string) {
	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}

	fmt.Printf("\nJSON Structure Analysis for %s:\n", filename)
	fmt.Printf("File size: %d bytes\n", len(data))
	fmt.Printf("First 100 characters: %s\n", string(data[:min(100, len(data))]))
	if len(data) > 100 {
		fmt.Println("...")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}