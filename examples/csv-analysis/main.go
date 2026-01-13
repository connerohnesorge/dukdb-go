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

	fmt.Println("=== CSV Data Analysis Example ===")

	// Create sample sales data for analysis
	fmt.Println("\n1. Creating sample sales dataset...")

	salesData := `order_id,customer_id,product_id,product_name,category,quantity,unit_price,order_date,region,sales_rep
1001,101,201,Laptop,Electronics,2,999.99,2023-01-05,North,Alice
1002,102,202,Mouse,Accessories,5,29.99,2023-01-07,South,Bob
1003,103,203,Keyboard,Accessories,3,79.99,2023-01-10,East,Charlie
1004,104,204,Monitor,Electronics,1,399.99,2023-01-12,West,Diana
1005,105,205,Headphones,Electronics,2,149.99,2023-01-15,North,Eve
1006,106,206,Webcam,Electronics,1,89.99,2023-01-18,South,Frank
1007,107,207,USB Hub,Accessories,4,49.99,2023-01-20,East,Grace
1008,108,208,External HDD,Electronics,1,129.99,2023-01-22,West,Henry
1009,109,209,Laptop Stand,Accessories,2,39.99,2023-01-25,North,Ivy
1010,110,210,Graphics Tablet,Electronics,1,299.99,2023-01-28,South,Jack
1011,101,211,Smartphone,Electronics,1,699.99,2023-02-02,East,Alice
1012,102,212,Phone Case,Accessories,3,19.99,2023-02-05,West,Bob
1013,103,213,Wireless Charger,Accessories,2,39.99,2023-02-08,North,Charlie
1014,104,214,Smart Watch,Electronics,1,249.99,2023-02-10,South,Diana
1015,105,215,Tablet,Electronics,1,449.99,2023-02-12,East,Eve
1016,106,216,Stylus,Accessories,2,29.99,2023-02-15,West,Frank
1017,107,217,Desk Lamp,Accessories,1,59.99,2023-02-18,North,Grace
1018,108,218,Printer,Electronics,1,199.99,2023-02-20,South,Henry
1019,109,219,Scanner,Electronics,1,179.99,2023-02-22,East,Ivy
1020,110,220,Projector,Electronics,1,599.99,2023-02-25,West,Jack`

	salesFile := "sales_data.csv"
	err = os.WriteFile(salesFile, []byte(salesData), 0644)
	if err != nil {
		log.Fatal("Failed to create sales data:", err)
	}
	defer os.Remove(salesFile)

	// Example 1: Basic descriptive statistics
	fmt.Println("\n2. Basic Descriptive Statistics:")

	statsQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) as total_orders,
			COUNT(DISTINCT customer_id) as unique_customers,
			COUNT(DISTINCT product_id) as unique_products,
			COUNT(DISTINCT sales_rep) as unique_sales_reps,
			SUM(quantity) as total_units_sold,
			SUM(quantity * unit_price) as total_revenue,
			AVG(AVG(quantity * unit_price) as avg_order_value,
			AVG(MIN(quantity * unit_price) as min_order_value,
			AVG(MAX(quantity * unit_price) as max_order_value
		FROM read_csv_auto('%s')
	`, salesFile)

	var stats struct {
		totalOrders      int
		uniqueCustomers  int
		uniqueProducts   int
		uniqueSalesReps  int
		totalUnits       int
		totalRevenue     float64
		avgOrderValue    float64
		minOrderValue    float64
		maxOrderValue    float64
	}

	err = db.QueryRow(statsQuery).Scan(
		&stats.totalOrders,
		&stats.uniqueCustomers,
		&stats.uniqueProducts,
		&stats.uniqueSalesReps,
		&stats.totalUnits,
		&stats.totalRevenue,
		&stats.avgOrderValue,
		&stats.minOrderValue,
		&stats.maxOrderValue,
	)
	if err != nil {
		log.Fatal("Failed to get basic statistics:", err)
	}

	fmt.Println("Dataset Overview:")
	fmt.Printf("  Total Orders: %d\n", stats.totalOrders)
	fmt.Printf("  Unique Customers: %d\n", stats.uniqueCustomers)
	fmt.Printf("  Unique Products: %d\n", stats.uniqueProducts)
	fmt.Printf("  Unique Sales Reps: %d\n", stats.uniqueSalesReps)
	fmt.Printf("  Total Units Sold: %d\n", stats.totalUnits)
	fmt.Printf("  Total Revenue: $%.2f\n", stats.totalRevenue)
	fmt.Printf("  Average Order Value: $%.2f\n", stats.avgOrderValue)
	fmt.Printf("  Min Order Value: $%.2f\n", stats.minOrderValue)
	fmt.Printf("  Max Order Value: $%.2f\n", stats.maxOrderValue)

	// Example 2: Category analysis
	fmt.Println("\n3. Product Category Analysis:")

	categoryQuery := fmt.Sprintf(`
		SELECT
			category,
			COUNT(*) as order_count,
			SUM(quantity) as units_sold,
			SUM(quantity * unit_price) as category_revenue,
			AVG(AVG(quantity * unit_price) as avg_order_value,
			AVG(category_revenue * 100.0 / SUM(quantity * unit_price) OVER () as revenue_percentage
		FROM read_csv_auto('%s')
		GROUP BY category
		ORDER BY category_revenue DESC
	`, salesFile)

	rows, err := db.Query(categoryQuery)
	if err != nil {
		log.Fatal("Failed to query category analysis:", err)
	}
	defer rows.Close()

	fmt.Println("Category Performance:")
	fmt.Printf("%-15s | %-10s | %-10s | %-15s | %-10s | %-10s\n",
		"Category", "Orders", "Units", "Revenue", "Avg Value", "% Revenue")
	fmt.Println(strings.Repeat("-", 80))

	for rows.Next() {
		var category string
		var orderCount, unitsSold int
		var categoryRevenue, avgOrderValue, revenuePercentage float64

		err := rows.Scan(&category, &orderCount, &unitsSold, &categoryRevenue, &avgOrderValue, &revenuePercentage)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("%-15s | %-10d | %-10d | $%-14.2f | $%-9.2f | %-9.1f%%\n",
			category, orderCount, unitsSold, categoryRevenue, avgOrderValue, revenuePercentage)
	}

	// Example 3: Regional analysis
	fmt.Println("\n4. Regional Sales Analysis:")

	regionalQuery := fmt.Sprintf(`
		SELECT
			region,
			COUNT(*) as orders,
			COUNT(DISTINCT customer_id) as unique_customers,
			SUM(quantity * unit_price) as revenue,
			AVG(AVG(quantity * unit_price) as avg_order_value,
			AVG(revenue / COUNT(DISTINCT customer_id) as revenue_per_customer
		FROM read_csv_auto('%s')
		GROUP BY region
		ORDER BY revenue DESC
	`, salesFile)

	rows, err = db.Query(regionalQuery)
	if err != nil {
		log.Fatal("Failed to query regional analysis:", err)
	}
	defer rows.Close()

	fmt.Println("Regional Performance:")
	fmt.Printf("%-10s | %-10s | %-15s | %-15s | %-15s | %-15s\n",
		"Region", "Orders", "Customers", "Revenue", "Avg Order", "Revenue/Customer")
	fmt.Println(strings.Repeat("-", 90))

	for rows.Next() {
		var region string
		var orders, uniqueCustomers int
		var revenue, avgOrderValue, revenuePerCustomer float64

		err := rows.Scan(&region, &orders, &uniqueCustomers, &revenue, &avgOrderValue, &revenuePerCustomer)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("%-10s | %-10d | %-15d | $%-14.2f | $%-14.2f | $%-14.2f\n",
			region, orders, uniqueCustomers, revenue, avgOrderValue, revenuePerCustomer)
	}

	// Example 4: Sales rep performance analysis
	fmt.Println("\n5. Sales Representative Performance:")

	salesRepQuery := fmt.Sprintf(`
		SELECT
			sales_rep,
			COUNT(*) as orders,
			SUM(quantity * unit_price) as revenue,
			AVG(AVG(quantity * unit_price) as avg_order_value,
			SUM(quantity) as total_units,
			AVG(SUM(quantity * unit_price) / SUM(quantity) as avg_unit_price
		FROM read_csv_auto('%s')
		GROUP BY sales_rep
		ORDER BY revenue DESC
	`, salesFile)

	rows, err = db.Query(salesRepQuery)
	if err != nil {
		log.Fatal("Failed to query sales rep performance:", err)
	}
	defer rows.Close()

	fmt.Println("Sales Rep Performance:")
	fmt.Printf("%-10s | %-10s | %-15s | %-15s | %-15s | %-15s\n",
		"Sales Rep", "Orders", "Revenue", "Avg Order", "Units", "Avg Unit Price")
	fmt.Println(strings.Repeat("-", 90))

	for rows.Next() {
		var salesRep string
		var orders int
		var revenue, avgOrderValue, totalUnits, avgUnitPrice float64

		err := rows.Scan(&salesRep, &orders, &revenue, &avgOrderValue, &totalUnits, &avgUnitPrice)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("%-10s | %-10d | $%-14.2f | $%-14.2f | %-15.0f | $%-14.2f\n",
			salesRep, orders, revenue, avgOrderValue, totalUnits, avgUnitPrice)
	}

	// Example 5: Time series analysis
	fmt.Println("\n6. Time Series Analysis:")

	timeSeriesQuery := fmt.Sprintf(`
		SELECT
			order_date,
			COUNT(*) as daily_orders,
			SUM(quantity) as daily_units,
			SUM(quantity * unit_price) as daily_revenue,
			AVG(AVG(quantity * unit_price) as avg_order_value
		FROM read_csv_auto('%s')
		GROUP BY order_date
		ORDER BY order_date
	`, salesFile)

	rows, err = db.Query(timeSeriesQuery)
	if err != nil {
		log.Fatal("Failed to query time series:", err)
	}
	defer rows.Close()

	fmt.Println("Daily Sales Trend (showing first 10 days):")
	fmt.Printf("%-12s | %-12s | %-12s | %-15s | %-15s\n",
		"Date", "Orders", "Units", "Revenue", "Avg Order")
	fmt.Println(strings.Repeat("-", 75))

	rowCount := 0
	for rows.Next() && rowCount < 10 {
		var orderDate string
		var dailyOrders int
		var dailyUnits int
		var dailyRevenue, avgOrderValue float64

		err := rows.Scan(&orderDate, &dailyOrders, &dailyUnits, &dailyRevenue, &avgOrderValue)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("%-12s | %-12d | %-12d | $%-14.2f | $%-14.2f\n",
			orderDate, dailyOrders, dailyUnits, dailyRevenue, avgOrderValue)
		rowCount++
	}

	// Example 6: Product performance analysis
	fmt.Println("\n7. Product Performance Analysis:")

	productQuery := fmt.Sprintf(`
		SELECT
			product_id,
			product_name,
			category,
			COUNT(*) as times_ordered,
			SUM(quantity) as total_quantity,
			SUM(quantity * unit_price) as total_revenue,
			AVG(AVG(unit_price) as avg_price
		FROM read_csv_auto('%s')
		GROUP BY product_id, product_name, category
		ORDER BY total_revenue DESC
	`, salesFile)

	rows, err = db.Query(productQuery)
	if err != nil {
		log.Fatal("Failed to query product performance:", err)
	}
	defer rows.Close()

	fmt.Println("Top Products by Revenue:")
	fmt.Printf("%-5s | %-20s | %-12s | %-12s | %-12s | %-15s | %-10s\n",
		"ID", "Product Name", "Category", "Orders", "Quantity", "Revenue", "Avg Price")
	fmt.Println(strings.Repeat("-", 105))

	for rows.Next() {
		var productID int
		var productName, category string
		var timesOrdered, totalQuantity int
		var totalRevenue, avgPrice float64

		err := rows.Scan(&productID, &productName, &category, &timesOrdered, &totalQuantity, &totalRevenue, &avgPrice)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		if len(productName) > 20 {
			productName = productName[:17] + "..."
		}

		fmt.Printf("%-5d | %-20s | %-12s | %-12d | %-12d | $%-14.2f | $%-9.2f\n",
			productID, productName, category, timesOrdered, totalQuantity, totalRevenue, avgPrice)
	}

	// Example 7: Customer analysis
	fmt.Println("\n8. Customer Purchase Behavior Analysis:")

	customerQuery := fmt.Sprintf(`
		SELECT
			customer_id,
			COUNT(*) as order_count,
			SUM(quantity) as total_units,
			SUM(quantity * unit_price) as total_spent,
			AVG(AVG(quantity * unit_price) as avg_order_value,
			COUNT(DISTINCT product_id) as unique_products,
			COUNT(DISTINCT category) as unique_categories
		FROM read_csv_auto('%s')
		GROUP BY customer_id
		ORDER BY total_spent DESC
	`, salesFile)

	rows, err = db.Query(customerQuery)
	if err != nil {
		log.Fatal("Failed to query customer analysis:", err)
	}
	defer rows.Close()

	fmt.Println("Top Customers by Spending:")
	fmt.Printf("%-5s | %-12s | %-12s | %-15s | %-15s | %-15s | %-15s\n",
		"ID", "Orders", "Units", "Total Spent", "Avg Order", "Products", "Categories")
	fmt.Println(strings.Repeat("-", 95))

	for rows.Next() {
		var customerID int
		var orderCount, totalUnits, uniqueProducts, uniqueCategories int
		var totalSpent, avgOrderValue float64

		err := rows.Scan(&customerID, &orderCount, &totalUnits, &totalSpent, &avgOrderValue, &uniqueProducts, &uniqueCategories)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("%-5d | %-12d | %-12d | $%-14.2f | $%-14.2f | %-15d | %-15d\n",
			customerID, orderCount, totalUnits, totalSpent, avgOrderValue, uniqueProducts, uniqueCategories)
	}

	// Example 8: Cross-selling analysis
	fmt.Println("\n9. Cross-Selling Analysis (Product Combinations):")

	crossSellQuery := fmt.Sprintf(`
		SELECT
			a.category as category_a,
			b.category as category_b,
			COUNT(*) as combination_count,
			COUNT(DISTINCT a.customer_id) as unique_customers
		FROM read_csv_auto('%s') a
		JOIN read_csv_auto('%s') b ON a.customer_id = b.customer_id AND a.order_id < b.order_id
		WHERE a.category != b.category
		GROUP BY a.category, b.category
		ORDER BY combination_count DESC
	`, salesFile, salesFile)

	rows, err = db.Query(crossSellQuery)
	if err != nil {
		log.Fatal("Failed to query cross-selling analysis:", err)
	}
	defer rows.Close()

	fmt.Println("Category Cross-Selling (Top 10):")
	fmt.Printf("%-15s | %-15s | %-18s | %-15s\n",
		"Category A", "Category B", "Combinations", "Customers")
	fmt.Println(strings.Repeat("-", 70))

	rowCount = 0
	for rows.Next() && rowCount < 10 {
		var categoryA, categoryB string
		var combinationCount, uniqueCustomers int

		err := rows.Scan(&categoryA, &categoryB, &combinationCount, &uniqueCustomers)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("%-15s | %-15s | %-18d | %-15d\n",
			categoryA, categoryB, combinationCount, uniqueCustomers)
		rowCount++
	}

	// Example 9: RFM Analysis (Recency, Frequency, Monetary)
	fmt.Println("\n10. RFM Analysis (Customer Segmentation):")

	rfmQuery := fmt.Sprintf(`
		WITH customer_metrics AS (
			SELECT
				customer_id,
				MAX(order_date) as last_order_date,
				COUNT(*) as frequency,
				SUM(quantity * unit_price) as monetary
			FROM read_csv_auto('%s')
			GROUP BY customer_id
		),
		rfm_scores AS (
			SELECT
				customer_id,
				DATE_DIFF('day', last_order_date, CURRENT_DATE) as recency,
				frequency,
				monetary,
				-- Recency score (lower is better)
				CASE
					WHEN DATE_DIFF('day', last_order_date, CURRENT_DATE) <= 7 THEN 5
					WHEN DATE_DIFF('day', last_order_date, CURRENT_DATE) <= 14 THEN 4
					WHEN DATE_DIFF('day', last_order_date, CURRENT_DATE) <= 30 THEN 3
					WHEN DATE_DIFF('day', last_order_date, CURRENT_DATE) <= 60 THEN 2
					ELSE 1
				END as r_score,
				-- Frequency score
				CASE
					WHEN frequency >= 4 THEN 5
					WHEN frequency >= 3 THEN 4
					WHEN frequency >= 2 THEN 3
					WHEN frequency >= 1 THEN 2
					ELSE 1
				END as f_score,
				-- Monetary score
				CASE
					WHEN monetary >= 1000 THEN 5
					WHEN monetary >= 500 THEN 4
					WHEN monetary >= 250 THEN 3
					WHEN monetary >= 100 THEN 2
					ELSE 1
				END as m_score
			FROM customer_metrics
		)
		SELECT
			customer_id,
			recency,
			frequency,
			monetary,
			r_score,
			f_score,
			m_score,
			CAST(r_score AS VARCHAR) || CAST(f_score AS VARCHAR) || CAST(m_score AS VARCHAR) as rfm_score,
			CASE
				WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
				WHEN r_score >= 3 AND f_score >= 4 AND m_score >= 4 THEN 'Loyal Customers'
				WHEN r_score >= 4 AND f_score >= 3 AND m_score >= 3 THEN 'Potential Loyalists'
				WHEN r_score >= 4 AND f_score <= 2 AND m_score >= 4 THEN 'New Customers'
				WHEN r_score >= 3 AND f_score >= 3 AND m_score <= 2 THEN 'Promising'
				WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'Need Attention'
				WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'About to Sleep'
				WHEN r_score <= 2 AND f_score <= 2 AND m_score >= 4 THEN 'At Risk'
				WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Lost'
				ELSE 'Others'
			END as customer_segment
		FROM rfm_scores
		ORDER BY monetary DESC
	`, salesFile)

	rows, err = db.Query(rfmQuery)
	if err != nil {
		log.Fatal("Failed to query RFM analysis:", err)
	}
	defer rows.Close()

	fmt.Println("Customer Segmentation (RFM Analysis):")
	fmt.Printf("%-5s | %-8s | %-9s | %-10s | %-8s | %-8s | %-8s | %-10s | %-15s\n",
		"ID", "Recency", "Frequency", "Monetary", "R", "F", "M", "RFM Score", "Segment")
	fmt.Println(strings.Repeat("-", 95))

	for rows.Next() {
		var customerID, recency, frequency int
		var monetary float64
		var rScore, fScore, mScore int
		var rfmScore, customerSegment string

		err := rows.Scan(&customerID, &recency, &frequency, &monetary, &rScore, &fScore, &mScore, &rfmScore, &customerSegment)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("%-5d | %-8d | %-9d | $%-9.2f | %-8d | %-8d | %-8d | %-10s | %-15s\n",
			customerID, recency, frequency, monetary, rScore, fScore, mScore, rfmScore, customerSegment)
	}

	// Example 10: Export analysis results
	fmt.Println("\n11. Exporting Analysis Results:")

	// Export category analysis
	_, err = db.Exec(fmt.Sprintf(`
		COPY (
			SELECT
				category,
				COUNT(*) as order_count,
				SUM(quantity) as units_sold,
				SUM(quantity * unit_price) as category_revenue,
				AVG(AVG(quantity * unit_price) as avg_order_value
			FROM read_csv_auto('%s')
			GROUP BY category
			ORDER BY category_revenue DESC
		) TO 'category_analysis.csv' WITH (HEADER true)
	`, salesFile))
	if err != nil {
		log.Printf("Warning: Failed to export category analysis: %v", err)
	} else {
		fmt.Println("Category analysis exported to category_analysis.csv")
		defer os.Remove("category_analysis.csv")
	}

	// Export RFM segments summary
	_, err = db.Exec(fmt.Sprintf(`
		COPY (
			WITH customer_metrics AS (
				SELECT
					customer_id,
					COUNT(*) as frequency,
					SUM(quantity * unit_price) as monetary
				FROM read_csv_auto('%s')
				GROUP BY customer_id
			),
			segments AS (
				SELECT
					CASE
						WHEN monetary >= 1000 THEN 'High Value'
						WHEN monetary >= 500 THEN 'Medium Value'
						ELSE 'Low Value'
					END as value_segment,
					COUNT(*) as customer_count,
					AVG(AVG(monetary) as avg_value,
					AVG(SUM(monetary) as total_value
				FROM customer_metrics
				GROUP BY value_segment
			)
			SELECT * FROM segments
			ORDER BY total_value DESC
		) TO 'customer_segments.csv' WITH (HEADER true)
	`, salesFile))
	if err != nil {
		log.Printf("Warning: Failed to export customer segments: %v", err)
	} else {
		fmt.Println("Customer segments exported to customer_segments.csv")
		defer os.Remove("customer_segments.csv")
	}

	fmt.Println("\n✓ CSV data analysis example completed successfully!")
	fmt.Println("\nAnalysis performed:")
	fmt.Println("- Basic descriptive statistics")
	fmt.Println("- Category performance analysis")
	fmt.Println("- Regional sales analysis")
	fmt.Println("- Sales representative performance")
	fmt.Println("- Time series trend analysis")
	fmt.Println("- Product performance ranking")
	fmt.Println("- Customer behavior analysis")
	fmt.Println("- Cross-selling category combinations")
	fmt.Println("- RFM customer segmentation")
	fmt.Println("- Export of analysis results")
}

// Helper function to demonstrate advanced SQL functions
func demonstrateAdvancedFunctions() {
	fmt.Println("\nAdvanced SQL Functions for Analysis:")
	fmt.Println("1. Window Functions:")
	fmt.Println("   - ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC)")
	fmt.Println("   - SUM(revenue) OVER (PARTITION BY region)")
	fmt.Println("   - LAG(revenue, 1) OVER (ORDER BY date)")
	fmt.Println("\n2. Date Functions:")
	fmt.Println("   - DATE_TRUNC('month', order_date)")
	fmt.Println("   - DATE_DIFF('day', start_date, end_date)")
	fmt.Println("   - EXTRACT(YEAR FROM date_column)")
	fmt.Println("\n3. String Functions:")
	fmt.Println("   - CONCAT() / || operator")
	fmt.Println("   - SUBSTRING(column, start, length)")
	fmt.Println("   - REGEXP_REPLACE()")
}

// Helper function to show visualization suggestions
func showVisualizationSuggestions() {
	fmt.Println("\nVisualization Suggestions:")
	fmt.Println("1. Bar Chart: Category revenue comparison")
	fmt.Println("2. Line Chart: Daily sales trend")
	fmt.Println("3. Pie Chart: Regional revenue distribution")
	fmt.Println("4. Scatter Plot: Order value vs quantity")
	fmt.Println("5. Heatmap: Sales rep performance by region")
	fmt.Println("6. Box Plot: Order value distribution by category")
	fmt.Println("7. Funnel: Customer journey stages")
}

// Helper function for performance optimization tips
func showPerformanceTips() {
	fmt.Println("\nPerformance Optimization Tips:")
	fmt.Println("1. Use columnar storage for large datasets")
	fmt.Println("2. Create indexes on frequently queried columns")
	fmt.Println("3. Use approximate aggregates for large datasets")
	fmt.Println("4. Partition data by date or category")
	fmt.Println("5. Use sampling for exploratory analysis")
	fmt.Println("6. Cache intermediate results in temporary tables")
	fmt.Println("7. Optimize query order (filter early)")
}

// Helper function for export formats
func showExportFormats() {
	fmt.Println("\nExport Format Options:")
	fmt.Println("1. CSV: Universal format, good for Excel")
	fmt.Println("2. JSON: Good for web APIs")
	fmt.Println("3. Parquet: Compressed columnar format")
	fmt.Println("4. Excel: With formatting and formulas")
	fmt.Println("5. SQL: INSERT statements for databases")
	fmt.Println("6. Markdown: For documentation")
	fmt.Println("7. HTML: For web dashboards")
}

// Helper function for further analysis suggestions
func showFurtherAnalysis() {
	fmt.Println("\nFurther Analysis Ideas:")
	fmt.Println("1. Cohort Analysis: Customer retention over time")
	fmt.Println("2. Market Basket Analysis: Product associations")
	fmt.Println("3. Predictive Analytics: Sales forecasting")
	fmt.Println("4. Anomaly Detection: Unusual sales patterns")
	fmt.Println("5. Geographic Analysis: Sales by location")
	fmt.Println("6. Price Elasticity: Demand vs price changes")
	fmt.Println("7. Seasonal Analysis: Monthly/quarterly patterns")
	fmt.Println("8. Customer Lifetime Value: Long-term value")
	fmt.Println("9. Attribution Analysis: Channel performance")
	fmt.Println("10. What-if Analysis: Scenario modeling")
}

// Helper function to create analysis template
func createAnalysisTemplate() string {
	return `
-- Sales Analysis Template
-- 1. Basic Metrics
SELECT
    COUNT(*) as total_orders,
    SUM(revenue) as total_revenue,
    AVG(revenue) as avg_order_value
FROM sales_data;

-- 2. Time Analysis
SELECT
    DATE_TRUNC('month', order_date) as month,
    COUNT(*) as orders,
    SUM(revenue) as revenue
FROM sales_data
GROUP BY month
ORDER BY month;

-- 3. Segment Analysis
SELECT
    customer_segment,
    COUNT(*) as customers,
    AVG(revenue) as avg_revenue
FROM customer_data
GROUP BY customer_segment;
`
}