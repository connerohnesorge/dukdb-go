package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/dukdb/dukdb-go"
	// Import engine to register backend
)

func main() {
	// Create a new database connection
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	fmt.Println("=== CSV Data Transformation Example ===")

	// Create sample raw data for transformation
	fmt.Println("\n1. Creating raw operational data for ETL transformation...")

	rawData := `transaction_id,customer_code,product_sku,quantity,unit_cost,unit_price,timestamp,store_id,raw_receipt_data
TXN001,CUST-101,SKU-LAPTOP-001,1,800.00,999.99,2023-01-05T14:30:00Z,STORE-N001,{discount: 5%, payment: card}
TXN002,CUST-102,SKU-MOUSE-002,5,20.00,29.99,2023-01-05T14:35:00Z,STORE-S002,{discount: 0%, payment: cash}
TXN003,CUST-103,SKU-KB-003,3,60.00,79.99,2023-01-06T10:15:00Z,STORE-E003,{discount: 10%, payment: card}
TXN004,CUST-104,SKU-MON-004,1,300.00,399.99,2023-01-06T11:20:00Z,STORE-W004,{discount: 0%, payment: card}
TXN005,CUST-105,SKU-HP-005,2,120.00,149.99,2023-01-07T09:45:00Z,STORE-N001,{discount: 15%, payment: card}
TXN006,CUST-106,SKU-WEB-006,1,70.00,89.99,2023-01-07T13:20:00Z,STORE-S002,{discount: 0%, payment: cash}
TXN007,CUST-107,SKU-HUB-007,4,35.00,49.99,2023-01-08T15:10:00Z,STORE-E003,{discount: 5%, payment: card}
TXN008,CUST-108,SKU-HDD-008,1,100.00,129.99,2023-01-08T16:30:00Z,STORE-W004,{discount: 0%, payment: card}
TXN009,CUST-109,SKU-STAND-009,2,30.00,39.99,2023-01-09T10:05:00Z,STORE-N001,{discount: 20%, payment: card}
TXN010,CUST-110,SKU-TAB-010,1,240.00,299.99,2023-01-09T14:40:00Z,STORE-S002,{discount: 0%, payment: card}`

	rawFile := "raw_transactions.csv"
	err = os.WriteFile(rawFile, []byte(rawData), 0644)
	if err != nil {
		log.Fatal("Failed to create raw data:", err)
	}
	defer os.Remove(rawFile)

	// Example 1: Basic data extraction and cleaning
	fmt.Println("\n2. Basic ETL - Extract, Clean, and Standardize:")

	cleanQuery := fmt.Sprintf(`
		COPY (
			SELECT
				-- Clean and standardize transaction ID
				CAST(SUBSTRING(transaction_id, 4) AS INTEGER) as transaction_id,

				-- Clean and standardize customer ID
				CAST(SUBSTRING(customer_code, 6) AS INTEGER) as customer_id,

				-- Clean and standardize product ID
				CAST(SUBSTRING(product_sku, 5) AS INTEGER) as product_id,

				-- Extract product name from SKU
				CASE
					WHEN product_sku LIKE '%%LAPTOP%%' THEN 'Laptop'
					WHEN product_sku LIKE '%%MOUSE%%' THEN 'Mouse'
					WHEN product_sku LIKE '%%KB%%' THEN 'Keyboard'
					WHEN product_sku LIKE '%%MON%%' THEN 'Monitor'
					WHEN product_sku LIKE '%%HP%%' THEN 'Headphones'
					WHEN product_sku LIKE '%%WEB%%' THEN 'Webcam'
					WHEN product_sku LIKE '%%HUB%%' THEN 'USB Hub'
					WHEN product_sku LIKE '%%HDD%%' THEN 'External HDD'
					WHEN product_sku LIKE '%%STAND%%' THEN 'Laptop Stand'
					WHEN product_sku LIKE '%%TAB%%' THEN 'Graphics Tablet'
					ELSE 'Unknown'
				END as product_name,

				-- Standardize quantity
				quantity,

				-- Calculate standard fields
				unit_price,
				quantity * unit_price as gross_amount,

				-- Parse and standardize timestamp
				TRY_CAST(timestamp AS TIMESTAMP) as order_timestamp,
				CAST(timestamp AS DATE) as order_date,

				-- Extract store information
				SUBSTRING(store_id, 7, 1) as store_region,
				CASE
					WHEN store_id LIKE '%%N%%' THEN 'North'
					WHEN store_id LIKE '%%S%%' THEN 'South'
					WHEN store_id LIKE '%%E%%' THEN 'East'
					WHEN store_id LIKE '%%W%%' THEN 'West'
					ELSE 'Unknown'
				END as region_name
			FROM read_csv_auto('%s')
		) TO 'cleaned_transactions.csv' WITH (HEADER true)
	`, rawFile)

	_, err = db.Exec(cleanQuery)
	if err != nil {
		log.Fatal("Failed to clean data:", err)
	}
	defer os.Remove("cleaned_transactions.csv")

	fmt.Println("Data cleaned and exported to cleaned_transactions.csv")

	// Example 2: Data enrichment
	fmt.Println("\n3. Data Enrichment - Adding Calculated Fields:")

	enrichmentQuery := fmt.Sprintf(`
		COPY (
			SELECT
				transaction_id,
				customer_id,
				product_id,
				product_name,
				quantity,
				unit_price,
				gross_amount,
				order_timestamp,
				order_date,
				store_region,
				region_name,

				-- Calculate profit margin
				ROUND(unit_price * 0.15, 2) as estimated_profit,

				-- Categorize order size
				CASE
					WHEN gross_amount >= 500 THEN 'Large'
					WHEN gross_amount >= 100 THEN 'Medium'
					ELSE 'Small'
				END as order_category,

				-- Day of week analysis
				CASE EXTRACT(DOW FROM order_date)
					WHEN 0 THEN 'Sunday'
					WHEN 1 THEN 'Monday'
					WHEN 2 THEN 'Tuesday'
					WHEN 3 THEN 'Wednesday'
					WHEN 4 THEN 'Thursday'
					WHEN 5 THEN 'Friday'
					WHEN 6 THEN 'Saturday'
				END as order_day_of_week,

				-- Time period analysis
				CASE
					WHEN EXTRACT(HOUR FROM order_timestamp) >= 6 AND EXTRACT(HOUR FROM order_timestamp) < 12 THEN 'Morning'
					WHEN EXTRACT(HOUR FROM order_timestamp) >= 12 AND EXTRACT(HOUR FROM order_timestamp) < 18 THEN 'Afternoon'
					ELSE 'Evening'
				END as order_time_period,

				-- Customer lifetime value indicator
				CASE
					WHEN customer_id <= 105 THEN 'High Value'
					WHEN customer_id <= 108 THEN 'Medium Value'
					ELSE 'Low Value'
				END as customer_value_segment
			FROM read_csv_auto('cleaned_transactions.csv')
		) TO 'enriched_transactions.csv' WITH (HEADER true)
	`)

	_, err = db.Exec(enrichmentQuery)
	if err != nil {
		log.Fatal("Failed to enrich data:", err)
	}
	defer os.Remove("enriched_transactions.csv")

	fmt.Println("Data enriched and exported to enriched_transactions.csv")

	// Example 3: Data aggregation and summarization
	fmt.Println("\n4. Data Aggregation - Creating Summary Tables:")

	// Daily summary
	dailySummaryQuery := `
		COPY (
			SELECT
				order_date,
				COUNT(*) as total_transactions,
				COUNT(DISTINCT customer_id) as unique_customers,
				SUM(gross_amount) as daily_revenue,
				ROUND(AVG(gross_amount), 2) as avg_transaction_value,
				SUM(quantity) as total_units_sold,
				COUNT(DISTINCT product_id) as unique_products_sold,
				COUNT(CASE WHEN order_category = 'Large' THEN 1 END) as large_orders,
				COUNT(CASE WHEN order_category = 'Medium' THEN 1 END) as medium_orders,
				COUNT(CASE WHEN order_category = 'Small' THEN 1 END) as small_orders,
				ROUND(SUM(estimated_profit), 2) as daily_profit
			FROM read_csv_auto('enriched_transactions.csv')
			GROUP BY order_date
			ORDER BY order_date
		) TO 'daily_summary.csv' WITH (HEADER true)
	`

	_, err = db.Exec(dailySummaryQuery)
	if err != nil {
		log.Fatal("Failed to create daily summary:", err)
	}
	defer os.Remove("daily_summary.csv")

	fmt.Println("Daily summary created: daily_summary.csv")

	// Product performance summary
	productSummaryQuery := `
		COPY (
			SELECT
				product_id,
				product_name,
				COUNT(*) as times_ordered,
				SUM(quantity) as total_quantity_sold,
				SUM(gross_amount) as total_revenue,
				ROUND(AVG(unit_price), 2) as avg_unit_price,
				ROUND(AVG(gross_amount), 2) as avg_order_value,
				COUNT(DISTINCT customer_id) as unique_customers,
				ROUND(SUM(estimated_profit), 2) as total_estimated_profit,
				ROUND(total_revenue / total_quantity_sold, 2) as revenue_per_unit,
				ROUND(total_estimated_profit / total_quantity_sold, 2) as profit_per_unit
			FROM read_csv_auto('enriched_transactions.csv')
			GROUP BY product_id, product_name
			ORDER BY total_revenue DESC
		) TO 'product_performance.csv' WITH (HEADER true)
	`

	_, err = db.Exec(productSummaryQuery)
	if err != nil {
		log.Fatal("Failed to create product summary:", err)
	}
	defer os.Remove("product_performance.csv")

	fmt.Println("Product performance summary created: product_performance.csv")

	// Example 4: Data pivoting and reshaping
	fmt.Println("\n5. Data Pivoting - Converting Rows to Columns:")

	pivotQuery := `
		COPY (
			SELECT
				order_date,
				SUM(CASE WHEN region_name = 'North' THEN gross_amount ELSE 0 END) as north_revenue,
				SUM(CASE WHEN region_name = 'South' THEN gross_amount ELSE 0 END) as south_revenue,
				SUM(CASE WHEN region_name = 'East' THEN gross_amount ELSE 0 END) as east_revenue,
				SUM(CASE WHEN region_name = 'West' THEN gross_amount ELSE 0 END) as west_revenue,
				SUM(CASE WHEN order_time_period = 'Morning' THEN gross_amount ELSE 0 END) as morning_revenue,
				SUM(CASE WHEN order_time_period = 'Afternoon' THEN gross_amount ELSE 0 END) as afternoon_revenue,
				SUM(CASE WHEN order_time_period = 'Evening' THEN gross_amount ELSE 0 END) as evening_revenue,
				SUM(CASE WHEN customer_value_segment = 'High Value' THEN gross_amount ELSE 0 END) as high_value_revenue,
				SUM(CASE WHEN customer_value_segment = 'Medium Value' THEN gross_amount ELSE 0 END) as medium_value_revenue,
				SUM(CASE WHEN customer_value_segment = 'Low Value' THEN gross_amount ELSE 0 END) as low_value_revenue
			FROM read_csv_auto('enriched_transactions.csv')
			GROUP BY order_date
			ORDER BY order_date
		) TO 'pivot_revenue_by_dimensions.csv' WITH (HEADER true)
	`

	_, err = db.Exec(pivotQuery)
	if err != nil {
		log.Fatal("Failed to create pivot table:", err)
	}
	defer os.Remove("pivot_revenue_by_dimensions.csv")

	fmt.Println("Pivot table created: pivot_revenue_by_dimensions.csv")

	// Example 5: Data quality and validation
	fmt.Println("\n6. Data Quality Validation:")

	qualityQuery := `
		SELECT
			'Overall Statistics' as metric_type,
			COUNT(*) as total_records,
			COUNT(DISTINCT transaction_id) as unique_transactions,
			COUNT(DISTINCT customer_id) as unique_customers,
			COUNT(DISTINCT product_id) as unique_products,
			ROUND(AVG(gross_amount), 2) as avg_transaction_amount,
			ROUND(MIN(gross_amount), 2) as min_transaction,
			ROUND(MAX(gross_amount), 2) as max_transaction
		FROM read_csv_auto('enriched_transactions.csv')

		UNION ALL

		SELECT
			'Order Category Distribution' as metric_type,
			COUNT(CASE WHEN order_category = 'Large' THEN 1 END) as large_orders,
			COUNT(CASE WHEN order_category = 'Medium' THEN 1 END) as medium_orders,
			COUNT(CASE WHEN order_category = 'Small' THEN 1 END) as small_orders,
			ROUND(AVG(CASE WHEN order_category = 'Large' THEN gross_amount END), 2) as avg_large_amount,
			ROUND(MIN(CASE WHEN order_category = 'Large' THEN gross_amount END), 2) as min_large,
			ROUND(MAX(CASE WHEN order_category = 'Large' THEN gross_amount END), 2) as max_large
		FROM read_csv_auto('enriched_transactions.csv')

		UNION ALL

		SELECT
			'Revenue by Dimension' as metric_type,
			SUM(CASE WHEN region_name = 'North' THEN gross_amount ELSE 0 END) as north_total,
			SUM(CASE WHEN region_name = 'South' THEN gross_amount ELSE 0 END) as south_total,
			SUM(CASE WHEN region_name = 'East' THEN gross_amount ELSE 0 END) as east_total,
			SUM(CASE WHEN region_name = 'West' THEN gross_amount ELSE 0 END) as west_total,
			ROUND(AVG(CASE WHEN region_name = 'North' THEN gross_amount END), 2) as north_avg,
			ROUND(MIN(CASE WHEN region_name = 'North' THEN gross_amount END), 2) as north_min
		FROM read_csv_auto('enriched_transactions.csv')
	`

	rows, err := db.Query(qualityQuery)
	if err != nil {
		log.Fatal("Failed to query quality metrics:", err)
	}
	defer rows.Close()

	fmt.Println("Data Quality Metrics:")
	fmt.Printf(
		"%-25s | %-12s | %-15s | %-15s | %-15s | %-12s | %-12s\n",
		"Metric Type",
		"Total/Count",
		"Unique/Category 1",
		"Category 2",
		"Category 3",
		"Average",
		"Min/Max",
	)
	fmt.Println(strings.Repeat("-", 120))

	for rows.Next() {
		var metricType string
		var col1, col2, col3, col4, col5, col6 sql.NullFloat64

		err := rows.Scan(&metricType, &col1, &col2, &col3, &col4, &col5, &col6)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf(
			"%-25s | %-12.0f | %-15.0f | %-15.0f | %-15.0f | %-12.2f | %-12.2f\n",
			metricType,
			col1.Float64,
			col2.Float64,
			col3.Float64,
			col4.Float64,
			col5.Float64,
			col6.Float64,
		)
	}

	// Example 6: Complex business logic transformation
	fmt.Println("\n7. Complex Business Logic Transformation:")

	businessLogicQuery := `
		COPY (
			SELECT
				transaction_id,
				customer_id,
				product_id,
				product_name,
				quantity,
				unit_price,
				gross_amount,
				order_date,
				region_name,

				-- Apply complex discount rules
				CASE
					-- Volume discount
					WHEN quantity >= 5 THEN gross_amount * 0.95
					WHEN quantity >= 3 THEN gross_amount * 0.97
					-- Regional discount
					WHEN region_name = 'North' AND EXTRACT(DOW FROM order_date) IN (1,2) THEN gross_amount * 0.98
					WHEN region_name = 'South' AND EXTRACT(DOW FROM order_date) IN (6,0) THEN gross_amount * 0.99
					-- Customer loyalty discount
					WHEN customer_id <= 103 THEN gross_amount * 0.96
					ELSE gross_amount
				END as discounted_amount,

				-- Calculate net profit with business rules
				CASE
					WHEN product_name IN ('Laptop', 'Monitor', 'Graphics Tablet') THEN
						(gross_amount - (gross_amount * 0.15)) - (quantity * unit_price * 0.65)
					WHEN product_name IN ('Mouse', 'Keyboard', 'USB Hub') THEN
						(gross_amount - (gross_amount * 0.20)) - (quantity * unit_price * 0.70)
					ELSE
						(gross_amount - (gross_amount * 0.25)) - (quantity * unit_price * 0.75)
				END as calculated_profit,

				-- Commission calculation
				CASE
					WHEN gross_amount >= 500 THEN gross_amount * 0.05
					WHEN gross_amount >= 200 THEN gross_amount * 0.03
					ELSE gross_amount * 0.01
				END as sales_commission,

				-- Tax calculation
				CASE
					WHEN region_name = 'North' THEN gross_amount * 0.08
					WHEN region_name = 'South' THEN gross_amount * 0.07
					WHEN region_name = 'East' THEN gross_amount * 0.09
					WHEN region_name = 'West' THEN gross_amount * 0.06
					ELSE gross_amount * 0.075
				END as calculated_tax,

				-- Final net amount
				gross_amount -
				(CASE
					WHEN quantity >= 5 THEN gross_amount * 0.05
					WHEN quantity >= 3 THEN gross_amount * 0.03
					WHEN region_name = 'North' AND EXTRACT(DOW FROM order_date) IN (1,2) THEN gross_amount * 0.02
					WHEN region_name = 'South' AND EXTRACT(DOW FROM order_date) IN (6,0) THEN gross_amount * 0.01
					WHEN customer_id <= 103 THEN gross_amount * 0.04
					ELSE 0
				END) -
				(CASE
					WHEN region_name = 'North' THEN gross_amount * 0.08
					WHEN region_name = 'South' THEN gross_amount * 0.07
					WHEN region_name = 'East' THEN gross_amount * 0.09
					WHEN region_name = 'West' THEN gross_amount * 0.06
					ELSE gross_amount * 0.075
				END) as net_amount
			FROM read_csv_auto('enriched_transactions.csv')
		) TO 'business_transformed_data.csv' WITH (HEADER true)
	`

	_, err = db.Exec(businessLogicQuery)
	if err != nil {
		log.Fatal("Failed to apply business logic:", err)
	}
	defer os.Remove("business_transformed_data.csv")

	fmt.Println("Business logic transformations applied: business_transformed_data.csv")

	// Example 7: Final ETL summary report
	fmt.Println("\n8. ETL Process Summary Report:")

	summaryQuery := `
		SELECT
			'ETL Stage' as stage,
			'Source Data' as description,
			COUNT(*) as record_count,
			SUM(gross_amount) as total_gross,
			ROUND(AVG(gross_amount), 2) as avg_amount,
			NULL as net_amount,
			NULL as profit
		FROM read_csv_auto('cleaned_transactions.csv')

		UNION ALL

		SELECT
			'Enriched Data' as stage,
			'Added calculated fields' as description,
			COUNT(*) as record_count,
			SUM(gross_amount) as total_gross,
			ROUND(AVG(gross_amount), 2) as avg_amount,
			NULL as net_amount,
			NULL as profit
		FROM read_csv_auto('enriched_transactions.csv')

		UNION ALL

		SELECT
			'Business Transformed' as stage,
			'Applied discounts, taxes, commissions' as description,
			COUNT(*) as record_count,
			SUM(gross_amount) as total_gross,
			ROUND(AVG(gross_amount), 2) as avg_amount,
			ROUND(SUM(net_amount), 2) as net_amount,
			ROUND(SUM(calculated_profit), 2) as profit
		FROM read_csv_auto('business_transformed_data.csv')

		UNION ALL

		SELECT
			'Final Summary' as stage,
			'Revenue impact of transformations' as description,
			NULL as record_count,
			NULL as total_gross,
			NULL as avg_amount,
			ROUND(
				(SELECT SUM(net_amount) FROM read_csv_auto('business_transformed_data.csv')) -
				(SELECT SUM(gross_amount) FROM read_csv_auto('enriched_transactions.csv')), 2
			) as net_amount,
			ROUND(
				(SELECT SUM(calculated_profit) FROM read_csv_auto('business_transformed_data.csv')) -
				(SELECT SUM(estimated_profit) FROM read_csv_auto('enriched_transactions.csv')), 2
			) as profit
		FROM (SELECT 1) t
	`

	rows, err = db.Query(summaryQuery)
	if err != nil {
		log.Fatal("Failed to query ETL summary:", err)
	}
	defer rows.Close()

	fmt.Println("ETL Process Summary:")
	fmt.Printf("%-20s | %-30s | %-12s | %-12s | %-12s | %-12s | %-12s\n",
		"Stage", "Description", "Records", "Total Gross", "Avg Amount", "Net Amount", "Profit")
	fmt.Println(strings.Repeat("-", 130))

	for rows.Next() {
		var stage, description string
		var recordCount sql.NullInt64
		var totalGross, avgAmount, netAmount, profit sql.NullFloat64

		err := rows.Scan(
			&stage,
			&description,
			&recordCount,
			&totalGross,
			&avgAmount,
			&netAmount,
			&profit,
		)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf("%-20s | %-30s | %-12s | $%-11.2f | $%-11.2f | $%-11.2f | $%-11.2f\n",
			stage, description,
			displayNullInt(recordCount),
			displayNullFloat(totalGross),
			displayNullFloat(avgAmount),
			displayNullFloat(netAmount),
			displayNullFloat(profit))
	}

	fmt.Println("\n✓ CSV data transformation example completed successfully!")
	fmt.Println("\nTransformation pipeline completed:")
	fmt.Println("1. Raw data extraction and cleaning")
	fmt.Println("2. Data enrichment with calculated fields")
	fmt.Println("3. Aggregation and summarization")
	fmt.Println("4. Data pivoting and reshaping")
	fmt.Println("5. Data quality validation")
	fmt.Println("6. Complex business logic application")
	fmt.Println("7. Final ETL summary report")
}

// Helper functions for display
func displayNullInt(n sql.NullInt64) string {
	if n.Valid {
		return fmt.Sprintf("%d", n.Int64)
	}
	return "NULL"
}

func displayNullFloat(f sql.NullFloat64) string {
	if f.Valid {
		return fmt.Sprintf("%.2f", f.Float64)
	}
	return "NULL"
}

// Helper function to demonstrate ETL best practices
func demonstrateETLBestPractices() {
	fmt.Println("\nETL Best Practices:")
	fmt.Println("1. Extract:")
	fmt.Println("   - Validate source data availability")
	fmt.Println("   - Check data freshness and completeness")
	fmt.Println("   - Log extraction start/end times")
	fmt.Println("\n2. Transform:")
	fmt.Println("   - Apply transformations in logical order")
	fmt.Println("   - Validate data types and formats")
	fmt.Println("   - Handle NULL values explicitly")
	fmt.Println("   - Apply business rules consistently")
	fmt.Println("\n3. Load:")
	fmt.Println("   - Validate target schema compatibility")
	fmt.Println("   - Implement error handling and rollback")
	fmt.Println("   - Monitor load performance")
	fmt.Println("   - Verify row counts and data integrity")
}

// Helper function to show common transformation patterns
func showTransformationPatterns() {
	fmt.Println("\nCommon Data Transformation Patterns:")
	fmt.Println("1. String Manipulation:")
	fmt.Println("   - UPPER(), LOWER(), INITCAP()")
	fmt.Println("   - TRIM(), LTRIM(), RTRIM()")
	fmt.Println("   - SUBSTRING(), REPLACE(), REGEXP_REPLACE()")
	fmt.Println("\n2. Date/Time Transformations:")
	fmt.Println("   - CAST(date_string AS DATE)")
	fmt.Println("   - EXTRACT(YEAR/MONTH/DAY FROM date)")
	fmt.Println("   - DATE_TRUNC('month', date)")
	fmt.Println("   - DATE_DIFF('day', start_date, end_date)")
	fmt.Println("\n3. Numeric Transformations:")
	fmt.Println("   - ROUND(number, decimal_places)")
	fmt.Println("   - FLOOR(), CEIL(), ABS()")
	fmt.Println("   - Arithmetic operations (+, -, *, /)")
	fmt.Println("\n4. Conditional Transformations:")
	fmt.Println("   - CASE WHEN condition THEN result ELSE default END")
	fmt.Println("   - COALESCE(column, default_value)")
	fmt.Println("   - NULLIF(value1, value2)")
}

// Helper function for data quality checks
func showDataQualityChecks() {
	fmt.Println("\nData Quality Checks:")
	fmt.Println("1. Completeness: Check for NULL values")
	fmt.Println("2. Consistency: Validate data formats and ranges")
	fmt.Println("3. Accuracy: Cross-reference with source systems")
	fmt.Println("4. Uniqueness: Check for duplicate records")
	fmt.Println("5. Validity: Ensure values meet business rules")
	fmt.Println("6. Timeliness: Verify data freshness")
	fmt.Println("7. Integrity: Check referential integrity")
}

// Helper function to create transformation template
func createTransformationTemplate() string {
	return `
-- ETL Transformation Template

-- 1. Extract raw data
CREATE TABLE raw_data AS
SELECT * FROM read_csv_auto('source_file.csv');

-- 2. Clean and standardize
CREATE TABLE cleaned_data AS
SELECT
    TRIM(id) as id,
    UPPER(name) as name,
    TRY_CAST(date_string AS DATE) as order_date
FROM raw_data;

-- 3. Enrich with calculated fields
CREATE TABLE enriched_data AS
SELECT
    *,
    EXTRACT(YEAR FROM order_date) as order_year,
    CASE WHEN amount > 1000 THEN 'High' ELSE 'Low' END as value_category
FROM cleaned_data;

-- 4. Apply business logic
CREATE TABLE final_data AS
SELECT
    *,
    amount * 0.08 as tax,
    amount - (amount * 0.08) as net_amount
FROM enriched_data;

-- 5. Export results
COPY final_data TO 'output_file.csv' WITH (HEADER true);
`
}
