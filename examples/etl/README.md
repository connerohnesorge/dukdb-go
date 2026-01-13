# ETL (Extract, Transform, Load) Examples

This example demonstrates a complete ETL pipeline using dukdb-go, from data extraction through transformation to loading into a data warehouse schema.

## Features Demonstrated

- Extracting data from CSV files with `read_csv_auto()`
- Data cleaning and transformation with SQL
- Adding derived columns and calculations
- Loading into fact and dimension tables
- Creating aggregate summaries

## Running the Example

```bash
go run main.go
```

## ETL Pipeline

### 1. Extract
Read raw data from source systems (CSV files):
```sql
CREATE TABLE staging_sales AS
SELECT * FROM read_csv_auto('sales.csv')
```

### 2. Transform
Clean and enrich the data:
```sql
CREATE TABLE transformed_sales AS
SELECT 
    sale_id,
    amount,
    sale_date,
    region,
    CASE 
        WHEN amount > 200 THEN 'High'
        WHEN amount > 100 THEN 'Medium'
        ELSE 'Low'
    END as value_category
FROM staging_sales
```

### 3. Load
Load into final fact table:
```sql
INSERT INTO fact_sales 
SELECT * FROM transformed_sales
```

### 4. Aggregate
Create summary tables:
```sql
CREATE TABLE daily_sales_summary AS
SELECT 
    sale_date,
    region,
    COUNT(*) as total_sales,
    SUM(amount) as total_revenue
FROM fact_sales
GROUP BY sale_date, region
```

## Best Practices

- Use staging tables for raw data
- Apply transformations in SQL for performance
- Track ETL metadata (load timestamps)
- Create aggregate tables for common queries
- Handle errors and data quality issues
