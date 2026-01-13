# CSV Data Transformation Example

This example demonstrates comprehensive ETL (Extract, Transform, Load) operations on CSV files using dukdb-go, including data cleaning, enrichment, aggregation, and complex business logic application.

## Concept

Data transformation is the process of converting data from one format or structure into another. This is essential for preparing raw operational data for analysis, reporting, and decision-making.

## ETL Process Overview

1. **Extract**: Read data from source CSV files
2. **Transform**: Clean, enrich, and apply business logic
3. **Load**: Save transformed data to new CSV files

## Key Transformation Types

### 1. Data Cleaning
- Remove inconsistencies
- Standardize formats
- Handle NULL values
- Validate data types

### 2. Data Enrichment
- Add calculated fields
- Derive new attributes
- Combine data sources
- Add metadata

### 3. Data Aggregation
- Summarize by dimensions
- Create roll-up tables
- Calculate metrics
- Generate reports

### 4. Data Reshaping
- Pivot/unpivot data
- Transpose rows/columns
- Normalize/denormalize
- Restructure for analysis

## Examples in This Demo

1. **Basic ETL**: Extract, clean, and standardize raw data
2. **Data Enrichment**: Add calculated and derived fields
3. **Aggregation**: Create summary tables
4. **Pivoting**: Convert rows to columns
5. **Quality Validation**: Check data integrity
6. **Business Logic**: Apply complex transformation rules
7. **ETL Summary**: Track transformation impact

## Step-by-Step Transformation

### 1. Extract and Clean
```sql
-- Extract relevant fields and clean
SELECT
    CAST(SUBSTRING(transaction_id, 4) AS INTEGER) as transaction_id,
    CAST(SUBSTRING(customer_code, 6) AS INTEGER) as customer_id,
    TRIM(product_name) as product_name,
    TRY_CAST(date_string AS DATE) as order_date
FROM read_csv_auto('raw_data.csv');
```

### 2. Data Standardization
```sql
-- Standardize formats
SELECT
    UPPER(region_code) as region,
    INITCAP(customer_name) as customer_name,
    REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as clean_phone,
    CASE
        WHEN amount >= 1000 THEN 'High Value'
        WHEN amount >= 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END as value_segment
FROM cleaned_data;
```

### 3. Add Calculated Fields
```sql
-- Enrich with calculations
SELECT
    *,
    quantity * unit_price as gross_amount,
    gross_amount * 0.08 as tax_amount,
    gross_amount - tax_amount as net_amount,
    EXTRACT(YEAR FROM order_date) as order_year,
    EXTRACT(MONTH FROM order_date) as order_month,
    CASE EXTRACT(DOW FROM order_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        -- ... more days
    END as order_day
FROM standardized_data;
```

### 4. Apply Business Rules
```sql
-- Complex business logic
SELECT
    *,
    CASE
        -- Volume discounts
        WHEN quantity >= 10 THEN gross_amount * 0.9
        WHEN quantity >= 5 THEN gross_amount * 0.95
        -- Regional pricing
        WHEN region = 'North' AND order_day = 'Monday' THEN gross_amount * 0.98
        -- Customer loyalty
        WHEN customer_id IN (SELECT customer_id FROM loyal_customers) THEN gross_amount * 0.97
        ELSE gross_amount
    END as discounted_amount,
    -- Commission calculation
    CASE
        WHEN gross_amount >= 1000 THEN gross_amount * 0.05
        WHEN gross_amount >= 500 THEN gross_amount * 0.03
        ELSE gross_amount * 0.01
    END as commission
FROM enriched_data;
```

### 5. Aggregate and Summarize
```sql
-- Create summary tables
CREATE TABLE daily_summary AS
SELECT
    order_date,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(net_amount) as daily_revenue,
    AVG(net_amount) as avg_transaction_value,
    SUM(quantity) as total_units
FROM business_data
GROUP BY order_date;

CREATE TABLE product_summary AS
SELECT
    product_id,
    product_name,
    COUNT(*) as times_ordered,
    SUM(quantity) as total_quantity,
    SUM(net_amount) as total_revenue,
    AVG(net_amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM business_data
GROUP BY product_id, product_name;
```

### 6. Pivot and Reshape
```sql
-- Pivot by region
SELECT
    order_date,
    SUM(CASE WHEN region = 'North' THEN net_amount ELSE 0 END) as north_revenue,
    SUM(CASE WHEN region = 'South' THEN net_amount ELSE 0 END) as south_revenue,
    SUM(CASE WHEN region = 'East' THEN net_amount ELSE 0 END) as east_revenue,
    SUM(CASE WHEN region = 'West' THEN net_amount ELSE 0 END) as west_revenue
FROM business_data
GROUP BY order_date;

-- Pivot by time period
SELECT
    order_date,
    SUM(CASE WHEN order_period = 'Morning' THEN net_amount ELSE 0 END) as morning_revenue,
    SUM(CASE WHEN order_period = 'Afternoon' THEN net_amount ELSE 0 END) as afternoon_revenue,
    SUM(CASE WHEN order_period = 'Evening' THEN net_amount ELSE 0 END) as evening_revenue
FROM business_data
GROUP BY order_date;
```

## Advanced Transformations

### 1. Data Type Conversions
```sql
-- Safe type conversions
SELECT
    TRY_CAST(string_number AS INTEGER) as safe_integer,
    CAST(valid_date AS DATE) as order_date,
    CASE
        WHEN string_boolean = 'true' THEN true
        WHEN string_boolean = 'false' THEN false
        ELSE NULL
    END as boolean_value
FROM raw_data;
```

### 2. String Manipulations
```sql
-- Complex string transformations
SELECT
    REGEXP_REPLACE(email, '[A-Z]', LOWER(email), 'g') as clean_email,
    REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as digits_only,
    CONCAT(first_name, ' ', last_name) as full_name,
    SUBSTRING(description, 1, 50) || '...' as short_description
FROM customer_data;
```

### 3. Date/Time Transformations
```sql
-- Date calculations
SELECT
    order_date,
    DATE_TRUNC('month', order_date) as order_month,
    DATE_TRUNC('week', order_date) as order_week,
    DATE_DIFF('day', order_date, CURRENT_DATE) as days_ago,
    EXTRACT(YEAR FROM order_date) as order_year,
    EXTRACT(QUARTER FROM order_date) as order_quarter,
    CASE
        WHEN EXTRACT(MONTH FROM order_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM order_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM order_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END as order_season
FROM orders;
```

### 4. Lookup Transformations
```sql
-- Add descriptions from lookup tables
SELECT
    t.*,
    l.category_name,
    l.category_description,
    l.is_active
FROM transactions t
LEFT JOIN category_lookup l ON t.category_code = l.category_code;
```

## Data Quality Validation

### 1. Completeness Checks
```sql
-- Check for missing values
SELECT
    'customer_id' as column_name,
    COUNT(*) FILTER (WHERE customer_id IS NULL) as null_count,
    COUNT(*) FILTER (WHERE customer_id = '') as empty_count,
    COUNT(*) FILTER (WHERE customer_id IS NOT NULL AND customer_id != '') as valid_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE customer_id IS NOT NULL AND customer_id != '') / COUNT(*), 2) as completeness_pct
FROM transformed_data;
```

### 2. Consistency Checks
```sql
-- Check data ranges
SELECT
    MIN(order_date) as min_date,
    MAX(order_date) as max_date,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    AVG(amount) as avg_amount
FROM transformed_data;
```

### 3. Business Rule Validation
```sql
-- Validate business rules
SELECT
    COUNT(*) FILTER (WHERE quantity <= 0) as invalid_quantity,
    COUNT(*) FILTER (WHERE unit_price <= 0) as invalid_price,
    COUNT(*) FILTER (WHERE order_date > CURRENT_DATE) as future_dates,
    COUNT(*) FILTER (WHERE net_amount != gross_amount - tax_amount) as calculation_errors
FROM transformed_data;
```

## Performance Optimization

### 1. Batch Processing
```sql
-- Process in batches for large datasets
FOR batch_start IN (SELECT generate_series(0, 1000000, 10000))
    INSERT INTO transformed_data
    SELECT transform_function(*)
    FROM raw_data
    WHERE id >= batch_start AND id < batch_start + 10000;
```

### 2. Parallel Processing
```sql
-- Process partitions in parallel
-- Partition 1: Process customers A-C
SELECT transform_function(*)
FROM raw_data
WHERE customer_name BETWEEN 'A' AND 'C';

-- Partition 2: Process customers D-F
SELECT transform_function(*)
FROM raw_data
WHERE customer_name BETWEEN 'D' AND 'F';
```

### 3. Incremental Processing
```sql
-- Process only new/changed records
INSERT INTO transformed_data
SELECT transform_function(*)
FROM raw_data
WHERE last_updated > (SELECT MAX(processed_date) FROM etl_log);
```

## Error Handling

### 1. Handle Conversion Errors
```sql
-- Safe conversions with error handling
SELECT
    CASE
        WHEN string_number ~ '^[0-9]+$' THEN CAST(string_number AS INTEGER)
        ELSE NULL
    END as safe_integer,
    TRY_CAST(date_string AS DATE) as safe_date
FROM raw_data;
```

### 2. Data Quality Thresholds
```sql
-- Stop processing if quality is too low
SELECT
    CASE
        WHEN error_rate > 0.05 THEN
            RAISE_ERROR('Data quality too low: ' || error_rate || '% errors')
        ELSE 'Continue processing'
    END as quality_check
FROM (
    SELECT 100.0 * COUNT(*) FILTER (WHERE validation_error) / COUNT(*) as error_rate
    FROM quality_checks
) t;
```

## Best Practices

1. **Document all transformations** with clear comments
2. **Test with sample data** before full processing
3. **Validate results** at each transformation step
4. **Use consistent naming** conventions
5. **Handle NULL values** explicitly
6. **Preserve original data** in separate columns
7. **Log transformation statistics** for monitoring
8. **Implement rollback procedures** for errors
9. **Use transactions** for atomic operations
10. **Monitor performance** and optimize bottlenecks

## Common Patterns

### 1. Slowly Changing Dimensions (SCD)
```sql
-- Track historical changes
SELECT
    customer_id,
    customer_name,
    customer_status,
    effective_date,
    end_date,
    CASE
        WHEN end_date IS NULL THEN 'Current'
        ELSE 'Historical'
    END as record_status
FROM customer_scd;
```

### 2. Surrogate Key Generation
```sql
-- Add surrogate keys
SELECT
    ROW_NUMBER() OVER (ORDER BY transaction_date, transaction_id) as surrogate_key,
    transaction_id as natural_key,
    -- other fields
FROM transactions;
```

### 3. Data Deduplication
```sql
-- Remove duplicates
SELECT DISTINCT
    customer_id,
    email,
    phone,
    MAX(last_updated) as last_updated
FROM customer_data
GROUP BY customer_id, email, phone;
```

## Summary

Data transformation involves:
- **Cleaning**: Removing inconsistencies and errors
- **Standardizing**: Converting to consistent formats
- **Enriching**: Adding calculated and derived fields
- **Validating**: Ensuring data quality
- **Aggregating**: Creating summary views
- **Reshaping**: Restructuring for analysis
- **Exporting**: Saving transformed results

With these techniques, you can build robust ETL pipelines for any CSV dataset."} "file_path"/home/connerohnesorge/Documents/001Repos/dukdb-go/examples/csv-transformation/README.md