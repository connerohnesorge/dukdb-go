# CSV Data Analysis Example

This example demonstrates comprehensive data analysis techniques on CSV files using dukdb-go, including descriptive statistics, trend analysis, segmentation, and advanced analytics.

## Concept

CSV files often contain valuable business data that needs to be analyzed to extract insights. This example shows how to perform various types of analysis using SQL queries on CSV data.

## Types of Analysis

1. **Descriptive Statistics**: Basic metrics and summaries
2. **Diagnostic Analysis**: Understanding patterns and relationships
3. **Trend Analysis**: Time-based patterns
4. **Segmentation**: Grouping and categorizing data
5. **Comparative Analysis**: Comparing groups or periods
6. **Predictive Indicators**: Leading indicators and forecasts

## Key Analysis Techniques

### 1. Descriptive Statistics
```sql
-- Basic metrics
SELECT
    COUNT(*) as total_records,
    SUM(revenue) as total_revenue,
    AVG(revenue) as avg_revenue,
    MIN(revenue) as min_revenue,
    MAX(revenue) as max_revenue,
    STDDEV(revenue) as stddev_revenue
FROM sales_data;
```

### 2. Grouping and Aggregation
```sql
-- Group by categories
SELECT
    category,
    COUNT(*) as count,
    SUM(revenue) as total_revenue,
    AVG(revenue) as avg_revenue
FROM sales_data
GROUP BY category
ORDER BY total_revenue DESC;
```

### 3. Time Series Analysis
```sql
-- Daily trends
SELECT
    order_date,
    COUNT(*) as orders,
    SUM(revenue) as daily_revenue
FROM sales_data
GROUP BY order_date
ORDER BY order_date;
```

### 4. Window Functions
```sql
-- Running totals
SELECT
    order_date,
    revenue,
    SUM(revenue) OVER (ORDER BY order_date) as cumulative_revenue
FROM sales_data;
```

## Examples in This Demo

1. **Basic Descriptive Statistics**: Overall dataset metrics
2. **Category Analysis**: Performance by product category
3. **Regional Analysis**: Sales performance by region
4. **Sales Rep Analysis**: Individual performance metrics
5. **Time Series Analysis**: Daily sales trends
6. **Product Performance**: Product-level analysis
7. **Customer Analysis**: Customer behavior patterns
8. **Cross-selling Analysis**: Product combination patterns
9. **RFM Analysis**: Customer segmentation
10. **Export Results**: Save analysis outputs

## Detailed Analysis Examples

### 1. Revenue Analysis by Category
```sql
SELECT
    category,
    COUNT(*) as order_count,
    SUM(quantity * unit_price) as category_revenue,
    ROUND(AVG(quantity * unit_price), 2) as avg_order_value,
    ROUND(category_revenue * 100.0 / SUM(quantity * unit_price) OVER (), 2) as revenue_percentage
FROM sales_data
GROUP BY category
ORDER BY category_revenue DESC;
```

### 2. Regional Performance Comparison
```sql
SELECT
    region,
    COUNT(*) as orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(quantity * unit_price) as revenue,
    ROUND(AVG(quantity * unit_price), 2) as avg_order_value,
    ROUND(revenue / COUNT(DISTINCT customer_id), 2) as revenue_per_customer
FROM sales_data
GROUP BY region
ORDER BY revenue DESC;
```

### 3. Sales Rep Performance
```sql
SELECT
    sales_rep,
    COUNT(*) as orders,
    SUM(quantity * unit_price) as revenue,
    ROUND(AVG(quantity * unit_price), 2) as avg_order_value,
    SUM(quantity) as total_units,
    ROUND(SUM(quantity * unit_price) / SUM(quantity), 2) as avg_unit_price
FROM sales_data
GROUP BY sales_rep
ORDER BY revenue DESC;
```

### 4. Time Series Trend Analysis
```sql
SELECT
    order_date,
    COUNT(*) as daily_orders,
    SUM(quantity) as daily_units,
    SUM(quantity * unit_price) as daily_revenue,
    ROUND(AVG(quantity * unit_price), 2) as avg_order_value
FROM sales_data
GROUP BY order_date
ORDER BY order_date;
```

### 5. Product Performance Ranking
```sql
SELECT
    product_id,
    product_name,
    category,
    COUNT(*) as times_ordered,
    SUM(quantity) as total_quantity,
    SUM(quantity * unit_price) as total_revenue,
    ROUND(AVG(unit_price), 2) as avg_price
FROM sales_data
GROUP BY product_id, product_name, category
ORDER BY total_revenue DESC;
```

### 6. Customer Behavior Analysis
```sql
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(quantity) as total_units,
    SUM(quantity * unit_price) as total_spent,
    ROUND(AVG(quantity * unit_price), 2) as avg_order_value,
    COUNT(DISTINCT product_id) as unique_products,
    COUNT(DISTINCT category) as unique_categories
FROM sales_data
GROUP BY customer_id
ORDER BY total_spent DESC;
```

### 7. Cross-Selling Analysis
```sql
SELECT
    a.category as category_a,
    b.category as category_b,
    COUNT(*) as combination_count,
    COUNT(DISTINCT a.customer_id) as unique_customers
FROM sales_data a
JOIN sales_data b ON a.customer_id = b.customer_id AND a.order_id < b.order_id
WHERE a.category != b.category
GROUP BY a.category, b.category
ORDER BY combination_count DESC;
```

### 8. RFM Customer Segmentation
```sql
WITH customer_metrics AS (
    SELECT
        customer_id,
        MAX(order_date) as last_order_date,
        COUNT(*) as frequency,
        SUM(quantity * unit_price) as monetary
    FROM sales_data
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT
        customer_id,
        DATE_DIFF('day', last_order_date, CURRENT_DATE) as recency,
        frequency,
        monetary,
        CASE
            WHEN DATE_DIFF('day', last_order_date, CURRENT_DATE) <= 7 THEN 5
            WHEN DATE_DIFF('day', last_order_date, CURRENT_DATE) <= 14 THEN 4
            WHEN DATE_DIFF('day', last_order_date, CURRENT_DATE) <= 30 THEN 3
            WHEN DATE_DIFF('day', last_order_date, CURRENT_DATE) <= 60 THEN 2
            ELSE 1
        END as r_score,
        CASE
            WHEN frequency >= 4 THEN 5
            WHEN frequency >= 3 THEN 4
            WHEN frequency >= 2 THEN 3
            WHEN frequency >= 1 THEN 2
            ELSE 1
        END as f_score,
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
ORDER BY monetary DESC;
```

## Advanced Analytics

### 1. Cohort Analysis
```sql
-- Customer retention by acquisition month
WITH cohorts AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) as acquisition_month
    FROM sales_data
    GROUP BY customer_id
),
activity AS (
    SELECT
        c.customer_id,
        c.acquisition_month,
        DATE_TRUNC('month', s.order_date) as activity_month
    FROM cohorts c
    JOIN sales_data s ON c.customer_id = s.customer_id
    GROUP BY c.customer_id, c.acquisition_month, DATE_TRUNC('month', s.order_date)
)
SELECT
    acquisition_month,
    DATE_DIFF('month', acquisition_month, activity_month) as months_since_acquisition,
    COUNT(DISTINCT customer_id) as active_customers
FROM activity
GROUP BY acquisition_month, months_since_acquisition
ORDER BY acquisition_month, months_since_acquisition;
```

### 2. Moving Averages
```sql
-- 7-day moving average
SELECT
    order_date,
    daily_revenue,
    AVG(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7day
FROM daily_revenue;
```

### 3. Percentile Analysis
```sql
-- Revenue percentiles
SELECT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY revenue) as p25,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue) as median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY revenue) as p75,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY revenue) as p90,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY revenue) as p95
FROM sales_data;
```

## Performance Optimization

### 1. Use Appropriate Indexes
```sql
-- Create indexes on frequently queried columns
CREATE INDEX idx_order_date ON sales_data(order_date);
CREATE INDEX idx_customer_id ON sales_data(customer_id);
CREATE INDEX idx_category ON sales_data(category);
```

### 2. Pre-aggregate Data
```sql
-- Create summary tables for faster queries
CREATE TABLE daily_summary AS
SELECT
    order_date,
    COUNT(*) as orders,
    SUM(revenue) as revenue
FROM sales_data
GROUP BY order_date;
```

### 3. Sampling for Large Datasets
```sql
-- Use sampling for exploratory analysis
SELECT * FROM sales_data
USING SAMPLE 10%;  -- 10% sample
```

## Exporting Results

### Export to CSV
```sql
COPY (
    SELECT
        category,
        COUNT(*) as order_count,
        SUM(revenue) as category_revenue
    FROM sales_data
    GROUP BY category
    ORDER BY category_revenue DESC
) TO 'category_analysis.csv' WITH (HEADER true);
```

### Export to JSON
```sql
COPY (
    SELECT
        customer_id,
        SUM(revenue) as total_spent,
        COUNT(*) as order_count
    FROM sales_data
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 100
) TO 'top_customers.json';
```

## Visualization Suggestions

1. **Bar Charts**: Category revenue comparison
2. **Line Charts**: Daily/monthly sales trends
3. **Pie Charts**: Regional revenue distribution
4. **Scatter Plots**: Order value vs quantity
5. **Heatmaps**: Sales rep performance by region
6. **Box Plots**: Order value distribution by category
7. **Funnel Charts**: Customer journey stages

## Best Practices

1. **Start with descriptive statistics** to understand the data
2. **Use appropriate aggregations** for the analysis goal
3. **Handle NULL values** explicitly in calculations
4. **Use window functions** for advanced analytics
5. **Create reusable views** for common analyses
6. **Document analysis logic** for reproducibility
7. **Validate results** with known benchmarks
8. **Consider performance** for large datasets
9. **Export key results** for reporting
10. **Automate repetitive analyses**

## Further Analysis Ideas

1. **Cohort Analysis**: Customer retention over time
2. **Market Basket Analysis**: Product associations
3. **Predictive Analytics**: Sales forecasting
4. **Anomaly Detection**: Unusual sales patterns
5. **Geographic Analysis**: Sales by location
6. **Price Elasticity**: Demand vs price changes
7. **Seasonal Analysis**: Monthly/quarterly patterns
8. **Customer Lifetime Value**: Long-term value
9. **Attribution Analysis**: Channel performance
10. **What-if Analysis**: Scenario modeling

## Summary

CSV data analysis involves:
- **Descriptive statistics** for data understanding
- **Diagnostic analysis** for pattern identification
- **Segmentation** for targeted insights
- **Trend analysis** for time-based patterns
- **Comparative analysis** for performance evaluation
- **Advanced analytics** for deeper insights
- **Result export** for sharing and reporting

With these techniques, you can extract valuable insights from any CSV dataset."} "file_path"/home/connerohnesorge/Documents/001Repos/dukdb-go/examples/csv-analysis/README.md