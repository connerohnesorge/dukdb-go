# JSON Transformation Example

This example demonstrates transforming and reshaping JSON data using SQL operations.

## Overview

DuckDB makes it easy to transform JSON data using standard SQL: filtering, aggregating, reshaping, and exporting in new formats.

## What You'll Learn

1. **Filtering**: Extract subsets of JSON data
2. **Computed Columns**: Add calculated fields
3. **Aggregation**: Summarize and group data
4. **Reshaping**: Change data structure
5. **Export**: Save transformed data in new formats

## Running the Example

```bash
cd examples/json-transformation
go run main.go
```

### Expected Output
```
=== JSON Transformation Example ===

1. Original Data:
  Laptop: $999.99 (Stock: 5, Category: Electronics)
  Mouse: $29.99 (Stock: 50, Category: Electronics)
  Desk: $199.99 (Stock: 12, Category: Furniture)
  Chair: $149.99 (Stock: 30, Category: Furniture)

2. Filtered data (Electronics only):
  Laptop: $999.99
  Mouse: $29.99

3. Data with computed columns:
  Laptop: $999.99 → $899.99 (Total Value: $4999.95)
  Desk: $199.99 → $179.99 (Total Value: $2399.88)
...

4. Aggregated data by category:
  Electronics: 2 items, Avg Price: $514.99, Total Stock: 55
  Furniture: 2 items, Avg Price: $174.99, Total Stock: 42

✓ JSON transformation example completed successfully!
```

## Transformation Types

### 1. Filtering (SELECT...WHERE)
Extract rows matching criteria:
```sql
SELECT * FROM read_json_auto('data.json') WHERE price > 100
```

### 2. Projection (SELECT columns)
Choose specific columns:
```sql
SELECT product, price, stock FROM read_json_auto('data.json')
```

### 3. Computed Columns (SELECT...AS)
Add calculated fields:
```sql
SELECT 
  product,
  price,
  price * 0.9 as discounted_price,
  price * stock as total_value
FROM read_json_auto('data.json')
```

### 4. Aggregation (GROUP BY)
Summarize data by groups:
```sql
SELECT 
  category,
  COUNT(*) as count,
  AVG(price) as avg_price
FROM read_json_auto('data.json')
GROUP BY category
```

### 5. Sorting (ORDER BY)
Arrange results:
```sql
SELECT * FROM read_json_auto('data.json')
ORDER BY price DESC
```

### 6. Limiting (LIMIT)
Restrict result count:
```sql
SELECT * FROM read_json_auto('data.json')
LIMIT 10
```

## Real-World Examples

### Sales Analysis
```sql
SELECT 
  category,
  SUM(quantity) as total_sold,
  SUM(price * quantity) as revenue,
  AVG(price) as avg_price
FROM read_json_auto('sales.json')
GROUP BY category
ORDER BY revenue DESC
```

### Data Quality Check
```sql
SELECT 
  COUNT(*) as total,
  COUNT(CASE WHEN price > 0 THEN 1 END) as valid_prices,
  COUNT(CASE WHEN stock >= 0 THEN 1 END) as valid_stock
FROM read_json_auto('products.json')
```

### Top Products
```sql
SELECT 
  product,
  price,
  stock,
  price * stock as inventory_value
FROM read_json_auto('products.json')
ORDER BY inventory_value DESC
LIMIT 5
```

### Category Summary
```sql
SELECT 
  category,
  COUNT(*) as products,
  MIN(price) as min_price,
  MAX(price) as max_price,
  AVG(price) as avg_price
FROM read_json_auto('products.json')
GROUP BY category
HAVING COUNT(*) > 5
ORDER BY products DESC
```

## Common Transformations

| Task | SQL |
|------|-----|
| Filter | `WHERE column = value` |
| Calculate | `SELECT col1 * col2 as result` |
| Aggregate | `GROUP BY, COUNT(*), AVG()` |
| Limit | `LIMIT 10, OFFSET 20` |
| Sort | `ORDER BY column DESC` |
| Rename | `AS new_name` |
| Conditional | `CASE WHEN ... THEN ... ELSE ... END` |
| Distinct | `SELECT DISTINCT category` |
| Join | `JOIN other_table ON` |

## Export Transformed Data

Save transformed results:

```go
db.Exec(`
    COPY (
        SELECT product, price, stock
        FROM read_json_auto('data.json')
        WHERE stock > 0
        ORDER BY price DESC
    )
    TO 'output.json'
    (FORMAT JSON)
`)
```

## Performance Tips

1. **Filter Early**: Use WHERE before aggregation
2. **Specific Columns**: Select only needed columns
3. **Appropriate Aggregates**: COUNT vs SUM vs AVG
4. **Proper Grouping**: GROUP BY only needed columns
5. **Indexing**: Consider if repeatedly querying

## Advanced Transformations

### Window Functions
```sql
SELECT 
  product,
  price,
  ROW_NUMBER() OVER (ORDER BY price DESC) as rank
FROM read_json_auto('data.json')
```

### Case Statements
```sql
SELECT 
  product,
  price,
  CASE 
    WHEN price > 500 THEN 'Premium'
    WHEN price > 100 THEN 'Standard'
    ELSE 'Budget'
  END as tier
FROM read_json_auto('data.json')
```

### String Functions
```sql
SELECT 
  UPPER(product) as product_upper,
  LENGTH(product) as name_length
FROM read_json_auto('data.json')
```

## Data Type Conversions

```sql
SELECT 
  CAST(price AS INTEGER) as whole_price,
  CAST(id AS VARCHAR) as id_string,
  CAST(created_at AS DATE) as creation_date
FROM read_json_auto('data.json')
```

## Next Steps

- See [json-querying](../json-querying) for more query examples
- See [json-write-basic](../json-write-basic) for exporting data
- See [json-read-basic](../json-read-basic) for basic reading

## Related Functions

- `read_json_auto()` - Read JSON data
- `SELECT` - Project and filter
- `GROUP BY` - Aggregate data
- `ORDER BY` - Sort results
- `HAVING` - Filter groups
- `LIMIT` - Limit results
- `CASE` - Conditional logic
- `CAST` - Type conversion

## SQL Aggregate Functions

- `COUNT(*)` - Count rows
- `SUM(column)` - Sum values
- `AVG(column)` - Average value
- `MIN(column)` - Minimum value
- `MAX(column)` - Maximum value
- `COUNT(DISTINCT column)` - Count unique values

## Documentation

https://duckdb.org/docs/sql/functions/list
https://duckdb.org/docs/data/json/overview
