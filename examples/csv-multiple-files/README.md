# Multiple CSV Files Handling Example

This example demonstrates techniques for reading, combining, and analyzing data from multiple CSV files in dukdb-go, including handling different schemas and file patterns.

## Concept

Often data is distributed across multiple CSV files, either by time periods, regions, categories, or due to size limitations. This example shows how to efficiently work with such distributed datasets.

## Common Scenarios

1. **Time-based files**: Daily, monthly, or yearly data files
2. **Geographic regions**: Data split by country, state, or city
3. **Categories**: Different files for product categories
4. **Size limitations**: Large datasets split into chunks
5. **Different sources**: Data from multiple systems

## Basic Techniques

### UNION ALL
Combine multiple files with identical schemas:
```sql
SELECT * FROM read_csv_auto('file1.csv')
UNION ALL
SELECT * FROM read_csv_auto('file2.csv')
UNION ALL
SELECT * FROM read_csv_auto('file3.csv');
```

### UNION
Combine and remove duplicates:
```sql
SELECT * FROM read_csv_auto('file1.csv')
UNION
SELECT * FROM read_csv_auto('file2.csv');
```

### Add Source Tracking
Track which file each record came from:
```sql
SELECT 'file1.csv' as source_file, f1.* FROM read_csv_auto('file1.csv') f1
UNION ALL
SELECT 'file2.csv' as source_file, f2.* FROM read_csv_auto('file2.csv') f2;
```

## Examples in This Demo

1. **Create multiple files**: Generate monthly sales data
2. **UNION ALL**: Combine files while preserving duplicates
3. **Source tracking**: Add filename column to track origin
4. **Aggregation**: Calculate totals across all files
5. **Master data combination**: Create customer master from regions
6. **Join operations**: Combine sales with customer data
7. **Different schemas**: Handle files with different columns
8. **Glob patterns**: Dynamically read files matching patterns
9. **Summary statistics**: Calculate metrics across all files
10. **Export combined data**: Create single consolidated file

## Advanced Techniques

### Dynamic Query Building
```go
// Build UNION ALL query dynamically
files, _ := filepath.Glob("data/month_*.csv")
var parts []string
for _, file := range files {
    parts = append(parts, fmt.Sprintf(
        "SELECT * FROM read_csv_auto('%s')", file))
}
query := strings.Join(parts, "\nUNION ALL\n")
```

### Schema Unification
```sql
-- Handle different schemas
SELECT
    id,
    name,
    price,
    NULL as stock  -- Add missing column
FROM read_csv_auto('old_format.csv')
UNION ALL
SELECT * FROM read_csv_auto('new_format.csv');
```

### Partitioned Reading
```sql
-- Read only specific partitions
SELECT * FROM read_csv_auto('sales_2023_01.csv')
WHERE sale_date >= '2023-01-01' AND sale_date < '2023-02-01'
UNION ALL
SELECT * FROM read_csv_auto('sales_2023_02.csv')
WHERE sale_date >= '2023-02-01' AND sale_date < '2023-03-01';
```

## Performance Optimization

### Use Temporary Views
```sql
-- Create view for repeated queries
CREATE TEMPORARY VIEW all_sales AS
SELECT * FROM read_csv_auto('jan.csv')
UNION ALL
SELECT * FROM read_csv_auto('feb.csv');

-- Query multiple times
SELECT * FROM all_sales WHERE customer_id = 123;
```

### Parallel Processing
```sql
-- Process partitions in parallel (with multiple connections)
-- Connection 1:
SELECT * FROM read_csv_auto('sales_north.csv');

-- Connection 2:
SELECT * FROM read_csv_auto('sales_south.csv');
```

### Incremental Loading
```sql
-- Load only new files
SELECT MAX(last_updated) FROM existing_data;
-- Then load only files newer than this date
```

## Error Handling

### File Existence Check
```go
// Check if file exists before reading
if _, err := os.Stat(filename); os.IsNotExist(err) {
    log.Printf("File not found: %s", filename)
    continue
}
```

### Schema Validation
```sql
-- Validate schema before combining
DESCRIBE SELECT * FROM read_csv_auto('file1.csv');
DESCRIBE SELECT * FROM read_csv_auto('file2.csv');
```

### Graceful Degradation
```sql
-- Continue even if some files fail
SELECT * FROM read_csv_auto('file1.csv')
UNION ALL
SELECT * FROM read_csv_auto('file2.csv')  -- Skip if fails
UNION ALL
SELECT * FROM read_csv_auto('file3.csv');
```

## Common Patterns

### Time Series Data
```sql
-- Monthly aggregation
SELECT
    SUBSTRING(date_column, 1, 7) as month,
    COUNT(*) as record_count,
    SUM(amount) as total_amount
FROM (
    SELECT * FROM read_csv_auto('sales_2023_01.csv')
    UNION ALL
    SELECT * FROM read_csv_auto('sales_2023_02.csv')
    -- ... more months
) combined
GROUP BY SUBSTRING(date_column, 1, 7);
```

### Geographic Data
```sql
-- Add region identifier
SELECT 'north' as region, n.* FROM read_csv_auto('north.csv') n
UNION ALL
SELECT 'south' as region, s.* FROM read_csv_auto('south.csv') s
UNION ALL
SELECT 'east' as region, e.* FROM read_csv_auto('east.csv') e;
```

### Master Data Combination
```sql
-- Build master customer list
SELECT DISTINCT customer_id, customer_name, email
FROM (
    SELECT customer_id, customer_name, email FROM read_csv_auto('customers_north.csv')
    UNION
    SELECT customer_id, customer_name, email FROM read_csv_auto('customers_south.csv')
    UNION
    SELECT customer_id, customer_name, email FROM read_csv_auto('customers_east.csv')
) all_customers;
```

## Best Practices

1. **Use UNION ALL** when duplicates aren't a concern (faster)
2. **Add source tracking** for data lineage
3. **Create temporary views** for repeated queries
4. **Handle schema differences** explicitly
5. **Validate data types** across files
6. **Check file existence** before reading
7. **Use consistent naming** patterns
8. **Document file structure** and relationships
9. **Test with sample files** first
10. **Monitor memory usage** with large file sets

## Common Issues and Solutions

### Issue: Different Schemas
**Solution**: Explicitly select and align columns
```sql
SELECT id, name, price, NULL as stock FROM old_files
UNION ALL
SELECT id, name, price, stock FROM new_files;
```

### Issue: Encoding Problems
**Solution**: Specify encoding explicitly
```sql
SELECT * FROM read_csv_auto('file.csv', encoding := 'UTF-8');
```

### Issue: Memory Errors
**Solution**: Process in smaller batches
```sql
-- Process files one at a time
FOR file IN (SELECT filename FROM file_list)
    EXECUTE 'SELECT * FROM read_csv_auto(''' || file || ''')';
```

### Issue: Inconsistent Date Formats
**Solution**: Standardize dates during import
```sql
SELECT
    id,
    TRY_CAST(order_date AS DATE) as order_date,
    amount
FROM read_csv_auto('file.csv');
```

## Performance Tips

1. **Use UNION ALL** instead of UNION when possible
2. **Create indexes** on combined data for faster queries
3. **Process in parallel** using multiple connections
4. **Use sampling** for initial exploration
5. **Compress output** for large combined files
6. **Monitor system resources** during processing

## Summary

Working with multiple CSV files requires:
- **Consistent schemas** or explicit handling of differences
- **Efficient combination** techniques (UNION ALL)
- **Source tracking** for data lineage
- **Error handling** for missing or invalid files
- **Performance optimization** for large file sets
- **Quality validation** of combined results

With these techniques, you can efficiently work with distributed CSV datasets of any size."}