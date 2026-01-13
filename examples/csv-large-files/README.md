# Large CSV Files Handling Example

This example demonstrates techniques for efficiently working with large CSV files in dukdb-go, including streaming, batching, sampling, and memory management.

## Concept

When working with large CSV files (millions of rows), it's important to use techniques that minimize memory usage and maximize performance. This example shows various strategies for handling such files efficiently.

## Key Techniques

1. **Sampling**: Read a subset of data for analysis
2. **Columnar Projection**: Read only needed columns
3. **Batch Processing**: Process data in chunks
4. **Streaming**: Process data without loading entirely into memory
5. **Parallel Processing**: Utilize multiple cores
6. **Compression**: Reduce file size for faster I/O
7. **Temporary Tables**: Store intermediate results
8. **Indexing**: Speed up repeated queries
9. **Progressive Loading**: Load data incrementally
10. **Memory Management**: Set appropriate memory limits

## Configuration Options

### Memory Limits
```sql
-- Set memory limit for DuckDB
SET memory_limit='1GB';
SET memory_limit='500MB';
```

### Sampling
```sql
-- Sample first 1000 rows for type detection
SELECT * FROM read_csv_auto('file.csv', sample_size=1000);
```

### Parallel Processing
```sql
-- DuckDB automatically parallelizes where possible
-- Use multiple connections for explicit parallelism
```

## Examples in This Demo

1. **Generate large CSV**: Create a 100k row test file
2. **Sampling**: Read subset for quick analysis
3. **Columnar reading**: Aggregate without loading all columns
4. **Batch processing**: Process in 10k row chunks
5. **Parallel processing**: Partition by date ranges
6. **Compression**: Export with gzip compression
7. **Memory-efficient aggregation**: Use temporary tables
8. **Indexing**: Create indexes for faster queries
9. **Progressive loading**: Load data incrementally
10. **Performance summary**: Compare all techniques

## Memory Management

### Set Memory Limits
```go
// Limit DuckDB memory usage
_, err := db.Exec("SET memory_limit='1GB'")
_, err := db.Exec("SET memory_limit='500MB'")
```

### Monitor Memory Usage
```go
// Check current memory usage
var memoryUsage string
db.QueryRow("SELECT current_setting('memory_limit')").Scan(&memoryUsage)
```

## Sampling Techniques

### Random Sampling
```sql
-- Sample 1% of data randomly
SELECT * FROM read_csv_auto('file.csv')
WHERE random() < 0.01;
```

### Stratified Sampling
```sql
-- Sample evenly from each category
SELECT * FROM read_csv_auto('file.csv')
WHERE id IN (
  SELECT id FROM (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY category) as rn
    FROM read_csv_auto('file.csv')
  ) WHERE rn <= 100
);
```

## Batch Processing

### Basic Batching
```go
batchSize := 10000
for offset := 0; offset < totalRows; offset += batchSize {
    query := fmt.Sprintf(`
        SELECT * FROM read_csv_auto('file.csv')
        LIMIT %d OFFSET %d
    `, batchSize, offset)
    // Process batch
}
```

### Cursor-Based Batching
```sql
-- Process based on ID ranges
SELECT * FROM read_csv_auto('file.csv')
WHERE id >= 0 AND id < 10000;

SELECT * FROM read_csv_auto('file.csv')
WHERE id >= 10000 AND id < 20000;
```

## Columnar Optimization

### Project Only Needed Columns
```sql
-- Good: Only read needed columns
SELECT customer_id, SUM(sale_amount)
FROM read_csv_auto('file.csv');

-- Bad: Reads all columns
SELECT * FROM read_csv_auto('file.csv');
```

### Early Aggregation
```sql
-- Aggregate early to reduce data size
SELECT
    DATE(sale_date) as sale_day,
    COUNT(*) as orders,
    SUM(sale_amount) as revenue
FROM read_csv_auto('file.csv')
GROUP BY DATE(sale_date);
```

## Parallel Processing

### Partition by Range
```go
// Partition by date ranges
partitions := []struct {
    name  string
    start string
    end   string
}{
    {"Q1", "2023-01-01", "2023-03-31"},
    {"Q2", "2023-04-01", "2023-06-30"},
    {"Q3", "2023-07-01", "2023-09-30"},
    {"Q4", "2023-10-01", "2023-12-31"},
}

// Process partitions in parallel
for _, partition := range partitions {
    go processPartition(partition)
}
```

### Hash Partitioning
```sql
-- Partition by hash of key
SELECT * FROM read_csv_auto('file.csv')
WHERE HASH(customer_id) % 4 = 0;

SELECT * FROM read_csv_auto('file.csv')
WHERE HASH(customer_id) % 4 = 1;
```

## Streaming Techniques

### Progressive Loading
```go
// Generate and process data incrementally
for chunk := 0; chunk < totalChunks; chunk++ {
    // Generate chunk
    chunkData := generateChunk(chunk)

    // Write to file
    appendToFile(filename, chunkData)

    // Process immediately
    processChunk(chunkData)
}
```

### Pipeline Processing
```sql
-- Chain operations without intermediate storage
CREATE TABLE results AS
SELECT
    customer_id,
    SUM(sale_amount) as total_spent
FROM read_csv_auto('file.csv')
GROUP BY customer_id
HAVING SUM(sale_amount) > 1000;
```

## Compression

### Export Compressed
```sql
-- Export with compression
COPY (SELECT * FROM large_table) TO 'output.csv.gz'
WITH (COMPRESSION 'gzip');
```

### Read Compressed Files
```sql
-- DuckDB can read compressed files automatically
SELECT * FROM read_csv_auto('file.csv.gz');
```

## Temporary Tables

### Store Intermediate Results
```sql
-- Create temporary table for repeated use
CREATE TEMPORARY TABLE monthly_sales AS
SELECT
    DATE_TRUNC('month', sale_date) as month,
    COUNT(*) as orders,
    SUM(sale_amount) as revenue
FROM read_csv_auto('file.csv')
GROUP BY DATE_TRUNC('month', sale_date);

-- Query multiple times
SELECT * FROM monthly_sales WHERE month >= '2023-01-01';
```

## Indexing

### Create Indexes
```sql
-- Create index on frequently queried columns
CREATE INDEX idx_customer_id ON sales_data(customer_id);
CREATE INDEX idx_sale_date ON sales_data(sale_date);
```

### Use Indexes
```sql
-- Query will use index automatically
SELECT * FROM sales_data
WHERE customer_id = 12345;
```

## Performance Metrics

### Track Performance
```go
startTime := time.Now()
// ... operation ...
duration := time.Since(startTime)
fmt.Printf("Operation took: %v\n", duration)
```

### Compare Techniques
```
Dataset size: 100000 rows (15.23 MB)
Sampling: 2.1s
Aggregation: 0.8s
Batch processing: 12.3s
Parallel processing: 3.2s
Compression: 1.5s
Memory aggregation: 0.5s
Indexed query: 0.01s
Progressive loading: 8.7s
```

## Error Handling

### Handle Memory Errors
```go
// Catch memory errors and adjust strategy
if err != nil && strings.Contains(err.Error(), "out of memory") {
    // Fall back to batch processing
    processInBatches(filename)
}
```

### Handle File Errors
```go
// Check file existence and permissions
if _, err := os.Stat(filename); os.IsNotExist(err) {
    log.Fatal("File not found:", filename)
}
```

## Best Practices

1. **Start with sampling** to understand data characteristics
2. **Use columnar projection** to read only needed data
3. **Process in batches** to control memory usage
4. **Aggregate early** to reduce data volume
5. **Use temporary tables** for complex multi-step operations
6. **Create indexes** for repeated queries
7. **Monitor memory usage** and adjust limits
8. **Compress large exports** to save space
9. **Test with smaller samples** before full processing
10. **Document performance** for future reference

## Common Pitfalls

1. **Loading entire files** into memory
2. **Reading all columns** when only a few are needed
3. **Processing row by row** without batching
4. **Not using indexes** for repeated queries
5. **Ignoring memory limits** leading to crashes

## Summary

Efficient large CSV handling requires:
- **Memory awareness**: Set limits and monitor usage
- **Strategic sampling**: Understand data before full processing
- **Batch processing**: Control memory usage
- **Parallel execution**: Utilize multiple cores
- **Early aggregation**: Reduce data volume
- **Proper indexing**: Speed up queries
- **Compression**: Save space and I/O time

With these techniques, you can efficiently process CSV files with millions of rows on standard hardware."}