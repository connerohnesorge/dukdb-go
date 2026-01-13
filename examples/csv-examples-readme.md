# CSV Examples for dukdb-go

This directory contains 10 comprehensive examples demonstrating different aspects of working with CSV files in dukdb-go, from basic reading to advanced ETL transformations.

## Examples Overview

### 1. [csv-read-basic](csv-read-basic/)
Basic CSV reading with `read_csv()` function. Learn how to read CSV files directly with SQL queries and the Go API.

**Key concepts:**
- Direct CSV reading without importing
- SQL queries on CSV data
- Basic filtering and aggregation

### 2. [csv-read-options](csv-read-options/)
CSV reading with various options like custom delimiters, headers, null strings, and more.

**Key concepts:**
- Custom delimiters (pipe, tab, semicolon)
- Header control
- Null value handling
- Date format specification

### 3. [csv-read-auto](csv-read-auto/)
Using `read_csv_auto()` for automatic format detection including delimiters, data types, and date formats.

**Key concepts:**
- Automatic delimiter detection
- Type inference
- Date format recognition
- When to use auto vs manual

### 4. [csv-write-basic](csv-write-basic/)
Basic CSV writing using `COPY TO` statement to export data from tables or query results.

**Key concepts:**
- Export entire tables
- Export query results
- Go API with parameters
- Verification of exports

### 5. [csv-write-options](csv-write-options/)
CSV writing with advanced options like custom delimiters, quoting, encoding, and formatting.

**Key concepts:**
- Custom delimiters and quotes
- Header control
- Null representation
- Character encoding
- Date formatting

### 6. [csv-large-files](csv-large-files/)
Efficient techniques for handling large CSV files including sampling, batching, and streaming.

**Key concepts:**
- Memory management
- Sampling for analysis
- Batch processing
- Parallel processing
- Compression

### 7. [csv-data-cleaning](csv-data-cleaning/)
Data cleaning techniques for fixing inconsistencies, validating formats, and standardizing data.

**Key concepts:**
- Whitespace cleaning
- Case standardization
- Email validation
- Phone number formatting
- Data type validation

### 8. [csv-multiple-files](csv-multiple-files/)
Working with multiple CSV files including combining, joining, and aggregating across files.

**Key concepts:**
- UNION and UNION ALL
- Source tracking
- Schema unification
- Glob patterns
- Dynamic query building

### 9. [csv-analysis](csv-analysis/)
Comprehensive data analysis techniques including statistics, trends, segmentation, and RFM analysis.

**Key concepts:**
- Descriptive statistics
- Category analysis
- Time series analysis
- Customer segmentation
- Cross-selling analysis

### 10. [csv-transformation](csv-transformation/)
ETL (Extract, Transform, Load) operations for complex data transformations and business logic.

**Key concepts:**
- Data cleaning and standardization
- Data enrichment
- Business logic application
- Quality validation
- Summary reporting

## Getting Started

1. Enter the development environment:
   ```bash
   nix develop
   ```

2. Navigate to any example directory:
   ```bash
   cd csv-read-basic
   ```

3. Run the example:
   ```bash
   go run main.go
   ```

4. Read the README.md in each directory for detailed explanations.

## Common Patterns

### Basic CSV Reading
```go
// Read CSV with SQL
rows, err := db.Query("SELECT * FROM read_csv('file.csv')")

// Read with options
rows, err := db.Query("SELECT * FROM read_csv('file.csv', delimiter := '|', header := false)")

// Auto-detect format
rows, err := db.Query("SELECT * FROM read_csv_auto('file.csv')")
```

### CSV Writing
```go
// Export table to CSV
_, err := db.Exec("COPY table_name TO 'output.csv'")

// Export query results
_, err := db.Exec("COPY (SELECT * FROM table WHERE condition) TO 'output.csv'")

// Export with options
_, err := db.Exec("COPY table TO 'output.csv' WITH (DELIMITER '|', HEADER false)")
```

## Best Practices

1. **Always check for errors** after database operations
2. **Use defer rows.Close()** to ensure resources are cleaned up
3. **Handle NULL values** explicitly in your code
4. **Test with sample data** before processing large files
5. **Use absolute paths** or ensure correct working directory
6. **Clean up temporary files** when done
7. **Validate data types** when reading from CSV
8. **Use transactions** for multi-step operations
9. **Monitor memory usage** with large files
10. **Document your transformations** for reproducibility

## Performance Tips

- Use `read_csv_auto()` for unknown formats
- Specify explicit options when format is known
- Process large files in batches
- Use columnar projection to read only needed columns
- Create indexes on frequently queried columns
- Use temporary views for repeated queries
- Consider compression for large exports

## Error Handling

Common issues and solutions:

1. **File not found**: Check file path and permissions
2. **Permission denied**: Ensure read/write access to directories
3. **Type conversion errors**: Use TRY_CAST or validate data first
4. **Memory errors**: Process in smaller batches
5. **Schema mismatches**: Handle different schemas explicitly

## Further Learning

- Review individual README files in each example directory
- Experiment with the provided sample data
- Modify examples to work with your own CSV files
- Combine techniques from multiple examples
- Refer to DuckDB documentation for additional functions

## Contributing

Feel free to submit issues or pull requests if you find bugs or have suggestions for additional examples.