# CSV Read Basic Example

This example demonstrates the basic usage of reading CSV files in dukdb-go using the `read_csv()` function.

## Concept

The `read_csv()` function allows you to read CSV files directly into DuckDB as virtual tables that can be queried with SQL. This provides a simple way to analyze CSV data without importing it into the database.

## Key Features

1. **Direct CSV Reading**: Read CSV files without importing them into the database
2. **SQL Queries**: Use full SQL syntax to query CSV data
3. **Schema Detection**: Automatically detects column types from CSV data
4. **Filtering**: Apply WHERE clauses to filter CSV data
5. **Aggregation**: Use aggregate functions like COUNT, AVG, etc.

## Usage

### SQL API
```sql
-- Read entire CSV file
SELECT * FROM read_csv('filename.csv');

-- Filter CSV data
SELECT * FROM read_csv('filename.csv') WHERE age > 25;

-- Aggregate CSV data
SELECT COUNT(*), AVG(age) FROM read_csv('filename.csv');
```

### Go API
```go
// Execute SQL query on CSV
db.Query("SELECT * FROM read_csv('filename.csv')")

// Scan results into variables
rows.Scan(&id, &name, &age, &city)
```

## Example Output

```
=== Basic CSV Reading Example ===

1. Reading CSV using SQL:
Columns: [id name age city]

Data:
ID: 1, Name: Alice, Age: 25, City: New York
ID: 2, Name: Bob, Age: 30, City: San Francisco
ID: 3, Name: Charlie, Age: 28, City: Chicago
ID: 4, Name: Diana, Age: 35, City: Boston
ID: 5, Name: Eve, Age: 22, City: Seattle

2. Reading CSV with Go API:
People older than 25:
- Bob (30 years old) from San Francisco
- Charlie (28 years old) from Chicago
- Diana (35 years old) from Boston

3. CSV Statistics:
Total rows: 5
Average age: 28.00
```

## Error Handling

The example includes proper error handling for:
- Database connection failures
- CSV file not found or inaccessible
- SQL query errors
- Data type mismatches during scanning

## Best Practices

1. Always check for errors after database operations
2. Use `defer rows.Close()` to ensure resources are cleaned up
3. Check `rows.Err()` after iteration to catch any errors
4. Use absolute paths or ensure CSV files are in the correct relative path
5. Consider using `read_csv_auto()` for automatic format detection (see next example)