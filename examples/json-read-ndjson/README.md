# JSON Read NDJSON Example

This example demonstrates how to read NDJSON (Newline-Delimited JSON) files using dukdb-go's `read_ndjson()` function.

## Overview

NDJSON is a streaming format where each line contains a complete JSON object. This format is ideal for:
- Large datasets that are generated incrementally
- Streaming data from APIs and services
- Log files with JSON entries
- Data that doesn't fit in memory as a single JSON array

## What You'll Learn

1. **NDJSON Format**: Understand how NDJSON differs from JSON arrays
2. **Reading NDJSON**: Use `read_ndjson()` to query NDJSON files
3. **Streaming Data**: Work with large datasets efficiently
4. **Complex Queries**: Apply aggregations and joins to NDJSON data
5. **Performance**: Leverage NDJSON's streaming benefits

## Key Concepts

### NDJSON Format
NDJSON has one JSON object per line:
```
{"id": 1, "name": "Alice", "age": 25}
{"id": 2, "name": "Bob", "age": 30}
{"id": 3, "name": "Charlie", "age": 28}
```

### Advantages of NDJSON
- **Streaming**: Process data line-by-line without loading entire file
- **Large Files**: Handle gigabytes or terabytes of data
- **Incremental Writing**: Append new records without rewriting file
- **Log Compatible**: Works with standard logging systems
- **Simple Parsing**: Each line is independent and parseable

### read_ndjson() Function
- Reads newline-delimited JSON from files
- Automatically infers column types from first record
- Supports standard SQL queries
- Memory-efficient streaming

## Running the Example

```bash
cd examples/json-read-ndjson
go run main.go
```

### Expected Output
```
=== NDJSON Reading Example ===

NDJSON Format (Newline-Delimited JSON):
Each line contains a complete JSON object
--------------------------------------------------

1. Reading NDJSON using SQL:
Columns: [age city id name salary]

All Records:
ID: 1 | Name: Alice      | Age: 25 | City: New York       | Salary: $75000
ID: 2 | Name: Bob        | Age: 30 | City: San Francisco  | Salary: $95000
...

2. Reading NDJSON with Filtering:
Employees older than 28:
- Bob, age 30, earning $95000
...

3. NDJSON Statistics:
Total records: 5
Average salary: $86000.00
Highest paid: Diana ($105000)

Salary information by city:
  Boston: 1 employees, avg salary: $105000.00
...

✓ NDJSON reading example completed successfully!
```

## Code Walkthrough

### 1. Reading NDJSON
```go
rows, err := db.Query("SELECT * FROM read_ndjson('sample_data.ndjson')")
```

### 2. Basic Scanning
```go
for rows.Next() {
    var id int
    var name string
    var age int
    
    err := rows.Scan(&id, &name, &age)
    // Process the row...
}
```

### 3. Filtering
```go
rows, err := db.Query("SELECT * FROM read_ndjson('file.ndjson') WHERE age > 28")
```

### 4. Aggregations
```go
var avgSalary float64
err = db.QueryRow("SELECT AVG(salary) FROM read_ndjson('file.ndjson')").Scan(&avgSalary)
```

### 5. Subqueries
```go
rows, err := db.Query(`
    SELECT name, salary 
    FROM read_ndjson('file.ndjson')
    WHERE salary > (SELECT AVG(salary) FROM read_ndjson('file.ndjson'))
`)
```

## NDJSON vs JSON Array

| Feature | NDJSON | JSON Array |
|---------|--------|-----------|
| Format | Line-delimited | Single array |
| Memory | Streaming | Full load |
| Large Files | Excellent | Poor |
| Append Data | Easy | Requires rewrite |
| Parse Speed | Fast | Slower |
| Use Case | Logs, Streams | Small datasets |

## Important Notes

- **Type Inference**: Types are inferred from the first JSON object
- **Consistency**: All objects should have the same field names
- **No Validation**: Invalid JSON lines will cause errors
- **Column Order**: DuckDB sorts columns alphabetically
- **Efficiency**: NDJSON is more efficient than JSON arrays for large files

## Real-World Examples

### API Response Log
```
{"timestamp": "2024-01-01T10:00:00Z", "status": 200, "response_time": 45}
{"timestamp": "2024-01-01T10:00:01Z", "status": 200, "response_time": 52}
{"timestamp": "2024-01-01T10:00:02Z", "status": 500, "response_time": 1000}
```

### Application Logs
```
{"level": "INFO", "message": "Server started", "timestamp": 1234567890}
{"level": "ERROR", "message": "Connection failed", "timestamp": 1234567891}
{"level": "INFO", "message": "Retrying...", "timestamp": 1234567892}
```

### User Events
```
{"user_id": 123, "event": "login", "timestamp": 1234567890}
{"user_id": 123, "event": "view_page", "timestamp": 1234567891}
{"user_id": 123, "event": "logout", "timestamp": 1234567892}
```

## Next Steps

- See [json-read-auto](../json-read-auto) for automatic format detection
- See [json-read-basic](../json-read-basic) for JSON arrays
- See [json-querying](../json-querying) for advanced queries

## Related Functions

- `read_ndjson()` - Read newline-delimited JSON
- `read_json()` - Read JSON arrays
- `read_json_auto()` - Automatically detect JSON format
- `json_extract()` - Extract values from JSON
- `json_valid()` - Validate JSON

## Documentation

For more information about JSON functions in DuckDB, visit:
https://duckdb.org/docs/data/json/overview
