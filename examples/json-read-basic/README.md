# JSON Read Basic Example

This example demonstrates how to read basic JSON array files using dukdb-go's `read_json()` function.

## Overview

JSON is a widely-used data format for storing and exchanging structured data. DuckDB provides the `read_json()` function to query JSON arrays directly without requiring data transformation.

## What You'll Learn

1. **Reading JSON Arrays**: Load and query JSON array data directly from files
2. **Column Inference**: Understand how DuckDB automatically infers column types from JSON structure
3. **SQL Queries**: Apply SQL queries to JSON data (WHERE, SELECT, aggregations)
4. **Data Filtering**: Filter and query JSON data using standard SQL syntax
5. **Aggregations**: Perform calculations like COUNT() and AVG() on JSON data

## Key Concepts

### JSON Arrays
JSON arrays contain multiple objects with consistent structure:
```json
[
  {"id": 1, "name": "Alice", "age": 25},
  {"id": 2, "name": "Bob", "age": 30}
]
```

### read_json() Function
- Reads JSON arrays from files
- Automatically infers column types
- Supports standard SQL queries
- Works with filtering and aggregations

### Common Use Cases
- Loading API responses that return JSON arrays
- Reading exported JSON data from other systems
- Querying JSON exports from databases
- Processing structured JSON data files

## Running the Example

```bash
cd examples/json-read-basic
go run main.go
```

### Expected Output
```
=== Basic JSON Reading Example ===

1. Reading JSON Array using SQL:
Columns: [id name age city]

Data:
ID: 1, Name: Alice, Age: 25, City: New York
ID: 2, Name: Bob, Age: 30, City: San Francisco
...

2. Reading JSON with Filtering:
People older than 25:
- Bob (30 years old) from San Francisco
...

3. JSON Statistics:
Total records: 5
Average age: 28.00

People per city:
  Boston: 1
  Chicago: 1
  New York: 1
  San Francisco: 1
  Seattle: 1

✓ JSON reading example completed successfully!
```

## Code Walkthrough

### 1. Basic Reading
```go
rows, err := db.Query("SELECT * FROM read_json('sample_data.json')")
```

### 2. Column Inspection
```go
columns, err := rows.Columns()
fmt.Printf("Columns: %v\n", columns)
```

### 3. Data Scanning
```go
for rows.Next() {
    var id int
    var name string
    var age int
    var city string
    
    err := rows.Scan(&id, &name, &age, &city)
    // Use the values...
}
```

### 4. Filtering
```go
rows, err := db.Query("SELECT * FROM read_json('sample_data.json') WHERE age > 25")
```

### 5. Aggregations
```go
var avgAge float64
err = db.QueryRow("SELECT AVG(age) FROM read_json('sample_data.json')").Scan(&avgAge)
```

## Important Notes

- **Type Inference**: DuckDB infers JSON field types automatically based on values
- **Consistent Structure**: JSON objects should have consistent field names and types
- **File Format**: Use proper JSON array format `[{...}, {...}]`
- **Error Handling**: Always check errors from SQL operations
- **Memory**: Large JSON files are processed efficiently by DuckDB

## Next Steps

- See [json-read-ndjson](../json-read-ndjson) for NDJSON (newline-delimited JSON)
- See [json-read-auto](../json-read-auto) for automatic format detection
- See [json-querying](../json-querying) for advanced JSON queries

## Related Functions

- `read_json()` - Read JSON arrays
- `read_ndjson()` - Read newline-delimited JSON
- `read_json_auto()` - Automatically detect JSON format
- `json_extract()` - Extract values from JSON
- `json_keys()` - Get keys from JSON objects

## Documentation

For more information about JSON functions in DuckDB, visit:
https://duckdb.org/docs/data/json/overview
