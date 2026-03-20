# JSON Write Basic Example

This example demonstrates how to export data from tables to JSON files using DuckDB's COPY TO syntax.

## Overview

DuckDB makes it easy to export table data to JSON format using the COPY TO command. You can export to JSON arrays or NDJSON format with filtering and aggregations.

## What You'll Learn

1. **Exporting to JSON**: Use COPY TO for JSON export
2. **Array vs NDJSON**: Choose the right output format
3. **Filtered Exports**: Export subsets of data
4. **Aggregations**: Export computed results
5. **Verification**: Read back exported JSON to verify

## Key Concepts

### COPY TO Syntax
```sql
COPY (SELECT ...) TO 'filename' (FORMAT JSON)
COPY (SELECT ...) TO 'filename' (FORMAT NDJSON)
```

### JSON Export Formats

#### JSON Array Format
- All records in single array
- Good for small to medium datasets
- Complete array structure
- Requires full load into memory

#### NDJSON Format
- One JSON object per line
- Good for large datasets and streaming
- Efficient for append operations
- Memory efficient

## Running the Example

```bash
cd examples/json-write-basic
go run main.go
```

### Expected Output
```
=== JSON Write Example ===

1. Creating sample data:
Created table with 5 employees

Employee Data:
  [1] Alice - Engineering - $75000
  [2] Bob - Marketing - $65000
  [3] Charlie - Engineering - $80000
  [4] Diana - Sales - $70000
  [5] Eve - Engineering - $85000

2. Exporting to JSON array format:
Generated JSON Array Format:
[
  {"department":"Engineering","id":1,"name":"Alice","salary":75000},
  {"department":"Engineering","id":2,"name":"Bob","salary":65000},
  ...
]

3. Exporting to NDJSON format:
Generated NDJSON Format:
{"department":"Engineering","id":1,"name":"Alice","salary":75000}
{"department":"Engineering","id":2,"name":"Bob","salary":65000}
...

4. Exporting filtered data (Engineering department only):
Engineering Employees (NDJSON):
{"id":5,"name":"Eve","salary":85000}
{"id":3,"name":"Charlie","salary":80000}
{"id":1,"name":"Alice","salary":75000}

5. Exporting aggregated data by department:
Department Summary (JSON Array):
[
  {"avg_salary":83333.333...,"count":3,"department":"Engineering","max_salary":85000,"min_salary":75000},
  {"avg_salary":70000.0,"count":1,"department":"Sales","max_salary":70000,"min_salary":70000},
  {"avg_salary":65000.0,"count":1,"department":"Marketing","max_salary":65000,"min_salary":65000}
]

6. Verifying exported data can be read back:
✓ Successfully exported and verified 5 records in JSON array
✓ Successfully exported and verified 5 records in NDJSON

✓ JSON write example completed successfully!
```

## Code Walkthrough

### 1. Simple Export to JSON Array
```go
db.Exec(`
    COPY (SELECT * FROM employees)
    TO 'output.json'
    (FORMAT JSON)
`)
```

### 2. Export to NDJSON
```go
db.Exec(`
    COPY (SELECT * FROM employees)
    TO 'output.ndjson'
    (FORMAT NDJSON)
`)
```

### 3. Export with Filtering
```go
db.Exec(`
    COPY (
        SELECT id, name, salary 
        FROM employees 
        WHERE department = 'Engineering'
    )
    TO 'engineers.json'
    (FORMAT JSON)
`)
```

### 4. Export with Aggregations
```go
db.Exec(`
    COPY (
        SELECT 
            department,
            COUNT(*) as count,
            AVG(salary) as avg_salary
        FROM employees
        GROUP BY department
    )
    TO 'summary.json'
    (FORMAT JSON)
`)
```

## Export Formats Comparison

| Feature | JSON Array | NDJSON |
|---------|-----------|--------|
| Format | `[{...}, {...}]` | Line-delimited |
| Best For | Small/medium data | Large data, streams |
| Memory Usage | All at once | Per line |
| Parsing Speed | Slower | Faster |
| Append Data | Difficult | Easy |
| File Size | Slightly smaller | Slightly larger |

## Real-World Use Cases

### Scenario 1: API Response Export
```sql
COPY (SELECT id, name, email FROM users WHERE active = true)
TO 'active_users.json'
(FORMAT JSON)
```

### Scenario 2: Log Export
```sql
COPY (SELECT timestamp, level, message FROM logs WHERE level = 'ERROR')
TO 'errors.ndjson'
(FORMAT NDJSON)
```

### Scenario 3: Report Generation
```sql
COPY (
    SELECT date, product, SUM(sales) as total
    FROM transactions
    GROUP BY date, product
)
TO 'daily_report.json'
(FORMAT JSON)
```

### Scenario 4: Data Backup
```sql
COPY (SELECT * FROM important_table)
TO 'backup.ndjson'
(FORMAT NDJSON)
```

## Advanced Features

### Export with Sorting
```go
db.Exec(`
    COPY (SELECT * FROM employees ORDER BY salary DESC)
    TO 'by_salary.json'
    (FORMAT JSON)
`)
```

### Export with LIMIT
```go
db.Exec(`
    COPY (SELECT * FROM employees LIMIT 10)
    TO 'top10.json'
    (FORMAT NDJSON)
`)
```

### Export with JOINs
```go
db.Exec(`
    COPY (
        SELECT e.name, d.name as department
        FROM employees e
        JOIN departments d ON e.dept_id = d.id
    )
    TO 'employees_with_depts.json'
    (FORMAT JSON)
`)
```

## Important Notes

- **Column Order**: DuckDB sorts columns alphabetically in JSON
- **NULL Handling**: NULL values are included in JSON output
- **Type Preservation**: Types are preserved (strings, numbers, etc.)
- **File Paths**: Use absolute paths or ensure working directory is correct
- **Permissions**: Ensure write permissions to target directory
- **File Overwrite**: COPY TO will overwrite existing files

## Performance Tips

- Use NDJSON for large datasets (more memory efficient)
- Use JSON arrays for small, self-contained datasets
- Filter data in SQL before export (faster than post-processing)
- Aggregate data before export when possible
- Use ORDER BY to organize output

## Next Steps

- See [json-read-basic](../json-read-basic) for reading exported JSON
- See [json-querying](../json-querying) for advanced JSON queries
- See [json-transformation](../json-transformation) for data transformation

## Related Functions

- `COPY TO` - Export data to files
- `COPY FROM` - Import data from files
- `read_json()` - Read JSON arrays
- `read_ndjson()` - Read NDJSON files
- `read_json_auto()` - Auto-detect JSON format

## Documentation

For more information about COPY and JSON in DuckDB, visit:
https://duckdb.org/docs/sql/statements/copy
https://duckdb.org/docs/data/json/overview
