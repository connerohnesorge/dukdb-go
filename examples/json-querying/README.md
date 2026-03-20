# JSON Querying Example

This example demonstrates querying and analyzing JSON data using SQL functions and aggregations.

## Overview

JSON data can be queried just like regular tables using DuckDB's SQL engine. You can filter, aggregate, and join JSON data without transformation.

## What You'll Learn

1. **Filtering JSON**: Use WHERE clauses on JSON data
2. **Aggregations**: COUNT, AVG, MAX, MIN on JSON fields
3. **GROUP BY**: Group and aggregate JSON data
4. **ORDER BY**: Sort JSON results
5. **JOINs**: Combine multiple JSON sources

## Running the Example

```bash
cd examples/json-querying
go run main.go
```

### Expected Output
```
=== JSON Querying Example ===

1. All employees from JSON:
Columns: [company email employee_id name salary skill_count]
  - Alice (ID:1, Email: alice@tech.com, Salary: $75000, Skills: 3)
  - Bob (ID:2, Email: bob@tech.com, Salary: $85000, Skills: 2)
  - Charlie (ID:3, Email: charlie@tech.com, Salary: $95000, Skills: 3)

2. Employees with salary > 80000:
  - Charlie: $95000
  - Bob: $85000

3. Salary statistics:
  - Count: 3 employees
  - Average salary: $85000.00
  - Max salary: $95000
  - Min salary: $75000

4. Employees grouped by skill count:
  - 3 skills: 2 employee(s)
  - 2 skills: 1 employee(s)

✓ JSON querying example completed successfully!
```

## Code Examples

### Basic Filtering
```go
rows, err := db.Query("SELECT * FROM read_json_auto('employees.json') WHERE salary > 80000")
```

### Aggregation
```go
var count int
var avgSalary float64
err := db.QueryRow("SELECT COUNT(*), AVG(salary) FROM read_json_auto('employees.json')").
    Scan(&count, &avgSalary)
```

### GROUP BY
```go
rows, err := db.Query(`
    SELECT department, COUNT(*) as count
    FROM read_json_auto('employees.json')
    GROUP BY department
`)
```

### ORDER BY
```go
rows, err := db.Query(`
    SELECT name, salary
    FROM read_json_auto('employees.json')
    ORDER BY salary DESC
`)
```

## Supported SQL Operations

| Operation | Example |
|-----------|---------|
| SELECT | `SELECT name, salary FROM ...` |
| WHERE | `WHERE salary > 50000` |
| ORDER BY | `ORDER BY salary DESC` |
| GROUP BY | `GROUP BY department` |
| LIMIT | `LIMIT 10` |
| COUNT | `COUNT(*)` |
| AVG | `AVG(salary)` |
| MAX/MIN | `MAX(salary)` |
| SUM | `SUM(amount)` |
| LIKE | `WHERE name LIKE 'A%'` |
| IN | `WHERE id IN (1,2,3)` |
| BETWEEN | `WHERE age BETWEEN 25 AND 35` |

## Nested JSON Handling

For deeply nested JSON:
- Flatten complex structures before querying
- Use JSON extraction functions if needed
- Consider NDJSON for array data

## Performance Tips

- Filter early with WHERE clauses
- Use specific columns in SELECT
- Aggregate before returning large result sets
- Use LIMIT for result pagination

## Related Functions

- `read_json()` - Read JSON arrays
- `read_ndjson()` - Read NDJSON
- `read_json_auto()` - Auto-detect JSON format
- `json_extract()` - Extract JSON values
- `json_keys()` - Get object keys

## Next Steps

- See [json-nested](../json-nested) for complex nested JSON
- See [json-write-basic](../json-write-basic) for exporting JSON
- See [json-transformation](../json-transformation) for data transformation

## Documentation

https://duckdb.org/docs/data/json/overview
