# JSON Write Basic Example

This example demonstrates writing JSON data from database tables using the `COPY TO` statement in dukdb-go.

## Overview

The `COPY TO` statement allows you to export query results or entire tables to JSON format. You can export to standard JSON arrays or NDJSON (newline-delimited JSON) format.

## Key Concepts

### Basic Syntax
```sql
COPY (SELECT * FROM table) TO 'file.json' (FORMAT JSON)
```

### Supported Formats
1. **JSON Array**: `[{"col1": "val1"}, {"col1": "val2"}]`
2. **NDJSON**: One JSON object per line

## Examples Included

1. **Export Table to JSON**: Basic table export
2. **Export Selected Columns**: Export specific columns with filtering
3. **Export Query Results**: Export aggregated/summarized data
4. **Export to NDJSON**: Using newline-delimited format
5. **Export with Options**: Using compression and other options
6. **Export Joined Data**: Export results from JOIN queries
7. **Error Handling**: Handling export errors

## Running the Example

```bash
cd json-write-basic
go run main.go
```

## Sample Data

The example creates sample tables:
- `employees`: Employee information (id, name, department, salary, hire_date)
- `products`: Product catalog (product_id, name, category, price, stock_quantity)
- `sales`: Sales transactions (sale_id, product_id, employee_id, quantity, sale_date)

## SQL Functions Used

- `COPY (query) TO 'file.json' (FORMAT JSON)`: Export to JSON array
- `COPY (query) TO 'file.ndjson' (FORMAT JSON, ARRAY FALSE)`: Export to NDJSON

## Export Options

- `FORMAT JSON`: Export as JSON array (default)
- `ARRAY FALSE`: Export as NDJSON (one object per line)
- `COMPRESSION GZIP`: Compress output (if supported)

## Export Examples

### Basic Export
```sql
COPY (SELECT * FROM employees) TO 'employees.json' (FORMAT JSON)
```

### Filtered Export
```sql
COPY (
  SELECT name, salary
  FROM employees
  WHERE department = 'Engineering'
) TO 'engineering.json' (FORMAT JSON)
```

### Aggregated Export
```sql
COPY (
  SELECT department, COUNT(*), AVG(salary)
  FROM employees
  GROUP BY department
) TO 'summary.json' (FORMAT JSON)
```

### Joined Data Export
```sql
COPY (
  SELECT e.name, p.name, s.quantity
  FROM sales s
  JOIN employees e ON s.employee_id = e.id
  JOIN products p ON s.product_id = p.product_id
) TO 'report.json' (FORMAT JSON)
```

## Error Handling

The example demonstrates:
- Invalid file path errors
- Invalid format options
- Permission issues

## Output Files

The program creates several output files:
- `employees.json`: All employee data
- `engineering_employees.json`: Filtered employee data
- `department_summary.json`: Department statistics
- `products.ndjson`: Product data in NDJSON format
- `sales_report.json`: Joined sales data

## Performance Considerations

- Large exports may take significant time
- Consider using LIMIT for testing
- NDJSON format is better for streaming large datasets
- Compression reduces file size but adds processing time

## Notes

- Export creates new files or overwrites existing ones
- File paths can be absolute or relative
- Query results are materialized before export
- JSON keys match column names from the query
- NULL values are omitted from JSON output