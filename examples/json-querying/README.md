# JSON Querying Example

This example demonstrates advanced querying and analysis of JSON data in dukdb-go, including filtering, aggregation, and working with nested structures.

## Overview

JSON querying allows you to perform SQL operations on JSON data directly, without needing to transform it into a traditional relational format first. This is particularly useful for semi-structured data where the schema might vary.

## Key Concepts

### Direct JSON Querying
You can query JSON files directly using table functions:
```sql
SELECT * FROM read_json('file.json') WHERE column = 'value'
```

### Nested Data Handling
Nested JSON objects are returned as JSON strings that can be processed further.

## Examples Included

1. **Basic JSON Querying**: Reading and displaying JSON data
2. **Filtering JSON Data**: Using WHERE clauses on JSON fields
3. **Aggregating JSON Data**: GROUP BY and aggregate functions
4. **Working with Date Fields**: Date-based filtering and sorting
5. **Creating Views from JSON**: Creating persistent views over JSON data
6. **Complex Queries**: CASE statements and advanced filtering
7. **Export Query Results**: Saving query results back to JSON
8. **Working with Nested Fields**: Handling deeply nested JSON structures

## Running the Example

```bash
cd json-querying
go run main.go
```

## Sample Data

The example creates sample order data with nested structures:
- Order information (id, date, status, total)
- Customer details (nested object)
- Items array (multiple products per order)
- Shipping information (nested with address)

## SQL Functions Used

- `read_json(path)`: Read JSON array files
- `read_json_auto(path)`: Auto-detect format and read
- `COPY (query) TO 'file'`: Export query results

## Query Examples

### Basic Filtering
```sql
SELECT * FROM read_json('orders.json') WHERE total > 500
```

### Aggregation
```sql
SELECT status, COUNT(*), AVG(total), SUM(total) as revenue
FROM read_json('orders.json')
GROUP BY status
ORDER BY revenue DESC
```

### Date Filtering
```sql
SELECT order_id, order_date, total
FROM read_json('orders.json')
WHERE order_date >= '2024-01-15'
ORDER BY order_date
```

### Complex Queries with CASE
```sql
SELECT order_id, total,
       CASE
           WHEN total > 1000 THEN 'High Value'
           WHEN total > 500 THEN 'Medium Value'
           ELSE 'Low Value'
       END as value_category
FROM read_json('orders.json')
WHERE status IN ('shipped', 'delivered')
ORDER BY total DESC
```

## Working with Nested Data

Nested objects are flattened as JSON strings:
```json
{
  "customer": {
    "id": 1,
    "name": "Alice Johnson"
  }
}
```
Results in a column named `customer` containing the JSON string.

## Performance Considerations

- JSON querying is suitable for small to medium datasets
- For large datasets, consider importing to tables first
- Views can improve performance for repeated queries
- Indexing is not available on JSON files directly

## Error Handling

The example demonstrates:
- Handling missing files
- Invalid JSON format errors
- Query syntax errors
- Export failures

## Output

The program will output:
- Column information from JSON files
- Filtered and aggregated results
- Date-based query results
- Customer spending summaries
- Value category analysis
- Nested data structure information

## Notes

- Column order is alphabetical based on JSON keys
- All values from JSON are initially strings and may need casting
- Complex nested queries might require multiple steps
- JSON export may have limitations with complex expressions
- For production use, consider validating JSON structure first

## Advanced Usage

For more complex analysis:
1. Import JSON to temporary tables for better performance
2. Use JSON functions to extract specific nested values
3. Combine multiple JSON files using UNION
4. Create materialized views for frequently accessed JSON data

## Limitations

- No direct indexing on JSON file queries
- Nested objects returned as JSON strings
- Complex subqueries in COPY statements may not be supported
- Performance depends on file size and query complexity