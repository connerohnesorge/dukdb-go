# JSON Examples

This example demonstrates JSON operations available in dukdb-go for storing and querying semi-structured data.

## Features Demonstrated

- Storing JSON data in tables
- Extracting values using JSON operators (`->`, `->>`)
- Checking key existence with `?` operator
- Filtering and aggregating JSON data
- Creating JSON objects from query results

## Running the Example

```bash
go run main.go
```

## JSON Functions and Operators

### Extraction Operators
- `json->'key'` - Extract JSON object field as JSON
- `json->>'key'` - Extract JSON object field as text
- `json->'index'` - Extract JSON array element as JSON

### Existence Operators
- `json ? 'key'` - Check if key exists in JSON object
- `json ?| array` - Check if any keys exist
- `json ?& array` - Check if all keys exist

### Creation Functions
- `JSON_OBJECT(key1, val1, ...)` - Create JSON object
- `JSON_ARRAY(val1, val2, ...)` - Create JSON array

## Example Queries

```sql
-- Extract nested values
SELECT data->>'user_id' FROM events;

-- Filter by JSON content
SELECT * FROM events WHERE data->>'amount' > '50';

-- Aggregate to JSON
SELECT JSON_OBJECT('count', COUNT(*), 'total', SUM(amount))
FROM purchases;
```

## Use Cases

JSON columns are ideal for:
- Event logs with variable structure
- User preferences and settings
- API response caching
- Flexible metadata storage
