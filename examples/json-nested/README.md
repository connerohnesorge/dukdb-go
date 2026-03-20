# JSON Nested Structures Example

This example demonstrates working with deeply nested JSON structures containing objects, arrays, and complex types.

## Overview

DuckDB can handle complex nested JSON with objects (structs) and arrays. Nested structures are automatically converted to appropriate DuckDB types.

## What You'll Learn

1. **Nested Objects**: Working with STRUCT types from JSON
2. **Arrays in JSON**: Handling ARRAY types
3. **Type Conversion**: How DuckDB maps JSON to types
4. **Accessing Nested Data**: Using JSON functions
5. **Querying Complex Structures**: SQL operations on nested data

## Running the Example

```bash
cd examples/json-nested
go run main.go
```

### Expected Output
```
=== JSON Nested Structures Example ===

1. Reading nested JSON structure:
Columns: [id profile tags user]
(Note: profile is a STRUCT containing age, city, active)
(Note: tags is an ARRAY of strings)

2. Accessing nested data through JSON functions:
DuckDB automatically converts nested structures to proper types

...

5. JSON Structure Information:
Nested JSON contains:
  - id: INTEGER
  - user: VARCHAR
  - profile: STRUCT with fields (age: INT, city: VARCHAR, active: BOOL)
  - tags: ARRAY of VARCHAR

6. Counting records from nested JSON:
Total records: 4

7. Listing users from nested JSON:
  - alice
  - bob
  - charlie
  - diana

✓ JSON nested structures example completed successfully!
```

## Type Mapping

JSON to DuckDB Type Mapping:

| JSON | DuckDB |
|------|--------|
| `"string"` | VARCHAR |
| `123` | INTEGER |
| `123.45` | DOUBLE |
| `true/false` | BOOLEAN |
| `[1, 2, 3]` | INTEGER[] |
| `{"key": "value"}` | STRUCT |
| `null` | NULL |

## Nested Structure Examples

### JSON Object (becomes STRUCT)
```json
{
  "name": "Alice",
  "profile": {
    "age": 28,
    "city": "New York"
  }
}
```

Maps to:
```
name: VARCHAR
profile: STRUCT(age INTEGER, city VARCHAR)
```

### JSON Array
```json
{
  "id": 1,
  "tags": ["python", "golang", "rust"]
}
```

Maps to:
```
id: INTEGER
tags: VARCHAR[]
```

### Complex Nesting
```json
{
  "company": "TechCorp",
  "employees": [
    {
      "name": "Alice",
      "skills": ["Go", "Python"],
      "contact": {"email": "alice@tech.com", "phone": "555-0001"}
    }
  ]
}
```

## Working with Nested Data

### Access STRUCT Fields (if supported)
```sql
SELECT profile.age, profile.city FROM read_json_auto('data.json')
```

### Access ARRAY Elements
```sql
SELECT tags[1] FROM read_json_auto('data.json')
```

### Filter by Nested Fields
```sql
SELECT name FROM read_json_auto('data.json') WHERE profile.age > 25
```

## Complex Queries

### Flatten and Unnest Arrays
```sql
SELECT name, unnest(tags) as tag
FROM read_json_auto('data.json')
```

### Join with Nested Data
```sql
SELECT e.name, d.name as department
FROM employees e
CROSS JOIN unnest(e.departments) as d
```

## Real-World Examples

### API Response with Nested Data
```json
{
  "status": "success",
  "data": {
    "user": {"id": 1, "name": "Alice"},
    "posts": [{"id": 101, "title": "Post 1"}]
  }
}
```

### Log Entry with Nested Metadata
```json
{
  "timestamp": "2024-01-01T10:00:00Z",
  "level": "error",
  "message": "Connection failed",
  "metadata": {
    "host": "server1",
    "port": 5432,
    "retry_count": 3
  }
}
```

### E-Commerce Product with Variations
```json
{
  "id": "SKU-123",
  "name": "T-Shirt",
  "variants": [
    {"color": "red", "size": "M", "stock": 10},
    {"color": "blue", "size": "L", "stock": 5}
  ]
}
```

## Performance Considerations

- **Flattening**: Consider flattening deeply nested structures for better performance
- **Indexing**: Regular columns perform better than nested fields
- **Memory**: Nested structures require more memory during processing
- **Unnesting**: Use carefully on large arrays (can multiply rows)

## Important Notes

- Nested data types depend on JSON structure
- Null values in JSON become SQL NULL
- Empty arrays/objects are preserved
- Deep nesting (3+ levels) may require special handling
- Some backends may have limitations on nesting depth

## Next Steps

- See [json-querying](../json-querying) for query examples
- See [json-transformation](../json-transformation) for data transformation
- See [json-schema](../json-schema) for schema inference

## Related Functions

- `read_json()` - Read JSON arrays
- `read_ndjson()` - Read NDJSON
- `json_extract()` - Extract nested values
- `json_keys()` - Get object keys
- `json_typeof()` - Get value type
- `unnest()` - Flatten arrays

## Documentation

https://duckdb.org/docs/data/json/overview
