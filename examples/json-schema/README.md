# JSON Schema Inference Example

This example demonstrates how DuckDB automatically infers schemas from JSON data.

## Overview

DuckDB automatically infers column names and types from JSON structure, eliminating the need to define schemas manually.

## What You'll Learn

1. **Schema Inference**: How DuckDB discovers column types
2. **Type Detection**: What types are inferred from values
3. **Consistency**: How type consistency affects inference
4. **Type Mapping**: JSON types to DuckDB types
5. **Query Performance**: How inference affects queries

## Running the Example

```bash
cd examples/json-schema
go run main.go
```

### Expected Output
```
=== JSON Schema Inference Example ===

1. Inferring schema from consistent JSON array:
Schema inferred from 'products.json':
Columns: [id in_stock name price]
  Column 0: name='id', type='BIGINT'
  Column 1: name='in_stock', type='BOOLEAN'
  Column 2: name='name', type='VARCHAR'
  Column 3: name='price', type='DOUBLE'

2. Inferring schema from NDJSON:
Schema inferred from 'transactions.json':
Columns: [amount status timestamp transaction_id]

3. Numeric type inference:
Inferred numeric types:
  counter: BIGINT
  percentage: BIGINT
  rating: DOUBLE

4. Type consistency:
Schema from consistent types:
  id: DuckDB type='BIGINT', Go type='int64'
  label: DuckDB type='VARCHAR', Go type='string'
  value: DuckDB type='BIGINT', Go type='int64'

✓ JSON schema inference example completed successfully!
```

## Type Inference Rules

| JSON Value | Inferred Type | Go Type |
|-----------|---------------|---------|
| `"text"` | VARCHAR | string |
| `123` | BIGINT | int64 |
| `123.45` | DOUBLE | float64 |
| `true` | BOOLEAN | bool |
| `[1,2,3]` | BIGINT[] | []int64 |
| `{"key":"val"}` | STRUCT | struct |
| `null` | Depends on other values | nil |

## How Inference Works

1. **First Record Analysis**: Schema is inferred from the first JSON object/record
2. **Field Names**: Become column names
3. **Field Values**: Determine column types
4. **Consistency**: All records should have same structure
5. **Nulls**: NULL values inherit type from other records

## Best Practices

### Ensure Consistent Types
```json
{"id": 1, "value": 100}
{"id": 2, "value": 200}
```
✓ Good - both have same types

```json
{"id": 1, "value": 100}
{"id": 2, "value": "200"}
```
✗ Bad - value changes type

### Order Columns Consistently
```json
{"id": 1, "name": "A", "price": 10}
{"id": 2, "name": "B", "price": 20}
```
✓ Good - same order

```json
{"id": 1, "name": "A", "price": 10}
{"name": "B", "id": 2, "price": 20}
```
Okay - different order (alphabetical handling)

### Provide All Fields
```json
{"id": 1, "name": "A", "price": 10, "active": true}
{"id": 2, "name": "B", "price": 20, "active": false}
```
✓ Good - all fields present

## Type Conversions in Go

When scanning inferred types:

```go
var id int64              // for BIGINT
var name string           // for VARCHAR
var price float64         // for DOUBLE
var active bool           // for BOOLEAN
var tags []string         // for VARCHAR[]
```

## Numeric Type Examples

### Integer Inference
```json
{"count": 100, "total": 5000}
```
Both inferred as BIGINT (can hold 64-bit integers)

### Float Inference
```json
{"rate": 3.14, "percentage": 99.5}
```
Both inferred as DOUBLE (floating point)

### Mixed Numeric
```json
{"id": 1, "price": 29.99, "quantity": 5}
```
- id → BIGINT
- price → DOUBLE
- quantity → BIGINT

## Handling Schema Changes

If you need explicit schema control:

```go
db.Query(`
    SELECT 
        CAST(id AS INTEGER) as id,
        name,
        CAST(price AS DECIMAL(10,2)) as price
    FROM read_json_auto('data.json')
`)
```

## Performance Impact

- **Auto-detection**: No performance penalty
- **Type accuracy**: Ensures correct type operations
- **Query optimization**: Helps DuckDB optimize queries
- **Memory usage**: Proper types reduce memory footprint

## Common Issues

### Null First Field
If the first value is null, type can't be inferred:
```json
{"id": null, "name": "A"}  // id type unknown
```
Solution: Ensure non-null first value

### Type Conflicts
If types conflict across records:
```json
{"id": 1}
{"id": "2"}  // Conflict!
```
Solution: Keep types consistent

### Optional Fields
If some records lack fields:
```json
{"id": 1, "email": "a@b.com"}
{"id": 2}                      // Missing email
```
Solution: Include all fields (use null if needed)

## Verification

To check inferred schema:

```go
rows, _ := db.Query("SELECT * FROM read_json_auto('data.json')")
colTypes, _ := rows.ColumnTypes()

for _, col := range colTypes {
    fmt.Printf("%s: %s\n", col.Name(), col.DatabaseTypeName())
}
```

## Next Steps

- See [json-read-auto](../json-read-auto) for format detection
- See [json-querying](../json-querying) for query examples
- See [json-transformation](../json-transformation) for data transformation

## Related Functions

- `read_json_auto()` - Auto-detect format and infer schema
- `PRAGMA table_info()` - Get table schema info
- `DESCRIBE` - Get result schema

## Documentation

https://duckdb.org/docs/data/json/overview
