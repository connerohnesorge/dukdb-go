# Index Examples

This example demonstrates creating and using indexes in dukdb-go to optimize query performance.

## Features Demonstrated

- Creating indexes on table columns
- Query performance improvements
- Index usage for filtering and sorting
- Composite index strategies

## Running the Example

```bash
go run main.go
```

## Creating Indexes

### Single Column Index
```sql
CREATE INDEX idx_category ON products(category);
```

### Composite Index
```sql
CREATE INDEX idx_cat_price ON products(category, price);
```

## Index Benefits

Indexes improve performance for:
- `WHERE` clause filtering: `WHERE category = 'Electronics'`
- `ORDER BY` sorting: `ORDER BY price`
- `JOIN` operations: faster lookups on join columns
- `GROUP BY` operations: pre-sorted data

## When to Use Indexes

✅ Good candidates:
- Columns used frequently in WHERE clauses
- Foreign key columns
- Columns used in JOINs
- Columns used for sorting

❌ Avoid indexes on:
- Very small tables
- Frequently updated columns
- Columns with low cardinality (few unique values)
```
