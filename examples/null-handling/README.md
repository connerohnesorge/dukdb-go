# NULL Handling Example

This example demonstrates how to work with NULL values in dukdb-go, including proper handling, querying, and best practices.

## Overview

The example shows how to:
- Insert NULL values into the database
- Query NULL values using sql.Null types
- Find NULL and non-NULL values with IS NULL/IS NOT NULL
- Use COALESCE to handle NULL values in queries
- Update NULL values
- Count NULL values by column
- Write complex queries with NULL handling
- Apply NULL-safe comparisons
- Follow best practices for NULL handling

## Key Concepts

### What is NULL?
- NULL represents "unknown" or "missing" data
- NULL is different from empty string ("") or zero (0)
- Any column that is not declared NOT NULL can contain NULL

### Inserting NULL Values
```go
// Using nil for NULL
_, err := db.Exec("INSERT INTO products (name, price) VALUES (?, ?)",
    "Product Name", nil)

// Using pointers
var description *string = nil
_, err := db.Exec("INSERT INTO products (name, description) VALUES (?, ?)",
    "Product Name", description)
```

### Querying NULL Values
```go
// Use sql.Null types for nullable columns
var price sql.NullFloat64
err := row.Scan(&price)
if price.Valid {
    // price.Float64 contains the value
} else {
    // price is NULL
}
```

### Finding NULL Values
```sql
-- Find NULL values
SELECT * FROM products WHERE price IS NULL

-- Find non-NULL values
SELECT * FROM products WHERE price IS NOT NULL

-- Important: Don't use = or != with NULL
-- These won't work:
SELECT * FROM products WHERE price = NULL    -- Wrong!
SELECT * FROM products WHERE price != NULL   -- Wrong!
```

### Using COALESCE
```sql
-- Replace NULL with default value
SELECT COALESCE(price, 0.00) FROM products
SELECT COALESCE(description, 'No description') FROM products

-- Multiple fallbacks
SELECT COALESCE(phone, email, 'No contact') FROM customers
```

### NULL-Safe Comparisons
```sql
-- Count NULL values
SELECT COUNT(*) FROM products WHERE price IS NULL

-- Check multiple NULL conditions
WHERE price IS NULL AND discontinued_date IS NULL

-- Use in CASE statements
CASE
    WHEN price IS NULL THEN 'Price TBD'
    WHEN price < 50 THEN 'Budget'
    ELSE 'Premium'
END
```

## Sample Data

The example creates a products table with various nullable columns:
- description (TEXT)
- price (DECIMAL)
- category (VARCHAR)
- stock_quantity (INTEGER)
- manufacturer (VARCHAR)
- release_date (DATE)
- discontinued_date (DATE)
- is_featured (BOOLEAN)
- rating (DECIMAL)
- warranty_months (INTEGER)

## NULL Examples Demonstrated

1. **Inserting NULLs**: Using pointers to insert NULL values
2. **Querying with sql.Null**: Using sql.NullFloat64, sql.NullString, etc.
3. **Finding NULLs**: Using IS NULL to find missing data
4. **Finding non-NULLs**: Using IS NOT NULL to find existing data
5. **COALESCE**: Replacing NULL with default values
6. **Updating NULLs**: Setting NULL values to actual values
7. **Counting NULLs**: Getting count of NULL values by column
8. **Complex queries**: Business logic with NULL handling
9. **NULL-safe comparisons**: Proper patterns for NULL checks
10. **Best practices**: Common mistakes and correct approaches

## Running the Example

```bash
cd examples/basic-08
go run main.go
```

## Expected Output

```
=== Basic Example 08: Working with NULL Values ===

✓ Products table created with nullable columns

=== Example 1: Inserting NULL values ===
✓ Products inserted with various NULL values

=== Example 2: Querying NULL values with sql.Null types ===
ID | Name              | Price     | Category    | Stock
---|-------------------|-----------|-------------|-------
 1 | Laptop Pro        | $1299.99  | Electronics |    25
 2 | Basic Mouse       | $  19.99  | Accessories |   100
 3 | Vintage Keyboard  | NULL      | Accessories |     0
 4 | Wireless Headphones| $ 299.99 | NULL        | NULL

=== Example 3: Finding NULL values with IS NULL ===
Products with NULL prices (discontinued or not yet priced):
  - ID 3: Vintage Keyboard
  - ID 4: Wireless Headphones

=== Example 4: Finding non-NULL values with IS NOT NULL ===
Products with ratings (highest rated first):
  - ID 3: Vintage Keyboard (Rating: 4.8/5.0)
  - ID 4: Wireless Headphones (Rating: 4.7/5.0)
  - ID 1: Laptop Pro (Rating: 4.5/5.0)

=== Example 5: Using COALESCE to handle NULL values ===
Products with NULL values replaced by defaults:
ID | Name              | Price  | Category     | Description
---|-------------------|--------|--------------|------------------------------
 1 | Laptop Pro        | $1299.99 | Electronics | High-performance laptop
 2 | Basic Mouse       | $  19.99 | Accessories | No description available
 3 | Vintage Keyboard  | $   0.00 | Accessories | Classic mechanical keyboard
 4 | Wireless Headphones| $ 299.99 | Uncategorized | Premium noise-cancelling headphones

=== Example 6: Updating NULL values ===
✓ Updated 1 products with default description
✓ Updated 1 products with default featured flag

=== Example 7: Counting NULL values ===
NULL value counts by column:
  - description: 1 NULL values
  - price: 1 NULL values
  - category: 1 NULL values
  - stock_quantity: 1 NULL values
  - release_date: 1 NULL values

=== Example 8: Complex query with NULL handling ===
Product availability analysis:
Name              | Price Cat | Stock Status | Availability
------------------|-----------|--------------|-------------
Laptop Pro        | Premium   | In Stock     | Available
Basic Mouse       | Budget    | In Stock     | Available
Vintage Keyboard  | Price TBD | Out of Stock | Available
Wireless Headphones| Mid-range | Unknown      | Coming Soon

=== Example 9: NULL-safe comparisons ===
Products that might need attention:
- 1 products without prices (not discontinued)
- 1 products without stock information
- 1 products without categories

=== Example 10: NULL handling best practices ===

Proper NULL checking patterns:
✓ Use IS NULL:      WHERE column IS NULL
✓ Use IS NOT NULL:  WHERE column IS NOT NULL
✗ Don't use = NULL: WHERE column = NULL  (won't work)
✗ Don't use != NULL: WHERE column != NULL (won't work)

Using COALESCE for default values:
SELECT COALESCE(price, 0.00) FROM products
SELECT COALESCE(description, 'No description') FROM products

✓ Table dropped successfully

=== Summary ===
This example demonstrated:
- Inserting NULL values using pointers
- Querying NULL values with sql.Null types
- Finding NULL values with IS NULL
- Finding non-NULL values with IS NOT NULL
- Using COALESCE to handle NULL values
- Updating NULL values
- Counting NULL values by column
- Complex queries with NULL handling
- NULL-safe comparisons and business logic
- Best practices for NULL handling

All operations completed successfully!
```

## Notes

- The example uses an in-memory database
- NULL values are inserted using pointer types (*string, *float64, etc.)
- The example demonstrates sql.Null types for safe querying
- COALESCE is used to provide default values for NULLs
- Complex business logic handles NULL values appropriately
- Best practices emphasize proper NULL comparison syntax

## Best Practices

1. **Use IS NULL/IS NOT NULL** - Never use = or != with NULL
2. **Use sql.Null types** - For safe NULL handling in Go
3. **Handle NULL explicitly** - Don't assume values are not NULL
4. **Use COALESCE** - To provide sensible defaults
5. **Check Valid field** - When using sql.Null types
6. **Document NULLable columns** - Make it clear which can be NULL
7. **Use NOT NULL** - When columns should always have values
8. **Consider default values** - Instead of allowing NULL

## Common Pitfalls

- Using = NULL instead of IS NULL
- Not checking sql.Null Valid field
- Forgetting NULL in calculations (NULL + 1 = NULL)
- Not handling NULL in string concatenation
- Assuming NULL means empty string or zero
- Not considering NULL in sorting operations

## NULL in Different Operations

### Arithmetic
```sql
-- NULL in calculations
SELECT price * 1.1 FROM products  -- If price is NULL, result is NULL
SELECT SUM(price) FROM products   -- SUM ignores NULL values
SELECT COUNT(price) FROM products -- COUNT only counts non-NULL
```

### String Operations
```sql
-- String concatenation with NULL
SELECT 'Price: ' || price FROM products  -- Result is NULL if price is NULL
SELECT CONCAT('Price: ', COALESCE(price, 0)) FROM products  -- Safe concatenation
```

### Sorting
```sql
-- NULL in ORDER BY
SELECT * FROM products ORDER BY price DESC NULLS LAST   -- PostgreSQL
SELECT * FROM products ORDER BY -price DESC            -- SQLite workaround
```

This example provides a comprehensive overview of working with NULL values in SQL databases, emphasizing proper handling techniques and common pitfalls to avoid."}