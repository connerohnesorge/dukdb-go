# SELECT Queries Example

This example demonstrates various WHERE clause conditions and filtering techniques when querying data with dukdb-go.

## Overview

The example shows how to:
- Use basic comparison operators (=, <, >, <=, >=, !=)
- Filter with BETWEEN for ranges
- Combine multiple conditions with AND/OR
- Use IN clause for multiple values
- Pattern matching with LIKE
- Handle NULL values with IS NULL/IS NOT NULL
- Filter based on dates

## Key Concepts

### Comparison Operators
```sql
-- Equal to
WHERE category = 'Electronics'

-- Not equal to
WHERE category != 'Electronics'

-- Less than/greater than
WHERE price < 200
WHERE age >= 18
```

### Range Queries
```sql
-- Using BETWEEN
WHERE price BETWEEN 200 AND 500

-- Equivalent to:
WHERE price >= 200 AND price <= 500
```

### Logical Operators
```sql
-- AND - all conditions must be true
WHERE category = 'Electronics' AND available = true AND price < 300

-- OR - at least one condition must be true
WHERE stock = 0 OR available = false
```

### IN Clause
```sql
-- Check if value is in a list
WHERE category IN ('Electronics', 'Furniture')

-- Equivalent to multiple OR conditions
WHERE category = 'Electronics' OR category = 'Furniture'
```

### LIKE Pattern Matching
```sql
-- % matches zero or more characters
WHERE name LIKE '%Desk%'  -- Contains 'Desk'
WHERE name LIKE 'Smart%'  -- Starts with 'Smart'
WHERE name LIKE '%phone'  -- Ends with 'phone'
```

### NULL Handling
```sql
-- Check for NULL values
WHERE category IS NULL

-- Check for non-NULL values
WHERE category IS NOT NULL
```

### Date Comparisons
```sql
-- Compare dates as strings (YYYY-MM-DD format)
WHERE created_at > '2024-02-01'
```

## Sample Data

The example creates a products table with:
- 10 different products
- Categories: Electronics, Furniture
- Prices ranging from $59.99 to $999.99
- Stock levels and availability flags
- Creation dates

## Running the Example

```bash
cd examples/basic-02
go run main.go
```

## Expected Output

```
Sample data inserted successfully

=== Example 1: Simple WHERE with equality ===
Find all Electronics products:
  Headphones - $149.99
  Laptop - $999.99
  Monitor - $299.99
  Smartphone - $699.99
  Tablet - $449.99

=== Example 2: WHERE with comparison operators ===
Products with price less than $200:
  Desk Lamp - $59.99
  Headphones - $149.99

=== Example 3: WHERE with range (BETWEEN) ===
Products with price between $200 and $500:
  Desk Chair - $249.99
  Monitor - $299.99
  Smartphone - $699.99
  Tablet - $449.99

=== Example 4: WHERE with multiple conditions (AND) ===
Available Electronics products under $300:
  Headphones - $149.99 (Stock: 50)
  Monitor - $299.99 (Stock: 15)

=== Example 5: WHERE with OR conditions ===
Products that are either out of stock or unavailable:
  Coffee Table (Stock: 0, Available: false)
  Tablet (Stock: 0, Available: false)

=== Example 6: WHERE with IN clause ===
Products in specific categories:
  ... (all products listed by category)

=== Example 7: WHERE with LIKE (pattern matching) ===
Products with names containing 'Desk':
  Desk Chair
  Desk Lamp
  Office Desk

=== Example 8: WHERE with NOT conditions ===
Products that are NOT in Electronics category:
  ... (Furniture products listed)

=== Example 9: WHERE with NULL handling ===
Products with NULL category:
  Mystery Item (Category: NULL)

=== Example 10: WHERE with date comparisons ===
Products created after February 1, 2024:
  ... (products created after Feb 1)

=== Summary Statistics ===
Total products: 11
Available products: 7
Average price: $361.80
Total inventory value: $38,449.08
```

## Notes

- The example uses an in-memory database
- All WHERE clause examples are demonstrated with practical use cases
- The code shows both single-row queries (`QueryRow`) and multi-row queries (`Query`)
- NULL handling is demonstrated using `sql.NullString` for nullable columns
- Date comparisons work with string format YYYY-MM-DD
- The database is cleaned up at the end of the example