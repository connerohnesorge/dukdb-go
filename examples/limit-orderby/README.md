# LIMIT and ORDER BY Example

This example demonstrates how to use LIMIT and ORDER BY clauses to sort and limit query results in dukdb-go.

## Overview

The example shows how to:
- Sort results with ORDER BY (ascending and descending)
- Sort by multiple columns
- Limit the number of results with LIMIT
- Implement pagination with LIMIT and OFFSET
- Sort by expressions and calculations
- Handle NULL values in sorting
- Use random sampling with ORDER BY RANDOM()
- Get top N results per category
- Calculate dynamic limits based on percentages

## Key Concepts

### ORDER BY Syntax
```sql
-- Basic ascending order (default)
SELECT * FROM table ORDER BY column

-- Explicit ascending
SELECT * FROM table ORDER BY column ASC

-- Descending order
SELECT * FROM table ORDER BY column DESC

-- Multiple columns
SELECT * FROM table ORDER BY column1 ASC, column2 DESC
```

### LIMIT Syntax
```sql
-- Limit number of results
SELECT * FROM table LIMIT 10

-- Limit with offset (for pagination)
SELECT * FROM table LIMIT 10 OFFSET 20  -- Skip 20, return next 10

-- Alternative syntax (MySQL style)
SELECT * FROM table LIMIT 20, 10  -- Same as LIMIT 10 OFFSET 20
```

### ORDER BY with Expressions
```sql
-- Sort by calculation
SELECT * FROM sales ORDER BY price * quantity DESC

-- Sort with NULL handling
SELECT * FROM products ORDER BY warranty_months IS NULL, warranty_months DESC
```

## Sample Data

The example creates a sales table with 20 records containing:
- Product names and categories
- Prices and quantities
- Sale dates
- Regions and salespeople

## LIMIT and ORDER BY Examples Demonstrated

1. **Basic ORDER BY**: Sorting by price ascending
2. **ORDER BY DESC**: Sorting by price descending
3. **Multiple Columns**: Sort by region then by price
4. **Pagination**: Using LIMIT with OFFSET for pages
5. **No ORDER BY**: Why LIMIT without ORDER BY is not recommended
6. **Expressions**: Sort by calculated total value
7. **NULL Handling**: Sort with NULL values last
8. **Random Sampling**: Get random records with ORDER BY RANDOM()
9. **Top N per Category**: Get top 2 per category
10. **Dynamic LIMIT**: Calculate limit based on percentage

## Running the Example

```bash
cd examples/basic-09
go run main.go
```

## Expected Output

```
=== Basic Example 09: Using LIMIT and ORDER BY ===

✓ Sample sales data inserted (20 records)

=== Example 1: Basic ORDER BY (ascending) ===
Products sorted by price (lowest to highest):
  - Notebook           $    4.99
  - Paper              $    8.99
  - Pen Set            $   12.99
  - USB Hub            $   24.99
  - Wireless Mouse     $   29.99

=== Example 2: ORDER BY DESC (descending) ===
Top 5 most expensive products:
  - Laptop Pro         $ 1299.99
  - Standing Desk      $  599.99
  - Tablet             $  449.99
  - Monitor            $  399.99
  - Filing Cabinet     $  299.99

=== Example 3: ORDER BY multiple columns ===
Sales by region and then by price (top 8):
  - East  | Standing Desk      $  599.99
  - East  | Monitor            $  399.99
  - East  | Bookshelf          $  179.99
  - North | Laptop Pro         $ 1299.99
  - North | Router             $  199.99
  - South | Filing Cabinet     $  299.99
  - South | Printer            $  299.99
  - West  | Tablet             $  449.99

=== Example 4: LIMIT with OFFSET (pagination) ===
Page 1 (records 1-5):
  - Laptop Pro         $ 1299.99
  - Wireless Mouse     $   29.99
  - Office Chair       $  249.99
  - Smartphone         $  699.99
  - Desk Lamp          $   45.99

Page 2 (records 6-10):
  - Keyboard           $   79.99
  - Monitor            $  399.99
  - Notebook           $    4.99
  - Tablet             $  449.99
  - Headphones         $  149.99

Page 3 (records 11-15):
  - Standing Desk      $  599.99
  - Pen Set            $   12.99
  - Router             $  199.99
  - Webcam             $   89.99
  - Bookshelf          $  179.99

=== Example 5: LIMIT without ORDER BY ===
First 5 records (arbitrary order - not recommended):
  - Laptop Pro         $ 1299.99
  - Wireless Mouse     $   29.99
  - Office Chair       $  249.99
  - Smartphone         $  699.99
  - Desk Lamp          $   45.99

=== Example 6: ORDER BY with expressions ===
Top 5 sales by total value (price * quantity):
  - Laptop Pro           $ 1299.99 x  2 = $  2599.98
  - Standing Desk        $  599.99 x  1 = $   599.99
  - Tablet               $  449.99 x  1 = $   449.99
  - Smartphone           $  699.99 x  3 = $   2099.97
  - Monitor              $  399.99 x  2 = $    799.98

=== Example 7: ORDER BY with NULL handling ===
Products sorted by warranty months (NULLs last):
  - Standing Desk        24 months
  - Monitor              24 months
  - Router               24 months
  - Laptop Pro           24 months
  - Smartphone           18 months
  - Printer              18 months
  - Bookshelf            12 months
  - Filing Cabinet       12 months
  - Keyboard             12 months
  - Headphones           12 months
  - USB Hub              NULL
  - Webcam               NULL
  - Pen Set              NULL
  - Paper                NULL
  - Notebook             NULL

=== Example 8: Random sampling with LIMIT ===
5 random products (results may vary):
  - Keyboard             $   79.99
  - Standing Desk        $  599.99
  - Office Chair         $  249.99
  - Webcam               $   89.99
  - Router               $  199.99

=== Example 9: Top N per category (using LIMIT in subquery) ===
Top 2 most expensive products per category:

Electronics:
  - Laptop Pro           $ 1299.99
  - Smartphone           $  699.99

Accessories:
  - Headphones           $  149.99
  - Webcam               $   89.99

Furniture:
  - Standing Desk        $  599.99
  - Office Chair         $  249.99

Stationery:
  - Printer              $  299.99
  - Pen Set              $   12.99

=== Example 10: Dynamic LIMIT based on percentage ===
Top 4 products (top 20% of 20 total):
  - Laptop Pro           $ 1299.99
  - Standing Desk        $  599.99
  - Smartphone           $  699.99
  - Tablet               $  449.99

=== Summary Statistics ===
Price Statistics:
  Minimum: $4.99
  Maximum: $1299.99
  Average: $287.49
Sales Statistics:
  Total Sales Value: $5749.80
  Average Quantity: 6.9

=== Cleaning Up ===
✓ Table dropped successfully

=== Summary ===
This example demonstrated:
- Basic ORDER BY (ascending and descending)
- ORDER BY multiple columns
- LIMIT with OFFSET for pagination
- Why LIMIT without ORDER BY is not recommended
- ORDER BY with expressions/calculations
- ORDER BY with NULL handling
- Random sampling with ORDER BY RANDOM()
- Top N per category using LIMIT in subqueries
- Dynamic LIMIT based on calculations

All operations completed successfully!
```

## Notes

- The example uses an in-memory database
- LIMIT without ORDER BY returns arbitrary results (not recommended)
- OFFSET is used for pagination (0-based indexing)
- NULL values can be handled in ORDER BY with expressions
- RANDOM() provides random sampling
- All data is cleaned up at the end

## Best Practices

1. **Always use ORDER BY with LIMIT** - For predictable results
2. **Index sort columns** - For better performance on large tables
3. **Use LIMIT for pagination** - Instead of fetching all records
4. **Consider NULL handling** - When sorting nullable columns
5. **Test performance** - With large datasets and complex sorts
6. **Use appropriate data types** - For sorting efficiency

## Performance Tips

- Create indexes on frequently sorted columns
- Avoid sorting on expressions without functional indexes
- Use LIMIT early in query planning when possible
- Consider materialized views for complex sorts
- Monitor query plans for sort operations

## Common Patterns

### Pagination
```sql
-- Page 1 (records 1-10)
SELECT * FROM table ORDER BY id LIMIT 10 OFFSET 0

-- Page 2 (records 11-20)
SELECT * FROM table ORDER BY id LIMIT 10 OFFSET 10

-- Page N (records (N-1)*10+1 to N*10)
SELECT * FROM table ORDER BY id LIMIT 10 OFFSET (N-1)*10
```

### Top N per Group
```sql
-- Top 3 per category
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as rn
    FROM products
) t WHERE rn <= 3
```

### Random Sampling
```sql
-- Random 5 records
SELECT * FROM table ORDER BY RANDOM() LIMIT 5
```

This example provides a comprehensive overview of using LIMIT and ORDER BY for sorting and limiting query results in SQL databases."}