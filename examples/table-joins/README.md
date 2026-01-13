# Table JOINs Example

This example demonstrates how to use SQL JOINs to combine data from multiple tables in dukdb-go.

## Overview

The example shows how to:
- Use INNER JOIN to get matching records from both tables
- Use LEFT JOIN to get all records from the left table
- Use RIGHT JOIN to get all records from the right table
- Join multiple tables in a single query
- Perform self-joins on the same table
- Simulate FULL OUTER JOIN using UNION
- Use aggregate functions with JOINs
- Write subqueries with JOINs
- Update records using JOINs
- Create complex multi-table JOINs

## Key Concepts

### JOIN Types

#### INNER JOIN
Returns only matching records from both tables.
```sql
SELECT * FROM table1 t1
INNER JOIN table2 t2 ON t1.id = t2.table1_id
```

#### LEFT JOIN (LEFT OUTER JOIN)
Returns all records from the left table, and matching records from the right table.
```sql
SELECT * FROM table1 t1
LEFT JOIN table2 t2 ON t1.id = t2.table1_id
```

#### RIGHT JOIN (RIGHT OUTER JOIN)
Returns all records from the right table, and matching records from the left table.
```sql
SELECT * FROM table1 t1
RIGHT JOIN table2 t2 ON t1.id = t2.table1_id
```

#### FULL OUTER JOIN
Returns all records when there is a match in either table.
Note: Not directly supported in all databases, can be simulated with UNION.

### Multi-Table JOINs
```sql
SELECT * FROM table1 t1
JOIN table2 t2 ON t1.id = t2.table1_id
JOIN table3 t3 ON t2.id = t3.table2_id
```

### Self JOINs
```sql
SELECT * FROM employees e1
JOIN employees e2 ON e1.manager_id = e2.id
```

## Sample Data

The example creates four tables:
- **customers**: Customer information (8 records)
- **orders**: Order headers (11 records, including some with invalid customer IDs)
- **products**: Product catalog (10 records)
- **order_items**: Order line items (17 records)

## JOIN Examples Demonstrated

1. **INNER JOIN**: Get orders with customer information
2. **LEFT JOIN**: Get all customers and their orders
3. **Multiple JOINs**: Orders with items and products
4. **RIGHT JOIN**: Get products that have never been ordered
5. **FULL OUTER JOIN Simulation**: Using UNION for complete results
6. **Self JOIN**: Find customers from the same city
7. **Aggregate with JOINs**: Sales by category and country
8. **Subquery with JOINs**: Top customer per category
9. **Update with JOINs**: Set order status based on customer location
10. **Complex Multi-table JOIN**: Complete order summary with all details

## Running the Example

```bash
cd examples/basic-10
go run main.go
```

## Expected Output

```
=== Basic Example 10: Simple JOIN Operations ===

✓ Sample customers inserted (8 records)
✓ Sample products inserted (10 records)
✓ Sample orders inserted (11 records)
✓ Sample order items inserted (17 records)

=== Example 1: INNER JOIN - Orders with Customer Info ===
Order ID | Customer Name     | Order Date | Total    | Status
---------|-------------------|------------|----------|--------
       1 | John Doe          | 2024-01-15 | $1329.98 | Completed
       2 | Jane Smith        | 2024-01-16 | $ 729.98 | Completed
       3 | Bob Johnson       | 2024-01-17 | $ 249.99 | Shipped
       4 | Alice Williams    | 2024-01-18 | $ 704.98 | Completed
       5 | Charlie Brown     | 2024-01-19 | $  45.99 | Processing
       6 | Diana Prince      | 2024-01-20 | $ 404.98 | Completed
       7 | Edward Norton     | 2024-01-21 | $  79.99 | Shipped
       8 | Fiona Garcia      | 2024-01-22 | $ 454.98 | Completed
       9 | John Doe          | 2024-01-23 | $1549.98 | Processing
      10 | Jane Smith        | 2024-01-24 | $ 154.98 | Shipped

=== Example 2: LEFT JOIN - All Customers and Their Orders ===
Customer Name     | City          | Country | Orders | Total Spent
------------------|---------------|---------|--------|------------
Alice Williams    | Toronto       | Canada  |      1 | $   704.98
Bob Johnson       | Chicago       | USA     |      1 | $   249.99
Charlie Brown     | Vancouver     | Canada  |      1 | $    45.99
Diana Prince      | London        | UK      |      1 | $   404.98
Edward Norton     | Manchester    | UK      |      1 | $    79.99
Fiona Garcia      | Madrid        | Spain   |      1 | $   454.98
Jane Smith        | Los Angeles   | USA     |      2 | $   884.96
John Doe          | New York      | USA     |      2 | $  2879.96

=== Example 3: Multiple JOINs - Order Details ===
Order | Customer          | Product              | Qty | Price   | Total
------|-------------------|----------------------|-----|---------|--------
    1 | John Doe          | Laptop Pro           |   1 | $1299.99 | $1299.99
    1 | John Doe          | Wireless Mouse       |   1 | $  29.99 | $  29.99
    2 | Jane Smith        | Smartphone           |   1 | $ 699.99 | $ 699.99
    2 | Jane Smith        | Keyboard             |   1 | $  79.99 | $  79.99
    3 | Bob Johnson       | Office Chair         |   1 | $ 249.99 | $ 249.99
    4 | Alice Williams    | Smartphone           |   1 | $ 699.99 | $ 699.99
    4 | Alice Williams    | Notebook             |   1 | $   4.99 | $   4.99
    5 | Charlie Brown     | Desk Lamp            |   1 | $  45.99 | $  45.99
    6 | Diana Prince      | Monitor              |   1 | $ 399.99 | $ 399.99
    6 | Diana Prince      | Notebook             |   1 | $   4.99 | $   4.99
    7 | Edward Norton     | Keyboard             |   1 | $  79.99 | $  79.99
    8 | Fiona Garcia      | Tablet               |   1 | $ 449.99 | $ 449.99
    8 | Fiona Garcia      | Wireless Mouse       |   1 | $  24.99 | $  24.99
    9 | John Doe          | Laptop Pro           |   1 | $1299.99 | $1299.99
    9 | John Doe          | Tablet               |   1 | $ 449.99 | $ 449.99
   10 | Jane Smith        | Headphones           |   1 | $ 149.99 | $ 149.99
   10 | Jane Smith        | Notebook             |   1 | $   4.99 | $   4.99

=== Example 4: RIGHT JOIN - Products Never Ordered ===
ID | Product Name         | Category    | Price    | Stock
---|----------------------|-------------|----------|-------
 5 | Desk Lamp            | Furniture   | $  45.99 |    30

=== Example 5: FULL OUTER JOIN Simulation ===
All customers and their orders (including customers without orders and orders without valid customers):
Customer ID | Customer Name     | Country     | Order ID | Amount   | Relationship
------------|-------------------|-------------|----------|----------|----------------------
          4 | Alice Williams    | Canada      |        4 | $  704.98 | Customer with Order
          3 | Bob Johnson       | USA         |        3 | $  249.99 | Customer with Order
          5 | Charlie Brown     | Canada      |        5 | $   45.99 | Customer with Order
          6 | Diana Prince      | UK          |        6 | $  404.98 | Customer with Order
          7 | Edward Norton     | UK          |        7 | $   79.99 | Customer with Order
          8 | Fiona Garcia      | Spain       |        8 | $  454.98 | Customer with Order
          2 | Jane Smith        | USA         |        2 | $  729.98 | Customer with Order
          2 | Jane Smith        | USA         |       10 | $  154.98 | Customer with Order
          1 | John Doe          | USA         |        1 | $ 1329.98 | Customer with Order
          1 | John Doe          | USA         |        9 | $ 1549.98 | Customer with Order
       NULL | Unknown Customer  | Unknown     |       11 | $    0.00 | Order without Valid Customer

=== Example 6: Self JOIN - Customers in Same City ===
Customer 1          | Customer 2          | City          | Country
--------------------|---------------------|---------------|--------

=== Example 7: Aggregate with JOIN - Sales by Category and Country ===
Country | Category    | Orders | Quantity | Total Sales
--------|-------------|--------|----------|------------
Canada  | Electronics |      1 |        1 | $   699.99
Spain   | Electronics |      1 |        2 | $   474.98
UK      | Accessories |      1 |        1 | $    79.99
UK      | Electronics |      1 |        1 | $   399.99
USA     | Accessories |      3 |        5 | $   259.97
USA     | Electronics |      5 |        8 | $  5349.93
USA     | Furniture   |      1 |        1 | $   249.99
USA     | Stationery  |      2 |        2 | $     9.98

=== Example 8: Subquery with JOIN - Top Customer per Category ===
Category    | Top Customer      | Amount Spent
------------|-------------------|--------------
Accessories | Jane Smith        | $      259.97
Electronics | John Doe          | $     5349.93
Furniture   | Bob Johnson       | $      249.99
Stationery  | Jane Smith        | $        9.98

=== Example 9: Update with JOIN - Set Order Status ===
Before update:
  Processing orders for Canadian customers: 1
✓ Updated Canadian orders to Priority status
After update:
  Priority orders for Canadian customers: 1

=== Example 10: Complex Multi-table JOIN - Complete Order Summary ===
Order | Date       | Customer          | Items | Qty | Amount   | Status   | Shipping Type
------|------------|-------------------|-------|-----|----------|----------|------------------
    1 | 2024-01-15 | John Doe          |     2 |   2 | $ 1329.98 | Completed | Local Delivery
    2 | 2024-01-16 | Jane Smith        |     2 |   2 | $  779.98 | Completed | Local Delivery
    3 | 2024-01-17 | Bob Johnson       |     1 |   1 | $  249.99 | Shipped  | Local Delivery
    4 | 2024-01-18 | Alice Williams    |     2 |   2 | $  704.98 | Completed | Local Delivery
    5 | 2024-01-19 | Charlie Brown     |     1 |   1 | $   45.99 | Priority | Local Delivery

=== JOIN Operations Summary ===
Total Customers: 8
Total Orders: 10
Customers with Orders: 8
Average Order Value: $478.49
Products Never Ordered: 1

✓ All tables dropped successfully

=== Summary ===
This example demonstrated:
- INNER JOIN - Matching records in both tables
- LEFT JOIN - All records from left table, matching from right
- Multiple JOINs - Joining more than two tables
- RIGHT JOIN - All records from right table, matching from left
- FULL OUTER JOIN simulation - Using UNION
- Self JOIN - Joining a table to itself
- Aggregate functions with JOINs
- Subqueries with JOINs
- Update statements with JOINs
- Complex multi-table JOINs

All JOIN operations completed successfully!
```

## Notes

- The example uses an in-memory database
- Sample data includes realistic relationships between tables
- Some orders reference non-existent customers to demonstrate JOIN behavior
- RIGHT JOIN shows products that have never been ordered
- All data is cleaned up at the end

## Best Practices

1. **Use explicit JOIN syntax** - Instead of comma-separated tables
2. **Index foreign key columns** - For better JOIN performance
3. **Be consistent with table aliases** - Makes queries easier to read
4. **Use meaningful aliases** - Single letters are fine for small queries
5. **Consider NULL handling** - When joining nullable columns
6. **Test with edge cases** - Like missing related records
7. **Document complex joins** - Add comments for clarity

## Performance Tips

- Create indexes on columns used in JOIN conditions
- Limit the number of rows before joining when possible
- Use appropriate JOIN types for your needs
- Consider denormalization for frequently joined data
- Monitor query execution plans for JOIN performance

## Common Patterns

### Finding Orphans
```sql
-- Records without matching related records
SELECT * FROM table1 t1
LEFT JOIN table2 t2 ON t1.id = t2.table1_id
WHERE t2.id IS NULL
```

### Many-to-Many Relationships
```sql
-- Through junction table
SELECT * FROM table1 t1
JOIN junction j ON t1.id = j.table1_id
JOIN table2 t2 ON j.table2_id = t2.id
```

### Aggregates with JOINs
```sql
SELECT t1.*, COUNT(t2.id) as count, SUM(t2.value) as total
FROM table1 t1
LEFT JOIN table2 t2 ON t1.id = t2.table1_id
GROUP BY t1.id
```

This example provides a comprehensive overview of SQL JOIN operations, demonstrating various types of joins and their practical applications in database queries."}],