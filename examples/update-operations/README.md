# UPDATE Operations Example

This example demonstrates various UPDATE operations with different types of conditions and techniques in dukdb-go.

## Overview

The example shows how to:
- Update single and multiple columns
- Use calculations in UPDATE statements
- Apply complex WHERE conditions
- Update based on aggregate values
- Use CASE statements for conditional updates
- Update with COALESCE for NULL handling
- Perform conditional updates using Go logic
- Track rows affected by updates

## Key Concepts

### Basic UPDATE Syntax
```sql
-- Update single column
UPDATE table SET column = value WHERE condition

-- Update multiple columns
UPDATE table SET col1 = val1, col2 = val2 WHERE condition

-- Update with calculation
UPDATE products SET price = price * 0.9 WHERE category = 'Electronics'
```

### UPDATE with Complex Conditions
```sql
-- Multiple conditions with AND/OR
UPDATE inventory
SET discount = 15.00
WHERE quantity > 20 AND price < 100 AND category != 'Electronics'

-- Using IN clause
UPDATE products
SET status = 'discontinued'
WHERE id IN (1, 2, 3, 4, 5)
```

### UPDATE with CASE Statement
```sql
UPDATE inventory
SET quantity = CASE
    WHEN quantity < 10 THEN quantity + 20
    WHEN quantity < 25 THEN quantity + 10
    ELSE quantity + 5
END
```

### UPDATE with COALESCE
```sql
-- Handle NULL values
UPDATE products
SET discount = COALESCE(discount, 0.00) + 5.00,
    price = COALESCE(price, 0.00)
WHERE discount IS NULL
```

### Checking Rows Affected
```go
result, err := db.Exec("UPDATE ...", params...)
if err != nil {
    log.Fatal(err)
}
rowsAffected, _ := result.RowsAffected()
fmt.Printf("Updated %d rows\n", rowsAffected)
```

## Sample Data

The example creates an inventory table with:
- 10 different products
- Categories: Electronics, Furniture, Stationery
- Prices ranging from $4.99 to $999.99
- Various quantities in stock
- Timestamp column for last update (can be set manually)
- Discount percentages

## UPDATE Examples Demonstrated

1. **Simple UPDATE**: Update price of a specific product
2. **Multiple Columns**: Update price and quantity together
3. **Calculation-Based**: Apply 10% discount to all Electronics
4. **Complex WHERE**: Apply discount based on multiple conditions
5. **Aggregate-Based**: Update items above category average price
6. **CASE Statement**: Different quantity adjustments based on current stock
7. **LIMIT Simulation**: Update only a subset of matching rows
8. **Simple Arithmetic**: Apply calculations to update prices
9. **COALESCE**: Handle NULL values safely
10. **Conditional Logic**: Use Go code to determine update values

## Running the Example

```bash
cd examples/basic-04
go run main.go
```

## Expected Output

```
Initial inventory data inserted

=== Initial Inventory ===
ID | Product Name         | Category   | Price  | Qty | Discount
---|----------------------|------------|--------|-----|----------
 1 | Laptop               | Electronics| $999.99|  10 |   0.0%
 2 | Mouse                | Electronics| $29.99 |  50 |   0.0%
... (other products)

=== Example 1: Simple UPDATE with WHERE clause ===
Updated 1 item(s): Laptop price reduced to $899.99

=== Example 2: UPDATE multiple columns ===
Updated 1 item(s): Mouse price and quantity adjusted

=== Example 3: UPDATE with calculation ===
Updated 4 item(s): All Electronics discounted by 10%

=== Example 4: UPDATE with complex WHERE conditions ===
Updated 3 item(s): 15% discount applied to qualifying items

=== Example 5: UPDATE based on aggregate ===
Updated 2 item(s): Extra 5% discount for Furniture above average ($179.99)

=== Example 6: UPDATE with CASE statement ===
Updated all quantities based on current stock levels

=== Example 7: UPDATE with LIMIT ===
Updated 2 Stationery items under $10

=== Example 8: UPDATE with simple arithmetic ===
Updated 0 item(s): 5% restocking fee applied to low-quantity items

=== Example 9: UPDATE with COALESCE ===
Updated 2 item(s): Applied minimum discount using COALESCE

=== Example 10: Conditional UPDATE with Go logic ===
Updated 1 high-quantity items with conditional discounts

=== Final Inventory After All Updates ===
ID | Product Name         | Category   | Price  | Qty | Discount
---|----------------------|------------|--------|-----|----------
 1 | Laptop               | Electronics| $899.99|  15 |   0.0%
 2 | Mouse                | Electronics| $24.99 |  80 |   0.0%
... (updated products with discounts)

=== Summary of Changes ===
Total items: 10
Items with discounts: 7 (70.0%)
Average discount: 16.4%

Table dropped successfully
```

## Notes

- The example uses an in-memory database
- All UPDATE operations show the number of affected rows
- Complex updates use transactions implicitly
- The example demonstrates safe NULL handling with COALESCE
- Some advanced features like UPDATE with LIMIT may vary by database
- The inventory table is cleaned up at the end
- Real applications should use transactions for multiple related updates

## Best Practices

1. Always check RowsAffected() to verify updates
2. Use WHERE clauses to avoid updating all rows accidentally
3. Handle NULL values explicitly with COALESCE or IS NULL checks
4. Use prepared statements for repeated updates
5. Consider transactions for multiple related updates
6. Consider adding timestamps when modifying records (if supported by the database)
7. Test UPDATE statements with SELECT first to verify the WHERE clause

## Common Pitfalls

- Forgetting the WHERE clause updates ALL rows
- Not handling NULL values properly
- Using calculations that might cause data type issues
- Not verifying the number of affected rows
- Updating related data without using transactions (in production code)

This example provides a comprehensive overview of UPDATE operations that you'll commonly use in database applications. Each technique is demonstrated with practical examples and proper error handling."}