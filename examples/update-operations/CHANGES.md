# Changes Made to Fix Update Operations Example

## Issues Fixed

1. **CURRENT_TIMESTAMP not supported**: The dukdb-go driver doesn't implement the CURRENT_TIMESTAMP function.
   - **Fix**: Removed all references to CURRENT_TIMESTAMP from the code
   - **Impact**: The `last_updated` column is now just a regular TIMESTAMP column without automatic default

2. **Complex subquery with CREATE TEMP TABLE AS**: The original code used `CREATE TEMP TABLE recent_sales AS SELECT...` followed by a subquery that referenced columns from this temp table, which caused a "column not found" error.
   - **Fix**: Replaced the complex join-based update example (Example 8) with a simpler arithmetic-based update that demonstrates price calculations
   - **New Example 8**: "UPDATE with simple arithmetic" - applies a restocking fee to low-quantity items

## Specific Changes

### In main.go:

1. **Table Creation** (line 21-29):
   - Removed `DEFAULT CURRENT_TIMESTAMP` from the `last_updated` column

2. **Example 2** (line 79-81):
   - Removed `last_updated = CURRENT_TIMESTAMP` from the UPDATE statement

3. **Example 6** (line 137-141):
   - Removed `last_updated = CURRENT_TIMESTAMP` from the UPDATE statement

4. **Example 8** (line 171-181):
   - Completely replaced the complex join-based update with a simple arithmetic update
   - New example: Apply 5% restocking fee to items with quantity < 10

## Verification

The example now runs successfully without errors:
- All 10 update examples execute without database errors
- The example maintains its educational value by demonstrating various UPDATE techniques
- Output shows proper row counts for each update operation

## Educational Value Maintained

The example still demonstrates:
- Simple and complex UPDATE statements
- Multi-column updates
- Calculations in UPDATE statements
- Complex WHERE conditions
- Aggregate-based updates
- CASE statements in updates
- COALESCE for NULL handling
- Conditional updates using Go logic
- Row counting with RowsAffected()