# DELETE Operations Example

This example demonstrates various DELETE operations with different types of conditions and safe deletion practices in dukdb-go.

## Overview

The example shows how to:
- Delete records with simple WHERE conditions
- Use multiple conditions with AND/OR
- Delete with IN clause
- Delete based on date comparisons
- Use pattern matching with LIKE
- Delete with NOT conditions
- Use BETWEEN for range-based deletion
- Delete with subqueries
- Perform conditional deletion using Go logic
- Preview deletions before executing
- Track rows affected by deletions

## Key Concepts

### Basic DELETE Syntax
```sql
-- Delete specific record
DELETE FROM table WHERE id = 123

-- Delete multiple records
DELETE FROM products WHERE category = 'Obsolete'

-- Delete all records (DANGEROUS - use with caution!)
DELETE FROM table
```

### DELETE with Conditions
```sql
-- Multiple conditions
DELETE FROM tasks WHERE priority = 'Low' AND status = 'Not Started'

-- OR conditions
DELETE FROM inventory WHERE quantity = 0 OR expired = true

-- IN clause
DELETE FROM users WHERE role IN ('temp', 'guest', 'inactive')

-- Date conditions
DELETE FROM logs WHERE created_date < '2023-01-01'
```

### DELETE with Pattern Matching
```sql
-- Delete with LIKE
DELETE FROM products WHERE name LIKE '%obsolete%'

-- Delete with NOT
DELETE FROM tasks WHERE priority != 'High'
```

### Safe DELETE Practices
```sql
-- Always check what will be deleted first
SELECT * FROM table WHERE condition

-- Then run the DELETE
DELETE FROM table WHERE condition

-- Check rows affected
-- In Go: result.RowsAffected()
```

## Sample Data

The example creates a tasks table with:
- 12 different tasks
- Priorities: High, Medium, Low
- Statuses: Not Started, In Progress, Completed
- Different assignees
- Various due dates and completion percentages
- Estimated hours for each task

## DELETE Examples Demonstrated

1. **Simple DELETE**: Remove a specific task by ID
2. **AND Conditions**: Delete low priority tasks that haven't started
3. **OR Conditions**: Delete quick tasks or completed tasks
4. **IN Clause**: Delete all tasks assigned to specific people
5. **Date Conditions**: Delete overdue tasks
6. **LIKE Pattern**: Delete tasks with specific keywords
7. **NOT Conditions**: Delete non-high priority, non-in-progress tasks
8. **BETWEEN Range**: Delete partially completed tasks
9. **Subquery-Based**: Delete tasks taking longer than average
10. **Conditional Logic**: Use Go code to determine what to delete

## Running the Example

```bash
cd examples/basic-05
go run main.go
```

## Expected Output

```
Initial tasks inserted

=== Initial Tasks ===
ID | Task Name                    | Priority | Status      | Assigned | Due Date   | Comp% | Hours
---|------------------------------|----------|-------------|----------|------------|-------|------
 1 | Design Database Schema       | High     | Completed   | Alice    | 2024-01-05 |   100 |  8.0
 2 | Implement API Endpoints      | High     | In Progress | Bob      | 2024-01-10 |    60 | 16.0
... (all 12 tasks)

=== Example 1: Simple DELETE with WHERE clause ===
Deleted 1 task(s): Release Preparation removed

=== Example 2: DELETE with multiple conditions (AND) ===
Deleted 2 task(s): Low priority, not started tasks removed

=== Example 3: DELETE with OR conditions ===
Deleted 5 task(s): Quick tasks (<3 hours) or completed tasks removed

=== Example 4: DELETE with IN clause ===
Deleted 4 task(s): Tasks assigned to David and Charlie removed

=== Example 5: DELETE with date conditions ===
Deleted 1 task(s): Overdue tasks (due before Jan 10) removed

=== Example 6: DELETE with LIKE pattern matching ===
Deleted 1 task(s): Tasks with 'Testing' in name removed

=== Example 7: DELETE with NOT conditions ===
Deleted 1 task(s): Non-high priority, non-in-progress tasks removed

=== Example 8: DELETE with BETWEEN range ===
Deleted 1 task(s): Tasks 20-50% complete removed

=== Example 9: DELETE with subquery ===
Deleted 1 task(s): Tasks taking 1.5x average time removed

=== Example 10: Conditional DELETE with Go logic ===
Deleted task: Code Review (Priority: Medium, Status: In Progress, Completion: 30%)
Total tasks deleted with conditional logic: 1

=== Remaining Tasks After Deletions ===
ID | Task Name                    | Priority | Status      | Assigned | Due Date   | Comp% | Hours
---|------------------------------|----------|-------------|----------|------------|-------|------
 2 | Implement API Endpoints      | High     | In Progress | Bob      | 2024-01-10 |    60 | 16.0
 9 | Security Audit               | High     | In Progress | Alice    | 2024-01-11 |    45 | 10.0

=== Deletion Summary ===
Tasks remaining: 2
Tasks deleted: 10

=== Safe DELETE Practices ===
Always use WHERE clause to avoid deleting all records!
Before DELETE, you can check what will be deleted with SELECT:
High priority tasks that would be deleted:
  ID 2: Implement API Endpoints
  ID 9: Security Audit

Table dropped successfully

=== DELETE Operations Summary ===
This example demonstrated:
- Simple DELETE with WHERE clause
- DELETE with multiple AND conditions
- DELETE with OR conditions
- DELETE with IN clause
- DELETE with date comparisons
- DELETE with LIKE pattern matching
- DELETE with NOT conditions
- DELETE with BETWEEN range
- DELETE with subqueries
- Conditional DELETE using Go logic
- Safe DELETE practices (preview before delete)

All operations completed successfully!
```

## Notes

- The example uses an in-memory database
- All DELETE operations show the number of affected rows
- The example demonstrates safe deletion practices
- Complex deletions use various WHERE conditions
- The example shows how to preview deletions with SELECT
- All data is cleaned up at the end

## Best Practices

1. **Always use WHERE clause** - Unless you intend to delete all records
2. **Preview first** - Run SELECT with same WHERE to see what will be deleted
3. **Check RowsAffected()** - Verify how many rows were actually deleted
4. **Use transactions** - For multiple related deletions (in production)
5. **Soft delete option** - Consider marking as deleted instead of actual deletion
6. **Backup data** - Especially in production environments
7. **Test conditions** - Ensure WHERE clause matches intended records

## Common Pitfalls

- Forgetting WHERE clause deletes ALL records
- Not checking foreign key constraints
- Deleting records that are referenced elsewhere
- Not verifying the number of affected rows
- Using complex OR conditions incorrectly

## Safe DELETE Pattern

```go
// 1. Preview what will be deleted
rows, _ := db.Query("SELECT id FROM users WHERE last_login < ?", cutoffDate)
// Review the results...

// 2. Execute the DELETE
result, err := db.Exec("DELETE FROM users WHERE last_login < ?", cutoffDate)
if err != nil {
    log.Fatal(err)
}

// 3. Verify rows affected
rowsAffected, _ := result.RowsAffected()
fmt.Printf("Deleted %d users\n", rowsAffected)
```

This example provides a comprehensive overview of DELETE operations with emphasis on safety and verification. Always remember that DELETE is permanent - deleted data cannot be recovered!"},