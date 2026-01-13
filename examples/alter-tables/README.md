# ALTER TABLE Example

This example demonstrates how to modify existing database tables using ALTER TABLE statements in dukdb-go.

## Overview

The example shows how to:
- Add new columns to existing tables
- Add columns with DEFAULT values
- Add columns with constraints (NOT NULL, CHECK)
- Rename columns
- Modify column data types
- Drop columns from tables
- Rename entire tables
- Add multiple columns
- Add foreign key references
- Handle ALTER TABLE operations that may not be supported

## Key Concepts

### Basic ALTER TABLE Syntax

#### ADD COLUMN
```sql
-- Add simple column
ALTER TABLE table_name ADD COLUMN column_name data_type

-- Add column with DEFAULT
ALTER TABLE table_name ADD COLUMN column_name data_type DEFAULT default_value

-- Add column with constraint
ALTER TABLE table_name ADD COLUMN column_name data_type CONSTRAINT constraint_name
```

#### RENAME COLUMN
```sql
-- Standard syntax
ALTER TABLE table_name RENAME COLUMN old_name TO new_name

-- Alternative syntax (MySQL style)
ALTER TABLE table_name CHANGE old_name new_name data_type
```

#### MODIFY COLUMN
```sql
-- Change data type
ALTER TABLE table_name ALTER COLUMN column_name TYPE new_data_type

-- Alternative syntax
ALTER TABLE table_name MODIFY COLUMN column_name new_data_type
```

#### DROP COLUMN
```sql
ALTER TABLE table_name DROP COLUMN column_name
```

#### RENAME TABLE
```sql
ALTER TABLE old_table_name RENAME TO new_table_name
```

## ALTER TABLE Examples Demonstrated

1. **ADD COLUMN - Simple**: Adding email column
2. **ADD COLUMN with DEFAULT**: Adding hire_date with default value
3. **ADD COLUMN with NOT NULL**: Adding is_active column
4. **RENAME COLUMN**: Renaming department to dept
5. **MODIFY COLUMN**: Changing salary precision
6. **ADD COLUMN with CHECK**: Adding experience_years with constraint
7. **DROP COLUMN**: Removing experience_years column
8. **ADD MULTIPLE COLUMNS**: Adding phone, address, city
9. **RENAME TABLE**: Renaming employees to staff
10. **ADD FOREIGN KEY**: Adding department_id reference

## Running the Example

```bash
cd examples/basic-07
go run main.go
```

## Expected Output

```
=== Basic Example 07: ALTER TABLE Operations ===

=== Step 1: Creating initial employee table ===
✓ Sample employees inserted

=== Initial Table Structure ===
Structure of employees:
CID | Column Name        | Type        | NotNull | Default       | PK
----|-------------------|-------------|---------|---------------|----
  0 | id                | INTEGER     | YES     | NULL          |  1
  1 | first_name        | VARCHAR(50) | YES     | NULL          |  0
  2 | last_name         | VARCHAR(50) | YES     | NULL          |  0
  3 | department        | VARCHAR(50) | NO      | NULL          |  0
  4 | salary            | DECIMAL(10,2)| NO      | NULL          |  0

ID | Name              | Department  | Salary    |
---|-------------------|-------------|-----------|
 1 | John Doe          | Engineering | $75000.00 |
 2 | Jane Smith        | Marketing   | $65000.00 |
 3 | Bob Johnson       | Sales       | $60000.00 |
 4 | Alice Williams    | Engineering | $80000.00 |
 5 | Charlie Brown     | HR          | $55000.00 |

=== Example 1: ADD COLUMN - Adding email column ===
✓ Email column added successfully
✓ Email addresses updated

=== Example 2: ADD COLUMN with DEFAULT value ===
✓ Hire date column added with default value

=== Example 3: ADD COLUMN with NOT NULL constraint ===
✓ Is active column added and populated

=== Example 4: RENAME COLUMN ===
✓ Department column renamed to dept

=== Example 5: MODIFY COLUMN type ===
✓ Salary column type modified to DECIMAL(12,2)

=== Example 6: ADD COLUMN with CHECK constraint ===
✓ Experience years column added with CHECK constraint
✓ Experience years updated

=== Table Structure After Additions ===
Structure of employees:
CID | Column Name        | Type        | NotNull | Default       | PK
----|-------------------|-------------|---------|---------------|----
  0 | id                | INTEGER     | YES     | NULL          |  1
  1 | first_name        | VARCHAR(50) | YES     | NULL          |  0
  2 | last_name         | VARCHAR(50) | YES     | NULL          |  0
  3 | dept              | VARCHAR(50) | NO      | NULL          |  0
  4 | salary            | DECIMAL(12,2)| NO      | NULL          |  0
  5 | email             | VARCHAR(100)| NO      | NULL          |  0
  6 | hire_date         | DATE        | NO      | '2024-01-01'  |  0
  7 | is_active         | BOOLEAN     | NO      | NULL          |  0
  8 | experience_years  | INTEGER     | NO      | NULL          |  0

=== Example 7: DROP COLUMN ===
✓ Experience years column dropped

=== Example 8: ADD MULTIPLE COLUMNS ===
✓ Phone column added
✓ Address column added
✓ City column added

=== Example 9: RENAME TABLE ===
✓ Table renamed from employees to staff

=== Example 10: Add department reference ===
✓ Departments table created and populated
✓ Department ID column added
✓ Department references updated

=== Final Table Structure ===
Structure of staff:
CID | Column Name        | Type        | NotNull | Default       | PK
----|-------------------|-------------|---------|---------------|----
  0 | id                | INTEGER     | YES     | NULL          |  1
  1 | first_name        | VARCHAR(50) | YES     | NULL          |  0
  2 | last_name         | VARCHAR(50) | YES     | NULL          |  0
  3 | dept              | VARCHAR(50) | NO      | NULL          |  0
  4 | salary            | DECIMAL(12,2)| NO      | NULL          |  0
  5 | email             | VARCHAR(100)| NO      | NULL          |  0
  6 | hire_date         | DATE        | NO      | '2024-01-01'  |  0
  7 | is_active         | BOOLEAN     | NO      | NULL          |  0
  8 | phone             | VARCHAR(20) | NO      | NULL          |  0
  9 | address           | VARCHAR(200)| NO      | NULL          |  0
 10 | city              | VARCHAR(50) | NO      | NULL          |  0
 11 | department_id     | INTEGER     | NO      | NULL          |  0

=== Sample Data After All Changes ===
ID | Name              | Department  | Salary    | Email                  | Hire Date  | Active | Phone | Address | City | Dept ID |
---|-------------------|-------------|-----------|------------------------|------------|--------|-------|---------|------|---------|
 1 | John Doe          | Engineering | $75000.00 | john.doe@company.com   | 2024-01-01 | Yes    | NULL  | NULL    | NULL | 1       |
 2 | Jane Smith        | Marketing   | $65000.00 | jane.smith@company.com | 2024-01-01 | Yes    | NULL  | NULL    | NULL | 2       |
 3 | Bob Johnson       | Sales       | $60000.00 | bob.johnson@company.com| 2024-01-01 | Yes    | NULL  | NULL    | NULL | 3       |
 4 | Alice Williams    | Engineering | $80000.00 | alice.williams@company.| 2024-01-01 | Yes    | NULL  | NULL    | NULL | 1       |
 5 | Charlie Brown     | HR          | $55000.00 | charlie.brown@company.c| 2024-01-01 | Yes    | NULL  | NULL    | NULL | 4       |

=== Cleaning Up ===
✓ All tables dropped successfully

=== Summary ===
This example demonstrated:
- ADD COLUMN (simple, with DEFAULT, with constraints)
- RENAME COLUMN (with fallback for unsupported syntax)
- MODIFY COLUMN type (with fallback for unsupported syntax)
- DROP COLUMN (with check for support)
- RENAME TABLE
- ADD MULTIPLE COLUMNS
- ADD FOREIGN KEY references
- Handling operations that may not be supported

Note: ALTER TABLE capabilities vary by database implementation.
Some operations may not be supported in all databases.
```

## Notes

- The example uses an in-memory database
- ALTER TABLE capabilities vary significantly between database systems
- Some operations show fallback approaches for unsupported features
- All changes are demonstrated with practical examples
- The example includes error handling for unsupported operations
- Tables are cleaned up at the end

## Best Practices

1. **Test ALTER operations** - Always test on development data first
2. **Backup data** - Before major schema changes in production
3. **Check support** - Not all databases support all ALTER operations
4. **Handle existing data** - When adding NOT NULL columns
5. **Use transactions** - For multiple related ALTER operations
6. **Document changes** - Keep track of schema modifications
7. **Consider downtime** - Some ALTER operations may lock tables

## Common Pitfalls

- Not all databases support DROP COLUMN
- RENAME COLUMN syntax varies between databases
- MODIFY COLUMN may not be supported
- Adding NOT NULL to existing tables requires careful handling
- Foreign key constraints may require special handling

## ALTER TABLE Support Matrix

| Operation | SQLite | PostgreSQL | MySQL | dukdb-go |
|-----------|--------|------------|-------|----------|
| ADD COLUMN | ✓ | ✓ | ✓ | ✓ |
| RENAME COLUMN | ✓* | ✓ | ✓ | ✓* |
| MODIFY TYPE | ✗ | ✓ | ✓ | ✓* |
| DROP COLUMN | ✗* | ✓ | ✓ | ✓* |
| RENAME TABLE | ✓ | ✓ | ✓ | ✓ |

*May require specific syntax or version

## Safe ALTER Pattern

```go
// 1. Check if operation is supported
_, err := db.Exec("ALTER TABLE users ADD COLUMN email VARCHAR(100)")
if err != nil {
    // Handle unsupported operation
    log.Printf("ALTER TABLE not supported: %v", err)
    // Use alternative approach or skip
}

// 2. For production, always:
// - Backup data first
// - Test on development database
// - Run during low-traffic period
// - Have rollback plan
```

This example provides a comprehensive overview of ALTER TABLE operations with emphasis on handling databases that may not support all features. Always check your specific database documentation for supported operations."}