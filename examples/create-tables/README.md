# CREATE TABLE Example

This example demonstrates how to create tables with various column types, constraints, and options in dukdb-go.

## Overview

The example shows how to:
- Create tables with different data types
- Use column constraints (NOT NULL, UNIQUE, CHECK, DEFAULT)
- Define primary keys and auto-increment columns
- Create indexes on frequently queried columns
- Handle nullable vs non-nullable columns
- Set up default values for columns
- Create tables for specific use cases (products, users, events, etc.)

## Key Concepts

### Basic CREATE TABLE Syntax
```sql
CREATE TABLE table_name (
    column1 datatype constraint,
    column2 datatype constraint,
    ...
)
```

### Common Data Types

#### Numeric Types
```sql
INTEGER     -- Whole numbers (-2,147,483,648 to 2,147,483,647)
SMALLINT    -- Small integers (-32,768 to 32,767)
BIGINT      -- Large integers (-9.2 quintillion to 9.2 quintillion)
DECIMAL(p,s)-- Fixed-point decimal (precision p, scale s)
NUMERIC(p,s)-- Same as DECIMAL
FLOAT       -- 32-bit floating point
DOUBLE      -- 64-bit floating point
BOOLEAN     -- true/false
```

#### String Types
```sql
VARCHAR(n)  -- Variable-length string up to n characters
TEXT        -- Unlimited length text
CHAR(n)     -- Fixed-length string of exactly n characters
```

#### Date/Time Types
```sql
DATE        -- Date (YYYY-MM-DD)
TIME        -- Time of day (HH:MM:SS)
TIMESTAMP   -- Date and time (YYYY-MM-DD HH:MM:SS)
```

#### Binary Type
```sql
BLOB        -- Binary large object
```

### Column Constraints

#### NOT NULL
```sql
column_name VARCHAR(100) NOT NULL  -- Must have a value
```

#### UNIQUE
```sql
column_name VARCHAR(50) UNIQUE  -- Values must be unique
```

#### PRIMARY KEY
```sql
id INTEGER PRIMARY KEY  -- Uniquely identifies each row
id INTEGER PRIMARY KEY AUTOINCREMENT  -- Auto-generates values
```

#### CHECK
```sql
age INTEGER CHECK (age >= 18 AND age <= 120)
quantity INTEGER CHECK (quantity >= 0)
```

#### DEFAULT
```sql
is_active BOOLEAN DEFAULT true
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
```

### Creating Indexes
```sql
CREATE INDEX index_name ON table_name(column_name)
CREATE UNIQUE INDEX idx_email ON users(email)
```

## Table Examples Demonstrated

1. **Products Table**: Basic table with common types
2. **Documents Table**: Various string types (VARCHAR, TEXT, CHAR)
3. **Financial Records**: Numeric types with precision
4. **Events Table**: Date and time types
5. **Users Table**: Constraints (UNIQUE, CHECK, NOT NULL)
6. **Orders Table**: Nullable vs non-nullable columns
7. **Products Catalog**: With indexes
8. **Categories Table**: Self-referencing foreign key
9. **Invoices Table**: Computed/generated columns
10. **API Logs Table**: For storing JSON data

## Running the Example

```bash
cd examples/basic-06
go run main.go
```

## Expected Output

```
=== Basic Example 06: CREATE TABLE with Different Column Types ===

=== Example 1: Basic table with common data types ===
✓ Products table created successfully

=== Example 2: Table with various string types ===
✓ Documents table created successfully

=== Example 3: Table with numeric precision ===
✓ Financial records table created successfully

=== Example 4: Table with date and time types ===
✓ Events table created successfully

=== Example 5: Table with various constraints ===
✓ Users table created successfully

=== Example 6: Table with nullable vs non-nullable columns ===
✓ Orders table created successfully

=== Example 7: Table with indexes ===
✓ Products catalog table created successfully
  - Index on SKU created
  - Index on category_id created

=== Example 8: Table with foreign key relationship ===
✓ Categories table created successfully

=== Example 9: Table with default values ===
✓ Invoices table created successfully (with computed columns)

=== Example 10: Table for storing JSON data ===
✓ API logs table created successfully

=== Testing Data Insertion ===
✓ Sample products inserted
✓ Sample events inserted
✓ Sample users inserted

=== Table Structures Summary ===
- products: 3 rows
- documents: 0 rows
- financial_records: 0 rows
- events: 2 rows
- users: 2 rows
- orders: 0 rows
- products_catalog: 0 rows
- categories: 0 rows
- invoices: 0 rows
- api_logs: 0 rows

=== Testing Constraints ===
✓ UNIQUE constraint prevented duplicate username
✓ CHECK constraint prevented invalid age
✓ NOT NULL constraint prevented NULL username

=== Cleaning Up ===
All tables dropped successfully

=== Summary ===
This example demonstrated:
- Basic data types (INTEGER, VARCHAR, DECIMAL, BOOLEAN, TIMESTAMP)
- String types with different lengths (VARCHAR, TEXT, CHAR)
- Numeric types with precision (DECIMAL, NUMERIC, SMALLINT, BIGINT)
- Date and time types (DATE, TIME, TIMESTAMP)
- Table constraints (PRIMARY KEY, NOT NULL, UNIQUE, CHECK, DEFAULT)
- Nullable vs non-nullable columns
- Creating indexes on frequently queried columns
- Self-referencing tables (parent-child relationships)
- Tables for storing JSON data (as TEXT)
- Default values and auto-increment columns

All operations completed successfully!

Note: Some advanced features like computed columns and foreign key constraints
may vary depending on the database engine implementation.
```

## Notes

- The example uses an in-memory database
- All tables are created with various column types and constraints
- The example demonstrates constraint enforcement
- Indexes are created for performance optimization
- Some advanced features may vary by database implementation
- All tables are cleaned up at the end

## Best Practices

1. **Choose appropriate data types** - Use the smallest type that fits your data
2. **Use NOT NULL** - When a column should always have a value
3. **Add CHECK constraints** - To enforce business rules
4. **Create indexes** - On columns used in WHERE clauses or JOINs
5. **Use UNIQUE** - For columns that must have unique values
6. **Set sensible defaults** - For columns that can have default values
7. **Document your schema** - Add comments about table purpose
8. **Consider nullable carefully** - NULL has special handling in SQL

## Common Pitfalls

- Using VARCHAR without length limit when not needed
- Not adding indexes on frequently queried columns
- Forgetting NOT NULL on required columns
- Using inappropriate numeric types (too small or too large)
- Not handling time zones properly with TIMESTAMP
- Forgetting to add constraints that enforce business rules

This example provides a comprehensive overview of creating tables with various column types and constraints that you'll commonly use in database applications."}