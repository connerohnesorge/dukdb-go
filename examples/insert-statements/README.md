# INSERT Statements Example

This example demonstrates how to insert data of different types into a dukdb-go database, including handling NULL values, binary data, and JSON.

## Overview

The example shows how to:
- Insert various numeric types (INTEGER, SMALLINT, BIGINT, DECIMAL, NUMERIC)
- Work with string types (VARCHAR, TEXT, CHAR)
- Insert date and time values (DATE, TIMESTAMP, TIME)
- Handle boolean values
- Insert floating point numbers (FLOAT, DOUBLE)
- Store binary data (BLOB)
- Store JSON data
- Handle NULL values
- Use bulk inserts with prepared statements
- Perform type conversions

## Key Concepts

### Data Types Supported

#### Numeric Types
```sql
-- Integer types
INTEGER    -- Standard integer (-2,147,483,648 to 2,147,483,647)
SMALLINT   -- Small integer (-32,768 to 32,767)
BIGINT     -- Big integer (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)

-- Decimal types
DECIMAL(p,s)  -- Fixed-point decimal
NUMERIC(p,s)  -- Same as DECIMAL

-- Floating point
FLOAT       -- 32-bit floating point
DOUBLE      -- 64-bit floating point
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
TIMESTAMP   -- Date and time (YYYY-MM-DD HH:MM:SS)
TIME        -- Time of day (HH:MM:SS)
```

#### Other Types
```sql
BOOLEAN     -- true/false
BLOB        -- Binary large object
```

### Inserting NULL Values
```go
// Pass nil for NULL values
db.Exec("INSERT INTO table (nullable_column) VALUES (?)", nil)

// Or use sql.Null types for better control
var nullPrice sql.NullFloat64
if someCondition {
    nullPrice.Valid = true
    nullPrice.Float64 = 99.99
} else {
    nullPrice.Valid = false  // This will insert NULL
}
db.Exec("INSERT INTO products (price) VALUES (?)", nullPrice)
```

### Inserting Binary Data
```go
binaryData := []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F} // "Hello"
db.Exec("INSERT INTO products (binary_data) VALUES (?)", binaryData)
```

### Inserting JSON Data
```go
jsonData := `{"color": "blue", "size": "large"}`
db.Exec("INSERT INTO products (metadata) VALUES (?)", jsonData)
```

### Bulk Inserts with Prepared Statements
```go
stmt, err := db.Prepare("INSERT INTO products (id, name) VALUES (?, ?)")
defer stmt.Close()

for _, product := range products {
    stmt.Exec(product.ID, product.Name)
}
```

### Type Conversions
DukDB automatically converts compatible types:
```go
// String "123.45" will be converted to number 123.45
db.Exec("INSERT INTO products (price) VALUES (?)", "123.45")
```

## Running the Example

```bash
cd examples/basic-03
go run main.go
```

## Expected Output

```
Table 'data_types_demo' created successfully

=== Example 1: Basic data types ===
Basic data inserted successfully

=== Example 2: Insert with NULL values ===
Data with NULLs inserted successfully

=== Example 3: Binary data (BLOB) ===
Binary data inserted successfully

=== Example 4: JSON data ===
JSON data inserted successfully

=== Example 5: Current timestamp ===
Data with current timestamp inserted successfully

=== Example 6: Bulk insert with prepared statement ===
Bulk data inserted successfully

=== Example 7: Type conversions ===
String to number conversion successful

=== Displaying all inserted data ===
ID | Name                 | Price    | Active | Birth Date | Blob Size | JSON Size
---|----------------------|----------|--------|------------|-----------|----------
 1 | Sample Product       | $1234.56 | true   | 1990-05-15 |      NULL |     NULL
 2 | Product with NULLs   | NULL     | false  | NULL       |      NULL |     NULL
 3 | Binary Data Product  | NULL     | NULL   | NULL       |        10 |     NULL
 4 | JSON Product         | NULL     | NULL   | NULL       |      NULL |       49
 5 | Current Time Product | NULL     | NULL   | NULL       |      NULL |     NULL
... (bulk insert products)
11 | String to Number     | $123.45  | NULL   | NULL       |      NULL |     NULL

=== Verifying binary data ===
Retrieved binary data: [72 101 108 108 111 32 66 76 79 66]
As string: Hello BLOB

=== Verifying JSON data ===
Retrieved JSON data: {"color": "blue", "size": "large", "tags": ["new", "featured"]}

Table dropped successfully

=== Summary ===
This example demonstrated:
- Various numeric types (INTEGER, SMALLINT, BIGINT, DECIMAL, NUMERIC)
- String types (VARCHAR, TEXT, CHAR)
- Date/Time types (DATE, TIMESTAMP, TIME)
- Boolean type
- Floating point types (FLOAT, DOUBLE)
- Binary data (BLOB)
- JSON data (stored as VARCHAR)
- NULL value handling
- Bulk inserts with prepared statements
- Type conversions
- Retrieving and displaying various data types

All operations completed successfully!
```

## Notes

- The example uses an in-memory database
- NULL values are handled using Go's `nil` and `sql.Null*` types
- Binary data is stored as BLOB and can be retrieved as []byte
- JSON data is stored as VARCHAR/STRING in the database
- Current timestamps are inserted using Go's `time.Now()`
- Bulk inserts use prepared statements for better performance
- The database automatically handles type conversions when possible
- All data is cleaned up at the end of the example