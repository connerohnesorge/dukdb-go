# CRUD Operations Example

This example demonstrates basic CRUD (Create, Read, Update, Delete) operations using dukdb-go.

## Overview

The example shows how to:
- Create a table
- Insert single and multiple records (CREATE)
- Query data with SELECT statements (READ)
- Update existing records (UPDATE)
- Delete records with conditions (DELETE)

## Key Concepts

### Table Creation
```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    age INTEGER
)
```

### Prepared Statements
The example uses prepared statements with `?` placeholders for safe parameter binding:
```go
db.Exec("INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
    1, "Alice Johnson", "alice@example.com", 28)
```

### Error Handling
- Proper error handling for database operations
- Special handling for `sql.ErrNoRows` when querying single records
- Using `RowsAffected()` to verify operation results

### Row Scanning
```go
var id, age int
var name, email string
err := rows.Scan(&id, &name, &email, &age)
```

## Running the Example

```bash
cd examples/basic-01
go run main.go
```

## Expected Output

```
Table 'users' created successfully

=== INSERT Operations ===
Inserted 1 row(s)
Inserted user: Bob Smith
Inserted user: Carol Davis
Inserted user: David Wilson

=== SELECT Operations ===
All users:
  ID: 1, Name: Alice Johnson, Email: alice@example.com, Age: 28
  ID: 2, Name: Bob Smith, Email: bob@example.com, Age: 35
  ID: 3, Name: Carol Davis, Email: carol@example.com, Age: 42
  ID: 4, Name: David Wilson, Email: david@example.com, Age: 31

User with ID 2: Name=Bob Smith, Email=bob@example.com, Age=35

=== UPDATE Operations ===
Updated 1 row(s)
Updated 1 row(s) for Bob Smith

After updates:
  ID: 1, Name: Alice Johnson, Email: alice@example.com, Age: 29
  ID: 2, Name: Bob Smith, Email: bob.smith@newdomain.com, Age: 36
  ID: 3, Name: Carol Davis, Email: carol@example.com, Age: 42
  ID: 4, Name: David Wilson, Email: david@example.com, Age: 31

=== DELETE Operations ===
Deleted 1 row(s)
Deleted 1 user(s) with age > 40

After deletions:
  ID: 1, Name: Alice Johnson, Email: alice@example.com, Age: 29
  ID: 2, Name: Bob Smith, Email: bob.smith@newdomain.com, Age: 36
Total users remaining: 2

Final count: 2 users
```

## Notes

- The example uses an in-memory database (empty connection string)
- All operations are performed within the same connection
- The database is automatically cleaned up when the program exits
- This is a simplified example; in production, you would typically use connection pooling and more sophisticated error handling