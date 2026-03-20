# Struct Type Specification

## Requirements

### Requirement: STRUCT Type Definition and DDL

The system MUST implement a STRUCT data type that stores named fields of various types.

#### Scenario: Create table with STRUCT column
```sql
-- Given: A database connection
-- When: I execute:
CREATE TABLE users (
    id INTEGER,
    profile STRUCT(name VARCHAR, age INTEGER, active BOOLEAN)
);
-- Then: The table should be created successfully
-- And: The profile column should have type STRUCT with specified fields
```

#### Scenario: Create table with nested STRUCT
```sql
-- Given: A database connection
-- When: I execute:
CREATE TABLE departments (
    dept_id INTEGER,
    manager STRUCT(
        name VARCHAR,
        contact STRUCT(email VARCHAR, phone VARCHAR)
    )
);
-- Then: The table should be created successfully
-- And: Should support nested STRUCT types
```

#### Scenario: Reject invalid STRUCT definition
```sql
-- Given: A database connection
-- When: I execute:
CREATE TABLE invalid (
    bad_struct STRUCT  -- Missing field definitions
);
-- Then: Should fail with error "STRUCT requires field definitions"
```

### Requirement: STRUCT Construction with struct_pack

The system MUST provide functions to construct STRUCT values.

#### Scenario: Construct STRUCT with struct_pack function
```sql
-- Given: Database connection
-- When: I execute:
SELECT struct_pack(
    name := 'Alice',
    age := 30,
    active := true
) as profile;
-- Then: Should return STRUCT with three fields
-- And: Fields should have specified values
```

#### Scenario: Construct nested STRUCT
```sql
-- Given: Database connection
-- When: I execute:
SELECT struct_pack(
    name := 'Bob',
    contact := struct_pack(
        email := 'bob@example.com',
        phone := '555-1234'
    )
) as manager;
-- Then: Should return nested STRUCT
-- And: Inner STRUCT should have contact fields
```

#### Scenario: Use STRUCT literal syntax
```sql
-- Given: Database connection
-- When: I execute:
SELECT {'name': 'Charlie', 'age': 25} as profile;
-- Then: Should return STRUCT equivalent to struct_pack call
-- And: Field types inferred from values
```

### Requirement: Static Field Access with Dot Notation

The system MUST support static field access on STRUCT columns using dot notation.

#### Scenario: Access structure fields with dot notation
```sql
-- Given: A table with STRUCT column
-- When: I execute:
SELECT
    profile.name,
    profile.age,
    profile.active
FROM users;
-- Then: Should extract each field value
-- And: Return NULL for fields that don't exist
```

#### Scenario: Use fields in WHERE clause
```sql
-- Given: Table with STRUCT column
-- When: I execute:
SELECT * FROM users
WHERE profile.age > 18 AND profile.active = true;
-- Then: Should filter based on struct fields
```

#### Scenario: Use fields in GROUP BY
```sql
-- Given: Table with STRUCT column
-- When: I execute:
SELECT profile.name, COUNT(*) as count
FROM users
GROUP BY profile.name;
-- Then: Should group by field value
```

#### Scenario: Use fields in ORDER BY
```sql
-- Given: Table with STRUCT column
-- When: I execute:
SELECT * FROM users
ORDER BY profile.age DESC;
-- Then: Should sort by field value
```

### Requirement: Dynamic Field Access with struct_extract

The system MUST support dynamic field access on STRUCT values.

#### Scenario: Use struct_extract for field access
```sql
-- Given: Table with STRUCT column
-- When: I execute:
SELECT struct_extract(profile, 'name') as name
FROM users;
-- Then: Should return same as profile.name
-- And: Allow dynamic field specification
```

#### Scenario: Use variable field names
```sql
-- Given: Table with STRUCT column
-- When: I execute:
WITH fields(field_name) AS (
    VALUES ('name'), ('age')
)
SELECT struct_extract(profile, field_name) as value
FROM users, fields;
-- Then: Should extract different fields per row
```

### Requirement: STRUCT Type Consistency

The system MUST enforce type consistency for STRUCT fields.

#### Scenario: Enforce consistent field names
```sql
-- Given: Table with STRUCT column
-- When: I execute:
INSERT INTO users VALUES
    (1, {'name': 'Alice', 'age': 30}),
    (2, {'name': 'Bob', 'age': 25});
-- Then: Should succeed
-- And: Then attempt:
INSERT INTO users VALUES
    (3, {'name': 'Charlie', 'height': 180});
-- Then: Should fail with error "Field 'height' not defined in STRUCT"
```

#### Scenario: Enforce consistent field types
```sql
-- Given: Table with STRUCT column of type STRUCT(name VARCHAR, age INTEGER)
-- When: I attempt:
INSERT INTO users VALUES
    (3, {'name': 'Charlie', 'age': 'thirty'});
-- Then: Should fail or try to cast 'thirty' to INTEGER
```

### Requirement: STRUCT Functions

The system MUST provide utility functions for working with STRUCTs.

#### Scenario: Use struct_insert to add fields
```sql
-- Given: A STRUCT value
-- When: I execute:
SELECT struct_insert(
    profile,
    'email' := 'alice@example.com'
) as extended_profile
FROM users
WHERE id = 1;
-- Then: Should return new STRUCT with additional field
-- Note: This creates new struct, doesn't modify original
```

#### Scenario: Use struct_pack with NULL handling
```sql
-- Given: Data with possible NULLs
-- When: I execute:
SELECT struct_pack(
    name := name_col,
    age := age_col
) as profile
FROM raw_data;
-- Then: Should create STRUCT with NULL for missing values
```

### Requirement: Nested STRUCT Operations

The system MUST support operations on nested STRUCTs.

#### Scenario: Navigate nested structs with dot notation
```sql
-- Given: Table with nested STRUCT
-- When: I execute:
SELECT
    manager.name,
    manager.contact.email,
    manager.contact.phone
FROM departments;
-- Then: Should navigate nested structure
-- And: Return deeply nested fields
```

#### Scenario: Use row_to_json on STRUCT
```sql
-- Given: Table with STRUCT column
-- When: I execute:
SELECT row_to_json(profile) as profile_json
FROM users;
-- Then: Should convert STRUCT to JSON object
-- And: Nested STRUCTs become nested JSON
```

### Requirement: STRUCT in Complex Queries

The system MUST support STRUCT values in complex query clauses.

#### Scenario: Join on struct fields
```sql
-- Given: Two tables with STRUCT columns
-- When: I execute:
SELECT u1.id, u2.id
FROM users u1
JOIN users u2 ON u1.profile.name = u2.profile.name
WHERE u1.id < u2.id;
-- Then: Should join on field values
```

#### Scenario: Use STRUCT in subqueries
```sql
-- Given: Table with STRUCT column
-- When: I execute:
SELECT profile.name
FROM (
    SELECT * FROM users WHERE profile.age > 18
) sub;
-- Then: Should work in subqueries
```

### Requirement: STRUCT Default Values

The system MUST support default values for STRUCT fields.

#### Scenario: Use default values in struct_pack
```sql
-- Given: Database connection
-- When: I execute:
CREATE TABLE products (
    id INTEGER,
    attrs STRUCT(
        color VARCHAR DEFAULT 'blue',
        size INTEGER DEFAULT 1
    )
);
INSERT INTO products(id) VALUES (1);
SELECT * FROM products;
-- Then: Should return STRUCT with default values
```

### Requirement: ALTER TABLE with STRUCT columns

The system MUST support adding STRUCT columns via ALTER TABLE.

#### Scenario: Add STRUCT column to existing table
```sql
-- Given: Existing table
-- When: I execute:
ALTER TABLE users ADD COLUMN metadata STRUCT(tags VARCHAR[]);
-- Then: Should add STRUCT column successfully
```

### Requirement: Expression Framework Updates

The system MUST update the expression framework to support STRUCT operators.

#### Scenario: Add STRUCT field access operators
- **Given** executor operators
- **When** updated
- **Then** StructFieldAccessOperator supports . notation
- **And** StructExtractOperator supports dynamic access
- **And** Type checking validates field existence

### Requirement: Type System Updates

The system MUST update the type system to support STRUCT literals.

#### Scenario: Enhance type inference for struct literals
- **Given** binder expressions
- **When** updated
- **Then** STRUCT literal parsing works
- **And** Type inference determines struct type
- **And** Field type checking ensures consistency

