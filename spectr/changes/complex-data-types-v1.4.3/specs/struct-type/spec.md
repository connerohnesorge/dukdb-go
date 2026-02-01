# Specification: STRUCT Type Implementation

## Overview
This spec implements the STRUCT data type in dukdb-go for DuckDB v1.4.3 compatibility, enabling named field access and nested data structures.

## ADDED Requirements

### Requirement 1: STRUCT Type Definition and DDL
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

### Requirement 2: STRUCT Construction with struct_pack
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

### Requirement 3: Static Field Access with Dot Notation
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

### Requirement 4: Dynamic Field Access with struct_extract
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

### Requirement 5: STRUCT Type Consistency
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

### Requirement 6: STRUCT Functions
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

### Requirement 7: Nested STRUCT Operations
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

### Requirement 8: STRUCT in Complex Queries
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

### Requirement 9: STRUCT Default Values
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

### Requirement 10: ALTER TABLE with STRUCT columns
#### Scenario: Add STRUCT column to existing table
```sql
-- Given: Existing table
-- When: I execute:
ALTER TABLE users ADD COLUMN metadata STRUCT(tags VARCHAR[]);
-- Then: Should add STRUCT column successfully
```

## MODIFIED Requirements

### Expression Framework
#### Scenario: Add STRUCT field access operators
```
-- In: internal/executor/operators.go
-- Add: StructFieldAccessOperator for . notation
-- Add: StructExtractOperator for dynamic access
-- Add: Type checking for field existence
```

### Type System
#### Scenario: Enhance type inference for struct literals
```
-- In: internal/binder/expressions.go
-- Add: STRUCT literal parsing and type inference
-- Add: Field type checking and consistency validation
```

## REMOVED Requirements
*None - purely additive*

## STORAGE IMPLEMENTATION

### Physical Layout
```
StructVector {
    field_vectors: {
        "name": StringVector {...},
        "age": IntVector {...},
        "active": BoolVector {...}
    },
    validity: ValidityMask {...}
}
```

### Serialization Format
- Field names stored in schema only once
- Each field vector serialized independently
- ValidityMask applies to entire struct
- Schema evolution: New fields = NULL for old rows

## PERFORMANCE CHARACTERISTICS

### Field Access Patterns
- Static access (dot notation): O(1) vector indexing
- Dynamic access (struct_extract): O(log n) field lookup
- Nested access: O(d) where d = nesting depth

### Memory Overhead
- Field names stored once per schema (negligible)
- ValidityMask: 1 bit per row
- No per-field validity (struct is atomic)

### Optimization Opportunities
- Inline small structs (< 64 bytes)
- Cache field name to index mapping
- Vectorized operations on each field independently

## TEST COVERAGE

### Unit Tests (20 scenarios)
- STRUCT creation and validation
- Field access operations
- Type consistency enforcement
- NULL handling
- Nested structure navigation

### Integration Tests (10 scenarios)
- CREATE TABLE with STRUCT
- INSERT/SELECT workflows
- Struct in WHERE/GROUP BY/ORDER BY
- JOIN on struct fields
- COPY TO/FROM operations

### Compatibility Tests (5 scenarios)
- DuckDB STRUCT literal syntax
- function-based struct_pack
- Nested struct creation
- Field access methods
- SQL standard compliance

## ACCEPTANCE CRITERIA

1. All scenarios above pass matching DuckDB v1.4.3 behavior
2. Field access performance within 100ns per lookup
3. Memory overhead < 20% compared to flat columns
4. Type safety enforced at INSERT time
5. Error messages clearly identify field/type mismatches
6. Integration with existing dukdb-go type system

## KNOWN LIMITATIONS (by design)

- All rows in column must have same struct definition
- Cannot add/remove fields from existing rows
- Field names are case-sensitive
- Maximum nesting depth: 64 levels

## FUTURE ENHANCEMENTS

- STRUCT schema evolution
- Partial struct updates
- Field-level nullability
- Polymorphic structs (different fields per row)
- Struct compression for sparse fields

## REFERENCES

- DuckDB STRUCT: https://duckdb.org/docs/stable/sql/data_types/struct.html
- PostgreSQL Composite Types: https://www.postgresql.org/docs/current/rowtypes.html
