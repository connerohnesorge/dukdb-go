# Specification: UNION Type Completion

## Overview
This spec completes the UNION type implementation in dukdb-go for DuckDB v1.4.3 compatibility, enabling polymorphic columns where each row can be a different type.

## ADDED Requirements

### Requirement 1: UNION Type Definition
#### Scenario: Create table with UNION column
```sql
-- Given: A database connection
-- When: I execute:
CREATE TABLE flexible_values (
    id INTEGER,
    value UNION(num INTEGER, str VARCHAR, flag BOOLEAN)
);
-- Then: The table should be created successfully
-- And: The value column should have type UNION with defined members
```

#### Scenario: Create table with nested UNION
```sql
-- Given: A database connection
-- When: I execute:
CREATE TABLE complex (
    data UNION(
        point STRUCT(x INTEGER, y INTEGER),
        text VARCHAR,
        arr INTEGER[]
    )
);
-- Then: The table should be created successfully
-- And: Should support nested complex types within UNION
```

### Requirement 2: UNION Construction with union_value
#### Scenario: Construct UNION with integer member
```sql
-- Given: Database connection
-- When: I execute:
INSERT INTO flexible_values VALUES (1, union_value(num := 42));
-- Then: Should insert row with value set to num member
-- And: The union should store integer 42
```

#### Scenario: Construct UNION with string member
```sql
-- Given: Database connection
-- When: I execute:
INSERT INTO flexible_values VALUES (2, union_value(str := 'hello'));
-- Then: Should insert row with value set to str member
-- And: The union should store string 'hello'
```

#### Scenario: Construct UNION with boolean member
```sql
-- Given: Database connection
-- When: I execute:
INSERT INTO flexible_values VALUES (3, union_value(flag := true));
-- Then: Should insert row with value set to flag member
-- And: The union should store boolean true
```

#### Scenario: Use alternative syntax
```sql
-- Given: Database connection
-- When: I execute:
SELECT union_value(num = 100) as value;
-- Then: Should accept = syntax as alternative to :=
-- And: Work equivalently
```

### Requirement 3: Union Tag Inspection
#### Scenario: Use union_tag function
```sql
-- Given: Table with UNION data
-- When: I execute:
SELECT id, union_tag(value) as member_type
FROM flexible_values;
-- Then: Should return 'num', 'str', or 'flag' for each row
-- And: Indicates which member is active
```

#### Scenario: Filter by union tag
```sql
-- Given: Table with UNION data
-- When: I execute:
SELECT * FROM flexible_values
WHERE union_tag(value) = 'num';
-- Then: Should return only rows where num member is active
```

### Requirement 4: Union Value Extraction
#### Scenario: Extract union values with union_extract
```sql
-- Given: Table with UNION data
-- When: I execute:
SELECT
    union_extract(value, 'num') as num_val,
    union_extract(value, 'str') as str_val,
    union_extract(value, 'flag') as flag_val
FROM flexible_values;
-- Then: Should extract values based on member name
-- And: Return NULL for inactive members
```

#### Scenario: Access union values in WHERE clause
```sql
-- Given: Table with UNION data
-- When: I execute:
SELECT * FROM flexible_values
WHERE union_extract(value, 'num') > 20;
-- Then: Should filter rows where num member active and > 20
-- And: Other rows return NULL for extract
```

### Requirement 5: UNION Type Discrimination
#### Scenario: Different rows use different members
```sql
-- Given: Empty table with UNION column
-- When: I execute:
INSERT INTO flexible VALUES
    (1, union_value(num := 42)),
    (2, union_value(str := 'text')),
    (3, union_value(flag := true)),
    (4, union_value(num := 100));
-- Then: All inserts should succeed
-- And: Each row can use different union member
```

#### Scenario: Query handles mixed members
```sql
-- Given: Table with mixed union values
-- When: I execute:
SELECT id,
    CASE union_tag(value)
        WHEN 'num' THEN 'Number: ' || union_extract(value, 'num')::VARCHAR
        WHEN 'str' THEN 'String: ' || union_extract(value, 'str')
        WHEN 'flag' THEN 'Boolean: ' || union_extract(value, 'flag')::VARCHAR
    END as description
FROM flexible_values;
-- Then: Should handle each row based on active member
-- And: Correctly extract and format values
```

### Requirement 6: UNION with Complex Nested Types
#### Scenario: UNION containing STRUCT
```sql
-- Given: Table with UNION containing STRUCT
-- When: I execute:
INSERT INTO complex VALUES (
    1,
    union_value(point := {'x': 10, 'y': 20})
);
-- Then: Should insert nested STRUCT in UNION
```

#### Scenario: Navigate through UNION to STRUCT fields
```sql
-- Given: Table with complex nested data
-- When: I execute:
SELECT
    (union_extract(data, 'point')).x as x_coord,
    (union_extract(data, 'point')).y as y_coord
FROM complex
WHERE union_tag(data) = 'point';
-- Then: Should extract fields from STRUCT within UNION
-- And: Handle NULL for non-STRUCT rows
```

### Requirement 7: UNION NULL Handling
#### Scenario: NULL union values
```sql
-- Given: Table with UNION column
-- When: I execute:
INSERT INTO flexible_values VALUES (10, NULL);
-- Then: Should insert NULL UNION value
```

#### Scenario: Query NULL union values
```sql
-- Given: Table with NULL and non-NULL unions
-- When: I execute:
SELECT id, union_tag(value)
FROM flexible_values
WHERE value IS NULL;
-- Then: Should return rows where union is NULL
-- And: union_tag should return NULL
```

### Requirement 8: UNION in Expressions
#### Scenario: UNION values in CASE expressions
```sql
-- Given: Table with UNION values
-- When: I execute:
SELECT id,
    CASE
        WHEN union_tag(value) = 'num' THEN union_extract(value, 'num') * 2
        WHEN union_tag(value) = 'str' THEN LENGTH(union_extract(value, 'str'))
        ELSE 0
    END as computed
FROM flexible_values;
-- Then: Should compute based on union member type
```

#### Scenario: Coalesce UNION extractions
```sql
-- Given: Table with UNION values
-- When: I execute:
SELECT id,
    COALESCE(
        union_extract(value, 'num')::VARCHAR,
        union_extract(value, 'str'),
        union_extract(value, 'flag')::VARCHAR
    ) as unified
FROM flexible_values;
-- Then: Should return first non-NULL extraction
```

### Requirement 9: UNION Type Validation
#### Scenario: Reject invalid member names
```sql
-- Given: Database connection
-- When: I execute:
SELECT union_value(invalid_member := 42);
-- Then: Should fail with error "Member 'invalid_member' not defined in UNION type"
```

#### Scenario: Type check on union construction
```sql
-- Given: UNION defined as UNION(num INTEGER, str VARCHAR)
-- When: I attempt:
SELECT union_value(num := 'not a number');
-- Then: Should fail or attempt to cast string to INTEGER
```

### Requirement 10: UNION Storage Format
#### Scenario: UNION preserves type information
```sql
-- Given: Table with UNION column
-- When: I:
--   1. Insert various union values
--   2. Restart database
--   3. Query the values
-- Then: Member types should be preserved
-- And: union_tag should return correct member names
-- And: union_extract should return correct values
```

## MODIFIED Requirements

### Existing Union Type Definition
#### Scenario: Complete types.go Union implementation
```
-- In: /home/connerohnesorge/Documents/001Repos/dukdb-go/types.go
-- Current: Union struct defined for scanning
-- Modify: Add discriminator field storage
-- Add: Serialization to DuckDB format
-- Add: Type information preservation
```

### Expression Framework
#### Scenario: Add union construction and extraction
```
-- In: internal/functions/union/
-- Add: union_value construction function
-- Add: union_tag inspection function
-- Add: union_extract accessor function
-- Add: Type resolution based on active member
```

## STORAGE IMPLEMENTATION

### Physical Layout
```
UnionVector {
    discriminator: StringVector,  -- Which member is active
    member_vectors: {
        "num":  IntVector,
        "str":  StringVector,
        "flag": BoolVector
    },
    validity: ValidityMask
}
```

### Access Patterns
- Check discriminator O(1)
- Access member vector O(1) if known at compile time
- Extract operation: (1) check discriminator, (2) access member, (3) return or NULL

## PERFORMANCE CHARACTERISTICS

### Space Efficiency
- Only active member stored per row
- Other members don't consume space
- Discriminator adds 1 byte overhead per row
- NULL unions only cost validity bit

### Access Performance
- Check discriminator: 1 memory read
- Extract known member: 2 memory reads
- Extract unknown member: Add branch prediction
- Bulk operations: Vectorized per-member access

## TEST COVERAGE

### Unit Tests (15 scenarios)
- UNION construction with all member types
- Member extraction edge cases
- NULL handling
- Type validation
- Discriminator accuracy

### Integration Tests (8 scenarios)
- CREATE TABLE with UNION columns
- Mixed member values across rows
- Nested complex types in UNION
- COPY TO/FROM operations
- Query patterns (WHERE, GROUP BY, etc)

### Compatibility Tests (5 scenarios)
- DuckDB member naming conventions
- Construction syntax (:= vs =)
- Discriminator behavior
- Extract semantics

## ACCEPTANCE CRITERIA

1. All scenarios pass matching DuckDB v1.4.3 behavior
2. UNION construction accepts all member types
3. Discriminator correctly identifies active member
4. Extraction returns NULL for inactive members
5. Space usage ≤1.1x size of stored data
6. Integration with existing dukdb-go architecture
7. Handles nested complex types correctly

## KNOWN LIMITATIONS

- Member names must be unique within UNION
- Cannot change UNION definition after creation
- No implicit conversions between member types
- Member access requires explicit extract/tag calls

## FUTURE ENHANCEMENTS

- UNION schema evolution (add/remove members)
- Pattern matching on unions (CASE ... OF syntax)
- Optimized storage for small unions
- Union-aware query optimization
- Type inference for union construction

## REFERENCES

- DuckDB UNION: https://duckdb.org/docs/stable/sql/data_types/union.html
- C UNION types: https://en.wikipedia.org/wiki/Union_type
- PostgreSQL Any type (conceptual reference)
