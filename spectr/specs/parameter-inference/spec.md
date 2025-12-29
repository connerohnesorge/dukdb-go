# Parameter Inference Specification

## Requirements

### Requirement: Column Comparison Type Inference

The system SHALL infer parameter types from column comparisons.

#### Scenario: Equality with integer column
- GIVEN a table with column "id" of type INTEGER
- AND query "SELECT * FROM t WHERE id = ?"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER

#### Scenario: Equality with varchar column
- GIVEN a table with column "name" of type VARCHAR
- AND query "SELECT * FROM t WHERE name = $1"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_VARCHAR

#### Scenario: Parameter on left side
- GIVEN a table with column "value" of type DOUBLE
- AND query "SELECT * FROM t WHERE ? < value"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_DOUBLE

#### Scenario: Multiple parameters same type
- GIVEN a table with column "x" of type INTEGER
- AND query "SELECT * FROM t WHERE x BETWEEN ? AND ?"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER
- AND ParamType(2) returns TYPE_INTEGER

#### Scenario: Multiple parameters different types
- GIVEN a table with columns "id" INTEGER and "name" VARCHAR
- AND query "SELECT * FROM t WHERE id = ? AND name = ?"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER
- AND ParamType(2) returns TYPE_VARCHAR

### Requirement: INSERT Value Type Inference

The system SHALL infer parameter types from INSERT column contexts.

#### Scenario: Single column insert
- GIVEN a table with column "value" of type DOUBLE
- AND query "INSERT INTO t (value) VALUES (?)"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_DOUBLE

#### Scenario: Multiple column insert
- GIVEN a table with columns "id" INTEGER and "name" VARCHAR
- AND query "INSERT INTO t (id, name) VALUES (?, ?)"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER
- AND ParamType(2) returns TYPE_VARCHAR

#### Scenario: Multiple rows insert
- GIVEN a table with column "x" of type INTEGER
- AND query "INSERT INTO t (x) VALUES (?), (?)"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER
- AND ParamType(2) returns TYPE_INTEGER
- AND each parameter infers type from its column position

#### Scenario: Multi-column multi-row insert
- GIVEN a table with columns "id" INTEGER and "name" VARCHAR
- AND query "INSERT INTO t (id, name) VALUES (?, ?), (?, ?)"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER
- AND ParamType(2) returns TYPE_VARCHAR
- AND ParamType(3) returns TYPE_INTEGER
- AND ParamType(4) returns TYPE_VARCHAR

### Requirement: UPDATE Value Type Inference

The system SHALL infer parameter types from UPDATE SET contexts.

#### Scenario: Simple update
- GIVEN a table with column "value" of type DOUBLE
- AND query "UPDATE t SET value = ?"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_DOUBLE

#### Scenario: Update with where
- GIVEN a table with columns "value" DOUBLE and "id" INTEGER
- AND query "UPDATE t SET value = ? WHERE id = ?"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_DOUBLE
- AND ParamType(2) returns TYPE_INTEGER

### Requirement: Arithmetic Expression Type Inference

The system SHALL infer numeric types for arithmetic expressions.

#### Scenario: Addition expression
- GIVEN query "SELECT ? + ?"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_DOUBLE
- AND ParamType(2) returns TYPE_DOUBLE

#### Scenario: Multiplication expression
- GIVEN query "SELECT ? * 10"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_DOUBLE

### Requirement: Function Argument Type Inference

The system SHALL infer parameter types from function signatures.

#### Scenario: ABS function
- GIVEN query "SELECT abs(?)"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_DOUBLE

#### Scenario: LENGTH function
- GIVEN query "SELECT length(?)"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_VARCHAR

#### Scenario: COALESCE with typed column
- GIVEN a table with column "x" of type INTEGER
- AND query "SELECT coalesce(x, ?) FROM t"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER

### Requirement: IN List Type Inference

The system SHALL infer parameter types from IN list contexts.

#### Scenario: IN list with column
- GIVEN a table with column "status" of type VARCHAR
- AND query "SELECT * FROM t WHERE status IN (?, ?, ?)"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_VARCHAR
- AND ParamType(2) returns TYPE_VARCHAR
- AND ParamType(3) returns TYPE_VARCHAR

### Requirement: LIKE Pattern Type Inference

The system SHALL infer VARCHAR for LIKE patterns.

#### Scenario: LIKE with parameter
- GIVEN a table with column "name" of type VARCHAR
- AND query "SELECT * FROM t WHERE name LIKE ?"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_VARCHAR

### Requirement: Fallback to TYPE_ANY

The system SHALL return TYPE_ANY when type cannot be inferred.

#### Scenario: Standalone parameter
- GIVEN query "SELECT ?"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_ANY

#### Scenario: Conflicting contexts
- GIVEN a table with columns "id" INTEGER and "name" VARCHAR
- AND query "SELECT * FROM t WHERE id = $1 OR name = $1"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_ANY

#### Scenario: Unknown table
- GIVEN query "SELECT * FROM unknown_table WHERE x = ?"
- WHEN preparing fails or succeeds
- THEN ParamType(1) returns TYPE_ANY if accessible

#### Scenario: UNION with conflicting branch types
- GIVEN a table with "id" INTEGER and "name" VARCHAR
- AND query "SELECT id FROM t WHERE id = ? UNION SELECT name FROM t WHERE name = ?"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER
- AND ParamType(2) returns TYPE_VARCHAR

#### Scenario: Same parameter in conflicting UNION branches
- GIVEN a table with "id" INTEGER and "name" VARCHAR
- AND query "SELECT id FROM t WHERE id = $1 UNION SELECT name FROM t WHERE name = $1"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_ANY

#### Scenario: CAST expression inference
- GIVEN query "SELECT CAST(? AS INTEGER)"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER

#### Scenario: NULL comparison
- GIVEN a table with "value" INTEGER
- AND query "SELECT * FROM t WHERE value = ? OR value IS NULL"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER

#### Scenario: Mixed positional and named parameters
- GIVEN a table with "id" INTEGER and "name" VARCHAR
- AND query "SELECT * FROM t WHERE id = ? AND name = $name"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER
- AND ParamType(2) returns TYPE_VARCHAR

### Requirement: Named Parameter Type Inference

The system SHALL infer types for named parameters.

#### Scenario: Named parameter in comparison
- GIVEN a table with column "id" of type INTEGER
- AND query "SELECT * FROM t WHERE id = $id"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER

#### Scenario: Reused named parameter same type
- GIVEN a table with column "x" of type INTEGER
- AND query "SELECT * FROM t WHERE x > $val AND x < $val + 10"
- WHEN preparing the statement
- THEN ParamType(1) returns TYPE_INTEGER

### Requirement: ParamType API Behavior

The system SHALL return appropriate types from ParamType().

#### Scenario: Valid parameter index
- GIVEN a prepared statement with 2 parameters
- WHEN calling ParamType(1)
- THEN returns the inferred type

#### Scenario: Invalid parameter index
- GIVEN a prepared statement with 2 parameters
- WHEN calling ParamType(0)
- THEN returns TYPE_INVALID

#### Scenario: Out of range parameter index
- GIVEN a prepared statement with 2 parameters
- WHEN calling ParamType(3)
- THEN returns TYPE_INVALID

#### Scenario: Closed statement
- GIVEN a closed prepared statement
- WHEN calling ParamType(1)
- THEN error indicates statement is closed

