# Struct Field Access

## ADDED Requirements

### Requirement: Struct dot notation

Struct dot notation SHALL allow accessing struct fields using the syntax `struct_column.field_name`. It MUST be equivalent to calling `STRUCT_EXTRACT(struct_column, 'field_name')`.

#### Scenario: Basic field access

Given a table with column `s` of type `STRUCT(name VARCHAR, age INTEGER)`
When `SELECT s.name FROM t` is executed
Then the result MUST return the name field value from each struct

#### Scenario: Field access in WHERE clause

Given a table with struct column `s`
When `SELECT * FROM t WHERE s.age > 25` is executed
Then only rows where the age field exceeds 25 MUST be returned

### Requirement: Ambiguity resolution

When a dot expression `a.b` could refer to either table.column or struct.field, table.column resolution SHALL take priority.

#### Scenario: Table takes priority

Given a table named `s` with column `name` AND a struct column named `s`
When `SELECT s.name FROM s` is executed
Then `s.name` MUST resolve to the table column, not the struct field
