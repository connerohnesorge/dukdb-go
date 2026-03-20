# Type Casting Specification

## Requirements

### Requirement: TRY_CAST Safe Casting

The system SHALL support the `TRY_CAST(expression AS type)` syntax which attempts to cast a value to the target type and returns NULL on failure instead of raising an error. TRY_CAST SHALL follow standard SQL NULL propagation: a NULL input always produces a NULL output regardless of the target type.

#### Scenario: TRY_CAST returns NULL on invalid cast

- WHEN a user executes `SELECT TRY_CAST('abc' AS INTEGER)`
- THEN the result SHALL be NULL
- AND no error SHALL be raised

#### Scenario: TRY_CAST succeeds on valid cast

- WHEN a user executes `SELECT TRY_CAST('42' AS INTEGER)`
- THEN the result SHALL be the integer value 42

#### Scenario: TRY_CAST with NULL input

- WHEN a user executes `SELECT TRY_CAST(NULL AS INTEGER)`
- THEN the result SHALL be NULL
- AND no error SHALL be raised

#### Scenario: Nested TRY_CAST

- WHEN a user executes `SELECT TRY_CAST(TRY_CAST('abc' AS INTEGER) AS VARCHAR)`
- THEN the inner TRY_CAST SHALL return NULL because 'abc' cannot be cast to INTEGER
- AND the outer TRY_CAST SHALL return NULL because NULL cast to VARCHAR is NULL

#### Scenario: TRY_CAST with overflow

- WHEN a user executes `SELECT TRY_CAST(99999999999 AS TINYINT)`
- THEN the result SHALL be NULL
- AND no error SHALL be raised

### Requirement: PostgreSQL-Style Cast Operator

The system SHALL support the `::` (double-colon) postfix operator as syntactic sugar for `CAST(expression AS type)`. The `::` operator SHALL always perform a strict cast (equivalent to CAST, not TRY_CAST) and SHALL raise an error on conversion failure.

#### Scenario: :: operator works for valid cast

- WHEN a user executes `SELECT 42::VARCHAR`
- THEN the result SHALL be the string '42'

#### Scenario: :: operator with chained casts

- WHEN a user executes `SELECT 42::VARCHAR::INTEGER`
- THEN the value 42 SHALL first be cast to VARCHAR producing '42'
- AND '42' SHALL then be cast to INTEGER producing 42

#### Scenario: :: operator raises error on invalid cast

- WHEN a user executes `SELECT 'abc'::INTEGER`
- THEN the system SHALL raise a cast error
- AND the error message SHALL indicate that 'abc' cannot be cast to INTEGER

#### Scenario: :: operator in expressions

- WHEN a user executes `SELECT '123'::INTEGER + 1`
- THEN the string '123' SHALL be cast to INTEGER
- AND the result SHALL be 124

### Requirement: TRY_CAST Serialization Roundtrip

When a `TRY_CAST` expression appears in DDL contexts (e.g., view definitions, computed column defaults), the serializer SHALL output `TRY_CAST(...)` rather than `CAST(...)`. This ensures that re-parsing the serialized SQL preserves the safe-cast semantics.

#### Scenario: TRY_CAST in view definition

- WHEN a view is created with `CREATE VIEW v AS SELECT TRY_CAST(col AS INTEGER) FROM t`
- AND the view definition is serialized for storage
- THEN the serialized SQL SHALL contain `TRY_CAST(col AS INTEGER)`, not `CAST(col AS INTEGER)`

### Requirement: TRY_CAST Preservation Through Optimization

The `TryCast` flag SHALL be preserved when expressions are rewritten by the query optimizer or planner. A `TRY_CAST` expression SHALL NOT silently degrade to a strict `CAST` after optimization rewrites.

#### Scenario: TRY_CAST preserved after constant folding

- WHEN user executes `SELECT TRY_CAST('abc' AS INTEGER)` and the optimizer applies constant folding
- THEN the folded expression SHALL still use TRY_CAST semantics and return NULL instead of raising an error

