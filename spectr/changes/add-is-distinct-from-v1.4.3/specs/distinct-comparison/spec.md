# Distinct Comparison Operators

## ADDED Requirements

### Requirement: IS DISTINCT FROM SHALL perform NULL-safe inequality comparison

The `IS DISTINCT FROM` operator SHALL return true when two values are different, treating NULLs as equal to each other (unlike standard `!=` which returns NULL when either operand is NULL).

#### Scenario: Both non-NULL and equal
```
When the user executes "SELECT 1 IS DISTINCT FROM 1"
Then the result SHALL be false
```

#### Scenario: Both non-NULL and different
```
When the user executes "SELECT 1 IS DISTINCT FROM 2"
Then the result SHALL be true
```

#### Scenario: One operand is NULL
```
When the user executes "SELECT 1 IS DISTINCT FROM NULL"
Then the result SHALL be true (not NULL)
```

#### Scenario: Both operands are NULL
```
When the user executes "SELECT NULL IS DISTINCT FROM NULL"
Then the result SHALL be false
```

### Requirement: IS NOT DISTINCT FROM SHALL perform NULL-safe equality comparison

The `IS NOT DISTINCT FROM` operator SHALL return true when two values are equal, treating NULLs as equal to each other.

#### Scenario: Both non-NULL and equal
```
When the user executes "SELECT 1 IS NOT DISTINCT FROM 1"
Then the result SHALL be true
```

#### Scenario: Both non-NULL and different
```
When the user executes "SELECT 1 IS NOT DISTINCT FROM 2"
Then the result SHALL be false
```

#### Scenario: One operand is NULL
```
When the user executes "SELECT NULL IS NOT DISTINCT FROM 1"
Then the result SHALL be false (not NULL)
```

#### Scenario: Both operands are NULL
```
When the user executes "SELECT NULL IS NOT DISTINCT FROM NULL"
Then the result SHALL be true
```

### Requirement: IS [NOT] DISTINCT FROM SHALL work in WHERE and JOIN conditions

The operators SHALL be usable in WHERE clauses and JOIN ON conditions for NULL-safe filtering and matching.

#### Scenario: WHERE clause filtering
```
Given a table with rows containing NULL values in column "x"
When the user executes "SELECT * FROM t WHERE x IS NOT DISTINCT FROM NULL"
Then rows where x is NULL SHALL be returned
```

#### Scenario: JOIN condition
```
When the user executes "SELECT * FROM a JOIN b ON a.x IS NOT DISTINCT FROM b.x"
Then rows SHALL be joined including NULL=NULL matches
```
