# Named Window Definitions

## ADDED Requirements

### Requirement: WINDOW clause SHALL define reusable named window specifications

The WINDOW clause SHALL allow defining one or more named window specifications that can be referenced by window functions in the same SELECT statement via OVER.

#### Scenario: Single named window with multiple functions
```
Given a table "sales" with columns (dept TEXT, amount INT)
When the user executes:
  SELECT ROW_NUMBER() OVER w AS rn, SUM(amount) OVER w AS total
  FROM sales
  WINDOW w AS (PARTITION BY dept ORDER BY amount)
Then both window functions SHALL use the same window specification
And results SHALL be partitioned by dept and ordered by amount
```

#### Scenario: Multiple named windows in same query
```
Given a table "employees" with columns (dept TEXT, salary INT, hire_date DATE)
When the user executes:
  SELECT
    RANK() OVER salary_w AS salary_rank,
    RANK() OVER date_w AS date_rank
  FROM employees
  WINDOW salary_w AS (ORDER BY salary DESC),
         date_w AS (ORDER BY hire_date)
Then salary_rank SHALL rank by salary descending
And date_rank SHALL rank by hire_date ascending
```

### Requirement: OVER SHALL accept a bare window name reference

Window functions SHALL support `OVER name` syntax (without parentheses) to reference a named window definition.

#### Scenario: Bare name reference
```
Given WINDOW w AS (ORDER BY x)
When the user writes "SUM(x) OVER w"
Then the window function SHALL use the specification defined for w
```

### Requirement: OVER SHALL support window inheritance with additional clauses

When OVER references a named window inside parentheses, additional clauses (PARTITION BY, ORDER BY, frame) SHALL be merged with the base window. Overriding an existing clause in the base window SHALL produce an error.

#### Scenario: Inherit PARTITION BY and add ORDER BY
```
Given a table "t" with columns (dept TEXT, x INT, y INT)
When the user executes:
  SELECT SUM(x) OVER (w ORDER BY y)
  FROM t
  WINDOW w AS (PARTITION BY dept)
Then the effective window SHALL have PARTITION BY dept ORDER BY y
```

#### Scenario: Error on overriding base ORDER BY
```
Given WINDOW w AS (ORDER BY x)
When the user writes "SUM(x) OVER (w ORDER BY y)"
Then the system SHALL return an error indicating ORDER BY cannot be overridden
```

### Requirement: Named windows SHALL support transitive references

A named window definition SHALL be able to reference another named window as its base. Circular references SHALL produce an error.

#### Scenario: Transitive window reference
```
Given:
  WINDOW w1 AS (PARTITION BY dept),
         w2 AS (w1 ORDER BY salary)
When a function uses "OVER w2"
Then the effective window SHALL have PARTITION BY dept ORDER BY salary
```

#### Scenario: Circular reference error
```
Given:
  WINDOW w1 AS (w2),
         w2 AS (w1)
Then the system SHALL return an error indicating a circular window reference
```

### Requirement: Undefined window name references SHALL produce an error

Referencing a window name that is not defined in the WINDOW clause SHALL produce a clear error message.

#### Scenario: Undefined window name
```
When the user executes "SELECT SUM(x) OVER undefined_window FROM t"
And no WINDOW clause defines "undefined_window"
Then the system SHALL return an error indicating the window is not defined
```
