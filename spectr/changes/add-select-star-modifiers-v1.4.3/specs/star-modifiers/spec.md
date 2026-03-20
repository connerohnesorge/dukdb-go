# SELECT * Modifiers — EXCLUDE, REPLACE, COLUMNS

## ADDED Requirements

### Requirement: SELECT * EXCLUDE SHALL omit specified columns from star expansion

The EXCLUDE modifier on star expressions SHALL filter out the named columns from the result.

#### Scenario: Single column exclusion
```
When the user executes "CREATE TABLE t(a INT, b INT, c INT); INSERT INTO t VALUES (1, 2, 3); SELECT * EXCLUDE(b) FROM t"
Then the result contains columns a and c only
And the values are 1 and 3
```

#### Scenario: Multiple column exclusion
```
When the user executes "SELECT * EXCLUDE(a, c) FROM t"
Then the result contains only column b
```

#### Scenario: Table-qualified star with EXCLUDE
```
When the user executes "SELECT t.* EXCLUDE(b) FROM t"
Then the result contains columns a and c from table t
```

#### Scenario: Non-existent column in EXCLUDE returns error
```
When the user executes "SELECT * EXCLUDE(nonexistent) FROM t"
Then an error is returned indicating the column was not found
```

### Requirement: SELECT * REPLACE SHALL substitute column expressions

The REPLACE modifier on star expressions SHALL replace the named column's value with the provided expression while keeping all other columns.

#### Scenario: Replace single column with expression
```
When the user executes "CREATE TABLE users(id INT, name VARCHAR); INSERT INTO users VALUES (1, 'alice'); SELECT * REPLACE(UPPER(name) AS name) FROM users"
Then the result contains columns id and name
And name is 'ALICE'
```

#### Scenario: Replace with computed expression
```
When the user executes "CREATE TABLE nums(a INT, b INT); INSERT INTO nums VALUES (1, 2); SELECT * REPLACE(a + 10 AS a) FROM nums"
Then a is 11 and b is 2
```

#### Scenario: Non-existent column in REPLACE returns error
```
When the user executes "SELECT * REPLACE(1 AS nonexistent) FROM t"
Then an error is returned indicating the column was not found
```

### Requirement: COLUMNS expression SHALL select columns matching a regex pattern

The COLUMNS function SHALL accept a regex pattern and expand to all columns whose names match.

#### Scenario: Select columns by prefix pattern
```
When the user executes "CREATE TABLE products(price_usd INT, price_eur INT, name VARCHAR); INSERT INTO products VALUES (10, 9, 'Widget'); SELECT COLUMNS('price_.*') FROM products"
Then the result contains columns price_usd and price_eur only
```

#### Scenario: Select all columns with wildcard
```
When the user executes "SELECT COLUMNS('.*') FROM products"
Then the result contains all columns
```

#### Scenario: No columns match returns error
```
When the user executes "SELECT COLUMNS('zzz_.*') FROM products"
Then an error is returned indicating no columns match the pattern
```

#### Scenario: Invalid regex returns error
```
When the user executes "SELECT COLUMNS('[invalid') FROM products"
Then an error is returned indicating invalid regex
```
