# Missing Functions Round 2

## ADDED Requirements

### Requirement: SHA1 hash function

SHA1(string) SHALL return the SHA-1 hash of the input as a lowercase hexadecimal string.

#### Scenario: SHA1 of known value

When `SELECT SHA1('hello')` is executed
Then the result MUST be 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'

#### Scenario: SHA1 NULL propagation

When `SELECT SHA1(NULL)` is executed
Then the result MUST be NULL

### Requirement: SETSEED random seed

SETSEED(value) SHALL set the random number generator seed for the current connection. Value MUST be between 0 and 1.

#### Scenario: SETSEED produces deterministic RANDOM

When `SETSEED(0.5)` is called followed by `RANDOM()`
Then calling `SETSEED(0.5)` and `RANDOM()` again MUST produce the same value

#### Scenario: SETSEED range validation

When `SELECT SETSEED(2.0)` is executed
Then an error MUST be returned indicating seed must be between 0 and 1

### Requirement: LIST_VALUE list constructor

LIST_VALUE(args...) SHALL create a list containing all arguments, including NULL values.

#### Scenario: LIST_VALUE creates list

When `SELECT LIST_VALUE(1, 2, 3)` is executed
Then the result MUST be [1, 2, 3]

#### Scenario: LIST_VALUE preserves NULLs

When `SELECT LIST_VALUE(1, NULL, 3)` is executed
Then the result MUST be [1, NULL, 3]

#### Scenario: LIST_VALUE empty call

When `SELECT LIST_VALUE()` is executed
Then the result MUST be an empty list []

### Requirement: ANY_VALUE aggregate

ANY_VALUE(expr) SHALL return an arbitrary non-NULL value from the group. It MUST return NULL only if all values are NULL.

#### Scenario: ANY_VALUE returns a value

Given a table with values 1, 2, 3
When `SELECT ANY_VALUE(x) FROM t` is executed
Then the result MUST be one of 1, 2, or 3

#### Scenario: ANY_VALUE with all NULLs

Given a table where column x is all NULL
When `SELECT ANY_VALUE(x) FROM t` is executed
Then the result MUST be NULL

### Requirement: HISTOGRAM aggregate

HISTOGRAM(expr) SHALL return a MAP of distinct values to their occurrence counts.

#### Scenario: HISTOGRAM counts values

Given values 1, 1, 2, 3
When `SELECT HISTOGRAM(x) FROM t` is executed
Then the result MUST be a map where 1→2, 2→1, 3→1

#### Scenario: HISTOGRAM skips NULLs

Given values 1, NULL, 1
When `SELECT HISTOGRAM(x) FROM t` is executed
Then the result MUST be {1: 2} (NULL values SHALL be excluded)

### Requirement: ARG_MIN and ARG_MAX underscore aliases

ARG_MIN and ARG_MAX SHALL be aliases for ARGMIN and ARGMAX respectively.

#### Scenario: ARG_MIN matches ARGMIN

When `SELECT ARG_MIN(name, salary) FROM emp` is executed
Then the result MUST equal `SELECT ARGMIN(name, salary) FROM emp`
