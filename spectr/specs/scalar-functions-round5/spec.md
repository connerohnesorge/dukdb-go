# Scalar Functions Round5 Specification

## Requirements

### Requirement: Math constant functions SHALL return correct values

E(), INF(), and NAN() SHALL return the corresponding mathematical constants (Euler's number, positive infinity, and Not-a-Number respectively) as DOUBLE values.

#### Scenario: E returns Euler's number

Given a running database
When the user executes `SELECT E()`
Then the result MUST be approximately 2.718281828459045

#### Scenario: INF returns positive infinity

Given a running database
When the user executes `SELECT INF() > 1e308`
Then the result MUST be true

#### Scenario: NAN returns Not-a-Number

Given a running database
When the user executes `SELECT NAN() != NAN()`
Then the result MUST be true because NaN is not equal to itself

### Requirement: UUID generation SHALL return unique v4 UUIDs

UUID() and GEN_RANDOM_UUID() SHALL return a new random UUID v4 string on each call. Both function names MUST be supported as aliases.

#### Scenario: UUID returns valid format

Given a running database
When the user executes `SELECT UUID()`
Then the result MUST match the UUID v4 format (xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx)

#### Scenario: UUID returns unique values

Given a running database
When the user executes `SELECT UUID() != UUID()`
Then the result MUST be true

### Requirement: SPLIT_PART SHALL split strings by delimiter

SPLIT_PART(string, delimiter, index) SHALL split a string by the given delimiter and return the part at the 1-based index. Negative indices MUST count from the end.

#### Scenario: Basic split

Given a running database
When the user executes `SELECT SPLIT_PART('a-b-c', '-', 2)`
Then the result MUST be 'b'

#### Scenario: Negative index

Given a running database
When the user executes `SELECT SPLIT_PART('a-b-c', '-', -1)`
Then the result MUST be 'c'

#### Scenario: Out of range returns empty string

Given a running database
When the user executes `SELECT SPLIT_PART('a-b', '-', 5)`
Then the result MUST be ''

### Requirement: LOG SHALL support optional base argument

LOG(x) SHALL return the base-10 logarithm (existing behavior). LOG(x, base) SHALL return the logarithm of x with the given base.

#### Scenario: Single argument LOG10

Given a running database
When the user executes `SELECT LOG(100)`
Then the result MUST be 2.0

#### Scenario: Two argument arbitrary base

Given a running database
When the user executes `SELECT LOG(8, 2)`
Then the result MUST be 3.0

### Requirement: SHA512 SHALL return hex digest

SHA512(value) SHALL return the SHA-512 hash of the input as a lowercase hexadecimal string. NULL input MUST return NULL.

#### Scenario: SHA512 hash

Given a running database
When the user executes `SELECT LENGTH(SHA512('hello'))`
Then the result MUST be 128 (SHA-512 produces 64 bytes = 128 hex chars)

### Requirement: MILLISECOND and MICROSECOND SHALL extract sub-second components

MILLISECOND(timestamp) SHALL return the millisecond component (0-999). MICROSECOND(timestamp) SHALL return the microsecond component (0-999999).

#### Scenario: Millisecond extraction

Given a running database
When the user executes `SELECT MILLISECOND(TIMESTAMP '2024-01-01 12:34:56.789')`
Then the result MUST be 789

#### Scenario: Microsecond extraction

Given a running database
When the user executes `SELECT MICROSECOND(TIMESTAMP '2024-01-01 12:34:56.789012')`
Then the result MUST be 789012

