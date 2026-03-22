# Numeric Functions Specification

## Requirements

### Requirement: SIGNBIT SHALL check the sign bit

SIGNBIT(x) SHALL return true if the sign bit of the floating-point value is set. It MUST correctly handle negative zero (-0.0).

#### Scenario: Negative value

Given a running database
When the user executes `SELECT SIGNBIT(-1.0)`
Then the result MUST be true

#### Scenario: Positive value

Given a running database
When the user executes `SELECT SIGNBIT(1.0)`
Then the result MUST be false

### Requirement: WIDTH_BUCKET SHALL assign values to histogram buckets

WIDTH_BUCKET(value, min, max, num_buckets) SHALL return the bucket number (1-indexed) for an equi-width histogram. Values below min MUST return 0. Values at or above max MUST return num_buckets + 1.

#### Scenario: Value in range

Given a running database
When the user executes `SELECT WIDTH_BUCKET(5.0, 0.0, 10.0, 5)`
Then the result MUST be 3

#### Scenario: Value below range

Given a running database
When the user executes `SELECT WIDTH_BUCKET(-1.0, 0.0, 10.0, 5)`
Then the result MUST be 0

### Requirement: BETA SHALL compute the beta function

BETA(a, b) SHALL return the value of the beta function B(a,b) = Gamma(a) * Gamma(b) / Gamma(a+b).

#### Scenario: Beta of 1 and 1

Given a running database
When the user executes `SELECT BETA(1, 1)`
Then the result MUST be 1.0

### Requirement: Conditional aggregates SHALL filter rows by condition

SUM_IF(expr, condition), AVG_IF(expr, condition), MIN_IF(expr, condition), and MAX_IF(expr, condition) SHALL aggregate only rows where the condition evaluates to true. They MUST return NULL when no rows match.

#### Scenario: SUM_IF with condition

Given a table t with columns amount INTEGER and status VARCHAR containing rows (100, 'paid'), (200, 'paid'), (50, 'pending')
When the user executes `SELECT SUM_IF(amount, status = 'paid') FROM t`
Then the result MUST be 300

#### Scenario: AVG_IF with no matching rows

Given a table t with columns score INTEGER and passed BOOLEAN containing rows (80, true), (90, true)
When the user executes `SELECT AVG_IF(score, passed = false) FROM t`
Then the result MUST be NULL

