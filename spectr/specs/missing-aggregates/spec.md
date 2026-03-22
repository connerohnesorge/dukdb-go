# Missing Aggregates Specification

## Requirements

### Requirement: PRODUCT aggregate

PRODUCT SHALL compute the multiplicative product of all non-NULL values. It MUST return NULL for empty sets.

#### Scenario: Basic product

When `SELECT PRODUCT(x) FROM (VALUES (2), (3), (4)) t(x)` is executed
Then the result MUST be 24.0

#### Scenario: Product with NULLs

When `SELECT PRODUCT(x) FROM (VALUES (2), (NULL), (3)) t(x)` is executed
Then the result MUST be 6.0

### Requirement: MAD aggregate

MAD (Median Absolute Deviation) SHALL compute the median of absolute deviations from the median. It MUST return a DOUBLE value.

#### Scenario: Basic MAD

When `SELECT MAD(x) FROM (VALUES (1), (2), (3), (4), (5)) t(x)` is executed
Then the result MUST be 1.0

### Requirement: FAVG and FSUM aggregates

FAVG and FSUM SHALL use Kahan summation for numerically stable floating-point aggregation. They MUST return DOUBLE values.

#### Scenario: FAVG accuracy

When `SELECT FAVG(x) FROM table_with_many_small_values` is executed
Then the result MUST be at least as accurate as standard AVG

#### Scenario: FSUM accuracy

When `SELECT FSUM(x) FROM table_with_many_small_values` is executed
Then the result MUST be at least as accurate as standard SUM

### Requirement: BITSTRING_AGG aggregate

BITSTRING_AGG SHALL aggregate boolean values into a bitstring representation. It MUST return a VARCHAR of '0' and '1' characters.

#### Scenario: Bitstring aggregation

When `SELECT BITSTRING_AGG(x) FROM (VALUES (true), (false), (true)) t(x)` is executed
Then the result MUST be '101'

