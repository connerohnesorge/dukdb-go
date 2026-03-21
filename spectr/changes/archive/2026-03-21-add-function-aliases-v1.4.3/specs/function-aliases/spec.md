# Function Aliases and Small Scalar Functions

## ADDED Requirements

### Requirement: DATETRUNC and DATEADD aliases

DATETRUNC and DATEADD SHALL be recognized as aliases for DATE_TRUNC and DATE_ADD respectively, producing identical results.

#### Scenario: DATETRUNC produces same result as DATE_TRUNC

Given a timestamp value '2024-03-15 10:30:00'
When `SELECT DATETRUNC('month', TIMESTAMP '2024-03-15 10:30:00')` is executed
Then the result MUST equal the result of `SELECT DATE_TRUNC('month', TIMESTAMP '2024-03-15 10:30:00')`

#### Scenario: DATEADD produces same result as DATE_ADD

Given a date value '2024-03-15'
When `SELECT DATEADD(DATE '2024-03-15', INTERVAL 1 DAY)` is executed
Then the result MUST equal the result of `SELECT DATE_ADD(DATE '2024-03-15', INTERVAL 1 DAY)`

### Requirement: ORD alias for ASCII

ORD SHALL be recognized as an alias for the ASCII function, returning the Unicode code point of the first character.

#### Scenario: ORD returns same value as ASCII

When `SELECT ORD('A')` is executed
Then the result MUST be 65
And it MUST equal the result of `SELECT ASCII('A')`

#### Scenario: ORD with empty string

When `SELECT ORD('')` is executed
Then the result MUST be 0

### Requirement: IFNULL null-replacement function

IFNULL(a, b) SHALL return a if a is not NULL, otherwise b. It MUST enforce exactly 2 arguments.

#### Scenario: IFNULL returns first non-NULL argument

When `SELECT IFNULL(NULL, 42)` is executed
Then the result MUST be 42

#### Scenario: IFNULL returns first argument when non-NULL

When `SELECT IFNULL(1, 42)` is executed
Then the result MUST be 1

#### Scenario: IFNULL with both NULL

When `SELECT IFNULL(NULL, NULL)` is executed
Then the result MUST be NULL

#### Scenario: IFNULL rejects wrong argument count

When `SELECT IFNULL(1, 2, 3)` is executed
Then an error MUST be returned indicating IFNULL requires exactly 2 arguments

### Requirement: NVL alias for IFNULL

NVL SHALL behave identically to IFNULL.

#### Scenario: NVL returns replacement for NULL

When `SELECT NVL(NULL, 'default')` is executed
Then the result MUST be 'default'

### Requirement: BIT_LENGTH function

BIT_LENGTH(value) SHALL return the number of bits in the value's byte representation.

#### Scenario: BIT_LENGTH of string

When `SELECT BIT_LENGTH('hello')` is executed
Then the result MUST be 40

#### Scenario: BIT_LENGTH of empty string

When `SELECT BIT_LENGTH('')` is executed
Then the result MUST be 0

#### Scenario: BIT_LENGTH with NULL

When `SELECT BIT_LENGTH(NULL)` is executed
Then the result MUST be NULL

### Requirement: GET_BIT bit extraction

GET_BIT(value, index) SHALL return the bit at the specified index (0-based, big-endian) as 0 or 1.

#### Scenario: GET_BIT extracts MSB

Given a byte value 0x80 (binary 10000000)
When `GET_BIT` is called with index 0
Then the result MUST be 1

#### Scenario: GET_BIT out of range error

When GET_BIT is called with an index beyond the value's bit length
Then an error MUST be returned

### Requirement: SET_BIT bit modification

SET_BIT(value, index, new_bit) SHALL return a copy of value with the bit at index set to new_bit (0 or 1).

#### Scenario: SET_BIT sets a bit

Given a byte value 0x00
When `SET_BIT` is called with index 0 and new_bit 1
Then the result MUST be 0x80

#### Scenario: SET_BIT rejects invalid bit value

When SET_BIT is called with new_bit value 2
Then an error MUST be returned

### Requirement: ENCODE character encoding

ENCODE(string, encoding) SHALL convert a string to a BLOB using the specified encoding. Supported encodings MUST include UTF-8, LATIN1/ISO-8859-1, and ASCII.

#### Scenario: ENCODE to UTF-8

When `SELECT ENCODE('hello', 'UTF-8')` is executed
Then the result MUST be a BLOB containing the UTF-8 bytes of 'hello'

#### Scenario: ENCODE with unsupported encoding

When `SELECT ENCODE('hello', 'EBCDIC')` is executed
Then an error MUST be returned indicating unsupported encoding

### Requirement: DECODE character decoding

DECODE(blob, encoding) SHALL convert a BLOB to a string using the specified encoding.

#### Scenario: DECODE from UTF-8

When `SELECT DECODE(ENCODE('hello', 'UTF-8'), 'UTF-8')` is executed
Then the result MUST be 'hello'

#### Scenario: DECODE with NULL input

When `SELECT DECODE(NULL, 'UTF-8')` is executed
Then the result MUST be NULL
