# String Functions Specification

## Overview

This specification documents the string manipulation functions available in dukdb-go. These functions provide comprehensive text processing capabilities including regular expression operations, string splitting and concatenation, padding, encoding, cryptographic hashing, and string distance/similarity calculations.

## Table of Contents

1. [Regular Expression Functions](#regular-expression-functions)
2. [String Concatenation and Splitting](#string-concatenation-and-splitting)
3. [Padding Functions](#padding-functions)
4. [String Manipulation Functions](#string-manipulation-functions)
5. [Character Encoding Functions](#character-encoding-functions)
6. [Cryptographic Hash Functions](#cryptographic-hash-functions)
7. [String Distance Functions](#string-distance-functions)
8. [Whitespace Trimming Aliases](#whitespace-trimming-aliases)
9. [NULL Handling](#null-handling)
10. [Error Handling](#error-handling)
11. [Performance Considerations](#performance-considerations)
12. [Common Use Cases](#common-use-cases)

---

## Regular Expression Functions

dukdb-go uses RE2 regular expression syntax, which is the same engine used by DuckDB and Go's `regexp` package. RE2 provides linear time matching guarantees, making it safe for use with untrusted patterns.

### RE2 Syntax Reference

| Pattern | Description |
|---------|-------------|
| `.` | Any single character |
| `*` | Zero or more of previous |
| `+` | One or more of previous |
| `?` | Zero or one of previous |
| `[abc]` | Character class |
| `[^abc]` | Negated character class |
| `[a-z]` | Character range |
| `^` | Start of string |
| `$` | End of string |
| `\d` | Digit `[0-9]` |
| `\w` | Word character `[a-zA-Z0-9_]` |
| `\s` | Whitespace character |
| `(...)` | Capture group |
| `(?:...)` | Non-capture group |
| `\|` | Alternation |

### REGEXP_MATCHES

Tests if a string matches a regular expression pattern.

**Signature:** `REGEXP_MATCHES(string, pattern) -> BOOLEAN`

**Parameters:**
- `string` (VARCHAR): The input string to test
- `pattern` (VARCHAR): RE2 regular expression pattern

**Returns:** `TRUE` if the pattern matches anywhere in the string, `FALSE` otherwise

**Examples:**
```sql
-- Basic matching
SELECT REGEXP_MATCHES('hello world', 'h.*o');       -- TRUE
SELECT REGEXP_MATCHES('hello world', '^world');     -- FALSE (world not at start)
SELECT REGEXP_MATCHES('hello world', 'world$');     -- TRUE (world at end)

-- Character classes
SELECT REGEXP_MATCHES('abc123', '\d+');             -- TRUE (contains digits)
SELECT REGEXP_MATCHES('hello', '\d+');              -- FALSE (no digits)

-- Case sensitivity
SELECT REGEXP_MATCHES('Hello', 'hello');            -- FALSE (case sensitive)
SELECT REGEXP_MATCHES('Hello', '(?i)hello');        -- TRUE (case insensitive)
```

### REGEXP_REPLACE

Replaces occurrences of a pattern in a string with a replacement string.

**Signature:** `REGEXP_REPLACE(string, pattern, replacement [, flags]) -> VARCHAR`

**Parameters:**
- `string` (VARCHAR): The input string
- `pattern` (VARCHAR): RE2 regular expression pattern
- `replacement` (VARCHAR): The replacement string (supports backreferences like `$1`, `$2`)
- `flags` (VARCHAR, optional): `'g'` for global replacement (all matches)

**Returns:** The string with pattern matches replaced

**Default Behavior:** Without the `'g'` flag, REGEXP_REPLACE replaces **only the first match**. This matches DuckDB behavior.

**Examples:**
```sql
-- First match only (default)
SELECT REGEXP_REPLACE('hello world hello', 'hello', 'hi');
-- Result: 'hi world hello'

-- Global replacement with 'g' flag
SELECT REGEXP_REPLACE('hello world hello', 'hello', 'hi', 'g');
-- Result: 'hi world hi'

-- Using capture groups
SELECT REGEXP_REPLACE('John Doe', '(\w+) (\w+)', '$2, $1');
-- Result: 'Doe, John'

-- No match returns original string
SELECT REGEXP_REPLACE('hello world', 'foo', 'bar');
-- Result: 'hello world'
```

### REGEXP_EXTRACT

Extracts the first match of a pattern from a string.

**Signature:** `REGEXP_EXTRACT(string, pattern [, group]) -> VARCHAR`

**Parameters:**
- `string` (VARCHAR): The input string
- `pattern` (VARCHAR): RE2 regular expression pattern with optional capture groups
- `group` (INTEGER, optional): The capture group to extract (0 = full match, 1+ = specific group). Default is 0.

**Returns:** The matched substring, or `NULL` if no match

**Examples:**
```sql
-- Extract full match (group 0)
SELECT REGEXP_EXTRACT('Price: $19.99', '\$[0-9.]+');
-- Result: '$19.99'

-- Extract specific capture group
SELECT REGEXP_EXTRACT('Price: $19.99', '\$([0-9.]+)', 1);
-- Result: '19.99'

-- Extract from log entry
SELECT REGEXP_EXTRACT('2024-01-15 ERROR: Connection failed', '(\d{4}-\d{2}-\d{2}) (\w+): (.*)', 2);
-- Result: 'ERROR'

-- No match returns NULL
SELECT REGEXP_EXTRACT('No price here', '\$([0-9.]+)');
-- Result: NULL
```

### REGEXP_EXTRACT_ALL

Extracts all matches of a pattern from a string.

**Signature:** `REGEXP_EXTRACT_ALL(string, pattern [, group]) -> LIST<VARCHAR>`

**Parameters:**
- `string` (VARCHAR): The input string
- `pattern` (VARCHAR): RE2 regular expression pattern
- `group` (INTEGER, optional): The capture group to extract. Default is 0.

**Returns:** An array of all matched substrings

**Examples:**
```sql
-- Extract all prices
SELECT REGEXP_EXTRACT_ALL('Prices: $10.50, $20.99, $5.00', '\$([0-9.]+)', 1);
-- Result: ['10.50', '20.99', '5.00']

-- Extract all words
SELECT REGEXP_EXTRACT_ALL('hello world foo bar', '\w+');
-- Result: ['hello', 'world', 'foo', 'bar']

-- No matches returns empty array
SELECT REGEXP_EXTRACT_ALL('hello world', '\d+');
-- Result: []
```

### REGEXP_SPLIT_TO_ARRAY

Splits a string by a regular expression pattern.

**Signature:** `REGEXP_SPLIT_TO_ARRAY(string, pattern) -> LIST<VARCHAR>`

**Parameters:**
- `string` (VARCHAR): The input string to split
- `pattern` (VARCHAR): RE2 pattern to split on

**Returns:** An array of substrings

**Examples:**
```sql
-- Split by multiple delimiters
SELECT REGEXP_SPLIT_TO_ARRAY('one,two;three:four', '[,;:]');
-- Result: ['one', 'two', 'three', 'four']

-- Split by whitespace
SELECT REGEXP_SPLIT_TO_ARRAY('hello   world  foo', '\s+');
-- Result: ['hello', 'world', 'foo']

-- No matches returns single-element array
SELECT REGEXP_SPLIT_TO_ARRAY('no-separators-here', ',');
-- Result: ['no-separators-here']
```

---

## String Concatenation and Splitting

### CONCAT_WS

Concatenates strings with a separator, **skipping NULL values**.

**Signature:** `CONCAT_WS(separator, str1, str2, ...) -> VARCHAR`

**Parameters:**
- `separator` (VARCHAR): The separator to place between strings
- `str1, str2, ...` (VARCHAR): Strings to concatenate

**Returns:** The concatenated string

**NULL Handling:** Unlike most string functions, CONCAT_WS **skips NULL values** rather than propagating NULL. This is the exception to the standard NULL propagation rule.

**Examples:**
```sql
-- Basic concatenation
SELECT CONCAT_WS(' ', 'John', 'M', 'Doe');
-- Result: 'John M Doe'

-- NULL values are skipped
SELECT CONCAT_WS(' ', 'John', NULL, 'Doe');
-- Result: 'John Doe'

-- All NULL values returns empty string
SELECT CONCAT_WS(',', NULL, NULL, NULL);
-- Result: ''

-- NULL separator returns NULL
SELECT CONCAT_WS(NULL, 'a', 'b');
-- Result: NULL

-- Building CSV lines
SELECT CONCAT_WS(',', name, CAST(age AS VARCHAR), city) FROM users;
```

### STRING_SPLIT

Splits a string by a literal separator (not a regex pattern).

**Signature:** `STRING_SPLIT(string, separator) -> LIST<VARCHAR>`

**Parameters:**
- `string` (VARCHAR): The input string to split
- `separator` (VARCHAR): The literal separator string

**Returns:** An array of substrings

**Note:** For regex-based splitting, use `REGEXP_SPLIT_TO_ARRAY`.

**Examples:**
```sql
-- Split by comma
SELECT STRING_SPLIT('one,two,three', ',');
-- Result: ['one', 'two', 'three']

-- Empty separator splits into characters
SELECT STRING_SPLIT('hello', '');
-- Result: ['h', 'e', 'l', 'l', 'o']

-- Multi-character separator
SELECT STRING_SPLIT('foo::bar::baz', '::');
-- Result: ['foo', 'bar', 'baz']
```

---

## Padding Functions

### LPAD

Left-pads a string to a specified length.

**Signature:** `LPAD(string, length [, fill]) -> VARCHAR`

**Parameters:**
- `string` (VARCHAR): The input string
- `length` (INTEGER): The target length
- `fill` (VARCHAR, optional): The padding character(s). Default is space `' '`.

**Returns:** The padded string, or truncated if longer than target length

**Examples:**
```sql
-- Pad with spaces (default)
SELECT LPAD('hello', 10);
-- Result: '     hello'

-- Pad with custom character
SELECT LPAD('hello', 10, '*');
-- Result: '*****hello'

-- Multi-character fill
SELECT LPAD('hello', 12, 'xy');
-- Result: 'xyxyxyxhello'

-- Truncation when string exceeds length
SELECT LPAD('hello world', 5);
-- Result: 'hello'

-- Zero-padding numbers
SELECT LPAD(CAST(42 AS VARCHAR), 5, '0');
-- Result: '00042'
```

### RPAD

Right-pads a string to a specified length.

**Signature:** `RPAD(string, length [, fill]) -> VARCHAR`

**Parameters:**
- `string` (VARCHAR): The input string
- `length` (INTEGER): The target length
- `fill` (VARCHAR, optional): The padding character(s). Default is space `' '`.

**Returns:** The padded string, or truncated if longer than target length

**Examples:**
```sql
-- Pad with spaces (default)
SELECT RPAD('hello', 10);
-- Result: 'hello     '

-- Pad with custom character
SELECT RPAD('hello', 10, '*');
-- Result: 'hello*****'

-- Creating fixed-width columns
SELECT RPAD(name, 20) || LPAD(CAST(price AS VARCHAR), 10) FROM products;
```

---

## String Manipulation Functions

### REVERSE

Reverses a string. Handles Unicode characters correctly.

**Signature:** `REVERSE(string) -> VARCHAR`

**Examples:**
```sql
SELECT REVERSE('hello');    -- Result: 'olleh'
SELECT REVERSE('cafe');      -- Result: 'efac' (Unicode-safe)
```

### REPEAT

Repeats a string N times.

**Signature:** `REPEAT(string, count) -> VARCHAR`

**Parameters:**
- `string` (VARCHAR): The string to repeat
- `count` (INTEGER): Number of repetitions (must be >= 0)

**Error:** Returns error if count is negative: "REPEAT count must be non-negative"

**Examples:**
```sql
SELECT REPEAT('ab', 3);      -- Result: 'ababab'
SELECT REPEAT('hello', 0);   -- Result: ''
SELECT REPEAT('-', 50);      -- Result: '--------------------------------------------------'
```

### LEFT

Extracts the leftmost N characters from a string.

**Signature:** `LEFT(string, count) -> VARCHAR`

**Examples:**
```sql
SELECT LEFT('hello world', 5);   -- Result: 'hello'
SELECT LEFT('hello', 100);       -- Result: 'hello' (full string if count > length)
SELECT LEFT('hello', -1);        -- Result: '' (empty string for negative count)
```

### RIGHT

Extracts the rightmost N characters from a string.

**Signature:** `RIGHT(string, count) -> VARCHAR`

**Examples:**
```sql
SELECT RIGHT('hello world', 5);  -- Result: 'world'
SELECT RIGHT('hello', 100);      -- Result: 'hello' (full string if count > length)
SELECT RIGHT('hello', -1);       -- Result: '' (empty string for negative count)
```

### POSITION

Finds the 1-based position of a substring within a string.

**Signature:** `POSITION(substring IN string) -> BIGINT`

**Returns:** The 1-based position of the first occurrence, or 0 if not found

**Note:** SQL uses 1-based indexing. Position 0 means "not found".

**Examples:**
```sql
SELECT POSITION('world' IN 'hello world');  -- Result: 7
SELECT POSITION('foo' IN 'hello world');    -- Result: 0 (not found)
SELECT POSITION('l' IN 'hello');            -- Result: 3 (first 'l')
```

### STRPOS and INSTR

Aliases for finding substring position with reversed argument order.

**Signature:**
- `STRPOS(string, substring) -> BIGINT`
- `INSTR(string, substring) -> BIGINT`

**Examples:**
```sql
SELECT STRPOS('hello world', 'world');  -- Result: 7
SELECT INSTR('hello world', 'world');   -- Result: 7
```

### CONTAINS

Tests if a string contains a substring.

**Signature:** `CONTAINS(string, substring) -> BOOLEAN`

**Examples:**
```sql
SELECT CONTAINS('hello world', 'world');  -- Result: TRUE
SELECT CONTAINS('hello world', 'foo');    -- Result: FALSE
```

### PREFIX / STARTS_WITH

Tests if a string starts with a prefix.

**Signature:**
- `PREFIX(string, prefix) -> BOOLEAN`
- `STARTS_WITH(string, prefix) -> BOOLEAN`

**Examples:**
```sql
SELECT PREFIX('hello world', 'hello');       -- Result: TRUE
SELECT STARTS_WITH('hello world', 'world');  -- Result: FALSE
```

### SUFFIX / ENDS_WITH

Tests if a string ends with a suffix.

**Signature:**
- `SUFFIX(string, suffix) -> BOOLEAN`
- `ENDS_WITH(string, suffix) -> BOOLEAN`

**Examples:**
```sql
SELECT SUFFIX('hello world', 'world');     -- Result: TRUE
SELECT ENDS_WITH('hello world', 'hello');  -- Result: FALSE
```

---

## Character Encoding Functions

### ASCII

Returns the ASCII code of the first character.

**Signature:** `ASCII(character) -> BIGINT`

**Examples:**
```sql
SELECT ASCII('A');    -- Result: 65
SELECT ASCII('a');    -- Result: 97
SELECT ASCII('');     -- Result: 0 (empty string)
SELECT ASCII('ABC');  -- Result: 65 (first character only)
```

### CHR

Converts an ASCII code to a character.

**Signature:** `CHR(code) -> VARCHAR`

**Parameters:**
- `code` (INTEGER): ASCII code in range [0, 127]

**Error:** Returns error if code is outside ASCII range: "CHR code must be in ASCII range [0, 127]"

**Examples:**
```sql
SELECT CHR(65);   -- Result: 'A'
SELECT CHR(97);   -- Result: 'a'
SELECT CHR(0);    -- Result: '' (null character)
SELECT CHR(200);  -- ERROR: CHR code must be in ASCII range [0, 127]
```

### UNICODE

Returns the Unicode codepoint of the first character.

**Signature:** `UNICODE(character) -> BIGINT`

**Examples:**
```sql
SELECT UNICODE('A');    -- Result: 65
SELECT UNICODE('e');    -- Result: 233
SELECT UNICODE('');    -- Result: 9733 (star symbol)
SELECT UNICODE('');     -- Result: 0 (empty string)
```

---

## Cryptographic Hash Functions

All hash functions return **lowercase hexadecimal strings**.

### MD5

Generates an MD5 hash (32 hex characters).

**Signature:** `MD5(string) -> VARCHAR`

**Output Format:** 32-character lowercase hexadecimal string

**Examples:**
```sql
SELECT MD5('hello');
-- Result: '5d41402abc4b2a76b9719d911017c592'

SELECT MD5('');
-- Result: 'd41d8cd98f00b204e9800998ecf8427e' (hash of empty string)
```

### SHA256

Generates a SHA-256 hash (64 hex characters).

**Signature:** `SHA256(string) -> VARCHAR`

**Output Format:** 64-character lowercase hexadecimal string

**Examples:**
```sql
SELECT SHA256('hello');
-- Result: '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'

SELECT SHA256('');
-- Result: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
```

### HASH

Generates a 64-bit hash using FNV-1a algorithm.

**Signature:** `HASH(string) -> BIGINT`

**Output Format:** Signed 64-bit integer

**Examples:**
```sql
SELECT HASH('hello');
-- Result: (a 64-bit integer, e.g., -5234425846883963895)

-- Useful for hash-based partitioning
SELECT *, HASH(customer_id) % 10 AS partition FROM customers;
```

---

## String Distance Functions

String distance functions measure how different two strings are. **Lower values indicate more similar strings** for distance metrics.

### LEVENSHTEIN

Calculates the Levenshtein edit distance (minimum number of single-character edits: insertions, deletions, or substitutions).

**Signature:** `LEVENSHTEIN(string1, string2) -> BIGINT`

**Interpretation:** Lower values = more similar strings. 0 = identical strings.

**Performance:** O(n * m) time and space complexity where n and m are the string lengths.

**Examples:**
```sql
SELECT LEVENSHTEIN('kitten', 'sitting');  -- Result: 3 (k->s, e->i, +g)
SELECT LEVENSHTEIN('hello', 'hello');     -- Result: 0 (identical)
SELECT LEVENSHTEIN('abc', 'xyz');         -- Result: 3 (all different)
SELECT LEVENSHTEIN('', 'hello');          -- Result: 5 (5 insertions)
```

### DAMERAU_LEVENSHTEIN

Calculates the Damerau-Levenshtein distance, which extends Levenshtein by also counting **transpositions** (swapping two adjacent characters) as a single edit.

**Signature:** `DAMERAU_LEVENSHTEIN(string1, string2) -> BIGINT`

**Transposition Support:** Swapping two adjacent characters counts as 1 edit (not 2 as in standard Levenshtein).

**Performance:** O(n * m) time and space complexity.

**Examples:**
```sql
-- Transposition example
SELECT DAMERAU_LEVENSHTEIN('ab', 'ba');   -- Result: 1 (one transposition)
SELECT LEVENSHTEIN('ab', 'ba');           -- Result: 2 (for comparison)

SELECT DAMERAU_LEVENSHTEIN('hello', 'hello');  -- Result: 0 (identical)
SELECT DAMERAU_LEVENSHTEIN('ca', 'abc');       -- Result: 2

-- Useful for typo detection
SELECT * FROM products
WHERE DAMERAU_LEVENSHTEIN(name, 'keyboard') <= 2;
```

### HAMMING

Calculates the Hamming distance (number of positions where characters differ). **Requires strings of equal length.**

**Signature:** `HAMMING(string1, string2) -> BIGINT`

**Requirement:** Both strings must have the same length

**Error:** Returns error if strings have different lengths: "HAMMING requires strings of equal length"

**Examples:**
```sql
SELECT HAMMING('karolin', 'kathrin');   -- Result: 3
SELECT HAMMING('1011101', '1001001');   -- Result: 2
SELECT HAMMING('hello', 'world');       -- Result: 4
SELECT HAMMING('hello', 'world!');      -- ERROR: HAMMING requires strings of equal length
```

### JACCARD

Calculates the Jaccard similarity coefficient based on character sets.

**Signature:** `JACCARD(string1, string2) -> DOUBLE`

**Returns:** A value in the range [0, 1] where:
- 1.0 = identical character sets
- 0.0 = no common characters

**Formula:** |intersection| / |union| of character sets

**Examples:**
```sql
SELECT JACCARD('hello', 'hallo');  -- Result: ~0.8 (4 common / 5 total)
SELECT JACCARD('abc', 'abc');      -- Result: 1.0 (identical)
SELECT JACCARD('abc', 'xyz');      -- Result: 0.0 (no common chars)
```

### JARO_SIMILARITY

Calculates the Jaro similarity between two strings.

**Signature:** `JARO_SIMILARITY(string1, string2) -> DOUBLE`

**Returns:** A value in the range [0, 1] where:
- 1.0 = identical strings
- 0.0 = completely different strings

**Algorithm:** Considers matching characters within a certain distance and the number of transpositions.

**Examples:**
```sql
SELECT JARO_SIMILARITY('martha', 'marhta');   -- Result: ~0.944
SELECT JARO_SIMILARITY('hello', 'hello');     -- Result: 1.0
SELECT JARO_SIMILARITY('abc', 'xyz');         -- Result: 0.0
```

### JARO_WINKLER_SIMILARITY

Calculates the Jaro-Winkler similarity, which adds a prefix bonus to the Jaro similarity.

**Signature:** `JARO_WINKLER_SIMILARITY(string1, string2) -> DOUBLE`

**Returns:** A value in the range [0, 1] where:
- 1.0 = identical strings
- 0.0 = completely different strings

**Prefix Bonus:** Strings that match from the beginning receive a higher score. The bonus is applied for up to 4 matching prefix characters.

**Examples:**
```sql
-- Higher than Jaro due to common prefix 'mar'
SELECT JARO_WINKLER_SIMILARITY('martha', 'marhta');  -- Result: ~0.961
SELECT JARO_SIMILARITY('martha', 'marhta');          -- Result: ~0.944

SELECT JARO_WINKLER_SIMILARITY('hello', 'hello');    -- Result: 1.0
SELECT JARO_WINKLER_SIMILARITY('abc', 'xyz');        -- Result: 0.0
```

---

## Whitespace Trimming Aliases

These are Python/PostgreSQL-compatible aliases for existing trimming functions.

| Alias | Equivalent |
|-------|------------|
| `STRIP(string)` | `TRIM(string)` |
| `LSTRIP(string)` | `LTRIM(string)` |
| `RSTRIP(string)` | `RTRIM(string)` |

**Examples:**
```sql
SELECT STRIP('  hello  ');   -- Result: 'hello'
SELECT LSTRIP('  hello  ');  -- Result: 'hello  '
SELECT RSTRIP('  hello  ');  -- Result: '  hello'
```

---

## NULL Handling

### Standard NULL Propagation

All string functions follow standard SQL NULL propagation: **if any input argument is NULL, the result is NULL**.

**Examples:**
```sql
SELECT REVERSE(NULL);              -- Result: NULL
SELECT REGEXP_MATCHES('hello', NULL);  -- Result: NULL
SELECT LEVENSHTEIN('hello', NULL);     -- Result: NULL
SELECT LPAD(NULL, 10);             -- Result: NULL
SELECT MD5(NULL);                  -- Result: NULL
```

### Exception: CONCAT_WS

`CONCAT_WS` is the only exception. It **skips NULL values** rather than propagating them.

```sql
SELECT CONCAT_WS(',', 'a', NULL, 'b');  -- Result: 'a,b' (NULL skipped)
SELECT CONCAT_WS(',', NULL, NULL);      -- Result: '' (empty string)

-- But NULL separator still returns NULL
SELECT CONCAT_WS(NULL, 'a', 'b');       -- Result: NULL
```

---

## Error Handling

### Invalid Regular Expression

Regex functions return an error when given an invalid pattern.

**Error Message:** "Invalid regular expression: <details>"

```sql
SELECT REGEXP_MATCHES('test', '[');
-- ERROR: Invalid regular expression: error parsing regexp: missing closing ]: `[`
```

### HAMMING Length Mismatch

**Error Message:** "HAMMING requires strings of equal length"

```sql
SELECT HAMMING('hello', 'world!');
-- ERROR: HAMMING requires strings of equal length
```

### CHR Out of Range

**Error Message:** "CHR code must be in ASCII range [0, 127]"

```sql
SELECT CHR(200);
-- ERROR: CHR code must be in ASCII range [0, 127]
```

### REPEAT Negative Count

**Error Message:** "REPEAT count must be non-negative"

```sql
SELECT REPEAT('x', -1);
-- ERROR: REPEAT count must be non-negative
```

---

## Performance Considerations

### LEVENSHTEIN and DAMERAU_LEVENSHTEIN

**Time Complexity:** O(n * m) where n and m are string lengths
**Space Complexity:** O(n * m) for the dynamic programming matrix

**Recommendations:**
- Avoid using on very long strings (>1000 characters) in hot paths
- Consider pre-filtering candidates with cheaper operations like `CONTAINS` or prefix matching
- Use `LIMIT` to avoid computing distances for all rows

```sql
-- Efficient: Pre-filter with cheaper operation
SELECT * FROM products
WHERE name LIKE 'key%' AND LEVENSHTEIN(name, 'keyboard') <= 2;

-- Less efficient: Compute distance for all rows
SELECT * FROM products
WHERE LEVENSHTEIN(name, 'keyboard') <= 2;
```

### Regular Expression Functions

- Patterns are compiled for each execution; for repeated use with the same pattern, performance is good due to Go's regexp caching
- Avoid overly complex patterns with many alternations or nested groups
- `REGEXP_MATCHES` (boolean check) is faster than `REGEXP_EXTRACT` (extraction)

### Hash Functions

- MD5 and SHA256 use Go's optimized `crypto` implementations
- HASH (FNV-1a) is the fastest for non-cryptographic use cases

---

## Common Use Cases

### Log Parsing

```sql
-- Extract timestamp, level, and message from log entries
SELECT
  REGEXP_EXTRACT(line, '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', 1) AS timestamp,
  REGEXP_EXTRACT(line, '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} (\w+)', 1) AS level,
  REGEXP_EXTRACT(line, '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \w+ (.*)', 1) AS message
FROM logs;

-- Filter error logs
SELECT * FROM logs
WHERE REGEXP_MATCHES(line, '(ERROR|FATAL|CRITICAL)');
```

### Data Cleaning

```sql
-- Normalize phone numbers
SELECT REGEXP_REPLACE(phone, '[^0-9]', '', 'g') AS clean_phone FROM contacts;

-- Trim whitespace and normalize spaces
SELECT REGEXP_REPLACE(TRIM(name), '\s+', ' ', 'g') AS clean_name FROM users;

-- Remove HTML tags
SELECT REGEXP_REPLACE(content, '<[^>]+>', '', 'g') AS plain_text FROM articles;

-- Standardize case
SELECT UPPER(TRIM(email)) AS normalized_email FROM users;
```

### Fuzzy Matching

```sql
-- Find similar product names
SELECT p1.name, p2.name, LEVENSHTEIN(p1.name, p2.name) AS distance
FROM products p1
CROSS JOIN products p2
WHERE p1.id < p2.id
  AND LEVENSHTEIN(p1.name, p2.name) <= 3
ORDER BY distance;

-- Search with typo tolerance
SELECT * FROM customers
WHERE JARO_WINKLER_SIMILARITY(name, 'Jon Smith') > 0.9;

-- Deduplication based on similarity
SELECT DISTINCT ON (cluster_id) *
FROM (
  SELECT *, CASE
    WHEN JARO_WINKLER_SIMILARITY(name, LAG(name) OVER (ORDER BY name)) > 0.95
    THEN 0 ELSE 1
  END AS cluster_id
  FROM companies
) sub;
```

### Data Validation

```sql
-- Validate email format
SELECT email, REGEXP_MATCHES(email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') AS valid
FROM users;

-- Check for valid phone numbers
SELECT phone, REGEXP_MATCHES(phone, '^\+?[1-9]\d{1,14}$') AS valid_e164
FROM contacts;

-- Validate date format
SELECT date_str, REGEXP_MATCHES(date_str, '^\d{4}-\d{2}-\d{2}$') AS valid_iso
FROM imports;
```

### Data Integrity

```sql
-- Verify checksums
SELECT filename, stored_hash, MD5(content) AS computed_hash,
       stored_hash = MD5(content) AS valid
FROM files;

-- Generate unique identifiers
SELECT SHA256(CONCAT_WS('|', customer_id, email, created_at)) AS fingerprint
FROM customers;
```

---

## Requirements

### Requirement: Regular Expression Matching

The system SHALL provide regular expression matching functions using RE2 syntax for pattern-based text searching.

#### Scenario: REGEXP_MATCHES tests pattern match
- GIVEN a string 'hello world'
- WHEN executing `SELECT REGEXP_MATCHES('hello world', 'h.*o')`
- THEN result is TRUE

#### Scenario: REGEXP_MATCHES with non-matching pattern
- GIVEN a string 'hello world'
- WHEN executing `SELECT REGEXP_MATCHES('hello world', '^world')`
- THEN result is FALSE

#### Scenario: REGEXP_MATCHES with invalid pattern
- GIVEN an invalid regex pattern '['
- WHEN executing `SELECT REGEXP_MATCHES('test', '[')`
- THEN an error is returned
- AND error message contains "Invalid regular expression"

#### Scenario: REGEXP_MATCHES with NULL input
- GIVEN a NULL value
- WHEN executing `SELECT REGEXP_MATCHES(NULL, 'pattern')`
- THEN result is NULL

### Requirement: Regular Expression Replacement

The system SHALL provide regular expression replacement functions with support for global and first-match-only modes.

#### Scenario: REGEXP_REPLACE replaces first match only by default
- GIVEN a string 'hello world hello'
- WHEN executing `SELECT REGEXP_REPLACE('hello world hello', 'hello', 'hi')`
- THEN result is 'hi world hello' (only first occurrence replaced)

#### Scenario: REGEXP_REPLACE with 'g' flag replaces all matches
- GIVEN a string 'hello world hello'
- WHEN executing `SELECT REGEXP_REPLACE('hello world hello', 'hello', 'hi', 'g')`
- THEN result is 'hi world hi' (all occurrences replaced)

#### Scenario: REGEXP_REPLACE with no matches
- GIVEN a string 'hello world'
- WHEN executing `SELECT REGEXP_REPLACE('hello world', 'foo', 'bar')`
- THEN result is 'hello world' (unchanged)

#### Scenario: REGEXP_REPLACE with capture groups
- GIVEN a string 'John Doe'
- WHEN executing `SELECT REGEXP_REPLACE('John Doe', '(\w+) (\w+)', '$2, $1')`
- THEN result is 'Doe, John'

### Requirement: Regular Expression Extraction

The system SHALL provide functions to extract matched substrings from regular expressions with optional group selection.

#### Scenario: REGEXP_EXTRACT extracts first match
- GIVEN a string 'Price: $19.99'
- WHEN executing `SELECT REGEXP_EXTRACT('Price: $19.99', '\$([0-9.]+)')`
- THEN result is '$19.99' (group 0, full match)

#### Scenario: REGEXP_EXTRACT with group parameter
- GIVEN a string 'Price: $19.99'
- WHEN executing `SELECT REGEXP_EXTRACT('Price: $19.99', '\$([0-9.]+)', 1)`
- THEN result is '19.99' (group 1, captured value)

#### Scenario: REGEXP_EXTRACT with no match
- GIVEN a string 'No price here'
- WHEN executing `SELECT REGEXP_EXTRACT('No price here', '\$([0-9.]+)')`
- THEN result is NULL

#### Scenario: REGEXP_EXTRACT_ALL extracts all matches
- GIVEN a string 'Prices: $10.50, $20.99, $5.00'
- WHEN executing `SELECT REGEXP_EXTRACT_ALL('Prices: $10.50, $20.99, $5.00', '\$([0-9.]+)', 1)`
- THEN result is ['10.50', '20.99', '5.00'] (array of all group 1 matches)

### Requirement: Regular Expression Splitting

The system SHALL provide functions to split strings by regular expression patterns into arrays.

#### Scenario: REGEXP_SPLIT_TO_ARRAY splits by pattern
- GIVEN a string 'one,two;three:four'
- WHEN executing `SELECT REGEXP_SPLIT_TO_ARRAY('one,two;three:four', '[,;:]')`
- THEN result is ['one', 'two', 'three', 'four']

#### Scenario: REGEXP_SPLIT_TO_ARRAY with no matches
- GIVEN a string 'no-separators-here'
- WHEN executing `SELECT REGEXP_SPLIT_TO_ARRAY('no-separators-here', ',')`
- THEN result is ['no-separators-here'] (single-element array)

### Requirement: String Concatenation with Separator

The system SHALL provide a function to concatenate strings with a separator, skipping NULL values.

#### Scenario: CONCAT_WS concatenates with separator
- GIVEN values 'John', 'M', 'Doe'
- WHEN executing `SELECT CONCAT_WS(' ', 'John', 'M', 'Doe')`
- THEN result is 'John M Doe'

#### Scenario: CONCAT_WS skips NULL values
- GIVEN values 'John', NULL, 'Doe'
- WHEN executing `SELECT CONCAT_WS(' ', 'John', NULL, 'Doe')`
- THEN result is 'John Doe' (NULL value skipped)

#### Scenario: CONCAT_WS with all NULL values
- GIVEN all NULL values
- WHEN executing `SELECT CONCAT_WS(',', NULL, NULL, NULL)`
- THEN result is '' (empty string)

### Requirement: String Splitting

The system SHALL provide functions to split strings by separators into arrays.

#### Scenario: STRING_SPLIT splits by separator
- GIVEN a string 'one,two,three'
- WHEN executing `SELECT STRING_SPLIT('one,two,three', ',')`
- THEN result is ['one', 'two', 'three']

#### Scenario: STRING_SPLIT with empty separator splits into characters
- GIVEN a string 'hello'
- WHEN executing `SELECT STRING_SPLIT('hello', '')`
- THEN result is ['h', 'e', 'l', 'l', 'o']

#### Scenario: STRING_SPLIT with NULL input
- GIVEN a NULL value
- WHEN executing `SELECT STRING_SPLIT(NULL, ',')`
- THEN result is NULL

### Requirement: String Padding

The system SHALL provide functions to pad strings to a specified length with optional fill characters.

#### Scenario: LPAD pads left with spaces by default
- GIVEN a string 'hello'
- WHEN executing `SELECT LPAD('hello', 10)`
- THEN result is '     hello' (5 spaces added)

#### Scenario: LPAD with custom fill character
- GIVEN a string 'hello'
- WHEN executing `SELECT LPAD('hello', 10, '*')`
- THEN result is '*****hello'

#### Scenario: LPAD truncates if string exceeds length
- GIVEN a string 'hello world'
- WHEN executing `SELECT LPAD('hello world', 5)`
- THEN result is 'hello' (truncated to 5 characters)

#### Scenario: RPAD pads right with spaces by default
- GIVEN a string 'hello'
- WHEN executing `SELECT RPAD('hello', 10)`
- THEN result is 'hello     ' (5 spaces added)

### Requirement: String Reversal and Repetition

The system SHALL provide functions to reverse and repeat strings.

#### Scenario: REVERSE reverses string
- GIVEN a string 'hello'
- WHEN executing `SELECT REVERSE('hello')`
- THEN result is 'olleh'

#### Scenario: REVERSE handles Unicode characters
- GIVEN a string 'cafe'
- WHEN executing `SELECT REVERSE('cafe')`
- THEN result is 'efac'

#### Scenario: REPEAT repeats string N times
- GIVEN a string 'ab'
- WHEN executing `SELECT REPEAT('ab', 3)`
- THEN result is 'ababab'

#### Scenario: REPEAT with zero count
- GIVEN a string 'hello'
- WHEN executing `SELECT REPEAT('hello', 0)`
- THEN result is '' (empty string)

#### Scenario: REPEAT with negative count causes error
- GIVEN a string 'hello'
- WHEN executing `SELECT REPEAT('hello', -1)`
- THEN an error is returned
- AND error message contains "REPEAT count must be non-negative"

### Requirement: Substring Extraction

The system SHALL provide functions to extract left and right substrings.

#### Scenario: LEFT extracts left N characters
- GIVEN a string 'hello world'
- WHEN executing `SELECT LEFT('hello world', 5)`
- THEN result is 'hello'

#### Scenario: LEFT with count exceeding length
- GIVEN a string 'hello'
- WHEN executing `SELECT LEFT('hello', 100)`
- THEN result is 'hello' (full string)

#### Scenario: RIGHT extracts right N characters
- GIVEN a string 'hello world'
- WHEN executing `SELECT RIGHT('hello world', 5)`
- THEN result is 'world'

#### Scenario: RIGHT with negative count
- GIVEN a string 'hello'
- WHEN executing `SELECT RIGHT('hello', -1)`
- THEN result is '' (empty string)

### Requirement: Substring Position

The system SHALL provide functions to find the position of a substring within a string using 1-based indexing.

#### Scenario: POSITION finds substring location
- GIVEN a string 'hello world'
- WHEN executing `SELECT POSITION('world' IN 'hello world')`
- THEN result is 7 (1-based index)

#### Scenario: POSITION with substring not found
- GIVEN a string 'hello world'
- WHEN executing `SELECT POSITION('foo' IN 'hello world')`
- THEN result is 0 (not found)

#### Scenario: STRPOS is alias with reversed arguments
- GIVEN a string 'hello world'
- WHEN executing `SELECT STRPOS('hello world', 'world')`
- THEN result is 7

#### Scenario: INSTR is alias for STRPOS
- GIVEN a string 'hello world'
- WHEN executing `SELECT INSTR('hello world', 'world')`
- THEN result is 7

### Requirement: Substring Contains and Affixes

The system SHALL provide functions to test if strings contain substrings or start/end with specific prefixes/suffixes.

#### Scenario: CONTAINS tests if substring exists
- GIVEN a string 'hello world'
- WHEN executing `SELECT CONTAINS('hello world', 'world')`
- THEN result is TRUE

#### Scenario: CONTAINS with substring not found
- GIVEN a string 'hello world'
- WHEN executing `SELECT CONTAINS('hello world', 'foo')`
- THEN result is FALSE

#### Scenario: PREFIX tests if string starts with prefix
- GIVEN a string 'hello world'
- WHEN executing `SELECT PREFIX('hello world', 'hello')`
- THEN result is TRUE

#### Scenario: STARTS_WITH is alias for PREFIX
- GIVEN a string 'hello world'
- WHEN executing `SELECT STARTS_WITH('hello world', 'hello')`
- THEN result is TRUE

#### Scenario: SUFFIX tests if string ends with suffix
- GIVEN a string 'hello world'
- WHEN executing `SELECT SUFFIX('hello world', 'world')`
- THEN result is TRUE

#### Scenario: ENDS_WITH is alias for SUFFIX
- GIVEN a string 'hello world'
- WHEN executing `SELECT ENDS_WITH('hello world', 'world')`
- THEN result is TRUE

### Requirement: Character Encoding Functions

The system SHALL provide functions to convert between characters and their numeric codes.

#### Scenario: ASCII returns ASCII code
- GIVEN a character 'A'
- WHEN executing `SELECT ASCII('A')`
- THEN result is 65

#### Scenario: ASCII with empty string
- GIVEN an empty string ''
- WHEN executing `SELECT ASCII('')`
- THEN result is 0

#### Scenario: CHR converts ASCII code to character
- GIVEN a code 65
- WHEN executing `SELECT CHR(65)`
- THEN result is 'A'

#### Scenario: CHR with out-of-range code causes error
- GIVEN a code 200
- WHEN executing `SELECT CHR(200)`
- THEN an error is returned
- AND error message contains "CHR code must be in ASCII range [0, 127]"

#### Scenario: UNICODE returns Unicode codepoint
- GIVEN a character 'e'
- WHEN executing `SELECT UNICODE('e')`
- THEN result is 233

### Requirement: Cryptographic Hash Functions

The system SHALL provide cryptographic hash functions returning lowercase hexadecimal strings.

#### Scenario: MD5 generates 32-character hex hash
- GIVEN a string 'hello'
- WHEN executing `SELECT MD5('hello')`
- THEN result is '5d41402abc4b2a76b9719d911017c592'

#### Scenario: SHA256 generates 64-character hex hash
- GIVEN a string 'hello'
- WHEN executing `SELECT SHA256('hello')`
- THEN result is '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'

#### Scenario: HASH generates signed 64-bit integer
- GIVEN a string 'hello'
- WHEN executing `SELECT HASH('hello')`
- THEN result is a BIGINT value (specific value depends on FNV-1a algorithm)

#### Scenario: Hash functions with NULL input
- GIVEN a NULL value
- WHEN executing `SELECT MD5(NULL)`
- THEN result is NULL

### Requirement: String Distance Functions

The system SHALL provide string distance and similarity measurement functions.

#### Scenario: LEVENSHTEIN calculates edit distance
- GIVEN strings 'kitten' and 'sitting'
- WHEN executing `SELECT LEVENSHTEIN('kitten', 'sitting')`
- THEN result is 3 (3 edits: k->s, e->i, insert g)

#### Scenario: LEVENSHTEIN with identical strings
- GIVEN strings 'hello' and 'hello'
- WHEN executing `SELECT LEVENSHTEIN('hello', 'hello')`
- THEN result is 0

#### Scenario: LEVENSHTEIN with completely different strings
- GIVEN strings 'abc' and 'xyz'
- WHEN executing `SELECT LEVENSHTEIN('abc', 'xyz')`
- THEN result is 3

#### Scenario: DAMERAU_LEVENSHTEIN calculates edit distance with transpositions
- GIVEN strings 'ca' and 'abc'
- WHEN executing `SELECT DAMERAU_LEVENSHTEIN('ca', 'abc')`
- THEN result is 2 (delete c, add b, add c OR other paths)

#### Scenario: DAMERAU_LEVENSHTEIN handles transposition as single edit
- GIVEN strings 'ab' and 'ba'
- WHEN executing `SELECT DAMERAU_LEVENSHTEIN('ab', 'ba')`
- THEN result is 1 (one transposition)

#### Scenario: DAMERAU_LEVENSHTEIN with identical strings
- GIVEN strings 'hello' and 'hello'
- WHEN executing `SELECT DAMERAU_LEVENSHTEIN('hello', 'hello')`
- THEN result is 0

#### Scenario: DAMERAU_LEVENSHTEIN with NULL input
- GIVEN a NULL value
- WHEN executing `SELECT DAMERAU_LEVENSHTEIN('hello', NULL)`
- THEN result is NULL

#### Scenario: HAMMING calculates bit difference for equal-length strings
- GIVEN strings 'karolin' and 'kathrin'
- WHEN executing `SELECT HAMMING('karolin', 'kathrin')`
- THEN result is 3

#### Scenario: HAMMING with unequal-length strings causes error
- GIVEN strings 'hello' and 'world!'
- WHEN executing `SELECT HAMMING('hello', 'world!')`
- THEN an error is returned
- AND error message contains "HAMMING requires strings of equal length"

#### Scenario: JACCARD calculates character set similarity
- GIVEN strings 'hello' and 'hallo'
- WHEN executing `SELECT JACCARD('hello', 'hallo')`
- THEN result is approximately 0.8 (4 common chars / 5 total unique chars)

#### Scenario: JARO_SIMILARITY calculates Jaro similarity
- GIVEN strings 'martha' and 'marhta'
- WHEN executing `SELECT JARO_SIMILARITY('martha', 'marhta')`
- THEN result is approximately 0.944

#### Scenario: JARO_WINKLER_SIMILARITY with common prefix
- GIVEN strings 'martha' and 'marhta'
- WHEN executing `SELECT JARO_WINKLER_SIMILARITY('martha', 'marhta')`
- THEN result is approximately 0.961 (higher than Jaro due to common prefix 'mar')

#### Scenario: JARO_WINKLER_SIMILARITY with identical strings
- GIVEN strings 'hello' and 'hello'
- WHEN executing `SELECT JARO_WINKLER_SIMILARITY('hello', 'hello')`
- THEN result is 1.0

### Requirement: Whitespace Trimming Aliases

The system SHALL provide aliases for trimming functions compatible with Python/PostgreSQL naming.

#### Scenario: STRIP is alias for TRIM
- GIVEN a string '  hello  '
- WHEN executing `SELECT STRIP('  hello  ')`
- THEN result is 'hello'

#### Scenario: LSTRIP is alias for LTRIM
- GIVEN a string '  hello  '
- WHEN executing `SELECT LSTRIP('  hello  ')`
- THEN result is 'hello  '

#### Scenario: RSTRIP is alias for RTRIM
- GIVEN a string '  hello  '
- WHEN executing `SELECT RSTRIP('  hello  ')`
- THEN result is '  hello'

### Requirement: Type Coercion for String Functions

The system SHALL automatically coerce non-string inputs to VARCHAR for string functions.

#### Scenario: Integer input to string functions
- GIVEN an INTEGER value 12345
- WHEN executing `SELECT REVERSE(12345)`
- THEN value is coerced to VARCHAR
- AND result is '54321'

#### Scenario: Boolean input to CONCAT_WS
- GIVEN boolean values TRUE and FALSE
- WHEN executing `SELECT CONCAT_WS(',', TRUE, FALSE)`
- THEN values are coerced to VARCHAR
- AND result is 'true,false'

### Requirement: NULL Propagation

The system SHALL return NULL for any string function when any input argument is NULL, except for CONCAT_WS which skips NULL values.

#### Scenario: String function with NULL argument
- GIVEN a NULL value
- WHEN executing `SELECT REVERSE(NULL)`
- THEN result is NULL

#### Scenario: REGEXP_MATCHES with NULL pattern
- GIVEN a valid string and NULL pattern
- WHEN executing `SELECT REGEXP_MATCHES('hello', NULL)`
- THEN result is NULL

#### Scenario: CONCAT_WS skips NULL values
- GIVEN values 'a', NULL, 'b'
- WHEN executing `SELECT CONCAT_WS(',', 'a', NULL, 'b')`
- THEN result is 'a,b' (NULL skipped, not propagated)

#### Scenario: LEVENSHTEIN with NULL argument
- GIVEN values 'hello' and NULL
- WHEN executing `SELECT LEVENSHTEIN('hello', NULL)`
- THEN result is NULL
