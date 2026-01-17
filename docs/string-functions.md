# String Functions

dukdb-go provides a comprehensive set of string functions compatible with DuckDB. This document covers all available string functions, their signatures, behaviors, and usage examples.

## Table of Contents

- [Regular Expression Functions](#regular-expression-functions)
- [String Concatenation and Splitting](#string-concatenation-and-splitting)
- [Padding Functions](#padding-functions)
- [String Manipulation Functions](#string-manipulation-functions)
- [String Search Functions](#string-search-functions)
- [String Encoding Functions](#string-encoding-functions)
- [Cryptographic Hash Functions](#cryptographic-hash-functions)
- [String Distance Functions](#string-distance-functions)
- [Whitespace and Trimming](#whitespace-and-trimming)
- [NULL Propagation](#null-propagation)
- [Error Handling](#error-handling)
- [Common Use Cases](#common-use-cases)

---

## Regular Expression Functions

dukdb-go uses RE2 regex syntax (implemented by Go's `regexp` package), which is compatible with DuckDB's regex engine.

### REGEXP_MATCHES

Tests whether a string matches a regular expression pattern.

**Syntax:**
```sql
REGEXP_MATCHES(string, pattern)
```

**Parameters:**
- `string` - The string to test
- `pattern` - RE2 regular expression pattern

**Returns:** BOOLEAN

**Examples:**
```sql
SELECT REGEXP_MATCHES('hello123', '[0-9]+');        -- true
SELECT REGEXP_MATCHES('hello', '[0-9]+');           -- false
SELECT REGEXP_MATCHES('abc@example.com', '@.*\.');  -- true
SELECT REGEXP_MATCHES('test', '^t.*t$');            -- true
```

### REGEXP_REPLACE

Replaces occurrences of a regex pattern with a replacement string.

**Syntax:**
```sql
REGEXP_REPLACE(string, pattern, replacement)
REGEXP_REPLACE(string, pattern, replacement, flags)
```

**Parameters:**
- `string` - The source string
- `pattern` - RE2 regular expression pattern
- `replacement` - Replacement string (supports `$1`, `$2` for capture groups)
- `flags` - Optional flags: 'g' for global replacement (replace all matches)

**Returns:** VARCHAR

**Default Behavior:** Replaces only the **first** match. Use 'g' flag for all matches.

**Examples:**
```sql
-- Replace first match only (default)
SELECT REGEXP_REPLACE('foo bar foo', 'foo', 'baz');       -- 'baz bar foo'

-- Replace all matches with 'g' flag
SELECT REGEXP_REPLACE('foo bar foo', 'foo', 'baz', 'g');  -- 'baz bar baz'

-- Using capture groups
SELECT REGEXP_REPLACE('John Smith', '(\w+) (\w+)', '$2, $1');  -- 'Smith, John'

-- Remove all digits
SELECT REGEXP_REPLACE('a1b2c3', '[0-9]', '', 'g');  -- 'abc'
```

### REGEXP_EXTRACT

Extracts the first match of a regex pattern from a string.

**Syntax:**
```sql
REGEXP_EXTRACT(string, pattern)
REGEXP_EXTRACT(string, pattern, group)
```

**Parameters:**
- `string` - The source string
- `pattern` - RE2 regular expression pattern
- `group` - Optional capture group index (0 = entire match, 1 = first group, etc.)

**Returns:** VARCHAR (or NULL if no match)

**Examples:**
```sql
-- Extract entire match (group 0)
SELECT REGEXP_EXTRACT('hello123world', '[0-9]+');           -- '123'
SELECT REGEXP_EXTRACT('hello123world', '[0-9]+', 0);        -- '123'

-- Extract capture groups
SELECT REGEXP_EXTRACT('user@example.com', '([^@]+)@(.+)', 1);  -- 'user'
SELECT REGEXP_EXTRACT('user@example.com', '([^@]+)@(.+)', 2);  -- 'example.com'

-- No match returns NULL
SELECT REGEXP_EXTRACT('hello', '[0-9]+');  -- NULL
```

### REGEXP_EXTRACT_ALL

Extracts all matches of a regex pattern from a string.

**Syntax:**
```sql
REGEXP_EXTRACT_ALL(string, pattern)
REGEXP_EXTRACT_ALL(string, pattern, group)
```

**Parameters:**
- `string` - The source string
- `pattern` - RE2 regular expression pattern
- `group` - Optional capture group index

**Returns:** LIST<VARCHAR>

**Examples:**
```sql
-- Extract all matches
SELECT REGEXP_EXTRACT_ALL('a1b2c3', '[0-9]+');           -- ['1', '2', '3']
SELECT REGEXP_EXTRACT_ALL('a1b2c3', '[0-9]+', 0);        -- ['1', '2', '3']

-- Extract from capture groups
SELECT REGEXP_EXTRACT_ALL('John:25,Jane:30', '(\w+):(\d+)', 1);  -- ['John', 'Jane']
SELECT REGEXP_EXTRACT_ALL('John:25,Jane:30', '(\w+):(\d+)', 2);  -- ['25', '30']

-- No matches returns empty array
SELECT REGEXP_EXTRACT_ALL('hello', '[0-9]+');  -- []
```

### REGEXP_SPLIT_TO_ARRAY

Splits a string by a regex pattern into an array.

**Syntax:**
```sql
REGEXP_SPLIT_TO_ARRAY(string, pattern)
```

**Parameters:**
- `string` - The source string
- `pattern` - RE2 regular expression pattern to split on

**Returns:** LIST<VARCHAR>

**Examples:**
```sql
-- Split by pattern
SELECT REGEXP_SPLIT_TO_ARRAY('one,two;three', '[,;]');  -- ['one', 'two', 'three']

-- Split by whitespace
SELECT REGEXP_SPLIT_TO_ARRAY('hello   world', '\s+');   -- ['hello', 'world']

-- Split by multiple characters
SELECT REGEXP_SPLIT_TO_ARRAY('a--b---c', '-+');         -- ['a', 'b', 'c']
```

---

## String Concatenation and Splitting

### CONCAT_WS

Concatenates strings with a separator, **skipping NULL values**.

**Syntax:**
```sql
CONCAT_WS(separator, string1, string2, ...)
```

**Parameters:**
- `separator` - String to insert between values
- `string1, string2, ...` - Values to concatenate

**Returns:** VARCHAR

**NULL Behavior:** Unlike most functions, CONCAT_WS skips NULL arguments (exception to NULL propagation). However, if the separator itself is NULL, the result is NULL.

**Examples:**
```sql
SELECT CONCAT_WS(', ', 'Alice', 'Bob', 'Charlie');  -- 'Alice, Bob, Charlie'
SELECT CONCAT_WS('-', 'a', 'b', 'c');               -- 'a-b-c'

-- NULL values are skipped
SELECT CONCAT_WS(', ', 'Alice', NULL, 'Bob');       -- 'Alice, Bob'
SELECT CONCAT_WS(', ', NULL, NULL, 'Bob');          -- 'Bob'

-- NULL separator returns NULL
SELECT CONCAT_WS(NULL, 'a', 'b');                   -- NULL
```

### STRING_SPLIT

Splits a string by a literal separator into an array.

**Syntax:**
```sql
STRING_SPLIT(string, separator)
```

**Parameters:**
- `string` - The source string
- `separator` - Literal string to split on (not a regex pattern)

**Returns:** LIST<VARCHAR>

**Note:** For regex-based splitting, use `REGEXP_SPLIT_TO_ARRAY`.

**Examples:**
```sql
SELECT STRING_SPLIT('a,b,c', ',');        -- ['a', 'b', 'c']
SELECT STRING_SPLIT('hello world', ' ');  -- ['hello', 'world']
SELECT STRING_SPLIT('a::b::c', '::');     -- ['a', 'b', 'c']

-- Empty separator splits into characters
SELECT STRING_SPLIT('abc', '');           -- ['a', 'b', 'c']
```

---

## Padding Functions

### LPAD

Left-pads a string to a specified length.

**Syntax:**
```sql
LPAD(string, length)
LPAD(string, length, fill)
```

**Parameters:**
- `string` - The source string
- `length` - Target length in characters
- `fill` - Optional fill string (default: space)

**Returns:** VARCHAR

**Behavior:**
- If string is longer than target length, it is truncated from the right
- Fill string is repeated as needed
- Unicode-safe (counts characters, not bytes)

**Examples:**
```sql
SELECT LPAD('42', 5);          -- '   42' (padded with spaces)
SELECT LPAD('42', 5, '0');     -- '00042'
SELECT LPAD('hello', 10, '*'); -- '*****hello'
SELECT LPAD('hello', 3);       -- 'hel' (truncated)

-- Multi-character fill
SELECT LPAD('x', 6, 'ab');     -- 'ababax'
```

### RPAD

Right-pads a string to a specified length.

**Syntax:**
```sql
RPAD(string, length)
RPAD(string, length, fill)
```

**Parameters:**
- `string` - The source string
- `length` - Target length in characters
- `fill` - Optional fill string (default: space)

**Returns:** VARCHAR

**Examples:**
```sql
SELECT RPAD('42', 5);          -- '42   ' (padded with spaces)
SELECT RPAD('42', 5, '0');     -- '42000'
SELECT RPAD('hello', 10, '.'); -- 'hello.....'
SELECT RPAD('hello', 3);       -- 'hel' (truncated)
```

---

## String Manipulation Functions

### REVERSE

Reverses a string.

**Syntax:**
```sql
REVERSE(string)
```

**Returns:** VARCHAR

**Examples:**
```sql
SELECT REVERSE('hello');    -- 'olleh'
SELECT REVERSE('12345');    -- '54321'
SELECT REVERSE('');         -- ''
```

### REPEAT

Repeats a string N times.

**Syntax:**
```sql
REPEAT(string, count)
```

**Parameters:**
- `string` - The string to repeat
- `count` - Number of repetitions (must be non-negative)

**Returns:** VARCHAR

**Examples:**
```sql
SELECT REPEAT('ab', 3);     -- 'ababab'
SELECT REPEAT('x', 5);      -- 'xxxxx'
SELECT REPEAT('hi', 0);     -- ''

-- Error case
SELECT REPEAT('x', -1);     -- ERROR: REPEAT count must be non-negative
```

### LEFT

Extracts the leftmost N characters from a string.

**Syntax:**
```sql
LEFT(string, count)
```

**Parameters:**
- `string` - The source string
- `count` - Number of characters to extract

**Returns:** VARCHAR

**Examples:**
```sql
SELECT LEFT('hello', 2);    -- 'he'
SELECT LEFT('hello', 10);   -- 'hello' (returns full string if count > length)
SELECT LEFT('hello', 0);    -- ''
SELECT LEFT('hello', -1);   -- '' (negative returns empty)
```

### RIGHT

Extracts the rightmost N characters from a string.

**Syntax:**
```sql
RIGHT(string, count)
```

**Parameters:**
- `string` - The source string
- `count` - Number of characters to extract

**Returns:** VARCHAR

**Examples:**
```sql
SELECT RIGHT('hello', 2);   -- 'lo'
SELECT RIGHT('hello', 10);  -- 'hello'
SELECT RIGHT('hello', 0);   -- ''
```

---

## String Search Functions

### POSITION

Finds the 1-based position of a substring within a string.

**Syntax:**
```sql
POSITION(substring IN string)
```

**Returns:** BIGINT (1-based index, or 0 if not found)

**Examples:**
```sql
SELECT POSITION('world' IN 'hello world');  -- 7
SELECT POSITION('x' IN 'hello');            -- 0 (not found)
SELECT POSITION('l' IN 'hello');            -- 3 (first occurrence)
SELECT POSITION('' IN 'hello');             -- 1 (empty string at start)
```

### STRPOS / INSTR

Aliases for finding substring position with different argument order.

**Syntax:**
```sql
STRPOS(string, substring)
INSTR(string, substring)
```

**Returns:** BIGINT (1-based index, or 0 if not found)

**Examples:**
```sql
SELECT STRPOS('hello world', 'world');  -- 7
SELECT INSTR('hello world', 'world');   -- 7
SELECT STRPOS('hello', 'x');            -- 0
```

### CONTAINS

Tests whether a string contains a substring.

**Syntax:**
```sql
CONTAINS(string, substring)
```

**Returns:** BOOLEAN

**Examples:**
```sql
SELECT CONTAINS('hello world', 'world');  -- true
SELECT CONTAINS('hello world', 'x');      -- false
SELECT CONTAINS('hello', '');             -- true (empty string is in everything)
```

### PREFIX / STARTS_WITH

Tests whether a string starts with a prefix.

**Syntax:**
```sql
PREFIX(string, prefix)
STARTS_WITH(string, prefix)
```

**Returns:** BOOLEAN

**Examples:**
```sql
SELECT PREFIX('hello', 'he');       -- true
SELECT PREFIX('hello', 'lo');       -- false
SELECT STARTS_WITH('hello', 'he');  -- true
SELECT STARTS_WITH('hello', '');    -- true
```

### SUFFIX / ENDS_WITH

Tests whether a string ends with a suffix.

**Syntax:**
```sql
SUFFIX(string, suffix)
ENDS_WITH(string, suffix)
```

**Returns:** BOOLEAN

**Examples:**
```sql
SELECT SUFFIX('hello', 'lo');     -- true
SELECT SUFFIX('hello', 'he');     -- false
SELECT ENDS_WITH('hello', 'lo');  -- true
SELECT ENDS_WITH('hello', '');    -- true
```

---

## String Encoding Functions

### ASCII

Returns the ASCII code of the first character of a string.

**Syntax:**
```sql
ASCII(string)
```

**Returns:** BIGINT

**Examples:**
```sql
SELECT ASCII('A');      -- 65
SELECT ASCII('abc');    -- 97 (first character only)
SELECT ASCII('');       -- 0 (empty string)
SELECT ASCII('1');      -- 49
```

### CHR

Converts an ASCII code to a character.

**Syntax:**
```sql
CHR(code)
```

**Parameters:**
- `code` - ASCII code (must be in range 0-127)

**Returns:** VARCHAR

**Examples:**
```sql
SELECT CHR(65);   -- 'A'
SELECT CHR(97);   -- 'a'
SELECT CHR(48);   -- '0'
SELECT CHR(10);   -- newline character

-- Error case
SELECT CHR(200);  -- ERROR: CHR code must be in ASCII range [0, 127]
```

### UNICODE

Returns the Unicode codepoint of the first character of a string.

**Syntax:**
```sql
UNICODE(string)
```

**Returns:** BIGINT

**Examples:**
```sql
SELECT UNICODE('A');      -- 65
SELECT UNICODE('$');      -- 36
SELECT UNICODE('');       -- 0
```

---

## Cryptographic Hash Functions

### MD5

Computes the MD5 hash of a string.

**Syntax:**
```sql
MD5(string)
```

**Returns:** VARCHAR (32-character lowercase hexadecimal string)

**Examples:**
```sql
SELECT MD5('hello');  -- '5d41402abc4b2a76b9719d911017c592'
SELECT MD5('');       -- 'd41d8cd98f00b204e9800998ecf8427e'
SELECT MD5('test');   -- '098f6bcd4621d373cade4e832627b4f6'
```

### SHA256

Computes the SHA-256 hash of a string.

**Syntax:**
```sql
SHA256(string)
```

**Returns:** VARCHAR (64-character lowercase hexadecimal string)

**Examples:**
```sql
SELECT SHA256('hello');
-- '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'

SELECT SHA256('');
-- 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
```

### HASH

Computes a general-purpose hash (FNV-1a 64-bit).

**Syntax:**
```sql
HASH(string)
```

**Returns:** BIGINT

**Examples:**
```sql
SELECT HASH('hello');  -- Returns a 64-bit integer
SELECT HASH('test');   -- Returns a different 64-bit integer
```

---

## String Distance Functions

These functions compute the similarity or distance between two strings. They are useful for fuzzy matching, typo tolerance, and record linkage.

### LEVENSHTEIN

Computes the Levenshtein (edit) distance between two strings. The distance is the minimum number of single-character edits (insertions, deletions, substitutions) needed to transform one string into the other.

**Syntax:**
```sql
LEVENSHTEIN(string1, string2)
```

**Returns:** BIGINT (lower = more similar)

**Performance:** O(n*m) time complexity where n and m are the string lengths.

**Examples:**
```sql
SELECT LEVENSHTEIN('kitten', 'sitting');  -- 3
SELECT LEVENSHTEIN('hello', 'hello');     -- 0 (identical)
SELECT LEVENSHTEIN('abc', 'def');         -- 3 (all different)
SELECT LEVENSHTEIN('', 'hello');          -- 5 (length of 'hello')
```

### DAMERAU_LEVENSHTEIN

Computes the Damerau-Levenshtein distance, which extends Levenshtein by also counting transpositions (swapping adjacent characters) as a single edit.

**Syntax:**
```sql
DAMERAU_LEVENSHTEIN(string1, string2)
```

**Returns:** BIGINT (lower = more similar)

**Examples:**
```sql
SELECT DAMERAU_LEVENSHTEIN('ab', 'ba');   -- 1 (one transposition)
SELECT LEVENSHTEIN('ab', 'ba');           -- 2 (delete + insert)

SELECT DAMERAU_LEVENSHTEIN('teh', 'the'); -- 1 (transposition)
SELECT DAMERAU_LEVENSHTEIN('hello', 'hello'); -- 0
```

### HAMMING

Computes the Hamming distance between two equal-length strings. The distance is the number of positions where the corresponding characters differ.

**Syntax:**
```sql
HAMMING(string1, string2)
```

**Returns:** BIGINT

**Requirement:** Both strings must have the **same length**, or an error is returned.

**Examples:**
```sql
SELECT HAMMING('karolin', 'kathrin');  -- 3
SELECT HAMMING('1011101', '1001001');  -- 2
SELECT HAMMING('abc', 'abc');          -- 0

-- Error case
SELECT HAMMING('abc', 'ab');  -- ERROR: HAMMING requires strings of equal length
```

### JACCARD

Computes the Jaccard similarity coefficient between two strings based on their character sets.

**Syntax:**
```sql
JACCARD(string1, string2)
```

**Returns:** DOUBLE (0.0 to 1.0, higher = more similar)

**Formula:** |intersection| / |union| of character sets

**Examples:**
```sql
SELECT JACCARD('hello', 'hallo');   -- ~0.6
SELECT JACCARD('abc', 'abc');       -- 1.0 (identical sets)
SELECT JACCARD('abc', 'xyz');       -- 0.0 (no common characters)
SELECT JACCARD('', '');             -- 1.0 (both empty)
```

### JARO_SIMILARITY

Computes the Jaro similarity between two strings.

**Syntax:**
```sql
JARO_SIMILARITY(string1, string2)
```

**Returns:** DOUBLE (0.0 to 1.0, higher = more similar)

**Examples:**
```sql
SELECT JARO_SIMILARITY('MARTHA', 'MARHTA');      -- ~0.944
SELECT JARO_SIMILARITY('DWAYNE', 'DUANE');       -- ~0.822
SELECT JARO_SIMILARITY('DIXON', 'DICKSONX');     -- ~0.767
SELECT JARO_SIMILARITY('hello', 'hello');        -- 1.0
SELECT JARO_SIMILARITY('abc', 'xyz');            -- 0.0
```

### JARO_WINKLER_SIMILARITY

Computes the Jaro-Winkler similarity, which extends Jaro similarity by giving extra weight to common prefixes.

**Syntax:**
```sql
JARO_WINKLER_SIMILARITY(string1, string2)
```

**Returns:** DOUBLE (0.0 to 1.0, higher = more similar)

**Examples:**
```sql
SELECT JARO_WINKLER_SIMILARITY('MARTHA', 'MARHTA');  -- ~0.961
SELECT JARO_WINKLER_SIMILARITY('DWAYNE', 'DUANE');   -- ~0.840
SELECT JARO_WINKLER_SIMILARITY('hello', 'hello');   -- 1.0

-- Higher than JARO when strings share a common prefix
SELECT JARO_SIMILARITY('prefix_abc', 'prefix_xyz');        -- Lower
SELECT JARO_WINKLER_SIMILARITY('prefix_abc', 'prefix_xyz'); -- Higher (prefix bonus)
```

---

## Whitespace and Trimming

These are aliases for existing trim functions.

### STRIP

Alias for TRIM. Removes leading and trailing whitespace.

**Syntax:**
```sql
STRIP(string)
```

**Examples:**
```sql
SELECT STRIP('  hello  ');  -- 'hello'
```

### LSTRIP

Alias for LTRIM. Removes leading whitespace.

**Syntax:**
```sql
LSTRIP(string)
```

**Examples:**
```sql
SELECT LSTRIP('  hello');  -- 'hello'
```

### RSTRIP

Alias for RTRIM. Removes trailing whitespace.

**Syntax:**
```sql
RSTRIP(string)
```

**Examples:**
```sql
SELECT RSTRIP('hello  ');  -- 'hello'
```

---

## NULL Propagation

All string functions propagate NULL values, with one exception:

### Standard NULL Propagation

If any argument is NULL, the result is NULL:

```sql
SELECT REVERSE(NULL);              -- NULL
SELECT MD5(NULL);                  -- NULL
SELECT LEVENSHTEIN('hello', NULL); -- NULL
SELECT LPAD(NULL, 5, '0');         -- NULL
SELECT LPAD('x', NULL, '0');       -- NULL
SELECT REGEXP_MATCHES(NULL, 'x');  -- NULL
SELECT REGEXP_MATCHES('x', NULL);  -- NULL
```

### Exception: CONCAT_WS

`CONCAT_WS` skips NULL arguments (does not propagate them):

```sql
SELECT CONCAT_WS(', ', 'a', NULL, 'b');  -- 'a, b' (not NULL)
SELECT CONCAT_WS(', ', NULL, NULL);      -- '' (empty string)

-- But NULL separator still returns NULL
SELECT CONCAT_WS(NULL, 'a', 'b');        -- NULL
```

---

## Error Handling

String functions return errors for invalid inputs rather than returning NULL.

### Error Summary

| Function | Error Condition | Error Message |
|----------|----------------|---------------|
| REGEXP_* | Invalid regex pattern | "Invalid regular expression: <details>" |
| CHR | Code outside [0, 127] | "CHR code must be in ASCII range [0, 127]" |
| REPEAT | Negative count | "REPEAT count must be non-negative" |
| HAMMING | Unequal string lengths | "HAMMING requires strings of equal length" |

### Error Examples

```sql
-- Invalid regex
SELECT REGEXP_MATCHES('x', '[');
-- ERROR: Invalid regular expression: error parsing regexp: missing closing ]: `[`

-- CHR out of range
SELECT CHR(200);
-- ERROR: CHR code must be in ASCII range [0, 127]

-- Negative REPEAT
SELECT REPEAT('x', -5);
-- ERROR: REPEAT count must be non-negative

-- HAMMING length mismatch
SELECT HAMMING('abc', 'ab');
-- ERROR: HAMMING requires strings of equal length
```

---

## Common Use Cases

### Data Validation

```sql
-- Email validation
SELECT email, REGEXP_MATCHES(email, '^[^@]+@[^@]+\.[^@]+$') AS valid
FROM users;

-- Phone number format validation
SELECT phone, REGEXP_MATCHES(phone, '^\d{3}-\d{3}-\d{4}$') AS valid_us_phone
FROM contacts;

-- Check for required prefixes
SELECT * FROM codes WHERE PREFIX(code, 'PRD-');
```

### Data Cleaning

```sql
-- Normalize whitespace
SELECT TRIM(REGEXP_REPLACE(text, '\s+', ' ', 'g')) AS clean_text
FROM documents;

-- Remove special characters
SELECT REGEXP_REPLACE(name, '[^a-zA-Z0-9 ]', '', 'g') AS clean_name
FROM raw_data;

-- Standardize case
SELECT LOWER(TRIM(email)) AS normalized_email
FROM users;

-- Format phone numbers
SELECT CONCAT_WS('-',
    LEFT(phone, 3),
    SUBSTR(phone, 4, 3),
    RIGHT(phone, 4)
) AS formatted_phone
FROM contacts WHERE LENGTH(phone) = 10;
```

### Data Extraction

```sql
-- Extract domain from email
SELECT email,
    REGEXP_EXTRACT(email, '@(.+)$', 1) AS domain
FROM users;

-- Parse log lines
SELECT
    REGEXP_EXTRACT(line, '\[([^\]]+)\]', 1) AS timestamp,
    REGEXP_EXTRACT(line, '(ERROR|WARN|INFO)', 1) AS level,
    REGEXP_EXTRACT(line, '] (.+)$', 1) AS message
FROM logs;

-- Extract all hashtags
SELECT post_text,
    REGEXP_EXTRACT_ALL(post_text, '#(\w+)', 1) AS hashtags
FROM posts;
```

### Fuzzy Matching

```sql
-- Find similar customer names
SELECT c1.name AS name1, c2.name AS name2,
    JARO_WINKLER_SIMILARITY(c1.name, c2.name) AS similarity
FROM customers c1
CROSS JOIN customers c2
WHERE c1.id < c2.id
    AND JARO_WINKLER_SIMILARITY(c1.name, c2.name) > 0.85;

-- Typo-tolerant product search
SELECT name, LEVENSHTEIN(LOWER(name), 'widgett') AS distance
FROM products
WHERE LEVENSHTEIN(LOWER(name), 'widgett') <= 2
ORDER BY distance;

-- Duplicate detection
SELECT a.*, b.*
FROM records a
JOIN records b ON a.id < b.id
WHERE DAMERAU_LEVENSHTEIN(a.name, b.name) <= 2
    AND a.name != b.name;
```

### Formatting

```sql
-- Pad IDs
SELECT LPAD(CAST(id AS VARCHAR), 8, '0') AS padded_id
FROM orders;

-- Create fixed-width reports
SELECT
    RPAD(name, 30) ||
    LPAD(CAST(price AS VARCHAR), 10) ||
    LPAD(CAST(qty AS VARCHAR), 5)
FROM products;

-- Mask sensitive data
SELECT
    LEFT(ssn, 3) || '-XX-' || RIGHT(ssn, 4) AS masked_ssn
FROM users;
```

### Data Integrity

```sql
-- Generate checksums
SELECT id, name, email,
    MD5(CONCAT(CAST(id AS VARCHAR), name, email)) AS checksum
FROM users;

-- Verify data consistency across tables
SELECT t1.id, t1.data, t2.data
FROM table1 t1
JOIN table2 t2 ON t1.id = t2.id
WHERE SHA256(t1.data) != SHA256(t2.data);

-- Find modified records
SELECT *
FROM audit_log
WHERE MD5(current_value) != stored_hash;
```
