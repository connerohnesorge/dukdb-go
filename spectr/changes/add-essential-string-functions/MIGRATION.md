# String Functions Migration Guide

This guide covers the addition of string functions to dukdb-go. This is an **additive-only** change with **no breaking changes** to existing functionality.

## Overview

The string functions feature adds 30+ string functions to dukdb-go, providing comprehensive text processing capabilities including regular expressions, string manipulation, cryptographic hashing, and fuzzy matching. All existing code continues to work without modification.

## Migration Summary

| Aspect | Impact |
|--------|--------|
| Breaking Changes | None |
| API Changes | None |
| New Features | 30+ string functions |
| Required Action | None (use new functions as needed) |

## What's New

### New Functions Available

After upgrading, you can use these new functions in your SQL queries:

**Regular Expressions (RE2 compatible):**
- `REGEXP_MATCHES(string, pattern)` - Test if string matches regex
- `REGEXP_REPLACE(string, pattern, replacement [, flags])` - Replace matches ('g' for global)
- `REGEXP_EXTRACT(string, pattern [, group])` - Extract first match
- `REGEXP_EXTRACT_ALL(string, pattern [, group])` - Extract all matches as array
- `REGEXP_SPLIT_TO_ARRAY(string, pattern)` - Split by regex pattern

**Concatenation and Splitting:**
- `CONCAT_WS(separator, str1, str2, ...)` - Concatenate with separator (skips NULLs)
- `STRING_SPLIT(string, separator)` - Split into array by literal separator

**Padding:**
- `LPAD(string, length [, fill])` - Left-pad to length
- `RPAD(string, length [, fill])` - Right-pad to length

**Manipulation:**
- `REVERSE(string)` - Reverse string
- `REPEAT(string, count)` - Repeat N times
- `LEFT(string, count)` - Extract left N characters
- `RIGHT(string, count)` - Extract right N characters
- `POSITION(substring IN string)` - Find position (1-based, 0 = not found)
- `STRPOS(string, substring)` - Alias for POSITION
- `INSTR(string, substring)` - Alias for STRPOS
- `CONTAINS(string, substring)` - Check if contains
- `PREFIX(string, prefix)` / `STARTS_WITH(string, prefix)` - Check prefix
- `SUFFIX(string, suffix)` / `ENDS_WITH(string, suffix)` - Check suffix

**Encoding:**
- `ASCII(character)` - Get ASCII code
- `CHR(code)` - Convert ASCII code to character
- `UNICODE(character)` - Get Unicode codepoint

**Cryptographic Hashes:**
- `MD5(string)` - MD5 hash (32-char hex)
- `SHA256(string)` - SHA256 hash (64-char hex)
- `HASH(string)` - FNV-1a 64-bit hash (BIGINT)

**String Distance (Fuzzy Matching):**
- `LEVENSHTEIN(s1, s2)` - Edit distance
- `DAMERAU_LEVENSHTEIN(s1, s2)` - Edit distance with transpositions
- `HAMMING(s1, s2)` - Hamming distance (equal-length strings)
- `JACCARD(s1, s2)` - Jaccard similarity (0.0-1.0)
- `JARO_SIMILARITY(s1, s2)` - Jaro similarity (0.0-1.0)
- `JARO_WINKLER_SIMILARITY(s1, s2)` - Jaro-Winkler similarity (0.0-1.0)

**Whitespace Aliases:**
- `STRIP(string)` - Alias for TRIM
- `LSTRIP(string)` - Alias for LTRIM
- `RSTRIP(string)` - Alias for RTRIM

### Existing Functions

These functions were already available and remain unchanged:
- `UPPER(string)` - Convert to uppercase
- `LOWER(string)` - Convert to lowercase
- `LENGTH(string)` - String length
- `TRIM(string)` - Remove leading/trailing whitespace
- `LTRIM(string)` - Remove leading whitespace
- `RTRIM(string)` - Remove trailing whitespace
- `CONCAT(str1, str2, ...)` - Concatenate strings
- `SUBSTR(string, start [, length])` / `SUBSTRING(...)` - Extract substring
- `REPLACE(string, from, to)` - Replace occurrences

## Upgrade Steps

1. **Update your dependency:**
   ```bash
   go get -u github.com/dukdb/dukdb-go
   ```

2. **Use new functions as needed:**
   ```sql
   -- Before: Manual text processing in application code
   -- After: Use SQL string functions directly
   SELECT REGEXP_EXTRACT(email, '([^@]+)@', 1) AS username FROM users;
   SELECT * FROM products WHERE LEVENSHTEIN(name, 'widget') <= 2;
   ```

3. **No code changes required** for existing functionality

## Compatibility Notes

### DuckDB Compatibility

All string functions are compatible with DuckDB behavior:
- Same function names and signatures
- Same return types
- RE2 regex syntax (Go's regexp package implements RE2)
- 1-based string indexing for POSITION

### Key Behavioral Notes

**REGEXP_REPLACE default behavior:**
By default, `REGEXP_REPLACE` replaces only the first match. Use the 'g' flag for global replacement:
```sql
SELECT REGEXP_REPLACE('foo bar foo', 'foo', 'baz');       -- 'baz bar foo'
SELECT REGEXP_REPLACE('foo bar foo', 'foo', 'baz', 'g');  -- 'baz bar baz'
```

**CONCAT_WS NULL handling:**
Unlike most functions, `CONCAT_WS` skips NULL values instead of propagating them:
```sql
SELECT CONCAT_WS(', ', 'a', NULL, 'b');  -- 'a, b' (not NULL)
SELECT CONCAT_WS(NULL, 'a', 'b');        -- NULL (separator is NULL)
```

**HAMMING equal-length requirement:**
`HAMMING` requires strings of equal length:
```sql
SELECT HAMMING('abc', 'abd');  -- 1 (valid)
SELECT HAMMING('abc', 'ab');   -- ERROR: HAMMING requires strings of equal length
```

**Distance vs Similarity:**
- Distance functions (LEVENSHTEIN, DAMERAU_LEVENSHTEIN, HAMMING) return counts (lower = more similar)
- Similarity functions (JACCARD, JARO, JARO_WINKLER) return ratios 0.0-1.0 (higher = more similar)

### Type Behavior

| Function Category | Return Type |
|------------------|-------------|
| REGEXP_MATCHES, CONTAINS, PREFIX, SUFFIX | BOOLEAN |
| REGEXP_REPLACE, REGEXP_EXTRACT, string manipulation | VARCHAR |
| REGEXP_EXTRACT_ALL, STRING_SPLIT, REGEXP_SPLIT_TO_ARRAY | LIST<VARCHAR> |
| POSITION, ASCII, UNICODE | BIGINT |
| MD5, SHA256 | VARCHAR (hex) |
| HASH | BIGINT |
| LEVENSHTEIN, DAMERAU_LEVENSHTEIN, HAMMING | BIGINT |
| JACCARD, JARO_SIMILARITY, JARO_WINKLER_SIMILARITY | DOUBLE |

### NULL Handling

All functions propagate NULL values (except CONCAT_WS):
```sql
SELECT REVERSE(NULL);      -- Returns NULL
SELECT MD5(NULL);          -- Returns NULL
SELECT LEVENSHTEIN('a', NULL);  -- Returns NULL
```

### Error Handling

Domain violations produce descriptive errors:
```sql
SELECT CHR(200);           -- Error: CHR code must be in ASCII range [0, 127]
SELECT REPEAT('x', -1);    -- Error: REPEAT count must be non-negative
SELECT HAMMING('a', 'ab'); -- Error: HAMMING requires strings of equal length
SELECT REGEXP_MATCHES('x', '[');  -- Error: Invalid regular expression: ...
```

## Performance Considerations

String functions are optimized for efficiency:
- Unicode-safe implementations using Go rune slices
- Standard Go library packages for cryptographic operations
- LEVENSHTEIN/DAMERAU_LEVENSHTEIN: O(n*m) time complexity (be cautious with very long strings)

No performance changes to existing functionality.

## Testing Your Upgrade

Verify the upgrade works:

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/dukdb/dukdb-go"
    _ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
    db, err := sql.Open("dukdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Test new string functions
    var strResult string
    var boolResult bool
    var intResult int64
    var floatResult float64

    // Regex
    db.QueryRow("SELECT REGEXP_MATCHES('hello123', '[0-9]+')").Scan(&boolResult)
    fmt.Printf("REGEXP_MATCHES: %v\n", boolResult)  // true

    // String manipulation
    db.QueryRow("SELECT REVERSE('hello')").Scan(&strResult)
    fmt.Printf("REVERSE: %v\n", strResult)  // olleh

    db.QueryRow("SELECT LPAD('42', 5, '0')").Scan(&strResult)
    fmt.Printf("LPAD: %v\n", strResult)  // 00042

    // Hash
    db.QueryRow("SELECT MD5('hello')").Scan(&strResult)
    fmt.Printf("MD5: %v\n", strResult)  // 5d41402abc4b2a76b9719d911017c592

    // Distance
    db.QueryRow("SELECT LEVENSHTEIN('kitten', 'sitting')").Scan(&intResult)
    fmt.Printf("LEVENSHTEIN: %v\n", intResult)  // 3

    // Similarity
    db.QueryRow("SELECT JARO_WINKLER_SIMILARITY('MARTHA', 'MARHTA')").Scan(&floatResult)
    fmt.Printf("JARO_WINKLER: %v\n", floatResult)  // ~0.961

    fmt.Println("All string functions working correctly!")
}
```

## Troubleshooting

### Function Not Found

If you get "unknown function" errors, ensure:
1. You've updated to the latest version
2. Function names are spelled correctly (case-insensitive)
3. Function arguments match expected types

### Invalid Regex Pattern

RE2 regex syntax differs from some other engines:
```sql
-- No lookahead/lookbehind in RE2
SELECT REGEXP_MATCHES('foo', '(?=bar)');  -- ERROR

-- Use standard RE2 patterns
SELECT REGEXP_MATCHES('foobar', 'foo.*bar');  -- Works
```

### Type Mismatch

Ensure correct argument types:
```sql
-- Error: REPEAT needs integer count
SELECT REPEAT('x', 2.5);

-- Correct: use integer
SELECT REPEAT('x', 2);
```

### Large String Performance

For very long strings, distance functions can be slow:
```sql
-- Potentially slow for large strings
SELECT LEVENSHTEIN(large_text1, large_text2);

-- Consider limiting string length or using simpler comparisons
SELECT LEVENSHTEIN(LEFT(text1, 100), LEFT(text2, 100));
```

## Use Cases

### Data Validation

```sql
-- Email validation
SELECT * FROM users WHERE REGEXP_MATCHES(email, '^[^@]+@[^@]+\.[^@]+$');

-- Phone number extraction
SELECT REGEXP_EXTRACT(phone, '\d{3}-\d{3}-\d{4}', 0) FROM contacts;
```

### Data Cleaning

```sql
-- Normalize whitespace and case
SELECT TRIM(LOWER(REGEXP_REPLACE(name, '\s+', ' ', 'g'))) FROM raw_data;

-- Remove special characters
SELECT REGEXP_REPLACE(text, '[^a-zA-Z0-9 ]', '', 'g') FROM documents;
```

### Fuzzy Matching

```sql
-- Find similar names
SELECT a.name, b.name, JARO_WINKLER_SIMILARITY(a.name, b.name) AS score
FROM customers a, prospects b
WHERE JARO_WINKLER_SIMILARITY(a.name, b.name) > 0.85;

-- Typo-tolerant search
SELECT * FROM products WHERE LEVENSHTEIN(name, 'widgett') <= 2;
```

### Log Parsing

```sql
-- Extract components from log lines
SELECT
    REGEXP_EXTRACT(line, '\[([^\]]+)\]', 1) AS timestamp,
    REGEXP_EXTRACT(line, '(ERROR|WARN|INFO)', 1) AS level,
    REGEXP_EXTRACT(line, '] (.+)$', 1) AS message
FROM logs;
```

### Data Integrity

```sql
-- Generate checksums
SELECT *, MD5(CONCAT(id, name, email)) AS checksum FROM users;

-- Verify data consistency
SELECT * FROM table1 t1
JOIN table2 t2 ON t1.id = t2.id
WHERE MD5(t1.data) != MD5(t2.data);
```

## Documentation

- [String Functions Reference](../../docs/string-functions.md) - Complete function documentation
- [README](../../README.md#string-functions) - Quick examples
- [CHANGELOG](../../CHANGELOG.md) - Full release notes

## Support

If you encounter any issues with the string functions:
1. Check the documentation for correct usage
2. Verify input types match function requirements
3. Review error messages for domain violations
4. Check RE2 regex syntax compatibility
5. File an issue on GitHub with reproduction steps
