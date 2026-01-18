# Change: Add Essential String Functions

## Why

String manipulation functions are **CRITICAL** for text processing, data cleaning, and general SQL queries. DuckDB v1.4.3 has 50+ string/text functions that users expect for standard operations like regular expression matching, string splitting, padding, case conversion, and cryptographic hashing. Without these, users must:
- Perform text processing in application code instead of SQL (inefficient)
- Cannot validate or transform text data within queries (limited data quality)
- Cannot migrate queries from DuckDB CLI (compatibility broken)
- Blocked from log parsing, ETL, and text analytics use cases

Currently, dukdb-go has only **10 string functions** (UPPER, LOWER, LENGTH, TRIM, LTRIM, RTRIM, CONCAT, SUBSTR, SUBSTRING, REPLACE), missing **40+ essential functions** including REGEXP_MATCHES, REGEXP_REPLACE, CONCAT_WS, STRING_SPLIT, LPAD, RPAD, INITCAP, MD5, SHA256, and many more.

This is the **#3 highest priority** missing feature after glob pattern support and math functions.

## What Changes

- **ADDED**: Regular expression functions
  - `REGEXP_MATCHES(string, pattern)` - Test if string matches regex
  - `REGEXP_REPLACE(string, pattern, replacement [, flags])` - Replace regex matches
  - `REGEXP_EXTRACT(string, pattern [, group])` - Extract regex match
  - `REGEXP_EXTRACT_ALL(string, pattern [, group])` - Extract all matches
  - `REGEXP_SPLIT_TO_ARRAY(string, pattern)` - Split string by regex into array

- **ADDED**: String concatenation and splitting
  - `CONCAT_WS(separator, str1, str2, ...)` - Concatenate with separator
  - `STRING_SPLIT(string, separator)` - Split string into array

- **ADDED**: Padding functions
  - `LPAD(string, length [, fill])` - Left-pad to length
  - `RPAD(string, length [, fill])` - Right-pad to length

- **ADDED**: String manipulation
  - `REVERSE(string)` - Reverse string
  - `REPEAT(string, count)` - Repeat string N times
  - `LEFT(string, count)` - Extract left N characters
  - `RIGHT(string, count)` - Extract right N characters
  - `POSITION(substring IN string)` - Find substring position
  - `STRPOS(string, substring)` - Alias for POSITION
  - `INSTR(string, substring)` - Alias for POSITION
  - `CONTAINS(string, substring)` - Test if string contains substring
  - `PREFIX(string, prefix)` - Test if string starts with prefix
  - `SUFFIX(string, suffix)` - Test if string ends with suffix
  - `STARTS_WITH(string, prefix)` - Alias for PREFIX
  - `ENDS_WITH(string, suffix)` - Alias for SUFFIX

- **ADDED**: String encoding functions
  - `ASCII(character)` - Get ASCII code of first character
  - `CHR(code)` - Convert ASCII code to character
  - `UNICODE(character)` - Get Unicode codepoint

- **ADDED**: Cryptographic hash functions
  - `MD5(string)` - MD5 hash (32 hex chars)
  - `SHA256(string)` - SHA256 hash (64 hex chars)
  - `HASH(string)` - DuckDB's default hash function

- **ADDED**: String distance and similarity functions
  - `LEVENSHTEIN(string1, string2)` - Edit distance between strings
  - `DAMERAU_LEVENSHTEIN(string1, string2)` - Edit distance with transpositions
  - `HAMMING(string1, string2)` - Hamming distance between strings
  - `JACCARD(string1, string2)` - Jaccard similarity
  - `JARO_SIMILARITY(string1, string2)` - Jaro similarity
  - `JARO_WINKLER_SIMILARITY(string1, string2)` - Jaro-Winkler similarity

- **ADDED**: Whitespace and trimming
  - `STRIP(string)` - Alias for TRIM
  - `LSTRIP(string)` - Alias for LTRIM
  - `RSTRIP(string)` - Alias for RTRIM

- **MODIFIED**: Expression evaluator to handle string function calls
- **MODIFIED**: Type inference for string function results

## Impact

- Affected specs: `specs/string/spec.md` (NEW)
- Affected code:
  - `internal/executor/expr.go` - Add string function cases
  - `internal/executor/string.go` (NEW) - String function implementations
  - `internal/executor/regex.go` (NEW) - Regex function implementations
  - `internal/executor/hash.go` (NEW) - Cryptographic hash implementations
  - `internal/binder/` - Function binding and type checking
- Breaking changes: **None** (additive only)
- Dependencies:
  - Go standard library `strings`, `regexp`, `crypto/md5`, `crypto/sha256` packages
  - Existing scalar function infrastructure

## Priority

**CRITICAL** - Unblocks text processing use cases (third most common use case) and provides parity with DuckDB CLI for string operations.
