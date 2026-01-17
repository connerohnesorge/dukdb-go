# Changelog

All notable changes to dukdb-go will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### Glob Pattern Support for File Reading

Added comprehensive glob pattern support for reading multiple files in table functions. This is a non-breaking, additive change.

**Glob Patterns:**
- `*` - Match any characters within a path segment
- `**` - Match any characters across path segments (recursive)
- `?` - Match a single character
- `[abc]` - Match any character in the set
- `[a-z]` - Match any character in the range
- `[!abc]` - Match any character not in the set

**Array of Files Syntax:**
- `read_csv(['file1.csv', 'file2.csv'])` - Read specific files as array
- Supports mixing local and cloud storage files
- Can combine array elements with glob patterns

**Metadata Columns:**
- `filename=true` - Adds source file path as a column
- `file_row_number=true` - Adds 1-indexed row number within each file
- `file_index=true` - Adds 0-indexed file position in sorted file list

**Schema Alignment (Union by Name):**
- Columns matched by name across files
- Missing columns filled with NULL
- Type widening for compatible types (INTEGER → BIGINT, FLOAT → DOUBLE)
- Error on incompatible types (INTEGER + VARCHAR)

**Hive Partitioning:**
- `hive_partitioning=true` - Extract partition columns from paths (`year=2024/month=01/`)
- `hive_types_autocast=true` - Automatic type inference
- `hive_types={'year': 'INTEGER'}` - Explicit partition column types

**File Glob Options:**
- `file_glob_behavior='DISALLOW_EMPTY'` (default) - Error when no files match
- `file_glob_behavior='ALLOW_EMPTY'` - Return empty result when no files match
- `file_glob_behavior='FALLBACK_GLOB'` - Treat pattern as literal if no matches

**Supported File Formats:**
- CSV: `read_csv('*.csv')`, `read_csv_auto('*.csv')`
- JSON: `read_json('*.json')`, `read_json_auto('*.json')`, `read_ndjson('*.ndjson')`
- Parquet: `read_parquet('*.parquet')`
- XLSX: `read_xlsx('*.xlsx')`, `read_xlsx_auto('*.xlsx')`
- Arrow IPC: `read_arrow('*.arrow')`, `read_arrow_auto('*.arrow')`

**Limits and Configuration:**
- Default maximum 10,000 files per glob pattern
- Files sorted alphabetically for deterministic ordering

#### String Functions

Added comprehensive string function support with 30+ functions for text processing, data validation, pattern matching, and fuzzy matching. This is a non-breaking, additive change.

**Regular Expression Functions (RE2 compatible):**
- `REGEXP_MATCHES(string, pattern)` - Test if string matches regex pattern
- `REGEXP_REPLACE(string, pattern, replacement [, flags])` - Replace regex matches ('g' flag for global)
- `REGEXP_EXTRACT(string, pattern [, group])` - Extract first regex match
- `REGEXP_EXTRACT_ALL(string, pattern [, group])` - Extract all regex matches as array
- `REGEXP_SPLIT_TO_ARRAY(string, pattern)` - Split string by regex pattern

**String Concatenation and Splitting:**
- `CONCAT_WS(separator, str1, str2, ...)` - Concatenate with separator, skips NULL values
- `STRING_SPLIT(string, separator)` - Split string into array by literal separator

**Padding Functions:**
- `LPAD(string, length [, fill])` - Left-pad string to target length
- `RPAD(string, length [, fill])` - Right-pad string to target length

**String Manipulation:**
- `REVERSE(string)` - Reverse string (Unicode-safe)
- `REPEAT(string, count)` - Repeat string N times
- `LEFT(string, count)` - Extract leftmost N characters
- `RIGHT(string, count)` - Extract rightmost N characters
- `POSITION(substring IN string)` - Find 1-based position of substring (0 = not found)
- `STRPOS(string, substring)` - Alias for POSITION with different argument order
- `INSTR(string, substring)` - Alias for STRPOS
- `CONTAINS(string, substring)` - Test if string contains substring
- `PREFIX(string, prefix)` / `STARTS_WITH(string, prefix)` - Test if string starts with prefix
- `SUFFIX(string, suffix)` / `ENDS_WITH(string, suffix)` - Test if string ends with suffix

**String Encoding Functions:**
- `ASCII(character)` - Get ASCII code of first character
- `CHR(code)` - Convert ASCII code to character (validates range [0, 127])
- `UNICODE(character)` - Get Unicode codepoint of first character

**Cryptographic Hash Functions:**
- `MD5(string)` - MD5 hash (32-character lowercase hex)
- `SHA256(string)` - SHA256 hash (64-character lowercase hex)
- `HASH(string)` - FNV-1a 64-bit hash (returns BIGINT)

**String Distance Functions (Fuzzy Matching):**
- `LEVENSHTEIN(string1, string2)` - Edit distance (insertions, deletions, substitutions)
- `DAMERAU_LEVENSHTEIN(string1, string2)` - Edit distance with transpositions
- `HAMMING(string1, string2)` - Hamming distance (requires equal-length strings)
- `JACCARD(string1, string2)` - Jaccard similarity coefficient (0.0 to 1.0)
- `JARO_SIMILARITY(string1, string2)` - Jaro similarity (0.0 to 1.0)
- `JARO_WINKLER_SIMILARITY(string1, string2)` - Jaro-Winkler similarity with prefix bonus (0.0 to 1.0)

**Whitespace and Trimming Aliases:**
- `STRIP(string)` - Alias for TRIM
- `LSTRIP(string)` - Alias for LTRIM
- `RSTRIP(string)` - Alias for RTRIM

#### Type Inference

- Added proper type inference for all string functions
- Regex match functions return BOOLEAN or VARCHAR
- Distance functions return BIGINT (counts) or DOUBLE (similarity ratios)
- Hash functions return VARCHAR (MD5, SHA256) or BIGINT (HASH)
- Array-returning functions return LIST<VARCHAR>

#### Error Handling

- Invalid regex patterns: "Invalid regular expression: <details>"
- HAMMING with unequal strings: "HAMMING requires strings of equal length"
- CHR with out-of-range code: "CHR code must be in ASCII range [0, 127]"
- REPEAT with negative count: "REPEAT count must be non-negative"

#### Documentation

- Added comprehensive string functions documentation in `docs/string-functions.md`
- Documented all function signatures, return types, and behaviors
- Added examples for common use cases (data validation, log parsing, fuzzy matching)

#### Performance

- All string functions optimized for single-value and batch operations
- Unicode-safe implementations using rune slices
- Standard Go library packages: `strings`, `regexp`, `crypto/md5`, `crypto/sha256`, `hash/fnv`
- LEVENSHTEIN/DAMERAU_LEVENSHTEIN: O(n*m) time complexity

#### Compatibility

- Full compatibility with DuckDB string function behavior
- RE2 regex syntax (Go's regexp package)
- NULL propagation for all functions (except CONCAT_WS which skips NULLs)
- 1-based string indexing for POSITION

#### Math Functions

Added comprehensive mathematical function support, providing 45+ functions for numerical analysis, scientific computing, and general SQL operations.

**Rounding Functions:**
- `ROUND(value)` / `ROUND(value, decimals)` - Round to specified decimal places
- `CEIL(value)` / `CEILING(value)` - Round up to nearest integer
- `FLOOR(value)` - Round down to nearest integer
- `TRUNC(value)` - Truncate towards zero
- `ROUND_EVEN(value, decimals)` - Banker's rounding (round half to even)
- `EVEN(value)` - Round to nearest even integer

**Scientific Functions:**
- `SQRT(x)` - Square root (returns error for negative input)
- `CBRT(x)` - Cube root
- `POW(base, exponent)` / `POWER(base, exponent)` - Exponentiation
- `EXP(x)` - Natural exponential (e^x)
- `LN(x)` - Natural logarithm (returns error for non-positive input)
- `LOG(x)` / `LOG10(x)` - Base-10 logarithm
- `LOG2(x)` - Base-2 logarithm
- `GAMMA(x)` - Gamma function
- `LGAMMA(x)` - Log-gamma function
- `FACTORIAL(n)` - Factorial (returns error for n > 20 due to overflow)

**Trigonometric Functions:**
- `SIN(x)`, `COS(x)`, `TAN(x)`, `COT(x)` - Basic trigonometric functions (radians)
- `ASIN(x)`, `ACOS(x)`, `ATAN(x)` - Inverse trigonometric functions
- `ATAN2(y, x)` - Two-argument arctangent
- `DEGREES(radians)` - Convert radians to degrees
- `RADIANS(degrees)` - Convert degrees to radians

**Hyperbolic Functions:**
- `SINH(x)`, `COSH(x)`, `TANH(x)` - Hyperbolic sine, cosine, tangent
- `ASINH(x)`, `ACOSH(x)`, `ATANH(x)` - Inverse hyperbolic functions

**Utility Functions:**
- `PI()` - Mathematical constant pi (3.141592653589793)
- `RANDOM()` - Random number between 0 and 1
- `GCD(a, b)` - Greatest common divisor
- `LCM(a, b)` - Least common multiple
- `ISNAN(x)` - Check if value is NaN
- `ISINF(x)` - Check if value is infinity
- `ISFINITE(x)` - Check if value is finite (not NaN and not Inf)

**Bitwise Operators:**
- `&` (AND), `|` (OR), `^` (XOR), `~` (NOT) - Bitwise operations
- `<<` (left shift), `>>` (right shift) - Bit shifting
- `BIT_COUNT(x)` - Count number of set bits

#### Type Inference

- Added proper type inference for all math functions
- Rounding functions preserve input type for integers
- Scientific and trigonometric functions return DOUBLE
- FACTORIAL returns BIGINT
- ISNAN/ISINF/ISFINITE return BOOLEAN
- GCD/LCM return BIGINT
- BIT_COUNT returns INTEGER

#### Error Handling

- Domain errors for SQRT with negative input
- Domain errors for LN/LOG with non-positive input
- Domain errors for ASIN/ACOS with values outside [-1, 1]
- Domain errors for ACOSH with values less than 1
- Domain errors for ATANH with values at or outside [-1, 1]
- Overflow errors for FACTORIAL with n > 20
- Clear, descriptive error messages matching DuckDB style

### Documentation

- Added comprehensive math functions documentation in `docs/math-functions.md`
- Documented all function signatures, return types, and domain restrictions
- Added examples for common use cases (financial calculations, scientific computing)

### Performance

- All math functions optimized for single-value and batch operations
- Rounding functions: ~14-30 ns/op
- Scientific functions: ~14-30 ns/op
- Trigonometric functions: ~13-27 ns/op
- Bitwise operations: ~3-15 ns/op
- No allocations for many utility functions

### Compatibility

- Full compatibility with DuckDB math function behavior
- IEEE 754 floating-point precision
- NULL propagation for all functions
- Consistent type coercion behavior
