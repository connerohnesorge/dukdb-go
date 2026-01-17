# Implementation Tasks

## 1. Core Infrastructure

- [x] 1.1 Create `internal/executor/regex.go` file
- [x] 1.2 Create `internal/executor/string.go` file
- [x] 1.3 Create `internal/executor/hash.go` file
- [x] 1.4 Create `internal/executor/string_distance.go` file
- [x] 1.5 Add helper functions if not already present: `toString`, `toInt64`
- [x] 1.6 Write unit tests for helper functions

## 2. Regular Expression Functions

- [x] 2.1 Implement `regexpMatchesValue(str, pattern)` using `regexp.Compile` and `MatchString`
- [x] 2.2 Implement `regexpReplaceValue(str, pattern, replacement, flags)` with 'g' flag support
- [x] 2.3 Implement `regexpExtractValue(str, pattern, group)` using `FindStringSubmatch`
- [x] 2.4 Implement `regexpExtractAllValue(str, pattern, group)` using `FindAllStringSubmatch`
- [x] 2.5 Implement `regexpSplitToArrayValue(str, pattern)` using `Split`
- [x] 2.6 Add REGEXP_MATCHES case to `internal/executor/expr.go`
- [x] 2.7 Add REGEXP_REPLACE case with optional flags parameter
- [x] 2.8 Add REGEXP_EXTRACT case with optional group parameter
- [x] 2.9 Add REGEXP_EXTRACT_ALL case to expression evaluator
- [x] 2.10 Add REGEXP_SPLIT_TO_ARRAY case to expression evaluator
- [x] 2.11 Write unit tests for all regex functions
- [x] 2.12 Test edge cases: invalid patterns, NULL inputs, empty strings
- [x] 2.13 Test 'g' flag behavior in REGEXP_REPLACE (global vs first match)
- [x] 2.14 Integration test: Regex functions in WHERE clauses

## 3. String Concatenation and Splitting

- [x] 3.1 Implement `concatWSValue(separator, ...args)` skipping NULL values
- [x] 3.2 Implement `stringSplitValue(str, separator)` returning array
- [x] 3.3 Handle empty separator case (split into individual characters)
- [x] 3.4 Add CONCAT_WS case to expression evaluator
- [x] 3.5 Add STRING_SPLIT case to expression evaluator
- [x] 3.6 Write unit tests for concatenation and splitting
- [x] 3.7 Test NULL handling: CONCAT_WS skips NULLs, STRING_SPLIT returns NULL
- [ ] 3.8 Integration test: STRING_SPLIT with UNNEST

## 4. Padding Functions

- [x] 4.1 Implement `lpadValue(str, length, fill)` with default fill ' '
- [x] 4.2 Implement `rpadValue(str, length, fill)` with default fill ' '
- [x] 4.3 Handle truncation when string exceeds target length
- [x] 4.4 Handle empty fill string case (return original string)
- [x] 4.5 Add LPAD case to expression evaluator
- [x] 4.6 Add RPAD case to expression evaluator
- [x] 4.7 Write unit tests for padding functions
- [x] 4.8 Test edge cases: negative length, multi-character fill strings
- [x] 4.9 Integration test: Padding for formatting output

## 5. String Manipulation Functions

- [x] 5.1 Implement `reverseValue(str)` using rune slices for Unicode safety
- [x] 5.2 Implement `repeatValue(str, count)` with negative count validation
- [x] 5.3 Implement `leftValue(str, count)` extracting left N characters
- [x] 5.4 Implement `rightValue(str, count)` extracting right N characters
- [x] 5.5 Implement `positionValue(substring, str)` using `strings.Index`
- [x] 5.6 Convert position to 1-based indexing (0 = not found)
- [x] 5.7 Implement `containsValue(str, substring)` using `strings.Contains`
- [x] 5.8 Implement `prefixValue(str, prefix)` using `strings.HasPrefix`
- [x] 5.9 Implement `suffixValue(str, suffix)` using `strings.HasSuffix`
- [x] 5.10 Add REVERSE case to expression evaluator
- [x] 5.11 Add REPEAT case to expression evaluator
- [x] 5.12 Add LEFT, RIGHT cases to expression evaluator
- [x] 5.13 Add POSITION case to expression evaluator
- [x] 5.14 Add STRPOS, INSTR cases (aliases with reversed argument order)
- [x] 5.15 Add CONTAINS case to expression evaluator
- [x] 5.16 Add PREFIX, STARTS_WITH cases to expression evaluator
- [x] 5.17 Add SUFFIX, ENDS_WITH cases to expression evaluator
- [x] 5.18 Write unit tests for all string manipulation functions
- [x] 5.19 Test edge cases: empty strings, negative counts, not found cases
- [x] 5.20 Integration test: String manipulation in SELECT and WHERE

## 6. String Encoding Functions

- [x] 6.1 Implement `asciiValue(char)` returning ASCII code of first character
- [x] 6.2 Implement `chrValue(code)` converting ASCII code to character
- [x] 6.3 Add range validation for CHR (0-127 for ASCII)
- [x] 6.4 Implement `unicodeValue(char)` returning Unicode codepoint
- [x] 6.5 Use rune slices for Unicode support
- [x] 6.6 Add ASCII case to expression evaluator
- [x] 6.7 Add CHR case to expression evaluator
- [x] 6.8 Add UNICODE case to expression evaluator
- [x] 6.9 Write unit tests for encoding functions
- [x] 6.10 Test edge cases: empty strings, out-of-range codes, Unicode characters
- [x] 6.11 Integration test: ASCII/CHR for character code manipulation

## 7. Cryptographic Hash Functions

- [x] 7.1 Implement `md5Value(str)` using `crypto/md5`
- [x] 7.2 Implement `sha256Value(str)` using `crypto/sha256`
- [x] 7.3 Implement `hashValue(str)` using `hash/fnv` (FNV-1a 64-bit)
- [x] 7.4 Convert all hash outputs to lowercase hex strings
- [x] 7.5 Add MD5 case to expression evaluator
- [x] 7.6 Add SHA256 case to expression evaluator
- [x] 7.7 Add HASH case to expression evaluator
- [x] 7.8 Write unit tests for all hash functions
- [x] 7.9 Test known hash values: MD5('hello') = '5d41402abc4b2a76b9719d911017c592'
- [x] 7.10 Test SHA256('hello') = '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'
- [x] 7.11 Test edge cases: empty string, NULL input, binary data
- [x] 7.12 Integration test: Hash functions for data integrity checks

## 8. String Distance Functions

- [x] 8.1 Implement `levenshteinValue(str1, str2)` using dynamic programming
- [x] 8.2 Create distance matrix (len1+1 x len2+1)
- [x] 8.3 Implement minimum edit distance algorithm
- [x] 8.4 Implement `damerauLevenshteinValue(str1, str2)` with transpositions
- [x] 8.5 Create extended matrix for Damerau-Levenshtein (len1+2 x len2+2)
- [x] 8.6 Track last occurrence of characters for transposition detection
- [x] 8.7 Implement `hammingValue(str1, str2)` with equal-length validation
- [x] 8.8 Implement `jaccardValue(str1, str2)` calculating character set similarity
- [x] 8.9 Implement `jaroSimilarityValue(str1, str2)` with match distance calculation
- [x] 8.10 Implement `jaroWinklerSimilarityValue(str1, str2)` with prefix bonus
- [x] 8.11 Add helper functions: min, max for distance calculations
- [x] 8.12 Add LEVENSHTEIN case to expression evaluator
- [x] 8.13 Add DAMERAU_LEVENSHTEIN case to expression evaluator
- [x] 8.14 Add HAMMING case to expression evaluator
- [x] 8.15 Add JACCARD case to expression evaluator
- [x] 8.16 Add JARO_SIMILARITY case to expression evaluator
- [x] 8.17 Add JARO_WINKLER_SIMILARITY case to expression evaluator
- [x] 8.18 Write unit tests for all distance functions
- [x] 8.19 Test known distances: LEVENSHTEIN('kitten', 'sitting') = 3
- [x] 8.20 Test DAMERAU_LEVENSHTEIN('ab', 'ba') = 1 (transposition)
- [x] 8.21 Test HAMMING error for unequal-length strings
- [x] 8.22 Test similarity functions return values in [0, 1]
- [x] 8.23 Performance test: LEVENSHTEIN and DAMERAU_LEVENSHTEIN on large strings (1000 chars)
- [x] 8.24 Integration test: String distance for fuzzy matching

## 9. Whitespace and Trimming Aliases

- [x] 9.1 Add STRIP case as alias for TRIM
- [x] 9.2 Add LSTRIP case as alias for LTRIM
- [x] 9.3 Add RSTRIP case as alias for RTRIM
- [x] 9.4 Write unit tests for aliases
- [x] 9.5 Verify aliases behave identically to original functions

## 10. Type Inference

- [x] 10.1 Add `inferStringFunctionType()` to `internal/binder/type_inference.go`
- [x] 10.2 Define return types for regex functions (BOOLEAN for REGEXP_MATCHES, VARCHAR for others)
- [x] 10.3 Define return types for string manipulation (VARCHAR, BIGINT for POSITION)
- [x] 10.4 Define return types for hash functions (VARCHAR for MD5/SHA256, BIGINT for HASH)
- [x] 10.5 Define return types for distance functions (BIGINT for LEVENSHTEIN/DAMERAU_LEVENSHTEIN/HAMMING, DOUBLE for similarity)
- [x] 10.6 Define return type for REGEXP_EXTRACT_ALL and STRING_SPLIT (LIST<VARCHAR>)
- [x] 10.7 Define return types for boolean functions (CONTAINS, PREFIX, SUFFIX)
- [x] 10.8 Write unit tests for type inference
- [x] 10.9 Integration test: Type compatibility in UNION queries

## 11. NULL Handling

- [x] 11.1 Add NULL checks to all string function implementations
- [x] 11.2 Return NULL for any function with NULL input (except CONCAT_WS)
- [x] 11.3 Implement CONCAT_WS NULL-skipping behavior (skip NULL args, keep non-NULL)
- [x] 11.4 Write unit tests for NULL propagation
- [x] 11.5 Test NULL handling in all string functions
- [x] 11.6 Test CONCAT_WS with mixed NULL and non-NULL arguments
- [x] 11.7 Integration test: NULL handling in complex queries

## 12. Error Handling

- [x] 12.1 Add validation error for invalid regex patterns
- [x] 12.2 Add error message: "Invalid regular expression: <details>"
- [x] 12.3 Add validation error for HAMMING with unequal-length strings
- [x] 12.4 Add error message: "HAMMING requires strings of equal length"
- [x] 12.5 Add validation error for CHR with out-of-range codes
- [x] 12.6 Add error message: "CHR code must be in ASCII range [0, 127]"
- [x] 12.7 Add validation error for REPEAT with negative count
- [x] 12.8 Add error message: "REPEAT count must be non-negative"
- [x] 12.9 Write unit tests for all error cases
- [x] 12.10 Integration test: Error handling in complex queries

## 13. DuckDB Compatibility Testing

- [x] 13.1 Create compatibility test suite comparing dukdb-go vs DuckDB CLI
- [x] 13.2 Test REGEXP_MATCHES with various RE2 patterns
- [x] 13.3 Test REGEXP_REPLACE with and without 'g' flag
- [x] 13.4 Test REGEXP_EXTRACT with group parameter
- [x] 13.5 Test CONCAT_WS NULL-skipping behavior
- [x] 13.6 Test STRING_SPLIT with various separators
- [x] 13.7 Test LPAD/RPAD with multi-character fill strings
- [x] 13.8 Test hash functions match DuckDB output (MD5, SHA256, HASH)
- [x] 13.9 Test LEVENSHTEIN distance matches DuckDB results
- [x] 13.10 Test DAMERAU_LEVENSHTEIN distance matches DuckDB results
- [x] 13.11 Test HAMMING, JACCARD, JARO, JARO_WINKLER outputs
- [x] 13.12 Verify error messages match DuckDB wording
- [x] 13.13 Compare regex behavior edge cases (empty matches, overlapping matches)
- [x] 13.14 Test Unicode handling in all string functions

## 14. Integration Tests

- [x] 14.1 Test string functions in SELECT clauses
- [x] 14.2 Test string functions in WHERE clauses
- [x] 14.3 Test string functions in computed columns
- [x] 14.4 Test string functions with aggregate functions
- [x] 14.5 Test nested string function calls: `SELECT UPPER(REGEXP_REPLACE(name, '[0-9]', ''))`
- [ ] 14.6 Test STRING_SPLIT with UNNEST for array expansion
- [x] 14.7 Test regex functions with table joins
- [x] 14.8 Integration test: Log parsing (REGEXP_EXTRACT for parsing log lines)
- [x] 14.9 Integration test: Data cleaning (TRIM, REGEXP_REPLACE for normalization)
- [x] 14.10 Integration test: Fuzzy matching (LEVENSHTEIN and DAMERAU_LEVENSHTEIN for similarity search)

## 15. Performance Testing

- [x] 15.1 Benchmark regex functions (REGEXP_MATCHES, REGEXP_REPLACE)
- [x] 15.2 Test regex pattern compilation caching (compile once, reuse)
- [x] 15.3 Benchmark hash functions (MD5, SHA256) on various string lengths
- [x] 15.4 Benchmark LEVENSHTEIN and DAMERAU_LEVENSHTEIN on strings of varying lengths (10, 100, 1000 chars)
- [x] 15.5 Benchmark STRING_SPLIT on large strings with many separators
- [x] 15.6 Profile memory usage for string operations
- [x] 15.7 Compare performance with DuckDB (target: within 2x)
- [x] 15.8 Identify optimization opportunities (pattern caching, SIMD)

## 16. Documentation

- [x] 16.1 Document all string functions in user guide
- [x] 16.2 Document regex syntax (RE2) and flags ('g' for global replacement)
- [x] 16.3 Document REGEXP_REPLACE default behavior (first match only)
- [x] 16.4 Document CONCAT_WS NULL-skipping behavior
- [x] 16.5 Document HAMMING equal-length requirement
- [x] 16.6 Document hash function output formats (lowercase hex)
- [x] 16.7 Document string distance interpretation (lower = more similar)
- [x] 16.8 Document DAMERAU_LEVENSHTEIN transposition support
- [x] 16.9 Document similarity functions return range [0, 1]
- [x] 16.10 Add examples for common use cases (log parsing, data cleaning, fuzzy matching)
- [x] 16.11 Document NULL propagation (all functions except CONCAT_WS)
- [x] 16.12 Document error handling behavior
- [x] 16.13 Add performance notes (LEVENSHTEIN and DAMERAU_LEVENSHTEIN O(n*m) complexity)

## 17. Validation and Release

- [x] 17.1 Run full test suite (unit + integration + compatibility)
- [x] 17.2 Validate all string functions work correctly
- [x] 17.3 Verify error handling is comprehensive
- [x] 17.4 Check performance benchmarks are acceptable
- [x] 17.5 Update CHANGELOG with string function support
- [x] 17.6 Update README with string function examples
- [x] 17.7 Create migration guide (no breaking changes, additive only)

## Dependencies and Parallelization

**Can be parallelized:**
- Tasks 2.x (Regex), 3.x (Concatenation), 4.x (Padding), 5.x (Case), 6.x (Manipulation), 7.x (Encoding), 8.x (Hash), 9.x (Distance), 10.x (Aliases) can be implemented concurrently
- Documentation tasks 17.x can be done anytime after corresponding features are implemented

**Sequential dependencies:**
- Task 1 (Infrastructure) must complete before all others
- Task 11 (Type inference) depends on tasks 2-10 being defined
- Task 12 (NULL handling) can be done concurrently with implementation
- Task 14 (Compatibility testing) depends on all implementations being complete
- Task 15 (Integration tests) depends on all implementations being complete

**Critical path:**
Task 1 → Tasks 2-10 (parallel) → Task 11 → Task 12 → Tasks 13-15 (parallel) → Task 16 → Tasks 17-18

**Estimated completion:**
- Task 1: 1 day (infrastructure)
- Tasks 2-10: 1.5 weeks (function implementations, can parallelize)
- Task 11-12: 2 days (type inference and NULL handling)
- Tasks 13-15: 1 week (error handling, compatibility, integration tests)
- Task 16: 2 days (performance testing)
- Tasks 17-18: 2 days (documentation and validation)
- **Total: 3.5 weeks** (with parallelization, could be 2.5-3 weeks)
