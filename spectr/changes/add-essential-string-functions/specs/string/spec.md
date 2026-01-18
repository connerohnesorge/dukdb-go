## ADDED Requirements

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
