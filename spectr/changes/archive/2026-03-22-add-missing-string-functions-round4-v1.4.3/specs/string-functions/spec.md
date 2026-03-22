# String Functions Round 4

## ADDED Requirements

### Requirement: OCTET_LENGTH SHALL return byte count

OCTET_LENGTH(string) SHALL return the number of bytes in the string representation. It MUST follow SQL standard semantics.

#### Scenario: ASCII string

Given a running database
When the user executes `SELECT OCTET_LENGTH('hello')`
Then the result MUST be 5

#### Scenario: Multi-byte UTF-8 string

Given a running database
When the user executes `SELECT OCTET_LENGTH('héllo')`
Then the result MUST be 6

### Requirement: LCASE and UCASE SHALL be aliases

LCASE SHALL be an alias for LOWER. UCASE SHALL be an alias for UPPER. Both MUST produce identical results to their canonical forms.

#### Scenario: LCASE alias

Given a running database
When the user executes `SELECT LCASE('HELLO')`
Then the result MUST be 'hello'

#### Scenario: UCASE alias

Given a running database
When the user executes `SELECT UCASE('hello')`
Then the result MUST be 'HELLO'

### Requirement: INITCAP SHALL capitalize word beginnings

INITCAP(string) SHALL capitalize the first letter of each word and lowercase all other letters. Word boundaries MUST include spaces, tabs, and common punctuation.

#### Scenario: Basic capitalization

Given a running database
When the user executes `SELECT INITCAP('hello world')`
Then the result MUST be 'Hello World'

#### Scenario: Mixed case normalization

Given a running database
When the user executes `SELECT INITCAP('hELLO wORLD')`
Then the result MUST be 'Hello World'

### Requirement: SOUNDEX SHALL return phonetic code

SOUNDEX(string) SHALL return a 4-character SOUNDEX phonetic code. It MUST follow the standard American SOUNDEX algorithm.

#### Scenario: Standard SOUNDEX

Given a running database
When the user executes `SELECT SOUNDEX('Robert')`
Then the result MUST be 'R163'

#### Scenario: Phonetic equivalence

Given a running database
When the user executes `SELECT SOUNDEX('Robert') = SOUNDEX('Rupert')`
Then the result MUST be true

### Requirement: LIKE_ESCAPE SHALL support custom escape characters

LIKE_ESCAPE(string, pattern, escape_char) SHALL perform LIKE pattern matching with a custom escape character. It MUST return a BOOLEAN value.

#### Scenario: Escaped percent sign

Given a running database
When the user executes `SELECT LIKE_ESCAPE('10%', '10#%', '#')`
Then the result MUST be true
