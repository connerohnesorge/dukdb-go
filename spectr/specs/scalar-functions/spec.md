# Scalar Functions Specification

## Requirements

### Requirement: IF/IFF conditional function SHALL return branch value based on condition

The IF and IFF functions SHALL accept three arguments (condition, true_value, false_value) and return the appropriate branch value.

#### Scenario: IF with true condition
```
When the user executes "SELECT IF(true, 'yes', 'no')"
Then the result is 'yes'
```

#### Scenario: IF with false condition
```
When the user executes "SELECT IF(false, 'yes', 'no')"
Then the result is 'no'
```

#### Scenario: IF with NULL condition returns false branch
```
When the user executes "SELECT IF(NULL, 'yes', 'no')"
Then the result is 'no'
Because NULL condition is treated as false
```

#### Scenario: IFF is alias for IF
```
When the user executes "SELECT IFF(1 > 0, 'bigger', 'smaller')"
Then the result is 'bigger'
```

#### Scenario: IF with different branch types
```
When the user executes "SELECT IF(true, 1, 2.5)"
Then the result is 1.0 (DOUBLE, promoted from common supertype)
```

### Requirement: FORMAT/PRINTF SHALL format strings with printf-style specifiers

The FORMAT and PRINTF functions SHALL accept a format string and arguments and return a formatted VARCHAR.

#### Scenario: FORMAT with %s specifier
```
When the user executes "SELECT FORMAT('Hello, %s!', 'world')"
Then the result is 'Hello, world!'
```

#### Scenario: FORMAT with %d specifier
```
When the user executes "SELECT FORMAT('Count: %d', 42)"
Then the result is 'Count: 42'
```

#### Scenario: FORMAT with %f and precision
```
When the user executes "SELECT FORMAT('Pi is %.2f', 3.14159)"
Then the result is 'Pi is 3.14'
```

#### Scenario: FORMAT with %% literal percent
```
When the user executes "SELECT FORMAT('100%%')"
Then the result is '100%'
```

#### Scenario: FORMAT with width and padding
```
When the user executes "SELECT FORMAT('%05d', 42)"
Then the result is '00042'
```

#### Scenario: PRINTF is alias for FORMAT
```
When the user executes "SELECT PRINTF('%s=%d', 'x', 10)"
Then the result is 'x=10'
```

#### Scenario: FORMAT with NULL format string
```
When the user executes "SELECT FORMAT(NULL, 'arg')"
Then the result is NULL
```

### Requirement: TYPEOF SHALL return DuckDB-style type name as string

The TYPEOF function SHALL accept one argument and return its DuckDB type name as VARCHAR.

#### Scenario: TYPEOF on integer
```
When the user executes "SELECT TYPEOF(42)"
Then the result is 'INTEGER'
```

#### Scenario: TYPEOF on varchar
```
When the user executes "SELECT TYPEOF('hello')"
Then the result is 'VARCHAR'
```

#### Scenario: TYPEOF on NULL
```
When the user executes "SELECT TYPEOF(NULL)"
Then the result is 'NULL'
```

#### Scenario: TYPEOF on boolean
```
When the user executes "SELECT TYPEOF(true)"
Then the result is 'BOOLEAN'
```

### Requirement: PG_TYPEOF SHALL return PostgreSQL-style type name as string

The PG_TYPEOF function SHALL return PostgreSQL-compatible type names.

#### Scenario: PG_TYPEOF on integer
```
When the user executes "SELECT PG_TYPEOF(42)"
Then the result is 'integer'
```

#### Scenario: PG_TYPEOF on varchar
```
When the user executes "SELECT PG_TYPEOF('hello')"
Then the result is 'character varying'
```

#### Scenario: PG_TYPEOF on double
```
When the user executes "SELECT PG_TYPEOF(3.14)"
Then the result is 'double precision'
```

### Requirement: BASE64 encoding functions SHALL encode and decode base64

BASE64_ENCODE/BASE64/TO_BASE64 SHALL encode data to base64 string. BASE64_DECODE/FROM_BASE64 SHALL decode base64 string to binary.

#### Scenario: BASE64_ENCODE on string
```
When the user executes "SELECT BASE64_ENCODE('Hello')"
Then the result is 'SGVsbG8='
```

#### Scenario: BASE64_DECODE on encoded string
```
When the user executes "SELECT BASE64_DECODE('SGVsbG8=')"
Then the result is the binary equivalent of 'Hello'
```

#### Scenario: BASE64 alias for BASE64_ENCODE
```
When the user executes "SELECT BASE64('test')"
Then the result is 'dGVzdA=='
```

#### Scenario: FROM_BASE64 alias for BASE64_DECODE
```
When the user executes "SELECT FROM_BASE64('dGVzdA==')"
Then the result is the binary equivalent of 'test'
```

#### Scenario: BASE64_DECODE on invalid input
```
When the user executes "SELECT BASE64_DECODE('not-valid-base64!!!')"
Then an error is returned indicating invalid base64 input
```

#### Scenario: BASE64 with NULL input
```
When the user executes "SELECT BASE64_ENCODE(NULL)"
Then the result is NULL
```

### Requirement: URL encoding functions SHALL percent-encode and decode strings

URL_ENCODE SHALL percent-encode special characters. URL_DECODE SHALL decode percent-encoded strings.

#### Scenario: URL_ENCODE on string with spaces
```
When the user executes "SELECT URL_ENCODE('hello world')"
Then the result is 'hello+world'
Because form encoding represents spaces as +
```

#### Scenario: URL_ENCODE on string with special characters
```
When the user executes "SELECT URL_ENCODE('key=value&foo=bar')"
Then the result is 'key%3Dvalue%26foo%3Dbar'
```

#### Scenario: URL_DECODE on encoded string
```
When the user executes "SELECT URL_DECODE('hello+world')"
Then the result is 'hello world'
```

#### Scenario: URL_DECODE on percent-encoded string
```
When the user executes "SELECT URL_DECODE('key%3Dvalue')"
Then the result is 'key=value'
```

#### Scenario: URL functions with NULL input
```
When the user executes "SELECT URL_ENCODE(NULL)"
Then the result is NULL
```

