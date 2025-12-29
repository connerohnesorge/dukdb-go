## ADDED Requirements

### Requirement: Uhugeint Type Creation

The package SHALL create Uhugeint values from various sources.

#### Scenario: Create from *big.Int
- GIVEN valid *big.Int within 128-bit unsigned range
- WHEN calling NewUhugeint(bigInt)
- THEN Uhugeint is returned without error

#### Scenario: Create from negative *big.Int
- GIVEN negative *big.Int
- WHEN calling NewUhugeint(bigInt)
- THEN error indicating negative value is returned

#### Scenario: Create from overflow *big.Int
- GIVEN *big.Int exceeding 128-bit unsigned max
- WHEN calling NewUhugeint(bigInt)
- THEN error indicating overflow is returned

#### Scenario: Zero value
- GIVEN *big.Int with value 0
- WHEN calling NewUhugeint(bigInt)
- THEN Uhugeint with lower=0, upper=0 is returned

### Requirement: Uhugeint Conversion

The Uhugeint type SHALL convert to other representations.

#### Scenario: ToBigInt conversion
- GIVEN Uhugeint with value 12345
- WHEN calling ToBigInt()
- THEN *big.Int with value 12345 is returned

#### Scenario: String representation
- GIVEN Uhugeint with maximum value
- WHEN calling String()
- THEN "340282366920938463463374607431768211455" is returned

#### Scenario: Roundtrip conversion
- GIVEN any valid Uhugeint
- WHEN converted to *big.Int and back
- THEN original value is preserved

### Requirement: Uhugeint SQL Integration

The Uhugeint type SHALL integrate with database operations.

#### Scenario: Scan from query result
- GIVEN query returning UHUGEINT column
- WHEN scanning into *Uhugeint
- THEN value is correctly populated

#### Scenario: Bind as parameter
- GIVEN Uhugeint value
- WHEN binding to prepared statement
- THEN query executes with correct value

#### Scenario: Append via Appender
- GIVEN Appender for table with UHUGEINT column
- WHEN appending Uhugeint value
- THEN row is inserted correctly

### Requirement: Bit Type Creation

The package SHALL create Bit values from various sources.

#### Scenario: Create from bit string
- GIVEN bit string "10110"
- WHEN calling NewBit("10110")
- THEN Bit with length 5 is returned

#### Scenario: Create from empty string
- GIVEN empty bit string ""
- WHEN calling NewBit("")
- THEN Bit with length 0 is returned

#### Scenario: Create from bytes
- GIVEN byte slice and length
- WHEN calling NewBitFromBytes(data, length)
- THEN Bit with specified length is returned

#### Scenario: Invalid bit string
- GIVEN bit string with invalid character "102"
- WHEN calling NewBit("102")
- THEN error is returned

### Requirement: Bit Access Operations

The Bit type SHALL provide bit-level access.

#### Scenario: Get bit at position
- GIVEN Bit "10110"
- WHEN calling Get(0)
- THEN true is returned

#### Scenario: Get second bit
- GIVEN Bit "10110"
- WHEN calling Get(1)
- THEN false is returned

#### Scenario: Get out of range
- GIVEN Bit with length 5
- WHEN calling Get(5)
- THEN error is returned

#### Scenario: Set bit to true
- GIVEN Bit "00000"
- WHEN calling Set(2, true)
- THEN Bit becomes "00100"

#### Scenario: Set bit to false
- GIVEN Bit "11111"
- WHEN calling Set(2, false)
- THEN Bit becomes "11011"

### Requirement: Bit Bitwise Operations

The Bit type SHALL support bitwise operations.

#### Scenario: AND operation
- GIVEN Bit "1010" and Bit "1100"
- WHEN calling And()
- THEN Bit "1000" is returned

#### Scenario: OR operation
- GIVEN Bit "1010" and Bit "1100"
- WHEN calling Or()
- THEN Bit "1110" is returned

#### Scenario: XOR operation
- GIVEN Bit "1010" and Bit "1100"
- WHEN calling Xor()
- THEN Bit "0110" is returned

#### Scenario: NOT operation
- GIVEN Bit "1010"
- WHEN calling Not()
- THEN Bit "0101" is returned

#### Scenario: Operation length mismatch
- GIVEN Bit "1010" and Bit "11"
- WHEN calling And()
- THEN error is returned

### Requirement: Bit SQL Integration

The Bit type SHALL integrate with database operations.

#### Scenario: Scan from query result
- GIVEN query returning BIT column
- WHEN scanning into *Bit
- THEN value is correctly populated

#### Scenario: Bind as parameter
- GIVEN Bit value
- WHEN binding to prepared statement
- THEN query executes with correct value

#### Scenario: String representation
- GIVEN Bit with value
- WHEN calling String()
- THEN bit string like "10110" is returned

### Requirement: TimeNS Type Creation

The package SHALL create TimeNS values from components.

#### Scenario: Create from components
- GIVEN hour=14, min=30, sec=45, nsec=123456789
- WHEN calling NewTimeNS(14, 30, 45, 123456789)
- THEN TimeNS with correct value is returned

#### Scenario: Midnight value
- GIVEN hour=0, min=0, sec=0, nsec=0
- WHEN calling NewTimeNS(0, 0, 0, 0)
- THEN TimeNS with value 0 is returned

#### Scenario: End of day
- GIVEN hour=23, min=59, sec=59, nsec=999999999
- WHEN calling NewTimeNS(23, 59, 59, 999999999)
- THEN TimeNS with maximum value is returned

### Requirement: TimeNS Component Extraction

The TimeNS type SHALL extract time components.

#### Scenario: Extract components
- GIVEN TimeNS representing 14:30:45.123456789
- WHEN calling Components()
- THEN hour=14, min=30, sec=45, nsec=123456789 is returned

#### Scenario: Nanosecond precision preserved
- GIVEN TimeNS with 1 nanosecond
- WHEN calling Components()
- THEN nsec=1 is returned

### Requirement: TimeNS Conversion

The TimeNS type SHALL convert to other representations.

#### Scenario: Convert to time.Time
- GIVEN TimeNS value
- WHEN calling ToTime()
- THEN time.Time with correct time is returned

#### Scenario: String formatting
- GIVEN TimeNS representing 14:30:45.123456789
- WHEN calling String()
- THEN "14:30:45.123456789" is returned

#### Scenario: String with leading zeros
- GIVEN TimeNS representing 01:02:03.000000004
- WHEN calling String()
- THEN "01:02:03.000000004" is returned

### Requirement: TimeNS SQL Integration

The TimeNS type SHALL integrate with database operations.

#### Scenario: Scan from query result
- GIVEN query returning TIME column with nanosecond precision
- WHEN scanning into *TimeNS
- THEN value is correctly populated with nanosecond precision

#### Scenario: Bind as parameter
- GIVEN TimeNS value
- WHEN binding to prepared statement
- THEN query executes with correct value

#### Scenario: Roundtrip preserves precision
- GIVEN TimeNS with specific nanoseconds
- WHEN inserted and queried back
- THEN nanosecond precision is preserved
