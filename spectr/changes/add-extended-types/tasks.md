## 1. Uhugeint Type Implementation

- [ ] 1.1 Define Uhugeint struct with lower/upper uint64
- [ ] 1.2 Implement NewUhugeint from *big.Int
- [ ] 1.3 Implement ToBigInt() conversion
- [ ] 1.4 Implement sql.Scanner interface
- [ ] 1.5 Implement driver.Valuer interface
- [ ] 1.6 Implement String() method
- [ ] 1.7 Add arithmetic helper methods (Add, Sub, Mul, Div)
- [ ] 1.8 Add comparison methods (Cmp, Equal)
- [ ] 1.9 Write unit tests for edge cases (0, max, overflow)

## 2. BIT Type Implementation

- [ ] 2.1 Define Bit struct with data []byte and length int
- [ ] 2.2 Implement NewBit from string ("10110")
- [ ] 2.3 Implement NewBitFromBytes constructor
- [ ] 2.4 Implement Get(pos) method
- [ ] 2.5 Implement Set(pos, val) method
- [ ] 2.6 Implement Len() method
- [ ] 2.7 Implement Bytes() method
- [ ] 2.8 Implement String() method
- [ ] 2.9 Implement sql.Scanner interface
- [ ] 2.10 Implement driver.Valuer interface
- [ ] 2.11 Implement bitwise operations (And, Or, Xor, Not)
- [ ] 2.12 Write unit tests for operations and edge cases

## 3. TIME_NS Type Implementation

- [ ] 3.1 Define TimeNS as int64 type
- [ ] 3.2 Implement NewTimeNS constructor
- [ ] 3.3 Implement Components() extraction
- [ ] 3.4 Implement ToTime() conversion
- [ ] 3.5 Implement sql.Scanner interface
- [ ] 3.6 Implement driver.Valuer interface
- [ ] 3.7 Implement String() with nanosecond formatting
- [ ] 3.8 Write unit tests for precision edge cases

## 4. Row Scanning Integration

- [ ] 4.1 Add scanUhugeint helper function
- [ ] 4.2 Add scanBit helper function
- [ ] 4.3 Add scanTimeNS helper function
- [ ] 4.4 Update Rows.scanValue for new types
- [ ] 4.5 Write integration tests for scanning

## 5. Parameter Binding Integration

- [ ] 5.1 Add bindUhugeint method to Stmt
- [ ] 5.2 Add bindBit method to Stmt
- [ ] 5.3 Add bindTimeNS method to Stmt
- [ ] 5.4 Update bindParameter switch for new types
- [ ] 5.5 Write integration tests for binding

## 6. Appender Integration

- [ ] 6.1 Update FormatValue for Uhugeint
- [ ] 6.2 Update FormatValue for Bit
- [ ] 6.3 Update FormatValue for TimeNS
- [ ] 6.4 Write appender tests for new types

## 7. DataChunk Integration (depends on DataChunk proposal)

- [ ] 7.1 Add getUhugeint vector getter
- [ ] 7.2 Add setUhugeint vector setter
- [ ] 7.3 Add getBit vector getter
- [ ] 7.4 Add setBit vector setter
- [ ] 7.5 Add getTimeNS vector getter
- [ ] 7.6 Add setTimeNS vector setter
- [ ] 7.7 Write DataChunk tests for new types

## 8. Validation

- [ ] 8.1 Run `go test -race`
- [ ] 8.2 Run `golangci-lint`
- [ ] 8.3 Benchmark operations vs big.Int baseline
- [ ] 8.4 Test roundtrip for all types
