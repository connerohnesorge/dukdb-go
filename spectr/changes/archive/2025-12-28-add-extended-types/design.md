## Context

DuckDB supports several specialized types that require specific handling beyond standard SQL types. While dukdb-go defines type constants for UHUGEINT, BIT, and TIME_NS, the actual implementations for scanning, binding, and value conversion are missing.

**Stakeholders**: Cryptography developers, IoT application builders, high-frequency data processors

**Constraints**:
- Must use pure Go (no CGO)
- Must integrate with existing sql.Scanner/driver.Valuer interfaces
- UHUGEINT must handle full 128-bit unsigned range
- BIT operations must be efficient for large bit strings
- TIME_NS must preserve nanosecond precision

## Goals / Non-Goals

### Goals
- Full UHUGEINT support (scan, bind, arithmetic)
- Full BIT support (scan, bind, bitwise operations)
- Full TIME_NS support (scan, bind, formatting)
- Integration with Appender for bulk loading
- Integration with DataChunk for vectorized access
- Comprehensive test coverage including edge cases

### Non-Goals
- VARINT type (not in DuckDB core, internal use only)
- BIGNUM type (internal type for arbitrary precision)
- Extending existing HUGEINT implementation (already works)

## Decisions

### Decision 1: Uhugeint Internal Representation

**What**: Store as struct with lower and upper uint64 fields

**Why**:
- Matches DuckDB internal storage format
- Efficient memory layout (16 bytes)
- Direct conversion to/from *big.Int for arithmetic
- Avoids heap allocation for simple operations

**Implementation**:
```go
type Uhugeint struct {
    lower uint64
    upper uint64
}

// ToBigInt converts to *big.Int for arithmetic
func (u Uhugeint) ToBigInt() *big.Int {
    result := new(big.Int).SetUint64(u.upper)
    result.Lsh(result, 64)
    result.Or(result, new(big.Int).SetUint64(u.lower))
    return result
}

// FromBigInt creates Uhugeint from *big.Int
func UhugeintFromBigInt(val *big.Int) (Uhugeint, error) {
    if val.Sign() < 0 {
        return Uhugeint{}, errors.New("uhugeint cannot be negative")
    }

    // Check if value fits in 128 bits
    maxVal := new(big.Int)
    maxVal.SetString(UhugeintMax, 10)
    if val.Cmp(maxVal) > 0 {
        return Uhugeint{}, errors.New("value exceeds uhugeint max")
    }

    mask64 := new(big.Int).SetUint64(^uint64(0))
    lower := new(big.Int).And(val, mask64).Uint64()
    upper := new(big.Int).Rsh(val, 64).Uint64()

    return Uhugeint{lower: lower, upper: upper}, nil
}
```

### Decision 2: Bit Type Storage

**What**: Store as byte slice with explicit bit length

**Why**:
- Efficient storage (8 bits per byte)
- Variable length support
- Direct compatibility with DuckDB binary format
- Supports bit strings not aligned to byte boundaries

**Implementation**:
```go
type Bit struct {
    data   []byte
    length int  // Actual number of bits
}

// Get returns bit at position (0-indexed from left/MSB)
func (b Bit) Get(pos int) (bool, error) {
    if pos < 0 || pos >= b.length {
        return false, errors.New("position out of range")
    }
    byteIdx := pos / 8
    bitIdx := 7 - (pos % 8)  // MSB first
    return (b.data[byteIdx] & (1 << bitIdx)) != 0, nil
}

// Set sets bit at position
func (b *Bit) Set(pos int, val bool) error {
    if pos < 0 || pos >= b.length {
        return errors.New("position out of range")
    }
    byteIdx := pos / 8
    bitIdx := 7 - (pos % 8)
    if val {
        b.data[byteIdx] |= (1 << bitIdx)
    } else {
        b.data[byteIdx] &^= (1 << bitIdx)
    }
    return nil
}

// String returns bit string like "10110"
func (b Bit) String() string {
    var sb strings.Builder
    sb.Grow(b.length)
    for i := 0; i < b.length; i++ {
        val, _ := b.Get(i)
        if val {
            sb.WriteByte('1')
        } else {
            sb.WriteByte('0')
        }
    }
    return sb.String()
}
```

### Decision 3: TIME_NS Storage Format

**What**: Store as int64 nanoseconds since midnight

**Why**:
- Simple arithmetic for time calculations
- Full nanosecond precision
- Consistent with DuckDB internal format
- Easy conversion to/from time.Time

**Implementation**:
```go
type TimeNS int64

const (
    nsPerSecond = 1_000_000_000
    nsPerMinute = 60 * nsPerSecond
    nsPerHour   = 60 * nsPerMinute
)

func NewTimeNS(hour, min, sec int, nsec int64) TimeNS {
    return TimeNS(
        int64(hour)*nsPerHour +
        int64(min)*nsPerMinute +
        int64(sec)*nsPerSecond +
        nsec,
    )
}

func (t TimeNS) Components() (hour, min, sec int, nsec int64) {
    ns := int64(t)
    hour = int(ns / nsPerHour)
    ns %= nsPerHour
    min = int(ns / nsPerMinute)
    ns %= nsPerMinute
    sec = int(ns / nsPerSecond)
    nsec = ns % nsPerSecond
    return
}

func (t TimeNS) String() string {
    h, m, s, ns := t.Components()
    return fmt.Sprintf("%02d:%02d:%02d.%09d", h, m, s, ns)
}
```

### Decision 4: Clock Injection for TIME_NS "Now" Operations

**What**: Use injected quartz.Clock for operations that reference current time

**Why**:
- Per deterministic-testing spec, all time-dependent code must use injected clock
- Enables deterministic testing of TIME_NS values
- Ensures zero flaky tests for time-based operations

**Implementation**:
```go
// CurrentTimeNS creates a TimeNS from the current time using injected clock
func CurrentTimeNS(clock quartz.Clock) TimeNS {
    now := clock.Now()
    return NewTimeNS(now.Hour(), now.Minute(), now.Second(), int64(now.Nanosecond()))
}

// Tests use mock clock for deterministic time values
func TestTimeNSDeterministic(t *testing.T) {
    mClock := quartz.NewMock(t)
    mClock.Set(time.Date(2024, 1, 15, 14, 30, 45, 123456789, time.UTC))

    timeNS := CurrentTimeNS(mClock)
    h, m, s, ns := timeNS.Components()

    assert.Equal(t, 14, h)
    assert.Equal(t, 30, m)
    assert.Equal(t, 45, s)
    assert.Equal(t, int64(123456789), ns)
}
```

### Decision 5: Backend SQL Generation

**What**: Generate SQL literals for parameter binding

**Why**:
- Backend executes SQL directly
- Must properly format values for SQL parser
- Handle edge cases (max values, special formats)

**Implementation**:
```go
// FormatValue extension for new types
func FormatValue(val any) (string, error) {
    switch v := val.(type) {
    case Uhugeint:
        return v.ToBigInt().String(), nil
    case Bit:
        return fmt.Sprintf("'%s'::BIT", v.String()), nil
    case TimeNS:
        return fmt.Sprintf("'%s'::TIME", v.String()), nil
    // ... existing cases
    }
}
```

## Risks / Trade-offs

### Risk 1: Uhugeint Arithmetic Overflow
**Risk**: Operations may overflow 128-bit range
**Mitigation**:
- Use *big.Int for arithmetic operations
- Provide explicit overflow checking methods
- Return errors for out-of-range operations

### Risk 2: Bit Type Memory for Large Strings
**Risk**: Very large bit strings consume memory
**Mitigation**:
- Document memory implications
- Use byte slices for efficient storage
- Consider streaming interface for very large bit strings

### Risk 3: TIME_NS Parsing Edge Cases
**Risk**: Various time formats may be encountered
**Mitigation**:
- Support multiple input formats in Scan
- Document expected format
- Provide explicit parsing functions

## Migration Plan

New types with no migration required. Existing code continues to work unchanged.
