# Change: Add Extended Type Support (UHUGEINT, BIT, TIME_NS)

## Why

The current dukdb-go implementation has type constants defined for UHUGEINT, BIT, and TIME_NS but lacks full implementation support. Users need:
- UHUGEINT (128-bit unsigned integer) for large unsigned values (addresses, hashes, counters)
- BIT (variable-length bit string) for binary flags, bitmaps, and packed data
- TIME_NS (nanosecond-precision time) for high-precision temporal data

The duckdb-go CGO reference explicitly marks these types as unsupported in its `unsupportedTypeToStringMap`. Implementing them in the pure Go dukdb-go provides feature parity and demonstrates the advantages of the pure Go approach.

## What Changes

### UHUGEINT Type

```go
// Uhugeint represents a 128-bit unsigned integer.
// Uses *big.Int internally for arithmetic operations.
type Uhugeint struct {
    lower uint64 // Lower 64 bits
    upper uint64 // Upper 64 bits
}

// NewUhugeint creates a Uhugeint from a *big.Int.
func NewUhugeint(val *big.Int) (Uhugeint, error)

// ToBigInt converts Uhugeint to *big.Int.
func (u Uhugeint) ToBigInt() *big.Int

// Scan implements sql.Scanner.
func (u *Uhugeint) Scan(v any) error

// Value implements driver.Valuer.
func (u Uhugeint) Value() (driver.Value, error)

// String returns decimal string representation.
func (u Uhugeint) String() string

// Max value: 340282366920938463463374607431768211455
const UhugeintMax = "340282366920938463463374607431768211455"
```

### BIT Type

```go
// Bit represents a variable-length bit string.
// Stores bits efficiently in byte slices.
type Bit struct {
    data   []byte // Bit data (MSB first)
    length int    // Number of valid bits
}

// NewBit creates a Bit from a bit string (e.g., "10110").
func NewBit(bitString string) (Bit, error)

// NewBitFromBytes creates a Bit from bytes with specified length.
func NewBitFromBytes(data []byte, length int) Bit

// Get returns the bit at position (0-indexed).
func (b Bit) Get(pos int) (bool, error)

// Set sets the bit at position.
func (b *Bit) Set(pos int, val bool) error

// Len returns the number of bits.
func (b Bit) Len() int

// Bytes returns the underlying byte slice.
func (b Bit) Bytes() []byte

// String returns bit string representation.
func (b Bit) String() string

// Scan implements sql.Scanner.
func (b *Bit) Scan(v any) error

// Value implements driver.Valuer.
func (b Bit) Value() (driver.Value, error)

// Bitwise operations
func (b Bit) And(other Bit) (Bit, error)
func (b Bit) Or(other Bit) (Bit, error)
func (b Bit) Xor(other Bit) (Bit, error)
func (b Bit) Not() Bit
```

### TIME_NS Type

```go
// TimeNS represents time with nanosecond precision.
// Stored as nanoseconds since midnight.
type TimeNS int64

// NewTimeNS creates TimeNS from hour, minute, second, nanosecond.
func NewTimeNS(hour, min, sec int, nsec int64) TimeNS

// Components extracts hour, minute, second, nanosecond.
func (t TimeNS) Components() (hour, min, sec int, nsec int64)

// ToTime converts to time.Time (date portion is zero).
func (t TimeNS) ToTime() time.Time

// Scan implements sql.Scanner.
func (t *TimeNS) Scan(v any) error

// Value implements driver.Valuer.
func (t TimeNS) Value() (driver.Value, error)

// String returns "HH:MM:SS.nnnnnnnnn" format.
func (t TimeNS) String() string
```

### Row/Result Integration

```go
// Updated scanValue in rows.go
func (r *Rows) scanValue(colIdx int, dest any) error {
    // ... existing cases ...
    case TYPE_UHUGEINT:
        return scanUhugeint(value, dest)
    case TYPE_BIT:
        return scanBit(value, dest)
    case TYPE_TIME_NS:
        return scanTimeNS(value, dest)
}
```

### Parameter Binding

```go
// Updated bindParameter in statement.go
func (s *Stmt) bindParameter(idx int, val any) error {
    switch v := val.(type) {
    case Uhugeint:
        return s.bindUhugeint(idx, v)
    case *Uhugeint:
        return s.bindUhugeint(idx, *v)
    case Bit:
        return s.bindBit(idx, v)
    case *Bit:
        return s.bindBit(idx, *v)
    case TimeNS:
        return s.bindTimeNS(idx, v)
    // ... existing cases ...
    }
}
```

## Impact

- **Affected specs**: Extends type-system capability, **deterministic-testing**
- **Affected code**: `types.go`, `rows.go`, `statement.go`, `appender.go`
- **Dependencies**: Existing type system, *big.Int for Uhugeint, quartz.Clock for TIME_NS
- **Consumers**: Cryptography apps, IoT systems, high-precision time applications

## Deterministic Testing Requirements

Per `spectr/specs/deterministic-testing/spec.md`, TIME_NS operations must use injected clock:

```go
// TimeNS operations that reference "now" use injected clock
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

**Zero Flaky Tests Policy**: No `time.Now()` in TIME_NS tests. All tests use `quartz.Mock` for reproducible nanosecond values.

## Breaking Changes

None. This adds new types while preserving existing behavior.
