package dukdb

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/coder/quartz"
)

// UhugeintMaxString is the maximum value for UHUGEINT as a string.
// Max value: 2^128 - 1 = 340282366920938463463374607431768211455
const UhugeintMaxString = "340282366920938463463374607431768211455"

// Uhugeint represents a 128-bit unsigned integer.
// Uses two uint64 values for storage and *big.Int for arithmetic operations.
type Uhugeint struct {
	lower uint64 // Lower 64 bits
	upper uint64 // Upper 64 bits
}

// NewUhugeint creates a Uhugeint from a *big.Int.
// Returns an error if the value is negative or exceeds 128 bits.
func NewUhugeint(val *big.Int) (Uhugeint, error) {
	if val == nil {
		return Uhugeint{}, nil
	}

	if val.Sign() < 0 {
		return Uhugeint{}, fmt.Errorf("Uhugeint cannot be negative: %s", val.String())
	}

	// Check if value fits in 128 bits.
	maxVal, _ := new(big.Int).SetString(UhugeintMaxString, 10)
	if val.Cmp(maxVal) > 0 {
		return Uhugeint{}, fmt.Errorf("value exceeds UHUGEINT maximum: %s", val.String())
	}

	// Split into upper and lower 64-bit parts.
	divisor := new(big.Int).SetUint64(1)
	divisor.Lsh(divisor, 64)

	lower := new(big.Int)
	upper := new(big.Int)
	upper.DivMod(val, divisor, lower)

	return Uhugeint{
		lower: lower.Uint64(),
		upper: upper.Uint64(),
	}, nil
}

// NewUhugeintFromUint64 creates a Uhugeint from a single uint64.
func NewUhugeintFromUint64(val uint64) Uhugeint {
	return Uhugeint{lower: val, upper: 0}
}

// NewUhugeintFromParts creates a Uhugeint from upper and lower uint64 values.
func NewUhugeintFromParts(upper, lower uint64) Uhugeint {
	return Uhugeint{lower: lower, upper: upper}
}

// ToBigInt converts Uhugeint to *big.Int.
func (u Uhugeint) ToBigInt() *big.Int {
	result := new(big.Int).SetUint64(u.upper)
	result.Lsh(result, 64)
	result.Add(result, new(big.Int).SetUint64(u.lower))
	return result
}

// IsZero returns true if the value is zero.
func (u Uhugeint) IsZero() bool {
	return u.lower == 0 && u.upper == 0
}

// String returns the decimal string representation.
func (u Uhugeint) String() string {
	return u.ToBigInt().String()
}

// Scan implements the sql.Scanner interface.
func (u *Uhugeint) Scan(v any) error {
	if v == nil {
		*u = Uhugeint{}
		return nil
	}

	switch val := v.(type) {
	case Uhugeint:
		*u = val
	case *Uhugeint:
		*u = *val
	case *big.Int:
		result, err := NewUhugeint(val)
		if err != nil {
			return err
		}
		*u = result
	case int64:
		if val < 0 {
			return fmt.Errorf("Uhugeint cannot be negative: %d", val)
		}
		*u = NewUhugeintFromUint64(uint64(val))
	case uint64:
		*u = NewUhugeintFromUint64(val)
	case string:
		bi, ok := new(big.Int).SetString(val, 10)
		if !ok {
			return fmt.Errorf("invalid Uhugeint string: %s", val)
		}
		result, err := NewUhugeint(bi)
		if err != nil {
			return err
		}
		*u = result
	case []byte:
		bi := new(big.Int).SetBytes(val)
		result, err := NewUhugeint(bi)
		if err != nil {
			return err
		}
		*u = result
	default:
		return fmt.Errorf("cannot scan %T into Uhugeint", v)
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (u Uhugeint) Value() (driver.Value, error) {
	return u.String(), nil
}

// Upper returns the upper 64 bits.
func (u Uhugeint) Upper() uint64 {
	return u.upper
}

// Lower returns the lower 64 bits.
func (u Uhugeint) Lower() uint64 {
	return u.lower
}

// Add adds two Uhugeint values. Returns error on overflow.
func (u Uhugeint) Add(other Uhugeint) (Uhugeint, error) {
	result := u.ToBigInt().Add(u.ToBigInt(), other.ToBigInt())
	return NewUhugeint(result)
}

// Sub subtracts other from u. Returns error if result would be negative.
func (u Uhugeint) Sub(other Uhugeint) (Uhugeint, error) {
	result := new(big.Int).Sub(u.ToBigInt(), other.ToBigInt())
	if result.Sign() < 0 {
		return Uhugeint{}, fmt.Errorf("Uhugeint subtraction would result in negative value")
	}
	return NewUhugeint(result)
}

// Mul multiplies two Uhugeint values. Returns error on overflow.
func (u Uhugeint) Mul(other Uhugeint) (Uhugeint, error) {
	result := new(big.Int).Mul(u.ToBigInt(), other.ToBigInt())
	return NewUhugeint(result)
}

// Div divides u by other. Returns error on division by zero.
func (u Uhugeint) Div(other Uhugeint) (Uhugeint, error) {
	if other.IsZero() {
		return Uhugeint{}, fmt.Errorf("division by zero")
	}
	result := new(big.Int).Div(u.ToBigInt(), other.ToBigInt())
	return NewUhugeint(result)
}

// Cmp compares two Uhugeint values.
// Returns -1 if u < other, 0 if u == other, 1 if u > other.
func (u Uhugeint) Cmp(other Uhugeint) int {
	return u.ToBigInt().Cmp(other.ToBigInt())
}

// Equal returns true if u equals other.
func (u Uhugeint) Equal(other Uhugeint) bool {
	return u.lower == other.lower && u.upper == other.upper
}

// Bit represents a variable-length bit string.
// Bits are stored MSB first in the byte slice.
type Bit struct {
	data   []byte // Bit data (MSB first per byte)
	length int    // Number of valid bits
}

// NewBit creates a Bit from a bit string (e.g., "10110").
// Only '0' and '1' characters are allowed.
func NewBit(bitString string) (Bit, error) {
	if bitString == "" {
		return Bit{data: nil, length: 0}, nil
	}

	// Validate characters.
	for i, c := range bitString {
		if c != '0' && c != '1' {
			return Bit{}, fmt.Errorf("invalid bit character '%c' at position %d", c, i)
		}
	}

	length := len(bitString)
	byteLen := (length + 7) / 8
	data := make([]byte, byteLen)

	// Fill bytes MSB first.
	for i, c := range bitString {
		if c == '1' {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			data[byteIdx] |= (1 << bitIdx)
		}
	}

	return Bit{data: data, length: length}, nil
}

// NewBitFromBytes creates a Bit from bytes with specified length.
func NewBitFromBytes(data []byte, length int) Bit {
	if length < 0 {
		length = 0
	}
	if length == 0 {
		return Bit{data: nil, length: 0}
	}

	byteLen := (length + 7) / 8
	if len(data) < byteLen {
		byteLen = len(data)
	}

	// Make a copy of the data.
	newData := make([]byte, byteLen)
	copy(newData, data[:byteLen])

	// Clear any excess bits in the last byte.
	if excess := length % 8; excess != 0 {
		mask := byte(0xFF) << (8 - excess)
		newData[byteLen-1] &= mask
	}

	return Bit{data: newData, length: length}
}

// Get returns the bit at position (0-indexed).
func (b Bit) Get(pos int) (bool, error) {
	if pos < 0 || pos >= b.length {
		return false, fmt.Errorf("bit position %d out of range [0, %d)", pos, b.length)
	}
	byteIdx := pos / 8
	bitIdx := 7 - (pos % 8)
	return (b.data[byteIdx] & (1 << bitIdx)) != 0, nil
}

// Set sets the bit at position.
func (b *Bit) Set(pos int, val bool) error {
	if pos < 0 || pos >= b.length {
		return fmt.Errorf("bit position %d out of range [0, %d)", pos, b.length)
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

// Len returns the number of bits.
func (b Bit) Len() int {
	return b.length
}

// Bytes returns a copy of the underlying byte slice.
func (b Bit) Bytes() []byte {
	if b.data == nil {
		return nil
	}
	result := make([]byte, len(b.data))
	copy(result, b.data)
	return result
}

// String returns bit string representation.
func (b Bit) String() string {
	if b.length == 0 {
		return ""
	}

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

// Scan implements the sql.Scanner interface.
func (b *Bit) Scan(v any) error {
	if v == nil {
		*b = Bit{}
		return nil
	}

	switch val := v.(type) {
	case Bit:
		*b = val
	case *Bit:
		*b = *val
	case string:
		result, err := NewBit(val)
		if err != nil {
			return err
		}
		*b = result
	case []byte:
		// Interpret as a bit string.
		result, err := NewBit(string(val))
		if err != nil {
			// If it fails, try interpreting as raw bytes.
			*b = NewBitFromBytes(val, len(val)*8)
			return nil
		}
		*b = result
	default:
		return fmt.Errorf("cannot scan %T into Bit", v)
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (b Bit) Value() (driver.Value, error) {
	return b.String(), nil
}

// And returns the bitwise AND of b and other.
// Both Bits must have the same length.
func (b Bit) And(other Bit) (Bit, error) {
	if b.length != other.length {
		return Bit{}, fmt.Errorf("bit lengths must match: %d vs %d", b.length, other.length)
	}
	if b.length == 0 {
		return Bit{}, nil
	}

	result := make([]byte, len(b.data))
	for i := range b.data {
		result[i] = b.data[i] & other.data[i]
	}
	return Bit{data: result, length: b.length}, nil
}

// Or returns the bitwise OR of b and other.
// Both Bits must have the same length.
func (b Bit) Or(other Bit) (Bit, error) {
	if b.length != other.length {
		return Bit{}, fmt.Errorf("bit lengths must match: %d vs %d", b.length, other.length)
	}
	if b.length == 0 {
		return Bit{}, nil
	}

	result := make([]byte, len(b.data))
	for i := range b.data {
		result[i] = b.data[i] | other.data[i]
	}
	return Bit{data: result, length: b.length}, nil
}

// Xor returns the bitwise XOR of b and other.
// Both Bits must have the same length.
func (b Bit) Xor(other Bit) (Bit, error) {
	if b.length != other.length {
		return Bit{}, fmt.Errorf("bit lengths must match: %d vs %d", b.length, other.length)
	}
	if b.length == 0 {
		return Bit{}, nil
	}

	result := make([]byte, len(b.data))
	for i := range b.data {
		result[i] = b.data[i] ^ other.data[i]
	}
	return Bit{data: result, length: b.length}, nil
}

// Not returns the bitwise NOT of b.
func (b Bit) Not() Bit {
	if b.length == 0 {
		return Bit{}
	}

	result := make([]byte, len(b.data))
	for i := range b.data {
		result[i] = ^b.data[i]
	}

	// Clear excess bits in the last byte.
	if excess := b.length % 8; excess != 0 {
		mask := byte(0xFF) << (8 - excess)
		result[len(result)-1] &= mask
	}

	return Bit{data: result, length: b.length}
}

// nanosPerSecond is used for TimeNS calculations.
const nanosPerSecond = 1_000_000_000

// nanosPerMinute is used for TimeNS calculations.
const nanosPerMinute = 60 * nanosPerSecond

// nanosPerHour is used for TimeNS calculations.
const nanosPerHour = 60 * nanosPerMinute

// TimeNS represents time with nanosecond precision.
// Stored as nanoseconds since midnight (0 to 86399999999999).
type TimeNS int64

// NewTimeNS creates TimeNS from hour, minute, second, and nanosecond components.
func NewTimeNS(hour, min, sec int, nsec int64) TimeNS {
	total := int64(hour)*nanosPerHour +
		int64(min)*nanosPerMinute +
		int64(sec)*nanosPerSecond +
		nsec
	return TimeNS(total)
}

// Components extracts hour, minute, second, and nanosecond from TimeNS.
func (t TimeNS) Components() (hour, min, sec int, nsec int64) {
	remaining := int64(t)
	hour = int(remaining / nanosPerHour)
	remaining %= nanosPerHour
	min = int(remaining / nanosPerMinute)
	remaining %= nanosPerMinute
	sec = int(remaining / nanosPerSecond)
	nsec = remaining % nanosPerSecond
	return
}

// ToTime converts TimeNS to time.Time.
// The date portion is set to Unix epoch (1970-01-01).
func (t TimeNS) ToTime() time.Time {
	h, m, s, ns := t.Components()
	return time.Date(1970, 1, 1, h, m, s, int(ns), time.UTC)
}

// FromTime creates TimeNS from a time.Time value.
func TimeNSFromTime(t time.Time) TimeNS {
	return NewTimeNS(t.Hour(), t.Minute(), t.Second(), int64(t.Nanosecond()))
}

// String returns the time in "HH:MM:SS.nnnnnnnnn" format.
func (t TimeNS) String() string {
	h, m, s, ns := t.Components()
	if ns == 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d:%02d.%09d", h, m, s, ns)
}

// Scan implements the sql.Scanner interface.
func (t *TimeNS) Scan(v any) error {
	if v == nil {
		*t = 0
		return nil
	}

	switch val := v.(type) {
	case TimeNS:
		*t = val
	case int64:
		*t = TimeNS(val)
	case time.Time:
		*t = TimeNSFromTime(val)
	case string:
		// Parse "HH:MM:SS" or "HH:MM:SS.nnnnnnnnn".
		var h, m, s int
		var ns int64

		parts := strings.Split(val, ".")
		if _, err := fmt.Sscanf(parts[0], "%d:%d:%d", &h, &m, &s); err != nil {
			return fmt.Errorf("invalid TimeNS format: %s", val)
		}

		if len(parts) == 2 {
			// Parse nanoseconds, padding to 9 digits.
			nsPart := parts[1]
			for len(nsPart) < 9 {
				nsPart += "0"
			}
			if len(nsPart) > 9 {
				nsPart = nsPart[:9]
			}
			if _, err := fmt.Sscanf(nsPart, "%d", &ns); err != nil {
				return fmt.Errorf("invalid nanosecond format: %s", val)
			}
		}

		*t = NewTimeNS(h, m, s, ns)
	default:
		return fmt.Errorf("cannot scan %T into TimeNS", v)
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (t TimeNS) Value() (driver.Value, error) {
	return t.String(), nil
}

// NowNS returns the current time as TimeNS using the provided clock.
// This is an alias for CurrentTimeNS, providing a more intuitive name.
// This enables deterministic testing by using quartz.Mock instead of real time.
func NowNS(clock quartz.Clock) TimeNS {
	if clock == nil {
		clock = quartz.NewReal()
	}
	now := clock.Now()
	return TimeNSFromTime(now)
}

// CurrentTimeNS returns the current time as TimeNS using the provided clock.
// This enables deterministic testing by using quartz.Mock instead of real time.
func CurrentTimeNS(clock quartz.Clock) TimeNS {
	if clock == nil {
		clock = quartz.NewReal()
	}
	now := clock.Now()
	return TimeNSFromTime(now)
}

// CurrentTimestampNS returns the current timestamp in nanoseconds since epoch.
// This enables deterministic testing by using quartz.Mock instead of real time.
func CurrentTimestampNS(clock quartz.Clock) int64 {
	if clock == nil {
		clock = quartz.NewReal()
	}
	return clock.Now().UnixNano()
}

// Interface assertions.
var (
	_ sql.Scanner   = (*Uhugeint)(nil)
	_ driver.Valuer = Uhugeint{}
	_ sql.Scanner   = (*Bit)(nil)
	_ driver.Valuer = Bit{}
	_ sql.Scanner   = (*TimeNS)(nil)
	_ driver.Valuer = TimeNS(0)
)
