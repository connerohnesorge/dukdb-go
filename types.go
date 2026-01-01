package dukdb

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
)

// dukdb-go exports the following type wrappers:
// UUID, Map, Interval, Decimal, Union, Composite (optional, used to scan LIST and STRUCT).
// This file contains pure Go implementations that mirror duckdb-go's type system.

const uuidLength = 16

// secondsPerDay is used for date calculations.
const secondsPerDay = 86400

// UUID represents a 128-bit UUID value.
// Implements sql.Scanner and driver.Valuer interfaces.
type UUID [uuidLength]byte

// Scan implements the sql.Scanner interface.
// Accepts []byte (16 bytes) or string (UUID format) as input.
// NULL values should be handled by the caller; passing nil will result in an error.
func (u *UUID) Scan(v any) error {
	switch val := v.(type) {
	case []byte:
		if len(val) != uuidLength {
			// Try parsing as string if not exactly 16 bytes
			return u.Scan(string(val))
		}
		copy(u[:], val)
	case string:
		id, err := uuid.Parse(val)
		if err != nil {
			return err
		}
		copy(u[:], id[:])
	default:
		return fmt.Errorf("invalid UUID value type: %T", val)
	}

	return nil
}

// String implements the fmt.Stringer interface.
// Returns the UUID in hyphenated format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
func (u *UUID) String() string {
	buf := make([]byte, 36)

	hex.Encode(buf, u[:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], u[10:])

	return string(buf)
}

// Value implements the driver.Valuer interface.
// Returns the UUID as a hyphenated string.
func (u *UUID) Value() (driver.Value, error) {
	return u.String(), nil
}

// Map represents a DuckDB MAP type as a Go map with any key and value types.
// This type only implements Scan (no Value method) since maps are read-only from query results.
type Map map[any]any

// Scan implements the sql.Scanner interface.
// Accepts only Map type as input (type assertion).
// NULL values should be handled by the caller.
func (m *Map) Scan(v any) error {
	data, ok := v.(Map)
	if !ok {
		return fmt.Errorf(
			"invalid type `%T` for scanning `Map`, expected `Map`",
			v,
		)
	}

	*m = data

	return nil
}

// Interval represents a DuckDB INTERVAL type.
// An interval consists of months, days, and microseconds components.
type Interval struct {
	Days   int32 `json:"days"`
	Months int32 `json:"months"`
	Micros int64 `json:"micros"`
}

// Decimal represents a DuckDB DECIMAL type with fixed precision and scale.
// Width is the total number of digits (precision).
// Scale is the number of digits after the decimal point.
// Value is the unscaled integer value.
type Decimal struct {
	Width uint8
	Scale uint8
	Value *big.Int
}

// Float64 converts the Decimal to a float64.
// Note: This may lose precision for large decimals.
func (d Decimal) Float64() float64 {
	if d.Value == nil {
		return 0
	}
	scale := big.NewInt(int64(d.Scale))
	factor := new(
		big.Float,
	).SetInt(new(big.Int).Exp(big.NewInt(10), scale, nil))
	value := new(big.Float).SetInt(d.Value)
	value.Quo(value, factor)
	f, _ := value.Float64()

	return f
}

// String returns the decimal as a formatted string.
// Trailing zeros are trimmed for cleaner output.
func (d Decimal) String() string {
	// Get the sign, and return early, if zero.
	if d.Value == nil || d.Value.Sign() == 0 {
		return "0"
	}

	// Remove the sign from the string integer value
	var signStr string
	scaleless := d.Value.String()
	if d.Value.Sign() < 0 {
		signStr = "-"
		scaleless = scaleless[1:]
	}

	// Remove all zeros from the right side
	zeroTrimmed := strings.TrimRightFunc(
		scaleless,
		func(r rune) bool { return r == '0' },
	)
	scale := int(
		d.Scale,
	) - (len(scaleless) - len(zeroTrimmed))

	// If the string is still bigger than the scale factor, output it without a decimal point
	if scale <= 0 {
		return signStr + zeroTrimmed + strings.Repeat(
			"0",
			-1*scale,
		)
	}

	// Pad a number with 0.0's if needed
	if len(zeroTrimmed) <= scale {
		return fmt.Sprintf(
			"%s0.%s%s",
			signStr,
			strings.Repeat(
				"0",
				scale-len(zeroTrimmed),
			),
			zeroTrimmed,
		)
	}

	return signStr + zeroTrimmed[:len(zeroTrimmed)-scale] + "." + zeroTrimmed[len(zeroTrimmed)-scale:]
}

// Union represents a DuckDB UNION type value.
// A union contains a single value along with a tag indicating which union member it represents.
type Union struct {
	Value driver.Value `json:"value"`
	Tag   string       `json:"tag"`
}

// Composite can be used as the Scanner type for any composite types (maps, lists, structs).
// It uses mapstructure to decode the value into the target type T.
type Composite[T any] struct {
	t T
}

// Get returns the underlying value.
func (s Composite[T]) Get() T {
	return s.t
}

// Scan implements the sql.Scanner interface.
// Uses mapstructure to decode the value into the target type T.
// NULL values should be handled by the caller.
func (s *Composite[T]) Scan(v any) error {
	return mapstructure.Decode(v, &s.t)
}

// hugeInt is an internal type representing a 128-bit signed integer.
// Used for HugeInt and UUID conversions.
type hugeInt struct {
	lower uint64
	upper int64
}

// hugeIntToBigInt converts a hugeInt to a *big.Int.
// The value is computed as: upper * 2^64 + lower
func hugeIntToBigInt(h hugeInt) *big.Int {
	i := big.NewInt(h.upper)
	i.Lsh(i, 64)
	i.Add(i, new(big.Int).SetUint64(h.lower))

	return i
}

// bigIntToHugeInt converts a *big.Int to a hugeInt.
// Returns an error if the value is too large to fit in 128 bits.
func bigIntToHugeInt(
	b *big.Int,
) (hugeInt, error) {
	if b == nil {
		return hugeInt{}, fmt.Errorf(
			"cannot convert nil *big.Int to hugeInt",
		)
	}

	// Check if value fits in 128-bit signed range
	// Range: -2^127 to 2^127-1
	minVal := new(
		big.Int,
	).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	maxVal := new(
		big.Int,
	).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))

	if b.Cmp(minVal) < 0 || b.Cmp(maxVal) > 0 {
		return hugeInt{}, fmt.Errorf(
			"big.Int(%s) is too big for HUGEINT",
			b.String(),
		)
	}

	d := big.NewInt(1)
	d.Lsh(d, 64)

	q := new(big.Int)
	r := new(big.Int)
	q.DivMod(b, d, r)

	return hugeInt{
		lower: r.Uint64(),
		upper: q.Int64(),
	}, nil
}

// inferInterval converts a value to an internal Interval representation.
func inferInterval(val any) (Interval, error) {
	switch v := val.(type) {
	case Interval:
		return v, nil
	default:
		return Interval{}, fmt.Errorf("cannot cast %T to Interval", val)
	}
}

// inferTimestamp converts a time.Time value to microseconds since epoch.
func inferTimestamp(val any) (int64, error) {
	ti, err := castToTime(val)
	if err != nil {
		return 0, err
	}

	return ti.UnixMicro(), nil
}

// inferDate converts a time.Time value to days since epoch.
func inferDate(val any) (int32, error) {
	ti, err := castToTime(val)
	if err != nil {
		return 0, err
	}

	return int32(ti.Unix() / secondsPerDay), nil
}

// inferTime converts a time.Time value to microseconds since midnight.
func inferTime(val any) (int64, error) {
	ti, err := castToTime(val)
	if err != nil {
		return 0, err
	}

	// DuckDB stores time as microseconds since 00:00:00.
	base := time.Date(
		1970,
		time.January,
		1,
		ti.Hour(),
		ti.Minute(),
		ti.Second(),
		ti.Nanosecond(),
		time.UTC,
	)

	return base.UnixMicro(), nil
}

// castToTime attempts to convert any value to time.Time.
func castToTime(val any) (time.Time, error) {
	switch v := val.(type) {
	case time.Time:
		return v.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("cannot cast %T to time.Time", val)
	}
}

// NULL Handling
//
// All Scan methods require the caller to handle NULL values before calling Scan.
// When a database column contains NULL:
//   - For pointer types (e.g., *UUID), the driver should set the pointer to nil
//   - For value types, the caller should check for nil before calling Scan
//
// Example:
//
//	var u *UUID
//	if value != nil {
//	    u = new(UUID)
//	    err := u.Scan(value)
//	}
//
// This follows the standard database/sql pattern where nullable columns should
// use pointer types or sql.Null* wrapper types.

// Interface assertions to verify that types implement the expected interfaces.
// These are compile-time checks - if a type does not implement an interface,
// the code will not compile.
var (
	// UUID implements both sql.Scanner and driver.Valuer
	_ sql.Scanner   = (*UUID)(nil)
	_ driver.Valuer = (*UUID)(nil)

	// Map implements sql.Scanner only (read-only from query results)
	_ sql.Scanner = (*Map)(nil)

	// Composite implements sql.Scanner only
	// Note: Cannot assert generic type directly, but Scan method exists
)
