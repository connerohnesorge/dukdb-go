package dukdb

import (
	"math/big"
	"testing"
	"time"

	"github.com/coder/quartz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUhugeint(t *testing.T) {
	t.Run("NewUhugeint zero", func(t *testing.T) {
		u, err := NewUhugeint(big.NewInt(0))
		require.NoError(t, err)
		assert.True(t, u.IsZero())
		assert.Equal(t, "0", u.String())
	})

	t.Run(
		"NewUhugeint simple value",
		func(t *testing.T) {
			u, err := NewUhugeint(
				big.NewInt(12345),
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				uint64(12345),
				u.Lower(),
			)
			assert.Equal(t, uint64(0), u.Upper())
			assert.Equal(t, "12345", u.String())
		},
	)

	t.Run(
		"NewUhugeint max uint64",
		func(t *testing.T) {
			bi := new(
				big.Int,
			).SetUint64(^uint64(0))
			u, err := NewUhugeint(bi)
			require.NoError(t, err)
			assert.Equal(t, ^uint64(0), u.Lower())
			assert.Equal(t, uint64(0), u.Upper())
		},
	)

	t.Run(
		"NewUhugeint large value",
		func(t *testing.T) {
			// Value larger than uint64 max.
			bi, _ := new(
				big.Int,
			).SetString("18446744073709551616", 10)
			// 2^64
			u, err := NewUhugeint(bi)
			require.NoError(t, err)
			assert.Equal(t, uint64(0), u.Lower())
			assert.Equal(t, uint64(1), u.Upper())
		},
	)

	t.Run(
		"NewUhugeint max value",
		func(t *testing.T) {
			bi, _ := new(
				big.Int,
			).SetString(UhugeintMaxString, 10)
			u, err := NewUhugeint(bi)
			require.NoError(t, err)
			assert.Equal(t, ^uint64(0), u.Lower())
			assert.Equal(t, ^uint64(0), u.Upper())
		},
	)

	t.Run(
		"NewUhugeint overflow",
		func(t *testing.T) {
			// Value exceeding 128 bits.
			bi, _ := new(
				big.Int,
			).SetString("340282366920938463463374607431768211456", 10)
			// Max + 1
			_, err := NewUhugeint(bi)
			assert.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"exceeds UHUGEINT maximum",
			)
		},
	)

	t.Run(
		"NewUhugeint negative",
		func(t *testing.T) {
			_, err := NewUhugeint(big.NewInt(-1))
			assert.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"cannot be negative",
			)
		},
	)

	t.Run(
		"ToBigInt roundtrip",
		func(t *testing.T) {
			original, _ := new(
				big.Int,
			).SetString("123456789012345678901234567890", 10)
			u, err := NewUhugeint(original)
			require.NoError(t, err)
			result := u.ToBigInt()
			assert.Equal(
				t,
				0,
				original.Cmp(result),
			)
		},
	)

	t.Run(
		"NewUhugeintFromParts",
		func(t *testing.T) {
			u := NewUhugeintFromParts(1, 2)
			assert.Equal(t, uint64(2), u.Lower())
			assert.Equal(t, uint64(1), u.Upper())
		},
	)

	t.Run(
		"NewUhugeintFromUint64",
		func(t *testing.T) {
			u := NewUhugeintFromUint64(42)
			assert.Equal(t, uint64(42), u.Lower())
			assert.Equal(t, uint64(0), u.Upper())
		},
	)

	t.Run("Add", func(t *testing.T) {
		a := NewUhugeintFromUint64(100)
		b := NewUhugeintFromUint64(50)
		result, err := a.Add(b)
		require.NoError(t, err)
		assert.Equal(t, "150", result.String())
	})

	t.Run("Add overflow", func(t *testing.T) {
		maxVal, _ := new(
			big.Int,
		).SetString(UhugeintMaxString, 10)
		a, _ := NewUhugeint(maxVal)
		b := NewUhugeintFromUint64(1)
		_, err := a.Add(b)
		assert.Error(t, err)
	})

	t.Run("Sub", func(t *testing.T) {
		a := NewUhugeintFromUint64(100)
		b := NewUhugeintFromUint64(50)
		result, err := a.Sub(b)
		require.NoError(t, err)
		assert.Equal(t, "50", result.String())
	})

	t.Run("Sub underflow", func(t *testing.T) {
		a := NewUhugeintFromUint64(50)
		b := NewUhugeintFromUint64(100)
		_, err := a.Sub(b)
		assert.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"negative value",
		)
	})

	t.Run("Mul", func(t *testing.T) {
		a := NewUhugeintFromUint64(100)
		b := NewUhugeintFromUint64(5)
		result, err := a.Mul(b)
		require.NoError(t, err)
		assert.Equal(t, "500", result.String())
	})

	t.Run("Div", func(t *testing.T) {
		a := NewUhugeintFromUint64(100)
		b := NewUhugeintFromUint64(5)
		result, err := a.Div(b)
		require.NoError(t, err)
		assert.Equal(t, "20", result.String())
	})

	t.Run("Div by zero", func(t *testing.T) {
		a := NewUhugeintFromUint64(100)
		b := Uhugeint{}
		_, err := a.Div(b)
		assert.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"division by zero",
		)
	})

	t.Run("Cmp", func(t *testing.T) {
		a := NewUhugeintFromUint64(100)
		b := NewUhugeintFromUint64(50)
		c := NewUhugeintFromUint64(100)

		assert.Equal(t, 1, a.Cmp(b))
		assert.Equal(t, -1, b.Cmp(a))
		assert.Equal(t, 0, a.Cmp(c))
	})

	t.Run("Equal", func(t *testing.T) {
		a := NewUhugeintFromUint64(100)
		b := NewUhugeintFromUint64(100)
		c := NewUhugeintFromUint64(50)

		assert.True(t, a.Equal(b))
		assert.False(t, a.Equal(c))
	})

	t.Run("Scan string", func(t *testing.T) {
		var u Uhugeint
		err := u.Scan(
			"123456789012345678901234567890",
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			"123456789012345678901234567890",
			u.String(),
		)
	})

	t.Run("Scan int64", func(t *testing.T) {
		var u Uhugeint
		err := u.Scan(int64(42))
		require.NoError(t, err)
		assert.Equal(t, "42", u.String())
	})

	t.Run("Scan nil", func(t *testing.T) {
		var u Uhugeint
		err := u.Scan(nil)
		require.NoError(t, err)
		assert.True(t, u.IsZero())
	})

	t.Run("Value", func(t *testing.T) {
		u := NewUhugeintFromUint64(42)
		v, err := u.Value()
		require.NoError(t, err)
		assert.Equal(t, "42", v)
	})
}

func TestBit(t *testing.T) {
	t.Run("NewBit empty", func(t *testing.T) {
		b, err := NewBit("")
		require.NoError(t, err)
		assert.Equal(t, 0, b.Len())
		assert.Equal(t, "", b.String())
	})

	t.Run("NewBit valid", func(t *testing.T) {
		b, err := NewBit("10110")
		require.NoError(t, err)
		assert.Equal(t, 5, b.Len())
		assert.Equal(t, "10110", b.String())
	})

	t.Run("NewBit all zeros", func(t *testing.T) {
		b, err := NewBit("00000000")
		require.NoError(t, err)
		assert.Equal(t, 8, b.Len())
		assert.Equal(t, "00000000", b.String())
	})

	t.Run("NewBit all ones", func(t *testing.T) {
		b, err := NewBit("11111111")
		require.NoError(t, err)
		assert.Equal(t, 8, b.Len())
		assert.Equal(t, "11111111", b.String())
	})

	t.Run(
		"NewBit invalid character",
		func(t *testing.T) {
			_, err := NewBit("1012")
			assert.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"invalid bit character",
			)
		},
	)

	t.Run("NewBitFromBytes", func(t *testing.T) {
		b := NewBitFromBytes(
			[]byte{0b10110000},
			5,
		)
		assert.Equal(t, 5, b.Len())
		assert.Equal(t, "10110", b.String())
	})

	t.Run("Get", func(t *testing.T) {
		b, _ := NewBit("10110")

		val, err := b.Get(0)
		require.NoError(t, err)
		assert.True(t, val)

		val, err = b.Get(1)
		require.NoError(t, err)
		assert.False(t, val)

		val, err = b.Get(4)
		require.NoError(t, err)
		assert.False(t, val)
	})

	t.Run("Get out of range", func(t *testing.T) {
		b, _ := NewBit("10110")
		_, err := b.Get(5)
		assert.Error(t, err)
		_, err = b.Get(-1)
		assert.Error(t, err)
	})

	t.Run("Set", func(t *testing.T) {
		b, _ := NewBit("00000")

		err := b.Set(0, true)
		require.NoError(t, err)
		assert.Equal(t, "10000", b.String())

		err = b.Set(4, true)
		require.NoError(t, err)
		assert.Equal(t, "10001", b.String())

		err = b.Set(0, false)
		require.NoError(t, err)
		assert.Equal(t, "00001", b.String())
	})

	t.Run("Set out of range", func(t *testing.T) {
		b, _ := NewBit("10110")
		err := b.Set(5, true)
		assert.Error(t, err)
	})

	t.Run("Bytes", func(t *testing.T) {
		b, _ := NewBit("10110000")
		bytes := b.Bytes()
		assert.Equal(t, []byte{0b10110000}, bytes)

		// Verify it's a copy.
		bytes[0] = 0
		assert.Equal(t, "10110000", b.String())
	})

	t.Run("And", func(t *testing.T) {
		a, _ := NewBit("10110")
		b, _ := NewBit("11010")
		result, err := a.And(b)
		require.NoError(t, err)
		assert.Equal(t, "10010", result.String())
	})

	t.Run(
		"And length mismatch",
		func(t *testing.T) {
			a, _ := NewBit("10110")
			b, _ := NewBit("1101")
			_, err := a.And(b)
			assert.Error(t, err)
		},
	)

	t.Run("Or", func(t *testing.T) {
		a, _ := NewBit("10110")
		b, _ := NewBit("11010")
		result, err := a.Or(b)
		require.NoError(t, err)
		assert.Equal(t, "11110", result.String())
	})

	t.Run("Xor", func(t *testing.T) {
		a, _ := NewBit("10110")
		b, _ := NewBit("11010")
		result, err := a.Xor(b)
		require.NoError(t, err)
		assert.Equal(t, "01100", result.String())
	})

	t.Run("Not", func(t *testing.T) {
		b, _ := NewBit("10110")
		result := b.Not()
		assert.Equal(t, "01001", result.String())
	})

	t.Run("Scan string", func(t *testing.T) {
		var b Bit
		err := b.Scan("10110")
		require.NoError(t, err)
		assert.Equal(t, "10110", b.String())
	})

	t.Run("Scan nil", func(t *testing.T) {
		var b Bit
		err := b.Scan(nil)
		require.NoError(t, err)
		assert.Equal(t, 0, b.Len())
	})

	t.Run("Value", func(t *testing.T) {
		b, _ := NewBit("10110")
		v, err := b.Value()
		require.NoError(t, err)
		assert.Equal(t, "10110", v)
	})
}

func TestTimeNS(t *testing.T) {
	t.Run("NewTimeNS", func(t *testing.T) {
		tns := NewTimeNS(14, 30, 45, 123456789)
		h, m, s, ns := tns.Components()
		assert.Equal(t, 14, h)
		assert.Equal(t, 30, m)
		assert.Equal(t, 45, s)
		assert.Equal(t, int64(123456789), ns)
	})

	t.Run("NewTimeNS zero", func(t *testing.T) {
		tns := NewTimeNS(0, 0, 0, 0)
		h, m, s, ns := tns.Components()
		assert.Equal(t, 0, h)
		assert.Equal(t, 0, m)
		assert.Equal(t, 0, s)
		assert.Equal(t, int64(0), ns)
	})

	t.Run("NewTimeNS max", func(t *testing.T) {
		tns := NewTimeNS(23, 59, 59, 999999999)
		h, m, s, ns := tns.Components()
		assert.Equal(t, 23, h)
		assert.Equal(t, 59, m)
		assert.Equal(t, 59, s)
		assert.Equal(t, int64(999999999), ns)
	})

	t.Run(
		"String without nanos",
		func(t *testing.T) {
			tns := NewTimeNS(14, 30, 45, 0)
			assert.Equal(
				t,
				"14:30:45",
				tns.String(),
			)
		},
	)

	t.Run(
		"String with nanos",
		func(t *testing.T) {
			tns := NewTimeNS(
				14,
				30,
				45,
				123456789,
			)
			assert.Equal(
				t,
				"14:30:45.123456789",
				tns.String(),
			)
		},
	)

	t.Run(
		"String with leading zero nanos",
		func(t *testing.T) {
			tns := NewTimeNS(14, 30, 45, 1)
			assert.Equal(
				t,
				"14:30:45.000000001",
				tns.String(),
			)
		},
	)

	t.Run("ToTime", func(t *testing.T) {
		tns := NewTimeNS(14, 30, 45, 123456789)
		tm := tns.ToTime()
		assert.Equal(t, 14, tm.Hour())
		assert.Equal(t, 30, tm.Minute())
		assert.Equal(t, 45, tm.Second())
		assert.Equal(
			t,
			123456789,
			tm.Nanosecond(),
		)
	})

	t.Run("TimeNSFromTime", func(t *testing.T) {
		tm := time.Date(
			2024,
			1,
			15,
			14,
			30,
			45,
			123456789,
			time.UTC,
		)
		tns := TimeNSFromTime(tm)
		h, m, s, ns := tns.Components()
		assert.Equal(t, 14, h)
		assert.Equal(t, 30, m)
		assert.Equal(t, 45, s)
		assert.Equal(t, int64(123456789), ns)
	})

	t.Run("Scan int64", func(t *testing.T) {
		var tns TimeNS
		// 14:30:45.123456789 in nanoseconds since midnight.
		nanos := int64(
			14,
		)*nanosPerHour + int64(
			30,
		)*nanosPerMinute + int64(
			45,
		)*nanosPerSecond + 123456789
		err := tns.Scan(nanos)
		require.NoError(t, err)
		h, m, s, ns := tns.Components()
		assert.Equal(t, 14, h)
		assert.Equal(t, 30, m)
		assert.Equal(t, 45, s)
		assert.Equal(t, int64(123456789), ns)
	})

	t.Run("Scan string", func(t *testing.T) {
		var tns TimeNS
		err := tns.Scan("14:30:45.123456789")
		require.NoError(t, err)
		h, m, s, ns := tns.Components()
		assert.Equal(t, 14, h)
		assert.Equal(t, 30, m)
		assert.Equal(t, 45, s)
		assert.Equal(t, int64(123456789), ns)
	})

	t.Run(
		"Scan string no nanos",
		func(t *testing.T) {
			var tns TimeNS
			err := tns.Scan("14:30:45")
			require.NoError(t, err)
			h, m, s, ns := tns.Components()
			assert.Equal(t, 14, h)
			assert.Equal(t, 30, m)
			assert.Equal(t, 45, s)
			assert.Equal(t, int64(0), ns)
		},
	)

	t.Run(
		"Scan string partial nanos",
		func(t *testing.T) {
			var tns TimeNS
			err := tns.Scan("14:30:45.123")
			require.NoError(t, err)
			h, m, s, ns := tns.Components()
			assert.Equal(t, 14, h)
			assert.Equal(t, 30, m)
			assert.Equal(t, 45, s)
			assert.Equal(t, int64(123000000), ns)
		},
	)

	t.Run("Scan time.Time", func(t *testing.T) {
		var tns TimeNS
		tm := time.Date(
			2024,
			1,
			15,
			14,
			30,
			45,
			123456789,
			time.UTC,
		)
		err := tns.Scan(tm)
		require.NoError(t, err)
		h, m, s, ns := tns.Components()
		assert.Equal(t, 14, h)
		assert.Equal(t, 30, m)
		assert.Equal(t, 45, s)
		assert.Equal(t, int64(123456789), ns)
	})

	t.Run("Scan nil", func(t *testing.T) {
		var tns TimeNS
		err := tns.Scan(nil)
		require.NoError(t, err)
		assert.Equal(t, TimeNS(0), tns)
	})

	t.Run("Value", func(t *testing.T) {
		tns := NewTimeNS(14, 30, 45, 123456789)
		v, err := tns.Value()
		require.NoError(t, err)
		assert.Equal(t, "14:30:45.123456789", v)
	})
}

func TestTimeNS_EdgeCases(t *testing.T) {
	t.Run("midnight", func(t *testing.T) {
		tns := NewTimeNS(0, 0, 0, 0)
		assert.Equal(t, "00:00:00", tns.String())
	})

	t.Run(
		"one nanosecond before midnight",
		func(t *testing.T) {
			tns := NewTimeNS(
				23,
				59,
				59,
				999999999,
			)
			h, m, s, ns := tns.Components()
			assert.Equal(t, 23, h)
			assert.Equal(t, 59, m)
			assert.Equal(t, 59, s)
			assert.Equal(t, int64(999999999), ns)
		},
	)

	t.Run(
		"nanosecond precision roundtrip",
		func(t *testing.T) {
			for _, ns := range []int64{1, 123, 123456, 123456789, 999999999} {
				tns := NewTimeNS(12, 30, 45, ns)
				_, _, _, gotNS := tns.Components()
				assert.Equal(
					t,
					ns,
					gotNS,
					"nanosecond precision lost for %d",
					ns,
				)
			}
		},
	)
}

func TestCurrentTimeNS(t *testing.T) {
	t.Run("basic mock", func(t *testing.T) {
		mockClock := quartz.NewMock(t)

		// Set a specific time.
		fixedTime := time.Date(
			2024,
			6,
			15,
			14,
			30,
			45,
			123456789,
			time.UTC,
		)
		mockClock.Set(fixedTime)

		tns := CurrentTimeNS(mockClock)
		h, m, s, ns := tns.Components()

		assert.Equal(t, 14, h)
		assert.Equal(t, 30, m)
		assert.Equal(t, 45, s)
		assert.Equal(t, int64(123456789), ns)
	})

	t.Run("midnight", func(t *testing.T) {
		mockClock := quartz.NewMock(t)

		fixedTime := time.Date(
			2024,
			6,
			15,
			0,
			0,
			0,
			0,
			time.UTC,
		)
		mockClock.Set(fixedTime)

		tns := CurrentTimeNS(mockClock)
		assert.Equal(t, "00:00:00", tns.String())
	})

	t.Run(
		"nanosecond precision",
		func(t *testing.T) {
			mockClock := quartz.NewMock(t)

			// Test various nanosecond values.
			testCases := []struct {
				ns       int
				expected int64
			}{
				{1, 1},
				{123, 123},
				{123456789, 123456789},
				{999999999, 999999999},
			}

			for _, tc := range testCases {
				fixedTime := time.Date(
					2024,
					6,
					15,
					12,
					0,
					0,
					tc.ns,
					time.UTC,
				)
				mockClock.Set(fixedTime)

				tns := CurrentTimeNS(mockClock)
				_, _, _, gotNS := tns.Components()
				assert.Equal(
					t,
					tc.expected,
					gotNS,
				)
			}
		},
	)

	t.Run(
		"deterministic behavior",
		func(t *testing.T) {
			mockClock := quartz.NewMock(t)

			// Same time should give same result.
			fixedTime := time.Date(
				2024,
				6,
				15,
				14,
				30,
				45,
				123456789,
				time.UTC,
			)
			mockClock.Set(fixedTime)

			tns1 := CurrentTimeNS(mockClock)
			tns2 := CurrentTimeNS(mockClock)

			assert.Equal(t, tns1, tns2)
			assert.Equal(
				t,
				"14:30:45.123456789",
				tns1.String(),
			)
		},
	)
}
