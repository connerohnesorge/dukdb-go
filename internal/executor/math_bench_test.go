// Package executor provides performance benchmarks for math function implementations.
//
// Performance Targets:
// These benchmarks are designed to ensure that dukdb-go math functions perform
// within an acceptable range compared to DuckDB's native C++ implementation.
// Target: Within 2x of DuckDB performance for most operations.
//
// DuckDB uses optimized SIMD instructions and C++ standard library math functions,
// which are typically faster than Go's pure-Go implementations. However, Go's math
// package uses assembly implementations for many functions, so the performance
// gap should be minimal for basic operations.
//
// Optimization Opportunities (Task 14.7):
// 1. Type conversion overhead: The toFloat64/toInt64 helper functions use type
//    switches which have some overhead. For hot paths, direct type assertions
//    could be faster.
// 2. NULL checking: Every function checks for NULL which adds overhead. For
//    bulk operations where NULL is guaranteed not to be present, specialized
//    non-NULL paths could be added.
// 3. Memory allocations: Some functions allocate new values; using sync.Pool
//    or pre-allocated buffers could reduce GC pressure.
// 4. Vectorized operations: For bulk operations on columns, SIMD operations
//    via packages like gonum could provide significant speedups.
package executor

import (
	"math"
	"testing"
)

// =============================================================================
// Task 14.1: Benchmark Rounding Functions (ROUND, CEIL, FLOOR, TRUNC)
// =============================================================================

// BenchmarkRounding groups all rounding function benchmarks.
func BenchmarkRounding(b *testing.B) {
	b.Run("ROUND_Simple", BenchmarkRound)
	b.Run("ROUND_WithDecimals", BenchmarkRoundWithDecimals)
	b.Run("CEIL", BenchmarkCeil)
	b.Run("FLOOR", BenchmarkFloor)
	b.Run("TRUNC", BenchmarkTrunc)
	b.Run("ROUND_EVEN", BenchmarkRoundEven)
	b.Run("EVEN", BenchmarkEven)
}

// BenchmarkRound benchmarks the ROUND function with default precision.
func BenchmarkRound(b *testing.B) {
	testValue := 3.14159265358979
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = roundValue(testValue, int64(0))
	}
}

// BenchmarkRoundWithDecimals benchmarks ROUND with decimal precision.
func BenchmarkRoundWithDecimals(b *testing.B) {
	testValue := 3.14159265358979
	decimals := int64(2)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = roundValue(testValue, decimals)
	}
}

// BenchmarkCeil benchmarks the CEIL function.
func BenchmarkCeil(b *testing.B) {
	testValue := 3.14159265358979
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ceilValue(testValue)
	}
}

// BenchmarkFloor benchmarks the FLOOR function.
func BenchmarkFloor(b *testing.B) {
	testValue := 3.14159265358979
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = floorValue(testValue)
	}
}

// BenchmarkTrunc benchmarks the TRUNC function.
func BenchmarkTrunc(b *testing.B) {
	testValue := 3.14159265358979
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = truncValue(testValue)
	}
}

// BenchmarkRoundEven benchmarks banker's rounding (ROUND_EVEN).
func BenchmarkRoundEven(b *testing.B) {
	testValue := 2.5
	decimals := int64(0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = roundEvenValue(testValue, decimals)
	}
}

// BenchmarkEven benchmarks the EVEN function.
func BenchmarkEven(b *testing.B) {
	testValue := 3.5
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = evenValue(testValue)
	}
}

// BenchmarkRounding_Batch benchmarks rounding on a batch of values to simulate
// real-world columnar operations.
func BenchmarkRounding_Batch(b *testing.B) {
	const batchSize = 1000
	values := make([]float64, batchSize)
	for i := range values {
		values[i] = float64(i) * 0.123456789
	}

	b.Run("ROUND_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				_, _ = roundValue(v, int64(2))
			}
		}
	})

	b.Run("CEIL_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				_, _ = ceilValue(v)
			}
		}
	})

	b.Run("FLOOR_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				_, _ = floorValue(v)
			}
		}
	})
}

// =============================================================================
// Task 14.2: Benchmark Scientific Functions (SQRT, POW, EXP, LOG)
// =============================================================================

// BenchmarkScientific groups all scientific function benchmarks.
func BenchmarkScientific(b *testing.B) {
	b.Run("SQRT", BenchmarkSqrt)
	b.Run("CBRT", BenchmarkCbrt)
	b.Run("POW", BenchmarkPow)
	b.Run("EXP", BenchmarkExp)
	b.Run("LN", BenchmarkLn)
	b.Run("LOG10", BenchmarkLog10)
	b.Run("LOG2", BenchmarkLog2)
	b.Run("GAMMA", BenchmarkGamma)
	b.Run("LGAMMA", BenchmarkLgamma)
	b.Run("FACTORIAL", BenchmarkFactorial)
}

// BenchmarkSqrt benchmarks the SQRT function.
func BenchmarkSqrt(b *testing.B) {
	testValue := 123.456
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sqrtValue(testValue)
	}
}

// BenchmarkCbrt benchmarks the CBRT function.
func BenchmarkCbrt(b *testing.B) {
	testValue := 123.456
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cbrtValue(testValue)
	}
}

// BenchmarkPow benchmarks the POW function.
func BenchmarkPow(b *testing.B) {
	base := 2.0
	exponent := 10.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = powValue(base, exponent)
	}
}

// BenchmarkExp benchmarks the EXP function.
func BenchmarkExp(b *testing.B) {
	testValue := 5.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expValue(testValue)
	}
}

// BenchmarkLn benchmarks the LN (natural logarithm) function.
func BenchmarkLn(b *testing.B) {
	testValue := 123.456
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = lnValue(testValue)
	}
}

// BenchmarkLog10 benchmarks the LOG10 function.
func BenchmarkLog10(b *testing.B) {
	testValue := 123.456
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = log10Value(testValue)
	}
}

// BenchmarkLog2 benchmarks the LOG2 function.
func BenchmarkLog2(b *testing.B) {
	testValue := 123.456
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = log2Value(testValue)
	}
}

// BenchmarkGamma benchmarks the GAMMA function.
func BenchmarkGamma(b *testing.B) {
	testValue := 5.5
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gammaValue(testValue)
	}
}

// BenchmarkLgamma benchmarks the LGAMMA function.
func BenchmarkLgamma(b *testing.B) {
	testValue := 5.5
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = lgammaValue(testValue)
	}
}

// BenchmarkFactorial benchmarks the FACTORIAL function.
func BenchmarkFactorial(b *testing.B) {
	testValue := int64(15)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = factorialValue(testValue)
	}
}

// BenchmarkScientific_Batch benchmarks scientific functions on batches of values.
func BenchmarkScientific_Batch(b *testing.B) {
	const batchSize = 1000
	values := make([]float64, batchSize)
	for i := range values {
		// Use positive values only for functions with domain restrictions
		values[i] = float64(i+1) * 0.1
	}

	b.Run("SQRT_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				_, _ = sqrtValue(v)
			}
		}
	})

	b.Run("POW_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				_, _ = powValue(v, 2.0)
			}
		}
	})

	b.Run("EXP_Batch", func(b *testing.B) {
		// Cap values to avoid overflow
		expValues := make([]float64, batchSize)
		for i := range expValues {
			expValues[i] = float64(i%10) * 0.5
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range expValues {
				_, _ = expValue(v)
			}
		}
	})

	b.Run("LN_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				_, _ = lnValue(v)
			}
		}
	})
}

// =============================================================================
// Task 14.3: Benchmark Trigonometric Functions (SIN, COS, TAN)
// =============================================================================

// BenchmarkTrigonometric groups all trigonometric function benchmarks.
func BenchmarkTrigonometric(b *testing.B) {
	b.Run("SIN", BenchmarkSin)
	b.Run("COS", BenchmarkCos)
	b.Run("TAN", BenchmarkTan)
	b.Run("COT", BenchmarkCot)
	b.Run("ASIN", BenchmarkAsin)
	b.Run("ACOS", BenchmarkAcos)
	b.Run("ATAN", BenchmarkAtan)
	b.Run("ATAN2", BenchmarkAtan2)
	b.Run("DEGREES", BenchmarkDegrees)
	b.Run("RADIANS", BenchmarkRadians)
}

// BenchmarkSin benchmarks the SIN function.
func BenchmarkSin(b *testing.B) {
	testValue := math.Pi / 4
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sinValue(testValue)
	}
}

// BenchmarkCos benchmarks the COS function.
func BenchmarkCos(b *testing.B) {
	testValue := math.Pi / 4
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cosValue(testValue)
	}
}

// BenchmarkTan benchmarks the TAN function.
func BenchmarkTan(b *testing.B) {
	testValue := math.Pi / 4
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tanValue(testValue)
	}
}

// BenchmarkCot benchmarks the COT function.
func BenchmarkCot(b *testing.B) {
	testValue := math.Pi / 4
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cotValue(testValue)
	}
}

// BenchmarkAsin benchmarks the ASIN function.
func BenchmarkAsin(b *testing.B) {
	testValue := 0.5 // Within domain [-1, 1]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = asinValue(testValue)
	}
}

// BenchmarkAcos benchmarks the ACOS function.
func BenchmarkAcos(b *testing.B) {
	testValue := 0.5 // Within domain [-1, 1]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = acosValue(testValue)
	}
}

// BenchmarkAtan benchmarks the ATAN function.
func BenchmarkAtan(b *testing.B) {
	testValue := 1.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = atanValue(testValue)
	}
}

// BenchmarkAtan2 benchmarks the ATAN2 function.
func BenchmarkAtan2(b *testing.B) {
	y := 1.0
	x := 1.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = atan2Value(y, x)
	}
}

// BenchmarkDegrees benchmarks the DEGREES function.
func BenchmarkDegrees(b *testing.B) {
	testValue := math.Pi
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = degreesValue(testValue)
	}
}

// BenchmarkRadians benchmarks the RADIANS function.
func BenchmarkRadians(b *testing.B) {
	testValue := 180.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = radiansValue(testValue)
	}
}

// BenchmarkTrigonometric_Batch benchmarks trigonometric functions on batches.
func BenchmarkTrigonometric_Batch(b *testing.B) {
	const batchSize = 1000
	angles := make([]float64, batchSize)
	for i := range angles {
		// Generate angles from 0 to 2*Pi
		angles[i] = float64(i) * (2 * math.Pi / float64(batchSize))
	}

	b.Run("SIN_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range angles {
				_, _ = sinValue(v)
			}
		}
	})

	b.Run("COS_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range angles {
				_, _ = cosValue(v)
			}
		}
	})

	b.Run("TAN_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range angles {
				_, _ = tanValue(v)
			}
		}
	})
}

// =============================================================================
// Task 14.3 (continued): Benchmark Hyperbolic Functions
// =============================================================================

// BenchmarkHyperbolic groups all hyperbolic function benchmarks.
func BenchmarkHyperbolic(b *testing.B) {
	b.Run("SINH", BenchmarkSinh)
	b.Run("COSH", BenchmarkCosh)
	b.Run("TANH", BenchmarkTanh)
	b.Run("ASINH", BenchmarkAsinh)
	b.Run("ACOSH", BenchmarkAcosh)
	b.Run("ATANH", BenchmarkAtanh)
}

// BenchmarkSinh benchmarks the SINH function.
func BenchmarkSinh(b *testing.B) {
	testValue := 1.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sinhValue(testValue)
	}
}

// BenchmarkCosh benchmarks the COSH function.
func BenchmarkCosh(b *testing.B) {
	testValue := 1.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = coshValue(testValue)
	}
}

// BenchmarkTanh benchmarks the TANH function.
func BenchmarkTanh(b *testing.B) {
	testValue := 1.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tanhValue(testValue)
	}
}

// BenchmarkAsinh benchmarks the ASINH function.
func BenchmarkAsinh(b *testing.B) {
	testValue := 1.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = asinhValue(testValue)
	}
}

// BenchmarkAcosh benchmarks the ACOSH function.
func BenchmarkAcosh(b *testing.B) {
	testValue := 2.0 // Must be >= 1 for domain
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = acoshValue(testValue)
	}
}

// BenchmarkAtanh benchmarks the ATANH function.
func BenchmarkAtanh(b *testing.B) {
	testValue := 0.5 // Must be in (-1, 1) for domain
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = atanhValue(testValue)
	}
}

// =============================================================================
// Task 14.4: Benchmark Bitwise Operators
// =============================================================================

// BenchmarkBitwise groups all bitwise operator benchmarks.
func BenchmarkBitwise(b *testing.B) {
	b.Run("AND", BenchmarkBitwiseAnd)
	b.Run("OR", BenchmarkBitwiseOr)
	b.Run("XOR", BenchmarkBitwiseXor)
	b.Run("NOT", BenchmarkBitwiseNot)
	b.Run("SHIFT_LEFT", BenchmarkBitwiseShiftLeft)
	b.Run("SHIFT_RIGHT", BenchmarkBitwiseShiftRight)
	b.Run("BIT_COUNT", BenchmarkBitCount)
}

// BenchmarkBitwiseAnd benchmarks the bitwise AND operator.
func BenchmarkBitwiseAnd(b *testing.B) {
	left := int64(0xDEADBEEF)
	right := int64(0x0F0F0F0F)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bitwiseAndValue(left, right)
	}
}

// BenchmarkBitwiseOr benchmarks the bitwise OR operator.
func BenchmarkBitwiseOr(b *testing.B) {
	left := int64(0xDEAD0000)
	right := int64(0x0000BEEF)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bitwiseOrValue(left, right)
	}
}

// BenchmarkBitwiseXor benchmarks the bitwise XOR operator.
func BenchmarkBitwiseXor(b *testing.B) {
	left := int64(0xAAAAAAAA)
	right := int64(0x55555555)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bitwiseXorValue(left, right)
	}
}

// BenchmarkBitwiseNot benchmarks the bitwise NOT operator.
func BenchmarkBitwiseNot(b *testing.B) {
	testValue := int64(0xDEADBEEF)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bitwiseNotValue(testValue)
	}
}

// BenchmarkBitwiseShiftLeft benchmarks the left shift operator.
func BenchmarkBitwiseShiftLeft(b *testing.B) {
	left := int64(1)
	right := uint64(10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bitwiseShiftLeftValue(left, right)
	}
}

// BenchmarkBitwiseShiftRight benchmarks the right shift operator.
func BenchmarkBitwiseShiftRight(b *testing.B) {
	left := int64(1024)
	right := uint64(5)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bitwiseShiftRightValue(left, right)
	}
}

// BenchmarkBitCount benchmarks the BIT_COUNT function.
func BenchmarkBitCount(b *testing.B) {
	testValue := int64(0xDEADBEEF)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bitCountValue(testValue)
	}
}

// BenchmarkBitwise_Batch benchmarks bitwise operations on batches of values.
func BenchmarkBitwise_Batch(b *testing.B) {
	const batchSize = 1000
	values := make([]int64, batchSize)
	for i := range values {
		values[i] = int64(i * 17) // Some pattern
	}
	mask := int64(0xFF)

	b.Run("AND_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				_, _ = bitwiseAndValue(v, mask)
			}
		}
	})

	b.Run("OR_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				_, _ = bitwiseOrValue(v, mask)
			}
		}
	})

	b.Run("BIT_COUNT_Batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				_, _ = bitCountValue(v)
			}
		}
	})
}

// =============================================================================
// Task 14.5: Profile Memory Usage for Math Operations
// =============================================================================

// BenchmarkMemory_TypeConversion benchmarks the memory overhead of type
// conversion helpers used in math functions.
func BenchmarkMemory_TypeConversion(b *testing.B) {
	b.Run("toFloat64_float64", func(b *testing.B) {
		v := float64(3.14159)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = toFloat64(v)
		}
	})

	b.Run("toFloat64_int64", func(b *testing.B) {
		v := int64(42)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = toFloat64(v)
		}
	})

	b.Run("toFloat64_int", func(b *testing.B) {
		v := 42
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = toFloat64(v)
		}
	})

	b.Run("toInt64_int64", func(b *testing.B) {
		v := int64(42)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = toInt64(v)
		}
	})

	b.Run("toInt64_float64", func(b *testing.B) {
		v := float64(42.7)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = toInt64(v)
		}
	})

	b.Run("toUint64_uint64", func(b *testing.B) {
		v := uint64(42)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = toUint64(v)
		}
	})
}

// BenchmarkMemory_NullChecking benchmarks the overhead of NULL checking.
func BenchmarkMemory_NullChecking(b *testing.B) {
	b.Run("isNull_nil", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = isNull(nil)
		}
	})

	b.Run("isNull_value", func(b *testing.B) {
		v := float64(42)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = isNull(v)
		}
	})

	b.Run("checkNullUnary", func(b *testing.B) {
		v := float64(42)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = checkNullUnary(v)
		}
	})

	b.Run("checkNullBinary", func(b *testing.B) {
		v1 := float64(42)
		v2 := float64(3.14)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = checkNullBinary(v1, v2)
		}
	})
}

// BenchmarkMemory_FunctionOverhead measures the overhead of math function
// wrappers compared to direct math package calls.
func BenchmarkMemory_FunctionOverhead(b *testing.B) {
	testValue := 3.14159

	b.Run("direct_math.Sin", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = math.Sin(testValue)
		}
	})

	b.Run("wrapped_sinValue", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = sinValue(testValue)
		}
	})

	b.Run("direct_math.Sqrt", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = math.Sqrt(testValue)
		}
	})

	b.Run("wrapped_sqrtValue", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = sqrtValue(testValue)
		}
	})

	b.Run("direct_math.Floor", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = math.Floor(testValue)
		}
	})

	b.Run("wrapped_floorValue", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = floorValue(testValue)
		}
	})
}

// =============================================================================
// Task 14.5 (continued): Utility Functions Memory Profile
// =============================================================================

// BenchmarkUtility groups utility function benchmarks.
func BenchmarkUtility(b *testing.B) {
	b.Run("PI", BenchmarkPi)
	b.Run("RANDOM", BenchmarkRandom)
	b.Run("SIGN", BenchmarkSign)
	b.Run("GCD", BenchmarkGcd)
	b.Run("LCM", BenchmarkLcm)
	b.Run("ISNAN", BenchmarkIsnan)
	b.Run("ISINF", BenchmarkIsinf)
	b.Run("ISFINITE", BenchmarkIsfinite)
}

// BenchmarkPi benchmarks the PI function.
func BenchmarkPi(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = piValue()
	}
}

// BenchmarkRandom benchmarks the RANDOM function.
func BenchmarkRandom(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = randomValue()
	}
}

// BenchmarkSign benchmarks the SIGN function.
func BenchmarkSign(b *testing.B) {
	testValue := -42.5
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = signValue(testValue)
	}
}

// BenchmarkGcd benchmarks the GCD function.
func BenchmarkGcd(b *testing.B) {
	a := int64(48)
	bVal := int64(18)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gcdValue(a, bVal)
	}
}

// BenchmarkLcm benchmarks the LCM function.
func BenchmarkLcm(b *testing.B) {
	a := int64(48)
	bVal := int64(18)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = lcmValue(a, bVal)
	}
}

// BenchmarkIsnan benchmarks the ISNAN function.
func BenchmarkIsnan(b *testing.B) {
	testValue := 42.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = isnanValue(testValue)
	}
}

// BenchmarkIsinf benchmarks the ISINF function.
func BenchmarkIsinf(b *testing.B) {
	testValue := 42.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = isinfValue(testValue)
	}
}

// BenchmarkIsfinite benchmarks the ISFINITE function.
func BenchmarkIsfinite(b *testing.B) {
	testValue := 42.0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = isfiniteValue(testValue)
	}
}

// =============================================================================
// Task 14.6: Performance Comparison Notes (DuckDB Target: Within 2x)
// =============================================================================

// NOTE: Direct comparison with DuckDB requires running DuckDB benchmarks
// separately and comparing results. DuckDB uses C++ with SIMD optimizations.
//
// Expected performance characteristics:
//
// 1. Simple operations (ADD, SUB, MUL, DIV, bitwise):
//    - Go should be very close to DuckDB (within 1.2x) since these compile
//      to single CPU instructions.
//
// 2. Math library functions (SIN, COS, SQRT, EXP, LOG):
//    - Go's math package uses assembly implementations for common functions.
//    - Expected: 1.5-2x slower than DuckDB's C++ implementations.
//
// 3. Complex functions (GAMMA, LGAMMA, FACTORIAL):
//    - These require iterative or series computations.
//    - Expected: 1.5-2x slower than DuckDB.
//
// 4. Batch operations:
//    - DuckDB benefits from vectorized execution with SIMD.
//    - Without SIMD, Go batch operations are 2-4x slower.
//    - Using packages like gonum can close this gap.
//
// Performance tuning recommendations (Task 14.7):
// 1. Use sync.Pool for temporary allocations in batch operations
// 2. Consider specialized code paths for common types (int64, float64)
// 3. Investigate SIMD via gonum or avo for vectorized operations
// 4. Profile with pprof to identify hot spots

// BenchmarkComparison_SimpleVsComplex compares simple vs complex math operations.
func BenchmarkComparison_SimpleVsComplex(b *testing.B) {
	testValue := 2.0

	b.Run("Simple_Addition", func(b *testing.B) {
		a := 1.0
		c := 0.0
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c = a + testValue
		}
		_ = c // Prevent optimization
	})

	b.Run("Complex_Sqrt", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = sqrtValue(testValue)
		}
	})

	b.Run("Complex_Sin", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = sinValue(testValue)
		}
	})

	b.Run("Complex_Gamma", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = gammaValue(testValue)
		}
	})
}

// =============================================================================
// Task 14.7: Identified Optimization Opportunities
// =============================================================================

// Optimization Analysis (documented through benchmark results):
//
// 1. TYPE SWITCH OVERHEAD
//    The toFloat64/toInt64/toUint64 functions use type switches which have
//    measurable overhead (~2-5ns per call). For hot paths processing millions
//    of values, this adds up.
//
//    Recommendation: Add specialized non-interface versions for common types:
//      sqrtFloat64(v float64) float64
//      sqrtInt64(v int64) float64
//
// 2. NULL CHECK OVERHEAD
//    Every function checks for NULL even when processing non-nullable columns.
//
//    Recommendation: Add batch processing functions that skip NULL checks:
//      sqrtBatchNonNull(values []float64, results []float64)
//
// 3. ERROR OBJECT ALLOCATION
//    Domain errors create new error objects via newMathDomainError().
//
//    Recommendation: Use pre-allocated error sentinels (already done for
//    ErrMathDomainSqrt etc.) and return those directly instead of wrapping.
//
// 4. RETURN VALUE BOXING
//    Returning (any, error) requires boxing float64 values into interface{}.
//
//    Recommendation: For batch operations, use typed slice returns:
//      sqrtBatch(values []float64) ([]float64, error)
//
// 5. VECTORIZED OPERATIONS
//    DuckDB uses SIMD for batch math operations. Go's standard library
//    doesn't expose SIMD directly.
//
//    Recommendation: Investigate:
//      - gonum for vectorized math
//      - avo for generating SIMD assembly
//      - Runtime detection of AVX/AVX2/AVX512 capabilities

// BenchmarkOptimization_DirectVsInterface demonstrates the interface overhead.
func BenchmarkOptimization_DirectVsInterface(b *testing.B) {
	testValue := 3.14159

	b.Run("Direct_math.Sqrt", func(b *testing.B) {
		var result float64
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result = math.Sqrt(testValue)
		}
		_ = result
	})

	b.Run("WithTypeSwitch_sqrtValue", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = sqrtValue(testValue)
		}
	})

	b.Run("Interface_Return", func(b *testing.B) {
		var result any
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result = math.Sqrt(testValue)
		}
		_ = result
	})
}

// BenchmarkOptimization_BatchVsIndividual demonstrates batch optimization potential.
func BenchmarkOptimization_BatchVsIndividual(b *testing.B) {
	const batchSize = 1000
	values := make([]float64, batchSize)
	results := make([]float64, batchSize)
	for i := range values {
		values[i] = float64(i+1) * 0.1
	}

	b.Run("Individual_Calls", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j, v := range values {
				res, _ := sqrtValue(v)
				results[j] = res.(float64)
			}
		}
	})

	b.Run("Direct_Loop", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j, v := range values {
				results[j] = math.Sqrt(v)
			}
		}
	})
}
