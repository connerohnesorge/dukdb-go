// Package executor provides math function implementations for DuckDB compatibility.
package executor

import (
	"errors"
	"math"
	"math/bits"
	"math/rand"

	dukdb "github.com/dukdb/dukdb-go"
)

// Math domain error types for error checking in tests and error handling.
var (
	// ErrMathDomainSqrt is returned when SQRT is called with a negative number.
	ErrMathDomainSqrt = errors.New("SQRT of negative number not allowed")

	// ErrMathDomainLog is returned when LN/LOG is called with a non-positive number.
	ErrMathDomainLog = errors.New("cannot take logarithm of non-positive number")

	// ErrMathDomainAsin is returned when ASIN is called with a value outside [-1, 1].
	ErrMathDomainAsin = errors.New("ASIN domain error: input must be in [-1, 1]")

	// ErrMathDomainAcos is returned when ACOS is called with a value outside [-1, 1].
	ErrMathDomainAcos = errors.New("ACOS domain error: input must be in [-1, 1]")

	// ErrMathDomainAtanh is returned when ATANH is called with a value outside (-1, 1).
	ErrMathDomainAtanh = errors.New("ATANH domain error: input must be in (-1, 1)")

	// ErrMathDomainAcosh is returned when ACOSH is called with a value less than 1.
	ErrMathDomainAcosh = errors.New("ACOSH domain error: input must be >= 1")

	// ErrMathDomainFactorial is returned when FACTORIAL is called with a negative number or overflow.
	ErrMathDomainFactorial = errors.New(
		"FACTORIAL domain error: input must be non-negative and <= 20",
	)
)

// toFloat64 converts a value to float64 for math operations.
// Returns 0 for nil or non-numeric types, and a bool indicating if the value was valid.
func toFloat64(v any) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int16:
		return float64(val), true
	case int8:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint8:
		return float64(val), true
	default:
		return 0, false
	}
}

// toInt64 converts a value to int64 for integer math operations.
// Returns 0 for nil or non-numeric types, and a bool indicating if the value was valid.
func toInt64(v any) (int64, bool) {
	if v == nil {
		return 0, false
	}
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case int16:
		return int64(val), true
	case int8:
		return int64(val), true
	case uint64:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint8:
		return int64(val), true
	case float64:
		return int64(val), true
	case float32:
		return int64(val), true
	default:
		return 0, false
	}
}

// toUint64 converts a value to uint64 for unsigned integer operations (e.g., bit shifts).
// Returns 0 for nil or non-numeric types, and a bool indicating if the value was valid.
func toUint64(v any) (uint64, bool) {
	if v == nil {
		return 0, false
	}
	switch val := v.(type) {
	case uint64:
		return val, true
	case uint32:
		return uint64(val), true
	case uint16:
		return uint64(val), true
	case uint8:
		return uint64(val), true
	case int64:
		return uint64(val), true
	case int:
		return uint64(val), true
	case int32:
		return uint64(val), true
	case int16:
		return uint64(val), true
	case int8:
		return uint64(val), true
	case float64:
		return uint64(val), true
	case float32:
		return uint64(val), true
	default:
		return 0, false
	}
}

// newMathDomainError creates a standardized math domain error for the executor.
func newMathDomainError(msg string) *dukdb.Error {
	return &dukdb.Error{
		Type: dukdb.ErrorTypeExecutor,
		Msg:  msg,
	}
}

// isNull checks if a value represents SQL NULL.
func isNull(v any) bool {
	return v == nil
}

// checkNullUnary returns true if the argument is NULL (caller should return NULL).
func checkNullUnary(v any) bool {
	return isNull(v)
}

// checkNullBinary returns true if either argument is NULL (caller should return NULL).
func checkNullBinary(a, b any) bool {
	return isNull(a) || isNull(b)
}

// Rounding Functions

// roundValue implements ROUND(value, decimals).
// If decimals is 0 or not provided, rounds to nearest integer.
// Positive decimals round to that many decimal places.
// Negative decimals round to the left of the decimal point.
func roundValue(value any, decimals any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("ROUND requires numeric argument")
	}

	// Default decimals to 0 if nil
	d := int64(0)
	if decimals != nil {
		d, _ = toInt64(decimals)
	}

	if d == 0 {
		return math.Round(v), nil
	}

	multiplier := math.Pow(10, float64(d))
	return math.Round(v*multiplier) / multiplier, nil
}

// ceilValue implements CEIL/CEILING(value).
// Returns the smallest integer value not less than the argument.
func ceilValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("CEIL requires numeric argument")
	}

	return math.Ceil(v), nil
}

// floorValue implements FLOOR(value).
// Returns the largest integer value not greater than the argument.
func floorValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("FLOOR requires numeric argument")
	}

	return math.Floor(v), nil
}

// truncValue implements TRUNC/TRUNCATE(value).
// Returns the integer part of the number, truncating toward zero.
func truncValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("TRUNC requires numeric argument")
	}

	return math.Trunc(v), nil
}

// roundEvenValue implements ROUND_EVEN(value, decimals).
// Uses banker's rounding (round half to even).
func roundEvenValue(value any, decimals any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("ROUND_EVEN requires numeric argument")
	}

	// Default decimals to 0 if nil
	d := int64(0)
	if decimals != nil {
		d, _ = toInt64(decimals)
	}

	if d == 0 {
		return math.RoundToEven(v), nil
	}

	multiplier := math.Pow(10, float64(d))
	return math.RoundToEven(v*multiplier) / multiplier, nil
}

// evenValue implements EVEN(value).
// Rounds to the nearest even integer.
func evenValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("EVEN requires numeric argument")
	}

	// Round to nearest integer first
	rounded := math.Round(v)

	// If already even, return as is
	if int64(rounded)%2 == 0 {
		return rounded, nil
	}

	// Otherwise round away from zero to next even
	if v >= 0 {
		return rounded + 1, nil
	}
	return rounded - 1, nil
}

// Scientific Functions

// sqrtValue implements SQRT(value).
// Returns the square root of the argument.
// Returns error for negative numbers.
func sqrtValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("SQRT requires numeric argument")
	}

	if v < 0 {
		return nil, newMathDomainError(ErrMathDomainSqrt.Error())
	}

	return math.Sqrt(v), nil
}

// cbrtValue implements CBRT(value).
// Returns the cube root of the argument.
func cbrtValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("CBRT requires numeric argument")
	}

	return math.Cbrt(v), nil
}

// powValue implements POW/POWER(base, exponent).
// Returns base raised to the power of exponent.
func powValue(base any, exponent any) (any, error) {
	if checkNullBinary(base, exponent) {
		return nil, nil
	}

	b, ok := toFloat64(base)
	if !ok {
		return nil, newMathDomainError("POW requires numeric arguments")
	}

	e, ok := toFloat64(exponent)
	if !ok {
		return nil, newMathDomainError("POW requires numeric arguments")
	}

	return math.Pow(b, e), nil
}

// expValue implements EXP(value).
// Returns e raised to the power of the argument.
func expValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("EXP requires numeric argument")
	}

	return math.Exp(v), nil
}

// lnValue implements LN(value).
// Returns the natural logarithm of the argument.
// Returns error for non-positive numbers.
func lnValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("LN requires numeric argument")
	}

	if v <= 0 {
		return nil, newMathDomainError(ErrMathDomainLog.Error())
	}

	return math.Log(v), nil
}

// log10Value implements LOG10/LOG(value).
// Returns the base-10 logarithm of the argument.
// Returns error for non-positive numbers.
func log10Value(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("LOG10 requires numeric argument")
	}

	if v <= 0 {
		return nil, newMathDomainError(ErrMathDomainLog.Error())
	}

	return math.Log10(v), nil
}

// log2Value implements LOG2(value).
// Returns the base-2 logarithm of the argument.
// Returns error for non-positive numbers.
func log2Value(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("LOG2 requires numeric argument")
	}

	if v <= 0 {
		return nil, newMathDomainError(ErrMathDomainLog.Error())
	}

	return math.Log2(v), nil
}

// gammaValue implements GAMMA(value).
// Returns the gamma function of the argument.
func gammaValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("GAMMA requires numeric argument")
	}

	return math.Gamma(v), nil
}

// lgammaValue implements LGAMMA(value).
// Returns the natural logarithm of the absolute value of the gamma function.
func lgammaValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("LGAMMA requires numeric argument")
	}

	result, _ := math.Lgamma(v)
	return result, nil
}

// factorialValue implements FACTORIAL(value).
// Returns n! for non-negative integers.
// Returns error for negative numbers or numbers > 20 (overflow).
func factorialValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toInt64(value)
	if !ok {
		return nil, newMathDomainError("FACTORIAL requires integer argument")
	}

	if v < 0 || v > 20 {
		return nil, newMathDomainError(ErrMathDomainFactorial.Error())
	}

	// Calculate factorial iteratively to avoid recursion overhead
	result := int64(1)
	for i := int64(2); i <= v; i++ {
		result *= i
	}

	return result, nil
}

// Trigonometric Functions

// sinValue implements SIN(value).
// Returns the sine of the argument (in radians).
func sinValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("SIN requires numeric argument")
	}

	return math.Sin(v), nil
}

// cosValue implements COS(value).
// Returns the cosine of the argument (in radians).
func cosValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("COS requires numeric argument")
	}

	return math.Cos(v), nil
}

// tanValue implements TAN(value).
// Returns the tangent of the argument (in radians).
func tanValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("TAN requires numeric argument")
	}

	return math.Tan(v), nil
}

// cotValue implements COT(value).
// Returns the cotangent of the argument (1/tan(x)).
func cotValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("COT requires numeric argument")
	}

	tanVal := math.Tan(v)
	if tanVal == 0 {
		return math.Inf(1), nil // COT(0) = infinity
	}

	return 1.0 / tanVal, nil
}

// asinValue implements ASIN(value).
// Returns the arc sine of the argument.
// Returns error for values outside [-1, 1].
func asinValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("ASIN requires numeric argument")
	}

	if v < -1 || v > 1 {
		return nil, newMathDomainError(ErrMathDomainAsin.Error())
	}

	return math.Asin(v), nil
}

// acosValue implements ACOS(value).
// Returns the arc cosine of the argument.
// Returns error for values outside [-1, 1].
func acosValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("ACOS requires numeric argument")
	}

	if v < -1 || v > 1 {
		return nil, newMathDomainError(ErrMathDomainAcos.Error())
	}

	return math.Acos(v), nil
}

// atanValue implements ATAN(value).
// Returns the arc tangent of the argument.
func atanValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("ATAN requires numeric argument")
	}

	return math.Atan(v), nil
}

// atan2Value implements ATAN2(y, x).
// Returns the arc tangent of y/x, using the signs to determine the quadrant.
func atan2Value(y any, x any) (any, error) {
	if checkNullBinary(y, x) {
		return nil, nil
	}

	yVal, ok := toFloat64(y)
	if !ok {
		return nil, newMathDomainError("ATAN2 requires numeric arguments")
	}

	xVal, ok := toFloat64(x)
	if !ok {
		return nil, newMathDomainError("ATAN2 requires numeric arguments")
	}

	return math.Atan2(yVal, xVal), nil
}

// degreesValue implements DEGREES(radians).
// Converts radians to degrees.
func degreesValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("DEGREES requires numeric argument")
	}

	return v * (180.0 / math.Pi), nil
}

// radiansValue implements RADIANS(degrees).
// Converts degrees to radians.
func radiansValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("RADIANS requires numeric argument")
	}

	return v * (math.Pi / 180.0), nil
}

// Hyperbolic Functions

// sinhValue implements SINH(value).
// Returns the hyperbolic sine of the argument.
func sinhValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("SINH requires numeric argument")
	}

	return math.Sinh(v), nil
}

// coshValue implements COSH(value).
// Returns the hyperbolic cosine of the argument.
func coshValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("COSH requires numeric argument")
	}

	return math.Cosh(v), nil
}

// tanhValue implements TANH(value).
// Returns the hyperbolic tangent of the argument.
func tanhValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("TANH requires numeric argument")
	}

	return math.Tanh(v), nil
}

// asinhValue implements ASINH(value).
// Returns the inverse hyperbolic sine of the argument.
func asinhValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("ASINH requires numeric argument")
	}

	return math.Asinh(v), nil
}

// acoshValue implements ACOSH(value).
// Returns the inverse hyperbolic cosine of the argument.
// Returns error for values less than 1.
func acoshValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("ACOSH requires numeric argument")
	}

	if v < 1 {
		return nil, newMathDomainError(ErrMathDomainAcosh.Error())
	}

	return math.Acosh(v), nil
}

// atanhValue implements ATANH(value).
// Returns the inverse hyperbolic tangent of the argument.
// Returns error for values outside (-1, 1).
func atanhValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("ATANH requires numeric argument")
	}

	if v <= -1 || v >= 1 {
		return nil, newMathDomainError(ErrMathDomainAtanh.Error())
	}

	return math.Atanh(v), nil
}

// Utility Functions

// piValue implements PI().
// Returns the mathematical constant Pi.
func piValue() (any, error) {
	return math.Pi, nil
}

// randomValue implements RANDOM().
// Returns a random float64 between 0 (inclusive) and 1 (exclusive).
func randomValue() (any, error) {
	return rand.Float64(), nil
}

// signValue implements SIGN(value).
// Returns -1, 0, or 1 depending on the sign of the argument.
func signValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return nil, newMathDomainError("SIGN requires numeric argument")
	}

	if v < 0 {
		return int64(-1), nil
	} else if v > 0 {
		return int64(1), nil
	}
	return int64(0), nil
}

// gcdValue implements GCD(a, b).
// Returns the greatest common divisor of two integers.
func gcdValue(a any, b any) (any, error) {
	if checkNullBinary(a, b) {
		return nil, nil
	}

	aVal, ok := toInt64(a)
	if !ok {
		return nil, newMathDomainError("GCD requires integer arguments")
	}

	bVal, ok := toInt64(b)
	if !ok {
		return nil, newMathDomainError("GCD requires integer arguments")
	}

	// Use absolute values
	if aVal < 0 {
		aVal = -aVal
	}
	if bVal < 0 {
		bVal = -bVal
	}

	// Euclidean algorithm
	for bVal != 0 {
		aVal, bVal = bVal, aVal%bVal
	}

	return aVal, nil
}

// lcmValue implements LCM(a, b).
// Returns the least common multiple of two integers.
func lcmValue(a any, b any) (any, error) {
	if checkNullBinary(a, b) {
		return nil, nil
	}

	aVal, ok := toInt64(a)
	if !ok {
		return nil, newMathDomainError("LCM requires integer arguments")
	}

	bVal, ok := toInt64(b)
	if !ok {
		return nil, newMathDomainError("LCM requires integer arguments")
	}

	// Use absolute values
	if aVal < 0 {
		aVal = -aVal
	}
	if bVal < 0 {
		bVal = -bVal
	}

	// LCM(0, x) = LCM(x, 0) = 0
	if aVal == 0 || bVal == 0 {
		return int64(0), nil
	}

	// LCM = |a * b| / GCD(a, b)
	gcd, _ := gcdValue(aVal, bVal)
	gcdVal := gcd.(int64)

	return (aVal / gcdVal) * bVal, nil
}

// isnanValue implements ISNAN(value).
// Returns true if the value is NaN.
func isnanValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return false, nil // Non-numeric values are not NaN
	}

	return math.IsNaN(v), nil
}

// isinfValue implements ISINF(value).
// Returns true if the value is positive or negative infinity.
func isinfValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return false, nil // Non-numeric values are not infinite
	}

	return math.IsInf(v, 0), nil
}

// isfiniteValue implements ISFINITE(value).
// Returns true if the value is neither NaN nor infinity.
func isfiniteValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toFloat64(value)
	if !ok {
		return true, nil // Non-numeric values are considered finite
	}

	return !math.IsNaN(v) && !math.IsInf(v, 0), nil
}

// Bitwise Functions

// bitwiseAndValue implements the & (bitwise AND) operator.
// Returns the bitwise AND of two integer values.
func bitwiseAndValue(left any, right any) (any, error) {
	if checkNullBinary(left, right) {
		return nil, nil
	}

	l, ok := toInt64(left)
	if !ok {
		return nil, newMathDomainError("bitwise AND requires integer arguments")
	}

	r, ok := toInt64(right)
	if !ok {
		return nil, newMathDomainError("bitwise AND requires integer arguments")
	}

	return l & r, nil
}

// bitwiseOrValue implements the | (bitwise OR) operator.
// Returns the bitwise OR of two integer values.
func bitwiseOrValue(left any, right any) (any, error) {
	if checkNullBinary(left, right) {
		return nil, nil
	}

	l, ok := toInt64(left)
	if !ok {
		return nil, newMathDomainError("bitwise OR requires integer arguments")
	}

	r, ok := toInt64(right)
	if !ok {
		return nil, newMathDomainError("bitwise OR requires integer arguments")
	}

	return l | r, nil
}

// bitwiseXorValue implements the ^ (bitwise XOR) operator.
// Returns the bitwise XOR of two integer values.
func bitwiseXorValue(left any, right any) (any, error) {
	if checkNullBinary(left, right) {
		return nil, nil
	}

	l, ok := toInt64(left)
	if !ok {
		return nil, newMathDomainError("bitwise XOR requires integer arguments")
	}

	r, ok := toInt64(right)
	if !ok {
		return nil, newMathDomainError("bitwise XOR requires integer arguments")
	}

	return l ^ r, nil
}

// bitwiseNotValue implements the ~ (bitwise NOT) operator.
// Returns the bitwise complement of an integer value.
func bitwiseNotValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toInt64(value)
	if !ok {
		return nil, newMathDomainError("bitwise NOT requires integer argument")
	}

	return ^v, nil
}

// bitwiseShiftLeftValue implements the << (left shift) operator.
// Shifts the bits of the left operand to the left by the number of positions
// specified by the right operand.
func bitwiseShiftLeftValue(left any, right any) (any, error) {
	if checkNullBinary(left, right) {
		return nil, nil
	}

	l, ok := toInt64(left)
	if !ok {
		return nil, newMathDomainError("left shift requires integer arguments")
	}

	r, ok := toUint64(right)
	if !ok {
		return nil, newMathDomainError("left shift requires integer arguments")
	}

	// Shifting by 64 or more bits results in 0 (or undefined behavior in some languages)
	// We follow the behavior of most SQL databases: shift beyond 63 returns 0
	if r >= 64 {
		return int64(0), nil
	}

	return l << r, nil
}

// bitwiseShiftRightValue implements the >> (right shift) operator.
// Shifts the bits of the left operand to the right by the number of positions
// specified by the right operand. This is an arithmetic shift (preserves sign).
func bitwiseShiftRightValue(left any, right any) (any, error) {
	if checkNullBinary(left, right) {
		return nil, nil
	}

	l, ok := toInt64(left)
	if !ok {
		return nil, newMathDomainError("right shift requires integer arguments")
	}

	r, ok := toUint64(right)
	if !ok {
		return nil, newMathDomainError("right shift requires integer arguments")
	}

	// Shifting by 64 or more bits results in 0 for positive, -1 for negative
	// (due to arithmetic shift preserving sign)
	if r >= 64 {
		if l < 0 {
			return int64(-1), nil
		}
		return int64(0), nil
	}

	return l >> r, nil
}

// bitCountValue implements BIT_COUNT(value).
// Returns the number of set bits (1s) in the binary representation of an integer.
func bitCountValue(value any) (any, error) {
	if checkNullUnary(value) {
		return nil, nil
	}

	v, ok := toInt64(value)
	if !ok {
		return nil, newMathDomainError("BIT_COUNT requires integer argument")
	}

	// For negative numbers, we count the bits in the two's complement representation
	// bits.OnesCount64 works on uint64, so we cast appropriately
	return int64(bits.OnesCount64(uint64(v))), nil
}
