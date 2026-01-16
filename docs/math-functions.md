# Math Functions

dukdb-go provides a comprehensive set of mathematical functions compatible with DuckDB. This document covers all available math functions, their signatures, behaviors, and usage examples.

## Table of Contents

- [Rounding Functions](#rounding-functions)
- [Scientific Functions](#scientific-functions)
- [Trigonometric Functions](#trigonometric-functions)
- [Hyperbolic Functions](#hyperbolic-functions)
- [Utility Functions](#utility-functions)
- [Bitwise Operators](#bitwise-operators)
- [Type Coercion](#type-coercion)
- [NULL Propagation](#null-propagation)
- [Error Handling](#error-handling)
- [Common Use Cases](#common-use-cases)

---

## Rounding Functions

Functions for rounding numeric values to specified precision levels.

### ROUND

Rounds a number to the specified number of decimal places.

**Syntax:**
```sql
ROUND(value)
ROUND(value, precision)
```

**Parameters:**
- `value` - Numeric value to round
- `precision` - Number of decimal places (default: 0)

**Precision Parameter Behavior:**
- **Positive precision**: Rounds to that many decimal places after the decimal point
- **Zero precision** (or omitted): Rounds to the nearest integer
- **Negative precision**: Rounds to the left of the decimal point (e.g., -1 rounds to nearest 10, -2 to nearest 100)

**Examples:**
```sql
-- Default precision (0) - rounds to nearest integer
SELECT ROUND(123.456);        -- Returns 123

-- Positive precision - decimal places
SELECT ROUND(123.456, 2);     -- Returns 123.46
SELECT ROUND(123.456, 1);     -- Returns 123.5

-- Negative precision - rounds to left of decimal
SELECT ROUND(123.456, -1);    -- Returns 120
SELECT ROUND(1567.89, -2);    -- Returns 1600
SELECT ROUND(1567.89, -3);    -- Returns 2000
```

**Rounding Behavior:**
Standard "round half away from zero" rounding is used. Values exactly at the midpoint (0.5) are rounded away from zero:
```sql
SELECT ROUND(2.5);   -- Returns 3.0
SELECT ROUND(3.5);   -- Returns 4.0
SELECT ROUND(-2.5);  -- Returns -3.0
SELECT ROUND(-3.5);  -- Returns -4.0
```

### ROUND_EVEN (Banker's Rounding)

Rounds a number using banker's rounding (round half to even), which reduces cumulative rounding bias in financial calculations.

**Syntax:**
```sql
ROUND_EVEN(value)
ROUND_EVEN(value, precision)
```

**Banker's Rounding Behavior:**
When a value is exactly at the midpoint (0.5), it rounds to the nearest even number instead of always rounding up. This eliminates statistical bias when rounding large datasets.

**Examples:**
```sql
-- Standard ROUND vs ROUND_EVEN at midpoints
SELECT ROUND(2.5);       -- Returns 3.0 (rounds up)
SELECT ROUND_EVEN(2.5);  -- Returns 2.0 (rounds to even)

SELECT ROUND(3.5);       -- Returns 4.0 (rounds up)
SELECT ROUND_EVEN(3.5);  -- Returns 4.0 (rounds to even)

SELECT ROUND(4.5);       -- Returns 5.0 (rounds up)
SELECT ROUND_EVEN(4.5);  -- Returns 4.0 (rounds to even)

-- With precision parameter
SELECT ROUND_EVEN(2.25, 1);   -- Returns 2.2
SELECT ROUND_EVEN(2.35, 1);   -- Returns 2.4
SELECT ROUND_EVEN(2.45, 1);   -- Returns 2.4
```

**Use Case:** Banker's rounding is preferred for financial calculations, statistical analysis, and any scenario where cumulative rounding bias could affect results.

### CEIL / CEILING

Returns the smallest integer greater than or equal to the argument.

**Syntax:**
```sql
CEIL(value)
CEILING(value)
```

**Examples:**
```sql
SELECT CEIL(123.456);   -- Returns 124.0
SELECT CEIL(123.001);   -- Returns 124.0
SELECT CEIL(123.0);     -- Returns 123.0
SELECT CEIL(-123.456);  -- Returns -123.0 (toward positive infinity)
```

### FLOOR

Returns the largest integer less than or equal to the argument.

**Syntax:**
```sql
FLOOR(value)
```

**Examples:**
```sql
SELECT FLOOR(123.456);   -- Returns 123.0
SELECT FLOOR(123.999);   -- Returns 123.0
SELECT FLOOR(123.0);     -- Returns 123.0
SELECT FLOOR(-123.456);  -- Returns -124.0 (toward negative infinity)
```

### TRUNC / TRUNCATE

Returns the integer part of a number, truncating toward zero.

**Syntax:**
```sql
TRUNC(value)
TRUNCATE(value)
```

**Examples:**
```sql
SELECT TRUNC(123.456);   -- Returns 123.0
SELECT TRUNC(123.999);   -- Returns 123.0
SELECT TRUNC(-123.456);  -- Returns -123.0 (toward zero, not -124)
SELECT TRUNC(-123.999);  -- Returns -123.0
```

**Note:** TRUNC differs from FLOOR for negative numbers. TRUNC always truncates toward zero, while FLOOR rounds toward negative infinity.

### EVEN

Rounds a number to the nearest even integer, rounding away from zero when necessary.

**Syntax:**
```sql
EVEN(value)
```

**Examples:**
```sql
SELECT EVEN(3);    -- Returns 4.0
SELECT EVEN(4);    -- Returns 4.0
SELECT EVEN(5.1);  -- Returns 6.0
SELECT EVEN(-3);   -- Returns -4.0
SELECT EVEN(-4);   -- Returns -4.0
```

---

## Scientific Functions

Functions for scientific calculations including roots, powers, logarithms, and special mathematical functions.

### SQRT

Returns the square root of a number.

**Syntax:**
```sql
SQRT(value)
```

**Domain Restriction:** Input must be greater than or equal to 0. Negative inputs return an error.

**Examples:**
```sql
SELECT SQRT(16);    -- Returns 4.0
SELECT SQRT(2);     -- Returns 1.4142135623730951
SELECT SQRT(0);     -- Returns 0.0

-- Error case
SELECT SQRT(-16);   -- ERROR: SQRT of negative number not allowed
```

### CBRT

Returns the cube root of a number. Unlike SQRT, CBRT accepts negative numbers.

**Syntax:**
```sql
CBRT(value)
```

**Examples:**
```sql
SELECT CBRT(27);    -- Returns 3.0
SELECT CBRT(8);     -- Returns 2.0
SELECT CBRT(-27);   -- Returns -3.0
SELECT CBRT(0);     -- Returns 0.0
```

### POW / POWER

Returns the base raised to the power of the exponent.

**Syntax:**
```sql
POW(base, exponent)
POWER(base, exponent)
```

**Examples:**
```sql
SELECT POW(2, 3);      -- Returns 8.0
SELECT POW(2, 0.5);    -- Returns 1.4142135623730951 (square root of 2)
SELECT POW(10, -2);    -- Returns 0.01
SELECT POWER(3, 4);    -- Returns 81.0

-- Fractional exponent for nth root
SELECT POW(16, 0.5);   -- Returns 4.0 (square root)
SELECT POW(27, 1.0/3); -- Returns 3.0 (cube root)
```

### EXP

Returns e (Euler's number, approximately 2.71828) raised to the specified power.

**Syntax:**
```sql
EXP(value)
```

**Examples:**
```sql
SELECT EXP(1);     -- Returns 2.718281828459045
SELECT EXP(0);     -- Returns 1.0
SELECT EXP(2);     -- Returns 7.38905609893065
SELECT EXP(-1);    -- Returns 0.36787944117144233
```

### LN

Returns the natural (base-e) logarithm of a number.

**Syntax:**
```sql
LN(value)
```

**Domain Restriction:** Input must be greater than 0. Zero or negative inputs return an error.

**Examples:**
```sql
SELECT LN(2.718281828);  -- Returns approximately 1.0
SELECT LN(1);            -- Returns 0.0
SELECT LN(10);           -- Returns 2.302585092994046

-- Error cases
SELECT LN(0);    -- ERROR: cannot take logarithm of non-positive number
SELECT LN(-1);   -- ERROR: cannot take logarithm of non-positive number
```

### LOG / LOG10

Returns the base-10 logarithm of a number.

**Syntax:**
```sql
LOG(value)
LOG10(value)
```

**Domain Restriction:** Input must be greater than 0.

**Examples:**
```sql
SELECT LOG(100);    -- Returns 2.0
SELECT LOG10(1000); -- Returns 3.0
SELECT LOG(10);     -- Returns 1.0
SELECT LOG(1);      -- Returns 0.0

-- Error cases
SELECT LOG(0);      -- ERROR: cannot take logarithm of non-positive number
```

### LOG2

Returns the base-2 logarithm of a number.

**Syntax:**
```sql
LOG2(value)
```

**Domain Restriction:** Input must be greater than 0.

**Examples:**
```sql
SELECT LOG2(8);     -- Returns 3.0
SELECT LOG2(16);    -- Returns 4.0
SELECT LOG2(1);     -- Returns 0.0
SELECT LOG2(2);     -- Returns 1.0
```

### GAMMA

Returns the gamma function of a number. For positive integers, GAMMA(n) = (n-1)!

**Syntax:**
```sql
GAMMA(value)
```

**Examples:**
```sql
SELECT GAMMA(5);    -- Returns 24.0 (which is 4!)
SELECT GAMMA(6);    -- Returns 120.0 (which is 5!)
SELECT GAMMA(0.5);  -- Returns 1.7724538509055159 (sqrt(pi))
```

### LGAMMA

Returns the natural logarithm of the absolute value of the gamma function.

**Syntax:**
```sql
LGAMMA(value)
```

**Examples:**
```sql
SELECT LGAMMA(5);   -- Returns 3.178053830347945 (ln(24))
SELECT LGAMMA(10);  -- Returns 12.801827480081469 (ln(362880))
```

### FACTORIAL

Returns the factorial of a non-negative integer.

**Syntax:**
```sql
FACTORIAL(value)
```

**Domain Restriction:** Input must be a non-negative integer between 0 and 20 (inclusive). Values outside this range will cause an overflow error.

**Examples:**
```sql
SELECT FACTORIAL(0);   -- Returns 1
SELECT FACTORIAL(1);   -- Returns 1
SELECT FACTORIAL(5);   -- Returns 120
SELECT FACTORIAL(10);  -- Returns 3628800
SELECT FACTORIAL(20);  -- Returns 2432902008176640000

-- Error case
SELECT FACTORIAL(21);  -- ERROR: FACTORIAL domain error: input must be non-negative and <= 20
SELECT FACTORIAL(-1);  -- ERROR: FACTORIAL domain error: input must be non-negative and <= 20
```

---

## Trigonometric Functions

All trigonometric functions use radians for angle measurements. Use RADIANS() to convert from degrees if needed.

### SIN

Returns the sine of an angle in radians.

**Syntax:**
```sql
SIN(radians)
```

**Examples:**
```sql
SELECT SIN(0);           -- Returns 0.0
SELECT SIN(PI() / 2);    -- Returns 1.0
SELECT SIN(PI());        -- Returns approximately 0.0
SELECT SIN(RADIANS(90)); -- Returns 1.0 (90 degrees)
```

### COS

Returns the cosine of an angle in radians.

**Syntax:**
```sql
COS(radians)
```

**Examples:**
```sql
SELECT COS(0);           -- Returns 1.0
SELECT COS(PI() / 2);    -- Returns approximately 0.0
SELECT COS(PI());        -- Returns -1.0
SELECT COS(RADIANS(60)); -- Returns 0.5
```

### TAN

Returns the tangent of an angle in radians.

**Syntax:**
```sql
TAN(radians)
```

**Examples:**
```sql
SELECT TAN(0);           -- Returns 0.0
SELECT TAN(PI() / 4);    -- Returns approximately 1.0
SELECT TAN(RADIANS(45)); -- Returns approximately 1.0
```

### COT

Returns the cotangent of an angle in radians (1/tan(x)).

**Syntax:**
```sql
COT(radians)
```

**Examples:**
```sql
SELECT COT(PI() / 4);    -- Returns approximately 1.0
SELECT COT(PI() / 2);    -- Returns approximately 0.0
SELECT COT(0);           -- Returns Infinity
```

### ASIN

Returns the arc sine (inverse sine) of a value, in radians.

**Syntax:**
```sql
ASIN(value)
```

**Domain Restriction:** Input must be in the range [-1, 1]. Values outside this range return an error.

**Examples:**
```sql
SELECT ASIN(0);          -- Returns 0.0
SELECT ASIN(0.5);        -- Returns 0.5235987755982989 (pi/6)
SELECT ASIN(1);          -- Returns 1.5707963267948966 (pi/2)
SELECT ASIN(-1);         -- Returns -1.5707963267948966 (-pi/2)

-- Error case
SELECT ASIN(2);          -- ERROR: ASIN domain error: input must be in [-1, 1]
```

### ACOS

Returns the arc cosine (inverse cosine) of a value, in radians.

**Syntax:**
```sql
ACOS(value)
```

**Domain Restriction:** Input must be in the range [-1, 1].

**Examples:**
```sql
SELECT ACOS(1);          -- Returns 0.0
SELECT ACOS(0.5);        -- Returns 1.0471975511965979 (pi/3)
SELECT ACOS(0);          -- Returns 1.5707963267948966 (pi/2)
SELECT ACOS(-1);         -- Returns 3.141592653589793 (pi)

-- Error case
SELECT ACOS(-2);         -- ERROR: ACOS domain error: input must be in [-1, 1]
```

### ATAN

Returns the arc tangent (inverse tangent) of a value, in radians.

**Syntax:**
```sql
ATAN(value)
```

**Examples:**
```sql
SELECT ATAN(0);          -- Returns 0.0
SELECT ATAN(1);          -- Returns 0.7853981633974483 (pi/4)
SELECT ATAN(-1);         -- Returns -0.7853981633974483 (-pi/4)
```

### ATAN2

Returns the arc tangent of y/x, using the signs to determine the quadrant. This is useful for converting Cartesian coordinates to polar coordinates.

**Syntax:**
```sql
ATAN2(y, x)
```

**Examples:**
```sql
SELECT ATAN2(1, 1);      -- Returns 0.7853981633974483 (pi/4, first quadrant)
SELECT ATAN2(1, -1);     -- Returns 2.356194490192345 (3pi/4, second quadrant)
SELECT ATAN2(-1, -1);    -- Returns -2.356194490192345 (-3pi/4, third quadrant)
SELECT ATAN2(-1, 1);     -- Returns -0.7853981633974483 (-pi/4, fourth quadrant)
```

### DEGREES

Converts radians to degrees.

**Syntax:**
```sql
DEGREES(radians)
```

**Examples:**
```sql
SELECT DEGREES(PI());        -- Returns 180.0
SELECT DEGREES(PI() / 2);    -- Returns 90.0
SELECT DEGREES(PI() / 4);    -- Returns 45.0
SELECT DEGREES(2 * PI());    -- Returns 360.0
```

### RADIANS

Converts degrees to radians.

**Syntax:**
```sql
RADIANS(degrees)
```

**Examples:**
```sql
SELECT RADIANS(180);     -- Returns 3.141592653589793 (pi)
SELECT RADIANS(90);      -- Returns 1.5707963267948966 (pi/2)
SELECT RADIANS(45);      -- Returns 0.7853981633974483 (pi/4)
SELECT RADIANS(360);     -- Returns 6.283185307179586 (2*pi)
```

---

## Hyperbolic Functions

Hyperbolic trigonometric functions and their inverses.

### SINH

Returns the hyperbolic sine of a value.

**Syntax:**
```sql
SINH(value)
```

**Examples:**
```sql
SELECT SINH(0);    -- Returns 0.0
SELECT SINH(1);    -- Returns 1.1752011936438014
SELECT SINH(-1);   -- Returns -1.1752011936438014
```

### COSH

Returns the hyperbolic cosine of a value.

**Syntax:**
```sql
COSH(value)
```

**Examples:**
```sql
SELECT COSH(0);    -- Returns 1.0
SELECT COSH(1);    -- Returns 1.5430806348152437
SELECT COSH(-1);   -- Returns 1.5430806348152437
```

### TANH

Returns the hyperbolic tangent of a value.

**Syntax:**
```sql
TANH(value)
```

**Examples:**
```sql
SELECT TANH(0);    -- Returns 0.0
SELECT TANH(1);    -- Returns 0.7615941559557649
SELECT TANH(-1);   -- Returns -0.7615941559557649
```

### ASINH

Returns the inverse hyperbolic sine of a value.

**Syntax:**
```sql
ASINH(value)
```

**Examples:**
```sql
SELECT ASINH(0);   -- Returns 0.0
SELECT ASINH(1);   -- Returns 0.881373587019543
SELECT ASINH(-1);  -- Returns -0.881373587019543
```

### ACOSH

Returns the inverse hyperbolic cosine of a value.

**Syntax:**
```sql
ACOSH(value)
```

**Domain Restriction:** Input must be greater than or equal to 1.

**Examples:**
```sql
SELECT ACOSH(1);   -- Returns 0.0
SELECT ACOSH(2);   -- Returns 1.3169578969248166

-- Error case
SELECT ACOSH(0);   -- ERROR: ACOSH domain error: input must be >= 1
```

### ATANH

Returns the inverse hyperbolic tangent of a value.

**Syntax:**
```sql
ATANH(value)
```

**Domain Restriction:** Input must be in the range (-1, 1) exclusive.

**Examples:**
```sql
SELECT ATANH(0);     -- Returns 0.0
SELECT ATANH(0.5);   -- Returns 0.5493061443340548

-- Error cases
SELECT ATANH(1);     -- ERROR: ATANH domain error: input must be in (-1, 1)
SELECT ATANH(-1);    -- ERROR: ATANH domain error: input must be in (-1, 1)
```

---

## Utility Functions

Mathematical utilities and constants.

### PI

Returns the mathematical constant Pi (approximately 3.14159265358979323846).

**Syntax:**
```sql
PI()
```

**Examples:**
```sql
SELECT PI();                    -- Returns 3.141592653589793
SELECT 2 * PI();                -- Returns 6.283185307179586
SELECT PI() * POW(5, 2);        -- Area of circle with radius 5: 78.53981633974483
```

### RANDOM

Returns a random floating-point number between 0 (inclusive) and 1 (exclusive).

**Syntax:**
```sql
RANDOM()
```

**Examples:**
```sql
-- Each call returns a different random value
SELECT RANDOM();  -- Returns e.g., 0.6543217854123456

-- Generate random integer between 1 and 100
SELECT FLOOR(RANDOM() * 100) + 1;

-- Random boolean
SELECT RANDOM() < 0.5;
```

### SIGN

Returns the sign of a number: -1 for negative, 0 for zero, 1 for positive.

**Syntax:**
```sql
SIGN(value)
```

**Examples:**
```sql
SELECT SIGN(42);     -- Returns 1
SELECT SIGN(-42);    -- Returns -1
SELECT SIGN(0);      -- Returns 0
```

### GCD

Returns the greatest common divisor of two integers.

**Syntax:**
```sql
GCD(a, b)
```

**Examples:**
```sql
SELECT GCD(12, 18);    -- Returns 6
SELECT GCD(100, 75);   -- Returns 25
SELECT GCD(17, 13);    -- Returns 1 (coprime)
SELECT GCD(-12, 18);   -- Returns 6 (uses absolute values)
SELECT GCD(0, 5);      -- Returns 5
```

### LCM

Returns the least common multiple of two integers.

**Syntax:**
```sql
LCM(a, b)
```

**Examples:**
```sql
SELECT LCM(12, 18);    -- Returns 36
SELECT LCM(4, 6);      -- Returns 12
SELECT LCM(7, 11);     -- Returns 77 (coprime numbers)
SELECT LCM(-12, 18);   -- Returns 36 (uses absolute values)
SELECT LCM(0, 5);      -- Returns 0
```

### ISNAN

Returns TRUE if the value is NaN (Not a Number).

**Syntax:**
```sql
ISNAN(value)
```

**Examples:**
```sql
SELECT ISNAN(1.0);             -- Returns FALSE
SELECT ISNAN(0.0 / 0.0);       -- Returns TRUE (0/0 produces NaN)
SELECT ISNAN(SQRT(-1));        -- Error (SQRT of negative not allowed)
```

### ISINF

Returns TRUE if the value is positive or negative infinity.

**Syntax:**
```sql
ISINF(value)
```

**Examples:**
```sql
SELECT ISINF(1.0);             -- Returns FALSE
SELECT ISINF(1.0 / 0.0);       -- Returns TRUE (division by zero produces infinity)
SELECT ISINF(-1.0 / 0.0);      -- Returns TRUE (negative infinity)
```

### ISFINITE

Returns TRUE if the value is neither NaN nor infinity.

**Syntax:**
```sql
ISFINITE(value)
```

**Examples:**
```sql
SELECT ISFINITE(1.0);          -- Returns TRUE
SELECT ISFINITE(1.0 / 0.0);    -- Returns FALSE (infinity)
SELECT ISFINITE(0.0 / 0.0);    -- Returns FALSE (NaN)
```

---

## Bitwise Operators

Bitwise operators work exclusively on INTEGER types (TINYINT, SMALLINT, INTEGER, BIGINT). Attempting to use them with floating-point types will result in an error.

### Bitwise AND (&)

Returns the bitwise AND of two integers.

**Syntax:**
```sql
a & b
```

**Examples:**
```sql
SELECT 91 & 15;    -- Returns 11
-- Binary: 1011011 & 0001111 = 0001011

SELECT 255 & 128;  -- Returns 128
-- Binary: 11111111 & 10000000 = 10000000

SELECT 12 & 10;    -- Returns 8
-- Binary: 1100 & 1010 = 1000
```

### Bitwise OR (|)

Returns the bitwise OR of two integers.

**Syntax:**
```sql
a | b
```

**Examples:**
```sql
SELECT 32 | 3;     -- Returns 35
-- Binary: 100000 | 000011 = 100011

SELECT 12 | 10;    -- Returns 14
-- Binary: 1100 | 1010 = 1110
```

### Bitwise XOR (^)

Returns the bitwise XOR (exclusive OR) of two integers.

**Syntax:**
```sql
a ^ b
```

**Examples:**
```sql
SELECT 6 ^ 3;      -- Returns 5
-- Binary: 110 ^ 011 = 101

SELECT 12 ^ 10;    -- Returns 6
-- Binary: 1100 ^ 1010 = 0110
```

### Bitwise NOT (~)

Returns the bitwise complement of an integer (flips all bits).

**Syntax:**
```sql
~value
```

**Examples:**
```sql
SELECT ~5;         -- Returns -6
-- Binary: ~0...0101 = 1...1010 (two's complement: -6)

SELECT ~0;         -- Returns -1
-- Binary: ~0...0000 = 1...1111 (two's complement: -1)

SELECT ~(-1);      -- Returns 0
```

### Left Shift (<<)

Shifts bits to the left by the specified number of positions.

**Syntax:**
```sql
value << positions
```

**Examples:**
```sql
SELECT 5 << 2;     -- Returns 20
-- Binary: 101 << 2 = 10100

SELECT 1 << 10;    -- Returns 1024

-- Shifting by 64 or more returns 0
SELECT 5 << 64;    -- Returns 0
```

### Right Shift (>>)

Shifts bits to the right by the specified number of positions. This is an arithmetic shift that preserves the sign bit.

**Syntax:**
```sql
value >> positions
```

**Examples:**
```sql
SELECT 20 >> 2;    -- Returns 5
-- Binary: 10100 >> 2 = 101

SELECT 1024 >> 10; -- Returns 1

-- Arithmetic shift preserves sign
SELECT -8 >> 2;    -- Returns -2

-- Shifting by 64 or more returns 0 (positive) or -1 (negative)
SELECT 5 >> 64;    -- Returns 0
SELECT -5 >> 64;   -- Returns -1
```

### BIT_COUNT

Returns the number of set bits (1s) in the binary representation of an integer.

**Syntax:**
```sql
BIT_COUNT(value)
```

**Examples:**
```sql
SELECT BIT_COUNT(7);     -- Returns 3 (binary: 111)
SELECT BIT_COUNT(8);     -- Returns 1 (binary: 1000)
SELECT BIT_COUNT(255);   -- Returns 8 (binary: 11111111)
SELECT BIT_COUNT(0);     -- Returns 0

-- For negative numbers, counts bits in two's complement representation
SELECT BIT_COUNT(-1);    -- Returns 64 (all bits set in int64)
```

---

## Type Coercion

Math functions automatically coerce numeric types as needed.

### Integer to Double Coercion

Functions that require floating-point precision (SQRT, trigonometric functions, etc.) automatically convert INTEGER inputs to DOUBLE:

```sql
-- Integer input, DOUBLE output
SELECT SQRT(16);         -- Input: INTEGER 16, Output: DOUBLE 4.0
SELECT SIN(0);           -- Input: INTEGER 0, Output: DOUBLE 0.0
SELECT LOG(100);         -- Input: INTEGER 100, Output: DOUBLE 2.0
```

### Type Preservation for Rounding Functions

Rounding functions preserve the input type when appropriate:

```sql
-- DOUBLE input preserved
SELECT ROUND(123.456, 2);  -- Returns DOUBLE 123.46

-- Note: Integer inputs may still produce DOUBLE outputs
-- depending on the operation
SELECT ROUND(123.0, 0);    -- Returns DOUBLE 123.0
```

### Return Types by Function Category

| Category | Functions | Return Type |
|----------|-----------|-------------|
| Rounding | ROUND, CEIL, FLOOR, TRUNC | DOUBLE |
| Scientific | SQRT, CBRT, POW, EXP, LN, LOG | DOUBLE |
| Trigonometric | SIN, COS, TAN, ASIN, ACOS, ATAN | DOUBLE |
| Hyperbolic | SINH, COSH, TANH, ASINH, ACOSH, ATANH | DOUBLE |
| Integer-specific | FACTORIAL, GCD, LCM, BIT_COUNT | INTEGER/BIGINT |
| Boolean | ISNAN, ISINF, ISFINITE | BOOLEAN |
| Bitwise | &, |, ^, ~, <<, >> | INTEGER (same as input) |

---

## NULL Propagation

All math functions follow SQL NULL propagation rules: if any input is NULL, the output is NULL.

### Single-Argument Functions

```sql
SELECT SQRT(NULL);       -- Returns NULL
SELECT ABS(NULL);        -- Returns NULL
SELECT SIN(NULL);        -- Returns NULL
SELECT ROUND(NULL, 2);   -- Returns NULL
```

### Two-Argument Functions

```sql
-- NULL in first argument
SELECT POW(NULL, 2);     -- Returns NULL

-- NULL in second argument
SELECT POW(2, NULL);     -- Returns NULL

-- NULL in both arguments
SELECT POW(NULL, NULL);  -- Returns NULL

-- ROUND with NULL precision
SELECT ROUND(123.456, NULL);  -- Returns NULL
```

### Bitwise Operators with NULL

```sql
SELECT 5 & NULL;    -- Returns NULL
SELECT NULL | 3;    -- Returns NULL
SELECT ~NULL;       -- Returns NULL
SELECT 8 << NULL;   -- Returns NULL
```

### Utility Functions

```sql
SELECT GCD(12, NULL);    -- Returns NULL
SELECT LCM(NULL, 18);    -- Returns NULL
SELECT ISNAN(NULL);      -- Returns NULL
SELECT ISFINITE(NULL);   -- Returns NULL
```

---

## Error Handling

Math functions return errors for domain violations rather than returning NULL or special values. This ensures data integrity and helps identify problems early.

### Domain Restrictions Summary

| Function | Domain Restriction | Error Message |
|----------|-------------------|---------------|
| SQRT | value >= 0 | "SQRT of negative number not allowed" |
| LN, LOG, LOG10, LOG2 | value > 0 | "cannot take logarithm of non-positive number" |
| ASIN | -1 <= value <= 1 | "ASIN domain error: input must be in [-1, 1]" |
| ACOS | -1 <= value <= 1 | "ACOS domain error: input must be in [-1, 1]" |
| ACOSH | value >= 1 | "ACOSH domain error: input must be >= 1" |
| ATANH | -1 < value < 1 | "ATANH domain error: input must be in (-1, 1)" |
| FACTORIAL | 0 <= value <= 20, integer | "FACTORIAL domain error: input must be non-negative and <= 20" |

### Example Error Handling

```sql
-- These queries will return errors:
SELECT SQRT(-4);      -- ERROR: SQRT of negative number not allowed
SELECT LN(0);         -- ERROR: cannot take logarithm of non-positive number
SELECT LOG(-5);       -- ERROR: cannot take logarithm of non-positive number
SELECT ASIN(1.5);     -- ERROR: ASIN domain error: input must be in [-1, 1]
SELECT FACTORIAL(25); -- ERROR: FACTORIAL domain error: input must be non-negative and <= 20
```

### Handling Errors in Queries

Use CASE expressions or COALESCE to handle potential domain errors:

```sql
-- Safe square root that returns NULL for negative values
SELECT CASE
    WHEN value >= 0 THEN SQRT(value)
    ELSE NULL
END AS safe_sqrt
FROM numbers;

-- Safe logarithm
SELECT CASE
    WHEN value > 0 THEN LOG(value)
    ELSE NULL
END AS safe_log
FROM numbers;
```

---

## Common Use Cases

### Financial Calculations

```sql
-- Round currency to 2 decimal places
SELECT ROUND(price * 1.0825, 2) AS price_with_tax FROM products;

-- Use banker's rounding to avoid cumulative bias
SELECT ROUND_EVEN(SUM(amount), 2) AS total FROM transactions;

-- Calculate compound interest
SELECT principal * POW(1 + rate/100, years) AS future_value
FROM investments;

-- Round to nearest cent
SELECT ROUND(amount * exchange_rate, 2) AS converted_amount
FROM transactions;
```

### Scientific Calculations

```sql
-- Calculate distance between two points (Pythagorean theorem)
SELECT SQRT(POW(x2 - x1, 2) + POW(y2 - y1, 2)) AS distance
FROM points;

-- Convert polar coordinates to Cartesian
SELECT r * COS(RADIANS(theta)) AS x,
       r * SIN(RADIANS(theta)) AS y
FROM polar_coords;

-- Calculate bearing between coordinates
SELECT DEGREES(ATAN2(y2 - y1, x2 - x1)) AS bearing
FROM coordinates;

-- Exponential decay
SELECT initial_value * EXP(-decay_rate * time) AS current_value
FROM samples;
```

### Trigonometry

```sql
-- Calculate the angle of a right triangle
SELECT DEGREES(ATAN(opposite / adjacent)) AS angle_degrees
FROM triangles;

-- Calculate hypotenuse
SELECT SQRT(POW(side_a, 2) + POW(side_b, 2)) AS hypotenuse
FROM triangles;

-- Sine wave generation
SELECT amplitude * SIN(RADIANS(frequency * time + phase)) AS value
FROM waveform_params;
```

### Bitwise Operations for Flags

```sql
-- Check if a specific flag is set (flag at bit 2)
SELECT * FROM permissions WHERE (flags & 4) = 4;

-- Set a flag (add permission)
UPDATE permissions SET flags = flags | 8 WHERE id = 1;

-- Clear a flag (remove permission)
UPDATE permissions SET flags = flags & ~8 WHERE id = 1;

-- Toggle a flag
UPDATE permissions SET flags = flags ^ 4 WHERE id = 1;

-- Count active permissions
SELECT BIT_COUNT(flags) AS num_permissions FROM permissions;

-- Extract specific bits (e.g., last 4 bits)
SELECT flags & 15 AS last_four_bits FROM permissions;
```

### Random Data Generation

```sql
-- Generate random sample
SELECT * FROM large_table WHERE RANDOM() < 0.1;  -- ~10% sample

-- Random ordering
SELECT * FROM items ORDER BY RANDOM() LIMIT 10;

-- Generate random ID
SELECT FLOOR(RANDOM() * 1000000) AS random_id;
```

### Statistical Calculations

```sql
-- Logarithmic transformation (useful for skewed data)
SELECT LOG(value + 1) AS log_value FROM measurements;

-- Normalize values to 0-1 range using sigmoid function
SELECT 1 / (1 + EXP(-value)) AS normalized FROM scores;

-- Calculate percentage change
SELECT (new_value - old_value) / old_value * 100 AS pct_change
FROM comparisons;
```
