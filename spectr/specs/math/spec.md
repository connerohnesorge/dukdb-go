# Math Specification

## Requirements

### Requirement: Rounding Functions

The system SHALL provide rounding functions for numeric values with various rounding strategies.

#### Scenario: ROUND with default precision
- GIVEN a value 123.456
- WHEN executing `SELECT ROUND(123.456)`
- THEN result is 123

#### Scenario: ROUND with positive precision
- GIVEN a value 123.456
- WHEN executing `SELECT ROUND(123.456, 2)`
- THEN result is 123.46

#### Scenario: ROUND with negative precision
- GIVEN a value 123.456
- WHEN executing `SELECT ROUND(123.456, -1)`
- THEN result is 120

#### Scenario: CEIL rounds up
- GIVEN a value 123.456
- WHEN executing `SELECT CEIL(123.456)`
- THEN result is 124

#### Scenario: FLOOR rounds down
- GIVEN a value 123.456
- WHEN executing `SELECT FLOOR(123.456)`
- THEN result is 123

#### Scenario: TRUNC truncates decimal
- GIVEN a value -123.456
- WHEN executing `SELECT TRUNC(-123.456)`
- THEN result is -123

#### Scenario: ROUND_EVEN uses banker's rounding
- GIVEN a value 2.5
- WHEN executing `SELECT ROUND_EVEN(2.5)`
- THEN result is 2 (rounds to even)
- AND `SELECT ROUND_EVEN(3.5)` returns 4

#### Scenario: ROUND with NULL input
- GIVEN a NULL value
- WHEN executing `SELECT ROUND(NULL, 2)`
- THEN result is NULL

### Requirement: Scientific Functions

The system SHALL provide scientific mathematical functions for exponential, logarithmic, and power calculations.

#### Scenario: SQRT calculates square root
- GIVEN a value 16
- WHEN executing `SELECT SQRT(16)`
- THEN result is 4.0

#### Scenario: SQRT of negative number causes error
- GIVEN a value -16
- WHEN executing `SELECT SQRT(-16)`
- THEN an error is returned
- AND error message contains "SQRT of negative number not allowed"

#### Scenario: CBRT calculates cube root
- GIVEN a value 27
- WHEN executing `SELECT CBRT(27)`
- THEN result is 3.0

#### Scenario: POW calculates power
- GIVEN base 2 and exponent 3
- WHEN executing `SELECT POW(2, 3)`
- THEN result is 8.0

#### Scenario: POW with fractional exponent
- GIVEN base 16 and exponent 0.5
- WHEN executing `SELECT POW(16, 0.5)`
- THEN result is 4.0

#### Scenario: EXP calculates e to the power
- GIVEN a value 1
- WHEN executing `SELECT EXP(1)`
- THEN result is approximately 2.718281828

#### Scenario: LN calculates natural logarithm
- GIVEN a value (e)
- WHEN executing `SELECT LN(2.718281828)`
- THEN result is approximately 1.0

#### Scenario: LN of zero or negative causes error
- GIVEN a value 0
- WHEN executing `SELECT LN(0)`
- THEN an error is returned
- AND error message contains "LN of non-positive number not allowed"

#### Scenario: LOG calculates base-10 logarithm
- GIVEN a value 100
- WHEN executing `SELECT LOG(100)`
- THEN result is 2.0

#### Scenario: LOG10 is alias for LOG
- GIVEN a value 1000
- WHEN executing `SELECT LOG10(1000)`
- THEN result is 3.0

#### Scenario: LOG2 calculates base-2 logarithm
- GIVEN a value 8
- WHEN executing `SELECT LOG2(8)`
- THEN result is 3.0

#### Scenario: FACTORIAL calculates factorial
- GIVEN a value 5
- WHEN executing `SELECT FACTORIAL(5)`
- THEN result is 120

#### Scenario: FACTORIAL of large number causes overflow error
- GIVEN a value 25
- WHEN executing `SELECT FACTORIAL(25)`
- THEN an error is returned
- AND error message indicates factorial overflow

### Requirement: Trigonometric Functions

The system SHALL provide trigonometric functions using radians for angle measurements.

#### Scenario: SIN calculates sine
- GIVEN an angle 0 radians
- WHEN executing `SELECT SIN(0)`
- THEN result is 0.0

#### Scenario: SIN of π/2 radians
- GIVEN an angle π/2 radians
- WHEN executing `SELECT SIN(PI() / 2)`
- THEN result is approximately 1.0

#### Scenario: COS calculates cosine
- GIVEN an angle 0 radians
- WHEN executing `SELECT COS(0)`
- THEN result is 1.0

#### Scenario: TAN calculates tangent
- GIVEN an angle π/4 radians
- WHEN executing `SELECT TAN(PI() / 4)`
- THEN result is approximately 1.0

#### Scenario: COT calculates cotangent
- GIVEN an angle π/4 radians
- WHEN executing `SELECT COT(PI() / 4)`
- THEN result is approximately 1.0

#### Scenario: ASIN calculates inverse sine
- GIVEN a value 0.5
- WHEN executing `SELECT ASIN(0.5)`
- THEN result is approximately π/6 radians

#### Scenario: ASIN domain error for value outside [-1, 1]
- GIVEN a value 2.0
- WHEN executing `SELECT ASIN(2.0)`
- THEN an error is returned
- AND error message contains "ASIN domain error: input must be in [-1, 1]"

#### Scenario: ACOS calculates inverse cosine
- GIVEN a value 0.5
- WHEN executing `SELECT ACOS(0.5)`
- THEN result is approximately π/3 radians

#### Scenario: ATAN calculates inverse tangent
- GIVEN a value 1.0
- WHEN executing `SELECT ATAN(1.0)`
- THEN result is approximately π/4 radians

#### Scenario: ATAN2 calculates two-argument inverse tangent
- GIVEN y = 1 and x = 1
- WHEN executing `SELECT ATAN2(1, 1)`
- THEN result is approximately π/4 radians

#### Scenario: DEGREES converts radians to degrees
- GIVEN π radians
- WHEN executing `SELECT DEGREES(PI())`
- THEN result is 180.0

#### Scenario: RADIANS converts degrees to radians
- GIVEN 180 degrees
- WHEN executing `SELECT RADIANS(180)`
- THEN result is approximately π

### Requirement: Hyperbolic Functions

The system SHALL provide hyperbolic trigonometric functions.

#### Scenario: SINH calculates hyperbolic sine
- GIVEN a value 0
- WHEN executing `SELECT SINH(0)`
- THEN result is 0.0

#### Scenario: COSH calculates hyperbolic cosine
- GIVEN a value 0
- WHEN executing `SELECT COSH(0)`
- THEN result is 1.0

#### Scenario: TANH calculates hyperbolic tangent
- GIVEN a value 0
- WHEN executing `SELECT TANH(0)`
- THEN result is 0.0

#### Scenario: ASINH calculates inverse hyperbolic sine
- GIVEN a value 0
- WHEN executing `SELECT ASINH(0)`
- THEN result is 0.0

#### Scenario: ACOSH calculates inverse hyperbolic cosine
- GIVEN a value 1
- WHEN executing `SELECT ACOSH(1)`
- THEN result is 0.0

#### Scenario: ATANH calculates inverse hyperbolic tangent
- GIVEN a value 0
- WHEN executing `SELECT ATANH(0)`
- THEN result is 0.0

### Requirement: Utility Functions

The system SHALL provide mathematical utility functions and constants.

#### Scenario: PI returns π constant
- WHEN executing `SELECT PI()`
- THEN result is approximately 3.141592653589793

#### Scenario: RANDOM returns value in [0, 1)
- WHEN executing `SELECT RANDOM()`
- THEN result is a float value >= 0.0 and < 1.0

#### Scenario: GCD calculates greatest common divisor
- GIVEN values 12 and 18
- WHEN executing `SELECT GCD(12, 18)`
- THEN result is 6

#### Scenario: LCM calculates least common multiple
- GIVEN values 12 and 18
- WHEN executing `SELECT LCM(12, 18)`
- THEN result is 36

#### Scenario: ISNAN detects NaN values
- GIVEN a NaN value
- WHEN executing `SELECT ISNAN('NaN'::FLOAT)`
- THEN result is TRUE

#### Scenario: ISINF detects infinity values
- GIVEN an infinity value
- WHEN executing `SELECT ISINF('Infinity'::FLOAT)`
- THEN result is TRUE

#### Scenario: ISFINITE checks for finite values
- GIVEN a finite value 123.45
- WHEN executing `SELECT ISFINITE(123.45)`
- THEN result is TRUE

#### Scenario: EVEN rounds to next even number
- GIVEN a value 3
- WHEN executing `SELECT EVEN(3)`
- THEN result is 4
- AND `SELECT EVEN(-3)` returns -4

### Requirement: Bitwise Operators

The system SHALL provide bitwise operators for INTEGER types.

#### Scenario: Bitwise AND operator
- GIVEN values 91 and 15
- WHEN executing `SELECT 91 & 15`
- THEN result is 11

#### Scenario: Bitwise OR operator
- GIVEN values 32 and 3
- WHEN executing `SELECT 32 | 3`
- THEN result is 35

#### Scenario: Bitwise XOR operator
- GIVEN values 6 and 3
- WHEN executing `SELECT 6 ^ 3`
- THEN result is 5

#### Scenario: Bitwise NOT operator
- GIVEN value 5
- WHEN executing `SELECT ~5`
- THEN result is -6 (two's complement)

#### Scenario: Bitwise shift left operator
- GIVEN value 5 and shift 2
- WHEN executing `SELECT 5 << 2`
- THEN result is 20

#### Scenario: Bitwise shift right operator
- GIVEN value 20 and shift 2
- WHEN executing `SELECT 20 >> 2`
- THEN result is 5

#### Scenario: BIT_COUNT counts set bits
- GIVEN value 7 (binary 0111)
- WHEN executing `SELECT BIT_COUNT(7)`
- THEN result is 3

#### Scenario: Bitwise operators on NULL
- GIVEN a NULL value
- WHEN executing `SELECT 5 & NULL`
- THEN result is NULL

### Requirement: Type Coercion for Math Functions

The system SHALL automatically coerce INTEGER inputs to DOUBLE for math functions that require floating-point precision.

#### Scenario: Integer input to SQRT
- GIVEN an INTEGER value 16
- WHEN executing `SELECT SQRT(16)`
- THEN value is coerced to DOUBLE
- AND result is 4.0 (DOUBLE type)

#### Scenario: Integer input to trigonometric functions
- GIVEN an INTEGER value 0
- WHEN executing `SELECT SIN(0)`
- THEN value is coerced to DOUBLE
- AND result is 0.0 (DOUBLE type)

#### Scenario: ROUND preserves INTEGER type
- GIVEN an INTEGER value 123
- WHEN executing `SELECT ROUND(123, 0)`
- THEN result type is INTEGER
- AND value is 123

### Requirement: NULL Propagation

The system SHALL return NULL for any math function when any input argument is NULL.

#### Scenario: Math function with NULL argument
- GIVEN a NULL value
- WHEN executing `SELECT SQRT(NULL)`
- THEN result is NULL

#### Scenario: Two-argument function with one NULL
- GIVEN values 5 and NULL
- WHEN executing `SELECT POW(5, NULL)`
- THEN result is NULL

#### Scenario: Two-argument function with both NULL
- GIVEN both values NULL
- WHEN executing `SELECT POW(NULL, NULL)`
- THEN result is NULL

