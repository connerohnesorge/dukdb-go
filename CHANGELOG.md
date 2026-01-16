# Changelog

All notable changes to dukdb-go will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### Math Functions

Added comprehensive mathematical function support, providing 45+ functions for numerical analysis, scientific computing, and general SQL operations.

**Rounding Functions:**
- `ROUND(value)` / `ROUND(value, decimals)` - Round to specified decimal places
- `CEIL(value)` / `CEILING(value)` - Round up to nearest integer
- `FLOOR(value)` - Round down to nearest integer
- `TRUNC(value)` - Truncate towards zero
- `ROUND_EVEN(value, decimals)` - Banker's rounding (round half to even)
- `EVEN(value)` - Round to nearest even integer

**Scientific Functions:**
- `SQRT(x)` - Square root (returns error for negative input)
- `CBRT(x)` - Cube root
- `POW(base, exponent)` / `POWER(base, exponent)` - Exponentiation
- `EXP(x)` - Natural exponential (e^x)
- `LN(x)` - Natural logarithm (returns error for non-positive input)
- `LOG(x)` / `LOG10(x)` - Base-10 logarithm
- `LOG2(x)` - Base-2 logarithm
- `GAMMA(x)` - Gamma function
- `LGAMMA(x)` - Log-gamma function
- `FACTORIAL(n)` - Factorial (returns error for n > 20 due to overflow)

**Trigonometric Functions:**
- `SIN(x)`, `COS(x)`, `TAN(x)`, `COT(x)` - Basic trigonometric functions (radians)
- `ASIN(x)`, `ACOS(x)`, `ATAN(x)` - Inverse trigonometric functions
- `ATAN2(y, x)` - Two-argument arctangent
- `DEGREES(radians)` - Convert radians to degrees
- `RADIANS(degrees)` - Convert degrees to radians

**Hyperbolic Functions:**
- `SINH(x)`, `COSH(x)`, `TANH(x)` - Hyperbolic sine, cosine, tangent
- `ASINH(x)`, `ACOSH(x)`, `ATANH(x)` - Inverse hyperbolic functions

**Utility Functions:**
- `PI()` - Mathematical constant pi (3.141592653589793)
- `RANDOM()` - Random number between 0 and 1
- `GCD(a, b)` - Greatest common divisor
- `LCM(a, b)` - Least common multiple
- `ISNAN(x)` - Check if value is NaN
- `ISINF(x)` - Check if value is infinity
- `ISFINITE(x)` - Check if value is finite (not NaN and not Inf)

**Bitwise Operators:**
- `&` (AND), `|` (OR), `^` (XOR), `~` (NOT) - Bitwise operations
- `<<` (left shift), `>>` (right shift) - Bit shifting
- `BIT_COUNT(x)` - Count number of set bits

#### Type Inference

- Added proper type inference for all math functions
- Rounding functions preserve input type for integers
- Scientific and trigonometric functions return DOUBLE
- FACTORIAL returns BIGINT
- ISNAN/ISINF/ISFINITE return BOOLEAN
- GCD/LCM return BIGINT
- BIT_COUNT returns INTEGER

#### Error Handling

- Domain errors for SQRT with negative input
- Domain errors for LN/LOG with non-positive input
- Domain errors for ASIN/ACOS with values outside [-1, 1]
- Domain errors for ACOSH with values less than 1
- Domain errors for ATANH with values at or outside [-1, 1]
- Overflow errors for FACTORIAL with n > 20
- Clear, descriptive error messages matching DuckDB style

### Documentation

- Added comprehensive math functions documentation in `docs/math-functions.md`
- Documented all function signatures, return types, and domain restrictions
- Added examples for common use cases (financial calculations, scientific computing)

### Performance

- All math functions optimized for single-value and batch operations
- Rounding functions: ~14-30 ns/op
- Scientific functions: ~14-30 ns/op
- Trigonometric functions: ~13-27 ns/op
- Bitwise operations: ~3-15 ns/op
- No allocations for many utility functions

### Compatibility

- Full compatibility with DuckDB math function behavior
- IEEE 754 floating-point precision
- NULL propagation for all functions
- Consistent type coercion behavior
