# Change: Add Essential Math Functions

## Why

Mathematical functions are **CRITICAL** for numerical data analysis, scientific computing, and general SQL queries. DuckDB v1.4.3 has 45+ math functions that users expect for standard operations like rounding decimals, calculating square roots, and performing trigonometric calculations. Without these, users must:
- Perform calculations in application code instead of SQL (inefficient)
- Use workarounds for simple operations (complex, error-prone)
- Cannot migrate queries from DuckDB CLI (compatibility broken)
- Blocked from scientific and financial analysis use cases

Currently, dukdb-go has only **5 math functions** (ABS, SIGN, MOD, GREATEST, LEAST), missing **40+ essential functions** including ROUND, CEIL, FLOOR, SQRT, POW, SIN, COS, LOG, and all trigonometric functions.

This is the **#2 highest priority** missing feature after glob pattern support.

## What Changes

- **ADDED**: Rounding functions
  - `ROUND(v, s)` - Round to s decimal places
  - `CEIL`/`CEILING(x)` - Round up
  - `FLOOR(x)` - Round down
  - `TRUNC(x)` - Truncate
  - `ROUND_EVEN(v, s)` - Banker's rounding
  - `EVEN(x)` - Round to next even number

- **ADDED**: Scientific functions
  - `SQRT(x)` - Square root
  - `CBRT(x)` - Cube root
  - `POW(x, y)`/`POWER(x, y)` - Power
  - `EXP(x)` - e^x
  - `LN(x)` - Natural logarithm
  - `LOG(x)`/`LOG10(x)` - Base-10 logarithm
  - `LOG2(x)` - Base-2 logarithm
  - `GAMMA(x)`, `LGAMMA(x)` - Gamma functions
  - `FACTORIAL(x)` - Factorial

- **ADDED**: Trigonometric functions
  - `SIN(x)`, `COS(x)`, `TAN(x)`, `COT(x)` - Basic trig
  - `ASIN(x)`, `ACOS(x)`, `ATAN(x)`, `ATAN2(y, x)` - Inverse trig
  - `DEGREES(x)`, `RADIANS(x)` - Angular conversion

- **ADDED**: Hyperbolic functions
  - `SINH(x)`, `COSH(x)`, `TANH(x)` - Hyperbolic trig
  - `ASINH(x)`, `ACOSH(x)`, `ATANH(x)` - Inverse hyperbolic

- **ADDED**: Utility functions
  - `PI()` - π constant
  - `RANDOM()` - Random number generation
  - `GCD(x, y)`, `LCM(x, y)` - Number theory
  - `ISNAN(x)`, `ISINF(x)`, `ISFINITE(x)` - Floating-point validation

- **ADDED**: Bitwise operators (INTEGER types only)
  - `&` (AND), `|` (OR), `^` (XOR), `~` (NOT)
  - `<<` (shift left), `>>` (shift right)
  - `BIT_COUNT(x)` - Count set bits

- **MODIFIED**: Expression evaluator to handle math function calls
- **MODIFIED**: Type inference for math function results

## Impact

- Affected specs: `specs/math/spec.md` (NEW)
- Affected code:
  - `internal/executor/expr.go` - Add math function cases
  - `internal/executor/math.go` (NEW) - Math function implementations
  - `internal/parser/` - Bitwise operator tokens
  - `internal/binder/` - Function binding and type checking
- Breaking changes: **None** (additive only)
- Dependencies:
  - Go standard library `math` package
  - Existing scalar function infrastructure

## Priority

**CRITICAL** - Unblocks numerical analysis (second most common use case) and provides parity with DuckDB CLI for mathematical operations.
