# Implementation Tasks

## 1. Core Infrastructure

- [x] 1.1 Create `internal/executor/math.go` file
- [x] 1.2 Add helper functions: `toFloat64`, `toInt64`, `toUint64` (if not already present)
- [x] 1.3 Define Error types for math domain violations
- [x] 1.4 Write unit tests for helper functions

## 2. Rounding Functions

- [x] 2.1 Implement `roundValue(value, decimals)` using `math.Round`
- [x] 2.2 Implement `ceilValue(value)` using `math.Ceil`
- [x] 2.3 Implement `floorValue(value)` using `math.Floor`
- [x] 2.4 Implement `truncValue(value)` using `math.Trunc`
- [x] 2.5 Implement `roundEvenValue(value, decimals)` using `math.RoundToEven`
- [x] 2.6 Implement `evenValue(value)` for rounding to even numbers
- [x] 2.7 Add ROUND, CEIL/CEILING, FLOOR, TRUNC cases to `internal/executor/expr.go`
- [x] 2.8 Add ROUND_EVEN, EVEN cases to expression evaluator
- [x] 2.9 Write unit tests for all rounding functions
- [x] 2.10 Test edge cases: NULL inputs, negative precision, INTEGER vs DOUBLE
- [x] 2.11 Integration test: ROUND with positive, zero, and negative precision

## 3. Scientific Functions

- [x] 3.1 Implement `sqrtValue(value)` with negative number check
- [x] 3.2 Implement `cbrtValue(value)` using `math.Cbrt`
- [x] 3.3 Implement `powValue(base, exponent)` using `math.Pow`
- [x] 3.4 Implement `expValue(value)` using `math.Exp`
- [x] 3.5 Implement `lnValue(value)` with non-positive check
- [x] 3.6 Implement `log10Value(value)` with non-positive check
- [x] 3.7 Implement `log2Value(value)` with non-positive check
- [x] 3.8 Implement `gammaValue(value)` using `math.Gamma`
- [x] 3.9 Implement `lgammaValue(value)` using `math.Lgamma`
- [x] 3.10 Implement `factorialValue(value)` with overflow check
- [x] 3.11 Add SQRT, CBRT, POW/POWER cases to expression evaluator
- [x] 3.12 Add EXP, LN, LOG/LOG10, LOG2 cases to expression evaluator
- [x] 3.13 Add GAMMA, LGAMMA, FACTORIAL cases to expression evaluator
- [x] 3.14 Write unit tests for all scientific functions
- [x] 3.15 Test error cases: SQRT(-1), LN(0), LN(-1), FACTORIAL(25)
- [x] 3.16 Integration test: Combined scientific functions in queries

## 4. Trigonometric Functions

- [x] 4.1 Implement `sinValue(value)`, `cosValue(value)`, `tanValue(value)`
- [x] 4.2 Implement `cotValue(value)` as 1/tan(x)
- [x] 4.3 Implement `asinValue(value)` with domain check [-1, 1]
- [x] 4.4 Implement `acosValue(value)` with domain check [-1, 1]
- [x] 4.5 Implement `atanValue(value)` using `math.Atan`
- [x] 4.6 Implement `atan2Value(y, x)` using `math.Atan2`
- [x] 4.7 Implement `degreesValue(radians)` for radian-to-degree conversion
- [x] 4.8 Implement `radiansValue(degrees)` for degree-to-radian conversion
- [x] 4.9 Add SIN, COS, TAN, COT cases to expression evaluator
- [x] 4.10 Add ASIN, ACOS, ATAN, ATAN2 cases to expression evaluator
- [x] 4.11 Add DEGREES, RADIANS cases to expression evaluator
- [x] 4.12 Write unit tests for all trigonometric functions
- [x] 4.13 Test domain errors: ASIN(2), ACOS(-2)
- [x] 4.14 Test special values: SIN(0), COS(0), TAN(PI/4)
- [x] 4.15 Integration test: Trigonometric calculations in queries

## 5. Hyperbolic Functions

- [x] 5.1 Implement `sinhValue(value)` using `math.Sinh`
- [x] 5.2 Implement `coshValue(value)` using `math.Cosh`
- [x] 5.3 Implement `tanhValue(value)` using `math.Tanh`
- [x] 5.4 Implement `asinhValue(value)` using `math.Asinh`
- [x] 5.5 Implement `acoshValue(value)` using `math.Acosh`
- [x] 5.6 Implement `atanhValue(value)` using `math.Atanh`
- [x] 5.7 Add SINH, COSH, TANH cases to expression evaluator
- [x] 5.8 Add ASINH, ACOSH, ATANH cases to expression evaluator
- [x] 5.9 Write unit tests for all hyperbolic functions
- [x] 5.10 Test special values: SINH(0), COSH(0), TANH(0)
- [x] 5.11 Integration test: Hyperbolic calculations

## 6. Utility Functions

- [x] 6.1 Implement `piValue()` returning `math.Pi`
- [x] 6.2 Implement `randomValue()` using `math.Float64`
- [x] 6.3 Implement `gcdValue(a, b)` for greatest common divisor
- [x] 6.4 Implement `lcmValue(a, b)` for least common multiple
- [x] 6.5 Implement `isnanValue(value)` using `math.IsNaN`
- [x] 6.6 Implement `isinfValue(value)` using `math.IsInf`
- [x] 6.7 Implement `isfiniteValue(value)` checking !NaN && !Inf
- [x] 6.8 Add PI, RANDOM cases to expression evaluator
- [x] 6.9 Add GCD, LCM cases to expression evaluator
- [x] 6.10 Add ISNAN, ISINF, ISFINITE cases to expression evaluator
- [x] 6.11 Write unit tests for all utility functions
- [x] 6.12 Test GCD/LCM with various integer pairs
- [x] 6.13 Test ISNAN/ISINF with NaN, Infinity, and normal values

## 7. Bitwise Operators - Parser Integration

- [x] 7.1 Add bitwise operator tokens to `internal/parser/parser_scanner.go`
- [x] 7.2 Define TOKEN_AMPERSAND (&)
- [x] 7.3 Define TOKEN_PIPE (|)
- [x] 7.4 Define TOKEN_CARET (^)
- [x] 7.5 Define TOKEN_TILDE (~)
- [x] 7.6 Define TOKEN_SHIFT_LEFT (<<)
- [x] 7.7 Define TOKEN_SHIFT_RIGHT (>>)
- [x] 7.8 Update scanner to recognize bitwise operator tokens
- [x] 7.9 Add bitwise operator precedence to parser
- [x] 7.10 Implement `parseBitwiseExpression()` in parser
- [x] 7.11 Write parser tests for bitwise operators

## 8. Bitwise Operators - Execution

- [x] 8.1 Implement `evaluateBitwiseAnd(left, right)` for & operator
- [x] 8.2 Implement `evaluateBitwiseOr(left, right)` for | operator
- [x] 8.3 Implement `evaluateBitwiseXor(left, right)` for ^ operator
- [x] 8.4 Implement `evaluateBitwiseNot(value)` for ~ operator
- [x] 8.5 Implement `evaluateBitwiseShiftLeft(left, right)` for << operator
- [x] 8.6 Implement `evaluateBitwiseShiftRight(left, right)` for >> operator
- [x] 8.7 Implement `bitCountValue(value)` using `bits.OnesCount64`
- [x] 8.8 Add bitwise operator cases to `evaluateBinaryOp` in expr.go
- [x] 8.9 Add BIT_COUNT function case to expression evaluator
- [x] 8.10 Write unit tests for all bitwise operators
- [x] 8.11 Test edge cases: 0 & x, x | 0, shift by 0, shift by 64
- [x] 8.12 Integration test: Bitwise operations in WHERE clauses

## 9. Type Inference

- [x] 9.1 Add `inferMathFunctionType()` to `internal/binder/type_inference.go`
- [x] 9.2 Define return types for rounding functions (preserve input type for integers)
- [x] 9.3 Define return types for scientific functions (DOUBLE)
- [x] 9.4 Define return types for trigonometric functions (DOUBLE)
- [x] 9.5 Define return types for utility functions (varies by function)
- [x] 9.6 Define return type for FACTORIAL (BIGINT)
- [x] 9.7 Define return type for BIT_COUNT (INTEGER)
- [x] 9.8 Define return types for ISNAN/ISINF/ISFINITE (BOOLEAN)
- [x] 9.9 Add type coercion for INTEGER → DOUBLE where needed
- [x] 9.10 Write unit tests for type inference
- [x] 9.11 Integration test: Type preservation and coercion

## 10. NULL Handling

- [x] 10.1 Add NULL checks to all math function implementations
- [x] 10.2 Return NULL for any function with NULL input
- [x] 10.3 Ensure NULL propagation for two-argument functions
- [x] 10.4 Write unit tests for NULL propagation
- [x] 10.5 Test NULL handling in all math functions
- [x] 10.6 Test NULL handling in bitwise operators

## 11. Error Handling

- [x] 11.1 Add domain error for SQRT of negative numbers
- [x] 11.2 Add domain error for LN/LOG of non-positive numbers
- [x] 11.3 Add domain error for ASIN/ACOS outside [-1, 1]
- [x] 11.4 Add overflow error for FACTORIAL(>20)
- [x] 11.5 Add clear error messages for all domain violations
- [x] 11.6 Write unit tests for all error cases
- [x] 11.7 Integration test: Error handling in complex queries

## 12. DuckDB Compatibility Testing

- [x] 12.1 Create compatibility test suite comparing dukdb-go vs DuckDB CLI
- [x] 12.2 Test ROUND with various precision values
- [x] 12.3 Test ROUND_EVEN banker's rounding behavior
- [x] 12.4 Test all scientific functions (SQRT, POW, EXP, LOG family)
- [x] 12.5 Test all trigonometric functions (SIN, COS, TAN, inverses)
- [x] 12.6 Test hyperbolic functions
- [x] 12.7 Test utility functions (PI, GCD, LCM, ISNAN, ISINF, ISFINITE)
- [x] 12.8 Test bitwise operators match DuckDB behavior
- [x] 12.9 Verify error messages match DuckDB wording
- [x] 12.10 Compare floating-point precision (accept IEEE 754 tolerance)

## 13. Integration Tests

- [x] 13.1 Test math functions in SELECT clauses
- [x] 13.2 Test math functions in WHERE clauses
- [x] 13.3 Test math functions in computed columns
- [x] 13.4 Test math functions with aggregate functions
- [x] 13.5 Test nested math function calls: `SELECT SQRT(POW(x, 2) + POW(y, 2))`
- [x] 13.6 Test bitwise operators in complex expressions
- [x] 13.7 Test type coercion in mixed-type expressions
- [x] 13.8 Integration test: Financial calculations (ROUND for currency)
- [x] 13.9 Integration test: Scientific calculations (trigonometry)
- [x] 13.10 Integration test: Bitwise operations for flags/masks

## 14. Performance Testing

- [x] 14.1 Benchmark rounding functions (ROUND, CEIL, FLOOR)
- [x] 14.2 Benchmark scientific functions (SQRT, POW, EXP, LOG)
- [x] 14.3 Benchmark trigonometric functions (SIN, COS, TAN)
- [x] 14.4 Benchmark bitwise operators
- [x] 14.5 Profile memory usage for math operations
- [x] 14.6 Compare performance with DuckDB (target: within 2x)
- [x] 14.7 Identify optimization opportunities

## 15. Documentation

- [x] 15.1 Document all math functions in user guide
- [x] 15.2 Document ROUND precision parameter behavior
- [x] 15.3 Document banker's rounding (ROUND_EVEN)
- [x] 15.4 Document trigonometric functions use radians
- [x] 15.5 Document domain restrictions (SQRT ≥ 0, LOG > 0, ASIN/ACOS in [-1,1])
- [x] 15.6 Document bitwise operators work on INTEGER types only
- [x] 15.7 Add examples for common use cases
- [x] 15.8 Document type coercion behavior
- [x] 15.9 Document NULL propagation
- [x] 15.10 Document error handling behavior

## 16. Validation and Release

- [x] 16.1 Run full test suite (unit + integration + compatibility)
- [x] 16.2 Validate all math functions work correctly
- [x] 16.3 Verify error handling is comprehensive
- [x] 16.4 Check performance benchmarks are acceptable
- [x] 16.5 Update CHANGELOG with math function support
- [x] 16.6 Update README with math function examples
- [x] 16.7 Create migration guide (no breaking changes, additive only)

## Dependencies and Parallelization

**Can be parallelized:**
- Tasks 2.x (Rounding), 3.x (Scientific), 4.x (Trigonometric), 5.x (Hyperbolic), 6.x (Utility) can be implemented concurrently
- Tasks 7.x (Parser) and 8.x (Bitwise execution) can be done in parallel
- Documentation tasks 15.x can be done anytime after corresponding features are implemented

**Sequential dependencies:**
- Task 1 (Infrastructure) must complete before all others
- Task 9 (Type inference) depends on tasks 2-6 being defined
- Task 10 (NULL handling) can be done concurrently with implementation
- Task 12 (Compatibility testing) depends on all implementations being complete
- Task 13 (Integration tests) depends on all implementations being complete

**Critical path:**
Task 1 → Tasks 2-8 (parallel) → Task 9 → Task 10 → Tasks 11-13 (parallel) → Task 14 → Tasks 15-16

**Estimated completion:**
- Task 1: 1 day (infrastructure)
- Tasks 2-8: 1 week (function implementations, can parallelize)
- Task 9-10: 2 days (type inference and NULL handling)
- Tasks 11-13: 1 week (error handling, compatibility, integration tests)
- Task 14: 2 days (performance testing)
- Tasks 15-16: 2 days (documentation and validation)
- **Total: 3 weeks** (with parallelization, could be 2-2.5 weeks)
