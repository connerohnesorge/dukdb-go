# Implementation Tasks

## 1. Core Infrastructure

- [ ] 1.1 Create `internal/executor/math.go` file
- [ ] 1.2 Add helper functions: `toFloat64`, `toInt64`, `toUint64` (if not already present)
- [ ] 1.3 Define Error types for math domain violations
- [ ] 1.4 Write unit tests for helper functions

## 2. Rounding Functions

- [ ] 2.1 Implement `roundValue(value, decimals)` using `math.Round`
- [ ] 2.2 Implement `ceilValue(value)` using `math.Ceil`
- [ ] 2.3 Implement `floorValue(value)` using `math.Floor`
- [ ] 2.4 Implement `truncValue(value)` using `math.Trunc`
- [ ] 2.5 Implement `roundEvenValue(value, decimals)` using `math.RoundToEven`
- [ ] 2.6 Implement `evenValue(value)` for rounding to even numbers
- [ ] 2.7 Add ROUND, CEIL/CEILING, FLOOR, TRUNC cases to `internal/executor/expr.go`
- [ ] 2.8 Add ROUND_EVEN, EVEN cases to expression evaluator
- [ ] 2.9 Write unit tests for all rounding functions
- [ ] 2.10 Test edge cases: NULL inputs, negative precision, INTEGER vs DOUBLE
- [ ] 2.11 Integration test: ROUND with positive, zero, and negative precision

## 3. Scientific Functions

- [ ] 3.1 Implement `sqrtValue(value)` with negative number check
- [ ] 3.2 Implement `cbrtValue(value)` using `math.Cbrt`
- [ ] 3.3 Implement `powValue(base, exponent)` using `math.Pow`
- [ ] 3.4 Implement `expValue(value)` using `math.Exp`
- [ ] 3.5 Implement `lnValue(value)` with non-positive check
- [ ] 3.6 Implement `log10Value(value)` with non-positive check
- [ ] 3.7 Implement `log2Value(value)` with non-positive check
- [ ] 3.8 Implement `gammaValue(value)` using `math.Gamma`
- [ ] 3.9 Implement `lgammaValue(value)` using `math.Lgamma`
- [ ] 3.10 Implement `factorialValue(value)` with overflow check
- [ ] 3.11 Add SQRT, CBRT, POW/POWER cases to expression evaluator
- [ ] 3.12 Add EXP, LN, LOG/LOG10, LOG2 cases to expression evaluator
- [ ] 3.13 Add GAMMA, LGAMMA, FACTORIAL cases to expression evaluator
- [ ] 3.14 Write unit tests for all scientific functions
- [ ] 3.15 Test error cases: SQRT(-1), LN(0), LN(-1), FACTORIAL(25)
- [ ] 3.16 Integration test: Combined scientific functions in queries

## 4. Trigonometric Functions

- [ ] 4.1 Implement `sinValue(value)`, `cosValue(value)`, `tanValue(value)`
- [ ] 4.2 Implement `cotValue(value)` as 1/tan(x)
- [ ] 4.3 Implement `asinValue(value)` with domain check [-1, 1]
- [ ] 4.4 Implement `acosValue(value)` with domain check [-1, 1]
- [ ] 4.5 Implement `atanValue(value)` using `math.Atan`
- [ ] 4.6 Implement `atan2Value(y, x)` using `math.Atan2`
- [ ] 4.7 Implement `degreesValue(radians)` for radian-to-degree conversion
- [ ] 4.8 Implement `radiansValue(degrees)` for degree-to-radian conversion
- [ ] 4.9 Add SIN, COS, TAN, COT cases to expression evaluator
- [ ] 4.10 Add ASIN, ACOS, ATAN, ATAN2 cases to expression evaluator
- [ ] 4.11 Add DEGREES, RADIANS cases to expression evaluator
- [ ] 4.12 Write unit tests for all trigonometric functions
- [ ] 4.13 Test domain errors: ASIN(2), ACOS(-2)
- [ ] 4.14 Test special values: SIN(0), COS(0), TAN(PI/4)
- [ ] 4.15 Integration test: Trigonometric calculations in queries

## 5. Hyperbolic Functions

- [ ] 5.1 Implement `sinhValue(value)` using `math.Sinh`
- [ ] 5.2 Implement `coshValue(value)` using `math.Cosh`
- [ ] 5.3 Implement `tanhValue(value)` using `math.Tanh`
- [ ] 5.4 Implement `asinhValue(value)` using `math.Asinh`
- [ ] 5.5 Implement `acoshValue(value)` using `math.Acosh`
- [ ] 5.6 Implement `atanhValue(value)` using `math.Atanh`
- [ ] 5.7 Add SINH, COSH, TANH cases to expression evaluator
- [ ] 5.8 Add ASINH, ACOSH, ATANH cases to expression evaluator
- [ ] 5.9 Write unit tests for all hyperbolic functions
- [ ] 5.10 Test special values: SINH(0), COSH(0), TANH(0)
- [ ] 5.11 Integration test: Hyperbolic calculations

## 6. Utility Functions

- [ ] 6.1 Implement `piValue()` returning `math.Pi`
- [ ] 6.2 Implement `randomValue()` using `math.Float64`
- [ ] 6.3 Implement `gcdValue(a, b)` for greatest common divisor
- [ ] 6.4 Implement `lcmValue(a, b)` for least common multiple
- [ ] 6.5 Implement `isnanValue(value)` using `math.IsNaN`
- [ ] 6.6 Implement `isinfValue(value)` using `math.IsInf`
- [ ] 6.7 Implement `isfiniteValue(value)` checking !NaN && !Inf
- [ ] 6.8 Add PI, RANDOM cases to expression evaluator
- [ ] 6.9 Add GCD, LCM cases to expression evaluator
- [ ] 6.10 Add ISNAN, ISINF, ISFINITE cases to expression evaluator
- [ ] 6.11 Write unit tests for all utility functions
- [ ] 6.12 Test GCD/LCM with various integer pairs
- [ ] 6.13 Test ISNAN/ISINF with NaN, Infinity, and normal values

## 7. Bitwise Operators - Parser Integration

- [ ] 7.1 Add bitwise operator tokens to `internal/parser/parser_scanner.go`
- [ ] 7.2 Define TOKEN_AMPERSAND (&)
- [ ] 7.3 Define TOKEN_PIPE (|)
- [ ] 7.4 Define TOKEN_CARET (^)
- [ ] 7.5 Define TOKEN_TILDE (~)
- [ ] 7.6 Define TOKEN_SHIFT_LEFT (<<)
- [ ] 7.7 Define TOKEN_SHIFT_RIGHT (>>)
- [ ] 7.8 Update scanner to recognize bitwise operator tokens
- [ ] 7.9 Add bitwise operator precedence to parser
- [ ] 7.10 Implement `parseBitwiseExpression()` in parser
- [ ] 7.11 Write parser tests for bitwise operators

## 8. Bitwise Operators - Execution

- [ ] 8.1 Implement `evaluateBitwiseAnd(left, right)` for & operator
- [ ] 8.2 Implement `evaluateBitwiseOr(left, right)` for | operator
- [ ] 8.3 Implement `evaluateBitwiseXor(left, right)` for ^ operator
- [ ] 8.4 Implement `evaluateBitwiseNot(value)` for ~ operator
- [ ] 8.5 Implement `evaluateBitwiseShiftLeft(left, right)` for << operator
- [ ] 8.6 Implement `evaluateBitwiseShiftRight(left, right)` for >> operator
- [ ] 8.7 Implement `bitCountValue(value)` using `bits.OnesCount64`
- [ ] 8.8 Add bitwise operator cases to `evaluateBinaryOp` in expr.go
- [ ] 8.9 Add BIT_COUNT function case to expression evaluator
- [ ] 8.10 Write unit tests for all bitwise operators
- [ ] 8.11 Test edge cases: 0 & x, x | 0, shift by 0, shift by 64
- [ ] 8.12 Integration test: Bitwise operations in WHERE clauses

## 9. Type Inference

- [ ] 9.1 Add `inferMathFunctionType()` to `internal/binder/type_inference.go`
- [ ] 9.2 Define return types for rounding functions (preserve input type for integers)
- [ ] 9.3 Define return types for scientific functions (DOUBLE)
- [ ] 9.4 Define return types for trigonometric functions (DOUBLE)
- [ ] 9.5 Define return types for utility functions (varies by function)
- [ ] 9.6 Define return type for FACTORIAL (BIGINT)
- [ ] 9.7 Define return type for BIT_COUNT (INTEGER)
- [ ] 9.8 Define return types for ISNAN/ISINF/ISFINITE (BOOLEAN)
- [ ] 9.9 Add type coercion for INTEGER → DOUBLE where needed
- [ ] 9.10 Write unit tests for type inference
- [ ] 9.11 Integration test: Type preservation and coercion

## 10. NULL Handling

- [ ] 10.1 Add NULL checks to all math function implementations
- [ ] 10.2 Return NULL for any function with NULL input
- [ ] 10.3 Ensure NULL propagation for two-argument functions
- [ ] 10.4 Write unit tests for NULL propagation
- [ ] 10.5 Test NULL handling in all math functions
- [ ] 10.6 Test NULL handling in bitwise operators

## 11. Error Handling

- [ ] 11.1 Add domain error for SQRT of negative numbers
- [ ] 11.2 Add domain error for LN/LOG of non-positive numbers
- [ ] 11.3 Add domain error for ASIN/ACOS outside [-1, 1]
- [ ] 11.4 Add overflow error for FACTORIAL(>20)
- [ ] 11.5 Add clear error messages for all domain violations
- [ ] 11.6 Write unit tests for all error cases
- [ ] 11.7 Integration test: Error handling in complex queries

## 12. DuckDB Compatibility Testing

- [ ] 12.1 Create compatibility test suite comparing dukdb-go vs DuckDB CLI
- [ ] 12.2 Test ROUND with various precision values
- [ ] 12.3 Test ROUND_EVEN banker's rounding behavior
- [ ] 12.4 Test all scientific functions (SQRT, POW, EXP, LOG family)
- [ ] 12.5 Test all trigonometric functions (SIN, COS, TAN, inverses)
- [ ] 12.6 Test hyperbolic functions
- [ ] 12.7 Test utility functions (PI, GCD, LCM, ISNAN, ISINF, ISFINITE)
- [ ] 12.8 Test bitwise operators match DuckDB behavior
- [ ] 12.9 Verify error messages match DuckDB wording
- [ ] 12.10 Compare floating-point precision (accept IEEE 754 tolerance)

## 13. Integration Tests

- [ ] 13.1 Test math functions in SELECT clauses
- [ ] 13.2 Test math functions in WHERE clauses
- [ ] 13.3 Test math functions in computed columns
- [ ] 13.4 Test math functions with aggregate functions
- [ ] 13.5 Test nested math function calls: `SELECT SQRT(POW(x, 2) + POW(y, 2))`
- [ ] 13.6 Test bitwise operators in complex expressions
- [ ] 13.7 Test type coercion in mixed-type expressions
- [ ] 13.8 Integration test: Financial calculations (ROUND for currency)
- [ ] 13.9 Integration test: Scientific calculations (trigonometry)
- [ ] 13.10 Integration test: Bitwise operations for flags/masks

## 14. Performance Testing

- [ ] 14.1 Benchmark rounding functions (ROUND, CEIL, FLOOR)
- [ ] 14.2 Benchmark scientific functions (SQRT, POW, EXP, LOG)
- [ ] 14.3 Benchmark trigonometric functions (SIN, COS, TAN)
- [ ] 14.4 Benchmark bitwise operators
- [ ] 14.5 Profile memory usage for math operations
- [ ] 14.6 Compare performance with DuckDB (target: within 2x)
- [ ] 14.7 Identify optimization opportunities

## 15. Documentation

- [ ] 15.1 Document all math functions in user guide
- [ ] 15.2 Document ROUND precision parameter behavior
- [ ] 15.3 Document banker's rounding (ROUND_EVEN)
- [ ] 15.4 Document trigonometric functions use radians
- [ ] 15.5 Document domain restrictions (SQRT ≥ 0, LOG > 0, ASIN/ACOS in [-1,1])
- [ ] 15.6 Document bitwise operators work on INTEGER types only
- [ ] 15.7 Add examples for common use cases
- [ ] 15.8 Document type coercion behavior
- [ ] 15.9 Document NULL propagation
- [ ] 15.10 Document error handling behavior

## 16. Validation and Release

- [ ] 16.1 Run full test suite (unit + integration + compatibility)
- [ ] 16.2 Validate all math functions work correctly
- [ ] 16.3 Verify error handling is comprehensive
- [ ] 16.4 Check performance benchmarks are acceptable
- [ ] 16.5 Update CHANGELOG with math function support
- [ ] 16.6 Update README with math function examples
- [ ] 16.7 Create migration guide (no breaking changes, additive only)

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
