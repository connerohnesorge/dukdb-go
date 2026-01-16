# Design: Essential Math Functions Implementation

## Implementation Details

### 1. Math Function Registry

**Location**: `internal/executor/math.go` (NEW)

```go
package executor

import (
	"math"
	"math/bits"
)

// Math function implementations using Go's math package

// Rounding Functions

func roundValue(value any, decimals any) (any, error) {
	v := toFloat64(value)
	d := toInt64(decimals)

	if d == 0 {
		return math.Round(v), nil
	}

	multiplier := math.Pow(10, float64(d))
	return math.Round(v*multiplier) / multiplier, nil
}

func ceilValue(value any) (any, error) {
	return math.Ceil(toFloat64(value)), nil
}

func floorValue(value any) (any, error) {
	return math.Floor(toFloat64(value)), nil
}

func truncValue(value any) (any, error) {
	return math.Trunc(toFloat64(value)), nil
}

func roundEvenValue(value any, decimals any) (any, error) {
	// Banker's rounding (round half to even)
	v := toFloat64(value)
	d := toInt64(decimals)

	if d == 0 {
		return math.RoundToEven(v), nil
	}

	multiplier := math.Pow(10, float64(d))
	return math.RoundToEven(v*multiplier) / multiplier, nil
}

// Scientific Functions

func sqrtValue(value any) (any, error) {
	v := toFloat64(value)
	if v < 0 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "SQRT of negative number not allowed",
		}
	}
	return math.Sqrt(v), nil
}

func cbrtValue(value any) (any, error) {
	return math.Cbrt(toFloat64(value)), nil
}

func powValue(base any, exponent any) (any, error) {
	b := toFloat64(base)
	e := toFloat64(exponent)
	return math.Pow(b, e), nil
}

func expValue(value any) (any, error) {
	return math.Exp(toFloat64(value)), nil
}

func lnValue(value any) (any, error) {
	v := toFloat64(value)
	if v <= 0 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "LN of non-positive number not allowed",
		}
	}
	return math.Log(v), nil
}

func log10Value(value any) (any, error) {
	v := toFloat64(value)
	if v <= 0 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "LOG of non-positive number not allowed",
		}
	}
	return math.Log10(v), nil
}

func log2Value(value any) (any, error) {
	v := toFloat64(value)
	if v <= 0 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "LOG2 of non-positive number not allowed",
		}
	}
	return math.Log2(v), nil
}

// Trigonometric Functions

func sinValue(value any) (any, error) {
	return math.Sin(toFloat64(value)), nil
}

func cosValue(value any) (any, error) {
	return math.Cos(toFloat64(value)), nil
}

func tanValue(value any) (any, error) {
	return math.Tan(toFloat64(value)), nil
}

func asinValue(value any) (any, error) {
	v := toFloat64(value)
	if v < -1 || v > 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "ASIN domain error: input must be in [-1, 1]",
		}
	}
	return math.Asin(v), nil
}

func acosValue(value any) (any, error) {
	v := toFloat64(value)
	if v < -1 || v > 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "ACOS domain error: input must be in [-1, 1]",
		}
	}
	return math.Acos(v), nil
}

func atanValue(value any) (any, error) {
	return math.Atan(toFloat64(value)), nil
}

func atan2Value(y any, x any) (any, error) {
	return math.Atan2(toFloat64(y), toFloat64(x)), nil
}

// Utility Functions

func piValue() (any, error) {
	return math.Pi, nil
}

func randomValue() (any, error) {
	return math.Float64(), nil
}

func isnanValue(value any) (any, error) {
	return math.IsNaN(toFloat64(value)), nil
}

func isinfValue(value any) (any, error) {
	return math.IsInf(toFloat64(value), 0), nil
}

func isfiniteValue(value any) (any, error) {
	v := toFloat64(value)
	return !math.IsNaN(v) && !math.IsInf(v, 0), nil
}
```

### 2. Expression Evaluator Integration

**Location**: `internal/executor/expr.go`

```go
// Add to evaluateFunction switch statement

case "ROUND":
	if len(args) < 1 || len(args) > 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "ROUND requires 1 or 2 arguments",
		}
	}
	decimals := int64(0)
	if len(args) == 2 {
		decimals = toInt64(args[1])
	}
	return roundValue(args[0], decimals)

case "CEIL", "CEILING":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "CEIL requires 1 argument",
		}
	}
	return ceilValue(args[0])

case "FLOOR":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "FLOOR requires 1 argument",
		}
	}
	return floorValue(args[0])

case "SQRT":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "SQRT requires 1 argument",
		}
	}
	return sqrtValue(args[0])

case "POW", "POWER":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "POW requires 2 arguments",
		}
	}
	return powValue(args[0], args[1])

case "SIN":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "SIN requires 1 argument",
		}
	}
	return sinValue(args[0])

case "COS":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "COS requires 1 argument",
		}
	}
	return cosValue(args[0])

case "TAN":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "TAN requires 1 argument",
		}
	}
	return tanValue(args[0])

case "PI":
	if len(args) != 0 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "PI requires 0 arguments",
		}
	}
	return piValue()

case "RANDOM":
	if len(args) != 0 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "RANDOM requires 0 arguments",
		}
	}
	return randomValue()

// ... Similar cases for all other math functions
```

### 3. Bitwise Operator Implementation

**Location**: `internal/executor/expr.go` (modify evaluateBinaryOp)

```go
// Add bitwise operators to binary expression evaluation

func evaluateBitwiseAnd(left, right any) (any, error) {
	l := toInt64(left)
	r := toInt64(right)
	return l & r, nil
}

func evaluateBitwiseOr(left, right any) (any, error) {
	l := toInt64(left)
	r := toInt64(right)
	return l | r, nil
}

func evaluateBitwiseXor(left, right any) (any, error) {
	l := toInt64(left)
	r := toInt64(right)
	return l ^ r, nil
}

func evaluateBitwiseNot(value any) (any, error) {
	v := toInt64(value)
	return ^v, nil
}

func evaluateBitwiseShiftLeft(left, right any) (any, error) {
	l := toInt64(left)
	r := toUint64(right)
	return l << r, nil
}

func evaluateBitwiseShiftRight(left, right any) (any, error) {
	l := toInt64(left)
	r := toUint64(right)
	return l >> r, nil
}

func bitCountValue(value any) (any, error) {
	v := toUint64(value)
	return bits.OnesCount64(v), nil
}
```

### 4. Parser Integration for Bitwise Operators

**Location**: `internal/parser/parser_scanner.go`

```go
// Add bitwise operator tokens

const (
	// Existing tokens...

	// Bitwise operators
	TOKEN_AMPERSAND       TokenType = iota + 100 // &
	TOKEN_PIPE                                   // |
	TOKEN_CARET                                  // ^
	TOKEN_TILDE                                  // ~
	TOKEN_SHIFT_LEFT                             // <<
	TOKEN_SHIFT_RIGHT                            // >>
)
```

**Location**: `internal/parser/parser.go`

```go
// Update parseBinaryExpression to handle bitwise operators

func (p *Parser) parseBitwiseExpression() Expr {
	left := p.parseUnaryExpression()

	for {
		switch p.current.Type {
		case TOKEN_AMPERSAND:
			p.advance()
			right := p.parseUnaryExpression()
			left = &BinaryExpr{
				Left:     left,
				Operator: "&",
				Right:    right,
			}
		case TOKEN_PIPE:
			p.advance()
			right := p.parseUnaryExpression()
			left = &BinaryExpr{
				Left:     left,
				Operator: "|",
				Right:    right,
			}
		case TOKEN_CARET:
			p.advance()
			right := p.parseUnaryExpression()
			left = &BinaryExpr{
				Left:     left,
				Operator: "^",
				Right:    right,
			}
		case TOKEN_SHIFT_LEFT:
			p.advance()
			right := p.parseUnaryExpression()
			left = &BinaryExpr{
				Left:     left,
				Operator: "<<",
				Right:    right,
			}
		case TOKEN_SHIFT_RIGHT:
			p.advance()
			right := p.parseUnaryExpression()
			left = &BinaryExpr{
				Left:     left,
				Operator: ">>",
				Right:    right,
			}
		default:
			return left
		}
	}
}
```

### 5. Type Inference

**Location**: `internal/binder/type_inference.go`

```go
// Math functions return DOUBLE except where noted

func inferMathFunctionType(fnName string, argTypes []Type) Type {
	switch fnName {
	case "ROUND", "CEIL", "CEILING", "FLOOR", "TRUNC", "ROUND_EVEN", "EVEN":
		// Rounding functions preserve input type
		if len(argTypes) > 0 {
			switch argTypes[0] {
			case TypeInteger, TypeBigint:
				return TypeBigint
			default:
				return TypeDouble
			}
		}
		return TypeDouble

	case "FACTORIAL":
		// Factorial returns BIGINT
		return TypeBigint

	case "BIT_COUNT":
		// BIT_COUNT returns INTEGER
		return TypeInteger

	case "ISNAN", "ISINF", "ISFINITE":
		// Boolean-returning functions
		return TypeBoolean

	case "GCD", "LCM":
		// Number theory functions return BIGINT
		return TypeBigint

	default:
		// Most math functions return DOUBLE
		return TypeDouble
	}
}
```

### 6. NULL Handling

All math functions follow DuckDB's NULL propagation rule:
- If any input is NULL → output is NULL
- No special casing except for NaN/Infinity checks

```go
// Add to each math function

if value == nil {
	return nil, nil  // NULL in → NULL out
}
```

## Context

**Problem**: Users cannot perform basic mathematical operations in SQL queries without workarounds.

**Constraints**:
- Must maintain pure Go (no cgo)
- Must match DuckDB v1.4.3 behavior exactly
- Must handle edge cases (NaN, Infinity, domain errors)
- Performance-critical for numerical queries

**Stakeholders**:
- Data analysts performing numerical analysis
- Scientific computing users
- Financial application developers
- Users migrating from DuckDB CLI

## Goals / Non-Goals

**Goals**:
- ✅ Full parity with DuckDB v1.4.3 math functions (45 functions)
- ✅ Accurate numerical results matching Go's math package
- ✅ Proper error handling for domain violations
- ✅ NULL propagation compatibility
- ✅ Bitwise operators for INTEGER types
- ✅ Type coercion matching DuckDB behavior

**Non-Goals**:
- ❌ Extended precision math (use DECIMAL type, not in scope)
- ❌ Custom rounding modes beyond standard and banker's
- ❌ BITSTRING type support (separate feature)
- ❌ Performance optimization beyond Go stdlib (defer to future)

## Decisions

### Decision 1: Rounding Algorithm
**Choice**: Use Go's math.Round (half away from zero) for ROUND, math.RoundToEven for ROUND_EVEN
**Rationale**:
- Matches DuckDB behavior
- Go stdlib provides both rounding modes
- Standard half-up rounding for ROUND
- Banker's rounding for ROUND_EVEN

**Alternatives Considered**:
- Custom rounding implementation: Unnecessary, Go stdlib sufficient
- Only half-up rounding: Missing banker's rounding compatibility

### Decision 2: Domain Error Handling
**Choice**: Return errors (not NULL) for domain violations (SQRT of negative, LOG of zero)
**Rationale**:
- Matches DuckDB behavior exactly
- Prevents silent data corruption
- Makes errors visible to users

**Alternatives Considered**:
- Return NULL: Hides errors, not DuckDB-compatible
- Return NaN: Confusing, harder to debug

### Decision 3: PI Constant Precision
**Choice**: Use math.Pi constant from Go stdlib (15 decimal places)
**Rationale**:
- Matches DuckDB precision
- Sufficient for all practical use cases
- Standard across systems

**Alternatives Considered**:
- Custom high-precision constant: Overkill, unnecessary

### Decision 4: Bitwise Operators on INTEGER Only
**Choice**: Restrict bitwise operators to INTEGER/BIGINT types
**Rationale**:
- Matches DuckDB behavior
- Bitwise operations on floats are undefined
- Clear type semantics

**Alternatives Considered**:
- Allow floats: Non-standard, confusing behavior

## Risks / Trade-offs

### Risk 1: Floating-Point Precision Differences
**Risk**: Go's math package may have slight precision differences from DuckDB's C++ implementation
**Mitigation**:
- Comprehensive test suite comparing outputs
- Document any known precision edge cases
- Accept differences within IEEE 754 tolerance

### Risk 2: Performance of Trigonometric Functions
**Risk**: Trig functions are computationally expensive
**Mitigation**:
- Use Go's optimized math package (already fast)
- Defer advanced optimizations (lookup tables) to future work
- Profile performance during testing

### Trade-off: Simplicity vs Performance
**Choice**: Use Go stdlib directly without custom optimizations in v1
**Rationale**:
- Get correct behavior first
- Go's math package is well-optimized
- Premature optimization avoided
- Can optimize later if benchmarks show need

## Migration Plan

**No migration needed** - this is additive functionality.

**Compatibility**:
- Existing queries work unchanged
- New math functions are opt-in
- No breaking changes to existing behavior

**Rollout**:
1. Phase 1: Rounding and basic scientific functions (ROUND, CEIL, FLOOR, SQRT, POW, EXP, LOG family)
2. Phase 2: Trigonometric functions (SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2)
3. Phase 3: Hyperbolic and special functions (SINH, COSH, TANH, GAMMA, FACTORIAL)
4. Phase 4: Bitwise operators and utility functions

## Open Questions

1. **Q**: Should RANDOM() use crypto/rand for security or math/rand for performance?
   **A**: Use math/rand (matches DuckDB, not cryptographically secure, faster)

2. **Q**: How to handle FACTORIAL of large numbers (overflow)?
   **A**: Return error for FACTORIAL > 20 (overflow point for int64)

3. **Q**: Should bitwise NOT (~) work on all integer widths or just BIGINT?
   **A**: Work on all integer types, coerce to BIGINT for result

4. **Q**: Precision for ROUND with negative decimals parameter (round to tens, hundreds, etc.)?
   **A**: Support negative decimals, match DuckDB behavior: ROUND(123.45, -1) = 120

## Performance Considerations

**Benchmarks to Add**:
- ROUND with various precision values
- SQRT on large datasets
- Trigonometric functions (SIN, COS) performance
- Bitwise operations throughput

**Expected Performance**:
- Rounding functions: ~10-50ns per operation
- Scientific functions (SQRT, POW): ~50-200ns per operation
- Trigonometric functions: ~100-300ns per operation (Go stdlib optimized)
- Bitwise operations: ~5-10ns per operation (direct CPU instructions)

**Optimization Opportunities** (future):
- Vectorize math operations for DataChunk processing
- SIMD acceleration for batch operations
- Memoization for expensive functions (GAMMA, FACTORIAL)
- Constant folding in query optimization
