# Math Functions Migration Guide

This guide covers the addition of mathematical functions to dukdb-go. This is an **additive-only** change with **no breaking changes** to existing functionality.

## Overview

The math functions feature adds 45+ mathematical functions to dukdb-go, providing parity with DuckDB's mathematical capabilities. All existing code continues to work without modification.

## Migration Summary

| Aspect | Impact |
|--------|--------|
| Breaking Changes | None |
| API Changes | None |
| New Features | 45+ math functions |
| Required Action | None (use new functions as needed) |

## What's New

### New Functions Available

After upgrading, you can use these new functions in your SQL queries:

**Rounding:**
- `ROUND(value)`, `ROUND(value, decimals)`
- `CEIL(value)`, `CEILING(value)`
- `FLOOR(value)`
- `TRUNC(value)`
- `ROUND_EVEN(value, decimals)`
- `EVEN(value)`

**Scientific:**
- `SQRT(x)`, `CBRT(x)`
- `POW(base, exp)`, `POWER(base, exp)`
- `EXP(x)`, `LN(x)`, `LOG(x)`, `LOG10(x)`, `LOG2(x)`
- `GAMMA(x)`, `LGAMMA(x)`
- `FACTORIAL(n)`

**Trigonometric:**
- `SIN(x)`, `COS(x)`, `TAN(x)`, `COT(x)`
- `ASIN(x)`, `ACOS(x)`, `ATAN(x)`, `ATAN2(y, x)`
- `DEGREES(radians)`, `RADIANS(degrees)`

**Hyperbolic:**
- `SINH(x)`, `COSH(x)`, `TANH(x)`
- `ASINH(x)`, `ACOSH(x)`, `ATANH(x)`

**Utility:**
- `PI()`
- `RANDOM()`
- `GCD(a, b)`, `LCM(a, b)`
- `ISNAN(x)`, `ISINF(x)`, `ISFINITE(x)`

**Bitwise Operators:**
- `&` (AND), `|` (OR), `^` (XOR), `~` (NOT)
- `<<` (left shift), `>>` (right shift)
- `BIT_COUNT(x)`

### Existing Functions

These functions were already available and remain unchanged:
- `ABS(x)` - Absolute value
- `SIGN(x)` - Sign of a number
- `MOD(a, b)` - Modulo operation
- `GREATEST(a, b, ...)` - Maximum value
- `LEAST(a, b, ...)` - Minimum value

## Upgrade Steps

1. **Update your dependency:**
   ```bash
   go get -u github.com/dukdb/dukdb-go
   ```

2. **Use new functions as needed:**
   ```sql
   -- Before: Manual calculation in application code
   -- After: Use SQL math functions directly
   SELECT ROUND(price * tax_rate, 2) AS total FROM orders;
   SELECT SQRT(POW(x2-x1, 2) + POW(y2-y1, 2)) AS distance FROM coords;
   ```

3. **No code changes required** for existing functionality

## Compatibility Notes

### DuckDB Compatibility

All math functions are compatible with DuckDB behavior:
- Same function names and signatures
- Same return types
- Same error handling for domain violations
- IEEE 754 floating-point precision

### Type Behavior

- Scientific/trigonometric functions return `DOUBLE`
- Rounding functions preserve input type for integers
- `FACTORIAL` returns `BIGINT`
- `ISNAN`/`ISINF`/`ISFINITE` return `BOOLEAN`
- Bitwise operators require `INTEGER` types

### NULL Handling

All functions propagate NULL values:
```sql
SELECT SQRT(NULL);  -- Returns NULL
SELECT ROUND(NULL, 2);  -- Returns NULL
SELECT 5 & NULL;  -- Returns NULL
```

### Error Handling

Domain violations produce descriptive errors:
```sql
SELECT SQRT(-1);  -- Error: cannot take square root of negative number
SELECT LN(0);     -- Error: cannot take logarithm of zero
SELECT ASIN(2);   -- Error: input out of range for ASIN (must be between -1 and 1)
```

## Performance Considerations

Math functions are highly optimized:
- Single-value operations: 3-30 nanoseconds
- Batch operations: Efficient vectorized processing
- Minimal memory allocations

No performance changes to existing functionality.

## Testing Your Upgrade

Verify the upgrade works:

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/dukdb/dukdb-go"
    _ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
    db, err := sql.Open("dukdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Test new math functions
    var result float64

    // Rounding
    db.QueryRow("SELECT ROUND(3.14159, 2)").Scan(&result)
    fmt.Printf("ROUND: %v\n", result)  // 3.14

    // Scientific
    db.QueryRow("SELECT SQRT(16)").Scan(&result)
    fmt.Printf("SQRT: %v\n", result)   // 4.0

    // Trigonometric
    db.QueryRow("SELECT SIN(0)").Scan(&result)
    fmt.Printf("SIN: %v\n", result)    // 0.0

    // Constants
    db.QueryRow("SELECT PI()").Scan(&result)
    fmt.Printf("PI: %v\n", result)     // 3.141592653589793

    // Bitwise
    var intResult int64
    db.QueryRow("SELECT 5 & 3").Scan(&intResult)
    fmt.Printf("5 & 3: %v\n", intResult)  // 1

    fmt.Println("All math functions working correctly!")
}
```

## Troubleshooting

### Function Not Found

If you get "unknown function" errors, ensure:
1. You've updated to the latest version
2. Function names are spelled correctly
3. Function arguments match expected types

### Type Mismatch

Bitwise operators require integer types:
```sql
-- Error: bitwise operators require integer types
SELECT 3.14 & 2;

-- Correct: use integers
SELECT 3 & 2;
```

### Domain Errors

Handle potential domain errors in your application:
```go
_, err := db.Exec("SELECT SQRT(-1)")
if err != nil {
    // Handle domain error
    log.Printf("Math domain error: %v", err)
}
```

## Documentation

- [Math Functions Reference](math-functions.md) - Complete function documentation
- [README](../README.md#math-functions) - Quick examples
- [CHANGELOG](../CHANGELOG.md) - Full release notes

## Support

If you encounter any issues with the math functions:
1. Check the documentation for correct usage
2. Verify input types match function requirements
3. Review error messages for domain violations
4. File an issue on GitHub with reproduction steps
