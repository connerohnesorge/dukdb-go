# Design: Missing Function Aliases and Small Scalar Functions

## Architecture

All changes are in two files:
1. **Executor** (`internal/executor/expr.go`): Add case labels and function implementations
2. **Binder** (`internal/binder/utils.go`): Add type inference entries

No parser or AST changes needed — all functions use the existing function call dispatch.

## 1. Simple Aliases (Add Case Labels)

### DATETRUNC → DATE_TRUNC

**File:** `internal/executor/expr.go`, line 1694

Current:
```go
case "DATE_TRUNC":
    return evalDateTrunc(args)
```

Change to:
```go
case "DATE_TRUNC", "DATETRUNC":
    return evalDateTrunc(args)
```

### DATEADD → DATE_ADD

**File:** `internal/executor/expr.go`, line 1685

Current:
```go
case "DATE_ADD":
    return evalDateAdd(args)
```

Change to:
```go
case "DATE_ADD", "DATEADD":
    return evalDateAdd(args)
```

### ORD → ASCII

**File:** `internal/executor/expr.go`, line 1515

Current:
```go
case "ASCII":
    // ... argument check ...
    return asciiValue(args[0])
```

Change to:
```go
case "ASCII", "ORD":
    // ... argument check ...
    return asciiValue(args[0])
```

### Binder changes for aliases

**File:** `internal/binder/utils.go`, in `inferFunctionResultType()`

Add aliases to existing cases:
- DATETRUNC: add to the DATE_TRUNC case that returns `dukdb.TYPE_TIMESTAMP` (around line 489)
- DATEADD: add to the DATE_ADD case that returns first-arg type (around line 480)
- ORD: add to the ASCII case that returns `dukdb.TYPE_BIGINT` (around line 444)

## 2. IFNULL and NVL Implementation

### Executor

**File:** `internal/executor/expr.go`, add near COALESCE (line 1162):

```go
case "IFNULL", "NVL":
    if len(args) != 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("IFNULL requires exactly 2 arguments, got %d", len(args)),
        }
    }
    if args[0] != nil {
        return args[0], nil
    }
    return args[1], nil
```

**Semantics:** IFNULL(a, b) returns a if a is not NULL, else b. Identical to COALESCE(a, b) but enforces exactly 2 arguments. NVL is the Oracle-compatible alias.

### Binder

**File:** `internal/binder/utils.go`, add to `inferFunctionResultType()`:

```go
case "IFNULL", "NVL":
    return inferCoalesceResultType(args)
```

This reuses the existing `inferCoalesceResultType()` helper at utils.go:161 which handles type promotion across arguments.

### NULL propagation

IFNULL does NOT propagate NULL — it's specifically designed to handle NULL. If first arg is NULL, return second arg. If both are NULL, return NULL.

**Important:** The function dispatch in evaluateFunctionCall must evaluate BOTH arguments before the NULL check. The current code at expr.go evaluates args in a loop before the switch, so both args are already evaluated. No special handling needed.

## 3. BIT_LENGTH Implementation

### Executor

**File:** `internal/executor/expr.go`, add near BIT_COUNT (line 1016):

```go
case "BIT_LENGTH":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("BIT_LENGTH requires exactly 1 argument, got %d", len(args)),
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    switch v := args[0].(type) {
    case string:
        return int64(len(v) * 8), nil
    case []byte:
        return int64(len(v) * 8), nil
    default:
        s := toString(args[0])
        return int64(len(s) * 8), nil
    }
```

**Semantics:** Returns the number of bits in the string representation. For VARCHAR, this is `len(string) * 8`. For BLOB, `len(bytes) * 8`. Matches DuckDB/PostgreSQL behavior.

### Binder

Add `"BIT_LENGTH"` to the case returning `dukdb.TYPE_BIGINT` alongside OCTET_LENGTH, LENGTH, etc.

## 4. GET_BIT and SET_BIT Implementation

### GET_BIT

**File:** `internal/executor/expr.go`:

```go
case "GET_BIT":
    if len(args) != 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("GET_BIT requires exactly 2 arguments, got %d", len(args)),
        }
    }
    if args[0] == nil || args[1] == nil {
        return nil, nil
    }
    // Convert first arg to bytes
    var data []byte
    switch v := args[0].(type) {
    case string:
        data = []byte(v)
    case []byte:
        data = v
    default:
        data = []byte(toString(args[0]))
    }
    // Get bit index using toInt64Value (returns int64 directly)
    idx := toInt64Value(args[1])
    if idx < 0 || idx >= int64(len(data)*8) {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("GET_BIT: bit index %d out of range [0, %d)", idx, len(data)*8),
        }
    }
    byteIdx := idx / 8
    bitIdx := uint(idx % 8)
    return int64((data[byteIdx] >> (7 - bitIdx)) & 1), nil
```

### SET_BIT

```go
case "SET_BIT":
    if len(args) != 3 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("SET_BIT requires exactly 3 arguments (value, index, new_bit), got %d", len(args)),
        }
    }
    if args[0] == nil || args[1] == nil || args[2] == nil {
        return nil, nil
    }
    // Convert first arg to mutable byte slice
    var data []byte
    switch v := args[0].(type) {
    case string:
        data = []byte(v)
    case []byte:
        data = make([]byte, len(v))
        copy(data, v)
    default:
        data = []byte(toString(args[0]))
    }
    // Use toInt64Value (returns int64 directly, no error)
    idx := toInt64Value(args[1])
    newBit := toInt64Value(args[2])
    if newBit != 0 && newBit != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("SET_BIT: bit value must be 0 or 1, got %d", newBit),
        }
    }
    if idx < 0 || idx >= int64(len(data)*8) {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("SET_BIT: bit index %d out of range [0, %d)", idx, len(data)*8),
        }
    }
    byteIdx := idx / 8
    bitIdx := uint(idx % 8)
    if newBit == 1 {
        data[byteIdx] |= 1 << (7 - bitIdx)
    } else {
        data[byteIdx] &^= 1 << (7 - bitIdx)
    }
    // Return same type as input
    if _, ok := args[0].(string); ok {
        return string(data), nil
    }
    return data, nil
```

### Binder

- GET_BIT: returns `dukdb.TYPE_INTEGER` (single bit value 0 or 1)
- SET_BIT: returns type of first argument (same as input)

## 5. ENCODE and DECODE Implementation

### ENCODE(string, encoding)

DuckDB's ENCODE converts a string to a BLOB using a specified encoding. DuckDB supports 'UTF-8' (default), 'LATIN1', 'ASCII'.

**File:** `internal/executor/expr.go`:

```go
case "ENCODE":
    if len(args) < 1 || len(args) > 2 {
        return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("ENCODE requires 1-2 arguments, got %d", len(args))}
    }
    if args[0] == nil {
        return nil, nil
    }
    s := toString(args[0])
    encoding := "UTF-8"
    if len(args) == 2 && args[1] != nil {
        encoding = strings.ToUpper(toString(args[1]))
    }
    switch encoding {
    case "UTF-8", "UTF8":
        return []byte(s), nil
    case "LATIN1", "ISO-8859-1", "ISO88591":
        // Use golang.org/x/text/encoding/charmap
        encoder := charmap.ISO8859_1.NewEncoder()
        encoded, err := encoder.Bytes([]byte(s))
        if err != nil {
            return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("ENCODE: cannot encode to %s: %v", encoding, err)}
        }
        return encoded, nil
    case "ASCII":
        // Strip non-ASCII bytes
        result := make([]byte, 0, len(s))
        for _, b := range []byte(s) {
            if b <= 127 {
                result = append(result, b)
            }
        }
        return result, nil
    default:
        return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("ENCODE: unsupported encoding %q", encoding)}
    }
```

### DECODE(blob, encoding)

```go
case "DECODE":
    if len(args) < 1 || len(args) > 2 {
        return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("DECODE requires 1-2 arguments, got %d", len(args))}
    }
    if args[0] == nil {
        return nil, nil
    }
    var data []byte
    switch v := args[0].(type) {
    case []byte:
        data = v
    case string:
        data = []byte(v)
    default:
        data = []byte(toString(args[0]))
    }
    encoding := "UTF-8"
    if len(args) == 2 && args[1] != nil {
        encoding = strings.ToUpper(toString(args[1]))
    }
    switch encoding {
    case "UTF-8", "UTF8":
        return string(data), nil
    case "LATIN1", "ISO-8859-1", "ISO88591":
        decoder := charmap.ISO8859_1.NewDecoder()
        decoded, err := decoder.Bytes(data)
        if err != nil {
            return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("DECODE: cannot decode from %s: %v", encoding, err)}
        }
        return string(decoded), nil
    case "ASCII":
        return string(data), nil
    default:
        return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf("DECODE: unsupported encoding %q", encoding)}
    }
```

### Import required

Add import for `golang.org/x/text/encoding/charmap` in the file that implements ENCODE/DECODE. This package is already a transitive dependency via golang.org/x/text v0.34.0 in go.mod.

### Binder

- ENCODE: returns `dukdb.TYPE_BLOB`
- DECODE: returns `dukdb.TYPE_VARCHAR`

## Helper Signatures Reference

- `toString(v any) string` — `expr.go:3710` — converts any value to string
- `toInt64(v any) (int64, bool)` — `math.go:75` — numeric conversion, returns (value, ok) pair
- `toInt64Value(v any) int64` — `expr.go:3718` — simpler numeric conversion, returns int64 directly
- `asciiValue(char any) (any, error)` — `string.go:283` — returns ASCII code of first char
- `evalDateTrunc(args []any) (any, error)` — `temporal_functions.go:776`
- `evalDateAdd(args []any) (any, error)` — `temporal_functions.go:512`
- `inferCoalesceResultType(args []BoundExpr) dukdb.Type` — `utils.go:161`
- `bitCountValue(v any) (any, error)` — `math.go:1040` — used for BIT_COUNT

## Testing Strategy

Integration tests:
1. DATETRUNC: `SELECT DATETRUNC('month', DATE '2024-03-15')` → same as DATE_TRUNC
2. DATEADD: `SELECT DATEADD(INTERVAL '1' DAY, DATE '2024-03-15')` → same as DATE_ADD
3. ORD: `SELECT ORD('A')` → 65 (same as ASCII)
4. IFNULL: `SELECT IFNULL(NULL, 42)` → 42, `SELECT IFNULL(1, 42)` → 1
5. NVL: `SELECT NVL(NULL, 'default')` → 'default'
6. BIT_LENGTH: `SELECT BIT_LENGTH('hello')` → 40
7. GET_BIT: `SELECT GET_BIT(b'\x80', 0)` → 1
8. SET_BIT: `SELECT SET_BIT(b'\x00', 0, 1)` → b'\x80'
9. ENCODE: `SELECT ENCODE('hello', 'UTF-8')` → BLOB
10. DECODE: `SELECT DECODE(b'hello', 'UTF-8')` → 'hello'
11. NULL propagation: all functions return NULL on NULL input
12. Error cases: wrong argument counts, invalid encodings, out-of-range bit indices
