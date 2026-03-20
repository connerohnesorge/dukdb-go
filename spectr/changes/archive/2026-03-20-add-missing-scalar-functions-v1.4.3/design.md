# Design: Missing Scalar Functions for DuckDB v1.4.3

## Architecture

All functions follow the established two-layer pattern:

1. **Binder** (`internal/binder/utils.go`, `inferFunctionResultType()`): Add return type inference
2. **Executor** (`internal/executor/expr.go`, function dispatch switch): Add evaluation logic

No parser changes needed — all functions use standard function call syntax `FUNC(args...)` which the parser already handles.

## 1. IF/IFF Implementation

### Binder (internal/binder/utils.go)

Add to `inferFunctionResultType()`:
```go
case "IF", "IFF":
    if len(args) == 3 {
        // Return type is common supertype of true and false branches
        return promoteType(args[1].ResultType(), args[2].ResultType())
    }
    return dukdb.TYPE_ANY
```

### Executor (internal/executor/expr.go)

Add to function dispatch switch:
```go
case "IF", "IFF":
    if len(args) != 3 {
        return nil, &dukdb.Error{
            Type:    dukdb.ErrorTypeExecutor,
            Msg: "IF requires exactly 3 arguments: IF(condition, true_val, false_val)",
        }
    }
    // Evaluate condition
    cond := args[0]
    // NULL condition → false branch (NOT standard NULL propagation)
    if cond == nil {
        return args[2], nil
    }
    // Convert to boolean — toBool() returns a single bool (no error)
    boolVal := toBool(cond)
    if boolVal {
        return args[1], nil
    }
    return args[2], nil
```

**Note on short-circuit evaluation:** The current executor evaluates all function arguments before dispatching. True short-circuit would require changes to the expression evaluator to defer argument evaluation. For now, all three arguments are evaluated, and only the return value is selected. This matches DuckDB behavior — DuckDB also evaluates all arguments.

### IF vs SQL IF Keyword

The parser distinguishes between the SQL `IF` keyword (used in `IF EXISTS`) and the `IF()` function call by context. In function call position (`IF(...)` with parentheses), the parser creates a `FunctionCall` AST node, not an IF keyword. This is already handled by the parser.

Verification: The parser handles `IF` as a keyword in keyword_suggestions.go but function calls like `IF(...)` are parsed as `FunctionCall` nodes via the expression parser, so there's no conflict.

## 2. FORMAT/PRINTF Implementation

### Binder (internal/binder/utils.go)

```go
case "FORMAT", "PRINTF":
    return dukdb.TYPE_VARCHAR
```

### Executor (internal/executor/expr.go)

Create a new file `internal/executor/format.go` for cleanliness:

```go
package executor

import (
    "fmt"
    "strings"
    "unicode/utf8"
)

// formatString implements DuckDB's FORMAT/PRINTF function.
// It parses a format string with %s, %d, %f, etc. specifiers
// and substitutes the provided arguments.
func formatString(formatStr string, args []any) (string, error) {
    if formatStr == "" {
        return "", nil
    }

    var result strings.Builder
    argIdx := 0
    i := 0

    for i < len(formatStr) {
        if formatStr[i] != '%' {
            result.WriteByte(formatStr[i])
            i++
            continue
        }

        // Found %, parse the format specifier
        i++ // skip %
        if i >= len(formatStr) {
            return "", fmt.Errorf("incomplete format specifier at end of string")
        }

        // %% → literal %
        if formatStr[i] == '%' {
            result.WriteByte('%')
            i++
            continue
        }

        // Parse flags, width, precision, specifier
        spec, consumed, err := parseFormatSpec(formatStr[i:])
        if err != nil {
            return "", err
        }
        i += consumed

        if argIdx >= len(args) {
            return "", fmt.Errorf("not enough arguments for format string")
        }

        // Format the argument using Go's fmt
        formatted, err := applyFormatSpec(spec, args[argIdx])
        if err != nil {
            return "", err
        }
        result.WriteString(formatted)
        argIdx++
    }

    return result.String(), nil
}

type formatSpec struct {
    flags     string // -, +, 0, space
    width     int
    hasWidth  bool
    precision int
    hasPrecision bool
    verb      byte // s, d, f, e, g, x, o, c
}

func parseFormatSpec(s string) (formatSpec, int, error) {
    var spec formatSpec
    i := 0

    // Parse flags
    for i < len(s) && (s[i] == '-' || s[i] == '+' || s[i] == '0' || s[i] == ' ') {
        spec.flags += string(s[i])
        i++
    }

    // Parse width
    for i < len(s) && s[i] >= '0' && s[i] <= '9' {
        spec.width = spec.width*10 + int(s[i]-'0')
        spec.hasWidth = true
        i++
    }

    // Parse precision
    if i < len(s) && s[i] == '.' {
        i++
        spec.hasPrecision = true
        for i < len(s) && s[i] >= '0' && s[i] <= '9' {
            spec.precision = spec.precision*10 + int(s[i]-'0')
            i++
        }
    }

    // Parse verb
    if i >= len(s) {
        return spec, i, fmt.Errorf("missing format specifier verb")
    }
    spec.verb = s[i]
    i++

    return spec, i, nil
}

func applyFormatSpec(spec formatSpec, arg any) (string, error) {
    // Build Go format string
    goFmt := "%"
    goFmt += spec.flags
    if spec.hasWidth {
        goFmt += fmt.Sprintf("%d", spec.width)
    }
    if spec.hasPrecision {
        goFmt += fmt.Sprintf(".%d", spec.precision)
    }

    // NOTE: toInt64() and toFloat64() return (value, bool) not (value, error).
    // Located in internal/executor/math.go.
    switch spec.verb {
    case 's':
        goFmt += "s"
        return fmt.Sprintf(goFmt, toString(arg)), nil
    case 'd':
        goFmt += "d"
        intVal, ok := toInt64(arg)
        if !ok {
            return "", fmt.Errorf("%%d requires integer argument, got %T", arg)
        }
        return fmt.Sprintf(goFmt, intVal), nil
    case 'f':
        goFmt += "f"
        floatVal, ok := toFloat64(arg)
        if !ok {
            return "", fmt.Errorf("%%f requires numeric argument, got %T", arg)
        }
        return fmt.Sprintf(goFmt, floatVal), nil
    case 'e':
        goFmt += "e"
        floatVal, ok := toFloat64(arg)
        if !ok {
            return "", fmt.Errorf("%%e requires numeric argument, got %T", arg)
        }
        return fmt.Sprintf(goFmt, floatVal), nil
    case 'g':
        goFmt += "g"
        floatVal, ok := toFloat64(arg)
        if !ok {
            return "", fmt.Errorf("%%g requires numeric argument, got %T", arg)
        }
        return fmt.Sprintf(goFmt, floatVal), nil
    case 'x':
        goFmt += "x"
        intVal, ok := toInt64(arg)
        if !ok {
            return "", fmt.Errorf("%%x requires integer argument, got %T", arg)
        }
        return fmt.Sprintf(goFmt, intVal), nil
    case 'o':
        goFmt += "o"
        intVal, ok := toInt64(arg)
        if !ok {
            return "", fmt.Errorf("%%o requires integer argument, got %T", arg)
        }
        return fmt.Sprintf(goFmt, intVal), nil
    case 'c':
        intVal, ok := toInt64(arg)
        if !ok {
            return "", fmt.Errorf("%%c requires integer argument, got %T", arg)
        }
        return string(rune(intVal)), nil
    default:
        return "", fmt.Errorf("unsupported format specifier: %%%c", spec.verb)
    }
}
```

### Executor dispatch (internal/executor/expr.go)

```go
case "FORMAT", "PRINTF":
    if len(args) < 1 {
        return nil, &dukdb.Error{
            Type:    dukdb.ErrorTypeExecutor,
            Msg: "FORMAT requires at least 1 argument",
        }
    }
    // NULL format string → NULL
    if args[0] == nil {
        return nil, nil
    }
    fmtStr, ok := args[0].(string)
    if !ok {
        fmtStr = fmt.Sprintf("%v", args[0])
    }
    result, err := formatString(fmtStr, args[1:])
    if err != nil {
        return nil, &dukdb.Error{
            Type:    dukdb.ErrorTypeExecutor,
            Msg: fmt.Sprintf("FORMAT error: %v", err),
        }
    }
    return result, nil
```

## 3. TYPEOF/PG_TYPEOF Implementation

### Binder (internal/binder/utils.go)

```go
case "TYPEOF", "PG_TYPEOF":
    return dukdb.TYPE_VARCHAR
```

**Bind-time optimization:** When the argument type is known at bind time, fold to a string literal:

```go
// In binder, when binding FunctionCall for TYPEOF/PG_TYPEOF:
if funcName == "TYPEOF" || funcName == "PG_TYPEOF" {
    argType := boundArgs[0].ResultType()
    if argType != dukdb.TYPE_ANY && argType != dukdb.TYPE_INVALID {
        typeName := typeToName(argType, funcName == "PG_TYPEOF")
        return &BoundLiteral{Value: typeName, Type: dukdb.TYPE_VARCHAR}, nil
    }
}
```

### Type Name Mappings

```go
var duckdbTypeNames = map[dukdb.Type]string{
    dukdb.TYPE_BOOLEAN:      "BOOLEAN",
    dukdb.TYPE_TINYINT:      "TINYINT",
    dukdb.TYPE_SMALLINT:     "SMALLINT",
    dukdb.TYPE_INTEGER:      "INTEGER",
    dukdb.TYPE_BIGINT:       "BIGINT",
    dukdb.TYPE_UTINYINT:     "UTINYINT",
    dukdb.TYPE_USMALLINT:    "USMALLINT",
    dukdb.TYPE_UINTEGER:     "UINTEGER",
    dukdb.TYPE_UBIGINT:      "UBIGINT",
    dukdb.TYPE_FLOAT:        "FLOAT",
    dukdb.TYPE_DOUBLE:       "DOUBLE",
    dukdb.TYPE_VARCHAR:      "VARCHAR",
    dukdb.TYPE_BLOB:         "BLOB",
    dukdb.TYPE_DATE:         "DATE",
    dukdb.TYPE_TIME:         "TIME",
    dukdb.TYPE_TIMESTAMP:    "TIMESTAMP",
    dukdb.TYPE_TIMESTAMP_TZ: "TIMESTAMP WITH TIME ZONE",
    dukdb.TYPE_INTERVAL:     "INTERVAL",
    dukdb.TYPE_DECIMAL:      "DECIMAL",
    dukdb.TYPE_HUGEINT:      "HUGEINT",
    dukdb.TYPE_UUID:         "UUID",
    dukdb.TYPE_JSON:         "JSON",
    dukdb.TYPE_LIST:         "LIST",   // Would include element type for full name
    dukdb.TYPE_MAP:          "MAP",
    dukdb.TYPE_STRUCT:       "STRUCT",
    dukdb.TYPE_UNION:        "UNION",
    dukdb.TYPE_ENUM:         "ENUM",
}

var pgTypeNames = map[dukdb.Type]string{
    dukdb.TYPE_BOOLEAN:      "boolean",
    dukdb.TYPE_TINYINT:      "tinyint",
    dukdb.TYPE_SMALLINT:     "smallint",
    dukdb.TYPE_INTEGER:      "integer",
    dukdb.TYPE_BIGINT:       "bigint",
    dukdb.TYPE_FLOAT:        "real",
    dukdb.TYPE_DOUBLE:       "double precision",
    dukdb.TYPE_VARCHAR:      "character varying",
    dukdb.TYPE_BLOB:         "bytea",
    dukdb.TYPE_DATE:         "date",
    dukdb.TYPE_TIME:         "time without time zone",
    dukdb.TYPE_TIMESTAMP:    "timestamp without time zone",
    dukdb.TYPE_TIMESTAMP_TZ: "timestamp with time zone",
    dukdb.TYPE_INTERVAL:     "interval",
    dukdb.TYPE_DECIMAL:      "numeric",
    dukdb.TYPE_UUID:         "uuid",
    dukdb.TYPE_JSON:         "json",
}
```

### Executor (internal/executor/expr.go)

```go
case "TYPEOF":
    if len(args) != 1 {
        return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "TYPEOF requires 1 argument"}
    }
    return typeOfValue(args[0], false), nil

case "PG_TYPEOF":
    if len(args) != 1 {
        return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "PG_TYPEOF requires 1 argument"}
    }
    return typeOfValue(args[0], true), nil
```

The `typeOfValue()` function inspects the Go runtime type of the value and maps to the appropriate type name string.

## 4. BASE64 Implementation

### Executor (internal/executor/expr.go)

```go
case "BASE64_ENCODE", "BASE64", "TO_BASE64":
    if len(args) != 1 {
        return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "BASE64_ENCODE requires 1 argument"}
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
        data = []byte(fmt.Sprintf("%v", v))
    }
    return base64.StdEncoding.EncodeToString(data), nil

case "BASE64_DECODE", "FROM_BASE64":
    if len(args) != 1 {
        return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "BASE64_DECODE requires 1 argument"}
    }
    if args[0] == nil {
        return nil, nil
    }
    str, ok := args[0].(string)
    if !ok {
        return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "BASE64_DECODE requires string argument"}
    }
    decoded, err := base64.StdEncoding.DecodeString(str)
    if err != nil {
        return nil, &dukdb.Error{
            Type:    dukdb.ErrorTypeExecutor,
            Msg: fmt.Sprintf("invalid base64 input: %v", err),
        }
    }
    return decoded, nil
```

### Binder

```go
case "BASE64_ENCODE", "BASE64", "TO_BASE64":
    return dukdb.TYPE_VARCHAR
case "BASE64_DECODE", "FROM_BASE64":
    return dukdb.TYPE_BLOB
```

## 5. URL Encoding Implementation

### Executor (internal/executor/expr.go)

```go
case "URL_ENCODE":
    if len(args) != 1 {
        return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "URL_ENCODE requires 1 argument"}
    }
    if args[0] == nil {
        return nil, nil
    }
    str := fmt.Sprintf("%v", args[0])
    return url.QueryEscape(str), nil

case "URL_DECODE":
    if len(args) != 1 {
        return nil, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "URL_DECODE requires 1 argument"}
    }
    if args[0] == nil {
        return nil, nil
    }
    str := fmt.Sprintf("%v", args[0])
    decoded, err := url.QueryUnescape(str)
    if err != nil {
        return nil, &dukdb.Error{
            Type:    dukdb.ErrorTypeExecutor,
            Msg: fmt.Sprintf("invalid URL-encoded input: %v", err),
        }
    }
    return decoded, nil
```

### Binder

```go
case "URL_ENCODE", "URL_DECODE":
    return dukdb.TYPE_VARCHAR
```

## Import Dependencies

New imports needed in executor:
- `encoding/base64` — for BASE64 functions
- `net/url` — for URL functions

These are Go standard library packages, no external dependencies added.

## Testing Strategy

Each function gets its own test file or test section. Tests verify:

1. **Happy path:** Normal inputs produce expected outputs
2. **NULL propagation:** NULL arguments → NULL return (except IF/IFF)
3. **Type validation:** Wrong argument types produce clear errors
4. **Edge cases:** Empty strings, zero values, special characters
5. **DuckDB compatibility:** Output matches DuckDB CLI for same inputs
