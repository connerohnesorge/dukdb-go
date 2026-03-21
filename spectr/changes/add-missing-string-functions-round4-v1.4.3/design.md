# Design: Missing String Functions Round 4

## Architecture

All functions are simple scalar additions to the evaluateFunctionCall() dispatch in expr.go. No parser or planner changes needed.

## 1. OCTET_LENGTH (byte count)

Returns the number of bytes in a string (vs LENGTH which returns character count — though in Go, len(string) returns bytes, so LENGTH currently returns bytes too). OCTET_LENGTH is SQL standard and always returns byte count.

Add near the existing LENGTH case at expr.go:1259:

```go
case "OCTET_LENGTH":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "OCTET_LENGTH requires 1 argument",
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    return int64(len(toString(args[0]))), nil
```

Type inference: `return dukdb.TYPE_INTEGER`

Note: In Go, `len(string)` returns byte count, which is correct for OCTET_LENGTH. The existing LENGTH at line 1259 also uses `len(toString(...))` which actually returns bytes too — this is a pre-existing inconsistency (LENGTH should return rune count via `utf8.RuneCountInString()`), but fixing that is out of scope for this proposal.

## 2. LCASE / UCASE (aliases for LOWER / UPPER)

Simply add to the existing case labels:

```go
// BEFORE (line 1235):
case "UPPER":
// AFTER:
case "UPPER", "UCASE":

// BEFORE (line 1247):
case "LOWER":
// AFTER:
case "LOWER", "LCASE":
```

Type inference: Add to existing UPPER/LOWER entry or add:
```go
case "LCASE":
    return dukdb.TYPE_VARCHAR
case "UCASE":
    return dukdb.TYPE_VARCHAR
```

## 3. INITCAP (capitalize first letter of each word)

PostgreSQL/DuckDB function that capitalizes the first letter of each word:

```go
case "INITCAP":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "INITCAP requires 1 argument",
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    s := toString(args[0])
    result := make([]byte, 0, len(s))
    capitalizeNext := true
    for i := 0; i < len(s); i++ {
        ch := s[i]
        if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' ||
           ch == '_' || ch == '-' || ch == '.' || ch == ',' ||
           ch == ';' || ch == ':' || ch == '!' || ch == '?' {
            capitalizeNext = true
            result = append(result, ch)
        } else if capitalizeNext {
            if ch >= 'a' && ch <= 'z' {
                result = append(result, ch-32)
            } else {
                result = append(result, ch)
            }
            capitalizeNext = false
        } else {
            if ch >= 'A' && ch <= 'Z' {
                result = append(result, ch+32)
            } else {
                result = append(result, ch)
            }
        }
    }
    return string(result), nil
```

DuckDB/PostgreSQL INITCAP behavior: capitalizes first letter of each word, lowercases the rest. Word boundaries are non-alphanumeric characters. Example: `INITCAP('hello WORLD foo')` → `'Hello World Foo'`.

Type inference: `return dukdb.TYPE_VARCHAR`

## 4. SOUNDEX (phonetic algorithm)

Returns a 4-character SOUNDEX code for a string. Standard algorithm:
1. Keep first letter (uppercase)
2. Map consonants to digits: B/F/P/V→1, C/G/J/K/Q/S/X/Z→2, D/T→3, L→4, M/N→5, R→6
3. Remove A/E/I/O/U/H/W/Y (vowels)
4. Remove consecutive duplicates
5. Pad with zeros to 4 characters

```go
case "SOUNDEX":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "SOUNDEX requires 1 argument",
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    s := strings.ToUpper(toString(args[0]))
    if len(s) == 0 {
        return "", nil
    }
    // Soundex mapping
    soundexMap := map[byte]byte{
        'B': '1', 'F': '1', 'P': '1', 'V': '1',
        'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
        'D': '3', 'T': '3',
        'L': '4',
        'M': '5', 'N': '5',
        'R': '6',
    }
    result := []byte{s[0]}
    lastCode := soundexMap[s[0]]
    for i := 1; i < len(s) && len(result) < 4; i++ {
        code, ok := soundexMap[s[i]]
        if ok && code != lastCode {
            result = append(result, code)
            lastCode = code
        } else if !ok {
            lastCode = 0 // Reset on vowel/H/W/Y
        }
    }
    for len(result) < 4 {
        result = append(result, '0')
    }
    return string(result), nil
```

Type inference: `return dukdb.TYPE_VARCHAR`

## 5. LIKE_ESCAPE

LIKE_ESCAPE(string, pattern, escape_char) — performs LIKE matching with a custom escape character. Returns BOOLEAN.

```go
case "LIKE_ESCAPE":
    if len(args) != 3 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIKE_ESCAPE requires 3 arguments (string, pattern, escape_char)",
        }
    }
    if args[0] == nil || args[1] == nil {
        return nil, nil
    }
    s := toString(args[0])
    pattern := toString(args[1])
    escapeChar := byte(0)
    if args[2] != nil {
        esc := toString(args[2])
        if len(esc) > 0 {
            escapeChar = esc[0]
        }
    }
    // Convert LIKE pattern to regex with custom escape
    // Reuse existing LIKE matching logic if available, or build regex
    matched := matchLikePattern(s, pattern, escapeChar)
    return matched, nil
```

Note: Need to check if there's an existing `matchLikePattern()` helper or similar LIKE matching code in the executor.

Type inference: `return dukdb.TYPE_BOOLEAN`

## Registration in inferFunctionResultType() (binder/utils.go:347)

```go
case "OCTET_LENGTH":
    return dukdb.TYPE_INTEGER
case "INITCAP", "SOUNDEX":
    return dukdb.TYPE_VARCHAR
case "LIKE_ESCAPE":
    return dukdb.TYPE_BOOLEAN
```

LCASE/UCASE: Add alongside existing UPPER/LOWER type inference entries.

## Helper Signatures Reference (Verified)

- `evaluateFunctionCall()` — expr.go:661 — function dispatch
- `inferFunctionResultType()` — binder/utils.go:347 — type inference
- UPPER case — expr.go:1235 — existing uppercase function
- LOWER case — expr.go:1247 — existing lowercase function
- LENGTH case — expr.go:1259 — existing length function
- `toString()` — expr.go:4202 — any → string conversion
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. OCTET_LENGTH('hello') → 5
2. OCTET_LENGTH('héllo') → 6 (é is 2 bytes in UTF-8)
3. LCASE('HELLO') → 'hello' (same as LOWER)
4. UCASE('hello') → 'HELLO' (same as UPPER)
5. INITCAP('hello world') → 'Hello World'
6. INITCAP('hELLO wORLD') → 'Hello World'
7. SOUNDEX('Robert') → 'R163'
8. SOUNDEX('Rupert') → 'R163' (same as Robert)
9. LIKE_ESCAPE('10%', '10#%', '#') → true (# escapes the %)
10. NULL propagation for all functions
