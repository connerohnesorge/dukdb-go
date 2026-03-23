# Design: Current Date/Time Functions

## Architecture

Two changes: (1) parser handles bare SQL keywords without parentheses, (2) executor dispatches the function calls.

## 1. Parser Changes (parser.go:5035)

### Problem

SQL standard says CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP can appear without parentheses:
```sql
SELECT CURRENT_DATE;          -- no parens, valid SQL
SELECT CURRENT_TIMESTAMP;     -- no parens, valid SQL
SELECT NOW();                 -- requires parens (not a keyword)
```

Currently, parseIdentExpr() at line 5035 only creates a FunctionCall when followed by `(`. Without parens, `CURRENT_DATE` becomes a ColumnRef which fails to resolve.

### Solution

Add cases to the existing keyword switch at parser.go:5039-5083, before the function call check at line 5086:

```go
// In parseIdentExpr(), inside the switch strings.ToUpper(name) block
// after the COLUMNS case (line 5067-5082) and before the closing brace:
case "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP":
    // SQL standard: these keywords work without parentheses
    // If followed by '()', parseFunctionCall will handle them below
    if p.current().typ != tokenLParen {
        return &FunctionCall{Name: strings.ToUpper(name)}, nil
    }
    // Fall through to parseFunctionCall for CURRENT_DATE() syntax
```

This goes INSIDE the existing switch at line 5039, NOT as a new switch. The fall-through allows `CURRENT_DATE()` with parens to also work via parseFunctionCall at line 5087.

NOW() and TODAY() always require parens (they're regular functions, not SQL keywords), so they need no parser change — they'll be handled by parseFunctionCall naturally.

## 2. Executor Changes (expr.go:661)

Add cases in evaluateFunctionCall() dispatch. The function should use `time.Now()` for the current time. In DuckDB, these are statement-time stable (same value within a statement), but for simplicity we use `time.Now()` which is correct since the executor runs synchronously.

### Implementation

Add BEFORE the existing function dispatch (after the lambda/struct_pack special cases, around line 730 where regular function evaluation begins):

```go
// Zero-arg temporal functions — add in the main switch fn.Name block
case "NOW", "CURRENT_TIMESTAMP":
    return time.Now(), nil

case "CURRENT_DATE", "TODAY":
    now := time.Now()
    return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()), nil

case "CURRENT_TIME":
    now := time.Now()
    // Return time portion only (zero date)
    return time.Date(0, 1, 1, now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), now.Location()), nil
```

### Where exactly in expr.go

The evaluateFunctionCall() function at line 661 has this structure:
1. Lines 668-706: Lambda-accepting functions (LIST_TRANSFORM, STRUCT_PACK, etc.) — special handling before arg evaluation
2. Lines 708-716: Argument evaluation loop
3. Line 719+: Main `switch fn.Name` dispatch for all functions (starts with ABS at line 720)

The current datetime functions should go in the main `switch fn.Name` dispatch since they need no argument evaluation (zero args). Add them as early cases in the switch.

### Import note

`time` package is already imported in expr.go (used by temporal functions like DATE_ADD, STRFTIME, etc.).

## 3. Binder — Minor Addition Needed

Type inference at internal/binder/utils.go:475-479 already handles NOW, CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME but is **missing TODAY**. Add TODAY alongside CURRENT_DATE:

```go
case "NOW", "CURRENT_TIMESTAMP":
    return dukdb.TYPE_TIMESTAMP
case "CURRENT_DATE", "TODAY":
    return dukdb.TYPE_DATE
case "CURRENT_TIME":
    return dukdb.TYPE_TIME
```

## 4. Query Cache — Minor Addition Needed

The query cache at internal/engine/query_cache.go:211-217 marks volatile functions in a `volatileFuncs` map. It currently includes NOW, CURRENT_TIMESTAMP, CURRENT_TIME, CURRENT_DATE but is **missing TODAY**. Add "TODAY" to the map:

```go
volatileFuncs := map[string]struct{}{
    "RANDOM":            {},
    "NOW":               {},
    "CURRENT_TIMESTAMP": {},
    "CURRENT_TIME":      {},
    "CURRENT_DATE":      {},
    "TODAY":             {},  // ADD THIS
}
```

## Helper Signatures Reference (Verified)

- `parseIdentExpr()` — parser.go:5035 — identifier expression parsing
- Keyword switch — parser.go:5039-5083 — NULL, TRUE, FALSE, CASE, CAST, etc.
- Function call fallthrough — parser.go:5086-5087 — `if p.current().typ == tokenLParen`
- `evaluateFunctionCall()` — expr.go:661 — function dispatch
- Argument evaluation — expr.go:708-716 — evaluates fn.Args
- Main switch fn.Name — expr.go:719+ — function name dispatch (ABS at line 720)
- Error location — expr.go:3335 — "unknown function" error
- `FunctionCall` — ast.go:845-853 — Name, Args, NamedArgs, Distinct, Star, OrderBy, Filter
- Type inference — binder/utils.go:475-479 — handles NOW, CURRENT_DATE, CURRENT_TIME (needs TODAY added)
- Query cache exclusion — query_cache.go:211-217 — volatileFuncs map (needs TODAY added)
- `time` package — already imported in expr.go

## Testing Strategy

1. `SELECT NOW()` → returns current timestamp (non-null, close to time.Now())
2. `SELECT CURRENT_TIMESTAMP` → same as NOW() (bare keyword, no parens)
3. `SELECT CURRENT_TIMESTAMP()` → same as NOW() (with parens)
4. `SELECT CURRENT_DATE` → returns today's date (bare keyword)
5. `SELECT CURRENT_DATE()` → returns today's date (with parens)
6. `SELECT TODAY()` → same as CURRENT_DATE
7. `SELECT CURRENT_TIME` → returns current time (bare keyword)
8. `SELECT CURRENT_TIME()` → returns current time (with parens)
9. `SELECT NOW() IS NOT NULL` → true (never returns NULL)
10. `SELECT CURRENT_DATE + INTERVAL '1 day'` → tomorrow's date (arithmetic)
11. `INSERT INTO t(created_at) VALUES (NOW())` → inserts current timestamp
