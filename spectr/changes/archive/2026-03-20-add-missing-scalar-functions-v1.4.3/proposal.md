# Add Missing Scalar Functions for DuckDB v1.4.3 Compatibility

**Change ID:** `add-missing-scalar-functions-v1.4.3`
**Created:** 2026-03-20
**Scope:** Medium — Adds 12 new scalar functions across 5 categories
**Estimated Complexity:** Medium — Each function is self-contained, touches executor and binder
**User-Visible:** Yes — New SQL functions available

## Summary

This proposal adds 12 missing scalar functions across 5 categories that DuckDB v1.4.3 supports but dukdb-go currently lacks:

1. **Conditional:** `IF(cond, true_val, false_val)`, `IFF(cond, true_val, false_val)`
2. **String formatting:** `FORMAT(fmt, args...)`, `PRINTF(fmt, args...)`
3. **Type introspection:** `TYPEOF(expr)`, `PG_TYPEOF(expr)`
4. **Base64 encoding:** `BASE64_ENCODE(data)` / `BASE64(data)` / `TO_BASE64(data)`, `BASE64_DECODE(str)` / `FROM_BASE64(str)`
5. **URL encoding:** `URL_ENCODE(str)`, `URL_DECODE(str)`

## Verification

All functions confirmed missing via code search:

- `IF`/`IFF`: Not in `internal/executor/expr.go` function switch. Only appears in `internal/parser/keyword_suggestions.go` as a keyword.
- `FORMAT`/`PRINTF`: Not in executor. Only appears in `internal/postgres/server/copy.go` for COPY FORMAT option.
- `TYPEOF`/`PG_TYPEOF`: No matches in entire `internal/` directory.
- `BASE64*`: No matches in `internal/executor/`.
- `URL_ENCODE`/`URL_DECODE`: No matches in entire `internal/` directory.

## Current Function Registration Pattern

Functions are registered in a large `switch` statement in `internal/executor/expr.go` (e.g., `case "COALESCE":` at line 1117, `case "MD5":` at line 1438). The binder infers return types via `inferFunctionResultType()` in `internal/binder/utils.go` (line 338).

Each function follows this pattern:
1. **Binder** (`internal/binder/utils.go`): Add case in `inferFunctionResultType()` to return the correct type
2. **Executor** (`internal/executor/expr.go`): Add case in the function dispatch switch to evaluate the function

## Goals

1. Register all 12 functions following the existing dispatch pattern
2. Add type inference for each function in the binder
3. NULL propagation: all functions return NULL when any argument is NULL (standard SQL behavior), except IF/IFF which has special NULL handling
4. Full alias support (IF=IFF, FORMAT=PRINTF, BASE64=BASE64_ENCODE=TO_BASE64, etc.)

## Non-Goals

- Custom format specifiers beyond what DuckDB supports
- Non-standard base64 variants (URL-safe, no-padding, etc.)
- FORMAT with positional arguments (DuckDB doesn't support `{0}` style)

## Capabilities

### Capability 1: IF/IFF Conditional Functions

`IF(condition, true_value, false_value)` — Returns `true_value` when condition is true, `false_value` otherwise. `IFF` is an alias.

**Binder:** Return type is the common supertype of `true_value` and `false_value`. Condition must be BOOLEAN.

**Executor:**
- Evaluate condition first
- If condition is NULL or false → return `false_value`
- If condition is true → return `true_value`
- Short-circuit: only evaluate the taken branch

**Special NULL handling:** Unlike most functions, IF does NOT return NULL when condition is NULL — it returns the false_value branch.

### Capability 2: FORMAT/PRINTF String Formatting

`FORMAT(format_string, arg1, arg2, ...)` — Printf-style formatting. `PRINTF` is an alias.

**Supported format specifiers (matching DuckDB):**
- `%s` — string
- `%d` — integer
- `%f` — float (default 6 decimal places)
- `%e` — scientific notation
- `%g` — general float (shortest representation)
- `%x` — hexadecimal
- `%o` — octal
- `%c` — character (from codepoint)
- `%%` — literal percent
- Width and precision: `%10d`, `%.2f`, `%10.2f`
- Left-justify: `%-10s`
- Zero-pad: `%05d`
- Plus sign: `%+d`

**Binder:** Return type is always VARCHAR.

**Executor:** Parse format string, match specifiers to args. Use Go's `fmt.Sprintf` with translated specifiers.

### Capability 3: TYPEOF/PG_TYPEOF Type Introspection

`TYPEOF(expr)` — Returns DuckDB-style type name. `PG_TYPEOF(expr)` — Returns PostgreSQL-style type name.

**Type name mappings:**

| DuckDB Type | TYPEOF Result | PG_TYPEOF Result |
|-------------|---------------|------------------|
| INTEGER | "INTEGER" | "integer" |
| BIGINT | "BIGINT" | "bigint" |
| VARCHAR | "VARCHAR" | "character varying" |
| BOOLEAN | "BOOLEAN" | "boolean" |
| DOUBLE | "DOUBLE" | "double precision" |
| FLOAT | "FLOAT" | "real" |
| DATE | "DATE" | "date" |
| TIMESTAMP | "TIMESTAMP" | "timestamp without time zone" |
| BLOB | "BLOB" | "bytea" |
| LIST | "INTEGER[]" (element type) | "ARRAY" |
| MAP | "MAP(KEY, VALUE)" | "map" |
| STRUCT | "STRUCT(field TYPE, ...)" | "record" |

**Binder:** This is special — TYPEOF operates at the TYPE level, not value level. The binder should resolve the argument's type and fold to a string literal at bind time. Return type is always VARCHAR.

**Executor:** Evaluate the expression and call a type-name formatter. For dynamic types (parameters), resolve at execution time.

### Capability 4: BASE64 Encoding/Decoding

- `BASE64_ENCODE(data)` / `BASE64(data)` / `TO_BASE64(data)` — Encode to base64 string
- `BASE64_DECODE(str)` / `FROM_BASE64(str)` — Decode from base64 string

**Binder:** Encode returns VARCHAR, decode returns BLOB.

**Executor:** Use `encoding/base64.StdEncoding`. Handle BLOB and VARCHAR input for encode. Return error on invalid base64 for decode.

### Capability 5: URL Encoding/Decoding

- `URL_ENCODE(str)` — Percent-encode a string (form encoding, spaces as `+`)
- `URL_DECODE(str)` — Decode a percent-encoded string

**Binder:** Both return VARCHAR.

**Executor:** Use `net/url.QueryEscape` and `net/url.QueryUnescape` (form encoding matches DuckDB behavior where spaces become `+`).

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| FORMAT specifier edge cases | Medium | Low | Test against DuckDB CLI output for all specifiers |
| IF keyword conflicts with SQL IF | Low | Medium | Parser already handles IF as keyword; function call context is different |
| TYPEOF on parameters | Low | Low | Resolve at execution time when bind-time type is unknown |
