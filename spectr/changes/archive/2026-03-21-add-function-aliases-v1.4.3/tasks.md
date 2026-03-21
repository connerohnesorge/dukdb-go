# Tasks: Add Missing Function Aliases and Small Scalar Functions

- [ ] 1. Add DATETRUNC and DATEADD aliases — In `internal/executor/expr.go`, add "DATETRUNC" to the DATE_TRUNC case label (line 1694) and "DATEADD" to the DATE_ADD case label (line 1685). In `internal/binder/utils.go`, add these aliases to the corresponding type inference cases. Validate: `SELECT DATETRUNC('month', TIMESTAMP '2024-03-15 10:30:00')` returns `2024-03-01 00:00:00`.

- [ ] 2. Add ORD alias for ASCII — In `internal/executor/expr.go`, add "ORD" to the ASCII case label (line 1515). In `internal/binder/utils.go`, add "ORD" to the BIGINT return case alongside ASCII (line 444). Validate: `SELECT ORD('A')` returns 65.

- [ ] 3. Implement IFNULL and NVL — In `internal/executor/expr.go`, add a new case for "IFNULL", "NVL" near COALESCE (line 1162). Enforce exactly 2 arguments. Return first arg if non-NULL, else second arg. In `internal/binder/utils.go`, add "IFNULL", "NVL" case using `inferCoalesceResultType()`. Validate: `SELECT IFNULL(NULL, 42)` → 42, `SELECT NVL(1, 99)` → 1, `SELECT IFNULL(NULL, NULL)` → NULL.

- [ ] 4. Implement BIT_LENGTH — In `internal/executor/expr.go`, add case for "BIT_LENGTH" near BIT_COUNT (line 1016). Return `len(string) * 8` for strings, `len(bytes) * 8` for blobs. In binder, return TYPE_BIGINT. Validate: `SELECT BIT_LENGTH('hello')` → 40, `SELECT BIT_LENGTH('')` → 0.

- [ ] 5. Implement GET_BIT and SET_BIT — In `internal/executor/expr.go`, add GET_BIT(value, index) returning 0 or 1, and SET_BIT(value, index, new_bit) returning modified value. Use big-endian bit ordering (bit 0 is MSB of first byte). In binder, GET_BIT returns TYPE_INTEGER, SET_BIT returns first-arg type. Validate: `SELECT GET_BIT(b'\x80', 0)` → 1, `SELECT GET_BIT(b'\x80', 1)` → 0.

- [ ] 6. Implement ENCODE and DECODE — In `internal/executor/expr.go`, add ENCODE(string, encoding) returning BLOB and DECODE(blob, encoding) returning VARCHAR. Support UTF-8 (default), LATIN1/ISO-8859-1, ASCII encodings. Use `golang.org/x/text/encoding/charmap` for LATIN1. In binder, ENCODE returns TYPE_BLOB, DECODE returns TYPE_VARCHAR. Validate: `SELECT ENCODE('hello', 'UTF-8')` returns blob, `SELECT DECODE(ENCODE('hello', 'UTF-8'), 'UTF-8')` → 'hello'.

- [ ] 7. Integration tests — Write tests covering all new functions and aliases. Test NULL propagation, error cases (wrong arg count, invalid encoding, out-of-range bit index), and equivalence with existing functions (DATETRUNC=DATE_TRUNC, ORD=ASCII, NVL=IFNULL).
