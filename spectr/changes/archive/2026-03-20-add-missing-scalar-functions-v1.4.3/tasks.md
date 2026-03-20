# Tasks: Add Missing Scalar Functions for DuckDB v1.4.3

- [ ] 1. Implement IF/IFF conditional functions — Add `case "IF", "IFF":` to function dispatch in `internal/executor/expr.go`. Add return type inference in `internal/binder/utils.go` `inferFunctionResultType()`. IF returns true_val when condition is true, false_val when false or NULL. Validate: `SELECT IF(true, 'y', 'n')` → 'y', `SELECT IF(NULL, 'y', 'n')` → 'n', `SELECT IFF(1>0, 1, 2)` → 1.

- [ ] 2. Implement FORMAT/PRINTF string formatting — Create `internal/executor/format.go` with `formatString()` function implementing DuckDB printf-style formatting. Add `case "FORMAT", "PRINTF":` to executor dispatch. Add binder return type (VARCHAR). Support specifiers: %s, %d, %f, %e, %g, %x, %o, %c, %%, width/precision. Validate: `SELECT FORMAT('%.2f', 3.14159)` → '3.14', `SELECT PRINTF('%s=%d', 'x', 10)` → 'x=10'.

- [ ] 3. Implement TYPEOF/PG_TYPEOF type introspection — Add `case "TYPEOF", "PG_TYPEOF":` to executor dispatch. Create type name mapping tables (DuckDB-style and PostgreSQL-style). Add binder optimization to fold to string literal at bind time when type is known. Validate: `SELECT TYPEOF(42)` → 'INTEGER', `SELECT PG_TYPEOF('hello')` → 'character varying'.

- [ ] 4. Implement BASE64 encoding/decoding functions — Add `case "BASE64_ENCODE", "BASE64", "TO_BASE64":` and `case "BASE64_DECODE", "FROM_BASE64":` to executor dispatch. Use Go's `encoding/base64.StdEncoding`. Add binder types: encode→VARCHAR, decode→BLOB. Validate: `SELECT BASE64_ENCODE('Hello')` → 'SGVsbG8=', `SELECT BASE64_DECODE('SGVsbG8=')` → binary 'Hello'.

- [ ] 5. Implement URL encoding/decoding functions — Add `case "URL_ENCODE":` and `case "URL_DECODE":` to executor dispatch. Use Go's `net/url.QueryEscape`/`QueryUnescape` (form encoding, spaces as +). Add binder type (VARCHAR). Validate: `SELECT URL_ENCODE('hello world')` → 'hello+world', `SELECT URL_DECODE('hello+world')` → 'hello world'.

- [ ] 6. Comprehensive integration tests — Write tests for all 12 function names covering: happy path, NULL propagation, wrong argument types, edge cases (empty strings, special characters, large inputs), and DuckDB CLI compatibility. Verify no regressions in existing function tests.
