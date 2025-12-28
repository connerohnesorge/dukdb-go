## 1. Parameter Binding (params.go)

- [ ] 1.1 Implement positional placeholder extraction
  - Regex pattern: `\$(\d+)` to find $1, $2, etc.
  - Skip placeholders inside single-quoted string literals
  - Ignore $0 (not a valid placeholder)
  - Return slice of placeholder positions
  - **Acceptance:** `"SELECT $1, $2"` extracts [1, 2], `"SELECT '$1' || $1"` extracts [1]

- [ ] 1.2 Implement named placeholder extraction
  - Regex pattern: `@([a-zA-Z_][a-zA-Z0-9_]*)` to find @name, @value, etc.
  - Skip placeholders inside single-quoted string literals
  - Return slice of unique placeholder names
  - **Acceptance:** `"SELECT @foo, @bar"` extracts ["foo", "bar"]

- [ ] 1.3 Implement BindParams(query string, args []driver.NamedValue) (string, error)
  - Replace positional placeholders with FormatValue result
  - Replace named placeholders matching arg.Name (case-sensitive)
  - Gaps allowed: `$1, $3` requires 3 args
  - Duplicates reuse: `$1, $1` requires 1 arg
  - Mixed mode error: both `$N` and `@name` → ErrorTypeInvalid "cannot mix positional ($N) and named (@name) parameters"
  - Return ErrorTypeInvalid "expected N parameters, got M" if param count mismatch
  - Return ErrorTypeInvalid "named parameter @name not found in args" if named not found
  - **Acceptance:** All placeholder types bind correctly, edge cases handled

- [ ] 1.4 Implement FormatValue(v any) (string, error)
  - nil → `NULL`
  - bool → `TRUE` or `FALSE`
  - int/int8/int16/int32/int64 → decimal string
  - uint/uint8/uint16/uint32/uint64 → decimal string
  - float32/float64 → strconv.FormatFloat with 'g' format, -1 precision
    - math.Inf → ErrorTypeInvalid "infinity values not supported as parameters"
    - math.NaN → ErrorTypeInvalid "NaN values not supported as parameters"
  - string → single-quoted with '' escaping
  - []byte → `X'...'` hex literal (uppercase)
  - time.Time → `'2006-01-02 15:04:05.000000'` (UTC, microseconds)
  - UUID → single-quoted hyphenated format
  - Decimal → unquoted string value
  - Interval → `INTERVAL '...'` literal
  - *big.Int → unquoted string value
  - Return ErrorTypeInvalid "unsupported type TYPE" for unsupported types
  - **Acceptance:** All types format to valid DuckDB SQL, Inf/NaN rejected

- [ ] 1.5 Implement formatString with SQL injection prevention
  - Replace `'` with `''`
  - Wrap in single quotes
  - Test vectors: `O'Brien`, `'; DROP TABLE--`, `\n\r\t`
  - **Acceptance:** No injection possible via string values

## 2. Statement Implementation (stmt.go)

- [ ] 2.1 Implement Stmt struct
  - Fields: conn *Conn, query string, paramCount int
  - Close() marks statement as closed
  - NumInput() returns detected parameter count
  - **Acceptance:** Implements driver.Stmt interface

- [ ] 2.2 Implement Stmt.ExecContext
  - Check context before execution
  - Bind parameters via BindParams
  - Execute via conn.backend.Exec
  - Parse result for RowsAffected
  - Return sql.Result implementation
  - **Acceptance:** INSERT/UPDATE/DELETE return correct RowsAffected

- [ ] 2.3 Implement Stmt.QueryContext
  - Check context before execution
  - Bind parameters via BindParams
  - Execute via conn.backend.Query
  - Return driver.Rows implementation (from add-result-handling)
  - **Acceptance:** SELECT returns iterable rows

- [ ] 2.4 Implement context deadline enforcement
  - Extract deadline from context
  - Override backend timeout if deadline is shorter
  - Return context.DeadlineExceeded if timeout reached
  - **Acceptance:** Context deadline < backend timeout uses context deadline

- [ ] 2.5 Implement pre-execution context check
  - Check ctx.Done() before sending query
  - Return ctx.Err() immediately if already cancelled
  - **Acceptance:** Cancelled context returns error without backend call

## 3. Transaction Implementation (tx.go)

- [ ] 3.1 Implement Conn.BeginTx
  - Validate opts.Isolation == sql.LevelDefault
  - Validate opts.ReadOnly == false
  - Execute "BEGIN TRANSACTION" via backend
  - Return &Tx{conn: c}
  - Return ErrorTypeSettings for invalid options
  - **Acceptance:** Valid BeginTx starts transaction

- [ ] 3.2 Implement Tx struct
  - Fields: conn *Conn, done bool, mu sync.Mutex
  - Thread-safe via mutex
  - **Acceptance:** Concurrent Commit/Rollback is safe

- [ ] 3.3 Implement Tx.Commit
  - Lock mutex, check done flag
  - Execute "COMMIT" via backend
  - Set done = true
  - Return ErrorTypeTransaction if already done
  - **Acceptance:** Commit persists changes

- [ ] 3.4 Implement Tx.Rollback
  - Lock mutex, check done flag
  - Execute "ROLLBACK" via backend
  - Set done = true
  - Return ErrorTypeTransaction if already done
  - **Acceptance:** Rollback discards changes

- [ ] 3.5 Implement transaction state validation
  - Stmt execution within Tx uses Tx's connection
  - Cannot use connection for other operations during active Tx
  - **Acceptance:** Transaction isolation maintained

## 4. Result Implementation

- [ ] 4.1 Implement sql.Result for ExecContext
  - Parse backend response for rows affected count
  - LastInsertId() returns 0, nil (DuckDB doesn't support)
  - RowsAffected() returns parsed count
  - **Acceptance:** RowsAffected matches actual affected rows

## 5. Testing

- [ ] 5.1 Parameter binding tests
  - All primitive types (bool, int variants, float variants, string, []byte)
  - Custom types (UUID, Decimal, Interval, *big.Int)
  - nil → NULL
  - Mixed positional and named (should error)
  - Param count mismatch
  - **Acceptance:** All parameter tests pass

- [ ] 5.2 SQL injection prevention tests
  - `O'Brien` in string
  - `'; DROP TABLE users; --` in string
  - Newlines, tabs, special characters
  - **Acceptance:** No injection possible

- [ ] 5.3 Transaction tests
  - Commit persists
  - Rollback discards
  - Invalid isolation returns error
  - Read-only returns error
  - Double commit/rollback returns error
  - **Acceptance:** All transaction tests pass

- [ ] 5.4 Context tests
  - Pre-cancelled context returns immediately
  - Deadline shorter than query returns DeadlineExceeded
  - No deadline runs to completion
  - **Acceptance:** Context handling correct
