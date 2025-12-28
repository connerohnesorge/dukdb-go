## 1. Statement Structure

- [ ] 1.1 Implement PreparedStmt struct
  - Fields: conn *Conn, query string, numParams int, closed bool, mu sync.Mutex
  - Thread-safe via mutex
  - **Acceptance:** Struct holds statement state correctly

- [ ] 1.2 Implement countPlaceholders(query string) int
  - Find positional params via regex `\$(\d+)` (not inside string literals)
  - Find named params via regex `@([a-zA-Z_][a-zA-Z0-9_]*)` (not inside string literals)
  - For positional: return max $N found (e.g., $1, $3 → 3)
  - For named: return count of unique @name (e.g., @a, @a, @b → 2)
  - For mixed: return sum of positional max + named count
  - **Acceptance:** "SELECT $1, $2, $3" returns 3, "SELECT @a, @b" returns 2, "SELECT $1, $3" returns 3

- [ ] 1.3 Implement Conn.Prepare(query string) (*PreparedStmt, error)
  - Count placeholders in query using countPlaceholders()
  - Return PreparedStmt with numParams set
  - No syntax validation at prepare time (defer to execution)
  - Always succeeds for any query string
  - **Acceptance:** Returns PreparedStmt for any query string

## 2. Statement Execution

- [ ] 2.1 Implement PreparedStmt.ExecContext(ctx, args []driver.NamedValue) (driver.Result, error)
  - Lock mutex
  - Check closed → return `&Error{Type: ErrorTypeClosed, Msg: "statement is closed"}`
  - Check conn.closed → return `&Error{Type: ErrorTypeConnection, Msg: "connection is closed"}`
  - Validate len(args) == numParams → return error if mismatch
  - Delegate to conn.ExecContext(ctx, query, args)
  - Unlock mutex
  - **Acceptance:** Execution works with correct params

- [ ] 2.2 Implement PreparedStmt.QueryContext(ctx, args []driver.NamedValue) (driver.Rows, error)
  - Lock mutex
  - Check closed → return ErrorTypeClosed
  - Check conn.closed → return ErrorTypeConnection
  - Validate len(args) == numParams
  - Delegate to conn.QueryContext(ctx, query, args)
  - Unlock mutex
  - **Acceptance:** Query returns Rows

- [ ] 2.3 Implement parameter count validation
  - If len(args) != numParams → return `&Error{Type: ErrorTypeInvalid, Msg: "expected N parameter, got M"}`
  - Use singular "parameter" for N=1, plural "parameters" otherwise
  - **Acceptance:** Wrong param count returns clear error

## 3. Statement Lifecycle

- [ ] 3.1 Implement PreparedStmt.Close() error
  - Lock mutex
  - Set closed = true
  - Unlock mutex
  - Return nil (no resources to release in CLI model)
  - Idempotent: second Close() returns nil (not error)
  - **Acceptance:** Close marks statement as closed

- [ ] 3.2 Implement PreparedStmt.NumInput() int
  - Return numParams (no lock needed, field is immutable)
  - Works even after Close()
  - **Acceptance:** Returns correct parameter count always

- [ ] 3.3 Implement closed statement error handling
  - ExecContext on closed statement → `&Error{Type: ErrorTypeClosed, Msg: "statement is closed"}`
  - QueryContext on closed statement → `&Error{Type: ErrorTypeClosed, Msg: "statement is closed"}`
  - **Acceptance:** Clear error for closed statement operations

## 4. Connection Association

- [ ] 4.1 Implement connection validity check
  - Before execution, check if conn.closed is true
  - If conn is closed → return `&Error{Type: ErrorTypeConnection, Msg: "connection is closed"}`
  - Check before parameter validation
  - **Acceptance:** Orphaned statements return connection error

- [ ] 4.2 Implement context cancellation handling
  - Check ctx.Err() before execution
  - Return context.Canceled or context.DeadlineExceeded immediately
  - Context checked by delegated Conn methods
  - **Acceptance:** Cancelled context returns immediately

## 5. Testing

- [ ] 5.1 Prepare/Execute/Close lifecycle tests
  - Test: Prepare → Execute → Close → Execute fails with ErrorTypeClosed
  - Test: Prepare → Execute 100 times → Close → verify all succeeded
  - Test: Prepare → Close → Close → no error (idempotent)
  - **Acceptance:** Full lifecycle works

- [ ] 5.2 Parameter count tests
  - Test: `SELECT $1` → NumInput()=1
  - Test: `SELECT $1, $3` → NumInput()=3 (gap)
  - Test: `SELECT $1, $1` → NumInput()=1 (duplicate)
  - Test: `SELECT @foo` → NumInput()=1
  - Test: `SELECT @foo, @foo` → NumInput()=1 (duplicate)
  - Test: `SELECT @foo, @bar` → NumInput()=2
  - Test: `SELECT $1, @foo` → NumInput()=2 (mixed, count only)
  - Test: `SELECT 1` → NumInput()=0
  - **Acceptance:** Count detection accurate for all cases

- [ ] 5.3 Concurrent execution tests
  - Execute same statement from 10 goroutines simultaneously
  - Each goroutine executes 10 times
  - go test -race passes
  - **Acceptance:** Thread-safe operation, no race conditions

- [ ] 5.4 Error handling tests
  - Test: Close then execute → ErrorTypeClosed "statement is closed"
  - Test: Wrong parameter count → ErrorTypeInvalid "expected N parameter, got M"
  - Test: Connection closed → ErrorTypeConnection "connection is closed"
  - Test: Cancelled context → context.Canceled
  - **Acceptance:** All error cases return correct types and messages
