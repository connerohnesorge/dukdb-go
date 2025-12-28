# Change: Add Replacement Scan API

## Why

Replacement scans allow users to intercept table references and replace them with custom function calls. This enables:
- Custom file format handling (e.g., intercept `read_parquet('file.parquet')`)
- Dynamic data source routing
- Virtual table implementations
- Custom URL scheme handling

## What Changes

### Core Types

```go
// ReplacementScanCallback is called when a table reference is encountered
// Returns: function name, parameters, and error (positional returns, not named)
type ReplacementScanCallback func(tableName string) (string, []any, error)
```

### Public API

```go
// Register a replacement scan callback for a connector (database-level)
// Note: Uses *Connector (driver type), not *sql.Conn
// Note: No error return - registration is synchronous
func RegisterReplacementScan(c *Connector, callback ReplacementScanCallback)
```

### Supported Parameter Types

The callback can return parameters of these types only:
- `string` - String values
- `int64` - Integer values
- `[]string` - String arrays

Other types (bool, float64, etc.) are NOT supported and will cause errors.

### Execution Flow

1. During query binding, when a table reference is encountered
2. Check if any replacement scan callback matches
3. If callback returns a function name, replace table reference with function call
4. If callback returns error, the replacement scan FAILS (sets error on scan info)
5. If callback returns empty function name with nil error, continue normal table resolution
6. If callback returns unsupported parameter type, replacement scan FAILS with error

## Impact

- **Affected specs**: Depends on binder integration, **deterministic-testing**
- **Affected code**: New file `replacement_scan.go`, binder modifications
- **Dependencies**: Table UDFs (for function replacement); quartz.Clock for callback timeout testing
- **Consumers**: Users implementing custom data sources

## Deterministic Testing Requirements

Per `spectr/specs/deterministic-testing/spec.md`, replacement scan callbacks that involve timeouts must use injected clock:

```go
// ReplacementScanContext provides clock for timeout checking
type ReplacementScanContext struct {
    ctx   context.Context
    clock quartz.Clock
}

// Execute callback with timeout checking
func (c *ReplacementScanContext) executeCallback(
    callback ReplacementScanCallback,
    tableName string,
) (string, []any, error) {
    if deadline, ok := c.ctx.Deadline(); ok {
        if c.clock.Until(deadline) <= 0 {
            return "", nil, context.DeadlineExceeded
        }
    }
    return callback(tableName)
}

// Tests use mock clock for deterministic timeout behavior
func TestReplacementScanTimeout(t *testing.T) {
    mClock := quartz.NewMock(t)
    ctx, cancel := context.WithDeadline(context.Background(),
        mClock.Now().Add(1*time.Second))
    defer cancel()

    scanCtx := NewReplacementScanContext(ctx, mClock)

    // Advance past deadline
    mClock.Advance(2*time.Second).MustWait()

    _, _, err := scanCtx.executeCallback(callback, "test_table")
    assert.ErrorIs(t, err, context.DeadlineExceeded)
}
```

**Zero Flaky Tests Policy**: No `time.Sleep` in replacement scan tests. Use `quartz.Mock` for timeout testing.

## Breaking Changes

None. This adds new functionality.
