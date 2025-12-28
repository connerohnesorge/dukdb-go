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

- **Affected specs**: Depends on binder integration
- **Affected code**: New file `replacement_scan.go`, binder modifications
- **Dependencies**: Table UDFs (for function replacement)
- **Consumers**: Users implementing custom data sources

## Breaking Changes

None. This adds new functionality.
