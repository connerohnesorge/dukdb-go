# Change: Implement Result Handling Layer

## Why

Result handling converts backend JSON responses into database/sql compatible types. This includes implementing driver.Rows, scanning values into Go types, and handling all DuckDB data types including nested structures.

**Clarification:** This change creates NEW code in the root package for the pure Go `dukdb-go` driver. The `duckdb-go/` folder is reference material only.

## What Changes

- Create `rows.go` implementing driver.Rows interface with JSON-backed storage
- Create `scan.go` with type scanning from JSON to Go types
- Handle all DuckDB types including LIST, STRUCT, MAP, UNION
- Implement column metadata extraction from backend response
- Support sql.Scanner interface for custom types

## Architecture

The Rows implementation stores pre-parsed JSON data:
```go
type Rows struct {
    columns  []string           // Column names in order
    colTypes []Type             // DuckDB type for each column
    data     [][]any            // Parsed row data
    index    int                // Current row index (-1 before first Next)
    closed   bool               // Closed flag
}
```

This differs from duckdb-go's streaming DataChunk approach - we buffer all results since the CLI returns complete JSON output.

## Dependencies (Explicit)

This change depends on:
1. `add-project-foundation` - Error types (ErrorTypeClosed, ErrorTypeInvalid, ErrorTypeBadState)
2. `add-type-system` - Type definitions (UUID, Interval, Decimal, Map, Union, Type enum)
3. `add-process-backend` - Backend JSON response format

**Note:** ErrorTypeClosed, ErrorTypeInvalid, and ErrorTypeBadState are defined in add-project-foundation.

## NULL Handling

| Destination Type | NULL Behavior |
|-----------------|---------------|
| *T (pointer) | Set pointer to nil |
| T (non-pointer) | Set to zero value of T |
| sql.Scanner | Call Scan(nil) |

## Impact

- Affected specs: `result-handling` (new capability)
- Affected code: NEW files `rows.go`, `scan.go` in root package
- Dependencies: Requires all previous proposals (see explicit list above)
- Enables: Full query result iteration and type conversion
