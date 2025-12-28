# Change: Complete database/sql Driver Integration

## Why

Full integration with Go's database/sql package requires implementing all remaining interfaces and ensuring seamless compatibility. This final layer ties together all components into a cohesive, production-ready driver.

**Clarification:** This change creates NEW code in the root package for the pure Go `dukdb-go` driver. The `duckdb-go/` folder is reference material only.

## What Changes

- Create `driver.go` implementing driver.Driver and driver.DriverContext
- Create `connector.go` implementing driver.Connector
- Create `config.go` with DSN parsing and configuration
- Register driver as "dukdb" via init()
- Implement connection pooling support via Connector
- Ensure thread-safety across all driver operations

## Dependencies (Explicit)

This change depends on ALL previous changes in order:
1. `add-project-foundation` - Error types, Backend interface
2. `add-type-system` - Type definitions
3. `add-process-backend` - ProcessBackend implementation
4. `add-query-execution` - Statement execution, transactions
5. `add-result-handling` - Rows implementation
6. `add-prepared-statements` - PreparedStmt implementation
7. `add-appender-api` - Appender implementation

## DSN Format

```
path?option=value&option2=value2
```

Examples:
- `:memory:` - In-memory database
- `` (empty) - In-memory database
- `/path/to/db.duckdb` - File database
- `/path/to/db.duckdb?access_mode=read_only` - Read-only file database
- `:memory:?threads=4&max_memory=4GB` - In-memory with options

Supported DSN options:
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| access_mode | string | "automatic" | "automatic", "read_only", "read_write" |
| threads | int | CPU count | Number of threads for parallel query execution |
| max_memory | string | "80%" | Maximum memory limit (e.g., "4GB", "1024MB") |

Unknown options return ErrorTypeSettings.

## Impact

- Affected specs: `database-driver` (new capability)
- Affected code: NEW files `driver.go`, `connector.go`, `config.go` in root package
- Dependencies: Requires all previous proposals (see explicit list above)
- Enables: Production use with standard Go database patterns
