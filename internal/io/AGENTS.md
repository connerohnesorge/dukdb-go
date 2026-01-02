# IO KNOWLEDGE BASE

## OVERVIEW
The `io` package provides support for importing and exporting data in various formats (CSV, JSON, Parquet). It integrates with the `COPY` statement and specific read functions.

## STRUCTURE
- `io.go`: Core interfaces.
- `csv/`: CSV format implementation.
- `json/`: JSON format implementation.
- `parquet/`: Parquet format implementation.
- `compress.go`: Compression utilities.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| **CSV Support** | `csv/` | `read_csv`, `write_csv` |
| **Parquet Support** | `parquet/` | `read_parquet` (using `parquet-go`) |
| **JSON Support** | `json/` | `read_json`, `read_ndjson` |

## CONVENTIONS
- **Streaming**: Implementations should support streaming where possible to avoid loading full files into memory.
- **Pure Go**: Must use pure Go libraries (e.g., `parquet-go`).
