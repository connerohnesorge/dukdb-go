# JSON Read Auto Format Detection Example

This example demonstrates how to use `read_json_auto()` to automatically detect and read both JSON array and NDJSON formats.

## Overview

`read_json_auto()` is a convenient function that automatically detects whether a JSON file is in array format or NDJSON format and handles both transparently.

## What You'll Learn

1. **Automatic Format Detection**: Let `read_json_auto()` determine the format
2. **JSON Arrays**: Reading arrays with auto-detection
3. **NDJSON Files**: Reading NDJSON with auto-detection
4. **Format Transparency**: Use the same code for both formats
5. **Consistency**: Verify results are identical regardless of format

## Key Concepts

### read_json_auto() Function
- Automatically detects JSON array vs NDJSON format
- Infers column types from data
- Supports both compact and pretty-printed JSON
- Single function for both formats

### When to Use read_json_auto()
- You don't know the file format in advance
- Processing multiple files with mixed formats
- Building flexible data pipelines
- Generic JSON import tools

### Format Detection
The function detects format by:
- Looking for array brackets `[...]`
- Checking for newline-delimited objects
- Examining the first few lines
- Inferring structure from content

## Running the Example

```bash
cd examples/json-read-auto
go run main.go
```

### Expected Output
```
=== JSON Auto Format Detection Example ===

1. JSON Array Format (Automatic Detection):
Reading JSON Array with read_json_auto():
Columns: [age city id name]
Data from JSON Array:
  - Alice (age 25) from New York
  - Bob (age 30) from San Francisco
  - Charlie (age 28) from Chicago

2. NDJSON Format (Automatic Detection):
Reading NDJSON with read_json_auto():
Columns: [id price product stock]
Data from NDJSON:
  - Laptop: $999.99 (stock: 15)
  - Mouse: $29.99 (stock: 150)
  - Keyboard: $79.99 (stock: 75)

3. Comparing Both Formats:
Array format row count:
  Count: 3
NDJSON format row count:
  Count: 3

Average price from Array format:
  Average: $150.00
Average price from NDJSON format:
  Average: $150.00

4. Auto-Detection Features:
✓ Automatically detects JSON array vs NDJSON format
✓ Infers column types from data
✓ Supports both compact and pretty-printed JSON
✓ Handles both formats transparently
✓ Same SQL interface regardless of format

✓ JSON auto-detection example completed successfully!
```

## Code Walkthrough

### Basic Usage
```go
rows, err := db.Query("SELECT * FROM read_json_auto('file.json')")
```

### Works with Both Formats
```go
// This works for both array and NDJSON formats
rows1, _ := db.Query("SELECT * FROM read_json_auto('array.json')")
rows2, _ := db.Query("SELECT * FROM read_json_auto('ndjson.json')")
// Same code, different formats!
```

### Type Inference
```go
// Types are automatically inferred from the data
// No need to specify schema
rows, _ := db.Query("SELECT * FROM read_json_auto('data.json')")
```

## Comparison Table

| Feature | read_json() | read_ndjson() | read_json_auto() |
|---------|------------|---------------|-----------------|
| JSON Arrays | ✓ | ✗ | ✓ |
| NDJSON Files | ✗ | ✓ | ✓ |
| Auto Detection | ✗ | ✗ | ✓ |
| Explicit Format | ✓ | ✓ | ✗ |
| Performance | Excellent | Excellent | Good |

## Real-World Scenarios

### Scenario 1: ETL Pipeline
```go
// Process mixed format inputs
formats := []string{"data1.json", "data2.ndjson", "data3.json"}
for _, file := range formats {
    // Same code works for all!
    rows, _ := db.Query("SELECT * FROM read_json_auto(?)", file)
}
```

### Scenario 2: Data Import Tool
```go
// Generic import from multiple sources
func ImportJSON(filename string) error {
    // Don't need to know the format
    _, err := db.Query("SELECT * FROM read_json_auto(?)", filename)
    return err
}
```

### Scenario 3: Log Analysis
```go
// Logs might be in different formats
rows, _ := db.Query("SELECT timestamp, level FROM read_json_auto('app.log')")
```

## Performance Notes

- `read_json_auto()` has minimal overhead for format detection
- Performance is comparable to explicit format functions
- Format detection is done once during table creation
- Subsequent queries use the detected format efficiently

## Important Notes

- **First Row Matters**: Type inference is based on initial data
- **Consistency**: All objects should have the same structure
- **Column Order**: May vary from input (DuckDB sorts alphabetically)
- **Large Files**: NDJSON more efficient for streaming large data
- **Format Mixing**: Don't mix formats in single file

## When NOT to Use read_json_auto()

- You know the format and want explicit control
- Performance is critical and you want to avoid detection
- You want to enforce strict format checking
- Building type-safe pipelines

## Next Steps

- See [json-read-basic](../json-read-basic) for explicit JSON array reading
- See [json-read-ndjson](../json-read-ndjson) for explicit NDJSON reading
- See [json-querying](../json-querying) for advanced JSON queries

## Related Functions

- `read_json()` - Read JSON arrays explicitly
- `read_ndjson()` - Read NDJSON explicitly
- `read_json_auto()` - Auto-detect and read both formats
- `json_extract()` - Extract values from JSON
- `json_typeof()` - Get JSON type

## Documentation

For more information about JSON functions in DuckDB, visit:
https://duckdb.org/docs/data/json/overview
