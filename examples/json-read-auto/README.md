# JSON Read Auto Example

This example demonstrates using `read_json_auto()` function to automatically detect JSON file formats in dukdb-go.

## Overview

The `read_json_auto()` function automatically detects whether a JSON file is in array format or NDJSON (newline-delimited) format, eliminating the need to specify the format explicitly.

## Key Concepts

### Auto-Detection
`read_json_auto()` examines the file content to determine:
- If the file starts with `[` and contains an array of JSON objects (JSON Array format)
- If the file contains one JSON object per line (NDJSON format)

### Supported Formats
1. **JSON Array**: `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`
2. **NDJSON**: Each line is a JSON object

## Examples Included

1. **Auto-detection on JSON Array**: Reading a JSON array file
2. **Auto-detection on NDJSON**: Reading an NDJSON file
3. **Comparison with explicit format**: Comparing performance and behavior
4. **Mixed data types**: Handling various data types (strings, numbers, booleans, dates)
5. **Nested structures**: Reading JSON with nested objects
6. **Error handling**: Dealing with invalid JSON files
7. **Performance comparison**: Timing differences between auto and explicit
8. **File paths**: Using absolute paths

## Running the Example

```bash
cd json-read-auto
go run main.go
```

## Sample Data

The example creates several sample files:
- `products_array.json`: JSON array format with product data
- `products_ndjson.json`: Same data in NDJSON format
- `mixed_types.json`: Various data types for testing
- `nested_data.json`: JSON with nested objects
- `large_array.json`: Large file for performance testing

## SQL Functions Used

- `read_json_auto(path)`: Automatically detects and reads JSON format
- `read_json(path, format = 'array'|'newline_delimited')`: Explicit format specification

## Auto-Detection Algorithm

The function:
1. Reads the first few lines of the file
2. Checks if content starts with `[` (array) or `{` (object)
3. Validates JSON structure
4. Selects appropriate parser

## Performance Considerations

- Auto-detection adds minimal overhead for small files
- For very large files, specifying format explicitly may be slightly faster
- First-time detection caches the format for subsequent reads

## Nested JSON Handling

Nested objects are returned as JSON strings:
```json
{"user": {"id": 1, "name": "Alice"}, "location": {"city": "NYC"}}
```
Results in columns: `location` (JSON string), `user` (JSON string)

## Error Handling

The example demonstrates:
- Invalid JSON file detection
- Format detection failures
- File not found errors

## Output

The program will output:
- Column information for each file
- Data from JSON array files
- Data from NDJSON files
- Performance comparison metrics
- Error messages for invalid files

## Notes

- Auto-detection works best with well-formed JSON
- Mixed format files (partial array) may fail detection
- For production systems with known formats, explicit format is recommended
- Nested structures require JSON parsing in queries or post-processing