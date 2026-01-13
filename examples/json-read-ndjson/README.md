# JSON Read NDJSON Example

This example demonstrates reading NDJSON (Newline Delimited JSON) files in dukdb-go.

## Overview

NDJSON (Newline Delimited JSON) is a format where each line is a valid JSON object. This format is ideal for streaming and processing large datasets as it allows reading one record at a time without loading the entire file into memory.

## Key Concepts

### NDJSON Format
NDJSON files contain one JSON object per line:
```ndjson
{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
{"id": 3, "name": "Charlie", "age": 35}
```

### Reading NDJSON
You can read NDJSON files using either:
1. `read_ndjson()` - Dedicated function for NDJSON
2. `read_json()` with format option - `read_json('file.ndjson', format = 'newline_delimited')`

## Examples Included

1. **Basic NDJSON Reading**: Using `read_ndjson()` function
2. **Using read_json() with format**: Reading NDJSON with format option
3. **Time-based Queries**: Filtering NDJSON data by timestamps
4. **Aggregating Data**: Grouping and aggregating NDJSON records
5. **Creating Tables**: Creating database tables from NDJSON data
6. **Large File Processing**: Handling large NDJSON files efficiently
7. **Error Handling**: Dealing with malformed JSON records
8. **File Paths**: Using absolute paths for NDJSON files

## Running the Example

```bash
cd json-read-ndjson
go run main.go
```

## Sample Data

The example creates a sample NDJSON file (`events.ndjson`) with event data containing:
- id (integer)
- name (string)
- age (integer)
- city (string)
- timestamp (string in ISO format)

## SQL Functions Used

- `read_ndjson(path)`: Reads an NDJSON file and returns it as a table
- `read_json(path, format = 'newline_delimited')`: Reads NDJSON using the generic JSON reader

## Options

When using `read_json()` with NDJSON format, you can specify additional options:
- `ignore_errors = true`: Skip invalid JSON lines instead of failing
- `sample_size = n`: Number of lines to sample for schema inference

## Error Handling

The example demonstrates:
- Handling malformed JSON lines with `ignore_errors` option
- Dealing with missing files
- Processing partial data when some lines are invalid

## Performance Considerations

- NDJSON is ideal for streaming large datasets
- Each line is processed independently, allowing parallel processing
- Memory usage is constant regardless of file size
- Suitable for log files and event streams

## Output

The program will output:
- Column information from the NDJSON file
- All records from the NDJSON file
- Time-filtered results
- Aggregated statistics (count, average age by city)
- Results from created table
- Processing results from large files
- Error handling examples

## Notes

- Each line must be a valid JSON object
- Lines are separated by newline characters (\n)
- The entire file doesn't need to be a valid JSON array
- Schema is inferred from the first few lines (configurable with sample_size)