# JSON Read Basic Example

This example demonstrates basic JSON reading capabilities in dukdb-go using the `read_json()` function.

## Overview

The `read_json()` function allows you to read JSON array files directly as database tables. Each object in the JSON array becomes a row in the result set, with object properties mapped to columns.

## Key Concepts

### JSON Array Format
The `read_json()` function expects a JSON file containing an array of objects:
```json
[
  {"id": 1, "name": "Alice", "age": 30},
  {"id": 2, "name": "Bob", "age": 25}
]
```

### Basic Usage
```sql
SELECT * FROM read_json('file.json')
```

## Examples Included

1. **Basic JSON Reading**: Reading a simple JSON array file
2. **Column Selection**: Querying specific columns from JSON data
3. **Creating Views**: Creating database views from JSON data
4. **Go API Usage**: Using the database driver from Go code
5. **Error Handling**: Handling missing files and other errors
6. **Absolute Paths**: Working with absolute file paths

## Running the Example

```bash
cd json-read-basic
go run main.go
```

## Sample Data

The example creates a sample JSON file (`users.json`) with user data containing:
- id (integer)
- name (string)
- age (integer)
- city (string)

## SQL Functions Used

- `read_json(path)`: Reads a JSON array file and returns it as a table

## Error Handling

The example demonstrates proper error handling for:
- Missing files
- Invalid JSON format
- Database connection issues

## Output

The program will output:
- Column information from the JSON file
- All records from the JSON file
- Filtered results (users older than 30)
- Aggregated results (users per city)
- Proper error messages for missing files

## Notes

- JSON files must contain an array of objects
- Each object in the array should have the same structure for consistent column types
- Column types are automatically inferred from the JSON data
- The function returns NULL for missing properties in objects