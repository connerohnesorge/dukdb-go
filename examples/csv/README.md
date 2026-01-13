# CSV Examples

This example demonstrates various CSV operations available in dukdb-go.

## Features Demonstrated

- Reading CSV files with `read_csv()`
- Auto-detecting CSV format with `read_csv_auto()`
- Reading CSV with custom options (delimiter, header, null handling)
- CSV schema inference
- Sample data files for testing

## Files

- `main.go` - Example application demonstrating CSV features
- `data/simple.csv` - Basic comma-separated CSV file
- `data/custom.csv` - Pipe-delimited CSV with product data
- `go.mod` - Go module definition

## Running the Example

```bash
# The example looks for CSV files in the data/ directory
go run main.go

# To test with specific data files
go run main.go
```

## CSV Functions in dukdb-go

### Reading
- `read_csv(path)` - Read CSV with default settings (comma delimiter, header)
- `read_csv_auto(path)` - Auto-detect CSV format (delimiter, types, header)
- `read_csv(path, delimiter, header, nullstr)` - Custom options
- `read_csv(path, delim='|', header=True, nullstr='N/A')` - Named parameters

### Writing
- `COPY table TO 'file.csv'` - Export table to CSV
- `COPY table TO 'file.csv' (HEADER, DELIMITER ',')` - With options

## Sample Data

### simple.csv
Comma-separated values with header:
```csv
id,name,age,city
1,Alice Smith,28,New York
2,Bob Johnson,35,San Francisco
```

### custom.csv
Pipe-delimited product catalog:
```csv
id|product|category|price|stock
101|Wireless Mouse|Electronics|29.99|50
102|USB-C Hub|Electronics|49.99|30
```

## CSV Options

### Delimiters
- Comma (`,`) - Default
- Pipe (`|`)
- Tab (`\t`)
- Semicolon (`;`)
- Custom characters

### Header Row
- `header=True` - First row contains column names
- `header=False` - No header, columns named c0, c1, etc.
- `header=42` - Skip 42 rows, then use header

### Null Values
- `nullstr=''` - Empty strings as NULL
- `nullstr='N/A'` - Custom NULL string
- `nullstr='NULL'` - String NULL as NULL

## Performance Tips

- Use `read_csv_auto()` for most cases - it detects format automatically
- For large files, use `read_csv()` with explicit options for faster parsing
- Column projection (`SELECT id, name`) is more efficient than `SELECT *`
- Consider using Parquet for large datasets

## Error Handling

The example demonstrates error handling for:
- Missing CSV files
- Malformed CSV data
- Type conversion failures
- Encoding issues
