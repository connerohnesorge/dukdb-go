# CSV Reading with Options Example

This example demonstrates various options available when reading CSV files in dukdb-go using the `read_csv()` function.

## Available Options

### 1. Delimiter
Specify a custom delimiter character (default is comma `,`):
```sql
SELECT * FROM read_csv('file.csv', delimiter := '|');
SELECT * FROM read_csv('file.csv', delimiter := '\t');  -- Tab delimiter
```

### 2. Header
Control whether the first row contains column names:
```sql
-- CSV has header row (default)
SELECT * FROM read_csv('file.csv', header := true);

-- CSV has no header, provide column names
SELECT * FROM read_csv('file.csv', header := false,
  columns := ['col1', 'col2', 'col3']);
```

### 3. Null String
Specify how NULL values are represented:
```sql
-- Treat empty strings as NULL
SELECT * FROM read_csv('file.csv', nullstr := '');

-- Treat 'N/A' as NULL
SELECT * FROM read_csv('file.csv', nullstr := 'N/A');
```

### 4. Skip Rows
Skip a number of rows at the beginning:
```sql
-- Skip first 3 rows (e.g., comments or metadata)
SELECT * FROM read_csv('file.csv', skip := 3);
```

### 5. Sample Size
Control how many rows to sample for type detection:
```sql
-- Sample 1000 rows for type detection
SELECT * FROM read_csv('file.csv', sample_size := 1000);
```

### 6. Date Format
Specify the date format for date columns:
```sql
-- European date format
SELECT * FROM read_csv('file.csv', dateformat := '%d/%m/%Y');

-- US date format
SELECT * FROM read_csv('file.csv', dateformat := '%m/%d/%Y');
```

### 7. Columns
Explicitly specify column names and types:
```sql
SELECT * FROM read_csv('file.csv',
  columns := ['id': 'INTEGER', 'name': 'VARCHAR', 'date': 'DATE']);
```

## Examples in This Demo

1. **Custom delimiter (|)**: Reading pipe-delimited files
2. **No header row**: Providing column names when CSV lacks headers
3. **Custom null string**: Treating 'N/A' as NULL values
4. **Quoted values**: Handling values containing commas
5. **Multiple options**: Combining delimiter, header, and null options
6. **Skip rows**: Ignoring comment lines at the beginning
7. **Date format**: Parsing dates in different formats

## Common Use Cases

### Tab-Delimited Files
```sql
SELECT * FROM read_csv('data.tsv', delimiter := '\t');
```

### Semicolon-Delimited (European CSV)
```sql
SELECT * FROM read_csv('data.csv', delimiter := ';');
```

### Files with Comments
```sql
SELECT * FROM read_csv('data.csv', skip := 2);  -- Skip 2 comment lines
```

### Custom NULL Representation
```sql
SELECT * FROM read_csv('data.csv', nullstr := 'NULL');
SELECT * FROM read_csv('data.csv', nullstr := '-999');  -- Missing numeric values
```

## Error Handling

The example demonstrates handling:
- Invalid delimiter characters
- Mismatched column counts
- Type conversion errors
- File not found errors

## Best Practices

1. **Always specify options explicitly** when the CSV format is known
2. **Use `read_csv_auto()`** for unknown formats (see next example)
3. **Handle NULL values explicitly** using the `nullstr` option
4. **Provide column names** when CSV has no header row
5. **Skip metadata rows** at the beginning of files
6. **Use appropriate date formats** for your region

## Performance Tips

- Smaller `sample_size` values speed up type detection but may be less accurate
- Explicitly specifying `columns` avoids sampling overhead
- Use appropriate data types to avoid conversion overhead

## Example Output

```
=== CSV Reading with Options Example ===

1. Reading CSV with custom delimiter (|):
id | name | department | salary
------------------------------
1 | John Doe | Engineering | 75000
2 | Jane Smith | Marketing | 65000
3 | Bob Johnson | Sales | 55000

2. Reading CSV without header row:
product_id | product_name | price | stock
----------------------------------
1001 | Product A | 19.99 | 150
1002 | Product B | 29.99 | 200
1003 | Product C | 39.99 | 75

3. Reading CSV with custom null representation (N/A):
order_id | customer_id | product_id | quantity | discount
--------------------------------------------------
1 | 101 | 201 | 5 | NULL
2 | 102 | 202 | 3 | 0.1
3 | 103 | NULL | NULL | 0.2
4 | 104 | 204 | 10 | NULL
```

Note: NULL values are shown as 'NULL' in the output for clarity. In actual database operations, they would be proper NULL values.