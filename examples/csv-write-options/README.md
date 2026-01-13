# CSV Writing with Options Example

This example demonstrates various options available when writing CSV files in dukdb-go using the `COPY TO` statement with different formatting options.

## Available Options

### 1. Delimiter
Change the field separator (default is comma `,`):
```sql
COPY table TO 'file.csv' WITH (DELIMITER '|');
COPY table TO 'file.csv' WITH (DELIMITER '\t');  -- Tab-delimited
```

### 2. Header
Control whether to include column names:
```sql
-- Include header (default)
COPY table TO 'file.csv' WITH (HEADER true);

-- Exclude header
COPY table TO 'file.csv' WITH (HEADER false);
```

### 3. Null Representation
Customize how NULL values are written:
```sql
-- Write NULL as empty string
COPY table TO 'file.csv' WITH (NULL '');

-- Write NULL as 'N/A'
COPY table TO 'file.csv' WITH (NULL 'N/A');
```

### 4. Force Quote
Force quotes around fields:
```sql
-- Quote all fields
COPY table TO 'file.csv' WITH (FORCE_QUOTE *);

-- Quote specific columns
COPY table TO 'file.csv' WITH (FORCE_QUOTE (name, description));
```

### 5. Escape Character
Set custom escape character for special characters:
```sql
COPY table TO 'file.csv' WITH (ESCAPE '\\');
```

### 6. Encoding
Specify character encoding:
```sql
COPY table TO 'file.csv' WITH (ENCODING 'UTF-8');
COPY table TO 'file.csv' WITH (ENCODING 'UTF-16');
```

### 7. Date Format
Control date formatting:
```sql
-- European date format
COPY table TO 'file.csv' WITH (DATEFORMAT '%d/%m/%Y');

-- US date format
COPY table TO 'file.csv' WITH (DATEFORMAT '%m/%d/%Y');
```

## Examples in This Demo

1. **Custom delimiter (pipe)**: Export with pipe delimiter
2. **No header**: Export without column headers
3. **Custom null**: Export NULL values as 'N/A'
4. **Force all quotes**: Quote every field
5. **Selective quoting**: Quote only specific columns
6. **Escape character**: Handle special characters
7. **UTF-8 encoding**: Export international characters
8. **Date format**: Custom date formatting
9. **Multiple options**: Combine multiple options
10. **Option comparison**: Side-by-side comparison

## Common Use Cases

### Tab-Delimited Files
```sql
COPY table TO 'data.tsv' WITH (DELIMITER '\t');
```

### Semicolon-Delimited (European Format)
```sql
COPY table TO 'data.csv' WITH (DELIMITER ';');
```

### No Headers for Machine Processing
```sql
COPY table TO 'data.csv' WITH (HEADER false);
```

### Financial Data with Quotes
```sql
COPY financial_data TO 'data.csv' WITH (FORCE_QUOTE (amount, balance));
```

### International Data
```sql
COPY table TO 'data.csv' WITH (ENCODING 'UTF-8', DELIMITER ';');
```

## Option Combinations

### European CSV Standard
```sql
COPY table TO 'european.csv' WITH (
  DELIMITER ';',
  DECIMAL ',',
  HEADER true,
  ENCODING 'UTF-8'
);
```

### Machine-Readable Format
```sql
COPY table TO 'machine.csv' WITH (
  HEADER false,
  DELIMITER '|',
  NULL '\\N',
  ESCAPE '\\'
);
```

### Human-Readable Format
```sql
COPY table TO 'human.csv' WITH (
  HEADER true,
  FORCE_QUOTE *,
  NULL 'N/A',
  DATEFORMAT '%d/%m/%Y'
);
```

## Error Handling

Common issues and solutions:

1. **Invalid delimiter**: Must be a single character
2. **Encoding errors**: Ensure database and file encoding match
3. **Special characters**: Use appropriate escape characters
4. **Date format errors**: Use valid strftime format strings

## Best Practices

1. **Choose appropriate delimiters** based on data content
2. **Use headers** for human-readable files
3. **Quote fields** containing special characters
4. **Handle NULLs explicitly** for clarity
5. **Specify encoding** for international data
6. **Test exports** with sample data first

## Performance Considerations

- Force quoting increases file size
- UTF-8 is generally most efficient
- Tab delimiters can be faster to parse
- Headers add minimal overhead

## Example Output

```
=== CSV Writing with Options Example ===

2. Export with custom delimiter (pipe):
Exported with pipe delimiter to products_pipe.csv:
id|name|category|price|stock|in_stock
1|Laptop Computer|Electronics|1299.99|15|true
2|Wireless Mouse|Accessories|29.99|50|true
3|USB-C Hub|Accessories|49.99|0|false
4|Mechanical Keyboard|Accessories|89.99|25|true
5|27" Monitor|Electronics|399.99|10|true

3. Export without header row:
Exported without header to products_no_header.csv:
1,Laptop Computer,Electronics,1299.99,15,true
2,Wireless Mouse,Accessories,29.99,50,true
3,USB-C Hub,Accessories,49.99,0,false
4,Mechanical Keyboard,Accessories,89.99,25,true
5,27" Monitor,Electronics,399.99,10,true

4. Export with custom null representation:
Exported with custom null to orders_custom_null.csv:
order_id,customer_id,order_date,total,discount,status
101,1001,2023-01-15,150.50,N/A,Completed
102,1002,2023-01-16,299.99,15.00,Completed
103,1003,2023-01-17,N/A,N/A,Pending
104,1004,2023-01-18,89.99,5.00,Completed

5. Export with all fields quoted:
Exported with all fields quoted to products_quoted.csv:
"id","name","category","price","stock","in_stock"
"1","Laptop Computer","Electronics","1299.99","15","true"
"2","Wireless Mouse","Accessories","29.99","50","true"
"3","USB-C Hub","Accessories","49.99","0","false"
"4","Mechanical Keyboard","Accessories","89.99","25","true"
"5","27"" Monitor","Electronics","399.99","10","true"
```

## Date Format Patterns

Common date format patterns:
- `%Y`: 4-digit year (2023)
- `%m`: Month as number (01-12)
- `%d`: Day of month (01-31)
- `%H`: Hour 24-hour format (00-23)
- `%M`: Minute (00-59)
- `%S`: Second (00-59)

## Tips

1. **Test with sample data** before exporting large datasets
2. **Document export options** used for reproducibility
3. **Consider downstream systems** when choosing options
4. **Validate exports** by reading them back
5. **Use consistent options** across related exports

## Summary

CSV export options provide flexibility for various use cases:
- **Data exchange**: Use standard formats
- **Machine processing**: Optimize for parsing
- **Human readability**: Include headers and formatting
- **International use**: Handle encoding and locales
- **Special requirements**: Customize delimiters and quoting

Choose options based on your specific needs and the requirements of systems that will consume the CSV files. Always test the exported files with the intended downstream processes to ensure compatibility."} "file_path"/home/connerohnesorge/Documents/001Repos/dukdb-go/examples/csv-write-options/README.md