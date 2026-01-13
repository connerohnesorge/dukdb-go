# CSV Auto-Detection Example

This example demonstrates the `read_csv_auto()` function which automatically detects CSV format properties including delimiter, data types, date formats, and more.

## Concept

The `read_csv_auto()` function intelligently analyzes CSV files to determine:
- Delimiter (comma, semicolon, tab, pipe, etc.)
- Data types for each column
- Date and timestamp formats
- Presence of header row
- Quoting style
- Null value representations

## Key Features

1. **Automatic Delimiter Detection**: Identifies the delimiter used in the file
2. **Type Inference**: Automatically determines appropriate data types for columns
3. **Date Format Recognition**: Detects various date formats
4. **Header Detection**: Determines if first row contains column names
5. **Null Value Detection**: Identifies patterns for missing values
6. **Quoted Value Handling**: Properly handles quoted fields

## Usage

### Basic Usage
```sql
-- Auto-detect all CSV properties
SELECT * FROM read_csv_auto('filename.csv');
```

### With Additional Options
```sql
-- Auto-detect with custom null string
SELECT * FROM read_csv_auto('filename.csv', nullstr := 'N/A');

-- Auto-detect with sample size limit
SELECT * FROM read_csv_auto('filename.csv', sample_size := 1000);
```

## Examples in This Demo

1. **Auto-detect delimiter**: Identifies semicolon, tab, or pipe delimiters
2. **Auto-detect data types**: Recognizes integers, floats, booleans, dates
3. **Irregular formatting**: Handles quoted values with embedded commas
4. **Date format detection**: Recognizes various date formats
5. **Null value detection**: Identifies empty fields as NULL
6. **Numeric format detection**: Handles currency symbols and percentages
7. **Comparison with read_csv**: Shows the advantage of auto-detection

## Common Auto-Detection Patterns

### Delimiter Detection
```
comma:      id,name,value
semicolon:  id;name;value
tab:        id\tname\tvalue
pipe:       id|name|value
```

### Data Type Detection
- **INTEGER**: Whole numbers without decimal points
- **FLOAT**: Numbers with decimal points
- **BOOLEAN**: true/false, yes/no, 1/0
- **DATE**: Various date formats (YYYY-MM-DD, DD/MM/YYYY, etc.)
- **VARCHAR**: Text data

### Date Format Patterns
```
ISO:        2023-01-15
European:   15/01/2023
US:         01/15/2023
Text:       15-Jan-2023
```

## When to Use read_csv_auto vs read_csv

### Use `read_csv_auto()` when:
- CSV format is unknown
- Exploring new datasets
- Building generic data loading tools
- Prototyping and data analysis

### Use `read_csv()` when:
- CSV format is known and consistent
- Performance is critical
- Specific options are needed
- Reproducible results are required

## Performance Considerations

- Auto-detection requires scanning the file
- Default sample size is -1 (scan entire file)
- Use `sample_size` parameter for large files
- Explicit options in `read_csv()` are faster

## Error Handling

The example includes handling for:
- Ambiguous delimiters
- Mixed data types in columns
- Unparseable dates
- Encoding issues
- File access errors

## Best Practices

1. **Start with read_csv_auto()** for exploration
2. **Switch to read_csv()** once format is known
3. **Specify sample_size** for very large files
4. **Verify detected types** using DESCRIBE
5. **Handle encoding issues** explicitly
6. **Test with representative data** samples

## Example Output

```
=== CSV Auto-Detection Example ===

1. Auto-detecting delimiter:
Column Information:
Column Name          Column Type          Nullable
------------------------------------------------------------
timestamp            TIMESTAMP            YES
user_id              INTEGER              YES
action               VARCHAR              YES
value                INTEGER              YES

Data:
timestamp | user_id | action | value
----------------------------------------
2023-01-15 10:30:00 | 1001 | login | 1
2023-01-15 10:35:00 | 1002 | purchase | 99
2023-01-15 10:40:00 | 1003 | view | 0
2023-01-15 10:45:00 | 1001 | logout | 1

2. Auto-detecting data types:
Column Information:
Column Name          Column Type          Nullable
------------------------------------------------------------
id                   INTEGER              YES
name                 VARCHAR              YES
age                  INTEGER              YES
salary               DOUBLE               YES
is_active            BOOLEAN              YES
join_date            DATE                 YES

Data:
id | name | age | salary | is_active | join_date
--------------------------------------------------
1 | Alice | 25 | 75000.5 | true | 2020-01-15
2 | Bob | 30 | 85000 | false | 2019-06-20
3 | Charlie | 28 | 80000 | true | 2021-03-10
4 | Diana | 35 | 95000.75 | true | 2018-11-05
```

## Limitations

- Cannot detect all possible date formats
- May misinterpret numeric codes as numbers
- Requires sufficient sample data for accurate detection
- Some edge cases may require manual specification

## Tips for Better Auto-Detection

1. Ensure consistent formatting throughout the file
2. Provide sufficient rows for sampling (at least 100)
3. Use standard date formats when possible
4. Avoid mixing data types in the same column
5. Use clear null representations (empty strings work best)