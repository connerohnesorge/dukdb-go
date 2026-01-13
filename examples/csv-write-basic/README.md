# CSV Write Basic Example

This example demonstrates the basic usage of writing data to CSV files in dukdb-go using the `COPY TO` statement.

## Concept

The `COPY TO` statement allows you to export data from DuckDB tables or query results directly to CSV files. This provides a simple way to persist query results or share data in a widely-supported format.

## Basic Syntax

```sql
-- Export entire table
COPY table_name TO 'filename.csv';

-- Export query results
COPY (SELECT * FROM table_name WHERE condition) TO 'filename.csv';

-- Export with header
COPY table_name TO 'filename.csv' WITH (HEADER true);
```

## Key Features

1. **Table Export**: Export entire tables to CSV
2. **Query Export**: Export any query result to CSV
3. **Automatic Headers**: Column names are included by default
4. **Type Preservation**: Data types are converted to appropriate string representations
5. **Flexible Output**: Support for various formatting options

## Examples in This Demo

1. **Basic table export**: Export entire table to CSV
2. **Query result export**: Export filtered and sorted data
3. **Parameterized export**: Use Go parameters in export queries
4. **Aggregated data export**: Export summary statistics
5. **Formatted export**: Export with calculated columns and formatting
6. **Verification**: Read exported CSV back to verify

## Usage Patterns

### Simple Export
```sql
COPY employees TO 'employees.csv';
```

### Export with Filtering
```sql
COPY (
  SELECT * FROM employees
  WHERE department = 'Engineering'
) TO 'engineers.csv';
```

### Export with Calculations
```sql
COPY (
  SELECT
    name,
    salary * 1.1 as new_salary,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, hire_date)) as tenure
  FROM employees
) TO 'salary_analysis.csv';
```

### Export with Formatting
```sql
COPY (
  SELECT
    '$' || CAST(salary AS VARCHAR) as formatted_salary,
    TO_CHAR(hire_date, 'Month DD, YYYY') as hire_date_formatted
  FROM employees
) TO 'formatted_report.csv';
```

## Go API Usage

### Basic Export
```go
_, err := db.Exec("COPY employees TO 'employees.csv'")
```

### Export with Parameters
```go
_, err := db.Exec("COPY (SELECT * FROM employees WHERE department = ?) TO 'dept.csv'", departmentName)
```

### Export Query Results
```go
query := "COPY (SELECT name, salary FROM employees WHERE salary > ?) TO 'high_earners.csv'"
_, err := db.Exec(query, minSalary)
```

## File Handling

### Check File Existence
```go
if _, err := os.Stat(filename); os.IsNotExist(err) {
    // File doesn't exist
}
```

### Read Exported File
```go
content, err := os.ReadFile(filename)
if err != nil {
    log.Fatal(err)
}
fmt.Println(string(content))
```

### Clean Up Temporary Files
```go
defer os.Remove(filename)  // Remove when done
```

## Error Handling

Common errors and solutions:

1. **Permission Denied**: Ensure write permissions to the directory
2. **Disk Full**: Check available disk space
3. **Invalid Path**: Use absolute paths or ensure relative path is correct
4. **Query Errors**: Verify the SELECT statement is valid

## Best Practices

1. **Use Absolute Paths**: Avoid relative path issues
2. **Check Permissions**: Ensure write access to target directory
3. **Clean Up**: Remove temporary files when done
4. **Verify Exports**: Read exported files to ensure correctness
5. **Use Transactions**: For consistent exports in multi-step operations
6. **Format Appropriately**: Use CAST and string functions for formatting

## Performance Tips

1. **Export Only Needed Columns**: Avoid SELECT *
2. **Filter Before Export**: Use WHERE clauses to reduce data
3. **Avoid Complex Calculations**: Do heavy processing before export
4. **Use Appropriate Types**: Ensure data types convert cleanly to strings

## Example Output

```
=== Basic CSV Writing Example ===

1. Creating sample data...

2. Basic CSV export using COPY TO:
Exported to employees_basic.csv:
id,name,department,salary,hire_date
1,Alice Johnson,Engineering,75000,2020-01-15
2,Bob Smith,Marketing,65000,2019-06-20
3,Charlie Brown,Sales,55000,2021-03-10
4,Diana Prince,HR,60000,2018-11-05
5,Eve Wilson,Engineering,80000,2022-01-30

3. Export query results to CSV:
Exported high earners to high_earners.csv:
name,department,salary,years_employed
Eve Wilson,Engineering,80000,1
Alice Johnson,Engineering,75000,3
Bob Smith,Marketing,65000,4

4. Export using Go API with prepared statement:
Exported Engineering team to engineering_team.csv:
id,name,salary,hire_date
1,Alice Johnson,75000,2020-01-15
5,Eve Wilson,80000,2022-01-30

5. Export aggregated department statistics:
Exported department statistics to dept_stats.csv:
department,employee_count,avg_salary,min_salary,max_salary
Engineering,2,77500,75000,80000
Marketing,1,65000,65000,65000
HR,1,60000,60000,60000
Sales,1,55000,55000,55000
```

## Advanced Options

While this example focuses on basic usage, dukdb-go also supports:
- Custom delimiters
- Quoting options
- Header control
- Compression
- Parallel export

See the `csv-write-options` example for these advanced features.

## Integration with Other Operations

CSV export works well with:
- Temporary tables for intermediate results
- Views for complex queries
- Joins and unions for combining data
- Window functions for advanced calculations

## Verification Strategy

Always verify exports by:
1. Checking file exists and has content
2. Reading back with `read_csv()`
3. Comparing row counts
4. Spot-checking values

This ensures data integrity and catches any formatting issues early.