# JSON Nested Example

This example demonstrates working with complex nested JSON structures in dukdb-go, including parsing nested objects, arrays, and multi-level hierarchies.

## Overview

Real-world JSON data often contains deeply nested structures with objects, arrays, and mixed data types. This example shows how to work with such complex data structures effectively.

## Key Concepts

### Nested JSON Structure
Nested JSON can contain:
- Objects within objects
- Arrays of objects
- Mixed data types
- Multiple levels of nesting

### Handling in SQL
- Nested objects are returned as JSON strings
- Arrays are returned as JSON strings
- Can be parsed using application code

## Examples Included

1. **Reading Nested JSON Structure**: Understanding the flattened output
2. **Parsing Nested JSON in Go**: Using Go's json package to parse structures
3. **Creating Flattened Views**: Working with denormalized data
4. **Aggregating Nested Data**: Calculating statistics from nested structures
5. **Extracting Specific Values**: Navigating to specific nested fields
6. **Working with Arrays**: Processing JSON arrays
7. **Creating Summary Reports**: Generating reports from nested data

## Running the Example

```bash
cd json-nested
go run main.go
```

## Sample Data

The example creates sample company data with multiple levels of nesting:
- Company information
- Employee arrays with nested address and project data
- Department objects
- Financial data with quarterly breakdowns

## Data Structure

```json
{
  "company": "TechCorp",
  "employees": [
    {
      "id": 101,
      "name": "Alice Johnson",
      "department": "Engineering",
      "skills": ["Go", "Python", "Kubernetes"],
      "address": {
        "street": "123 Tech St",
        "city": "San Francisco",
        "state": "CA",
        "zip": "94105"
      },
      "projects": [
        {
          "name": "Cloud Migration",
          "status": "completed",
          "budget": 500000,
          "timeline": {
            "start": "2023-01-01",
            "end": "2023-06-30"
          }
        }
      ]
    }
  ],
  "departments": {
    "Engineering": {
      "head": "Alice Johnson",
      "budget": 2000000,
      "locations": ["San Francisco", "Seattle"]
    }
  },
  "financials": {
    "revenue": 10000000,
    "expenses": 7500000,
    "profit": 2500000,
    "quarters": [
      {"q": "Q1", "revenue": 2000000, "profit": 400000}
    ]
  }
}
```

## SQL Output

When queried, nested structures are flattened:
- Columns: `company`, `employees`, `departments`, `financials`
- Nested objects become JSON strings
- Arrays become JSON strings

## Parsing Examples

### Parsing Employee Data
```go
var empData []map[string]interface{}
json.Unmarshal([]byte(employees), &empData)
for _, emp := range empData {
    fmt.Printf("%s works in %s\n", emp["name"], emp["department"])
}
```

### Extracting Nested Values
```go
var finData map[string]interface{}
json.Unmarshal([]byte(financials), &finData)
revenue := finData["revenue"].(float64)
```

### Working with Arrays
```go
if quarters, ok := finData["quarters"].([]interface{}); ok {
    for _, q := range quarters {
        quarter := q.(map[string]interface{})
        fmt.Printf("%s: $%.0f\n", quarter["q"], quarter["revenue"])
    }
}
```

## Best Practices

1. **Understand the Structure**: Know your JSON schema before querying
2. **Use Type Assertions**: Safely extract values with type checking
3. **Handle Errors**: Always check for JSON parsing errors
4. **Consider Performance**: Large nested structures may be slow to parse
5. **Create Helper Functions**: Reuse parsing logic across your application

## Common Patterns

### Counting Array Elements
```go
var items []interface{}
json.Unmarshal([]byte(jsonArray), &items)
count := len(items)
```

### Accessing Deeply Nested Values
```go
if level1, ok := data["level1"].(map[string]interface{}); ok {
    if level2, ok := level1["level2"].(map[string]interface{}); ok {
        value := level2["value"]
    }
}
```

### Filtering Arrays
```go
var filtered []map[string]interface{}
for _, item := range items {
    if item["status"] == "active" {
        filtered = append(filtered, item)
    }
}
```

## Limitations

- No direct SQL access to nested fields
- All parsing must be done in application code
- Type assertions required for type safety
- Memory usage increases with nested depth

## Performance Tips

1. **Parse Only What You Need**: Don't parse entire structures if you only need parts
2. **Use Structs**: Define Go structs for better performance and type safety
3. **Stream Large Files**: For very large files, consider streaming parsers
4. **Cache Parsed Data**: Cache frequently accessed parsed data

## Advanced Usage

For production applications:
1. Define strict Go structs matching your JSON schema
2. Use json tags for field mapping
3. Implement custom unmarshaling for complex types
4. Consider using JSONPath libraries for complex queries
5. Validate JSON against schemas before parsing

## Output

The program will output:
- Column information showing flattened structure
- Parsed employee details
- Financial summaries
- Department heads
- Employee skills
- Aggregate statistics
- Summary report

## Notes

- This example demonstrates parsing in Go; other languages have similar JSON libraries
- For very complex queries, consider using specialized JSON databases
- The example shows manual parsing; in production, use proper error handling
- Consider the trade-off between flexibility (map[string]interface{}) and type safety (structs)