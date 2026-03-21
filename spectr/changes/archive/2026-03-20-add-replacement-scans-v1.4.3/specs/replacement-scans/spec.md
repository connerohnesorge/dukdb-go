# Replacement Scans — File Path as Table Reference

## ADDED Requirements

### Requirement: String literals in FROM position SHALL be treated as file path table references

When a string literal appears where a table name is expected in a FROM clause, the engine SHALL interpret it as a file path and automatically select the appropriate reader function based on file extension.

#### Scenario: CSV file path
```
When the user executes "SELECT * FROM 'test.csv'"
Then the result is equivalent to "SELECT * FROM read_csv_auto('test.csv')"
```

#### Scenario: Parquet file path
```
When the user executes "SELECT * FROM 'test.parquet'"
Then the result is equivalent to "SELECT * FROM read_parquet('test.parquet')"
```

#### Scenario: JSON file path
```
When the user executes "SELECT * FROM 'test.json'"
Then the result is equivalent to "SELECT * FROM read_json_auto('test.json')"
```

#### Scenario: NDJSON file path
```
When the user executes "SELECT * FROM 'test.ndjson'"
Then the result is equivalent to "SELECT * FROM read_ndjson('test.ndjson')"
```

#### Scenario: Unrecognized extension returns error
```
When the user executes "SELECT * FROM 'data.xyz'"
Then an error is returned indicating the file format cannot be determined
```

### Requirement: Replacement scans SHALL support table aliases

File path table references SHALL support the AS keyword for aliasing, consistent with standard table reference behavior.

#### Scenario: Alias with file path
```
When the user executes "SELECT t.col1 FROM 'test.csv' AS t"
Then the result uses alias t for column resolution
```

#### Scenario: File path in JOIN
```
When the user executes "SELECT a.id, b.value FROM 'left.csv' AS a JOIN 'right.csv' AS b ON a.id = b.id"
Then the join operates on both file references
```

### Requirement: Replacement scans SHALL support cloud URLs

File path strings containing cloud storage URLs (s3://, gs://, az://, https://) SHALL be routed through the existing cloud filesystem infrastructure.

#### Scenario: S3 URL as table reference
```
When the user executes "SELECT * FROM 's3://bucket/data.parquet'"
Then the result is equivalent to "SELECT * FROM read_parquet('s3://bucket/data.parquet')"
```

#### Scenario: HTTPS URL as table reference
```
When the user executes "SELECT * FROM 'https://example.com/data.csv'"
Then the result is equivalent to "SELECT * FROM read_csv_auto('https://example.com/data.csv')"
```
