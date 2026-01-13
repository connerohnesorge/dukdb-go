# CSV Data Cleaning Example

This example demonstrates various data cleaning techniques for CSV files in dukdb-go, including validation, standardization, and transformation of messy data.

## Concept

Real-world CSV data often contains inconsistencies, errors, and formatting issues. This example shows how to use SQL functions and techniques to clean and standardize data before analysis.

## Common Data Quality Issues

1. **Whitespace**: Leading/trailing spaces, multiple spaces
2. **Case inconsistencies**: Mixed, upper, lower case
3. **Invalid formats**: Emails, phone numbers, dates
4. **Type mismatches**: Numbers as text, invalid dates
5. **NULL values**: Missing or placeholder values
6. **Outliers**: Values outside expected ranges

## Cleaning Techniques

### 1. Whitespace Cleaning
```sql
-- Remove leading and trailing spaces
TRIM(column_name)

-- Remove all spaces
REPLACE(column_name, ' ', '')

-- Replace multiple spaces with single space
REGEXP_REPLACE(column_name, '\s+', ' ')
```

### 2. Case Standardization
```sql
-- Convert to uppercase
UPPER(column_name)

-- Convert to lowercase
LOWER(column_name)

-- Convert to proper case (initcap)
INITCAP(column_name)
```

### 3. Email Validation
```sql
-- Basic email format check
column_name LIKE '%@%.%'

-- More comprehensive validation
REGEXP_MATCHES(column_name, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
```

### 4. Phone Number Standardization
```sql
-- Remove non-digit characters
REGEXP_REPLACE(phone, '[^0-9]', '', 'g')

-- Format as (XXX) XXX-XXXX
'(' || SUBSTRING(digits, 1, 3) || ') ' ||
SUBSTRING(digits, 4, 3) || '-' ||
SUBSTRING(digits, 7, 4)
```

### 5. Data Type Validation
```sql
-- Check if value is numeric
column_name ~ '^[0-9]+$'

-- Try to cast to number
TRY_CAST(column_name AS INTEGER)

-- Validate date
try_cast(column_name AS DATE) IS NOT NULL
```

## Examples in This Demo

1. **Data Quality Assessment**: Identify issues in the dataset
2. **Whitespace Cleaning**: Remove unwanted spaces
3. **Case Standardization**: Convert to consistent case
4. **Email Validation**: Check and clean email addresses
5. **Phone Standardization**: Format phone numbers consistently
6. **Type Validation**: Ensure data types are correct
7. **Date Validation**: Check and clean date values
8. **Cleaned Dataset**: Create a cleaned version of the data
9. **Quality Report**: Compare before and after cleaning

## Step-by-Step Cleaning Process

### Step 1: Assess Data Quality
```sql
-- Count NULL values in each column
SELECT
    COUNT(*) FILTER (WHERE column IS NULL) as null_count,
    COUNT(*) FILTER (WHERE TRIM(column) = '') as empty_count
FROM read_csv_auto('file.csv');
```

### Step 2: Clean Text Fields
```sql
-- Create cleaned version
SELECT
    TRIM(name) as clean_name,
    LOWER(TRIM(email)) as clean_email,
    INITCAP(TRIM(department)) as clean_department
FROM read_csv_auto('file.csv');
```

### Step 3: Validate Emails
```sql
-- Identify invalid emails
SELECT
    email,
    CASE
        WHEN email IS NULL THEN 'MISSING_EMAIL'
        WHEN email NOT LIKE '%@%.%' THEN 'INVALID_FORMAT'
        WHEN POSITION(' ' IN email) > 0 THEN 'CONTAINS_SPACE'
        ELSE 'VALID'
    END as email_status
FROM read_csv_auto('file.csv');
```

### Step 4: Standardize Phone Numbers
```sql
-- Format phone numbers
SELECT
    phone,
    REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as digits_only,
    CASE
        WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) = 10 THEN
            '(' || SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 1, 3) || ') ' ||
            SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 4, 3) || '-' ||
            SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 7, 4)
        ELSE NULL
    END as formatted_phone
FROM read_csv_auto('file.csv');
```

### Step 5: Validate Data Types
```sql
-- Check numeric fields
SELECT
    age,
    CASE
        WHEN age ~ '^[0-9]+$' THEN 'VALID_NUMBER'
        ELSE 'INVALID_FORMAT'
    END as validation,
    TRY_CAST(age AS INTEGER) as age_number
FROM read_csv_auto('file.csv');
```

### Step 6: Validate Dates
```sql
-- Check date fields
SELECT
    join_date,
    TRY_CAST(join_date AS DATE) as valid_date,
    CASE
        WHEN TRY_CAST(join_date AS DATE) IS NULL THEN 'INVALID_DATE'
        WHEN TRY_CAST(join_date AS DATE) > CURRENT_DATE THEN 'FUTURE_DATE'
        ELSE 'VALID'
    END as date_status
FROM read_csv_auto('file.csv');
```

### Step 7: Create Cleaned Dataset
```sql
-- Export cleaned data
COPY (
    SELECT
        id,
        INITCAP(TRIM(name)) as clean_name,
        LOWER(TRIM(email)) as clean_email,
        -- Additional cleaning...
    FROM read_csv_auto('file.csv')
    WHERE id IS NOT NULL  -- Remove invalid records
) TO 'cleaned_data.csv' WITH (HEADER true);
```

## Common Regex Patterns

### Email Validation
```
^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$
```

### Phone Number (Digits Only)
```
[^0-9]  -- Remove non-digits
```

### Whitespace Cleanup
```
^\s+|\s+$  -- Leading/trailing spaces
\s+        -- Multiple spaces
```

### Number Validation
```
^[0-9]+$           -- Integer
^[0-9]+\.[0-9]+$   -- Decimal
```

## Data Validation Functions

### DuckDB Validation Functions
- `TRY_CAST()`: Attempts to cast, returns NULL on failure
- `REGEXP_MATCHES()`: Pattern matching
- `POSITION()`: Find substring position
- `LENGTH()`: String length
- `TRIM()`: Remove whitespace

### Custom Validation Queries
```sql
-- Check age range
CASE
    WHEN TRY_CAST(age AS INTEGER) BETWEEN 18 AND 65 THEN 'VALID'
    WHEN TRY_CAST(age AS INTEGER) < 18 THEN 'TOO_YOUNG'
    WHEN TRY_CAST(age AS INTEGER) > 65 THEN 'TOO_OLD'
    ELSE 'INVALID'
END

-- Check for duplicates
SELECT email, COUNT(*)
FROM read_csv_auto('file.csv')
GROUP BY email
HAVING COUNT(*) > 1;
```

## Error Handling

### Handle Invalid Conversions
```sql
-- Use TRY_CAST to avoid errors
SELECT
    TRY_CAST(age AS INTEGER) as age_number,
    CASE
        WHEN TRY_CAST(age AS INTEGER) IS NULL THEN 'CONVERSION_FAILED'
        ELSE 'CONVERSION_SUCCESS'
    END as conversion_status
FROM read_csv_auto('file.csv');
```

### Handle NULL Values
```sql
-- Provide defaults for NULL values
SELECT
    COALESCE(TRIM(name), 'UNKNOWN_NAME') as clean_name,
    COALESCE(LOWER(TRIM(email)), 'noemail@unknown.com') as clean_email
FROM read_csv_auto('file.csv');
```

## Performance Tips

1. **Clean in stages**: Don't try to fix everything at once
2. **Use temporary tables**: Store intermediate results
3. **Index cleaned data**: Speed up subsequent queries
4. **Batch process**: Handle large files in chunks
5. **Validate early**: Check data quality before processing

## Quality Metrics

### Track Cleaning Progress
```sql
-- Calculate data quality score
SELECT
    COUNT(*) as total_rows,
    COUNT(CASE WHEN clean_name IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as name_completeness,
    COUNT(CASE WHEN valid_email THEN 1 END) * 100.0 / COUNT(*) as email_validity,
    COUNT(CASE WHEN valid_age THEN 1 END) * 100.0 / COUNT(*) as age_validity
FROM cleaned_data;
```

## Best Practices

1. **Always backup original data**
2. **Document all cleaning steps**
3. **Validate results after cleaning**
4. **Create reproducible scripts**
5. **Handle edge cases explicitly**
6. **Test with sample data first**
7. **Monitor performance on large files**

## Summary

Data cleaning is essential for accurate analysis. Key steps:
1. Assess data quality
2. Clean text (whitespace, case)
3. Validate formats (email, phone, dates)
4. Convert data types
5. Handle NULL values
6. Remove duplicates
7. Validate ranges
8. Create cleaned dataset
9. Generate quality report
10. Document the process

With these techniques, you can transform messy CSV data into clean, analysis-ready datasets."}