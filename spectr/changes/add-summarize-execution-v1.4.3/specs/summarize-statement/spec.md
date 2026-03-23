## ADDED Requirements

### Requirement: SUMMARIZE table SHALL return per-column statistics

The system SHALL execute `SUMMARIZE table_name` by scanning all rows and computing per-column statistics. The result SHALL be a table with one row per column and 12 columns: column_name (VARCHAR), column_type (VARCHAR), min (VARCHAR), max (VARCHAR), approx_unique (BIGINT), avg (DOUBLE), std (DOUBLE), q25 (VARCHAR), q50 (VARCHAR), q75 (VARCHAR), count (BIGINT), null_percentage (DOUBLE).

#### Scenario: SUMMARIZE numeric table

- WHEN the table 'scores' has columns (id INT, value DOUBLE) with rows (1, 10.0), (2, 20.0), (3, 30.0)
- AND the user executes `SUMMARIZE scores`
- THEN the result has 2 rows (one for 'id', one for 'value')
- AND the 'value' row has min='10.0', max='30.0', avg=20.0, count=3, null_percentage=0.0

#### Scenario: SUMMARIZE with NULL values

- WHEN the table 'data' has column (x INT) with rows (1), (NULL), (3)
- AND the user executes `SUMMARIZE data`
- THEN null_percentage is approximately 33.33
- AND count is 2

#### Scenario: SUMMARIZE empty table

- WHEN the table 'empty' has columns but no rows
- AND the user executes `SUMMARIZE empty`
- THEN the result has one row per column with count=0 and null_percentage=0.0

#### Scenario: SUMMARIZE with non-numeric columns

- WHEN the table has a VARCHAR column
- AND the user executes `SUMMARIZE table_name`
- THEN avg and std are NULL for the VARCHAR column
- AND min, max, q25, q50, q75 contain string representations

### Requirement: SUMMARIZE SELECT SHALL compute statistics over query results

The system SHALL execute `SUMMARIZE SELECT ...` by running the inner query and computing the same per-column statistics over the result set.

#### Scenario: SUMMARIZE query result

- WHEN the user executes `SUMMARIZE SELECT price FROM products WHERE category = 'Electronics'`
- THEN the result shows statistics for the 'price' column filtered to electronics products

#### Scenario: SUMMARIZE with aggregation query

- WHEN the user executes `SUMMARIZE SELECT category, COUNT(*) as cnt FROM products GROUP BY category`
- THEN the result shows statistics for 'category' and 'cnt' columns

### Requirement: SUMMARIZE SHALL support schema-qualified table names

The system SHALL resolve schema-qualified table references in SUMMARIZE statements.

#### Scenario: SUMMARIZE with schema prefix

- WHEN the table 'analytics.events' exists
- AND the user executes `SUMMARIZE analytics.events`
- THEN the result shows statistics for all columns in analytics.events
