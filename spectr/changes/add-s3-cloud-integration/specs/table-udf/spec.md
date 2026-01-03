# Table Udf Specification

## ADDED Requirements

### Requirement: Table Functions with Secrets

Table functions SHALL automatically use applicable secrets for cloud storage operations.

#### Scenario: read_parquet from S3 uses secret
- GIVEN a secret exists for 's3://my-bucket/'
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/data.parquet')`
- THEN secret is retrieved and credentials applied
- AND Parquet file is read from S3 using the credentials

#### Scenario: read_csv from GCS uses secret
- GIVEN a secret exists for 'gs://my-bucket/'
- WHEN executing `SELECT * FROM read_csv('gs://my-bucket/data.csv')`
- THEN secret is retrieved and credentials applied
- AND CSV file is read from GCS using the credentials

#### Scenario: glob pattern on S3 uses secret
- GIVEN a secret exists for 's3://my-bucket/'
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/*.parquet')`
- THEN secret is retrieved and credentials applied
- AND matching files are listed and read using the credentials

#### Scenario: write_json to Azure uses secret
- GIVEN a secret exists for 'azure://my-container/'
- WHEN executing `COPY (SELECT * FROM table) TO 'azure://my-container/data.json'`
- THEN secret is retrieved and credentials applied
- AND JSON file is written to Azure using the credentials