# Copy Statement Specification

## ADDED Requirements

### Requirement: COPY Statement with Secrets

COPY statement SHALL automatically use applicable secrets for cloud storage operations.

#### Scenario: COPY FROM S3 uses secret
- GIVEN a secret exists for 's3://my-bucket/'
- WHEN executing `COPY table FROM 's3://my-bucket/data.csv'`
- THEN secret is retrieved and credentials applied
- AND data is read from S3 using the credentials

#### Scenario: COPY TO S3 uses secret
- GIVEN a secret exists for 's3://my-bucket/'
- WHEN executing `COPY table TO 's3://my-bucket/output.parquet'`
- THEN secret is retrieved and credentials applied
- AND data is written to S3 using the credentials

#### Scenario: COPY with anonymous access
- GIVEN no secret exists for public S3 bucket
- WHEN executing `COPY FROM 's3://public-bucket/data.csv'`
- THEN COPY proceeds without credentials
- AND operation succeeds if bucket allows anonymous access