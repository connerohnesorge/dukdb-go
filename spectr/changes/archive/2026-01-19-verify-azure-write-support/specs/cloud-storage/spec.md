## ADDED Requirements

### Requirement: Azure Blob Storage Write Verification

The system SHALL support writing data to Azure Blob Storage via COPY TO statements.

#### Scenario: Write Parquet to Azure

- GIVEN a table with data
- AND an Azure secret is configured with valid credentials
- WHEN executing `COPY my_table TO 'azure://container/path/data.parquet' (FORMAT PARQUET)`
- THEN a Parquet file SHALL be created in Azure Blob Storage
- AND the file SHALL be readable by other tools

#### Scenario: Write CSV to Azure

- GIVEN a table with data
- AND an Azure secret is configured
- WHEN executing `COPY my_table TO 'az://container/path/data.csv' (FORMAT CSV)`
- THEN a CSV file SHALL be created in Azure Blob Storage
- AND the file SHALL be accessible via Azure SDK

#### Scenario: Write with connection string authentication

- GIVEN an Azure secret with connection string: `CREATE SECRET azure (TYPE AZURE, CONNECTION_STRING '...')`
- WHEN writing to Azure Blob Storage
- THEN the connection string SHALL be used for authentication
- AND write operation SHALL succeed

#### Scenario: Write with account key authentication

- GIVEN an Azure secret with account credentials: `CREATE SECRET azure (TYPE AZURE, ACCOUNT_NAME 'acct', ACCOUNT_KEY 'key')`
- WHEN writing to Azure Blob Storage
- THEN the account credentials SHALL be used for authentication
- AND write operation SHALL succeed

#### Scenario: Large file multipart upload

- GIVEN a table with >256MB of data
- WHEN writing to Azure Blob Storage
- THEN multipart upload SHALL be used if available
- AND the file SHALL be written correctly

#### Scenario: Write to existing container

- GIVEN an existing Azure Blob Storage container
- WHEN writing a file to that container
- THEN the container SHALL not be created
- AND the file SHALL be written to the container

#### Scenario: Write to non-existent container

- GIVEN Azure credentials with create container permission
- WHEN writing to a non-existent container
- THEN the container SHALL be created automatically
- AND the file SHALL be written to the new container

### Requirement: Azure Read/Write Parity

The system SHALL provide read and write access to Azure Blob Storage with feature parity.

#### Scenario: Read what you write

- GIVEN data written to Azure via COPY TO
- WHEN reading the same data via read_parquet()
- THEN all rows SHALL match the original data
- AND column types SHALL be preserved

#### Scenario: Round-trip through Azure

- GIVEN local table `local_table`
- WHEN executing:
  1. `COPY local_table TO 'azure://container/data.parquet'`
  2. `CREATE TABLE azure_table AS SELECT * FROM read_parquet('azure://container/data.parquet')`
- THEN `azure_table` SHALL contain identical data to `local_table`

### Requirement: Azure URL Formats

The system SHALL support multiple Azure URL formats.

#### Scenario: azure:// scheme

- GIVEN a file at 'azure://container/blob.parquet'
- WHEN reading or writing
- THEN Azure Blob Storage SHALL be accessed

#### Scenario: az:// scheme

- GIVEN a file at 'az://container/blob.parquet'
- WHEN reading or writing
- THEN Azure Blob Storage SHALL be accessed

#### Scenario: wasb:// scheme (legacy)

- GIVEN a file at 'wasb://container/blob.parquet'
- WHEN reading or writing
- THEN Azure Blob Storage SHALL be accessed (legacy compatibility)
