# Security Specification

## ADDED Requirements

### Requirement: Secret Manager

The system SHALL provide a secrets manager for storing and retrieving credentials for cloud storage and HTTP connections.

#### Scenario: Create S3 secret
- GIVEN no existing secrets
- WHEN executing `CREATE SECRET my_s3 (TYPE S3, PROVIDER config, KEY_ID 'xxx', SECRET 'yyy')`
- THEN secret is stored with type S3 and credentials
- AND secret is available for S3 operations

#### Scenario: Create S3 secret with scope
- GIVEN no existing secrets
- WHEN executing `CREATE SECRET my_s3 (TYPE S3, PROVIDER config, KEY_ID 'xxx', SECRET 'yyy', SCOPE 's3://my-bucket/')`
- THEN secret is stored with scope prefix 's3://my-bucket/'
- AND secret is only used for paths starting with that prefix

#### Scenario: Create S3 secret with env provider
- GIVEN AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in environment
- WHEN executing `CREATE SECRET my_s3 (TYPE S3, PROVIDER env)`
- THEN secret stores provider type as ENV
- AND credentials are resolved from environment at runtime

#### Scenario: Create S3 secret with credential chain
- GIVEN no explicit credentials
- WHEN executing `CREATE SECRET my_s3 (TYPE S3, PROVIDER credential_chain)`
- THEN secret stores provider type as CREDENTIAL_CHAIN
- AND credentials are resolved using the chain: config > env > IMDSv2

#### Scenario: Create Azure secret
- GIVEN Azure credentials
- WHEN executing `CREATE SECRET my_azure (TYPE AZURE, PROVIDER config, ACCOUNT_NAME 'myaccount', ACCESS_KEY 'xxx')`
- THEN secret is stored with type AZURE
- AND secret is available for Azure operations

#### Scenario: Create GCS secret
- GIVEN GCS credentials
- WHEN executing `CREATE SECRET my_gcs (TYPE GCS, PROVIDER config, KEY_FILE '/path/to/key.json')`
- THEN secret is stored with type GCS
- AND secret is available for GCS operations

#### Scenario: Create HTTP secret
- GIVEN HTTP basic auth credentials
- WHEN executing `CREATE SECRET my_http (TYPE HTTP, PROVIDER config, USER 'admin', PASSWORD 'secret')`
- THEN secret is stored with type HTTP
- AND secret is used for HTTP requests matching scope

#### Scenario: Create secret with HuggingFace provider
- GIVEN HuggingFace token
- WHEN executing `CREATE SECRET my_hf (TYPE HUGGINGFACE, PROVIDER config, TOKEN 'hf_xxx')`
- THEN secret is stored with type HUGGINGFACE
- AND secret is used for HuggingFace hub access

#### Scenario: Drop existing secret
- GIVEN a secret named 'my_s3' exists
- WHEN executing `DROP SECRET my_s3`
- THEN secret is removed from storage
- AND secret is no longer available for operations

#### Scenario: Drop non-existent secret with IF EXISTS
- GIVEN no secret named 'my_s3' exists
- WHEN executing `DROP SECRET IF EXISTS my_s3`
- THEN no error is returned
- AND operation completes successfully

#### Scenario: Alter secret
- GIVEN a secret named 'my_s3' exists with access key
- WHEN executing `ALTER SECRET my_s3 (SET SECRET 'new-secret')`
- THEN secret's secret value is updated
- AND other options remain unchanged

#### Scenario: Alter non-existent secret with IF EXISTS
- GIVEN no secret named 'my_s3' exists
- WHEN executing `ALTER SECRET IF EXISTS my_s3 (SET SECRET 'new')`
- THEN no error is returned
- AND operation completes successfully

### Requirement: Secret Persistence

Secrets SHALL be persisted in the database catalog and restored on connection.

#### Scenario: Secret persists across connections
- GIVEN a secret created in one session
- WHEN connection is closed and reopened
- THEN secret is loaded from catalog
- AND secret is available for operations

#### Scenario: Secret persists across database restarts
- GIVEN a secret created
- WHEN database is shut down and restarted
- THEN secret is loaded from persistent storage
- AND secret is available for operations

#### Scenario: Temporary secret
- GIVEN a temporary secret created with TEMPORARY option
- WHEN connection is closed
- THEN secret is not persisted
- AND secret is only available in current session

### Requirement: Secret Scope Matching

Secrets SHALL be matched to operations based on path or host scope.

#### Scenario: Path scope matching for S3
- GIVEN a secret with scope 's3://my-bucket/data/'
- WHEN accessing 's3://my-bucket/data/file.parquet'
- THEN secret is matched based on longest prefix
- AND secret is applied to the operation

#### Scenario: Global scope matching
- GIVEN a secret with no scope (global)
- WHEN accessing any S3 path
- THEN secret is matched
- AND used as fallback for non-scoped paths

#### Scenario: Host scope matching for HTTP
- GIVEN a secret with scope 'https://api.example.com/'
- WHEN accessing 'https://api.example.com/data.json'
- THEN secret is matched based on host prefix
- AND secret is applied to the operation

#### Scenario: Multiple secrets with different scopes
- GIVEN secret A with scope 's3://bucket-a/' and secret B with scope 's3://bucket-a/prefix/'
- WHEN accessing 's3://bucket-a/prefix/file.parquet'
- THEN secret B is selected (longest prefix match)
- AND secret A is not used

### Requirement: Secret Lookup for Operations

File operations SHALL automatically look up applicable secrets.

#### Scenario: S3 read uses secret
- GIVEN a secret for 's3://my-bucket/' exists
- WHEN reading from 's3://my-bucket/file.parquet'
- THEN secret is retrieved and credentials applied
- AND operation uses the secret's credentials

#### Scenario: S3 write uses secret
- GIVEN a secret for 's3://my-bucket/' exists
- WHEN writing to 's3://my-bucket/file.parquet'
- THEN secret is retrieved and credentials applied
- AND operation uses the secret's credentials

#### Scenario: No matching secret
- GIVEN no secret for the path exists
- WHEN accessing 's3://bucket/file.parquet'
- THEN operation proceeds without credentials
- AND may fail if credentials are required

### Requirement: Secret System Functions

The system SHALL provide functions for querying secrets.

#### Scenario: which_secret function
- GIVEN secrets with various scopes
- WHEN executing `SELECT * FROM which_secret('s3://bucket/path/file.parquet', 'S3')`
- THEN returns the secret name and provider for the path
- AND returns NULL if no matching secret

#### Scenario: duckdb_secrets function
- GIVEN several secrets exist in the catalog
- WHEN executing `SELECT * FROM duckdb_secrets()`
- THEN returns a table with all secret metadata
- AND includes name, type, provider, scope (not credentials)

#### Scenario: duckdb_secrets shows masked credentials
- GIVEN a secret with credentials
- WHEN selecting from duckdb_secrets()
- THEN credentials are masked or omitted
- AND only non-sensitive metadata is exposed

### Requirement: Secret Types

The system SHALL support multiple secret types for different storage backends.

#### Scenario: S3 secret type
- GIVEN a secret with TYPE S3
- WHEN used for S3 operations
- THEN key_id, secret, region, session_token are available
- AND endpoint override is supported

#### Scenario: GCS secret type
- GIVEN a secret with TYPE GCS
- WHEN used for GCS operations
- THEN key_file or credentials JSON are available
- AND project_id is supported

#### Scenario: Azure secret type
- GIVEN a secret with TYPE AZURE
- WHEN used for Azure operations
- THEN account_name and access_key are available
- AND tenant_id, client_id, client_secret for OAuth

#### Scenario: HTTP secret type
- GIVEN a secret with TYPE HTTP
- WHEN used for HTTP operations
- THEN user and password for basic auth are available
- AND token for bearer auth is supported

#### Scenario: HuggingFace secret type
- GIVEN a secret with TYPE HUGGINGFACE
- WHEN used for HuggingFace hub operations
- THEN token is available for API access

### Requirement: Secret Providers

The system SHALL support multiple credential providers for each secret type.

#### Scenario: Config provider
- GIVEN a secret with PROVIDER config
- WHEN resolving credentials
- THEN credentials are read directly from secret options
- AND used for authentication

#### Scenario: Env provider
- GIVEN a secret with PROVIDER env
- WHEN resolving credentials
- THEN credentials are read from environment variables
- AND environment variable names are provider-specific

#### Scenario: Credential chain provider
- GIVEN a secret with PROVIDER credential_chain
- WHEN resolving credentials
- THEN multiple providers are tried in sequence
- AND first successful provider is used

#### Scenario: IAM provider for S3
- GIVEN an S3 secret with PROVIDER iam
- WHEN resolving credentials in EC2
- THEN credentials are fetched from IMDSv2
- AND temporary credentials are provided

### Requirement: Secret Security

Secrets SHALL be handled securely with appropriate protections.

#### Scenario: Secrets not logged
- GIVEN secret values are stored
- WHEN logging operations
- THEN secret values are never logged
- AND logging shows masked values

#### Scenario: Secrets encrypted at rest
- GIVEN secrets are persisted to storage
- WHEN stored in database catalog
- THEN secrets are encrypted
- AND decrypted only when loaded into memory

#### Scenario: Secret memory protection
- GIVEN secret values are in memory
- WHEN secret is no longer needed
- THEN memory may be zeroed (future enhancement)
- AND secrets are not persisted longer than necessary

#### Scenario: Read-only secret access
- GIVEN user has read-only database access
- WHEN listing secrets with duckdb_secrets()
- THEN only non-sensitive metadata is visible
- AND secret credentials are not exposed

## MODIFIED Requirements

### Requirement: COPY Statement with Secrets

COPY statement SHALL automatically use applicable secrets for cloud storage operations.

**Previous**: No secret integration
**Updated**: COPY statements automatically look up and use secrets for cloud URLs

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

### Requirement: Table Functions with Secrets

Table functions SHALL automatically use applicable secrets for cloud storage operations.

**Previous**: No secret integration
**Updated**: Table functions automatically look up and use secrets for cloud URLs

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

## REMOVED Requirements

### Requirement: No Native Credential Storage

Previous approaches using connection string parameters for credentials are deprecated.

**Reason**: Secrets manager provides a more secure and manageable approach

#### Migration
- Replace `?access_key_id=xxx&secret_access_key=yyy` in connection strings
- Use `CREATE SECRET` statements instead
- Existing connection string parameters may still work for backward compatibility
