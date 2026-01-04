# Secrets Management

dukdb-go provides a comprehensive secrets management system for storing and managing credentials used to access cloud storage providers. This guide explains how to create, manage, and use secrets for S3, GCS, Azure, and HTTP authentication.

## Overview

Secrets in dukdb-go store credentials and configuration for accessing cloud resources. Key features include:

- **Scope-based matching**: Secrets can be scoped to specific buckets, paths, or applied globally
- **Multiple providers**: Support for explicit credentials, environment variables, and credential chains
- **Persistence**: Secrets can be stored persistently or only for the current session
- **Security**: Credentials are stored securely and never logged

## Creating Secrets

### Basic Syntax

```sql
CREATE [PERSISTENT | TEMPORARY] SECRET [IF NOT EXISTS] secret_name (
    TYPE secret_type,
    [PROVIDER provider_type,]
    [SCOPE 'scope_url',]
    option1 'value1',
    option2 'value2',
    ...
);
```

### Parameters

| Parameter | Description |
|-----------|-------------|
| `PERSISTENT` | Secret is stored and available across sessions (default) |
| `TEMPORARY` | Secret exists only for the current session |
| `IF NOT EXISTS` | Do not error if secret already exists |
| `secret_name` | Unique identifier for the secret |
| `TYPE` | Cloud provider type (S3, GCS, AZURE, HTTP) |
| `PROVIDER` | Credential provider type (CONFIG, ENV, CREDENTIAL_CHAIN, IAM) |
| `SCOPE` | URL prefix this secret applies to |

## Secret Types

### S3 (Amazon Web Services)

```sql
CREATE SECRET my_s3_secret (
    TYPE S3,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    REGION 'us-east-1'
);
```

#### S3 Options

| Option | Description | Required |
|--------|-------------|----------|
| `KEY_ID` | AWS access key ID | Yes (for CONFIG provider) |
| `SECRET` | AWS secret access key | Yes (for CONFIG provider) |
| `SESSION_TOKEN` | AWS session token for temporary credentials | No |
| `REGION` | AWS region (e.g., 'us-east-1') | Recommended |
| `ENDPOINT` | Custom endpoint URL for S3-compatible stores | No |
| `URL_STYLE` | URL style: 'path' or 'virtual' (default: 'virtual') | No |
| `USE_SSL` | Use HTTPS: 'true' or 'false' (default: 'true') | No |

### GCS (Google Cloud Storage)

```sql
CREATE SECRET my_gcs_secret (
    TYPE GCS,
    SERVICE_ACCOUNT_JSON '{
        "type": "service_account",
        "project_id": "my-project",
        "private_key_id": "...",
        "private_key": "...",
        ...
    }'
);
```

#### GCS Options

| Option | Description | Required |
|--------|-------------|----------|
| `SERVICE_ACCOUNT_JSON` | Service account JSON key (inline or file path) | Yes (for CONFIG provider) |
| `PROJECT_ID` | GCP project ID | No |

### Azure (Microsoft Azure Blob Storage)

```sql
CREATE SECRET my_azure_secret (
    TYPE AZURE,
    ACCOUNT_NAME 'mystorageaccount',
    ACCOUNT_KEY 'your-base64-encoded-account-key'
);
```

#### Azure Options

| Option | Description | Required |
|--------|-------------|----------|
| `ACCOUNT_NAME` | Azure storage account name | Yes |
| `ACCOUNT_KEY` | Azure storage account key | Yes (for key-based auth) |
| `CONNECTION_STRING` | Full Azure connection string | Alternative to ACCOUNT_KEY |
| `TENANT_ID` | Azure AD tenant ID | For service principal auth |
| `CLIENT_ID` | Azure AD client/application ID | For service principal auth |
| `CLIENT_SECRET` | Azure AD client secret | For service principal auth |

### HTTP (HTTP/HTTPS Endpoints)

```sql
CREATE SECRET my_http_secret (
    TYPE HTTP,
    BEARER_TOKEN 'your-api-token'
);
```

#### HTTP Options

| Option | Description | Required |
|--------|-------------|----------|
| `BEARER_TOKEN` | Bearer authentication token | No |
| `EXTRA_HTTP_HEADERS` | Additional HTTP headers as JSON | No |

## Provider Types

### CONFIG (Default)

Uses explicitly provided credentials in the secret options.

```sql
CREATE SECRET explicit_creds (
    TYPE S3,
    PROVIDER CONFIG,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
);
```

### ENV

Reads credentials from environment variables.

```sql
-- Uses AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
CREATE SECRET from_env (
    TYPE S3,
    PROVIDER ENV
);
```

#### Environment Variables by Type

**S3:**
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN` (optional)
- `AWS_REGION` or `AWS_DEFAULT_REGION`

**GCS:**
- `GOOGLE_APPLICATION_CREDENTIALS` (path to service account JSON)
- `GOOGLE_CLOUD_PROJECT`

**Azure:**
- `AZURE_STORAGE_ACCOUNT`
- `AZURE_STORAGE_KEY`
- `AZURE_STORAGE_CONNECTION_STRING`

### CREDENTIAL_CHAIN

Uses the default credential provider chain for the cloud platform.

```sql
CREATE SECRET chain_creds (
    TYPE S3,
    PROVIDER CREDENTIAL_CHAIN
);
```

The credential chain for S3 checks in order:
1. Environment variables
2. Shared credentials file (`~/.aws/credentials`)
3. Shared config file (`~/.aws/config`)
4. IAM role credentials (EC2/ECS/Lambda)

### IAM

Uses IAM role credentials from instance metadata (EC2, ECS, Lambda).

```sql
CREATE SECRET iam_role (
    TYPE S3,
    PROVIDER IAM
);
```

## Secret Scope

Scopes determine which URLs a secret applies to. When accessing a cloud URL, dukdb-go finds the secret with the longest matching scope prefix.

### Global Scope (Default)

Applies to all URLs of the secret type:

```sql
-- Matches all S3 URLs
CREATE SECRET global_s3 (
    TYPE S3,
    KEY_ID 'key',
    SECRET 'secret'
);
```

### Bucket/Host Scope

Applies to a specific bucket or host:

```sql
-- Only matches s3://my-bucket/*
CREATE SECRET bucket_specific (
    TYPE S3,
    SCOPE 's3://my-bucket',
    KEY_ID 'key',
    SECRET 'secret'
);
```

### Path Scope

Applies to a specific path prefix:

```sql
-- Only matches s3://my-bucket/sensitive/*
CREATE SECRET path_specific (
    TYPE S3,
    SCOPE 's3://my-bucket/sensitive/',
    KEY_ID 'different-key',
    SECRET 'different-secret'
);
```

### Scope Matching Example

```sql
-- Create secrets with different scopes
CREATE SECRET global_s3 (TYPE S3, KEY_ID 'key1', SECRET 'secret1');
CREATE SECRET bucket_s3 (TYPE S3, SCOPE 's3://data-bucket', KEY_ID 'key2', SECRET 'secret2');
CREATE SECRET path_s3 (TYPE S3, SCOPE 's3://data-bucket/private/', KEY_ID 'key3', SECRET 'secret3');

-- Query matching:
-- s3://other-bucket/file.csv        -> uses global_s3 (key1)
-- s3://data-bucket/public/file.csv  -> uses bucket_s3 (key2)
-- s3://data-bucket/private/file.csv -> uses path_s3 (key3)
```

## Managing Secrets

### Listing Secrets

```sql
-- List all secrets
SELECT * FROM duckdb_secrets();
```

Output columns:
- `name`: Secret name
- `type`: Secret type (S3, GCS, AZURE, HTTP)
- `provider`: Provider type (CONFIG, ENV, CREDENTIAL_CHAIN, IAM)
- `scope`: Secret scope
- `persistent`: Whether the secret persists across sessions

Note: Actual credential values (KEY_ID, SECRET, etc.) are not displayed for security.

### Finding Matching Secrets

```sql
-- Find which secret would be used for a URL
SELECT * FROM which_secret('s3://my-bucket/data/file.csv', 'S3');
```

### Dropping Secrets

```sql
-- Drop a secret
DROP SECRET my_secret;

-- Drop if exists (no error if not found)
DROP SECRET IF EXISTS my_secret;
```

### Altering Secrets

```sql
-- Update secret options
ALTER SECRET my_secret (
    REGION 'eu-west-1',
    KEY_ID 'new-key-id'
);
```

## Security Best Practices

### 1. Use Least Privilege

Create separate secrets with specific scopes instead of global secrets:

```sql
-- Better: Specific scope
CREATE SECRET analytics_data (
    TYPE S3,
    SCOPE 's3://analytics-bucket/reports/',
    KEY_ID 'key',
    SECRET 'secret'
);

-- Avoid: Global scope for sensitive credentials
CREATE SECRET all_access (
    TYPE S3,
    KEY_ID 'admin-key',
    SECRET 'admin-secret'
);
```

### 2. Use Credential Chains in Production

Prefer IAM roles and credential chains over hardcoded credentials:

```sql
-- Production (EC2/ECS/Lambda)
CREATE SECRET prod_s3 (
    TYPE S3,
    PROVIDER CREDENTIAL_CHAIN
);
```

### 3. Use Temporary Credentials

For scripts and temporary access, use session tokens:

```sql
CREATE TEMPORARY SECRET temp_access (
    TYPE S3,
    KEY_ID 'ASIATEMP...',
    SECRET 'temp-secret',
    SESSION_TOKEN 'FwoGZXIvYXdz...'
);
```

### 4. Rotate Credentials

Regularly rotate credentials and update secrets:

```sql
-- Update credentials
ALTER SECRET my_secret (
    KEY_ID 'new-rotated-key',
    SECRET 'new-rotated-secret'
);
```

### 5. Do Not Commit Credentials

Never commit SQL files containing credentials. Use environment variables or credential chains:

```sql
-- In your SQL file (safe to commit)
CREATE SECRET prod_creds (
    TYPE S3,
    PROVIDER ENV
);

-- Set environment variables separately
-- export AWS_ACCESS_KEY_ID=...
-- export AWS_SECRET_ACCESS_KEY=...
```

## Examples

### Multi-Account S3 Access

```sql
-- Production account
CREATE SECRET s3_prod (
    TYPE S3,
    SCOPE 's3://prod-bucket',
    KEY_ID 'prod-key',
    SECRET 'prod-secret',
    REGION 'us-east-1'
);

-- Development account
CREATE SECRET s3_dev (
    TYPE S3,
    SCOPE 's3://dev-bucket',
    KEY_ID 'dev-key',
    SECRET 'dev-secret',
    REGION 'us-west-2'
);

-- Now queries automatically use the correct credentials
SELECT * FROM read_parquet('s3://prod-bucket/data.parquet');  -- Uses s3_prod
SELECT * FROM read_parquet('s3://dev-bucket/data.parquet');   -- Uses s3_dev
```

### Cross-Cloud Data Access

```sql
-- S3 credentials
CREATE SECRET aws_creds (
    TYPE S3,
    KEY_ID 'aws-key',
    SECRET 'aws-secret'
);

-- GCS credentials
CREATE SECRET gcp_creds (
    TYPE GCS,
    SERVICE_ACCOUNT_JSON '/path/to/sa.json'
);

-- Azure credentials
CREATE SECRET azure_creds (
    TYPE AZURE,
    ACCOUNT_NAME 'myaccount',
    ACCOUNT_KEY 'mykey'
);

-- Query from multiple clouds
SELECT 'AWS' as source, * FROM read_parquet('s3://bucket/data.parquet')
UNION ALL
SELECT 'GCP' as source, * FROM read_parquet('gs://bucket/data.parquet')
UNION ALL
SELECT 'Azure' as source, * FROM read_parquet('azure://container/data.parquet');
```

### S3-Compatible Storage (MinIO)

```sql
CREATE SECRET minio (
    TYPE S3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'http://localhost:9000',
    URL_STYLE 'path',
    USE_SSL 'false'
);

-- Access MinIO storage
SELECT * FROM read_csv('s3://local-bucket/data.csv');
```

### API with Authentication

```sql
CREATE SECRET api_auth (
    TYPE HTTP,
    SCOPE 'https://api.example.com',
    BEARER_TOKEN 'your-api-token',
    EXTRA_HTTP_HEADERS '{"X-API-Version": "2.0"}'
);

-- Access authenticated API
SELECT * FROM read_json('https://api.example.com/data/users.json');
```

## Troubleshooting

### Secret Not Found

```sql
-- Check if secret exists
SELECT * FROM duckdb_secrets() WHERE name = 'my_secret';

-- Check which secret matches your URL
SELECT * FROM which_secret('s3://my-bucket/file.csv', 'S3');
```

### Access Denied

1. Verify credentials are correct
2. Check IAM policies/permissions
3. Verify the secret scope matches the URL
4. Check for typos in bucket/container names

### Credential Chain Issues

```sql
-- Debug by creating explicit secret
CREATE TEMPORARY SECRET debug_s3 (
    TYPE S3,
    PROVIDER CONFIG,
    KEY_ID 'test-key',
    SECRET 'test-secret'
);

-- If explicit works but chain doesn't, check:
-- 1. Environment variables are set
-- 2. ~/.aws/credentials file exists
-- 3. IAM role is attached (for EC2/ECS)
```

## See Also

- [Cloud Storage Integration](cloud-storage.md) - Using cloud storage with dukdb-go
- [Extended Types](types.md) - Information on supported data types
