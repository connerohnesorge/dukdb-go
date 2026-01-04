# Cloud Provider Credential Setup

This guide covers how to set up credentials for each cloud provider to use with dukdb-go. It includes both the cloud provider console setup and the dukdb-go configuration.

## Table of Contents

- [Amazon Web Services (S3)](#amazon-web-services-s3)
- [Google Cloud Storage (GCS)](#google-cloud-storage-gcs)
- [Microsoft Azure Blob Storage](#microsoft-azure-blob-storage)
- [HTTP/HTTPS Authentication](#httphttps-authentication)

---

## Amazon Web Services (S3)

### Option 1: IAM User with Access Keys

This is the most common approach for development and non-AWS deployments.

#### Step 1: Create an IAM User

1. Go to the [AWS IAM Console](https://console.aws.amazon.com/iam/)
2. Navigate to Users > Add users
3. Enter a username (e.g., `dukdb-s3-access`)
4. Select "Access key - Programmatic access"
5. Click Next

#### Step 2: Attach Permissions

For read-only access:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```

For read-write access:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```

#### Step 3: Download Credentials

After creating the user, download or copy:
- Access key ID (e.g., `AKIAIOSFODNN7EXAMPLE`)
- Secret access key (e.g., `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)

#### Step 4: Configure dukdb-go

```sql
CREATE SECRET aws_creds (
    TYPE S3,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    REGION 'us-east-1'
);
```

### Option 2: AWS CLI Shared Credentials

If you have the AWS CLI configured, dukdb-go can use those credentials.

#### Step 1: Configure AWS CLI

```bash
aws configure
# Enter your Access Key ID
# Enter your Secret Access Key
# Enter your default region (e.g., us-east-1)
```

This creates `~/.aws/credentials`:
```ini
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

And `~/.aws/config`:
```ini
[default]
region = us-east-1
```

#### Step 2: Configure dukdb-go

```sql
-- Use environment provider to read from AWS CLI config
CREATE SECRET aws_cli (
    TYPE S3,
    PROVIDER CREDENTIAL_CHAIN
);
```

### Option 3: Environment Variables

Set credentials via environment variables:

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_REGION=us-east-1
# Optional for temporary credentials
export AWS_SESSION_TOKEN=...
```

Configure dukdb-go:
```sql
CREATE SECRET aws_env (
    TYPE S3,
    PROVIDER ENV
);
```

### Option 4: IAM Roles (EC2/ECS/Lambda)

For applications running on AWS infrastructure, use IAM roles instead of access keys.

#### Step 1: Create an IAM Role

1. Go to IAM Console > Roles > Create role
2. Select the trusted entity:
   - EC2 for EC2 instances
   - ECS for ECS tasks
   - Lambda for Lambda functions
3. Attach the S3 access policy from Option 1
4. Name the role (e.g., `dukdb-s3-role`)

#### Step 2: Attach Role to Resource

- **EC2**: Instance Settings > Attach/Replace IAM Role
- **ECS**: Set `taskRoleArn` in task definition
- **Lambda**: Set execution role in function configuration

#### Step 3: Configure dukdb-go

```sql
-- Use IAM role credentials
CREATE SECRET iam_role (
    TYPE S3,
    PROVIDER CREDENTIAL_CHAIN
);
```

### Option 5: Temporary Credentials (STS)

For temporary access, use AWS STS to generate session credentials:

```bash
aws sts assume-role \
    --role-arn arn:aws:iam::123456789012:role/MyRole \
    --role-session-name dukdb-session
```

Use the returned credentials:
```sql
CREATE TEMPORARY SECRET sts_creds (
    TYPE S3,
    KEY_ID 'ASIATEMP...',
    SECRET 'temp-secret...',
    SESSION_TOKEN 'FwoGZXIvYXdz...',
    REGION 'us-east-1'
);
```

---

## Google Cloud Storage (GCS)

### Option 1: Service Account Key

This is the most common approach for server-to-server authentication.

#### Step 1: Create a Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Navigate to IAM & Admin > Service Accounts
3. Click "Create Service Account"
4. Enter a name (e.g., `dukdb-gcs-access`)
5. Click Create

#### Step 2: Grant Permissions

Add roles to the service account:
- **Read-only**: Storage Object Viewer (`roles/storage.objectViewer`)
- **Read-write**: Storage Object Admin (`roles/storage.objectAdmin`)
- **Full access**: Storage Admin (`roles/storage.admin`)

#### Step 3: Create Key

1. Click on the service account
2. Go to Keys tab
3. Click Add Key > Create new key
4. Select JSON format
5. Download the key file (e.g., `service-account.json`)

The key file looks like:
```json
{
    "type": "service_account",
    "project_id": "your-project-id",
    "private_key_id": "key-id",
    "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
    "client_email": "dukdb-gcs-access@your-project.iam.gserviceaccount.com",
    "client_id": "123456789",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token"
}
```

#### Step 4: Configure dukdb-go

Using file path:
```sql
CREATE SECRET gcs_sa (
    TYPE GCS,
    SERVICE_ACCOUNT_JSON '/path/to/service-account.json'
);
```

Using inline JSON:
```sql
CREATE SECRET gcs_inline (
    TYPE GCS,
    SERVICE_ACCOUNT_JSON '{
        "type": "service_account",
        "project_id": "your-project-id",
        ...
    }'
);
```

### Option 2: Application Default Credentials (ADC)

For local development or GCE/GKE deployments.

#### Local Development

```bash
# Install gcloud CLI and authenticate
gcloud auth application-default login
```

This creates credentials at `~/.config/gcloud/application_default_credentials.json`.

Configure dukdb-go:
```sql
CREATE SECRET gcs_adc (
    TYPE GCS,
    PROVIDER CREDENTIAL_CHAIN
);
```

#### GCE/GKE

When running on Google Cloud infrastructure, ADC automatically uses the attached service account.

```sql
CREATE SECRET gcs_metadata (
    TYPE GCS,
    PROVIDER CREDENTIAL_CHAIN
);
```

### Option 3: Environment Variable

Point to the service account key file:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Configure dukdb-go:
```sql
CREATE SECRET gcs_env (
    TYPE GCS,
    PROVIDER ENV
);
```

### Option 4: Workload Identity (GKE)

For GKE deployments using Workload Identity:

1. Enable Workload Identity on the GKE cluster
2. Create a Kubernetes service account
3. Bind it to a GCP service account
4. Use the Kubernetes service account in your pod

```sql
CREATE SECRET gcs_wi (
    TYPE GCS,
    PROVIDER CREDENTIAL_CHAIN
);
```

---

## Microsoft Azure Blob Storage

### Option 1: Storage Account Access Key

The simplest approach for development.

#### Step 1: Get Access Keys

1. Go to [Azure Portal](https://portal.azure.com/)
2. Navigate to your Storage Account
3. Go to Security + networking > Access keys
4. Copy the Storage account name and Key

#### Step 2: Configure dukdb-go

```sql
CREATE SECRET azure_key (
    TYPE AZURE,
    ACCOUNT_NAME 'mystorageaccount',
    ACCOUNT_KEY 'your-base64-encoded-access-key=='
);
```

### Option 2: Connection String

Azure provides a connection string that combines all credentials.

#### Step 1: Get Connection String

1. Go to Storage Account > Access keys
2. Copy the Connection string

#### Step 2: Configure dukdb-go

```sql
CREATE SECRET azure_conn (
    TYPE AZURE,
    CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=your-key;EndpointSuffix=core.windows.net'
);
```

### Option 3: Service Principal

For applications requiring fine-grained access control.

#### Step 1: Register an Application

1. Go to Azure Active Directory > App registrations
2. Click New registration
3. Enter a name and register
4. Note the Application (client) ID and Directory (tenant) ID

#### Step 2: Create Client Secret

1. Go to Certificates & secrets
2. Click New client secret
3. Add a description and expiry
4. Copy the secret value immediately

#### Step 3: Grant Storage Access

1. Go to your Storage Account
2. Access control (IAM) > Add role assignment
3. Select role: "Storage Blob Data Contributor" (or Reader for read-only)
4. Assign to your application

#### Step 4: Configure dukdb-go

```sql
CREATE SECRET azure_sp (
    TYPE AZURE,
    ACCOUNT_NAME 'mystorageaccount',
    TENANT_ID 'your-tenant-id',
    CLIENT_ID 'your-client-id',
    CLIENT_SECRET 'your-client-secret'
);
```

### Option 4: Managed Identity

For applications running on Azure (VMs, App Service, AKS, etc.).

#### Step 1: Enable Managed Identity

1. Go to your Azure resource (VM, App Service, etc.)
2. Identity > System assigned > Enable
3. Copy the Object ID

#### Step 2: Grant Storage Access

1. Go to your Storage Account
2. Access control (IAM) > Add role assignment
3. Select "Storage Blob Data Contributor"
4. Assign to the managed identity

#### Step 3: Configure dukdb-go

```sql
CREATE SECRET azure_mi (
    TYPE AZURE,
    ACCOUNT_NAME 'mystorageaccount',
    PROVIDER CREDENTIAL_CHAIN
);
```

### Option 5: Environment Variables

```bash
export AZURE_STORAGE_ACCOUNT=mystorageaccount
export AZURE_STORAGE_KEY=your-access-key
# Or use connection string
export AZURE_STORAGE_CONNECTION_STRING='DefaultEndpointsProtocol=https;...'
```

Configure dukdb-go:
```sql
CREATE SECRET azure_env (
    TYPE AZURE,
    PROVIDER ENV
);
```

---

## HTTP/HTTPS Authentication

### Public URLs

No authentication needed:
```sql
SELECT * FROM read_csv('https://example.com/public/data.csv');
```

### Bearer Token Authentication

For APIs that use bearer tokens (OAuth, API keys):

```sql
CREATE SECRET api_auth (
    TYPE HTTP,
    SCOPE 'https://api.example.com',
    BEARER_TOKEN 'your-api-token-here'
);
```

### Custom Headers

For APIs requiring custom headers:

```sql
CREATE SECRET custom_api (
    TYPE HTTP,
    SCOPE 'https://api.example.com',
    BEARER_TOKEN 'token',
    EXTRA_HTTP_HEADERS '{"X-API-Key": "your-key", "X-Custom-Header": "value"}'
);
```

### Basic Authentication (via headers)

```sql
-- Base64 encode "username:password"
CREATE SECRET basic_auth (
    TYPE HTTP,
    SCOPE 'https://api.example.com',
    EXTRA_HTTP_HEADERS '{"Authorization": "Basic dXNlcm5hbWU6cGFzc3dvcmQ="}'
);
```

---

## Security Best Practices

### 1. Principle of Least Privilege

Only grant the minimum permissions needed:
- Use read-only access when writes are not required
- Scope permissions to specific buckets/containers

### 2. Avoid Hardcoding Credentials

- Use environment variables or credential chains in production
- Never commit credentials to version control
- Use `.gitignore` to exclude credential files

### 3. Rotate Credentials Regularly

- Set up credential rotation policies
- Update secrets when credentials are rotated:
```sql
ALTER SECRET my_secret (
    KEY_ID 'new-key-id',
    SECRET 'new-secret'
);
```

### 4. Use IAM Roles When Possible

- On AWS: Use EC2 instance roles, ECS task roles, or Lambda execution roles
- On GCP: Use service accounts attached to GCE/GKE resources
- On Azure: Use Managed Identity

### 5. Monitor Access

- Enable cloud provider audit logging
- Monitor for unusual access patterns
- Set up alerts for failed authentication attempts

### 6. Use Temporary Credentials for CI/CD

```sql
CREATE TEMPORARY SECRET ci_creds (
    TYPE S3,
    KEY_ID 'ASIATEMP...',
    SECRET 'temp-secret',
    SESSION_TOKEN 'session-token'
);
```

---

## Troubleshooting

### AWS S3

**Error: Access Denied**
- Verify IAM permissions include the bucket and object paths
- Check bucket policy doesn't block access
- Ensure region is correct

**Error: NoSuchBucket**
- Verify bucket name (no typos)
- Check region matches bucket location

### GCS

**Error: Could not load credentials**
- Verify service account key file path
- Check JSON is valid
- Ensure service account has required roles

**Error: Forbidden**
- Verify IAM roles are assigned
- Check bucket-level permissions

### Azure

**Error: AuthorizationFailure**
- Verify account name is correct
- Check access key hasn't been rotated
- Ensure container exists

**Error: AccountNotFound**
- Verify storage account name
- Check account is accessible from your network

### General

**Checking which secret is used:**
```sql
SELECT * FROM which_secret('s3://my-bucket/file.csv', 'S3');
```

**Listing all secrets:**
```sql
SELECT * FROM duckdb_secrets();
```

**Testing connectivity:**
```sql
-- Simple test query
SELECT COUNT(*) FROM read_csv('s3://my-bucket/test.csv') LIMIT 1;
```

## See Also

- [Cloud Storage Integration](cloud-storage.md) - Using cloud storage with dukdb-go
- [Secrets Management](secrets.md) - Creating and managing secrets
