# Design: S3 and Cloud Integration

## Context

This design document outlines the technical approach for implementing cloud storage support in dukdb-go. The implementation must be pure Go (no CGO), maintain API compatibility with DuckDB's cloud features, and support multiple cloud providers (S3, GCS, Azure, HTTPFS).

**Stakeholders**:
- Application developers needing cloud data access
- Data engineers working with cloud data lakes
- Serverless/WASM developers needing cloud-native storage
- Teams migrating from official DuckDB

**Constraints**:
- Pure Go implementation (no CGO)
- Must match DuckDB's cloud URL syntax and semantics
- Must integrate with DuckDB secrets system for credentials
- Should support WASM compilation (no OS-specific syscalls)
- Performance should be competitive with official DuckDB

## Goals / Non-Goals

**Goals**:
1. Implement pluggable FileSystem interface for local and cloud storage
2. Support S3, GCS, Azure, and HTTP/HTTPS protocols
3. Implement DuckDB-compatible secrets management
4. Enable COPY FROM/TO with cloud URLs
5. Enable table functions (read_parquet, etc.) with cloud URLs
6. Support range requests for columnar format efficiency
7. Support multipart uploads for large data exports

**Non-Goals**:
1. S3 Select or similar server-side filtering (future work)
2. Real-time replication or change data capture
3. Cloud provider-specific advanced features (e.g., S3 Glacier)
4. AWS Lake Formation or similar IAM integrations beyond basic credentials
5. S3-compatible object stores (MinIO, etc.) - may work but not tested

## Decisions

### Decision 1: FileSystem Interface Architecture

**Options**:
A. Implement cloud support directly in persistence layer
B. Create abstracted FileSystem interface with implementations
C. Use io/fs from Go standard library

**Choice**: B - Create abstracted FileSystem interface

**Rationale**:
- Clean separation of concerns
- Testability with mock filesystems
- Pluggable architecture for future providers
- Matches DuckDB's approach

```go
// internal/io/filesystem/filesystem.go

package filesystem

import (
    "context"
    "io"
    "time"
)

type FileInfo interface {
    Name() string
    Size() int64
    Mode() FileMode
    ModTime() time.Time
    IsDir() bool
    Sys() interface{}
}

type FileMode uint32

const (
    ModeDir    FileMode = 1 << 31
    ModeAppend FileMode = 1 << 30
)

type File interface {
    io.ReadWriteSeeker
    io.ReaderAt
    io.WriterAt
    Close() error
    Stat() (FileInfo, error)
}

type FileSystem interface {
    Open(path string) (File, error)
    Create(path string) (File, error)
    MkdirAll(path string) error
    Stat(path string) (FileInfo, error)
    Remove(path string) error
    RemoveDir(path string) error
    ReadDir(path string) ([]DirEntry, error)
    Exists(path string) (bool, error)
    URI() string
    Capabilities() FileSystemCapabilities
}

type DirEntry interface {
    Name() string
    IsDir() bool
    Type() FileMode
    Info() (FileInfo, error)
}

type FileSystemCapabilities struct {
    SupportsSeek    bool
    SupportsAppend  bool
    SupportsRange   bool
    SupportsDirList bool
    SupportsWrite   bool
    SupportsDelete  bool
    ContextTimeout  bool
}

// Context-aware file interface for cancellation
type ContextFile interface {
    File
    ReadContext(ctx context.Context, p []byte) (n int, err error)
    WriteContext(ctx context.Context, p []byte) (n int, err error)
}
```

### Decision 2: URL Parsing Strategy

**Options**:
A. Use standard library net/url with custom scheme handling
B. Create custom URL parser with scheme detection
C. Regex-based URL extraction

**Choice**: B - Create custom URL parser

**Rationale**:
- DuckDB URL syntax is more flexible than standard URLs
- Need to handle S3, GCS, Azure with specific syntax
- Path-style vs virtual-host-style for S3
- Connection overrides in query parameters

```go
// internal/io/url/parser.go

package url

import (
    "net/url"
    "strings"
)

type ParsedURL struct {
    Scheme    string
    Authority string
    Path      string
    Query     url.Values
    Fragment  string
    RawPath   string
    RawQuery  string
}

func Parse(raw string) (*ParsedURL, error) {
    // Handle duckdb:// prefix for local files
    if strings.HasPrefix(raw, "duckdb://") {
        raw = raw[9:]
    }
    
    // Detect scheme
    scheme := "file"
    rest := raw
    
    for _, s := range []string{"s3", "s3a", "s3n", "gcs", "gs", "azure", "az", "http", "https", "hf", "huggingface"} {
        if strings.HasPrefix(rest, s+"://") {
            scheme = s
            rest = rest[len(s)+3:]
            break
        }
    }
    
    // Parse authority and path
    // Format: [user:password@]host[:port][/path][?query][#fragment]
    authority := ""
    path := rest
    
    if idx := strings.Index(rest, "/"); idx >= 0 {
        authority = rest[:idx]
        path = rest[idx:]
    } else if strings.Contains(rest, "?") {
        // No path, query present
        idx := strings.Index(rest, "?")
        authority = rest[:idx]
        path = ""
    } else if strings.Contains(rest, "#") {
        // No path, fragment present
        idx := strings.Index(rest, "#")
        authority = rest[:idx]
        path = ""
    }
    
    // Parse query parameters
    query := make(url.Values)
    if qIdx := strings.Index(path, "?"); qIdx >= 0 {
        if fIdx := strings.Index(path, "#"); fIdx >= 0 {
            queryStr := path[qIdx+1 : fIdx]
            path = path[:qIdx]
            if parsed, err := url.ParseQuery(queryStr); err == nil {
                query = parsed
            }
        } else {
            queryStr := path[qIdx+1:]
            path = path[:qIdx]
            if parsed, err := url.ParseQuery(queryStr); err == nil {
                query = parsed
            }
        }
    }
    
    return &ParsedURL{
        Scheme:   scheme,
        Authority: authority,
        Path:     path,
        Query:    query,
    }, nil
}

func (u *ParsedURL) String() string {
    result := u.Scheme + "://"
    if u.Authority != "" {
        result += u.Authority
    }
    result += u.Path
    if len(u.RawQuery) > 0 {
        result += "?" + u.RawQuery
    }
    if u.Fragment != "" {
        result += "#" + u.Fragment
    }
    return result
}
```

### Decision 3: Secret Manager Architecture

**Options**:
A. Store secrets in database catalog
B. Use external secret providers (Vault, AWS Secrets Manager, etc.)
C. Implement DuckDB-compatible secret storage

**Choice**: C - Implement DuckDB-compatible secret storage

**Rationale**:
- Must be compatible with DuckDB secrets syntax
- Users expect secrets to persist across sessions
- Path-based scope matching is critical for usability

```go
// internal/secret/manager.go

package secret

import (
    "context"
    "sync"
)

type SecretType string

const (
    SecretTypeS3       SecretType = "S3"
    SecretTypeHTTP     SecretType = "HTTP"
    SecretTypeAzure    SecretType = "AZURE"
    SecretTypeGCS      SecretType = "GCS"
    SecretTypeHuggingFace SecretType = "HUGGINGFACE"
)

type ProviderType string

const (
    ProviderConfig       ProviderType = "CONFIG"
    ProviderEnv          ProviderType = "ENV"
    ProviderCredentialChain ProviderType = "CREDENTIAL_CHAIN"
    ProviderIAM          ProviderType = "IAM"
)

type Secret struct {
    Name       string
    Type       SecretType
    Provider   ProviderType
    Scope      SecretScope
    Options    SecretOptions
    Persistent bool
    CreatedAt  time.Time
    UpdatedAt  time.Time
}

type SecretScope struct {
    Type   ScopeType
    Prefix string
}

type ScopeType string

const (
    ScopeGlobal    ScopeType = "GLOBAL"
    ScopePath      ScopeType = "PATH"
    ScopeHost      ScopeType = "HOST"
)

type SecretOptions map[string]string

type Manager interface {
    CreateSecret(ctx context.Context, s Secret) error
    DropSecret(ctx context.Context, name string, ifExists bool) error
    AlterSecret(ctx context.Context, name string, opts SecretOptions) error
    GetSecret(ctx context.Context, url string, secretType SecretType) (*Secret, error)
    ListSecrets(ctx context.Context, scope SecretScope) ([]Secret, error)
}

type manager struct {
    mu      sync.RWMutex
    secrets map[string]Secret
    catalog Catalog
}

type Catalog interface {
    GetSecret(name string) (*Secret, error)
    SetSecret(s Secret) error
    DeleteSecret(name string) error
    ListSecrets() ([]Secret, error)
}
```

### Decision 4: S3 Implementation

**Options**:
A. Use aws-sdk-go v1 (current AWS SDK)
B. Use aws-sdk-go-v2 (modular, faster)
C. Use custom HTTP implementation

**Choice**: B - Use aws-sdk-go-v2

**Rationale**:
- Modular architecture reduces dependencies
- Better performance with HTTP/2
- Active development
- Supports IMDSv2 for IAM roles

```go
// internal/io/filesystem/s3.go

package filesystem

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strings"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/aws/aws-sdk-go-v2/service/sts"
)

type S3Config struct {
    Region          string
    Endpoint        string
    UseSSL          bool
    URLStyle        S3URLStyle
    AccessKeyID     string
    SecretAccessKey string
    SessionToken    string
    IMDSv2          bool
    Profile         string
    MaxRetries      int
}

type S3URLStyle string

const (
    S3URLStylePath     S3URLStyle = "path"
    S3URLStyleVirtual  S3URLStyle = "virtual"
)

type s3FileSystem struct {
    client   *s3.Client
    uploader *manager.Uploader
    config   S3Config
    bucket   string
    prefix   string
}

type s3File struct {
    client *s3.Client
    bucket string
    key    string
    offset int64
    data   []byte
}

func NewS3FileSystem(ctx context.Context, config S3Config) (FileSystem, error) {
    // Build AWS config
    awsConfig, err := buildAWSConfig(ctx, config)
    if err != nil {
        return nil, fmt.Errorf("failed to build AWS config: %w", err)
    }
    
    // Create S3 client
    client := s3.NewFromConfig(*awsConfig, func(o *s3.Options) {
        if config.Endpoint != "" {
            o.EndpointResolver = s3.EndpointResolverFunc(
                func(region string, ep s3.EndpointResolverOptions) (aws.Endpoint, error) {
                    return aws.Endpoint{
                        PartitionID:       "aws",
                        URL:               config.Endpoint,
                        SigningRegion:     region,
                        HostStyleResolver: resolveHostStyle(config.URLStyle),
                    }, nil
                },
            )
        }
        o.UsePathStyle = config.URLStyle == S3URLStylePath
    })
    
    uploader := manager.NewUploader(client)
    
    return &s3FileSystem{
        client:   client,
        uploader: uploader,
        config:   config,
    }, nil
}

func buildAWSConfig(ctx context.Context, config S3Config) (*aws.Config, error) {
    var opts []func(*config.LoadOptions) error
    
    // Region
    if config.Region != "" {
        opts = append(opts, config.WithRegion(config.Region))
    }
    
    // Credentials
    if config.AccessKeyID != "" && config.SecretAccessKey != "" {
        opts = append(opts, config.WithCredentialsProvider(
            credentials.NewStaticCredentialsProvider(
                config.AccessKeyID,
                config.SecretAccessKey,
                config.SessionToken,
            ),
        ))
    } else if config.Profile != "" {
        opts = append(opts, config.WithSharedConfigProfile(config.Profile))
    }
    
    // IMDSv2 for IAM roles
    if config.IMDSv2 {
        opts = append(opts, config.WithEC2IMDS(
            func(o *config.SharedConfig) {
                o.UseEC2MetadataV2 = true
            },
        ))
    }
    
    return config.LoadDefaultConfig(ctx, opts...)
}

func (fs *s3FileSystem) Open(path string) (File, error) {
    return fs.openFile(path, 0)
}

func (fs *s3FileSystem) openFile(path string, offset int64) (*s3File, error) {
    // Parse path as s3://bucket/key or s3://bucket/key?region=xxx
    parsed, err := url.Parse("s3://" + path)
    if err != nil {
        return nil, err
    }
    
    bucket := parsed.Host
    key := strings.TrimPrefix(parsed.Path, "/")
    
    return &s3File{
        client: fs.client,
        bucket: bucket,
        key:    key,
        offset: offset,
    }, nil
}

func (f *s3File) Read(p []byte) (n int, err error) {
    return f.ReadAt(p, f.offset)
}

func (f *s3File) ReadAt(p []byte, off int64) (n int, err error) {
    ctx := context.Background()
    
    input := &s3.GetObjectInput{
        Bucket: aws.String(f.bucket),
        Key:    aws.String(f.key),
        Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)),
    }
    
    resp, err := f.client.GetObject(ctx, input)
    if err != nil {
        return 0, err
    }
    defer resp.Body.Close()
    
    return io.ReadFull(resp.Body, p)
}

func (f *s3File) Write(p []byte) (n int, err error) {
    return f.WriteAt(p, f.offset)
}

func (f *s3File) WriteAt(p []byte, off int64) (n int, err error) {
    ctx := context.Background()
    
    // For simplicity, use PutObject; multipart for large writes
    if len(p) > 5*1024*1024 {
        return f.uploadPart(ctx, p, off)
    }
    
    input := &s3.PutObjectInput{
        Bucket: aws.String(f.bucket),
        Key:    aws.String(f.key),
        Body:   bytes.NewReader(p),
    }
    
    _, err = f.client.PutObject(ctx, input)
    if err != nil {
        return 0, err
    }
    
    return len(p), nil
}

func (f *s3File) uploadPart(ctx context.Context, data []byte, offset int64) (int, error) {
    // Simplified: Use PutObject for now; multipart upload implementation
    input := &s3.PutObjectInput{
        Bucket: aws.String(f.bucket),
        Key:    aws.String(f.key),
        Body:   bytes.NewReader(data),
    }
    
    _, err := f.client.PutObject(ctx, input)
    if err != nil {
        return 0, err
    }
    
    return len(data), nil
}

func (f *s3File) Seek(offset int64, whence int) (int64, error) {
    switch whence {
    case os.SEEK_SET:
        f.offset = offset
    case os.SEEK_CUR:
        f.offset += offset
    case os.SEEK_END:
        // Need to get file size first
        // Would require headObject call
    }
    return f.offset, nil
}

func (f *s3File) Stat() (FileInfo, error) {
    ctx := context.Background()
    
    input := &s3.HeadObjectInput{
        Bucket: aws.String(f.bucket),
        Key:    aws.String(f.key),
    }
    
    resp, err := f.client.HeadObject(ctx, input)
    if err != nil {
        return nil, err
    }
    
    return &s3FileInfo{
        size:     *resp.ContentLength,
        name:     f.key,
        modified: *resp.LastModified,
    }, nil
}

func (f *s3File) Close() error {
    return nil
}

type s3FileInfo struct {
    size     int64
    name     string
    modified time.Time
}

func (fi *s3FileInfo) Name() string        { return fi.name }
func (fi *s3FileInfo) Size() int64         { return fi.size }
func (fi *s3FileInfo) ModTime() time.Time  { return fi.modified }
func (fi *s3FileInfo) IsDir() bool         { return false }
func (fi *s3FileInfo) Mode() FileMode      { return 0 }
func (fi *s3FileInfo) Sys() interface{}    { return nil }
```

### Decision 5: GCS Implementation

**Options**:
A. Use cloud.google.com/go/storage
B. Use google-cloud-storage-go SDK
C. Use custom HTTP implementation

**Choice**: A - Use cloud.google.com/go/storage

**Rationale**:
- Official Google SDK
- Well-maintained and documented
- Supports all GCS features

```go
// internal/io/filesystem/gcs.go

package filesystem

import (
    "context"
    "fmt"
    "io"
    "net/url"
    "strings"
    "time"

    "cloud.google.com/go/storage"
    "google.golang.org/api/option"
)

type GCSConfig struct {
    ProjectID       string
    Bucket          string
    KeyFile         string
    CredentialsJSON string
}

type gcsFileSystem struct {
    client *storage.Client
    bucket *storage.BucketHandle
    config GCSConfig
}

type gcsFile struct {
    client   *storage.Client
    bucket   string
    object   string
    reader   *storage.Reader
    offset   int64
}

func NewGCSFileSystem(ctx context.Context, config GCSConfig) (FileSystem, error) {
    var opts []option.ClientOption
    if config.KeyFile != "" {
        opts = append(opts, option.WithCredentialsFile(config.KeyFile))
    } else if config.CredentialsJSON != "" {
        opts = append(opts, option.WithCredentialsJSON([]byte(config.CredentialsJSON)))
    }
    
    client, err := storage.NewClient(ctx, opts...)
    if err != nil {
        return nil, fmt.Errorf("failed to create GCS client: %w", err)
    }
    
    return &gcsFileSystem{
        client: client,
        bucket: client.Bucket(config.Bucket),
        config: config,
    }, nil
}

func (fs *gcsFileSystem) Open(path string) (File, error) {
    parsed, err := url.Parse("gs://" + path)
    if err != nil {
        return nil, err
    }
    
    bucket := parsed.Host
    object := strings.TrimPrefix(parsed.Path, "/")
    
    obj := fs.client.Bucket(bucket).Object(object)
    
    reader, err := obj.NewReader(context.Background())
    if err != nil {
        return nil, err
    }
    
    return &gcsFile{
        client: fs.client,
        bucket: bucket,
        object: object,
        reader: reader,
    }, nil
}

func (f *gcsFile) Read(p []byte) (n int, err error) {
    return f.reader.Read(p)
}

func (f *gcsFile) ReadAt(p []byte, off int64) (n int, err error) {
    // GCS doesn't support ReadAt directly; would need new reader
    // For now, seek and read
    if _, err := f.Seek(off, io.SeekStart); err != nil {
        return 0, err
    }
    return io.ReadFull(f, p)
}

func (f *gcsFile) Write(p []byte) (n int, err error) {
    ctx := context.Background()
    obj := f.client.Bucket(f.bucket).Object(f.object)
    writer := obj.NewWriter(ctx)
    writer.Offset = f.offset
    
    n, err = writer.Write(p)
    if err != nil {
        return n, err
    }
    
    return n, writer.Close()
}

func (f *gcsFile) Seek(offset int64, whence int) (int64, error) {
    // GCS reader doesn't support seeking; create new reader
    ctx := context.Background()
    obj := f.client.Bucket(f.bucket).Object(f.object)
    
    newReader, err := obj.NewRangeReader(ctx, offset, -1)
    if err != nil {
        return 0, err
    }
    
    if f.reader != nil {
        f.reader.Close()
    }
    f.reader = newReader
    f.offset = offset
    
    return offset, nil
}

func (f *gcsFile) Stat() (FileInfo, error) {
    ctx := context.Background()
    obj := f.client.Bucket(f.bucket).Object(f.object)
    
    attrs, err := obj.Attrs(ctx)
    if err != nil {
        return nil, err
    }
    
    return &gcsFileInfo{
        name:     attrs.Name,
        size:     attrs.Size,
        modified: attrs.LastModified,
    }, nil
}

func (f *gcsFile) Close() error {
    if f.reader != nil {
        return f.reader.Close()
    }
    return nil
}

type gcsFileInfo struct {
    name     string
    size     int64
    modified time.Time
}

func (fi *gcsFileInfo) Name() string        { return fi.name }
func (fi *gcsFileInfo) Size() int64         { return fi.size }
func (fi *gcsFileInfo) ModTime() time.Time  { return fi.modified }
func (fi *gcsFileInfo) IsDir() bool         { return false }
func (fi *gcsFileInfo) Mode() FileMode      { return 0 }
func (fi *gcsFileInfo) Sys() interface{}    { return nil }
```

### Decision 6: Azure Implementation

**Options**:
A. Use github.com/Azure/azure-sdk-for-go/storage (older)
B. Use github.com/Azure/azure-sdk-for-go/sdk/storage/azblob (newer)
C. Use custom HTTP implementation

**Choice**: B - Use github.com/Azure/azure-sdk-for-go/sdk/storage/azblob

**Rationale**:
- Modern SDK with better performance
- Supports all Azure Blob Storage features
- Active development

```go
// internal/io/filesystem/azure.go

package filesystem

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "strings"

    "github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
    "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type AzureConfig struct {
    AccountName   string
    AccountKey    string
    TenantID      string
    ClientID      string
    ClientSecret  string
    Container     string
}

type azureFileSystem struct {
    client    *azblob.Client
    container *azblob.ContainerClient
    config    AzureConfig
}

type azureFile struct {
    client   *azblob.Client
    container string
    blob     string
    data     []byte
    offset   int64
}

func NewAzureFileSystem(ctx context.Context, config AzureConfig) (FileSystem, error) {
    cred, err := azblob.NewSharedKeyCredential(config.AccountName, config.AccountKey)
    if err != nil {
        return nil, fmt.Errorf("failed to create Azure credential: %w", err)
    }
    
    serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", config.AccountName)
    client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
    if err != nil {
        return nil, fmt.Errorf("failed to create Azure client: %w", err)
    }
    
    container := client.ServiceClient().NewContainerClient(config.Container)
    
    return &azureFileSystem{
        client:    client,
        container: container,
        config:    config,
    }, nil
}

func (fs *azureFileSystem) Open(path string) (File, error) {
    // Parse path as azure://container/blob
    parts := strings.SplitN(path, "/", 2)
    container := fs.config.Container
    blob := path
    
    if len(parts) >= 1 {
        container = parts[0]
        if len(parts) >= 2 {
            blob = parts[1]
        }
    }
    
    return &azureFile{
        client:   fs.client,
        container: container,
        blob:     blob,
    }, nil
}

func (f *azureFile) Read(p []byte) (n int, err error) {
    return f.ReadAt(p, f.offset)
}

func (f *azureFile) ReadAt(p []byte, off int64) (n int, err error) {
    ctx := context.Background()
    
    downloadOpts := &azblob.DownloadFileOptions{
        Range: &azblob.HTTPRange{
            Offset: to.Ptr(off),
            Count:  to.Ptr(int64(len(p))),
        },
    }
    
    response, err := f.client.DownloadFile(ctx, f.container, f.blob, downloadOpts)
    if err != nil {
        return 0, err
    }
    
    body := response.Body
    defer body.Close()
    
    return io.ReadFull(body, p)
}

func (f *azureFile) Write(p []byte) (n int, err error) {
    ctx := context.Background()
    
    _, err = f.client.UploadBuffer(ctx, f.container, f.blob, p, &azblob.UploadBufferOptions{})
    if err != nil {
        return 0, err
    }
    
    return len(p), nil
}

func (f *azureFile) Seek(offset int64, whence int) (int64, error) {
    switch whence {
    case io.SeekStart:
        f.offset = offset
    case io.SeekCurrent:
        f.offset += offset
    case io.SeekEnd:
        // Would need to get blob size first
    }
    return f.offset, nil
}

func (f *azureFile) Stat() (FileInfo, error) {
    ctx := context.Background()
    
    props, err := f.client.GetProperties(ctx, f.container, f.blob, nil)
    if err != nil {
        return nil, err
    }
    
    return &azureFileInfo{
        name:     f.blob,
        size:     *props.ContentLength,
        modified: props.LastModified.Time,
    }, nil
}

func (f *azureFile) Close() error {
    return nil
}

type azureFileInfo struct {
    name     string
    size     int64
    modified string
}

func (fi *azureFileInfo) Name() string        { return fi.name }
func (fi *azureFileInfo) Size() int64         { return fi.size }
func (fi *azureFileInfo) ModTime() string     { return fi.modified }
func (fi *azureFileInfo) IsDir() bool         { return false }
func (fi *azureFileInfo) Mode() FileMode      { return 0 }
func (fi *azureFileInfo) Sys() interface{}    { return nil }
```

### Decision 7: HTTP/HTTPS Implementation

**Options**:
A. Use net/http directly
B. Use httputil.ReverseProxy
C. Custom HTTP client

**Choice**: A - Use net/http directly

**Rationale**:
- Simple for basic HTTPFS functionality
- Supports range requests for partial reads
- Can be extended for redirects, auth, etc.

```go
// internal/io/filesystem/http.go

package filesystem

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "strings"
    "time"
)

type HTTPConfig struct {
    Timeout       time.Duration
    FollowRedirects bool
    MaxRedirects  int
    Headers       map[string]string
}

type httpFileSystem struct {
    config HTTPConfig
    client *http.Client
}

type httpFile struct {
    url    string
    client *http.Client
    resp   *http.Response
    offset int64
}

func NewHTTPFileSystem(config HTTPConfig) FileSystem {
    return &httpFileSystem{
        config: config,
        client: &http.Client{
            Timeout: config.Timeout,
            CheckRedirect: func(req *http.Request, via []*http.Request) error {
                if !config.FollowRedirects {
                    return http.ErrUseLastResponse
                }
                if len(via) >= config.MaxRedirects {
                    return fmt.Errorf("too many redirects")
                }
                return nil
            },
        },
    }
}

func (fs *httpFileSystem) Open(path string) (File, error) {
    // Handle http://, https://, or just URL
    if !strings.HasPrefix(path, "http") {
        path = "https://" + path
    }
    
    return &httpFile{
        url:    path,
        client: fs.client,
    }, nil
}

func (f *httpFile) Read(p []byte) (n int, err error) {
    return f.ReadAt(p, f.offset)
}

func (f *httpFile) ReadAt(p []byte, off int64) (n int, err error) {
    if f.resp == nil {
        req, err := http.NewRequest("GET", f.url, nil)
        if err != nil {
            return 0, err
        }
        
        req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1))
        
        f.resp, err = f.client.Do(req)
        if err != nil {
            return 0, err
        }
        
        if f.resp.StatusCode != 200 && f.resp.StatusCode != 206 {
            f.resp.Body.Close()
            return 0, fmt.Errorf("HTTP status %d", f.resp.StatusCode)
        }
    }
    
    return io.ReadFull(f.resp.Body, p)
}

func (f *httpFile) Write(p []byte) (n int, err error) {
    // HTTP write not typically supported
    return 0, fmt.Errorf("HTTP write not supported")
}

func (f *httpFile) Seek(offset int64, whence int) (int64, error) {
    if f.resp != nil {
        f.resp.Body.Close()
        f.resp = nil
    }
    
    switch whence {
    case io.SeekStart:
        f.offset = offset
    case io.SeekCurrent:
        f.offset += offset
    case io.SeekEnd:
        // Need to get content-length first
        req, err := http.NewRequest("HEAD", f.url, nil)
        if err != nil {
            return 0, err
        }
        
        resp, err := f.client.Do(req)
        if err != nil {
            return 0, err
        }
        resp.Body.Close()
        
        if cl := resp.Header.Get("Content-Length"); cl != "" {
            var size int64
            fmt.Sscanf(cl, "%d", &size)
            f.offset = size + offset
        }
    }
    return f.offset, nil
}

func (f *httpFile) Stat() (FileInfo, error) {
    req, err := http.NewRequest("HEAD", f.url, nil)
    if err != nil {
        return nil, err
    }
    
    resp, err := f.client.Do(req)
    if err != nil {
        return nil, err
    }
    resp.Body.Close()
    
    size := int64(0)
    if cl := resp.Header.Get("Content-Length"); cl != "" {
        fmt.Sscanf(cl, "%d", &size)
    }
    
    return &httpFileInfo{
        name: f.url,
        size: size,
    }, nil
}

func (f *httpFile) Close() error {
    if f.resp != nil {
        f.resp.Body.Close()
        f.resp = nil
    }
    return nil
}

type httpFileInfo struct {
    name string
    size int64
}

func (fi *httpFileInfo) Name() string        { return fi.name }
func (fi *httpFileInfo) Size() int64         { return fi.size }
func (fi *httpFileInfo) ModTime() time.Time  { return time.Time{} }
func (fi *httpFileInfo) IsDir() bool         { return false }
func (fi *httpFileInfo) Mode() FileMode      { return 0 }
func (fi *httpFileInfo) Sys() interface{}    { return nil }
```

### Decision 8: FileSystem Factory

**Options**:
A. Global registry
B. Connection-scoped factory
C. URL-based lazy loading

**Choice**: C - URL-based lazy loading with connection context

```go
// internal/io/filesystem/factory.go

package filesystem

import (
    "context"
    "fmt"
    "strings"
)

type FileSystemFactory interface {
    GetFileSystem(ctx context.Context, url string) (FileSystem, error)
    RegisterScheme(scheme string, factory FileSystemFactoryFunc)
}

type FileSystemFactoryFunc func(ctx context.Context, url string) (FileSystem, error)

type factory struct {
    schemes map[string]FileSystemFactoryFunc
}

func NewFileSystemFactory() *factory {
    f := &factory{
        schemes: make(map[string]FileSystemFactoryFunc),
    }
    
    // Register default schemes
    f.schemes["file"] = f.newLocalFileSystem
    f.schemes[""] = f.newLocalFileSystem // Empty scheme means local
    f.schemes["s3"] = f.newS3FileSystem
    f.schemes["s3a"] = f.newS3FileSystem
    f.schemes["s3n"] = f.newS3FileSystem
    f.schemes["gcs"] = f.newGCSFileSystem
    f.schemes["gs"] = f.newGCSFileSystem
    f.schemes["azure"] = f.newAzureFileSystem
    f.schemes["az"] = f.newAzureFileSystem
    f.schemes["http"] = f.newHTTPFileSystem
    f.schemes["https"] = f.newHTTPFileSystem
    
    return f
}

func (f *factory) GetFileSystem(ctx context.Context, url string) (FileSystem, error) {
    scheme := extractScheme(url)
    
    factoryFunc, ok := f.schemes[scheme]
    if !ok {
        return nil, fmt.Errorf("unsupported scheme: %s", scheme)
    }
    
    return factoryFunc(ctx, url)
}

func (f *factory) RegisterScheme(scheme string, factoryFunc FileSystemFactoryFunc) {
    f.schemes[scheme] = factoryFunc
}

func extractScheme(url string) string {
    if idx := strings.Index(url, "://"); idx >= 0 {
        return url[:idx]
    }
    return ""
}
```

### Decision 9: Secret SQL Integration

**Options**:
A. Parse CREATE SECRET as special statement
B. Use existing binder infrastructure
C. Separate SQL parser for secrets

**Choice**: B - Use existing binder infrastructure

```go
// internal/secret/binder.go

package secret

import (
    "context"
    
    "github.com/duckdb/duckdb-go/internal/binder"
    "github.com/duckdb/duckdb-go/internal/parser"
)

func BindCreateSecret(ctx context.Context, stmt *parser.CreateSecretStmt, binder *binder.Binder) error {
    secret := Secret{
        Name:       stmt.Name,
        Type:       SecretType(strings.ToUpper(stmt.Type)),
        Provider:   ProviderType(strings.ToUpper(stmt.Provider)),
        Scope:      parseSecretScope(stmt.Scope),
        Options:    stmt.Options,
        Persistent: !stmt.Temporary,
        CreatedAt:  time.Now(),
        UpdatedAt:  time.Now(),
    }
    
    manager := GetManager(ctx)
    return manager.CreateSecret(ctx, secret)
}

func BindDropSecret(ctx context.Context, stmt *parser.DropSecretStmt, binder *binder.Binder) error {
    manager := GetManager(ctx)
    return manager.DropSecret(ctx, stmt.Name, stmt.IfExists)
}

func BindAlterSecret(ctx context.Context, stmt *parser.AlterSecretStmt, binder *binder.Binder) error {
    manager := GetManager manager.AlterSecret(ctx, stmt.Name, stmt.Options)
}

(ctx)
    returnfunc parseSecretScope(scope string) SecretScope {
    if scope == "" {
        return SecretScope{Type: ScopeGlobal}
    }
    
    // Parse scope like "s3://bucket/path" or "https://example.com"
    if strings.HasPrefix(scope, "s3://") {
        return SecretScope{
            Type:   ScopePath,
            Prefix: scope,
        }
    }
    
    return SecretScope{
        Type:   ScopeHost,
        Prefix: scope,
    }
}
```

### Decision 10: Integration with COPY Statement

**Options**:
A. Modify existing COPY implementation
B. Create cloud-specific COPY handler
C. Use abstracted FileSystem in existing COPY

**Choice**: C - Use abstracted FileSystem in existing COPY

```go
// Internal integration in COPY executor

func (e *CopyExecutor) Execute(ctx context.Context, stmt *parser.CopyStmt) error {
    // Get filesystem from URL
    fsFactory := filesystem.NewFileSystemFactory()
    fs, err := fsFactory.GetFileSystem(ctx, stmt.Path)
    if err != nil {
        return fmt.Errorf("failed to get filesystem: %w", err)
    }
    
    // Open file using filesystem
    file, err := fs.Open(stmt.Path)
    if err != nil {
        return fmt.Errorf("failed to open file: %w", err)
    }
    defer file.Close()
    
    // Get secret for the path
    secretManager := secret.GetManager(ctx)
    secret, err := secretManager.GetSecret(ctx, stmt.Path, secret.SecretTypeS3)
    if err == nil {
        // Apply secret credentials to filesystem
        fs = applySecret(fs, secret)
    }
    
    // Use file with existing reader/writer
    switch stmt.Direction {
    case parser.CopyFrom:
        return e.readFromFile(ctx, file, stmt.Format)
    case parser.CopyTo:
        return e.writeToFile(ctx, file, stmt.Format)
    }
    
    return nil
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| AWS SDK size | Medium | Use minimal dependencies; only load what needed |
| Platform support | High | Ensure WASM compatibility; avoid OS-specific syscalls |
| Credential security | High | Never log credentials; use secure defaults |
| Performance | Medium | Cache credentials; connection pooling |
| Secret storage | Medium | Encrypt secrets at rest; support external providers |

## Performance Considerations

1. **Connection Pooling**: Reuse S3/GCS/Azure clients where possible
2. **Credential Caching**: Cache credentials to avoid repeated provider calls
3. **Range Request Optimization**: Support partial reads for columnar formats
4. **Multipart Uploads**: Enable for large COPY TO operations
5. **Parallel Downloads**: Support concurrent reads for large files

## Migration Plan

### Phase 1: FileSystem Interface
1. Create `internal/io/filesystem/` package
2. Implement local filesystem (wrapper around os.File)
3. Create FileSystem interface and factory
4. Update persistence layer to use FileSystem

### Phase 2: S3 Support
1. Add AWS SDK v2 dependency
2. Implement S3FileSystem
3. Add credential provider chain
4. Implement range requests and multipart uploads

### Phase 3: Secrets Manager
1. Create `internal/secret/` package
2. Implement secret manager with catalog storage
3. Add SQL binding for CREATE/DROP/ALTER SECRET
4. Implement path-based secret lookup

### Phase 4: Cloud URL Integration
1. Update COPY statement to parse cloud URLs
2. Update table functions to use FileSystem
3. Add support for GCS and Azure filesystems
4. Add HTTP/HTTPS filesystem

### Phase 5: Testing
1. Unit tests for each filesystem
2. Integration tests with cloud providers
3. Performance benchmarks
4. Compatibility tests against DuckDB

## Open Questions

1. **Secret Storage Format**: Should secrets be stored in the DuckDB catalog or separate file?
   - Current decision: Store in catalog (consistent with DuckDB)
   
2. **Credential Provider Order**: What priority for AWS credentials?
   - Current decision: Config > Env > Shared Config > IMDSv2
   
3. **WASM Support**: How to handle async I/O in WASM?
   - Current decision: Use Go's existing async model; WASM may require synchronous shim

4. **Secret Scope Matching**: Should secret lookup be prefix or exact match?
   - Current decision: Longest prefix match (DuckDB behavior)
