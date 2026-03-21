// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"fmt"
	"io"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	arrowio "github.com/dukdb/dukdb-go/internal/io/arrow"
	csvio "github.com/dukdb/dukdb-go/internal/io/csv"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	jsonio "github.com/dukdb/dukdb-go/internal/io/json"
	parquetio "github.com/dukdb/dukdb-go/internal/io/parquet"
	"github.com/dukdb/dukdb-go/internal/io/url"
	"github.com/dukdb/dukdb-go/internal/secret"
)

// FileSystemProvider provides filesystem access for cloud URLs.
// It integrates with the secret manager for authentication.
type FileSystemProvider struct {
	factory       filesystem.FileSystemFactory
	secretManager secret.Manager
}

// NewFileSystemProvider creates a new FileSystemProvider.
func NewFileSystemProvider(secretMgr secret.Manager) *FileSystemProvider {
	return &FileSystemProvider{
		factory:       filesystem.NewFileSystemFactory(),
		secretManager: secretMgr,
	}
}

// GetFileSystem returns a FileSystem for the given URL.
// If the URL is a cloud URL, it looks up secrets for authentication.
func (p *FileSystemProvider) GetFileSystem(
	ctx context.Context,
	rawURL string,
) (filesystem.FileSystem, error) {
	// Parse the URL to determine scheme
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("IO Error: failed to parse URL %q: %s", rawURL, err),
		}
	}

	// Get base filesystem from factory
	fs, err := p.factory.GetFileSystem(ctx, rawURL)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("IO Error: failed to get filesystem for %q: %s", rawURL, err),
		}
	}

	// For cloud URLs, look up secrets and apply credentials.
	// If no secret manager is configured, fall through to use default credentials
	// (from environment variables, IAM roles, etc.).
	if parsedURL.IsCloudScheme() && p.secretManager != nil {
		secretType := getSecretTypeForScheme(parsedURL.Scheme)
		if secretType != "" {
			sec, err := p.secretManager.GetSecret(ctx, rawURL, secretType)
			if err == nil && sec != nil {
				// Apply secret credentials to filesystem
				fs, err = p.applySecretToFileSystem(ctx, fs, sec, parsedURL)
				if err != nil {
					return nil, &dukdb.Error{
						Type: dukdb.ErrorTypeIO,
						Msg:  fmt.Sprintf("IO Error: failed to apply credentials for %q: %s", rawURL, err),
					}
				}
			}
			// If no secret found, continue with default credentials (from env, IAM, etc.)
		}
	}

	return fs, nil
}

// getSecretTypeForScheme returns the secret type for a URL scheme.
func getSecretTypeForScheme(scheme string) secret.SecretType {
	switch strings.ToLower(scheme) {
	case "s3", "s3a", "s3n":
		return secret.SecretTypeS3
	case "gs", "gcs":
		return secret.SecretTypeGCS
	case "azure", "az":
		return secret.SecretTypeAzure
	case "http", "https":
		return secret.SecretTypeHTTP
	case "hf", "huggingface":
		return secret.SecretTypeHuggingFace
	default:
		return ""
	}
}

// applySecretToFileSystem creates a new filesystem with credentials from the secret.
//
//nolint:exhaustive // We handle common secret types; others fall through to default.
func (p *FileSystemProvider) applySecretToFileSystem(
	ctx context.Context,
	fs filesystem.FileSystem,
	sec *secret.Secret,
	parsedURL *url.ParsedURL,
) (filesystem.FileSystem, error) {
	switch sec.Type {
	case secret.SecretTypeS3:
		return p.applyS3Secret(ctx, sec, parsedURL)
	case secret.SecretTypeGCS:
		return p.applyGCSSecret(ctx, sec, parsedURL)
	case secret.SecretTypeAzure:
		return p.applyAzureSecret(ctx, sec, parsedURL)
	case secret.SecretTypeHTTP:
		return p.applyHTTPSecret(ctx, sec, parsedURL)
	default:
		// Return original filesystem for unknown secret types (including HuggingFace)
		return fs, nil
	}
}

// applyS3Secret creates an S3 filesystem with credentials from the secret.
func (p *FileSystemProvider) applyS3Secret(
	ctx context.Context,
	sec *secret.Secret,
	parsedURL *url.ParsedURL,
) (filesystem.FileSystem, error) {
	config := filesystem.DefaultS3Config()

	// Apply credentials from secret
	if keyID := sec.GetOption(secret.OptionKeyID); keyID != "" {
		config.AccessKeyID = keyID
	}
	if secretKey := sec.GetOption(secret.OptionSecret); secretKey != "" {
		config.SecretAccessKey = secretKey
	}
	if sessionToken := sec.GetOption(secret.OptionSessionToken); sessionToken != "" {
		config.SessionToken = sessionToken
	}
	if region := sec.GetOption(secret.OptionRegion); region != "" {
		config.Region = region
	}
	if endpoint := sec.GetOption(secret.OptionEndpoint); endpoint != "" {
		config.Endpoint = endpoint
	}
	if urlStyle := sec.GetOption(secret.OptionURLStyle); urlStyle != "" {
		switch strings.ToLower(urlStyle) {
		case "path":
			config.URLStyle = filesystem.S3URLStylePath
		case "vhost", "virtual":
			config.URLStyle = filesystem.S3URLStyleVirtual
		}
	}
	if useSSL := sec.GetOption(secret.OptionUseSsl); useSSL != "" {
		config.UseSSL = strings.EqualFold(useSSL, "true")
	}

	// Use region from URL if not in secret
	if config.Region == "" && parsedURL.Region() != "" {
		config.Region = parsedURL.Region()
	}

	s3fs, err := filesystem.NewS3FileSystem(ctx, config)
	if err != nil {
		return nil, wrapCloudFSError("S3", err)
	}
	return s3fs, nil
}

// applyGCSSecret creates a GCS filesystem with credentials from the secret.
func (p *FileSystemProvider) applyGCSSecret(
	ctx context.Context,
	sec *secret.Secret,
	parsedURL *url.ParsedURL,
) (filesystem.FileSystem, error) {
	config := filesystem.DefaultGCSConfig()

	// Apply credentials from secret
	if projectID := sec.GetOption(secret.OptionProjectID); projectID != "" {
		config.ProjectID = projectID
	}
	if saJSON := sec.GetOption(secret.OptionServiceAccountJSON); saJSON != "" {
		config.CredentialsJSON = saJSON
	}

	gcsfs, err := filesystem.NewGCSFileSystem(ctx, config)
	if err != nil {
		return nil, wrapCloudFSError("GCS", err)
	}
	return gcsfs, nil
}

// applyAzureSecret creates an Azure filesystem with credentials from the secret.
func (p *FileSystemProvider) applyAzureSecret(
	ctx context.Context,
	sec *secret.Secret,
	parsedURL *url.ParsedURL,
) (filesystem.FileSystem, error) {
	config := filesystem.DefaultAzureConfig()

	// Apply credentials from secret
	if accountName := sec.GetOption(secret.OptionAccountName); accountName != "" {
		config.AccountName = accountName
	}
	if accountKey := sec.GetOption(secret.OptionAccountKey); accountKey != "" {
		config.AccountKey = accountKey
	}
	if connStr := sec.GetOption(secret.OptionConnectionString); connStr != "" {
		config.ConnectionString = connStr
	}
	if tenantID := sec.GetOption(secret.OptionTenantID); tenantID != "" {
		config.TenantID = tenantID
	}
	if clientID := sec.GetOption(secret.OptionClientID); clientID != "" {
		config.ClientID = clientID
	}
	if clientSecret := sec.GetOption(secret.OptionClientSecret); clientSecret != "" {
		config.ClientSecret = clientSecret
	}

	azfs, err := filesystem.NewAzureFileSystem(ctx, config)
	if err != nil {
		return nil, wrapCloudFSError("Azure", err)
	}
	return azfs, nil
}

// applyHTTPSecret creates an HTTP filesystem with credentials from the secret.
func (p *FileSystemProvider) applyHTTPSecret(
	ctx context.Context,
	sec *secret.Secret,
	parsedURL *url.ParsedURL,
) (filesystem.FileSystem, error) {
	config := filesystem.DefaultHTTPConfig()

	// Apply headers from secret
	if bearerToken := sec.GetOption(secret.OptionBearerToken); bearerToken != "" {
		config.Headers["Authorization"] = "Bearer " + bearerToken
	}
	if extraHeaders := sec.GetOption(secret.OptionExtraHeaders); extraHeaders != "" {
		// Parse extra headers (format: "Header1: Value1; Header2: Value2")
		headers := strings.Split(extraHeaders, ";")
		for _, h := range headers {
			parts := strings.SplitN(strings.TrimSpace(h), ":", 2)
			if len(parts) == 2 {
				config.Headers[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}

	httpfs, err := filesystem.NewHTTPFileSystem(ctx, config)
	if err != nil {
		return nil, wrapCloudFSError("HTTP", err)
	}
	return httpfs, nil
}

// wrapCloudFSError wraps a cloud filesystem creation error with a typed dukdb.Error.
// It distinguishes authentication failures from network/endpoint errors based on
// the error message content.
func wrapCloudFSError(provider string, err error) *dukdb.Error {
	errMsg := err.Error()
	lowerMsg := strings.ToLower(errMsg)

	// Detect authentication-related failures
	if strings.Contains(lowerMsg, "access denied") ||
		strings.Contains(lowerMsg, "forbidden") ||
		strings.Contains(lowerMsg, "403") ||
		strings.Contains(lowerMsg, "invalid access key") ||
		strings.Contains(lowerMsg, "signature") ||
		strings.Contains(lowerMsg, "credential") ||
		strings.Contains(lowerMsg, "unauthorized") ||
		strings.Contains(lowerMsg, "401") ||
		strings.Contains(lowerMsg, "auth") {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg: fmt.Sprintf(
				"IO Error: %s authentication failed: %s. "+
					"Verify your credentials in CREATE SECRET are correct",
				provider, errMsg,
			),
		}
	}

	// Detect network/endpoint unreachable errors
	if strings.Contains(lowerMsg, "no such host") ||
		strings.Contains(lowerMsg, "connection refused") ||
		strings.Contains(lowerMsg, "connection reset") ||
		strings.Contains(lowerMsg, "timeout") ||
		strings.Contains(lowerMsg, "unreachable") ||
		strings.Contains(lowerMsg, "dial tcp") ||
		strings.Contains(lowerMsg, "dns") ||
		strings.Contains(lowerMsg, "network") ||
		strings.Contains(lowerMsg, "tls") ||
		strings.Contains(lowerMsg, "certificate") {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg: fmt.Sprintf(
				"IO Error: %s endpoint unreachable: %s. "+
					"Check the endpoint URL and network connectivity",
				provider, errMsg,
			),
		}
	}

	// Generic cloud filesystem error
	return &dukdb.Error{
		Type: dukdb.ErrorTypeIO,
		Msg:  fmt.Sprintf("IO Error: failed to create %s filesystem: %s", provider, errMsg),
	}
}

// fileWithStat wraps a filesystem.File with stat capability.
type fileWithStat interface {
	io.ReadWriteSeeker
	io.ReaderAt
	io.WriterAt
	io.Closer
	Stat() (filesystem.FileInfo, error)
}

// openFileWithStat opens a file and returns it with stat capability.
// This is used when we need to get file size (e.g., for Parquet).
func (p *FileSystemProvider) openFileWithStat(
	ctx context.Context,
	rawURL string,
) (fileWithStat, error) {
	fs, err := p.GetFileSystem(ctx, rawURL)
	if err != nil {
		return nil, err
	}

	// For cloud filesystems with context support, use OpenContext
	if ctxFS, ok := fs.(filesystem.ContextFileSystem); ok {
		file, err := ctxFS.OpenContext(ctx, rawURL)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("IO Error: failed to open file %q: %s", rawURL, err),
			}
		}
		return file, nil
	}

	file, err := fs.Open(rawURL)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("IO Error: failed to open file %q: %s", rawURL, err),
		}
	}
	return file, nil
}

// createFileForWriting creates a file at a cloud or local URL for writing.
func (p *FileSystemProvider) createFileForWriting(
	ctx context.Context,
	rawURL string,
) (io.WriteCloser, error) {
	fs, err := p.GetFileSystem(ctx, rawURL)
	if err != nil {
		return nil, err
	}

	// Check if filesystem supports writing
	caps := fs.Capabilities()
	if !caps.SupportsWrite {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("IO Error: filesystem does not support writing: %s", rawURL),
		}
	}

	// For cloud filesystems with context support, use CreateContext
	if ctxFS, ok := fs.(filesystem.ContextFileSystem); ok {
		file, err := ctxFS.CreateContext(ctx, rawURL)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("IO Error: failed to create file %q: %s", rawURL, err),
			}
		}
		return file, nil
	}

	file, err := fs.Create(rawURL)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("IO Error: failed to create file %q: %s", rawURL, err),
		}
	}
	return file, nil
}

// createFileReaderFromFS creates a FileReader using the filesystem interface.
func createFileReaderFromFS(
	ctx context.Context,
	provider *FileSystemProvider,
	path string,
	format fileio.Format,
	options map[string]any,
) (fileio.FileReader, error) {
	// Open file using filesystem provider
	file, err := provider.openFileWithStat(ctx, path)
	if err != nil {
		return nil, err
	}

	switch format {
	case fileio.FormatCSV:
		opts := csvio.DefaultReaderOptions()
		applyCSVReaderOptions(opts, options)
		return csvio.NewReader(file, opts)

	case fileio.FormatJSON:
		opts := jsonio.DefaultReaderOptions()
		applyJSONReaderOptions(opts, options)
		return jsonio.NewReader(file, opts)

	case fileio.FormatNDJSON:
		opts := jsonio.DefaultReaderOptions()
		opts.Format = jsonio.FormatNDJSON
		applyJSONReaderOptions(opts, options)
		return jsonio.NewReader(file, opts)

	case fileio.FormatParquet:
		// Parquet reader needs ReaderAtSeeker and file size
		opts := parquetio.DefaultReaderOptions()
		// Check if file implements ReaderAtSeeker
		if ras, ok := file.(parquetio.ReaderAtSeeker); ok {
			// Get file size
			info, err := file.Stat()
			if err != nil {
				_ = file.Close()
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeIO,
					Msg:  fmt.Sprintf("IO Error: failed to get file size for %q: %s", path, err),
				}
			}
			return parquetio.NewReader(ras, info.Size(), opts)
		}
		// Fallback: read entire file into memory for non-seekable readers
		return createParquetReaderFromStream(file, opts)

	case fileio.FormatArrow:
		// Arrow IPC file reader needs ReadAtSeeker
		opts := arrowio.DefaultReaderOptions()
		applyArrowReaderOptions(opts, options)
		// Check if file implements ReadAtSeeker
		if ras, ok := file.(arrowio.ReadAtSeeker); ok {
			return arrowio.NewReader(ras, opts)
		}
		// Fallback: read entire file into memory for non-seekable readers
		return createArrowReaderFromStream(file, opts)

	case fileio.FormatArrowStream:
		// Arrow IPC stream reader works with any io.Reader
		opts := arrowio.DefaultReaderOptions()
		applyArrowReaderOptions(opts, options)
		return arrowio.NewStreamReader(file, opts)

	case fileio.FormatUnknown:
		_ = file.Close()
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  "IO Error: unknown format specified",
		}

	default:
		_ = file.Close()
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("IO Error: unsupported format: %v", format),
		}
	}
}

// createFileWriterFromFS creates a FileWriter using the filesystem interface.
func createFileWriterFromFS(
	ctx context.Context,
	provider *FileSystemProvider,
	path string,
	format fileio.Format,
	options map[string]any,
) (fileio.FileWriter, error) {
	// Create file using filesystem provider
	writer, err := provider.createFileForWriting(ctx, path)
	if err != nil {
		return nil, err
	}

	switch format {
	case fileio.FormatCSV:
		opts := csvio.DefaultWriterOptions()
		applyCSVWriterOptions(opts, options)
		return csvio.NewWriter(writer, opts)

	case fileio.FormatJSON:
		opts := jsonio.DefaultWriterOptions()
		applyJSONWriterOptions(opts, options)
		opts.Format = jsonio.FormatArray
		return jsonio.NewWriter(writer, opts)

	case fileio.FormatNDJSON:
		opts := jsonio.DefaultWriterOptions()
		applyJSONWriterOptions(opts, options)
		opts.Format = jsonio.FormatNDJSON
		return jsonio.NewWriter(writer, opts)

	case fileio.FormatParquet:
		opts := parquetio.DefaultWriterOptions()
		applyParquetWriterOptions(opts, options)
		return parquetio.NewWriter(writer, opts)

	case fileio.FormatArrow:
		opts := arrowio.DefaultWriterOptions()
		applyArrowWriterOptions(opts, options)
		return arrowio.NewWriter(writer, opts)

	case fileio.FormatArrowStream:
		opts := arrowio.DefaultWriterOptions()
		applyArrowWriterOptions(opts, options)
		return arrowio.NewStreamWriter(writer, opts)

	case fileio.FormatUnknown:
		_ = writer.Close()
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  "IO Error: unknown format specified",
		}

	default:
		_ = writer.Close()
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("IO Error: unsupported format: %v", format),
		}
	}
}

// IsCloudURL returns true if the URL refers to a cloud storage location.
func IsCloudURL(rawURL string) bool {
	return filesystem.IsCloudURL(rawURL)
}

// IsLocalURL returns true if the URL refers to a local file.
func IsLocalURL(rawURL string) bool {
	return filesystem.IsLocalURL(rawURL)
}

// bytesReaderAt wraps a byte slice to implement io.ReaderAt and io.Seeker.
type bytesReaderAt struct {
	data   []byte
	offset int64
}

// Read implements io.Reader.
func (b *bytesReaderAt) Read(p []byte) (n int, err error) {
	if b.offset >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n = copy(p, b.data[b.offset:])
	b.offset += int64(n)
	return n, nil
}

// ReadAt implements io.ReaderAt.
func (b *bytesReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n = copy(p, b.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Seek implements io.Seeker.
func (b *bytesReaderAt) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = b.offset + offset
	case io.SeekEnd:
		newOffset = int64(len(b.data)) + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	if newOffset < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	b.offset = newOffset
	return newOffset, nil
}

// Close is a no-op for bytes reader.
func (b *bytesReaderAt) Close() error {
	return nil
}

// createParquetReaderFromStream reads the entire stream into memory and creates a Parquet reader.
// This is a fallback for streams that don't support seeking.
func createParquetReaderFromStream(
	reader io.ReadCloser,
	opts *parquetio.ReaderOptions,
) (fileio.FileReader, error) {
	defer func() { _ = reader.Close() }()

	// Read entire file into memory
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("IO Error: failed to read parquet data: %s", err),
		}
	}

	// Create a bytes reader that implements ReaderAtSeeker
	br := &bytesReaderAt{data: data}

	return parquetio.NewReader(br, int64(len(data)), opts)
}

// createArrowReaderFromStream reads the entire stream into memory and creates an Arrow reader.
// This is a fallback for streams that don't support seeking.
func createArrowReaderFromStream(
	reader io.ReadCloser,
	opts *arrowio.ReaderOptions,
) (fileio.FileReader, error) {
	defer func() { _ = reader.Close() }()

	// Read entire file into memory
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("IO Error: failed to read arrow data: %s", err),
		}
	}

	// Create a bytes reader that implements ReadAtSeeker
	br := &bytesReaderAt{data: data}

	return arrowio.NewReader(br, opts)
}
