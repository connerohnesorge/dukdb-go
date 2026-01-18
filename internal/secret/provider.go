package secret

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Common errors for credential providers.
var (
	// ErrNoCredentials is returned when a provider cannot retrieve credentials.
	ErrNoCredentials = errors.New("no credentials available")
	// ErrCredentialsExpired is returned when credentials have expired.
	ErrCredentialsExpired = errors.New("credentials expired")
	// ErrProviderNotSupported is returned when a provider type is not supported.
	ErrProviderNotSupported = errors.New("provider type not supported")
)

// errUnexpectedStatusCode is the format string for unexpected HTTP status codes.
const errUnexpectedStatusCode = "unexpected status code: %d"

// Credentials holds AWS-style credentials retrieved from a provider.
type Credentials struct {
	// AccessKeyID is the AWS access key ID.
	AccessKeyID string
	// SecretAccessKey is the AWS secret access key.
	SecretAccessKey string
	// SessionToken is an optional session token for temporary credentials.
	SessionToken string
	// Region is the AWS region.
	Region string
	// Expiration is the time when these credentials expire.
	// Zero time means no expiration.
	Expiration time.Time
}

// IsExpired returns true if the credentials have expired.
// Returns false if Expiration is zero (no expiration).
func (c *Credentials) IsExpired() bool {
	if c.Expiration.IsZero() {
		return false
	}

	return time.Now().After(c.Expiration)
}

// IsValid returns true if the credentials have the minimum required fields.
func (c *Credentials) IsValid() bool {
	return c.AccessKeyID != "" && c.SecretAccessKey != ""
}

// CredentialProvider provides credentials for cloud services.
type CredentialProvider interface {
	// Retrieve returns credentials or an error.
	Retrieve(ctx context.Context) (*Credentials, error)
	// Name returns the provider name for logging.
	Name() string
}

// ConfigProvider provides static credentials from a secret's options.
type ConfigProvider struct {
	credentials Credentials
}

// NewConfigProvider creates a ConfigProvider from static credentials.
func NewConfigProvider(accessKeyID, secretAccessKey, sessionToken, region string) *ConfigProvider {
	return &ConfigProvider{
		credentials: Credentials{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			SessionToken:    sessionToken,
			Region:          region,
		},
	}
}

// NewConfigProviderFromSecret creates a ConfigProvider from a Secret's options.
func NewConfigProviderFromSecret(s *Secret) *ConfigProvider {
	return &ConfigProvider{
		credentials: Credentials{
			AccessKeyID:     s.GetOption(OptionKeyID),
			SecretAccessKey: s.GetOption(OptionSecret),
			SessionToken:    s.GetOption(OptionSessionToken),
			Region:          s.GetOption(OptionRegion),
		},
	}
}

// Retrieve returns the static credentials.
func (p *ConfigProvider) Retrieve(_ context.Context) (*Credentials, error) {
	if !p.credentials.IsValid() {
		return nil, fmt.Errorf(
			"%w: access key ID or secret access key not configured",
			ErrNoCredentials,
		)
	}

	// Return a copy to prevent external modification
	creds := p.credentials

	return &creds, nil
}

// Name returns the provider name.
func (*ConfigProvider) Name() string {
	return "ConfigProvider"
}

// EnvProvider reads credentials from environment variables.
type EnvProvider struct{}

// AWS environment variable names.
const (
	EnvAccessKeyID     = "AWS_ACCESS_KEY_ID"
	EnvSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	EnvSessionToken    = "AWS_SESSION_TOKEN"
	EnvRegion          = "AWS_REGION"
	EnvDefaultRegion   = "AWS_DEFAULT_REGION"
)

// NewEnvProvider creates an EnvProvider.
func NewEnvProvider() *EnvProvider {
	return &EnvProvider{}
}

// Retrieve returns credentials from environment variables.
func (*EnvProvider) Retrieve(_ context.Context) (*Credentials, error) {
	accessKeyID := os.Getenv(EnvAccessKeyID)
	secretAccessKey := os.Getenv(EnvSecretAccessKey)

	if accessKeyID == "" || secretAccessKey == "" {
		return nil, fmt.Errorf(
			"%w: %s or %s not set",
			ErrNoCredentials,
			EnvAccessKeyID,
			EnvSecretAccessKey,
		)
	}

	region := os.Getenv(EnvRegion)
	if region == "" {
		region = os.Getenv(EnvDefaultRegion)
	}

	return &Credentials{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		SessionToken:    os.Getenv(EnvSessionToken),
		Region:          region,
	}, nil
}

// Name returns the provider name.
func (*EnvProvider) Name() string {
	return "EnvProvider"
}

// SharedConfigProvider reads credentials from AWS CLI config files.
type SharedConfigProvider struct {
	// profile is the AWS profile name to use. Defaults to "default".
	profile string
	// credentialsFile is the path to the credentials file.
	// Defaults to ~/.aws/credentials.
	credentialsFile string
	// configFile is the path to the config file.
	// Defaults to ~/.aws/config.
	configFile string
}

// SharedConfigOption is a functional option for SharedConfigProvider.
type SharedConfigOption func(*SharedConfigProvider)

// WithProfile sets the AWS profile to use.
func WithProfile(profile string) SharedConfigOption {
	return func(p *SharedConfigProvider) {
		p.profile = profile
	}
}

// WithCredentialsFile sets the path to the credentials file.
func WithCredentialsFile(path string) SharedConfigOption {
	return func(p *SharedConfigProvider) {
		p.credentialsFile = path
	}
}

// WithConfigFile sets the path to the config file.
func WithConfigFile(path string) SharedConfigOption {
	return func(p *SharedConfigProvider) {
		p.configFile = path
	}
}

// NewSharedConfigProvider creates a SharedConfigProvider.
func NewSharedConfigProvider(opts ...SharedConfigOption) *SharedConfigProvider {
	homeDir, _ := os.UserHomeDir()
	awsDir := filepath.Join(homeDir, ".aws")

	p := &SharedConfigProvider{
		profile:         "default",
		credentialsFile: filepath.Join(awsDir, "credentials"),
		configFile:      filepath.Join(awsDir, "config"),
	}

	for _, opt := range opts {
		opt(p)
	}

	// Check for AWS_PROFILE environment variable
	if envProfile := os.Getenv("AWS_PROFILE"); envProfile != "" && p.profile == "default" {
		p.profile = envProfile
	}

	// Check for AWS_SHARED_CREDENTIALS_FILE environment variable
	if envCredsFile := os.Getenv("AWS_SHARED_CREDENTIALS_FILE"); envCredsFile != "" &&
		p.credentialsFile == filepath.Join(awsDir, "credentials") {
		p.credentialsFile = envCredsFile
	}

	// Check for AWS_CONFIG_FILE environment variable
	if envConfigFile := os.Getenv("AWS_CONFIG_FILE"); envConfigFile != "" &&
		p.configFile == filepath.Join(awsDir, "config") {
		p.configFile = envConfigFile
	}

	return p
}

// Retrieve returns credentials from AWS CLI config files.
func (p *SharedConfigProvider) Retrieve(_ context.Context) (*Credentials, error) {
	creds := &Credentials{}

	// Try to read credentials file first
	credValues, err := p.readINIFile(p.credentialsFile, p.profile)
	if err == nil {
		creds.AccessKeyID = credValues["aws_access_key_id"]
		creds.SecretAccessKey = credValues["aws_secret_access_key"]
		creds.SessionToken = credValues["aws_session_token"]
	}

	// Read config file for region and additional settings
	// Config file uses "profile <name>" sections except for default
	configProfile := p.profile
	if configProfile != "default" {
		configProfile = "profile " + p.profile
	}

	configValues, err := p.readINIFile(p.configFile, configProfile)
	if err == nil {
		if creds.Region == "" {
			creds.Region = configValues["region"]
		}
		// Config file can also have credentials
		if creds.AccessKeyID == "" {
			creds.AccessKeyID = configValues["aws_access_key_id"]
		}

		if creds.SecretAccessKey == "" {
			creds.SecretAccessKey = configValues["aws_secret_access_key"]
		}

		if creds.SessionToken == "" {
			creds.SessionToken = configValues["aws_session_token"]
		}
	}

	if !creds.IsValid() {
		return nil, fmt.Errorf(
			"%w: no credentials found in profile %q",
			ErrNoCredentials,
			p.profile,
		)
	}

	return creds, nil
}

// readINIFile reads an INI file and returns the values for the specified section.
func (*SharedConfigProvider) readINIFile(filename, section string) (map[string]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer func() {
		_ = file.Close()
	}()

	return parseINI(file, section)
}

// parseINI parses an INI file and returns values for the specified section.
func parseINI(r io.Reader, section string) (map[string]string, error) {
	result := make(map[string]string)
	scanner := bufio.NewScanner(r)
	inTargetSection := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		// Check for section header
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			sectionName := strings.TrimPrefix(strings.TrimSuffix(line, "]"), "[")
			inTargetSection = strings.EqualFold(sectionName, section)

			continue
		}

		// Parse key=value pairs in the target section
		if !inTargetSection {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		result[key] = value
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return result, nil
}

// Name returns the provider name.
func (*SharedConfigProvider) Name() string {
	return "SharedConfigProvider"
}

// IMDSv2Provider gets credentials from EC2 instance metadata service (IMDSv2).
type IMDSv2Provider struct {
	// httpClient is the HTTP client to use for requests.
	httpClient *http.Client
	// endpoint is the IMDS endpoint. Defaults to the EC2 metadata endpoint.
	endpoint string
	// tokenTTL is the TTL for the IMDSv2 token in seconds.
	tokenTTL int
}

// IMDSv2Option is a functional option for IMDSv2Provider.
type IMDSv2Option func(*IMDSv2Provider)

// IMDS constants.
const (
	// DefaultIMDSEndpoint is the default EC2 metadata service endpoint.
	DefaultIMDSEndpoint = "http://169.254.169.254"
	// IMDSTokenPath is the path to get an IMDSv2 token.
	IMDSTokenPath = "/latest/api/token"
	// IMDSCredentialsPath is the path to get IAM credentials.
	IMDSCredentialsPath = "/latest/meta-data/iam/security-credentials/"
	// IMDSRegionPath is the path to get the instance region.
	IMDSRegionPath = "/latest/meta-data/placement/region"
	// DefaultIMDSTokenTTL is the default token TTL in seconds.
	DefaultIMDSTokenTTL = 21600 // 6 hours
	// DefaultIMDSTimeout is the default timeout for IMDS requests.
	DefaultIMDSTimeout = 2 * time.Second
)

// WithHTTPClient sets the HTTP client to use.
func WithHTTPClient(client *http.Client) IMDSv2Option {
	return func(p *IMDSv2Provider) {
		p.httpClient = client
	}
}

// WithIMDSEndpoint sets the IMDS endpoint.
func WithIMDSEndpoint(endpoint string) IMDSv2Option {
	return func(p *IMDSv2Provider) {
		p.endpoint = endpoint
	}
}

// WithTokenTTL sets the token TTL in seconds.
func WithTokenTTL(ttl int) IMDSv2Option {
	return func(p *IMDSv2Provider) {
		p.tokenTTL = ttl
	}
}

// NewIMDSv2Provider creates an IMDSv2Provider.
func NewIMDSv2Provider(opts ...IMDSv2Option) *IMDSv2Provider {
	p := &IMDSv2Provider{
		httpClient: &http.Client{
			Timeout: DefaultIMDSTimeout,
		},
		endpoint: DefaultIMDSEndpoint,
		tokenTTL: DefaultIMDSTokenTTL,
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

// Retrieve returns credentials from EC2 instance metadata.
func (p *IMDSv2Provider) Retrieve(ctx context.Context) (*Credentials, error) {
	// Step 1: Get IMDSv2 token
	token, err := p.getToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get IMDSv2 token: %w", err)
	}

	// Step 2: Get IAM role name
	roleName, err := p.getRoleName(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("failed to get IAM role name: %w", err)
	}

	// Step 3: Get credentials for the role
	creds, err := p.getCredentials(ctx, token, roleName)
	if err != nil {
		return nil, fmt.Errorf("failed to get credentials: %w", err)
	}

	// Step 4: Get region (optional, don't fail if not available)
	if region, err := p.getRegion(ctx, token); err == nil {
		creds.Region = region
	}

	return creds, nil
}

// getToken retrieves an IMDSv2 token.
func (p *IMDSv2Provider) getToken(ctx context.Context) (string, error) {
	url := p.endpoint + IMDSTokenPath

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, http.NoBody)
	if err != nil {
		return "", err
	}

	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", fmt.Sprintf("%d", p.tokenTTL))

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf(errUnexpectedStatusCode, resp.StatusCode)
	}

	token, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(token), nil
}

// getRoleName retrieves the IAM role name from instance metadata.
func (p *IMDSv2Provider) getRoleName(ctx context.Context, token string) (string, error) {
	url := p.endpoint + IMDSCredentialsPath

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return "", err
	}

	req.Header.Set("X-aws-ec2-metadata-token", token)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf(errUnexpectedStatusCode, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// The response is the role name(s), one per line. We take the first one.
	roleName := strings.TrimSpace(strings.Split(string(body), "\n")[0])
	if roleName == "" {
		return "", fmt.Errorf("%w: no IAM role attached to instance", ErrNoCredentials)
	}

	return roleName, nil
}

// imdsCredentialsResponse represents the JSON response from IMDS credentials endpoint.
type imdsCredentialsResponse struct {
	Code            string    `json:"Code"`
	LastUpdated     string    `json:"LastUpdated"`
	Type            string    `json:"Type"`
	AccessKeyID     string    `json:"AccessKeyId"`
	SecretAccessKey string    `json:"SecretAccessKey"`
	Token           string    `json:"Token"`
	Expiration      time.Time `json:"Expiration"`
}

// getCredentials retrieves credentials for the specified IAM role.
func (p *IMDSv2Provider) getCredentials(
	ctx context.Context,
	token, roleName string,
) (*Credentials, error) {
	url := p.endpoint + IMDSCredentialsPath + roleName

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-aws-ec2-metadata-token", token)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(errUnexpectedStatusCode, resp.StatusCode)
	}

	var credsResp imdsCredentialsResponse
	if err := json.NewDecoder(resp.Body).Decode(&credsResp); err != nil {
		return nil, fmt.Errorf("failed to decode credentials response: %w", err)
	}

	if credsResp.Code != "Success" {
		return nil, fmt.Errorf("credentials request failed: %s", credsResp.Code)
	}

	return &Credentials{
		AccessKeyID:     credsResp.AccessKeyID,
		SecretAccessKey: credsResp.SecretAccessKey,
		SessionToken:    credsResp.Token,
		Expiration:      credsResp.Expiration,
	}, nil
}

// getRegion retrieves the instance region from metadata.
func (p *IMDSv2Provider) getRegion(ctx context.Context, token string) (string, error) {
	url := p.endpoint + IMDSRegionPath

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return "", err
	}

	req.Header.Set("X-aws-ec2-metadata-token", token)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf(errUnexpectedStatusCode, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(body)), nil
}

// Name returns the provider name.
func (*IMDSv2Provider) Name() string {
	return "IMDSv2Provider"
}

// CredentialChain tries multiple providers in order until one succeeds.
type CredentialChain struct {
	// providers is the list of providers to try in order.
	providers []CredentialProvider
	// logger is used for logging which provider succeeded.
	logger *slog.Logger
}

// NewCredentialChain creates a CredentialChain with the given providers.
func NewCredentialChain(providers ...CredentialProvider) *CredentialChain {
	return &CredentialChain{
		providers: providers,
		logger:    slog.Default(),
	}
}

// SetLogger sets the logger for the credential chain.
func (c *CredentialChain) SetLogger(logger *slog.Logger) {
	c.logger = logger
}

// Retrieve tries each provider in order until one succeeds.
func (c *CredentialChain) Retrieve(ctx context.Context) (*Credentials, error) {
	var errs []error

	for _, provider := range c.providers {
		creds, err := provider.Retrieve(ctx)
		if err == nil && creds.IsValid() {
			c.logger.Debug("credentials retrieved successfully",
				"provider", provider.Name(),
			)

			return creds, nil
		}

		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", provider.Name(), err))
		}
	}

	if len(errs) == 0 {
		return nil, ErrNoCredentials
	}

	return nil, fmt.Errorf("%w: all providers failed: %v", ErrNoCredentials, errs)
}

// Name returns the provider name.
func (*CredentialChain) Name() string {
	return "CredentialChain"
}

// NewDefaultProviderChain creates a default credential provider chain.
// The chain tries providers in this order:
// 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
// 2. Shared credentials file (~/.aws/credentials)
// 3. EC2 instance metadata (IMDSv2)
func NewDefaultProviderChain() *CredentialChain {
	return NewCredentialChain(
		NewEnvProvider(),
		NewSharedConfigProvider(),
		NewIMDSv2Provider(),
	)
}

// NewProviderFromSecret creates a CredentialProvider based on a secret's provider type.
func NewProviderFromSecret(s *Secret) (CredentialProvider, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: secret is nil", ErrProviderNotSupported)
	}

	switch s.Provider {
	case ProviderConfig:
		return NewConfigProviderFromSecret(s), nil

	case ProviderEnv:
		return NewEnvProvider(), nil

	case ProviderCredentialChain:
		// Build a chain that includes config credentials if present
		var providers []CredentialProvider

		// If the secret has explicit credentials, try them first
		if s.HasOption(OptionKeyID) && s.HasOption(OptionSecret) {
			providers = append(providers, NewConfigProviderFromSecret(s))
		}

		// Then try the default chain
		providers = append(providers,
			NewEnvProvider(),
			NewSharedConfigProvider(),
			NewIMDSv2Provider(),
		)

		return NewCredentialChain(providers...), nil

	case ProviderIAM:
		return NewIMDSv2Provider(), nil

	default:
		return nil, fmt.Errorf("%w: %s", ErrProviderNotSupported, s.Provider)
	}
}

// ConfigFromSecret creates an S3Config from a secret using the appropriate provider.
// This is a helper function for integrating with the S3 filesystem.
func ConfigFromSecret(ctx context.Context, s *Secret) (*Credentials, error) {
	provider, err := NewProviderFromSecret(s)
	if err != nil {
		return nil, err
	}

	creds, err := provider.Retrieve(ctx)
	if err != nil {
		return nil, err
	}

	// Override region from secret options if specified
	if region := s.GetOption(OptionRegion); region != "" && creds.Region == "" {
		creds.Region = region
	}

	return creds, nil
}

// Verify interface implementations.
var (
	_ CredentialProvider = (*ConfigProvider)(nil)
	_ CredentialProvider = (*EnvProvider)(nil)
	_ CredentialProvider = (*SharedConfigProvider)(nil)
	_ CredentialProvider = (*IMDSv2Provider)(nil)
	_ CredentialProvider = (*CredentialChain)(nil)
)
