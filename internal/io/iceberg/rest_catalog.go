// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements the REST catalog client for accessing Iceberg tables via the REST API.
package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// RESTCatalogOptions contains options for configuring a REST catalog client.
type RESTCatalogOptions struct {
	// URI is the base URL of the REST catalog API.
	URI string
	// Warehouse is the warehouse location (optional, depends on catalog implementation).
	Warehouse string
	// Credential is the OAuth2 client credentials in the format "client_id:client_secret".
	// Used for client_credentials grant type.
	Credential string
	// Token is a pre-existing bearer token for authentication.
	// If set, OAuth2 flow is skipped and this token is used directly.
	Token string
	// Scope is the OAuth2 scope to request (optional).
	Scope string
	// Headers contains custom HTTP headers to include in all requests.
	Headers map[string]string
	// Timeout is the HTTP client timeout (default: 30 seconds).
	Timeout time.Duration
	// RetryConfig configures retry behavior for transient errors.
	RetryConfig *RetryConfig
}

// DefaultRESTCatalogOptions returns default options for the REST catalog.
func DefaultRESTCatalogOptions() *RESTCatalogOptions {
	return &RESTCatalogOptions{
		Timeout: 30 * time.Second,
		Headers: make(map[string]string),
	}
}

// RESTCatalog implements the Iceberg REST catalog API client.
// It provides methods to list namespaces, tables, and load table metadata.
// The client supports OAuth2 authentication with automatic token refresh.
//
// Reference: https://iceberg.apache.org/docs/latest/rest-catalog/
type RESTCatalog struct {
	// baseURL is the base URL of the REST catalog API.
	baseURL string
	// warehouse is the configured warehouse location.
	warehouse string
	// client is the HTTP client for making requests.
	client *http.Client
	// token holds the current OAuth2 token.
	token *OAuth2Token
	// tokenMu protects concurrent access to the token.
	tokenMu sync.RWMutex
	// opts contains the catalog options.
	opts *RESTCatalogOptions
	// catalogConfig holds server-provided configuration.
	catalogConfig map[string]string
	// configLoaded tracks whether catalog config has been fetched.
	configLoaded bool
}

// OAuth2Token represents an OAuth2 access token.
type OAuth2Token struct {
	// AccessToken is the bearer token.
	AccessToken string `json:"access_token"`
	// TokenType is typically "bearer".
	TokenType string `json:"token_type"`
	// ExpiresIn is the token lifetime in seconds.
	ExpiresIn int `json:"expires_in"`
	// Scope is the granted scope.
	Scope string `json:"scope,omitempty"`
	// RefreshToken is the refresh token (if provided).
	RefreshToken string `json:"refresh_token,omitempty"`
	// ExpiresAt is the calculated expiration time.
	ExpiresAt time.Time `json:"-"`
}

// IsExpired returns true if the token has expired or will expire soon.
// Tokens are considered expired 30 seconds before actual expiration to allow for clock skew.
func (t *OAuth2Token) IsExpired() bool {
	if t == nil {
		return true
	}
	// Consider expired 30 seconds before actual expiration
	return time.Now().Add(30 * time.Second).After(t.ExpiresAt)
}

// RetryConfig contains retry configuration for the REST catalog.
type RetryConfig struct {
	// MaxRetries is the maximum number of retry attempts.
	MaxRetries int
	// InitialDelay is the initial delay between retries.
	InitialDelay time.Duration
	// MaxDelay is the maximum delay between retries.
	MaxDelay time.Duration
	// BackoffFactor is the multiplier for exponential backoff.
	BackoffFactor float64
}

// DefaultRetryConfig returns a default retry configuration.
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:    3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      10 * time.Second,
		BackoffFactor: 2.0,
	}
}

// NewRESTCatalog creates a new REST catalog client with the given options.
func NewRESTCatalog(ctx context.Context, opts *RESTCatalogOptions) (*RESTCatalog, error) {
	if opts == nil {
		opts = DefaultRESTCatalogOptions()
	}

	if opts.URI == "" {
		return nil, fmt.Errorf("%w: URI is required", ErrRESTCatalogRequest)
	}

	// Normalize the base URL
	baseURL := strings.TrimSuffix(opts.URI, "/")

	// Create HTTP client with timeout
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	client := &http.Client{
		Timeout: timeout,
	}

	catalog := &RESTCatalog{
		baseURL:   baseURL,
		warehouse: opts.Warehouse,
		client:    client,
		opts:      opts,
	}

	// If a bearer token is provided directly, use it
	if opts.Token != "" {
		catalog.token = &OAuth2Token{
			AccessToken: opts.Token,
			TokenType:   "bearer",
			// Set a far-future expiration since we don't know the actual expiration
			ExpiresAt: time.Now().Add(24 * 365 * time.Hour), // 1 year
		}
	} else if opts.Credential != "" {
		// Perform OAuth2 token exchange
		if err := catalog.authenticate(ctx); err != nil {
			return nil, err
		}
	}

	// Load catalog configuration
	if err := catalog.loadConfig(ctx); err != nil {
		// Config loading is optional - some catalogs may not support it
		// Just log and continue
	}

	return catalog, nil
}

// authenticate performs OAuth2 client credentials authentication.
func (c *RESTCatalog) authenticate(ctx context.Context) error {
	// Parse credentials
	parts := strings.SplitN(c.opts.Credential, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf(
			"%w: invalid credential format, expected client_id:client_secret",
			ErrRESTCatalogAuth,
		)
	}

	clientID := parts[0]
	clientSecret := parts[1]

	// Build token request
	tokenURL := c.baseURL + "/v1/oauth/tokens"

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)
	if c.opts.Scope != "" {
		data.Set("scope", c.opts.Scope)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		tokenURL,
		strings.NewReader(data.Encode()),
	)
	if err != nil {
		return fmt.Errorf("%w: failed to create token request: %w", ErrRESTCatalogAuth, err)
	}

	// OAuth2 token requests use form-urlencoded, not JSON
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "dukdb-go/1.0 iceberg-rest-client")
	// Apply custom headers but preserve Content-Type
	for key, value := range c.opts.Headers {
		if key != "Content-Type" {
			req.Header.Set(key, value)
		}
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: token request failed: %w", ErrRESTCatalogAuth, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf(
			"%w: token request returned %d: %s",
			ErrRESTCatalogAuth,
			resp.StatusCode,
			string(body),
		)
	}

	var token OAuth2Token
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return fmt.Errorf("%w: failed to decode token response: %w", ErrRESTCatalogAuth, err)
	}

	// Calculate expiration time
	if token.ExpiresIn > 0 {
		token.ExpiresAt = time.Now().Add(time.Duration(token.ExpiresIn) * time.Second)
	} else {
		// Default to 1 hour if not specified
		token.ExpiresAt = time.Now().Add(1 * time.Hour)
	}

	c.tokenMu.Lock()
	c.token = &token
	c.tokenMu.Unlock()

	return nil
}

// ensureAuthenticated ensures we have a valid token, refreshing if necessary.
func (c *RESTCatalog) ensureAuthenticated(ctx context.Context) error {
	c.tokenMu.RLock()
	token := c.token
	c.tokenMu.RUnlock()

	if token == nil {
		// No authentication configured
		return nil
	}

	if !token.IsExpired() {
		return nil
	}

	// Token is expired, refresh it
	if c.opts.Credential != "" {
		return c.authenticate(ctx)
	}

	return fmt.Errorf("%w: token expired and no credentials to refresh", ErrRESTCatalogTokenExpired)
}

// loadConfig loads the catalog configuration from the server.
func (c *RESTCatalog) loadConfig(ctx context.Context) error {
	config, err := c.GetConfig(ctx)
	if err != nil {
		return err
	}

	c.catalogConfig = config
	c.configLoaded = true

	// Update warehouse from config if not already set
	if c.warehouse == "" {
		if warehouse, ok := config["warehouse"]; ok {
			c.warehouse = warehouse
		}
	}

	return nil
}

// GetConfig retrieves the catalog configuration from the server.
// GET /v1/config
func (c *RESTCatalog) GetConfig(ctx context.Context) (map[string]string, error) {
	if err := c.ensureAuthenticated(ctx); err != nil {
		return nil, err
	}

	configURL := c.baseURL + "/v1/config"
	if c.warehouse != "" {
		configURL += "?warehouse=" + url.QueryEscape(c.warehouse)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, configURL, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: failed to create config request: %w",
			ErrRESTCatalogRequest,
			err,
		)
	}

	c.applyHeaders(req)
	c.applyAuth(req)

	resp, err := c.doWithRetry(ctx, req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp)
	}

	var result struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf(
			"%w: failed to decode config response: %w",
			ErrRESTCatalogResponse,
			err,
		)
	}

	// Merge defaults and overrides
	config := make(map[string]string)
	for k, v := range result.Defaults {
		config[k] = v
	}
	for k, v := range result.Overrides {
		config[k] = v
	}

	return config, nil
}

// ListNamespaces lists all namespaces in the catalog.
// GET /v1/namespaces
func (c *RESTCatalog) ListNamespaces(ctx context.Context, parent []string) ([][]string, error) {
	if err := c.ensureAuthenticated(ctx); err != nil {
		return nil, err
	}

	nsURL := c.baseURL + "/v1/namespaces"
	if len(parent) > 0 {
		nsURL += "?parent=" + url.QueryEscape(strings.Join(parent, "\x1f"))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nsURL, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: failed to create namespaces request: %w",
			ErrRESTCatalogRequest,
			err,
		)
	}

	c.applyHeaders(req)
	c.applyAuth(req)

	resp, err := c.doWithRetry(ctx, req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp)
	}

	var result struct {
		Namespaces [][]string `json:"namespaces"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf(
			"%w: failed to decode namespaces response: %w",
			ErrRESTCatalogResponse,
			err,
		)
	}

	return result.Namespaces, nil
}

// ListTables lists all tables in the given namespace.
// GET /v1/namespaces/{namespace}/tables
func (c *RESTCatalog) ListTables(
	ctx context.Context,
	namespace []string,
) ([]TableIdentifier, error) {
	if err := c.ensureAuthenticated(ctx); err != nil {
		return nil, err
	}

	nsPath := encodeNamespace(namespace)
	tablesURL := fmt.Sprintf("%s/v1/namespaces/%s/tables", c.baseURL, nsPath)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tablesURL, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: failed to create tables request: %w",
			ErrRESTCatalogRequest,
			err,
		)
	}

	c.applyHeaders(req)
	c.applyAuth(req)

	resp, err := c.doWithRetry(ctx, req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp)
	}

	var result struct {
		Identifiers []TableIdentifier `json:"identifiers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf(
			"%w: failed to decode tables response: %w",
			ErrRESTCatalogResponse,
			err,
		)
	}

	return result.Identifiers, nil
}

// TableIdentifier represents a fully qualified table name.
type TableIdentifier struct {
	// Namespace is the namespace components.
	Namespace []string `json:"namespace"`
	// Name is the table name.
	Name string `json:"name"`
}

// String returns the full table identifier as a string.
func (t TableIdentifier) String() string {
	if len(t.Namespace) == 0 {
		return t.Name
	}
	return strings.Join(t.Namespace, ".") + "." + t.Name
}

// LoadTableMetadata loads the metadata for a table.
// GET /v1/namespaces/{namespace}/tables/{table}
func (c *RESTCatalog) LoadTableMetadata(
	ctx context.Context,
	namespace []string,
	tableName string,
) (*TableMetadata, string, error) {
	if err := c.ensureAuthenticated(ctx); err != nil {
		return nil, "", err
	}

	nsPath := encodeNamespace(namespace)
	tableURL := fmt.Sprintf(
		"%s/v1/namespaces/%s/tables/%s",
		c.baseURL,
		nsPath,
		url.PathEscape(tableName),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tableURL, http.NoBody)
	if err != nil {
		return nil, "", fmt.Errorf(
			"%w: failed to create table request: %w",
			ErrRESTCatalogRequest,
			err,
		)
	}

	c.applyHeaders(req)
	c.applyAuth(req)

	resp, err := c.doWithRetry(ctx, req)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, "", c.handleErrorResponse(resp)
	}

	// The response contains the full table metadata
	var result struct {
		MetadataLocation string          `json:"metadata-location"`
		Metadata         json.RawMessage `json:"metadata"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, "", fmt.Errorf(
			"%w: failed to decode table response: %w",
			ErrRESTCatalogResponse,
			err,
		)
	}

	// Parse the metadata
	metadata, err := ParseMetadataBytes(result.Metadata)
	if err != nil {
		return nil, "", fmt.Errorf(
			"%w: failed to parse table metadata: %w",
			ErrRESTCatalogResponse,
			err,
		)
	}

	return metadata, result.MetadataLocation, nil
}

// LoadTable loads a table from the catalog and returns a Table object.
func (c *RESTCatalog) LoadTable(
	ctx context.Context,
	namespace []string,
	tableName string,
) (*Table, error) {
	metadata, metadataLocation, err := c.LoadTableMetadata(ctx, namespace, tableName)
	if err != nil {
		return nil, err
	}

	// Create a Table from the metadata
	table := &Table{
		location:         metadata.Location,
		metadata:         metadata,
		metadataLocation: metadataLocation,
		schemaMapper:     NewSchemaMapper(),
		snapshotSelector: NewSnapshotSelector(metadata),
		// Note: manifestReader requires a filesystem, which will be configured separately
	}

	return table, nil
}

// applyHeaders applies custom headers to the request.
func (c *RESTCatalog) applyHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "dukdb-go/1.0 iceberg-rest-client")

	for key, value := range c.opts.Headers {
		req.Header.Set(key, value)
	}
}

// applyAuth applies authentication to the request.
func (c *RESTCatalog) applyAuth(req *http.Request) {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()

	if c.token != nil && c.token.AccessToken != "" {
		tokenType := c.token.TokenType
		if tokenType == "" {
			tokenType = "Bearer"
		}
		req.Header.Set("Authorization", tokenType+" "+c.token.AccessToken)
	}
}

// doWithRetry executes a request with retry logic.
func (c *RESTCatalog) doWithRetry(ctx context.Context, req *http.Request) (*http.Response, error) {
	cfg := c.opts.RetryConfig
	if cfg == nil {
		cfg = DefaultRetryConfig()
	}

	var lastErr error
	var resp *http.Response

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		// Check context
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Clone the request for retry
		reqCopy := req.Clone(ctx)

		resp, lastErr = c.client.Do(reqCopy)
		if lastErr == nil && !isRetryableStatus(resp.StatusCode) {
			return resp, nil
		}

		if resp != nil {
			_ = resp.Body.Close()
		}

		// Don't retry after the last attempt
		if attempt < cfg.MaxRetries {
			delay := c.calculateDelay(cfg, attempt)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// Continue to next attempt
			}
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrRESTCatalogRequest, lastErr)
	}

	return resp, nil
}

// calculateDelay computes the delay for a given attempt.
func (c *RESTCatalog) calculateDelay(cfg *RetryConfig, attempt int) time.Duration {
	delay := cfg.InitialDelay
	for i := 0; i < attempt; i++ {
		delay = time.Duration(float64(delay) * cfg.BackoffFactor)
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
			break
		}
	}
	return delay
}

// isRetryableStatus returns true if the HTTP status code is retryable.
func isRetryableStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
		http.StatusRequestTimeout:
		return true
	default:
		return false
	}
}

// handleErrorResponse handles error responses from the REST catalog.
func (c *RESTCatalog) handleErrorResponse(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)

	// Try to parse as Iceberg error response
	var errorResp struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    int    `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &errorResp); err == nil && errorResp.Error.Message != "" {
		switch resp.StatusCode {
		case http.StatusNotFound:
			return fmt.Errorf("%w: %s", ErrRESTCatalogNotFound, errorResp.Error.Message)
		case http.StatusUnauthorized, http.StatusForbidden:
			return fmt.Errorf("%w: %s", ErrRESTCatalogAuth, errorResp.Error.Message)
		default:
			return fmt.Errorf(
				"%w: %s (code: %d)",
				ErrRESTCatalogRequest,
				errorResp.Error.Message,
				resp.StatusCode,
			)
		}
	}

	// Fallback to generic error
	switch resp.StatusCode {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", ErrRESTCatalogNotFound, string(body))
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("%w: %s", ErrRESTCatalogAuth, string(body))
	default:
		return fmt.Errorf("%w: HTTP %d: %s", ErrRESTCatalogRequest, resp.StatusCode, string(body))
	}
}

// encodeNamespace encodes a namespace for use in URL paths.
// Multi-level namespaces are separated by %1F (unit separator).
func encodeNamespace(namespace []string) string {
	if len(namespace) == 0 {
		return ""
	}
	// URL-encode each part and join with %1F
	parts := make([]string, len(namespace))
	for i, part := range namespace {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "%1F")
}

// ParseCatalogURI parses a catalog URI in the format "iceberg://catalog-uri/namespace/table".
// Returns the catalog URI, namespace, and table name.
func ParseCatalogURI(
	uri string,
) (catalogURI string, namespace []string, tableName string, err error) {
	// Remove iceberg:// prefix
	if !strings.HasPrefix(uri, "iceberg://") {
		return "", nil, "", fmt.Errorf("invalid catalog URI: must start with iceberg://")
	}

	remainder := strings.TrimPrefix(uri, "iceberg://")

	// Split into catalog host and path
	parts := strings.SplitN(remainder, "/", 2)
	if len(parts) < 2 {
		return "", nil, "", fmt.Errorf("invalid catalog URI: missing namespace and table")
	}

	catalogHost := parts[0]
	path := parts[1]

	// The path contains namespace.table or namespace/table
	// First, try to find the last component as the table name
	pathParts := strings.Split(path, "/")
	if len(pathParts) < 2 {
		return "", nil, "", fmt.Errorf(
			"invalid catalog URI: must have at least namespace and table",
		)
	}

	tableName = pathParts[len(pathParts)-1]
	namespace = pathParts[:len(pathParts)-1]

	// Reconstruct catalog URI with https://
	catalogURI = "https://" + catalogHost

	return catalogURI, namespace, tableName, nil
}

// Close releases any resources held by the catalog client.
func (c *RESTCatalog) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

// GetToken returns the current OAuth2 token (for debugging/testing).
func (c *RESTCatalog) GetToken() *OAuth2Token {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return c.token
}

// RefreshToken forces a token refresh.
func (c *RESTCatalog) RefreshToken(ctx context.Context) error {
	if c.opts.Credential == "" {
		return fmt.Errorf("%w: no credentials configured", ErrRESTCatalogAuth)
	}
	return c.authenticate(ctx)
}

// CreateNamespace creates a new namespace (if supported by the catalog).
// POST /v1/namespaces
func (c *RESTCatalog) CreateNamespace(
	ctx context.Context,
	namespace []string,
	properties map[string]string,
) error {
	if err := c.ensureAuthenticated(ctx); err != nil {
		return err
	}

	nsURL := c.baseURL + "/v1/namespaces"

	body := struct {
		Namespace  []string          `json:"namespace"`
		Properties map[string]string `json:"properties,omitempty"`
	}{
		Namespace:  namespace,
		Properties: properties,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("%w: failed to marshal namespace request: %w", ErrRESTCatalogRequest, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, nsURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("%w: failed to create namespace request: %w", ErrRESTCatalogRequest, err)
	}

	c.applyHeaders(req)
	c.applyAuth(req)

	resp, err := c.doWithRetry(ctx, req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return c.handleErrorResponse(resp)
	}

	return nil
}

// DropNamespace drops a namespace (if supported and empty).
// DELETE /v1/namespaces/{namespace}
func (c *RESTCatalog) DropNamespace(ctx context.Context, namespace []string) error {
	if err := c.ensureAuthenticated(ctx); err != nil {
		return err
	}

	nsPath := encodeNamespace(namespace)
	nsURL := fmt.Sprintf("%s/v1/namespaces/%s", c.baseURL, nsPath)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, nsURL, http.NoBody)
	if err != nil {
		return fmt.Errorf(
			"%w: failed to create drop namespace request: %w",
			ErrRESTCatalogRequest,
			err,
		)
	}

	c.applyHeaders(req)
	c.applyAuth(req)

	resp, err := c.doWithRetry(ctx, req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return c.handleErrorResponse(resp)
	}

	return nil
}
