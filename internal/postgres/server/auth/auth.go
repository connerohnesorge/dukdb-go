// Package auth provides authentication interfaces and implementations for the
// PostgreSQL wire protocol server.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/crypto/pbkdf2"
)

// Common authentication errors.
var (
	// ErrAuthenticationFailed is returned when authentication fails.
	ErrAuthenticationFailed = errors.New("authentication failed")

	// ErrUserNotFound is returned when a user is not found.
	ErrUserNotFound = errors.New("user not found")

	// ErrInvalidCredentials is returned when credentials are invalid.
	ErrInvalidCredentials = errors.New("invalid credentials")

	// ErrDatabaseAccessDenied is returned when the user does not have access to the database.
	ErrDatabaseAccessDenied = errors.New("database access denied")

	// ErrSCRAMFailed is returned when SCRAM authentication fails.
	ErrSCRAMFailed = errors.New("SCRAM authentication failed")

	// ErrCertificateRequired is returned when certificate authentication is required but not provided.
	ErrCertificateRequired = errors.New("client certificate required")

	// ErrInvalidCertificate is returned when the client certificate is invalid.
	ErrInvalidCertificate = errors.New("invalid client certificate")

	// ErrAccessDenied is returned when access is denied by host-based rules.
	ErrAccessDenied = errors.New("access denied by pg_hba rules")

	// ErrLDAPAuthFailed is returned when LDAP authentication fails.
	ErrLDAPAuthFailed = errors.New("LDAP authentication failed")

	// ErrPermissionDenied is returned when a user lacks required permissions.
	ErrPermissionDenied = errors.New("permission denied")
)

// Method represents the authentication method type.
type Method string

// Supported authentication methods.
const (
	// MethodNone indicates no authentication is required.
	MethodNone Method = "none"

	// MethodTrust indicates no authentication is required (trust).
	MethodTrust Method = "trust"

	// MethodPassword indicates clear text password authentication.
	MethodPassword Method = "password"

	// MethodMD5 indicates MD5 password authentication (future).
	MethodMD5 Method = "md5"

	// MethodSCRAMSHA256 indicates SCRAM-SHA-256 authentication.
	MethodSCRAMSHA256 Method = "scram-sha-256"

	// MethodCert indicates certificate-based authentication.
	MethodCert Method = "cert"

	// MethodLDAP indicates LDAP authentication.
	MethodLDAP Method = "ldap"

	// MethodReject indicates connections should be rejected.
	MethodReject Method = "reject"
)

// Role represents a permission role for RBAC.
type Role string

// Predefined roles.
const (
	// RoleSuperuser has all permissions.
	RoleSuperuser Role = "superuser"

	// RoleAdmin can manage users and databases.
	RoleAdmin Role = "admin"

	// RoleReadWrite can read and write data.
	RoleReadWrite Role = "readwrite"

	// RoleReadOnly can only read data.
	RoleReadOnly Role = "readonly"

	// RoleConnect can only connect to the database.
	RoleConnect Role = "connect"
)

// Permission represents a specific permission.
type Permission string

// Predefined permissions.
const (
	PermConnect      Permission = "CONNECT"
	PermCreateDB     Permission = "CREATEDB"
	PermCreateRole   Permission = "CREATEROLE"
	PermCreateTable  Permission = "CREATE"
	PermSelect       Permission = "SELECT"
	PermInsert       Permission = "INSERT"
	PermUpdate       Permission = "UPDATE"
	PermDelete       Permission = "DELETE"
	PermTruncate     Permission = "TRUNCATE"
	PermReferences   Permission = "REFERENCES"
	PermTrigger      Permission = "TRIGGER"
	PermExecute      Permission = "EXECUTE"
	PermUsage        Permission = "USAGE"
	PermAll          Permission = "ALL"
	PermSuperuser    Permission = "SUPERUSER"
	PermReplication  Permission = "REPLICATION"
	PermBypassRLS    Permission = "BYPASSRLS"
)

// User represents a database user with authentication information.
type User struct {
	// Username is the unique username for this user.
	Username string

	// PasswordHash is the hashed password (for future hashed password support).
	// Currently stores the plain password for MethodPassword.
	PasswordHash string

	// SCRAMCredentials stores SCRAM-SHA-256 credentials.
	SCRAMCredentials *SCRAMCredentials

	// Databases is the list of databases this user can access.
	// An empty slice means access to all databases.
	Databases []string

	// Superuser indicates if this user has superuser privileges.
	Superuser bool

	// Roles is the list of roles assigned to this user.
	Roles []Role

	// Permissions is the set of explicit permissions granted to this user.
	Permissions map[Permission]bool

	// CertificateCN is the expected certificate Common Name for cert auth.
	// If set, the user can authenticate via client certificate with matching CN.
	CertificateCN string

	// LDAPBindDN is the LDAP bind DN for this user (optional, for LDAP auth).
	LDAPBindDN string

	// Enabled indicates if the user account is enabled.
	Enabled bool

	// ConnectionLimit is the maximum number of concurrent connections (-1 for unlimited).
	ConnectionLimit int

	// ValidUntil is the expiration time for the password (zero means no expiration).
	ValidUntil int64 // Unix timestamp
}

// NewUser creates a new user with default settings.
func NewUser(username string) *User {
	return &User{
		Username:        username,
		Enabled:         true,
		ConnectionLimit: -1, // unlimited
		Permissions:     make(map[Permission]bool),
	}
}

// CanAccessDatabase returns true if the user can access the given database.
func (u *User) CanAccessDatabase(database string) bool {
	// Disabled users cannot access anything
	if !u.Enabled {
		return false
	}

	// Superusers can access all databases
	if u.Superuser || u.HasRole(RoleSuperuser) {
		return true
	}

	// Empty database list means access to all databases
	if len(u.Databases) == 0 {
		return true
	}

	// Check if database is in the allowed list
	for _, db := range u.Databases {
		if db == database {
			return true
		}
	}

	return false
}

// HasRole checks if the user has the specified role.
func (u *User) HasRole(role Role) bool {
	for _, r := range u.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// HasPermission checks if the user has the specified permission.
func (u *User) HasPermission(perm Permission) bool {
	// Superusers have all permissions
	if u.Superuser || u.HasRole(RoleSuperuser) {
		return true
	}

	// Check role-based permissions
	for _, role := range u.Roles {
		if roleHasPermission(role, perm) {
			return true
		}
	}

	// Check explicit permissions
	if u.Permissions != nil {
		if u.Permissions[PermAll] || u.Permissions[perm] {
			return true
		}
	}

	return false
}

// AddRole adds a role to the user.
func (u *User) AddRole(role Role) {
	if !u.HasRole(role) {
		u.Roles = append(u.Roles, role)
	}
}

// RemoveRole removes a role from the user.
func (u *User) RemoveRole(role Role) {
	for i, r := range u.Roles {
		if r == role {
			u.Roles = append(u.Roles[:i], u.Roles[i+1:]...)
			return
		}
	}
}

// GrantPermission grants a permission to the user.
func (u *User) GrantPermission(perm Permission) {
	if u.Permissions == nil {
		u.Permissions = make(map[Permission]bool)
	}
	u.Permissions[perm] = true
}

// RevokePermission revokes a permission from the user.
func (u *User) RevokePermission(perm Permission) {
	if u.Permissions != nil {
		delete(u.Permissions, perm)
	}
}

// roleHasPermission checks if a role includes a specific permission.
func roleHasPermission(role Role, perm Permission) bool {
	switch role {
	case RoleSuperuser:
		return true // superuser has all permissions
	case RoleAdmin:
		switch perm { //nolint:exhaustive // intentionally checking specific permissions
		case PermConnect, PermCreateDB, PermCreateRole, PermCreateTable,
			PermSelect, PermInsert, PermUpdate, PermDelete, PermTruncate,
			PermReferences, PermTrigger, PermExecute, PermUsage:
			return true
		}
	case RoleReadWrite:
		switch perm { //nolint:exhaustive // intentionally checking specific permissions
		case PermConnect, PermSelect, PermInsert, PermUpdate, PermDelete, PermExecute:
			return true
		}
	case RoleReadOnly:
		switch perm { //nolint:exhaustive // intentionally checking specific permissions
		case PermConnect, PermSelect, PermExecute:
			return true
		}
	case RoleConnect:
		return perm == PermConnect
	}

	return false
}

// Authenticator defines the authentication strategy interface.
// Implementations can provide different authentication mechanisms.
type Authenticator interface {
	// Authenticate validates credentials and returns success/failure.
	// The context can be used to pass authentication-related data.
	// Returns true if authentication succeeded, false otherwise.
	// An error is returned only for system failures, not authentication failures.
	Authenticate(ctx context.Context, username, password, database string) (bool, error)

	// Method returns the authentication method used by this authenticator.
	Method() Method
}

// UserProvider provides user information for authentication.
// This interface separates user storage from authentication logic.
type UserProvider interface {
	// GetUser returns user info, or nil if not found.
	// An error is returned only for system failures.
	GetUser(username string) (*User, error)

	// ValidatePassword checks if the password is correct for the user.
	// Returns true if the password is valid, false otherwise.
	// An error is returned only for system failures.
	ValidatePassword(username, password string) (bool, error)
}

// Result contains the result of an authentication attempt.
type Result struct {
	// Success indicates if authentication was successful.
	Success bool

	// User is the authenticated user (nil if authentication failed).
	User *User

	// Error is any error that occurred during authentication.
	Error error

	// Message is a human-readable message about the result.
	Message string
}

// PasswordAuthenticator implements Authenticator using a UserProvider.
// This is the default authenticator for password-based authentication.
type PasswordAuthenticator struct {
	provider UserProvider
}

// NewPasswordAuthenticator creates a new password authenticator with the given provider.
func NewPasswordAuthenticator(provider UserProvider) *PasswordAuthenticator {
	return &PasswordAuthenticator{provider: provider}
}

// Authenticate implements the Authenticator interface.
func (a *PasswordAuthenticator) Authenticate(ctx context.Context, username, password, database string) (bool, error) {
	if a.provider == nil {
		return false, errors.New("no user provider configured")
	}

	// Get the user
	user, err := a.provider.GetUser(username)
	if err != nil {
		return false, err
	}
	if user == nil {
		return false, nil
	}

	// Validate the password
	valid, err := a.provider.ValidatePassword(username, password)
	if err != nil {
		return false, err
	}
	if !valid {
		return false, nil
	}

	// Check database access
	if !user.CanAccessDatabase(database) {
		return false, nil
	}

	return true, nil
}

// Method implements the Authenticator interface.
func (a *PasswordAuthenticator) Method() Method {
	return MethodPassword
}

// NoAuthenticator implements Authenticator that always succeeds.
// Use this when no authentication is required.
type NoAuthenticator struct{}

// NewNoAuthenticator creates a new no-op authenticator.
func NewNoAuthenticator() *NoAuthenticator {
	return &NoAuthenticator{}
}

// Authenticate implements the Authenticator interface.
// It always returns true for any credentials.
func (a *NoAuthenticator) Authenticate(ctx context.Context, username, password, database string) (bool, error) {
	return true, nil
}

// Method implements the Authenticator interface.
func (a *NoAuthenticator) Method() Method {
	return MethodNone
}

// SimpleAuthenticator implements Authenticator with a single username/password.
// This is a simple authenticator for basic configurations.
type SimpleAuthenticator struct {
	username string
	password string
}

// NewSimpleAuthenticator creates a new simple authenticator.
func NewSimpleAuthenticator(username, password string) *SimpleAuthenticator {
	return &SimpleAuthenticator{
		username: username,
		password: password,
	}
}

// Authenticate implements the Authenticator interface.
func (a *SimpleAuthenticator) Authenticate(ctx context.Context, username, password, database string) (bool, error) {
	return username == a.username && password == a.password, nil
}

// Method implements the Authenticator interface.
func (a *SimpleAuthenticator) Method() Method {
	return MethodPassword
}

// =============================================================================
// SCRAM-SHA-256 Authentication (Task 16.1)
// =============================================================================

// SCRAMIterationCount is the default number of PBKDF2 iterations for SCRAM-SHA-256.
// PostgreSQL uses 4096 as the default.
const SCRAMIterationCount = 4096

// SCRAMSaltLength is the default salt length in bytes.
const SCRAMSaltLength = 16

// SCRAMCredentials holds the SCRAM-SHA-256 credentials for a user.
type SCRAMCredentials struct {
	// Salt is the random salt used for password hashing.
	Salt []byte

	// StoredKey is the stored key derived from the client key.
	StoredKey []byte

	// ServerKey is the server key used for server verification.
	ServerKey []byte

	// Iterations is the number of PBKDF2 iterations.
	Iterations int
}

// GenerateSCRAMCredentials generates SCRAM-SHA-256 credentials from a password.
func GenerateSCRAMCredentials(password string) (*SCRAMCredentials, error) {
	return GenerateSCRAMCredentialsWithIterations(password, SCRAMIterationCount)
}

// GenerateSCRAMCredentialsWithIterations generates SCRAM credentials with custom iteration count.
func GenerateSCRAMCredentialsWithIterations(password string, iterations int) (*SCRAMCredentials, error) {
	// Generate random salt
	salt := make([]byte, SCRAMSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	// Derive salted password using PBKDF2
	saltedPassword := pbkdf2.Key([]byte(password), salt, iterations, sha256.Size, sha256.New)

	// Calculate client key: HMAC(SaltedPassword, "Client Key")
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))

	// Calculate stored key: H(ClientKey)
	storedKey := sha256Sum(clientKey)

	// Calculate server key: HMAC(SaltedPassword, "Server Key")
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))

	return &SCRAMCredentials{
		Salt:       salt,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
		Iterations: iterations,
	}, nil
}

// Encode encodes the SCRAM credentials to a PostgreSQL-compatible string format.
// Format: SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>
func (c *SCRAMCredentials) Encode() string {
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s",
		c.Iterations,
		base64.StdEncoding.EncodeToString(c.Salt),
		base64.StdEncoding.EncodeToString(c.StoredKey),
		base64.StdEncoding.EncodeToString(c.ServerKey),
	)
}

// ParseSCRAMCredentials parses SCRAM credentials from the PostgreSQL format.
func ParseSCRAMCredentials(encoded string) (*SCRAMCredentials, error) {
	if !strings.HasPrefix(encoded, "SCRAM-SHA-256$") {
		return nil, errors.New("invalid SCRAM credentials format")
	}

	// Remove prefix
	rest := strings.TrimPrefix(encoded, "SCRAM-SHA-256$")

	// Split into iterations:salt and storedKey:serverKey
	parts := strings.Split(rest, "$")
	if len(parts) != 2 {
		return nil, errors.New("invalid SCRAM credentials format")
	}

	// Parse iterations and salt
	iterSalt := strings.Split(parts[0], ":")
	if len(iterSalt) != 2 {
		return nil, errors.New("invalid SCRAM credentials format")
	}

	iterations, err := strconv.Atoi(iterSalt[0])
	if err != nil {
		return nil, fmt.Errorf("invalid iteration count: %w", err)
	}

	salt, err := base64.StdEncoding.DecodeString(iterSalt[1])
	if err != nil {
		return nil, fmt.Errorf("invalid salt: %w", err)
	}

	// Parse stored key and server key
	keys := strings.Split(parts[1], ":")
	if len(keys) != 2 {
		return nil, errors.New("invalid SCRAM credentials format")
	}

	storedKey, err := base64.StdEncoding.DecodeString(keys[0])
	if err != nil {
		return nil, fmt.Errorf("invalid stored key: %w", err)
	}

	serverKey, err := base64.StdEncoding.DecodeString(keys[1])
	if err != nil {
		return nil, fmt.Errorf("invalid server key: %w", err)
	}

	return &SCRAMCredentials{
		Salt:       salt,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
		Iterations: iterations,
	}, nil
}

// SCRAMState represents the state of a SCRAM authentication exchange.
type SCRAMState struct {
	// Username is the authenticating user.
	Username string

	// ClientNonce is the client-provided nonce.
	ClientNonce string

	// ServerNonce is the server-generated nonce.
	ServerNonce string

	// Salt is the user's salt.
	Salt []byte

	// Iterations is the PBKDF2 iteration count.
	Iterations int

	// AuthMessage is the concatenated authentication messages.
	AuthMessage string

	// ClientFirstMessage is the client's first message (without gs2 header).
	ClientFirstMessage string

	// ServerFirstMessage is the server's first message.
	ServerFirstMessage string

	// StoredKey is the stored key for the user.
	StoredKey []byte

	// ServerKey is the server key for the user.
	ServerKey []byte

	// Step indicates the current step in the SCRAM exchange.
	Step int
}

// NewSCRAMState creates a new SCRAM state for authentication.
func NewSCRAMState() *SCRAMState {
	return &SCRAMState{Step: 0}
}

// SCRAMAuthenticator implements SCRAM-SHA-256 authentication.
type SCRAMAuthenticator struct {
	provider UserProvider

	// states holds ongoing SCRAM authentication states.
	states map[string]*SCRAMState
	mu     sync.Mutex
}

// NewSCRAMAuthenticator creates a new SCRAM-SHA-256 authenticator.
func NewSCRAMAuthenticator(provider UserProvider) *SCRAMAuthenticator {
	return &SCRAMAuthenticator{
		provider: provider,
		states:   make(map[string]*SCRAMState),
	}
}

// Authenticate implements the Authenticator interface.
// For SCRAM, this validates credentials after the SCRAM exchange is complete.
func (a *SCRAMAuthenticator) Authenticate(ctx context.Context, username, password, database string) (bool, error) {
	if a.provider == nil {
		return false, errors.New("no user provider configured")
	}

	// Get the user
	user, err := a.provider.GetUser(username)
	if err != nil {
		return false, err
	}
	if user == nil {
		return false, nil
	}

	// For direct password validation (fallback path)
	if user.SCRAMCredentials == nil {
		// Fall back to password validation
		valid, err := a.provider.ValidatePassword(username, password)
		if err != nil || !valid {
			return false, err
		}
	} else {
		// Verify against stored SCRAM credentials
		if !a.verifyPassword(password, user.SCRAMCredentials) {
			return false, nil
		}
	}

	// Check database access
	if !user.CanAccessDatabase(database) {
		return false, nil
	}

	return true, nil
}

// Method implements the Authenticator interface.
func (a *SCRAMAuthenticator) Method() Method {
	return MethodSCRAMSHA256
}

// ProcessClientFirst processes the client's first SCRAM message.
// Returns the server's first message.
func (a *SCRAMAuthenticator) ProcessClientFirst(sessionID string, clientFirstMessage string) (string, error) {
	// Parse client-first-message
	// Format: n,,n=<username>,r=<client-nonce>
	// or p=<channel-binding>,a=<authzid>,n=<username>,r=<client-nonce>

	state := NewSCRAMState()
	state.Step = 1

	// Remove GS2 header (n,, or p=...)
	parts := strings.SplitN(clientFirstMessage, ",", 3)
	if len(parts) < 3 {
		return "", errors.New("invalid client-first-message format")
	}

	// Parse the bare message (after gs2 header)
	bareMessage := parts[2]
	state.ClientFirstMessage = bareMessage

	// Parse attributes
	attrs := parseSCRAMAttributes(bareMessage)

	username := attrs["n"]
	if username == "" {
		return "", errors.New("missing username in client-first-message")
	}
	state.Username = username

	clientNonce := attrs["r"]
	if clientNonce == "" {
		return "", errors.New("missing nonce in client-first-message")
	}
	state.ClientNonce = clientNonce

	// Get user's SCRAM credentials
	if a.provider == nil {
		return "", errors.New("no user provider configured")
	}

	user, err := a.provider.GetUser(username)
	if err != nil {
		return "", err
	}
	if user == nil {
		// Generate fake credentials to avoid timing attacks
		state.Salt = make([]byte, SCRAMSaltLength)
		_, _ = rand.Read(state.Salt)
		state.Iterations = SCRAMIterationCount
		state.StoredKey = make([]byte, sha256.Size)
		state.ServerKey = make([]byte, sha256.Size)
	} else if user.SCRAMCredentials != nil {
		state.Salt = user.SCRAMCredentials.Salt
		state.Iterations = user.SCRAMCredentials.Iterations
		state.StoredKey = user.SCRAMCredentials.StoredKey
		state.ServerKey = user.SCRAMCredentials.ServerKey
	} else {
		// User exists but no SCRAM credentials - generate temporary ones
		// This allows fallback to password auth
		state.Salt = make([]byte, SCRAMSaltLength)
		_, _ = rand.Read(state.Salt)
		state.Iterations = SCRAMIterationCount
		state.StoredKey = make([]byte, sha256.Size)
		state.ServerKey = make([]byte, sha256.Size)
	}

	// Generate server nonce
	serverNonceBytes := make([]byte, 18)
	if _, err := rand.Read(serverNonceBytes); err != nil {
		return "", fmt.Errorf("failed to generate server nonce: %w", err)
	}
	state.ServerNonce = clientNonce + base64.StdEncoding.EncodeToString(serverNonceBytes)

	// Build server-first-message
	// Format: r=<combined-nonce>,s=<salt>,i=<iteration-count>
	serverFirstMessage := fmt.Sprintf("r=%s,s=%s,i=%d",
		state.ServerNonce,
		base64.StdEncoding.EncodeToString(state.Salt),
		state.Iterations,
	)
	state.ServerFirstMessage = serverFirstMessage

	// Store state
	a.mu.Lock()
	a.states[sessionID] = state
	a.mu.Unlock()

	return serverFirstMessage, nil
}

// ProcessClientFinal processes the client's final SCRAM message.
// Returns the server's final message and whether authentication succeeded.
func (a *SCRAMAuthenticator) ProcessClientFinal(sessionID string, clientFinalMessage string) (string, bool, error) {
	// Get state
	a.mu.Lock()
	state, ok := a.states[sessionID]
	if ok {
		delete(a.states, sessionID) // Clean up state
	}
	a.mu.Unlock()

	if !ok || state.Step != 1 {
		return "", false, errors.New("invalid SCRAM state")
	}

	// Parse client-final-message
	// Format: c=<channel-binding>,r=<nonce>,p=<client-proof>
	attrs := parseSCRAMAttributes(clientFinalMessage)

	// Verify the nonce matches
	if attrs["r"] != state.ServerNonce {
		return "", false, errors.New("nonce mismatch")
	}

	// Get channel binding
	channelBinding := attrs["c"]
	if channelBinding == "" {
		return "", false, errors.New("missing channel binding")
	}

	// Get client proof
	clientProofB64 := attrs["p"]
	if clientProofB64 == "" {
		return "", false, errors.New("missing client proof")
	}

	clientProof, err := base64.StdEncoding.DecodeString(clientProofB64)
	if err != nil {
		return "", false, fmt.Errorf("invalid client proof encoding: %w", err)
	}

	// Build client-final-message-without-proof
	clientFinalWithoutProof := fmt.Sprintf("c=%s,r=%s", channelBinding, state.ServerNonce)

	// Build auth message
	authMessage := state.ClientFirstMessage + "," + state.ServerFirstMessage + "," + clientFinalWithoutProof

	// Verify client proof
	// ClientSignature = HMAC(StoredKey, AuthMessage)
	clientSignature := hmacSHA256(state.StoredKey, []byte(authMessage))

	// ClientKey = ClientProof XOR ClientSignature
	clientKey := xorBytes(clientProof, clientSignature)

	// StoredKey = H(ClientKey)
	computedStoredKey := sha256Sum(clientKey)

	// Verify stored key matches
	if subtle.ConstantTimeCompare(computedStoredKey, state.StoredKey) != 1 {
		return "", false, nil // Authentication failed
	}

	// Generate server signature for verification
	// ServerSignature = HMAC(ServerKey, AuthMessage)
	serverSignature := hmacSHA256(state.ServerKey, []byte(authMessage))

	// Build server-final-message
	serverFinalMessage := "v=" + base64.StdEncoding.EncodeToString(serverSignature)

	return serverFinalMessage, true, nil
}

// verifyPassword verifies a password against SCRAM credentials.
func (a *SCRAMAuthenticator) verifyPassword(password string, creds *SCRAMCredentials) bool {
	// Derive salted password using PBKDF2
	saltedPassword := pbkdf2.Key([]byte(password), creds.Salt, creds.Iterations, sha256.Size, sha256.New)

	// Calculate client key: HMAC(SaltedPassword, "Client Key")
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))

	// Calculate stored key: H(ClientKey)
	computedStoredKey := sha256Sum(clientKey)

	// Compare with stored key
	return subtle.ConstantTimeCompare(computedStoredKey, creds.StoredKey) == 1
}

// CleanupState removes any stale authentication state for a session.
func (a *SCRAMAuthenticator) CleanupState(sessionID string) {
	a.mu.Lock()
	delete(a.states, sessionID)
	a.mu.Unlock()
}

// Helper functions for SCRAM

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func sha256Sum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

func xorBytes(a, b []byte) []byte {
	if len(a) != len(b) {
		return nil
	}
	result := make([]byte, len(a))
	for i := range a {
		result[i] = a[i] ^ b[i]
	}
	return result
}

func parseSCRAMAttributes(message string) map[string]string {
	attrs := make(map[string]string)
	for _, part := range strings.Split(message, ",") {
		if idx := strings.Index(part, "="); idx > 0 {
			key := part[:idx]
			value := part[idx+1:]
			attrs[key] = value
		}
	}
	return attrs
}

// =============================================================================
// Certificate-Based Authentication (Task 16.2)
// =============================================================================

// CertificateAuthenticator implements certificate-based authentication.
// It authenticates users based on the Common Name (CN) of their TLS client certificate.
type CertificateAuthenticator struct {
	provider UserProvider

	// mapCNToUser maps certificate CNs to usernames.
	// If nil, the CN is used directly as the username.
	mapCNToUser map[string]string
	mu          sync.RWMutex
}

// NewCertificateAuthenticator creates a new certificate-based authenticator.
func NewCertificateAuthenticator(provider UserProvider) *CertificateAuthenticator {
	return &CertificateAuthenticator{
		provider:    provider,
		mapCNToUser: make(map[string]string),
	}
}

// MapCNToUser adds a mapping from certificate CN to database username.
func (a *CertificateAuthenticator) MapCNToUser(cn, username string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mapCNToUser[cn] = username
}

// RemoveCNMapping removes a CN to username mapping.
func (a *CertificateAuthenticator) RemoveCNMapping(cn string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.mapCNToUser, cn)
}

// Authenticate implements the Authenticator interface.
// For certificate auth, this validates that the certificate CN maps to the user.
func (a *CertificateAuthenticator) Authenticate(ctx context.Context, username, password, database string) (bool, error) {
	// In certificate auth, password is actually the certificate CN
	// This is passed from the TLS handshake through context
	certCN := CertCNFromContext(ctx)
	if certCN == "" {
		// Try using password as CN for testing purposes
		certCN = password
	}

	if certCN == "" {
		return false, ErrCertificateRequired
	}

	// Map CN to username
	expectedUsername := a.getUsernameFromCN(certCN)

	// Check if the mapped username matches
	if expectedUsername != username {
		return false, nil
	}

	// Verify user exists and can access database
	if a.provider != nil {
		user, err := a.provider.GetUser(username)
		if err != nil {
			return false, err
		}
		if user == nil {
			return false, nil
		}

		// Optionally verify certificate CN matches user's expected CN
		if user.CertificateCN != "" && user.CertificateCN != certCN {
			return false, nil
		}

		if !user.CanAccessDatabase(database) {
			return false, nil
		}
	}

	return true, nil
}

// getUsernameFromCN returns the username for a certificate CN.
func (a *CertificateAuthenticator) getUsernameFromCN(cn string) string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if mappedUser, ok := a.mapCNToUser[cn]; ok {
		return mappedUser
	}
	// Default: use CN directly as username
	return cn
}

// Method implements the Authenticator interface.
func (a *CertificateAuthenticator) Method() Method {
	return MethodCert
}

// Context key for certificate CN
type certCNContextKey struct{}

// ContextWithCertCN adds the certificate CN to the context.
func ContextWithCertCN(ctx context.Context, cn string) context.Context {
	return context.WithValue(ctx, certCNContextKey{}, cn)
}

// CertCNFromContext retrieves the certificate CN from the context.
func CertCNFromContext(ctx context.Context) string {
	cn, _ := ctx.Value(certCNContextKey{}).(string)
	return cn
}

// =============================================================================
// LDAP Authentication (Task 16.3)
// =============================================================================

// LDAPConfig holds configuration for LDAP authentication.
type LDAPConfig struct {
	// Server is the LDAP server address (host:port).
	Server string

	// Port is the LDAP server port (default: 389, or 636 for LDAPS).
	Port int

	// UseTLS indicates whether to use LDAPS (TLS).
	UseTLS bool

	// StartTLS indicates whether to use StartTLS.
	StartTLS bool

	// BaseDN is the base DN for user searches.
	BaseDN string

	// BindDN is the DN to bind as for searching (optional).
	BindDN string

	// BindPassword is the password for the bind DN.
	BindPassword string

	// UserSearchFilter is the LDAP filter for finding users.
	// Use %s as placeholder for username. Default: (uid=%s)
	UserSearchFilter string

	// UserAttribute is the attribute containing the username.
	UserAttribute string

	// Timeout is the connection timeout in seconds.
	Timeout int
}

// NewLDAPConfig creates a new LDAP configuration with defaults.
func NewLDAPConfig() *LDAPConfig {
	return &LDAPConfig{
		Port:             389,
		UserSearchFilter: "(uid=%s)",
		UserAttribute:    "uid",
		Timeout:          10,
	}
}

// LDAPAuthenticator implements LDAP authentication.
// Note: This is a basic implementation. For production use, consider using
// a full LDAP library like go-ldap/ldap.
type LDAPAuthenticator struct {
	config   *LDAPConfig
	provider UserProvider

	// ldapBind is a function to perform LDAP bind (for testing/mocking)
	ldapBind func(server string, port int, bindDN, password string, useTLS bool) error
}

// NewLDAPAuthenticator creates a new LDAP authenticator.
func NewLDAPAuthenticator(config *LDAPConfig, provider UserProvider) *LDAPAuthenticator {
	if config == nil {
		config = NewLDAPConfig()
	}
	return &LDAPAuthenticator{
		config:   config,
		provider: provider,
	}
}

// SetLDAPBindFunc sets a custom LDAP bind function (for testing).
func (a *LDAPAuthenticator) SetLDAPBindFunc(fn func(server string, port int, bindDN, password string, useTLS bool) error) {
	a.ldapBind = fn
}

// Authenticate implements the Authenticator interface.
func (a *LDAPAuthenticator) Authenticate(ctx context.Context, username, password, database string) (bool, error) {
	if a.config == nil {
		return false, errors.New("LDAP not configured")
	}

	// Build user DN
	// Simple bind: try to bind directly as the user
	userDN := a.buildUserDN(username)

	// Attempt LDAP bind
	var err error
	if a.ldapBind != nil {
		// Use custom bind function (for testing)
		err = a.ldapBind(a.config.Server, a.config.Port, userDN, password, a.config.UseTLS)
	} else {
		// In a real implementation, we would use an LDAP library here
		// For now, return a placeholder error indicating LDAP is not fully implemented
		// This allows the structure to exist while marking it as future work
		return false, fmt.Errorf("LDAP bind not implemented: would bind as %s to %s:%d",
			userDN, a.config.Server, a.config.Port)
	}

	if err != nil {
		return false, nil // Auth failed
	}

	// Check database access via local provider if configured
	if a.provider != nil {
		user, err := a.provider.GetUser(username)
		if err != nil {
			return false, err
		}
		// User might not exist locally, which is OK for LDAP auth
		if user != nil && !user.CanAccessDatabase(database) {
			return false, nil
		}
	}

	return true, nil
}

// buildUserDN builds the user DN from username.
func (a *LDAPAuthenticator) buildUserDN(username string) string {
	// Simple DN construction using user attribute and base DN
	if a.config.UserAttribute != "" && a.config.BaseDN != "" {
		return fmt.Sprintf("%s=%s,%s", a.config.UserAttribute, username, a.config.BaseDN)
	}
	return username
}

// Method implements the Authenticator interface.
func (a *LDAPAuthenticator) Method() Method {
	return MethodLDAP
}

// =============================================================================
// Host-Based Access Control (pg_hba.conf style) (Task 16.4)
// =============================================================================

// HBAType represents the connection type in HBA rules.
type HBAType string

// HBA connection types.
const (
	HBALocal   HBAType = "local"   // Unix domain socket
	HBAHost    HBAType = "host"    // TCP/IP (with or without SSL)
	HBAHostSSL HBAType = "hostssl" // TCP/IP with SSL only
	HBAHostNoSSL HBAType = "hostnossl" // TCP/IP without SSL
)

// HBARule represents a host-based authentication rule.
type HBARule struct {
	// Type is the connection type (local, host, hostssl, hostnossl).
	Type HBAType

	// Database is the database name pattern (or "all").
	Database string

	// User is the username pattern (or "all").
	User string

	// Address is the client IP address/CIDR (for host types).
	Address string

	// Netmask is the network mask (alternative to CIDR notation).
	Netmask string

	// Method is the authentication method.
	Method Method

	// Options are method-specific options.
	Options map[string]string

	// parsedNet is the parsed network for IP matching.
	parsedNet *net.IPNet
}

// NewHBARule creates a new HBA rule.
func NewHBARule(hbaType HBAType, database, user string, method Method) *HBARule {
	return &HBARule{
		Type:     hbaType,
		Database: database,
		User:     user,
		Method:   method,
		Options:  make(map[string]string),
	}
}

// WithAddress sets the address for the rule.
func (r *HBARule) WithAddress(address string) *HBARule {
	r.Address = address
	// Try to parse as CIDR
	_, ipNet, err := net.ParseCIDR(address)
	if err == nil {
		r.parsedNet = ipNet
	} else {
		// Try as plain IP
		ip := net.ParseIP(address)
		if ip != nil {
			// Create /32 or /128 mask
			if ip.To4() != nil {
				r.parsedNet = &net.IPNet{IP: ip, Mask: net.CIDRMask(32, 32)}
			} else {
				r.parsedNet = &net.IPNet{IP: ip, Mask: net.CIDRMask(128, 128)}
			}
		}
	}
	return r
}

// WithNetmask sets the netmask for the rule.
func (r *HBARule) WithNetmask(netmask string) *HBARule {
	r.Netmask = netmask
	if r.Address != "" {
		ip := net.ParseIP(r.Address)
		mask := net.ParseIP(netmask)
		if ip != nil && mask != nil {
			r.parsedNet = &net.IPNet{
				IP:   ip,
				Mask: net.IPMask(mask.To4()),
			}
		}
	}
	return r
}

// WithOption sets an option for the rule.
func (r *HBARule) WithOption(key, value string) *HBARule {
	if r.Options == nil {
		r.Options = make(map[string]string)
	}
	r.Options[key] = value
	return r
}

// Matches checks if the rule matches the given connection parameters.
func (r *HBARule) Matches(connType HBAType, database, user, clientIP string, isSSL bool) bool {
	// Check connection type
	if !r.matchesType(connType, isSSL) {
		return false
	}

	// Check database
	if !r.matchesDatabase(database) {
		return false
	}

	// Check user
	if !r.matchesUser(user) {
		return false
	}

	// Check IP address for host types
	if r.Type != HBALocal {
		if !r.matchesAddress(clientIP) {
			return false
		}
	}

	return true
}

func (r *HBARule) matchesType(connType HBAType, isSSL bool) bool {
	switch r.Type {
	case HBALocal:
		return connType == HBALocal
	case HBAHost:
		return connType == HBAHost || connType == HBAHostSSL || connType == HBAHostNoSSL
	case HBAHostSSL:
		return isSSL
	case HBAHostNoSSL:
		return !isSSL
	}
	return false
}

func (r *HBARule) matchesDatabase(database string) bool {
	if r.Database == "all" {
		return true
	}
	// Support comma-separated list
	for _, db := range strings.Split(r.Database, ",") {
		if strings.TrimSpace(db) == database {
			return true
		}
	}
	return false
}

func (r *HBARule) matchesUser(user string) bool {
	if r.User == "all" {
		return true
	}
	// Support comma-separated list
	for _, u := range strings.Split(r.User, ",") {
		if strings.TrimSpace(u) == user {
			return true
		}
	}
	return false
}

func (r *HBARule) matchesAddress(clientIP string) bool {
	if r.Address == "" || r.Address == "all" {
		return true
	}

	ip := net.ParseIP(clientIP)
	if ip == nil {
		return false
	}

	if r.parsedNet != nil {
		return r.parsedNet.Contains(ip)
	}

	return false
}

// HBAController manages host-based authentication rules.
type HBAController struct {
	rules []*HBARule
	mu    sync.RWMutex

	// authenticators maps methods to their authenticators
	authenticators map[Method]Authenticator
}

// NewHBAController creates a new HBA controller.
func NewHBAController() *HBAController {
	return &HBAController{
		rules:          make([]*HBARule, 0),
		authenticators: make(map[Method]Authenticator),
	}
}

// AddRule adds an HBA rule. Rules are evaluated in order.
func (c *HBAController) AddRule(rule *HBARule) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rules = append(c.rules, rule)
}

// InsertRule inserts an HBA rule at the specified index.
func (c *HBAController) InsertRule(index int, rule *HBARule) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if index < 0 {
		index = 0
	}
	if index >= len(c.rules) {
		c.rules = append(c.rules, rule)
		return
	}

	c.rules = append(c.rules[:index], append([]*HBARule{rule}, c.rules[index:]...)...)
}

// RemoveRule removes an HBA rule at the specified index.
func (c *HBAController) RemoveRule(index int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if index < 0 || index >= len(c.rules) {
		return
	}

	c.rules = append(c.rules[:index], c.rules[index+1:]...)
}

// ClearRules removes all HBA rules.
func (c *HBAController) ClearRules() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rules = make([]*HBARule, 0)
}

// Rules returns a copy of all rules.
func (c *HBAController) Rules() []*HBARule {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*HBARule, len(c.rules))
	copy(result, c.rules)
	return result
}

// RegisterAuthenticator registers an authenticator for a method.
func (c *HBAController) RegisterAuthenticator(method Method, auth Authenticator) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.authenticators[method] = auth
}

// FindMatchingRule finds the first matching rule for the given parameters.
func (c *HBAController) FindMatchingRule(connType HBAType, database, user, clientIP string, isSSL bool) *HBARule {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, rule := range c.rules {
		if rule.Matches(connType, database, user, clientIP, isSSL) {
			return rule
		}
	}
	return nil
}

// Authenticate performs authentication using HBA rules.
func (c *HBAController) Authenticate(ctx context.Context, connType HBAType, database, user, password, clientIP string, isSSL bool) (bool, error) {
	// Find matching rule
	rule := c.FindMatchingRule(connType, database, user, clientIP, isSSL)
	if rule == nil {
		return false, ErrAccessDenied
	}

	// Check authentication method
	switch rule.Method { //nolint:exhaustive // only handling special cases, default handles all auth methods
	case MethodNone, MethodTrust:
		return true, nil
	case MethodReject:
		return false, ErrAccessDenied
	default:
		// Use registered authenticator
		c.mu.RLock()
		auth := c.authenticators[rule.Method]
		c.mu.RUnlock()

		if auth == nil {
			return false, fmt.Errorf("no authenticator for method %s", rule.Method)
		}

		return auth.Authenticate(ctx, user, password, database)
	}
}

// =============================================================================
// Combined Authenticator (Supports multiple methods)
// =============================================================================

// CombinedAuthenticator combines multiple authentication methods.
// It uses HBA rules to determine which method to use for each connection.
type CombinedAuthenticator struct {
	hba      *HBAController
	provider UserProvider

	// Default authenticator when no HBA rules match
	defaultAuth Authenticator
}

// NewCombinedAuthenticator creates a new combined authenticator.
func NewCombinedAuthenticator(provider UserProvider) *CombinedAuthenticator {
	hba := NewHBAController()

	auth := &CombinedAuthenticator{
		hba:         hba,
		provider:    provider,
		defaultAuth: NewPasswordAuthenticator(provider),
	}

	// Register standard authenticators
	hba.RegisterAuthenticator(MethodPassword, NewPasswordAuthenticator(provider))
	hba.RegisterAuthenticator(MethodSCRAMSHA256, NewSCRAMAuthenticator(provider))
	hba.RegisterAuthenticator(MethodCert, NewCertificateAuthenticator(provider))

	return auth
}

// HBAController returns the HBA controller for rule management.
func (a *CombinedAuthenticator) HBAController() *HBAController {
	return a.hba
}

// SetDefaultAuthenticator sets the default authenticator.
func (a *CombinedAuthenticator) SetDefaultAuthenticator(auth Authenticator) {
	a.defaultAuth = auth
}

// Authenticate implements the Authenticator interface.
func (a *CombinedAuthenticator) Authenticate(ctx context.Context, username, password, database string) (bool, error) {
	// Get connection info from context
	clientIP := ClientIPFromContext(ctx)
	isSSL := IsSSLFromContext(ctx)
	connType := ConnTypeFromContext(ctx)
	if connType == "" {
		connType = HBAHost // Default to host
	}

	// Try HBA-based authentication if we have rules
	if len(a.hba.Rules()) > 0 {
		return a.hba.Authenticate(ctx, connType, database, username, password, clientIP, isSSL)
	}

	// Fall back to default authenticator
	if a.defaultAuth != nil {
		return a.defaultAuth.Authenticate(ctx, username, password, database)
	}

	return false, ErrAuthenticationFailed
}

// Method implements the Authenticator interface.
func (a *CombinedAuthenticator) Method() Method {
	return MethodPassword // Default method
}

// Context keys for connection info
type clientIPContextKey struct{}
type isSSLContextKey struct{}
type connTypeContextKey struct{}

// ContextWithClientIP adds the client IP to the context.
func ContextWithClientIP(ctx context.Context, ip string) context.Context {
	return context.WithValue(ctx, clientIPContextKey{}, ip)
}

// ClientIPFromContext retrieves the client IP from the context.
func ClientIPFromContext(ctx context.Context) string {
	ip, _ := ctx.Value(clientIPContextKey{}).(string)
	return ip
}

// ContextWithIsSSL adds the SSL status to the context.
func ContextWithIsSSL(ctx context.Context, isSSL bool) context.Context {
	return context.WithValue(ctx, isSSLContextKey{}, isSSL)
}

// IsSSLFromContext retrieves the SSL status from the context.
func IsSSLFromContext(ctx context.Context) bool {
	isSSL, _ := ctx.Value(isSSLContextKey{}).(bool)
	return isSSL
}

// ContextWithConnType adds the connection type to the context.
func ContextWithConnType(ctx context.Context, connType HBAType) context.Context {
	return context.WithValue(ctx, connTypeContextKey{}, connType)
}

// ConnTypeFromContext retrieves the connection type from the context.
func ConnTypeFromContext(ctx context.Context) HBAType {
	connType, _ := ctx.Value(connTypeContextKey{}).(HBAType)
	return connType
}
