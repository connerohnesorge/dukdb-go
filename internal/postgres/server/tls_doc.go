// Package server provides a PostgreSQL wire protocol server for dukdb-go.
//
// # TLS Configuration
//
// The server supports comprehensive TLS configuration for secure connections.
// TLS is configured using the TLSConfig type, which provides a builder pattern
// for creating crypto/tls.Config instances.
//
// # Quick Start
//
// For development and testing, use auto-generated self-signed certificates:
//
//	config := server.NewConfig()
//	tlsConfig := server.NewTLSConfig()
//	tlsConfig.AutoGenerate = true
//	config.TLSConfig, _ = tlsConfig.Build()
//
// For production, use real certificates:
//
//	config := server.NewConfig()
//	tlsConfig := server.NewTLSConfig()
//	tlsConfig.CertFile = "/path/to/server.crt"
//	tlsConfig.KeyFile = "/path/to/server.key"
//	config.TLSConfig, _ = tlsConfig.Build()
//
// # Auto-Generated Certificates
//
// The AutoGenerate option creates self-signed ECDSA P-256 certificates
// on-the-fly. This is convenient for development but should NOT be used
// in production.
//
// Configuration options for auto-generated certificates:
//   - AutoGenerateHosts: DNS names and IP addresses for the certificate
//     (default: ["localhost", "127.0.0.1", "::1"])
//   - AutoGenerateOrganization: Organization name in the certificate
//     (default: "dukdb Development")
//   - AutoGenerateValidity: Certificate validity period
//     (default: 365 days)
//
// Generated certificates are cached in memory and reused for the lifetime
// of the TLSConfig instance.
//
// # Client Certificate Authentication
//
// The server supports client certificate authentication, compatible with
// PostgreSQL's sslmode=verify-full connections.
//
// Client certificate modes:
//   - ClientCertNone: Do not request client certificates (default)
//   - ClientCertRequest: Request but do not require client certificates
//   - ClientCertRequire: Require a client certificate (any certificate)
//   - ClientCertVerifyCA: Verify the client certificate against the CA
//   - ClientCertVerifyFull: Require and verify client certificates (sslmode=verify-full)
//
// Example with client certificate verification:
//
//	tlsConfig := server.NewTLSConfig()
//	tlsConfig.CertFile = "/path/to/server.crt"
//	tlsConfig.KeyFile = "/path/to/server.key"
//	tlsConfig.ClientCAFile = "/path/to/ca.crt"
//	tlsConfig.ClientCertMode = server.ClientCertVerifyFull
//
// Extract client identity from the certificate:
//
//	// In your authentication handler:
//	tlsState := conn.TLSConnectionState()
//	username := server.GetClientCN(tlsState)
//
// For more control, use CertificateAuthenticator:
//
//	auth := server.NewCertificateAuthenticator()
//	auth.AllowedCNs = []string{"allowed-user-1", "allowed-user-2"}
//	auth.UserMapper = func(cn string) string {
//	    return "db_" + cn  // Map certificate CN to database username
//	}
//	username, err := auth.ValidateCertificate(tlsState)
//
// # Certificate Revocation Checking
//
// The server supports Certificate Revocation List (CRL) checking.
// When a CRL file is specified, all client certificates are checked
// against the revocation list.
//
//	tlsConfig := server.NewTLSConfig()
//	tlsConfig.CertFile = "/path/to/server.crt"
//	tlsConfig.KeyFile = "/path/to/server.key"
//	tlsConfig.ClientCertMode = server.ClientCertVerifyFull
//	tlsConfig.ClientCAFile = "/path/to/ca.crt"
//	tlsConfig.CRLFile = "/path/to/crl.pem"
//
// The CRL file can be in PEM or DER format.
//
// Note: OCSP (Online Certificate Status Protocol) is noted as future work.
// The current implementation only supports CRL-based revocation checking.
//
// # TLS Version Control
//
// By default, the server accepts TLS 1.2 and TLS 1.3 connections.
// You can configure the minimum and maximum TLS versions:
//
//	tlsConfig := server.NewTLSConfig()
//	tlsConfig.MinVersion = server.TLSVersion12  // Default
//	tlsConfig.MaxVersion = server.TLSVersion13  // Default
//
// Supported versions:
//   - TLSVersion10: TLS 1.0 (not recommended)
//   - TLSVersion11: TLS 1.1 (not recommended)
//   - TLSVersion12: TLS 1.2 (default minimum)
//   - TLSVersion13: TLS 1.3 (default maximum)
//
// For security, TLS 1.2 should be the minimum version used in production.
//
// # Cipher Suites
//
// For TLS 1.2, you can configure the allowed cipher suites:
//
//	tlsConfig := server.NewTLSConfig()
//	tlsConfig.CipherSuites = server.DefaultSecureCipherSuites()
//
// The DefaultSecureCipherSuites() returns a list of secure cipher suites:
//   - TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
//   - TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
//   - TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
//   - TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
//   - TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256
//   - TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
//
// TLS 1.3 cipher suites are managed automatically by the Go TLS library.
//
// # Complete Example
//
// Production configuration with all security features:
//
//	config := server.NewConfig()
//	config.Host = "0.0.0.0"
//	config.Port = 5432
//	config.RequireAuth = true
//
//	tlsConfig := server.NewTLSConfig()
//	tlsConfig.CertFile = "/etc/ssl/server.crt"
//	tlsConfig.KeyFile = "/etc/ssl/server.key"
//	tlsConfig.ClientCAFile = "/etc/ssl/ca.crt"
//	tlsConfig.CRLFile = "/etc/ssl/crl.pem"
//	tlsConfig.ClientCertMode = server.ClientCertVerifyFull
//	tlsConfig.MinVersion = server.TLSVersion12
//	tlsConfig.MaxVersion = server.TLSVersion13
//	tlsConfig.CipherSuites = server.DefaultSecureCipherSuites()
//
//	var err error
//	config.TLSConfig, err = tlsConfig.Build()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	srv, _ := server.NewServer(config)
//	srv.ListenAndServe()
package server
