// Package server provides a PostgreSQL wire protocol server for dukdb-go.
// This file contains TLS configuration and certificate management utilities.
package server

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"sync"
)

// TLS configuration errors.
var (
	// ErrNoCertificate is returned when no certificate is available.
	ErrNoCertificate = errors.New("no TLS certificate available")

	// ErrInvalidCertificate is returned when a certificate is invalid.
	ErrInvalidCertificate = errors.New("invalid TLS certificate")

	// ErrCertificateRevoked is returned when a certificate has been revoked.
	ErrCertificateRevoked = errors.New("certificate has been revoked")

	// ErrCRLCheckFailed is returned when CRL checking fails.
	ErrCRLCheckFailed = errors.New("CRL check failed")
)

// TLSVersion represents a TLS protocol version.
type TLSVersion uint16

// Supported TLS versions.
const (
	TLSVersion10 TLSVersion = tls.VersionTLS10
	TLSVersion11 TLSVersion = tls.VersionTLS11
	TLSVersion12 TLSVersion = tls.VersionTLS12
	TLSVersion13 TLSVersion = tls.VersionTLS13
)

// String returns a human-readable string for the TLS version.
func (v TLSVersion) String() string {
	switch v {
	case TLSVersion10:
		return "TLS 1.0"
	case TLSVersion11:
		return "TLS 1.1"
	case TLSVersion12:
		return "TLS 1.2"
	case TLSVersion13:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("Unknown TLS version (0x%04x)", uint16(v))
	}
}

// ClientCertMode specifies how to handle client certificates.
type ClientCertMode int

// Client certificate modes.
const (
	// ClientCertNone does not request client certificates.
	ClientCertNone ClientCertMode = iota

	// ClientCertRequest requests a client certificate but does not require one.
	ClientCertRequest

	// ClientCertRequire requires a client certificate.
	ClientCertRequire

	// ClientCertVerifyCA verifies the client certificate against the CA.
	ClientCertVerifyCA

	// ClientCertVerifyFull verifies the client certificate and hostname.
	ClientCertVerifyFull
)

// TLSConfig holds comprehensive TLS configuration options.
// See tls_config.go for detailed documentation and examples.
type TLSConfig struct {
	// CertFile is the path to the server certificate file.
	CertFile string

	// KeyFile is the path to the server private key file.
	KeyFile string

	// ClientCAFile is the path to the CA certificate file for verifying client certificates.
	ClientCAFile string

	// CRLFile is the path to the Certificate Revocation List (CRL) file.
	CRLFile string

	// AutoGenerate enables automatic generation of self-signed certificates.
	AutoGenerate bool

	// AutoGenerateHosts specifies hosts for auto-generated certificates.
	AutoGenerateHosts []string

	// AutoGenerateOrganization specifies the organization name for auto-generated certificates.
	AutoGenerateOrganization string

	// AutoGenerateValidity specifies how long auto-generated certificates are valid.
	AutoGenerateValidity Duration

	// ClientCertMode specifies how to handle client certificates.
	ClientCertMode ClientCertMode

	// MinVersion is the minimum TLS version to accept (default: TLS 1.2).
	MinVersion TLSVersion

	// MaxVersion is the maximum TLS version to accept (default: TLS 1.3).
	MaxVersion TLSVersion

	// PreferServerCipherSuites indicates server cipher suite preference.
	PreferServerCipherSuites bool

	// CipherSuites is the list of supported cipher suites.
	CipherSuites []uint16

	// InsecureSkipVerify disables certificate verification (testing only).
	InsecureSkipVerify bool

	// generated holds the auto-generated certificate.
	generated *tls.Certificate

	// crl holds the parsed Certificate Revocation List.
	crl *x509.RevocationList

	// mu protects concurrent access.
	mu sync.RWMutex
}

// NewTLSConfig creates a new TLSConfig with secure defaults.
func NewTLSConfig() *TLSConfig {
	return &TLSConfig{
		MinVersion:               TLSVersion12,
		MaxVersion:               TLSVersion13,
		ClientCertMode:           ClientCertNone,
		PreferServerCipherSuites: true,
	}
}

// Build creates a tls.Config from this configuration.
func (c *TLSConfig) Build() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: uint16(c.MinVersion),
		MaxVersion: uint16(c.MaxVersion),
	}

	cert, err := c.loadOrGenerateCert()
	if err != nil {
		return nil, err
	}

	tlsConfig.Certificates = []tls.Certificate{cert}

	if err := c.configureClientAuth(tlsConfig); err != nil {
		return nil, err
	}

	if len(c.CipherSuites) > 0 {
		tlsConfig.CipherSuites = c.CipherSuites
	}

	if c.CRLFile != "" {
		if err := c.loadCRL(); err != nil {
			return nil, fmt.Errorf("failed to load CRL: %w", err)
		}

		tlsConfig.VerifyPeerCertificate = c.verifyCertificateWithCRL
	}

	return tlsConfig, nil
}

// loadOrGenerateCert loads certificate from files or generates one.
func (c *TLSConfig) loadOrGenerateCert() (tls.Certificate, error) {
	switch {
	case c.AutoGenerate:
		cert, err := c.generateCertificate()
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("failed to generate certificate: %w", err)
		}

		return cert, nil
	case c.CertFile != "" && c.KeyFile != "":
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("failed to load certificate: %w", err)
		}

		return cert, nil
	default:
		return tls.Certificate{}, ErrNoCertificate
	}
}

// configureClientAuth sets up client certificate authentication.
func (c *TLSConfig) configureClientAuth(tlsConfig *tls.Config) error {
	switch c.ClientCertMode {
	case ClientCertNone:
		tlsConfig.ClientAuth = tls.NoClientCert
	case ClientCertRequest:
		tlsConfig.ClientAuth = tls.RequestClientCert
	case ClientCertRequire:
		tlsConfig.ClientAuth = tls.RequireAnyClientCert
	case ClientCertVerifyCA:
		tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven

		if err := c.loadClientCA(tlsConfig); err != nil {
			return err
		}
	case ClientCertVerifyFull:
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert

		if err := c.loadClientCA(tlsConfig); err != nil {
			return err
		}
	}

	return nil
}

// loadClientCA loads the client CA certificate pool.
func (c *TLSConfig) loadClientCA(tlsConfig *tls.Config) error {
	if c.ClientCAFile == "" {
		return nil
	}

	caCert, err := os.ReadFile(c.ClientCAFile)
	if err != nil {
		return fmt.Errorf("failed to read client CA file: %w", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCert) {
		return errors.New("failed to parse client CA certificate")
	}

	tlsConfig.ClientCAs = caPool

	return nil
}

// loadCRL loads and parses the Certificate Revocation List.
func (c *TLSConfig) loadCRL() error {
	crlData, err := os.ReadFile(c.CRLFile)
	if err != nil {
		return fmt.Errorf("failed to read CRL file: %w", err)
	}

	block, _ := pem.Decode(crlData)

	var crlDER []byte
	if block != nil && block.Type == "X509 CRL" {
		crlDER = block.Bytes
	} else {
		crlDER = crlData
	}

	crl, err := x509.ParseRevocationList(crlDER)
	if err != nil {
		return fmt.Errorf("failed to parse CRL: %w", err)
	}

	c.mu.Lock()
	c.crl = crl
	c.mu.Unlock()

	return nil
}

// verifyCertificateWithCRL verifies a certificate against the CRL.
func (c *TLSConfig) verifyCertificateWithCRL(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
	c.mu.RLock()
	crl := c.crl
	c.mu.RUnlock()

	if crl == nil {
		return nil
	}

	for _, chain := range verifiedChains {
		for _, cert := range chain {
			for _, revokedCert := range crl.RevokedCertificateEntries {
				if cert.SerialNumber.Cmp(revokedCert.SerialNumber) == 0 {
					return ErrCertificateRevoked
				}
			}
		}
	}

	return nil
}

// DefaultSecureCipherSuites returns a list of secure cipher suites for TLS 1.2.
func DefaultSecureCipherSuites() []uint16 {
	return []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
	}
}
