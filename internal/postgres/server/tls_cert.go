// Package server provides a PostgreSQL wire protocol server for dukdb-go.
// This file contains certificate generation and client certificate authentication.
package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"time"
)

// Duration is an alias for time.Duration for TLS configuration.
type Duration = time.Duration

// Certificate generation constants.
const (
	// serialNumberBitLength is the bit length for certificate serial numbers.
	serialNumberBitLength = 128

	// defaultCertValidityDays is the default validity period in days.
	defaultCertValidityDays = 365

	// hoursPerDay is used for validity calculation.
	hoursPerDay = 24
)

// generateCertificate creates a self-signed certificate for development use.
func (c *TLSConfig) generateCertificate() (tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.generated != nil {
		return *c.generated, nil
	}

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate private key: %w", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), serialNumberBitLength))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate serial number: %w", err)
	}

	template := c.buildCertTemplate(serialNumber)
	c.addHostsToCertTemplate(&template)

	cert, err := createAndParseCert(&template, privateKey)
	if err != nil {
		return tls.Certificate{}, err
	}

	c.generated = &cert

	return cert, nil
}

// buildCertTemplate creates the certificate template with configured options.
func (c *TLSConfig) buildCertTemplate(serialNumber *big.Int) x509.Certificate {
	validity := c.AutoGenerateValidity
	if validity == 0 {
		validity = defaultCertValidityDays * hoursPerDay * time.Hour
	}

	org := c.AutoGenerateOrganization
	if org == "" {
		org = "dukdb Development"
	}

	now := time.Now()

	return x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{org},
			CommonName:   "dukdb Development Server",
		},
		NotBefore:             now,
		NotAfter:              now.Add(validity),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}
}

// addHostsToCertTemplate adds DNS names and IP addresses to the certificate template.
func (c *TLSConfig) addHostsToCertTemplate(template *x509.Certificate) {
	hosts := c.AutoGenerateHosts
	if len(hosts) == 0 {
		hosts = []string{"localhost", "127.0.0.1", "::1"}
	}

	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, host)
		}
	}
}

// createAndParseCert creates the certificate DER and parses it into a tls.Certificate.
func createAndParseCert(
	template *x509.Certificate,
	privateKey *ecdsa.PrivateKey,
) (tls.Certificate, error) {
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	privKeyBytes, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to marshal private key: %w", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privKeyBytes})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to parse generated certificate: %w", err)
	}

	return cert, nil
}

// GetClientCN extracts the Common Name from a client certificate.
// Returns an empty string if no client certificate is present.
func GetClientCN(state *tls.ConnectionState) string {
	if state == nil || len(state.PeerCertificates) == 0 {
		return ""
	}

	return state.PeerCertificates[0].Subject.CommonName
}

// GetClientCertificateSubject extracts the full subject from a client certificate.
// Returns nil if no client certificate is present.
func GetClientCertificateSubject(state *tls.ConnectionState) *pkix.Name {
	if state == nil || len(state.PeerCertificates) == 0 {
		return nil
	}

	return &state.PeerCertificates[0].Subject
}

// IsClientCertVerified returns true if the client presented a verified certificate.
func IsClientCertVerified(state *tls.ConnectionState) bool {
	if state == nil {
		return false
	}

	return len(state.VerifiedChains) > 0
}

// CertificateAuthenticator implements client certificate-based authentication.
type CertificateAuthenticator struct {
	// AllowedCNs is a list of allowed Common Names.
	// If empty, any valid certificate is accepted.
	AllowedCNs []string

	// RequireValidCert requires a verified certificate chain.
	RequireValidCert bool

	// UserMapper maps certificate CN to database username.
	// If nil, the CN is used directly as the username.
	UserMapper func(cn string) string
}

// NewCertificateAuthenticator creates a new certificate-based authenticator.
func NewCertificateAuthenticator() *CertificateAuthenticator {
	return &CertificateAuthenticator{
		RequireValidCert: true,
	}
}

// ValidateCertificate validates a client certificate from a TLS connection.
// Returns the username extracted from the certificate CN, or an error.
func (a *CertificateAuthenticator) ValidateCertificate(state *tls.ConnectionState) (string, error) {
	if state == nil {
		return "", errors.New("no TLS connection state")
	}

	if a.RequireValidCert && !IsClientCertVerified(state) {
		return "", errors.New("client certificate not verified")
	}

	cn := GetClientCN(state)
	if cn == "" {
		return "", errors.New("no client certificate CN")
	}

	if err := a.checkAllowedCN(cn); err != nil {
		return "", err
	}

	username := cn
	if a.UserMapper != nil {
		username = a.UserMapper(cn)
	}

	return username, nil
}

// checkAllowedCN checks if the CN is in the allowed list.
func (a *CertificateAuthenticator) checkAllowedCN(cn string) error {
	if len(a.AllowedCNs) == 0 {
		return nil
	}

	for _, allowedCN := range a.AllowedCNs {
		if cn == allowedCN {
			return nil
		}
	}

	return fmt.Errorf("certificate CN %q not in allowed list", cn)
}
