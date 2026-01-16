package server

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTLSVersion_String(t *testing.T) {
	tests := []struct {
		version  TLSVersion
		expected string
	}{
		{TLSVersion10, "TLS 1.0"},
		{TLSVersion11, "TLS 1.1"},
		{TLSVersion12, "TLS 1.2"},
		{TLSVersion13, "TLS 1.3"},
		{TLSVersion(0x0000), "Unknown TLS version (0x0000)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.version.String())
		})
	}
}

func TestNewTLSConfig_Defaults(t *testing.T) {
	config := NewTLSConfig()

	assert.Equal(t, TLSVersion12, config.MinVersion)
	assert.Equal(t, TLSVersion13, config.MaxVersion)
	assert.Equal(t, ClientCertNone, config.ClientCertMode)
	assert.True(t, config.PreferServerCipherSuites)
}

func TestTLSConfig_AutoGenerate(t *testing.T) {
	config := NewTLSConfig()
	config.AutoGenerate = true

	tlsConfig, err := config.Build()
	require.NoError(t, err)
	require.NotNil(t, tlsConfig)

	assert.Equal(t, uint16(tls.VersionTLS12), tlsConfig.MinVersion)
	assert.Equal(t, uint16(tls.VersionTLS13), tlsConfig.MaxVersion)
	assert.Len(t, tlsConfig.Certificates, 1)
}

func TestTLSConfig_AutoGenerate_CustomHosts(t *testing.T) {
	config := NewTLSConfig()
	config.AutoGenerate = true
	config.AutoGenerateHosts = []string{"myserver.local", "192.168.1.100"}
	config.AutoGenerateOrganization = "Test Org"
	config.AutoGenerateValidity = 30 * 24 * time.Hour

	tlsConfig, err := config.Build()
	require.NoError(t, err)
	require.NotNil(t, tlsConfig)

	// Verify the certificate contains our hosts
	cert := tlsConfig.Certificates[0]
	parsedCert, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)

	assert.Contains(t, parsedCert.DNSNames, "myserver.local")
	assert.Equal(t, "Test Org", parsedCert.Subject.Organization[0])
}

func TestTLSConfig_AutoGenerate_CachedCertificate(t *testing.T) {
	config := NewTLSConfig()
	config.AutoGenerate = true

	// Generate first time
	tlsConfig1, err := config.Build()
	require.NoError(t, err)

	// Generate second time - should return cached
	tlsConfig2, err := config.Build()
	require.NoError(t, err)

	// Both should have the same certificate
	assert.Equal(t, tlsConfig1.Certificates[0].Certificate, tlsConfig2.Certificates[0].Certificate)
}

func TestTLSConfig_NoCertificate_Error(t *testing.T) {
	config := NewTLSConfig()
	// Don't set AutoGenerate or CertFile/KeyFile

	_, err := config.Build()
	assert.ErrorIs(t, err, ErrNoCertificate)
}

func TestTLSConfig_InvalidCertFile_Error(t *testing.T) {
	config := NewTLSConfig()
	config.CertFile = "/nonexistent/cert.pem"
	config.KeyFile = "/nonexistent/key.pem"

	_, err := config.Build()
	assert.Error(t, err)
}

func TestTLSConfig_ClientCertModes(t *testing.T) {
	tests := []struct {
		mode     ClientCertMode
		expected tls.ClientAuthType
	}{
		{ClientCertNone, tls.NoClientCert},
		{ClientCertRequest, tls.RequestClientCert},
		{ClientCertRequire, tls.RequireAnyClientCert},
	}

	for _, tt := range tests {
		t.Run(tt.expected.String(), func(t *testing.T) {
			config := NewTLSConfig()
			config.AutoGenerate = true
			config.ClientCertMode = tt.mode

			tlsConfig, err := config.Build()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, tlsConfig.ClientAuth)
		})
	}
}

func TestTLSConfig_CustomCipherSuites(t *testing.T) {
	config := NewTLSConfig()
	config.AutoGenerate = true
	config.CipherSuites = []uint16{
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	}

	tlsConfig, err := config.Build()
	require.NoError(t, err)
	assert.Equal(t, config.CipherSuites, tlsConfig.CipherSuites)
}

func TestDefaultSecureCipherSuites(t *testing.T) {
	suites := DefaultSecureCipherSuites()
	assert.NotEmpty(t, suites)

	// Verify all returned suites are valid
	for _, suite := range suites {
		found := false
		for _, supported := range tls.CipherSuites() {
			if supported.ID == suite {
				found = true

				break
			}
		}

		assert.True(t, found, "cipher suite 0x%04x should be supported", suite)
	}
}

func TestGetClientCN(t *testing.T) {
	t.Run("nil state", func(t *testing.T) {
		cn := GetClientCN(nil)
		assert.Empty(t, cn)
	})

	t.Run("no peer certificates", func(t *testing.T) {
		state := &tls.ConnectionState{}
		cn := GetClientCN(state)
		assert.Empty(t, cn)
	})
}

func TestGetClientCertificateSubject(t *testing.T) {
	t.Run("nil state", func(t *testing.T) {
		subject := GetClientCertificateSubject(nil)
		assert.Nil(t, subject)
	})

	t.Run("no peer certificates", func(t *testing.T) {
		state := &tls.ConnectionState{}
		subject := GetClientCertificateSubject(state)
		assert.Nil(t, subject)
	})
}

func TestIsClientCertVerified(t *testing.T) {
	t.Run("nil state", func(t *testing.T) {
		verified := IsClientCertVerified(nil)
		assert.False(t, verified)
	})

	t.Run("no verified chains", func(t *testing.T) {
		state := &tls.ConnectionState{}
		verified := IsClientCertVerified(state)
		assert.False(t, verified)
	})
}

func TestCertificateAuthenticator_ValidateCertificate(t *testing.T) {
	auth := NewCertificateAuthenticator()

	t.Run("nil state", func(t *testing.T) {
		_, err := auth.ValidateCertificate(nil)
		assert.Error(t, err)
	})

	t.Run("no verified chains", func(t *testing.T) {
		state := &tls.ConnectionState{}
		_, err := auth.ValidateCertificate(state)
		assert.Error(t, err)
	})
}

func TestCertificateAuthenticator_AllowedCNs(t *testing.T) {
	auth := NewCertificateAuthenticator()
	auth.AllowedCNs = []string{"allowed-user"}
	auth.RequireValidCert = false // For testing

	// Verify the structure is correct
	assert.Equal(t, []string{"allowed-user"}, auth.AllowedCNs)
}

func TestCertificateAuthenticator_UserMapper(t *testing.T) {
	auth := NewCertificateAuthenticator()
	auth.UserMapper = func(cn string) string {
		return "db_" + cn
	}

	// Verify mapper is set
	assert.NotNil(t, auth.UserMapper)
	assert.Equal(t, "db_testuser", auth.UserMapper("testuser"))
}

func TestTLSConfig_TLSVersions(t *testing.T) {
	tests := []struct {
		name       string
		minVersion TLSVersion
		maxVersion TLSVersion
	}{
		{"TLS 1.2 only", TLSVersion12, TLSVersion12},
		{"TLS 1.3 only", TLSVersion13, TLSVersion13},
		{"TLS 1.2-1.3", TLSVersion12, TLSVersion13},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewTLSConfig()
			config.AutoGenerate = true
			config.MinVersion = tt.minVersion
			config.MaxVersion = tt.maxVersion

			tlsConfig, err := config.Build()
			require.NoError(t, err)

			assert.Equal(t, uint16(tt.minVersion), tlsConfig.MinVersion)
			assert.Equal(t, uint16(tt.maxVersion), tlsConfig.MaxVersion)
		})
	}
}

func TestGenerateCertificate_IPAddresses(t *testing.T) {
	config := NewTLSConfig()
	config.AutoGenerate = true
	config.AutoGenerateHosts = []string{"localhost", "127.0.0.1", "::1", "192.168.1.1"}

	tlsConfig, err := config.Build()
	require.NoError(t, err)

	cert := tlsConfig.Certificates[0]
	parsedCert, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)

	// Should have DNS name for localhost
	assert.Contains(t, parsedCert.DNSNames, "localhost")

	// Should have IP addresses for the IPs
	ipStrings := make([]string, len(parsedCert.IPAddresses))
	for i, ip := range parsedCert.IPAddresses {
		ipStrings[i] = ip.String()
	}

	assert.Contains(t, ipStrings, "127.0.0.1")
	assert.Contains(t, ipStrings, "::1")
	assert.Contains(t, ipStrings, "192.168.1.1")
}

func TestCertificateAuthenticator_checkAllowedCN(t *testing.T) {
	auth := NewCertificateAuthenticator()

	t.Run("empty allowed list permits all", func(t *testing.T) {
		auth.AllowedCNs = nil
		err := auth.checkAllowedCN("any-user")
		assert.NoError(t, err)
	})

	t.Run("allowed CN passes", func(t *testing.T) {
		auth.AllowedCNs = []string{"user1", "user2"}
		err := auth.checkAllowedCN("user1")
		assert.NoError(t, err)
	})

	t.Run("disallowed CN fails", func(t *testing.T) {
		auth.AllowedCNs = []string{"user1", "user2"}
		err := auth.checkAllowedCN("user3")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not in allowed list")
	})
}
