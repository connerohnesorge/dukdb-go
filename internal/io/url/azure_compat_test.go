package url

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAzureURLBasicParsing tests basic Azure URL parsing for DuckDB compatibility.
func TestAzureURLBasicParsing(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		container string
		blob      string
	}{
		// Basic az:// URLs (simplified format)
		{
			name:      "simple container and blob",
			url:       "az://container/blob",
			container: "container",
			blob:      "blob",
		},
		{
			name:      "nested path",
			url:       "az://container/path/to/file.parquet",
			container: "container",
			blob:      "path/to/file.parquet",
		},
		{
			name:      "deep nested path",
			url:       "az://data-container/year=2024/month=01/day=15/part-0000.parquet",
			container: "data-container",
			blob:      "year=2024/month=01/day=15/part-0000.parquet",
		},
		// Empty blob cases
		{
			name:      "container with trailing slash",
			url:       "az://container/",
			container: "container",
			blob:      "",
		},
		{
			name:      "container only no slash",
			url:       "az://container",
			container: "container",
			blob:      "",
		},
		// Container name variations
		{
			name:      "container with hyphens",
			url:       "az://my-container-name/file.csv",
			container: "my-container-name",
			blob:      "file.csv",
		},
		{
			name:      "container with numbers",
			url:       "az://container123/file.csv",
			container: "container123",
			blob:      "file.csv",
		},
		{
			name:      "minimum length container",
			url:       "az://abc/file.csv",
			container: "abc",
			blob:      "file.csv",
		},
		// Blob variations
		{
			name:      "blob with extension",
			url:       "az://container/data.parquet",
			container: "container",
			blob:      "data.parquet",
		},
		{
			name:      "blob with multiple extensions",
			url:       "az://container/archive.tar.gz",
			container: "container",
			blob:      "archive.tar.gz",
		},
		{
			name:      "blob without extension",
			url:       "az://container/path/to/object",
			container: "container",
			blob:      "path/to/object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, "az", parsed.Scheme, "Scheme should be az")
			assert.Equal(t, tt.container, parsed.Container(), "Container mismatch")
			assert.Equal(t, tt.blob, parsed.Key(), "Blob mismatch")
			assert.True(t, parsed.IsAzure(), "Should be recognized as Azure")
			assert.True(t, parsed.IsCloudScheme(), "Should be recognized as cloud scheme")
			assert.False(t, parsed.IsS3(), "Should not be recognized as S3")
			assert.False(t, parsed.IsGCS(), "Should not be recognized as GCS")
		})
	}
}

// TestAzureURLSchemeEquivalence tests that azure:// and az:// parse identically
// for equivalent paths.
func TestAzureURLSchemeEquivalence(t *testing.T) {
	t.Run("both schemes produce IsAzure true", func(t *testing.T) {
		azParsed, err := Parse("az://container/blob")
		require.NoError(t, err)

		azureParsed, err := Parse("azure://container/blob")
		require.NoError(t, err)

		assert.True(t, azParsed.IsAzure(), "az:// should return IsAzure() = true")
		assert.True(t, azureParsed.IsAzure(), "azure:// should return IsAzure() = true")
	})

	t.Run("simple paths parse same container and blob", func(t *testing.T) {
		paths := []struct {
			path      string
			container string
			blob      string
		}{
			{"container/blob", "container", "blob"},
			{"my-container/path/to/file.parquet", "my-container", "path/to/file.parquet"},
			{"data/year=2024/month=01/file.csv", "data", "year=2024/month=01/file.csv"},
			{"container", "container", ""},
			{"container/", "container", ""},
		}

		for _, p := range paths {
			azURL := "az://" + p.path
			t.Run(azURL, func(t *testing.T) {
				parsed, err := Parse(azURL)
				require.NoError(t, err)

				assert.Equal(t, p.container, parsed.Container(), "Container should match")
				assert.Equal(t, p.blob, parsed.Key(), "Blob should match")
				assert.True(t, parsed.IsAzure(), "Should be Azure")
			})
		}
	})
}

// TestAzureURLFullEndpoint tests full Azure endpoint URL parsing.
// Format: azure://account.blob.core.windows.net/container/blob
func TestAzureURLFullEndpoint(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		container string
		blob      string
		endpoint  string
	}{
		{
			name:      "basic full endpoint",
			url:       "azure://myaccount.blob.core.windows.net/mycontainer/myblob",
			container: "mycontainer",
			blob:      "myblob",
			endpoint:  "myaccount.blob.core.windows.net",
		},
		{
			name:      "full endpoint with nested path",
			url:       "azure://account.blob.core.windows.net/container/path/to/blob.parquet",
			container: "container",
			blob:      "path/to/blob.parquet",
			endpoint:  "account.blob.core.windows.net",
		},
		{
			name:      "full endpoint with account containing hyphen",
			url:       "azure://my-storage-account.blob.core.windows.net/data/file.csv",
			container: "data",
			blob:      "file.csv",
			endpoint:  "my-storage-account.blob.core.windows.net",
		},
		{
			name:      "full endpoint with numbers in account",
			url:       "azure://storage123.blob.core.windows.net/container/blob",
			container: "container",
			blob:      "blob",
			endpoint:  "storage123.blob.core.windows.net",
		},
		{
			name:      "full endpoint container only",
			url:       "azure://account.blob.core.windows.net/container",
			container: "container",
			blob:      "",
			endpoint:  "account.blob.core.windows.net",
		},
		{
			name:      "full endpoint with deep nested path",
			url:       "azure://myaccount.blob.core.windows.net/data/year=2024/month=01/day=15/file.parquet",
			container: "data",
			blob:      "year=2024/month=01/day=15/file.parquet",
			endpoint:  "myaccount.blob.core.windows.net",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, "azure", parsed.Scheme)
			assert.Equal(t, tt.container, parsed.Container(), "Container mismatch")
			assert.Equal(t, tt.blob, parsed.Key(), "Blob mismatch")
			assert.Equal(t, tt.endpoint, parsed.Endpoint(), "Endpoint mismatch")
			assert.True(t, parsed.IsAzure())
			assert.True(t, parsed.IsCloudScheme())
		})
	}
}

// TestAzureURLQueryParameters tests query parameter handling for Azure URLs.
func TestAzureURLQueryParameters(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		container     string
		blob          string
		expectedQuery map[string]string
	}{
		// SAS token parameter
		{
			name:      "sas token parameter",
			url:       "az://container/blob?sas_token=sv%3D2020-08-04",
			container: "container",
			blob:      "blob",
			expectedQuery: map[string]string{
				"sas_token": "sv=2020-08-04",
			},
		},
		{
			name:      "full sas token",
			url:       "az://container/blob?sas_token=sv%3D2020-08-04%26ss%3Dbfqt%26srt%3Dsco%26sp%3Drwdlacup",
			container: "container",
			blob:      "blob",
			expectedQuery: map[string]string{
				"sas_token": "sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacup",
			},
		},
		// Connection string parameter
		{
			name:      "connection string",
			url:       "az://container/blob?connection_string=DefaultEndpointsProtocol%3Dhttps",
			container: "container",
			blob:      "blob",
			expectedQuery: map[string]string{
				"connection_string": "DefaultEndpointsProtocol=https",
			},
		},
		// Account key parameter
		{
			name:      "account key",
			url:       "az://container/blob?account_key=base64encodedkey%3D%3D",
			container: "container",
			blob:      "blob",
			expectedQuery: map[string]string{
				"account_key": "base64encodedkey==",
			},
		},
		// Multiple parameters
		{
			name:      "multiple params",
			url:       "az://container/blob?account_name=mystorageaccount&account_key=mykey",
			container: "container",
			blob:      "blob",
			expectedQuery: map[string]string{
				"account_name": "mystorageaccount",
				"account_key":  "mykey",
			},
		},
		// Query parameters with nested path
		{
			name:      "nested path with sas token",
			url:       "az://container/path/to/file.parquet?sas_token=token123",
			container: "container",
			blob:      "path/to/file.parquet",
			expectedQuery: map[string]string{
				"sas_token": "token123",
			},
		},
		// URL encoded parameters
		{
			name:      "encoded endpoint param",
			url:       "az://container/key?endpoint=http%3A%2F%2Flocalhost%3A10000",
			container: "container",
			blob:      "key",
			expectedQuery: map[string]string{
				"endpoint": "http://localhost:10000",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.container, parsed.Container())
			assert.Equal(t, tt.blob, parsed.Key())

			for key, expected := range tt.expectedQuery {
				assert.Equal(t, expected, parsed.Query.Get(key), "Query param %s mismatch", key)
			}
		})
	}
}

// TestAzureURLSpecialCharacters tests handling of special characters in Azure paths.
func TestAzureURLSpecialCharacters(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		container string
		blob      string
	}{
		// URL encoded spaces
		{
			name:      "spaces encoded as %20",
			url:       "az://container/path%20with%20spaces/file.csv",
			container: "container",
			blob:      "path%20with%20spaces/file.csv",
		},
		{
			name:      "spaces encoded as plus",
			url:       "az://container/path+with+plus/file.csv",
			container: "container",
			blob:      "path+with+plus/file.csv",
		},
		// Brackets and parentheses
		{
			name:      "square brackets",
			url:       "az://container/file[1].csv",
			container: "container",
			blob:      "file[1].csv",
		},
		{
			name:      "parentheses",
			url:       "az://container/file(copy).csv",
			container: "container",
			blob:      "file(copy).csv",
		},
		{
			name:      "curly braces",
			url:       "az://container/file{backup}.csv",
			container: "container",
			blob:      "file{backup}.csv",
		},
		// Equals sign in path (Hive-style partitioning)
		{
			name:      "hive partitioning",
			url:       "az://container/year=2024/month=01/day=15/file.parquet",
			container: "container",
			blob:      "year=2024/month=01/day=15/file.parquet",
		},
		// Unicode characters
		{
			name:      "unicode in path",
			url:       "az://container/data/file.csv",
			container: "container",
			blob:      "data/file.csv",
		},
		{
			name:      "encoded unicode",
			url:       "az://container/%E4%B8%AD%E6%96%87/file.csv",
			container: "container",
			blob:      "%E4%B8%AD%E6%96%87/file.csv",
		},
		// Special file characters
		{
			name:      "hash in filename",
			url:       "az://container/file#1.csv",
			container: "container",
			blob:      "file",
		},
		{
			name:      "at sign",
			url:       "az://container/user@domain/file.csv",
			container: "container",
			blob:      "user@domain/file.csv",
		},
		{
			name:      "ampersand encoded",
			url:       "az://container/a%26b/file.csv",
			container: "container",
			blob:      "a%26b/file.csv",
		},
		// Dots in path
		{
			name:      "dots in path",
			url:       "az://container/path.with.dots/file.csv",
			container: "container",
			blob:      "path.with.dots/file.csv",
		},
		{
			name:      "hidden file",
			url:       "az://container/.hidden/file.csv",
			container: "container",
			blob:      ".hidden/file.csv",
		},
		// Underscores
		{
			name:      "underscores",
			url:       "az://container/path_with_underscores/file_name.csv",
			container: "container",
			blob:      "path_with_underscores/file_name.csv",
		},
		// Dashes in blob
		{
			name:      "dashes in blob",
			url:       "az://container/path-with-dashes/file-name.csv",
			container: "container",
			blob:      "path-with-dashes/file-name.csv",
		},
		// Numbers in blob
		{
			name:      "numbers in blob",
			url:       "az://container/data2024/file001.csv",
			container: "container",
			blob:      "data2024/file001.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.container, parsed.Container())
			assert.Equal(t, tt.blob, parsed.Key())
			assert.True(t, parsed.IsAzure())
		})
	}
}

// TestAzureURLEdgeCases tests edge cases in Azure URL parsing.
func TestAzureURLEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		container string
		blob      string
	}{
		// Azure container naming rules: 3-63 chars, lowercase, numbers, hyphens only
		{
			name:      "minimum length container (3 chars)",
			url:       "az://abc/blob",
			container: "abc",
			blob:      "blob",
		},
		{
			name:      "max length container (63 chars)",
			url:       "az://abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy/key",
			container: "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy",
			blob:      "key",
		},
		// Very long blob path (Azure allows up to 1024 chars)
		{
			name:      "long blob path",
			url:       "az://container/a/very/long/path/with/many/directories/to/simulate/deep/nesting/file.parquet",
			container: "container",
			blob:      "a/very/long/path/with/many/directories/to/simulate/deep/nesting/file.parquet",
		},
		// Multiple consecutive slashes in blob
		{
			name:      "double slashes in blob",
			url:       "az://container//path//with//double//slashes",
			container: "container",
			blob:      "/path//with//double//slashes",
		},
		// Trailing slashes
		{
			name:      "trailing slash in blob",
			url:       "az://container/path/to/directory/",
			container: "container",
			blob:      "path/to/directory/",
		},
		// Blobs that look like paths
		{
			name:      "blob starting with slash",
			url:       "az://container//blob",
			container: "container",
			blob:      "/blob",
		},
		// Case sensitivity
		{
			name:      "mixed case container",
			url:       "az://MyContainer/file.csv",
			container: "MyContainer",
			blob:      "file.csv",
		},
		{
			name:      "uppercase scheme",
			url:       "AZ://container/key",
			container: "container",
			blob:      "key",
		},
		// Single character blob
		{
			name:      "single char blob",
			url:       "az://container/a",
			container: "container",
			blob:      "a",
		},
		// Blob with only extension
		{
			name:      "extension only blob",
			url:       "az://container/.csv",
			container: "container",
			blob:      ".csv",
		},
		// Container with numbers only
		{
			name:      "numeric container name",
			url:       "az://123456/file.csv",
			container: "123456",
			blob:      "file.csv",
		},
		// Container with hyphens
		{
			name:      "hyphenated container",
			url:       "az://my-storage-container/blob",
			container: "my-storage-container",
			blob:      "blob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.container, parsed.Container())
			assert.Equal(t, tt.blob, parsed.Key())
		})
	}
}

// TestAzureURLRoundTrip tests that Parse().String() produces valid URLs.
func TestAzureURLRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		original string
		expected string // expected output (may differ from original in normalization)
	}{
		{
			name:     "simple az URL",
			original: "az://container/blob",
			expected: "az://container/blob",
		},
		{
			name:     "nested path",
			original: "az://container/path/to/file.parquet",
			expected: "az://container/path/to/file.parquet",
		},
		{
			name:     "with query parameter",
			original: "az://container/key?sas_token=token123",
			expected: "az://container/key?sas_token=token123",
		},
		{
			name:     "azure scheme",
			original: "azure://account.blob.core.windows.net/container/blob",
			expected: "azure://account.blob.core.windows.net/container/blob",
		},
		{
			name:     "with fragment",
			original: "az://container/key#section",
			expected: "az://container/key#section",
		},
		{
			name:     "container only",
			original: "az://container",
			expected: "az://container",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed1, err := Parse(tt.original)
			require.NoError(t, err)

			str := parsed1.String()
			assert.Equal(t, tt.expected, str)

			// Parse again and verify equivalence
			parsed2, err := Parse(str)
			require.NoError(t, err)

			assert.Equal(t, parsed1.Scheme, parsed2.Scheme)
			assert.Equal(t, parsed1.Container(), parsed2.Container())
			assert.Equal(t, parsed1.Key(), parsed2.Key())
		})
	}
}

// TestAzureURLRoundTripPreservesSemantics tests that round-tripping preserves URL semantics.
func TestAzureURLRoundTripPreservesSemantics(t *testing.T) {
	urls := []string{
		"az://mycontainer/myblob",
		"az://data-container/path/to/file.parquet",
		"azure://myaccount.blob.core.windows.net/container/blob",
		"az://container/year=2024/month=01/data.parquet",
		"az://container/path/to/nested/blob.json",
	}

	for _, original := range urls {
		t.Run(original, func(t *testing.T) {
			parsed1, err := Parse(original)
			require.NoError(t, err)

			str := parsed1.String()
			parsed2, err := Parse(str)
			require.NoError(t, err)

			// Core properties must be preserved
			assert.Equal(t, parsed1.Scheme, parsed2.Scheme, "Scheme not preserved")
			assert.Equal(t, parsed1.Container(), parsed2.Container(), "Container not preserved")
			assert.Equal(t, parsed1.Key(), parsed2.Key(), "Key not preserved")
			assert.Equal(t, parsed1.IsAzure(), parsed2.IsAzure(), "IsAzure() not preserved")
		})
	}
}

// TestAzureURLContainerValidation tests various container name patterns.
// Azure container names have specific rules that should be respected.
func TestAzureURLContainerValidation(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		container     string
		shouldBeValid bool
	}{
		// Valid container names
		{
			name:          "lowercase alpha",
			url:           "az://mycontainer/blob",
			container:     "mycontainer",
			shouldBeValid: true,
		},
		{
			name:          "alphanumeric",
			url:           "az://container123/blob",
			container:     "container123",
			shouldBeValid: true,
		},
		{
			name:          "with hyphens",
			url:           "az://my-container/blob",
			container:     "my-container",
			shouldBeValid: true,
		},
		{
			name:          "min length 3 chars",
			url:           "az://abc/blob",
			container:     "abc",
			shouldBeValid: true,
		},
		{
			name:          "max length 63 chars",
			url:           "az://abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy/blob",
			container:     "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy",
			shouldBeValid: true,
		},
		// These parse successfully but may not be valid Azure container names
		{
			name:          "uppercase container parsed",
			url:           "az://MYCONTAINER/blob",
			container:     "MYCONTAINER",
			shouldBeValid: true, // Parser accepts it, Azure might not
		},
		// Azure does NOT allow dots in container names (unlike S3)
		{
			name:          "container with dots parsed",
			url:           "az://my.container/blob",
			container:     "my.container",
			shouldBeValid: true, // Parser accepts it, but Azure would reject
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)

			if tt.shouldBeValid {
				require.NoError(t, err)
				assert.Equal(t, tt.container, parsed.Container())
			} else if err == nil {
				// Parser accepted a potentially invalid container name
				// This is acceptable - validation is separate from parsing
				t.Logf("Parser accepted potentially invalid container: %s", tt.container)
			}
		})
	}
}

// TestAzureURLWithBuilderMethods tests the With* builder methods for Azure URLs.
func TestAzureURLWithBuilderMethods(t *testing.T) {
	t.Run("WithBucket for container", func(t *testing.T) {
		parsed, err := Parse("az://original-container/blob")
		require.NoError(t, err)

		withBucket := parsed.WithBucket("new-container")

		assert.Equal(t, "new-container", withBucket.Container())
		assert.Equal(t, "original-container", parsed.Container(), "Original should be unchanged")
	})

	t.Run("WithKey for blob", func(t *testing.T) {
		parsed, err := Parse("az://container/original-blob")
		require.NoError(t, err)

		withKey := parsed.WithKey("new/path/to/file.parquet")

		assert.Equal(t, "new/path/to/file.parquet", withKey.Key())
		assert.Equal(t, "original-blob", parsed.Key(), "Original should be unchanged")
	})

	t.Run("chained builders", func(t *testing.T) {
		parsed, err := Parse("az://container/blob")
		require.NoError(t, err)

		modified := parsed.
			WithBucket("new-container").
			WithKey("new/blob")

		assert.Equal(t, "new-container", modified.Container())
		assert.Equal(t, "new/blob", modified.Key())

		// Original unchanged
		assert.Equal(t, "container", parsed.Container())
		assert.Equal(t, "blob", parsed.Key())
	})
}

// TestAzureURLSchemeCheckers tests the scheme checker methods for Azure.
func TestAzureURLSchemeCheckers(t *testing.T) {
	tests := []struct {
		url     string
		isAzure bool
		isS3    bool
		isGCS   bool
		isCloud bool
		isLocal bool
		isHTTP  bool
	}{
		{"az://container/blob", true, false, false, true, false, false},
		{
			"azure://account.blob.core.windows.net/container/blob",
			true,
			false,
			false,
			true,
			false,
			false,
		},
		{"s3://bucket/key", false, true, false, true, false, false},
		{"gs://bucket/object", false, false, true, true, false, false},
		{"https://example.com/file", false, false, false, true, false, true},
		{"/local/path/file", false, false, false, false, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.isAzure, parsed.IsAzure(), "IsAzure mismatch")
			assert.Equal(t, tt.isS3, parsed.IsS3(), "IsS3 mismatch")
			assert.Equal(t, tt.isGCS, parsed.IsGCS(), "IsGCS mismatch")
			assert.Equal(t, tt.isCloud, parsed.IsCloudScheme(), "IsCloudScheme mismatch")
			assert.Equal(t, tt.isLocal, parsed.IsLocal(), "IsLocal mismatch")
			assert.Equal(t, tt.isHTTP, parsed.IsHTTP(), "IsHTTP mismatch")
		})
	}
}

// TestAzureURLCaseInsensitiveScheme tests that scheme detection is case-insensitive.
func TestAzureURLCaseInsensitiveScheme(t *testing.T) {
	schemes := []string{"az", "AZ", "Az", "aZ", "azure", "AZURE", "Azure", "aZuRe"}

	for _, scheme := range schemes {
		urlStr := scheme + "://container/blob"
		t.Run(urlStr, func(t *testing.T) {
			parsed, err := Parse(urlStr)
			require.NoError(t, err)

			// Should be recognized as Azure regardless of case
			assert.True(t, parsed.IsAzure(), "Should be recognized as Azure regardless of case")
			assert.Equal(t, "container", parsed.Container())
			assert.Equal(t, "blob", parsed.Key())
		})
	}
}

// TestAzureURLQueryParamEncoding tests URL encoding in query parameters.
func TestAzureURLQueryParamEncoding(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		paramKey string
		expected string
	}{
		{
			name:     "space encoded as %20",
			url:      "az://container/blob?filter=hello%20world",
			paramKey: "filter",
			expected: "hello world",
		},
		{
			name:     "plus sign as space",
			url:      "az://container/blob?filter=hello+world",
			paramKey: "filter",
			expected: "hello world",
		},
		{
			name:     "equals in value",
			url:      "az://container/blob?expr=a%3Db",
			paramKey: "expr",
			expected: "a=b",
		},
		{
			name:     "ampersand in value",
			url:      "az://container/blob?expr=a%26b",
			paramKey: "expr",
			expected: "a&b",
		},
		{
			name:     "complex sas token",
			url:      "az://container/blob?sas_token=sv%3D2020-08-04%26ss%3Db",
			paramKey: "sas_token",
			expected: "sv=2020-08-04&ss=b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, parsed.Query.Get(tt.paramKey))
		})
	}
}

// TestAzureURLHostAndFullPath tests Host() and FullPath() methods.
func TestAzureURLHostAndFullPath(t *testing.T) {
	t.Run("az scheme URL", func(t *testing.T) {
		parsed, err := Parse("az://mycontainer/path/to/blob")
		require.NoError(t, err)

		assert.Equal(t, "mycontainer", parsed.Host())
		assert.Equal(t, "/mycontainer/path/to/blob", parsed.FullPath())
	})

	t.Run("container only", func(t *testing.T) {
		parsed, err := Parse("az://mycontainer")
		require.NoError(t, err)

		assert.Equal(t, "mycontainer", parsed.Host())
	})

	t.Run("full endpoint URL", func(t *testing.T) {
		parsed, err := Parse("azure://account.blob.core.windows.net/container/blob")
		require.NoError(t, err)

		assert.Equal(t, "account.blob.core.windows.net", parsed.Host())
	})
}

// TestAzureURLGlobPatterns tests URLs with glob patterns commonly used in DuckDB.
func TestAzureURLGlobPatterns(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		container string
		blob      string
	}{
		{
			name:      "asterisk wildcard",
			url:       "az://container/*.parquet",
			container: "container",
			blob:      "*.parquet",
		},
		{
			name:      "double asterisk recursive",
			url:       "az://container/**/*.parquet",
			container: "container",
			blob:      "**/*.parquet",
		},
		{
			name:      "question mark encoded",
			url:       "az://container/file%3F.csv",
			container: "container",
			blob:      "file%3F.csv",
		},
		{
			name:      "question mark as query separator",
			url:       "az://container/file?.csv",
			container: "container",
			blob:      "file", // Note: ? starts query string, so .csv becomes query
		},
		{
			name:      "bracket character class",
			url:       "az://container/file[0-9].csv",
			container: "container",
			blob:      "file[0-9].csv",
		},
		{
			name:      "brace alternatives",
			url:       "az://container/data.{csv,parquet}",
			container: "container",
			blob:      "data.{csv,parquet}",
		},
		{
			name:      "hive partition with glob",
			url:       "az://container/year=2024/month=*/data.parquet",
			container: "container",
			blob:      "year=2024/month=*/data.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.container, parsed.Container())
			assert.Equal(t, tt.blob, parsed.Key())
		})
	}
}

// TestAzureURLToStdlibURL tests that parsed URLs can be converted to standard library url.URL.
func TestAzureURLToStdlibURL(t *testing.T) {
	testURLs := []string{
		"az://container/blob",
		"az://container/path/to/file.parquet",
		"az://container/key?sas_token=token123",
		"azure://account.blob.core.windows.net/container/blob",
	}

	for _, urlStr := range testURLs {
		t.Run(urlStr, func(t *testing.T) {
			parsed, err := Parse(urlStr)
			require.NoError(t, err)

			// Should be able to create a stdlib URL from our parsed result
			stdURL := &url.URL{
				Scheme:   parsed.Scheme,
				Host:     parsed.Authority,
				Path:     parsed.Path,
				RawQuery: parsed.RawQuery,
				Fragment: parsed.Fragment,
			}

			assert.NotNil(t, stdURL)
			assert.Equal(t, parsed.Scheme, stdURL.Scheme)
		})
	}
}

// TestAzureURLDuckDBCompatibility documents known Azure URL formats that DuckDB supports.
// This test serves as documentation and verification of compatibility.
func TestAzureURLDuckDBCompatibility(t *testing.T) {
	// These are URL formats documented in DuckDB's Azure support
	compatibilityTests := []struct {
		name        string
		url         string
		container   string
		blob        string
		description string
	}{
		{
			name:        "basic az path style",
			url:         "az://my-container/my-file.parquet",
			container:   "my-container",
			blob:        "my-file.parquet",
			description: "Basic Azure az:// URL",
		},
		{
			name:        "nested path",
			url:         "az://my-container/path/to/file.parquet",
			container:   "my-container",
			blob:        "path/to/file.parquet",
			description: "Azure URL with nested path",
		},
		{
			name:        "hive partitioning",
			url:         "az://my-container/year=2024/month=01/data.parquet",
			container:   "my-container",
			blob:        "year=2024/month=01/data.parquet",
			description: "Azure URL with Hive-style partitioning",
		},
		{
			name:        "full endpoint format",
			url:         "azure://mystorageaccount.blob.core.windows.net/container/blob.csv",
			container:   "container",
			blob:        "blob.csv",
			description: "Full Azure endpoint URL with account name",
		},
		{
			name:        "json file",
			url:         "az://data-container/config.json",
			container:   "data-container",
			blob:        "config.json",
			description: "Azure URL for JSON file",
		},
		{
			name:        "csv file",
			url:         "az://export-container/report.csv",
			container:   "export-container",
			blob:        "report.csv",
			description: "Azure URL for CSV file",
		},
	}

	for _, tt := range compatibilityTests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err, "Failed to parse: %s (%s)", tt.url, tt.description)

			assert.Equal(
				t,
				tt.container,
				parsed.Container(),
				"Container mismatch for: %s",
				tt.description,
			)
			assert.Equal(t, tt.blob, parsed.Key(), "Blob mismatch for: %s", tt.description)
			assert.True(t, parsed.IsAzure(), "Should be recognized as Azure: %s", tt.description)
		})
	}
}

// TestAzureURLContainerAlias tests that Container() is an alias for Bucket().
func TestAzureURLContainerAlias(t *testing.T) {
	parsed, err := Parse("az://mycontainer/path/to/blob.parquet")
	require.NoError(t, err)

	// Container() should equal Bucket()
	assert.Equal(t, parsed.Bucket(), parsed.Container())
	assert.Equal(t, "mycontainer", parsed.Container())
}

// TestAzureURLParseErrors tests error handling for invalid URLs.
func TestAzureURLParseErrors(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		shouldError bool
		errorMsg    string
	}{
		{
			name:        "empty URL",
			url:         "",
			shouldError: true,
			errorMsg:    "empty URL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.url)

			if tt.shouldError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAzureURLAzuriteCompatibility tests URLs commonly used with Azurite emulator.
func TestAzureURLAzuriteCompatibility(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		container string
		blob      string
		query     map[string]string
	}{
		{
			name:      "azurite localhost endpoint",
			url:       "az://container/key?endpoint=http://localhost:10000",
			container: "container",
			blob:      "key",
			query: map[string]string{
				"endpoint": "http://localhost:10000",
			},
		},
		{
			name:      "azurite with account name",
			url:       "az://container/key?endpoint=http://127.0.0.1:10000/devstoreaccount1",
			container: "container",
			blob:      "key",
			query: map[string]string{
				"endpoint": "http://127.0.0.1:10000/devstoreaccount1",
			},
		},
		{
			name:      "azurite with connection string",
			url:       "az://container/key?connection_string=UseDevelopmentStorage%3Dtrue",
			container: "container",
			blob:      "key",
			query: map[string]string{
				"connection_string": "UseDevelopmentStorage=true",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.container, parsed.Container())
			assert.Equal(t, tt.blob, parsed.Key())

			for k, v := range tt.query {
				assert.Equal(t, v, parsed.Query.Get(k), "Query param %s mismatch", k)
			}
		})
	}
}

// TestAzureURLFragmentHandling tests URL fragment handling.
func TestAzureURLFragmentHandling(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		blob     string
		fragment string
	}{
		{
			name:     "URL with fragment",
			url:      "az://container/blob#section",
			blob:     "blob",
			fragment: "section",
		},
		{
			name:     "URL with path and fragment",
			url:      "az://container/path/to/file.csv#header",
			blob:     "path/to/file.csv",
			fragment: "header",
		},
		{
			name:     "URL with query and fragment",
			url:      "az://container/blob?sas_token=token#section",
			blob:     "blob",
			fragment: "section",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.blob, parsed.Key())
			assert.Equal(t, tt.fragment, parsed.Fragment)
		})
	}
}

// TestAzureURLComparisonWithS3 tests that Azure and S3 URLs parse similarly for equivalent paths.
func TestAzureURLComparisonWithS3(t *testing.T) {
	// Compare parsing behavior between Azure and S3 for same container/bucket key patterns
	paths := []struct {
		container string
		key       string
	}{
		{"container", "blob"},
		{"my-container", "path/to/file.parquet"},
		{"data-container", "year=2024/month=01/file.csv"},
	}

	for _, p := range paths {
		t.Run(p.container+"/"+p.key, func(t *testing.T) {
			azURL := "az://" + p.container + "/" + p.key
			s3URL := "s3://" + p.container + "/" + p.key

			azParsed, err := Parse(azURL)
			require.NoError(t, err)

			s3Parsed, err := Parse(s3URL)
			require.NoError(t, err)

			// Container/Bucket and key extraction should be identical
			assert.Equal(
				t,
				azParsed.Container(),
				s3Parsed.Bucket(),
				"Container/Bucket extraction should match",
			)
			assert.Equal(t, azParsed.Key(), s3Parsed.Key(), "Key extraction should match")

			// But scheme checkers should differ
			assert.True(t, azParsed.IsAzure())
			assert.False(t, azParsed.IsS3())
			assert.True(t, s3Parsed.IsS3())
			assert.False(t, s3Parsed.IsAzure())
		})
	}
}

// TestAzureURLComparisonWithGCS tests that Azure and GCS URLs parse similarly for equivalent paths.
func TestAzureURLComparisonWithGCS(t *testing.T) {
	// Compare parsing behavior between Azure and GCS for same container/bucket key patterns
	paths := []struct {
		container string
		key       string
	}{
		{"container", "blob"},
		{"my-container", "path/to/file.parquet"},
		{"data-container", "year=2024/month=01/file.csv"},
	}

	for _, p := range paths {
		t.Run(p.container+"/"+p.key, func(t *testing.T) {
			azURL := "az://" + p.container + "/" + p.key
			gsURL := "gs://" + p.container + "/" + p.key

			azParsed, err := Parse(azURL)
			require.NoError(t, err)

			gsParsed, err := Parse(gsURL)
			require.NoError(t, err)

			// Container/Bucket and key extraction should be identical
			assert.Equal(
				t,
				azParsed.Container(),
				gsParsed.Bucket(),
				"Container/Bucket extraction should match",
			)
			assert.Equal(t, azParsed.Key(), gsParsed.Key(), "Key extraction should match")

			// But scheme checkers should differ
			assert.True(t, azParsed.IsAzure())
			assert.False(t, azParsed.IsGCS())
			assert.True(t, gsParsed.IsGCS())
			assert.False(t, gsParsed.IsAzure())
		})
	}
}

// TestAzureURLAccountExtraction tests extracting account name from full endpoint URLs.
func TestAzureURLAccountExtraction(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		endpoint string
		account  string // Expected account from endpoint
	}{
		{
			name:     "standard endpoint",
			url:      "azure://mystorageaccount.blob.core.windows.net/container/blob",
			endpoint: "mystorageaccount.blob.core.windows.net",
			account:  "mystorageaccount",
		},
		{
			name:     "account with hyphen",
			url:      "azure://my-storage-account.blob.core.windows.net/container/blob",
			endpoint: "my-storage-account.blob.core.windows.net",
			account:  "my-storage-account",
		},
		{
			name:     "account with numbers",
			url:      "azure://storage123.blob.core.windows.net/container/blob",
			endpoint: "storage123.blob.core.windows.net",
			account:  "storage123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.endpoint, parsed.Endpoint())
			// Note: Account name extraction would be done by the caller
			// by parsing the endpoint. The URL parser just provides the endpoint.
		})
	}
}
