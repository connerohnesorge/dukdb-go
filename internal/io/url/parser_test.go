package url

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParse_S3PathStyle tests parsing S3 path-style URLs.
func TestParse_S3PathStyle(t *testing.T) {
	tests := []struct {
		name           string
		url            string
		expectedScheme string
		expectedBucket string
		expectedKey    string
	}{
		{
			name:           "simple bucket and key",
			url:            "s3://mybucket/mykey",
			expectedScheme: "s3",
			expectedBucket: "mybucket",
			expectedKey:    "mykey",
		},
		{
			name:           "nested key path",
			url:            "s3://mybucket/path/to/object.parquet",
			expectedScheme: "s3",
			expectedBucket: "mybucket",
			expectedKey:    "path/to/object.parquet",
		},
		{
			name:           "bucket only",
			url:            "s3://mybucket",
			expectedScheme: "s3",
			expectedBucket: "mybucket",
			expectedKey:    "",
		},
		{
			name:           "bucket with trailing slash",
			url:            "s3://mybucket/",
			expectedScheme: "s3",
			expectedBucket: "mybucket",
			expectedKey:    "",
		},
		{
			name:           "s3a scheme",
			url:            "s3a://mybucket/mykey",
			expectedScheme: "s3a",
			expectedBucket: "mybucket",
			expectedKey:    "mykey",
		},
		{
			name:           "s3n scheme",
			url:            "s3n://mybucket/mykey",
			expectedScheme: "s3n",
			expectedBucket: "mybucket",
			expectedKey:    "mykey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedScheme, parsed.Scheme)
			assert.Equal(t, tt.expectedBucket, parsed.Bucket())
			assert.Equal(t, tt.expectedKey, parsed.Key())
			assert.True(t, parsed.IsS3())
			assert.True(t, parsed.IsCloudScheme())
			assert.False(t, parsed.IsVirtualHostStyle())
		})
	}
}

// TestParse_S3VirtualHostStyle tests parsing S3 virtual-host-style URLs.
func TestParse_S3VirtualHostStyle(t *testing.T) {
	tests := []struct {
		name           string
		url            string
		expectedBucket string
		expectedKey    string
		expectedRegion string
	}{
		{
			name:           "virtual host with region",
			url:            "s3://mybucket.s3.us-east-1.amazonaws.com/mykey",
			expectedBucket: "mybucket",
			expectedKey:    "mykey",
			expectedRegion: "us-east-1",
		},
		{
			name:           "virtual host with us-west-2",
			url:            "s3://data-bucket.s3.us-west-2.amazonaws.com/path/to/file.csv",
			expectedBucket: "data-bucket",
			expectedKey:    "path/to/file.csv",
			expectedRegion: "us-west-2",
		},
		{
			name:           "virtual host with hyphenated region",
			url:            "s3://mybucket.s3-us-east-1.amazonaws.com/mykey",
			expectedBucket: "mybucket",
			expectedKey:    "mykey",
			expectedRegion: "us-east-1",
		},
		{
			name:           "virtual host eu-west-1",
			url:            "s3://eu-bucket.s3.eu-west-1.amazonaws.com/data.parquet",
			expectedBucket: "eu-bucket",
			expectedKey:    "data.parquet",
			expectedRegion: "eu-west-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, "s3", parsed.Scheme)
			assert.Equal(t, tt.expectedBucket, parsed.Bucket())
			assert.Equal(t, tt.expectedKey, parsed.Key())
			assert.Equal(t, tt.expectedRegion, parsed.Region())
			assert.True(t, parsed.IsVirtualHostStyle())
			assert.True(t, parsed.IsS3())
		})
	}
}

// TestParse_S3WithQueryParams tests S3 URLs with query parameters.
func TestParse_S3WithQueryParams(t *testing.T) {
	parsed, err := Parse("s3://mybucket/mykey?region=us-west-2")
	require.NoError(t, err)

	assert.Equal(t, "s3", parsed.Scheme)
	assert.Equal(t, "mybucket", parsed.Bucket())
	assert.Equal(t, "mykey", parsed.Key())
	assert.Equal(t, "us-west-2", parsed.Region())
	assert.Equal(t, "us-west-2", parsed.Query.Get("region"))
}

// TestParse_GCS tests parsing GCS URLs.
func TestParse_GCS(t *testing.T) {
	tests := []struct {
		name           string
		url            string
		expectedScheme string
		expectedBucket string
		expectedKey    string
	}{
		{
			name:           "gs scheme",
			url:            "gs://mybucket/myobject",
			expectedScheme: "gs",
			expectedBucket: "mybucket",
			expectedKey:    "myobject",
		},
		{
			name:           "gcs scheme",
			url:            "gcs://mybucket/path/to/object.json",
			expectedScheme: "gcs",
			expectedBucket: "mybucket",
			expectedKey:    "path/to/object.json",
		},
		{
			name:           "bucket only",
			url:            "gs://mybucket",
			expectedScheme: "gs",
			expectedBucket: "mybucket",
			expectedKey:    "",
		},
		{
			name:           "nested path",
			url:            "gs://data-bucket/year=2024/month=01/data.parquet",
			expectedScheme: "gs",
			expectedBucket: "data-bucket",
			expectedKey:    "year=2024/month=01/data.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedScheme, parsed.Scheme)
			assert.Equal(t, tt.expectedBucket, parsed.Bucket())
			assert.Equal(t, tt.expectedKey, parsed.Key())
			assert.True(t, parsed.IsGCS())
			assert.True(t, parsed.IsCloudScheme())
			assert.False(t, parsed.IsS3())
		})
	}
}

// TestParse_Azure tests parsing Azure URLs.
func TestParse_Azure(t *testing.T) {
	tests := []struct {
		name              string
		url               string
		expectedScheme    string
		expectedContainer string
		expectedBlob      string
		expectedEndpoint  string
	}{
		{
			name:              "az scheme simplified",
			url:               "az://mycontainer/myblob",
			expectedScheme:    "az",
			expectedContainer: "mycontainer",
			expectedBlob:      "myblob",
			expectedEndpoint:  "",
		},
		{
			name:              "az scheme with path",
			url:               "az://mycontainer/path/to/blob.csv",
			expectedScheme:    "az",
			expectedContainer: "mycontainer",
			expectedBlob:      "path/to/blob.csv",
			expectedEndpoint:  "",
		},
		{
			name:              "azure full format",
			url:               "azure://myaccount.blob.core.windows.net/mycontainer/myblob",
			expectedScheme:    "azure",
			expectedContainer: "mycontainer",
			expectedBlob:      "myblob",
			expectedEndpoint:  "myaccount.blob.core.windows.net",
		},
		{
			name:              "azure with nested path",
			url:               "azure://account.blob.core.windows.net/container/path/to/blob.parquet",
			expectedScheme:    "azure",
			expectedContainer: "container",
			expectedBlob:      "path/to/blob.parquet",
			expectedEndpoint:  "account.blob.core.windows.net",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedScheme, parsed.Scheme)
			assert.Equal(t, tt.expectedContainer, parsed.Container())
			assert.Equal(t, tt.expectedBlob, parsed.Key())
			assert.Equal(t, tt.expectedEndpoint, parsed.Endpoint())
			assert.True(t, parsed.IsAzure())
			assert.True(t, parsed.IsCloudScheme())
		})
	}
}

// TestParse_HTTP tests parsing HTTP/HTTPS URLs.
func TestParse_HTTP(t *testing.T) {
	tests := []struct {
		name             string
		url              string
		expectedScheme   string
		expectedHost     string
		expectedPath     string
		expectedEndpoint string
	}{
		{
			name:             "https URL",
			url:              "https://example.com/data/file.csv",
			expectedScheme:   "https",
			expectedHost:     "example.com",
			expectedPath:     "/data/file.csv",
			expectedEndpoint: "example.com",
		},
		{
			name:             "http URL",
			url:              "http://data.example.org/public/dataset.parquet",
			expectedScheme:   "http",
			expectedHost:     "data.example.org",
			expectedPath:     "/public/dataset.parquet",
			expectedEndpoint: "data.example.org",
		},
		{
			name:             "https with port",
			url:              "https://localhost:8080/api/data.json",
			expectedScheme:   "https",
			expectedHost:     "localhost:8080",
			expectedPath:     "/api/data.json",
			expectedEndpoint: "localhost:8080",
		},
		{
			name:             "http root path",
			url:              "http://example.com/",
			expectedScheme:   "http",
			expectedHost:     "example.com",
			expectedPath:     "/",
			expectedEndpoint: "example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedScheme, parsed.Scheme)
			assert.Equal(t, tt.expectedHost, parsed.Host())
			assert.Equal(t, tt.expectedPath, parsed.Key())
			assert.Equal(t, tt.expectedEndpoint, parsed.Endpoint())
			assert.True(t, parsed.IsHTTP())
			assert.True(t, parsed.IsCloudScheme())
		})
	}
}

// TestParse_LocalFile tests parsing local file paths.
func TestParse_LocalFile(t *testing.T) {
	tests := []struct {
		name         string
		url          string
		expectedPath string
	}{
		{
			name:         "absolute path",
			url:          "/home/user/data/file.csv",
			expectedPath: "/home/user/data/file.csv",
		},
		{
			name:         "file URL",
			url:          "file:///home/user/data/file.csv",
			expectedPath: "/home/user/data/file.csv",
		},
		{
			name:         "relative path",
			url:          "data/file.csv",
			expectedPath: "data/file.csv",
		},
		{
			name:         "windows-like path",
			url:          "C:/Users/data/file.csv",
			expectedPath: "C:/Users/data/file.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, "file", parsed.Scheme)
			assert.Equal(t, tt.expectedPath, parsed.Path)
			assert.True(t, parsed.IsLocal())
			assert.False(t, parsed.IsCloudScheme())
		})
	}
}

// TestParse_QueryParameters tests parsing URLs with query parameters.
func TestParse_QueryParameters(t *testing.T) {
	tests := []struct {
		name           string
		url            string
		expectedParams map[string]string
	}{
		{
			name: "single parameter",
			url:  "s3://bucket/key?region=us-east-1",
			expectedParams: map[string]string{
				"region": "us-east-1",
			},
		},
		{
			name: "multiple parameters",
			url:  "s3://bucket/key?region=us-east-1&endpoint=localhost:9000",
			expectedParams: map[string]string{
				"region":   "us-east-1",
				"endpoint": "localhost:9000",
			},
		},
		{
			name: "encoded parameter",
			url:  "https://example.com/path?query=hello%20world",
			expectedParams: map[string]string{
				"query": "hello world",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			for key, expected := range tt.expectedParams {
				assert.Equal(t, expected, parsed.Query.Get(key))
			}
		})
	}
}

// TestParse_Fragment tests parsing URLs with fragments.
func TestParse_Fragment(t *testing.T) {
	tests := []struct {
		name             string
		url              string
		expectedFragment string
		expectedPath     string
	}{
		{
			name:             "URL with fragment",
			url:              "s3://bucket/key#section1",
			expectedFragment: "section1",
			expectedPath:     "/key",
		},
		{
			name:             "URL with query and fragment",
			url:              "https://example.com/page?param=value#anchor",
			expectedFragment: "anchor",
			expectedPath:     "/page",
		},
		{
			name:             "file with fragment",
			url:              "/path/to/file.html#section",
			expectedFragment: "section",
			expectedPath:     "/path/to/file.html",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedFragment, parsed.Fragment)
			assert.Equal(t, tt.expectedPath, parsed.Path)
		})
	}
}

// TestParse_DuckDBPrefix tests parsing URLs with duckdb:// prefix.
func TestParse_DuckDBPrefix(t *testing.T) {
	parsed, err := Parse("duckdb:///home/user/data.db")
	require.NoError(t, err)

	assert.Equal(t, "file", parsed.Scheme)
	assert.Equal(t, "/home/user/data.db", parsed.Path)
}

// TestParse_EmptyURL tests parsing an empty URL.
func TestParse_EmptyURL(t *testing.T) {
	_, err := Parse("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty URL")
}

// TestParse_CaseInsensitiveScheme tests that scheme detection is case-insensitive.
func TestParse_CaseInsensitiveScheme(t *testing.T) {
	tests := []struct {
		url            string
		expectedScheme string
	}{
		{"S3://bucket/key", "s3"},
		{"GS://bucket/key", "gs"},
		{"HTTPS://example.com/path", "https"},
		{"Azure://account.blob.core.windows.net/container/blob", "azure"},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedScheme, parsed.Scheme)
		})
	}
}

// TestParsedURL_String tests the String method for round-trip parsing.
func TestParsedURL_String(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected string
	}{
		{
			name:     "simple S3 URL",
			url:      "s3://bucket/key",
			expected: "s3://bucket/key",
		},
		{
			name:     "S3 with query",
			url:      "s3://bucket/key?region=us-east-1",
			expected: "s3://bucket/key?region=us-east-1",
		},
		{
			name:     "HTTPS URL",
			url:      "https://example.com/path/to/file",
			expected: "https://example.com/path/to/file",
		},
		{
			name:     "GCS URL",
			url:      "gs://bucket/object/path",
			expected: "gs://bucket/object/path",
		},
		{
			name:     "URL with fragment",
			url:      "s3://bucket/key#section",
			expected: "s3://bucket/key#section",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			result := parsed.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestParsedURL_RoundTrip tests that parsing and stringifying yields equivalent URLs.
func TestParsedURL_RoundTrip(t *testing.T) {
	urls := []string{
		"s3://mybucket/mykey",
		"gs://bucket/object",
		"az://container/blob",
		"https://example.com/data.csv",
		"http://localhost:8080/api/data",
	}

	for _, original := range urls {
		t.Run(original, func(t *testing.T) {
			parsed1, err := Parse(original)
			require.NoError(t, err)

			str := parsed1.String()
			parsed2, err := Parse(str)
			require.NoError(t, err)

			assert.Equal(t, parsed1.Scheme, parsed2.Scheme)
			assert.Equal(t, parsed1.Authority, parsed2.Authority)
			assert.Equal(t, parsed1.Path, parsed2.Path)
			assert.Equal(t, parsed1.Bucket(), parsed2.Bucket())
			assert.Equal(t, parsed1.Key(), parsed2.Key())
		})
	}
}

// TestParsedURL_WithMethods tests the With* builder methods.
func TestParsedURL_WithMethods(t *testing.T) {
	t.Run("WithRegion", func(t *testing.T) {
		parsed, err := Parse("s3://bucket/key")
		require.NoError(t, err)

		withRegion := parsed.WithRegion("eu-west-1")
		assert.Equal(t, "eu-west-1", withRegion.Region())
		assert.Equal(t, "eu-west-1", withRegion.Query.Get("region"))

		// Original should be unchanged
		assert.Equal(t, "", parsed.Region())
	})

	t.Run("WithBucket", func(t *testing.T) {
		parsed, err := Parse("s3://bucket/key")
		require.NoError(t, err)

		withBucket := parsed.WithBucket("new-bucket")
		assert.Equal(t, "new-bucket", withBucket.Bucket())
		assert.Equal(t, "new-bucket", withBucket.Authority)

		// Original should be unchanged
		assert.Equal(t, "bucket", parsed.Bucket())
	})

	t.Run("WithKey", func(t *testing.T) {
		parsed, err := Parse("s3://bucket/key")
		require.NoError(t, err)

		withKey := parsed.WithKey("new/path/to/file")
		assert.Equal(t, "new/path/to/file", withKey.Key())

		// Original should be unchanged
		assert.Equal(t, "key", parsed.Key())
	})
}

// TestParsedURL_Aliases tests the alias methods.
func TestParsedURL_Aliases(t *testing.T) {
	parsed, err := Parse("azure://account.blob.core.windows.net/container/blob/path")
	require.NoError(t, err)

	// Container is alias for Bucket
	assert.Equal(t, parsed.Bucket(), parsed.Container())

	// ObjectPath is alias for Key
	assert.Equal(t, parsed.Key(), parsed.ObjectPath())
}

// TestParsedURL_SchemeCheckers tests the Is* methods.
func TestParsedURL_SchemeCheckers(t *testing.T) {
	tests := []struct {
		url     string
		isS3    bool
		isGCS   bool
		isAzure bool
		isHTTP  bool
		isLocal bool
		isCloud bool
	}{
		{"s3://bucket/key", true, false, false, false, false, true},
		{"s3a://bucket/key", true, false, false, false, false, true},
		{"s3n://bucket/key", true, false, false, false, false, true},
		{"gs://bucket/key", false, true, false, false, false, true},
		{"gcs://bucket/key", false, true, false, false, false, true},
		{"azure://account.blob.core.windows.net/c/b", false, false, true, false, false, true},
		{"az://container/blob", false, false, true, false, false, true},
		{"http://example.com/file", false, false, false, true, false, true},
		{"https://example.com/file", false, false, false, true, false, true},
		{"/local/path/file", false, false, false, false, true, false},
		{"file:///local/path/file", false, false, false, false, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.isS3, parsed.IsS3())
			assert.Equal(t, tt.isGCS, parsed.IsGCS())
			assert.Equal(t, tt.isAzure, parsed.IsAzure())
			assert.Equal(t, tt.isHTTP, parsed.IsHTTP())
			assert.Equal(t, tt.isLocal, parsed.IsLocal())
			assert.Equal(t, tt.isCloud, parsed.IsCloudScheme())
		})
	}
}

// TestParsedURL_SpecialCharacters tests parsing URLs with special characters.
func TestParsedURL_SpecialCharacters(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		expectedKey string
	}{
		{
			name:        "spaces encoded",
			url:         "s3://bucket/path%20with%20spaces/file.csv",
			expectedKey: "path%20with%20spaces/file.csv",
		},
		{
			name:        "equals in path",
			url:         "s3://bucket/year=2024/month=01/file.parquet",
			expectedKey: "year=2024/month=01/file.parquet",
		},
		{
			name:        "plus sign",
			url:         "s3://bucket/file+name.csv",
			expectedKey: "file+name.csv",
		},
		{
			name:        "unicode",
			url:         "s3://bucket/data/file.csv",
			expectedKey: "data/file.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedKey, parsed.Key())
		})
	}
}

// TestParsedURL_Host tests the Host method.
func TestParsedURL_Host(t *testing.T) {
	tests := []struct {
		url          string
		expectedHost string
	}{
		{"s3://mybucket/key", "mybucket"},
		{"https://example.com:8080/path", "example.com:8080"},
		{"gs://bucket/object", "bucket"},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedHost, parsed.Host())
		})
	}
}

// TestParsedURL_FullPath tests the FullPath method.
func TestParsedURL_FullPath(t *testing.T) {
	tests := []struct {
		url              string
		expectedFullPath string
	}{
		{"s3://bucket/key/path", "/bucket/key/path"},
		{"https://example.com/path/to/file", "/path/to/file"},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedFullPath, parsed.FullPath())
		})
	}
}

// TestParse_EdgeCases tests various edge cases.
func TestParse_EdgeCases(t *testing.T) {
	t.Run("authority only no path", func(t *testing.T) {
		parsed, err := Parse("s3://bucket")
		require.NoError(t, err)
		assert.Equal(t, "bucket", parsed.Authority)
		assert.Equal(t, "", parsed.Path)
		assert.Equal(t, "bucket", parsed.Bucket())
	})

	t.Run("query without path", func(t *testing.T) {
		parsed, err := Parse("s3://bucket?region=us-east-1")
		require.NoError(t, err)
		assert.Equal(t, "bucket", parsed.Authority)
		assert.Equal(t, "us-east-1", parsed.Query.Get("region"))
	})

	t.Run("fragment without path", func(t *testing.T) {
		parsed, err := Parse("https://example.com#section")
		require.NoError(t, err)
		assert.Equal(t, "example.com", parsed.Authority)
		assert.Equal(t, "section", parsed.Fragment)
	})

	t.Run("multiple slashes in key", func(t *testing.T) {
		parsed, err := Parse("s3://bucket//path//with//slashes")
		require.NoError(t, err)
		assert.Equal(t, "/path//with//slashes", parsed.Key())
	})

	t.Run("dot in bucket name", func(t *testing.T) {
		parsed, err := Parse("s3://my.bucket.name/key")
		require.NoError(t, err)
		assert.Equal(t, "my.bucket.name", parsed.Bucket())
	})

	t.Run("hyphen in bucket name", func(t *testing.T) {
		parsed, err := Parse("s3://my-bucket-name/key")
		require.NoError(t, err)
		assert.Equal(t, "my-bucket-name", parsed.Bucket())
	})
}
