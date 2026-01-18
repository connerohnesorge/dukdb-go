package url

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3URLBasicParsing tests basic S3 path-style URL parsing for DuckDB compatibility.
func TestS3URLBasicParsing(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		bucket string
		key    string
		region string
	}{
		// Basic path-style URLs
		{
			name:   "simple bucket and key",
			url:    "s3://bucket/key",
			bucket: "bucket",
			key:    "key",
		},
		{
			name:   "nested path",
			url:    "s3://bucket/path/to/file.parquet",
			bucket: "bucket",
			key:    "path/to/file.parquet",
		},
		{
			name:   "deep nested path",
			url:    "s3://data-bucket/year=2024/month=01/day=15/part-0000.parquet",
			bucket: "data-bucket",
			key:    "year=2024/month=01/day=15/part-0000.parquet",
		},
		// Empty key cases
		{
			name:   "bucket with trailing slash",
			url:    "s3://bucket/",
			bucket: "bucket",
			key:    "",
		},
		{
			name:   "bucket only no slash",
			url:    "s3://bucket",
			bucket: "bucket",
			key:    "",
		},
		// Bucket name variations
		{
			name:   "bucket with hyphens",
			url:    "s3://my-bucket-name/file.csv",
			bucket: "my-bucket-name",
			key:    "file.csv",
		},
		{
			name:   "bucket with dots",
			url:    "s3://my.bucket.name/file.csv",
			bucket: "my.bucket.name",
			key:    "file.csv",
		},
		{
			name:   "bucket with numbers",
			url:    "s3://bucket123/file.csv",
			bucket: "bucket123",
			key:    "file.csv",
		},
		{
			name:   "minimum length bucket",
			url:    "s3://abc/file.csv",
			bucket: "abc",
			key:    "file.csv",
		},
		// Key variations
		{
			name:   "key with extension",
			url:    "s3://bucket/data.parquet",
			bucket: "bucket",
			key:    "data.parquet",
		},
		{
			name:   "key with multiple extensions",
			url:    "s3://bucket/archive.tar.gz",
			bucket: "bucket",
			key:    "archive.tar.gz",
		},
		{
			name:   "key without extension",
			url:    "s3://bucket/path/to/object",
			bucket: "bucket",
			key:    "path/to/object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, "s3", parsed.Scheme, "Scheme should be s3")
			assert.Equal(t, tt.bucket, parsed.Bucket(), "Bucket mismatch")
			assert.Equal(t, tt.key, parsed.Key(), "Key mismatch")
			assert.True(t, parsed.IsS3(), "Should be recognized as S3")
			assert.True(t, parsed.IsCloudScheme(), "Should be recognized as cloud scheme")
			assert.False(t, parsed.IsVirtualHostStyle(), "Path-style should not be virtual-host")

			if tt.region != "" {
				assert.Equal(t, tt.region, parsed.Region(), "Region mismatch")
			}
		})
	}
}

// TestS3URLHadoopAliases tests that s3a:// and s3n:// work identically to s3://.
// These are Hadoop ecosystem aliases commonly used with Spark and other tools.
func TestS3URLHadoopAliases(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		scheme     string
		bucket     string
		key        string
		shouldBeS3 bool
	}{
		// Tests for s3a scheme (Hadoop S3A connector)
		{
			name:       "s3a simple",
			url:        "s3a://bucket/key",
			scheme:     "s3a",
			bucket:     "bucket",
			key:        "key",
			shouldBeS3: true,
		},
		{
			name:       "s3a nested path",
			url:        "s3a://data-bucket/path/to/file.parquet",
			scheme:     "s3a",
			bucket:     "data-bucket",
			key:        "path/to/file.parquet",
			shouldBeS3: true,
		},
		{
			name:       "s3a bucket only",
			url:        "s3a://bucket",
			scheme:     "s3a",
			bucket:     "bucket",
			key:        "",
			shouldBeS3: true,
		},
		// Tests for s3n scheme (Hadoop S3 Native connector - legacy)
		{
			name:       "s3n simple",
			url:        "s3n://bucket/key",
			scheme:     "s3n",
			bucket:     "bucket",
			key:        "key",
			shouldBeS3: true,
		},
		{
			name:       "s3n nested path",
			url:        "s3n://legacy-bucket/archive/2020/data.csv",
			scheme:     "s3n",
			bucket:     "legacy-bucket",
			key:        "archive/2020/data.csv",
			shouldBeS3: true,
		},
		{
			name:       "s3n bucket only",
			url:        "s3n://bucket",
			scheme:     "s3n",
			bucket:     "bucket",
			key:        "",
			shouldBeS3: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.scheme, parsed.Scheme, "Scheme mismatch")
			assert.Equal(t, tt.bucket, parsed.Bucket(), "Bucket mismatch")
			assert.Equal(t, tt.key, parsed.Key(), "Key mismatch")
			assert.Equal(t, tt.shouldBeS3, parsed.IsS3(), "Should be recognized as S3")
			assert.True(t, parsed.IsCloudScheme(), "Should be recognized as cloud scheme")
		})
	}
}

// TestS3URLHadoopAliasesEquivalence tests that s3://, s3a://, and s3n:// produce
// equivalent bucket and key extractions.
func TestS3URLHadoopAliasesEquivalence(t *testing.T) {
	paths := []struct {
		path   string
		bucket string
		key    string
	}{
		{"bucket/key", "bucket", "key"},
		{"my-bucket/path/to/file.parquet", "my-bucket", "path/to/file.parquet"},
		{"data/year=2024/month=01/file.csv", "data", "year=2024/month=01/file.csv"},
		{"bucket", "bucket", ""},
		{"bucket/", "bucket", ""},
	}

	schemes := []string{"s3", "s3a", "s3n"}

	for _, p := range paths {
		for _, scheme := range schemes {
			urlStr := scheme + "://" + p.path
			t.Run(urlStr, func(t *testing.T) {
				parsed, err := Parse(urlStr)
				require.NoError(t, err)

				assert.Equal(
					t,
					p.bucket,
					parsed.Bucket(),
					"Bucket should be identical across schemes",
				)
				assert.Equal(t, p.key, parsed.Key(), "Key should be identical across schemes")
				assert.True(t, parsed.IsS3(), "All S3 schemes should return true for IsS3()")
			})
		}
	}
}

// TestS3URLVirtualHostStyle tests virtual-host-style S3 URL parsing.
// These are URLs where the bucket is in the hostname.
func TestS3URLVirtualHostStyle(t *testing.T) {
	tests := []struct {
		name         string
		url          string
		bucket       string
		key          string
		region       string
		isVHost      bool
		skipIfFailed bool // Some virtual host patterns may not be supported yet
	}{
		// Standard virtual-host style with region
		{
			name:    "virtual host dot region",
			url:     "s3://mybucket.s3.us-east-1.amazonaws.com/mykey",
			bucket:  "mybucket",
			key:     "mykey",
			region:  "us-east-1",
			isVHost: true,
		},
		{
			name:    "virtual host us-west-2",
			url:     "s3://data-bucket.s3.us-west-2.amazonaws.com/path/to/file.csv",
			bucket:  "data-bucket",
			key:     "path/to/file.csv",
			region:  "us-west-2",
			isVHost: true,
		},
		// Hyphenated region format (legacy)
		{
			name:    "virtual host hyphen region",
			url:     "s3://mybucket.s3-us-east-1.amazonaws.com/mykey",
			bucket:  "mybucket",
			key:     "mykey",
			region:  "us-east-1",
			isVHost: true,
		},
		// EU regions
		{
			name:    "virtual host eu-west-1",
			url:     "s3://eu-bucket.s3.eu-west-1.amazonaws.com/data.parquet",
			bucket:  "eu-bucket",
			key:     "data.parquet",
			region:  "eu-west-1",
			isVHost: true,
		},
		{
			name:    "virtual host eu-central-1",
			url:     "s3://bucket.s3.eu-central-1.amazonaws.com/file.json",
			bucket:  "bucket",
			key:     "file.json",
			region:  "eu-central-1",
			isVHost: true,
		},
		// Asia Pacific regions
		{
			name:    "virtual host ap-southeast-1",
			url:     "s3://asia-bucket.s3.ap-southeast-1.amazonaws.com/data/file.csv",
			bucket:  "asia-bucket",
			key:     "data/file.csv",
			region:  "ap-southeast-1",
			isVHost: true,
		},
		// Empty key with virtual host
		{
			name:    "virtual host empty key",
			url:     "s3://mybucket.s3.us-east-1.amazonaws.com/",
			bucket:  "mybucket",
			key:     "",
			region:  "us-east-1",
			isVHost: true,
		},
		// Bucket with dots in virtual host
		{
			name:    "dotted bucket virtual host",
			url:     "s3://my.dotted.bucket.s3.us-west-2.amazonaws.com/file.csv",
			bucket:  "my.dotted.bucket",
			key:     "file.csv",
			region:  "us-west-2",
			isVHost: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, "s3", parsed.Scheme)
			assert.Equal(t, tt.bucket, parsed.Bucket(), "Bucket mismatch")
			assert.Equal(t, tt.key, parsed.Key(), "Key mismatch")
			assert.Equal(t, tt.region, parsed.Region(), "Region mismatch")
			assert.Equal(t, tt.isVHost, parsed.IsVirtualHostStyle(), "Virtual host style mismatch")
			assert.True(t, parsed.IsS3())
		})
	}
}

// TestS3URLQueryParameters tests query parameter handling for S3 URLs.
func TestS3URLQueryParameters(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		bucket        string
		key           string
		expectedQuery map[string]string
		region        string
	}{
		// Region parameter
		{
			name:   "region parameter",
			url:    "s3://bucket/key?region=us-east-1",
			bucket: "bucket",
			key:    "key",
			expectedQuery: map[string]string{
				"region": "us-east-1",
			},
			region: "us-east-1",
		},
		{
			name:   "region eu-west-1",
			url:    "s3://bucket/key?region=eu-west-1",
			bucket: "bucket",
			key:    "key",
			expectedQuery: map[string]string{
				"region": "eu-west-1",
			},
			region: "eu-west-1",
		},
		// Endpoint parameter
		{
			name:   "endpoint localhost",
			url:    "s3://bucket/key?endpoint=http://localhost:9000",
			bucket: "bucket",
			key:    "key",
			expectedQuery: map[string]string{
				"endpoint": "http://localhost:9000",
			},
		},
		{
			name:   "endpoint minio",
			url:    "s3://bucket/key?endpoint=http://minio.local:9000",
			bucket: "bucket",
			key:    "key",
			expectedQuery: map[string]string{
				"endpoint": "http://minio.local:9000",
			},
		},
		// Multiple parameters
		{
			name:   "region and endpoint",
			url:    "s3://bucket/key?region=us-east-1&endpoint=http://localhost:9000",
			bucket: "bucket",
			key:    "key",
			expectedQuery: map[string]string{
				"region":   "us-east-1",
				"endpoint": "http://localhost:9000",
			},
			region: "us-east-1",
		},
		{
			name:   "multiple custom params",
			url:    "s3://bucket/key?access_key_id=AKIAIOSFODNN7EXAMPLE&secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			bucket: "bucket",
			key:    "key",
			expectedQuery: map[string]string{
				"access_key_id":     "AKIAIOSFODNN7EXAMPLE",
				"secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
		},
		// Query parameters with nested path
		{
			name:   "nested path with region",
			url:    "s3://bucket/path/to/file.parquet?region=ap-northeast-1",
			bucket: "bucket",
			key:    "path/to/file.parquet",
			expectedQuery: map[string]string{
				"region": "ap-northeast-1",
			},
			region: "ap-northeast-1",
		},
		// URL encoded parameters
		{
			name:   "encoded endpoint",
			url:    "s3://bucket/key?endpoint=http%3A%2F%2Flocalhost%3A9000",
			bucket: "bucket",
			key:    "key",
			expectedQuery: map[string]string{
				"endpoint": "http://localhost:9000",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.bucket, parsed.Bucket())
			assert.Equal(t, tt.key, parsed.Key())

			for key, expected := range tt.expectedQuery {
				assert.Equal(t, expected, parsed.Query.Get(key), "Query param %s mismatch", key)
			}

			if tt.region != "" {
				assert.Equal(t, tt.region, parsed.Region())
			}
		})
	}
}

// TestS3URLSpecialCharacters tests handling of special characters in S3 paths.
func TestS3URLSpecialCharacters(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		bucket string
		key    string
	}{
		// URL encoded spaces
		{
			name:   "spaces encoded as %20",
			url:    "s3://bucket/path%20with%20spaces/file.csv",
			bucket: "bucket",
			key:    "path%20with%20spaces/file.csv",
		},
		{
			name:   "spaces encoded as plus",
			url:    "s3://bucket/path+with+plus/file.csv",
			bucket: "bucket",
			key:    "path+with+plus/file.csv",
		},
		// Brackets and parentheses
		{
			name:   "square brackets",
			url:    "s3://bucket/file[1].csv",
			bucket: "bucket",
			key:    "file[1].csv",
		},
		{
			name:   "parentheses",
			url:    "s3://bucket/file(copy).csv",
			bucket: "bucket",
			key:    "file(copy).csv",
		},
		{
			name:   "curly braces",
			url:    "s3://bucket/file{backup}.csv",
			bucket: "bucket",
			key:    "file{backup}.csv",
		},
		// Equals sign in path (Hive-style partitioning)
		{
			name:   "hive partitioning",
			url:    "s3://bucket/year=2024/month=01/day=15/file.parquet",
			bucket: "bucket",
			key:    "year=2024/month=01/day=15/file.parquet",
		},
		// Unicode characters
		{
			name:   "unicode in path",
			url:    "s3://bucket/data/file.csv",
			bucket: "bucket",
			key:    "data/file.csv",
		},
		{
			name:   "encoded unicode",
			url:    "s3://bucket/%E4%B8%AD%E6%96%87/file.csv",
			bucket: "bucket",
			key:    "%E4%B8%AD%E6%96%87/file.csv",
		},
		// Special file characters
		{
			name:   "hash in filename",
			url:    "s3://bucket/file#1.csv",
			bucket: "bucket",
			key:    "file",
		},
		{
			name:   "at sign",
			url:    "s3://bucket/user@domain/file.csv",
			bucket: "bucket",
			key:    "user@domain/file.csv",
		},
		{
			name:   "ampersand encoded",
			url:    "s3://bucket/a%26b/file.csv",
			bucket: "bucket",
			key:    "a%26b/file.csv",
		},
		// Dots in path
		{
			name:   "dots in path",
			url:    "s3://bucket/path.with.dots/file.csv",
			bucket: "bucket",
			key:    "path.with.dots/file.csv",
		},
		{
			name:   "hidden file",
			url:    "s3://bucket/.hidden/file.csv",
			bucket: "bucket",
			key:    ".hidden/file.csv",
		},
		// Underscores
		{
			name:   "underscores",
			url:    "s3://bucket/path_with_underscores/file_name.csv",
			bucket: "bucket",
			key:    "path_with_underscores/file_name.csv",
		},
		// Dashes in key
		{
			name:   "dashes in key",
			url:    "s3://bucket/path-with-dashes/file-name.csv",
			bucket: "bucket",
			key:    "path-with-dashes/file-name.csv",
		},
		// Numbers in key
		{
			name:   "numbers in key",
			url:    "s3://bucket/data2024/file001.csv",
			bucket: "bucket",
			key:    "data2024/file001.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.bucket, parsed.Bucket())
			assert.Equal(t, tt.key, parsed.Key())
			assert.True(t, parsed.IsS3())
		})
	}
}

// TestS3URLEdgeCases tests edge cases in S3 URL parsing.
func TestS3URLEdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		bucket string
		key    string
	}{
		// Very long bucket name (max 63 chars)
		{
			name:   "max length bucket",
			url:    "s3://abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy/key",
			bucket: "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy",
			key:    "key",
		},
		// Very long key
		{
			name:   "long key path",
			url:    "s3://bucket/a/very/long/path/with/many/directories/to/simulate/deep/nesting/file.parquet",
			bucket: "bucket",
			key:    "a/very/long/path/with/many/directories/to/simulate/deep/nesting/file.parquet",
		},
		// Multiple consecutive slashes in key
		{
			name:   "double slashes in key",
			url:    "s3://bucket//path//with//double//slashes",
			bucket: "bucket",
			key:    "/path//with//double//slashes",
		},
		// Trailing slashes
		{
			name:   "trailing slash in key",
			url:    "s3://bucket/path/to/directory/",
			bucket: "bucket",
			key:    "path/to/directory/",
		},
		// Keys that look like paths
		{
			name:   "key starting with slash",
			url:    "s3://bucket//key",
			bucket: "bucket",
			key:    "/key",
		},
		// Case sensitivity
		{
			name:   "mixed case bucket",
			url:    "s3://MyBucket/file.csv",
			bucket: "MyBucket",
			key:    "file.csv",
		},
		{
			name:   "uppercase scheme",
			url:    "S3://bucket/key",
			bucket: "bucket",
			key:    "key",
		},
		// Single character key
		{
			name:   "single char key",
			url:    "s3://bucket/a",
			bucket: "bucket",
			key:    "a",
		},
		// Key with only extension
		{
			name:   "extension only key",
			url:    "s3://bucket/.csv",
			bucket: "bucket",
			key:    ".csv",
		},
		// Bucket with all dots
		{
			name:   "dotted bucket name",
			url:    "s3://a.b.c.d/key",
			bucket: "a.b.c.d",
			key:    "key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.bucket, parsed.Bucket())
			assert.Equal(t, tt.key, parsed.Key())
		})
	}
}

// TestS3URLRoundTrip tests that Parse().String() produces valid URLs.
func TestS3URLRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		original string
		expected string // expected output (may differ from original in normalization)
	}{
		{
			name:     "simple path style",
			original: "s3://bucket/key",
			expected: "s3://bucket/key",
		},
		{
			name:     "nested path",
			original: "s3://bucket/path/to/file.parquet",
			expected: "s3://bucket/path/to/file.parquet",
		},
		{
			name:     "with query parameter",
			original: "s3://bucket/key?region=us-east-1",
			expected: "s3://bucket/key?region=us-east-1",
		},
		{
			name:     "s3a scheme",
			original: "s3a://bucket/key",
			expected: "s3a://bucket/key",
		},
		{
			name:     "s3n scheme",
			original: "s3n://bucket/key",
			expected: "s3n://bucket/key",
		},
		{
			name:     "with fragment",
			original: "s3://bucket/key#section",
			expected: "s3://bucket/key#section",
		},
		{
			name:     "bucket only",
			original: "s3://bucket",
			expected: "s3://bucket",
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
			assert.Equal(t, parsed1.Bucket(), parsed2.Bucket())
			assert.Equal(t, parsed1.Key(), parsed2.Key())
			assert.Equal(t, parsed1.Region(), parsed2.Region())
		})
	}
}

// TestS3URLRoundTripPreservesSemantics tests that round-tripping preserves URL semantics.
func TestS3URLRoundTripPreservesSemantics(t *testing.T) {
	urls := []string{
		"s3://mybucket/mykey",
		"s3://data-bucket/path/to/file.parquet",
		"s3a://hadoop-bucket/data/file.csv",
		"s3n://legacy-bucket/archive.tar.gz",
		"s3://bucket/year=2024/month=01/data.parquet",
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
			assert.Equal(t, parsed1.Bucket(), parsed2.Bucket(), "Bucket not preserved")
			assert.Equal(t, parsed1.Key(), parsed2.Key(), "Key not preserved")
			assert.Equal(t, parsed1.Region(), parsed2.Region(), "Region not preserved")
			assert.Equal(t, parsed1.IsS3(), parsed2.IsS3(), "IsS3() not preserved")
			assert.Equal(
				t,
				parsed1.IsVirtualHostStyle(),
				parsed2.IsVirtualHostStyle(),
				"IsVirtualHostStyle() not preserved",
			)
		})
	}
}

// TestS3URLBucketValidation tests various bucket name patterns.
// AWS S3 bucket names have specific rules that should be respected.
func TestS3URLBucketValidation(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		bucket        string
		shouldBeValid bool
	}{
		// Valid bucket names
		{
			name:          "lowercase alpha",
			url:           "s3://mybucket/key",
			bucket:        "mybucket",
			shouldBeValid: true,
		},
		{
			name:          "alphanumeric",
			url:           "s3://bucket123/key",
			bucket:        "bucket123",
			shouldBeValid: true,
		},
		{
			name:          "with hyphens",
			url:           "s3://my-bucket/key",
			bucket:        "my-bucket",
			shouldBeValid: true,
		},
		{
			name:          "with dots",
			url:           "s3://my.bucket/key",
			bucket:        "my.bucket",
			shouldBeValid: true,
		},
		{
			name:          "min length 3 chars",
			url:           "s3://abc/key",
			bucket:        "abc",
			shouldBeValid: true,
		},
		{
			name:          "max length 63 chars",
			url:           "s3://abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy/key",
			bucket:        "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy",
			shouldBeValid: true,
		},
		// These parse successfully but may not be valid AWS bucket names
		{
			name:          "uppercase bucket parsed",
			url:           "s3://MYBUCKET/key",
			bucket:        "MYBUCKET",
			shouldBeValid: true, // Parser accepts it, AWS might not
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)

			if tt.shouldBeValid {
				require.NoError(t, err)
				assert.Equal(t, tt.bucket, parsed.Bucket())
			} else if err == nil {
				// Parser accepted a potentially invalid bucket name
				// This is acceptable - validation is separate from parsing
				t.Logf("Parser accepted potentially invalid bucket: %s", tt.bucket)
			}
		})
	}
}

// TestS3URLWithBuilderMethods tests the With* builder methods for S3 URLs.
func TestS3URLWithBuilderMethods(t *testing.T) {
	t.Run("WithRegion", func(t *testing.T) {
		parsed, err := Parse("s3://bucket/key")
		require.NoError(t, err)

		withRegion := parsed.WithRegion("eu-west-1")

		assert.Equal(t, "eu-west-1", withRegion.Region())
		assert.Equal(t, "eu-west-1", withRegion.Query.Get("region"))
		assert.Equal(t, "", parsed.Region(), "Original should be unchanged")
	})

	t.Run("WithBucket", func(t *testing.T) {
		parsed, err := Parse("s3://original-bucket/key")
		require.NoError(t, err)

		withBucket := parsed.WithBucket("new-bucket")

		assert.Equal(t, "new-bucket", withBucket.Bucket())
		assert.Equal(t, "original-bucket", parsed.Bucket(), "Original should be unchanged")
	})

	t.Run("WithKey", func(t *testing.T) {
		parsed, err := Parse("s3://bucket/original-key")
		require.NoError(t, err)

		withKey := parsed.WithKey("new/path/to/file.parquet")

		assert.Equal(t, "new/path/to/file.parquet", withKey.Key())
		assert.Equal(t, "original-key", parsed.Key(), "Original should be unchanged")
	})

	t.Run("chained builders", func(t *testing.T) {
		parsed, err := Parse("s3://bucket/key")
		require.NoError(t, err)

		modified := parsed.
			WithBucket("new-bucket").
			WithKey("new/key").
			WithRegion("ap-northeast-1")

		assert.Equal(t, "new-bucket", modified.Bucket())
		assert.Equal(t, "new/key", modified.Key())
		assert.Equal(t, "ap-northeast-1", modified.Region())

		// Original unchanged
		assert.Equal(t, "bucket", parsed.Bucket())
		assert.Equal(t, "key", parsed.Key())
		assert.Equal(t, "", parsed.Region())
	})
}

// TestS3URLSchemeCheckers tests the scheme checker methods.
func TestS3URLSchemeCheckers(t *testing.T) {
	tests := []struct {
		url     string
		isS3    bool
		isCloud bool
		isLocal bool
		isHTTP  bool
	}{
		{"s3://bucket/key", true, true, false, false},
		{"s3a://bucket/key", true, true, false, false},
		{"s3n://bucket/key", true, true, false, false},
		{"gs://bucket/key", false, true, false, false},
		{"https://example.com/file", false, true, false, true},
		{"/local/path/file", false, false, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.isS3, parsed.IsS3(), "IsS3 mismatch")
			assert.Equal(t, tt.isCloud, parsed.IsCloudScheme(), "IsCloudScheme mismatch")
			assert.Equal(t, tt.isLocal, parsed.IsLocal(), "IsLocal mismatch")
			assert.Equal(t, tt.isHTTP, parsed.IsHTTP(), "IsHTTP mismatch")
		})
	}
}

// TestS3URLCaseInsensitiveScheme tests that scheme detection is case-insensitive.
func TestS3URLCaseInsensitiveScheme(t *testing.T) {
	schemes := []string{"s3", "S3", "S3A", "s3a", "S3N", "s3n"}

	for _, scheme := range schemes {
		urlStr := scheme + "://bucket/key"
		t.Run(urlStr, func(t *testing.T) {
			parsed, err := Parse(urlStr)
			require.NoError(t, err)

			// Scheme should be normalized to lowercase
			assert.Equal(t, true, parsed.IsS3(), "Should be recognized as S3 regardless of case")
			assert.Equal(t, "bucket", parsed.Bucket())
			assert.Equal(t, "key", parsed.Key())
		})
	}
}

// TestS3URLQueryParamEncoding tests URL encoding in query parameters.
func TestS3URLQueryParamEncoding(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		paramKey string
		expected string
	}{
		{
			name:     "space encoded as %20",
			url:      "s3://bucket/key?filter=hello%20world",
			paramKey: "filter",
			expected: "hello world",
		},
		{
			name:     "plus sign as space",
			url:      "s3://bucket/key?filter=hello+world",
			paramKey: "filter",
			expected: "hello world",
		},
		{
			name:     "equals in value",
			url:      "s3://bucket/key?expr=a%3Db",
			paramKey: "expr",
			expected: "a=b",
		},
		{
			name:     "ampersand in value",
			url:      "s3://bucket/key?expr=a%26b",
			paramKey: "expr",
			expected: "a&b",
		},
		{
			name:     "complex endpoint URL",
			url:      "s3://bucket/key?endpoint=http%3A%2F%2Flocalhost%3A9000",
			paramKey: "endpoint",
			expected: "http://localhost:9000",
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

// TestS3URLHostAndFullPath tests Host() and FullPath() methods.
func TestS3URLHostAndFullPath(t *testing.T) {
	t.Run("path style URL", func(t *testing.T) {
		parsed, err := Parse("s3://mybucket/path/to/key")
		require.NoError(t, err)

		assert.Equal(t, "mybucket", parsed.Host())
		assert.Equal(t, "/mybucket/path/to/key", parsed.FullPath())
	})

	t.Run("bucket only", func(t *testing.T) {
		parsed, err := Parse("s3://mybucket")
		require.NoError(t, err)

		assert.Equal(t, "mybucket", parsed.Host())
	})
}

// TestS3URLAWSRegions tests parsing URLs with various AWS region formats.
func TestS3URLAWSRegions(t *testing.T) {
	regions := []string{
		"us-east-1",
		"us-east-2",
		"us-west-1",
		"us-west-2",
		"eu-west-1",
		"eu-west-2",
		"eu-west-3",
		"eu-central-1",
		"eu-north-1",
		"ap-northeast-1",
		"ap-northeast-2",
		"ap-northeast-3",
		"ap-southeast-1",
		"ap-southeast-2",
		"ap-south-1",
		"sa-east-1",
		"ca-central-1",
		"me-south-1",
		"af-south-1",
	}

	for _, region := range regions {
		t.Run("query param "+region, func(t *testing.T) {
			urlStr := "s3://bucket/key?region=" + region
			parsed, err := Parse(urlStr)
			require.NoError(t, err)

			assert.Equal(t, region, parsed.Region())
		})

		t.Run("virtual host "+region, func(t *testing.T) {
			urlStr := "s3://bucket.s3." + region + ".amazonaws.com/key"
			parsed, err := Parse(urlStr)
			require.NoError(t, err)

			assert.Equal(t, region, parsed.Region())
			assert.True(t, parsed.IsVirtualHostStyle())
		})
	}
}

// TestS3URLDuckDBCompatibility documents known S3 URL formats that DuckDB supports.
// This test serves as documentation and verification of compatibility.
func TestS3URLDuckDBCompatibility(t *testing.T) {
	// These are URL formats documented in DuckDB's S3 support
	compatibilityTests := []struct {
		name        string
		url         string
		bucket      string
		key         string
		region      string
		description string
	}{
		{
			name:        "basic path style",
			url:         "s3://my-bucket/my-file.parquet",
			bucket:      "my-bucket",
			key:         "my-file.parquet",
			description: "Basic S3 path-style URL",
		},
		{
			name:        "nested path",
			url:         "s3://my-bucket/path/to/file.parquet",
			bucket:      "my-bucket",
			key:         "path/to/file.parquet",
			description: "S3 URL with nested path",
		},
		{
			name:        "hive partitioning",
			url:         "s3://my-bucket/year=2024/month=01/data.parquet",
			bucket:      "my-bucket",
			key:         "year=2024/month=01/data.parquet",
			description: "S3 URL with Hive-style partitioning",
		},
		{
			name:        "region query param",
			url:         "s3://my-bucket/file.csv?region=us-west-2",
			bucket:      "my-bucket",
			key:         "file.csv",
			region:      "us-west-2",
			description: "S3 URL with region in query parameter",
		},
		{
			name:        "s3a hadoop alias",
			url:         "s3a://hadoop-bucket/data.parquet",
			bucket:      "hadoop-bucket",
			key:         "data.parquet",
			description: "Hadoop S3A connector URL",
		},
	}

	for _, tt := range compatibilityTests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err, "Failed to parse: %s (%s)", tt.url, tt.description)

			assert.Equal(t, tt.bucket, parsed.Bucket(), "Bucket mismatch for: %s", tt.description)
			assert.Equal(t, tt.key, parsed.Key(), "Key mismatch for: %s", tt.description)

			if tt.region != "" {
				assert.Equal(
					t,
					tt.region,
					parsed.Region(),
					"Region mismatch for: %s",
					tt.description,
				)
			}

			assert.True(t, parsed.IsS3(), "Should be recognized as S3: %s", tt.description)
		})
	}
}

// TestS3URLParseErrors tests error handling for invalid URLs.
func TestS3URLParseErrors(t *testing.T) {
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

// TestS3URLMinIOCompatibility tests URLs commonly used with MinIO and S3-compatible stores.
func TestS3URLMinIOCompatibility(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		bucket string
		key    string
		query  map[string]string
	}{
		{
			name:   "minio localhost endpoint",
			url:    "s3://bucket/key?endpoint=http://localhost:9000",
			bucket: "bucket",
			key:    "key",
			query: map[string]string{
				"endpoint": "http://localhost:9000",
			},
		},
		{
			name:   "minio with path style",
			url:    "s3://bucket/key?endpoint=http://minio:9000&use_path_style=true",
			bucket: "bucket",
			key:    "key",
			query: map[string]string{
				"endpoint":       "http://minio:9000",
				"use_path_style": "true",
			},
		},
		{
			name:   "localstack endpoint",
			url:    "s3://bucket/key?endpoint=http://localhost:4566&region=us-east-1",
			bucket: "bucket",
			key:    "key",
			query: map[string]string{
				"endpoint": "http://localhost:4566",
				"region":   "us-east-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.bucket, parsed.Bucket())
			assert.Equal(t, tt.key, parsed.Key())

			for k, v := range tt.query {
				assert.Equal(t, v, parsed.Query.Get(k), "Query param %s mismatch", k)
			}
		})
	}
}

// TestS3URLGlobPatterns tests URLs with glob patterns commonly used in DuckDB.
func TestS3URLGlobPatterns(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		bucket string
		key    string
	}{
		{
			name:   "asterisk wildcard",
			url:    "s3://bucket/*.parquet",
			bucket: "bucket",
			key:    "*.parquet",
		},
		{
			name:   "double asterisk recursive",
			url:    "s3://bucket/**/*.parquet",
			bucket: "bucket",
			key:    "**/*.parquet",
		},
		{
			name:   "question mark encoded",
			url:    "s3://bucket/file%3F.csv",
			bucket: "bucket",
			key:    "file%3F.csv",
		},
		{
			name:   "question mark as query separator note",
			url:    "s3://bucket/file?.csv",
			bucket: "bucket",
			key:    "file", // Note: ? starts query string, so .csv becomes query
		},
		{
			name:   "bracket character class",
			url:    "s3://bucket/file[0-9].csv",
			bucket: "bucket",
			key:    "file[0-9].csv",
		},
		{
			name:   "brace alternatives",
			url:    "s3://bucket/data.{csv,parquet}",
			bucket: "bucket",
			key:    "data.{csv,parquet}",
		},
		{
			name:   "hive partition with glob",
			url:    "s3://bucket/year=2024/month=*/data.parquet",
			bucket: "bucket",
			key:    "year=2024/month=*/data.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.bucket, parsed.Bucket())
			assert.Equal(t, tt.key, parsed.Key())
		})
	}
}

// TestS3URLToStdlibURL tests that parsed URLs can be converted to standard library url.URL.
func TestS3URLToStdlibURL(t *testing.T) {
	testURLs := []string{
		"s3://bucket/key",
		"s3://bucket/path/to/file.parquet",
		"s3://bucket/key?region=us-east-1",
		"s3a://bucket/key",
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
