package url

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGCSURLBasicParsing tests basic GCS URL parsing for DuckDB compatibility.
func TestGCSURLBasicParsing(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		bucket string
		object string
	}{
		// Basic gs:// URLs
		{
			name:   "simple bucket and object",
			url:    "gs://bucket/object",
			bucket: "bucket",
			object: "object",
		},
		{
			name:   "nested path",
			url:    "gs://bucket/path/to/file.parquet",
			bucket: "bucket",
			object: "path/to/file.parquet",
		},
		{
			name:   "deep nested path",
			url:    "gs://data-bucket/year=2024/month=01/day=15/part-0000.parquet",
			bucket: "data-bucket",
			object: "year=2024/month=01/day=15/part-0000.parquet",
		},
		// Empty object cases
		{
			name:   "bucket with trailing slash",
			url:    "gs://bucket/",
			bucket: "bucket",
			object: "",
		},
		{
			name:   "bucket only no slash",
			url:    "gs://bucket",
			bucket: "bucket",
			object: "",
		},
		// Bucket name variations
		{
			name:   "bucket with hyphens",
			url:    "gs://my-bucket-name/file.csv",
			bucket: "my-bucket-name",
			object: "file.csv",
		},
		{
			name:   "bucket with dots",
			url:    "gs://my.bucket.name/file.csv",
			bucket: "my.bucket.name",
			object: "file.csv",
		},
		{
			name:   "bucket with numbers",
			url:    "gs://bucket123/file.csv",
			bucket: "bucket123",
			object: "file.csv",
		},
		{
			name:   "minimum length bucket",
			url:    "gs://abc/file.csv",
			bucket: "abc",
			object: "file.csv",
		},
		// Object variations
		{
			name:   "object with extension",
			url:    "gs://bucket/data.parquet",
			bucket: "bucket",
			object: "data.parquet",
		},
		{
			name:   "object with multiple extensions",
			url:    "gs://bucket/archive.tar.gz",
			bucket: "bucket",
			object: "archive.tar.gz",
		},
		{
			name:   "object without extension",
			url:    "gs://bucket/path/to/object",
			bucket: "bucket",
			object: "path/to/object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, "gs", parsed.Scheme, "Scheme should be gs")
			assert.Equal(t, tt.bucket, parsed.Bucket(), "Bucket mismatch")
			assert.Equal(t, tt.object, parsed.Key(), "Object mismatch")
			assert.True(t, parsed.IsGCS(), "Should be recognized as GCS")
			assert.True(t, parsed.IsCloudScheme(), "Should be recognized as cloud scheme")
			assert.False(t, parsed.IsS3(), "Should not be recognized as S3")
		})
	}
}

// TestGCSURLSchemeEquivalence tests that gs:// and gcs:// parse identically.
func TestGCSURLSchemeEquivalence(t *testing.T) {
	paths := []struct {
		path   string
		bucket string
		object string
	}{
		{"bucket/object", "bucket", "object"},
		{"my-bucket/path/to/file.parquet", "my-bucket", "path/to/file.parquet"},
		{"data/year=2024/month=01/file.csv", "data", "year=2024/month=01/file.csv"},
		{"bucket", "bucket", ""},
		{"bucket/", "bucket", ""},
	}

	schemes := []string{"gs", "gcs"}

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
				assert.Equal(t, p.object, parsed.Key(), "Object should be identical across schemes")
				assert.True(t, parsed.IsGCS(), "Both schemes should return true for IsGCS()")
				assert.True(t, parsed.IsCloudScheme(), "Both should be cloud schemes")
			})
		}
	}
}

// TestGCSURLSchemeEquivalenceSameBucketKey tests that gs:// and gcs:// produce
// equivalent bucket and key extractions for the same path.
func TestGCSURLSchemeEquivalenceSameBucketKey(t *testing.T) {
	testPaths := []string{
		"bucket/key",
		"my-bucket/path/to/object.json",
		"data-bucket/year=2024/month=01/day=15/file.parquet",
		"bucket-only",
		"bucket/",
	}

	for _, path := range testPaths {
		t.Run(path, func(t *testing.T) {
			gsURL := "gs://" + path
			gcsURL := "gcs://" + path

			gsParsed, err := Parse(gsURL)
			require.NoError(t, err)

			gcsParsed, err := Parse(gcsURL)
			require.NoError(t, err)

			// Core properties must match
			assert.Equal(
				t,
				gsParsed.Bucket(),
				gcsParsed.Bucket(),
				"Bucket should match between gs:// and gcs://",
			)
			assert.Equal(
				t,
				gsParsed.Key(),
				gcsParsed.Key(),
				"Key should match between gs:// and gcs://",
			)
			assert.Equal(t, gsParsed.IsGCS(), gcsParsed.IsGCS(), "IsGCS() should match")
		})
	}
}

// TestGCSURLQueryParameters tests query parameter handling for GCS URLs.
func TestGCSURLQueryParameters(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		bucket        string
		object        string
		expectedQuery map[string]string
	}{
		// Project parameter (GCS-specific)
		{
			name:   "project parameter",
			url:    "gs://bucket/object?project=my-project",
			bucket: "bucket",
			object: "object",
			expectedQuery: map[string]string{
				"project": "my-project",
			},
		},
		{
			name:   "project with dashes",
			url:    "gs://bucket/object?project=my-gcp-project-123",
			bucket: "bucket",
			object: "object",
			expectedQuery: map[string]string{
				"project": "my-gcp-project-123",
			},
		},
		// Endpoint parameter (for emulators)
		{
			name:   "endpoint localhost",
			url:    "gs://bucket/key?endpoint=http://localhost:4443",
			bucket: "bucket",
			object: "key",
			expectedQuery: map[string]string{
				"endpoint": "http://localhost:4443",
			},
		},
		// Multiple parameters
		{
			name:   "project and endpoint",
			url:    "gs://bucket/key?project=my-project&endpoint=http://localhost:4443",
			bucket: "bucket",
			object: "key",
			expectedQuery: map[string]string{
				"project":  "my-project",
				"endpoint": "http://localhost:4443",
			},
		},
		{
			name:   "multiple custom params",
			url:    "gs://bucket/key?credentials=path/to/creds.json&project=my-project",
			bucket: "bucket",
			object: "key",
			expectedQuery: map[string]string{
				"credentials": "path/to/creds.json",
				"project":     "my-project",
			},
		},
		// Query parameters with nested path
		{
			name:   "nested path with project",
			url:    "gs://bucket/path/to/file.parquet?project=analytics-prod",
			bucket: "bucket",
			object: "path/to/file.parquet",
			expectedQuery: map[string]string{
				"project": "analytics-prod",
			},
		},
		// URL encoded parameters
		{
			name:   "encoded endpoint",
			url:    "gs://bucket/key?endpoint=http%3A%2F%2Flocalhost%3A4443",
			bucket: "bucket",
			object: "key",
			expectedQuery: map[string]string{
				"endpoint": "http://localhost:4443",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.bucket, parsed.Bucket())
			assert.Equal(t, tt.object, parsed.Key())

			for key, expected := range tt.expectedQuery {
				assert.Equal(t, expected, parsed.Query.Get(key), "Query param %s mismatch", key)
			}
		})
	}
}

// TestGCSURLSpecialCharacters tests handling of special characters in GCS paths.
func TestGCSURLSpecialCharacters(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		bucket string
		object string
	}{
		// URL encoded spaces
		{
			name:   "spaces encoded as %20",
			url:    "gs://bucket/path%20with%20spaces/file.csv",
			bucket: "bucket",
			object: "path%20with%20spaces/file.csv",
		},
		{
			name:   "spaces encoded as plus",
			url:    "gs://bucket/path+with+plus/file.csv",
			bucket: "bucket",
			object: "path+with+plus/file.csv",
		},
		// Brackets and parentheses
		{
			name:   "square brackets",
			url:    "gs://bucket/file[1].csv",
			bucket: "bucket",
			object: "file[1].csv",
		},
		{
			name:   "parentheses",
			url:    "gs://bucket/file(copy).csv",
			bucket: "bucket",
			object: "file(copy).csv",
		},
		{
			name:   "curly braces",
			url:    "gs://bucket/file{backup}.csv",
			bucket: "bucket",
			object: "file{backup}.csv",
		},
		// Equals sign in path (Hive-style partitioning)
		{
			name:   "hive partitioning",
			url:    "gs://bucket/year=2024/month=01/day=15/file.parquet",
			bucket: "bucket",
			object: "year=2024/month=01/day=15/file.parquet",
		},
		// Unicode characters
		{
			name:   "unicode in path",
			url:    "gs://bucket/data/file.csv",
			bucket: "bucket",
			object: "data/file.csv",
		},
		{
			name:   "encoded unicode",
			url:    "gs://bucket/%E4%B8%AD%E6%96%87/file.csv",
			bucket: "bucket",
			object: "%E4%B8%AD%E6%96%87/file.csv",
		},
		// Special file characters
		{
			name:   "hash in filename",
			url:    "gs://bucket/file#1.csv",
			bucket: "bucket",
			object: "file",
		},
		{
			name:   "at sign",
			url:    "gs://bucket/user@domain/file.csv",
			bucket: "bucket",
			object: "user@domain/file.csv",
		},
		{
			name:   "ampersand encoded",
			url:    "gs://bucket/a%26b/file.csv",
			bucket: "bucket",
			object: "a%26b/file.csv",
		},
		// Dots in path
		{
			name:   "dots in path",
			url:    "gs://bucket/path.with.dots/file.csv",
			bucket: "bucket",
			object: "path.with.dots/file.csv",
		},
		{
			name:   "hidden file",
			url:    "gs://bucket/.hidden/file.csv",
			bucket: "bucket",
			object: ".hidden/file.csv",
		},
		// Underscores
		{
			name:   "underscores",
			url:    "gs://bucket/path_with_underscores/file_name.csv",
			bucket: "bucket",
			object: "path_with_underscores/file_name.csv",
		},
		// Dashes in object
		{
			name:   "dashes in object",
			url:    "gs://bucket/path-with-dashes/file-name.csv",
			bucket: "bucket",
			object: "path-with-dashes/file-name.csv",
		},
		// Numbers in object
		{
			name:   "numbers in object",
			url:    "gs://bucket/data2024/file001.csv",
			bucket: "bucket",
			object: "data2024/file001.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.bucket, parsed.Bucket())
			assert.Equal(t, tt.object, parsed.Key())
			assert.True(t, parsed.IsGCS())
		})
	}
}

// TestGCSURLEdgeCases tests edge cases in GCS URL parsing.
func TestGCSURLEdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		bucket string
		object string
	}{
		// GCS bucket naming rules: 3-63 chars, lowercase, numbers, hyphens, dots
		{
			name:   "minimum length bucket (3 chars)",
			url:    "gs://abc/object",
			bucket: "abc",
			object: "object",
		},
		{
			name:   "max length bucket (63 chars)",
			url:    "gs://abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy/key",
			bucket: "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy",
			object: "key",
		},
		// Very long object path
		{
			name:   "long object path",
			url:    "gs://bucket/a/very/long/path/with/many/directories/to/simulate/deep/nesting/file.parquet",
			bucket: "bucket",
			object: "a/very/long/path/with/many/directories/to/simulate/deep/nesting/file.parquet",
		},
		// Multiple consecutive slashes in object
		{
			name:   "double slashes in object",
			url:    "gs://bucket//path//with//double//slashes",
			bucket: "bucket",
			object: "/path//with//double//slashes",
		},
		// Trailing slashes
		{
			name:   "trailing slash in object",
			url:    "gs://bucket/path/to/directory/",
			bucket: "bucket",
			object: "path/to/directory/",
		},
		// Objects that look like paths
		{
			name:   "object starting with slash",
			url:    "gs://bucket//object",
			bucket: "bucket",
			object: "/object",
		},
		// Case sensitivity
		{
			name:   "mixed case bucket",
			url:    "gs://MyBucket/file.csv",
			bucket: "MyBucket",
			object: "file.csv",
		},
		{
			name:   "uppercase scheme",
			url:    "GS://bucket/key",
			bucket: "bucket",
			object: "key",
		},
		// Single character object
		{
			name:   "single char object",
			url:    "gs://bucket/a",
			bucket: "bucket",
			object: "a",
		},
		// Object with only extension
		{
			name:   "extension only object",
			url:    "gs://bucket/.csv",
			bucket: "bucket",
			object: ".csv",
		},
		// Bucket with all dots
		{
			name:   "dotted bucket name",
			url:    "gs://a.b.c.d/key",
			bucket: "a.b.c.d",
			object: "key",
		},
		// Bucket with numbers only
		{
			name:   "numeric bucket name",
			url:    "gs://123456/file.csv",
			bucket: "123456",
			object: "file.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.bucket, parsed.Bucket())
			assert.Equal(t, tt.object, parsed.Key())
		})
	}
}

// TestGCSURLRoundTrip tests that Parse().String() produces valid URLs.
func TestGCSURLRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		original string
		expected string // expected output (may differ from original in normalization)
	}{
		{
			name:     "simple gs URL",
			original: "gs://bucket/object",
			expected: "gs://bucket/object",
		},
		{
			name:     "nested path",
			original: "gs://bucket/path/to/file.parquet",
			expected: "gs://bucket/path/to/file.parquet",
		},
		{
			name:     "with query parameter",
			original: "gs://bucket/key?project=my-project",
			expected: "gs://bucket/key?project=my-project",
		},
		{
			name:     "gcs scheme",
			original: "gcs://bucket/key",
			expected: "gcs://bucket/key",
		},
		{
			name:     "with fragment",
			original: "gs://bucket/key#section",
			expected: "gs://bucket/key#section",
		},
		{
			name:     "bucket only",
			original: "gs://bucket",
			expected: "gs://bucket",
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
		})
	}
}

// TestGCSURLRoundTripPreservesSemantics tests that round-tripping preserves URL semantics.
func TestGCSURLRoundTripPreservesSemantics(t *testing.T) {
	urls := []string{
		"gs://mybucket/myobject",
		"gs://data-bucket/path/to/file.parquet",
		"gcs://analytics-bucket/data/file.csv",
		"gs://bucket/year=2024/month=01/data.parquet",
		"gs://bucket/path/to/nested/object.json",
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
			assert.Equal(t, parsed1.IsGCS(), parsed2.IsGCS(), "IsGCS() not preserved")
		})
	}
}

// TestGCSURLBucketValidation tests various bucket name patterns.
// GCS bucket names have specific rules that should be respected.
func TestGCSURLBucketValidation(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		bucket        string
		shouldBeValid bool
	}{
		// Valid bucket names
		{
			name:          "lowercase alpha",
			url:           "gs://mybucket/object",
			bucket:        "mybucket",
			shouldBeValid: true,
		},
		{
			name:          "alphanumeric",
			url:           "gs://bucket123/object",
			bucket:        "bucket123",
			shouldBeValid: true,
		},
		{
			name:          "with hyphens",
			url:           "gs://my-bucket/object",
			bucket:        "my-bucket",
			shouldBeValid: true,
		},
		{
			name:          "with dots",
			url:           "gs://my.bucket/object",
			bucket:        "my.bucket",
			shouldBeValid: true,
		},
		{
			name:          "min length 3 chars",
			url:           "gs://abc/object",
			bucket:        "abc",
			shouldBeValid: true,
		},
		{
			name:          "max length 63 chars",
			url:           "gs://abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy/object",
			bucket:        "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy",
			shouldBeValid: true,
		},
		// These parse successfully but may not be valid GCS bucket names
		{
			name:          "uppercase bucket parsed",
			url:           "gs://MYBUCKET/object",
			bucket:        "MYBUCKET",
			shouldBeValid: true, // Parser accepts it, GCS might not
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

// TestGCSURLWithBuilderMethods tests the With* builder methods for GCS URLs.
func TestGCSURLWithBuilderMethods(t *testing.T) {
	t.Run("WithBucket", func(t *testing.T) {
		parsed, err := Parse("gs://original-bucket/object")
		require.NoError(t, err)

		withBucket := parsed.WithBucket("new-bucket")

		assert.Equal(t, "new-bucket", withBucket.Bucket())
		assert.Equal(t, "original-bucket", parsed.Bucket(), "Original should be unchanged")
	})

	t.Run("WithKey", func(t *testing.T) {
		parsed, err := Parse("gs://bucket/original-object")
		require.NoError(t, err)

		withKey := parsed.WithKey("new/path/to/file.parquet")

		assert.Equal(t, "new/path/to/file.parquet", withKey.Key())
		assert.Equal(t, "original-object", parsed.Key(), "Original should be unchanged")
	})

	t.Run("chained builders", func(t *testing.T) {
		parsed, err := Parse("gs://bucket/object")
		require.NoError(t, err)

		modified := parsed.
			WithBucket("new-bucket").
			WithKey("new/object")

		assert.Equal(t, "new-bucket", modified.Bucket())
		assert.Equal(t, "new/object", modified.Key())

		// Original unchanged
		assert.Equal(t, "bucket", parsed.Bucket())
		assert.Equal(t, "object", parsed.Key())
	})
}

// TestGCSURLSchemeCheckers tests the scheme checker methods for GCS.
func TestGCSURLSchemeCheckers(t *testing.T) {
	tests := []struct {
		url     string
		isGCS   bool
		isS3    bool
		isCloud bool
		isLocal bool
		isHTTP  bool
	}{
		{"gs://bucket/object", true, false, true, false, false},
		{"gcs://bucket/object", true, false, true, false, false},
		{"s3://bucket/key", false, true, true, false, false},
		{"https://example.com/file", false, false, true, false, true},
		{"/local/path/file", false, false, false, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.isGCS, parsed.IsGCS(), "IsGCS mismatch")
			assert.Equal(t, tt.isS3, parsed.IsS3(), "IsS3 mismatch")
			assert.Equal(t, tt.isCloud, parsed.IsCloudScheme(), "IsCloudScheme mismatch")
			assert.Equal(t, tt.isLocal, parsed.IsLocal(), "IsLocal mismatch")
			assert.Equal(t, tt.isHTTP, parsed.IsHTTP(), "IsHTTP mismatch")
		})
	}
}

// TestGCSURLCaseInsensitiveScheme tests that scheme detection is case-insensitive.
func TestGCSURLCaseInsensitiveScheme(t *testing.T) {
	schemes := []string{"gs", "GS", "Gs", "gS", "gcs", "GCS", "Gcs", "gCs"}

	for _, scheme := range schemes {
		urlStr := scheme + "://bucket/object"
		t.Run(urlStr, func(t *testing.T) {
			parsed, err := Parse(urlStr)
			require.NoError(t, err)

			// Scheme should be normalized to lowercase
			assert.True(t, parsed.IsGCS(), "Should be recognized as GCS regardless of case")
			assert.Equal(t, "bucket", parsed.Bucket())
			assert.Equal(t, "object", parsed.Key())
		})
	}
}

// TestGCSURLQueryParamEncoding tests URL encoding in query parameters.
func TestGCSURLQueryParamEncoding(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		paramKey string
		expected string
	}{
		{
			name:     "space encoded as %20",
			url:      "gs://bucket/object?filter=hello%20world",
			paramKey: "filter",
			expected: "hello world",
		},
		{
			name:     "plus sign as space",
			url:      "gs://bucket/object?filter=hello+world",
			paramKey: "filter",
			expected: "hello world",
		},
		{
			name:     "equals in value",
			url:      "gs://bucket/object?expr=a%3Db",
			paramKey: "expr",
			expected: "a=b",
		},
		{
			name:     "ampersand in value",
			url:      "gs://bucket/object?expr=a%26b",
			paramKey: "expr",
			expected: "a&b",
		},
		{
			name:     "complex endpoint URL",
			url:      "gs://bucket/object?endpoint=http%3A%2F%2Flocalhost%3A4443",
			paramKey: "endpoint",
			expected: "http://localhost:4443",
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

// TestGCSURLHostAndFullPath tests Host() and FullPath() methods.
func TestGCSURLHostAndFullPath(t *testing.T) {
	t.Run("path style URL", func(t *testing.T) {
		parsed, err := Parse("gs://mybucket/path/to/object")
		require.NoError(t, err)

		assert.Equal(t, "mybucket", parsed.Host())
		assert.Equal(t, "/mybucket/path/to/object", parsed.FullPath())
	})

	t.Run("bucket only", func(t *testing.T) {
		parsed, err := Parse("gs://mybucket")
		require.NoError(t, err)

		assert.Equal(t, "mybucket", parsed.Host())
	})
}

// TestGCSURLGlobPatterns tests URLs with glob patterns commonly used in DuckDB.
func TestGCSURLGlobPatterns(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		bucket string
		object string
	}{
		{
			name:   "asterisk wildcard",
			url:    "gs://bucket/*.parquet",
			bucket: "bucket",
			object: "*.parquet",
		},
		{
			name:   "double asterisk recursive",
			url:    "gs://bucket/**/*.parquet",
			bucket: "bucket",
			object: "**/*.parquet",
		},
		{
			name:   "question mark encoded",
			url:    "gs://bucket/file%3F.csv",
			bucket: "bucket",
			object: "file%3F.csv",
		},
		{
			name:   "question mark as query separator",
			url:    "gs://bucket/file?.csv",
			bucket: "bucket",
			object: "file", // Note: ? starts query string, so .csv becomes query
		},
		{
			name:   "bracket character class",
			url:    "gs://bucket/file[0-9].csv",
			bucket: "bucket",
			object: "file[0-9].csv",
		},
		{
			name:   "brace alternatives",
			url:    "gs://bucket/data.{csv,parquet}",
			bucket: "bucket",
			object: "data.{csv,parquet}",
		},
		{
			name:   "hive partition with glob",
			url:    "gs://bucket/year=2024/month=*/data.parquet",
			bucket: "bucket",
			object: "year=2024/month=*/data.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.bucket, parsed.Bucket())
			assert.Equal(t, tt.object, parsed.Key())
		})
	}
}

// TestGCSURLToStdlibURL tests that parsed URLs can be converted to standard library url.URL.
func TestGCSURLToStdlibURL(t *testing.T) {
	testURLs := []string{
		"gs://bucket/object",
		"gs://bucket/path/to/file.parquet",
		"gs://bucket/key?project=my-project",
		"gcs://bucket/object",
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

// TestGCSURLDuckDBCompatibility documents known GCS URL formats that DuckDB supports.
// This test serves as documentation and verification of compatibility.
func TestGCSURLDuckDBCompatibility(t *testing.T) {
	// These are URL formats documented in DuckDB's GCS support
	compatibilityTests := []struct {
		name        string
		url         string
		bucket      string
		object      string
		description string
	}{
		{
			name:        "basic gs path style",
			url:         "gs://my-bucket/my-file.parquet",
			bucket:      "my-bucket",
			object:      "my-file.parquet",
			description: "Basic GCS gs:// URL",
		},
		{
			name:        "basic gcs path style",
			url:         "gcs://my-bucket/my-file.parquet",
			bucket:      "my-bucket",
			object:      "my-file.parquet",
			description: "Basic GCS gcs:// URL (alternative scheme)",
		},
		{
			name:        "nested path",
			url:         "gs://my-bucket/path/to/file.parquet",
			bucket:      "my-bucket",
			object:      "path/to/file.parquet",
			description: "GCS URL with nested path",
		},
		{
			name:        "hive partitioning",
			url:         "gs://my-bucket/year=2024/month=01/data.parquet",
			bucket:      "my-bucket",
			object:      "year=2024/month=01/data.parquet",
			description: "GCS URL with Hive-style partitioning",
		},
		{
			name:        "project query param",
			url:         "gs://my-bucket/file.csv?project=my-gcp-project",
			bucket:      "my-bucket",
			object:      "file.csv",
			description: "GCS URL with project in query parameter",
		},
		{
			name:        "json file",
			url:         "gs://data-bucket/config.json",
			bucket:      "data-bucket",
			object:      "config.json",
			description: "GCS URL for JSON file",
		},
		{
			name:        "csv file",
			url:         "gs://export-bucket/report.csv",
			bucket:      "export-bucket",
			object:      "report.csv",
			description: "GCS URL for CSV file",
		},
	}

	for _, tt := range compatibilityTests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err, "Failed to parse: %s (%s)", tt.url, tt.description)

			assert.Equal(t, tt.bucket, parsed.Bucket(), "Bucket mismatch for: %s", tt.description)
			assert.Equal(t, tt.object, parsed.Key(), "Object mismatch for: %s", tt.description)
			assert.True(t, parsed.IsGCS(), "Should be recognized as GCS: %s", tt.description)
		})
	}
}

// TestGCSURLObjectAlias tests that Key() and ObjectPath() return the same value.
func TestGCSURLObjectAlias(t *testing.T) {
	parsed, err := Parse("gs://bucket/path/to/object.parquet")
	require.NoError(t, err)

	// ObjectPath() is an alias for Key()
	assert.Equal(t, parsed.Key(), parsed.ObjectPath())
	assert.Equal(t, "path/to/object.parquet", parsed.ObjectPath())
}

// TestGCSURLParseErrors tests error handling for invalid GCS URLs.
func TestGCSURLParseErrors(t *testing.T) {
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

// TestGCSURLEmulatorCompatibility tests URLs commonly used with GCS emulators.
func TestGCSURLEmulatorCompatibility(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		bucket string
		object string
		query  map[string]string
	}{
		{
			name:   "fake-gcs-server localhost endpoint",
			url:    "gs://bucket/key?endpoint=http://localhost:4443",
			bucket: "bucket",
			object: "key",
			query: map[string]string{
				"endpoint": "http://localhost:4443",
			},
		},
		{
			name:   "gcs emulator with project",
			url:    "gs://bucket/key?endpoint=http://gcs-emulator:4443&project=test-project",
			bucket: "bucket",
			object: "key",
			query: map[string]string{
				"endpoint": "http://gcs-emulator:4443",
				"project":  "test-project",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.bucket, parsed.Bucket())
			assert.Equal(t, tt.object, parsed.Key())

			for k, v := range tt.query {
				assert.Equal(t, v, parsed.Query.Get(k), "Query param %s mismatch", k)
			}
		})
	}
}

// TestGCSURLFragmentHandling tests URL fragment handling.
func TestGCSURLFragmentHandling(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		object   string
		fragment string
	}{
		{
			name:     "URL with fragment",
			url:      "gs://bucket/object#section",
			object:   "object",
			fragment: "section",
		},
		{
			name:     "URL with path and fragment",
			url:      "gs://bucket/path/to/file.csv#header",
			object:   "path/to/file.csv",
			fragment: "header",
		},
		{
			name:     "URL with query and fragment",
			url:      "gs://bucket/object?project=test#section",
			object:   "object",
			fragment: "section",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.object, parsed.Key())
			assert.Equal(t, tt.fragment, parsed.Fragment)
		})
	}
}

// TestGCSURLComparisonWithS3 tests that GCS and S3 URLs parse similarly for equivalent paths.
func TestGCSURLComparisonWithS3(t *testing.T) {
	// Compare parsing behavior between GCS and S3 for same bucket/key patterns
	paths := []struct {
		bucket string
		key    string
	}{
		{"bucket", "object"},
		{"my-bucket", "path/to/file.parquet"},
		{"data-bucket", "year=2024/month=01/file.csv"},
	}

	for _, p := range paths {
		t.Run(p.bucket+"/"+p.key, func(t *testing.T) {
			gsURL := "gs://" + p.bucket + "/" + p.key
			s3URL := "s3://" + p.bucket + "/" + p.key

			gsParsed, err := Parse(gsURL)
			require.NoError(t, err)

			s3Parsed, err := Parse(s3URL)
			require.NoError(t, err)

			// Bucket and key extraction should be identical
			assert.Equal(t, gsParsed.Bucket(), s3Parsed.Bucket(), "Bucket extraction should match")
			assert.Equal(t, gsParsed.Key(), s3Parsed.Key(), "Key extraction should match")

			// But scheme checkers should differ
			assert.True(t, gsParsed.IsGCS())
			assert.False(t, gsParsed.IsS3())
			assert.True(t, s3Parsed.IsS3())
			assert.False(t, s3Parsed.IsGCS())
		})
	}
}
