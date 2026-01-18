package dukdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseThreshold tests the ParseThreshold function with various inputs.
func TestParseThreshold(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    int64
		expectError bool
	}{
		// Byte suffix tests
		{
			name:        "single byte suffix",
			input:       "1024b",
			expected:    1024,
			expectError: false,
		},
		{
			name:        "byte suffix uppercase",
			input:       "1024B",
			expected:    1024,
			expectError: false,
		},
		{
			name:        "multiple bytes",
			input:       "2048b",
			expected:    2048,
			expectError: false,
		},

		// Kilobyte suffix tests
		{
			name:        "kilobyte suffix lowercase",
			input:       "512kb",
			expected:    512 * 1024,
			expectError: false,
		},
		{
			name:        "kilobyte suffix mixed case",
			input:       "512Kb",
			expected:    512 * 1024,
			expectError: false,
		},
		{
			name:        "kilobyte suffix uppercase",
			input:       "512KB",
			expected:    512 * 1024,
			expectError: false,
		},
		{
			name:        "1 kilobyte",
			input:       "1KB",
			expected:    1024,
			expectError: false,
		},

		// Megabyte suffix tests
		{
			name:        "megabyte suffix lowercase",
			input:       "256mb",
			expected:    256 * 1024 * 1024,
			expectError: false,
		},
		{
			name:        "megabyte suffix mixed case",
			input:       "256Mb",
			expected:    256 * 1024 * 1024,
			expectError: false,
		},
		{
			name:        "megabyte suffix uppercase",
			input:       "256MB",
			expected:    256 * 1024 * 1024,
			expectError: false,
		},
		{
			name:        "1 megabyte",
			input:       "1MB",
			expected:    1048576,
			expectError: false,
		},

		// Gigabyte suffix tests
		{
			name:        "gigabyte suffix lowercase",
			input:       "1gb",
			expected:    1024 * 1024 * 1024,
			expectError: false,
		},
		{
			name:        "gigabyte suffix mixed case",
			input:       "1Gb",
			expected:    1024 * 1024 * 1024,
			expectError: false,
		},
		{
			name:        "gigabyte suffix uppercase",
			input:       "1GB",
			expected:    1024 * 1024 * 1024,
			expectError: false,
		},
		{
			name:        "2 gigabytes",
			input:       "2GB",
			expected:    2 * 1024 * 1024 * 1024,
			expectError: false,
		},

		// Plain number tests (no suffix = bytes)
		{
			name:        "plain number as bytes",
			input:       "1000000",
			expected:    1000000,
			expectError: false,
		},
		{
			name:        "single byte as plain number",
			input:       "1",
			expected:    1,
			expectError: false,
		},
		{
			name:        "large plain number",
			input:       "268435456",
			expected:    268435456,
			expectError: false,
		},

		// Invalid format tests
		{
			name:        "invalid suffix",
			input:       "256XB",
			expectError: true,
		},
		{
			name:        "non-numeric prefix",
			input:       "abcMB",
			expectError: true,
		},
		{
			name:        "empty string",
			input:       "",
			expectError: true,
		},
		{
			name:        "invalid number format",
			input:       "1.5.5MB",
			expectError: true,
		},
		{
			name:        "floating point not supported",
			input:       "1.5MB",
			expectError: true,
		},
		{
			name:        "only suffix",
			input:       "MB",
			expectError: true,
		},
		{
			name:        "negative number",
			input:       "-100MB",
			expected:    -104857600, // ParseInt allows negative values (ValidateThreshold rejects)
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseThreshold(tt.input)
			if tt.expectError {
				require.Error(t, err, "expected error for input: %s", tt.input)
			} else {
				require.NoError(t, err, "unexpected error for input: %s", tt.input)
				require.Equal(t, tt.expected, result, "incorrect result for input: %s", tt.input)
			}
		})
	}
}

// TestValidateThreshold tests the ValidateThreshold function.
func TestValidateThreshold(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		errorMsg    string
	}{
		// Valid thresholds
		{
			name:        "valid 256MB",
			input:       "256MB",
			expectError: false,
		},
		{
			name:        "valid 1GB",
			input:       "1GB",
			expectError: false,
		},
		{
			name:        "valid 512KB",
			input:       "512KB",
			expectError: false,
		},
		{
			name:        "valid 1KB minimum",
			input:       "1KB",
			expectError: false,
		},
		{
			name:        "valid 1024 bytes",
			input:       "1024",
			expectError: false,
		},
		{
			name:        "valid 1MB",
			input:       "1MB",
			expectError: false,
		},

		// Invalid - below minimum
		{
			name:        "invalid below 1KB",
			input:       "512",
			expectError: true,
		},
		{
			name:        "invalid 0 bytes",
			input:       "0",
			expectError: true,
		},
		{
			name:        "invalid 1 byte",
			input:       "1",
			expectError: true,
		},
		{
			name:        "invalid negative number",
			input:       "-100MB",
			expectError: true,
		},

		// Invalid - format errors
		{
			name:        "invalid format",
			input:       "invalid",
			expectError: true,
		},
		{
			name:        "invalid empty",
			input:       "",
			expectError: true,
		},
		{
			name:        "invalid bad suffix",
			input:       "256XB",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateThreshold(tt.input)
			if tt.expectError {
				require.Error(t, err, "expected error for input: %s", tt.input)
			} else {
				require.NoError(t, err, "unexpected error for input: %s", tt.input)
			}
		})
	}
}

// TestConfigWithThreshold tests that the Config struct properly handles checkpoint_threshold.
func TestConfigWithThreshold(t *testing.T) {
	// Test NewConfig has default threshold
	config := NewConfig()
	require.Equal(t, "256MB", config.CheckpointThreshold)

	// Test ParseDSN with checkpoint_threshold
	dsn := ":memory:?checkpoint_threshold=512MB"
	config, err := ParseDSN(dsn)
	require.NoError(t, err)
	require.Equal(t, "512MB", config.CheckpointThreshold)

	// Test ParseDSN with invalid checkpoint_threshold
	dsn = ":memory:?checkpoint_threshold=invalid"
	_, err = ParseDSN(dsn)
	require.Error(t, err)
}

// TestThresholdEdgeCases tests edge cases for threshold parsing.
func TestThresholdEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    int64
		expectError bool
	}{
		// Large values
		{
			name:        "large value",
			input:       "100GB",
			expected:    100 * 1024 * 1024 * 1024,
			expectError: false,
		},
		{
			name:        "1TB",
			input:       "1TB",
			expectError: true, // TB suffix not supported
		},
		// Very small valid values
		{
			name:        "minimum 1KB",
			input:       "1024",
			expected:    1024,
			expectError: false,
		},
		// Decimal not supported
		{
			name:        "decimal value",
			input:       "1.5GB",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseThreshold(tt.input)
			if tt.expectError {
				require.Error(t, err, "expected error for input: %s", tt.input)
			} else {
				require.NoError(t, err, "unexpected error for input: %s", tt.input)
				require.Equal(t, tt.expected, result, "incorrect result for input: %s", tt.input)
			}
		})
	}
}

// TestThresholdCaseSensitivity ensures parsing is case-insensitive for suffixes.
func TestThresholdCaseSensitivity(t *testing.T) {
	testCases := []string{
		"256mb", "256MB", "256Mb", "256mB",
		"1gb", "1GB", "1Gb", "1gB",
		"512kb", "512KB", "512Kb", "512kB",
		"1024b", "1024B",
	}

	for _, testCase := range testCases {
		_, err := ParseThreshold(testCase)
		require.NoError(t, err, "case sensitivity check failed for: %s", testCase)
	}
}
