package dukdb

import (
	"runtime"
	"testing"
)

func TestParseDSNConfig(t *testing.T) {
	defaultThreads := runtime.NumCPU()

	tests := []struct {
		name        string
		dsn         string
		wantPath    string
		wantAccess  string
		wantThreads int
		wantMemory  string
		wantErr     bool
		errType     ErrorType
	}{
		// Special DSN values
		{
			name:        "empty DSN",
			dsn:         "",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: defaultThreads,
			wantMemory:  "80%",
		},
		{
			name:        "memory DSN",
			dsn:         ":memory:",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: defaultThreads,
			wantMemory:  "80%",
		},
		{
			name:        "memory DSN with options",
			dsn:         ":memory:?threads=4&max_memory=2GB",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: 4,
			wantMemory:  "2GB",
		},
		{
			name:        "query only DSN",
			dsn:         "?threads=2",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: 2,
			wantMemory:  "80%",
		},

		// File path DSNs
		{
			name:        "simple file path",
			dsn:         "/path/to/db.duckdb",
			wantPath:    "/path/to/db.duckdb",
			wantAccess:  "automatic",
			wantThreads: defaultThreads,
			wantMemory:  "80%",
		},
		{
			name:        "file path with options",
			dsn:         "/path/to/db.duckdb?access_mode=read_only&threads=8",
			wantPath:    "/path/to/db.duckdb",
			wantAccess:  "read_only",
			wantThreads: 8,
			wantMemory:  "80%",
		},
		{
			name:        "file path with all options",
			dsn:         "/data/test.db?access_mode=read_write&threads=16&max_memory=4GB",
			wantPath:    "/data/test.db",
			wantAccess:  "read_write",
			wantThreads: 16,
			wantMemory:  "4GB",
		},

		// access_mode validation
		{
			name:        "access_mode automatic",
			dsn:         ":memory:?access_mode=automatic",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: defaultThreads,
			wantMemory:  "80%",
		},
		{
			name:        "access_mode read_only",
			dsn:         ":memory:?access_mode=read_only",
			wantPath:    ":memory:",
			wantAccess:  "read_only",
			wantThreads: defaultThreads,
			wantMemory:  "80%",
		},
		{
			name:        "access_mode read_write",
			dsn:         ":memory:?access_mode=read_write",
			wantPath:    ":memory:",
			wantAccess:  "read_write",
			wantThreads: defaultThreads,
			wantMemory:  "80%",
		},
		{
			name:    "invalid access_mode",
			dsn:     ":memory:?access_mode=invalid",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
		{
			name:    "access_mode uppercase rejected",
			dsn:     ":memory:?access_mode=READ_ONLY",
			wantErr: true,
			errType: ErrorTypeSettings,
		},

		// threads validation
		{
			name:        "threads minimum",
			dsn:         ":memory:?threads=1",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: 1,
			wantMemory:  "80%",
		},
		{
			name:        "threads maximum",
			dsn:         ":memory:?threads=128",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: 128,
			wantMemory:  "80%",
		},
		{
			name:    "threads below minimum",
			dsn:     ":memory:?threads=0",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
		{
			name:    "threads above maximum",
			dsn:     ":memory:?threads=129",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
		{
			name:    "threads non-integer",
			dsn:     ":memory:?threads=abc",
			wantErr: true,
			errType: ErrorTypeSettings,
		},

		// max_memory validation
		{
			name:        "max_memory bytes",
			dsn:         ":memory:?max_memory=1073741824",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: defaultThreads,
			wantMemory:  "1073741824",
		},
		{
			name:        "max_memory KB",
			dsn:         ":memory:?max_memory=1024KB",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: defaultThreads,
			wantMemory:  "1024KB",
		},
		{
			name:        "max_memory MB",
			dsn:         ":memory:?max_memory=512MB",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: defaultThreads,
			wantMemory:  "512MB",
		},
		{
			name:        "max_memory GB",
			dsn:         ":memory:?max_memory=4GB",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: defaultThreads,
			wantMemory:  "4GB",
		},
		{
			name:        "max_memory TB",
			dsn:         ":memory:?max_memory=1TB",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: defaultThreads,
			wantMemory:  "1TB",
		},
		{
			name:        "max_memory percentage",
			dsn:         ":memory:?max_memory=50%25",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: defaultThreads,
			wantMemory:  "50%",
		},
		{
			name:        "max_memory lowercase",
			dsn:         ":memory:?max_memory=4gb",
			wantPath:    ":memory:",
			wantAccess:  "automatic",
			wantThreads: defaultThreads,
			wantMemory:  "4gb",
		},
		{
			name:    "max_memory invalid percentage below",
			dsn:     ":memory:?max_memory=0%25",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
		{
			name:    "max_memory invalid percentage above",
			dsn:     ":memory:?max_memory=101%25",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
		{
			name:    "max_memory invalid format",
			dsn:     ":memory:?max_memory=invalid",
			wantErr: true,
			errType: ErrorTypeSettings,
		},

		// Unknown options
		{
			name:    "unknown option",
			dsn:     ":memory:?unknown_option=value",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
		{
			name:    "unknown option with valid options",
			dsn:     ":memory:?threads=4&unknown=value",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseDSN(tt.dsn)

			if tt.wantErr {
				if err == nil {
					t.Errorf(
						"ParseDSN(%q) expected error, got nil",
						tt.dsn,
					)

					return
				}
				if dErr, ok := err.(*Error); ok {
					if dErr.Type != tt.errType {
						t.Errorf(
							"ParseDSN(%q) error type = %v, want %v",
							tt.dsn,
							dErr.Type,
							tt.errType,
						)
					}
				}

				return
			}

			if err != nil {
				t.Errorf(
					"ParseDSN(%q) unexpected error: %v",
					tt.dsn,
					err,
				)

				return
			}

			if config.Path != tt.wantPath {
				t.Errorf(
					"ParseDSN(%q) Path = %q, want %q",
					tt.dsn,
					config.Path,
					tt.wantPath,
				)
			}
			if config.AccessMode != tt.wantAccess {
				t.Errorf(
					"ParseDSN(%q) AccessMode = %q, want %q",
					tt.dsn,
					config.AccessMode,
					tt.wantAccess,
				)
			}
			if config.Threads != tt.wantThreads {
				t.Errorf(
					"ParseDSN(%q) Threads = %d, want %d",
					tt.dsn,
					config.Threads,
					tt.wantThreads,
				)
			}
			if config.MaxMemory != tt.wantMemory {
				t.Errorf(
					"ParseDSN(%q) MaxMemory = %q, want %q",
					tt.dsn,
					config.MaxMemory,
					tt.wantMemory,
				)
			}
		})
	}
}

func TestNewConfig(t *testing.T) {
	config := NewConfig()

	if config.Path != "" {
		t.Errorf(
			"NewConfig() Path = %q, want empty",
			config.Path,
		)
	}
	if config.AccessMode != "automatic" {
		t.Errorf(
			"NewConfig() AccessMode = %q, want \"automatic\"",
			config.AccessMode,
		)
	}
	if config.Threads != runtime.NumCPU() {
		t.Errorf(
			"NewConfig() Threads = %d, want %d",
			config.Threads,
			runtime.NumCPU(),
		)
	}
	if config.MaxMemory != "80%" {
		t.Errorf(
			"NewConfig() MaxMemory = %q, want \"80%%\"",
			config.MaxMemory,
		)
	}
}

func TestResolveMaxMemory(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    int64
		wantErr bool
	}{
		// Empty
		{name: "empty", value: "", want: 0},

		// Bytes
		{
			name:  "bytes",
			value: "1024",
			want:  1024,
		},
		{name: "1B", value: "1B", want: 1},

		// KB
		{name: "1KB", value: "1KB", want: 1024},
		{name: "1kb", value: "1kb", want: 1024},

		// MB
		{
			name:  "1MB",
			value: "1MB",
			want:  1024 * 1024,
		},
		{
			name:  "512mb",
			value: "512mb",
			want:  512 * 1024 * 1024,
		},

		// GB
		{
			name:  "1GB",
			value: "1GB",
			want:  1024 * 1024 * 1024,
		},
		{
			name:  "4gb",
			value: "4gb",
			want:  4 * 1024 * 1024 * 1024,
		},

		// TB
		{
			name:  "1TB",
			value: "1TB",
			want:  1024 * 1024 * 1024 * 1024,
		},

		// Fractional
		{
			name:  "1.5GB",
			value: "1.5GB",
			want: int64(
				1.5 * 1024 * 1024 * 1024,
			),
		},

		// Percentage (returns error as system memory detection not implemented)
		{
			name:    "percentage",
			value:   "80%",
			wantErr: true,
		},

		// Invalid
		{
			name:    "invalid",
			value:   "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveMaxMemory(tt.value)

			if tt.wantErr {
				if err == nil {
					t.Errorf(
						"ResolveMaxMemory(%q) expected error, got nil",
						tt.value,
					)
				}

				return
			}

			if err != nil {
				t.Errorf(
					"ResolveMaxMemory(%q) unexpected error: %v",
					tt.value,
					err,
				)

				return
			}

			if got != tt.want {
				t.Errorf(
					"ResolveMaxMemory(%q) = %d, want %d",
					tt.value,
					got,
					tt.want,
				)
			}
		})
	}
}

func TestValidateAccessMode(t *testing.T) {
	validModes := []string{
		"automatic",
		"read_only",
		"read_write",
	}
	invalidModes := []string{
		"AUTOMATIC",
		"READ_ONLY",
		"READ_WRITE",
		"invalid",
		"",
	}

	for _, mode := range validModes {
		if err := validateAccessMode(mode); err != nil {
			t.Errorf(
				"validateAccessMode(%q) unexpected error: %v",
				mode,
				err,
			)
		}
	}

	for _, mode := range invalidModes {
		if err := validateAccessMode(mode); err == nil {
			t.Errorf(
				"validateAccessMode(%q) expected error, got nil",
				mode,
			)
		}
	}
}

func TestValidateMaxMemory(t *testing.T) {
	validValues := []string{
		"", "1", "1024",
		"1B", "1b",
		"1KB", "1kb", "1024KB",
		"1MB", "1mb", "512MB",
		"1GB", "1gb", "4GB",
		"1TB", "1tb",
		"1.5GB", "0.5TB",
		"1%", "50%", "100%",
	}
	invalidValues := []string{
		"0%", "101%", "-1%",
		"invalid", "abc",
		"GB", "1XB",
	}

	for _, val := range validValues {
		if err := validateMaxMemory(val); err != nil {
			t.Errorf(
				"validateMaxMemory(%q) unexpected error: %v",
				val,
				err,
			)
		}
	}

	for _, val := range invalidValues {
		if err := validateMaxMemory(val); err == nil {
			t.Errorf(
				"validateMaxMemory(%q) expected error, got nil",
				val,
			)
		}
	}
}

func TestParseThreads(t *testing.T) {
	tests := []struct {
		value   string
		want    int
		wantErr bool
	}{
		{"1", 1, false},
		{"64", 64, false},
		{"128", 128, false},
		{"0", 0, true},
		{"129", 0, true},
		{"-1", 0, true},
		{"abc", 0, true},
		{"", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			got, err := parseThreads(tt.value)

			if tt.wantErr {
				if err == nil {
					t.Errorf(
						"parseThreads(%q) expected error, got nil",
						tt.value,
					)
				}

				return
			}

			if err != nil {
				t.Errorf(
					"parseThreads(%q) unexpected error: %v",
					tt.value,
					err,
				)

				return
			}

			if got != tt.want {
				t.Errorf(
					"parseThreads(%q) = %d, want %d",
					tt.value,
					got,
					tt.want,
				)
			}
		})
	}
}