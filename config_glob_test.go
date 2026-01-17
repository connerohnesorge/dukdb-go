package dukdb

import (
	"testing"
)

func TestGlobSettingsDefaults(t *testing.T) {
	// Test that NewConfig returns the expected default values
	config := NewConfig()

	if config.MaxFilesPerGlob != DefaultMaxFilesPerGlob {
		t.Errorf(
			"NewConfig() MaxFilesPerGlob = %d, want %d",
			config.MaxFilesPerGlob,
			DefaultMaxFilesPerGlob,
		)
	}

	if config.FileGlobTimeout != DefaultFileGlobTimeout {
		t.Errorf(
			"NewConfig() FileGlobTimeout = %d, want %d",
			config.FileGlobTimeout,
			DefaultFileGlobTimeout,
		)
	}
}

func TestGlobSettingsDSNParsing(t *testing.T) {
	tests := []struct {
		name               string
		dsn                string
		wantMaxFiles       int
		wantTimeout        int
		wantErr            bool
		errType            ErrorType
	}{
		// Default values
		{
			name:         "default values",
			dsn:          ":memory:",
			wantMaxFiles: DefaultMaxFilesPerGlob,
			wantTimeout:  DefaultFileGlobTimeout,
		},

		// max_files_per_glob setting
		{
			name:         "max_files_per_glob minimum",
			dsn:          ":memory:?max_files_per_glob=1",
			wantMaxFiles: 1,
			wantTimeout:  DefaultFileGlobTimeout,
		},
		{
			name:         "max_files_per_glob typical value",
			dsn:          ":memory:?max_files_per_glob=50000",
			wantMaxFiles: 50000,
			wantTimeout:  DefaultFileGlobTimeout,
		},
		{
			name:         "max_files_per_glob maximum",
			dsn:          ":memory:?max_files_per_glob=1000000",
			wantMaxFiles: 1000000,
			wantTimeout:  DefaultFileGlobTimeout,
		},
		{
			name:    "max_files_per_glob below minimum",
			dsn:     ":memory:?max_files_per_glob=0",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
		{
			name:    "max_files_per_glob above maximum",
			dsn:     ":memory:?max_files_per_glob=1000001",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
		{
			name:    "max_files_per_glob non-integer",
			dsn:     ":memory:?max_files_per_glob=abc",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
		{
			name:    "max_files_per_glob negative",
			dsn:     ":memory:?max_files_per_glob=-1",
			wantErr: true,
			errType: ErrorTypeSettings,
		},

		// file_glob_timeout setting
		{
			name:         "file_glob_timeout minimum",
			dsn:          ":memory:?file_glob_timeout=1",
			wantMaxFiles: DefaultMaxFilesPerGlob,
			wantTimeout:  1,
		},
		{
			name:         "file_glob_timeout typical value",
			dsn:          ":memory:?file_glob_timeout=120",
			wantMaxFiles: DefaultMaxFilesPerGlob,
			wantTimeout:  120,
		},
		{
			name:         "file_glob_timeout maximum",
			dsn:          ":memory:?file_glob_timeout=600",
			wantMaxFiles: DefaultMaxFilesPerGlob,
			wantTimeout:  600,
		},
		{
			name:    "file_glob_timeout below minimum",
			dsn:     ":memory:?file_glob_timeout=0",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
		{
			name:    "file_glob_timeout above maximum",
			dsn:     ":memory:?file_glob_timeout=601",
			wantErr: true,
			errType: ErrorTypeSettings,
		},
		{
			name:    "file_glob_timeout non-integer",
			dsn:     ":memory:?file_glob_timeout=abc",
			wantErr: true,
			errType: ErrorTypeSettings,
		},

		// Combined settings
		{
			name:         "both glob settings",
			dsn:          ":memory:?max_files_per_glob=25000&file_glob_timeout=300",
			wantMaxFiles: 25000,
			wantTimeout:  300,
		},
		{
			name:         "glob settings with other options",
			dsn:          ":memory:?threads=4&max_files_per_glob=20000&file_glob_timeout=90",
			wantMaxFiles: 20000,
			wantTimeout:  90,
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

			if config.MaxFilesPerGlob != tt.wantMaxFiles {
				t.Errorf(
					"ParseDSN(%q) MaxFilesPerGlob = %d, want %d",
					tt.dsn,
					config.MaxFilesPerGlob,
					tt.wantMaxFiles,
				)
			}

			if config.FileGlobTimeout != tt.wantTimeout {
				t.Errorf(
					"ParseDSN(%q) FileGlobTimeout = %d, want %d",
					tt.dsn,
					config.FileGlobTimeout,
					tt.wantTimeout,
				)
			}
		})
	}
}

func TestGlobSettingsParseMaxFilesPerGlob(t *testing.T) {
	validValues := []string{
		"1", "100", "10000", "50000", "1000000",
	}
	invalidValues := []string{
		"0", "-1", "1000001", "abc", "", "1.5",
	}

	for _, val := range validValues {
		_, err := parseMaxFilesPerGlob(val)
		if err != nil {
			t.Errorf(
				"parseMaxFilesPerGlob(%q) unexpected error: %v",
				val,
				err,
			)
		}
	}

	for _, val := range invalidValues {
		_, err := parseMaxFilesPerGlob(val)
		if err == nil {
			t.Errorf(
				"parseMaxFilesPerGlob(%q) expected error, got nil",
				val,
			)
		}
	}
}

func TestGlobSettingsParseFileGlobTimeout(t *testing.T) {
	validValues := []string{
		"1", "60", "120", "300", "600",
	}
	invalidValues := []string{
		"0", "-1", "601", "abc", "", "1.5",
	}

	for _, val := range validValues {
		_, err := parseFileGlobTimeout(val)
		if err != nil {
			t.Errorf(
				"parseFileGlobTimeout(%q) unexpected error: %v",
				val,
				err,
			)
		}
	}

	for _, val := range invalidValues {
		_, err := parseFileGlobTimeout(val)
		if err == nil {
			t.Errorf(
				"parseFileGlobTimeout(%q) expected error, got nil",
				val,
			)
		}
	}
}

// Note: SQL-based tests for SET/SHOW glob settings are in the compatibility package
// to avoid import cycles (compatibility/glob_settings_test.go)

func TestGlobSettingsDefaultConstants(t *testing.T) {
	// Verify the default constants have the expected values
	if DefaultMaxFilesPerGlob != 10000 {
		t.Errorf("DefaultMaxFilesPerGlob = %d, want 10000", DefaultMaxFilesPerGlob)
	}

	if DefaultFileGlobTimeout != 60 {
		t.Errorf("DefaultFileGlobTimeout = %d, want 60", DefaultFileGlobTimeout)
	}

	// Verify the range constants
	if minMaxFilesPerGlob != 1 {
		t.Errorf("minMaxFilesPerGlob = %d, want 1", minMaxFilesPerGlob)
	}

	if maxMaxFilesPerGlob != 1000000 {
		t.Errorf("maxMaxFilesPerGlob = %d, want 1000000", maxMaxFilesPerGlob)
	}

	if minFileGlobTimeout != 1 {
		t.Errorf("minFileGlobTimeout = %d, want 1", minFileGlobTimeout)
	}

	if maxFileGlobTimeout != 600 {
		t.Errorf("maxFileGlobTimeout = %d, want 600", maxFileGlobTimeout)
	}
}
