package dukdb

import (
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"strings"
)

// validAccessModes contains the valid access mode values.
var validAccessModes = map[string]bool{
	"automatic":  true,
	"read_only":  true,
	"read_write": true,
}

// validConfigKeys contains the recognized configuration keys.
var validConfigKeys = map[string]bool{
	"access_mode":        true,
	"threads":            true,
	"max_memory":         true,
	"storage_format":     true,
	"max_files_per_glob": true,
	"file_glob_timeout":  true,
}

// minThreads is the minimum number of threads allowed.
const minThreads = 1

// maxThreads is the maximum number of threads allowed.
const maxThreads = 128

// DefaultMaxFilesPerGlob is the default limit on files matched by a glob pattern.
// This limit prevents memory exhaustion when glob patterns match too many files.
const DefaultMaxFilesPerGlob = 10000

// minMaxFilesPerGlob is the minimum allowed value for max_files_per_glob.
const minMaxFilesPerGlob = 1

// maxMaxFilesPerGlob is the maximum allowed value for max_files_per_glob.
const maxMaxFilesPerGlob = 1000000

// DefaultFileGlobTimeout is the default timeout in seconds for cloud storage glob operations.
const DefaultFileGlobTimeout = 60

// minFileGlobTimeout is the minimum allowed timeout in seconds.
const minFileGlobTimeout = 1

// maxFileGlobTimeout is the maximum allowed timeout in seconds (10 minutes).
const maxFileGlobTimeout = 600

// ParseDSN parses a DSN string and returns a Config.
// The DSN format is: path?option=value&option2=value2
//
// Special path values:
//   - ":memory:" or empty string: in-memory database
//   - File path: persistent database (e.g., "/path/to/db.duckdb")
//
// Supported options:
//   - access_mode: "automatic", "read_only", "read_write" (default: "automatic")
//   - threads: number of threads, 1-128 (default: runtime.NumCPU())
//   - max_memory: memory limit, e.g., "4GB", "1024MB", "80%" (default: "80%")
//
// Returns an error with ErrorTypeSettings for unknown options.
func ParseDSN(dsn string) (*Config, error) {
	config := NewConfig()

	// Handle empty DSN (in-memory database)
	if dsn == "" {
		config.Path = ":memory:"

		return config, nil
	}

	// Handle :memory: database
	if dsn == ":memory:" {
		config.Path = ":memory:"

		return config, nil
	}

	// Handle :memory: with options
	if strings.HasPrefix(dsn, ":memory:?") {
		config.Path = ":memory:"
		queryStr := dsn[len(":memory:?"):]
		if err := parseOptions(queryStr, config); err != nil {
			return nil, err
		}

		return config, nil
	}

	// Handle query-only DSN (starts with ?)
	if strings.HasPrefix(dsn, "?") {
		config.Path = ":memory:"
		if err := parseOptions(dsn[1:], config); err != nil {
			return nil, err
		}

		return config, nil
	}

	// Handle file path with optional query parameters
	idx := strings.Index(dsn, "?")
	if idx == -1 {
		// No query parameters, just a path
		config.Path = dsn

		return config, nil
	}

	// Path with query parameters
	config.Path = dsn[:idx]
	if err := parseOptions(dsn[idx+1:], config); err != nil {
		return nil, err
	}

	return config, nil
}

// NewConfig creates a new Config with default values.
// Defaults:
//   - Path: "" (empty, will be set by ParseDSN)
//   - AccessMode: "automatic"
//   - Threads: runtime.NumCPU()
//   - MaxMemory: "80%"
//   - MaxFilesPerGlob: DefaultMaxFilesPerGlob (10000)
//   - FileGlobTimeout: DefaultFileGlobTimeout (60 seconds)
func NewConfig() *Config {
	return &Config{
		Path:            "",
		AccessMode:      "automatic",
		Threads:         runtime.NumCPU(),
		MaxMemory:       "80%",
		MaxFilesPerGlob: DefaultMaxFilesPerGlob,
		FileGlobTimeout: DefaultFileGlobTimeout,
	}
}

// parseOptions parses query parameters and updates the config.
// Returns an error with ErrorTypeSettings for unknown options.
func parseOptions(
	queryStr string,
	config *Config,
) error {
	if queryStr == "" {
		return nil
	}

	values, err := url.ParseQuery(queryStr)
	if err != nil {
		return &Error{
			Type: ErrorTypeSettings,
			Msg: fmt.Sprintf(
				"invalid query string: %v",
				err,
			),
		}
	}

	// Check for unknown options first
	for key := range values {
		if !validConfigKeys[key] {
			return &Error{
				Type: ErrorTypeSettings,
				Msg: fmt.Sprintf(
					"unknown option: %s",
					key,
				),
			}
		}
	}

	// Parse known options
	for key, vals := range values {
		if len(vals) == 0 {
			continue
		}
		value := vals[0]

		switch key {
		case "access_mode":
			if err := validateAccessMode(value); err != nil {
				return err
			}
			config.AccessMode = value

		case "threads":
			threads, err := parseThreads(value)
			if err != nil {
				return err
			}
			config.Threads = threads

		case "max_memory":
			if err := validateMaxMemory(value); err != nil {
				return err
			}
			config.MaxMemory = value

		case "storage_format":
			if err := validateStorageFormat(value); err != nil {
				return err
			}
			config.Format = value

		case "max_files_per_glob":
			maxFiles, err := parseMaxFilesPerGlob(value)
			if err != nil {
				return err
			}
			config.MaxFilesPerGlob = maxFiles

		case "file_glob_timeout":
			timeout, err := parseFileGlobTimeout(value)
			if err != nil {
				return err
			}
			config.FileGlobTimeout = timeout
		}
	}

	return nil
}

// validateAccessMode validates the access_mode option.
func validateAccessMode(mode string) error {
	if !validAccessModes[mode] {
		return &Error{
			Type: ErrorTypeSettings,
			Msg: fmt.Sprintf(
				"invalid access_mode: %s (must be one of: automatic, read_only, read_write)",
				mode,
			),
		}
	}

	return nil
}

// validStorageFormats contains the valid storage format values.
var validStorageFormats = map[string]bool{
	"auto":   true,
	"duckdb": true,
	"wal":    true,
}

// validateStorageFormat validates the storage_format option.
func validateStorageFormat(format string) error {
	if !validStorageFormats[format] {
		return &Error{
			Type: ErrorTypeSettings,
			Msg: fmt.Sprintf(
				"invalid storage_format: %s (must be one of: auto, duckdb, wal)",
				format,
			),
		}
	}

	return nil
}

// parseThreads parses and validates the threads option.
func parseThreads(value string) (int, error) {
	threads, err := strconv.Atoi(value)
	if err != nil {
		return 0, &Error{
			Type: ErrorTypeSettings,
			Msg: fmt.Sprintf(
				"invalid threads value: %s (must be an integer)",
				value,
			),
		}
	}

	if threads < minThreads ||
		threads > maxThreads {
		return 0, &Error{
			Type: ErrorTypeSettings,
			Msg: fmt.Sprintf(
				"threads must be between %d and %d, got %d",
				minThreads,
				maxThreads,
				threads,
			),
		}
	}

	return threads, nil
}

// validateMaxMemory validates the max_memory option.
// Valid formats: "4GB", "1024MB", "512KB", "80%"
func validateMaxMemory(value string) error {
	if value == "" {
		return nil
	}

	// Check for percentage format
	if strings.HasSuffix(value, "%") {
		percentStr := strings.TrimSuffix(
			value,
			"%",
		)
		percent, err := strconv.Atoi(percentStr)
		if err != nil {
			return &Error{
				Type: ErrorTypeSettings,
				Msg: fmt.Sprintf(
					"invalid max_memory percentage: %s",
					value,
				),
			}
		}
		if percent < 1 || percent > 100 {
			return &Error{
				Type: ErrorTypeSettings,
				Msg: fmt.Sprintf(
					"max_memory percentage must be between 1 and 100, got %d",
					percent,
				),
			}
		}

		return nil
	}

	// Check for byte format (case-insensitive)
	upperValue := strings.ToUpper(value)
	validSuffixes := []string{
		"TB",
		"GB",
		"MB",
		"KB",
		"B",
	}
	foundSuffix := false

	for _, suffix := range validSuffixes {
		if strings.HasSuffix(upperValue, suffix) {
			numStr := strings.TrimSuffix(
				upperValue,
				suffix,
			)
			_, err := strconv.ParseFloat(
				numStr,
				64,
			)
			if err != nil {
				return &Error{
					Type: ErrorTypeSettings,
					Msg: fmt.Sprintf(
						"invalid max_memory value: %s",
						value,
					),
				}
			}
			foundSuffix = true

			break
		}
	}

	if !foundSuffix {
		// Try parsing as raw number (bytes)
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return &Error{
				Type: ErrorTypeSettings,
				Msg: fmt.Sprintf(
					"invalid max_memory format: %s (use format like 4GB, 1024MB, or 80%%)",
					value,
				),
			}
		}
	}

	return nil
}

// ResolveMaxMemory resolves a max_memory value to bytes.
// If the value is a percentage, it returns the percentage of total system memory.
// Note: For percentage values, this function requires system memory information
// which may not be available on all platforms. In such cases, it returns an error.
func ResolveMaxMemory(
	value string,
) (int64, error) {
	if value == "" {
		return 0, nil
	}

	// Check for percentage format
	if strings.HasSuffix(value, "%") {
		percentStr := strings.TrimSuffix(
			value,
			"%",
		)
		percent, err := strconv.Atoi(percentStr)
		if err != nil {
			return 0, errors.New(
				"invalid percentage format",
			)
		}
		// For percentage resolution, we would need system memory info
		// Return a placeholder error for now - actual implementation depends on platform
		return 0, fmt.Errorf(
			"percentage memory resolution not implemented: %d%%",
			percent,
		)
	}

	// Parse byte format
	upperValue := strings.ToUpper(value)

	type unitMultiplier struct {
		suffix     string
		multiplier int64
	}

	units := []unitMultiplier{
		{"TB", 1024 * 1024 * 1024 * 1024},
		{"GB", 1024 * 1024 * 1024},
		{"MB", 1024 * 1024},
		{"KB", 1024},
		{"B", 1},
	}

	for _, unit := range units {
		if strings.HasSuffix(
			upperValue,
			unit.suffix,
		) {
			numStr := strings.TrimSuffix(
				upperValue,
				unit.suffix,
			)
			num, err := strconv.ParseFloat(
				numStr,
				64,
			)
			if err != nil {
				return 0, fmt.Errorf(
					"invalid number in max_memory: %s",
					value,
				)
			}

			return int64(
				num * float64(unit.multiplier),
			), nil
		}
	}

	// Try parsing as raw bytes
	bytes, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf(
			"invalid max_memory format: %s",
			value,
		)
	}

	return bytes, nil
}

// parseMaxFilesPerGlob parses and validates the max_files_per_glob option.
func parseMaxFilesPerGlob(value string) (int, error) {
	maxFiles, err := strconv.Atoi(value)
	if err != nil {
		return 0, &Error{
			Type: ErrorTypeSettings,
			Msg: fmt.Sprintf(
				"invalid max_files_per_glob value: %s (must be an integer)",
				value,
			),
		}
	}

	if maxFiles < minMaxFilesPerGlob ||
		maxFiles > maxMaxFilesPerGlob {
		return 0, &Error{
			Type: ErrorTypeSettings,
			Msg: fmt.Sprintf(
				"max_files_per_glob must be between %d and %d, got %d",
				minMaxFilesPerGlob,
				maxMaxFilesPerGlob,
				maxFiles,
			),
		}
	}

	return maxFiles, nil
}

// parseFileGlobTimeout parses and validates the file_glob_timeout option.
func parseFileGlobTimeout(value string) (int, error) {
	timeout, err := strconv.Atoi(value)
	if err != nil {
		return 0, &Error{
			Type: ErrorTypeSettings,
			Msg: fmt.Sprintf(
				"invalid file_glob_timeout value: %s (must be an integer in seconds)",
				value,
			),
		}
	}

	if timeout < minFileGlobTimeout ||
		timeout > maxFileGlobTimeout {
		return 0, &Error{
			Type: ErrorTypeSettings,
			Msg: fmt.Sprintf(
				"file_glob_timeout must be between %d and %d seconds, got %d",
				minFileGlobTimeout,
				maxFileGlobTimeout,
				timeout,
			),
		}
	}

	return timeout, nil
}
