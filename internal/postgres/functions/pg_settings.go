package functions

import (
	"fmt"
	"sync"
)

// Settings stores session-level configuration settings.
type Settings struct {
	mu       sync.RWMutex
	settings map[string]string
}

// NewSettings creates a new Settings instance with defaults.
func NewSettings() *Settings {
	return &Settings{
		settings: map[string]string{
			"server_version":                "15.0 (dukdb-go)",
			"server_version_num":            "150000",
			"server_encoding":               "UTF8",
			"client_encoding":               "UTF8",
			"is_superuser":                  "on",
			"session_authorization":         "dukdb",
			"DateStyle":                     "ISO, MDY",
			"IntervalStyle":                 "postgres",
			"TimeZone":                      "UTC",
			"integer_datetimes":             "on",
			"standard_conforming_strings":   "on",
			"application_name":              "",
			"search_path":                   "main, pg_catalog, information_schema",
			"default_transaction_isolation": "read committed",
			"transaction_isolation":         "read committed",
			"default_transaction_read_only": "off",
			"in_hot_standby":                "off",
			"max_identifier_length":         "63",
		},
	}
}

// Get returns the value of a setting.
func (s *Settings) Get(name string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.settings[name]

	return val, ok
}

// Set sets the value of a setting.
func (s *Settings) Set(name, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settings[name] = value
}

// CurrentSetting returns the value of a configuration setting.
type CurrentSetting struct {
	Settings *Settings
}

// NewCurrentSetting creates a CurrentSetting function.
func NewCurrentSetting(settings *Settings) *CurrentSetting {
	s := settings
	if s == nil {
		s = NewSettings()
	}

	return &CurrentSetting{Settings: s}
}

// EvaluateStrict returns the value of a setting.
// Returns an error for missing settings.
func (f *CurrentSetting) EvaluateStrict(name string) (string, error) {
	val, ok := f.Settings.Get(name)
	if !ok {
		return "", fmt.Errorf("unrecognized configuration parameter %q", name)
	}

	return val, nil
}

// EvaluateOptional returns the value of a setting.
// Returns empty string for missing settings.
func (f *CurrentSetting) EvaluateOptional(name string) string {
	val, _ := f.Settings.Get(name)

	return val
}

// SetConfig sets a configuration setting and returns the new value.
type SetConfig struct {
	Settings *Settings
}

// NewSetConfig creates a SetConfig function.
func NewSetConfig(settings *Settings) *SetConfig {
	s := settings
	if s == nil {
		s = NewSettings()
	}

	return &SetConfig{Settings: s}
}

// Evaluate sets a configuration parameter and returns the new value.
// isLocal determines if the setting is local to the current transaction.
func (f *SetConfig) Evaluate(name, value string, isLocal bool) string {
	_ = isLocal // We don't differentiate between local and session settings
	f.Settings.Set(name, value)

	return value
}

// Global default settings instance.
var defaultSettings = NewSettings()

// GetDefaultSettings returns the global default settings.
func GetDefaultSettings() *Settings {
	return defaultSettings
}
