package metadata

// SettingProvider provides access to session-level settings.
type SettingProvider interface {
	GetSetting(key string) string
}

var defaultDuckDBSettings = []SettingMetadata{
	{
		Name:        "threads",
		Value:       "0",
		Description: "Number of worker threads (0=auto)",
		InputType:   "INTEGER",
		Scope:       "GLOBAL",
	},
	{
		Name:        "memory_limit",
		Value:       "80%",
		Description: "Maximum memory limit",
		InputType:   "VARCHAR",
		Scope:       "GLOBAL",
	},
	{
		Name:        "max_memory",
		Value:       "80%",
		Description: "Maximum memory limit",
		InputType:   "VARCHAR",
		Scope:       "GLOBAL",
	},
	{
		Name:        "temp_directory",
		Value:       "/tmp",
		Description: "Temporary directory",
		InputType:   "VARCHAR",
		Scope:       "SESSION",
	},
	{
		Name:        "checkpoint_threshold",
		Value:       "256MB",
		Description: "WAL checkpoint threshold",
		InputType:   "VARCHAR",
		Scope:       "SESSION",
	},
	{
		Name:        "query_cache_enabled",
		Value:       "false",
		Description: "Enable query result cache",
		InputType:   "BOOLEAN",
		Scope:       "SESSION",
	},
	{
		Name:        "query_cache_max_bytes",
		Value:       "67108864",
		Description: "Maximum query cache size in bytes",
		InputType:   "VARCHAR",
		Scope:       "SESSION",
	},
	{
		Name:        "query_cache_ttl",
		Value:       "5m",
		Description: "Default query cache TTL",
		InputType:   "VARCHAR",
		Scope:       "SESSION",
	},
	{
		Name:        "query_cache_parameter_mode",
		Value:       "exact",
		Description: "Query cache parameter mode (exact|structure)",
		InputType:   "VARCHAR",
		Scope:       "SESSION",
	},
	{
		Name:        "access_mode",
		Value:       "automatic",
		Description: "Database access mode",
		InputType:   "VARCHAR",
		Scope:       "SESSION",
	},
	{
		Name:        "storage_format",
		Value:       "duckdb",
		Description: "Storage format",
		InputType:   "VARCHAR",
		Scope:       "SESSION",
	},
	{
		Name:        "max_files_per_glob",
		Value:       "10000",
		Description: "Maximum files per glob",
		InputType:   "INTEGER",
		Scope:       "SESSION",
	},
	{
		Name:        "file_glob_timeout",
		Value:       "60",
		Description: "Glob timeout in seconds",
		InputType:   "INTEGER",
		Scope:       "SESSION",
	},
}

// GetSettings returns metadata for supported settings.
func GetSettings(provider SettingProvider) []SettingMetadata {
	result := make([]SettingMetadata, 0, len(defaultDuckDBSettings))
	for _, setting := range defaultDuckDBSettings {
		value := setting.Value
		if provider != nil {
			if override := provider.GetSetting(setting.Name); override != "" {
				value = override
			}
		}
		result = append(result, SettingMetadata{
			Name:        setting.Name,
			Value:       value,
			Description: setting.Description,
			InputType:   setting.InputType,
			Scope:       setting.Scope,
		})
	}
	return result
}
